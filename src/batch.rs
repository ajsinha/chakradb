//! The columnar batch — Arrow arrays under a [`Schema`].
//!
//! M0–M2 used a hand-rolled struct-of-vectors. This is now backed by Apache
//! Arrow (`docs/dynamic-schema-design.md`, M3 spike): sealed parts hold Arrow
//! arrays, so the layout is columnar end to end, zero-copy to DataFusion, and
//! persistable as the open Arrow IPC format. A `Batch` is a thin wrapper over an
//! Arrow `RecordBatch` plus the ChakraDB [`Schema`] that names the key column.

use crate::schema::{Row, Schema};
use crate::value::{DataType, Value};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// A columnar batch of rows sharing one schema.
#[derive(Debug, Clone)]
pub struct Batch {
    schema: Schema,
    rb: RecordBatch,
}

impl Batch {
    /// An empty batch with the given schema.
    pub fn empty(schema: &Schema) -> Self {
        Batch {
            schema: schema.clone(),
            rb: RecordBatch::new_empty(schema.arrow()),
        }
    }

    /// Build a batch from rows, one Arrow array per column. Rows must match the
    /// schema arity (checked by the caller / `Schema::check_row`).
    pub fn from_rows(schema: &Schema, rows: &[Row]) -> Self {
        let cols = schema.columns();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
        for (ci, col) in cols.iter().enumerate() {
            arrays.push(build_column(col.ty, rows, ci));
        }
        Batch::from_arrays(schema, arrays)
    }

    /// Wrap pre-built Arrow arrays (used by the IPC/persist path).
    pub fn from_arrays(schema: &Schema, arrays: Vec<ArrayRef>) -> Self {
        let rb = RecordBatch::try_new(schema.arrow(), arrays)
            .expect("arrays match schema arity and types");
        Batch {
            schema: schema.clone(),
            rb,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub fn record_batch(&self) -> &RecordBatch {
        &self.rb
    }
    pub fn columns(&self) -> &[ArrayRef] {
        self.rb.columns()
    }
    pub fn len(&self) -> usize {
        self.rb.num_rows()
    }
    pub fn is_empty(&self) -> bool {
        self.rb.num_rows() == 0
    }

    /// Read the value at `(column, row)`.
    #[inline]
    pub fn value(&self, col: usize, i: usize) -> Value {
        array_value(self.rb.column(col), self.schema.column(col).ty, i)
    }

    /// The key value of row `i`.
    #[inline]
    pub fn key(&self, i: usize) -> Value {
        self.value(self.schema.key_index(), i)
    }

    /// Materialise row `i` across all columns.
    pub fn row(&self, i: usize) -> Row {
        let values = (0..self.schema.arity()).map(|c| self.value(c, i)).collect();
        Row::from_values(values)
    }

    /// Every key in order (used to build the part's Bloom filter and bounds).
    pub fn keys(&self) -> Vec<Value> {
        (0..self.len()).map(|i| self.key(i)).collect()
    }

    /// Materialise every row (row-at-a-time consumers: DELETE/UPDATE scans).
    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        (0..self.len()).map(move |i| self.row(i))
    }

    /// Select rows by ordinal into a new batch (columnar `take`). Used to
    /// materialise the visible subset of a partially-visible part.
    pub fn take(&self, indices: &[u32]) -> Batch {
        let idx = UInt32Array::from(indices.to_vec());
        let arrays: Vec<ArrayRef> = self
            .rb
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c, &idx, None).expect("take"))
            .collect();
        Batch::from_arrays(&self.schema, arrays)
    }

    /// Concatenate batches of the same schema into one.
    pub fn concat(schema: &Schema, batches: &[Batch]) -> Batch {
        let non_empty: Vec<&RecordBatch> = batches
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.record_batch())
            .collect();
        if non_empty.is_empty() {
            return Batch::empty(schema);
        }
        let rb = arrow::compute::concat_batches(&schema.arrow(), non_empty).expect("concat");
        Batch {
            schema: schema.clone(),
            rb,
        }
    }

    /// True if the key column is non-decreasing (the sorted-part invariant).
    pub fn is_sorted_by_key(&self) -> bool {
        (1..self.len()).all(|i| self.key(i - 1).total_cmp(&self.key(i)).is_le())
    }

    /// Encode the columns as an Arrow IPC stream (the open on-disk part format).
    pub fn to_ipc(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &self.schema.arrow())
                .expect("ipc writer");
            if !self.is_empty() {
                w.write(&self.rb).expect("ipc write");
            }
            w.finish().expect("ipc finish");
        }
        buf
    }

    /// Decode an Arrow IPC stream produced by [`Batch::to_ipc`], reattaching the
    /// ChakraDB schema (which carries the key column identity Arrow does not).
    pub fn from_ipc(schema: &Schema, bytes: &[u8]) -> Option<Batch> {
        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).ok()?;
        let mut batches = Vec::new();
        for rb in reader {
            batches.push(rb.ok()?);
        }
        if batches.is_empty() {
            return Some(Batch::empty(schema));
        }
        let rb = arrow::compute::concat_batches(&schema.arrow(), &batches).ok()?;
        Some(Batch {
            schema: schema.clone(),
            rb,
        })
    }

    /// Approximate resident bytes.
    pub fn memory_bytes(&self) -> usize {
        self.rb
            .columns()
            .iter()
            .map(|c| c.get_array_memory_size())
            .sum()
    }

    // ---- default-schema conveniences (M0–M2 tests and benches) ----
    /// An empty batch over [`Schema::default_schema`].
    pub fn new() -> Self {
        Batch::empty(&Schema::default_schema())
    }
    /// Capacity is advisory here (Arrow arrays are built at once); default shape.
    pub fn with_capacity(_n: usize) -> Self {
        Batch::new()
    }
}

impl Default for Batch {
    fn default() -> Self {
        Batch::new()
    }
}

/// Collect default-schema rows into a batch — used pervasively by the M0–M2
/// tests. Real engine code builds batches with an explicit schema.
impl FromIterator<Row> for Batch {
    fn from_iter<T: IntoIterator<Item = Row>>(iter: T) -> Self {
        let rows: Vec<Row> = iter.into_iter().collect();
        Batch::from_rows(&Schema::default_schema(), &rows)
    }
}

/// Build one Arrow column from `rows`, reading column `ci` of each.
fn build_column(ty: DataType, rows: &[Row], ci: usize) -> ArrayRef {
    match ty {
        DataType::Int => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for r in rows {
                match &r.values[ci] {
                    Value::Int(v) => b.append_value(*v),
                    Value::Null => b.append_null(),
                    other => b.append_value(other.as_int().unwrap_or(0)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for r in rows {
                match &r.values[ci] {
                    Value::Null => b.append_null(),
                    other => match other.as_f64() {
                        Some(f) => b.append_value(f),
                        None => b.append_null(),
                    },
                }
            }
            Arc::new(b.finish())
        }
        DataType::Text => {
            let mut b = StringBuilder::new();
            for r in rows {
                match &r.values[ci] {
                    Value::Text(s) => b.append_value(s),
                    Value::Null => b.append_null(),
                    other => b.append_value(other.render()),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Bool => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for r in rows {
                match &r.values[ci] {
                    Value::Bool(v) => b.append_value(*v),
                    Value::Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

/// Read one value from an Arrow array, honouring nulls.
#[inline]
fn array_value(arr: &ArrayRef, ty: DataType, i: usize) -> Value {
    if arr.is_null(i) {
        return Value::Null;
    }
    match ty {
        DataType::Int => Value::Int(
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column")
                .value(i),
        ),
        DataType::Float => Value::Float(
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .expect("float column")
                .value(i),
        ),
        DataType::Text => Value::Text(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .expect("text column")
                .value(i)
                .to_string(),
        ),
        DataType::Bool => Value::Bool(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .expect("bool column")
                .value(i),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, Schema};

    fn r(pk: i64) -> Row {
        Row::new(pk, pk * 2, pk as f64 / 2.0, format!("v{pk}"))
    }

    #[test]
    fn round_trips_rows_through_arrow() {
        let s = Schema::default_schema();
        let rows: Vec<Row> = (0..5).map(r).collect();
        let b = Batch::from_rows(&s, &rows);
        assert_eq!(b.len(), 5);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(&b.row(i), row);
        }
    }

    #[test]
    fn value_and_key_access() {
        let s = Schema::default_schema();
        let b = Batch::from_rows(&s, &[r(3), r(4)]);
        assert_eq!(b.value(0, 0), Value::Int(3));
        assert_eq!(b.value(3, 1), Value::Text("v4".into()));
        assert_eq!(b.key(1), Value::Int(4)); // key_index 0
    }

    #[test]
    fn nulls_survive_the_round_trip() {
        let s = Schema::default_schema();
        let row = Row::from_values(vec![Value::Int(1), Value::Null, Value::Null, Value::Null]);
        let b = Batch::from_rows(&s, std::slice::from_ref(&row));
        assert_eq!(b.row(0), row);
    }

    #[test]
    fn concat_and_sorted_check() {
        let s = Schema::default_schema();
        let a = Batch::from_rows(&s, &[r(0), r(1)]);
        let c = Batch::from_rows(&s, &[r(2), r(3)]);
        let all = Batch::concat(&s, &[a, c]);
        assert_eq!(all.len(), 4);
        assert!(all.is_sorted_by_key());
    }

    #[test]
    fn take_selects_a_subset() {
        let s = Schema::default_schema();
        let b = Batch::from_rows(&s, &[r(0), r(1), r(2), r(3)]);
        let sub = b.take(&[1, 3]);
        assert_eq!(sub.len(), 2);
        assert_eq!(sub.key(0), Value::Int(1));
        assert_eq!(sub.key(1), Value::Int(3));
    }

    #[test]
    fn text_key_batch() {
        let s = Schema::from_user_columns(vec![ColumnDef::new("email", DataType::Text)], Some(0));
        let rows = vec![
            Row::from_values(vec![Value::Text("a@x".into())]),
            Row::from_values(vec![Value::Text("b@x".into())]),
        ];
        let b = Batch::from_rows(&s, &rows);
        assert_eq!(b.key(0), Value::Text("a@x".into()));
        assert!(b.is_sorted_by_key());
    }

    #[test]
    fn empty_batch_has_schema() {
        let s = Schema::default_schema();
        let b = Batch::empty(&s);
        assert!(b.is_empty());
        assert_eq!(b.schema().arity(), 4);
    }
}
