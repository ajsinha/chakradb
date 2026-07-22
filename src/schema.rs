//! Table schema and rows — arbitrary columns and types (the "be like DuckDB"
//! work).
//!
//! M0–M2 hard-coded `(pk i64, a i64, b f64, c String)`. This module replaces
//! that with a [`Schema`] describing any number of [`crate::value::Value`]
//! columns, and a row that is a `Vec<Value>`.
//!
//! The one idea that keeps the engine simple (`docs/dynamic-schema-design.md`):
//! **every table has exactly one key column.** It is either a user column
//! declared `PRIMARY KEY` (of any type) or a hidden auto-increment `_rowid`
//! synthesised when none is declared. The storage engine never learns which — it
//! just sorts, seeks, and blooms on a key column of `Value`s. So "PK-less" is not
//! a second code path; it is a table whose key is a hidden rowid.
//!
//! `default_schema()` reproduces the old four-column shape so the M0–M2 test
//! suite keeps exercising the engine through the new types.

use crate::error::{Error, Result};
use crate::value::{DataType, Value};
use arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema, SchemaRef};
use std::sync::Arc;

/// The hidden key column synthesised for a table declared without a PRIMARY KEY.
pub const ROWID: &str = "_rowid";

/// One column: a name and a declared type. Every column is nullable (SQL).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: DataType,
    /// `false` for a `NOT NULL` column (and for a user `PRIMARY KEY`, which is
    /// implicitly `NOT NULL`). Defaults to `true` — SQL columns are nullable.
    pub nullable: bool,
    /// A literal `DEFAULT` applied to this column when an `INSERT` omits it.
    pub default: Option<Value>,
    /// Maximum length in characters for a `VARCHAR(n)`/`CHAR(n)` column. `None`
    /// means unbounded text. Enforced at write time.
    pub max_len: Option<u32>,
}

impl ColumnDef {
    /// A plain nullable column with no default.
    pub fn new(name: impl Into<String>, ty: DataType) -> Self {
        ColumnDef {
            name: name.into(),
            ty,
            nullable: true,
            default: None,
            max_len: None,
        }
    }

    /// Mark this column `NOT NULL` (builder style).
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Give this column a literal `DEFAULT` (builder style).
    pub fn with_default(mut self, v: Value) -> Self {
        self.default = Some(v);
        self
    }

    /// Bound this text column to `n` characters (builder style).
    pub fn with_max_len(mut self, n: u32) -> Self {
        self.max_len = Some(n);
        self
    }
}

/// A table's shape: its columns, which one is the key, and whether that key is a
/// synthesised rowid (hidden from `SELECT *`).
#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<ColumnDef>,
    key_index: usize,
    synthetic_key: bool,
    /// Table-level `CHECK` predicates, stored as the original SQL text. Kept as
    /// text (not a parsed expression) so this core type stays free of the SQL
    /// layer; the executor parses and evaluates them against each written row.
    checks: Vec<String>,
    arrow: SchemaRef,
}

fn arrow_type(ty: DataType) -> ArrowType {
    use arrow::datatypes::TimeUnit;
    match ty {
        DataType::Int => ArrowType::Int64,
        DataType::Float => ArrowType::Float64,
        DataType::Text => ArrowType::Utf8,
        DataType::Bool => ArrowType::Boolean,
        // DATE/TIMESTAMP are physically integers but exposed to Arrow (and thus
        // DataFusion) as their native temporal types, so date functions work.
        DataType::Date => ArrowType::Date32,
        DataType::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, None),
        // Exact fixed-point over Arrow's 128-bit decimal, so DataFusion aggregates
        // it exactly too.
        DataType::Decimal(p, s) => ArrowType::Decimal128(p, s as i8),
    }
}

impl Schema {
    /// Build a schema from explicit columns and a key column index.
    pub fn new(columns: Vec<ColumnDef>, key_index: usize, synthetic_key: bool) -> Self {
        assert!(key_index < columns.len(), "key_index out of range");
        let arrow = Arc::new(ArrowSchema::new(
            columns
                .iter()
                .map(|c| Field::new(&c.name, arrow_type(c.ty), true))
                .collect::<Vec<_>>(),
        ));
        Schema {
            columns,
            key_index,
            synthetic_key,
            checks: Vec::new(),
            arrow,
        }
    }

    /// Attach table-level `CHECK` predicates (builder style). Each entry is the
    /// SQL text of one check clause.
    pub fn with_checks(mut self, checks: Vec<String>) -> Self {
        self.checks = checks;
        self
    }

    /// The table-level `CHECK` predicates, as SQL text.
    pub fn checks(&self) -> &[String] {
        &self.checks
    }

    /// User columns plus a hidden `_rowid` key appended at the end. `key` is the
    /// index of a user PRIMARY KEY column, or `None` for a rowid table.
    pub fn from_user_columns(mut columns: Vec<ColumnDef>, key: Option<usize>) -> Self {
        match key {
            Some(k) => {
                // A PRIMARY KEY column is implicitly NOT NULL.
                columns[k].nullable = false;
                Schema::new(columns, k, false)
            }
            None => {
                let key_index = columns.len();
                columns.push(ColumnDef::new(ROWID, DataType::Int));
                Schema::new(columns, key_index, true)
            }
        }
    }

    /// The M0–M2 shape: `(pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)`.
    pub fn default_schema() -> Self {
        Schema::new(
            vec![
                ColumnDef::new("pk", DataType::Int),
                ColumnDef::new("a", DataType::Int),
                ColumnDef::new("b", DataType::Float),
                ColumnDef::new("c", DataType::Text),
            ],
            0,
            false,
        )
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
    pub fn arity(&self) -> usize {
        self.columns.len()
    }
    pub fn key_index(&self) -> usize {
        self.key_index
    }
    pub fn synthetic_key(&self) -> bool {
        self.synthetic_key
    }
    pub fn key_type(&self) -> DataType {
        self.columns[self.key_index].ty
    }
    pub fn arrow(&self) -> SchemaRef {
        self.arrow.clone()
    }
    pub fn column(&self, i: usize) -> &ColumnDef {
        &self.columns[i]
    }

    /// Resolve a column name to its index (case-insensitive), including the
    /// hidden rowid.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Indices of the columns a bare `SELECT *` expands to — every column except
    /// a synthesised rowid key.
    pub fn star_indices(&self) -> Vec<usize> {
        (0..self.columns.len())
            .filter(|&i| !(self.synthetic_key && i == self.key_index))
            .collect()
    }

    /// Validate and type-coerce a row against this schema.
    pub fn check_row(&self, row: &Row) -> Result<()> {
        if row.values.len() != self.columns.len() {
            return Err(Error::SchemaMismatch(format!(
                "expected {} columns, got {}",
                self.columns.len(),
                row.values.len()
            )));
        }
        for (v, c) in row.values.iter().zip(&self.columns) {
            if !v.fits(c.ty) {
                return Err(Error::SchemaMismatch(format!(
                    "column {} expects {}, got {:?}",
                    c.name,
                    c.ty.name(),
                    v
                )));
            }
        }
        Ok(())
    }

    /// Fill any column an `INSERT` left as `NULL` with its declared `DEFAULT`.
    /// A column with no default keeps its `NULL` (which a later `NOT NULL` check
    /// may then reject). The synthesised rowid key is left untouched — the table
    /// assigns it.
    pub fn apply_defaults(&self, row: &mut Row) {
        for (i, c) in self.columns.iter().enumerate() {
            if matches!(row.values[i], Value::Null) {
                if let Some(d) = &c.default {
                    row.values[i] = d.clone();
                }
            }
        }
    }

    /// Reject a row that puts `NULL` in a `NOT NULL` column. A synthesised rowid
    /// key is exempt — it is assigned by the table after this check.
    pub fn check_not_null(&self, row: &Row) -> Result<()> {
        for (i, c) in self.columns.iter().enumerate() {
            if self.synthetic_key && i == self.key_index {
                continue;
            }
            if !c.nullable && matches!(row.values.get(i), Some(Value::Null) | None) {
                return Err(Error::ConstraintViolation(format!(
                    "NULL in NOT NULL column {}",
                    c.name
                )));
            }
        }
        Ok(())
    }

    /// Reject a row whose text exceeds a column's `VARCHAR(n)`/`CHAR(n)` length,
    /// measured in characters (Unicode scalar values), per SQL.
    pub fn check_lengths(&self, row: &Row) -> Result<()> {
        for (i, c) in self.columns.iter().enumerate() {
            if let (Some(max), Some(Value::Text(s))) = (c.max_len, row.values.get(i)) {
                let len = s.chars().count();
                if len > max as usize {
                    return Err(Error::ConstraintViolation(format!(
                        "value for {} is {len} chars, exceeds {}({max})",
                        c.name,
                        c.ty.name()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Two schemas are structurally equal (columns, key, rowid flag, checks).
    pub fn same_shape(&self, other: &Schema) -> bool {
        self.columns == other.columns
            && self.key_index == other.key_index
            && self.synthetic_key == other.synthetic_key
            && self.checks == other.checks
    }
}

// Equality ignores the cached Arrow schema, which is derived from the columns.
impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.same_shape(other)
    }
}
impl Eq for Schema {}

/// A single row: one `Value` per schema column, including the key column (and a
/// hidden rowid when the table has one).
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    /// General constructor.
    pub fn from_values(values: Vec<Value>) -> Self {
        Row { values }
    }

    /// Default-schema convenience `(pk, a, b, c)`, used pervasively by the M0–M2
    /// tests and benches. Equivalent to a row over [`Schema::default_schema`].
    pub fn new(pk: i64, a: i64, b: f64, c: impl Into<String>) -> Self {
        Row {
            values: vec![
                Value::Int(pk),
                Value::Int(a),
                Value::Float(b),
                Value::Text(c.into()),
            ],
        }
    }

    pub fn get(&self, i: usize) -> &Value {
        &self.values[i]
    }

    /// The key value at `key_index`.
    pub fn key(&self, key_index: usize) -> &Value {
        &self.values[key_index]
    }

    /// Heap bytes owned beyond the row's own vector.
    pub fn heap_bytes(&self) -> usize {
        self.values.iter().map(|v| v.heap_bytes()).sum()
    }

    // ---- default-schema accessors (compat with the M0–M2 tests) ----
    /// Default-schema `pk` (column 0 as i64). Panics off the default schema.
    pub fn pk(&self) -> i64 {
        self.values[0].as_int().expect("default-schema pk is Int")
    }
    pub fn a(&self) -> i64 {
        self.values[1].as_int().expect("default-schema a is Int")
    }
    pub fn b(&self) -> f64 {
        self.values[2].as_f64().expect("default-schema b is Float")
    }
    pub fn c(&self) -> String {
        self.values[2 + 1].render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_schema_is_the_m0_shape() {
        let s = Schema::default_schema();
        assert_eq!(s.arity(), 4);
        assert_eq!(s.key_index(), 0);
        assert!(!s.synthetic_key());
        assert_eq!(s.key_type(), DataType::Int);
        assert_eq!(s.column_index("c"), Some(3));
        assert_eq!(s.star_indices(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn rowid_table_hides_its_key() {
        let s = Schema::from_user_columns(
            vec![
                ColumnDef::new("name", DataType::Text),
                ColumnDef::new("age", DataType::Int),
            ],
            None,
        );
        assert!(s.synthetic_key());
        assert_eq!(s.key_index(), 2);
        assert_eq!(s.column(2).name, ROWID);
        // SELECT * skips the hidden rowid.
        assert_eq!(s.star_indices(), vec![0, 1]);
    }

    #[test]
    fn text_primary_key() {
        let s = Schema::from_user_columns(
            vec![
                ColumnDef::new("email", DataType::Text),
                ColumnDef::new("n", DataType::Int),
            ],
            Some(0),
        );
        assert!(!s.synthetic_key());
        assert_eq!(s.key_type(), DataType::Text);
    }

    #[test]
    fn check_row_enforces_arity_and_type() {
        let s = Schema::default_schema();
        assert!(s.check_row(&Row::new(1, 2, 3.0, "x")).is_ok());
        assert!(s.check_row(&Row::from_values(vec![Value::Int(1)])).is_err());
        let wrong = Row::from_values(vec![
            Value::Text("no".into()),
            Value::Int(2),
            Value::Float(3.0),
            Value::Text("x".into()),
        ]);
        assert!(s.check_row(&wrong).is_err(), "text in an int key column");
    }

    #[test]
    fn default_row_accessors() {
        let r = Row::new(7, 8, 9.5, "hi");
        assert_eq!(r.pk(), 7);
        assert_eq!(r.a(), 8);
        assert_eq!(r.b(), 9.5);
        assert_eq!(r.c(), "hi");
    }

    #[test]
    fn null_fits_every_column() {
        let s = Schema::default_schema();
        let r = Row::from_values(vec![Value::Int(1), Value::Null, Value::Null, Value::Null]);
        assert!(s.check_row(&r).is_ok());
    }
}
