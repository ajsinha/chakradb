//! SQL values — the column helpers over the core value type.
//!
//! There used to be a second `Value` here for the SQL layer. It has merged into
//! the engine's [`crate::value::Value`] (`docs/dynamic-schema-design.md` §7): one
//! scalar type with three-valued logic, NULL ordering, and coercion, shared by
//! storage and SQL. This module now only carries the helpers that read columns
//! out of a row or batch as values.

pub use crate::value::{DataType, Value};

use crate::batch::Batch;
use crate::schema::Row;

/// The default (M0) schema's column names, addressable by position. Dynamic
/// schemas resolve names through their own [`crate::schema::Schema`]; this array
/// backs the fixed-schema SQL path and its tests.
pub const COLUMNS: [&str; 4] = ["pk", "a", "b", "c"];

/// Resolve a default-schema column name to its index, case-insensitively.
pub fn column_index(name: &str) -> Option<usize> {
    let lower = name.to_ascii_lowercase();
    COLUMNS.iter().position(|c| *c == lower)
}

/// Read column `idx` of a row as a [`Value`].
pub fn row_value(row: &Row, idx: usize) -> Value {
    row.values.get(idx).cloned().unwrap_or(Value::Null)
}

/// Read column `idx` of batch row `i` as a [`Value`] — columnar, no `Row`
/// materialised.
#[inline]
pub fn batch_value(batch: &Batch, idx: usize, i: usize) -> Value {
    if idx < batch.schema().arity() {
        batch.value(idx, i)
    } else {
        Value::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_names_resolve() {
        assert_eq!(column_index("pk"), Some(0));
        assert_eq!(column_index("C"), Some(3));
        assert_eq!(column_index("nope"), None);
    }

    #[test]
    fn row_and_batch_values_agree() {
        let row = Row::new(1, 2, 3.0, "x");
        assert_eq!(row_value(&row, 0), Value::Int(1));
        assert_eq!(row_value(&row, 3), Value::Text("x".into()));
        let b = Batch::from_rows(&crate::schema::Schema::default_schema(), &[row]);
        assert_eq!(batch_value(&b, 2, 0), Value::Float(3.0));
    }
}
