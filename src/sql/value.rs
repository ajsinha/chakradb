//! SQL values and the M0 column schema.
//!
//! M2's SQL surface is defined over the fixed four-column schema from M0
//! (`schema.rs`): `(pk BIGINT, a BIGINT, b DOUBLE, c TEXT)`. A general type
//! system and DDL are explicitly *not* M2 — the goal is a real query surface
//! and the correctness harness that goes with it, not arbitrary schemas. That
//! keeps this file about SQL *semantics* (three-valued logic, coercion, NULL
//! ordering) rather than about a catalog.

use crate::schema::Row;
use std::cmp::Ordering;

/// A SQL scalar. `Null` is distinct from every other value, including itself,
/// under comparison — that is three-valued logic, and it is where most naive
/// SQL implementations go wrong.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Truthiness for `WHERE`/`HAVING`. `NULL` is *not* true — a row whose
    /// predicate evaluates to NULL is excluded, exactly as SQL requires.
    pub fn is_true(&self) -> bool {
        matches!(self, Value::Bool(true))
    }

    /// Numeric coercion for arithmetic and comparison. `None` for non-numbers.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// The type name, for error messages and `sqllogictest` column typing.
    pub fn type_char(&self) -> char {
        match self {
            Value::Int(_) | Value::Bool(_) => 'I',
            Value::Float(_) => 'R',
            Value::Text(_) => 'T',
            Value::Null => '?',
        }
    }

    /// Render for result output. NULL renders as `NULL`, matching most engines'
    /// text mode.
    pub fn render(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int(i) => i.to_string(),
            Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            Value::Float(f) => {
                if f.fract() == 0.0 && f.is_finite() {
                    format!("{f:.1}")
                } else {
                    format!("{f}")
                }
            }
            Value::Text(s) => s.clone(),
        }
    }

    /// SQL three-valued comparison. Returns `None` when either side is NULL —
    /// the caller turns that into a NULL result, not `false`.
    pub fn sql_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            _ => {
                let (a, b) = (self.as_f64()?, other.as_f64()?);
                a.partial_cmp(&b)
            }
        }
    }

    /// Total order for `ORDER BY`, where NULL must sort somewhere definite.
    /// SQL leaves NULL ordering implementation-defined; we sort NULLs first,
    /// matching SQLite (and therefore the sqllogictest corpus).
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            _ => {
                let a = self.as_f64().unwrap_or(f64::NAN);
                let b = other.as_f64().unwrap_or(f64::NAN);
                a.total_cmp(&b)
            }
        }
    }
}

/// The four columns of the M0 schema, addressable by name.
pub const COLUMNS: [&str; 4] = ["pk", "a", "b", "c"];

/// Resolve a column name to its index, case-insensitively.
pub fn column_index(name: &str) -> Option<usize> {
    let lower = name.to_ascii_lowercase();
    COLUMNS.iter().position(|c| *c == lower)
}

/// Read column `idx` of a row as a [`Value`].
pub fn row_value(row: &Row, idx: usize) -> Value {
    match idx {
        0 => Value::Int(row.pk),
        1 => Value::Int(row.a),
        2 => Value::Float(row.b),
        3 => Value::Text(row.c.clone()),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_is_distinguished() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int(0).is_null());
        assert!(!Value::Null.is_true());
    }

    #[test]
    fn truthiness_only_for_bool_true() {
        assert!(Value::Bool(true).is_true());
        assert!(!Value::Bool(false).is_true());
        assert!(!Value::Int(1).is_true(), "int is not a bool in WHERE");
        assert!(!Value::Null.is_true());
    }

    #[test]
    fn numeric_coercion() {
        assert_eq!(Value::Int(5).as_f64(), Some(5.0));
        assert_eq!(Value::Float(2.5).as_f64(), Some(2.5));
        assert_eq!(Value::Bool(true).as_f64(), Some(1.0));
        assert_eq!(Value::Text("x".into()).as_f64(), None);
        assert_eq!(Value::Null.as_f64(), None);
    }

    #[test]
    fn null_comparison_is_unknown() {
        assert_eq!(Value::Null.sql_cmp(&Value::Int(1)), None);
        assert_eq!(Value::Int(1).sql_cmp(&Value::Null), None);
        assert_eq!(Value::Null.sql_cmp(&Value::Null), None);
    }

    #[test]
    fn numeric_and_text_comparison() {
        assert_eq!(Value::Int(1).sql_cmp(&Value::Int(2)), Some(Ordering::Less));
        assert_eq!(
            Value::Float(2.0).sql_cmp(&Value::Int(2)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Value::Text("a".into()).sql_cmp(&Value::Text("b".into())),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn cross_type_number_text_is_unordered() {
        // Comparing a number to text yields no ordering (NULL result).
        assert_eq!(Value::Int(1).sql_cmp(&Value::Text("1".into())), None);
    }

    #[test]
    fn total_order_puts_nulls_first() {
        let mut v = [
            Value::Int(3),
            Value::Null,
            Value::Int(1),
            Value::Null,
            Value::Int(2),
        ];
        v.sort_by(|a, b| a.total_cmp(b));
        assert!(v[0].is_null() && v[1].is_null());
        assert_eq!(v[2].render(), "1");
        assert_eq!(v[4].render(), "3");
    }

    #[test]
    fn rendering() {
        assert_eq!(Value::Null.render(), "NULL");
        assert_eq!(Value::Int(-5).render(), "-5");
        assert_eq!(Value::Float(2.0).render(), "2.0");
        assert_eq!(Value::Float(2.5).render(), "2.5");
        assert_eq!(Value::Text("hi".into()).render(), "hi");
        assert_eq!(Value::Bool(true).render(), "1");
    }

    #[test]
    fn type_chars_match_sqllogictest_conventions() {
        assert_eq!(Value::Int(1).type_char(), 'I');
        assert_eq!(Value::Float(1.0).type_char(), 'R');
        assert_eq!(Value::Text("x".into()).type_char(), 'T');
    }

    #[test]
    fn column_resolution_is_case_insensitive() {
        assert_eq!(column_index("pk"), Some(0));
        assert_eq!(column_index("PK"), Some(0));
        assert_eq!(column_index("C"), Some(3));
        assert_eq!(column_index("missing"), None);
    }

    #[test]
    fn row_values_read_each_column() {
        let r = Row::new(7, 14, 3.5, "hello");
        assert_eq!(row_value(&r, 0).render(), "7");
        assert_eq!(row_value(&r, 1).render(), "14");
        assert_eq!(row_value(&r, 2).render(), "3.5");
        assert_eq!(row_value(&r, 3).render(), "hello");
    }
}
