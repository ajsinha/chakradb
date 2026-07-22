//! The core value type and the column type system.
//!
//! This is the foundation of the dynamic-schema work (the "be more like DuckDB"
//! request). The engine's row is currently a fixed struct
//! `(pk i64, a i64, b f64, c String)`; the migration plan in
//! `docs/dynamic-schema-design.md` replaces it with a `Vec<Value>` described by a
//! schema, so tables can have arbitrary columns and types.
//!
//! `Value` carries SQL semantics: three-valued logic, NULL ordering, and numeric
//! coercion. It is also the intended *key* type — under the plan a primary key
//! can be any type, compared by [`Value::total_cmp`], so the sorted-part index
//! works over strings and floats, not just integers. This module is committed
//! ahead of the migration because it is self-contained and testable in
//! isolation, and it is already what the SQL layer's value type should be.

use std::cmp::Ordering;

/// A column's declared type. `Null` is a *value*, not a type — every column is
/// nullable, matching SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    Text,
    Bool,
    /// A calendar date, physically an `Int` of days since 1970-01-01 (Arrow
    /// `Date32`). A *logical* type over `Value::Int` — no new `Value` variant.
    Date,
    /// A timestamp, physically an `Int` of microseconds since the Unix epoch
    /// (Arrow `Timestamp(Microsecond)`). Logical type over `Value::Int`.
    Timestamp,
}

impl DataType {
    pub fn name(self) -> &'static str {
        match self {
            DataType::Int => "INTEGER",
            DataType::Float => "DOUBLE",
            DataType::Text => "TEXT",
            DataType::Bool => "BOOLEAN",
            DataType::Date => "DATE",
            DataType::Timestamp => "TIMESTAMP",
        }
    }

    /// The sqllogictest column-type character. Dates and timestamps render as
    /// strings, so they report as text.
    pub fn type_char(self) -> char {
        match self {
            DataType::Int | DataType::Bool => 'I',
            DataType::Float => 'R',
            DataType::Text | DataType::Date | DataType::Timestamp => 'T',
        }
    }

    /// Parse a SQL type name (case-insensitive), accepting common aliases.
    pub fn parse(name: &str) -> Option<DataType> {
        // A parameterised type like `DECIMAL(10,2)` arrives as its head word.
        let head = name.split(['(', ' ']).next().unwrap_or(name);
        match head.to_ascii_lowercase().as_str() {
            "int" | "integer" | "bigint" | "smallint" | "tinyint" => Some(DataType::Int),
            "float" | "double" | "real" | "decimal" | "numeric" => Some(DataType::Float),
            "text" | "varchar" | "char" | "string" => Some(DataType::Text),
            "bool" | "boolean" => Some(DataType::Bool),
            "date" => Some(DataType::Date),
            "timestamp" | "datetime" => Some(DataType::Timestamp),
            _ => None,
        }
    }
}

/// A SQL scalar. `Null` is distinct from every other value under comparison —
/// that is three-valued logic, and getting it wrong is the most common source of
/// silently-wrong query answers.
#[derive(Debug, Clone, PartialEq)]
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

    /// Truthiness for `WHERE`/`HAVING`. `NULL` is not true.
    pub fn is_true(&self) -> bool {
        matches!(self, Value::Bool(true))
    }

    /// Definitely false — `NULL`/UNKNOWN is *not* false. A `CHECK` constraint is
    /// violated only by a definite FALSE, so this is the test it uses.
    pub fn is_false(&self) -> bool {
        matches!(self, Value::Bool(false))
    }

    /// Numeric coercion. `None` for non-numbers and NULL.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// The integer inside, if this is an `Int`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Does this value fit the declared type (NULL fits everything)?
    pub fn fits(&self, ty: DataType) -> bool {
        match (self, ty) {
            (Value::Null, _) => true,
            (Value::Int(_), DataType::Int) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Int(_), DataType::Float) => true, // widening
            (Value::Text(_), DataType::Text) => true,
            (Value::Bool(_), DataType::Bool) => true,
            // Date/Timestamp are physically Int (epoch days / micros).
            (Value::Int(_), DataType::Date | DataType::Timestamp) => true,
            _ => false,
        }
    }

    /// Coerce to a declared type where a lossless conversion exists.
    pub fn coerce(self, ty: DataType) -> Option<Value> {
        match (&self, ty) {
            (Value::Null, _) => Some(Value::Null),
            (Value::Int(i), DataType::Float) => Some(Value::Float(*i as f64)),
            // A `DATE '2024-01-15'` / `TIMESTAMP '...'` literal arrives as text and
            // is stored as its epoch integer.
            (Value::Text(s), DataType::Date) => parse_date(s).map(Value::Int),
            (Value::Text(s), DataType::Timestamp) => parse_timestamp(s).map(Value::Int),
            (v, t) if v.fits(t) => Some(self),
            _ => None,
        }
    }

    /// Render this value as it should appear for a column of type `ty`. For a
    /// `DATE`/`TIMESTAMP` column (physically an `Int`) this formats the epoch
    /// integer back to `YYYY-MM-DD[ HH:MM:SS]`; otherwise it is plain
    /// [`Value::render`].
    pub fn render_as(&self, ty: DataType) -> String {
        match (self, ty) {
            (Value::Int(d), DataType::Date) => render_date(*d),
            (Value::Int(t), DataType::Timestamp) => render_timestamp(*t),
            _ => self.render(),
        }
    }

    pub fn type_char(&self) -> char {
        match self {
            Value::Int(_) | Value::Bool(_) => 'I',
            Value::Float(_) => 'R',
            Value::Text(_) => 'T',
            Value::Null => '?',
        }
    }

    /// Render for result output. NULL renders as the empty string in text mode
    /// (matching sqllogictest), except where a caller overrides.
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

    /// SQL three-valued comparison: `None` when either side is NULL.
    pub fn sql_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            _ => self.as_f64()?.partial_cmp(&other.as_f64()?),
        }
    }

    /// Total order for sorting and for the sorted-part key index. NULLs sort
    /// first (SQLite convention). Cross-type order is by a fixed type rank so it
    /// is at least deterministic.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Int(_) | Value::Float(_) => 2,
                Value::Text(_) => 3,
            }
        }
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            // Compare two integers exactly. Routing through f64 (below) would
            // conflate integers beyond 2^53 — fatal for an integer key column.
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            _ => match (self.as_f64(), other.as_f64()) {
                (Some(a), Some(b)) => a.total_cmp(&b),
                _ => rank(self).cmp(&rank(other)),
            },
        }
    }

    /// Approximate heap bytes owned beyond the enum itself.
    pub fn heap_bytes(&self) -> usize {
        match self {
            Value::Text(s) => s.capacity(),
            _ => 0,
        }
    }
}

// --- Temporal encoding: DATE (days) / TIMESTAMP (micros) since 1970-01-01 ---
//
// DATE and TIMESTAMP are logical types stored as `Value::Int`. Conversions use
// Howard Hinnant's proleptic-Gregorian civil<->days algorithm — exact, branchy,
// and dependency-free (the core crate is `forbid(unsafe_code)` and has no chrono).

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Days since 1970-01-01 for a proleptic-Gregorian `(year, month, day)`.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Inverse of [`days_from_civil`]: `(year, month, day)` for a day number.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Parse `YYYY-MM-DD` into days since the epoch.
fn parse_date(s: &str) -> Option<i64> {
    let s = s.trim();
    let (y, m, d) = parse_ymd(s)?;
    Some(days_from_civil(y, m, d))
}

fn parse_ymd(s: &str) -> Option<(i64, i64, i64)> {
    let mut it = s.splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
}

/// Parse `YYYY-MM-DD[ T]HH:MM:SS[.ffffff]` (time optional) into micros since epoch.
fn parse_timestamp(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_part, time_part) = match s.split_once(['T', ' ']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let days = parse_date(date_part)?;
    let micros_of_day = match time_part {
        None => 0,
        Some(t) => parse_time_micros(t)?,
    };
    Some(days * MICROS_PER_DAY + micros_of_day)
}

/// Parse `HH:MM:SS[.ffffff]` into microseconds within a day.
fn parse_time_micros(t: &str) -> Option<i64> {
    let mut it = t.splitn(3, ':');
    let h: i64 = it.next()?.parse().ok()?;
    let mi: i64 = it.next()?.parse().ok()?;
    let (sec, frac) = match it.next() {
        Some(rest) => match rest.split_once('.') {
            Some((s, f)) => {
                // Right-pad/truncate the fraction to 6 digits (microseconds).
                let mut digits = f.chars().take(6).collect::<String>();
                while digits.len() < 6 {
                    digits.push('0');
                }
                (s.parse::<i64>().ok()?, digits.parse::<i64>().ok()?)
            }
            None => (rest.parse::<i64>().ok()?, 0),
        },
        None => (0, 0),
    };
    if !(0..=23).contains(&h) || !(0..=59).contains(&mi) || !(0..=60).contains(&sec) {
        return None;
    }
    Some(((h * 60 + mi) * 60 + sec) * 1_000_000 + frac)
}

/// Format days-since-epoch as `YYYY-MM-DD`.
fn render_date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Format micros-since-epoch as `YYYY-MM-DD HH:MM:SS[.ffffff]`.
fn render_timestamp(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let rem = micros.rem_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    let (hh, mm, ss) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if frac == 0 {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{frac:06}")
    }
}

/// A `Value` wrapped to be a total-ordered map/set key, ordered by
/// [`Value::total_cmp`]. This is the engine's key type: it lets a `BTreeMap` /
/// `BTreeSet` key on primary keys of any type (int, text, float, bool), which is
/// what "any-type PK" needs. Grouping, sorting, and DISTINCT reuse it too.
#[derive(Clone, Debug)]
pub struct Key(pub Value);

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0).is_eq()
    }
}
impl Eq for Key {}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_orders_and_dedups_like_total_cmp() {
        use std::collections::BTreeSet;
        let mut s = BTreeSet::new();
        s.insert(Key(Value::Int(2)));
        s.insert(Key(Value::Int(2))); // dup
        s.insert(Key(Value::Text("a".into())));
        assert_eq!(s.len(), 2);
        // Large integers must not collide (regression: f64 coercion lost these).
        let mut t = BTreeSet::new();
        t.insert(Key(Value::Int(9_007_199_254_740_993)));
        t.insert(Key(Value::Int(9_007_199_254_740_992)));
        assert_eq!(t.len(), 2, "large ints must stay distinct keys");
    }

    #[test]
    fn datatype_parsing_and_aliases() {
        assert_eq!(DataType::parse("INT"), Some(DataType::Int));
        assert_eq!(DataType::parse("bigint"), Some(DataType::Int));
        assert_eq!(DataType::parse("Double"), Some(DataType::Float));
        assert_eq!(DataType::parse("VARCHAR"), Some(DataType::Text));
        assert_eq!(DataType::parse("boolean"), Some(DataType::Bool));
        assert_eq!(DataType::parse("blob"), None);
    }

    #[test]
    fn null_semantics() {
        assert!(Value::Null.is_null());
        assert!(!Value::Null.is_true());
        assert_eq!(Value::Null.as_f64(), None);
        assert_eq!(Value::Null.sql_cmp(&Value::Int(1)), None);
    }

    #[test]
    fn fits_and_coerce() {
        assert!(Value::Int(1).fits(DataType::Int));
        assert!(Value::Int(1).fits(DataType::Float), "int widens to float");
        assert!(!Value::Text("x".into()).fits(DataType::Int));
        assert!(Value::Null.fits(DataType::Bool));

        assert_eq!(
            Value::Int(3).coerce(DataType::Float),
            Some(Value::Float(3.0))
        );
        assert_eq!(Value::Text("x".into()).coerce(DataType::Int), None);
    }

    #[test]
    fn total_order_across_types_is_deterministic() {
        let mut v = vec![
            Value::Text("z".into()),
            Value::Int(5),
            Value::Null,
            Value::Bool(true),
            Value::Float(2.0),
        ];
        v.sort_by(|a, b| a.total_cmp(b));
        assert!(v[0].is_null(), "nulls sort first");
        // And the sort is stable across runs.
        let mut v2 = v.clone();
        v2.sort_by(|a, b| a.total_cmp(b));
        assert_eq!(v, v2);
    }

    #[test]
    fn total_order_within_ints() {
        let mut v = [Value::Int(3), Value::Int(1), Value::Int(2)];
        v.sort_by(|a, b| a.total_cmp(b));
        assert_eq!(v[0].as_int(), Some(1));
        assert_eq!(v[2].as_int(), Some(3));
    }

    #[test]
    fn text_keys_order_correctly() {
        // Any-type PK: strings must order as keys.
        let mut v = [
            Value::Text("banana".into()),
            Value::Text("apple".into()),
            Value::Text("cherry".into()),
        ];
        v.sort_by(|a, b| a.total_cmp(b));
        assert_eq!(v[0].render(), "apple");
        assert_eq!(v[2].render(), "cherry");
    }

    #[test]
    fn rendering() {
        assert_eq!(Value::Int(-5).render(), "-5");
        assert_eq!(Value::Float(2.0).render(), "2.0");
        assert_eq!(Value::Text("hi".into()).render(), "hi");
    }

    #[test]
    fn heap_accounting() {
        assert_eq!(Value::Int(1).heap_bytes(), 0);
        assert!(Value::Text("hello".into()).heap_bytes() >= 5);
    }

    // --- Temporal encoding ------------------------------------------------

    #[test]
    fn date_epoch_and_known_dates() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("1970-01-02"), Some(1));
        assert_eq!(parse_date("1969-12-31"), Some(-1));
        // 2000-01-01 is 10957 days after the epoch.
        assert_eq!(parse_date("2000-01-01"), Some(10957));
    }

    #[test]
    fn date_round_trips_render() {
        for s in ["1970-01-01", "1999-12-31", "2024-02-29", "2100-03-01", "1900-01-01"] {
            let days = parse_date(s).unwrap();
            assert_eq!(render_date(days), s, "round trip {s}");
        }
    }

    #[test]
    fn timestamp_parse_and_render() {
        let micros = parse_timestamp("2024-01-15 13:45:06").unwrap();
        assert_eq!(render_timestamp(micros), "2024-01-15 13:45:06");
        // Date-only timestamp is midnight; 'T' separator is accepted.
        assert_eq!(
            parse_timestamp("2024-01-15"),
            parse_timestamp("2024-01-15 00:00:00")
        );
        assert_eq!(
            parse_timestamp("2024-01-15T13:45:06"),
            parse_timestamp("2024-01-15 13:45:06")
        );
    }

    #[test]
    fn timestamp_fraction_round_trips() {
        let m = parse_timestamp("2024-01-15 00:00:00.123456").unwrap();
        assert_eq!(render_timestamp(m), "2024-01-15 00:00:00.123456");
    }

    #[test]
    fn temporal_coercion_and_rendering() {
        // Text literal coerces to the epoch integer; render_as formats it back.
        let d = Value::Text("2024-01-15".into()).coerce(DataType::Date).unwrap();
        assert!(matches!(d, Value::Int(_)));
        assert_eq!(d.render_as(DataType::Date), "2024-01-15");
        // render_as is a no-op for non-temporal types.
        assert_eq!(Value::Int(19737).render_as(DataType::Int), "19737");
        assert_eq!(Value::Text("x".into()).render_as(DataType::Text), "x");
        // A bad date literal does not coerce.
        assert_eq!(Value::Text("nope".into()).coerce(DataType::Date), None);
    }
}
