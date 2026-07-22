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
    /// Exact fixed-point number `DECIMAL(precision, scale)` — `precision` total
    /// significant digits, `scale` digits after the point. Stored as an `i128`
    /// unscaled mantissa (Arrow `Decimal128`), never as `f64`, so money is exact.
    Decimal(u8, u8),
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
            DataType::Decimal(..) => "DECIMAL",
        }
    }

    /// The `(precision, scale)` of a `DECIMAL`, or `None` for other types.
    pub fn decimal_params(self) -> Option<(u8, u8)> {
        match self {
            DataType::Decimal(p, s) => Some((p, s)),
            _ => None,
        }
    }

    /// The sqllogictest column-type character. Dates and timestamps render as
    /// strings, so they report as text.
    pub fn type_char(self) -> char {
        match self {
            DataType::Int | DataType::Bool => 'I',
            DataType::Float | DataType::Decimal(..) => 'R',
            DataType::Text | DataType::Date | DataType::Timestamp => 'T',
        }
    }

    /// Parse a SQL type name (case-insensitive), accepting common aliases.
    pub fn parse(name: &str) -> Option<DataType> {
        // A parameterised type like `DECIMAL(10,2)` arrives as its head word.
        let head = name.split(['(', ' ']).next().unwrap_or(name);
        match head.to_ascii_lowercase().as_str() {
            "int" | "integer" | "bigint" | "smallint" | "tinyint" => Some(DataType::Int),
            "float" | "double" | "real" => Some(DataType::Float),
            // A bare DECIMAL/NUMERIC defaults to scale 0; the DDL layer supplies
            // the real precision/scale from `DECIMAL(p, s)`.
            "decimal" | "numeric" | "dec" => Some(DataType::Decimal(38, 0)),
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
    /// Exact fixed-point: `(mantissa, scale)` — the number is `mantissa / 10^scale`
    /// (e.g. `12.34` is `Decimal(1234, 2)`). Backs `DECIMAL(p, s)` columns.
    Decimal(i128, u32),
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
            Value::Decimal(m, s) => Some(*m as f64 / 10f64.powi(*s as i32)),
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
            // A decimal fits a decimal column of the *same* scale (coerce handles
            // rescaling); an integer widens into any decimal.
            (Value::Decimal(_, s), DataType::Decimal(_, ds)) => *s == ds as u32,
            (Value::Int(_), DataType::Decimal(..)) => true,
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
            // Into DECIMAL(_, scale): integers scale up exactly; a decimal rescales
            // (rounding to the column scale); a float rounds to the scale; a text
            // literal parses exactly then rescales.
            (Value::Int(i), DataType::Decimal(_, s)) => {
                rescale(*i as i128, 0, s as u32).map(|m| Value::Decimal(m, s as u32))
            }
            (Value::Decimal(m, from), DataType::Decimal(_, s)) => {
                rescale(*m, *from, s as u32).map(|m| Value::Decimal(m, s as u32))
            }
            (Value::Float(f), DataType::Decimal(_, s)) => {
                float_to_decimal(*f, s as u32).map(|m| Value::Decimal(m, s as u32))
            }
            (Value::Text(t), DataType::Decimal(_, s)) => parse_decimal(t)
                .and_then(|(m, from)| rescale(m, from, s as u32))
                .map(|m| Value::Decimal(m, s as u32)),
            // A decimal flowing into a FLOAT column becomes its (approximate) f64.
            (Value::Decimal(..), DataType::Float) => self.as_f64().map(Value::Float),
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
            Value::Float(_) | Value::Decimal(..) => 'R',
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
            Value::Decimal(m, s) => render_decimal(*m, *s),
        }
    }

    /// SQL three-valued comparison: `None` when either side is NULL.
    pub fn sql_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            _ => {
                if let Some(o) = cmp_decimal_exact(self, other) {
                    return Some(o);
                }
                self.as_f64()?.partial_cmp(&other.as_f64()?)
            }
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
                Value::Int(_) | Value::Float(_) | Value::Decimal(..) => 2,
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
            // Exact decimal ordering (also vs integers) — no f64 rounding.
            _ if cmp_decimal_exact(self, other).is_some() => {
                cmp_decimal_exact(self, other).unwrap()
            }
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

// --- Exact DECIMAL: i128 mantissa + scale ----------------------------------

/// Largest `scale` we support (an `i128` holds 38 decimal digits).
const MAX_DECIMAL_SCALE: u32 = 38;

/// `10^n` as `i128`, or `None` on overflow.
fn pow10(n: u32) -> Option<i128> {
    if n > MAX_DECIMAL_SCALE {
        return None;
    }
    10i128.checked_pow(n)
}

/// Rescale mantissa `m` from `from` to `to` decimal places. Increasing scale is
/// exact; decreasing rounds half-away-from-zero (SQL's `CAST` rounding).
pub(crate) fn rescale(m: i128, from: u32, to: u32) -> Option<i128> {
    match to.cmp(&from) {
        Ordering::Equal => Some(m),
        Ordering::Greater => m.checked_mul(pow10(to - from)?),
        Ordering::Less => {
            let div = pow10(from - to)?;
            let half = div / 2;
            let adj = if m >= 0 { m + half } else { m - half };
            Some(adj / div)
        }
    }
}

/// Exact ordering between two numeric values when at least one is a `Decimal`
/// (the other may be `Int` or `Decimal`). `None` if either side is neither.
fn cmp_decimal_exact(a: &Value, b: &Value) -> Option<Ordering> {
    if !matches!(a, Value::Decimal(..)) && !matches!(b, Value::Decimal(..)) {
        return None;
    }
    let as_dec = |v: &Value| -> Option<(i128, u32)> {
        match v {
            Value::Decimal(m, s) => Some((*m, *s)),
            Value::Int(i) => Some((*i as i128, 0)),
            _ => None,
        }
    };
    let (am, asc) = as_dec(a)?;
    let (bm, bsc) = as_dec(b)?;
    let scale = asc.max(bsc);
    // Align both to the common scale; fall back to f64 if the shift overflows.
    match (rescale(am, asc, scale), rescale(bm, bsc, scale)) {
        (Some(x), Some(y)) => Some(x.cmp(&y)),
        _ => a.as_f64()?.partial_cmp(&b.as_f64()?),
    }
}

/// Parse a plain decimal string (`-12.34`, `42`, `.5`) into `(mantissa, scale)`.
/// Rejects exponents and other float syntax — callers fall back to `f64` there.
fn parse_decimal(s: &str) -> Option<(i128, u32)> {
    let s = s.trim();
    if s.is_empty() || s.contains(['e', 'E']) {
        return None;
    }
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let scale = frac_part.len() as u32;
    if scale > MAX_DECIMAL_SCALE {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let digits = if digits.is_empty() { "0" } else { &digits };
    let mut m: i128 = digits.parse().ok()?;
    if neg {
        m = -m;
    }
    Some((m, scale))
}

/// Round a float to a decimal of the given scale (lossy — only for `f64` inputs).
fn float_to_decimal(f: f64, scale: u32) -> Option<i128> {
    if !f.is_finite() {
        return None;
    }
    let scaled = f * 10f64.powi(scale as i32);
    if scaled.abs() >= i128::MAX as f64 {
        return None;
    }
    Some(scaled.round() as i128)
}

/// Format a decimal mantissa/scale as its exact string (`1234`, `2` → `12.34`).
fn render_decimal(m: i128, scale: u32) -> String {
    if scale == 0 {
        return m.to_string();
    }
    let neg = m < 0;
    let digits = m.unsigned_abs().to_string();
    let scale = scale as usize;
    let s = if digits.len() > scale {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    } else {
        format!("0.{:0>width$}", digits, width = scale)
    };
    if neg {
        format!("-{s}")
    } else {
        s
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
        assert_eq!(DataType::parse("date"), Some(DataType::Date));
        // A bare DECIMAL defaults to scale 0; DDL supplies real precision/scale.
        assert_eq!(DataType::parse("decimal"), Some(DataType::Decimal(38, 0)));
        assert_eq!(DataType::parse("numeric"), Some(DataType::Decimal(38, 0)));
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

    // --- Exact decimal ----------------------------------------------------

    #[test]
    fn decimal_parse_and_render() {
        assert_eq!(parse_decimal("12.34"), Some((1234, 2)));
        assert_eq!(parse_decimal("-0.01"), Some((-1, 2)));
        assert_eq!(parse_decimal("42"), Some((42, 0)));
        assert_eq!(parse_decimal(".5"), Some((5, 1)));
        assert_eq!(parse_decimal("1e5"), None); // exponents rejected
        for (m, s, out) in [
            (1234i128, 2u32, "12.34"),
            (-1, 2, "-0.01"),
            (5, 0, "5"),
            (5, 3, "0.005"),
            (1000, 2, "10.00"),
        ] {
            assert_eq!(render_decimal(m, s), out, "render {m}/{s}");
        }
    }

    #[test]
    fn decimal_rescale_rounds_half_away() {
        assert_eq!(rescale(5, 0, 2), Some(500)); // 5 -> 5.00
        assert_eq!(rescale(1234, 2, 1), Some(123)); // 12.34 -> 12.3
        assert_eq!(rescale(1235, 2, 1), Some(124)); // 12.35 -> 12.4 (half up)
        assert_eq!(rescale(-1235, 2, 1), Some(-124)); // -12.35 -> -12.4
    }

    #[test]
    fn decimal_compares_exactly() {
        let a = Value::Decimal(1000, 2); // 10.00
        let b = Value::Decimal(100, 1); // 10.0
        assert_eq!(a.total_cmp(&b), Ordering::Equal);
        assert_eq!(a.sql_cmp(&b), Some(Ordering::Equal));
        assert_eq!(a.total_cmp(&Value::Int(10)), Ordering::Equal);
        assert_eq!(
            Value::Decimal(1001, 2).total_cmp(&Value::Int(10)),
            Ordering::Greater
        );
    }

    #[test]
    fn decimal_coercion() {
        // Text -> Decimal at a target scale (rescales exactly).
        assert_eq!(
            Value::Text("9.9".into()).coerce(DataType::Decimal(10, 2)),
            Some(Value::Decimal(990, 2))
        );
        // Int widens exactly.
        assert_eq!(
            Value::Int(5).coerce(DataType::Decimal(10, 2)),
            Some(Value::Decimal(500, 2))
        );
        // Decimal -> Float is the (approximate) f64.
        assert_eq!(
            Value::Decimal(1234, 2).coerce(DataType::Float),
            Some(Value::Float(12.34))
        );
        assert_eq!(Value::Decimal(1234, 2).render(), "12.34");
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
