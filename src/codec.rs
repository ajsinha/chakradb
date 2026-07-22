//! Binary encoding primitives and CRC-32.
//!
//! Everything ChakraDB writes to disk goes through here. Two properties matter
//! more than compactness:
//!
//! * **Self-describing lengths.** Every variable-length field is length-prefixed
//!   so a truncated record is detectable rather than misinterpreted.
//! * **Checksums on every record.** A torn write must be *rejected*, not read
//!   as plausible garbage. This is what makes crash recovery safe when the tail
//!   of a file was being written at the moment of the crash.
//!
//! Little-endian throughout, matching every target platform we care about.

use crate::schema::Row;
use crate::value::Value;

/// CRC-32 (IEEE 802.3), table-driven.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Append-only byte buffer.
#[derive(Debug, Default)]
pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(n: usize) -> Self {
        Encoder {
            buf: Vec::with_capacity(n),
        }
    }
    pub fn len(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn f64(&mut self, v: f64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
        self
    }
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u32(v.len() as u32);
        self.buf.extend_from_slice(v);
        self
    }
    pub fn str(&mut self, v: &str) -> &mut Self {
        self.bytes(v.as_bytes())
    }
    /// A single tagged value: 1 type byte then the payload. Self-describing, so
    /// a row of any schema decodes without knowing the schema.
    pub fn value(&mut self, v: &Value) -> &mut Self {
        match v {
            Value::Null => {
                self.u8(0);
            }
            Value::Int(i) => {
                self.u8(1).i64(*i);
            }
            Value::Float(f) => {
                self.u8(2).f64(*f);
            }
            Value::Text(s) => {
                self.u8(3).str(s);
            }
            Value::Bool(b) => {
                self.u8(4).u8(*b as u8);
            }
            Value::Decimal(m, s) => {
                // 128-bit mantissa as two u64 (hi, lo), then the scale.
                self.u8(5)
                    .u64((*m >> 64) as u64)
                    .u64(*m as u64)
                    .u32(*s);
            }
        }
        self
    }
    /// A row: arity then one tagged value per column.
    pub fn row(&mut self, r: &Row) -> &mut Self {
        self.u32(r.values.len() as u32);
        for v in &r.values {
            self.value(v);
        }
        self
    }

    /// A table schema: version, columns (name, type, nullable, default), key
    /// index, rowid flag, and table-level CHECK clauses.
    pub fn schema(&mut self, s: &crate::schema::Schema) -> &mut Self {
        self.u8(SCHEMA_VERSION);
        self.u32(s.arity() as u32);
        for c in s.columns() {
            self.str(&c.name).u8(datatype_tag(c.ty));
            if let crate::value::DataType::Decimal(p, sc) = c.ty {
                self.u8(p).u8(sc);
            }
            self.u8(c.nullable as u8);
            match &c.default {
                Some(v) => {
                    self.u8(1).value(v);
                }
                None => {
                    self.u8(0);
                }
            }
            // max_len: 0 sentinel = unbounded (a real VARCHAR(0) is meaningless).
            self.u32(c.max_len.unwrap_or(0));
        }
        self.u32(s.key_index() as u32).u8(s.synthetic_key() as u8);
        self.u32(s.checks().len() as u32);
        for chk in s.checks() {
            self.str(chk);
        }
        self
    }
}

/// Schema encoding version. v2 added per-column nullable + default and
/// table-level CHECK clauses; v3 added a per-column `VARCHAR(n)` max length;
/// v4 carries `DECIMAL(precision, scale)` parameters.
const SCHEMA_VERSION: u8 = 4;

fn datatype_tag(ty: crate::value::DataType) -> u8 {
    use crate::value::DataType::*;
    match ty {
        Int => 1,
        Float => 2,
        Text => 3,
        Bool => 4,
        Date => 5,
        Timestamp => 6,
        // Decimal is written with two extra bytes (precision, scale) by the
        // schema encoder — see `Encoder::schema`.
        Decimal(..) => 7,
    }
}

fn tag_datatype(tag: u8) -> Option<crate::value::DataType> {
    use crate::value::DataType::*;
    match tag {
        1 => Some(Int),
        2 => Some(Float),
        3 => Some(Text),
        4 => Some(Bool),
        5 => Some(Date),
        6 => Some(Timestamp),
        _ => None,
    }
}

/// Errors from decoding. All of them mean "this data is not trustworthy".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Ran off the end — the record was truncated.
    Truncated,
    /// Checksum mismatch — the bytes were corrupted or torn.
    BadChecksum,
    /// A tag or version we do not understand.
    Malformed(&'static str),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Truncated => write!(f, "truncated record"),
            DecodeError::BadChecksum => write!(f, "checksum mismatch"),
            DecodeError::Malformed(m) => write!(f, "malformed record: {m}"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub type DecodeResult<T> = Result<T, DecodeError>;

/// Cursor over a byte slice.
#[derive(Debug)]
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Decoder { buf, pos: 0 }
    }
    pub fn pos(&self) -> usize {
        self.pos
    }
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
    pub fn is_done(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn take(&mut self, n: usize) -> DecodeResult<&'a [u8]> {
        if self.remaining() < n {
            return Err(DecodeError::Truncated);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> DecodeResult<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> DecodeResult<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn f64(&mut self) -> DecodeResult<f64> {
        Ok(f64::from_bits(u64::from_le_bytes(
            self.take(8)?.try_into().unwrap(),
        )))
    }
    pub fn bytes(&mut self) -> DecodeResult<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub fn string(&mut self) -> DecodeResult<String> {
        let b = self.bytes()?;
        String::from_utf8(b.to_vec()).map_err(|_| DecodeError::Malformed("invalid utf-8"))
    }
    /// One tagged value (mirror of [`Encoder::value`]).
    pub fn value(&mut self) -> DecodeResult<Value> {
        Ok(match self.u8()? {
            0 => Value::Null,
            1 => Value::Int(self.i64()?),
            2 => Value::Float(self.f64()?),
            3 => Value::Text(self.string()?),
            4 => Value::Bool(self.u8()? != 0),
            5 => {
                let hi = self.u64()? as u128;
                let lo = self.u64()? as u128;
                let scale = self.u32()?;
                Value::Decimal(((hi << 64) | lo) as i128, scale)
            }
            _ => return Err(DecodeError::Malformed("bad value tag")),
        })
    }
    pub fn row(&mut self) -> DecodeResult<Row> {
        let n = self.u32()? as usize;
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            values.push(self.value()?);
        }
        Ok(Row::from_values(values))
    }

    /// Decode a table schema (mirror of [`Encoder::schema`]).
    pub fn schema(&mut self) -> DecodeResult<crate::schema::Schema> {
        use crate::schema::ColumnDef;
        let version = self.u8()?;
        if version != SCHEMA_VERSION {
            return Err(DecodeError::Malformed("unsupported schema version"));
        }
        let n = self.u32()? as usize;
        let mut columns = Vec::with_capacity(n);
        for _ in 0..n {
            let name = self.string()?;
            let tag = self.u8()?;
            let ty = if tag == 7 {
                crate::value::DataType::Decimal(self.u8()?, self.u8()?)
            } else {
                tag_datatype(tag).ok_or(DecodeError::Malformed("bad type tag"))?
            };
            let nullable = self.u8()? != 0;
            let default = if self.u8()? != 0 {
                Some(self.value()?)
            } else {
                None
            };
            let max_len = match self.u32()? {
                0 => None,
                n => Some(n),
            };
            let mut col = ColumnDef::new(name, ty);
            col.nullable = nullable;
            col.default = default;
            col.max_len = max_len;
            columns.push(col);
        }
        let key_index = self.u32()? as usize;
        let synthetic = self.u8()? != 0;
        if key_index >= columns.len() {
            return Err(DecodeError::Malformed("key_index out of range"));
        }
        let n_checks = self.u32()? as usize;
        let mut checks = Vec::with_capacity(n_checks);
        for _ in 0..n_checks {
            checks.push(self.string()?);
        }
        Ok(crate::schema::Schema::new(columns, key_index, synthetic).with_checks(checks))
    }
}

/// Wrap a payload as `[len u32][crc u32][payload]`.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 8);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Read one framed record starting at `pos`.
///
/// Returns `(payload, next_pos)`. A truncated or corrupt frame yields an error,
/// which recovery treats as "the log ends here" rather than as a fatal fault —
/// that is exactly the situation after a crash mid-write.
pub fn unframe(buf: &[u8], pos: usize) -> DecodeResult<(&[u8], usize)> {
    if buf.len() < pos + 8 {
        return Err(DecodeError::Truncated);
    }
    let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap());
    let start = pos + 8;
    if buf.len() < start + len {
        return Err(DecodeError::Truncated);
    }
    let payload = &buf[start..start + len];
    if crc32(payload) != crc {
        return Err(DecodeError::BadChecksum);
    }
    Ok((payload, start + len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_matches_known_vectors() {
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
    }

    #[test]
    fn crc_detects_single_bit_flips() {
        let data = b"the quick brown fox".to_vec();
        let base = crc32(&data);
        for i in 0..data.len() {
            for bit in 0..8 {
                let mut d = data.clone();
                d[i] ^= 1 << bit;
                assert_ne!(crc32(&d), base, "flip at {i}:{bit} undetected");
            }
        }
    }

    #[test]
    fn scalar_roundtrip() {
        let mut e = Encoder::new();
        e.u8(7).u32(0xDEAD_BEEF).u64(u64::MAX).i64(-42).f64(1.5);
        let mut d = Decoder::new(e.as_slice());
        assert_eq!(d.u8().unwrap(), 7);
        assert_eq!(d.u32().unwrap(), 0xDEAD_BEEF);
        assert_eq!(d.u64().unwrap(), u64::MAX);
        assert_eq!(d.i64().unwrap(), -42);
        assert_eq!(d.f64().unwrap(), 1.5);
        assert!(d.is_done());
    }

    #[test]
    fn float_edge_cases_roundtrip_bitwise() {
        for v in [0.0f64, -0.0, f64::MIN, f64::MAX, f64::INFINITY, -1e-300] {
            let mut e = Encoder::new();
            e.f64(v);
            let got = Decoder::new(e.as_slice()).f64().unwrap();
            assert_eq!(got.to_bits(), v.to_bits(), "{v} did not roundtrip");
        }
        let mut e = Encoder::new();
        e.f64(f64::NAN);
        assert!(Decoder::new(e.as_slice()).f64().unwrap().is_nan());
    }

    #[test]
    fn string_roundtrip_including_unicode_and_empty() {
        for s in ["", "hello", "日本語テキスト", "emoji 🎯 ok"] {
            let mut e = Encoder::new();
            e.str(s);
            assert_eq!(Decoder::new(e.as_slice()).string().unwrap(), s);
        }
    }

    #[test]
    fn row_roundtrip() {
        let r = Row::new(-5, 99, -0.25, "value");
        let mut e = Encoder::new();
        e.row(&r);
        assert_eq!(Decoder::new(e.as_slice()).row().unwrap(), r);
    }

    #[test]
    fn truncation_is_detected_at_every_offset() {
        let mut e = Encoder::new();
        e.row(&Row::new(1, 2, 3.0, "abc"));
        let full = e.into_vec();
        for cut in 0..full.len() {
            let mut d = Decoder::new(&full[..cut]);
            assert!(d.row().is_err(), "truncation at {cut} not detected");
        }
    }

    #[test]
    fn invalid_utf8_is_rejected() {
        let mut e = Encoder::new();
        e.bytes(&[0xFF, 0xFE]);
        assert_eq!(
            Decoder::new(e.as_slice()).string(),
            Err(DecodeError::Malformed("invalid utf-8"))
        );
    }

    #[test]
    fn frame_roundtrip() {
        let payload = b"some record";
        let framed = frame(payload);
        let (got, next) = unframe(&framed, 0).unwrap();
        assert_eq!(got, payload);
        assert_eq!(next, framed.len());
    }

    #[test]
    fn frames_chain_sequentially() {
        let mut buf = Vec::new();
        for i in 0..5u32 {
            buf.extend_from_slice(&frame(&i.to_le_bytes()));
        }
        let mut pos = 0;
        for i in 0..5u32 {
            let (p, next) = unframe(&buf, pos).unwrap();
            assert_eq!(p, i.to_le_bytes());
            pos = next;
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn corrupt_payload_fails_checksum() {
        let mut framed = frame(b"important data");
        framed[10] ^= 0xFF;
        assert_eq!(unframe(&framed, 0), Err(DecodeError::BadChecksum));
    }

    #[test]
    fn torn_frame_is_truncated_not_misread() {
        // This is the post-crash case: the tail was mid-write.
        let framed = frame(b"a record that did not finish");
        for cut in 0..framed.len() {
            assert!(
                unframe(&framed[..cut], 0).is_err(),
                "torn frame at {cut} accepted"
            );
        }
    }

    #[test]
    fn empty_payload_frames_cleanly() {
        let framed = frame(b"");
        let (p, next) = unframe(&framed, 0).unwrap();
        assert!(p.is_empty());
        assert_eq!(next, 8);
    }

    #[test]
    fn decoder_reports_position_and_remaining() {
        let mut e = Encoder::new();
        e.u64(1).u64(2);
        let mut d = Decoder::new(e.as_slice());
        assert_eq!(d.remaining(), 16);
        d.u64().unwrap();
        assert_eq!(d.pos(), 8);
        assert_eq!(d.remaining(), 8);
    }

    #[test]
    fn schema_roundtrips_constraints() {
        use crate::schema::{ColumnDef, Schema};
        use crate::value::{DataType, Value};
        let schema = Schema::new(
            vec![
                ColumnDef::new("id", DataType::Int).not_null(),
                ColumnDef::new("status", DataType::Text)
                    .with_default(Value::Text("new".into()))
                    .with_max_len(16),
                ColumnDef::new("age", DataType::Int),
            ],
            0,
            false,
        )
        .with_checks(vec!["age >= 0".into()]);

        let mut e = Encoder::new();
        e.schema(&schema);
        let got = Decoder::new(e.as_slice()).schema().unwrap();

        assert!(!got.column(0).nullable, "NOT NULL survives");
        assert_eq!(got.column(1).default, Some(Value::Text("new".into())));
        assert_eq!(got.column(1).max_len, Some(16), "VARCHAR length survives");
        assert!(got.column(2).nullable);
        assert_eq!(got.checks(), &["age >= 0".to_string()]);
        assert!(schema.same_shape(&got));
    }
}
