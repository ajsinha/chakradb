//! Part serialisation.
//!
//! A sealed part is written once and never modified, so its file format can be
//! simple: a checksummed summary frame, then the immutable image (version
//! stamps, the ChakraDB schema, and the columns as an Arrow IPC stream), then
//! appended tombstone frames.
//!
//! The columns are stored as **Arrow IPC** (`docs/dynamic-schema-design.md`):
//! the open, columnar on-disk format that the M3 spike settled on. The schema is
//! written alongside because Arrow's own schema does not record which column is
//! the primary key.
//!
//! Deletion vectors *do* change after the part is written. Rather than rewrite
//! the file, tombstones are appended as framed records after the base image —
//! the same append-only, checksum-per-record discipline as the WAL, so a torn
//! append is detected and discarded rather than misread.

use crate::batch::Batch;
use crate::codec::{frame, unframe, DecodeError, Decoder, Encoder};
use crate::csn::Csn;
use crate::io::{File, Io};
use crate::part::{CreatedCsns, Part};
use std::io;
use std::sync::Arc;

const PART_MAGIC: u32 = 0x4348_4B50; // "CHKP"
const PART_VERSION: u8 = 3; // v3: Arrow-IPC columns + embedded schema
const SUMMARY_MAGIC: u32 = 0x4348_4B53; // "CHKS"

const STAMPS_UNIFORM: u8 = 0;
const STAMPS_PER_ROW: u8 = 1;

/// Encode the resident summary: bounds, row count, version range.
///
/// Written **first** in the file so that opening a database can read a bounded
/// prefix per part rather than the whole thing. That is what makes time-to-first
/// -query independent of database size (FR-06b) — see `pager.rs`.
pub fn encode_summary(part: &Part) -> Vec<u8> {
    let mut e = Encoder::with_capacity(64);
    e.u32(SUMMARY_MAGIC)
        .u8(PART_VERSION)
        .u64(part.id())
        .u64(part.num_rows() as u64)
        .value(part.min_key())
        .value(part.max_key())
        .u64(part.created_min())
        .u64(part.created_max());
    frame(e.as_slice())
}

fn decode_summary(payload: &[u8]) -> Result<crate::pager::PartSummary, DecodeError> {
    let mut d = Decoder::new(payload);
    if d.u32()? != SUMMARY_MAGIC {
        return Err(DecodeError::Malformed("bad summary magic"));
    }
    if d.u8()? != PART_VERSION {
        return Err(DecodeError::Malformed("unsupported part version"));
    }
    Ok(crate::pager::PartSummary {
        id: d.u64()?,
        num_rows: d.u64()? as usize,
        min_key: d.value()?,
        max_key: d.value()?,
        created_min: d.u64()?,
        created_max: d.u64()?,
    })
}

/// Read only the summary frame — a bounded read, regardless of part size.
pub fn read_part_summary(io: &dyn Io, path: &str) -> io::Result<crate::pager::PartSummary> {
    let f = io.open(path)?;
    // The summary frame is first and small; 8 bytes of header tell us its size.
    let mut head = [0u8; 8];
    let n = f.pread(0, &mut head)?;
    if n < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "part file too short for a summary",
        ));
    }
    let len = u32::from_le_bytes(head[0..4].try_into().unwrap()) as usize;
    let mut buf = vec![0u8; 8 + len];
    f.pread(0, &mut buf)?;
    let (payload, _) = unframe(&buf, 0)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    decode_summary(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Encode a part's immutable image (everything except later tombstones).
pub fn encode_part(part: &Part) -> Vec<u8> {
    let batch = part.batch();
    let n = batch.len();
    let mut e = Encoder::with_capacity(n * 24 + 128);

    e.u32(PART_MAGIC).u8(PART_VERSION).u64(part.id()).u64(n as u64);

    // Version stamps. Uniform costs 9 bytes total rather than 8 per row —
    // the M0-2 finding that this is ~86% of the index budget.
    if part.created_is_uniform() {
        e.u8(STAMPS_UNIFORM).u64(part.created_min());
    } else {
        e.u8(STAMPS_PER_ROW);
        for i in 0..n {
            e.u64(part.created_at(i));
        }
    }

    // Schema (names, types, key column) then the columns as an Arrow IPC stream.
    e.schema(batch.schema());
    e.bytes(&batch.to_ipc());

    frame(e.as_slice())
}

/// Encode a batch of tombstones for appending.
pub fn encode_tombstones(entries: &[(u32, Csn)]) -> Vec<u8> {
    let mut e = Encoder::with_capacity(entries.len() * 12 + 8);
    e.u32(entries.len() as u32);
    for &(ord, csn) in entries {
        e.u32(ord).u64(csn);
    }
    frame(e.as_slice())
}

fn decode_part_image(payload: &[u8]) -> Result<(u64, Batch, CreatedCsns), DecodeError> {
    let mut d = Decoder::new(payload);
    if d.u32()? != PART_MAGIC {
        return Err(DecodeError::Malformed("bad part magic"));
    }
    if d.u8()? != PART_VERSION {
        return Err(DecodeError::Malformed("unsupported part version"));
    }
    let id = d.u64()?;
    let n = d.u64()? as usize;

    let created = match d.u8()? {
        STAMPS_UNIFORM => CreatedCsns::Uniform(d.u64()?),
        STAMPS_PER_ROW => {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push(d.u64()?);
            }
            CreatedCsns::PerRow(v)
        }
        _ => return Err(DecodeError::Malformed("bad stamp encoding")),
    };

    let schema = d.schema()?;
    let ipc = d.bytes()?;
    let batch = Batch::from_ipc(&schema, ipc).ok_or(DecodeError::Malformed("bad ipc stream"))?;
    if batch.len() != n {
        return Err(DecodeError::Malformed("row count mismatch"));
    }
    Ok((id, batch, created))
}

fn decode_tombstones(payload: &[u8]) -> Result<Vec<(u32, Csn)>, DecodeError> {
    let mut d = Decoder::new(payload);
    let n = d.u32()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push((d.u32()?, d.u64()?));
    }
    Ok(out)
}

/// Write a part to `path` and make it durable.
pub fn write_part(io: &dyn Io, path: &str, part: &Part) -> io::Result<Arc<dyn File>> {
    let f = io.open(path)?;
    f.truncate(0)?;
    let mut img = encode_summary(part);
    img.extend_from_slice(&encode_part(part));
    f.pwrite(0, &img)?;
    let dv = part.dv_snapshot();
    let entries = dv.entries_after(0);
    if !entries.is_empty() {
        let len = f.len()?;
        f.pwrite(len, &encode_tombstones(&entries))?;
    }
    f.sync()?;
    Ok(f)
}

/// Append newly-recorded tombstones to an existing part file.
pub fn append_tombstones(file: &dyn File, entries: &[(u32, Csn)]) -> io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let len = file.len()?;
    file.pwrite(len, &encode_tombstones(entries))?;
    file.sync()
}

/// Read a part back, applying every intact tombstone record.
///
/// A torn trailing record is discarded: those tombstones were never
/// acknowledged as durable, exactly as with the WAL.
pub fn read_part(io: &dyn Io, path: &str) -> io::Result<Part> {
    let f = io.open(path)?;
    let len = f.len()? as usize;
    let mut buf = vec![0u8; len];
    if len > 0 {
        f.pread(0, &mut buf)?;
    }
    decode_part_file(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Decode a complete part file image.
pub fn decode_part_file(buf: &[u8]) -> Result<Part, DecodeError> {
    // Frame 0 is the summary; frame 1 is the image; the rest are tombstones.
    let (summary_payload, after_summary) = unframe(buf, 0)?;
    decode_summary(summary_payload)?;
    let (payload, mut pos) = unframe(buf, after_summary)?;
    let (id, batch, created) = decode_part_image(payload)?;

    let mut tombstones = Vec::new();
    while pos < buf.len() {
        match unframe(buf, pos) {
            Ok((p, next)) => match decode_tombstones(p) {
                Ok(mut t) => {
                    tombstones.append(&mut t);
                    pos = next;
                }
                Err(_) => break, // torn tail; stop here
            },
            Err(_) => break,
        }
    }

    Ok(Part::with_deletions(id, batch, created, &tombstones))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csn::Snapshot;
    use crate::io::MemIo;
    use crate::schema::Row;
    use crate::value::Value;

    fn part_of(id: u64, pks: &[i64], csn: Csn) -> Part {
        let batch: Batch = pks
            .iter()
            .map(|&pk| Row::new(pk, pk * 3, pk as f64 / 2.0, format!("row-{pk}")))
            .collect();
        Part::new(id, batch, CreatedCsns::Uniform(csn))
    }

    /// The key column of a batch as i64s.
    fn pks(b: &Batch) -> Vec<i64> {
        (0..b.len()).map(|i| b.key(i).as_int().unwrap()).collect()
    }
    /// The text column (index 3) of a batch.
    fn texts(b: &Batch) -> Vec<String> {
        (0..b.len()).map(|i| b.value(3, i).render()).collect()
    }
    fn rows(b: &Batch) -> Vec<Row> {
        b.iter().collect()
    }

    #[test]
    fn roundtrip_preserves_rows_and_id() {
        let p = part_of(7, &[1, 5, 9], 42);
        let got = decode_part_file(&full_image(&p)).unwrap();
        assert_eq!(got.id(), 7);
        assert_eq!(pks(got.batch()), vec![1, 5, 9]);
        assert_eq!(texts(got.batch()), texts(p.batch()));
        assert_eq!(got.created_min(), 42);
    }

    #[test]
    fn roundtrip_preserves_uniform_stamps() {
        let p = part_of(1, &[1, 2], 10);
        let got = decode_part_file(&full_image(&p)).unwrap();
        assert!(got.created_is_uniform(), "uniform encoding lost");
    }

    #[test]
    fn roundtrip_preserves_per_row_stamps() {
        let batch: Batch = [1i64, 2, 3]
            .iter()
            .map(|&pk| Row::new(pk, pk, 0.0, ""))
            .collect();
        let p = Part::new(1, batch, CreatedCsns::PerRow(vec![10, 20, 30]));
        let got = decode_part_file(&full_image(&p)).unwrap();
        assert!(!got.created_is_uniform());
        assert_eq!(got.created_at(0), 10);
        assert_eq!(got.created_at(2), 30);
    }

    #[test]
    fn roundtrip_of_empty_part() {
        let p = part_of(3, &[], 1);
        let got = decode_part_file(&full_image(&p)).unwrap();
        assert_eq!(got.num_rows(), 0);
        assert_eq!(got.id(), 3);
    }

    #[test]
    fn float_and_unicode_survive() {
        let batch: Batch = vec![
            Row::new(1, i64::MIN, f64::MIN, "日本語"),
            Row::new(2, i64::MAX, f64::MAX, "emoji 🎯"),
        ]
        .into_iter()
        .collect();
        let p = Part::new(1, batch.clone(), CreatedCsns::Uniform(1));
        let got = decode_part_file(&full_image(&p)).unwrap();
        assert_eq!(rows(got.batch()), rows(&batch));
    }

    #[test]
    fn tombstones_survive_the_roundtrip() {
        let p = part_of(1, &[1, 2, 3, 4], 5);
        p.mark_deleted(1, 50);
        p.mark_deleted(3, 60);
        let got = decode_part_file(&encode_part_with_dv(&p)).unwrap();
        assert_eq!(got.dv_len(), 2);
        assert_eq!(pks(&got.scan(Snapshot::at(60))), vec![1, 3]);
    }

    fn full_image(p: &Part) -> Vec<u8> {
        let mut v = encode_summary(p);
        v.extend_from_slice(&encode_part(p));
        v
    }

    fn encode_part_with_dv(p: &Part) -> Vec<u8> {
        let mut v = full_image(p);
        let entries = p.dv_snapshot().entries_after(0);
        v.extend_from_slice(&encode_tombstones(&entries));
        v
    }

    #[test]
    fn file_roundtrip_through_io() {
        let io = MemIo::new();
        let p = part_of(2, &[10, 20, 30], 7);
        p.mark_deleted(0, 99);
        write_part(&io, "part-2", &p).unwrap();
        let got = read_part(&io, "part-2").unwrap();
        assert_eq!(got.id(), 2);
        assert_eq!(got.num_rows(), 3);
        assert_eq!(got.dv_len(), 1);
        assert_eq!(pks(&got.scan(Snapshot::at(99))), vec![20, 30]);
    }

    #[test]
    fn appended_tombstones_are_read_back() {
        let io = MemIo::new();
        let p = part_of(1, &[1, 2, 3], 5);
        let f = write_part(&io, "p", &p).unwrap();
        append_tombstones(&*f, &[(0, 40)]).unwrap();
        append_tombstones(&*f, &[(2, 41)]).unwrap();
        let got = read_part(&io, "p").unwrap();
        assert_eq!(got.dv_len(), 2);
        assert_eq!(pks(&got.scan(Snapshot::at(50))), vec![2]);
    }

    #[test]
    fn torn_tombstone_append_is_discarded_prefix_kept() {
        let p = part_of(1, &[1, 2, 3], 5);
        let mut img = full_image(&p);
        img.extend_from_slice(&encode_tombstones(&[(0, 40)]));
        let good_len = img.len();
        img.extend_from_slice(&encode_tombstones(&[(1, 41)]));

        // Cut anywhere inside the second append: the first must survive.
        for cut in good_len..img.len() {
            let got = decode_part_file(&img[..cut]).unwrap();
            assert_eq!(got.dv_len(), 1, "prefix tombstone lost at cut {cut}");
        }
    }

    #[test]
    fn truncated_part_image_is_rejected() {
        let p = part_of(1, &[1, 2, 3], 5);
        let img = full_image(&p);
        for cut in 0..img.len() {
            assert!(
                decode_part_file(&img[..cut]).is_err(),
                "truncated image accepted at {cut}"
            );
        }
    }

    #[test]
    fn corrupted_image_is_rejected() {
        let p = part_of(1, &[1, 2, 3], 5);
        let mut img = full_image(&p);
        let mid = img.len() / 2;
        img[mid] ^= 0xFF;
        assert!(decode_part_file(&img).is_err());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut e = Encoder::new();
        e.u32(0xDEAD_BEEF).u8(3).u64(0).u64(0);
        assert!(decode_part_file(&frame(e.as_slice())).is_err());
    }

    #[test]
    fn summary_reads_without_decoding_columns() {
        let io = MemIo::new();
        let p = part_of(9, &(0..5_000).collect::<Vec<_>>(), 42);
        write_part(&io, "big", &p).unwrap();
        let s = read_part_summary(&io, "big").unwrap();
        assert_eq!(s.id, 9);
        assert_eq!(s.num_rows, 5_000);
        assert_eq!(s.min_key, Value::Int(0));
        assert_eq!(s.max_key, Value::Int(4_999));
        assert_eq!(s.created_min, 42);
        assert_eq!(s.created_max, 42);
    }

    #[test]
    fn summary_of_empty_part() {
        let io = MemIo::new();
        let p = part_of(1, &[], 7);
        write_part(&io, "e", &p).unwrap();
        let s = read_part_summary(&io, "e").unwrap();
        assert_eq!(s.num_rows, 0);
    }

    #[test]
    fn summary_read_is_bounded_regardless_of_part_size() {
        // The FR-06b property: opening a part is O(1), not O(rows).
        let io = MemIo::new();
        let small = part_of(1, &(0..10).collect::<Vec<_>>(), 1);
        let big = part_of(2, &(0..50_000).collect::<Vec<_>>(), 1);
        write_part(&io, "s", &small).unwrap();
        write_part(&io, "b", &big).unwrap();

        let s_len = encode_summary(&small).len();
        let b_len = encode_summary(&big).len();
        assert_eq!(s_len, b_len, "summary size must not depend on row count");
        assert!(s_len < 100, "summary is {s_len} bytes");
    }

    #[test]
    fn truncated_summary_is_rejected() {
        let io = MemIo::new();
        let f = io.open("bad").unwrap();
        f.pwrite(0, &[1, 2, 3]).unwrap();
        assert!(read_part_summary(&io, "bad").is_err());
    }

    #[test]
    fn lookups_work_after_reload() {
        let io = MemIo::new();
        let pks_v: Vec<i64> = (0..5_000).map(|i| i * 7).collect();
        let p = part_of(4, &pks_v, 3);
        write_part(&io, "big", &p).unwrap();
        let got = read_part(&io, "big").unwrap();
        let snap = Snapshot::at(100);
        for &pk in pks_v.iter().step_by(53) {
            assert!(
                got.lookup(&Value::Int(pk), snap).ordinal().is_some(),
                "lost {pk}"
            );
        }
        assert!(got.lookup(&Value::Int(5), snap).ordinal().is_none());
    }
}
