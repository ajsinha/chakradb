//! Part serialisation.
//!
//! A sealed part is written once and never modified, so its file format can be
//! simple: a checksummed header followed by column runs, version stamps, and
//! the deletion vector.
//!
//! Deletion vectors *do* change after the part is written. Rather than rewrite
//! the file, tombstones are appended as framed records after the base image —
//! the same append-only, checksum-per-record discipline as the WAL, so a torn
//! append is detected and discarded rather than misread.

use crate::codec::{frame, unframe, DecodeError, Decoder, Encoder};
use crate::csn::Csn;
use crate::io::{File, Io};
use crate::part::{CreatedCsns, Part};
use crate::schema::Batch;
use std::io;
use std::sync::Arc;

const PART_MAGIC: u32 = 0x4348_4B50; // "CHKP"
const PART_VERSION: u8 = 1;

const STAMPS_UNIFORM: u8 = 0;
const STAMPS_PER_ROW: u8 = 1;

/// Encode a part's immutable image (everything except later tombstones).
pub fn encode_part(part: &Part) -> Vec<u8> {
    let batch = part.batch();
    let n = batch.len();
    let mut e = Encoder::with_capacity(n * 40 + 64);

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

    // Columns, run per column so a reader can project without parsing rows.
    for i in 0..n {
        e.i64(batch.pk[i]);
    }
    for i in 0..n {
        e.i64(batch.a[i]);
    }
    for i in 0..n {
        e.f64(batch.b[i]);
    }
    for i in 0..n {
        e.str(&batch.c[i]);
    }

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

    let mut batch = Batch::with_capacity(n);
    for _ in 0..n {
        batch.pk.push(d.i64()?);
    }
    for _ in 0..n {
        batch.a.push(d.i64()?);
    }
    for _ in 0..n {
        batch.b.push(d.f64()?);
    }
    for _ in 0..n {
        batch.c.push(d.string()?);
    }
    if !batch.is_well_formed() {
        return Err(DecodeError::Malformed("column length mismatch"));
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
    f.pwrite(0, &encode_part(part))?;
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
    let (payload, mut pos) = unframe(buf, 0)?;
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

    fn part_of(id: u64, pks: &[i64], csn: Csn) -> Part {
        let batch: Batch = pks
            .iter()
            .map(|&pk| Row::new(pk, pk * 3, pk as f64 / 2.0, format!("row-{pk}")))
            .collect();
        Part::new(id, batch, CreatedCsns::Uniform(csn))
    }

    #[test]
    fn roundtrip_preserves_rows_and_id() {
        let p = part_of(7, &[1, 5, 9], 42);
        let got = decode_part_file(&encode_part(&p)).unwrap();
        assert_eq!(got.id(), 7);
        assert_eq!(got.batch().pk, vec![1, 5, 9]);
        assert_eq!(got.batch().c, p.batch().c);
        assert_eq!(got.created_min(), 42);
    }

    #[test]
    fn roundtrip_preserves_uniform_stamps() {
        let p = part_of(1, &[1, 2], 10);
        let got = decode_part_file(&encode_part(&p)).unwrap();
        assert!(got.created_is_uniform(), "uniform encoding lost");
    }

    #[test]
    fn roundtrip_preserves_per_row_stamps() {
        let batch: Batch = [1i64, 2, 3]
            .iter()
            .map(|&pk| Row::new(pk, pk, 0.0, ""))
            .collect();
        let p = Part::new(1, batch, CreatedCsns::PerRow(vec![10, 20, 30]));
        let got = decode_part_file(&encode_part(&p)).unwrap();
        assert!(!got.created_is_uniform());
        assert_eq!(got.created_at(0), 10);
        assert_eq!(got.created_at(2), 30);
    }

    #[test]
    fn roundtrip_of_empty_part() {
        let p = part_of(3, &[], 1);
        let got = decode_part_file(&encode_part(&p)).unwrap();
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
        let got = decode_part_file(&encode_part(&p)).unwrap();
        assert_eq!(got.batch(), &batch);
    }

    #[test]
    fn tombstones_survive_the_roundtrip() {
        let p = part_of(1, &[1, 2, 3, 4], 5);
        p.mark_deleted(1, 50);
        p.mark_deleted(3, 60);
        let got = decode_part_file(&encode_part_with_dv(&p)).unwrap();
        assert_eq!(got.dv_len(), 2);
        assert_eq!(got.scan(Snapshot::at(60)).pk, vec![1, 3]);
    }

    fn encode_part_with_dv(p: &Part) -> Vec<u8> {
        let mut v = encode_part(p);
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
        assert_eq!(got.scan(Snapshot::at(99)).pk, vec![20, 30]);
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
        assert_eq!(got.scan(Snapshot::at(50)).pk, vec![2]);
    }

    #[test]
    fn torn_tombstone_append_is_discarded_prefix_kept() {
        let p = part_of(1, &[1, 2, 3], 5);
        let mut img = encode_part(&p);
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
        let img = encode_part(&p);
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
        let mut img = encode_part(&p);
        let mid = img.len() / 2;
        img[mid] ^= 0xFF;
        assert!(decode_part_file(&img).is_err());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut e = Encoder::new();
        e.u32(0xDEAD_BEEF).u8(1).u64(0).u64(0);
        assert!(decode_part_file(&frame(e.as_slice())).is_err());
    }

    #[test]
    fn lookups_work_after_reload() {
        let io = MemIo::new();
        let pks: Vec<i64> = (0..5_000).map(|i| i * 7).collect();
        let p = part_of(4, &pks, 3);
        write_part(&io, "big", &p).unwrap();
        let got = read_part(&io, "big").unwrap();
        let snap = Snapshot::at(100);
        for &pk in pks.iter().step_by(53) {
            assert!(got.lookup(pk, snap).ordinal().is_some(), "lost {pk}");
        }
        assert!(got.lookup(5, snap).ordinal().is_none());
    }
}
