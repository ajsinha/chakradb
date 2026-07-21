//! The durable catalog.
//!
//! Records which parts belong to which table, and the CSN up to which state is
//! captured in those parts. Recovery reads this first, loads the named parts,
//! then replays only the WAL beyond `checkpoint_csn` — which is what keeps
//! restart time proportional to the log tail rather than to database size
//! (`requirements.md` FR-06).
//!
//! Written as an **append-only log of complete snapshots**, each checksummed.
//! That avoids needing an atomic rename: recovery simply takes the last record
//! that decodes cleanly, and a crash mid-append discards a torn tail.

use crate::codec::{frame, unframe, DecodeError, Decoder, Encoder};
use crate::csn::Csn;
use crate::io::{File, Io};
use std::io;
use std::sync::{Arc, Mutex};

/// One table's durable metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMeta {
    pub id: u32,
    pub name: String,
    /// The table's schema, so recovery rebuilds it with the right shape.
    pub schema: crate::schema::Schema,
    /// Part ids, newest first — the order lookups traverse.
    pub part_ids: Vec<u64>,
    pub next_part_id: u64,
}

/// A complete catalog snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestState {
    pub tables: Vec<TableMeta>,
    /// Everything at or below this CSN is captured in the listed parts.
    pub checkpoint_csn: Csn,
    pub next_table_id: u32,
}

impl ManifestState {
    pub fn table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.iter().find(|t| t.name == name)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut e = Encoder::with_capacity(256);
        e.u64(self.checkpoint_csn)
            .u32(self.next_table_id)
            .u32(self.tables.len() as u32);
        for t in &self.tables {
            e.u32(t.id)
                .str(&t.name)
                .schema(&t.schema)
                .u64(t.next_part_id)
                .u32(t.part_ids.len() as u32);
            for &p in &t.part_ids {
                e.u64(p);
            }
        }
        frame(e.as_slice())
    }

    pub fn decode(payload: &[u8]) -> Result<Self, DecodeError> {
        let mut d = Decoder::new(payload);
        let checkpoint_csn = d.u64()?;
        let next_table_id = d.u32()?;
        let n = d.u32()? as usize;
        let mut tables = Vec::with_capacity(n);
        for _ in 0..n {
            let id = d.u32()?;
            let name = d.string()?;
            let schema = d.schema()?;
            let next_part_id = d.u64()?;
            let pn = d.u32()? as usize;
            let mut part_ids = Vec::with_capacity(pn);
            for _ in 0..pn {
                part_ids.push(d.u64()?);
            }
            tables.push(TableMeta {
                id,
                name,
                schema,
                part_ids,
                next_part_id,
            });
        }
        Ok(ManifestState {
            tables,
            checkpoint_csn,
            next_table_id,
        })
    }
}

/// Append-only manifest file.
#[derive(Debug)]
pub struct Manifest {
    file: Arc<dyn File>,
    len: Mutex<u64>,
    commits: Mutex<u64>,
}

impl Manifest {
    /// Open (or create) the manifest and return the last durable state.
    pub fn open(io: &dyn Io, path: &str) -> io::Result<(Self, ManifestState)> {
        let file = io.open(path)?;
        let len = file.len()?;
        let mut buf = vec![0u8; len as usize];
        if len > 0 {
            file.pread(0, &mut buf)?;
        }
        let (state, valid) = Self::scan(&buf);
        Ok((
            Manifest {
                file,
                len: Mutex::new(valid),
                commits: Mutex::new(0),
            },
            state,
        ))
    }

    /// Return the last cleanly-decodable state and how many bytes were valid.
    pub fn scan(buf: &[u8]) -> (ManifestState, u64) {
        let mut state = ManifestState::default();
        let mut pos = 0usize;
        let mut valid = 0u64;
        while pos < buf.len() {
            match unframe(buf, pos) {
                Ok((payload, next)) => match ManifestState::decode(payload) {
                    Ok(s) => {
                        state = s;
                        pos = next;
                        valid = next as u64;
                    }
                    Err(_) => break,
                },
                Err(_) => break,
            }
        }
        (state, valid)
    }

    /// Append a new snapshot and make it durable.
    pub fn commit(&self, state: &ManifestState) -> io::Result<()> {
        let bytes = state.encode();
        let mut len = self.len.lock().unwrap();
        self.file.pwrite(*len, &bytes)?;
        self.file.sync()?;
        *len += bytes.len() as u64;
        *self.commits.lock().unwrap() += 1;
        Ok(())
    }

    pub fn commit_count(&self) -> u64 {
        *self.commits.lock().unwrap()
    }

    pub fn bytes(&self) -> u64 {
        *self.len.lock().unwrap()
    }

    /// Rewrite the file with a single snapshot, discarding history.
    ///
    /// Only safe when nothing else is appending; used at checkpoint to stop the
    /// manifest growing without bound.
    pub fn compact(&self, state: &ManifestState) -> io::Result<()> {
        let bytes = state.encode();
        let mut len = self.len.lock().unwrap();
        self.file.truncate(0)?;
        self.file.pwrite(0, &bytes)?;
        self.file.sync()?;
        *len = bytes.len() as u64;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemIo;

    fn state(n: u32) -> ManifestState {
        ManifestState {
            tables: (0..n)
                .map(|i| TableMeta {
                    id: i,
                    name: format!("t{i}"),
                    schema: crate::schema::Schema::default_schema(),
                    part_ids: vec![i as u64 * 10, i as u64 * 10 + 1],
                    next_part_id: 100 + i as u64,
                })
                .collect(),
            checkpoint_csn: 500 + n as u64,
            next_table_id: n,
        }
    }

    #[test]
    fn roundtrip_preserves_everything() {
        let s = state(3);
        let bytes = s.encode();
        let (payload, _) = unframe(&bytes, 0).unwrap();
        assert_eq!(ManifestState::decode(payload).unwrap(), s);
    }

    #[test]
    fn empty_state_roundtrips() {
        let s = ManifestState::default();
        let bytes = s.encode();
        let (payload, _) = unframe(&bytes, 0).unwrap();
        assert_eq!(ManifestState::decode(payload).unwrap(), s);
    }

    #[test]
    fn table_lookup_by_name() {
        let s = state(3);
        assert_eq!(s.table("t1").unwrap().id, 1);
        assert!(s.table("missing").is_none());
    }

    #[test]
    fn fresh_manifest_is_empty() {
        let io = MemIo::new();
        let (_m, s) = Manifest::open(&io, "MANIFEST").unwrap();
        assert_eq!(s, ManifestState::default());
    }

    #[test]
    fn last_commit_wins() {
        let io = MemIo::new();
        {
            let (m, _) = Manifest::open(&io, "MANIFEST").unwrap();
            m.commit(&state(1)).unwrap();
            m.commit(&state(2)).unwrap();
            m.commit(&state(3)).unwrap();
            assert_eq!(m.commit_count(), 3);
        }
        let (_m, s) = Manifest::open(&io, "MANIFEST").unwrap();
        assert_eq!(s, state(3));
    }

    #[test]
    fn torn_tail_falls_back_to_previous_state() {
        let _io = MemIo::new();
        let good = state(2);
        let mut buf = good.encode();
        let good_len = buf.len();
        buf.extend_from_slice(&state(5).encode());

        // Cut anywhere inside the second record: the first must be recovered.
        for cut in good_len..buf.len() {
            let (s, valid) = Manifest::scan(&buf[..cut]);
            assert_eq!(s, good, "wrong state recovered at cut {cut}");
            assert_eq!(valid, good_len as u64);
        }
    }

    #[test]
    fn corrupted_record_stops_the_scan() {
        let mut buf = state(1).encode();
        let first = buf.len();
        buf.extend_from_slice(&state(2).encode());
        buf[first + 12] ^= 0xFF;
        let (s, valid) = Manifest::scan(&buf);
        assert_eq!(s, state(1));
        assert_eq!(valid, first as u64);
    }

    #[test]
    fn commit_after_torn_tail_overwrites_it() {
        let io = MemIo::new();
        let f = io.open("MANIFEST").unwrap();
        let good = state(1);
        f.pwrite(0, &good.encode()).unwrap();
        // Simulate a torn append.
        let junk = vec![0xAAu8; 20];
        f.pwrite(good.encode().len() as u64, &junk).unwrap();

        let (m, recovered) = Manifest::open(&io, "MANIFEST").unwrap();
        assert_eq!(recovered, good);
        // The next commit lands at the last valid offset, replacing the junk.
        m.commit(&state(4)).unwrap();
        let (_m2, s2) = Manifest::open(&io, "MANIFEST").unwrap();
        assert_eq!(s2, state(4));
    }

    #[test]
    fn compact_collapses_history() {
        let io = MemIo::new();
        let (m, _) = Manifest::open(&io, "MANIFEST").unwrap();
        for i in 1..20 {
            m.commit(&state(i)).unwrap();
        }
        let big = m.bytes();
        m.compact(&state(19)).unwrap();
        assert!(m.bytes() < big, "compact did not shrink the manifest");
        let (_m2, s) = Manifest::open(&io, "MANIFEST").unwrap();
        assert_eq!(s, state(19));
    }

    #[test]
    fn unicode_table_names_survive() {
        let io = MemIo::new();
        let mut s = ManifestState::default();
        s.tables.push(TableMeta {
            id: 1,
            name: "テーブル".to_string(),
            schema: crate::schema::Schema::default_schema(),
            part_ids: vec![],
            next_part_id: 0,
        });
        let (m, _) = Manifest::open(&io, "MANIFEST").unwrap();
        m.commit(&s).unwrap();
        let (_m2, got) = Manifest::open(&io, "MANIFEST").unwrap();
        assert_eq!(got.tables[0].name, "テーブル");
    }

    #[test]
    fn large_catalog_roundtrips() {
        let io = MemIo::new();
        let mut s = ManifestState::default();
        for i in 0..200u32 {
            s.tables.push(TableMeta {
                id: i,
                name: format!("table_{i}"),
                schema: crate::schema::Schema::default_schema(),
                part_ids: (0..50).map(|p| p as u64).collect(),
                next_part_id: 50,
            });
        }
        s.checkpoint_csn = 999;
        let (m, _) = Manifest::open(&io, "MANIFEST").unwrap();
        m.commit(&s).unwrap();
        let (_m2, got) = Manifest::open(&io, "MANIFEST").unwrap();
        assert_eq!(got, s);
    }
}
