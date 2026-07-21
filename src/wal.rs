//! Write-ahead log.
//!
//! Every mutation is appended here before it is acknowledged. Recovery replays
//! the log tail (`requirements.md` FR-06) — **bounded by tail size, not by
//! database size**, which is the hard constraint that makes restart times
//! independent of how much data you have.
//!
//! Records are framed with a length and CRC (see [`crate::codec`]). A crash
//! mid-append leaves a torn final record; recovery detects it by checksum and
//! stops there, which is the correct interpretation — that write was never
//! acknowledged.

use crate::codec::{frame, unframe, DecodeError, Decoder, Encoder};
use crate::csn::Csn;
use crate::durability::{CommitAction, Durability, GroupCommit};
use crate::io::{File, Io};
use crate::schema::Row;
use crate::value::Value;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const OP_INSERT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_SEAL: u8 = 3;
const OP_CHECKPOINT: u8 = 4;

/// One logged mutation.
#[derive(Debug, Clone, PartialEq)]
pub enum WalRecord {
    /// A new row version. Updates are logged as insert-of-new-version plus the
    /// delete of the old one, matching the engine's internal model.
    Insert {
        table: u32,
        csn: Csn,
        row: Row,
    },
    Delete {
        table: u32,
        csn: Csn,
        key: Value,
    },
    /// L0 was sealed into a part; everything before this is in that part.
    Seal {
        table: u32,
        csn: Csn,
        part_id: u64,
    },
    /// All state up to `csn` is durable in parts; the log before this record
    /// is reclaimable.
    Checkpoint { csn: Csn },
}

impl WalRecord {
    pub fn csn(&self) -> Csn {
        match self {
            WalRecord::Insert { csn, .. }
            | WalRecord::Delete { csn, .. }
            | WalRecord::Seal { csn, .. }
            | WalRecord::Checkpoint { csn } => *csn,
        }
    }

    pub fn table(&self) -> Option<u32> {
        match self {
            WalRecord::Insert { table, .. }
            | WalRecord::Delete { table, .. }
            | WalRecord::Seal { table, .. } => Some(*table),
            WalRecord::Checkpoint { .. } => None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut e = Encoder::with_capacity(64);
        match self {
            WalRecord::Insert { table, csn, row } => {
                e.u8(OP_INSERT).u32(*table).u64(*csn).row(row);
            }
            WalRecord::Delete { table, csn, key } => {
                e.u8(OP_DELETE).u32(*table).u64(*csn).value(key);
            }
            WalRecord::Seal {
                table,
                csn,
                part_id,
            } => {
                e.u8(OP_SEAL).u32(*table).u64(*csn).u64(*part_id);
            }
            WalRecord::Checkpoint { csn } => {
                e.u8(OP_CHECKPOINT).u64(*csn);
            }
        }
        frame(e.as_slice())
    }

    pub fn decode(payload: &[u8]) -> Result<Self, DecodeError> {
        let mut d = Decoder::new(payload);
        match d.u8()? {
            OP_INSERT => Ok(WalRecord::Insert {
                table: d.u32()?,
                csn: d.u64()?,
                row: d.row()?,
            }),
            OP_DELETE => Ok(WalRecord::Delete {
                table: d.u32()?,
                csn: d.u64()?,
                key: d.value()?,
            }),
            OP_SEAL => Ok(WalRecord::Seal {
                table: d.u32()?,
                csn: d.u64()?,
                part_id: d.u64()?,
            }),
            OP_CHECKPOINT => Ok(WalRecord::Checkpoint { csn: d.u64()? }),
            _ => Err(DecodeError::Malformed("unknown wal opcode")),
        }
    }
}

/// Appends records and coordinates group commit.
#[derive(Debug)]
pub struct Wal {
    file: Arc<dyn File>,
    /// Bytes appended (not necessarily durable).
    written: AtomicU64,
    /// Serialises appends so offsets are assigned in order.
    append_lock: Mutex<()>,
    group: GroupCommit,
    mode: Mutex<Durability>,
    syncs: AtomicU64,
    appends: AtomicU64,
}

impl Wal {
    pub fn open(io: &dyn Io, path: &str, mode: Durability) -> io::Result<Self> {
        let file = io.open(path)?;
        let written = file.len()?;
        let wal = Wal {
            file,
            written: AtomicU64::new(written),
            append_lock: Mutex::new(()),
            group: GroupCommit::new(),
            mode: Mutex::new(mode),
            syncs: AtomicU64::new(0),
            appends: AtomicU64::new(0),
        };
        // Anything already on disk is by definition durable.
        wal.group.complete(written);
        Ok(wal)
    }

    pub fn mode(&self) -> Durability {
        *self.mode.lock().unwrap()
    }

    pub fn set_mode(&self, mode: Durability) {
        *self.mode.lock().unwrap() = mode;
    }

    pub fn written_bytes(&self) -> u64 {
        self.written.load(Ordering::SeqCst)
    }

    pub fn durable_bytes(&self) -> u64 {
        self.group.durable_offset()
    }

    pub fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::SeqCst)
    }

    pub fn append_count(&self) -> u64 {
        self.appends.load(Ordering::SeqCst)
    }

    /// Syncs performed per append. The point of group commit is to keep this
    /// well below 1 under concurrency.
    pub fn syncs_per_append(&self) -> f64 {
        let a = self.append_count();
        if a == 0 {
            return 0.0;
        }
        self.sync_count() as f64 / a as f64
    }

    /// Append a record and honour the current durability mode.
    ///
    /// Returns the byte offset just past the record.
    pub fn append(&self, rec: &WalRecord) -> io::Result<u64> {
        let bytes = rec.encode();
        let end = {
            let _g = self.append_lock.lock().unwrap();
            let offset = self.written.load(Ordering::SeqCst);
            self.file.pwrite(offset, &bytes)?;
            let end = offset + bytes.len() as u64;
            self.written.store(end, Ordering::SeqCst);
            end
        };
        self.appends.fetch_add(1, Ordering::Relaxed);

        match self.mode() {
            Durability::Async => {}
            Durability::Sync | Durability::Group => self.commit_to(end)?,
        }
        Ok(end)
    }

    /// Append without syncing, regardless of durability mode. The caller is
    /// responsible for a later `flush`. Used by bulk load.
    pub fn append_async(&self, rec: &WalRecord) -> io::Result<u64> {
        let bytes = rec.encode();
        let _g = self.append_lock.lock().unwrap();
        let offset = self.written.load(Ordering::SeqCst);
        self.file.pwrite(offset, &bytes)?;
        let end = offset + bytes.len() as u64;
        self.written.store(end, Ordering::SeqCst);
        self.appends.fetch_add(1, Ordering::Relaxed);
        Ok(end)
    }

    /// Make everything up to `offset` durable, batching with concurrent callers.
    pub fn commit_to(&self, offset: u64) -> io::Result<()> {
        // `begin` blocks until either our offset is durable or we are elected
        // leader, so exactly one pass is needed here.
        match self.group.begin(offset) {
            CommitAction::Done => Ok(()),
            CommitAction::Sync => {
                // Sync covers everything written so far, not just our record —
                // that is what makes the batch worthwhile.
                let covered = self.written.load(Ordering::SeqCst);
                match self.file.sync() {
                    Ok(()) => {
                        self.syncs.fetch_add(1, Ordering::Relaxed);
                        self.group.complete(covered);
                        Ok(())
                    }
                    Err(e) => {
                        self.group.abort();
                        Err(e)
                    }
                }
            }
        }
    }

    /// Force a sync regardless of mode (used at checkpoint and shutdown).
    pub fn flush(&self) -> io::Result<()> {
        let end = self.written.load(Ordering::SeqCst);
        self.commit_to(end)
    }

    /// Drop everything before `offset`, having checkpointed it.
    ///
    /// M0-2's lesson applies here too: the log must not grow without bound, or
    /// recovery time stops being proportional to the tail.
    pub fn truncate_before(&self, offset: u64) -> io::Result<()> {
        let _g = self.append_lock.lock().unwrap();
        let total = self.written.load(Ordering::SeqCst);
        if offset == 0 || offset > total {
            return Ok(());
        }
        let mut tail = vec![0u8; (total - offset) as usize];
        if !tail.is_empty() {
            self.file.pread(offset, &mut tail)?;
        }
        self.file.truncate(0)?;
        if !tail.is_empty() {
            self.file.pwrite(0, &tail)?;
        }
        let new_len = tail.len() as u64;
        self.written.store(new_len, Ordering::SeqCst);
        self.file.sync()?;
        // Must be `reset_to`, not `complete`: the watermark has to move *down*
        // to match the shortened file. Leaving it high would make the next
        // append believe it was already durable and skip its fsync.
        self.group.reset_to(new_len);
        Ok(())
    }

    /// Read every intact record. Stops at the first torn or corrupt one.
    ///
    /// The stop is deliberate and is the crash-recovery contract: a partial
    /// record was never acknowledged, so discarding it loses nothing a client
    /// was told had committed.
    pub fn replay(io: &dyn Io, path: &str) -> io::Result<ReplayResult> {
        let file = io.open(path)?;
        let len = file.len()? as usize;
        let mut buf = vec![0u8; len];
        if len > 0 {
            file.pread(0, &mut buf)?;
        }
        Ok(Self::replay_bytes(&buf))
    }

    /// Replay from an in-memory image (also used by the crash tests).
    pub fn replay_bytes(buf: &[u8]) -> ReplayResult {
        let mut records = Vec::new();
        let mut pos = 0usize;
        let mut truncated = false;
        while pos < buf.len() {
            match unframe(buf, pos) {
                Ok((payload, next)) => match WalRecord::decode(payload) {
                    Ok(rec) => {
                        records.push(rec);
                        pos = next;
                    }
                    Err(_) => {
                        truncated = true;
                        break;
                    }
                },
                Err(_) => {
                    truncated = true;
                    break;
                }
            }
        }
        ReplayResult {
            records,
            valid_bytes: pos as u64,
            truncated_tail: truncated,
        }
    }
}

/// Outcome of scanning a log.
#[derive(Debug)]
pub struct ReplayResult {
    pub records: Vec<WalRecord>,
    /// Bytes that decoded cleanly.
    pub valid_bytes: u64,
    /// Whether a torn record was found at the end.
    pub truncated_tail: bool,
}

impl ReplayResult {
    /// Highest CSN observed, or 0.
    pub fn max_csn(&self) -> Csn {
        self.records.iter().map(|r| r.csn()).max().unwrap_or(0)
    }

    /// CSN of the last checkpoint, or 0.
    pub fn last_checkpoint(&self) -> Csn {
        self.records
            .iter()
            .filter_map(|r| match r {
                WalRecord::Checkpoint { csn } => Some(*csn),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }
}
