//! Durable storage: WAL, checkpointing, and recovery.
//!
//! Wraps a [`Database`] so that mutations are logged before acknowledgement and
//! survive a crash. The durability contract is the one stated in
//! `requirements.md` §7.2, and the important part is what it does *not* claim:
//! in [`Durability::Async`] mode, acknowledged writes can be lost.
//!
//! # Recovery is bounded by the log tail
//!
//! FR-06 requires restart time to be independent of database size. That falls
//! out of the layering: parts are already durable, the manifest names them, and
//! only WAL records beyond `checkpoint_csn` need replaying. Checkpointing seals
//! L0, writes the new parts, commits the manifest, and truncates the log.

use crate::backpressure::{Backpressure, BackpressureConfig};
use crate::clock::RealClock;
use crate::csn::Csn;
use crate::database::Database;
use crate::durability::Durability;
use crate::error::{Error, Result};
use crate::io::Io;
use crate::manifest::{Manifest, ManifestState, TableMeta};
use crate::metrics::Metrics;
use crate::persist;
use crate::schema::Row;
use crate::table::TableConfig;
use crate::wal::{Wal, WalRecord};
use std::collections::HashMap;
use std::io as stdio;
use std::sync::{Arc, Mutex};

const WAL_PATH: &str = "wal.log";
const MANIFEST_PATH: &str = "MANIFEST";

/// Part files are **generation-versioned**: each checkpoint writes a fresh set
/// and the manifest commit is what atomically switches to them.
///
/// Rewriting a part in place would not be crash-safe — a crash between the
/// truncate and the sync would leave a file the manifest still points at, but
/// which no longer decodes. Versioning means a torn write only ever affects a
/// generation nothing references yet.
fn part_path(table: u32, part: u64, gen: Csn) -> String {
    format!("part-{table}-{part}-g{gen}.dat")
}

/// Tunables for the durable layer.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub durability: Durability,
    pub table: TableConfig,
    pub backpressure: BackpressureConfig,
    /// Checkpoint once the log exceeds this many bytes.
    pub checkpoint_wal_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            durability: Durability::default(),
            table: TableConfig::default(),
            backpressure: BackpressureConfig::default(),
            checkpoint_wal_bytes: 4 * 1024 * 1024,
        }
    }
}

/// What recovery found on startup.
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub tables_loaded: usize,
    pub parts_loaded: usize,
    pub rows_from_parts: usize,
    pub wal_records_replayed: usize,
    pub wal_bytes_scanned: u64,
    /// A torn record was found and discarded — normal after a crash.
    pub truncated_tail: bool,
    pub recovered_csn: Csn,
}

/// A crash-safe database.
#[derive(Debug)]
pub struct Storage {
    io: Arc<dyn Io>,
    db: Arc<Database>,
    wal: Wal,
    manifest: Manifest,
    state: Mutex<ManifestState>,
    /// name -> stable numeric id used in the WAL.
    table_ids: Mutex<HashMap<String, u32>>,
    backpressure: Backpressure,
    clock: RealClock,
    config: StorageConfig,
    report: RecoveryReport,
}

impl Storage {
    /// Open a database, replaying anything left by a previous run.
    pub fn open(io: Arc<dyn Io>, config: StorageConfig) -> stdio::Result<Self> {
        let (manifest, state) = Manifest::open(&*io, MANIFEST_PATH)?;
        let db = Arc::new(Database::with_config(config.table.clone()));
        let mut report = RecoveryReport::default();

        // 1. Rebuild tables and load their durable parts.
        let mut table_ids = HashMap::new();
        for meta in &state.tables {
            let t = db
                .create_table(&meta.name)
                .map_err(|e| stdio::Error::new(stdio::ErrorKind::InvalidData, e.to_string()))?;
            table_ids.insert(meta.name.clone(), meta.id);
            let mut parts = Vec::new();
            for &pid in &meta.part_ids {
                let p = persist::read_part(&*io, &part_path(meta.id, pid, state.checkpoint_csn))?;
                report.rows_from_parts += p.num_rows();
                parts.push(Arc::new(p));
            }
            report.parts_loaded += parts.len();
            t.install_parts(parts, meta.next_part_id);
        }
        report.tables_loaded = state.tables.len();

        // 2. Replay the log beyond the checkpoint.
        let wal = Wal::open(&*io, WAL_PATH, config.durability)?;
        let replay = Wal::replay(&*io, WAL_PATH)?;
        report.wal_bytes_scanned = replay.valid_bytes;
        report.truncated_tail = replay.truncated_tail;

        let by_id: HashMap<u32, String> = state
            .tables
            .iter()
            .map(|t| (t.id, t.name.clone()))
            .collect();
        let mut max_csn = state.checkpoint_csn;

        for rec in &replay.records {
            if rec.csn() <= state.checkpoint_csn {
                continue; // already captured in a part
            }
            max_csn = max_csn.max(rec.csn());
            match rec {
                WalRecord::Insert { table, csn, row } => {
                    if let Some(name) = by_id.get(table) {
                        if let Ok(t) = db.table(name) {
                            t.replay_insert(row.clone(), *csn);
                            report.wal_records_replayed += 1;
                        }
                    }
                }
                WalRecord::Delete { table, csn, pk } => {
                    if let Some(name) = by_id.get(table) {
                        if let Ok(t) = db.table(name) {
                            t.replay_delete(*pk, *csn);
                            report.wal_records_replayed += 1;
                        }
                    }
                }
                WalRecord::Seal { .. } | WalRecord::Checkpoint { .. } => {}
            }
        }

        db.set_csn_floor(max_csn);
        report.recovered_csn = max_csn;

        Ok(Storage {
            io,
            db,
            wal,
            manifest,
            state: Mutex::new(state),
            table_ids: Mutex::new(table_ids),
            backpressure: Backpressure::new(config.backpressure.clone()),
            clock: RealClock::new(),
            config,
            report,
        })
    }

    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }
    pub fn recovery(&self) -> &RecoveryReport {
        &self.report
    }
    pub fn metrics(&self) -> &Metrics {
        self.db.metrics()
    }
    pub fn wal(&self) -> &Wal {
        &self.wal
    }
    pub fn durability(&self) -> Durability {
        self.wal.mode()
    }
    pub fn set_durability(&self, d: Durability) {
        self.wal.set_mode(d);
    }

    /// Create a table and record it durably.
    pub fn create_table(&self, name: &str) -> Result<()> {
        self.db.create_table(name)?;
        let mut st = self.state.lock().unwrap();
        let id = st.next_table_id;
        st.next_table_id += 1;
        st.tables.push(TableMeta {
            id,
            name: name.to_string(),
            part_ids: Vec::new(),
            next_part_id: 0,
        });
        self.table_ids.lock().unwrap().insert(name.to_string(), id);
        self.manifest
            .commit(&st)
            .map_err(|_| Error::WriteConflict)?;
        Ok(())
    }

    fn table_id(&self, name: &str) -> Result<u32> {
        self.table_ids
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    /// Insert, logging before acknowledging.
    pub fn insert(&self, table: &str, row: Row) -> Result<Csn> {
        let id = self.table_id(table)?;
        let t = self.db.table(table)?;
        self.throttle(&t);
        let csn = t.insert(row.clone())?;
        self.log(WalRecord::Insert {
            table: id,
            csn,
            row,
        })?;
        Ok(csn)
    }

    /// Insert or replace, logging before acknowledging.
    pub fn upsert(&self, table: &str, row: Row) -> Result<Csn> {
        let id = self.table_id(table)?;
        let t = self.db.table(table)?;
        self.throttle(&t);
        let csn = t.upsert(row.clone())?;
        self.log(WalRecord::Insert {
            table: id,
            csn,
            row,
        })?;
        Ok(csn)
    }

    pub fn update(&self, table: &str, row: Row) -> Result<Csn> {
        let id = self.table_id(table)?;
        let t = self.db.table(table)?;
        self.throttle(&t);
        let csn = t.update(row.clone())?;
        self.log(WalRecord::Insert {
            table: id,
            csn,
            row,
        })?;
        Ok(csn)
    }

    pub fn delete(&self, table: &str, pk: i64) -> Result<Csn> {
        let id = self.table_id(table)?;
        let t = self.db.table(table)?;
        let csn = t.delete(pk)?;
        self.log(WalRecord::Delete {
            table: id,
            csn,
            pk,
        })?;
        Ok(csn)
    }

    fn log(&self, rec: WalRecord) -> Result<()> {
        self.wal.append(&rec).map_err(|_| Error::WriteConflict)?;
        Ok(())
    }

    fn throttle(&self, t: &Arc<crate::table::Table>) {
        let parts = t.stats().num_parts;
        self.backpressure.apply(parts, &self.clock, self.metrics());
    }

    /// True once the log has grown past the configured threshold.
    pub fn checkpoint_due(&self) -> bool {
        self.wal.written_bytes() >= self.config.checkpoint_wal_bytes
    }

    /// Seal, persist parts, commit the manifest, and truncate the log.
    ///
    /// After this returns, recovery need only replay what was written since.
    pub fn checkpoint(&self) -> stdio::Result<Csn> {
        let mut st = self.state.lock().unwrap();
        self.db.seal_all();
        let csn = self.db.snapshot().csn;
        let old_gen = st.checkpoint_csn;

        // Phase 1: write the new generation. A crash anywhere in here leaves
        // the manifest pointing at the previous generation, which is intact.
        let mut written: Vec<(u32, u64)> = Vec::new();
        for meta in st.tables.iter_mut() {
            let t = match self.db.table(&meta.name) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let (parts, next_id) = t.parts_snapshot();
            for p in &parts {
                persist::write_part(&*self.io, &part_path(meta.id, p.id(), csn), p)?;
                written.push((meta.id, p.id()));
            }
            meta.part_ids = parts.iter().map(|p| p.id()).collect();
            meta.next_part_id = next_id;
        }

        // Phase 2: the atomic switch.
        st.checkpoint_csn = csn;
        self.manifest.compact(&st)?;

        // Phase 3: the previous generation is now unreferenced. Failing to
        // remove it wastes space but is not a correctness problem, so errors
        // here are deliberately ignored.
        if old_gen != csn {
            for (tid, pid) in &written {
                let _ = self.io.remove(&part_path(*tid, *pid, old_gen));
            }
        }
        self.wal.append(&WalRecord::Checkpoint { csn })?;
        self.wal.flush()?;
        // Everything up to here is captured in parts, so the log can go.
        self.wal.truncate_before(self.wal.written_bytes())?;
        Ok(csn)
    }

    /// Flush without checkpointing — makes async-mode writes durable.
    pub fn flush(&self) -> stdio::Result<()> {
        self.wal.flush()
    }

    /// Run compaction across all tables, then persist the result.
    pub fn compact_all(&self) -> usize {
        let horizon = self.db.snapshot().csn;
        self.db.compact_all(horizon)
    }
}
