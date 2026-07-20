//! Durable storage: WAL, checkpointing, and recovery.
//!
//! Wraps a [`Database`] so mutations are logged before acknowledgement and
//! survive a crash. Durability contract per `requirements.md` §7.2 — note
//! [`Durability::Async`] may lose acknowledged writes. Recovery replays only WAL
//! records beyond `checkpoint_csn`; parts are already durable and named by the
//! manifest (FR-06a), and are faulted in lazily on open (FR-06b, see `pager`).

use crate::backpressure::Backpressure;
pub use crate::storage_config::{RecoveryReport, StorageConfig};
use crate::clock::RealClock;
use crate::csn::Csn;
use crate::database::Database;
use crate::durability::Durability;
use crate::error::{Error, Result};
use crate::io::Io;
use crate::manifest::{Manifest, ManifestState, TableMeta};
use crate::metrics::Metrics;
use crate::pager::{PagedPart, PagerMetrics, PartSummary};

/// A table's parts registered lazily at open: (name, next part id, parts).
type PendingParts = Vec<(String, u64, Vec<Arc<PagedPart>>)>;
use crate::persist;
use crate::schema::Row;
use crate::wal::{Wal, WalRecord};
use std::collections::HashMap;
use std::io as stdio;
use std::sync::{Arc, Mutex, OnceLock};

const WAL_PATH: &str = "wal.log";
const MANIFEST_PATH: &str = "MANIFEST";

/// Part files are named by `(table, part_id)` and written **once**.
///
/// Crash-safe without generation-versioning: a part is immutable except for
/// *appended* tombstones (appends are self-checksumming, so a torn tail is
/// discarded), and new data always gets a fresh monotonic id — a file is never
/// rewritten in place. This is what lets checkpoint be **incremental**: skip
/// unchanged parts, append new tombstones, write only brand-new parts in full.
fn part_path(table: u32, part: u64) -> String {
    format!("part-{table}-{part}.dat")
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
    pager_metrics: Arc<PagerMetrics>,
    /// Parts registered from summaries, not yet handed to their tables.
    pending_parts: Mutex<PendingParts>,
    warmed: OnceLock<()>,
    /// (table_id, part_id) -> checkpoint CSN at which this part's tombstone
    /// state was last flushed. Lets checkpoint append only the *new* tombstones
    /// (those with `deleted_csn` beyond the recorded value) and skip parts that
    /// have not changed at all.
    persisted: Mutex<HashMap<(u32, u64), Csn>>,
}

impl Storage {
    /// Open a database, replaying anything left by a previous run.
    pub fn open(io: Arc<dyn Io>, config: StorageConfig) -> stdio::Result<Self> {
        let (manifest, state) = Manifest::open(&*io, MANIFEST_PATH)?;
        let db = Arc::new(Database::with_config(config.table.clone()));
        let mut report = RecoveryReport::default();
        let pager_metrics = Arc::new(PagerMetrics::default());
        let mut pending_parts: PendingParts = Vec::new();

        // 1. Rebuild tables and load their durable parts.
        let mut table_ids = HashMap::new();
        for meta in &state.tables {
            let t = db
                .create_table(&meta.name)
                .map_err(|e| stdio::Error::new(stdio::ErrorKind::InvalidData, e.to_string()))?;
            table_ids.insert(meta.name.clone(), meta.id);
            // FR-06b: read only each part's summary frame — a bounded read —
            // rather than decoding its columns. Column data is faulted in on
            // first touch, so open cost is O(parts), not O(rows).
            let mut lazy = Vec::new();
            for &pid in &meta.part_ids {
                let path = part_path(meta.id, pid);
                let summary: PartSummary = persist::read_part_summary(&*io, &path)?;
                report.rows_from_parts += summary.num_rows;
                lazy.push(Arc::new(PagedPart::register(
                    summary,
                    path,
                    io.clone(),
                    pager_metrics.clone(),
                )));
            }
            report.parts_loaded += lazy.len();
            report.parts_registered_lazily += lazy.len();
            pending_parts.push((meta.name.clone(), meta.next_part_id, lazy));
            let _ = &t;
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
        let state_checkpoint = state.checkpoint_csn;
        let mut max_csn = state.checkpoint_csn;

        for rec in &replay.records {
            if rec.csn() <= state.checkpoint_csn {
                continue; // already captured in a part
            }
            max_csn = max_csn.max(rec.csn());
            // Count only here; the actual apply happens after warming, below.
            match rec {
                WalRecord::Insert { table, .. } | WalRecord::Delete { table, .. } => {
                    if by_id.contains_key(table) {
                        report.wal_records_replayed += 1;
                    }
                }
                WalRecord::Seal { .. } | WalRecord::Checkpoint { .. } => {}
            }
        }

        db.set_csn_floor(max_csn);
        report.recovered_csn = max_csn;

        let mut persisted_init: HashMap<(u32, u64), Csn> = HashMap::new();
        for meta in &state.tables {
            for &pid in &meta.part_ids {
                persisted_init.insert((meta.id, pid), state.checkpoint_csn);
            }
        }

        let storage = Storage {
            io,
            db,
            pager_metrics,
            pending_parts: Mutex::new(pending_parts),
            warmed: OnceLock::new(),
            persisted: Mutex::new(persisted_init),
            wal,
            manifest,
            state: Mutex::new(state),
            table_ids: Mutex::new(table_ids),
            backpressure: Backpressure::new(config.backpressure.clone()),
            clock: RealClock::new(),
            config,
            report,
        };

        // Replay needs parts resident: a logged DELETE of a key in a sealed part
        // must tombstone that part's row, which a summary cannot do. So a
        // mutating tail forces warming. Lazy open thus pays off after a *clean*
        // checkpoint — the common case, and the one FR-06b targets. Per-part
        // deferral is possible but needs a fallible read path on Table; deferred
        // to the M2 query layer. See `m2-findings.md`.
        if storage.report.wal_records_replayed > 0 {
            storage.warm();
            // Re-apply the tail now that parts are present.
            storage.replay_tail(&replay.records, state_checkpoint)?;
        }
        Ok(storage)
    }

    /// Apply logged mutations beyond `checkpoint` to the in-memory tables.
    fn replay_tail(&self, records: &[WalRecord], checkpoint: Csn) -> stdio::Result<()> {
        let by_id: HashMap<u32, String> = self
            .state
            .lock()
            .unwrap()
            .tables
            .iter()
            .map(|t| (t.id, t.name.clone()))
            .collect();
        for rec in records {
            if rec.csn() <= checkpoint {
                continue;
            }
            match rec {
                WalRecord::Insert { table, csn, row } => {
                    if let Some(n) = by_id.get(table) {
                        if let Ok(t) = self.db.table(n) {
                            t.replay_insert(row.clone(), *csn);
                        }
                    }
                }
                WalRecord::Delete { table, csn, pk } => {
                    if let Some(n) = by_id.get(table) {
                        if let Ok(t) = self.db.table(n) {
                            t.replay_delete(*pk, *csn);
                        }
                    }
                }
                WalRecord::Seal { .. } | WalRecord::Checkpoint { .. } => {}
            }
        }
        Ok(())
    }

    /// The database, with all registered parts faulted in.
    ///
    /// The read path is infallible, so this materialises everything on first
    /// access. Bounds-only checks can use [`Storage::may_contain_key`] and stay
    /// cold.
    pub fn database(&self) -> &Arc<Database> {
        self.warm();
        &self.db
    }

    /// The database *without* forcing parts resident. Callers must not assume
    /// sealed data is visible through it.
    pub fn database_cold(&self) -> &Arc<Database> {
        &self.db
    }

    pub fn pager_metrics(&self) -> &PagerMetrics {
        &self.pager_metrics
    }

    /// Fault every registered part in and install it on its table.
    pub fn warm(&self) {  // idempotent via OnceLock
        self.warmed.get_or_init(|| {
            let pending = std::mem::take(&mut *self.pending_parts.lock().unwrap());
            for (name, next_id, lazy) in pending {
                if let Ok(t) = self.db.table(&name) {
                    let mut parts = Vec::with_capacity(lazy.len());
                    for lp in &lazy {
                        match lp.load() {
                            Ok(p) => parts.push(p.clone()),
                            Err(e) => panic!(
                                "part {} became unreadable after open: {e}. \
                                 The manifest references it, so this is corruption, \
                                 not a recoverable condition.",
                                lp.id()
                            ),
                        }
                    }
                    t.install_parts(parts, next_id);
                }
            }
        });
    }

    /// Answer "could this key exist?" from resident summaries only, without
    /// faulting anything in. `true` still needs a real lookup to confirm.
    pub fn may_contain_key(&self, table: &str, pk: i64) -> bool {
        if self.warmed.get().is_some() {
            return self
                .db
                .table(table)
                .map(|t| t.get_latest(pk).is_some())
                .unwrap_or(false);
        }
        let pending = self.pending_parts.lock().unwrap();
        pending
            .iter()
            .filter(|(n, _, _)| n == table)
            .any(|(_, _, parts)| parts.iter().any(|p| !p.definitely_excludes(pk)))
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

    /// Bulk-load rows known to have distinct, new keys, skipping the
    /// duplicate-key probe. For seeding and restore only — using it with a key
    /// that already exists produces two live versions and is a caller bug.
    pub fn load_batch(&self, table: &str, rows: Vec<Row>) -> Result<()> {
        self.warm();
        let id = self.table_id(table)?;
        let t = self.db.table(table)?;
        // Append all records without syncing each, then make the whole batch
        // durable with one flush — the batch equivalent of group commit.
        for row in rows {
            let csn = t.replay_insert_new(row.clone());
            self.wal
                .append_async(&WalRecord::Insert { table: id, csn, row })
                .map_err(|_| Error::WriteConflict)?;
        }
        self.wal.flush().map_err(|_| Error::WriteConflict)?;
        Ok(())
    }

    /// Insert, logging before acknowledging.
    pub fn insert(&self, table: &str, row: Row) -> Result<Csn> {
        self.warm();
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
        self.warm();
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
        self.warm();
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
        self.warm();
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

        let mut persisted = self.persisted.lock().unwrap();
        let mut live: std::collections::HashSet<(u32, u64)> = std::collections::HashSet::new();

        // Phase 1: bring each part's on-disk image up to date. New parts are
        // written in full; parts that gained tombstones get only those appended;
        // unchanged parts are skipped. A crash here leaves the previous manifest
        // valid — new files are orphans it does not reference, and appends are
        // self-checksumming.
        for meta in st.tables.iter_mut() {
            let t = match self.db.table(&meta.name) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let (parts, next_id) = t.parts_snapshot();
            for p in &parts {
                let key = (meta.id, p.id());
                live.insert(key);
                let path = part_path(meta.id, p.id());
                match persisted.get(&key).copied() {
                    None => {
                        persist::write_part(&*self.io, &path, p)?;
                    }
                    Some(since) => {
                        // Tombstones added after `since` are the only new bytes.
                        let new_dels = p.dv_snapshot().entries_after(since);
                        if !new_dels.is_empty() {
                            let f = self.io.open(&path)?;
                            persist::append_tombstones(&*f, &new_dels)?;
                        }
                    }
                }
                persisted.insert(key, csn);
            }
            meta.part_ids = parts.iter().map(|p| p.id()).collect();
            meta.next_part_id = next_id;
        }

        // Phase 2: the atomic switch.
        st.checkpoint_csn = csn;
        self.manifest.compact(&st)?;

        // Phase 3: drop files for parts no longer live (compacted away). Errors
        // here waste space but are not correctness problems.
        let dead: Vec<(u32, u64)> = persisted.keys().copied().filter(|k| !live.contains(k)).collect();
        for (tid, pid) in dead {
            let _ = self.io.remove(&part_path(tid, pid));
            persisted.remove(&(tid, pid));
        }
        drop(persisted);
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
