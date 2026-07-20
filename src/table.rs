//! A single table: L0 buffer + sealed parts + MVCC.
//!
//! One `Table` owns one primary-key space. A `Database` (see `database.rs`)
//! holds many of them and shares a single CSN generator, so that a snapshot
//! taken anywhere is consistent across every table.
//!
//! Concurrency model (`requirements.md` §7.1): writers serialise on one lock
//! and readers take a brief lock to capture a snapshot, then scan without
//! holding it. Contention on a single writer is deliberately cheaper than a
//! concurrent index; M0-1 and M0-3 measure whether that holds.

use crate::compaction::{self, CompactionPolicy};
use crate::csn::{Csn, CsnGenerator, Snapshot};
use crate::error::{Error, Result};
use crate::l0::L0Buffer;
use crate::metrics::Metrics;
use crate::part::{CreatedCsns, Part};
use crate::schema::{Batch, Row};
use std::sync::{Arc, RwLock};

/// Tunables. Defaults follow the sizes named in §5.1.
#[derive(Debug, Clone)]
pub struct TableConfig {
    /// Row versions buffered before L0 is sealed.
    pub seal_threshold: usize,
    pub compaction: CompactionPolicy,
}

impl Default for TableConfig {
    fn default() -> Self {
        TableConfig {
            seal_threshold: 10_000,
            compaction: CompactionPolicy::default(),
        }
    }
}

/// Where the live version of a key currently lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Location {
    L0(usize),
    /// `(index into parts, row ordinal)`
    Part(usize, u32),
}

#[derive(Debug, Default)]
struct TableInner {
    l0: L0Buffer,
    /// Newest first — lookups stop at the first hit, so cost tracks recency.
    parts: Vec<Arc<Part>>,
    next_part_id: u64,
}

/// A primary-keyed table.
#[derive(Debug)]
pub struct Table {
    name: String,
    inner: RwLock<TableInner>,
    csn: Arc<CsnGenerator>,
    metrics: Arc<Metrics>,
    config: TableConfig,
}

impl Table {
    pub fn new(
        name: impl Into<String>,
        csn: Arc<CsnGenerator>,
        metrics: Arc<Metrics>,
        config: TableConfig,
    ) -> Self {
        Table {
            name: name.into(),
            inner: RwLock::new(TableInner::default()),
            csn,
            metrics,
            config,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn snapshot(&self) -> Snapshot {
        self.csn.snapshot()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Insert a new row. Fails if the key already exists.
    pub fn insert(&self, row: Row) -> Result<Csn> {
        let mut inner = self.inner.write().unwrap();
        let snap = self.csn.snapshot();
        if Self::locate(&inner, row.pk, snap, &self.metrics).is_some() {
            return Err(Error::DuplicateKey(row.pk));
        }
        let csn = self.csn.allocate();
        inner.l0.insert(row, csn);
        Metrics::bump(&self.metrics.inserts);
        self.maybe_seal_locked(&mut inner);
        Ok(csn)
    }

    /// Insert or replace.
    pub fn upsert(&self, row: Row) -> Result<Csn> {
        let mut inner = self.inner.write().unwrap();
        let snap = self.csn.snapshot();
        let existing = Self::locate(&inner, row.pk, snap, &self.metrics);
        let csn = self.csn.allocate();
        if let Some(loc) = existing {
            Self::tombstone(&mut inner, loc, csn)?;
            Metrics::bump(&self.metrics.updates);
        } else {
            Metrics::bump(&self.metrics.inserts);
        }
        inner.l0.insert(row, csn);
        self.maybe_seal_locked(&mut inner);
        Ok(csn)
    }

    /// Replace an existing row. Fails if the key is absent.
    pub fn update(&self, row: Row) -> Result<Csn> {
        let mut inner = self.inner.write().unwrap();
        let snap = self.csn.snapshot();
        let loc = Self::locate(&inner, row.pk, snap, &self.metrics)
            .ok_or(Error::KeyNotFound(row.pk))?;
        let csn = self.csn.allocate();
        Self::tombstone(&mut inner, loc, csn)?;
        inner.l0.insert(row, csn);
        Metrics::bump(&self.metrics.updates);
        self.maybe_seal_locked(&mut inner);
        Ok(csn)
    }

    /// Delete by primary key. Fails if absent.
    pub fn delete(&self, pk: i64) -> Result<Csn> {
        let mut inner = self.inner.write().unwrap();
        let snap = self.csn.snapshot();
        let loc = Self::locate(&inner, pk, snap, &self.metrics).ok_or(Error::KeyNotFound(pk))?;
        let csn = self.csn.allocate();
        Self::tombstone(&mut inner, loc, csn)?;
        Metrics::bump(&self.metrics.deletes);
        Ok(csn)
    }

    /// Point read at a snapshot.
    pub fn get(&self, pk: i64, snap: Snapshot) -> Option<Row> {
        let inner = self.inner.read().unwrap();
        match Self::locate(&inner, pk, snap, &self.metrics)? {
            Location::L0(i) => Some(inner.l0.entries()[i].row.clone()),
            Location::Part(pi, ord) => Some(inner.parts[pi].batch().row(ord as usize)),
        }
    }

    /// Read at the newest committed state.
    pub fn get_latest(&self, pk: i64) -> Option<Row> {
        self.get(pk, self.snapshot())
    }

    /// Full scan at a snapshot.
    ///
    /// Captures the part list under a brief read lock, then scans outside it so
    /// writers are not blocked for the duration.
    pub fn scan(&self, snap: Snapshot) -> Batch {
        let (parts, l0_rows) = {
            let inner = self.inner.read().unwrap();
            (inner.parts.clone(), inner.l0.scan(snap))
        };
        Metrics::bump(&self.metrics.scans);

        let mut out = Batch::new();
        for p in &parts {
            if p.is_fully_visible_to(snap) {
                Metrics::bump(&self.metrics.scan_fast_path);
            } else {
                Metrics::bump(&self.metrics.scan_slow_path);
            }
            out.extend(&p.scan(snap));
        }
        out.extend(&l0_rows);
        out
    }

    /// Number of visible rows, without materialising them.
    pub fn row_count(&self, snap: Snapshot) -> usize {
        let inner = self.inner.read().unwrap();
        let from_parts: usize = inner.parts.iter().map(|p| p.visible_count(snap)).sum();
        from_parts + inner.l0.visible_count(snap)
    }

    /// Force L0 into a sealed part. No-op when L0 is empty.
    pub fn seal(&self) {
        let mut inner = self.inner.write().unwrap();
        Self::seal_locked(&mut inner, &self.metrics);
    }

    /// Run compaction if the policy says it is due. Returns parts merged.
    pub fn maybe_compact(&self, horizon: Csn) -> usize {
        let candidates = {
            let inner = self.inner.read().unwrap();
            if !self.config.compaction.should_compact(&inner.parts) {
                return 0;
            }
            self.config.compaction.select(&inner.parts).to_vec()
        };
        self.run_merge(candidates, horizon)
    }

    /// Compact unconditionally.
    pub fn force_compact(&self, horizon: Csn) -> usize {
        let candidates = { self.inner.read().unwrap().parts.clone() };
        self.run_merge(candidates, horizon)
    }

    /// Two-phase merge: build outside the lock, install under it.
    ///
    /// This is the fix for the M0 defect where compaction held the write lock
    /// for the whole merge and collapsed write throughput 18×. Writers keep
    /// running throughout phase 1; any tombstones they produce are replayed
    /// into the merged part during phase 2.
    fn run_merge(&self, candidates: Vec<Arc<Part>>, horizon: Csn) -> usize {
        if candidates.is_empty() {
            return 0;
        }
        // Reserve an id up front so phase 1 needs no shared mutable state.
        let new_id = {
            let mut inner = self.inner.write().unwrap();
            let id = inner.next_part_id;
            inner.next_part_id += 1;
            id
        };
        let started_at = self.csn.current();

        // ---- Phase 1: no locks held. This is the expensive part. ----
        let plan = match compaction::plan_merge(&candidates, new_id, horizon, started_at) {
            Some(p) => p,
            None => return 0,
        };
        drop(candidates);

        // ---- Phase 2: pointer swap plus tombstone replay. ----
        let mut inner = self.inner.write().unwrap();
        compaction::apply_plan(&mut inner.parts, plan, &self.metrics)
    }

    // ---- recovery hooks -------------------------------------------------

    /// Replace the part list wholesale. Used by recovery after loading part
    /// files from disk; never call this on a live table.
    pub fn install_parts(&self, parts: Vec<Arc<Part>>, next_part_id: u64) {
        let mut inner = self.inner.write().unwrap();
        inner.parts = parts;
        inner.next_part_id = next_part_id;
    }

    /// Insert a row assumed new, allocating a fresh CSN and skipping the
    /// duplicate-key probe. Backs `Storage::load_batch`.
    pub fn replay_insert_new(&self, row: Row) -> Csn {
        let mut inner = self.inner.write().unwrap();
        let csn = self.csn.allocate();
        inner.l0.insert(row, csn);
        Metrics::bump(&self.metrics.inserts);
        self.maybe_seal_locked(&mut inner);
        csn
    }

    /// Apply a logged insert during recovery, preserving its original CSN.
    ///
    /// Unlike [`Table::upsert`] this does not allocate a CSN — replay must
    /// reproduce the exact version stamps the original run wrote, or snapshots
    /// taken before the crash would resolve differently after it.
    pub fn replay_insert(&self, row: Row, csn: Csn) {
        let mut inner = self.inner.write().unwrap();
        let snap = Snapshot::at(csn.saturating_sub(1));
        if let Some(loc) = Self::locate(&inner, row.pk, snap, &self.metrics) {
            let _ = Self::tombstone(&mut inner, loc, csn);
        }
        inner.l0.insert(row, csn);
    }

    /// Apply a logged delete during recovery.
    pub fn replay_delete(&self, pk: i64, csn: Csn) {
        let mut inner = self.inner.write().unwrap();
        let snap = Snapshot::at(csn.saturating_sub(1));
        if let Some(loc) = Self::locate(&inner, pk, snap, &self.metrics) {
            let _ = Self::tombstone(&mut inner, loc, csn);
        }
    }

    /// Current parts and the next part id, for checkpointing.
    pub fn parts_snapshot(&self) -> (Vec<Arc<Part>>, u64) {
        let inner = self.inner.read().unwrap();
        (inner.parts.clone(), inner.next_part_id)
    }

    pub fn stats(&self) -> TableStats {
        let inner = self.inner.read().unwrap();
        let index_bytes: usize = inner.parts.iter().map(|p| p.index_memory_bytes()).sum();
        let data_bytes: usize = inner.parts.iter().map(|p| p.data_memory_bytes()).sum();
        let part_rows: usize = inner.parts.iter().map(|p| p.num_rows()).sum();
        let tombstones: usize = inner.parts.iter().map(|p| p.dv_len()).sum();
        TableStats {
            name: self.name.clone(),
            num_parts: inner.parts.len(),
            part_rows,
            l0_rows: inner.l0.len(),
            tombstones,
            index_bytes,
            data_bytes,
            l0_bytes: inner.l0.memory_bytes(),
        }
    }

    // ---- internals -------------------------------------------------------

    /// Find the live version of `pk`, newest tier first.
    fn locate(
        inner: &TableInner,
        pk: i64,
        snap: Snapshot,
        metrics: &Metrics,
    ) -> Option<Location> {
        Metrics::bump(&metrics.lookups);
        if let Some(i) = inner.l0.lookup(pk, snap) {
            return Some(Location::L0(i));
        }
        for (pi, part) in inner.parts.iter().enumerate() {
            Metrics::bump(&metrics.parts_probed);
            use crate::part::LookupResult::*;
            match part.lookup(pk, snap) {
                OutOfBounds => {
                    Metrics::bump(&metrics.bounds_skips);
                }
                BloomMiss => {
                    Metrics::bump(&metrics.bloom_skips);
                }
                Found(ord) => return Some(Location::Part(pi, ord)),
                NotPresent | NotVisible => {}
            }
        }
        None
    }

    fn tombstone(inner: &mut TableInner, loc: Location, csn: Csn) -> Result<()> {
        let ok = match loc {
            Location::L0(i) => inner.l0.mark_deleted(i, csn),
            Location::Part(pi, ord) => inner.parts[pi].mark_deleted(ord, csn),
        };
        if ok {
            Ok(())
        } else {
            Err(Error::WriteConflict)
        }
    }

    fn maybe_seal_locked(&self, inner: &mut TableInner) {
        if inner.l0.len() >= self.config.seal_threshold {
            Self::seal_locked(inner, &self.metrics);
        }
    }

    fn seal_locked(inner: &mut TableInner, metrics: &Metrics) {
        if inner.l0.is_empty() {
            return;
        }
        let sealed = inner.l0.seal();
        let id = inner.next_part_id;
        inner.next_part_id += 1;
        let part = Part::with_deletions(
            id,
            sealed.batch,
            CreatedCsns::PerRow(sealed.created),
            &sealed.deletions,
        );
        // Newest first.
        inner.parts.insert(0, Arc::new(part));
        Metrics::bump(&metrics.seals);
    }
}

/// A point-in-time view of a table's physical shape.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub name: String,
    pub num_parts: usize,
    pub part_rows: usize,
    pub l0_rows: usize,
    pub tombstones: usize,
    pub index_bytes: usize,
    pub data_bytes: usize,
    pub l0_bytes: usize,
}

impl TableStats {
    pub fn total_rows(&self) -> usize {
        self.part_rows + self.l0_rows
    }

    /// Index bytes per row — the M0-2 headline.
    pub fn index_bytes_per_row(&self) -> f64 {
        if self.part_rows == 0 {
            return 0.0;
        }
        self.index_bytes as f64 / self.part_rows as f64
    }
}
