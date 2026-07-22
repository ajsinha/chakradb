//! Sealed, immutable parts — the L1/L2 tier.
//!
//! This module implements the central insight of `requirements.md` §5.2,
//! borrowed from Apache Doris:
//!
//! > Because a part is written **sorted by primary key**, the ordinal position
//! > of a key in the index *is* the row offset in every column. No separate
//! > key→location map exists.
//!
//! The consequence is that per-row index overhead is **zero**. What we pay is a
//! Bloom filter and two bounds per part — regardless of row count. Contrast
//! StarRocks, which stores an explicit 8-byte `(rssid, rowid)` per row and
//! consequently needed an LSM to make the memory tractable. M0-2 measures this.
//!
//! Version metadata follows the same principle: a part records `created_min` /
//! `created_max`, and stores per-row CSNs *only* when they differ. Compaction
//! collapses them to uniform once no snapshot can tell them apart, after which
//! scans do zero per-row visibility work (§5.3). M0-4 measures that.

use crate::batch::Batch;
use crate::csn::{Csn, Snapshot};
use crate::delete_vector::DeleteVector;
use crate::value::Value;
use std::cmp::Ordering;
use std::sync::RwLock;

/// How a part stores row creation stamps.
#[derive(Debug, Clone)]
pub enum CreatedCsns {
    /// Every row shares one CSN — costs nothing per row.
    Uniform(Csn),
    /// Rows differ; one CSN each.
    PerRow(Vec<Csn>),
}

impl CreatedCsns {
    #[inline]
    pub fn at(&self, ordinal: usize) -> Csn {
        match self {
            CreatedCsns::Uniform(c) => *c,
            CreatedCsns::PerRow(v) => v[ordinal],
        }
    }

    pub fn is_uniform(&self) -> bool {
        matches!(self, CreatedCsns::Uniform(_))
    }

    pub fn memory_bytes(&self) -> usize {
        match self {
            CreatedCsns::Uniform(_) => 0,
            CreatedCsns::PerRow(v) => v.capacity() * 8,
        }
    }

    /// Collapse to uniform if all stamps are equal, or if every stamp is at or
    /// below `horizon` (no live snapshot can distinguish them).
    pub fn maybe_collapse(self, horizon: Csn) -> Self {
        match self {
            CreatedCsns::Uniform(_) => self,
            CreatedCsns::PerRow(ref v) => {
                if v.is_empty() {
                    return CreatedCsns::Uniform(0);
                }
                let min = *v.iter().min().unwrap();
                let max = *v.iter().max().unwrap();
                if min == max || max <= horizon {
                    CreatedCsns::Uniform(min)
                } else {
                    self
                }
            }
        }
    }
}

/// Outcome of a point lookup, for metrics and testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupResult {
    /// Key is outside this part's min/max.
    OutOfBounds,
    /// Bloom filter said definitely absent.
    BloomMiss,
    /// Bloom said maybe, binary search disagreed.
    NotPresent,
    /// Present but not visible to this snapshot (deleted, or created later).
    NotVisible,
    /// Live at this ordinal.
    Found(u32),
}

impl LookupResult {
    pub fn ordinal(self) -> Option<u32> {
        match self {
            LookupResult::Found(o) => Some(o),
            _ => None,
        }
    }
}

/// An immutable, PK-sorted run of rows plus its mutable deletion vector.
#[derive(Debug)]
pub struct Part {
    id: u64,
    batch: Batch,
    created: CreatedCsns,
    created_min: Csn,
    created_max: Csn,
    /// Key bounds (of any type), `Null` when the part is empty.
    min_key: Value,
    max_key: Value,
    /// Per-column min/max zonemap (`None` for empty/all-NULL columns), so bare
    /// MIN/MAX and part pruning can skip the scan for clean parts.
    col_bounds: Vec<Option<(Value, Value)>>,
    bloom: crate::bloom::BloomFilter,
    dv: RwLock<DeleteVector>,
}

impl Part {
    /// Build a part from a key-sorted batch.
    ///
    /// Panics if the batch is not sorted by its key column — an internal
    /// invariant whose violation would corrupt lookups.
    pub fn new(id: u64, batch: Batch, created: CreatedCsns) -> Self {
        assert!(
            batch.is_sorted_by_key(),
            "part {id}: batch not sorted by key"
        );
        if let CreatedCsns::PerRow(v) = &created {
            assert_eq!(v.len(), batch.len(), "part {id}: csn/row length mismatch");
        }

        let (min_key, max_key) = if batch.is_empty() {
            (Value::Null, Value::Null)
        } else {
            (batch.key(0), batch.key(batch.len() - 1))
        };

        let (created_min, created_max) = match &created {
            CreatedCsns::Uniform(c) => (*c, *c),
            CreatedCsns::PerRow(v) => (
                v.iter().copied().min().unwrap_or(0),
                v.iter().copied().max().unwrap_or(0),
            ),
        };

        let bloom = crate::bloom::BloomFilter::build_values(&batch.keys());
        let col_bounds = (0..batch.schema().arity())
            .map(|c| batch.column_bounds(c))
            .collect();

        Part {
            id,
            batch,
            created,
            created_min,
            created_max,
            min_key,
            max_key,
            col_bounds,
            bloom,
            dv: RwLock::new(DeleteVector::new()),
        }
    }

    /// The zonemap `(min, max)` of column `col` over *all* rows in this part
    /// (ignoring visibility). Exact for the visible set only when the part is
    /// fully visible to the querying snapshot.
    pub fn col_bounds(&self, col: usize) -> Option<&(Value, Value)> {
        self.col_bounds.get(col).and_then(|b| b.as_ref())
    }

    /// Every column's zonemap `(min, max)` bounds, indexed by column — for
    /// predicate-based part pruning ([`crate::sql::expr::Expr::excludes`]).
    pub fn col_bounds_all(&self) -> &[Option<(Value, Value)>] {
        &self.col_bounds
    }

    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn num_rows(&self) -> usize {
        self.batch.len()
    }
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }
    pub fn min_key(&self) -> &Value {
        &self.min_key
    }
    pub fn max_key(&self) -> &Value {
        &self.max_key
    }
    pub fn created_min(&self) -> Csn {
        self.created_min
    }
    pub fn created_max(&self) -> Csn {
        self.created_max
    }
    pub fn batch(&self) -> &Batch {
        &self.batch
    }
    pub fn created_is_uniform(&self) -> bool {
        self.created.is_uniform()
    }

    /// Creation stamp of row `ordinal`.
    #[inline]
    pub fn created_at(&self, ordinal: usize) -> Csn {
        self.created.at(ordinal)
    }

    /// The four-stage lookup funnel, cheapest filter first (§5.2).
    pub fn lookup(&self, key: &Value, snap: Snapshot) -> LookupResult {
        // Stage 1: bounds. Pure metadata, no probing.
        if self.batch.is_empty()
            || key.total_cmp(&self.min_key).is_lt()
            || key.total_cmp(&self.max_key).is_gt()
        {
            return LookupResult::OutOfBounds;
        }
        // Stage 2: Bloom filter.
        if !self.bloom.maybe_contains_value(key) {
            return LookupResult::BloomMiss;
        }
        // Stage 3: ordered seek. The ordinal *is* the row offset.
        let hit = match self.search_key(key) {
            Ok(i) => i,
            Err(_) => return LookupResult::NotPresent,
        };
        // A key may appear more than once: sealing preserves every version of
        // a row, ordered by creation CSN. Widen to the whole equal-key run.
        let (lo, hi) = self.equal_key_run(hit);

        // Stage 4: visibility, including the deletion-vector recheck.
        // The invariant is that at most one version in the run is visible.
        let dv = self.dv.read().unwrap();
        let dv_relevant = !dv.is_irrelevant_to(snap);
        for ordinal in lo..=hi {
            if self.created.at(ordinal) > snap.csn {
                continue;
            }
            if dv_relevant && dv.is_deleted_for(ordinal as u32, snap) {
                continue;
            }
            return LookupResult::Found(ordinal as u32);
        }
        LookupResult::NotVisible
    }

    /// Binary-search the sorted key column for `key` (via `total_cmp`).
    #[inline]
    fn search_key(&self, key: &Value) -> std::result::Result<usize, usize> {
        let mut lo = 0;
        let mut hi = self.batch.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.batch.key(mid).total_cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Inclusive `(lo, hi)` bounds of the run of rows sharing the key at `i`.
    #[inline]
    fn equal_key_run(&self, i: usize) -> (usize, usize) {
        let key = self.batch.key(i);
        let mut lo = i;
        while lo > 0 && self.batch.key(lo - 1).total_cmp(&key).is_eq() {
            lo -= 1;
        }
        let mut hi = i;
        while hi + 1 < self.batch.len() && self.batch.key(hi + 1).total_cmp(&key).is_eq() {
            hi += 1;
        }
        (lo, hi)
    }

    /// Build a part with pre-existing tombstones, as produced by sealing L0.
    pub fn with_deletions(
        id: u64,
        batch: Batch,
        created: CreatedCsns,
        deletions: &[(u32, Csn)],
    ) -> Self {
        let part = Part::new(id, batch, created);
        {
            let mut dv = part.dv.write().unwrap();
            for &(ordinal, csn) in deletions {
                dv.mark_deleted(ordinal, csn);
            }
        }
        part
    }

    /// Mark a row deleted. Returns false if it was already deleted.
    pub fn mark_deleted(&self, ordinal: u32, csn: Csn) -> bool {
        self.dv.write().unwrap().mark_deleted(ordinal, csn)
    }

    /// True when this snapshot can skip all per-row visibility work.
    ///
    /// This is the §5.3 claim M0-4 exists to verify.
    pub fn is_fully_visible_to(&self, snap: Snapshot) -> bool {
        snap.sees_all_created_up_to(self.created_max)
            && self.dv.read().unwrap().is_irrelevant_to(snap)
    }

    /// Rows visible to `snap`.
    ///
    /// Takes the zero-work fast path when possible, cloning the underlying batch
    /// wholesale (an Arc-cheap Arrow clone) rather than testing rows one by one.
    /// The partial path selects visible ordinals with a columnar `take`.
    pub fn scan(&self, snap: Snapshot) -> Batch {
        if self.is_fully_visible_to(snap) {
            return self.batch.clone();
        }
        let dv = self.dv.read().unwrap();
        let visible: Vec<u32> = (0..self.batch.len())
            .filter(|&i| self.created.at(i) <= snap.csn && !dv.is_deleted_for(i as u32, snap))
            .map(|i| i as u32)
            .collect();
        self.batch.take(&visible)
    }

    /// Count of visible rows, without materialising them.
    pub fn visible_count(&self, snap: Snapshot) -> usize {
        if self.is_fully_visible_to(snap) {
            return self.batch.len();
        }
        let dv = self.dv.read().unwrap();
        (0..self.batch.len())
            .filter(|&i| self.created.at(i) <= snap.csn && !dv.is_deleted_for(i as u32, snap))
            .count()
    }

    /// Fraction of rows tombstoned — a compaction trigger (§5.4).
    pub fn dv_density(&self) -> f64 {
        self.dv.read().unwrap().density(self.batch.len())
    }

    pub fn dv_len(&self) -> usize {
        self.dv.read().unwrap().len()
    }

    /// Bytes of *index* structures: Bloom filter, bounds, version stamps.
    ///
    /// Deliberately excludes column data. This is the M0-2 number, and the
    /// point is that it does not contain a per-row key→location entry.
    pub fn index_memory_bytes(&self) -> usize {
        self.bloom.memory_bytes()
            + self.created.memory_bytes()
            + self.dv.read().unwrap().memory_bytes()
            + std::mem::size_of::<Part>()
    }

    /// Bytes of column data.
    pub fn data_memory_bytes(&self) -> usize {
        self.batch.memory_bytes()
    }

    /// Snapshot the current deletion vector (for compaction).
    pub fn dv_snapshot(&self) -> DeleteVector {
        self.dv.read().unwrap().clone()
    }

    /// Collapse version stamps to uniform if `horizon` allows.
    pub fn collapse_versions(&mut self, horizon: Csn) {
        let taken = std::mem::replace(&mut self.created, CreatedCsns::Uniform(0));
        self.created = taken.maybe_collapse(horizon);
    }
}

#[cfg(test)]
mod tests {
    //! Tests that need access to `Part`'s internals. Behavioural coverage that
    //! only needs the public surface lives in `tests/part_behavior.rs`.
    use super::*;
    use crate::schema::Row;

    fn sorted_batch(pks: &[i64]) -> Batch {
        pks.iter()
            .map(|&pk| Row::new(pk, pk * 10, pk as f64, format!("r{pk}")))
            .collect()
    }

    #[test]
    fn equal_key_run_finds_full_span() {
        let batch: Batch = vec![
            Row::new(1, 0, 0.0, ""),
            Row::new(4, 0, 0.0, ""),
            Row::new(4, 0, 0.0, ""),
            Row::new(4, 0, 0.0, ""),
            Row::new(9, 0, 0.0, ""),
        ]
        .into_iter()
        .collect();
        let p = Part::new(1, batch, CreatedCsns::Uniform(1));
        assert_eq!(p.equal_key_run(2), (1, 3), "middle of a run");
        assert_eq!(p.equal_key_run(1), (1, 3), "start of a run");
        assert_eq!(p.equal_key_run(3), (1, 3), "end of a run");
        assert_eq!(p.equal_key_run(0), (0, 0), "singleton at start");
        assert_eq!(p.equal_key_run(4), (4, 4), "singleton at end");
    }

    #[test]
    fn bounds_are_derived_from_sorted_keys() {
        let p = Part::new(1, sorted_batch(&[3, 7, 19]), CreatedCsns::Uniform(1));
        assert_eq!(p.min_key(), &Value::Int(3));
        assert_eq!(p.max_key(), &Value::Int(19));
        assert_eq!(p.created_min(), 1);
        assert_eq!(p.created_max(), 1);
    }

    #[test]
    fn empty_part_rejects_every_lookup() {
        let p = Part::new(1, Batch::new(), CreatedCsns::Uniform(1));
        assert!(p.is_empty());
        assert!(p.min_key().is_null(), "empty bounds are Null");
        let snap = Snapshot { csn: 100 };
        assert!(matches!(
            p.lookup(&Value::Int(5), snap),
            LookupResult::OutOfBounds
        ));
    }

    #[test]
    #[should_panic(expected = "not sorted by key")]
    fn unsorted_batch_is_rejected() {
        let b: Batch = vec![Row::new(5, 0, 0.0, ""), Row::new(1, 0, 0.0, "")]
            .into_iter()
            .collect();
        Part::new(1, b, CreatedCsns::Uniform(1));
    }

    #[test]
    #[should_panic(expected = "csn/row length mismatch")]
    fn csn_length_mismatch_is_rejected() {
        Part::new(1, sorted_batch(&[1, 2, 3]), CreatedCsns::PerRow(vec![1, 2]));
    }

    #[test]
    fn uniform_versions_cost_no_memory() {
        assert_eq!(CreatedCsns::Uniform(5).memory_bytes(), 0);
        assert!(CreatedCsns::PerRow(vec![1, 2, 3]).memory_bytes() >= 24);
    }

    #[test]
    fn collapse_rules() {
        assert!(CreatedCsns::PerRow(vec![7, 7, 7])
            .maybe_collapse(0)
            .is_uniform());
        assert!(CreatedCsns::PerRow(vec![1, 2, 3])
            .maybe_collapse(10)
            .is_uniform());
        assert!(!CreatedCsns::PerRow(vec![1, 20])
            .maybe_collapse(10)
            .is_uniform());
        assert!(CreatedCsns::PerRow(vec![]).maybe_collapse(0).is_uniform());
    }

    #[test]
    fn collapse_via_part_reduces_memory() {
        let mut p = Part::new(
            1,
            sorted_batch(&[1, 2, 3]),
            CreatedCsns::PerRow(vec![1, 2, 3]),
        );
        assert!(!p.created_is_uniform());
        let before = p.index_memory_bytes();
        p.collapse_versions(100);
        assert!(p.created_is_uniform());
        assert!(p.index_memory_bytes() < before);
    }
}
