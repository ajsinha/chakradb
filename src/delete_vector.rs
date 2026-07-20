//! Deletion vectors — CSN-versioned row-level tombstones.
//!
//! Per `requirements.md` §5.3, deletes never modify a sealed part in place.
//! They append `(row_ordinal, deleted_csn)` to that part's deletion vector.
//!
//! The design specifies RoaringBitmaps (matching Delta's on-disk deletion
//! vectors byte-for-byte). M0 uses a sorted `Vec` instead, because M0 measures
//! index memory and concurrency rather than DV encoding, and because pure-Rust
//! `roaring`'s SIMD path requires nightly. The swap is localised to this file;
//! see `docs/m0-findings.md`.
//!
//! The critical property for §5.3's central claim is `min_deleted_csn`: it lets
//! a scan decide *once per part* that no deletion can affect its snapshot, and
//! then do zero per-row work.

use crate::csn::{Csn, Snapshot, NEVER_DELETED};

/// Row-level tombstones for one part.
#[derive(Debug, Clone, Default)]
pub struct DeleteVector {
    /// Sorted by ordinal. Each ordinal appears at most once — a row can only
    /// be deleted once, since the delete is what makes it invisible.
    entries: Vec<(u32, Csn)>,
    min_deleted: Csn,
    max_deleted: Csn,
}

impl DeleteVector {
    pub fn new() -> Self {
        DeleteVector {
            entries: Vec::new(),
            min_deleted: NEVER_DELETED,
            max_deleted: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of tombstoned rows, regardless of snapshot.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Lowest CSN at which any row here was deleted.
    #[inline]
    pub fn min_deleted_csn(&self) -> Csn {
        self.min_deleted
    }

    /// Highest CSN at which any row here was deleted.
    #[inline]
    pub fn max_deleted_csn(&self) -> Csn {
        self.max_deleted
    }

    /// Mark `ordinal` deleted at `csn`.
    ///
    /// Returns `false` if the row was already deleted (a caller bug — it means
    /// two transactions both believed they owned the live version).
    pub fn mark_deleted(&mut self, ordinal: u32, csn: Csn) -> bool {
        match self.entries.binary_search_by_key(&ordinal, |e| e.0) {
            Ok(_) => false,
            Err(pos) => {
                self.entries.insert(pos, (ordinal, csn));
                self.min_deleted = self.min_deleted.min(csn);
                self.max_deleted = self.max_deleted.max(csn);
                true
            }
        }
    }

    /// The CSN at which `ordinal` was deleted, or `NEVER_DELETED`.
    #[inline]
    pub fn deleted_at(&self, ordinal: u32) -> Csn {
        match self.entries.binary_search_by_key(&ordinal, |e| e.0) {
            Ok(i) => self.entries[i].1,
            Err(_) => NEVER_DELETED,
        }
    }

    /// True if `ordinal` is deleted as far as `snap` is concerned.
    #[inline]
    pub fn is_deleted_for(&self, ordinal: u32, snap: Snapshot) -> bool {
        self.deleted_at(ordinal) <= snap.csn
    }

    /// The §5.3 fast path: can this snapshot ignore the deletion vector
    /// entirely? True when empty, or when every deletion happened after the
    /// snapshot was taken.
    #[inline]
    pub fn is_irrelevant_to(&self, snap: Snapshot) -> bool {
        self.entries.is_empty() || snap.unaffected_by_deletes_from(self.min_deleted)
    }

    /// Count of rows deleted at or before `snap`.
    pub fn deleted_count_for(&self, snap: Snapshot) -> usize {
        if self.is_irrelevant_to(snap) {
            return 0;
        }
        self.entries.iter().filter(|(_, c)| *c <= snap.csn).count()
    }

    /// Fraction of `total_rows` that are tombstoned. Drives compaction (§5.4).
    pub fn density(&self, total_rows: usize) -> f64 {
        if total_rows == 0 {
            return 0.0;
        }
        self.entries.len() as f64 / total_rows as f64
    }

    /// Ordinals deleted at or before `snap`, ascending.
    pub fn deleted_ordinals_for(&self, snap: Snapshot) -> Vec<u32> {
        self.entries
            .iter()
            .filter(|(_, c)| *c <= snap.csn)
            .map(|(o, _)| *o)
            .collect()
    }

    /// Entries recorded strictly after `csn`, as `(ordinal, deleted_csn)`.
    ///
    /// Used by two-phase compaction to replay tombstones that landed while a
    /// merge was running outside the lock.
    pub fn entries_after(&self, csn: Csn) -> Vec<(u32, Csn)> {
        self.entries
            .iter()
            .filter(|(_, c)| *c > csn)
            .copied()
            .collect()
    }

    /// Approximate resident bytes.
    pub fn memory_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<(u32, Csn)>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty_and_irrelevant() {
        let dv = DeleteVector::new();
        assert!(dv.is_empty());
        assert_eq!(dv.len(), 0);
        assert!(dv.is_irrelevant_to(Snapshot::at(u64::MAX - 1)));
        assert_eq!(dv.min_deleted_csn(), NEVER_DELETED);
    }

    #[test]
    fn mark_and_query() {
        let mut dv = DeleteVector::new();
        assert!(dv.mark_deleted(5, 100));
        assert_eq!(dv.deleted_at(5), 100);
        assert_eq!(dv.deleted_at(4), NEVER_DELETED);
        assert_eq!(dv.len(), 1);
    }

    #[test]
    fn double_delete_is_rejected() {
        let mut dv = DeleteVector::new();
        assert!(dv.mark_deleted(5, 100));
        assert!(!dv.mark_deleted(5, 200));
        assert_eq!(dv.deleted_at(5), 100, "original CSN must be preserved");
        assert_eq!(dv.len(), 1);
    }

    #[test]
    fn entries_stay_sorted_under_random_insertion() {
        let mut dv = DeleteVector::new();
        for o in [50u32, 3, 99, 1, 27, 60, 12] {
            dv.mark_deleted(o, o as Csn + 1);
        }
        let ords: Vec<u32> = dv.entries.iter().map(|e| e.0).collect();
        let mut sorted = ords.clone();
        sorted.sort_unstable();
        assert_eq!(ords, sorted);
        // And lookups still work for every one.
        for o in [50u32, 3, 99, 1, 27, 60, 12] {
            assert_eq!(dv.deleted_at(o), o as Csn + 1);
        }
    }

    #[test]
    fn min_and_max_track_extremes() {
        let mut dv = DeleteVector::new();
        dv.mark_deleted(1, 50);
        dv.mark_deleted(2, 10);
        dv.mark_deleted(3, 90);
        assert_eq!(dv.min_deleted_csn(), 10);
        assert_eq!(dv.max_deleted_csn(), 90);
    }

    #[test]
    fn visibility_respects_snapshot() {
        let mut dv = DeleteVector::new();
        dv.mark_deleted(7, 100);
        assert!(!dv.is_deleted_for(7, Snapshot::at(99)), "before the delete");
        assert!(dv.is_deleted_for(7, Snapshot::at(100)), "at the delete");
        assert!(dv.is_deleted_for(7, Snapshot::at(101)), "after the delete");
    }

    #[test]
    fn fast_path_when_all_deletes_are_newer() {
        let mut dv = DeleteVector::new();
        dv.mark_deleted(1, 500);
        dv.mark_deleted(2, 600);
        assert!(dv.is_irrelevant_to(Snapshot::at(499)));
        assert!(!dv.is_irrelevant_to(Snapshot::at(500)));
    }

    #[test]
    fn fast_path_and_slow_path_agree() {
        let mut dv = DeleteVector::new();
        dv.mark_deleted(2, 100);
        dv.mark_deleted(5, 200);
        for csn in [0u64, 50, 99, 100, 150, 200, 300] {
            let snap = Snapshot::at(csn);
            if dv.is_irrelevant_to(snap) {
                for o in 0..10u32 {
                    assert!(!dv.is_deleted_for(o, snap), "fast path lied at csn={csn}");
                }
            }
        }
    }

    #[test]
    fn deleted_count_for_snapshot() {
        let mut dv = DeleteVector::new();
        dv.mark_deleted(1, 10);
        dv.mark_deleted(2, 20);
        dv.mark_deleted(3, 30);
        assert_eq!(dv.deleted_count_for(Snapshot::at(5)), 0);
        assert_eq!(dv.deleted_count_for(Snapshot::at(20)), 2);
        assert_eq!(dv.deleted_count_for(Snapshot::at(100)), 3);
    }

    #[test]
    fn deleted_ordinals_are_filtered_and_sorted() {
        let mut dv = DeleteVector::new();
        dv.mark_deleted(9, 10);
        dv.mark_deleted(1, 30);
        dv.mark_deleted(4, 20);
        assert_eq!(dv.deleted_ordinals_for(Snapshot::at(20)), vec![4, 9]);
        assert_eq!(dv.deleted_ordinals_for(Snapshot::at(30)), vec![1, 4, 9]);
    }

    #[test]
    fn density_computation() {
        let mut dv = DeleteVector::new();
        assert_eq!(dv.density(0), 0.0);
        assert_eq!(dv.density(100), 0.0);
        for o in 0..25u32 {
            dv.mark_deleted(o, 1);
        }
        assert!((dv.density(100) - 0.25).abs() < 1e-9);
    }

    #[test]
    fn entries_after_filters_by_csn() {
        let mut dv = DeleteVector::new();
        dv.mark_deleted(1, 10);
        dv.mark_deleted(2, 50);
        dv.mark_deleted(3, 90);
        assert_eq!(dv.entries_after(50), vec![(3, 90)]);
        assert_eq!(dv.entries_after(0).len(), 3);
        assert!(dv.entries_after(90).is_empty());
    }

    #[test]
    fn memory_grows_with_entries() {
        let mut dv = DeleteVector::new();
        let empty = dv.memory_bytes();
        for o in 0..1000u32 {
            dv.mark_deleted(o, 1);
        }
        assert!(dv.memory_bytes() > empty);
    }

    #[test]
    fn large_vector_lookups_are_correct() {
        let mut dv = DeleteVector::new();
        for o in (0..10_000u32).step_by(2) {
            dv.mark_deleted(o, 42);
        }
        for o in 0..1000u32 {
            let expect = if o % 2 == 0 { 42 } else { NEVER_DELETED };
            assert_eq!(dv.deleted_at(o), expect, "ordinal {o}");
        }
    }
}
