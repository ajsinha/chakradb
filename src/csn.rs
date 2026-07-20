//! Commit sequence numbers and snapshots.
//!
//! ChakraDB uses snapshot isolation governed by a monotonic commit sequence
//! number (`requirements.md` §5.3). A row version is visible to a snapshot `S`
//! iff `created_csn <= S < deleted_csn`.
//!
//! The CSN counter is the one piece of global state every writer touches. The
//! design deliberately serialises writers on it rather than building a
//! concurrent index — contention on a single counter is far cheaper than the
//! alternative, and M0-3 measures whether that holds.

use std::sync::atomic::{AtomicU64, Ordering};

/// A commit sequence number. Monotonically increasing, never reused.
pub type Csn = u64;

/// CSN reserved to mean "not deleted". Chosen as `u64::MAX` so that the
/// visibility predicate needs no branch for the common case.
pub const NEVER_DELETED: Csn = u64::MAX;

/// The first CSN handed out. Zero is reserved as "before all data", which
/// makes an empty snapshot trivially see nothing.
pub const FIRST_CSN: Csn = 1;

/// Allocates commit sequence numbers.
#[derive(Debug)]
pub struct CsnGenerator {
    next: AtomicU64,
}

impl CsnGenerator {
    pub fn new() -> Self {
        CsnGenerator {
            next: AtomicU64::new(FIRST_CSN),
        }
    }

    /// Allocate the next CSN.
    pub fn allocate(&self) -> Csn {
        self.next.fetch_add(1, Ordering::SeqCst)
    }

    /// The highest CSN allocated so far, i.e. the newest visible state.
    /// Returns `FIRST_CSN - 1` (== 0) when nothing has been allocated.
    pub fn current(&self) -> Csn {
        self.next.load(Ordering::SeqCst) - 1
    }

    /// Take a snapshot of the current committed state.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            csn: self.current(),
        }
    }
}

impl Default for CsnGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time view of the database.
///
/// Readers hold one of these and never block: visibility is a pure function of
/// the snapshot CSN and the row's version stamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Snapshot {
    pub csn: Csn,
}

impl Snapshot {
    pub fn at(csn: Csn) -> Self {
        Snapshot { csn }
    }

    /// The visibility predicate. This is the hot path.
    #[inline]
    pub fn sees(&self, created: Csn, deleted: Csn) -> bool {
        created <= self.csn && self.csn < deleted
    }

    /// True if every row created at or before `created_max` is visible —
    /// used to skip per-row version checks entirely for cold parts.
    #[inline]
    pub fn sees_all_created_up_to(&self, created_max: Csn) -> bool {
        created_max <= self.csn
    }

    /// True if no deletion in this part can affect this snapshot.
    #[inline]
    pub fn unaffected_by_deletes_from(&self, min_deleted: Csn) -> bool {
        self.csn < min_deleted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn allocation_starts_at_first_csn() {
        let g = CsnGenerator::new();
        assert_eq!(g.allocate(), FIRST_CSN);
    }

    #[test]
    fn allocation_is_monotonic() {
        let g = CsnGenerator::new();
        let mut last = 0;
        for _ in 0..1000 {
            let c = g.allocate();
            assert!(c > last);
            last = c;
        }
    }

    #[test]
    fn current_reflects_last_allocation() {
        let g = CsnGenerator::new();
        assert_eq!(g.current(), 0);
        let c = g.allocate();
        assert_eq!(g.current(), c);
    }

    #[test]
    fn empty_snapshot_sees_nothing() {
        let g = CsnGenerator::new();
        let s = g.snapshot();
        // Nothing allocated, so no row (created >= 1) can be visible.
        assert!(!s.sees(FIRST_CSN, NEVER_DELETED));
    }

    #[test]
    fn visibility_basic_cases() {
        let s = Snapshot::at(10);
        assert!(s.sees(5, NEVER_DELETED), "created before, never deleted");
        assert!(s.sees(10, NEVER_DELETED), "created exactly at snapshot");
        assert!(!s.sees(11, NEVER_DELETED), "created in the future");
        assert!(s.sees(5, 20), "deleted in the future");
        assert!(!s.sees(5, 10), "deleted exactly at snapshot");
        assert!(!s.sees(5, 7), "deleted before snapshot");
    }

    #[test]
    fn update_shows_exactly_one_version() {
        // Old version created at 1, deleted at 5. New version created at 5.
        let old = (1, 5);
        let new = (5, NEVER_DELETED);
        for csn in 1..10u64 {
            let s = Snapshot::at(csn);
            let visible = [s.sees(old.0, old.1), s.sees(new.0, new.1)]
                .iter()
                .filter(|v| **v)
                .count();
            assert_eq!(visible, 1, "at csn={csn} exactly one version must be visible");
        }
    }

    #[test]
    fn snapshot_before_creation_sees_neither() {
        let s = Snapshot::at(0);
        assert!(!s.sees(1, 5));
        assert!(!s.sees(5, NEVER_DELETED));
    }

    #[test]
    fn fast_path_predicates() {
        let s = Snapshot::at(100);
        assert!(s.sees_all_created_up_to(100));
        assert!(s.sees_all_created_up_to(50));
        assert!(!s.sees_all_created_up_to(101));

        assert!(s.unaffected_by_deletes_from(101));
        assert!(s.unaffected_by_deletes_from(NEVER_DELETED));
        assert!(!s.unaffected_by_deletes_from(100));
        assert!(!s.unaffected_by_deletes_from(50));
    }

    #[test]
    fn fast_path_agrees_with_per_row_check() {
        // If both fast-path predicates hold, every row must be visible.
        let s = Snapshot::at(50);
        let created_max = 40;
        let min_deleted = 60;
        assert!(s.sees_all_created_up_to(created_max));
        assert!(s.unaffected_by_deletes_from(min_deleted));
        for created in 1..=created_max {
            for deleted in [min_deleted, min_deleted + 5, NEVER_DELETED] {
                assert!(s.sees(created, deleted));
            }
        }
    }

    #[test]
    fn snapshots_order_naturally() {
        assert!(Snapshot::at(1) < Snapshot::at(2));
        assert_eq!(Snapshot::at(3), Snapshot::at(3));
    }

    #[test]
    fn concurrent_allocation_is_unique() {
        let g = Arc::new(CsnGenerator::new());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let g = g.clone();
                thread::spawn(move || (0..1000).map(|_| g.allocate()).collect::<Vec<_>>())
            })
            .collect();
        let mut all: Vec<Csn> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
        all.sort_unstable();
        let before = all.len();
        all.dedup();
        assert_eq!(all.len(), before, "duplicate CSNs allocated");
        assert_eq!(all.len(), 8000);
    }
}
