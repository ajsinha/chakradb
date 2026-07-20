//! Engine counters.
//!
//! `requirements.md` §5.4 requires that compaction debt and backpressure be
//! *observable* rather than inferred — "silent degradation is forbidden".
//! These counters are also how the M0 benchmark verifies that the fast paths it
//! claims to be measuring were actually taken.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic counters. Cheap enough to update on hot paths.
#[derive(Debug, Default)]
pub struct Metrics {
    pub inserts: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub scans: AtomicU64,

    /// Point lookups issued.
    pub lookups: AtomicU64,
    /// Parts examined across all lookups — the fan-out cost of §5.2.
    pub parts_probed: AtomicU64,
    /// Parts skipped by the min/max bounds check (funnel stage 1).
    pub bounds_skips: AtomicU64,
    /// Parts skipped by the Bloom filter (funnel stage 2).
    pub bloom_skips: AtomicU64,

    /// Part scans that took the zero-per-row-work fast path (§5.3).
    pub scan_fast_path: AtomicU64,
    /// Part scans that had to test rows individually.
    pub scan_slow_path: AtomicU64,

    pub seals: AtomicU64,
    pub compactions: AtomicU64,
    pub parts_merged: AtomicU64,
    pub rows_reclaimed: AtomicU64,
    /// Times ingest was slowed because compaction fell behind.
    pub backpressure_events: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Average number of parts probed per lookup — the number that grows as
    /// part count grows, and which compaction exists to bound.
    pub fn fanout_per_lookup(&self) -> f64 {
        let l = Self::get(&self.lookups);
        if l == 0 {
            return 0.0;
        }
        Self::get(&self.parts_probed) as f64 / l as f64
    }

    /// Fraction of part-scans that avoided per-row visibility checks.
    pub fn fast_path_ratio(&self) -> f64 {
        let fast = Self::get(&self.scan_fast_path);
        let slow = Self::get(&self.scan_slow_path);
        if fast + slow == 0 {
            return 0.0;
        }
        fast as f64 / (fast + slow) as f64
    }

    /// Fraction of probes eliminated before touching data.
    pub fn skip_ratio(&self) -> f64 {
        let probed = Self::get(&self.parts_probed);
        if probed == 0 {
            return 0.0;
        }
        let skipped = Self::get(&self.bounds_skips) + Self::get(&self.bloom_skips);
        skipped as f64 / probed as f64
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            inserts: Self::get(&self.inserts),
            updates: Self::get(&self.updates),
            deletes: Self::get(&self.deletes),
            scans: Self::get(&self.scans),
            lookups: Self::get(&self.lookups),
            parts_probed: Self::get(&self.parts_probed),
            bounds_skips: Self::get(&self.bounds_skips),
            bloom_skips: Self::get(&self.bloom_skips),
            scan_fast_path: Self::get(&self.scan_fast_path),
            scan_slow_path: Self::get(&self.scan_slow_path),
            seals: Self::get(&self.seals),
            compactions: Self::get(&self.compactions),
            parts_merged: Self::get(&self.parts_merged),
            rows_reclaimed: Self::get(&self.rows_reclaimed),
            backpressure_events: Self::get(&self.backpressure_events),
        }
    }
}

/// A plain-value copy for reporting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub scans: u64,
    pub lookups: u64,
    pub parts_probed: u64,
    pub bounds_skips: u64,
    pub bloom_skips: u64,
    pub scan_fast_path: u64,
    pub scan_slow_path: u64,
    pub seals: u64,
    pub compactions: u64,
    pub parts_merged: u64,
    pub rows_reclaimed: u64,
    pub backpressure_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        let m = Metrics::new();
        assert_eq!(Metrics::get(&m.inserts), 0);
        assert_eq!(m.snapshot(), MetricsSnapshot::default());
    }

    #[test]
    fn bump_and_add() {
        let m = Metrics::new();
        Metrics::bump(&m.inserts);
        Metrics::bump(&m.inserts);
        Metrics::add(&m.lookups, 10);
        assert_eq!(Metrics::get(&m.inserts), 2);
        assert_eq!(Metrics::get(&m.lookups), 10);
    }

    #[test]
    fn fanout_is_zero_without_lookups() {
        assert_eq!(Metrics::new().fanout_per_lookup(), 0.0);
    }

    #[test]
    fn fanout_computation() {
        let m = Metrics::new();
        Metrics::add(&m.lookups, 4);
        Metrics::add(&m.parts_probed, 10);
        assert!((m.fanout_per_lookup() - 2.5).abs() < 1e-9);
    }

    #[test]
    fn fast_path_ratio_computation() {
        let m = Metrics::new();
        assert_eq!(m.fast_path_ratio(), 0.0);
        Metrics::add(&m.scan_fast_path, 9);
        Metrics::add(&m.scan_slow_path, 1);
        assert!((m.fast_path_ratio() - 0.9).abs() < 1e-9);
    }

    #[test]
    fn skip_ratio_computation() {
        let m = Metrics::new();
        assert_eq!(m.skip_ratio(), 0.0);
        Metrics::add(&m.parts_probed, 100);
        Metrics::add(&m.bounds_skips, 60);
        Metrics::add(&m.bloom_skips, 30);
        assert!((m.skip_ratio() - 0.9).abs() < 1e-9);
    }

    #[test]
    fn snapshot_captures_all_fields() {
        let m = Metrics::new();
        Metrics::add(&m.inserts, 1);
        Metrics::add(&m.updates, 2);
        Metrics::add(&m.deletes, 3);
        Metrics::add(&m.compactions, 4);
        let s = m.snapshot();
        assert_eq!(s.inserts, 1);
        assert_eq!(s.updates, 2);
        assert_eq!(s.deletes, 3);
        assert_eq!(s.compactions, 4);
    }

    #[test]
    fn concurrent_bumps_are_not_lost() {
        use std::sync::Arc;
        let m = Arc::new(Metrics::new());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let m = m.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        Metrics::bump(&m.inserts);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(Metrics::get(&m.inserts), 8000);
    }
}
