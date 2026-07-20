//! Compaction — the mechanism that pays for fast writes.
//!
//! `requirements.md` §5.4 is explicit that compaction is a designed subsystem
//! rather than a background thread that runs when convenient. It is
//! load-bearing for *both* halves of the cost model:
//!
//! * **Reads**: tombstoned rows waste scan bandwidth until reclaimed.
//! * **Writes**: point lookups fan out across parts (§5.2), so unbounded part
//!   growth degrades the write path too. M0-3 measures this.
//!
//! Compaction also performs version-metadata GC: once every row's creation
//! stamp is at or below the horizon, no live snapshot can distinguish them, so
//! the per-row CSN array collapses to a single value and scans of that part
//! become zero-per-row-work (§5.3).

use crate::csn::{Csn, Snapshot, NEVER_DELETED};
use crate::metrics::Metrics;
use crate::part::{CreatedCsns, Part};
use crate::schema::Batch;
use std::sync::Arc;

/// When to compact.
#[derive(Debug, Clone)]
pub struct CompactionPolicy {
    /// Merge once this many parts exist.
    pub max_parts: usize,
    /// Merge when any part is at least this fraction tombstoned.
    pub max_dv_density: f64,
    /// Never merge more than this many parts at once.
    pub max_merge_width: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        CompactionPolicy {
            max_parts: 8,
            max_dv_density: 0.3,
            max_merge_width: 16,
        }
    }
}

impl CompactionPolicy {
    pub fn should_compact(&self, parts: &[Arc<Part>]) -> bool {
        if parts.is_empty() {
            return false;
        }
        if parts.len() >= self.max_parts {
            return true;
        }
        // Deliberately *not* gated on part count. A single part that is mostly
        // tombstones still wastes scan bandwidth on every read, and rewriting
        // it also collapses version stamps. Gating this behind "two or more
        // parts" would let a heavily-deleted table degrade indefinitely.
        parts.iter().any(|p| p.dv_density() >= self.max_dv_density)
    }
}

/// Merge parts into one, dropping rows no snapshot at or after `horizon` can
/// see, and collapsing version stamps where possible.
///
/// Returns the number of parts merged (0 if there are none).
///
/// Accepts a single part: rewriting one part is still useful work, since it
/// reclaims tombstoned rows and collapses version stamps.
///
/// Crash-safety note for M1: this builds the replacement before swapping it in,
/// so a failure part-way leaves the original parts authoritative.
pub fn compact(
    parts: &mut Vec<Arc<Part>>,
    next_part_id: &mut u64,
    horizon: Csn,
    metrics: &Metrics,
) -> usize {
    if parts.is_empty() {
        return 0;
    }

    let snap = Snapshot::at(horizon);
    let merged_count = parts.len();

    // Gather surviving rows from every part.
    let mut rows: Vec<(i64, Csn, Csn, usize, usize)> = Vec::new(); // (pk, created, deleted, part_idx, ordinal)
    for (pi, part) in parts.iter().enumerate() {
        let dv = part.dv_snapshot();
        let batch = part.batch();
        for ord in 0..batch.len() {
            let created = part.created_at(ord);
            let deleted = dv.deleted_at(ord as u32);
            // A row is reclaimable once it is invisible to the horizon
            // snapshot *and* to every newer one, i.e. deleted at/below horizon.
            if deleted <= horizon {
                continue;
            }
            rows.push((batch.pk[ord], created, deleted, pi, ord));
        }
    }

    let reclaimed = parts
        .iter()
        .map(|p| p.num_rows())
        .sum::<usize>()
        .saturating_sub(rows.len());

    // Sort by (pk, created) so the output satisfies Part's invariants.
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut batch = Batch::with_capacity(rows.len());
    let mut created = Vec::with_capacity(rows.len());
    let mut deletions = Vec::new();

    for (out_ord, &(_, c, d, pi, ord)) in rows.iter().enumerate() {
        batch.push(&parts[pi].batch().row(ord));
        created.push(c);
        if d != NEVER_DELETED {
            deletions.push((out_ord as u32, d));
        }
    }

    let id = *next_part_id;
    *next_part_id += 1;

    let mut merged = Part::with_deletions(
        id,
        batch,
        CreatedCsns::PerRow(created),
        &deletions,
    );
    // Version-metadata GC: collapse per-row stamps when indistinguishable.
    merged.collapse_versions(horizon);

    let _ = snap; // horizon is expressed as a Csn throughout
    parts.clear();
    parts.push(Arc::new(merged));

    Metrics::bump(&metrics.compactions);
    Metrics::add(&metrics.parts_merged, merged_count as u64);
    Metrics::add(&metrics.rows_reclaimed, reclaimed as u64);
    merged_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Row;

    fn part(id: u64, pks: &[i64], csn: Csn) -> Arc<Part> {
        let batch: Batch = pks
            .iter()
            .map(|&pk| Row::new(pk, pk, pk as f64, format!("v{pk}")))
            .collect();
        Arc::new(Part::new(id, batch, CreatedCsns::Uniform(csn)))
    }

    #[test]
    fn policy_ignores_trivial_part_counts() {
        let p = CompactionPolicy::default();
        assert!(!p.should_compact(&[]));
        assert!(!p.should_compact(&[part(0, &[1], 1)]));
    }

    #[test]
    fn policy_triggers_on_part_count() {
        let p = CompactionPolicy {
            max_parts: 3,
            ..Default::default()
        };
        let parts: Vec<_> = (0..3).map(|i| part(i, &[i as i64], 1)).collect();
        assert!(p.should_compact(&parts));
    }

    #[test]
    fn policy_triggers_on_dv_density() {
        let p = CompactionPolicy {
            max_parts: 100,
            max_dv_density: 0.4,
            ..Default::default()
        };
        let a = part(0, &[1, 2, 3, 4], 1);
        a.mark_deleted(0, 5);
        a.mark_deleted(1, 5);
        let parts = vec![a, part(1, &[9], 1)];
        assert!(p.should_compact(&parts));
    }

    #[test]
    fn policy_triggers_on_single_heavily_deleted_part() {
        let p = CompactionPolicy {
            max_parts: 100,
            max_dv_density: 0.3,
            ..Default::default()
        };
        let a = part(0, &[1, 2, 3, 4], 1);
        a.mark_deleted(0, 5);
        a.mark_deleted(1, 5);
        assert!(
            p.should_compact(&[a]),
            "a lone part that is 50% tombstones must still compact"
        );
    }

    #[test]
    fn compact_of_empty_set_is_noop() {
        let mut parts: Vec<Arc<Part>> = vec![];
        let mut next = 0;
        assert_eq!(compact(&mut parts, &mut next, 10, &Metrics::new()), 0);
    }

    #[test]
    fn compact_of_single_part_still_reclaims() {
        let p = part(0, &[1, 2, 3], 1);
        p.mark_deleted(1, 5);
        let mut parts = vec![p];
        let mut next = 1;
        assert_eq!(compact(&mut parts, &mut next, 10, &Metrics::new()), 1);
        assert_eq!(parts[0].batch().pk, vec![1, 3], "tombstoned row reclaimed");
        assert!(parts[0].created_is_uniform(), "stamps collapsed");
    }

    #[test]
    fn compact_merges_into_one_sorted_part() {
        let mut parts = vec![part(0, &[5, 9], 1), part(1, &[1, 7], 1)];
        let mut next = 2;
        let n = compact(&mut parts, &mut next, 10, &Metrics::new());
        assert_eq!(n, 2);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].batch().pk, vec![1, 5, 7, 9]);
        assert!(parts[0].batch().is_sorted_by_pk());
    }

    #[test]
    fn compact_drops_rows_deleted_below_horizon() {
        let a = part(0, &[1, 2, 3], 1);
        a.mark_deleted(1, 5); // pk=2 deleted at 5
        let mut parts = vec![a, part(1, &[4], 1)];
        let mut next = 2;
        compact(&mut parts, &mut next, 10, &Metrics::new());
        assert_eq!(parts[0].batch().pk, vec![1, 3, 4], "pk=2 should be reclaimed");
    }

    #[test]
    fn compact_retains_rows_still_visible_to_horizon() {
        let a = part(0, &[1, 2], 1);
        a.mark_deleted(1, 100); // deleted in the future relative to horizon
        let mut parts = vec![a, part(1, &[3], 1)];
        let mut next = 2;
        compact(&mut parts, &mut next, 10, &Metrics::new());
        assert!(
            parts[0].batch().pk.contains(&2),
            "row still visible at horizon must survive"
        );
    }

    #[test]
    fn compact_collapses_version_stamps() {
        let mut parts = vec![part(0, &[1], 5), part(1, &[2], 7)];
        let mut next = 2;
        compact(&mut parts, &mut next, 100, &Metrics::new());
        assert!(
            parts[0].created_is_uniform(),
            "stamps below horizon should collapse to uniform"
        );
    }

    #[test]
    fn compact_keeps_stamps_when_distinguishable() {
        let mut parts = vec![part(0, &[1], 5), part(1, &[2], 500)];
        let mut next = 2;
        compact(&mut parts, &mut next, 10, &Metrics::new());
        assert!(!parts[0].created_is_uniform());
    }

    #[test]
    fn compacted_part_scans_correctly() {
        let a = part(0, &[1, 2, 3], 1);
        a.mark_deleted(0, 4);
        let mut parts = vec![a, part(1, &[10, 11], 2)];
        let mut next = 2;
        compact(&mut parts, &mut next, 50, &Metrics::new());
        let got = parts[0].scan(Snapshot::at(60));
        assert_eq!(got.pk, vec![2, 3, 10, 11]);
    }

    #[test]
    fn compact_updates_metrics() {
        let m = Metrics::new();
        let a = part(0, &[1, 2], 1);
        a.mark_deleted(0, 3);
        let mut parts = vec![a, part(1, &[5], 1)];
        let mut next = 2;
        compact(&mut parts, &mut next, 10, &m);
        assert_eq!(Metrics::get(&m.compactions), 1);
        assert_eq!(Metrics::get(&m.parts_merged), 2);
        assert_eq!(Metrics::get(&m.rows_reclaimed), 1);
    }

    #[test]
    fn repeated_compaction_is_stable() {
        let mut parts = vec![part(0, &[1], 1), part(1, &[2], 1)];
        let mut next = 2;
        compact(&mut parts, &mut next, 10, &Metrics::new());
        let before = parts[0].batch().pk.clone();
        // Compacting again is idempotent in content.
        compact(&mut parts, &mut next, 10, &Metrics::new());
        assert_eq!(parts[0].batch().pk, before);
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn compaction_preserves_duplicate_key_versions() {
        let batch: Batch = vec![
            Row::new(1, 0, 0.0, "old"),
            Row::new(1, 0, 0.0, "new"),
        ]
        .into_iter()
        .collect();
        let p = Arc::new(Part::with_deletions(
            0,
            batch,
            CreatedCsns::PerRow(vec![10, 20]),
            &[(0, 20)],
        ));
        let mut parts = vec![p, part(1, &[9], 1)];
        let mut next = 2;
        // Horizon below the delete: the old version is still needed.
        compact(&mut parts, &mut next, 15, &Metrics::new());

        let version_of_pk1 = |csn: Csn| -> Vec<String> {
            let b = parts[0].scan(Snapshot::at(csn));
            (0..b.len())
                .filter(|&i| b.pk[i] == 1)
                .map(|i| b.c[i].clone())
                .collect()
        };
        assert_eq!(version_of_pk1(15), vec!["old".to_string()]);
        assert_eq!(version_of_pk1(25), vec!["new".to_string()]);
        // The unrelated part's row survives untouched in both views.
        assert!(parts[0].scan(Snapshot::at(15)).pk.contains(&9));
    }
}
