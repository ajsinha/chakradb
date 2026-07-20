//! Compaction — the mechanism that pays for fast writes.
//!
//! `requirements.md` §5.4 is explicit that compaction is a designed subsystem
//! rather than a background thread that runs when convenient. It is
//! load-bearing for *both* halves of the cost model:
//!
//! * **Reads**: tombstoned rows waste scan bandwidth until reclaimed.
//! * **Writes**: point lookups fan out across parts (§5.2), so unbounded part
//!   growth degrades the write path too.
//!
//! # Two-phase merge (the M0 defect fix)
//!
//! M0 held the table write lock for the entire merge, which collapsed write
//! throughput 18× (see `m0-findings.md` §4). Compaction is now split:
//!
//! 1. [`plan_merge`] builds the replacement part **holding no lock at all**.
//! 2. [`apply_plan`] takes the write lock only to swap pointers, and to replay
//!    any tombstones that landed on the source parts while the merge ran.
//!
//! Step 2 is what makes step 1 safe: writers are free to keep deleting rows
//! from parts that are being merged, because those deletions are carried
//! forward through the ordinal mapping rather than lost.

use crate::csn::{Csn, NEVER_DELETED};
use crate::metrics::Metrics;
use crate::part::{CreatedCsns, Part};
use crate::schema::Batch;
use std::collections::HashMap;
use std::sync::Arc;

/// When to compact.
#[derive(Debug, Clone)]
pub struct CompactionPolicy {
    /// Merge once this many parts exist.
    pub max_parts: usize,
    /// Merge when any part is at least this fraction tombstoned.
    ///
    /// M0-4 found that a *single* tombstone disables a part's zero-per-row scan
    /// fast path, so this is deliberately lower than intuition suggests.
    pub max_dv_density: f64,
    /// Never merge more than this many parts at once, so a single compaction
    /// cannot monopolise the I/O budget.
    pub max_merge_width: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        CompactionPolicy {
            max_parts: 8,
            max_dv_density: 0.1,
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
        // it also collapses version stamps.
        parts.iter().any(|p| p.dv_density() >= self.max_dv_density)
    }

    /// Choose which parts to merge: the oldest `max_merge_width`, since those
    /// are the ones lookups reach last and are most likely to be cold.
    pub fn select<'a>(&self, parts: &'a [Arc<Part>]) -> &'a [Arc<Part>] {
        let n = parts.len().min(self.max_merge_width);
        &parts[parts.len() - n..]
    }
}

/// A merge built outside the lock, ready to be installed.
#[derive(Debug)]
pub struct MergePlan {
    /// Ids of the parts this plan replaces.
    pub source_ids: Vec<u64>,
    merged: Part,
    /// `(source part id, source ordinal) -> merged ordinal`, used to replay
    /// tombstones that arrived while the merge was running.
    mapping: HashMap<(u64, u32), u32>,
    /// CSN at which the merge began; deletions after this need replaying.
    started_at: Csn,
    rows_reclaimed: usize,
}

impl MergePlan {
    pub fn merged_rows(&self) -> usize {
        self.merged.num_rows()
    }
    pub fn rows_reclaimed(&self) -> usize {
        self.rows_reclaimed
    }
}

/// Build the replacement part. **Takes no locks and mutates nothing.**
///
/// `horizon` is the oldest CSN any live snapshot may observe; rows deleted at
/// or below it are reclaimable. `started_at` should be the current CSN.
pub fn plan_merge(
    parts: &[Arc<Part>],
    new_part_id: u64,
    horizon: Csn,
    started_at: Csn,
) -> Option<MergePlan> {
    if parts.is_empty() {
        return None;
    }

    // (pk, created, deleted, source part id, source ordinal)
    let mut rows: Vec<(i64, Csn, Csn, u64, u32)> = Vec::new();
    let mut total_source_rows = 0usize;

    for part in parts {
        let dv = part.dv_snapshot();
        let batch = part.batch();
        total_source_rows += batch.len();
        for ord in 0..batch.len() {
            let deleted = dv.deleted_at(ord as u32);
            // Reclaimable once invisible to the horizon and everything newer.
            if deleted <= horizon {
                continue;
            }
            rows.push((
                batch.pk[ord],
                part.created_at(ord),
                deleted,
                part.id(),
                ord as u32,
            ));
        }
    }

    let rows_reclaimed = total_source_rows.saturating_sub(rows.len());

    // Sort by (pk, created) so the output satisfies Part's invariants.
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let by_id: HashMap<u64, &Arc<Part>> = parts.iter().map(|p| (p.id(), p)).collect();
    let mut batch = Batch::with_capacity(rows.len());
    let mut created = Vec::with_capacity(rows.len());
    let mut deletions = Vec::new();
    let mut mapping = HashMap::with_capacity(rows.len());

    for (out_ord, &(_, c, d, src_id, src_ord)) in rows.iter().enumerate() {
        let src = by_id[&src_id];
        batch.push(&src.batch().row(src_ord as usize));
        created.push(c);
        if d != NEVER_DELETED {
            deletions.push((out_ord as u32, d));
        }
        mapping.insert((src_id, src_ord), out_ord as u32);
    }

    let mut merged = Part::with_deletions(new_part_id, batch, CreatedCsns::PerRow(created), &deletions);
    // Version-metadata GC: collapse per-row stamps when indistinguishable.
    // M0-2 found this is ~86% of the index budget.
    merged.collapse_versions(horizon);

    Some(MergePlan {
        source_ids: parts.iter().map(|p| p.id()).collect(),
        merged,
        mapping,
        started_at,
        rows_reclaimed,
    })
}

/// Install a plan. **Call this holding the write lock — it is pointer work
/// plus a small tombstone replay, not a merge.**
///
/// Returns the number of parts replaced, or 0 if the plan is stale (its source
/// parts are no longer all present, e.g. a concurrent compaction won the race).
pub fn apply_plan(parts: &mut Vec<Arc<Part>>, plan: MergePlan, metrics: &Metrics) -> usize {
    let present: HashMap<u64, usize> = parts
        .iter()
        .enumerate()
        .map(|(i, p)| (p.id(), i))
        .collect();

    if !plan.source_ids.iter().all(|id| present.contains_key(id)) {
        Metrics::bump(&metrics.compactions_discarded);
        return 0;
    }

    // Replay tombstones that landed while the merge was running. Writers were
    // never blocked, so this is the price of that freedom — and it is bounded
    // by the number of deletes issued during the merge, not by table size.
    let mut replayed = 0u64;
    for &src_id in &plan.source_ids {
        let src = &parts[present[&src_id]];
        let dv = src.dv_snapshot();
        for (src_ord, csn) in dv.entries_after(plan.started_at) {
            if let Some(&out_ord) = plan.mapping.get(&(src_id, src_ord)) {
                if plan.merged.mark_deleted(out_ord, csn) {
                    replayed += 1;
                }
            }
        }
    }
    Metrics::add(&metrics.tombstones_replayed, replayed);

    let replaced = plan.source_ids.len();
    let source: std::collections::HashSet<u64> = plan.source_ids.into_iter().collect();
    parts.retain(|p| !source.contains(&p.id()));
    // Merged output is the oldest run, so it belongs at the end (newest first).
    parts.push(Arc::new(plan.merged));

    Metrics::bump(&metrics.compactions);
    Metrics::add(&metrics.parts_merged, replaced as u64);
    Metrics::add(&metrics.rows_reclaimed, plan.rows_reclaimed as u64);
    replaced
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

    fn run(parts: &mut Vec<Arc<Part>>, next_id: u64, horizon: Csn) -> usize {
        let snapshot = parts.clone();
        match plan_merge(&snapshot, next_id, horizon, horizon) {
            Some(plan) => apply_plan(parts, plan, &Metrics::new()),
            None => 0,
        }
    }

    #[test]
    fn policy_ignores_empty_and_clean_single_parts() {
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
    fn policy_triggers_on_single_heavily_deleted_part() {
        let p = CompactionPolicy::default();
        let a = part(0, &[1, 2, 3, 4], 1);
        a.mark_deleted(0, 5);
        assert!(p.should_compact(&[a]), "25% tombstoned must compact");
    }

    #[test]
    fn select_bounds_merge_width() {
        let p = CompactionPolicy {
            max_merge_width: 3,
            ..Default::default()
        };
        let parts: Vec<_> = (0..10).map(|i| part(i, &[i as i64], 1)).collect();
        let chosen = p.select(&parts);
        assert_eq!(chosen.len(), 3);
        // Oldest (tail of the newest-first vector).
        assert_eq!(chosen[0].id(), 7);
    }

    #[test]
    fn plan_merge_of_empty_is_none() {
        assert!(plan_merge(&[], 0, 10, 10).is_none());
    }

    #[test]
    fn merge_produces_one_sorted_part() {
        let mut parts = vec![part(0, &[5, 9], 1), part(1, &[1, 7], 1)];
        assert_eq!(run(&mut parts, 2, 10), 2);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].batch().pk, vec![1, 5, 7, 9]);
        assert!(parts[0].batch().is_sorted_by_pk());
    }

    #[test]
    fn merge_reclaims_rows_below_horizon() {
        let a = part(0, &[1, 2, 3], 1);
        a.mark_deleted(1, 5);
        let mut parts = vec![a, part(1, &[4], 1)];
        run(&mut parts, 2, 10);
        assert_eq!(parts[0].batch().pk, vec![1, 3, 4]);
    }

    #[test]
    fn merge_retains_rows_visible_to_horizon() {
        let a = part(0, &[1, 2], 1);
        a.mark_deleted(1, 100);
        let mut parts = vec![a, part(1, &[3], 1)];
        run(&mut parts, 2, 10);
        assert!(parts[0].batch().pk.contains(&2));
    }

    #[test]
    fn merge_collapses_version_stamps() {
        let mut parts = vec![part(0, &[1], 5), part(1, &[2], 7)];
        run(&mut parts, 2, 100);
        assert!(parts[0].created_is_uniform());
    }

    #[test]
    fn merge_keeps_stamps_when_distinguishable() {
        let mut parts = vec![part(0, &[1], 5), part(1, &[2], 500)];
        run(&mut parts, 2, 10);
        assert!(!parts[0].created_is_uniform());
    }

    #[test]
    fn concurrent_delete_during_merge_is_replayed() {
        // The property the two-phase split exists to preserve.
        let a = part(0, &[1, 2, 3], 1);
        let b = part(1, &[10, 11], 1);
        let mut parts = vec![a.clone(), b];

        // Phase 1: plan the merge as of CSN 50.
        let snapshot = parts.clone();
        let plan = plan_merge(&snapshot, 2, 50, 50).unwrap();

        // A writer deletes pk=2 while the merge is "running".
        assert!(a.mark_deleted(1, 60));

        // Phase 2: install. The late delete must survive.
        assert_eq!(apply_plan(&mut parts, plan, &Metrics::new()), 2);
        let merged = &parts[0];
        assert!(
            merged.lookup(2, crate::csn::Snapshot::at(60)).ordinal().is_none(),
            "delete issued during the merge was lost"
        );
        assert!(
            merged.lookup(2, crate::csn::Snapshot::at(55)).ordinal().is_some(),
            "older snapshot must still see it"
        );
    }

    #[test]
    fn stale_plan_is_discarded() {
        let mut parts = vec![part(0, &[1], 1), part(1, &[2], 1)];
        let snapshot = parts.clone();
        let plan = plan_merge(&snapshot, 2, 10, 10).unwrap();
        // Someone else compacted first.
        parts.clear();
        parts.push(part(9, &[1, 2], 1));
        assert_eq!(apply_plan(&mut parts, plan, &Metrics::new()), 0);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].id(), 9);
    }

    #[test]
    fn merge_leaves_unrelated_parts_alone() {
        let older = vec![part(0, &[1], 1), part(1, &[2], 1)];
        let mut parts = vec![part(5, &[100], 1), older[0].clone(), older[1].clone()];
        let plan = plan_merge(&older, 6, 10, 10).unwrap();
        assert_eq!(apply_plan(&mut parts, plan, &Metrics::new()), 2);
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().any(|p| p.id() == 5), "newer part must survive");
    }

    #[test]
    fn merge_updates_metrics() {
        let m = Metrics::new();
        let a = part(0, &[1, 2], 1);
        a.mark_deleted(0, 3);
        let mut parts = vec![a, part(1, &[5], 1)];
        let snapshot = parts.clone();
        let plan = plan_merge(&snapshot, 2, 10, 10).unwrap();
        apply_plan(&mut parts, plan, &m);
        assert_eq!(Metrics::get(&m.compactions), 1);
        assert_eq!(Metrics::get(&m.parts_merged), 2);
        assert_eq!(Metrics::get(&m.rows_reclaimed), 1);
    }

    #[test]
    fn repeated_merge_is_content_stable() {
        let mut parts = vec![part(0, &[1], 1), part(1, &[2], 1)];
        run(&mut parts, 2, 10);
        let before = parts[0].batch().pk.clone();
        run(&mut parts, 3, 10);
        assert_eq!(parts[0].batch().pk, before);
        assert_eq!(parts.len(), 1);
    }
}
