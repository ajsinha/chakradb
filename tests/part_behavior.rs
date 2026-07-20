//! Behavioural coverage of sealed parts, via the public surface.
//!
//! Internals-dependent tests stay in `src/part.rs`. Everything here exercises
//! the lookup funnel (§5.2) and version visibility (§5.3) as a caller sees them.

use chakradb::csn::Snapshot;
use chakradb::part::{CreatedCsns, LookupResult, Part};
use chakradb::{Batch, Row};

fn sorted_batch(pks: &[i64]) -> Batch {
    pks.iter()
        .map(|&pk| Row::new(pk, pk * 10, pk as f64, format!("r{pk}")))
        .collect()
}

fn part_of(pks: &[i64], csn: u64) -> Part {
    Part::new(1, sorted_batch(pks), CreatedCsns::Uniform(csn))
}

#[test]
fn ordinal_equals_row_offset() {
    // The core Doris property: index position == data position.
    let pks = [10, 20, 30, 40, 50];
    let p = part_of(&pks, 1);
    let snap = Snapshot::at(100);
    for (expected, &pk) in pks.iter().enumerate() {
        let ord = p.lookup(pk, snap).ordinal().expect("must be found");
        assert_eq!(ord as usize, expected);
        assert_eq!(p.batch().pk[ord as usize], pk);
    }
}

#[test]
fn empty_part_rejects_everything() {
    let p = part_of(&[], 1);
    assert_eq!(p.lookup(5, Snapshot::at(10)), LookupResult::OutOfBounds);
    assert_eq!(p.num_rows(), 0);
}

#[test]
fn funnel_rejects_out_of_bounds_keys() {
    let p = part_of(&[10, 20, 30], 1);
    let s = Snapshot::at(100);
    assert_eq!(p.lookup(5, s), LookupResult::OutOfBounds);
    assert_eq!(p.lookup(35, s), LookupResult::OutOfBounds);
}

#[test]
fn funnel_rejects_absent_keys_within_bounds() {
    let p = part_of(&[10, 20, 30], 1);
    let r = p.lookup(15, Snapshot::at(100));
    // Either the Bloom filter or the seek may reject; both are correct.
    assert!(matches!(
        r,
        LookupResult::BloomMiss | LookupResult::NotPresent
    ));
}

#[test]
fn lookup_respects_creation_csn() {
    let p = part_of(&[1, 2, 3], 50);
    assert_eq!(p.lookup(2, Snapshot::at(49)), LookupResult::NotVisible);
    assert_eq!(p.lookup(2, Snapshot::at(50)), LookupResult::Found(1));
}

#[test]
fn lookup_respects_deletion() {
    let p = part_of(&[1, 2, 3], 10);
    assert!(p.mark_deleted(1, 20));
    assert_eq!(p.lookup(2, Snapshot::at(19)), LookupResult::Found(1));
    assert_eq!(p.lookup(2, Snapshot::at(20)), LookupResult::NotVisible);
    assert!(!p.mark_deleted(1, 21), "double delete must be rejected");
}

#[test]
fn scan_filters_deleted_rows() {
    let p = part_of(&[1, 2, 3, 4], 5);
    p.mark_deleted(1, 10);
    p.mark_deleted(3, 10);
    assert_eq!(p.scan(Snapshot::at(10)).pk, vec![1, 3]);
    // An older snapshot still sees them all.
    assert_eq!(p.scan(Snapshot::at(9)).pk, vec![1, 2, 3, 4]);
}

#[test]
fn fast_path_engages_and_disengages() {
    let p = part_of(&[1, 2, 3], 5);
    assert!(p.is_fully_visible_to(Snapshot::at(10)));
    p.mark_deleted(0, 8);
    assert!(!p.is_fully_visible_to(Snapshot::at(10)));
    // A snapshot older than the delete is still unaffected.
    assert!(p.is_fully_visible_to(Snapshot::at(7)));
}

#[test]
fn fast_and_slow_paths_agree_exhaustively() {
    let pks: Vec<i64> = (0..200).collect();
    let p = Part::new(
        1,
        sorted_batch(&pks),
        CreatedCsns::PerRow((0..200).map(|i| (i / 10 + 1) as u64).collect()),
    );
    for o in [3u32, 50, 77, 199] {
        p.mark_deleted(o, 500);
    }
    for csn in [0u64, 1, 5, 15, 21, 100, 499, 500, 1000] {
        let snap = Snapshot::at(csn);
        let scanned = p.scan(snap);
        assert_eq!(scanned.len(), p.visible_count(snap), "at csn={csn}");

        let expected = (0..p.num_rows())
            .filter(|&i| {
                let created = p.created_at(i);
                let deleted = p.dv_snapshot().deleted_at(i as u32);
                snap.sees(created, deleted)
            })
            .count();
        assert_eq!(scanned.len(), expected, "wrong visible set at csn={csn}");
    }
}

#[test]
fn duplicate_keys_resolve_to_the_visible_version() {
    let batch: Batch = vec![
        Row::new(5, 1, 1.0, "old"),
        Row::new(5, 2, 2.0, "new"),
        Row::new(9, 0, 0.0, "other"),
    ]
    .into_iter()
    .collect();
    let p = Part::with_deletions(1, batch, CreatedCsns::PerRow(vec![10, 20, 10]), &[(0, 20)]);

    let old = p.lookup(5, Snapshot::at(15)).ordinal().unwrap();
    assert_eq!(p.batch().c[old as usize], "old");
    let new = p.lookup(5, Snapshot::at(25)).ordinal().unwrap();
    assert_eq!(p.batch().c[new as usize], "new");
}

#[test]
fn duplicate_keys_never_show_two_versions() {
    let batch: Batch = vec![
        Row::new(1, 0, 0.0, "a"),
        Row::new(1, 0, 0.0, "b"),
        Row::new(1, 0, 0.0, "c"),
    ]
    .into_iter()
    .collect();
    let p = Part::with_deletions(
        1,
        batch,
        CreatedCsns::PerRow(vec![5, 10, 15]),
        &[(0, 10), (1, 15)],
    );
    for csn in 5..20u64 {
        let snap = Snapshot::at(csn);
        assert_eq!(p.scan(snap).len(), 1, "at csn={csn}");
        assert!(p.lookup(1, snap).ordinal().is_some());
    }
}

#[test]
fn index_cost_does_not_scale_with_rows() {
    // §5.2's result: no per-row key→location entry exists.
    let big = part_of(&(0..100_000).collect::<Vec<_>>(), 1);
    let per_row = big.index_memory_bytes() as f64 / 100_000.0;
    assert!(per_row < 2.5, "index cost {per_row} B/row is too high");
}

#[test]
fn dv_density_drives_compaction_trigger() {
    let p = part_of(&[1, 2, 3, 4], 1);
    assert_eq!(p.dv_density(), 0.0);
    p.mark_deleted(0, 2);
    p.mark_deleted(1, 2);
    assert!((p.dv_density() - 0.5).abs() < 1e-9);
    assert_eq!(p.dv_len(), 2);
}

#[test]
fn large_part_lookups_are_all_correct() {
    let pks: Vec<i64> = (0..50_000).map(|i| i * 3).collect();
    let p = part_of(&pks, 1);
    let snap = Snapshot::at(10);
    for &pk in pks.iter().step_by(97) {
        assert!(p.lookup(pk, snap).ordinal().is_some(), "missing {pk}");
    }
    for pk in (1..1000).step_by(3) {
        assert!(p.lookup(pk, snap).ordinal().is_none(), "phantom {pk}");
    }
}
