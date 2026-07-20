//! Snapshot-isolation invariants.
//!
//! These are the properties `requirements.md` §5.3 claims. If any of them
//! break, the MVCC design is wrong and M0 has failed regardless of what the
//! benchmark says.

use chakradb::{Database, Row, Snapshot};

fn row(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk * 2, pk as f64, tag)
}

#[test]
fn snapshot_never_sees_later_writes() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "a")).unwrap();

    let snap = db.snapshot();
    for pk in 2..100 {
        t.insert(row(pk, "later")).unwrap();
    }
    assert_eq!(t.row_count(snap), 1);
    assert_eq!(t.row_count(db.snapshot()), 99);
}

#[test]
fn snapshot_is_stable_across_many_mutations() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    for pk in 0..50 {
        t.insert(row(pk, "v0")).unwrap();
    }
    let snap = db.snapshot();
    let expected = t.scan(snap);

    for pk in 0..50 {
        t.update(row(pk, "v1")).unwrap();
    }
    for pk in 0..25 {
        t.delete(pk).unwrap();
    }
    t.seal();
    t.force_compact(snap.csn);

    let after = t.scan(snap);
    assert_eq!(after.len(), expected.len());
    assert!(after.c.iter().all(|c| c == "v0"), "old snapshot changed");
}

#[test]
fn exactly_one_version_visible_at_every_snapshot() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "v0")).unwrap();
    let mut checkpoints = vec![db.snapshot()];
    for i in 1..20 {
        t.update(row(1, &format!("v{i}"))).unwrap();
        checkpoints.push(db.snapshot());
    }
    t.seal();

    for (i, snap) in checkpoints.iter().enumerate() {
        let b = t.scan(*snap);
        let matching: Vec<_> = (0..b.len()).filter(|&j| b.pk[j] == 1).collect();
        assert_eq!(matching.len(), 1, "snapshot {i} saw {} versions", matching.len());
        assert_eq!(b.c[matching[0]], format!("v{i}"));
    }
}

#[test]
fn deleted_row_remains_visible_to_older_snapshots() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "alive")).unwrap();
    let before = db.snapshot();
    t.delete(1).unwrap();
    let after = db.snapshot();

    assert!(t.get(1, before).is_some());
    assert!(t.get(1, after).is_none());
}

#[test]
fn reinsert_after_delete_is_a_new_version() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "first")).unwrap();
    t.delete(1).unwrap();
    let gap = db.snapshot();
    t.insert(row(1, "second")).unwrap();

    assert!(t.get(1, gap).is_none(), "must be absent in the gap");
    assert_eq!(t.get_latest(1).unwrap().c, "second");
    assert_eq!(t.row_count(db.snapshot()), 1);
}

#[test]
fn scan_and_row_count_always_agree() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    let mut snaps = vec![];
    for pk in 0..60 {
        t.insert(row(pk, "x")).unwrap();
        if pk % 7 == 0 {
            snaps.push(db.snapshot());
        }
    }
    for pk in (0..60).step_by(3) {
        t.delete(pk).unwrap();
        snaps.push(db.snapshot());
    }
    t.seal();
    for snap in snaps {
        assert_eq!(t.scan(snap).len(), t.row_count(snap), "at csn={}", snap.csn);
    }
}

#[test]
fn visibility_survives_seal_and_compaction() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    for pk in 0..30 {
        t.insert(row(pk, "orig")).unwrap();
    }
    let early = db.snapshot();
    for pk in 0..30 {
        t.update(row(pk, "updated")).unwrap();
    }
    let late = db.snapshot();

    let early_before = t.scan(early);
    let late_before = t.scan(late);

    t.seal();
    // Compact with a horizon that must not reclaim what `early` can see.
    t.force_compact(early.csn);

    assert_eq!(t.scan(early).len(), early_before.len());
    assert!(t.scan(early).c.iter().all(|c| c == "orig"));
    assert_eq!(t.scan(late).len(), late_before.len());
    assert!(t.scan(late).c.iter().all(|c| c == "updated"));
}

#[test]
fn empty_snapshot_sees_empty_table() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    let empty = db.snapshot();
    t.insert(row(1, "a")).unwrap();
    assert_eq!(t.row_count(empty), 0);
    assert!(t.scan(empty).is_empty());
}

#[test]
fn very_old_snapshot_still_resolves() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "ancient")).unwrap();
    let ancient = db.snapshot();
    for i in 0..500 {
        t.upsert(row(1, &format!("v{i}"))).unwrap();
    }
    t.seal();
    assert_eq!(t.get(1, ancient).unwrap().c, "ancient");
}

#[test]
fn snapshot_at_zero_sees_nothing() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "a")).unwrap();
    assert_eq!(t.row_count(Snapshot::at(0)), 0);
}
