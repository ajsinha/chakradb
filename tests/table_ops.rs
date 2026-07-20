//! Table-level operations: insert/update/delete/upsert, sealing, and the
//! index-memory regimes that M0-2 reports.

use chakradb::{Database, Error, Row, TableConfig};

fn row(pk: i64) -> Row {
    Row::new(pk, pk * 2, pk as f64, format!("v{pk}"))
}

fn table() -> (Database, std::sync::Arc<chakradb::Table>) {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    (db, t)
}

#[test]
fn insert_then_read() {
    let (_db, t) = table();
    t.insert(row(1)).unwrap();
    assert_eq!(t.get_latest(1), Some(row(1)));
}

#[test]
fn duplicate_insert_is_rejected() {
    let (_db, t) = table();
    t.insert(row(1)).unwrap();
    assert!(matches!(t.insert(row(1)), Err(Error::DuplicateKey(1))));
}

#[test]
fn update_replaces_value() {
    let (_db, t) = table();
    t.insert(row(1)).unwrap();
    t.update(Row::new(1, 99, 9.0, "updated")).unwrap();
    assert_eq!(t.get_latest(1).unwrap().c, "updated");
}

#[test]
fn update_missing_key_fails() {
    let (_db, t) = table();
    assert!(matches!(t.update(row(7)), Err(Error::KeyNotFound(7))));
}

#[test]
fn delete_hides_row_and_is_not_repeatable() {
    let (_db, t) = table();
    t.insert(row(1)).unwrap();
    t.delete(1).unwrap();
    assert_eq!(t.get_latest(1), None);
    assert!(matches!(t.delete(1), Err(Error::KeyNotFound(1))));
}

#[test]
fn upsert_inserts_then_updates() {
    let (db, t) = table();
    t.upsert(row(1)).unwrap();
    assert_eq!(t.get_latest(1).unwrap().a, 2);
    t.upsert(Row::new(1, 42, 0.0, "x")).unwrap();
    assert_eq!(t.get_latest(1).unwrap().a, 42);
    assert_eq!(t.row_count(db.snapshot()), 1);
}

#[test]
fn scan_returns_all_live_rows() {
    let (db, t) = table();
    for pk in 0..10 {
        t.insert(row(pk)).unwrap();
    }
    t.delete(3).unwrap();
    let got = t.scan(db.snapshot());
    assert_eq!(got.len(), 9);
    assert!(!got.pk.contains(&3));
    assert!(got.is_well_formed());
}

#[test]
fn seal_moves_rows_into_a_part() {
    let (_db, t) = table();
    for pk in 0..5 {
        t.insert(row(pk)).unwrap();
    }
    assert_eq!(t.stats().l0_rows, 5);
    t.seal();
    let s = t.stats();
    assert_eq!(s.l0_rows, 0);
    assert_eq!(s.num_parts, 1);
    assert_eq!(s.part_rows, 5);
}

#[test]
fn seal_of_empty_l0_is_noop() {
    let (_db, t) = table();
    t.seal();
    assert_eq!(t.stats().num_parts, 0);
}

#[test]
fn reads_span_l0_and_parts() {
    let (db, t) = table();
    t.insert(row(1)).unwrap();
    t.seal();
    t.insert(row(2)).unwrap();
    assert_eq!(t.row_count(db.snapshot()), 2);
    assert!(t.get_latest(1).is_some());
    assert!(t.get_latest(2).is_some());
}

#[test]
fn update_of_sealed_row_works() {
    let (db, t) = table();
    t.insert(row(1)).unwrap();
    t.seal();
    t.update(Row::new(1, 77, 0.0, "new")).unwrap();
    assert_eq!(t.get_latest(1).unwrap().a, 77);
    assert_eq!(t.row_count(db.snapshot()), 1);
}

#[test]
fn auto_seal_fires_at_threshold() {
    let db = Database::new();
    let t = db
        .create_table_with(
            "t",
            TableConfig {
                seal_threshold: 10,
                ..Default::default()
            },
        )
        .unwrap();
    for pk in 0..25 {
        t.insert(row(pk)).unwrap();
    }
    assert!(t.stats().num_parts >= 2, "expected automatic sealing");
}

#[test]
fn snapshot_isolation_across_update_and_delete() {
    let (db, t) = table();
    t.insert(Row::new(1, 1, 1.0, "before")).unwrap();
    let before = db.snapshot();
    t.update(Row::new(1, 2, 2.0, "after")).unwrap();
    assert_eq!(t.get(1, before).unwrap().c, "before");
    assert_eq!(t.get_latest(1).unwrap().c, "after");

    let mid = db.snapshot();
    t.delete(1).unwrap();
    assert!(t.get(1, mid).is_some());
    assert!(t.get_latest(1).is_none());
}

#[test]
fn index_overhead_before_and_after_compaction() {
    // The M0-2 measurement in miniature, and it has two regimes.
    //
    // A freshly sealed part carries a per-row creation stamp (8 B/row) because
    // its rows genuinely were created at different CSNs. Once compaction
    // establishes that no live snapshot can distinguish them, the stamps
    // collapse and only the Bloom filter remains (~1.25 B/row). Neither regime
    // contains a key→location map — that is the §5.2 result.
    let (db, t) = table();
    for pk in 0..2000 {
        t.insert(row(pk)).unwrap();
    }
    t.seal();

    let fresh = t.stats().index_bytes_per_row();
    assert!(
        (8.0..12.0).contains(&fresh),
        "fresh part should carry per-row stamps, got {fresh}"
    );

    t.force_compact(db.snapshot().csn);
    let compacted = t.stats().index_bytes_per_row();
    assert!(
        compacted < 2.0,
        "after collapse only the Bloom filter should remain, got {compacted}"
    );
    assert!(compacted < fresh / 4.0, "collapse should be a large win");
    assert_eq!(t.row_count(db.snapshot()), 2000, "no rows lost");
}

#[test]
fn stats_track_tombstones_and_reclamation() {
    let (db, t) = table();
    for pk in 0..100 {
        t.insert(row(pk)).unwrap();
    }
    t.seal();
    for pk in 0..50 {
        t.delete(pk).unwrap();
    }
    assert_eq!(t.stats().tombstones, 50);

    t.force_compact(db.snapshot().csn);
    let s = t.stats();
    assert_eq!(s.tombstones, 0, "compaction should clear tombstones");
    assert_eq!(s.total_rows(), 50, "and physically reclaim the rows");
}
