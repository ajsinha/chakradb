//! Multi-table behaviour.
//!
//! ChakraDB holds many primary-keyed tables sharing one snapshot clock.
//! Foreign keys are an explicit non-goal (`requirements.md` §2.1) — referential
//! integrity is the application's business. What the engine *does* guarantee is
//! that a snapshot is consistent across every table, so a multi-table read
//! observes one instant.

use chakradb::{Database, Error, Row, TableConfig};
use std::sync::Arc;
use std::thread;

fn row(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk, pk as f64, tag)
}

#[test]
fn many_tables_coexist() {
    let db = Database::new();
    for i in 0..50 {
        let t = db.create_table(&format!("t{i}")).unwrap();
        t.insert(row(1, "x")).unwrap();
    }
    assert_eq!(db.len(), 50);
    assert!(db.stats().iter().all(|s| s.total_rows() == 1));
}

#[test]
fn key_spaces_are_independent() {
    let db = Database::new();
    let users = db.create_table("users").unwrap();
    let orders = db.create_table("orders").unwrap();

    // The same primary key in two tables is two distinct rows.
    users.insert(row(1, "alice")).unwrap();
    orders.insert(row(1, "order-1")).unwrap();

    assert_eq!(users.get_latest(1).unwrap().c, "alice");
    assert_eq!(orders.get_latest(1).unwrap().c, "order-1");

    // Deleting from one leaves the other untouched.
    users.delete(1).unwrap();
    assert!(users.get_latest(1).is_none());
    assert!(orders.get_latest(1).is_some());
}

#[test]
fn snapshot_is_consistent_across_tables() {
    let db = Database::new();
    let a = db.create_table("a").unwrap();
    let b = db.create_table("b").unwrap();
    for pk in 0..10 {
        a.insert(row(pk, "a")).unwrap();
        b.insert(row(pk, "b")).unwrap();
    }
    let snap = db.snapshot();

    // A "transfer": delete in one, insert in the other.
    a.delete(0).unwrap();
    b.insert(row(100, "moved")).unwrap();

    // The old snapshot must see neither half of it.
    assert_eq!(a.row_count(snap), 10);
    assert_eq!(b.row_count(snap), 10);
    // The new one sees both.
    let now = db.snapshot();
    assert_eq!(a.row_count(now), 9);
    assert_eq!(b.row_count(now), 11);
}

#[test]
fn csns_are_globally_ordered_across_tables() {
    let db = Database::new();
    let a = db.create_table("a").unwrap();
    let b = db.create_table("b").unwrap();
    let mut last = 0;
    for i in 0..100 {
        let t = if i % 2 == 0 { &a } else { &b };
        let csn = t.insert(row(i, "x")).unwrap();
        assert!(csn > last, "CSN went backwards across tables");
        last = csn;
    }
}

#[test]
fn per_table_configuration_is_respected() {
    let db = Database::new();
    let eager = db
        .create_table_with(
            "eager",
            TableConfig {
                seal_threshold: 5,
                ..Default::default()
            },
        )
        .unwrap();
    let lazy = db
        .create_table_with(
            "lazy",
            TableConfig {
                seal_threshold: 100_000,
                ..Default::default()
            },
        )
        .unwrap();

    for pk in 0..50 {
        eager.insert(row(pk, "x")).unwrap();
        lazy.insert(row(pk, "x")).unwrap();
    }
    assert!(eager.stats().num_parts > 0, "eager table should have sealed");
    assert_eq!(lazy.stats().num_parts, 0, "lazy table should not have sealed");
}

#[test]
fn catalog_errors_are_typed() {
    let db = Database::new();
    db.create_table("t").unwrap();
    assert!(matches!(db.create_table("t"), Err(Error::TableExists(_))));
    assert!(matches!(db.table("missing"), Err(Error::TableNotFound(_))));
    assert!(matches!(db.drop_table("missing"), Err(Error::TableNotFound(_))));
}

#[test]
fn dropped_table_is_gone_but_handles_survive() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "x")).unwrap();
    db.drop_table("t").unwrap();

    assert!(db.table("t").is_err());
    // An outstanding Arc keeps working — no dangling reference.
    assert!(t.get_latest(1).is_some());
}

#[test]
fn seal_and_compact_all_touch_every_table() {
    let db = Database::new();
    for i in 0..5 {
        let t = db.create_table(&format!("t{i}")).unwrap();
        for pk in 0..100 {
            t.insert(row(pk, "x")).unwrap();
        }
        for pk in 0..40 {
            t.delete(pk).unwrap();
        }
    }
    db.seal_all();
    assert!(db.stats().iter().all(|s| s.l0_rows == 0));

    db.compact_all(db.snapshot().csn);
    let snap = db.snapshot();
    for name in db.table_names() {
        let t = db.table(&name).unwrap();
        assert_eq!(t.row_count(snap), 60, "table {name} has wrong live count");
    }
    // Compaction must physically reclaim the tombstoned rows, not merely
    // hide them: otherwise a heavily-deleted table degrades indefinitely.
    for s in db.stats() {
        assert_eq!(s.total_rows(), 60, "table {} did not reclaim", s.name);
        assert_eq!(s.tombstones, 0, "table {} kept tombstones", s.name);
    }
}

#[test]
fn concurrent_writers_on_different_tables() {
    let db = Arc::new(Database::new());
    let names: Vec<String> = (0..4).map(|i| format!("t{i}")).collect();
    for n in &names {
        db.create_table(n).unwrap();
    }

    let handles: Vec<_> = names
        .iter()
        .cloned()
        .map(|name| {
            let db = db.clone();
            thread::spawn(move || {
                let t = db.table(&name).unwrap();
                for pk in 0..500 {
                    t.insert(row(pk, "x")).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let snap = db.snapshot();
    for n in &names {
        assert_eq!(db.table(n).unwrap().row_count(snap), 500, "table {n}");
    }
}

#[test]
fn table_or_create_is_safe_to_call_repeatedly() {
    let db = Database::new();
    for _ in 0..10 {
        let t = db.table_or_create("t").unwrap();
        t.upsert(row(1, "x")).unwrap();
    }
    assert_eq!(db.len(), 1);
    assert_eq!(db.table("t").unwrap().row_count(db.snapshot()), 1);
}

#[test]
fn shared_metrics_aggregate_across_tables() {
    let db = Database::new();
    for i in 0..3 {
        let t = db.create_table(&format!("t{i}")).unwrap();
        for pk in 0..10 {
            t.insert(row(pk, "x")).unwrap();
        }
    }
    assert_eq!(chakradb::Metrics::get(&db.metrics().inserts), 30);
}
