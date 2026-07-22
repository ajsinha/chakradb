//! GC watermark: compaction must not reclaim a version a live (pinned) reader can
//! still observe. This is the correctness guarantee behind the concurrency wedge —
//! a long-running analytical reader keeps a stable snapshot while writers (and now
//! compaction) proceed underneath it.

use chakradb::{Database, Row, Value};

#[test]
fn gc_horizon_respects_a_pinned_snapshot() {
    let db = Database::new();
    db.create_table("t").unwrap();
    let t = db.table("t").unwrap();
    for pk in 1..=3 {
        t.insert(Row::new(pk, pk * 10, pk as f64, "x")).unwrap();
    }

    // No pins: the horizon is the current clock.
    assert_eq!(db.gc_horizon(), db.snapshot().csn);

    // Pin an old snapshot, then advance the clock with more writes.
    let pin = db.pin();
    let pinned_csn = pin.snapshot().csn;
    t.insert(Row::new(4, 40, 4.0, "x")).unwrap();
    t.update(Row::new(1, 999, 1.0, "y")).unwrap();
    assert!(db.snapshot().csn > pinned_csn, "clock advanced past the pin");

    // The horizon is held back to the oldest live pin, not the current clock.
    assert_eq!(db.gc_horizon(), pinned_csn);

    // Releasing the pin lets the horizon advance again.
    drop(pin);
    assert_eq!(db.gc_horizon(), db.snapshot().csn);
}

#[test]
fn compaction_retains_rows_a_pinned_reader_can_see() {
    let db = Database::new();
    db.create_table("t").unwrap();
    let t = db.table("t").unwrap();
    for pk in 1..=3 {
        t.insert(Row::new(pk, pk * 10, pk as f64, "x")).unwrap();
    }
    db.seal_all(); // move the rows into a sealed part

    // A long-running reader pins its snapshot — it sees keys {1, 2, 3}.
    let pin = db.pin();
    let s = pin.snapshot();
    assert!(t.get(&Value::Int(2), s).is_some());

    // A writer deletes key 2 (at a CSN after the pin), then compaction runs with
    // the *safe* horizon (which the pin holds back).
    t.delete(&Value::Int(2)).unwrap();
    db.seal_all();
    assert_eq!(db.gc_horizon(), s.csn, "pin holds the horizon at its snapshot");
    t.force_compact(db.gc_horizon());

    // The pinned reader still sees key 2 — its version was NOT reclaimed, because
    // it was deleted after the reader's snapshot.
    assert!(
        t.get(&Value::Int(2), s).is_some(),
        "compaction reclaimed a row the pinned reader can still see"
    );
    // A fresh reader correctly does not see the deleted key.
    assert!(t.get(&Value::Int(2), db.snapshot()).is_none());

    // Once the reader releases its pin, the row becomes reclaimable.
    drop(pin);
    t.force_compact(db.gc_horizon());
    assert!(t.get(&Value::Int(2), db.snapshot()).is_none());
}

#[test]
fn multiple_pins_hold_the_oldest() {
    let db = Database::new();
    db.create_table("t").unwrap();
    let t = db.table("t").unwrap();
    t.insert(Row::new(1, 10, 1.0, "a")).unwrap();

    let old = db.pin();
    let old_csn = old.snapshot().csn;
    t.insert(Row::new(2, 20, 2.0, "b")).unwrap();
    let newer = db.pin();
    assert!(newer.snapshot().csn > old_csn);

    // The horizon tracks the oldest of the two.
    assert_eq!(db.gc_horizon(), old_csn);
    drop(old);
    // With the oldest gone, the horizon advances to the remaining (newer) pin.
    assert_eq!(db.gc_horizon(), newer.snapshot().csn);
    drop(newer);
    assert_eq!(db.gc_horizon(), db.snapshot().csn);
}
