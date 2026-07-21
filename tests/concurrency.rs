//! Concurrent readers and writers.
//!
//! This is the axis the whole project exists for (`requirements.md` §1.2,
//! NFR-03): scans must not block while writes are in flight, and neither side
//! may observe a torn or impossible state.

use chakradb::{Database, Error, Row, Rng, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

fn row(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk, pk as f64, tag)
}

#[test]
fn readers_are_never_blocked_by_writers() {
    let db = Arc::new(Database::new());
    let t = db.create_table("t").unwrap();
    for pk in 0..2_000 {
        t.insert(row(pk, "v0")).unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let scans = Arc::new(AtomicUsize::new(0));

    let writer = {
        let t = t.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut rng = Rng::new(1);
            while !stop.load(Ordering::Relaxed) {
                let pk = rng.range(0, 2_000);
                let _ = t.upsert(row(pk, "vN"));
            }
        })
    };

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let t = t.clone();
            let db = db.clone();
            let scans = scans.clone();
            thread::spawn(move || {
                for _ in 0..50 {
                    let snap = db.snapshot();
                    let b = t.scan(snap);
                    // Row count must be exactly right at all times: upserts
                    // replace rather than add.
                    assert_eq!(b.len(), 2_000, "torn read: {} rows", b.len());
                    scans.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for r in readers {
        r.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    assert_eq!(scans.load(Ordering::Relaxed), 200);
}

#[test]
fn concurrent_writers_never_lose_or_duplicate_keys() {
    let db = Arc::new(Database::new());
    let t = db.create_table("t").unwrap();
    let threads = 8;
    let per_thread = 500;
    let barrier = Arc::new(Barrier::new(threads));

    let handles: Vec<_> = (0..threads)
        .map(|id| {
            let t = t.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for i in 0..per_thread {
                    let pk = (id * per_thread + i) as i64;
                    t.insert(row(pk, "x")).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let snap = db.snapshot();
    assert_eq!(t.row_count(snap), threads * per_thread);
    let sb = t.scan(snap);
    let mut pks: Vec<i64> = (0..sb.len()).map(|i| sb.key(i).as_int().unwrap()).collect();
    pks.sort_unstable();
    let before = pks.len();
    pks.dedup();
    assert_eq!(pks.len(), before, "duplicate keys materialised");
}

#[test]
fn contended_updates_on_one_key_serialise() {
    let db = Arc::new(Database::new());
    let t = db.create_table("t").unwrap();
    t.insert(row(1, "start")).unwrap();

    let handles: Vec<_> = (0..8)
        .map(|id| {
            let t = t.clone();
            thread::spawn(move || {
                let mut applied = 0;
                for i in 0..200 {
                    match t.update(row(1, &format!("t{id}-{i}"))) {
                        Ok(_) => applied += 1,
                        Err(Error::WriteConflict) => {}
                        Err(e) => panic!("unexpected error: {e}"),
                    }
                }
                applied
            })
        })
        .collect();

    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total > 0, "no update succeeded");
    // Regardless of interleaving, exactly one row survives.
    assert_eq!(t.row_count(db.snapshot()), 1);
}

#[test]
fn insert_and_delete_race_leaves_consistent_state() {
    let db = Arc::new(Database::new());
    let t = db.create_table("t").unwrap();
    for pk in 0..1_000 {
        t.insert(row(pk, "x")).unwrap();
    }

    let deleter = {
        let t = t.clone();
        thread::spawn(move || {
            let mut deleted = 0;
            for pk in 0..1_000 {
                if t.delete(&Value::Int(pk)).is_ok() {
                    deleted += 1;
                }
            }
            deleted
        })
    };
    let scanner = {
        let t = t.clone();
        let db = db.clone();
        thread::spawn(move || {
            for _ in 0..100 {
                let snap = db.snapshot();
                // Count must match the materialised scan at every instant.
                assert_eq!(t.scan(snap).len(), t.row_count(snap));
            }
        })
    };

    let deleted = deleter.join().unwrap();
    scanner.join().unwrap();
    assert_eq!(deleted, 1_000);
    assert_eq!(t.row_count(db.snapshot()), 0);
}

#[test]
fn scans_stay_correct_while_sealing_and_compacting() {
    let db = Arc::new(Database::new());
    let t = db
        .create_table_with(
            "t",
            chakradb::TableConfig {
                seal_threshold: 200,
                ..Default::default()
            },
        )
        .unwrap();
    for pk in 0..1_000 {
        t.insert(row(pk, "x")).unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let maintainer = {
        let t = t.clone();
        let db = db.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                t.seal();
                t.maybe_compact(db.snapshot().csn);
            }
        })
    };

    for _ in 0..200 {
        let snap = db.snapshot();
        assert_eq!(t.row_count(snap), 1_000, "rows lost during maintenance");
    }
    stop.store(true, Ordering::Relaxed);
    maintainer.join().unwrap();
    assert_eq!(t.row_count(db.snapshot()), 1_000);
}

#[test]
fn mixed_workload_preserves_invariants() {
    let db = Arc::new(Database::new());
    let t = db.create_table("t").unwrap();
    let live = Arc::new(AtomicUsize::new(0));

    let writers: Vec<_> = (0..4)
        .map(|id| {
            let t = t.clone();
            let live = live.clone();
            thread::spawn(move || {
                let mut rng = Rng::new(id as u64 + 1);
                for i in 0..500 {
                    let pk = (id * 10_000 + i) as i64;
                    t.insert(row(pk, "x")).unwrap();
                    live.fetch_add(1, Ordering::Relaxed);
                    if rng.chance(0.3)
                        && t.delete(&Value::Int(pk)).is_ok() {
                            live.fetch_sub(1, Ordering::Relaxed);
                        }
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }

    let snap = db.snapshot();
    assert_eq!(t.row_count(snap), live.load(Ordering::Relaxed));
    assert_eq!(t.scan(snap).len(), t.row_count(snap));
}

#[test]
fn many_readers_one_writer_scales() {
    let db = Arc::new(Database::new());
    let t = db.create_table("t").unwrap();
    for pk in 0..500 {
        t.insert(row(pk, "v")).unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let t = t.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                let _ = t.upsert(row(i % 500, "w"));
                i += 1;
            }
        })
    };

    let readers: Vec<_> = (0..16)
        .map(|_| {
            let t = t.clone();
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..25 {
                    assert_eq!(t.row_count(db.snapshot()), 500);
                }
            })
        })
        .collect();
    for r in readers {
        r.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}
