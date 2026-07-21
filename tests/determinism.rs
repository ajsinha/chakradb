//! Determinism and workload replay.
//!
//! `requirements.md` §11.1 makes the case that the `Io`/`Clock`/`Rng` seams
//! must exist from M0 because they cannot be retrofitted. These tests prove the
//! seams actually deliver what they promise: a seeded workload replays exactly,
//! and virtual time never leaks into real time.
//!
//! Note the scope. Single-threaded replay is deterministic here. Full
//! deterministic simulation of *concurrent* execution needs a scheduler seam
//! too, which is M1 work — see §11.1's discussion of the Turso pattern.

use chakradb::{Clock, Database, RealClock, Rng, Row, SimClock, Value};
use std::time::Duration;

/// A seeded, single-threaded workload. Returns a fingerprint of final state.
fn run_workload(seed: u64, ops: usize) -> Vec<(i64, String)> {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    let mut rng = Rng::new(seed);

    for i in 0..ops {
        let pk = rng.range(0, 200);
        match rng.below(10) {
            0..=5 => {
                let _ = t.upsert(Row::new(pk, i as i64, pk as f64, format!("v{i}")));
            }
            6..=8 => {
                let _ = t.delete(&Value::Int(pk));
            }
            _ => {
                t.seal();
            }
        }
        if rng.chance(0.05) {
            t.maybe_compact(db.snapshot().csn);
        }
    }

    let b = t.scan(db.snapshot());
    let mut out: Vec<(i64, String)> = (0..b.len())
        .map(|i| (b.key(i).as_int().unwrap(), b.value(3, i).render()))
        .collect();
    out.sort();
    out
}

#[test]
fn identical_seeds_produce_identical_state() {
    for seed in [1u64, 42, 9999] {
        let a = run_workload(seed, 2_000);
        let b = run_workload(seed, 2_000);
        assert_eq!(a, b, "workload diverged for seed {seed}");
    }
}

#[test]
fn different_seeds_produce_different_state() {
    let a = run_workload(1, 2_000);
    let b = run_workload(2, 2_000);
    assert_ne!(a, b, "different seeds should explore different states");
}

#[test]
fn replay_is_stable_across_many_runs() {
    let reference = run_workload(7, 1_000);
    for _ in 0..5 {
        assert_eq!(run_workload(7, 1_000), reference);
    }
}

#[test]
fn workload_leaves_a_consistent_table() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    let mut rng = Rng::new(123);

    for i in 0..5_000 {
        let pk = rng.range(0, 300);
        if rng.chance(0.7) {
            let _ = t.upsert(Row::new(pk, i, 0.0, format!("v{i}")));
        } else {
            let _ = t.delete(&Value::Int(pk));
        }
    }
    t.seal();
    t.force_compact(db.snapshot().csn);

    let snap = db.snapshot();
    let b = t.scan(snap);
    assert_eq!(b.len(), t.row_count(snap));

    // No key may appear twice in a live scan.
    let mut pks: Vec<i64> = (0..b.len()).map(|i| b.key(i).as_int().unwrap()).collect();
    pks.sort_unstable();
    let before = pks.len();
    pks.dedup();
    assert_eq!(pks.len(), before, "duplicate live keys after compaction");

    // Every visible key must be individually retrievable.
    for pk in (0..b.len()).map(|i| b.key(i).as_int().unwrap()) {
        assert!(
            t.get(&Value::Int(pk), snap).is_some(),
            "scan/get disagree on {pk}"
        );
    }
}

#[test]
fn get_and_scan_agree_under_random_workload() {
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    let mut rng = Rng::new(555);
    for i in 0..3_000 {
        let pk = rng.range(0, 150);
        if rng.chance(0.6) {
            let _ = t.upsert(Row::new(pk, i, 0.0, "x"));
        } else {
            let _ = t.delete(&Value::Int(pk));
        }
    }
    t.seal();

    let snap = db.snapshot();
    let vb = t.scan(snap);
    let visible: std::collections::HashSet<i64> =
        (0..vb.len()).map(|i| vb.key(i).as_int().unwrap()).collect();
    for pk in 0..150 {
        assert_eq!(
            t.get(&Value::Int(pk), snap).is_some(),
            visible.contains(&pk),
            "disagreement on pk={pk}"
        );
    }
}

#[test]
fn sim_clock_does_not_track_real_time() {
    let sim = SimClock::new();
    let real = RealClock::new();
    let r0 = real.now_nanos();
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(sim.now_nanos(), 0, "virtual time advanced on its own");
    assert!(real.now_nanos() > r0);

    sim.advance(Duration::from_secs(3600));
    assert_eq!(sim.now_millis(), 3_600_000);
}

#[test]
fn rng_streams_are_reproducible_after_fork() {
    let mut parent = Rng::new(99);
    let mut a = parent.fork();
    let mut b = parent.fork();
    let seq_a: Vec<u64> = (0..100).map(|_| a.next_u64()).collect();
    let seq_b: Vec<u64> = (0..100).map(|_| b.next_u64()).collect();
    assert_ne!(seq_a, seq_b, "forks must be independent streams");

    let mut parent2 = Rng::new(99);
    let mut a2 = parent2.fork();
    let replay: Vec<u64> = (0..100).map(|_| a2.next_u64()).collect();
    assert_eq!(seq_a, replay, "fork sequence must replay");
}
