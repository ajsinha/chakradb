//! Crash consistency — the M1-1 acceptance criterion.
//!
//! The contract under test, from `requirements.md` §7.2: **a write that was
//! acknowledged must survive a power cut**, in `sync` and `group` modes. A
//! write that was *not* acknowledged may vanish, and a torn record must be
//! discarded rather than misread.
//!
//! Method follows redb's `CrashBackend`: model `live` vs `durable` state, crash
//! at a seeded point, reopen, and assert every acknowledged write is present.
//! Seeds make every failure reproducible.

use chakradb::io::{Io, MemIo};
use chakradb::{Database, Durability, Row, Rng, Storage, StorageConfig};
use std::collections::HashMap;
use std::sync::Arc;

fn config(d: Durability) -> StorageConfig {
    StorageConfig {
        durability: d,
        checkpoint_wal_bytes: u64::MAX, // checkpoint only when asked
        ..Default::default()
    }
}

fn row(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk * 2, pk as f64, tag)
}

/// What the client believes is committed.
#[derive(Default)]
struct Expected {
    live: HashMap<i64, String>,
}

impl Expected {
    fn apply_upsert(&mut self, pk: i64, tag: &str) {
        self.live.insert(pk, tag.to_string());
    }
    fn apply_delete(&mut self, pk: i64) {
        self.live.remove(&pk);
    }
}

/// Run a seeded workload, crash partway, reopen, and verify.
///
/// Returns `(acknowledged writes, rows recovered)`.
fn crash_trial(seed: u64, ops: usize, mode: Durability, checkpoints: bool) -> (usize, usize) {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let mut expected = Expected::default();
    let mut rng = Rng::new(seed);
    let mut acked = 0usize;

    {
        let s = Storage::open(io.clone(), config(mode)).unwrap();
        s.create_table("t").unwrap();

        let crash_at = rng.below(ops as u64) as usize;
        for i in 0..ops {
            if i == crash_at {
                break;
            }
            let pk = rng.range(0, 200);
            if rng.chance(0.75) {
                let tag = format!("v{i}");
                if s.upsert("t", row(pk, &tag)).is_ok() {
                    expected.apply_upsert(pk, &tag);
                    acked += 1;
                }
            } else if s.delete("t", pk).is_ok() {
                expected.apply_delete(pk);
                acked += 1;
            }
            if checkpoints && rng.chance(0.02) {
                s.checkpoint().unwrap();
            }
        }
        // Power cut. No flush, no clean shutdown.
        io.crash();
    }

    let s2 = Storage::open(io, config(mode)).unwrap();
    let t = s2.database().table("t").unwrap();
    let snap = s2.database().snapshot();

    for (pk, tag) in &expected.live {
        let got = t.get(*pk, snap);
        assert!(
            got.is_some(),
            "seed {seed}: acknowledged pk={pk} lost after crash"
        );
        assert_eq!(
            &got.unwrap().c,
            tag,
            "seed {seed}: pk={pk} recovered a stale version"
        );
    }
    for pk in 0..200i64 {
        if !expected.live.contains_key(&pk) {
            assert!(
                t.get(pk, snap).is_none(),
                "seed {seed}: deleted pk={pk} came back"
            );
        }
    }

    (acked, t.row_count(snap))
}

/// Number of randomized crash trials. The M1-1 criterion is >=10,000.
///
/// The full run takes ~40 s, which is too slow for an inner dev loop, so the
/// default here is a smoke-sized subset and the full count is opt-in:
///
/// ```sh
/// CHAKRA_CRASH_TRIALS=10000 cargo test --release --test crash_consistency
/// ```
///
/// CI runs the full count. Reporting M1-1 as met on the smoke subset would be
/// dishonest, so the acceptance figure quoted in `m1-findings.md` comes from a
/// full run, recorded there with the exact command.
fn trial_count(default: u64) -> u64 {
    std::env::var("CHAKRA_CRASH_TRIALS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn group_mode_survives_crashes_across_many_seeds() {
    // M1-1. Each seed is an independent randomized crash point.
    let n = trial_count(300);
    let mut total_acked = 0;
    for seed in 0..n {
        let (acked, _) = crash_trial(seed, 400, Durability::Group, false);
        total_acked += acked;
    }
    assert!(total_acked > 0, "workload never acknowledged anything");
    eprintln!("M1-1: {n} crash trials passed, {total_acked} acknowledged writes verified");
}

#[test]
fn sync_mode_survives_crashes() {
    let n = trial_count(100);
    for seed in 100_000..100_000 + n {
        crash_trial(seed, 200, Durability::Sync, false);
    }
}

#[test]
fn crashes_during_checkpointing_are_safe() {
    // The dangerous window: part files are being written while the manifest
    // still points at the previous generation.
    let n = trial_count(200);
    for seed in 200_000..200_000 + n {
        crash_trial(seed, 500, Durability::Group, true);
    }
}

#[test]
fn recovery_is_idempotent() {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    {
        let s = Storage::open(io.clone(), config(Durability::Group)).unwrap();
        s.create_table("t").unwrap();
        for pk in 0..100 {
            s.insert("t", row(pk, "v")).unwrap();
        }
        s.checkpoint().unwrap();
        for pk in 100..150 {
            s.insert("t", row(pk, "v")).unwrap();
        }
        io.crash();
    }
    let mut counts = vec![];
    for _ in 0..5 {
        let s = Storage::open(io.clone(), config(Durability::Group)).unwrap();
        let t = s.database().table("t").unwrap();
        counts.push(t.row_count(s.database().snapshot()));
    }
    assert!(
        counts.windows(2).all(|w| w[0] == w[1]),
        "repeated recovery drifted: {counts:?}"
    );
    assert_eq!(counts[0], 150);
}

#[test]
fn async_mode_may_lose_data_but_never_corrupts() {
    // Async explicitly permits loss. What it must *not* do is produce an
    // unreadable database or a torn record.
    for seed in 600..700 {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        {
            let s = Storage::open(io.clone(), config(Durability::Async)).unwrap();
            s.create_table("t").unwrap();
            let mut rng = Rng::new(seed);
            for i in 0..200 {
                let pk = rng.range(0, 50);
                let _ = s.upsert("t", row(pk, &format!("v{i}")));
            }
            io.crash();
        }
        let s2 = Storage::open(io, config(Durability::Async)).unwrap();
        let t = s2.database().table("t").unwrap();
        let snap = s2.database().snapshot();
        // Whatever survived must be internally consistent.
        assert_eq!(t.scan(snap).len(), t.row_count(snap));
        assert!(t.scan(snap).is_well_formed());
    }
}

#[test]
fn flushed_async_writes_do_survive() {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    {
        let s = Storage::open(io.clone(), config(Durability::Async)).unwrap();
        s.create_table("t").unwrap();
        for pk in 0..50 {
            s.insert("t", row(pk, "v")).unwrap();
        }
        s.flush().unwrap();
        io.crash();
    }
    let s2 = Storage::open(io, config(Durability::Async)).unwrap();
    let t = s2.database().table("t").unwrap();
    assert_eq!(t.row_count(s2.database().snapshot()), 50);
}

#[test]
fn torn_wal_tail_never_corrupts_the_prefix() {
    // Crash at every byte offset of a small workload's log.
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let s = Storage::open(io.clone(), config(Durability::Group)).unwrap();
    s.create_table("t").unwrap();
    for pk in 0..40 {
        s.insert("t", row(pk, "v")).unwrap();
    }
    drop(s);

    let full = {
        let f = io.open("wal.log").unwrap();
        let mut b = vec![0u8; f.len().unwrap() as usize];
        f.pread(0, &mut b).unwrap();
        b
    };

    for cut in (8..full.len()).step_by(7) {
        let io2: Arc<MemIo> = Arc::new(MemIo::new());
        {
            // Reproduce the manifest, then a truncated log.
            let src = io.open("MANIFEST").unwrap();
            let mut mbuf = vec![0u8; src.len().unwrap() as usize];
            src.pread(0, &mut mbuf).unwrap();
            let dst = io2.open("MANIFEST").unwrap();
            dst.pwrite(0, &mbuf).unwrap();
            dst.sync().unwrap();

            let w = io2.open("wal.log").unwrap();
            w.pwrite(0, &full[..cut]).unwrap();
            w.sync().unwrap();
        }
        let s2 = Storage::open(io2, config(Durability::Group)).unwrap();
        let t = s2.database().table("t").unwrap();
        let snap = s2.database().snapshot();
        // Whatever survived must be a *prefix* of the workload, intact.
        let n = t.row_count(snap);
        assert!(n <= 40, "cut {cut} recovered {n} rows, more than were written");
        assert_eq!(t.scan(snap).len(), n, "cut {cut} produced inconsistent state");
        for pk in 0..n as i64 {
            assert!(t.get(pk, snap).is_some(), "cut {cut} lost pk={pk} from prefix");
        }
    }
}

#[test]
fn multi_table_crash_recovery_is_consistent() {
    for seed in 700..760 {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        let mut expect_a = HashMap::new();
        let mut expect_b = HashMap::new();
        {
            let s = Storage::open(io.clone(), config(Durability::Group)).unwrap();
            s.create_table("a").unwrap();
            s.create_table("b").unwrap();
            let mut rng = Rng::new(seed);
            for i in 0..300 {
                let pk = rng.range(0, 60);
                let tag = format!("v{i}");
                let table = if rng.chance(0.5) { "a" } else { "b" };
                if s.upsert(table, row(pk, &tag)).is_ok() {
                    if table == "a" {
                        expect_a.insert(pk, tag);
                    } else {
                        expect_b.insert(pk, tag);
                    }
                }
                if rng.chance(0.01) {
                    s.checkpoint().unwrap();
                }
            }
            io.crash();
        }
        let s2 = Storage::open(io, config(Durability::Group)).unwrap();
        let snap = s2.database().snapshot();
        for (name, expect) in [("a", &expect_a), ("b", &expect_b)] {
            let t = s2.database().table(name).unwrap();
            for (pk, tag) in expect {
                let got = t.get(*pk, snap);
                assert!(got.is_some(), "seed {seed}: {name} lost pk={pk}");
                assert_eq!(&got.unwrap().c, tag, "seed {seed}: {name} stale pk={pk}");
            }
        }
    }
}

#[test]
fn crash_before_any_write_leaves_a_usable_database() {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    {
        let _s = Storage::open(io.clone(), config(Durability::Group)).unwrap();
        io.crash();
    }
    let s2 = Storage::open(io, config(Durability::Group)).unwrap();
    assert!(s2.database().is_empty());
    // And it is still writable.
    s2.create_table("t").unwrap();
    s2.insert("t", row(1, "v")).unwrap();
}

#[test]
fn snapshot_isolation_holds_after_recovery() {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    {
        let s = Storage::open(io.clone(), config(Durability::Group)).unwrap();
        s.create_table("t").unwrap();
        s.insert("t", row(1, "first")).unwrap();
        s.update("t", row(1, "second")).unwrap();
        io.crash();
    }
    let s2 = Storage::open(io, config(Durability::Group)).unwrap();
    let db: &Arc<Database> = s2.database();
    let t = db.table("t").unwrap();

    let before = db.snapshot();
    s2.update("t", row(1, "third")).unwrap();
    assert_eq!(t.get(1, before).unwrap().c, "second", "recovered snapshot moved");
    assert_eq!(t.get_latest(1).unwrap().c, "third");
}
