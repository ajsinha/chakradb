//! Sustained-ingest soak — the M1-3 acceptance criterion.
//!
//! The criterion is *"sustained ingest for ≥6 hours with compaction at
//! equilibrium and no unbounded part growth"*. That cannot run in a normal test
//! pass, so duration is configurable and defaults to a few seconds:
//!
//! ```sh
//! # default: ~5s smoke
//! cargo test --release --test soak
//!
//! # the actual M1-3 criterion
//! CHAKRA_SOAK_SECS=21600 cargo test --release --test soak -- --nocapture
//! ```
//!
//! **The full 6-hour run has not been performed.** Reporting M1-3 as met on the
//! strength of a 5-second proxy would be false; see `m1-findings.md` §7. What
//! the short run does establish is that the *equilibrium* invariant holds — part
//! count peaks and plateaus rather than climbing — which is the property a long
//! run would be checking for over a longer window.

use chakradb::io::MemIo;
use chakradb::{Clock, Durability, Metrics, RealClock, Rng, Row, Storage, StorageConfig, TableConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn soak_secs() -> u64 {
    std::env::var("CHAKRA_SOAK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

fn row(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk * 3, pk as f64, tag)
}

#[test]
fn sustained_ingest_reaches_equilibrium() {
    let secs = soak_secs();
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let s = Arc::new(
        Storage::open(
            io,
            StorageConfig {
                durability: Durability::Group,
                table: TableConfig {
                    seal_threshold: 2_000,
                    ..Default::default()
                },
                checkpoint_wal_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    s.create_table("t").unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let maint = {
        let s = s.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                s.database().seal_all();
                s.compact_all();
                if s.checkpoint_due() {
                    s.checkpoint().unwrap();
                }
                thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let clock = RealClock::new();
    let start = clock.now_nanos();
    let budget = secs * 1_000_000_000;
    let mut rng = Rng::new(42);
    let mut parts_series = Vec::new();
    let mut ops = 0u64;

    while clock.now_nanos() - start < budget {
        for _ in 0..500 {
            let pk = rng.range(0, 200_000);
            if rng.chance(0.85) {
                let _ = s.upsert("t", row(pk, "v"));
            } else {
                let _ = s.delete("t", pk);
            }
            ops += 1;
        }
        parts_series.push(s.database().table("t").unwrap().stats().num_parts);
    }
    stop.store(true, Ordering::Relaxed);
    maint.join().unwrap();

    let peak = parts_series.iter().copied().max().unwrap_or(0);
    let final_parts = parts_series.last().copied().unwrap_or(0);
    // Compare the last tenth of the run against the first tenth: if compaction
    // keeps up, these are comparable. If it does not, the tail is far larger.
    let chunk = (parts_series.len() / 10).max(1);
    let head_avg: f64 =
        parts_series[..chunk].iter().sum::<usize>() as f64 / chunk as f64;
    let tail_avg: f64 = parts_series[parts_series.len() - chunk..].iter().sum::<usize>() as f64
        / chunk as f64;

    let snap = s.database().snapshot();
    let live = s.database().table("t").unwrap().row_count(snap);
    let m = s.metrics().snapshot();

    eprintln!(
        "M1-3 soak: {secs}s · {ops} ops · parts peak {peak} final {final_parts} \
         (head avg {head_avg:.1}, tail avg {tail_avg:.1}) · {live} live rows · \
         {} compactions · {} checkpoints-worth of WAL",
        m.compactions,
        m.seals,
    );

    // The invariant: part count plateaus rather than growing without bound.
    assert!(
        tail_avg < head_avg.max(4.0) * 4.0,
        "part count grew without bound: head avg {head_avg:.1} -> tail avg {tail_avg:.1}"
    );
    assert!(peak < 500, "part count exploded to {peak}");
    assert!(ops > 0);
    assert_eq!(
        s.database().table("t").unwrap().scan(snap).len(),
        live,
        "scan and row_count disagreed after soak"
    );
    let _ = Metrics::get(&s.metrics().compactions);
}

#[test]
fn soak_state_survives_a_restart() {
    // A soak is only meaningful if what it built is still durable afterwards.
    let secs = soak_secs().min(3);
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let expected;
    {
        let s = Storage::open(io.clone(), StorageConfig::default()).unwrap();
        s.create_table("t").unwrap();
        let clock = RealClock::new();
        let start = clock.now_nanos();
        let mut rng = Rng::new(7);
        while clock.now_nanos() - start < secs * 1_000_000_000 {
            for _ in 0..200 {
                let pk = rng.range(0, 5_000);
                let _ = s.upsert("t", row(pk, "v"));
            }
        }
        s.checkpoint().unwrap();
        expected = s
            .database()
            .table("t")
            .unwrap()
            .row_count(s.database().snapshot());
    }
    let s2 = Storage::open(io, StorageConfig::default()).unwrap();
    let t = s2.database().table("t").unwrap();
    assert_eq!(
        t.row_count(s2.database().snapshot()),
        expected,
        "soak state did not survive restart"
    );
}
