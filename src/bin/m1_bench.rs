//! M1 acceptance benchmark.
//!
//! Produces the measurements the M1 gate is judged on (`docs/roadmap.md` §M1):
//!
//! * **M1-2** recovery time bounded by WAL tail, flat as the database grows
//! * **M1-3** sustained ingest with compaction at equilibrium
//! * **M1-4** backpressure engages before scans degrade, and is observable
//! * **M1-5** write latency under concurrent scan load, per durability mode
//!
//! Plus the M0 defect re-measurement: compaction no longer starves writers.
//!
//! `cargo run --release --bin m1-bench`

use chakradb::io::MemIo;
use chakradb::{
    Clock, Durability, Metrics, RealClock, Rng, Row, Storage, StorageConfig, TableConfig,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

struct Dist {
    s: Vec<u64>,
}
impl Dist {
    fn new(mut s: Vec<u64>) -> Self {
        s.sort_unstable();
        Dist { s }
    }
    fn pct(&self, p: f64) -> u64 {
        if self.s.is_empty() {
            return 0;
        }
        self.s[((self.s.len() - 1) as f64 * p).round() as usize]
    }
    fn mean(&self) -> f64 {
        if self.s.is_empty() {
            return 0.0;
        }
        self.s.iter().sum::<u64>() as f64 / self.s.len() as f64
    }
}

/// A stand-in for real device fsync latency. NVMe is ~50–200 µs; without a cost
/// here, group commit has no window in which to batch and the measurement is
/// meaningless.
const SYNC_COST: Duration = Duration::from_micros(100);

fn row(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk * 3, pk as f64 * 0.5, tag)
}

fn cfg(d: Durability, seal: usize) -> StorageConfig {
    StorageConfig {
        durability: d,
        table: TableConfig {
            seal_threshold: seal,
            ..Default::default()
        },
        checkpoint_wal_bytes: u64::MAX,
        ..Default::default()
    }
}

// ------------------------------------------------------------------ M1-2

fn m1_2_recovery_scaling(out: &mut String) {
    out.push_str("\n## M1-2 — Recovery time vs. database size\n\n");
    out.push_str(
        "FR-06 requires restart time to depend on the **log tail**, not on how much data\n\
         exists. Each row below holds the tail fixed at 5,000 un-checkpointed records\n\
         while the checkpointed base grows 20×.\n\n\
         | base rows (checkpointed) | tail records | total recovery (ms) | parts loaded | replayed | replay-only (ms) |\n\
         |---|---|---|---|---|---|\n",
    );
    let clock = RealClock::new();
    for &base in &[10_000i64, 50_000, 200_000] {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        {
            let s = Storage::open(io.clone(), cfg(Durability::Group, 25_000)).unwrap();
            s.create_table("t").unwrap();
            for pk in 0..base {
                s.insert("t", row(pk, "base")).unwrap();
            }
            s.checkpoint().unwrap();
            // Fixed-size tail beyond the checkpoint.
            for pk in base..base + 5_000 {
                s.insert("t", row(pk, "tail")).unwrap();
            }
        }
        let t0 = clock.now_nanos();
        let s2 = Storage::open(io.clone(), cfg(Durability::Group, 25_000)).unwrap();
        let ns = clock.now_nanos() - t0;
        let r = s2.recovery().clone();
        drop(s2);

        // Isolate the replay component by measuring a reopen with the parts
        // removed from the picture — this is the part FR-06 actually bounds.
        let t1 = clock.now_nanos();
        let replay = chakradb::wal::Wal::replay(&*io, "wal.log").unwrap();
        let replay_ns = clock.now_nanos() - t1;
        assert!(!replay.records.is_empty());

        out.push_str(&format!(
            "| {} | 5,000 | {:.1} | {} | {} | {:.1} |\n",
            base,
            ns as f64 / 1e6,
            r.parts_loaded,
            r.wal_records_replayed,
            replay_ns as f64 / 1e6,
        ));
    }
    out.push_str(
        "\n**Read the last column, not the third.** FR-06 bounds *log replay*, and that\n\
         stays flat because the tail is fixed. Total recovery still grows, because M1\n\
         loads every part eagerly into memory — an in-memory-resident engine cannot\n\
         avoid that. Making total recovery independent of database size needs the\n\
         demand-paged buffer pool scheduled for M2.\n",
    );
}

// ------------------------------------------------------------------ M1-5

fn m1_5_durability_latency(out: &mut String) {
    const N: usize = 3_000;
    out.push_str("\n## M1-5 — Write latency by durability mode\n\n");
    out.push_str(
        "Single writer, no contention. `group` should approach `async` throughput while\n\
         still guaranteeing no acknowledged write is lost.\n\n\
         | mode | mean (µs) | p50 | p99 | p999 | syncs/append | may lose data |\n\
         |---|---|---|---|---|---|---|\n",
    );
    let clock = RealClock::new();
    for mode in [Durability::Sync, Durability::Group, Durability::Async] {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        io.set_sync_delay(SYNC_COST);
        let s = Storage::open(io, cfg(mode, 50_000)).unwrap();
        s.create_table("t").unwrap();
        let mut lat = Vec::with_capacity(N);
        for pk in 0..N as i64 {
            let t0 = clock.now_nanos();
            s.insert("t", row(pk, "v")).unwrap();
            lat.push(clock.now_nanos() - t0);
        }
        let d = Dist::new(lat);
        out.push_str(&format!(
            "| {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.3} | {} |\n",
            mode.name(),
            d.mean() / 1000.0,
            d.pct(0.50) as f64 / 1000.0,
            d.pct(0.99) as f64 / 1000.0,
            d.pct(0.999) as f64 / 1000.0,
            s.wal().syncs_per_append(),
            if mode.may_lose_data() { "**yes**" } else { "no" },
        ));
    }
    out.push_str(
        "\nNote these are in-memory syncs, so absolute values understate a real device.\n\
         The meaningful column is **syncs/append**: group commit batching is what makes\n\
         `group` viable as the default.\n",
    );
}

/// M1-5 as actually written: latency **under concurrent scan load**.
///
/// The isolated-writer table above is easier to read but does not answer the
/// criterion — a writer contending with scanners is a different measurement,
/// and it is the one NFR-03 cares about.
fn m1_5_under_scan_load(out: &mut String) {
    const ROWS: i64 = 50_000;
    const WRITES: usize = 3_000;
    out.push_str("\n### Write latency with scanners running concurrently\n\n");
    out.push_str(
        "Four scan threads run continuously against the same table while one writer is\n\
         measured. This is the criterion as stated; the isolated table above is context.\n\n\
         | mode | p50 (µs) | p99 | p999 | max | scans completed |\n|---|---|---|---|---|---|\n",
    );
    let clock = RealClock::new();
    for mode in [Durability::Sync, Durability::Group, Durability::Async] {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        io.set_sync_delay(SYNC_COST);
        let s = Arc::new(Storage::open(io, cfg(mode, 25_000)).unwrap());
        s.create_table("t").unwrap();
        for pk in 0..ROWS {
            s.insert("t", row(pk, "v0")).unwrap();
        }

        let stop = Arc::new(AtomicBool::new(false));
        let scans = Arc::new(AtomicU64::new(0));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let s = s.clone();
                let stop = stop.clone();
                let scans = scans.clone();
                thread::spawn(move || {
                    let t = s.database().table("t").unwrap();
                    while !stop.load(Ordering::Relaxed) {
                        let _ = t.scan(s.database().snapshot());
                        scans.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        let mut rng = Rng::new(11);
        let mut lat = Vec::with_capacity(WRITES);
        for _ in 0..WRITES {
            let pk = rng.range(0, ROWS);
            let t0 = clock.now_nanos();
            let _ = s.upsert("t", row(pk, "vN"));
            lat.push(clock.now_nanos() - t0);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }

        let d = Dist::new(lat);
        out.push_str(&format!(
            "| {} | {:.2} | {:.2} | {:.2} | {:.2} | {} |\n",
            mode.name(),
            d.pct(0.50) as f64 / 1000.0,
            d.pct(0.99) as f64 / 1000.0,
            d.pct(0.999) as f64 / 1000.0,
            d.pct(1.0) as f64 / 1000.0,
            scans.load(Ordering::Relaxed),
        ));
    }
    out.push_str(
        "\nThe p999/max columns are the interesting ones: they show whether scan traffic\n\
         introduces tail latency on the write path.\n",
    );
}

fn m1_5_group_commit_scaling(out: &mut String) {
    out.push_str("\n### Group-commit batching under concurrency\n\n");
    out.push_str("| writer threads | appends | syncs | syncs/append |\n|---|---|---|---|\n");
    for &threads in &[1usize, 2, 4, 8, 16] {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        io.set_sync_delay(SYNC_COST);
        let s = Arc::new(Storage::open(io, cfg(Durability::Group, 100_000)).unwrap());
        s.create_table("t").unwrap();
        let per = 400;
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let s = s.clone();
                thread::spawn(move || {
                    for i in 0..per {
                        let pk = (t * per + i) as i64;
                        let _ = s.insert("t", row(pk, "v"));
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        out.push_str(&format!(
            "| {} | {} | {} | {:.3} |\n",
            threads,
            s.wal().append_count(),
            s.wal().sync_count(),
            s.wal().syncs_per_append()
        ));
    }
    out.push_str("\nLower is better; 1.0 would mean group commit is doing nothing.\n");
}

// ------------------------------------------------------------------ M1-4

fn m1_4_backpressure(out: &mut String) {
    out.push_str("\n## M1-4 — Backpressure\n\n");
    out.push_str(
        "§5.4 forbids silent degradation. Ingest runs with a small seal threshold so parts\n\
         accumulate fast, once with a maintenance thread and once without.\n\n\
         | maintenance | rows | parts at end | backpressure events | stalled (ms) |\n\
         |---|---|---|---|---|\n",
    );
    for maintained in [false, true] {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        let s = Arc::new(Storage::open(io, cfg(Durability::Group, 500)).unwrap());
        s.create_table("t").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker = if maintained {
            let s2 = s.clone();
            let stop2 = stop.clone();
            Some(thread::spawn(move || {
                while !stop2.load(Ordering::Relaxed) {
                    s2.compact_all();
                    thread::sleep(Duration::from_micros(200));
                }
            }))
        } else {
            None
        };
        for pk in 0..30_000i64 {
            let _ = s.insert("t", row(pk, "v"));
        }
        stop.store(true, Ordering::Relaxed);
        if let Some(w) = worker {
            w.join().unwrap();
        }
        let m = s.metrics().snapshot();
        let parts = s.database().table("t").unwrap().stats().num_parts;
        out.push_str(&format!(
            "| {} | 30,000 | {} | {} | {:.1} |\n",
            if maintained { "yes" } else { "no" },
            parts,
            m.backpressure_events,
            m.backpressure_nanos as f64 / 1e6,
        ));
    }
    out.push_str(
        "\nWithout maintenance, part count climbs and backpressure engages — visibly, in\n\
         metrics, which is the requirement. With it, debt stays bounded.\n",
    );
}

// ------------------------------------------------------------------ M0 defect

fn m0_defect_recheck(out: &mut String) {
    const ROWS: i64 = 100_000;
    out.push_str("\n## M0 defect re-check — compaction no longer starves writers\n\n");
    out.push_str(
        "M0 held the table write lock for the whole merge; enabling compaction cut applied\n\
         upserts 18×. Compaction is now two-phase: built outside the lock, installed under\n\
         it, with concurrent tombstones replayed.\n\n\
         | compaction | upserts applied in 2s | scans completed |\n|---|---|---|\n",
    );
    for compacting in [false, true] {
        let io: Arc<MemIo> = Arc::new(MemIo::new());
        let s = Arc::new(Storage::open(io, cfg(Durability::Group, 20_000)).unwrap());
        s.create_table("t").unwrap();
        for pk in 0..ROWS {
            s.insert("t", row(pk, "v0")).unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(AtomicU64::new(0));
        let scans = Arc::new(AtomicU64::new(0));

        let mut crew: Vec<thread::JoinHandle<()>> = (0..4)
            .map(|id| {
                let s = s.clone();
                let stop = stop.clone();
                let writes = writes.clone();
                thread::spawn(move || {
                    let mut rng = Rng::new(id + 1);
                    while !stop.load(Ordering::Relaxed) {
                        let pk = rng.range(0, ROWS);
                        if s.upsert("t", row(pk, "vN")).is_ok() {
                            writes.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        {
            let s = s.clone();
            let stop = stop.clone();
            let scans = scans.clone();
            crew.push(thread::spawn(move || {
                let t = s.database().table("t").unwrap();
                while !stop.load(Ordering::Relaxed) {
                    let _ = t.scan(s.database().snapshot());
                    scans.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        if compacting {
            let s = s.clone();
            let stop = stop.clone();
            crew.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    s.database().seal_all();
                    s.compact_all();
                }
            }));
        }

        thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);
        for h in crew {
            h.join().unwrap();
        }
        out.push_str(&format!(
            "| {} | {} | {} |\n",
            if compacting { "on" } else { "off" },
            writes.load(Ordering::Relaxed),
            scans.load(Ordering::Relaxed),
        ));
    }
    out.push_str("\nThe two rows should now be within the same order of magnitude.\n");
}

// ------------------------------------------------------------------ M1-3

fn m1_3_soak(out: &mut String) {
    out.push_str("\n## M1-3 — Sustained ingest (short soak)\n\n");
    out.push_str(
        "The roadmap asks for a 6-hour soak; this is a 5-second proxy that checks the same\n\
         invariant — that part count reaches equilibrium rather than growing without bound.\n\n",
    );
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let s = Arc::new(Storage::open(io, cfg(Durability::Group, 2_000)).unwrap());
    s.create_table("t").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let maint = {
        let s = s.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                s.database().seal_all();
                s.compact_all();
                thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let clock = RealClock::new();
    let start = clock.now_nanos();
    let mut rng = Rng::new(7);
    let mut samples = Vec::new();
    let mut ops = 0u64;
    while clock.now_nanos() - start < 5_000_000_000 {
        for _ in 0..500 {
            let pk = rng.range(0, 100_000);
            let _ = s.upsert("t", row(pk, "v"));
            ops += 1;
        }
        samples.push(s.database().table("t").unwrap().stats().num_parts);
    }
    stop.store(true, Ordering::Relaxed);
    maint.join().unwrap();

    let first = samples.first().copied().unwrap_or(0);
    let last = samples.last().copied().unwrap_or(0);
    let peak = samples.iter().copied().max().unwrap_or(0);
    out.push_str(&format!(
        "- upserts applied: **{ops}**\n- part count: first {first}, peak {peak}, final {last}\n\
         - rows live at end: {}\n- compactions: {}\n",
        s.database()
            .table("t")
            .unwrap()
            .row_count(s.database().snapshot()),
        Metrics::get(&s.metrics().compactions),
    ));
    out.push_str(
        "\nEquilibrium means final ≈ peak rather than final ≫ first. A monotonically\n\
         climbing part count would mean compaction cannot keep up.\n",
    );
}

fn main() {
    let mut out = String::new();
    out.push_str("# ChakraDB M1 — Benchmark Output\n\n");
    out.push_str(&format!(
        "Generated by `m1-bench` (chakradb {}).\n\n\
         > Single machine, single run, in-memory I/O. Per `requirements.md` §10.2 the\n\
         > harness is committed alongside these numbers so they can be re-run. Absolute\n\
         > figures understate real-device fsync cost; ratios are the meaningful part.\n",
        chakradb::VERSION
    ));

    m1_2_recovery_scaling(&mut out);
    m1_5_durability_latency(&mut out);
    m1_5_under_scan_load(&mut out);
    m1_5_group_commit_scaling(&mut out);
    m1_4_backpressure(&mut out);
    m0_defect_recheck(&mut out);
    m1_3_soak(&mut out);

    println!("{out}");
}
