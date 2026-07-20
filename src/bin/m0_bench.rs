//! M0 acceptance benchmark.
//!
//! Produces the four measurements the M0 gate is judged on
//! (`docs/roadmap.md` §M0):
//!
//! * **M0-1** scan throughput idle vs. under sustained keyed-update load
//! * **M0-2** primary-key index memory at increasing row counts
//! * **M0-3** point-update latency vs. part count
//! * **M0-4** scan cost on a part with zero recent mutations
//!
//! Run with `cargo run --release --bin m0-bench`. Output is markdown, intended
//! to be pasted into `docs/m0-findings.md` alongside interpretation.

use chakradb::{Clock, Database, Metrics, RealClock, Rng, Row, TableConfig};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

/// Latency distribution over a sample of nanosecond durations.
struct Dist {
    samples: Vec<u64>,
}

impl Dist {
    fn new(mut samples: Vec<u64>) -> Self {
        samples.sort_unstable();
        Dist { samples }
    }
    fn pct(&self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let i = ((self.samples.len() - 1) as f64 * p).round() as usize;
        self.samples[i]
    }
    fn mean(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples.iter().sum::<u64>() as f64 / self.samples.len() as f64
    }
    fn row(&self, label: &str) -> String {
        format!(
            "| {label} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
            self.mean() / 1000.0,
            self.pct(0.50) as f64 / 1000.0,
            self.pct(0.99) as f64 / 1000.0,
            self.pct(0.999) as f64 / 1000.0,
            self.pct(1.0) as f64 / 1000.0,
        )
    }
}

fn row_at(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk * 3, pk as f64 * 0.5, tag)
}

fn build_table(rows: i64, seal_threshold: usize) -> (Arc<Database>, Arc<chakradb::Table>) {
    let db = Arc::new(Database::new());
    let t = db
        .create_table_with(
            "bench",
            TableConfig {
                seal_threshold,
                ..Default::default()
            },
        )
        .unwrap();
    for pk in 0..rows {
        t.insert(row_at(pk, "v0")).unwrap();
    }
    t.seal();
    (db, t)
}

// ---------------------------------------------------------------- M0-1

fn m0_1_scan_under_write_load(out: &mut String) {
    const ROWS: i64 = 200_000;
    const SCANS: usize = 20;
    let clock = RealClock::new();

    out.push_str("\n## M0-1 — Scan throughput, idle vs. under write load\n\n");
    out.push_str(&format!(
        "Table of {ROWS} rows. {SCANS} full scans per phase.\n\n\
         | phase | mean (µs) | p50 | p99 | p999 | max |\n\
         |---|---|---|---|---|---|\n"
    ));

    // Phase 1: idle.
    let (db, t) = build_table(ROWS, 50_000);
    let mut idle = Vec::new();
    for _ in 0..SCANS {
        let s = clock.now_nanos();
        let b = t.scan(db.snapshot());
        idle.push(clock.now_nanos() - s);
        assert_eq!(b.len(), ROWS as usize);
    }
    let idle = Dist::new(idle);
    out.push_str(&idle.row("idle"));
    out.push('\n');

    // Phase 2: same scans, with writers hammering keyed updates.
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..4)
        .map(|id| {
            let t = t.clone();
            let stop = stop.clone();
            let writes = writes.clone();
            thread::spawn(move || {
                let mut rng = Rng::new(id + 1);
                while !stop.load(Ordering::Relaxed) {
                    let pk = rng.range(0, ROWS);
                    if t.upsert(row_at(pk, "vN")).is_ok() {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    let mut loaded = Vec::new();
    for _ in 0..SCANS {
        let s = clock.now_nanos();
        let b = t.scan(db.snapshot());
        loaded.push(clock.now_nanos() - s);
        assert_eq!(b.len(), ROWS as usize, "torn read under load");
    }
    stop.store(true, Ordering::Relaxed);
    let elapsed_s = {
        let start = clock.now_nanos();
        for w in writers {
            w.join().unwrap();
        }
        (clock.now_nanos() - start).max(1)
    };
    let _ = elapsed_s;

    let loaded = Dist::new(loaded);
    out.push_str(&loaded.row("under write load"));
    out.push('\n');

    let ratio = loaded.pct(0.50) as f64 / idle.pct(0.50).max(1) as f64;

    // Phase 3: identical write load, but with a maintenance thread sealing and
    // compacting. M0 has no background compactor of its own — this phase exists
    // to separate "the design degrades" from "nothing was reclaiming".
    let (db3, t3) = build_table(ROWS, 20_000);
    let stop3 = Arc::new(AtomicBool::new(false));
    let writes3 = Arc::new(AtomicU64::new(0));
    let mut crew: Vec<thread::JoinHandle<()>> = (0..4)
        .map(|id| {
            let t = t3.clone();
            let stop = stop3.clone();
            let writes = writes3.clone();
            thread::spawn(move || {
                let mut rng = Rng::new(id + 100);
                while !stop.load(Ordering::Relaxed) {
                    let pk = rng.range(0, ROWS);
                    if t.upsert(row_at(pk, "vN")).is_ok() {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    crew.push({
        let t = t3.clone();
        let db = db3.clone();
        let stop = stop3.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                t.seal();
                t.maybe_compact(db.snapshot().csn);
            }
        })
    });

    let mut maintained = Vec::new();
    for _ in 0..SCANS {
        let s = clock.now_nanos();
        let b = t3.scan(db3.snapshot());
        maintained.push(clock.now_nanos() - s);
        assert_eq!(b.len(), ROWS as usize, "torn read under maintenance");
    }
    stop3.store(true, Ordering::Relaxed);
    for h in crew {
        h.join().unwrap();
    }
    let maintained = Dist::new(maintained);
    out.push_str(&maintained.row("under load + compaction"));
    out.push('\n');

    let ratio3 = maintained.pct(0.50) as f64 / idle.pct(0.50).max(1) as f64;
    out.push_str(&format!(
        "\n**Degradation ratio (p50 vs idle)**\n\n\
         | condition | ratio | upserts applied |\n|---|---|---|\n\
         | write load, no compaction | {ratio:.2}× | {} |\n\
         | write load + compaction | {ratio3:.2}× | {} |\n",
        writes.load(Ordering::Relaxed),
        writes3.load(Ordering::Relaxed),
    ));
    out.push_str(&format!(
        "\nScan rate: idle {:.1}M rows/s · under load {:.1}M rows/s · \
         load+compaction {:.1}M rows/s.\n",
        ROWS as f64 / (idle.mean() / 1e9) / 1e6,
        ROWS as f64 / (loaded.mean() / 1e9) / 1e6,
        ROWS as f64 / (maintained.mean() / 1e9) / 1e6,
    ));
}

// ---------------------------------------------------------------- M0-2

fn m0_2_index_memory(out: &mut String) {
    out.push_str("\n## M0-2 — Primary-key index memory\n\n");
    out.push_str(
        "`index` = Bloom filter + version stamps + tombstones + per-part overhead.\n\
         It deliberately excludes column data. The comparison column is what an\n\
         explicit key→location map would cost (StarRocks-style, 8 B + hash overhead).\n\n\
         | rows | index (MB) | B/row (fresh) | B/row (compacted) | explicit map would cost (B/row) |\n\
         |---|---|---|---|---|\n",
    );

    for &rows in &[100_000i64, 500_000, 2_000_000] {
        let (db, t) = build_table(rows, 100_000);
        let fresh = t.stats();
        t.force_compact(db.snapshot().csn);
        let compacted = t.stats();
        out.push_str(&format!(
            "| {} | {:.2} | {:.2} | {:.2} | ~{:.1} |\n",
            rows,
            compacted.index_bytes as f64 / 1e6,
            fresh.index_bytes_per_row(),
            compacted.index_bytes_per_row(),
            12.0,
        ));
    }
    out.push_str(
        "\nExtrapolation to 1B rows at the compacted rate is the number that decides\n\
         maximum practical table size.\n",
    );
}

// ---------------------------------------------------------------- M0-3

fn m0_3_lookup_vs_parts(out: &mut String) {
    const ROWS: i64 = 100_000;
    out.push_str("\n## M0-3 — Point-update latency vs. part count\n\n");
    out.push_str(
        "Lookup fans out across parts newest-first (§5.2). This is the cost we accepted\n\
         in exchange for a zero-per-row index, and it is why compaction is load-bearing\n\
         for the *write* path and not only for scans.\n\n\
         | parts | mean (µs) | p50 | p99 | p999 | max | parts probed/lookup |\n\
         |---|---|---|---|---|---|---|\n",
    );

    for &target_parts in &[1usize, 2, 4, 8, 16, 32] {
        let per_part = (ROWS as usize / target_parts).max(1);
        let db = Arc::new(Database::new());
        let t = db
            .create_table_with(
                "t",
                TableConfig {
                    seal_threshold: per_part,
                    ..Default::default()
                },
            )
            .unwrap();
        for pk in 0..ROWS {
            t.insert(row_at(pk, "v0")).unwrap();
        }
        t.seal();

        let before = t.metrics().snapshot();
        let clock = RealClock::new();
        let mut rng = Rng::new(7);
        let mut lat = Vec::with_capacity(5_000);
        for _ in 0..5_000 {
            let pk = rng.range(0, ROWS);
            let s = clock.now_nanos();
            let _ = t.upsert(row_at(pk, "vN"));
            lat.push(clock.now_nanos() - s);
        }
        let after = t.metrics().snapshot();
        let probes = (after.parts_probed - before.parts_probed) as f64
            / (after.lookups - before.lookups).max(1) as f64;

        let d = Dist::new(lat);
        let actual = t.stats().num_parts;
        out.push_str(&format!(
            "| {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n",
            actual,
            d.mean() / 1000.0,
            d.pct(0.50) as f64 / 1000.0,
            d.pct(0.99) as f64 / 1000.0,
            d.pct(0.999) as f64 / 1000.0,
            d.pct(1.0) as f64 / 1000.0,
            probes,
        ));
    }
}

// ---------------------------------------------------------------- M0-4

fn m0_4_cold_scan_version_cost(out: &mut String) {
    const ROWS: i64 = 200_000;
    const SCANS: usize = 30;
    let clock = RealClock::new();

    out.push_str("\n## M0-4 — Version-check cost on cold data\n\n");
    out.push_str(
        "§5.3 claims a part with no mutations in the reader's visibility window does\n\
         **zero** per-row visibility work. Both configurations below hold the same\n\
         rows; they differ only in whether the fast path is reachable.\n\n\
         | configuration | mean (µs) | p50 | p99 | fast-path scans |\n\
         |---|---|---|---|---|\n",
    );

    // Cold: compacted, version stamps collapsed, no tombstones.
    let (db, t) = build_table(ROWS, 500_000);
    t.force_compact(db.snapshot().csn);
    let m_before = t.metrics().snapshot();
    let mut cold = Vec::new();
    for _ in 0..SCANS {
        let s = clock.now_nanos();
        let _ = t.scan(db.snapshot());
        cold.push(clock.now_nanos() - s);
    }
    let m_after = t.metrics().snapshot();
    let cold_fast = m_after.scan_fast_path - m_before.scan_fast_path;
    let cold = Dist::new(cold);
    out.push_str(&format!(
        "| cold (collapsed, no tombstones) | {:.2} | {:.2} | {:.2} | {}/{} |\n",
        cold.mean() / 1000.0,
        cold.pct(0.50) as f64 / 1000.0,
        cold.pct(0.99) as f64 / 1000.0,
        cold_fast,
        SCANS,
    ));

    // Warm: one recent delete forces per-row checks on every scan.
    let (db2, t2) = build_table(ROWS, 500_000);
    t2.force_compact(db2.snapshot().csn);
    t2.delete(ROWS / 2).unwrap();
    let m_before = t2.metrics().snapshot();
    let mut warm = Vec::new();
    for _ in 0..SCANS {
        let s = clock.now_nanos();
        let _ = t2.scan(db2.snapshot());
        warm.push(clock.now_nanos() - s);
    }
    let m_after = t2.metrics().snapshot();
    let warm_fast = m_after.scan_fast_path - m_before.scan_fast_path;
    let warm = Dist::new(warm);
    out.push_str(&format!(
        "| warm (one tombstone) | {:.2} | {:.2} | {:.2} | {}/{} |\n",
        warm.mean() / 1000.0,
        warm.pct(0.50) as f64 / 1000.0,
        warm.pct(0.99) as f64 / 1000.0,
        warm_fast,
        SCANS,
    ));

    let overhead = warm.mean() / cold.mean().max(1.0);
    out.push_str(&format!(
        "\n**Per-row visibility checking costs {overhead:.2}× a fast-path scan.**\n\
         A single tombstone is enough to disable the fast path for the whole part,\n\
         which is why compaction's tombstone-density trigger matters.\n"
    ));
}

// ----------------------------------------------------------------

fn main() {
    let mut out = String::new();
    out.push_str("# ChakraDB M0 — Benchmark Output\n\n");
    out.push_str(&format!(
        "Generated by `m0-bench` (chakradb {}). \
         Times in microseconds unless stated.\n\n\
         > Numbers are single-machine and unaudited. Per `requirements.md` §10.2, \
         > the harness and raw output are published alongside them so they can be re-run.\n",
        chakradb::VERSION
    ));

    m0_1_scan_under_write_load(&mut out);
    m0_2_index_memory(&mut out);
    m0_3_lookup_vs_parts(&mut out);
    m0_4_cold_scan_version_cost(&mut out);

    out.push_str("\n## Metrics sanity check\n\n");
    let db = Database::new();
    let t = db.create_table("t").unwrap();
    for pk in 0..10_000 {
        t.insert(row_at(pk, "v")).unwrap();
    }
    t.seal();
    for pk in (0..10_000).step_by(10) {
        t.delete(pk).unwrap();
    }
    let _ = t.scan(db.snapshot());
    let m = t.metrics();
    out.push_str(&format!(
        "- fan-out per lookup: {:.2}\n- probes eliminated before data: {:.1}%\n\
         - fast-path scan ratio: {:.1}%\n- inserts/updates/deletes: {}/{}/{}\n",
        m.fanout_per_lookup(),
        m.skip_ratio() * 100.0,
        m.fast_path_ratio() * 100.0,
        Metrics::get(&m.inserts),
        Metrics::get(&m.updates),
        Metrics::get(&m.deletes),
    ));

    println!("{out}");
}
