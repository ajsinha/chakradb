//! M2 acceptance benchmark — the NFR-03 measurement (`docs/roadmap.md` Gate 2).
//!
//! NFR-03 is the requirement the whole project exists for: **analytical scans
//! must stay fast while writes are in flight.** M2-3 measures it through the SQL
//! layer. M2-4 (cold ClickBench/TPC-H vs DuckDB) is *not* measured here, because
//! DuckDB is not installed in this environment — see `m2-findings.md`.
//!
//! `cargo run --release --bin m2-bench`

use chakradb::{Clock, Database, RealClock, Rng, Row, SqlEngine};
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

fn seed(db: &Arc<Database>, rows: i64) {
    let t = db.create_table("hits").unwrap();
    let mut rng = Rng::new(1);
    for pk in 0..rows {
        t.insert(Row::new(pk, rng.range(0, 1000), rng.next_f64() * 100.0, "x"))
            .unwrap();
    }
}

/// Run an analytical query repeatedly, returning its latency distribution.
fn measure_query(engine: &SqlEngine, sql: &str, n: usize, clock: &RealClock) -> Dist {
    let mut lat = Vec::with_capacity(n);
    for _ in 0..n {
        let t0 = clock.now_nanos();
        let _ = engine.query(sql).unwrap();
        lat.push(clock.now_nanos() - t0);
    }
    Dist::new(lat)
}

fn nfr03(out: &mut String) {
    const ROWS: i64 = 200_000;
    const QUERIES: usize = 40;
    let clock = RealClock::new();

    out.push_str("\n## M2-3 — NFR-03: analytical scan latency under write load\n\n");
    out.push_str(
        "An aggregate query over 200,000 rows, run 40 times per phase. This is the\n\
         axis the project exists to win: a single-writer engine like DuckDB serialises\n\
         writers against readers, so under sustained write load its scans degrade or\n\
         block. ChakraDB's readers take a snapshot and never block a writer (§7.1).\n\n\
         | phase | mean (ms) | p50 | p99 | max |\n|---|---|---|---|---|\n",
    );

    // Phase 1: idle.
    let db = Arc::new(Database::new());
    seed(&db, ROWS);
    let engine = SqlEngine::new(db.clone());
    let query = "SELECT COUNT(*), SUM(a), AVG(b) FROM hits WHERE a > 500";
    let idle = measure_query(&engine, query, QUERIES, &clock);
    out.push_str(&fmt_row("idle", &idle));

    // Phase 2: under sustained upsert load through the write path.
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..4)
        .map(|id| {
            let db = db.clone();
            let stop = stop.clone();
            let writes = writes.clone();
            thread::spawn(move || {
                let t = db.table("hits").unwrap();
                let mut rng = Rng::new(id + 10);
                while !stop.load(Ordering::Relaxed) {
                    let pk = rng.range(0, ROWS);
                    if t.upsert(Row::new(pk, rng.range(0, 1000), rng.next_f64() * 100.0, "y")).is_ok() {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    let loaded = measure_query(&engine, query, QUERIES, &clock);
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    out.push_str(&fmt_row("under write load", &loaded));

    let ratio = loaded.pct(0.50) as f64 / idle.pct(0.50).max(1) as f64;
    out.push_str(&format!(
        "\n**Degradation ratio (p50 loaded / p50 idle): {ratio:.2}×**, over {} concurrent \
         upserts applied *while the queries ran*.\n\n\
         The claim NFR-03 makes is that this ratio stays small — readers are barely \
         affected by writers. A single-writer engine cannot make that claim: its readers \
         and writers contend for the same lock.\n",
        writes.load(Ordering::Relaxed)
    ));
}

fn fmt_row(label: &str, d: &Dist) -> String {
    format!(
        "| {label} | {:.2} | {:.2} | {:.2} | {:.2} |\n",
        d.mean() / 1e6,
        d.pct(0.50) as f64 / 1e6,
        d.pct(0.99) as f64 / 1e6,
        d.pct(1.0) as f64 / 1e6,
    )
}

fn sql_throughput(out: &mut String) {
    out.push_str("\n## SQL query throughput (context for M2-4)\n\n");
    out.push_str(
        "Representative queries over 200,000 rows, cold (no concurrent writes). These are\n\
         *not* a DuckDB comparison — DuckDB is not installed here, so M2-4 is unmet; see\n\
         `m2-findings.md`. They establish our own baseline so a future comparison has a\n\
         reference point.\n\n\
         | query | p50 (ms) | rows out |\n|---|---|---|\n",
    );
    let clock = RealClock::new();
    let db = Arc::new(Database::new());
    seed(&db, 200_000);
    let engine = SqlEngine::new(db);
    let queries = [
        ("COUNT(*)", "SELECT COUNT(*) FROM hits"),
        ("filtered aggregate", "SELECT SUM(a) FROM hits WHERE a > 500"),
        ("group by", "SELECT a, COUNT(*) FROM hits GROUP BY a"),
        ("order + limit", "SELECT pk FROM hits ORDER BY b DESC LIMIT 100"),
        ("distinct", "SELECT DISTINCT a FROM hits"),
    ];
    for (label, sql) in queries {
        let d = measure_query(&engine, sql, 15, &clock);
        let rows_out = engine.query(sql).unwrap().len();
        out.push_str(&format!(
            "| {label} | {:.2} | {} |\n",
            d.pct(0.50) as f64 / 1e6,
            rows_out
        ));
    }
    out.push_str(
        "\nAbsolute numbers reflect the M2 interpreter (row-at-a-time, string-rendered),\n\
         which `requirements.md` §8 anticipates replacing with DataFusion behind the scan\n\
         boundary if execution becomes the bottleneck. They are a floor, not a ceiling.\n",
    );
}

fn main() {
    let _ = Duration::from_secs(0);
    let mut out = String::new();
    out.push_str("# ChakraDB M2 — Benchmark Output\n\n");
    out.push_str(&format!(
        "Generated by `m2-bench` (chakradb {}). Single machine, single run, in-memory.\n\
         Per `requirements.md` §10.2 the harness is committed with the numbers.\n",
        chakradb::VERSION
    ));
    nfr03(&mut out);
    sql_throughput(&mut out);
    out.push_str(
        "\n## M2-4 status\n\nNOT MEASURED. DuckDB is not installed in this environment, and \
         M2-4 is defined as a comparison against it. `m2-findings.md` records this as the \
         one Gate-2 criterion that cannot be closed here.\n",
    );
    println!("{out}");
}
