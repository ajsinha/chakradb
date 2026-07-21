//! Gate 2 head-to-head — ChakraDB cold-scan latency on the same 500k-row
//! dataset DuckDB is measured against (`/home/ashutosh/duckdb/hits.csv`).
//!
//! This is the ChakraDB half of the M2-4 comparison. The DuckDB half is a
//! sibling shell script (`scripts/gate2_duckdb.sh`) run against the identical
//! CSV. Both print median-of-N latency per query so the two tables line up.
//!
//! `cargo run --release --bin gate2-bench -- /home/ashutosh/duckdb/hits.csv`

use chakradb::{Clock, Database, RealClock, Row, SqlEngine};
use std::sync::Arc;

fn load_csv(db: &Arc<Database>, path: &str) -> usize {
    let text = std::fs::read_to_string(path).expect("read hits.csv");
    let t = db.create_table("hits").unwrap();
    let mut n = 0;
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            continue; // header: pk,a,b,c
        }
        let mut f = line.split(',');
        let pk: i64 = f.next().unwrap().parse().unwrap();
        let a: i64 = f.next().unwrap().parse().unwrap();
        let b: f64 = f.next().unwrap().parse().unwrap();
        let c = f.next().unwrap_or("");
        t.insert(Row::new(pk, a, b, c)).unwrap();
        n += 1;
    }
    n
}

fn median_ms(engine: &SqlEngine, sql: &str, runs: usize, clock: &RealClock) -> (f64, usize) {
    let mut lat = Vec::with_capacity(runs);
    let mut rows_out = 0;
    for _ in 0..runs {
        let t0 = clock.now_nanos();
        let r = engine.query(sql);
        let dt = clock.now_nanos() - t0;
        match r {
            Ok(rows) => {
                rows_out = rows.len();
                lat.push(dt);
            }
            Err(e) => {
                eprintln!("  query failed: {sql}\n    {e}");
                return (f64::NAN, 0);
            }
        }
    }
    lat.sort_unstable();
    (lat[lat.len() / 2] as f64 / 1e6, rows_out)
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/home/ashutosh/duckdb/hits.csv".to_string());
    let runs: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    let clock = RealClock::new();
    let db = Arc::new(Database::new());

    let t0 = clock.now_nanos();
    let n = load_csv(&db, &path);
    let load_ms = (clock.now_nanos() - t0) as f64 / 1e6;
    let engine = SqlEngine::new(db);

    println!("# ChakraDB Gate 2 head-to-head");
    println!(
        "Loaded {n} rows from {path} in {load_ms:.0} ms. Median of {runs} runs, cold in-process.\n"
    );
    println!("| query | ChakraDB p50 (ms) | rows out |");
    println!("|---|---|---|");

    let queries = [
        ("COUNT(*)", "SELECT COUNT(*) FROM hits"),
        ("SUM(a) WHERE a > 500", "SELECT SUM(a) FROM hits WHERE a > 500"),
        ("GROUP BY a", "SELECT a, COUNT(*) FROM hits GROUP BY a"),
        ("ORDER BY b LIMIT 100", "SELECT pk FROM hits ORDER BY b DESC LIMIT 100"),
        ("COUNT(DISTINCT a)", "SELECT DISTINCT a FROM hits"),
    ];
    for (label, sql) in queries {
        let (ms, rows) = median_ms(&engine, sql, runs, &clock);
        println!("| {label} | {ms:.2} | {rows} |");
    }
}
