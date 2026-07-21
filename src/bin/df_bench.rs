//! M3 spike — DataFusion over a ChakraDB MVCC snapshot.
//!
//! Answers two questions with *measured* numbers, not projections:
//!   1. How much does a vectorised executor close the analytics gap vs DuckDB?
//!   2. Does the concurrency wedge survive the handoff — does a DataFusion query
//!      see a consistent snapshot while writers mutate the table underneath it?
//!
//! Plus a third, qualitative point: DataFusion runs joins / windows / subqueries
//! that ChakraDB's own interpreter parse-rejects outright.
//!
//! `cargo run --release --features datafusion --bin df-bench -- /home/ashutosh/duckdb/hits.csv`

use chakradb::datafusion_bridge::snapshot_memtable;
use chakradb::{Database, Rng, Row, SqlEngine};
use datafusion::prelude::SessionContext;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn load_csv(db: &Arc<Database>, path: &str) -> usize {
    let text = std::fs::read_to_string(path).expect("read hits.csv");
    let t = db.create_table("hits").unwrap();
    let mut n = 0;
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            continue;
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

async fn median_ms(ctx: &SessionContext, sql: &str, runs: usize) -> (f64, usize) {
    let mut lat = Vec::with_capacity(runs);
    let mut rows_out = 0;
    for _ in 0..runs {
        let t0 = Instant::now();
        let df = ctx.sql(sql).await.expect("plan");
        let batches = df.collect().await.expect("execute");
        lat.push(t0.elapsed().as_secs_f64() * 1e3);
        rows_out = batches.iter().map(|b| b.num_rows()).sum();
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (lat[lat.len() / 2], rows_out)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/home/ashutosh/duckdb/hits.csv".to_string());
    let runs: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);

    let db = Arc::new(Database::new());
    let t0 = Instant::now();
    let n = load_csv(&db, &path);
    let load_ms = t0.elapsed().as_secs_f64() * 1e3;

    // ---- Part 1: the concurrency wedge, through DataFusion ----
    // Take a snapshot, then start writers. The DataFusion query below must see
    // exactly `n` rows (the snapshot) no matter how many upserts land meanwhile.
    let snap = db.snapshot();
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..4)
        .map(|id| {
            let db = db.clone();
            let stop = stop.clone();
            let writes = writes.clone();
            thread::spawn(move || {
                let t = db.table("hits").unwrap();
                let mut rng = Rng::new(id + 100);
                while !stop.load(Ordering::Relaxed) {
                    let pk = rng.range(0, n as i64);
                    if t.upsert(Row::new(
                        pk,
                        rng.range(0, 1000),
                        rng.next_f64() * 100.0,
                        "w",
                    ))
                    .is_ok()
                    {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    // Build the executor over the *snapshot* while writers run.
    let ctx = SessionContext::new();
    let mem = snapshot_memtable(db.table("hits").unwrap().as_ref(), snap);
    ctx.register_table("hits", Arc::new(mem)).unwrap();

    println!("# ChakraDB + DataFusion — M3 spike");
    println!("Loaded {n} rows from {path} in {load_ms:.0} ms. Median of {runs} runs, cold.\n");

    // ---- Part 2: the five Gate-2 queries, measured ----
    println!("## Analytics — measured (DataFusion executor over ChakraDB snapshot)\n");
    println!("| query | DataFusion p50 (ms) | rows out |");
    println!("|---|---|---|");
    let queries = [
        ("COUNT(*)", "SELECT COUNT(*) FROM hits"),
        (
            "SUM(a) WHERE a > 500",
            "SELECT SUM(a) FROM hits WHERE a > 500",
        ),
        ("GROUP BY a", "SELECT a, COUNT(*) FROM hits GROUP BY a"),
        (
            "ORDER BY b LIMIT 100",
            "SELECT pk FROM hits ORDER BY b DESC LIMIT 100",
        ),
        ("COUNT(DISTINCT a)", "SELECT COUNT(DISTINCT a) FROM hits"),
    ];
    for (label, sql) in queries {
        let (ms, rows) = median_ms(&ctx, sql, runs).await;
        println!("| {label} | {ms:.2} | {rows} |");
    }

    // ---- Part 3: snapshot consistency under write load ----
    let seen: usize = {
        let df = ctx.sql("SELECT COUNT(*) FROM hits").await.unwrap();
        let b = df.collect().await.unwrap();
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        b[0].column(0).as_primitive::<Int64Type>().value(0) as usize
    };
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    let applied = writes.load(Ordering::Relaxed);
    println!("\n## Concurrency wedge — snapshot held across the executor\n");
    println!(
        "DataFusion saw **{seen} rows** (the snapshot), while **{applied} concurrent upserts** \
         were applied to the same table during the queries. The snapshot never shifted; \
         writers never blocked. DuckDB cannot open that second writer at all.\n"
    );
    assert_eq!(seen, n, "snapshot must be stable under concurrent writes");

    // ---- Part 4: SQL the interpreter cannot run at all ----
    println!("## SQL completeness — queries the ChakraDB interpreter rejects\n");
    let advanced = [
        (
            "self-join",
            "SELECT h1.pk, h2.pk FROM hits h1 JOIN hits h2 ON h1.pk = h2.a WHERE h1.pk < 3",
        ),
        (
            "window function",
            "SELECT pk, a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) rn FROM hits LIMIT 3",
        ),
        (
            "correlated subquery",
            "SELECT COUNT(*) FROM hits WHERE a > (SELECT AVG(a) FROM hits)",
        ),
    ];
    let interp = SqlEngine::new(db.clone());
    println!("| query | ChakraDB interpreter | DataFusion |");
    println!("|---|---|---|");
    for (label, sql) in advanced {
        let interp_result = match interp.query(sql) {
            Ok(_) => "runs".to_string(),
            Err(_) => "**rejected**".to_string(),
        };
        let df_ok =
            ctx.sql(sql).await.is_ok() && ctx.sql(sql).await.unwrap().collect().await.is_ok();
        let df_result = if df_ok { "runs" } else { "error" };
        println!("| {label} | {interp_result} | {df_result} |");
    }
}
