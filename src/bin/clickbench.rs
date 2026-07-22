//! ClickBench-shaped validation: a wide (105-column) analytical table and the
//! standard ClickBench query subset, run through ChakraDB + DataFusion and
//! compared against DuckDB on the *same* CSV.
//!
//! Honest scope: this uses **synthetic data** over the real ClickBench `hits`
//! schema and real query shapes, at a scaled-down row count — not the official
//! 100M-row dataset. It validates (a) that the arbitrary-schema engine loads and
//! queries a 105-column table, and (b) relative performance vs DuckDB. It is not
//! an official ClickBench submission.
//!
//!   cargo run --release --features datafusion --bin clickbench -- <csv> <rows> <runs>
//!
//! If <csv> does not exist it is generated first. Then `scripts/clickbench_duckdb.sh
//! <csv> <runs>` runs the identical queries on DuckDB.

use chakradb::{ColumnDef, DataType, Database, Rng, Row, Schema, SqlEngine, Value};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;
use std::time::Instant;

/// (name, type) for the ClickBench-derived schema. `WatchID` is the primary key
/// (unique per row). The first 28 columns are real ClickBench columns the query
/// set touches; the rest are filler to reach the real table's 105-column width.
fn schema_cols() -> Vec<(String, DataType)> {
    use DataType::*;
    let mut c: Vec<(String, DataType)> = vec![
        ("WatchID".into(), Int),
        ("JavaEnable".into(), Int),
        ("Title".into(), Text),
        ("GoodEvent".into(), Int),
        ("EventTime".into(), Int),
        ("EventDate".into(), Int),
        ("CounterID".into(), Int),
        ("ClientIP".into(), Int),
        ("RegionID".into(), Int),
        ("UserID".into(), Int),
        ("CounterClass".into(), Int),
        ("OS".into(), Int),
        ("UserAgent".into(), Int),
        ("URL".into(), Text),
        ("Referer".into(), Text),
        ("IsRefresh".into(), Int),
        ("ResolutionWidth".into(), Int),
        ("ResolutionHeight".into(), Int),
        ("SearchPhrase".into(), Text),
        ("SearchEngineID".into(), Int),
        ("AdvEngineID".into(), Int),
        ("IsMobile".into(), Int),
        ("MobilePhoneModel".into(), Text),
        ("Age".into(), Int),
        ("Sex".into(), Int),
        ("Income".into(), Int),
        ("WindowClientWidth".into(), Int),
        ("WindowClientHeight".into(), Int),
    ];
    // Filler columns to the real 105-column width, cycling types.
    let tys = [Int, Float, Text, Int];
    for i in c.len()..105 {
        c.push((format!("c{i}"), tys[i % tys.len()]));
    }
    c
}

fn chakra_schema(cols: &[(String, DataType)]) -> Schema {
    let defs: Vec<ColumnDef> = cols
        .iter()
        .map(|(n, t)| ColumnDef::new(n.clone(), *t))
        .collect();
    // WatchID (column 0) is the primary key.
    Schema::from_user_columns(defs, Some(0))
}

/// Generate one row's field strings (CSV order = schema order).
fn gen_row(i: i64, n: i64, rng: &mut Rng) -> Vec<String> {
    let r = |rng: &mut Rng, m: i64| rng.range(0, m);
    let uid_space = (n / 10).max(1);
    let search = if r(rng, 10) < 7 {
        String::new()
    } else {
        format!("phrase{}", r(rng, 1000))
    };
    let is_mobile = r(rng, 2);
    let phone = if is_mobile == 1 {
        format!("m{}", r(rng, 50))
    } else {
        String::new()
    };
    let adv = if r(rng, 10) < 9 { 0 } else { r(rng, 5) + 1 };
    let mut f: Vec<String> = vec![
        i.to_string(),                           // WatchID (pk)
        r(rng, 2).to_string(),                   // JavaEnable
        format!("t{}", r(rng, 10000)),           // Title
        "1".into(),                              // GoodEvent
        (1_600_000_000 + r(rng, n)).to_string(), // EventTime
        (19000 + r(rng, 365)).to_string(),       // EventDate
        r(rng, 1000).to_string(),                // CounterID
        r(rng, 1_000_000).to_string(),           // ClientIP
        r(rng, 100).to_string(),                 // RegionID
        r(rng, uid_space).to_string(),           // UserID
        r(rng, 3).to_string(),                   // CounterClass
        r(rng, 10).to_string(),                  // OS
        r(rng, 20).to_string(),                  // UserAgent
        format!("http://s{}", r(rng, 100000)),   // URL
        format!("http://r{}", r(rng, 50000)),    // Referer
        r(rng, 2).to_string(),                   // IsRefresh
        r(rng, 2560).to_string(),                // ResolutionWidth
        r(rng, 1440).to_string(),                // ResolutionHeight
        search,                                  // SearchPhrase
        r(rng, 20).to_string(),                  // SearchEngineID
        adv.to_string(),                         // AdvEngineID
        is_mobile.to_string(),                   // IsMobile
        phone,                                   // MobilePhoneModel
        r(rng, 100).to_string(),                 // Age
        r(rng, 2).to_string(),                   // Sex
        r(rng, 10).to_string(),                  // Income
        r(rng, 2560).to_string(),                // WindowClientWidth
        r(rng, 1440).to_string(),                // WindowClientHeight
    ];
    // Filler values.
    for ci in f.len()..105 {
        match ci % 4 {
            1 => f.push(format!("{}.5", r(rng, 100))),
            2 => f.push("x".into()),
            _ => f.push(r(rng, 100).to_string()),
        }
    }
    f
}

fn generate_csv(path: &str, n: i64, cols: &[(String, DataType)]) {
    let file = std::fs::File::create(path).expect("create csv");
    let mut w = BufWriter::new(file);
    let header: Vec<&str> = cols.iter().map(|(name, _)| name.as_str()).collect();
    writeln!(w, "{}", header.join(",")).unwrap();
    let mut rng = Rng::new(42);
    for i in 0..n {
        writeln!(w, "{}", gen_row(i, n, &mut rng).join(",")).unwrap();
    }
}

fn parse_field(s: &str, ty: DataType) -> Value {
    if s.is_empty() {
        return Value::Null; // empty field -> NULL, matching DuckDB read_csv
    }
    match ty {
        DataType::Int | DataType::Date | DataType::Timestamp => {
            s.parse::<i64>().map(Value::Int).unwrap_or(Value::Null)
        }
        DataType::Float => s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
        DataType::Text => Value::Text(s.to_string()),
        DataType::Bool => Value::Bool(s == "1" || s.eq_ignore_ascii_case("true")),
    }
}

fn load(db: &Arc<Database>, path: &str, cols: &[(String, DataType)]) -> usize {
    const CHUNK: usize = 256 * 1024;
    let t = db.table("hits").unwrap();
    let file = std::fs::File::open(path).expect("open csv");
    let mut n = 0;
    let mut chunk: Vec<Row> = Vec::with_capacity(CHUNK);
    for (li, line) in BufReader::new(file).lines().enumerate() {
        let line = line.unwrap();
        if li == 0 {
            continue; // header
        }
        let values: Vec<Value> = line
            .split(',')
            .zip(cols.iter())
            .map(|(s, (_, ty))| parse_field(s, *ty))
            .collect();
        chunk.push(Row::from_values(values));
        if chunk.len() == CHUNK {
            n += chunk.len();
            t.bulk_load(std::mem::take(&mut chunk));
        }
    }
    if !chunk.is_empty() {
        n += chunk.len();
        t.bulk_load(chunk);
    }
    n
}

fn queries(n: i64) -> Vec<(String, String)> {
    let mut q: Vec<(String, String)> = base_queries()
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
    // Selective range scans on the sequential `WatchID` primary key — the shape
    // zonemap part pruning accelerates (and DuckDB accelerates via rowgroup
    // pruning). These aren't in the standard ClickBench subset, which has no
    // selective range predicate, so we add them to exercise pruning head-to-head.
    let lo_narrow = n - 20; // last ~20 rows: prunes all but the final part
    let lo_wide = n - n / 100; // last 1%
    q.push((
        "Q13 pk range ~20 rows".into(),
        format!("SELECT WatchID, Title, EventDate FROM hits WHERE WatchID >= {lo_narrow}"),
    ));
    q.push((
        "Q14 pk range ~1%".into(),
        format!("SELECT WatchID, EventDate FROM hits WHERE WatchID >= {lo_wide}"),
    ));
    q
}

fn base_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Q0  count", "SELECT COUNT(*) FROM hits"),
        ("Q1  count filter", "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"),
        ("Q2  sum/avg", "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits"),
        ("Q3  avg", "SELECT AVG(UserID) FROM hits"),
        ("Q4  distinct users", "SELECT COUNT(DISTINCT UserID) FROM hits"),
        ("Q5  distinct phrase", "SELECT COUNT(DISTINCT SearchPhrase) FROM hits"),
        ("Q6  min/max date", "SELECT MIN(EventDate), MAX(EventDate) FROM hits"),
        (
            "Q7  group adv",
            "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
        ),
        (
            "Q8  region distinct-u",
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q9  top phrases",
            "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q10 top users",
            "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
        ),
        (
            "Q11 phrase by time",
            "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
        ),
        (
            "Q12 top widths",
            "SELECT ResolutionWidth, COUNT(*) FROM hits GROUP BY ResolutionWidth ORDER BY COUNT(*) DESC LIMIT 10",
        ),
    ]
}

fn median_ms(engine: &SqlEngine, sql: &str, runs: usize) -> (f64, usize, String) {
    let mut lat = Vec::with_capacity(runs);
    let mut rows = 0;
    let mut fp = String::new();
    for _ in 0..runs {
        let t0 = Instant::now();
        let r = engine.query(sql).expect("query");
        lat.push(t0.elapsed().as_secs_f64() * 1e3);
        rows = r.len();
        // Fingerprint: first row joined, for cross-engine correctness checks.
        fp = r.first().map(|row| row.join("/")).unwrap_or_default();
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (lat[lat.len() / 2], rows, fp)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let csv = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "/tmp/clickbench.csv".to_string());
    let n: i64 = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let runs: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5);

    let cols = schema_cols();
    if !std::path::Path::new(&csv).exists() {
        eprintln!("generating {n} rows x {} cols -> {csv}", cols.len());
        let t0 = Instant::now();
        generate_csv(&csv, n, &cols);
        eprintln!("  generated in {:.1}s", t0.elapsed().as_secs_f64());
    }

    let db = Arc::new(Database::new());
    db.create_table_schema("hits", chakra_schema(&cols))
        .unwrap();
    let t0 = Instant::now();
    let loaded = load(&db, &csv, &cols);
    let load_s = t0.elapsed().as_secs_f64();
    let engine = SqlEngine::new(db);

    println!(
        "# ChakraDB + DataFusion — ClickBench-shaped ({} columns)",
        cols.len()
    );
    println!("Loaded {loaded} rows from {csv} in {load_s:.1}s. Median of {runs} runs.\n");
    println!("| query | ChakraDB p50 (ms) | rows | first-row |");
    println!("|---|---|---|---|");
    for (label, sql) in queries(n) {
        let (ms, rows, fp) = median_ms(&engine, &sql, runs);
        println!("| {label} | {ms:.1} | {rows} | {fp} |");
    }
}
