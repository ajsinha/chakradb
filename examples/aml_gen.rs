//! `aml_gen` — a standalone transaction-generator CLI.
//! ===================================================
//!
//! A separate command-line application that streams a synthetic payment feed —
//! legitimate traffic with laundering typologies injected — into a ChakraDB
//! database at full speed. Use it to populate a dataset, to benchmark ingest, or
//! (with the Kafka sink, see the AML chapter) as the producer feeding a
//! separately-running AML worker.
//!
//! ```text
//! cargo run --release --example aml_gen --no-default-features -- \
//!     --db ./amldata --count 1000000 --seed 42
//! ```
//!
//! Flags:
//!   --db PATH      durable directory, or ":memory:" (default) for a pure
//!                  ingest benchmark
//!   --count N      number of legitimate background transactions (default 200000)
//!   --seed S       PRNG seed (default 42) — the feed is fully deterministic
//!
//! The injected typologies always use the same account-id ranges as the AML
//! worker, so a consumer can assert on them:
//!   structuring collector 900000; smurfs 900001-900012; layering ring
//!   910000-910003; known-bad distributor 920000; mules 920001-920020.

use chakradb::{Database, PosixIo, SqlEngine};
use chakradb::storage::{Storage, StorageConfig};
use std::sync::Arc;
use std::time::Instant;

const COLLECTOR: u32 = 900_000;
const MULE_LO: u32 = 900_001;
const MULE_HI: u32 = 900_012;
const RING: [u32; 4] = [910_000, 910_001, 910_002, 910_003];
const DISTRIBUTOR: u32 = 920_000;
const FANOUT_LO: u32 = 920_001;
const FANOUT_HI: u32 = 920_020;
const CTR_THRESHOLD: f64 = 10_000.0;
const LEGIT_LO: u32 = 1;
const LEGIT_HI: u32 = 400_000;

struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, lo: u32, hi: u32) -> u32 {
        lo + (self.next_u64() % u64::from(hi - lo)) as u32
    }
}

struct Args {
    db: String,
    count: u64,
    seed: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        db: ":memory:".to_string(),
        count: 200_000,
        seed: 42,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--db" => args.db = it.next().unwrap_or_else(|| args.db.clone()),
            "--count" => {
                args.count = it.next().and_then(|v| v.parse().ok()).unwrap_or(args.count)
            }
            "--seed" => args.seed = it.next().and_then(|v| v.parse().ok()).unwrap_or(args.seed),
            "-h" | "--help" => {
                println!("usage: aml_gen [--db PATH] [--count N] [--seed S]");
                std::process::exit(0);
            }
            other => eprintln!("ignoring unknown flag: {other}"),
        }
    }
    args
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    let engine = if args.db.is_empty() || args.db == ":memory:" {
        SqlEngine::new(Arc::new(Database::new()))
    } else {
        std::fs::create_dir_all(&args.db)?;
        let io = PosixIo::open(&args.db)?;
        SqlEngine::durable(Arc::new(Storage::open(Arc::new(io), StorageConfig::default())?))
    };

    engine.run(
        "CREATE TABLE transactions (
            txn_id INTEGER PRIMARY KEY, src INTEGER, dst INTEGER,
            amount DECIMAL(14,2), ts TIMESTAMP )",
    )?;

    println!(
        "aml_gen → db={} count={} seed={}",
        args.db, args.count, args.seed
    );

    let mut rng = Rng(args.seed ^ 0x00A4_71CE_2026);
    let mut txn_id: u64 = 1;
    let start = Instant::now();
    let mut emit = |src: u32, dst: u32, amount: f64, rng: &mut Rng| -> Result<(), Box<dyn std::error::Error>> {
        let ts = format!(
            "2026-03-{:02} {:02}:{:02}:00",
            rng.range(1, 29),
            rng.range(0, 24),
            rng.range(0, 60)
        );
        engine.run(&format!(
            "INSERT INTO transactions VALUES ({txn_id}, {src}, {dst}, {amount:.2}, '{ts}')"
        ))?;
        txn_id += 1;
        Ok(())
    };

    // Injected laundering typologies (fixed account ranges).
    for mule in MULE_LO..=MULE_HI {
        emit(mule, COLLECTOR, CTR_THRESHOLD - f64::from(rng.range(50, 900)), &mut rng)?;
    }
    emit(COLLECTOR, RING[0], 95_000.0, &mut rng)?;
    for i in 0..RING.len() {
        emit(RING[i], RING[(i + 1) % RING.len()], 90_000.0, &mut rng)?;
    }
    for mule in FANOUT_LO..=FANOUT_HI {
        emit(DISTRIBUTOR, mule, f64::from(rng.range(2_000, 8_000)), &mut rng)?;
    }

    // Legitimate background traffic: a sparse DAG (money flows downhill in id
    // order), so the only strongly-connected structure is the planted ring.
    let mut done: u64 = 0;
    while done < args.count {
        let a = rng.range(LEGIT_LO, LEGIT_HI);
        let b = rng.range(LEGIT_LO, LEGIT_HI + 1);
        if a == b {
            continue;
        }
        let (src, dst) = if a < b { (a, b) } else { (b, a) };
        emit(src, dst, f64::from(rng.range(20, 4_000)) + 0.99, &mut rng)?;
        done += 1;
    }

    let elapsed = start.elapsed();
    let total = txn_id - 1;
    let rate = total as f64 / elapsed.as_secs_f64();
    println!(
        "wrote {total} transactions in {:.2}s = {:.0} txn/s ≈ {:.1} million/hour",
        elapsed.as_secs_f64(),
        rate,
        rate * 3600.0 / 1_000_000.0
    );
    Ok(())
}
