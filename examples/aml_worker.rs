//! `aml_worker` — a **separate-process** AML worker fed by a change log.
//! ====================================================================
//!
//! The cross-process half of the streaming AML system. `aml_gen --sink feed.jsonl`
//! (a different process) writes every committed transaction to a JSON-lines change
//! log via a `JsonlSink`; this worker **tails** that log and runs the same
//! detector ensemble — with no shared memory and no database lock between them.
//! That is the topology a Kafka deployment uses, with the file standing in for the
//! topic: swap `JsonlSink` for a Kafka sink and this worker for a Kafka consumer,
//! and nothing else changes.
//!
//! ```text
//! # terminal 1 — the producer
//! cargo run --release --example aml_gen --no-default-features -- \
//!     --db :memory: --count 200000 --sink /tmp/aml_feed.jsonl
//!
//! # terminal 2 — this worker, tailing the change log (start it first to follow live)
//! cargo run --release --example aml_worker --no-default-features -- \
//!     /tmp/aml_feed.jsonl
//! ```
//!
//! Flags: `<path>` the JSON-lines change log to tail; `--follow` keeps waiting for
//! new lines (Ctrl-C to stop); without it the worker exits at end-of-file.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::thread;
use std::time::Duration;

const DISTRIBUTOR: i64 = 920_000;
const FAN_IN_THRESHOLD: usize = 8;
const MIN_STRUCTURED: u32 = 5;
const FAN_OUT_THRESHOLD: usize = 15;
const CTR_THRESHOLD: f64 = 10_000.0;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap_or_else(|| {
        eprintln!("usage: aml_worker <feed.jsonl> [--follow]");
        std::process::exit(2);
    });
    let follow = args.any(|a| a == "--follow");

    println!("\n{}", "=".repeat(64));
    println!("  ChakraDB — Cross-Process AML Worker (tailing {path})");
    println!("{}\n", "=".repeat(64));

    let mut worker = Worker::new();
    let mut reader = BufReader::new(open_wait(&path)?);
    let mut seen: u64 = 0;
    let mut idle = 0;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            // End of current data. Either follow (wait for more) or finish.
            if follow {
                thread::sleep(Duration::from_millis(100));
                let pos = reader.stream_position()?;
                reader.seek(SeekFrom::Start(pos))?; // clear EOF, re-check for appends
                continue;
            }
            idle += 1;
            if idle > 3 {
                break;
            }
            thread::sleep(Duration::from_millis(100));
            continue;
        }
        idle = 0;
        if let Some((op, row)) = parse_line(&line) {
            if op == "insert" {
                worker.on_transaction(&row);
                seen += 1;
                if seen.is_multiple_of(20_000) {
                    println!("  … reacted to {seen} transactions");
                }
            }
        }
    }

    println!("\nReacted to {seen} committed transactions from the change log.");
    println!("\n── Accounts flagged (cross-process, over the tailed stream) ──");
    for (acct, reasons) in &worker.alerts {
        println!("  {acct:>7}: {}", reasons.iter().cloned().collect::<Vec<_>>().join(", "));
    }
    println!("\n{}\n", "=".repeat(64));
    Ok(())
}

/// Wait briefly for the change log to appear (the producer may start moments later).
fn open_wait(path: &str) -> std::io::Result<std::fs::File> {
    for _ in 0..50 {
        if let Ok(f) = std::fs::File::open(path) {
            return Ok(f);
        }
        thread::sleep(Duration::from_millis(100));
    }
    std::fs::File::open(path)
}

/// Parse one change-log line → (op, transaction row). Row fields: src, dst, amount.
struct Row {
    src: i64,
    dst: i64,
    amount: f64,
}
fn parse_line(line: &str) -> Option<(String, Row)> {
    let v: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    if v.get("table")?.as_str()? != "transactions" {
        return None;
    }
    let op = v.get("op")?.as_str()?.to_string();
    let new = v.get("new")?;
    Some((
        op,
        Row {
            src: new.get("src")?.as_i64()?,
            dst: new.get("dst")?.as_i64()?,
            amount: new.get("amount")?.as_f64()?,
        },
    ))
}

/// The detector state — the T0 incremental checks, identical to the in-process
/// worker but driven from the tailed change log instead of a live `ChangeStream`.
struct Worker {
    known_bad: HashSet<i64>,
    in_sources: HashMap<i64, HashSet<i64>>,
    out_targets: HashMap<i64, HashSet<i64>>,
    near_threshold: HashMap<i64, u32>,
    alerts: BTreeMap<i64, BTreeSet<String>>,
}

impl Worker {
    fn new() -> Self {
        Worker {
            known_bad: [DISTRIBUTOR].into_iter().collect(),
            in_sources: HashMap::new(),
            out_targets: HashMap::new(),
            near_threshold: HashMap::new(),
            alerts: BTreeMap::new(),
        }
    }

    fn on_transaction(&mut self, row: &Row) {
        let (src, dst, amt) = (row.src, row.dst, row.amount);
        let senders = self.in_sources.entry(dst).or_default();
        senders.insert(src);
        let fan_in = senders.len();
        if (9_000.0..CTR_THRESHOLD).contains(&amt) {
            *self.near_threshold.entry(dst).or_default() += 1;
        }
        let receivers = self.out_targets.entry(src).or_default();
        receivers.insert(dst);
        let fan_out = receivers.len();

        if fan_in >= FAN_IN_THRESHOLD
            && self.near_threshold.get(&dst).copied().unwrap_or(0) >= MIN_STRUCTURED
        {
            self.alert(dst, "structuring-collector");
        }
        if fan_out >= FAN_OUT_THRESHOLD {
            self.alert(src, "mule-fan-out");
        }
        if self.known_bad.contains(&src) && dst != src {
            self.alert(dst, "pays-from-known-bad");
        }
    }

    fn alert(&mut self, account: i64, typology: &str) {
        self.alerts.entry(account).or_default().insert(typology.to_string());
    }
}
