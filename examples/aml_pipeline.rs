//! Real-time AML **pipeline** — the event-driven, scale-oriented design.
//! ====================================================================
//!
//! Where `aml_realtime.rs` runs the detectors once over a finished dataset, this
//! example is the *streaming* system: a generator thread writes transactions into
//! ChakraDB as fast as it can, and — through the built-in **change stream (CDC)**
//! — a perpetually-running AML worker reacts to each committed transaction the
//! instant it lands, never blocking the writer.
//!
//! It demonstrates the three-tier detection model that lets one embedded engine
//! keep up with millions of transactions per hour:
//!
//!   * **T0 (per-event, O(1)):** degree counters + known-bad lookups on every
//!     committed row. Fires structuring / fan-out / known-bad alerts at ingest
//!     speed.
//!   * **T2 (periodic, snapshot-isolated):** the heavy global algorithms —
//!     `laundering_cycles` (SCC) and `personalized_pagerank` — run every few
//!     thousand events over a consistent `Graph::view()`, off the write path.
//!
//! The generator and worker run concurrently over one `SqlEngine`; readers never
//! block writers (MVCC), which is exactly why the analytics can be this heavy
//! without throttling ingest. Run it:
//!
//! ```text
//! cargo run --release --example aml_pipeline --no-default-features
//! ```

use chakradb::cdc::{Cdc, CdcBackend, Change, ChangeOp, MaterializedWorker};
use chakradb::{Database, Graph, NodeId, SqlEngine};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Tunables and the account-id layout (distinct ranges → assertable typologies).
// ---------------------------------------------------------------------------
const N_LEGIT_TXNS: u64 = 60_000; // background legitimate traffic
const T2_INTERVAL: u64 = 15_000; // run the heavy global pass every N events

const LEGIT_LO: NodeId = 1;
const LEGIT_HI: NodeId = 400_000; // a large, sparse account space (realistic degrees)

const COLLECTOR: NodeId = 900_000; // structuring collector
const MULE_LO: NodeId = 900_001;
const MULE_HI: NodeId = 900_012; // 12 smurfs

const RING: [NodeId; 4] = [910_000, 910_001, 910_002, 910_003]; // layering cycle

const DISTRIBUTOR: NodeId = 920_000; // known-bad illicit-funds source
const FANOUT_LO: NodeId = 920_001;
const FANOUT_HI: NodeId = 920_020; // 20 downstream mules

const CTR_THRESHOLD: f64 = 10_000.0;
const FAN_IN_THRESHOLD: usize = 8;
const MIN_STRUCTURED: u32 = 5;
const FAN_OUT_THRESHOLD: usize = 15;

// SplitMix64 — deterministic synthetic data (no `rand` dependency).
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rule = "=".repeat(64);
    println!("\n{rule}\n  ChakraDB — Real-Time AML Pipeline (event-driven)\n{rule}\n");

    // One database, CDC-wrapped so committed writes publish to the change stream.
    let db = Arc::new(Database::new());
    let cdc = Cdc::new();
    let engine = Arc::new(SqlEngine::with_backend(CdcBackend::wrap(db, cdc.clone())));

    engine.run(
        "CREATE TABLE transactions (
            txn_id INTEGER PRIMARY KEY, src INTEGER, dst INTEGER,
            amount DECIMAL(14,2), ts TIMESTAMP )",
    )?;
    engine.run("CREATE TABLE alerts (id INTEGER PRIMARY KEY, account INTEGER, typology VARCHAR(32))")?;

    let produced = Arc::new(AtomicU64::new(0));

    // Register the AML worker as a named, MATERIALIZED derivation over the
    // transactions change stream: it maintains its detectors incrementally on its
    // own thread, tracking a CSN cursor — observable in the worker registry.
    let aml = cdc.register("aml-detector", Some("transactions"), Worker::new(engine.clone()));

    // --- The generator: streams synthetic transactions at full speed -------
    let start = Instant::now();
    {
        let engine = engine.clone();
        let produced = produced.clone();
        let mut rng = Rng(0x00A4_71CE_2026);
        let mut txn_id: u64 = 1;
        let mut emit = |src: NodeId, dst: NodeId, amount: f64, rng: &mut Rng| {
            let ts = format!(
                "2026-03-{:02} {:02}:{:02}:00",
                rng.range(1, 29),
                rng.range(0, 24),
                rng.range(0, 60)
            );
            engine
                .run(&format!(
                    "INSERT INTO transactions VALUES ({txn_id}, {src}, {dst}, {amount:.2}, '{ts}')"
                ))
                .unwrap();
            txn_id += 1;
            produced.fetch_add(1, Ordering::Relaxed);
        };

        // Inject the laundering typologies up front so they are present early…
        for mule in MULE_LO..=MULE_HI {
            let amount = CTR_THRESHOLD - f64::from(rng.range(50, 900));
            emit(mule, COLLECTOR, amount, &mut rng);
        }
        emit(COLLECTOR, RING[0], 95_000.0, &mut rng);
        for i in 0..RING.len() {
            emit(RING[i], RING[(i + 1) % RING.len()], 90_000.0, &mut rng);
        }
        for mule in FANOUT_LO..=FANOUT_HI {
            emit(DISTRIBUTOR, mule, f64::from(rng.range(2_000, 8_000)), &mut rng);
        }

        // …then bury them under a stream of legitimate traffic (a sparse DAG:
        // money flows downhill in id order, so the only cycle is the ring).
        for _ in 0..N_LEGIT_TXNS {
            let a = rng.range(LEGIT_LO, LEGIT_HI);
            let b = rng.range(LEGIT_LO, LEGIT_HI + 1);
            if a == b {
                continue;
            }
            let (src, dst) = if a < b { (a, b) } else { (b, a) };
            emit(src, dst, f64::from(rng.range(20, 4_000)) + 0.99, &mut rng);
        }
    }
    let ingest = start.elapsed();
    let total = produced.load(Ordering::Relaxed);

    // Wait for the derivation to consume every produced transaction, then stop
    // the worker and run one final global pass.
    let deadline = Instant::now() + Duration::from_secs(30);
    while aml.query(|w| w.seen) < total && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    aml.stop();
    aml.update(|w| w.global_pass());
    let seen = aml.query(|w| w.seen);
    let persisted = aml.query(|w| w.persisted);
    let rate = total as f64 / ingest.as_secs_f64();

    println!("Ingest: {total} transactions in {:.2}s", ingest.as_secs_f64());
    println!(
        "        = {:.0} txn/s  ≈  {:.1} million txn/hour",
        rate,
        rate * 3600.0 / 1_000_000.0
    );
    println!("Worker: reacted to {seen} committed transactions via the change stream");
    println!("        persisted {persisted} alerts to the `alerts` table");
    for w in cdc.workers() {
        println!(
            "Registry: worker '{}' on `{}` — cursor(CSN)={}, running={}",
            w.name,
            w.table.as_deref().unwrap_or("*"),
            w.cursor,
            w.running
        );
    }
    println!();

    let flagged = aml.query(|w| w.alerts.clone());
    println!("── Accounts flagged (typology ensemble over the live stream) ──");
    for (acct, reasons) in &flagged {
        let label = account_label(*acct);
        println!("  {acct:>7} [{label:<12}]: {}", reasons.iter().cloned().collect::<Vec<_>>().join(", "));
    }
    println!();

    // Self-check: every planted typology was caught on the live stream.
    let has = |a: NodeId, kind: &str| flagged.get(&a).is_some_and(|r| r.iter().any(|x| x.contains(kind)));
    assert!(has(COLLECTOR, "structuring"), "collector must be flagged");
    assert!(RING.iter().all(|r| has(*r, "cycle")), "ring must be flagged");
    assert!(has(DISTRIBUTOR, "fan-out"), "distributor must be flagged");

    println!("{rule}\n  All typologies detected live — pipeline verified.\n{rule}\n");
    Ok(())
}

fn account_label(a: NodeId) -> &'static str {
    if a == COLLECTOR {
        "collector"
    } else if (MULE_LO..=MULE_HI).contains(&a) {
        "smurf"
    } else if RING.contains(&a) {
        "ring"
    } else if a == DISTRIBUTOR {
        "distributor"
    } else if (FANOUT_LO..=FANOUT_HI).contains(&a) {
        "mule"
    } else {
        "other"
    }
}

// ---------------------------------------------------------------------------
// The AML worker: T0 incremental counters + T2 periodic global graph passes.
// ---------------------------------------------------------------------------
struct Worker {
    engine: Arc<SqlEngine>,
    graph: Graph,
    known_bad: HashSet<NodeId>,
    in_sources: HashMap<NodeId, HashSet<NodeId>>, // dst → distinct senders (fan-in)
    out_targets: HashMap<NodeId, HashSet<NodeId>>, // src → distinct receivers (fan-out)
    near_threshold: HashMap<NodeId, u32>,         // dst → #near-threshold deposits
    alerts: BTreeMap<NodeId, BTreeSet<String>>,
    persisted: u64,
    seen: u64,
}

/// The AML worker IS a materialized worker: each committed transaction is folded
/// into the incremental detectors (T0), with a heavy global pass (T2) every
/// `T2_INTERVAL` events.
impl MaterializedWorker for Worker {
    fn apply(&mut self, change: &Change) {
        if change.op != ChangeOp::Insert {
            return;
        }
        if let Some(new) = &change.new {
            self.on_transaction(new);
            self.seen += 1;
            if self.seen.is_multiple_of(T2_INTERVAL) {
                self.global_pass();
            }
        }
    }
}

impl Worker {
    fn new(engine: Arc<SqlEngine>) -> Self {
        let backend = engine.backend().clone();
        let graph = Graph::open(backend, "transfers").expect("open graph");
        Worker {
            engine,
            graph,
            known_bad: [DISTRIBUTOR].into_iter().collect(),
            in_sources: HashMap::new(),
            out_targets: HashMap::new(),
            near_threshold: HashMap::new(),
            alerts: BTreeMap::new(),
            persisted: 0,
            seen: 0,
        }
    }

    /// T0 — runs on every committed transaction. O(1) amortized.
    fn on_transaction(&mut self, row: &[chakradb::value::Value]) {
        use chakradb::value::Value;
        let as_u32 = |v: &Value| match v {
            Value::Int(i) => *i as NodeId,
            _ => 0,
        };
        let amount = |v: &Value| match v {
            Value::Decimal(m, s) => *m as f64 / 10f64.powi(*s as i32),
            Value::Int(i) => *i as f64,
            Value::Float(f) => *f,
            _ => 0.0,
        };
        // Row layout: (txn_id, src, dst, amount, ts).
        let src = as_u32(&row[1]);
        let dst = as_u32(&row[2]);
        let amt = amount(&row[3]);

        // Mirror the edge into the payment graph for the periodic global pass.
        let _ = self.graph.add_edge(src, dst, amt);

        let senders = self.in_sources.entry(dst).or_default();
        senders.insert(src);
        let fan_in = senders.len();
        if (9_000.0..CTR_THRESHOLD).contains(&amt) {
            *self.near_threshold.entry(dst).or_default() += 1;
        }
        let receivers = self.out_targets.entry(src).or_default();
        receivers.insert(dst);
        let fan_out = receivers.len();

        // Structuring: high fan-in AND many near-threshold deposits (HTAP signal).
        if fan_in >= FAN_IN_THRESHOLD
            && self.near_threshold.get(&dst).copied().unwrap_or(0) >= MIN_STRUCTURED
        {
            self.alert(dst, "structuring-collector");
        }
        // Mule fan-out: one account paying out to many counterparties.
        if fan_out >= FAN_OUT_THRESHOLD {
            self.alert(src, "mule-fan-out");
        }
        // Direct interaction with a known-bad actor.
        if self.known_bad.contains(&src) && dst != src {
            self.alert(dst, "pays-from-known-bad");
        }
    }

    /// T2 — periodic global pass over a consistent snapshot. Off the write path.
    fn global_pass(&mut self) {
        let view = match self.graph.view() {
            Ok(v) => v,
            Err(_) => return,
        };
        // Layering: non-trivial SCCs are round-trip laundering cycles.
        for ring in view.laundering_cycles() {
            for member in ring {
                self.alert(member, "laundering-cycle");
            }
        }
        // Risk propagation: personalized PageRank seeded at known-bad actors.
        let seeds: Vec<NodeId> = self.known_bad.iter().copied().collect();
        let risk = view.personalized_pagerank(&seeds, 30, 0.85);
        let mut ranked: Vec<(NodeId, f64)> =
            risk.into_iter().filter(|(n, _)| !self.known_bad.contains(n)).collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        for (acct, _) in ranked.into_iter().take(5) {
            self.alert(acct, "high-risk-exposure");
        }
    }

    /// Record an alert (deduplicated) and persist new ones to the `alerts` table.
    fn alert(&mut self, account: NodeId, typology: &str) {
        let reasons = self.alerts.entry(account).or_default();
        if reasons.insert(typology.to_string()) {
            self.persisted += 1;
            let _ = self.engine.run(&format!(
                "INSERT INTO alerts VALUES ({}, {account}, '{typology}')",
                self.persisted
            ));
        }
    }
}
