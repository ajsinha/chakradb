//! Real-time Counterparty Credit Risk & Market Risk on ChakraDB
//! ============================================================
//!
//! The risk analogue of the AML flagship. A live portfolio of trades and a
//! streaming market-data feed drive **real-time exposure, limit, VaR, and
//! systemic-risk** calculations — the workload banks still run in overnight
//! batches. Everything reacts to the price stream through ChakraDB's change
//! stream (CDC) and a **materialized worker**, over one consistent live dataset,
//! never blocking ingest.
//!
//! Two risk domains, mapped to built-in primitives:
//!
//!   * **Market risk & current exposure (per tick, T0):** as each price ticks,
//!     reprice the affected trades, update each counterparty's netted current
//!     exposure, and check single-name limits. O(trades-on-that-instrument).
//!   * **Portfolio & systemic risk (periodic, T2, snapshot-isolated):**
//!     historical-simulation **VaR**, plus the **counterparty exposure network**:
//!     `pagerank` for systemic importance, **`eisenberg_noe`** for default-cascade
//!     contagion, and `laundering_cycles` for circular exposures (netting
//!     opportunities). Runs over a consistent `Graph::view()` while trades book.
//!
//! It generates its own synthetic portfolio and interbank network with risks
//! deliberately injected, then verifies each is caught live. Run it:
//!
//! ```text
//! cargo run --release --example ccr_pipeline --no-default-features
//! ```

use chakradb::cdc::{Cdc, CdcBackend, Change, ChangeOp, MaterializedWorker};
use chakradb::{Database, Graph, NodeId, SqlEngine};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Scenario layout. Counterparties are small ids; risks use fixed ones so the
// run can assert them. Instruments 101–104 are calm; 105 is volatile.
// ---------------------------------------------------------------------------
const HUB: NodeId = 1; // systemically important — many counterparties owe it
const WEAK: NodeId = 2; // thinly capitalised — origin of a default cascade
const CHAIN: [NodeId; 2] = [3, 4]; // dragged under by WEAK (contagion)
const RING: [NodeId; 3] = [6, 7, 8]; // circular exposures (netting opportunity)
const CONC: NodeId = HUB; // also holds a concentrated trade → limit breach
const PORTFOLIO: NodeId = 0; // the book itself, for VaR alerts

const INSTR_CONC: NodeId = 101; // ramps up → breaches CONC's limit
const INSTR_VOL: NodeId = 105; // swings violently → breaches portfolio VaR

const N_TICKS: u64 = 40_000;
const T2_INTERVAL: u64 = 8_000;
const VAR_WINDOW: usize = 512;
const VAR_LIMIT: f64 = 2_000_000.0; // 99% 1-tick VaR limit
const CONC_LIMIT: f64 = 3_000_000.0; // single-name current-exposure limit

struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next_u64() % (hi - lo) as u64) as i64
    }
}

#[derive(Clone)]
struct Trade {
    counterparty: NodeId,
    instrument: NodeId,
    notional: f64, // signed exposure per price point
    trade_price: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rule = "=".repeat(64);
    println!("\n{rule}\n  ChakraDB — Real-Time Counterparty Credit & Market Risk\n{rule}\n");

    let db = Arc::new(Database::new());
    let cdc = Cdc::new();
    let engine = Arc::new(SqlEngine::with_backend(CdcBackend::wrap(db.clone(), cdc.clone())));

    // --- Schema ------------------------------------------------------------
    engine.run(
        "CREATE TABLE counterparties (id INTEGER PRIMARY KEY, name VARCHAR(32),
            rating VARCHAR(4), external_assets DECIMAL(18,2))",
    )?;
    engine.run(
        "CREATE TABLE trades (trade_id INTEGER PRIMARY KEY, counterparty INTEGER,
            instrument INTEGER, notional DECIMAL(18,2), trade_price DECIMAL(12,4))",
    )?;
    engine.run(
        "CREATE TABLE market_data (tick_id INTEGER PRIMARY KEY, instrument INTEGER,
            price DECIMAL(12,4), ts TIMESTAMP)",
    )?;
    engine.run("CREATE TABLE limits (counterparty INTEGER PRIMARY KEY, limit_amount DECIMAL(18,2))")?;
    engine.run(
        "CREATE TABLE interbank (id INTEGER PRIMARY KEY, debtor INTEGER, creditor INTEGER,
            amount DECIMAL(18,2))",
    )?;
    engine.run("CREATE TABLE risk_alerts (id INTEGER PRIMARY KEY, entity INTEGER, kind VARCHAR(40))")?;

    // --- Structural setup: counterparties, trades, interbank network -------
    let mut external_assets: HashMap<NodeId, f64> = HashMap::new();
    let mut fund = |eng: &SqlEngine, id: NodeId, name: &str, rating: &str, assets: f64| {
        external_assets.insert(id, assets);
        eng.run(&format!(
            "INSERT INTO counterparties VALUES ({id}, '{name}', '{rating}', {assets:.2})"
        ))
        .unwrap();
    };
    fund(&engine, HUB, "HubBank", "AA", 5_000_000.0);
    fund(&engine, WEAK, "WeakCo", "B", 10_000.0); // thinly capitalised
    fund(&engine, CHAIN[0], "ChainA", "BB", 20_000.0);
    fund(&engine, CHAIN[1], "ChainB", "BB", 20_000.0);
    fund(&engine, 5, "Solid5", "A", 2_000_000.0);
    for &r in &RING {
        fund(&engine, r, &format!("Ring{r}"), "A", 2_000_000.0);
    }

    // Interbank liabilities → the exposure graph (debtor -> creditor : amount).
    let exposures = Graph::open(db.clone(), "exposure_net")?;
    let mut ib_id = 1u32;
    let mut owe = |eng: &SqlEngine, g: &Graph, d: NodeId, c: NodeId, amt: f64| {
        g.add_edge(d, c, amt).unwrap();
        eng.run(&format!(
            "INSERT INTO interbank VALUES ({ib_id}, {d}, {c}, {amt:.2})"
        ))
        .unwrap();
        ib_id += 1;
    };
    // Systemic hub: everyone owes HUB.
    for cp in [WEAK, CHAIN[0], CHAIN[1], 5, RING[0], RING[1], RING[2]] {
        owe(&engine, &exposures, cp, HUB, 200_000.0);
    }
    // Default cascade: WEAK owes ChainA owes ChainB (all thinly funded).
    owe(&engine, &exposures, WEAK, CHAIN[0], 100_000.0);
    owe(&engine, &exposures, CHAIN[0], CHAIN[1], 100_000.0);
    // Circular exposure: 6 -> 7 -> 8 -> 6 (a netting opportunity).
    owe(&engine, &exposures, RING[0], RING[1], 50_000.0);
    owe(&engine, &exposures, RING[1], RING[2], 50_000.0);
    owe(&engine, &exposures, RING[2], RING[0], 50_000.0);

    // Our trading book (drives market risk & current exposure).
    let mut trades: Vec<Trade> = Vec::new();
    let mut add_trade = |eng: &SqlEngine, id: &mut u32, cp: NodeId, instr: NodeId, notional: f64| {
        trades.push(Trade { counterparty: cp, instrument: instr, notional, trade_price: 100.0 });
        eng.run(&format!(
            "INSERT INTO trades VALUES ({}, {cp}, {instr}, {notional:.2}, 100.0000)",
            *id
        ))
        .unwrap();
        *id += 1;
    };
    let mut tid = 1u32;
    add_trade(&engine, &mut tid, CONC, INSTR_CONC, 100_000.0); // concentrated single name
    add_trade(&engine, &mut tid, 5, INSTR_VOL, 150_000.0); // large volatile-instrument position
    add_trade(&engine, &mut tid, RING[0], 102, 5_000.0);
    add_trade(&engine, &mut tid, RING[1], 103, 5_000.0);
    add_trade(&engine, &mut tid, WEAK, 104, 5_000.0);

    // Single-name exposure limits.
    engine.run(&format!("INSERT INTO limits VALUES ({CONC}, {CONC_LIMIT:.2})"))?;

    // --- Register the risk engine as a materialized worker over the ticks ---
    let worker = RiskWorker::new(engine.clone(), exposures, trades, external_assets);
    let ccr = cdc.materialize(Some("market_data"), worker);

    // --- Stream the market-data feed --------------------------------------
    let produced = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    {
        let mut rng = Rng(0x00A4_71CE_2026);
        let mut prices: HashMap<NodeId, f64> = (101..=105).map(|i| (i, 100.0)).collect();
        let mut tick_id = 1u64;
        let mut emit = |instr: NodeId, price: f64| {
            engine
                .run(&format!(
                    "INSERT INTO market_data VALUES ({tick_id}, {instr}, {price:.4}, '2026-03-01 09:00:00')"
                ))
                .unwrap();
            tick_id += 1;
            produced.fetch_add(1, Ordering::Relaxed);
        };
        for step in 0..N_TICKS {
            // Calm instruments drift by a tiny random walk.
            let instr = 101 + (rng.range(0, 5)) as NodeId;
            let p = prices.get_mut(&instr).unwrap();
            if instr == INSTR_CONC {
                *p += 0.02; // steady ramp → eventually breaches CONC's limit
            } else if instr == INSTR_VOL {
                *p += rng.range(-25, 26) as f64; // violent swings → VaR breach
                *p = p.max(1.0);
            } else {
                *p += rng.range(-1, 2) as f64 * 0.1;
            }
            let price = *p;
            emit(instr, price);
            let _ = step;
        }
    }
    let ingest = start.elapsed();
    let total = produced.load(Ordering::Relaxed);

    // Drain, stop, final pass.
    let deadline = Instant::now() + Duration::from_secs(30);
    while ccr.query(|w| w.seen) < total && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    ccr.stop();
    ccr.update(|w| w.global_pass());

    let rate = total as f64 / ingest.as_secs_f64();
    println!("Ingest: {total} market-data ticks in {:.2}s", ingest.as_secs_f64());
    println!(
        "        = {:.0} ticks/s  ≈  {:.1} million/hour\n",
        rate,
        rate * 3600.0 / 1_000_000.0
    );

    let alerts = ccr.query(|w| w.alerts.clone());
    let exposure = ccr.query(|w| w.cp_exposure.clone());
    println!("── Live counterparty current exposure (netted MtM ≥ 0) ──");
    let mut ex: Vec<_> = exposure.iter().filter(|(_, &v)| v > 0.0).collect();
    ex.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
    for (cp, v) in ex.iter().take(5) {
        println!("  counterparty {cp}: ${:.0}", v);
    }
    println!("\n── Risk alerts (live over the tick stream) ──");
    for (entity, kinds) in &alerts {
        let who = if *entity == PORTFOLIO { "portfolio".to_string() } else { format!("cp {entity}") };
        println!("  {who:<14}: {}", kinds.iter().cloned().collect::<Vec<_>>().join(", "));
    }
    println!();

    // --- Self-check: every planted risk surfaced --------------------------
    let has = |e: NodeId, kind: &str| alerts.get(&e).is_some_and(|k| k.iter().any(|x| x.contains(kind)));
    assert!(has(CONC, "limit"), "concentrated single-name limit breach must fire");
    assert!(has(PORTFOLIO, "VaR"), "portfolio VaR breach must fire");
    assert!(has(HUB, "systemic"), "the hub must be flagged systemically important");
    assert!(has(WEAK, "cascade"), "WEAK must default");
    assert!(CHAIN.iter().all(|c| has(*c, "cascade")), "the cascade must reach the chain");
    assert!(RING.iter().all(|r| has(*r, "circular")), "circular exposures must be flagged");

    println!("{rule}\n  All injected risks detected live — CCR pipeline verified.\n{rule}\n");
    Ok(())
}

// ---------------------------------------------------------------------------
// The risk worker: T0 per-tick exposure/limits/VaR-window; T2 systemic pass.
// ---------------------------------------------------------------------------
struct RiskWorker {
    engine: Arc<SqlEngine>,
    exposures: Graph,
    external_assets: HashMap<NodeId, f64>,
    trades: Vec<Trade>,
    by_instrument: HashMap<NodeId, Vec<usize>>,
    mtm: Vec<f64>,                       // per-trade current MtM
    cp_exposure: HashMap<NodeId, f64>,   // counterparty → netted MtM
    prices: HashMap<NodeId, f64>,
    limits: HashMap<NodeId, f64>,
    pnl_window: VecDeque<f64>,
    alerts: BTreeMap<NodeId, BTreeSet<String>>,
    persisted: u64,
    seen: u64,
}

impl RiskWorker {
    fn new(
        engine: Arc<SqlEngine>,
        exposures: Graph,
        trades: Vec<Trade>,
        external_assets: HashMap<NodeId, f64>,
    ) -> Self {
        let mut by_instrument: HashMap<NodeId, Vec<usize>> = HashMap::new();
        for (i, t) in trades.iter().enumerate() {
            by_instrument.entry(t.instrument).or_default().push(i);
        }
        let mtm = vec![0.0; trades.len()];
        let mut limits = HashMap::new();
        limits.insert(CONC, CONC_LIMIT);
        RiskWorker {
            engine,
            exposures,
            external_assets,
            trades,
            by_instrument,
            mtm,
            cp_exposure: HashMap::new(),
            prices: (101..=105).map(|i| (i, 100.0)).collect(),
            limits,
            pnl_window: VecDeque::with_capacity(VAR_WINDOW),
            alerts: BTreeMap::new(),
            persisted: 0,
            seen: 0,
        }
    }

    /// T0 — per price tick: reprice the affected book, update exposure, limits, VaR window.
    fn on_tick(&mut self, instrument: NodeId, price: f64) {
        let old = *self.prices.get(&instrument).unwrap_or(&100.0);
        let idxs = match self.by_instrument.get(&instrument) {
            Some(v) => v.clone(),
            None => {
                self.prices.insert(instrument, price);
                return;
            }
        };
        // Portfolio P&L for this tick = Δprice · Σ notional on this instrument.
        let notional_sum: f64 = idxs.iter().map(|&i| self.trades[i].notional).sum();
        let pnl = (price - old) * notional_sum;
        if self.pnl_window.len() == VAR_WINDOW {
            self.pnl_window.pop_front();
        }
        self.pnl_window.push_back(pnl);

        // Reprice each affected trade; update its counterparty's netted exposure.
        for &i in &idxs {
            let t = &self.trades[i];
            let new_mtm = t.notional * (price - t.trade_price);
            let delta = new_mtm - self.mtm[i];
            self.mtm[i] = new_mtm;
            *self.cp_exposure.entry(t.counterparty).or_default() += delta;
        }
        self.prices.insert(instrument, price);

        // Single-name current-exposure limit check (current exposure = max(MtM, 0)).
        for &i in &idxs {
            let cp = self.trades[i].counterparty;
            let ce = self.cp_exposure.get(&cp).copied().unwrap_or(0.0).max(0.0);
            if let Some(&lim) = self.limits.get(&cp) {
                if ce > lim {
                    self.alert(cp, "single-name-limit-breach");
                }
            }
        }
    }

    /// T2 — periodic portfolio & systemic-risk pass over a consistent snapshot.
    fn global_pass(&mut self) {
        // Historical-simulation 99% one-tick VaR: the worst 1% loss in the window.
        if self.pnl_window.len() >= 32 {
            let mut losses: Vec<f64> = self.pnl_window.iter().map(|&p| -p).collect();
            losses.sort_by(|a, b| b.partial_cmp(a).unwrap()); // largest loss first
            let idx = (losses.len() as f64 * 0.01) as usize;
            let var = losses[idx].max(0.0);
            if var > VAR_LIMIT {
                self.alert(PORTFOLIO, "portfolio-VaR-breach");
            }
        }

        // The counterparty exposure network (systemic risk), over one snapshot.
        if let Ok(view) = self.exposures.view() {
            // Systemic importance: PageRank on the liability network.
            let pr = view.pagerank(30, 0.85);
            if let Some((&hub, _)) = pr.iter().max_by(|a, b| a.1.partial_cmp(b.1).unwrap()) {
                self.alert(hub, "systemically-important");
            }
            // Default contagion: the Eisenberg–Noe clearing vector.
            let clearing = view.eisenberg_noe(&self.external_assets);
            for cp in clearing.defaulted {
                self.alert(cp, "default-cascade");
            }
            // Circular exposures (netting opportunities): non-trivial SCCs.
            for ring in view.laundering_cycles() {
                for cp in ring {
                    self.alert(cp, "circular-exposure");
                }
            }
        }
    }

    fn alert(&mut self, entity: NodeId, kind: &str) {
        let kinds = self.alerts.entry(entity).or_default();
        if kinds.insert(kind.to_string()) {
            self.persisted += 1;
            let _ = self.engine.run(&format!(
                "INSERT INTO risk_alerts VALUES ({}, {entity}, '{kind}')",
                self.persisted
            ));
        }
    }
}

impl MaterializedWorker for RiskWorker {
    fn apply(&mut self, change: &Change) {
        if change.op != ChangeOp::Insert {
            return;
        }
        if let Some(new) = &change.new {
            use chakradb::value::Value;
            let as_u32 = |v: &Value| match v {
                Value::Int(i) => *i as NodeId,
                _ => 0,
            };
            let as_f64 = |v: &Value| match v {
                Value::Decimal(m, s) => *m as f64 / 10f64.powi(*s as i32),
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                _ => 0.0,
            };
            // Row: (tick_id, instrument, price, ts).
            self.on_tick(as_u32(&new[1]), as_f64(&new[2]));
            self.seen += 1;
            if self.seen.is_multiple_of(T2_INTERVAL) {
                self.global_pass();
            }
        }
    }
}
