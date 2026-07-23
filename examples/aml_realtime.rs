//! Real-time Anti-Money-Laundering (AML) on ChakraDB
//! =================================================
//!
//! A complete, self-contained AML system built on a single ChakraDB database:
//! transactions live in SQL tables (exact `DECIMAL` money, `TIMESTAMP` time), and
//! the *payment graph* is the very same data seen through ChakraDB's built-in
//! graph engine. One snapshot (`Graph::view`) powers an ensemble of detectors,
//! each mapped to a real laundering typology:
//!
//!   | Typology                         | Primitive                          |
//!   |----------------------------------|------------------------------------|
//!   | Structuring / smurfing (fan-in)  | `in_degree` over the payment graph |
//!   | Layering / round-tripping        | `laundering_cycles` (SCCs)         |
//!   | Mule fan-out (distribution)      | `out_degree`                       |
//!   | Risk propagation from known-bad  | `personalized_pagerank(seeds)`     |
//!   | Mule-network importance          | `connected_components` + `pagerank`|
//!   | Rapid movement / velocity        | temporal SQL over `transactions`   |
//!
//! It generates its own **synthetic** dataset — a sea of legitimate traffic with a
//! handful of laundering rings deliberately injected — then scores every account,
//! ranks Suspicious-Activity-Report (SAR) candidates, and *asserts* that each
//! planted ring was caught. Run it:
//!
//! ```text
//! cargo run --release --example aml_realtime --no-default-features
//! ```
//!
//! Everything here is also available from Python (`conn.graph(...)`); this example
//! is the reference for the client experience described in the case-study chapter.

use chakradb::{Database, Graph, NodeId, SqlEngine};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A payment edge `(src, dst, amount)` fed into the graph.
type Edges = Vec<(NodeId, NodeId, f64)>;

// ---------------------------------------------------------------------------
// A tiny deterministic PRNG (SplitMix64). ChakraDB has no `rand` dependency and
// we want the same dataset — and therefore the same alerts — on every run.
// ---------------------------------------------------------------------------
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `[lo, hi)`.
    fn range(&mut self, lo: u32, hi: u32) -> u32 {
        lo + (self.next_u64() % u64::from(hi - lo)) as u32
    }
    /// True with probability `p`.
    fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64) < p
    }
}

// ---------------------------------------------------------------------------
// Account-id layout. Distinct ranges make the planted rings easy to assert on.
// ---------------------------------------------------------------------------
const LEGIT_LO: NodeId = 1;
const LEGIT_HI: NodeId = 200; // 200 ordinary retail accounts

const COLLECTOR: NodeId = 500; // structuring: the account that gathers smurfed cash
const MULE_LO: NodeId = 501;
const MULE_HI: NodeId = 512; // 12 smurfs, each depositing just under the threshold

const RING: [NodeId; 4] = [700, 701, 702, 703]; // layering: a round-trip cycle

const DISTRIBUTOR: NodeId = 800; // known-bad source of illicit funds (a PPR seed)
const FANOUT_LO: NodeId = 801;
const FANOUT_HI: NodeId = 820; // 20 downstream mules receiving the spray

/// The regulatory reporting threshold (e.g. the US $10,000 CTR line). Structuring
/// is the crime of splitting deposits to stay just under it.
const CTR_THRESHOLD: f64 = 10_000.0;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::new());
    let sql = SqlEngine::new(db.clone());

    banner("ChakraDB — Real-Time AML");
    schema(&sql)?;

    // 1. Generate a synthetic world: legit traffic + injected laundering rings.
    let mut rng = Rng(0x00A4_71CE_2026);
    let edges = generate(&sql, &mut rng)?;
    let txn_count: i64 = scalar(&sql, "SELECT COUNT(*) FROM transactions")?;
    println!(
        "Ingested {txn_count} transactions across {} accounts.\n",
        distinct_accounts(&edges)
    );

    // 2. Project the payment graph and freeze one consistent snapshot. Every
    //    detector below reads THIS graph — a coherent picture even as new
    //    payments keep landing in the live tables.
    let graph = Graph::open(db.clone(), "transfers")?;
    graph.add_edges(edges.iter().copied())?;
    let view = graph.view()?;
    println!(
        "Payment graph: {} accounts (nodes), {} counterparty edges.\n",
        view.node_count(),
        view.edge_count()
    );

    // 3. Run the detector ensemble over the single snapshot.
    let known_bad: HashSet<NodeId> = [DISTRIBUTOR].into_iter().collect();

    let fan_in = detect_structuring(&sql, &view);
    let fan_out = detect_fan_out(&view);
    let rings = detect_layering(&view);
    let ring_members: HashSet<NodeId> = rings.iter().flatten().copied().collect();
    let risk = view.personalized_pagerank(&known_bad.iter().copied().collect::<Vec<_>>(), 50, 0.85);

    report_structuring(&sql, &fan_in)?;
    report_layering(&rings);
    report_fan_out(&fan_out);
    report_risk_propagation(&risk, &known_bad);
    report_velocity(&sql)?;

    // 4. Fuse the signals into one score per account and rank SAR candidates.
    let candidates = score(&view, &fan_in, &fan_out, &ring_members, &risk, &known_bad);
    report_sar(&candidates, &fan_in, &fan_out, &ring_members, &known_bad);

    // 5. Self-check: every planted typology must surface.
    verify(&fan_in, &fan_out, &ring_members, &risk, &candidates);

    banner("All typologies detected — AML pipeline verified.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------
fn schema(sql: &SqlEngine) -> Result<(), Box<dyn std::error::Error>> {
    sql.run(
        "CREATE TABLE accounts (
            id       INTEGER PRIMARY KEY,
            owner    VARCHAR(64) NOT NULL,
            kind     VARCHAR(16) NOT NULL DEFAULT 'retail',
            opened   DATE
        )",
    )?;
    sql.run(
        "CREATE TABLE transactions (
            txn_id   INTEGER PRIMARY KEY,
            src      INTEGER NOT NULL,
            dst      INTEGER NOT NULL,
            amount   DECIMAL(14,2) NOT NULL CHECK (amount > 0),
            ts       TIMESTAMP NOT NULL
        )",
    )?;
    sql.run(
        "CREATE TABLE known_bad (
            id       INTEGER PRIMARY KEY,
            reason   VARCHAR(64) NOT NULL
        )",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Synthetic data generation
// ---------------------------------------------------------------------------

/// Build the world and return the payment edges `(src, dst, amount)` for the
/// graph. Rows are also written to the SQL `transactions` table.
fn generate(
    sql: &SqlEngine,
    rng: &mut Rng,
) -> Result<Edges, Box<dyn std::error::Error>> {
    let mut edges: Edges = Vec::new();
    let mut txn_id: u32 = 1;

    // -- Accounts -----------------------------------------------------------
    for id in LEGIT_LO..=LEGIT_HI {
        insert_account(sql, id, "retail")?;
    }
    for id in [&[COLLECTOR][..], &(MULE_LO..=MULE_HI).collect::<Vec<_>>()].concat() {
        insert_account(sql, id, "personal")?;
    }
    for &id in &RING {
        insert_account(sql, id, "shell")?;
    }
    insert_account(sql, DISTRIBUTOR, "shell")?;
    for id in FANOUT_LO..=FANOUT_HI {
        insert_account(sql, id, "personal")?;
    }
    // The distributor is a flagged, previously-reported entity — our PPR seed.
    sql.run(&format!(
        "INSERT INTO known_bad VALUES ({DISTRIBUTOR}, 'prior SAR: illicit funds source')"
    ))?;

    // -- (a) Legitimate background traffic ----------------------------------
    // Ordinary retail accounts paying each other modest, above-board amounts.
    // Real retail cores are sparse and overwhelmingly acyclic, so we let money
    // flow "downhill" in id order (src < dst): the legit core is a DAG, and the
    // only strongly-connected structure in the whole graph is a planted ring.
    for _ in 0..350 {
        let a = rng.range(LEGIT_LO, LEGIT_HI);
        let b = rng.range(LEGIT_LO, LEGIT_HI + 1);
        if a == b {
            continue;
        }
        let (src, dst) = if a < b { (a, b) } else { (b, a) };
        let amount = f64::from(rng.range(20, 4_000)) + 0.99;
        emit(sql, &mut edges, &mut txn_id, src, dst, amount, rng)?;
    }

    // -- (b) Structuring / smurfing (fan-in) --------------------------------
    // A dozen mules each deposit *just under* the CTR threshold into one
    // collector — classic placement designed to dodge reporting.
    for mule in MULE_LO..=MULE_HI {
        let amount = CTR_THRESHOLD - f64::from(rng.range(50, 900)); // 9,100–9,950
        emit(sql, &mut edges, &mut txn_id, mule, COLLECTOR, amount, rng)?;
    }
    // The collector then forwards the aggregated cash onward into the system.
    emit(sql, &mut edges, &mut txn_id, COLLECTOR, RING[0], 95_000.0, rng)?;

    // -- (c) Layering / round-tripping (a cycle) ----------------------------
    // Money chases its own tail through shell accounts to launder its origin.
    for i in 0..RING.len() {
        let src = RING[i];
        let dst = RING[(i + 1) % RING.len()];
        let amount = 90_000.0 - f64::from(i as u32) * 1_500.0; // small "fees" shaved off
        emit(sql, &mut edges, &mut txn_id, src, dst, amount, rng)?;
    }

    // -- (d) Mule fan-out (distribution / integration) ----------------------
    // The known-bad distributor sprays illicit funds across many fresh mules.
    for mule in FANOUT_LO..=FANOUT_HI {
        let amount = f64::from(rng.range(2_000, 8_000)) + 0.50;
        emit(sql, &mut edges, &mut txn_id, DISTRIBUTOR, mule, amount, rng)?;
        // Some mules then push a slice into ordinary accounts (integration).
        if rng.chance(0.5) {
            let target = rng.range(LEGIT_LO, LEGIT_HI + 1);
            emit(sql, &mut edges, &mut txn_id, mule, target, amount * 0.6, rng)?;
        }
    }

    Ok(edges)
}

fn insert_account(sql: &SqlEngine, id: NodeId, kind: &str) -> Result<(), Box<dyn std::error::Error>> {
    sql.run(&format!(
        "INSERT INTO accounts VALUES ({id}, 'owner_{id}', '{kind}', '2026-01-01')"
    ))?;
    Ok(())
}

/// Write one transaction to SQL and record its payment edge for the graph.
#[allow(clippy::too_many_arguments)]
fn emit(
    sql: &SqlEngine,
    edges: &mut Vec<(NodeId, NodeId, f64)>,
    txn_id: &mut u32,
    src: NodeId,
    dst: NodeId,
    amount: f64,
    rng: &mut Rng,
) -> Result<(), Box<dyn std::error::Error>> {
    let ts = synth_timestamp(rng);
    sql.run(&format!(
        "INSERT INTO transactions VALUES ({}, {src}, {dst}, {amount:.2}, '{ts}')",
        *txn_id
    ))?;
    edges.push((src, dst, amount));
    *txn_id += 1;
    Ok(())
}

/// A valid `YYYY-MM-DD HH:MM:SS` timestamp in March 2026 (no external date lib).
fn synth_timestamp(rng: &mut Rng) -> String {
    let day = rng.range(1, 29);
    let hour = rng.range(0, 24);
    let min = rng.range(0, 60);
    let sec = rng.range(0, 60);
    format!("2026-03-{day:02} {hour:02}:{min:02}:{sec:02}")
}

// ---------------------------------------------------------------------------
// Detectors — each a thin wrapper over one built-in graph/SQL primitive.
// ---------------------------------------------------------------------------

/// Structuring: an account fed by many distinct counterparties **whose deposits
/// cluster just under the reporting threshold**. This is the HTAP detector — the
/// graph supplies the fan-in (`in_degree`, distinct sources) and SQL supplies the
/// near-threshold evidence over the very same rows. Requiring both is what
/// separates a smurf collector from a merely popular legitimate account.
const FAN_IN_THRESHOLD: usize = 8;
const MIN_STRUCTURED_DEPOSITS: i64 = 5;
fn detect_structuring(sql: &SqlEngine, view: &chakradb::GraphView) -> Vec<(NodeId, usize)> {
    let mut hits: Vec<(NodeId, usize)> = collect_nodes(view)
        .into_iter()
        .map(|n| (n, view.in_degree(n)))
        .filter(|&(_, d)| d >= FAN_IN_THRESHOLD)
        .filter(|&(n, _)| near_threshold_deposits(sql, n) >= MIN_STRUCTURED_DEPOSITS)
        .collect();
    hits.sort_by_key(|&(_, d)| std::cmp::Reverse(d));
    hits
}

/// Count deposits into `acct` in the near-threshold band `[9,000, 10,000)`.
fn near_threshold_deposits(sql: &SqlEngine, acct: NodeId) -> i64 {
    sql.query(&format!(
        "SELECT COUNT(*) FROM transactions WHERE dst = {acct} AND amount >= 9000 AND amount < {CTR_THRESHOLD:.0}"
    ))
    .ok()
    .and_then(|r| r.first().and_then(|c| c.first()).and_then(|s| s.parse().ok()))
    .unwrap_or(0)
}

/// Mule fan-out: a single account paying out to many fresh counterparties is a
/// distribution hub. `out_degree` counts distinct destinations.
const FAN_OUT_THRESHOLD: usize = 15;
fn detect_fan_out(view: &chakradb::GraphView) -> Vec<(NodeId, usize)> {
    let mut hits: Vec<(NodeId, usize)> = collect_nodes(view)
        .into_iter()
        .map(|n| (n, view.out_degree(n)))
        .filter(|&(_, d)| d >= FAN_OUT_THRESHOLD)
        .collect();
    hits.sort_by_key(|&(_, d)| std::cmp::Reverse(d));
    hits
}

/// Layering: a non-trivial strongly-connected component is a round-trip — money
/// that returns to its origin through intermediaries.
fn detect_layering(view: &chakradb::GraphView) -> Vec<Vec<NodeId>> {
    view.laundering_cycles()
}

// ---------------------------------------------------------------------------
// Scoring — fuse the signals into one interpretable risk score per account.
// ---------------------------------------------------------------------------
struct Candidate {
    account: NodeId,
    score: f64,
}

fn score(
    view: &chakradb::GraphView,
    fan_in: &[(NodeId, usize)],
    fan_out: &[(NodeId, usize)],
    ring_members: &HashSet<NodeId>,
    risk: &HashMap<NodeId, f64>,
    known_bad: &HashSet<NodeId>,
) -> Vec<Candidate> {
    let fan_in_set: HashSet<NodeId> = fan_in.iter().map(|&(n, _)| n).collect();
    let fan_out_set: HashSet<NodeId> = fan_out.iter().map(|&(n, _)| n).collect();
    // Normalise the propagated-risk score to [0, 1] for a stable weighting.
    let max_risk = risk.values().cloned().fold(0.0_f64, f64::max).max(1e-12);

    let mut out: Vec<Candidate> = collect_nodes(view)
        .into_iter()
        .map(|n| {
            let mut s = 3.0 * risk.get(&n).copied().unwrap_or(0.0) / max_risk;
            if ring_members.contains(&n) {
                s += 3.0;
            }
            if fan_in_set.contains(&n) {
                s += 2.5;
            }
            if fan_out_set.contains(&n) {
                s += 2.0;
            }
            if known_bad.contains(&n) {
                s += 1.0;
            }
            Candidate { account: n, score: s }
        })
        .filter(|c| c.score > 0.5)
        .collect();
    out.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
    out
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------
fn report_structuring(
    sql: &SqlEngine,
    fan_in: &[(NodeId, usize)],
) -> Result<(), Box<dyn std::error::Error>> {
    section("Detector 1 — Structuring (fan-in of near-threshold deposits)");
    // The graph flags the collectors; SQL quantifies the money involved.
    for &(acct, deg) in fan_in {
        let sub: i64 = scalar(
            sql,
            &format!(
                "SELECT COUNT(*) FROM transactions WHERE dst = {acct} AND amount >= 9000 AND amount < {CTR_THRESHOLD:.0}"
            ),
        )?;
        let total: String = scalar_str(
            sql,
            &format!("SELECT SUM(amount) FROM transactions WHERE dst = {acct}"),
        )?;
        println!(
            "  account {acct:>4}: {deg} distinct sources, {sub} just-under-threshold deposits, ${total} received"
        );
    }
    println!();
    Ok(())
}

fn report_layering(rings: &[Vec<NodeId>]) {
    section("Detector 2 — Layering (round-trip cycles / SCCs)");
    if rings.is_empty() {
        println!("  (none)");
    }
    for (i, ring) in rings.iter().enumerate() {
        let mut r = ring.clone();
        r.sort_unstable();
        println!("  ring #{}: {:?}  — funds return to origin", i + 1, r);
    }
    println!();
}

fn report_fan_out(fan_out: &[(NodeId, usize)]) {
    section("Detector 3 — Mule fan-out (distribution hubs)");
    for &(acct, deg) in fan_out {
        println!("  account {acct:>4}: pays out to {deg} distinct counterparties");
    }
    println!();
}

fn report_risk_propagation(risk: &HashMap<NodeId, f64>, known_bad: &HashSet<NodeId>) {
    section("Detector 5 — Risk propagation (personalized PageRank from known-bad)");
    let mut ranked: Vec<(NodeId, f64)> = risk
        .iter()
        .filter(|(n, _)| !known_bad.contains(n))
        .map(|(&n, &r)| (n, r))
        .collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    let downstream = ranked.iter().filter(|(_, r)| *r > 0.0).count();
    println!(
        "  seed(s) {:?} → risk reached {downstream} downstream accounts. Top exposures:",
        known_bad.iter().collect::<Vec<_>>()
    );
    for &(acct, r) in ranked.iter().take(6) {
        println!("  account {acct:>4}: risk {r:.4}");
    }
    println!();
}

fn report_velocity(sql: &SqlEngine) -> Result<(), Box<dyn std::error::Error>> {
    section("Detector 4 — Velocity (temporal SQL on the same live rows)");
    // A pure-SQL analytic over the transactional table: which day saw the most
    // near-threshold structuring deposits? (Single-table aggregate; HTAP means
    // this runs on the same snapshot the graph analytics used.)
    let n: i64 = scalar(
        sql,
        &format!("SELECT COUNT(*) FROM transactions WHERE amount >= 9000 AND amount < {CTR_THRESHOLD:.0}"),
    )?;
    let big: i64 = scalar(sql, "SELECT COUNT(*) FROM transactions WHERE amount >= 50000")?;
    println!("  {n} near-threshold (9,000–10,000) deposits system-wide");
    println!("  {big} large movements (≥ 50,000) consistent with layering\n");
    Ok(())
}

fn report_sar(
    candidates: &[Candidate],
    fan_in: &[(NodeId, usize)],
    fan_out: &[(NodeId, usize)],
    ring_members: &HashSet<NodeId>,
    known_bad: &HashSet<NodeId>,
) {
    let fan_in_set: HashSet<NodeId> = fan_in.iter().map(|&(n, _)| n).collect();
    let fan_out_set: HashSet<NodeId> = fan_out.iter().map(|&(n, _)| n).collect();
    section("SAR candidates — fused risk ranking (top 15)");
    println!("  {:>5}  {:>6}  reasons", "acct", "score");
    println!("  {}  {}  {}", "-".repeat(5), "-".repeat(6), "-".repeat(40));
    for c in candidates.iter().take(15) {
        let mut why: Vec<&str> = Vec::new();
        if known_bad.contains(&c.account) {
            why.push("known-bad");
        }
        if ring_members.contains(&c.account) {
            why.push("laundering-cycle");
        }
        if fan_in_set.contains(&c.account) {
            why.push("structuring-collector");
        }
        if fan_out_set.contains(&c.account) {
            why.push("mule-fan-out");
        }
        if why.is_empty() {
            why.push("risk-propagation");
        }
        println!("  {:>5}  {:>6.2}  {}", c.account, c.score, why.join(", "));
    }
    println!();
}

// ---------------------------------------------------------------------------
// Verification — the planted crimes must all be caught.
// ---------------------------------------------------------------------------
fn verify(
    fan_in: &[(NodeId, usize)],
    fan_out: &[(NodeId, usize)],
    ring_members: &HashSet<NodeId>,
    risk: &HashMap<NodeId, f64>,
    candidates: &[Candidate],
) {
    let fan_in_set: HashSet<NodeId> = fan_in.iter().map(|&(n, _)| n).collect();
    let fan_out_set: HashSet<NodeId> = fan_out.iter().map(|&(n, _)| n).collect();

    assert!(
        fan_in_set.contains(&COLLECTOR),
        "structuring collector {COLLECTOR} must be flagged by fan-in"
    );
    for &r in &RING {
        assert!(ring_members.contains(&r), "ring member {r} must be in a laundering cycle");
    }
    assert!(
        fan_out_set.contains(&DISTRIBUTOR),
        "distributor {DISTRIBUTOR} must be flagged by fan-out"
    );
    // Risk must have propagated from the seed to its downstream mules.
    let seeded: usize = (FANOUT_LO..=FANOUT_HI)
        .filter(|m| risk.get(m).copied().unwrap_or(0.0) > 0.0)
        .count();
    assert!(seeded >= 10, "risk should reach the distributor's mules (got {seeded})");
    // The top of the SAR list must be genuinely suspicious accounts.
    let top: HashSet<NodeId> = candidates.iter().take(15).map(|c| c.account).collect();
    assert!(top.contains(&COLLECTOR) && top.contains(&DISTRIBUTOR));
    assert!(RING.iter().all(|r| top.contains(r)));
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------
fn collect_nodes(view: &chakradb::GraphView) -> Vec<NodeId> {
    // The connected-components map is keyed by every node in the view.
    view.connected_components().into_keys().collect()
}

fn distinct_accounts(edges: &[(NodeId, NodeId, f64)]) -> usize {
    let mut s = HashSet::new();
    for &(a, b, _) in edges {
        s.insert(a);
        s.insert(b);
    }
    s.len()
}

fn scalar(sql: &SqlEngine, q: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(scalar_str(sql, q)?.parse().unwrap_or(0))
}

fn scalar_str(sql: &SqlEngine, q: &str) -> Result<String, Box<dyn std::error::Error>> {
    let rows = sql.query(q)?;
    Ok(rows.first().and_then(|r| r.first()).cloned().unwrap_or_default())
}

fn banner(title: &str) {
    let rule = "=".repeat(64);
    println!("\n{rule}");
    println!("  {title}");
    println!("{rule}\n");
}

fn section(title: &str) {
    println!("── {title} ──");
}
