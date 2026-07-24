//! Real-time Recommendations on ChakraDB
//! =====================================
//!
//! The third flagship — and a deliberately *different* domain from the AML and
//! risk pipelines. A stream of user interactions (views, clicks, purchases)
//! drives a **live recommendation engine**: as engagement arrives, a materialized
//! worker maintains the user↔item graph and answers "what should this user see
//! next?" in real time — the personalization workload that usually needs a
//! separate feature store, a nightly model train, and a serving tier.
//!
//! It showcases the graph library's link-prediction family:
//!
//!   * **`recommend(user, k)`** — collaborative filtering as a graph walk
//!     (random-walk-with-restart / personalized PageRank): items reached through
//!     similar users that the user has not engaged with yet.
//!   * **`adamic_adar`** — "you might also like": item–item similarity weighted by
//!     the rarity of the users who engage with both.
//!   * **degree / PageRank** — what's trending.
//!
//! All over one consistent `Graph::view()` snapshot, reacting to the interaction
//! stream, never blocking ingest. Run it:
//!
//! ```text
//! cargo run --release --example reco_pipeline --no-default-features
//! ```

use chakradb::cdc::{Cdc, CdcBackend, Change, ChangeOp, MaterializedWorker};
use chakradb::{Database, Graph, NodeId, SqlEngine};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// Item node ids live above the user id range so the two never collide.
const ITEM_BASE: NodeId = 1_000_000;
const N_USERS: NodeId = 5_000;
const N_ITEMS: NodeId = 400;
const N_INTERACTIONS: u64 = 50_000;
const T2_INTERVAL: u64 = 12_000;

// The injected signal (the "typology" analogue): a cohort that co-engages with a
// bundle, and a target user who has seen all but one of it.
const COHORT: std::ops::RangeInclusive<NodeId> = 1..=12; // power users
const TARGET: NodeId = 13; // we recommend for this user
const BUNDLE: [NodeId; 3] = [100, 101, 102]; // raw item ids the cohort co-engages
const TRENDING: NodeId = 1; // raw item id nearly everyone touches

fn item_node(raw: NodeId) -> NodeId {
    ITEM_BASE + raw
}

struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, lo: NodeId, hi: NodeId) -> NodeId {
        lo + (self.next_u64() % u64::from(hi - lo)) as NodeId
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64) < p
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rule = "=".repeat(64);
    println!("\n{rule}\n  ChakraDB — Real-Time Recommendations\n{rule}\n");

    let db = Arc::new(Database::new());
    let cdc = Cdc::new();
    let engine = Arc::new(SqlEngine::with_backend(CdcBackend::wrap(db.clone(), cdc.clone())));
    engine.run(
        "CREATE TABLE interactions (id INTEGER PRIMARY KEY, user_id INTEGER,
            item_id INTEGER, kind VARCHAR(8), ts TIMESTAMP)",
    )?;
    engine.run("CREATE TABLE recommendations (id INTEGER PRIMARY KEY, user_id INTEGER, item_id INTEGER)")?;

    let engagement = Graph::open(db.clone(), "engagement")?;
    let reco = cdc.register("recommender", Some("interactions"), Recommender::new(engine.clone(), engagement));

    // --- Stream the interaction feed --------------------------------------
    let produced = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    {
        let mut rng = Rng(0x00A4_71CE_2026);
        let mut id = 1u64;
        let mut emit = |user: NodeId, item_raw: NodeId, rng: &mut Rng| {
            let ts = format!("2026-03-{:02} 12:00:00", rng.range(1, 29));
            engine
                .run(&format!(
                    "INSERT INTO interactions VALUES ({id}, {user}, {item_raw}, 'view', '{ts}')"
                ))
                .unwrap();
            id += 1;
            produced.fetch_add(1, Ordering::Relaxed);
        };

        // Injected co-engagement: the cohort engages the whole bundle; the target
        // engages all but the last item (which we expect to be recommended).
        for u in COHORT {
            for &b in &BUNDLE {
                emit(u, b, &mut rng);
            }
        }
        emit(TARGET, BUNDLE[0], &mut rng);
        emit(TARGET, BUNDLE[1], &mut rng);

        // A trending item nearly everyone touches.
        for u in 1..=N_USERS {
            if rng.chance(0.4) {
                emit(u, TRENDING, &mut rng);
            }
        }

        // Background engagement: random users on random items.
        while produced.load(Ordering::Relaxed) < N_INTERACTIONS {
            let u = rng.range(1, N_USERS + 1);
            let i = rng.range(2, N_ITEMS); // avoid TRENDING(1)/bundle domination
            emit(u, i, &mut rng);
        }
    }
    let ingest = start.elapsed();
    let total = produced.load(Ordering::Relaxed);

    // Drain, stop, final recommendation pass.
    let deadline = Instant::now() + Duration::from_secs(30);
    while reco.query(|w| w.seen) < total && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    reco.stop();
    reco.update(|w| w.refresh());

    let rate = total as f64 / ingest.as_secs_f64();
    println!("Ingest: {total} interactions in {:.2}s", ingest.as_secs_f64());
    println!(
        "        = {:.0} interactions/s  ≈  {:.1} million/hour\n",
        rate,
        rate * 3600.0 / 1_000_000.0
    );

    let (recs, trending, similar) = reco.query(|w| {
        (w.last_recommendations.clone(), w.trending.clone(), w.bundle_similarity.clone())
    });
    println!("── Recommendations for user {TARGET} (items not yet engaged) ──");
    for (item, score) in recs.iter().take(5) {
        let tag = if *item == BUNDLE[2] { "  ← the co-engaged item the cohort completes" } else { "" };
        println!("  item {item:>4}  score {score:.4}{tag}");
    }
    println!("\n── Trending items (most-engaged) ──");
    for (item, deg) in trending.iter().take(5) {
        println!("  item {item:>4}  {deg} distinct users");
    }
    println!("\n── \"You might also like\" — Adamic–Adar similarity to bundle item {} ──", BUNDLE[0]);
    for (item, score) in similar.iter().take(3) {
        println!("  item {item:>4}  similarity {score:.3}");
    }
    println!();

    // --- Self-check -------------------------------------------------------
    assert!(
        recs.iter().any(|(i, _)| *i == BUNDLE[2]),
        "the cohort's third bundle item ({}) should be recommended to the target",
        BUNDLE[2]
    );
    assert!(
        !recs.iter().any(|(i, _)| *i == BUNDLE[0] || *i == BUNDLE[1]),
        "already-engaged items are not recommended"
    );
    assert_eq!(trending.first().map(|(i, _)| *i), Some(TRENDING), "the trending item ranks first");
    assert!(
        reco.query(|w| w.view_adamic(BUNDLE[0], BUNDLE[2]) > w.view_adamic(BUNDLE[0], 250)),
        "the co-engaged item is more similar than an unrelated one"
    );

    println!("{rule}\n  Recommendations verified — the cohort's item surfaced live.\n{rule}\n");
    Ok(())
}

// ---------------------------------------------------------------------------
// The recommender: maintains the user↔item graph; refreshes recommendations.
// ---------------------------------------------------------------------------
struct Recommender {
    engine: Arc<SqlEngine>,
    graph: Graph,
    seen: u64,
    persisted: u64,
    last_recommendations: Vec<(NodeId, f64)>, // (raw item id, score)
    trending: Vec<(NodeId, usize)>,           // (raw item id, distinct users)
    bundle_similarity: Vec<(NodeId, f64)>,    // (raw item id, Adamic–Adar to BUNDLE[0])
}

impl Recommender {
    fn new(engine: Arc<SqlEngine>, graph: Graph) -> Self {
        Recommender {
            engine,
            graph,
            seen: 0,
            persisted: 0,
            last_recommendations: Vec::new(),
            trending: Vec::new(),
            bundle_similarity: Vec::new(),
        }
    }

    /// T0 — fold one interaction into the bipartite graph (edges both ways so a
    /// random walk can bounce user → item → user → item …).
    fn on_interaction(&mut self, user: NodeId, item_raw: NodeId) {
        let item = item_node(item_raw);
        let _ = self.graph.add_edge(user, item, 1.0);
        let _ = self.graph.add_edge(item, user, 1.0);
    }

    /// T2 — recompute recommendations, trending, and similarity over a snapshot.
    fn refresh(&mut self) {
        let view = match self.graph.view() {
            Ok(v) => v,
            Err(_) => return,
        };
        // Recommendations for the target: RWR items it hasn't engaged with.
        let mut recs: Vec<(NodeId, f64)> = view
            .recommend(TARGET, 40)
            .into_iter()
            .filter(|(n, _)| *n >= ITEM_BASE)
            .map(|(n, s)| (n - ITEM_BASE, s))
            .collect();
        recs.truncate(10);
        // Persist the top recommendations.
        for (item, _) in recs.iter().take(5) {
            self.persisted += 1;
            let _ = self.engine.run(&format!(
                "INSERT INTO recommendations VALUES ({}, {TARGET}, {item})",
                self.persisted
            ));
        }
        self.last_recommendations = recs;

        // Trending: items ranked by the number of distinct users engaging them.
        let mut trending: Vec<(NodeId, usize)> = view
            .connected_components()
            .into_keys()
            .filter(|n| *n >= ITEM_BASE)
            .map(|n| (n - ITEM_BASE, view.out_degree(n)))
            .collect();
        trending.sort_by_key(|&(_, d)| std::cmp::Reverse(d));
        trending.truncate(10);
        self.trending = trending;

        // "You might also like": Adamic–Adar similarity to the first bundle item.
        let anchor = item_node(BUNDLE[0]);
        let mut sim: Vec<(NodeId, f64)> = view
            .connected_components()
            .into_keys()
            .filter(|n| *n >= ITEM_BASE && *n != anchor)
            .map(|n| (n - ITEM_BASE, view.adamic_adar(anchor, n)))
            .filter(|(_, s)| *s > 0.0)
            .collect();
        sim.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        sim.truncate(5);
        self.bundle_similarity = sim;
    }

    /// Adamic–Adar between two raw item ids, over the current graph (for checks).
    fn view_adamic(&self, a_raw: NodeId, b_raw: NodeId) -> f64 {
        match self.graph.view() {
            Ok(v) => v.adamic_adar(item_node(a_raw), item_node(b_raw)),
            Err(_) => 0.0,
        }
    }
}

impl MaterializedWorker for Recommender {
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
            // Row: (id, user_id, item_id, kind, ts).
            self.on_interaction(as_u32(&new[1]), as_u32(&new[2]));
            self.seen += 1;
            if self.seen.is_multiple_of(T2_INTERVAL) {
                self.refresh();
            }
        }
    }
}
