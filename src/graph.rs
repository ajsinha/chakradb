//! Graph capabilities — a property-graph layer over ChakraDB tables.
//!
//! This is the "built-in graph" half of ChakraDB's HTAP + graph positioning. It
//! leans on three engine properties rather than adding a separate store:
//!
//! - **Clustered adjacency for free.** An edge's key encodes `(src, dst)`
//!   src-major, so a node's out-edges are one contiguous key range and
//!   [`Table::scan_key_range`] prunes to just the parts that hold them.
//! - **Live analytics.** [`Graph::view`] builds a CSR over one MVCC snapshot, so
//!   an algorithm sees a consistent graph while writers keep adding edges — the
//!   concurrency wedge applied to graphs.
//! - **Arrow ≈ CSR.** Edges stored sorted by `src` are already grouped by source,
//!   so building the CSR is a single linear scan.
//!
//! See `docs/graph-exploration.md` for the design and roadmap. v1 stores directed
//! edges in one table; node ids are `u32` (< 2^31 for the packed-key encoding).

use crate::error::Result;
use crate::schema::{ColumnDef, Row, Schema};
use crate::sql::backend::SqlBackend;
use crate::value::{DataType, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// A graph node identifier. Must be `< 2^31` for the packed edge-key encoding
/// (a native composite key will lift this — see the design doc).
pub type NodeId = u32;

/// Column layout of the edges table: `key` (packed `(src,dst)`, the primary key),
/// `src`, `dst`, `weight`.
const KEY: usize = 0;
const SRC: usize = 1;
const DST: usize = 2;
const WEIGHT: usize = 3;

/// A directed property graph backed by a ChakraDB edges table. Writes go through
/// the normal transactional path; whole-graph algorithms run over a consistent
/// snapshot via [`Graph::view`].
pub struct Graph {
    backend: Arc<dyn SqlBackend>,
    edges: String,
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph").field("edges", &self.edges).finish()
    }
}

impl Graph {
    /// Open (creating if absent) the graph named `name`, backed by a `{name}_edges`
    /// table on `backend` (an in-memory `Database` or a durable `Storage`).
    pub fn open(backend: Arc<dyn SqlBackend>, name: &str) -> Result<Graph> {
        let edges = format!("{name}_edges");
        if backend.table(&edges).is_err() {
            let schema = Schema::from_user_columns(
                vec![
                    ColumnDef::new("key", DataType::Int),
                    ColumnDef::new("src", DataType::Int),
                    ColumnDef::new("dst", DataType::Int),
                    ColumnDef::new("weight", DataType::Float),
                ],
                Some(KEY),
            );
            backend.create_table(&edges, schema)?;
        }
        Ok(Graph { backend, edges })
    }

    /// Pack `(src, dst)` into a single key that sorts src-major, then dst.
    #[inline]
    fn encode(src: NodeId, dst: NodeId) -> i64 {
        ((src as i64) << 32) | (dst as i64)
    }

    /// Add (or replace) a directed edge `src -> dst` with a weight.
    pub fn add_edge(&self, src: NodeId, dst: NodeId, weight: f64) -> Result<()> {
        let row = Row::from_values(vec![
            Value::Int(Self::encode(src, dst)),
            Value::Int(src as i64),
            Value::Int(dst as i64),
            Value::Float(weight),
        ]);
        self.backend.upsert(&self.edges, row).map(|_| ())
    }

    /// Add many edges. Convenience over [`Graph::add_edge`].
    pub fn add_edges(&self, edges: impl IntoIterator<Item = (NodeId, NodeId, f64)>) -> Result<()> {
        for (s, d, w) in edges {
            self.add_edge(s, d, w)?;
        }
        Ok(())
    }

    /// The direct out-neighbours of `node`, via a pruned key-range scan — the live
    /// adjacency lookup (does not build the whole-graph CSR).
    pub fn out_neighbors(&self, node: NodeId) -> Result<Vec<NodeId>> {
        let t = self.backend.table(&self.edges)?;
        let snap = self.backend.snapshot();
        let lo = Value::Int(Self::encode(node, 0));
        let hi = Value::Int(((node as i64) + 1) << 32);
        let rows = t.scan_key_range(&lo, &hi, snap);
        Ok(rows
            .iter()
            .filter_map(|r| r.get(DST).as_int().map(|d| d as NodeId))
            .collect())
    }

    /// Build a consistent in-memory view (CSR) of the whole graph over one MVCC
    /// snapshot. Writers may keep committing edges — the view is unaffected.
    pub fn view(&self) -> Result<GraphView> {
        let pin = self.backend.pin();
        let t = self.backend.table(&self.edges)?;
        let batch = t.scan(pin.snapshot());

        // Collect (src, dst) pairs directly from the Arrow columns.
        let mut pairs: Vec<(NodeId, NodeId)> = Vec::with_capacity(batch.len());
        for i in 0..batch.len() {
            let (s, d) = (batch.value(SRC, i).as_int(), batch.value(DST, i).as_int());
            let _ = batch.value(WEIGHT, i); // reserved for weighted algorithms
            if let (Some(s), Some(d)) = (s, d) {
                pairs.push((s as NodeId, d as NodeId));
            }
        }
        drop(pin);
        Ok(GraphView::from_edges(pairs))
    }
}

/// An immutable, consistent snapshot of a graph as CSR (compressed sparse row):
/// a dense node index, per-node out-edge offsets, and a flat neighbour array.
/// Algorithms run over this; it never changes once built.
#[derive(Debug, Clone)]
pub struct GraphView {
    /// Dense index -> node id.
    ids: Vec<NodeId>,
    /// Node id -> dense index.
    index: HashMap<NodeId, u32>,
    /// CSR out-edge offsets, `len == n + 1`.
    offsets: Vec<u32>,
    /// CSR out-neighbours (dense indices), grouped by source.
    adj: Vec<u32>,
    /// Reverse CSR: offsets and in-neighbours grouped by destination. Needed for
    /// backward traversal, fan-in detection, and strongly-connected components.
    in_offsets: Vec<u32>,
    in_adj: Vec<u32>,
}

impl GraphView {
    fn from_edges(pairs: Vec<(NodeId, NodeId)>) -> GraphView {
        // Dense node numbering over every id that appears as a source or target.
        let mut ids: Vec<NodeId> = pairs.iter().flat_map(|&(s, d)| [s, d]).collect();
        ids.sort_unstable();
        ids.dedup();
        let index: HashMap<NodeId, u32> =
            ids.iter().enumerate().map(|(i, &id)| (id, i as u32)).collect();
        let n = ids.len();

        // CSR via counting sort on the source's dense index.
        let mut offsets = vec![0u32; n + 1];
        for &(s, _) in &pairs {
            offsets[index[&s] as usize + 1] += 1;
        }
        for i in 0..n {
            offsets[i + 1] += offsets[i];
        }
        let mut adj = vec![0u32; pairs.len()];
        let mut cursor = offsets.clone();
        for &(s, d) in &pairs {
            let si = index[&s] as usize;
            adj[cursor[si] as usize] = index[&d];
            cursor[si] += 1;
        }

        // Reverse CSR (grouped by destination) — the same counting sort on `dst`.
        let mut in_offsets = vec![0u32; n + 1];
        for &(_, d) in &pairs {
            in_offsets[index[&d] as usize + 1] += 1;
        }
        for i in 0..n {
            in_offsets[i + 1] += in_offsets[i];
        }
        let mut in_adj = vec![0u32; pairs.len()];
        let mut in_cursor = in_offsets.clone();
        for &(s, d) in &pairs {
            let di = index[&d] as usize;
            in_adj[in_cursor[di] as usize] = index[&s];
            in_cursor[di] += 1;
        }

        GraphView {
            ids,
            index,
            offsets,
            adj,
            in_offsets,
            in_adj,
        }
    }

    /// Number of distinct nodes.
    pub fn node_count(&self) -> usize {
        self.ids.len()
    }
    /// Number of directed edges.
    pub fn edge_count(&self) -> usize {
        self.adj.len()
    }

    #[inline]
    fn neighbors(&self, dense: u32) -> &[u32] {
        let (a, b) = (self.offsets[dense as usize], self.offsets[dense as usize + 1]);
        &self.adj[a as usize..b as usize]
    }

    /// Out-degree of `node` (0 if the node is unknown or a pure sink).
    pub fn out_degree(&self, node: NodeId) -> usize {
        self.index.get(&node).map_or(0, |&i| self.neighbors(i).len())
    }

    #[inline]
    fn in_neighbors_dense(&self, dense: u32) -> &[u32] {
        let (a, b) = (
            self.in_offsets[dense as usize],
            self.in_offsets[dense as usize + 1],
        );
        &self.in_adj[a as usize..b as usize]
    }

    /// In-degree of `node` — the number of edges pointing *at* it. In an
    /// AML entity/flow graph this is the **fan-in**: a high in-degree with many
    /// small incoming transfers is the signature of structuring / smurfing.
    pub fn in_degree(&self, node: NodeId) -> usize {
        self.index
            .get(&node)
            .map_or(0, |&i| self.in_neighbors_dense(i).len())
    }

    /// The direct in-neighbours of `node` (sources of edges into it).
    pub fn in_neighbors(&self, node: NodeId) -> Vec<NodeId> {
        self.index.get(&node).map_or_else(Vec::new, |&i| {
            self.in_neighbors_dense(i)
                .iter()
                .map(|&d| self.ids[d as usize])
                .collect()
        })
    }

    /// Breadth-first shortest-hop distances from `start`, following out-edges.
    /// Returns `node -> hop distance`; unreachable nodes are absent.
    pub fn bfs(&self, start: NodeId) -> HashMap<NodeId, u32> {
        let mut dist = HashMap::new();
        let Some(&s) = self.index.get(&start) else {
            return dist;
        };
        let mut depth = vec![u32::MAX; self.node_count()];
        depth[s as usize] = 0;
        let mut queue = std::collections::VecDeque::from([s]);
        while let Some(u) = queue.pop_front() {
            dist.insert(self.ids[u as usize], depth[u as usize]);
            for &v in self.neighbors(u) {
                if depth[v as usize] == u32::MAX {
                    depth[v as usize] = depth[u as usize] + 1;
                    queue.push_back(v);
                }
            }
        }
        dist
    }

    /// A shortest (fewest-hops) path `from -> to` along out-edges, or `None` if
    /// unreachable. Includes both endpoints.
    pub fn shortest_path(&self, from: NodeId, to: NodeId) -> Option<Vec<NodeId>> {
        let (&s, &t) = (self.index.get(&from)?, self.index.get(&to)?);
        if s == t {
            return Some(vec![from]);
        }
        let mut parent = vec![u32::MAX; self.node_count()];
        parent[s as usize] = s; // mark visited (self-parent as sentinel)
        let mut queue = std::collections::VecDeque::from([s]);
        while let Some(u) = queue.pop_front() {
            for &v in self.neighbors(u) {
                if parent[v as usize] == u32::MAX {
                    parent[v as usize] = u;
                    if v == t {
                        // Walk parents back to the source.
                        let mut path = vec![v];
                        let mut cur = v;
                        while cur != s {
                            cur = parent[cur as usize];
                            path.push(cur);
                        }
                        path.reverse();
                        return Some(path.into_iter().map(|i| self.ids[i as usize]).collect());
                    }
                    queue.push_back(v);
                }
            }
        }
        None
    }

    /// PageRank over the directed graph. `damping` is typically 0.85; `iterations`
    /// power-iteration steps (e.g. 20). Returns `node -> score`, summing to ~1.
    /// Dangling nodes (no out-edges) redistribute their mass uniformly.
    pub fn pagerank(&self, iterations: usize, damping: f64) -> HashMap<NodeId, f64> {
        let n = self.node_count();
        if n == 0 {
            return HashMap::new();
        }
        let nf = n as f64;
        let mut rank = vec![1.0 / nf; n];
        for _ in 0..iterations {
            let dangling: f64 = (0..n)
                .filter(|&u| self.neighbors(u as u32).is_empty())
                .map(|u| rank[u])
                .sum();
            let base = (1.0 - damping) / nf + damping * dangling / nf;
            let mut next = vec![base; n];
            for (u, &rank_u) in rank.iter().enumerate() {
                let out = self.neighbors(u as u32);
                if !out.is_empty() {
                    let share = damping * rank_u / out.len() as f64;
                    for &v in out {
                        next[v as usize] += share;
                    }
                }
            }
            rank = next;
        }
        self.ids.iter().copied().zip(rank).collect()
    }

    /// Weakly-connected components (edges treated as undirected). Returns
    /// `node -> component id` (component ids are small integers `0..k`).
    pub fn connected_components(&self) -> HashMap<NodeId, u32> {
        let n = self.node_count();
        let mut uf: Vec<u32> = (0..n as u32).collect();
        fn find(uf: &mut [u32], mut x: u32) -> u32 {
            while uf[x as usize] != x {
                uf[x as usize] = uf[uf[x as usize] as usize]; // path halving
                x = uf[x as usize];
            }
            x
        }
        for u in 0..n {
            for &v in self.neighbors(u as u32) {
                let (ru, rv) = (find(&mut uf, u as u32), find(&mut uf, v));
                if ru != rv {
                    uf[ru as usize] = rv;
                }
            }
        }
        // Relabel roots to dense 0..k.
        let mut label = HashMap::new();
        let mut out = HashMap::with_capacity(n);
        for i in 0..n {
            let root = find(&mut uf, i as u32);
            let next = label.len() as u32;
            let comp = *label.entry(root).or_insert(next);
            out.insert(self.ids[i], comp);
        }
        out
    }

    /// The number of triangles in the undirected graph (each counted once).
    pub fn triangle_count(&self) -> u64 {
        let n = self.node_count();
        // Undirected adjacency as sorted, de-duplicated dense-index lists.
        let mut adj: Vec<Vec<u32>> = vec![Vec::new(); n];
        for u in 0..n {
            for &v in self.neighbors(u as u32) {
                if u as u32 != v {
                    adj[u].push(v);
                    adj[v as usize].push(u as u32);
                }
            }
        }
        for a in adj.iter_mut() {
            a.sort_unstable();
            a.dedup();
        }
        // Count triangles (u < v < w) via sorted intersection over w > v.
        let mut total = 0u64;
        for u in 0..n as u32 {
            for &v in &adj[u as usize] {
                if v <= u {
                    continue;
                }
                total += intersect_above(&adj[u as usize], &adj[v as usize], v);
            }
        }
        total
    }

    /// Strongly-connected components (Kosaraju): maximal sets of nodes each
    /// reachable from every other. Directed **cycles** — money that can return to
    /// its origin through a chain of transfers — are exactly the SCCs of size > 1
    /// (or a self-loop). This is the core of round-tripping / layering detection.
    pub fn strongly_connected_components(&self) -> Vec<Vec<NodeId>> {
        let n = self.node_count();
        // Pass 1 — iterative post-order DFS on forward edges to get a finish order.
        let mut visited = vec![false; n];
        let mut order = Vec::with_capacity(n);
        for s in 0..n as u32 {
            if visited[s as usize] {
                continue;
            }
            visited[s as usize] = true;
            let mut stack = vec![(s, 0usize)];
            while let Some(&mut (u, ref mut i)) = stack.last_mut() {
                let nbrs = self.neighbors(u);
                if *i < nbrs.len() {
                    let v = nbrs[*i];
                    *i += 1;
                    if !visited[v as usize] {
                        visited[v as usize] = true;
                        stack.push((v, 0));
                    }
                } else {
                    order.push(u);
                    stack.pop();
                }
            }
        }
        // Pass 2 — DFS on reverse edges in reverse finish order; each tree is an SCC.
        let mut comp = vec![u32::MAX; n];
        let mut sccs = Vec::new();
        for &s in order.iter().rev() {
            if comp[s as usize] != u32::MAX {
                continue;
            }
            let id = sccs.len() as u32;
            let mut members = Vec::new();
            let mut stack = vec![s];
            comp[s as usize] = id;
            while let Some(u) = stack.pop() {
                members.push(self.ids[u as usize]);
                for &v in self.in_neighbors_dense(u) {
                    if comp[v as usize] == u32::MAX {
                        comp[v as usize] = id;
                        stack.push(v);
                    }
                }
            }
            sccs.push(members);
        }
        sccs
    }

    /// The **laundering cycles**: strongly-connected components of size ≥ 2 — sets
    /// of accounts among which funds can circulate back to any starting point.
    /// A non-empty result is direct evidence of round-tripping / layering.
    pub fn laundering_cycles(&self) -> Vec<Vec<NodeId>> {
        self.strongly_connected_components()
            .into_iter()
            .filter(|c| c.len() >= 2)
            .collect()
    }

    /// **Personalized PageRank** — PageRank whose teleport mass concentrates on a
    /// `seeds` set instead of spreading uniformly. Seeded with known-bad accounts,
    /// it propagates *risk* through the transaction graph: an account's score is
    /// its proximity/exposure to the seeds. Returns `node -> score`.
    pub fn personalized_pagerank(
        &self,
        seeds: &[NodeId],
        iterations: usize,
        damping: f64,
    ) -> HashMap<NodeId, f64> {
        let n = self.node_count();
        let seed_idx: Vec<usize> = seeds
            .iter()
            .filter_map(|s| self.index.get(s).map(|&i| i as usize))
            .collect();
        if n == 0 || seed_idx.is_empty() {
            return HashMap::new();
        }
        let mut teleport = vec![0.0f64; n];
        let share = 1.0 / seed_idx.len() as f64;
        for &i in &seed_idx {
            teleport[i] = share;
        }
        let mut rank = teleport.clone();
        for _ in 0..iterations {
            // Dangling mass (nodes with no out-edge) teleports back to the seeds.
            let dangling: f64 = (0..n)
                .filter(|&u| self.neighbors(u as u32).is_empty())
                .map(|u| rank[u])
                .sum();
            let mut next = vec![0.0f64; n];
            for (u, &t) in teleport.iter().enumerate() {
                next[u] = (1.0 - damping) * t + damping * dangling * t;
            }
            for (u, &rank_u) in rank.iter().enumerate() {
                let out = self.neighbors(u as u32);
                if !out.is_empty() {
                    let contrib = damping * rank_u / out.len() as f64;
                    for &v in out {
                        next[v as usize] += contrib;
                    }
                }
            }
            rank = next;
        }
        self.ids.iter().copied().zip(rank).collect()
    }
}

/// Count common elements of two sorted slices that are strictly greater than `min`.
fn intersect_above(a: &[u32], b: &[u32], min: u32) -> u64 {
    let (mut i, mut j, mut count) = (0, 0, 0u64);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                if a[i] > min {
                    count += 1;
                }
                i += 1;
                j += 1;
            }
        }
    }
    count
}
