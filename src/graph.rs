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

        // Collect (src, dst, weight) triples directly from the Arrow columns.
        let mut pairs: Vec<(NodeId, NodeId, f64)> = Vec::with_capacity(batch.len());
        for i in 0..batch.len() {
            let (s, d) = (batch.value(SRC, i).as_int(), batch.value(DST, i).as_int());
            let w = batch.value(WEIGHT, i).as_f64().unwrap_or(1.0);
            if let (Some(s), Some(d)) = (s, d) {
                pairs.push((s as NodeId, d as NodeId, w));
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
    /// Edge weights, parallel to `adj` (the weight of the edge to `adj[k]`).
    wadj: Vec<f64>,
    /// Reverse CSR: offsets and in-neighbours grouped by destination. Needed for
    /// backward traversal, fan-in detection, and strongly-connected components.
    in_offsets: Vec<u32>,
    in_adj: Vec<u32>,
}

impl GraphView {
    fn from_edges(pairs: Vec<(NodeId, NodeId, f64)>) -> GraphView {
        // Dense node numbering over every id that appears as a source or target.
        let mut ids: Vec<NodeId> = pairs.iter().flat_map(|&(s, d, _)| [s, d]).collect();
        ids.sort_unstable();
        ids.dedup();
        let index: HashMap<NodeId, u32> =
            ids.iter().enumerate().map(|(i, &id)| (id, i as u32)).collect();
        let n = ids.len();

        // CSR via counting sort on the source's dense index.
        let mut offsets = vec![0u32; n + 1];
        for &(s, _, _) in &pairs {
            offsets[index[&s] as usize + 1] += 1;
        }
        for i in 0..n {
            offsets[i + 1] += offsets[i];
        }
        let mut adj = vec![0u32; pairs.len()];
        let mut wadj = vec![0.0f64; pairs.len()];
        let mut cursor = offsets.clone();
        for &(s, d, w) in &pairs {
            let si = index[&s] as usize;
            let slot = cursor[si] as usize;
            adj[slot] = index[&d];
            wadj[slot] = w;
            cursor[si] += 1;
        }

        // Reverse CSR (grouped by destination) — the same counting sort on `dst`.
        let mut in_offsets = vec![0u32; n + 1];
        for &(_, d, _) in &pairs {
            in_offsets[index[&d] as usize + 1] += 1;
        }
        for i in 0..n {
            in_offsets[i + 1] += in_offsets[i];
        }
        let mut in_adj = vec![0u32; pairs.len()];
        let mut in_cursor = in_offsets.clone();
        for &(s, d, _) in &pairs {
            let di = index[&d] as usize;
            in_adj[in_cursor[di] as usize] = index[&s];
            in_cursor[di] += 1;
        }

        GraphView {
            ids,
            index,
            offsets,
            adj,
            wadj,
            in_offsets,
            in_adj,
        }
    }

    #[inline]
    fn weights(&self, dense: u32) -> &[f64] {
        let (a, b) = (self.offsets[dense as usize], self.offsets[dense as usize + 1]);
        &self.wadj[a as usize..b as usize]
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

    /// Dijkstra single-source shortest **weighted** distances from `from`, using the
    /// edge weights as non-negative costs. Unreachable nodes are absent. `O(E log V)`.
    pub fn dijkstra(&self, from: NodeId) -> HashMap<NodeId, f64> {
        use std::collections::BinaryHeap;
        let mut out = HashMap::new();
        let Some(&s) = self.index.get(&from) else {
            return out;
        };
        let n = self.node_count();
        let mut dist = vec![f64::INFINITY; n];
        dist[s as usize] = 0.0;
        let mut heap = BinaryHeap::from([HeapItem(0.0, s)]);
        while let Some(HeapItem(d, u)) = heap.pop() {
            if d > dist[u as usize] {
                continue; // a stale, longer entry
            }
            let (nbrs, ws) = (self.neighbors(u), self.weights(u));
            for (k, &v) in nbrs.iter().enumerate() {
                let nd = d + ws[k].max(0.0);
                if nd < dist[v as usize] {
                    dist[v as usize] = nd;
                    heap.push(HeapItem(nd, v));
                }
            }
        }
        for (i, &d) in dist.iter().enumerate() {
            if d.is_finite() {
                out.insert(self.ids[i], d);
            }
        }
        out
    }

    /// The minimum-cost **weighted** path `from -> to` (Dijkstra with predecessors),
    /// returning the node sequence and its total cost, or `None` if unreachable.
    pub fn weighted_shortest_path(&self, from: NodeId, to: NodeId) -> Option<(Vec<NodeId>, f64)> {
        use std::collections::BinaryHeap;
        let (&s, &t) = (self.index.get(&from)?, self.index.get(&to)?);
        let n = self.node_count();
        let mut dist = vec![f64::INFINITY; n];
        let mut prev = vec![u32::MAX; n];
        dist[s as usize] = 0.0;
        let mut heap = BinaryHeap::from([HeapItem(0.0, s)]);
        while let Some(HeapItem(d, u)) = heap.pop() {
            if u == t {
                break;
            }
            if d > dist[u as usize] {
                continue;
            }
            let (nbrs, ws) = (self.neighbors(u), self.weights(u));
            for (k, &v) in nbrs.iter().enumerate() {
                let nd = d + ws[k].max(0.0);
                if nd < dist[v as usize] {
                    dist[v as usize] = nd;
                    prev[v as usize] = u;
                    heap.push(HeapItem(nd, v));
                }
            }
        }
        if !dist[t as usize].is_finite() {
            return None;
        }
        let mut path = vec![t];
        let mut cur = t;
        while cur != s {
            cur = prev[cur as usize];
            path.push(cur);
        }
        path.reverse();
        Some((
            path.into_iter().map(|i| self.ids[i as usize]).collect(),
            dist[t as usize],
        ))
    }

    /// Degree centrality — the (total, in, out) degree of every node, normalised by
    /// `n − 1`. A quick, cheap importance signal.
    pub fn degree_centrality(&self) -> HashMap<NodeId, f64> {
        let n = self.node_count();
        let denom = (n.saturating_sub(1)).max(1) as f64;
        (0..n)
            .map(|u| {
                let deg = self.neighbors(u as u32).len() + self.in_neighbors_dense(u as u32).len();
                (self.ids[u], deg as f64 / denom)
            })
            .collect()
    }

    /// Closeness centrality — for each node, `(reachable − 1) / Σ hop-distance`.
    /// High for nodes that reach the rest of the graph in few hops. Runs a BFS per
    /// node: `O(V·(V+E))`, so it suits moderate graphs.
    pub fn closeness_centrality(&self) -> HashMap<NodeId, f64> {
        let n = self.node_count();
        let mut out = HashMap::with_capacity(n);
        let mut depth = vec![u32::MAX; n];
        let mut queue = std::collections::VecDeque::new();
        for s in 0..n as u32 {
            depth.iter_mut().for_each(|d| *d = u32::MAX);
            depth[s as usize] = 0;
            queue.clear();
            queue.push_back(s);
            let (mut sum, mut reach) = (0u64, 0u64);
            while let Some(u) = queue.pop_front() {
                for &v in self.neighbors(u) {
                    if depth[v as usize] == u32::MAX {
                        depth[v as usize] = depth[u as usize] + 1;
                        sum += depth[v as usize] as u64;
                        reach += 1;
                        queue.push_back(v);
                    }
                }
            }
            let c = if sum > 0 { reach as f64 / sum as f64 } else { 0.0 };
            out.insert(self.ids[s as usize], c);
        }
        out
    }

    /// Betweenness centrality (Brandes, unweighted) — how often a node lies on
    /// shortest paths between other nodes. The classic "bridge / broker" score.
    /// `O(V·E)`.
    pub fn betweenness_centrality(&self) -> HashMap<NodeId, f64> {
        let n = self.node_count();
        let mut bc = vec![0.0f64; n];
        for s in 0..n as u32 {
            let mut stack = Vec::new();
            let mut preds: Vec<Vec<u32>> = vec![Vec::new(); n];
            let mut sigma = vec![0.0f64; n];
            let mut dist = vec![-1i64; n];
            sigma[s as usize] = 1.0;
            dist[s as usize] = 0;
            let mut queue = std::collections::VecDeque::from([s]);
            while let Some(u) = queue.pop_front() {
                stack.push(u);
                for &v in self.neighbors(u) {
                    if dist[v as usize] < 0 {
                        dist[v as usize] = dist[u as usize] + 1;
                        queue.push_back(v);
                    }
                    if dist[v as usize] == dist[u as usize] + 1 {
                        sigma[v as usize] += sigma[u as usize];
                        preds[v as usize].push(u);
                    }
                }
            }
            let mut delta = vec![0.0f64; n];
            while let Some(w) = stack.pop() {
                for &v in &preds[w as usize] {
                    delta[v as usize] +=
                        (sigma[v as usize] / sigma[w as usize]) * (1.0 + delta[w as usize]);
                }
                if w != s {
                    bc[w as usize] += delta[w as usize];
                }
            }
        }
        self.ids.iter().copied().zip(bc).collect()
    }

    /// Label propagation — fast community detection. Each node repeatedly adopts the
    /// label most common among its (undirected) neighbours, ties broken by lowest
    /// label. Returns `node -> community label`. `O(iterations·E)`.
    pub fn label_propagation(&self, iterations: usize) -> HashMap<NodeId, u32> {
        let n = self.node_count();
        let mut label: Vec<u32> = (0..n as u32).collect();
        let mut counts: HashMap<u32, u32> = HashMap::new();
        for _ in 0..iterations {
            let mut changed = false;
            for u in 0..n as u32 {
                counts.clear();
                for &v in self
                    .neighbors(u)
                    .iter()
                    .chain(self.in_neighbors_dense(u))
                {
                    *counts.entry(label[v as usize]).or_insert(0) += 1;
                }
                if let Some((&best, _)) =
                    counts.iter().max_by_key(|&(&l, &c)| (c, std::cmp::Reverse(l)))
                {
                    if label[u as usize] != best {
                        label[u as usize] = best;
                        changed = true;
                    }
                }
            }
            if !changed {
                break; // converged
            }
        }
        self.ids.iter().copied().zip(label).collect()
    }

    /// k-core decomposition — each node's **core number**: the largest `k` such that
    /// the node belongs to a subgraph where every node has (undirected) degree ≥ `k`.
    /// A high core number marks a node embedded in a dense cluster. `O(E)`.
    pub fn k_core(&self) -> HashMap<NodeId, u32> {
        let n = self.node_count();
        // Undirected degree.
        let mut deg: Vec<u32> = (0..n)
            .map(|u| {
                let mut s: std::collections::HashSet<u32> = self.neighbors(u as u32).iter().copied().collect();
                s.extend(self.in_neighbors_dense(u as u32).iter().copied());
                s.remove(&(u as u32));
                s.len() as u32
            })
            .collect();
        // Repeatedly peel the minimum-degree node.
        let mut core = vec![0u32; n];
        let mut removed = vec![false; n];
        let mut adj: Vec<std::collections::HashSet<u32>> = (0..n)
            .map(|u| {
                let mut s: std::collections::HashSet<u32> = self.neighbors(u as u32).iter().copied().collect();
                s.extend(self.in_neighbors_dense(u as u32).iter().copied());
                s.remove(&(u as u32));
                s
            })
            .collect();
        let mut k = 0u32;
        for _ in 0..n {
            // Find the min-degree remaining node.
            let Some(u) = (0..n).filter(|&i| !removed[i]).min_by_key(|&i| deg[i]) else {
                break;
            };
            k = k.max(deg[u]);
            core[u] = k;
            removed[u] = true;
            let neigh: Vec<u32> = adj[u].iter().copied().collect();
            for v in neigh {
                let v = v as usize;
                if !removed[v] {
                    adj[v].remove(&(u as u32));
                    deg[v] = deg[v].saturating_sub(1);
                }
            }
        }
        self.ids.iter().copied().zip(core).collect()
    }

    /// A topological ordering of the nodes (Kahn's algorithm), or `None` if the
    /// graph has a cycle. Useful for dependency / flow ordering. `O(V+E)`.
    pub fn topological_order(&self) -> Option<Vec<NodeId>> {
        let n = self.node_count();
        let mut indeg: Vec<u32> = (0..n).map(|u| self.in_neighbors_dense(u as u32).len() as u32).collect();
        let mut queue: std::collections::VecDeque<u32> =
            (0..n as u32).filter(|&u| indeg[u as usize] == 0).collect();
        let mut order = Vec::with_capacity(n);
        while let Some(u) = queue.pop_front() {
            order.push(self.ids[u as usize]);
            for &v in self.neighbors(u) {
                indeg[v as usize] = indeg[v as usize].saturating_sub(1);
                if indeg[v as usize] == 0 {
                    queue.push_back(v);
                }
            }
        }
        (order.len() == n).then_some(order) // shorter ⇒ a cycle blocked it
    }

    /// The common (out-)neighbours of two nodes — a link-prediction signal
    /// ("friends in common", or accounts both send to).
    pub fn common_neighbors(&self, a: NodeId, b: NodeId) -> Vec<NodeId> {
        let (ai, bi) = match (self.index.get(&a), self.index.get(&b)) {
            (Some(&ai), Some(&bi)) => (ai, bi),
            _ => return Vec::new(),
        };
        let bset: std::collections::HashSet<u32> = self.neighbors(bi).iter().copied().collect();
        self.neighbors(ai)
            .iter()
            .filter(|v| bset.contains(v))
            .map(|&v| self.ids[v as usize])
            .collect()
    }

    /// Jaccard similarity of two nodes' out-neighbourhoods: `|A∩B| / |A∪B|`, in
    /// `[0, 1]`. The standard neighbourhood-overlap link-prediction / recommendation
    /// score.
    pub fn jaccard_similarity(&self, a: NodeId, b: NodeId) -> f64 {
        let (ai, bi) = match (self.index.get(&a), self.index.get(&b)) {
            (Some(&ai), Some(&bi)) => (ai, bi),
            _ => return 0.0,
        };
        let aset: std::collections::HashSet<u32> = self.neighbors(ai).iter().copied().collect();
        let bset: std::collections::HashSet<u32> = self.neighbors(bi).iter().copied().collect();
        if aset.is_empty() && bset.is_empty() {
            return 0.0;
        }
        let inter = aset.intersection(&bset).count() as f64;
        let union = aset.union(&bset).count() as f64;
        inter / union
    }

    /// The **Adamic–Adar** index of `a` and `b` — a link-prediction score that
    /// weights each shared neighbour by its rarity: common neighbours that connect
    /// to few others count for more. `AA(a,b) = Σ_{z ∈ N(a)∩N(b)} 1 / ln deg(z)`.
    ///
    /// On a bipartite interaction graph (users ↔ items, edges both ways) this is a
    /// strong "you might also like" signal: two items are similar when the *same
    /// niche users* engage with both, and a niche user (low degree) is far more
    /// telling than someone who engages with everything.
    pub fn adamic_adar(&self, a: NodeId, b: NodeId) -> f64 {
        let (ai, bi) = match (self.index.get(&a), self.index.get(&b)) {
            (Some(&ai), Some(&bi)) => (ai, bi),
            _ => return 0.0,
        };
        let aset: std::collections::HashSet<u32> = self.neighbors(ai).iter().copied().collect();
        let mut score = 0.0;
        for &z in self.neighbors(bi) {
            if aset.contains(&z) {
                let deg = self.neighbors(z).len();
                if deg > 1 {
                    score += 1.0 / (deg as f64).ln();
                }
            }
        }
        score
    }

    /// Recommend the top-`k` nodes related to `seed` that it is **not already
    /// connected to** — link prediction by random-walk-with-restart. Runs
    /// personalized PageRank seeded at `seed`, drops `seed` and its direct
    /// out-neighbours, and returns the highest-scoring remainder.
    ///
    /// On a user↔item graph, `recommend(user, k)` yields items the user has not
    /// interacted with but that are reachable through similar users — collaborative
    /// filtering as a graph walk.
    pub fn recommend(&self, seed: NodeId, k: usize) -> Vec<(NodeId, f64)> {
        let si = match self.index.get(&seed) {
            Some(&i) => i,
            None => return Vec::new(),
        };
        let mut exclude: std::collections::HashSet<NodeId> = self
            .neighbors(si)
            .iter()
            .map(|&d| self.ids[d as usize])
            .collect();
        exclude.insert(seed);
        let mut scored: Vec<(NodeId, f64)> = self
            .personalized_pagerank(&[seed], 40, 0.85)
            .into_iter()
            .filter(|(n, s)| *s > 0.0 && !exclude.contains(n))
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        scored.truncate(k);
        scored
    }

    /// The **Eisenberg–Noe clearing vector** of a financial liability network —
    /// the canonical model of default contagion / systemic risk.
    ///
    /// Read each directed edge `i -> j` with weight `w` as a *liability*: node
    /// `i` owes node `j` the amount `w`. `external_assets[i]` is the cash node
    /// `i` holds from outside the network (its operating cash / collateral). The
    /// method solves the clearing payment vector `p` satisfying
    ///
    /// ```text
    ///   p_i = min( p̄_i ,  e_i + Σ_j Π_ji · p_j )
    /// ```
    ///
    /// where `p̄_i = Σ_j L_ij` is `i`'s total nominal liability and
    /// `Π_ji = L_ji / p̄_j` is the share of `j`'s payments owed to `i`. Intuition:
    /// a node pays the smaller of what it owes and what it actually has once
    /// upstream payments arrive. It is solved by the monotone Picard iteration
    /// from `p = p̄` downward, which converges to the greatest clearing vector.
    ///
    /// Returns each node's payment, nominal liability, surviving equity, and the
    /// set of **defaulters** (those that cannot pay in full) — i.e. exactly who a
    /// default cascade drags under. Feed it a stressed `external_assets` (e.g. one
    /// counterparty's cash set to zero) to simulate a shock and read the contagion.
    pub fn eisenberg_noe(&self, external_assets: &HashMap<NodeId, f64>) -> ClearingResult {
        let n = self.node_count();
        // Nominal liabilities p̄_i = Σ out-edge weights.
        let nominal: Vec<f64> = (0..n).map(|u| self.weights(u as u32).iter().sum()).collect();
        // External assets in dense order.
        let mut ext = vec![0.0f64; n];
        for (id, &e) in external_assets {
            if let Some(&d) = self.index.get(id) {
                ext[d as usize] = e;
            }
        }

        // Σ_j Π_ji · p_j — payments flowing INTO each node under vector `p`.
        let inflow = |p: &[f64]| -> Vec<f64> {
            let mut inflow = vec![0.0f64; n];
            for j in 0..n {
                if nominal[j] <= 0.0 {
                    continue;
                }
                let ratio = p[j] / nominal[j];
                let nbrs = self.neighbors(j as u32);
                let ws = self.weights(j as u32);
                for (k, &i) in nbrs.iter().enumerate() {
                    inflow[i as usize] += ws[k] * ratio;
                }
            }
            inflow
        };

        // Monotone Picard iteration from full payment downward.
        let mut p = nominal.clone();
        for _ in 0..1000 {
            let flow = inflow(&p);
            let mut delta = 0.0f64;
            let mut next = vec![0.0f64; n];
            for i in 0..n {
                let val = (ext[i] + flow[i]).min(nominal[i]).max(0.0);
                delta = delta.max((val - p[i]).abs());
                next[i] = val;
            }
            p = next;
            if delta < 1e-9 {
                break;
            }
        }

        let flow = inflow(&p);
        let mut payments = HashMap::with_capacity(n);
        let mut nominal_map = HashMap::with_capacity(n);
        let mut equity = HashMap::with_capacity(n);
        let mut defaulted = Vec::new();
        for i in 0..n {
            let id = self.ids[i];
            payments.insert(id, p[i]);
            nominal_map.insert(id, nominal[i]);
            equity.insert(id, (ext[i] + flow[i] - p[i]).max(0.0));
            if p[i] + 1e-6 < nominal[i] {
                defaulted.push(id);
            }
        }
        ClearingResult {
            payments,
            nominal: nominal_map,
            equity,
            defaulted,
        }
    }
}

/// The outcome of an [`GraphView::eisenberg_noe`] clearing computation over a
/// liability network.
#[derive(Clone, Debug)]
pub struct ClearingResult {
    /// Amount each node actually pays under the clearing vector.
    pub payments: HashMap<NodeId, f64>,
    /// Total nominal liability each node owes (`p̄_i`).
    pub nominal: HashMap<NodeId, f64>,
    /// Surviving equity after clearing (`e_i + received − paid`, floored at 0).
    pub equity: HashMap<NodeId, f64>,
    /// Nodes that cannot meet their liabilities in full — the defaulters.
    pub defaulted: Vec<NodeId>,
}

/// Min-heap entry for Dijkstra, ordered by cost ascending (over a max-heap).
struct HeapItem(f64, u32);
impl PartialEq for HeapItem {
    fn eq(&self, o: &Self) -> bool {
        self.0 == o.0
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        // Reversed so BinaryHeap (a max-heap) yields the smallest cost first.
        o.0.partial_cmp(&self.0).unwrap_or(std::cmp::Ordering::Equal)
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
