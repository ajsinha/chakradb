# ChakraDB as a Graph Database — Design Exploration

**Status:** exploration / design proposal (not implemented). This lays out how to
model a property graph on ChakraDB, why the engine is an unusually good substrate
for *live* graph analytics, the client API that would make it simple, which
algorithms to bake into the core, and the honest gaps.

---

## The thesis

Don't think of this as "add graph features to a SQL engine." Think of it as:
ChakraDB already has three properties that graph engines fight hard to get, and a
graph layer is mostly a matter of exposing them.

1. **Sorted-by-key columnar parts + zonemap part pruning = a clustered adjacency
   index, for free.** If an edge's key is `(src, dst)` encoded src-major, then all
   of a node's out-edges are a contiguous key range. "Neighbors of X" becomes a
   range scan that zonemap pruning answers by touching only the parts that hold
   `src = X` — the same mechanism that makes a selective `WHERE` fast (see
   `clickbench-findings.md`, Q13: O(1) in table size).

2. **MVCC non-blocking snapshot scans = run graph algorithms over a consistent
   graph *while it is being mutated*.** This is the wedge (README §concurrency).
   Neo4j takes locks; loading into NetworkX/igraph gives you a dead static copy.
   ChakraDB lets PageRank run to completion over a stable snapshot while edges
   keep streaming in. **Live graph analytics is the differentiated product**, and
   it falls out of the architecture rather than being bolted on.

3. **Arrow-native sorted edges ≈ CSR on disk.** Compressed Sparse Row — a `srcs`
   offset array plus a `dsts` neighbor array — is the representation nearly every
   graph algorithm wants. Edges stored sorted by `src` are *already grouped by
   source*, so building CSR from a snapshot is a single linear, vectorized scan
   over Arrow columns. The storage format is the algorithm input format.

The pitch to a user: **a graph you can keep writing to, and analyze at the same
time, in an embedded library — with the common algorithms one method call away.**

---

## 1. Modeling a property graph

A property graph is nodes and edges, each with a label/type and properties. Map it
to ChakraDB tables:

```sql
-- Nodes: keyed by node id (fast point lookup, fast id-range scan).
CREATE TABLE nodes (
  id     INT PRIMARY KEY,
  label  VARCHAR(64),
  props  TEXT            -- JSON, or promote hot properties to real columns
);

-- Out-edges: keyed (src, dst) src-major → clustered adjacency by source.
CREATE TABLE edges_out (
  key    INT PRIMARY KEY,   -- encode(src, dst); see §2
  src    INT,
  dst    INT,
  type   VARCHAR(32),
  weight DOUBLE
);

-- In-edges: keyed (dst, src) dst-major → reverse adjacency for backward traversal.
CREATE TABLE edges_in (
  key    INT PRIMARY KEY,   -- encode(dst, src)
  src    INT,
  dst    INT
);
```

Hot properties (weight, timestamp, a status flag) should be real typed columns —
they get zonemaps and pruning, so "edges of type=follows created after T" prunes.
Cold/sparse properties live in a `props` blob.

Two edge tables (forward + reverse) is the standard adjacency-list trick: keep both
directions so traversal is fast either way. They're kept consistent in one
transaction on each edge write.

---

## 2. The adjacency problem — and the key trick

**The one hard constraint:** ChakraDB has a *single-column* primary key and **no
secondary indexes** (`requirements.md` §2.1/§2.2). Adjacency ("find all edges with
`src = X`") is only fast if the edge table is physically *sorted by src*. Parts are
sorted by the primary key, so **the edge key must sort src-major.**

Since the key is one `Value`, encode `(src, dst)` into a single sortable key:

- **Packed integer** (simplest, today): `key = (src << 32) | dst` as `INT` when node
  ids fit in 32 bits. Keys then sort by `src`, then `dst`. Out-neighbors of `X`:
  ```sql
  SELECT dst FROM edges_out
  WHERE key >= (X << 32) AND key < ((X+1) << 32);
  ```
  This is a **key-range scan** — zonemap part pruning touches only the parts
  holding `src = X`. It's the graph-traversal primitive, and it already runs on
  today's engine, non-blocking, over an MVCC snapshot.

- **Lexicographic text** (any id size, today): zero-padded `"0000000001:0000000042"`
  sorts src-major. Works for arbitrary ids; keys are larger and text compare is
  slower than int.

- **Native composite / bytes key** (core enhancement, §6): the clean answer — let a
  table declare `PRIMARY KEY (src, dst)` or a `BYTES` key that sorts
  lexicographically. Removes the encoding entirely and makes edges first-class.

The takeaway: **adjacency works *today* via key encoding**, and a small core
feature (composite keys) would make it clean.

---

## 3. Why it's fast, and why it's different

**Fast — CSR alignment.** To run an algorithm, take a snapshot and scan `edges_out`
in key order. Because rows arrive grouped by `src`, one linear pass builds CSR:
`offsets[node]` and a flat `neighbors[]` array. No sort, no hash join — the parts
are already sorted, and the scan is vectorized over Arrow columns. Neighbor
iteration in the algorithm is then a cache-friendly array walk. For large graphs
CSR must fit in RAM, which matches ChakraDB's resident-index model (memory is the
ceiling, not disk).

**Different — live analytics under mutation.** The snapshot the CSR is built from
is stable (MVCC): writers keep adding edges, the GC watermark (`tests/gc_watermark.rs`)
keeps the snapshot's versions alive, and the algorithm sees one consistent instant.
You can run PageRank every minute over the latest consistent graph while ingest
never pauses. That is the thing pure graph libraries and lock-based graph DBs
cannot do in one embedded process.

---

## 4. The client experience — a `Graph` handle

The goal: the user never writes a traversal by hand. A thin handle over the tables,
with algorithms as methods.

**Python** (works like the rest of the DB-API driver):
```python
import chakradb
g = chakradb.graph("./social")          # opens/creates nodes + edges tables

g.add_node(1, label="person", name="Alice")
g.add_edge(1, 2, type="follows", weight=1.0)   # writes edges_out + edges_in

# algorithms are one call — computed over a consistent snapshot:
ranks  = g.pagerank()                    # {node_id: score}
path   = g.shortest_path(1, 5)           # [1, 3, 5] or None
comps  = g.connected_components()        # {node_id: component_id}
nbrs   = g.neighbors(1, hops=2)          # 2-hop neighborhood
tri    = g.triangle_count()              # global + per-node
sim    = g.similar_to(1, k=10)           # personalized PageRank / common-neighbors
```

**Rust:**
```rust
let g = Graph::open(&db, "social")?;
let ranks = g.pagerank(PageRank::default())?;         // Vec<(NodeId, f64)>
let path  = g.shortest_path(1, 5, Weighted::No)?;
```

**SQL table functions** (the composable one — algorithms as queryable tables that
`JOIN` with node properties, over live data):
```sql
-- Top 10 influencers, with their names, over the current snapshot:
SELECT n.props, p.score
FROM pagerank('social') p
JOIN nodes n ON n.id = p.node
ORDER BY p.score DESC
LIMIT 10;

SELECT * FROM shortest_path('social', 1, 5);
SELECT node, component FROM connected_components('social');
```
The table-function form is the sweet spot for "simple client": no traversal code,
and results compose with the SQL the user already knows — filter, join, aggregate —
which the interpreter/DataFusion split already handles.

---

## 5. Algorithms to bake into the core

Pick the set that covers ~80% of graph use and maps cleanly onto snapshot+CSR.
Everything below is read-only over a snapshot, so it runs concurrently with ingest.

| Algorithm | Use | Representation / cost |
| :--- | :--- | :--- |
| **BFS / DFS, k-hop neighbors** | reachability, neighborhoods | CSR frontier walk, O(V+E) |
| **Shortest path** (unweighted BFS, weighted Dijkstra) | routing, degrees of separation | CSR + heap, O(E log V) |
| **Connected components** | clustering, dedup, "islands" | union-find or label prop, ~O(E·α) |
| **PageRank / personalized PageRank** | influence, recommendation | CSR power-iteration, O(E) per iter |
| **Triangle counting / clustering coefficient** | community strength, spam | sorted-neighbor intersection, O(E^1.5) |
| **Degree / weighted degree** | trivial centrality | metadata scan (near-free) |
| **Common neighbors / Jaccard** | link prediction, "people you may know" | two range scans + intersect |
| **Label propagation** | fast community detection | CSR iterate, O(E) per round |
| **WCC/SCC** | connectivity structure | union-find / Tarjan |

Deliberately *not* v1 (expensive or hard, add later): exact betweenness centrality
(all-pairs), Louvain modularity, exact diameter, general subgraph isomorphism.

Each is a compact Rust kernel over CSR built from the snapshot. Degree centrality
and neighbor lookups need no CSR at all — they're the range/metadata scans the
engine already does.

---

## 6. Core enhancements that make it first-class

Ordered by leverage:

1. **Composite / bytes key** — `PRIMARY KEY (src, dst)`, or a `BYTES` key type that
   sorts lexicographically. Removes the encoding hack; makes clustered adjacency
   native. This is the single highest-value change and it benefits non-graph uses
   too (natural multi-column keys). *(Touches: `Value`, key comparison, DDL,
   codec — moderate.)*

2. **Algorithm SQL table functions (UDTFs)** — register `pagerank(g)`,
   `shortest_path(g, a, b)`, etc. so results compose with SQL and `JOIN` node
   props. This is what makes the client experience *simple and composable*.
   *(Touches: the executor / DataFusion bridge.)*

3. **A `graph` core module** — `Graph` handle, CSR builder over a snapshot, the
   algorithm kernels, Rust + Python surface. The bulk of the work, but
   self-contained and read-only (no storage-format risk).

4. **Result write-back** — `g.pagerank().into_table("ranks")` to persist scores as
   a normal (queryable, joinable) table. Falls out of the existing `COPY`/bulk path.

5. **Incremental adjacency / algorithms** (later) — maintain degree and component
   labels as edges arrive; incremental PageRank. Hard; a v2 differentiator that
   leans even harder on the live-mutation story.

Notably, **none of the algorithm work touches the durable format or MVCC** — it's
all read-only over snapshots, so it's low-risk relative to the storage engine.

---

## 7. A phased plan

- **Phase 0 — prove it (days).** A `Graph` helper that sets up the tables and does
  edge writes + neighbor scans via packed-int keys, on *today's* engine. Benchmark
  neighbor lookup vs table size to show pruning gives clustered-adjacency behavior.
- **Phase 1 — the core module (weeks).** CSR builder over a snapshot; BFS,
  shortest path, connected components, PageRank, triangle count. Rust + Python
  `Graph` API. This alone is a usable graph engine.
- **Phase 2 — SQL table functions.** `pagerank('g')` et al. as queryable tables;
  result write-back. This delivers the "simple client" promise.
- **Phase 3 — native composite/bytes keys.** Retire the encoding; clean edge DDL.
- **Phase 4 — incremental algorithms + more (label prop, SCC, personalized PR).**

---

## 8. Honest limitations

- **Adjacency needs the key trick until composite keys land.** Packed-int caps node
  ids at 32 bits; text keys are slower. It works, but it's an encoding the user (or
  the `Graph` helper) must own.
- **Algorithms are memory-bound.** CSR for the analyzed subgraph must fit in RAM —
  the same ceiling as the resident index. Trillion-edge graphs are out of scope;
  "fits in a big server's RAM" is in.
- **No recursion in the SQL surface.** You can't express BFS in a recursive CTE
  (unsupported), which is exactly *why* the algorithms belong in the core rather
  than as SQL.
- **Two edge tables to keep consistent.** Forward+reverse adjacency doubles edge
  storage and must be written in one transaction. (A native composite key could let
  one table serve both directions with two orderings — later.)
- **Snapshot recompute, not streaming results.** v1 recomputes an algorithm over a
  snapshot on demand; it does not maintain live results as edges change. Incremental
  is Phase 4.
- **Expensive algorithms excluded from v1** (betweenness, Louvain, exact diameter,
  subgraph iso) — offered later or left to export.

---

## Bottom line

ChakraDB doesn't need to become a graph database; it needs a thin graph layer that
exposes what it already is — **sorted clustered adjacency + non-blocking snapshot
analytics + Arrow-CSR alignment**. The differentiated product is *live* graph
analytics in an embedded engine: keep writing the graph, run the algorithms over a
consistent view, one method call from the client. The highest-leverage enabler is a
native composite/bytes key; everything else is a self-contained, read-only core
module that carries no risk to the storage engine.
