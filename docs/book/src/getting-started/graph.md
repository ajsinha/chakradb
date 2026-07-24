# A Graph in Five Minutes

```{=latex}
\epigraph{Everything is connected \ldots\ no one thing can change by itself.}{--- Paul Hawken}
```

ChakraDB has a graph engine built into the core — not a bolt-on, not a separate
store. A graph *is* a table of edges, clustered so that a node's neighbours sit
together on disk; analytics run over a consistent [MVCC snapshot](../architecture/mvcc.md)
of that table. This chapter is the whirlwind tour.

## Open a graph and add edges

A graph is backed by a named table in an ordinary database. In Rust:

```rust
use chakradb::{Database, Graph};
use std::sync::Arc;

let db = Arc::new(Database::new());
let g = Graph::open(db.clone(), "transfers")?;

// (src, dst, weight) — inserts are upserts, and fully transactional.
g.add_edges([(1, 2, 100.0), (2, 3, 250.0), (3, 1, 90.0)])?;
```

In Python, through an existing connection:

```python
g = conn.graph("transfers")
g.add_edges([(1, 2, 100.0), (2, 3, 250.0), (3, 1, 90.0)])
```

Because edges are just rows, the *same* data is visible to SQL
(`SELECT ... FROM transfers`) and to the graph engine simultaneously — that is
the HTAP promise applied to graphs.

## Freeze a snapshot, then compute

All algorithms run against a `view()` — an immutable in-memory
[CSR](../graph/csr.md) snapshot. Take it once and a whole pipeline sees one
coherent graph, even while writers keep appending edges:

```rust
let view = g.view()?;
println!("{} nodes, {} edges", view.node_count(), view.edge_count());

let dist   = view.bfs(1);                       // hops from node 1
let path   = view.shortest_path(1, 3);          // Some([1, 2, 3])
let cost   = view.weighted_shortest_path(1, 3); // Dijkstra with weights
let rank   = view.pagerank(20, 0.85);           // {node: score}
let rings  = view.laundering_cycles();          // non-trivial SCCs
```

The Python surface is identical, returning dicts and lists:

```python
view = g.view()
view.bfs(1)                       # {1: 0, 2: 1, 3: 1}
view.shortest_path(1, 3)          # [1, 2, 3]
view.pagerank()                   # {1: 0.33, 2: 0.33, 3: 0.33}
view.laundering_cycles()          # [[1, 2, 3]]
view.personalized_pagerank([1])   # risk spread from a seed set
```

## The algorithm catalogue

Every one of these is built into the core and available from both Rust and
Python. See [Graph Algorithms](../graph/algorithms.md) for the derivations.

| Category | Algorithms |
|----------|-----------|
| **Traversal & paths** | `bfs`, `shortest_path`, `dijkstra`, `weighted_shortest_path`, `topological_order` |
| **Centrality** | `pagerank`, `personalized_pagerank`, `degree_centrality`, `closeness_centrality`, `betweenness_centrality` |
| **Community & structure** | `connected_components`, `strongly_connected_components`, `laundering_cycles`, `label_propagation`, `k_core`, `triangle_count` |
| **Systemic risk** | `eisenberg_noe` (clearing vector / default cascade) |
| **Similarity** | `common_neighbors`, `jaccard_similarity` |
| **Degrees** | `in_degree`, `out_degree`, `in_neighbors`, `out_neighbors` |

## Snapshots are stable under writes

The defining property — an analysis is reproducible because its view does not
move under it:

```rust
let v = g.view()?;
let before = v.edge_count();
g.add_edges([(9, 10, 1.0)])?;           // keep writing after the view is taken
assert_eq!(v.edge_count(), before);     // the snapshot did not change
assert_eq!(g.view()?.edge_count(), before + 1);  // a fresh view sees the write
```

## A real application

The [Real-Time AML case study](../case-studies/aml.md) composes these primitives
into a working fraud-detection system. The runnable code is
`examples/aml_realtime.rs` (Rust) and `examples/aml_app.py` (Python).
