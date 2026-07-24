# Graph Algorithms

```{=latex}
\epigraph{A journey of a thousand miles begins with a single step.}{--- Lao Tzu}
```

All of ChakraDB's graph algorithms run over a `GraphView` — the immutable
[CSR snapshot](snapshot.md). Because the view is a consistent copy taken from one MVCC
snapshot, every algorithm below runs **concurrently with ingest**: writers keep
adding edges while the algorithm computes over a stable graph. Each is a compact
Rust kernel; this chapter gives the method, the pseudocode, and the complexity.

Throughout, `V` = number of nodes, `E` = number of directed edges, `d(u)` = out
-degree of `u`.

## Breadth-first search & shortest path

Unweighted shortest paths — "degrees of separation," reachability, k-hop
neighborhoods — are BFS over the out-edges.

```mermaid
flowchart LR
    S((start)):::f --> A:::f1 --> B:::f2
    S --> C:::f1
    C --> B
    C --> D:::f2
    classDef f fill:#8ecae6; classDef f1 fill:#bde0fe; classDef f2 fill:#e7f0ff;
```

```text
bfs(start):
  depth[*] = ∞;  depth[start] = 0;  queue = [start]
  while queue not empty:
    u = queue.pop_front()
    for v in neighbors(u):
      if depth[v] == ∞:
        depth[v] = depth[u] + 1
        queue.push_back(v)
  return depth
```

`shortest_path(a, b)` is the same wave with a `parent[]` array; on reaching `b`, it
walks parents back to `a`. **Complexity `O(V + E)`**, one queue, one visited array.

```rust
assert_eq!(view.shortest_path(1, 4), Some(vec![1, 3, 4]));
```

## PageRank

The influence/importance score: a node is important if important nodes point at it.
ChakraDB computes it by **power iteration**, scattering each node's rank across its
out-edges.

```text
pagerank(iterations, d):            # d = damping, ~0.85
  rank[*] = 1/V
  repeat `iterations` times:
    dangling = Σ rank[u] for u with d(u) == 0
    base = (1-d)/V + d·dangling/V     # teleport + redistributed dead-ends
    next[*] = base
    for u in 0..V:
      if d(u) > 0:
        share = d · rank[u] / d(u)
        for v in neighbors(u): next[v] += share
    rank = next
  return rank                         # sums to ~1
```

The **dangling-node** handling matters: nodes with no out-edges would leak
probability mass; their rank is collected and redistributed uniformly through
`base`, so the scores stay a proper distribution. **Complexity `O(iterations · E)`.**

```rust
// A star where 2,3,4,5 all point at 1 → node 1 ranks highest.
let pr = view.pagerank(30, 0.85);
```

`personalized_pagerank` is the same iteration with the teleport vector
concentrated on a **seed set** instead of uniform — the engine behind risk
propagation (AML/CCR) and the `recommend` walk (see below).

## Connected components

"Which nodes belong to the same island?" ChakraDB computes **weakly-connected
components** (edges treated as undirected) with a **union-find** (disjoint-set)
structure using path-halving.

```text
connected_components():
  parent[u] = u for all u
  for each edge (u, v):
    union(u, v)          # merge the two sets
  relabel roots to 0..k
  return node -> component id
```

Union-find with path compression runs in near-linear **`O(E · α(V))`** time, where
`α` is the inverse-Ackermann function (effectively constant). **Strongly**-connected
components (`strongly_connected_components`, and `laundering_cycles` for the
non-trivial ones) find *directed* cycles — the core of AML round-trip detection.

```rust
let c = view.connected_components();
assert_eq!(c[&1], c[&4]);   // same island
assert_ne!(c[&1], c[&5]);   // different island
```

## Triangle counting

Triangles measure local density — community strength, clustering coefficient, spam
detection. ChakraDB counts them on the **undirected** graph by sorted-neighbor
intersection with an ordering trick that counts each triangle exactly once.

```text
triangle_count():
  build sorted, de-duplicated undirected adjacency
  total = 0
  for u in 0..V:
    for v in neighbors(u) with v > u:
      total += | { w in adj(u) ∩ adj(v) : w > v } |   # sorted two-pointer
  return total
```

Requiring `u < v < w` means the triangle `{u,v,w}` is counted once, not six times.
**Complexity `O(E^{1.5})`** in the worst case (the classic node-iterator bound).

```rust
// Triangle 1-2-3, plus a dangling edge 3-4:
assert_eq!(view.triangle_count(), 1);
```

## Degree & neighborhoods

The cheapest signals need no CSR at all:

- `out_degree(node)` — the length of the node's neighbor range.
- `out_neighbors(node)` — a single [pruned range scan](model.md), live (no
  snapshot copy needed).
- k-hop neighborhood — BFS truncated at depth k.

## Systemic risk: the Eisenberg–Noe clearing vector

Reading each directed edge `i → j` of weight `w` as a *liability* — `i` owes `j`
the amount `w` — `eisenberg_noe(external_assets)` computes the clearing payment
vector that models **default contagion**: who can pay, who defaults, and who a
cascade drags under. It is the canonical systemic-risk model, solved by a monotone
Picard iteration to the greatest clearing vector, and it powers the
[Counterparty Risk case study](../case-studies/ccr.md). Full derivation there.

## Link prediction & recommendations

Two primitives turn the graph into a recommender ([Recommendations case
study](../case-studies/reco.md)):

- **`adamic_adar(a, b)`** — a link-prediction score summing `1/ln deg(z)` over
  shared neighbours `z`: rare, niche connections count for more than popular ones.
  On a user↔item graph it is "you might also like."
- **`recommend(seed, k)`** — personalized PageRank seeded at `seed`, with the
  seed's existing neighbours and zero-signal candidates removed, returning the
  top-`k` remainder. Collaborative filtering as a random-walk-with-restart, in one
  call.

## The algorithm menu

Every algorithm below is built into the core (`src/graph.rs`) and available from
Rust and Python, over a consistent [`GraphView`](snapshot.md) snapshot.

| Family | Algorithms |
|---|---|
| **Traversal & paths** | `bfs`, `shortest_path`, `dijkstra`, `weighted_shortest_path`, `topological_order` |
| **Centrality** | `pagerank`, `personalized_pagerank`, `degree_centrality`, `closeness_centrality`, `betweenness_centrality` (Brandes) |
| **Community & structure** | `connected_components` (WCC), `strongly_connected_components` / `laundering_cycles`, `label_propagation`, `k_core`, `triangle_count` |
| **Similarity & link prediction** | `common_neighbors`, `jaccard_similarity`, `adamic_adar`, `recommend` |
| **Systemic risk** | `eisenberg_noe` (clearing vector / default cascade) |
| **Degrees** | `in_degree`, `out_degree`, `in_neighbors`, `out_neighbors` |

Deliberately **not** yet in core (expensive or specialized — candidates for a
later release): Louvain/Leiden modularity, exact diameter, general subgraph
isomorphism, Johnson's all-simple-cycles, temporal motifs, and max-flow/min-cut.
See the [design exploration](../../../graph-exploration.md) for the roadmap.
