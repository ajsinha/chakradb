# Live Graph Analytics

```{=latex}
\epigraph{You can never cross the ocean until you have the courage to lose sight of the shore.}{--- Andr\'e Gide}
```

This is the chapter that says *why ChakraDB's graph layer is different*, not just
convenient. The difference is one word: **live.**

## The problem with the alternatives

To run a graph algorithm you need a consistent view of the graph. The usual ways to
get one all give something up:

- **Load into NetworkX / igraph.** You get a *dead static copy*. The moment you
  loaded it, it began to go stale. Re-analyzing the latest graph means reloading the
  whole thing.
- **A lock-based graph database (e.g. Neo4j).** A long analytical traversal
  contends with the writers mutating the graph; you either block ingest or read an
  inconsistent, partially-updated graph.
- **A separate OLAP copy (ETL the graph into a warehouse).** Now the analysis is
  minutes-to-hours stale, and you run two systems.

## What ChakraDB does instead

```mermaid
sequenceDiagram
    participant Writers
    participant Clock as MVCC clock
    participant Alg as Algorithm (GraphView)
    Writers->>Clock: add edge @ CSN 100
    Alg->>Clock: view() pins snapshot S = 100
    Note over Alg: builds CSR from S; runs PageRank...
    Writers->>Clock: add edge @ CSN 101  (never blocked)
    Writers->>Clock: add edge @ CSN 102
    Note over Alg: still computing over S = 100 — a consistent graph
    Alg-->>Alg: result reflects exactly the graph at S = 100
```

An algorithm builds its CSR from **one MVCC snapshot** and runs to completion over
that consistent instant. Meanwhile writers keep committing edges — **never
blocked**, because readers and writers share nothing but the append-only clock. The
[GC watermark](../algorithms/gc-watermark.md) holds the snapshot's versions alive
for the algorithm's duration even as newer data is compacted.

The result: you can run PageRank (or components, or a fraud traversal) **every
minute over the latest consistent graph, while ingest never pauses** — in one
embedded process. That is the combination the alternatives cannot offer.

## The pattern in code

```rust
// A background loop: recompute influence over the live graph, on a cadence.
loop {
    let view = graph.view()?;              // consistent snapshot; ingest continues
    let ranks = view.pagerank(20, 0.85);
    publish_top_influencers(&ranks);       // serve results
    sleep(Duration::from_secs(60));
}
```

Each `view()` is a fresh consistent snapshot. The writers feeding `graph` never
notice the analytics running.

## Why it composes with the rest of the database

Because the graph *is* tables, graph results compose with SQL and transactions over
the **same** live data:

- Score an entity with a graph traversal **and** join it to its transactional row
  in one consistent snapshot.
- Ingest an event (transactional), update the graph edge (transactional), and let
  the next analytics pass pick it up — no cross-system consistency to reason about.

This is HTAP extended to graphs: **T + A + G over one snapshot clock.** The
[fraud case study](../case-studies/fraud.md) works it end to end.

> **The honest scope.** v1 *recomputes* an algorithm over a snapshot on demand; it
> does not yet *maintain* live results incrementally as edges change. Incremental
> graph algorithms (streaming PageRank, incremental components) are a roadmap
> item — a v2 that leans even harder on this live-mutation story.
