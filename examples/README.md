# ChakraDB Example Applications

End-to-end, runnable applications built on ChakraDB. Each generates its own
synthetic data and **self-verifies** — it asserts that the effect it demonstrates
actually happens, so a green run is a passing integration test. Rust examples run
on the lean profile (`--no-default-features`); Python examples need the extension
built (`cd bindings/python && maturin develop`, or the in-tree `.abi3.so`).

Every streaming example reacts to committed data through the **change stream**
([book: Reactive](../docs/book/src/reactive/change-streams.md)) and a
**materialized worker**, with detection/serving running concurrently with ingest —
each sustains **150+ million events/hour on a single node**.

## Real-time Anti-Money-Laundering

| File | What it does |
|---|---|
| `aml_realtime.rs` | Batch reference: builds a synthetic payment network and runs the full detector ensemble (structuring, laundering cycles, mule fan-out, risk propagation) once over the finished dataset. |
| `aml_pipeline.rs` | **Streaming**: a generator thread feeds transactions while a registered materialized worker reacts to each committed transaction — T0 incremental detectors + a periodic T2 global pass (`laundering_cycles`, `personalized_pagerank`) over a snapshot. |
| `aml_app.py` | Python batch version — the same detectors through `conn.graph(...)`. |
| `aml_stream.py` | Python streaming version via `conn.on_change`. |
| `aml_gen.rs` | Standalone generator CLI (`--db`, `--count`, `--seed`, `--sink FILE`) — writes a transaction feed, optionally to a JSON-lines change log. |
| `aml_worker.rs` | **Separate process**: tails the `--sink` change log and runs the detectors — the Kafka topology with a file standing in for the topic. |

```bash
cargo run --release --example aml_pipeline --no-default-features        # streaming, in-process
python examples/aml_stream.py                                           # Python mirror

# cross-process (two terminals):
cargo run --release --example aml_gen --no-default-features -- --count 200000 --sink /tmp/aml.jsonl
cargo run --release --example aml_worker --no-default-features -- /tmp/aml.jsonl --follow
```

Walkthrough: [Real-Time AML case study](../docs/book/src/case-studies/aml.md).

## Real-time Counterparty Credit & Market Risk

| File | What it does |
|---|---|
| `ccr_pipeline.rs` | A streaming market-data feed drives per-tick current exposure & single-name limits (T0) and a periodic pass (T2): historical-simulation **VaR**, PageRank systemic importance, **Eisenberg–Noe** default-cascade contagion, and circular-exposure detection. |
| `ccr_stream.py` | Python mirror via `conn.on_change` + `view.eisenberg_noe`. |

```bash
cargo run --release --example ccr_pipeline --no-default-features
python examples/ccr_stream.py
```

Walkthrough: [CCR case study](../docs/book/src/case-studies/ccr.md).

## Real-time Recommendations

| File | What it does |
|---|---|
| `reco_pipeline.rs` | A stream of user interactions drives a live recommender — a materialized worker maintains the user↔item graph and produces recommendations (`recommend`), "you might also like" (`adamic_adar`), and trending (degree), over a snapshot. |
| `reco_stream.py` | Python mirror. |

```bash
cargo run --release --example reco_pipeline --no-default-features
python examples/reco_stream.py
```

Walkthrough: [Recommendations case study](../docs/book/src/case-studies/reco.md).

## The common shape

All three streaming systems share one design (see the
[Reactive chapter](../docs/book/src/reactive/change-streams.md)):

1. A **generator** streams synthetic events into a table.
2. A **materialized worker** (`cdc.register(name, table, worker)`) reacts to each
   committed row — cheap **T0** incremental updates at ingest speed, plus a heavy
   **T2** graph pass on a periodic cadence over a consistent `Graph::view()`.
3. Heavy analytics run on an **MVCC snapshot**, so the reader never blocks the
   writer — the reason detection keeps up with ingest.

Swap the in-process channel for the `JsonlSink` (or a Kafka sink) and the worker
becomes a separate process — the same worker code, a different transport.
