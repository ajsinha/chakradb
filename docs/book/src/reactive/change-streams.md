# Change Streams & Materialized Workers

```{=latex}
\epigraph{You can never step into the same river twice, for new waters are always flowing on to you.}{--- Heraclitus}
```

A traditional database is passive: you ask a question, it answers. The systems
this book cares about — real-time AML, live risk — are *reactive*: the instant a
transaction commits, something must happen. ChakraDB closes that loop inside the
engine, on one consistent live dataset, without ever slowing the writer. This
chapter is the machinery: a committed-change stream, workers that maintain
derived state from it, and the transports that carry it across processes.

## Why not in-SQL triggers

The obvious design — `CREATE TRIGGER` running a stored procedure inside the
transaction — is the wrong one for this workload. A synchronous trigger runs user
code (often Python, under the GIL) in the **writer's critical section**, so it
serializes the very ingest that ChakraDB is built to keep fast. Worse, it couples
the writer's latency and failure to arbitrary application logic.

ChakraDB instead exposes the model an event-driven system actually wants: a
**post-commit change stream** (change-data-capture, CDC). Changes are delivered
*after* they commit, on a separate thread — the writer never waits for a reader.

## The change stream

A `CdcBackend` decorates the engine's backend and publishes a `Change` for every
committed `INSERT` / `UPDATE` / `DELETE`, carrying the operation, the commit
sequence number (CSN), and the old and new row images:

```rust
pub struct Change {
    pub table: String,
    pub op: ChangeOp,          // Insert | Update | Delete
    pub csn: Csn,              // total commit order
    pub columns: Arc<Vec<String>>,
    pub old: Option<Vec<Value>>,   // pre-image; None for Insert
    pub new: Option<Vec<Value>>,   // post-image; None for Delete
}
```

Because the wrap sits *above* the write path, it adds nothing to the engine's hot
locks. An `INSERT` — the ingest hot path — pays only a channel send; the old-row
pre-image for `UPDATE`/`DELETE` is read outside any core lock. This reuses the two
things ChakraDB already has and most engines lack: an ordered commit log (the
[WAL](../engine/durability.md), stamped with CSN) and cheap non-blocking
[snapshots](../engine/mvcc.md).

Wire it up and subscribe:

```rust
let cdc = Cdc::new();
let engine = SqlEngine::with_backend(CdcBackend::wrap(db, cdc.clone()));
let stream = cdc.subscribe(Some("transactions"));   // one table, or None for all

while let Some(batch) = stream.recv() {              // one commit's changes, in order
    for change in &batch {
        if change.op == ChangeOp::Insert {
            react(change.new.as_ref().unwrap());
        }
    }
}
```

Delivery is **at-least-once** and **in commit order**; a rolled-back transaction
never appears. The stream is a *pull* primitive — the consumer owns its thread,
cadence, and backpressure.

> **ALGORITHM — publishing a committed change**
> ```text
> On a mutating call m (insert/upsert/update/delete) through CdcBackend:
> 1  if m is update/delete: old ← inner.get_latest(key)      ▷ pre-image, no core lock
> 2  csn ← inner.apply(m)                                    ▷ the real write (+ WAL)
> 3  op  ← Insert | Update | Delete   (upsert: Update iff old present)
> 4  publish Change{ table, op, csn, columns, old, new }     ▷ after commit, off the write path
> ```

## The Python hook

In Python the stream is a callback, and the connection is CDC-wrapped by default.
`on_change` returns a `Subscription` whose lifecycle the caller owns:

```python
def react(old, new):                     # dicts (column → value), or None
    if new and new["amount"] >= 9000:
        alert(new["src"], new["dst"])

sub = conn.on_change("transactions", react)
...                                       # worker runs on its own thread
sub.close()                              # stop it — or use it as a context manager
```

The callback fires after commit, so it never blocks the writer — the whole engine,
including the [graph algorithms](../graph/algorithms.md), reachable from a hook in
a few lines.

## Materialized workers

Most reactions are not one-shot: they *maintain state* — a running aggregate, a
graph projection, a set of alerts. That is a **materialized worker**: a named,
incrementally-maintained derivation of the data. You implement *what* is
maintained; the runtime owns the loop, commit ordering, cursor tracking, and
lifecycle.

```rust
pub trait MaterializedWorker: Send + 'static {
    fn apply(&mut self, change: &Change);      // fold one change into state
    fn on_commit(&mut self, _csn: Csn) {}      // periodic hook (heavier passes)
}

let m = cdc.register("aml-detector", Some("transactions"), Worker::new());
m.query(|w| w.current_exposure.clone());       // read the derived state any time
m.cursor();                                    // how far it has consumed (a resume point)
m.stop();                                      // client owns the lifecycle
```

This is the disciplined worker primitive — **a function of the data**, not an
application host. A worker consumes changes, maintains state, exposes it for
query, and writes derived rows back; it does not open sockets or run arbitrary
services. That boundary is what keeps ChakraDB a database and not an app server.

### The registry

Named workers are tracked and observable — the control surface for a fleet:

```rust
for w in cdc.workers() {
    // w.name, w.table, w.cursor (CSN), w.running
}
cdc.stop_worker("aml-detector");
```

The CSN cursor is the key to durability: on restart, rebuild the worker's state
from a snapshot and resume the stream from the last cursor — no missed or
double-counted change, a hard requirement for compliance workloads.

### The three-tier pattern

Firing per event rewards *incremental* work. The case studies use a tiered model
so one embedded engine keeps up with millions of events per hour:

| Tier | Runs | Scope | Example |
|---|---|---|---|
| **T0** | every committed change | O(1)–O(degree) | fan-in counter, limit check |
| **T1** | micro-batch (seconds) | touched subgraph | bounded cycle search |
| **T2** | periodic (minutes) | whole graph, on a `view()` | PageRank, Eisenberg–Noe, VaR |

T0 keeps up with the firehose; T2 is heavy but rare and runs on an MVCC snapshot,
so it never blocks ingest. That non-blocking split is the difference between
real-time reaction and an overnight batch.

## Transports: in-process, files, and Kafka

A `ChangeSink` carries the stream beyond the local thread:

```rust
pub trait ChangeSink: Send + Sync { fn emit(&self, batch: &[Change]); }
```

- **In-process channel** (default): lowest latency, single node — a worker in the
  same process, as the streaming case studies run.
- **`JsonlSink`**: appends each change as one JSON line to a file — a broker-less
  **cross-process** change log. A separate worker process tails the file and
  consumes the stream, with no shared memory and no database lock.
- **Kafka / Redpanda / NATS**: implement `emit` to produce each `change.to_json()`
  keyed by a partition key (e.g. account id). *N* workers then consume partitions
  in parallel for horizontal scale-out — the same worker code, a different
  transport.

```mermaid
flowchart LR
    W["committed writes"] --> CDC["change stream (CSN-ordered)"]
    CDC --> S{"ChangeSink"}
    S -->|in-process| A["worker (same process)"]
    S -->|JsonlSink| B["worker (separate process, tails the log)"]
    S -->|Kafka: key=hash(entity)| C["workers ×N (sharded)"]
```

The scaling story is one line: **resident workers for single-node hot state;
Kafka-fanned workers for scale-out — the same derivation either way.**

## Where it leads

Two complete systems are built on this machinery: the
[Real-Time AML](../case-studies/aml.md) and
[Counterparty Credit & Market Risk](../case-studies/ccr.md) case studies. Each
generates its own synthetic feed, reacts to every committed row through a
registered materialized worker, and sustains 150+ million events per hour on a
single node — detection running concurrently with ingest, because in ChakraDB the
reader never blocks the writer.
