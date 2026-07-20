# ChakraDB

An embedded, single-process analytical database that accepts a continuous
high-rate write stream while serving scans that never block — with on-disk state
other engines can read directly.

> **Status: M2 substantially complete (Gate 2 pending a DuckDB comparison).** This repository contains a durable storage engine —
> write-ahead logging, crash recovery, checkpointing and compaction — but no SQL
> layer yet, and it has only ever run against an in-memory filesystem.
> See [`docs/m1-findings.md`](docs/m1-findings.md) for what it proved and what it
> did not.

---

## The idea in one sentence

DuckDB gives you fast scans over data you loaded earlier. ChakraDB aims at fast
scans over data that is *still arriving*, with real transactions, in an open
format.

Individually, existing engines have three of the four properties below. None has
all four:

| | embedded | ACID + MVCC | concurrent writes + non-blocking scans | open on-disk format |
|---|---|---|---|---|
| DuckDB | ✅ | ✅ | ❌ single writer process | ⚠️ via DuckLake |
| chDB | ⚠️ needs a subprocess | ❌ | ⚠️ | ✅ |
| ArcticDB | ✅ | ❌ dropped by design | ✅ | ✅ |
| Umbra / CedarDB | ❌ server | ✅ | ✅ | ❌ |
| **ChakraDB (target)** | ✅ | ✅ | ✅ | ✅ |

Supporting evidence that this gap is structural rather than merely unoptimised:
ClickBench added a concurrent-query test, and **every embedded engine's result is
`null`** — the benchmark structurally exempts them, because a single-process
engine cannot meaningfully take the test.

---

## Repository layout

```
docs/
  requirements.md        Architecture & design specification (v2.0)
  roadmap.md             M0–M5 with decision gates and stop conditions
  m0-findings.md         M0 results, including the negative ones
  m1-findings.md         M1 results: durability, recovery, crash testing
  m2-findings.md         M2 results: query layer, buffer pool, the Gate-2 gap
  archive/               Superseded documents
src/                     Engine (zero dependencies)
tests/                   Integration suites
```

Start with `docs/requirements.md` §1–§3 for the wedge and the cost model, then
`docs/roadmap.md` for sequencing.

---

## Trying the prototype

```bash
cargo test                              # 337 tests, ~2s
cargo run --release --bin m0-bench      # M0 acceptance measurements
cargo run --release --bin m1-bench      # M1 acceptance measurements
cargo run --release --bin m2-bench      # M2 / NFR-03 measurements
```

```rust
use chakradb::{Database, Row};

let db = Database::new();
let users = db.create_table("users")?;

users.insert(Row::new(1, 100, 1.5, "alice"))?;

// Snapshots are stable across concurrent writes.
let before = db.snapshot();
users.update(Row::new(1, 999, 1.5, "alice-v2"))?;

assert_eq!(users.get(1, before).unwrap().c, "alice");
assert_eq!(users.get_latest(1).unwrap().c, "alice-v2");
```

Or through SQL (M2), over the fixed four-column schema:

```rust
use chakradb::{Database, SqlEngine};
use std::sync::Arc;

let sql = SqlEngine::new(Arc::new(Database::new()));
sql.run("CREATE TABLE t (pk INT)").unwrap();
sql.run("INSERT INTO t VALUES (1, 100, 1.5, 'alice')").unwrap();
let rows = sql.query("SELECT c FROM t WHERE a > 50").unwrap();
assert_eq!(rows[0][0], "alice");
```

Many tables share one snapshot clock, so a read across several of them observes a
single instant. Each table has its own primary-key space. **Foreign keys are an
explicit non-goal** — referential integrity is the application's business.

---

## What M1 established

| Result | Number |
|---|---|
| Crash injections passed | **~700 seeded trials**, all durability modes |
| Group-commit batching | **8× fewer fsyncs than commits** at 16 writer threads |
| Log replay time | **flat at 1.4 ms** across 20× database growth |
| Compaction's cost to write throughput | **18.4× → 1.56×** after the two-phase fix |

Two data-loss bugs were found by testing rather than by inspection: compaction
starving writers (M0), and a stale durable watermark after WAL truncation that
made post-checkpoint commits skip their fsync (M1). Both are regression-tested.

FR-06 had to be **split**: log replay is bounded by the WAL tail exactly as
designed, but total recovery still scales with database size because parts load
eagerly. That needs M2's buffer pool.

## What M0 established

| Result | Number |
|---|---|
| Primary-key index cost | **1.25 B/row, flat with table size** (≈1.25 GB at 1B rows) |
| Point-lookup latency, 1 → 33 parts | **flat at 1.1–1.7 µs** while fan-out grows 16× |
| Scan under sustained write load | **3.77×** degradation without compaction, **1.38×** with it |
| Cold-scan fast path | engages 30/30; disabled by a *single* tombstone |

The index result is the important one. Because parts are written sorted by
primary key, the ordinal position in the index *is* the row offset — so no
key→location map exists to pay for. The comparable explicit map would cost ~12
B/row, which is what forced StarRocks to build an LSM for its index.

Both M0 defects are now fixed: compaction no longer holds the write lock through
the merge, and the fan-out result has been re-verified against a key distribution
chosen to defeat min/max bounds.

---

## Design principles

1. **Risk-first sequencing.** Every milestone ends at a gate with an explicit
   stop condition. A gate you cannot fail is not a gate.
2. **Buy execution, build storage.** The differentiator is the storage engine and
   concurrency control; the query engine is a bought component.
3. **Some seams cannot be retrofitted.** `trait Io`, `trait Clock` and a seeded
   RNG exist from M0 — before anything needs them — because adding them after
   compaction threads and a buffer pool call ambient APIs is a rewrite.
4. **Publish the harness, not just the number.** No neutral benchmark measures
   the axis this project competes on, so every performance claim ships with the
   code that produced it.

## Reading the numbers

Every performance figure in this repository is single-machine, single-run, and
unaudited. `docs/requirements.md` opens with an explicit confidence tiering, and
the rule it sets is worth repeating here: **a number without a source is a
hypothesis, not a measurement.** The design reasoning does not depend on any of
them.

## License

Apache-2.0.
