# ChakraDB

An embedded **HTAP** database: it accepts a continuous high-rate write stream
while serving analytical queries that never block — with ACID/MVCC transactions
and an open, Arrow-native on-disk format.

> **Status: working HTAP engine.** Arrow-native storage with arbitrary schemas,
> a dual-execution SQL layer (interpreter + DataFusion) behind a cost-based
> router, durable crash-safe SQL, and a real POSIX filesystem backend.
> 458 tests green. The remaining frontier is scale hardening and packaging — not
> capability. See [`docs/arrow-schema-migration.md`](docs/arrow-schema-migration.md)
> and [`docs/m3-datafusion-spike.md`](docs/m3-datafusion-spike.md) for the recent
> arc, and the `docs/m*-findings.md` for the earlier point-in-time records.

---

## The idea in one sentence

DuckDB gives you fast scans over data you loaded earlier. ChakraDB gives you fast
scans over data that is *still arriving*, with real transactions, in an open
format. That is what "HTAP" (Hybrid Transactional/Analytical Processing) means:
one engine for both the writes and the analytics, on the same live data, with no
ETL between two systems.

Individually, existing engines have three of the four properties below. The gap
ChakraDB targets is having all four at once:

| | embedded | ACID + MVCC | concurrent writes + non-blocking scans | open on-disk format |
|---|---|---|---|---|
| DuckDB | ✅ | ✅ | ❌ single writer process | ⚠️ via DuckLake |
| chDB | ⚠️ needs a subprocess | ❌ | ⚠️ | ✅ |
| ArcticDB | ✅ | ❌ dropped by design | ✅ | ✅ |
| Umbra / CedarDB | ❌ server | ✅ | ✅ | ❌ |
| **ChakraDB** | ✅ | ✅ | ✅ | ✅ Arrow IPC parts |

The differentiator is **concurrency, not raw speed**. DuckDB holds a single-writer
file lock — a second writer is refused at the OS level (`IO Error: Could not set
lock`). ChakraDB permits continuous concurrent writers with non-blocking snapshot
reads. It does not aim to out-scan DuckDB; it aims to serve a workload DuckDB
structurally cannot.

---

## How it works

**Storage — Arrow-native, three tiers.** Writes land in an in-memory row buffer
(L0) at memory speed; when it fills it is sealed, sorted by key, into an immutable
columnar **Arrow** part. Parts persist as the open **Arrow IPC** format. Because a
part is written sorted by its key, the ordinal position *is* the row offset — so
there is no key→location map to pay for (the M0-2 result: ~1.25 B/row index cost,
flat with table size).

**Schema — arbitrary, like DuckDB.** Any number of columns, any types, per table.
The primary key can be any type (int, text, float, bool), or a table can declare
no key and get a hidden auto-increment `_rowid` (a keyless table). One idea keeps
the engine simple: every table has exactly one key column; "PK-less" is just a
table whose key is hidden.

**Concurrency — MVCC snapshot isolation.** Writers serialise on one lock; readers
take a snapshot and never block a writer. Cold, unmodified parts pay *zero*
per-row visibility cost on scan. This is the wedge.

**Execution — a dual-engine HTAP router.** Each query goes to whichever engine
fits its shape:

```
   COUNT(*) no filter         → interpreter  (metadata, ~0.01 ms)
   WHERE key = <literal>      → interpreter  (index funnel: bounds→bloom→seek)
   INSERT / UPDATE / DELETE   → interpreter  (owns the WAL + snapshot clock)
   GROUP BY / aggregates      → DataFusion   (vectorised, ~DuckDB-class)
   joins / windows / subquery → DataFusion   (the interpreter can't plan them)
```

DataFusion is the default. The hand-written interpreter is the transactional half
and the zero-dependency fallback.

**Durability.** A write-ahead log with group commit (three durability modes),
crash-safe incremental checkpointing, and recovery that replays the WAL and
rebuilds tables — including their schemas — from the manifest.

---

## Trying it

```bash
cargo test                       # 458 tests (default: DataFusion + HTAP router)
cargo test --no-default-features # 452 tests (lean interpreter-only, zero heavy deps)
./scripts/qa.sh                  # full QA: fmt + clippy + both test profiles
./scripts/qa.sh full             # + 10k crash trials + benchmarks + DuckDB compare
```

Arbitrary-schema SQL, durable across a crash:

```rust
use chakradb::{storage::{Storage, StorageConfig}, io::MemIo, SqlEngine};
use std::sync::Arc;

let io = Arc::new(MemIo::new());
{
    let db = SqlEngine::durable(Arc::new(Storage::open(io.clone(), StorageConfig::default())?));
    db.run("CREATE TABLE users (email TEXT PRIMARY KEY, age INT)")?;   // text primary key
    db.run("INSERT INTO users VALUES ('alice@x.com', 30)")?;
    db.run("INSERT INTO users VALUES ('bob@x.com', 41)")?;
}
// Reopen after a "crash": the table, its schema, and its data are recovered.
let db = SqlEngine::durable(Arc::new(Storage::open(io, StorageConfig::default())?));
assert_eq!(db.query("SELECT age FROM users WHERE email = 'bob@x.com'")?[0][0], "41");
```

From **Python**, via a standard DB-API 2.0 (PEP 249) driver — works like
`sqlite3` (`bindings/python/`):

```python
import chakradb
con = chakradb.connect("./mydb")             # a directory; ":memory:" for ephemeral
cur = con.cursor()
cur.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
cur.execute("INSERT INTO t VALUES (?, ?)", (1, "alice"))
print(cur.execute("SELECT name FROM t WHERE id = ?", (1,)).fetchone())   # ('alice',)
```

Or in-memory, straight to the storage API:

```rust
use chakradb::{Database, Row, Value};

let db = Database::new();
let users = db.create_table("users")?;                 // default (pk,a,b,c) schema
users.insert(Row::new(1, 100, 1.5, "alice"))?;

let before = db.snapshot();                            // snapshots are stable under writes
users.update(Row::new(1, 999, 1.5, "alice-v2"))?;
assert_eq!(users.get(&Value::Int(1), before).unwrap().c(), "alice");
assert_eq!(users.get_latest(&Value::Int(1)).unwrap().c(), "alice-v2");
```

---

## What's proven (with the harness that produced it)

**The concurrency wedge.** DuckDB refuses a second writer (`Conflicting lock is
held`). On the shipped stack, ChakraDB runs 4 threads issuing **durable,
WAL-logged** `INSERT`s — ~8,900 of them committed *during* the measurement —
while a DataFusion `GROUP BY` runs repeatedly: query p50 degrades just **1.49×**
(2.2 → 3.3 ms), readers never block, and every query sees a stable MVCC snapshot.
(`wedge-bench`; also `m2-bench`, `df-bench`.)

**Analytics at width and scale.** A 105-column ClickBench-shaped table, 1M rows:
ChakraDB + DataFusion lands within ~1–2× of DuckDB on most queries (faster on a
few), with **identical results**. (`cargo run --release --features datafusion
--bin clickbench`; DuckDB half in `scripts/clickbench_duckdb.sh`.)

**Durability.** 10,000 randomized crash trials verify every acknowledged write
survives, in all durability modes. Durable SQL adds **30,000 more crash trials
(~4.3M acknowledged writes verified)** across int, text, and keyless-rowid
schemas — every acked write recovered exactly. (`crash_consistency`,
`durable_sql_crash`.)

**Index cost.** ~1.25 B/row, flat with table size — the sorted key column *is* the
index, so no per-row key→location map exists. (`m0-bench`.)

Two data-loss bugs and two query-correctness bugs were found by testing rather
than inspection, and are regression-tested.

---

## Design principles

1. **Concurrency is the wedge.** ChakraDB competes on serving writes-plus-analytics
   on live data, which DuckDB structurally cannot — not on out-scanning it.
2. **Buy execution, build storage.** DataFusion supplies the vectorised engine;
   the hours go into storage, MVCC, and durability. The interpreter remains as the
   transactional path and the zero-dependency build.
3. **Some seams cannot be retrofitted.** `trait Io`, `trait Clock`, and a seeded
   RNG exist from the start, before anything needs them.
4. **Publish the harness, not just the number.** No neutral benchmark measures the
   concurrency axis this project competes on, so every performance claim ships
   with the code that produced it. A number without a source is a hypothesis.

Every figure here is single-machine, single-run, and unaudited; the design
reasoning does not depend on any of them.

---

## Layout

```
docs/
  requirements.md              Architecture & design specification (v2.0)
  roadmap.md                   Milestones, decision gates, stop conditions
  arrow-schema-migration.md    The Arrow-native + dynamic-schema rewrite
  m3-datafusion-spike.md       Adopting DataFusion behind the scan boundary
  m0/m1/m2-findings.md          Point-in-time milestone records (historical)
src/                           Engine: storage, MVCC, WAL, SQL, DataFusion bridge
bindings/python/               DB-API 2.0 (PEP 249) driver — works like sqlite3
tests/                         20 integration suites + SQLancer/sqllogictest oracles
scripts/                       qa.sh + DuckDB comparison drivers
```

Foreign keys are an explicit non-goal — referential integrity is the
application's business.

## License

Apache-2.0.
