# ChakraDB — Development Roadmap

**Version:** 1.0
**Companion to:** `requirements.md` (Architecture & Design Specification v2.0)
**Status:** M0–M2 complete and their gates passed; the vectorised-executor work
(originally the "M3 spike") shipped; **M4 hardening is in progress**. The
lakehouse-publication milestone (also numbered "M3" below) is not started. The
plan below is the original sequencing and gates.

> **Current state (supersedes the per-milestone status below).**
> - **M0 (storage risk spike)** — done. Sorted-part index (~1.25 B/row), MVCC,
>   compaction; two-phase merge fix. See `m0-findings.md`.
> - **M1 (durability)** — done. WAL + group commit, crash recovery, checkpointing;
>   10k crash trials (the 6-hour soak remains a proxy, not yet run). See
>   `m1-findings.md`.
> - **M2 (query layer)** — done, **Gate 2 passed**. SQL surface, sqllogictest +
>   SQLancer oracles, demand-paged parts, real POSIX backend; DuckDB v1.5.4
>   installed and the head-to-head run. See `m2-findings.md`, `gate2-results.md`.
> - **Vectorised executor (the "M3 spike", promoted and shipped):** **DataFusion**
>   is the default analytical executor behind a cost-based **HTAP router**, the
>   interpreter kept as the transactional path (`m3-datafusion-spike.md`).
>
> **Delivered beyond the original plan** (none of these live in the milestone
> bodies below):
> - **Arrow-native storage** with **arbitrary schemas / any-type keys / keyless
>   tables** (`arrow-schema-migration.md`).
> - **Durable SQL** — the SQL front end writes through the WAL.
> - **ACID transactions** — `BEGIN`/`COMMIT`/`ROLLBACK`, snapshot isolation,
>   crash-atomic commit, first-committer-wins conflict detection.
> - **Constraints** — `NOT NULL`, `DEFAULT`, `CHECK`, `VARCHAR(n)` length.
> - **Types** — `DATE`, `TIMESTAMP`, and exact `DECIMAL(p,s)` (Arrow Decimal128).
> - **Zonemap part pruning** on `WHERE` ranges.
> - **`COPY FROM`** bulk CSV ingest.
> - **Single-writer directory lock** (C-1), enforced via `File::try_lock`.
> - ClickBench-shaped validation at 105 columns, 100k–10M rows: identical results
>   to DuckDB, winning the big `COUNT(DISTINCT)`s at 10M (`clickbench-findings.md`).
> - Full SQL surface documented in `sql-reference.md`.
>
> - **Next (M4 in flight):** ✅ no-panic public-API audit + overflow hardening,
>   ✅ single-writer lock, ✅ `COPY` bulk ingest, ✅ docs pass. Remaining: publish
>   the Python wheel to PyPI, a streaming DataFusion `TableProvider`, and the
>   lakehouse-publication milestone (M3 below) if the product direction calls for
>   it.
>
> The milestone plan below remains a useful record of the *reasoning and gates*;
> the ordering held, and the "buy execution, build storage" bet paid off.

> **On estimates.** Durations assume **1–2 experienced Rust engineers working consistently**.
> They are calibration, not commitments — and they are the least reliable content in this
> document. The *ordering* and the *gates* are the parts that matter; if you disagree with a
> duration, change it. If you disagree with the ordering, read §0 first.

---

## 0. Principles

Four rules that determine the sequence. Everything else follows from them.

**1. Risk-first, not layer-first.** Build the thing that kills the project earliest. The
tempting order — storage, then SQL, then bindings — front-loads work that is *knowable* and
defers the work that is *uncertain*. v1.0 of the spec made exactly this mistake: it scheduled
FFI and multi-language bindings as P0 while the storage engine was one paragraph.

**2. Every milestone ends at a decision gate.** Each gate has an explicit criterion and an
explicit *stop* condition. A gate you cannot fail is not a gate.

**3. Some seams must exist from day one because they cannot be retrofitted.** Specifically the
`Io`/`Clock`/RNG abstraction (§11.1 of the spec). Adding it after compaction threads, a buffer
pool, and a table-format layer all call ambient APIs directly is a rewrite, not a refactor.

**4. Correctness infrastructure ships alongside the feature, never after.** No storage feature
is done until it has crash-test coverage. Retrofitting a test harness onto a working engine is
how you discover the engine was never testable.

---

## M0 — Kill the Biggest Risk ✅ **COMPLETE**

> **Outcome: PROCEED.** See `m0-findings.md` for the full report.
> Headline results: index cost is **1.25 B/row and flat** with table size
> (≈1.25 GB at 1B rows); point-lookup latency stays flat 1.1–1.7 µs as parts
> grow 1→33; scan degradation under sustained write load is **3.77× without
> compaction, 1.38× with it**.
>
> **Two defects found, both must be fixed before M1 work starts:**
> 1. Compaction holds the table write lock for the entire merge, collapsing
>    write throughput 18×. Build the replacement outside the lock.
> 2. M0-3 assumed selective min/max bounds; re-run with a hostile key
>    distribution before trusting the fan-out result.
>
> **Both were addressed in M1** — see `m1-findings.md` §2 and §2b.


**Goal:** answer one question before writing a real system.

> *Can we sustain high-rate keyed updates while scanning, at an acceptable PK-index memory
> footprint, without scan performance collapsing?*

**Estimate:** 3–6 weeks. **Actual:** delivered as ~2,900 lines across 15 modules
with 224 tests, zero dependencies.

### Scope

A deliberately throwaway prototype. Hardcoded schema, in-memory only, no SQL, no persistence,
no table format, no crash safety.

**Build:**
- Hardcoded schema (say `(i64 pk, i64 a, f64 b, string c)`).
- `trait Io`, `trait Clock`, seeded RNG seam — **even though M0 is in-memory.** These exist now
  because they are unretrofittable later, not because M0 needs them.
- L0 row buffer → seal → L1 sorted columnar parts. *(Built with a plain
  struct-of-vectors layout rather than Arrow — see `m0-findings.md` §6.)*
- PK index: per-part sorted index where **ordinal == row offset**, plus a bloom filter and
  part-level min/max bounds.
- MVCC: commit sequence numbers, per-part visibility, deletion vectors.
- Point UPDATE / DELETE by PK using the four-stage lookup funnel (bounds → bloom → ordered
  seek → DV recheck), searching parts newest-first.
- Compaction, simplest viable policy — triggered on part count and DV density.
- A scan that applies deletion vectors and hands out columnar batches.
- A load generator: N writer threads doing keyed updates, M reader threads scanning.

**Explicitly out of scope:** WAL, durability, SQL, DataFusion, Parquet, table formats,
recovery, joins, statistics, spilling, bindings.

*(Multi-table support was added mid-milestone at the project owner's direction —
`Database` holds many `Table`s over one shared CSN generator. It cost ~200 lines
and would have been considerably more expensive to retrofit after M1's WAL.)*

### Deliverables

1. The prototype.
2. A benchmark harness producing the four numbers below, reproducibly.
3. A written findings memo — **including negative results.**

### Acceptance criteria (measure, don't assert)

| # | Measurement | Why it matters |
| :--- | :--- | :--- |
| **M0-1** | Scan throughput on idle data vs. under sustained keyed-update load; report the **ratio** and the full latency distribution | This is NFR-03, the project's reason to exist |
| **M0-2** | Resident PK-index memory at 1M / 10M / 100M rows, plus extrapolation to 1B | Determines max practical table size |
| **M0-3** | Point-update p50/p99/p999 latency vs. **part count** — sweep it deliberately | Lookup fan-out is the cost we accepted for the cheap index; this finds where it bites |
| **M0-4** | Scan cost on a part with zero recent mutations | Should be ~free. If the version machinery leaks into cold scans, §5.3's core claim is wrong |

### 🚦 Gate 0

**Proceed if:** scans degrade gracefully (not catastrophically) under write load; index memory
extrapolates to a table size you're willing to support; cold scans pay no measurable version
cost.

**Stop or pivot if:** index memory makes the target table size impossible — evaluate
**ArcticDB's trade** (drop transactions, partition concurrency by key range) before continuing.
Or if scan-under-write degradation is severe and traces to something architectural rather than
to unimplemented compaction.

> **This is the cheapest possible moment to be wrong.** Do not rationalize a bad M0 result.
> Nothing downstream improves it.

---

## M1 — Durable Single-Table Engine ✅ **COMPLETE**

> **Outcome: PROCEED** — 3 of 5 criteria met as written, 2 with stated caveats.
> See `m1-findings.md` for the per-criterion assessment.
>
> **Outstanding:** the 6-hour soak (M1-3) is runnable but has not been run —
> `CHAKRA_SOAK_SECS=21600 cargo test --release --test soak`. It should gate M2's
> completion rather than block its start. FR-06 (M1-2) was split rather than met.
>
> WAL with group commit (8× fsync batching at 16 threads), crash-safe
> checkpointing via generation-versioned parts, recovery, and backpressure.
> ~700 seeded crash injections pass. The M0 compaction defect is fixed:
> the write-throughput cost of compaction fell from **18.4× to 1.56×**.
>
> **One requirement reclassified.** FR-06 was written more broadly than the
> mechanism it named could deliver. Log *replay* is flat at 1.4 ms across a 20×
> database growth (met). *Total* recovery still scales, because M1 loads every
> part eagerly — fixing that needs the M2 buffer pool. Split into FR-06a/FR-06b.
>
> **A second data-loss bug was found by the crash suite on its first run:**
> truncating the WAL at checkpoint left a stale durable watermark, so subsequent
> commits skipped their fsync and were lost on crash. Fixed and regression-tested.

**Goal:** make M0 survive power loss, and make compaction hold under sustained load.

**Estimate:** 8–12 weeks. **Actual:** delivered with 335 tests.

### Scope

- **WAL with group commit**, and the three durability modes (`sync` / `group` / `async`) named
  honestly. Group commit is the default.
- **Crash recovery** — bounded by WAL tail size, **not** database size. This is FR-06 and it is
  a hard constraint: if recovery scales with data volume, the design is wrong.
- **Persistent PK index** — falls out nearly free, since the index lives inside the immutable
  part file that had to be written anyway.
- **Compaction as a designed subsystem:** trigger policy (DV density, part count, age),
  resource budget, crash-safety (a crash mid-compaction leaves old parts authoritative), and
  **explicit ingest backpressure with observable metrics** when compaction debt grows.
- **Buffer pool with explicit I/O.** Thread pool over `pread`/`pwrite`. No mmap on the durable
  path. No io_uring yet.
- **Userspace I/O scheduler** — priority classes (compaction vs. foreground scan vs. WAL
  flush), bounded in-flight depth, inferred device queue depth. *This is where the
  tail-latency wins live and it is interface-independent.*
- **Crash-testing harness**, built in this milestone: redb-style `CrashBackend` modelling
  `live`/`durable`/`last_header` with a freeze-at-Nth-sync crash point.

### Acceptance criteria

| # | Criterion |
| :--- | :--- |
| M1-1 | Survives ≥10,000 randomized `kill -9` injections with no data loss beyond the declared durability mode |
| M1-2 | Recovery time bounded by WAL tail; **demonstrated flat as database size grows 10×** — ⚠️ *split into FR-06a (met) / FR-06b (M2)*; see `m1-findings.md` §3 |
| M1-3 | Sustained ingest for ≥6 hours with compaction at equilibrium and no unbounded part growth |
| M1-4 | Backpressure engages *before* scan performance degrades, and is visible in metrics |
| M1-5 | p99 write latency under concurrent scan load, per durability mode, documented |

### 🚦 Gate 1

**Proceed if:** recovery is correct under injected crashes and compaction is stable under
sustained load.

**Stop if:** compaction cannot keep up without unacceptable backpressure. That would mean the
§3 cost model doesn't balance, and no amount of query-layer work fixes it.

---

## M2 — Query Layer ✅ **COMPLETE — GATE 2 PASSED**

> **Outcome: passed.** See `m2-findings.md` and `gate2-results.md`.
> Done: demand-paged parts (FR-06b — reopen flat at ~0.02ms, zero column bytes
> read); real `PosixIo`; incremental checkpoint (fixed an O(n²) that predated
> M2); a SQL layer (`sqlparser` + interpreter) with a sqllogictest harness (M2-1)
> and in-process SQLancer oracles (M2-2, found 2 bugs). NFR-03 measured through
> SQL: **2.14× degradation under 346k concurrent upserts, readers never block** —
> a workload DuckDB refuses at the OS lock.
>
> **Gate 2 was evaluated against DuckDB v1.5.4 and passed.** The cold-scan half
> was originally 14–82× behind on the *interpreter* (`gate2-results.md`); that gap
> is closed by the vectorised executor (DataFusion), now within ~1–2× on a
> 105-column ClickBench-shaped table with identical results — and *ahead* on the
> big `COUNT(DISTINCT)`s at 10M rows (`clickbench-findings.md`). The concurrency
> wedge — the axis the project actually competes on — DuckDB structurally cannot
> match.
>

**Goal:** become a database rather than a storage engine — and get the first honest comparison
against DuckDB.

**Estimate:** 10–14 weeks.

### Scope

- **Demand-paged buffer pool** — carried from M1 and now the highest-value
  structural work. Prerequisite for FR-06b: parts must be openable without being
  fully resident, or time-to-first-query keeps scaling with database size.
- **A real `PosixIo`.** Everything through M1 runs on `MemIo`. The seam is
  exercised, but no test has touched a real filesystem, so real fsync ordering
  and partial-write behaviour are unverified.
- **Tune backpressure constants** against a real workload; the current ramp shape
  is right but the numbers are guesses (`m1-findings.md` §6).
- **Multi-hour soak in CI** with memory tracked across the run.
- **DataFusion integration** via a custom `TableProvider`, behind the narrow
  `scan(snapshot, projection, filters) → Stream<RecordBatch>` boundary.
- **Fork DataFusion and set up the rebase cadence.** Not optional — GreptimeDB, Spice.ai,
  ParadeDB and InfluxData all carry pinned forks. Budget the ~2.5-month major-release treadmill
  from the start.
- SQL surface: `sqlparser` with **`PostgreSqlDialect`** (never `GenericDialect` — it is a
  permissive union that accepts SQL no real database accepts and can parse valid SQL into a
  *different* tree).
- Multi-table, DDL, catalog, type system, transactions across tables.
- Projection/filter pushdown into our scan; statistics for the planner.
- **sqllogictest + SQLancer in CI.**

### Known hazards to handle explicitly

- **`HashJoinExec` does not spill** — it returns `ResourcesExhausted`. Document it, and test the
  `SortMergeJoinExec` path (which does spill) as the mitigation.
- **Window functions take no memory reservation at all** and can grow unbounded. In an embedded
  library that means **OOM-ing the host process**. Test under memory pressure early; this is
  arguably the single most dangerous inherited behavior.

### Acceptance criteria

| # | Criterion |
| :--- | :--- |
| M2-1 | sqllogictest passing on the documented subset (start from `apache/datafusion-testing` — 595 pre-converted `.slt` files, Apache-2.0) |
| M2-2 | SQLancer running TLP/NoREC/PQS oracles in CI with no unexplained wrong-answer bugs |
| M2-3 | **NFR-03 measured against DuckDB**: scan throughput under concurrent write load |
| M2-4 | NFR-04 measured: cold ClickBench / TPC-H, target within 2× of DuckDB |
| M2-5 | Memory-pressure behavior documented for every operator, especially joins and windows |

### 🚦 Gate 2 — the real go/no-go

**This is the milestone that validates or invalidates the project.**

**Proceed if:** NFR-03 shows a decisive win over DuckDB — concurrent readers substantially
unaffected by sustained write load, where DuckDB serializes or degrades.

**Reconsider seriously if:** the result is parity. Parity means we built a slower DuckDB with
more moving parts. Do not proceed to M3 on the hope that lakehouse compatibility rescues it.

---

## M3 — Lakehouse Publication  ⏸️ **NOT STARTED**

> **Naming note.** Two different things got called "M3." The **DataFusion
> executor spike** (`m3-datafusion-spike.md`) was informally promoted to "M3
> proper" and *shipped* — it's the default analytical engine now. **This** M3, the
> original one, is **lakehouse publication** (open-format Delta output) and is
> **not started**. There is no Parquet/Delta writer in the tree; the on-disk part
> format is Arrow IPC, readable but not yet a published table format. Whether to
> build it at all depends on §15 Q5 and the DuckLake competitive reality below.

**Goal:** make the on-disk state a valid open table that external engines can read.

**Estimate:** 8–12 weeks.

### Scope

- Table-format abstraction behind a trait.
- **Delta first.** Its deletion vectors are 64-bit portable RoaringBitmaps at physical row
  positions — *the same representation §5.3 already uses internally*, so no translation layer.
  `delta-kernel-rs` is the most credible Rust implementation.
- Two-level commit: publication scheduling, configurable interval, crash-safe publication.
- Correct `numRecords` / `cardinality` bookkeeping and the `tightBounds` flag — get this wrong
  and external readers produce wrong results.
- Multi-process readers against published state.
- **External interop tests**: Spark and DuckDB read our tables, including deletion vectors, and
  results must match ours.

### Acceptance criteria

| # | Criterion |
| :--- | :--- |
| M3-1 | Spark and DuckDB read our published tables correctly, deletion vectors honored |
| M3-2 | Publication interval configurable; staleness bounded and observable |
| M3-3 | Crash during publication leaves the last published snapshot valid and readable |
| M3-4 | Ingest throughput impact of publication quantified across interval settings |

### 🚦 Gate 3

**Proceed if:** external engines read our output correctly and publication doesn't materially
harm ingest.

**Note the competitive reality:** DuckLake v1.0 (April 2026) already does open-format plus
data inlining. This milestone is **table stakes, not differentiation.** Size the investment
accordingly — and if §15 question 5 resolves to "users need continuously-valid on-disk state,"
revisit the architecture *before* starting this milestone, not during it.

---

## M4 — Hardening  🔨 **IN PROGRESS**

> **Status.** Underway. Done so far: a no-panic public-API audit with the
> overflow paths hardened (date/time parsing, negation); the single-writer
> directory lock (C-1); `COPY` bulk ingest; `DECIMAL` precision enforcement; an
> **operational-metrics surface** (`Storage::stats() -> StorageStats`); the
> operating-envelope + limitations docs (§2.2, README); and the SQL-surface
> reference (`sql-reference.md`). The **GC-watermark** gap is **fixed**: a
> live-snapshot registry (`Database::pin`/`gc_horizon`) holds compaction's
> reclamation horizon back to the oldest pinned reader, so compacting while a
> long-running query or transaction is in flight never reclaims a version it can
> still see (`tests/gc_watermark.rs`). Remaining: the multi-day soak (the 6-hour
> M1-3 run), file-format versioning policy, backup/restore, and publishing the
> Python wheel to PyPI.

**Goal:** make it something a stranger can run in production.

**Estimate:** 8–12 weeks.

### Scope

- Multi-day soak tests under mixed read/write load.
- Memory-pressure behavior and documented limits.
- Operational metrics: compaction debt, backpressure state, publication lag, snapshot age.
- **GC watermark management** — max snapshot age with query cancellation. A long-running query
  pinning the watermark is how Postgres gets bloat; it needs a documented mitigation, not a
  discovered one.
- Error taxonomy; no panics across API boundaries.
- File-format versioning and a compatibility policy.
- Documentation: the supported SQL subset, durability semantics, staleness semantics, limits.
- Backup/restore.

### Acceptance criteria

| # | Criterion |
| :--- | :--- |
| M4-1 | 72-hour soak with no leaks, no unbounded growth, no degradation |
| M4-2 | Every documented limit has a test that hits it and fails cleanly |
| M4-3 | Zero panics reachable from the public API (fuzzed) |
| M4-4 | Miri clean on `unsafe` paths; ASan/TSan clean in CI |

---

## M5 — Reach

**Only after M4 is solid.** Each item is independently schedulable; none is a prerequisite for
another.

| Item | Notes |
| :--- | :--- |
| **Secondary indexes** | Deferred indefinitely; see §2.1 non-goals. Primary-key indexing is the engine's organising principle |
| **Python bindings** | PyO3 + Arrow PyCapsule. Largest audience, most direct DuckDB comparison. Transfer is O(1) in rows but O(schema width) — batch above ~30 rows; by 8k rows FFI overhead is irrelevant |
| **Windows** | Doubles the I/O and filesystem test matrix |
| **Iceberg** | ⚠️ Re-verify `iceberg-rust` maturity first |
| **C ABI** | Designed for throughout; ship when someone needs it |
| **Java** | Panama FFM. Largest surface area, smallest incremental audience |

---

## Cross-Cutting Tracks

These run continuously rather than as milestones.

### Correctness (starts M0, never stops)

| Layer | Tool | From |
| :--- | :--- | :--- |
| Trait seams for simulation | `trait Io` / `Clock` / seeded RNG | **M0** |
| Crash consistency | redb-style `CrashBackend` | M1 |
| Storage-engine simulation | Seeded, replayable; fault + latency injection | M1 |
| Concurrency | `shuttle` (PCT scheduling, deterministic replay) | M1 |
| Isolation verification | **Elle** via `elle-cli` — accepts plain `serde_json` output, no Clojure needed. Build from HEAD; the prebuilt jar lags | M1 |
| SQL correctness | `sqllogictest-rs` | M2 |
| Wrong-answer fuzzing | SQLancer (TLP / NoREC / PQS) | M2 |
| Linux-only nightly gate | `dm-flakey` with `drop_writes` first; `dm-log-writes` if barrier fidelity is needed | M2 |
| Interop | External-engine read tests | M3 |
| Memory safety | Miri, ASan, TSan | M1 |

### Benchmarking (starts M0)

Establish the harness in M0 and keep it in CI with tracked history. **Publish the harness, the
raw numbers, the full latency distributions, and the configuration.** No neutral leaderboard
measures the axis we win on — ClickBench structurally exempts embedded engines from its
concurrency test — so we are marking our own homework and the only defense is transparency.

---

## What Would Make Us Stop

Written down now, while it's cheap to be honest:

1. **M0:** PK-index memory makes the target table size impossible, and the ArcticDB trade
   (drop transactions) isn't acceptable either.
2. **M1:** Compaction can't keep up without backpressure that defeats the ingest goal.
3. **M2:** NFR-03 shows parity with DuckDB rather than a decisive win.
4. **Any time:** a competitor ships embedded transactional HTAP. DuckDB's Quack protocol
   (multi-writer, beta, stable targeted for v2.0) is the one to watch.
5. **Any time:** we cannot find a real user whose workload needs this. §15 question 6 —
   "what is the actual target workload?" — remains unanswered, and it should not stay that way
   through M2.

---

## Sequencing Rationale

Why this order, briefly:

- **PK index and MVCC before durability** because if they don't work, durability is wasted work.
- **Durability before SQL** because a query layer over an engine that loses data is a demo.
- **I/O priority scheduler before io_uring** because that's where the tail-latency wins actually
  are — ScyllaDB got up to 55% p99.9 reduction from a userspace scheduler, while io_uring itself
  delivered single digits in the most favorable environment it will ever have. And io_uring is
  off by default on RHEL and blocked by Docker's default seccomp profile, so the thread-pool
  path is the common path regardless.
- **SQL before lakehouse** because Gate 2 is the real go/no-go and we should reach it fast.
- **Everything before bindings** because bindings over a nonexistent core produce an impressive
  demo that cannot become a product. This is precisely the mistake v1.0 made.
