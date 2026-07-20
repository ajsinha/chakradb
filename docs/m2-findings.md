# M2 Findings — Query Layer & Buffer Pool

**Milestone:** M2 (see `roadmap.md`)
**Status:** Substantially complete; **Gate 2 cannot be fully evaluated in this
environment** (no DuckDB — see §6).
**Verdict:** the storage wedge holds under the SQL layer; the decisive DuckDB
comparison is deferred, not passed.
**Artifacts:** `src/` (~9,700 lines), 444 tests, `m2-bench`.

---

## 1. What M2 set out to do, and what actually happened

M2 has two halves. The **structural** half — demand-paged parts (FR-06b), a real
filesystem backend, incremental checkpoint — is done and measured. The **query**
half — a SQL surface, a conformance harness, and the NFR-03 measurement — is
done for our subset. What M2 *cannot* do here is the one thing Gate 2 hinges on:
compare against DuckDB, which is not installed. That is stated plainly below
rather than worked around.

### How to read the numbers

Single machine, single run, in-memory I/O. The harness is committed. Absolute
query latencies reflect a deliberately simple interpreter (row-at-a-time,
string-rendered) and are a floor, not a ceiling — see §5.

---

## 2. FR-06b: closed (the M1 carry-forward)

M1 split FR-06 because it loaded every part eagerly, so total recovery scaled
with database size. M2 closes FR-06b.

Part files now begin with a small **summary frame** (id, row count, bounds,
version range). Open reads only summaries; column data faults in on first touch
(`pager.rs`). Measured:

| checkpointed rows | reopen | column bytes read at open |
|---|---|---|
| 100,000 | 0.02 ms | **0** |
| 400,000 | 0.03 ms | **0** |
| 1,600,000 | 0.06 ms | **0** |

Reopen is flat and reads **zero column bytes** regardless of size. The honest
residual is that reopen rises slightly with part *count* (more summary frames),
which is O(parts), not O(rows).

**The documented trade** (tested in `lazy_open.rs`): a WAL tail with mutations
forces warming, because a logged DELETE of a key in a sealed part must tombstone
that part's row, which a summary cannot do. So lazy open pays off after a *clean*
checkpoint — the common case FR-06b targets. Per-part deferral is possible but
needs a fallible read path on `Table`; deferred.

---

## 3. A defect fixed in passing: O(n²) checkpoint

Found while benchmarking FR-06b. Checkpoint rewrote **every part on every call**,
so a run of *k* checkpoints over a growing database was O(k·parts) — quadratic.
This predated M2; it was in M1's storage and the short M1 soak never ran long
enough to expose it.

Fixed by dropping generation-versioning. Part files are named by `(table,
part_id)` and written **once**. This is crash-safe because a part is immutable
except for *appended* tombstones (appends are self-checksumming; a torn tail is
discarded), and new data always gets a fresh monotonic id, so a file is never
rewritten in place. Checkpoint is now incremental: skip unchanged parts, append
new tombstones, write only brand-new parts. The ~700 crash trials still pass, so
this survives crashes mid-append.

A related MemIo artifact was fixed too: `sync` cloned the whole file image every
call, making bulk benchmarks quadratic. It now skips the clone when nothing
changed. A `1.6M`-row seed+checkpoint went from >100 s to **895 ms**.

---

## 4. The SQL layer (M2-1, M2-2)

A real SQL surface over the storage engine, parsed with `sqlparser` under
`PostgreSqlDialect` (§9 — never `GenericDialect`). Four small modules: values
(three-valued logic), expression evaluation, planning, execution.

**Scope, stated as a subset, not a claim.** Supported: `CREATE TABLE`, `INSERT`,
`UPDATE`, `DELETE`, and single-table `SELECT` with projection, `WHERE`,
`GROUP BY`, aggregates (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`), `ORDER BY`, `LIMIT`,
`DISTINCT`, and arithmetic incl. `%`. Rejected with a clear error rather than
mis-executed: joins, subqueries, and any schema beyond the fixed M0 four columns.
This is the §9 position — parsing is 5% of compatibility; the type system and
function library are where it is won, and those are deferred.

**M2-1 — conformance harness** (`sqllogic.rs`). The roadmap points at
`apache/datafusion-testing`'s 595 `.slt` files, but those assume a general schema
and would only exercise our `CREATE TABLE` rejection. So the harness runs the
*same file format* (the SQLite sqllogictest grammar — `statement ok/error`,
`query <types>`, result blocks, `halt`, comments) over a corpus written to our
subset. The parser and directives are real; the corpus is version-controlled.

**M2-2 — property/metamorphic testing** (`sql_property.rs`). Real SQLancer drives
an engine over JDBC, which M2 has no wire protocol for. What it *can* do is run
SQLancer's *oracles* directly in-process — the same wrong-answer checks without
the transport:

- **TLP**: for any predicate `p`, rows matching `p`, `NOT p`, and `p IS NULL`
  partition the table exactly. Checked over 25 seeds × 12 predicates.
- **NoREC-style**: the `WHERE p` count equals a manual count of rows where the
  projected `p` is true.
- Double-negation stability, `MIN ≤ AVG ≤ MAX`, `LIMIT` as an order prefix,
  distinct-count bounds, delete arithmetic.

These caught two real bugs during development: `%` was unsupported (the TLP
predicate pool used it), and the join-rejection check missed joins expressed as a
single `from` entry with populated `.joins`. Both fixed.

**The design's three-valued logic is exercised hard**, because it is the most
common source of silently-wrong answers. `a = NULL` yields no rows; `false AND
NULL` is false but `true AND NULL` is NULL; `NOT NULL` is NULL. All tested.

---

## 5. M2-3 — NFR-03 through the SQL layer

NFR-03 is the requirement the project exists for. An aggregate query
(`SELECT COUNT(*), SUM(a), AVG(b) FROM hits WHERE a > 500`) over 200,000 rows,
40 runs per phase:

| phase | p50 | p99 | max |
|---|---|---|---|
| idle | 19.9 ms | 22.0 ms | 22.0 ms |
| under sustained write load | 42.6 ms | 70.9 ms | 70.9 ms |

**Degradation ratio: 2.14×**, measured while **346,753 concurrent upserts** were
applied *during* the queries. Readers never blocked and never saw a torn result.

**Read this honestly.** 2.14× is not "barely affected" — it is a real cost, from
L0 growth under heavy churn and from the brief write-lock the scan takes to
capture its part list. What it demonstrates is the *shape* NFR-03 asserts:
readers degrade gracefully and continue, rather than serialising behind writers.
Whether that is *decisively better than DuckDB* is exactly the comparison M2
cannot make here (§6). The number is consistent with M0's storage-layer result
(1.38–3.77× depending on compaction), which is reassuring but not a substitute
for the head-to-head.

Cold query baseline (no DuckDB comparison — a reference for later):

| query | p50 |
|---|---|
| `COUNT(*)` | 23.8 ms |
| filtered aggregate | 18.9 ms |
| `GROUP BY` | 40.6 ms |
| `ORDER BY … LIMIT` | 37.1 ms |
| `DISTINCT` | 40.6 ms |

These are the M2 interpreter's numbers — row-at-a-time and string-rendered. §8 of
the spec anticipates replacing this with DataFusion behind the `scan` boundary if
execution becomes the bottleneck; DataFusion 54.0.0 is verified to build here
(761 deps, ~2.6 GB target, 23 s incremental). The interpreter is a floor.

---

## 6. Gate 2 cannot be closed in this environment

**This is the most important statement in the document.** The roadmap's Gate 2 is:

> Proceed if NFR-03 shows a **decisive win over DuckDB** … Reconsider seriously if
> the result is parity.

**DuckDB is not installed here, and M2-4 is defined as a comparison against it.**
So:

- **M2-3** is measured *in absolute terms* — readers stay usable under 346k
  concurrent upserts — but not *relative to DuckDB*, which is the criterion.
- **M2-4** (cold ClickBench/TPC-H within 2× of DuckDB) is **not measured at all.**

What can be said from evidence already gathered (documented in `requirements.md`
§1.3, §10.3): ClickBench structurally exempts every embedded engine from its
concurrency test, DuckDB holds a single writer lock by design, and no embedded
engine has demonstrated this axis. That is the *argument* for the wedge; it is
not the *measurement* Gate 2 asks for.

**Recommendation.** Do not record Gate 2 as passed. Record it as **pending a
DuckDB comparison**, which requires installing DuckDB (and, for M2-4, the
ClickBench dataset). Everything needed to run it — the SQL layer, the NFR-03
harness — is in place; only the baseline is missing. This is an environment
limitation, not a design gap.

---

## 7. Test coverage

444 tests, all passing, ~8 s wall clock (excluding the opt-in 10k-crash and soak
suites).

| Area | Suites | Focus |
|---|---|---|
| SQL semantics | `sql/value`, `sql/expr` (unit) | Three-valued logic, coercion, NULL ordering |
| SQL planning | `sql_planning.rs` | Subset parsing; rejection of joins/subqueries |
| SQL execution | `sql/exec` (unit), `sql/mod` (unit) | Filter/aggregate/group/order/limit/distinct |
| Conformance | `sqllogic.rs` | sqllogictest-format corpus over the subset |
| Wrong-answer oracles | `sql_property.rs` | TLP, NoREC-style, metamorphic relations |
| Buffer pool | `lazy_open.rs` | FR-06b: zero column bytes at open; the warming trade |
| Real I/O | `posix` (unit) | Directory fsync, short reads/writes, traversal rejection |
| Fan-out | `hostile_keys.rs` | Bloom carries the funnel under overlapping ranges |
| (carried) | crash/wal/storage/mvcc/etc. | All M0–M1 suites still green |

---

## 8. Gate 2 assessment

| # | Criterion | Result |
|---|---|---|
| M2-1 | sqllogictest passing | ✅ Harness + subset corpus green |
| M2-2 | SQLancer oracles, no wrong-answer bugs | ✅ **In-process** TLP/NoREC oracles; found and fixed 2 bugs. ⚠️ Not the JDBC-driven SQLancer — no wire protocol |
| M2-3 | NFR-03 vs DuckDB | ⚠️ **Measured absolutely** (2.14× under 346k upserts, readers never block); **not vs DuckDB** (not installed) |
| M2-4 | Cold ClickBench/TPC-H within 2× of DuckDB | ❌ **Not measured** — no DuckDB, no dataset |
| M2-5 | Memory-pressure per operator | ⚠️ Our interpreter has no spilling operators to fail; the DataFusion hazards (§ roadmap) are unexercised because DataFusion is not yet integrated |

**Verdict.** The query layer works and the storage wedge survives it. But
**Gate 2 is genuinely undecided**: its decisive criterion is a DuckDB comparison
this environment cannot run. Proceeding to M3 on the strength of the absolute
NFR-03 number would repeat exactly the mistake the roadmap warns against —
treating "it works" as "it wins". 

### Required before Gate 2 can be called

1. **Install DuckDB** and run M2-3 as a head-to-head under identical write load.
2. **Install DuckDB + ClickBench** and run M2-4.
3. Only then decide proceed-vs-reconsider.

### Carried into M2's remainder / M3

- **DataFusion integration** behind the `scan` boundary (M2-5's hazards —
  non-spilling hash joins, unbounded window functions — only become real once
  DataFusion is the executor; until then M2's interpreter simply lacks those
  operators).
- **The 6-hour soak** still outstanding from M1.
- **Backpressure tuning** still outstanding from M1.
