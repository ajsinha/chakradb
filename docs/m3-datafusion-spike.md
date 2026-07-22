# M3 Spike — DataFusion over a ChakraDB MVCC snapshot

> **⚠️ Historical record.** This spike answered its question — yes, buy the
> vectorised executor — and was **promoted and shipped**: DataFusion is now the
> default analytical engine behind the HTAP router. Two details here are since
> superseded: (1) the bridge no longer materialises the snapshot into a `MemTable`
> up front — it is **zero-copy**, since sealed parts already *are* Arrow
> RecordBatches handed over by `Arc` clone (`arrow-schema-migration.md`); and
> (2) the "fixed 4-column shape" is gone (arbitrary schemas shipped). The measured
> numbers below are preserved as the point-in-time record. A streaming
> `TableProvider` with filter/projection pushdown remains future work.

**Status:** Spike complete, **measured** (not projected).
**Question:** Does a bought vectorised executor (§8) close the analytics gap
without breaking the concurrency wedge?
**Answer:** Yes on both counts. The 16–60× gap collapses to ~1–3× (and beats
DuckDB on two queries), and a DataFusion query holds a consistent snapshot while
writers mutate the table underneath it.
**Artifacts:** `src/datafusion_bridge.rs`, `src/bin/df_bench.rs`
(`--features datafusion`). Same 500k-row dataset as Gate 2.

---

## 1. What was built

DataFusion does *execution*; ChakraDB keeps owning *storage and MVCC*. The seam
is `datafusion_bridge::snapshot_memtable`: it turns a **consistent MVCC snapshot**
of a table into Arrow record batches (one per visible segment), wrapped in a
DataFusion `MemTable`. DataFusion plans and runs over it with no knowledge that
snapshots, deletion vectors, or concurrent writers exist.

The spike deliberately uses `MemTable` (materialise the snapshot's visible rows
into Arrow up front). A production M3 would implement a streaming `TableProvider`
with projection/filter pushdown so pruned columns are never copied — but MemTable
is enough to measure execution speed and prove the snapshot handoff, which is the
point of the experiment.

DataFusion is strictly optional: it lives behind `--features datafusion`, and the
core crate still builds with zero heavy dependencies. Turning the feature on pulls
in ~760 crates and a ~3 min (debug) / ~8 min (release) cold compile.

## 2. Analytics — measured

500k rows, cold, median of 25 runs, pinned cores. DuckDB v1.5.4 for reference
(from `gate2-results.md`); ChakraDB interpreter numbers are the tuned executor
from this branch.

| query | DuckDB | ChakraDB interpreter | ChakraDB + **DataFusion** |
|---|---|---|---|
| `COUNT(*)` | ~0 ms | 0.01 ms (metadata) | 0.2–0.4 ms |
| `SUM(a) WHERE a > 500` | 1 ms | 16 ms | **0.7–1.3 ms** |
| `GROUP BY a` | 1 ms | 50 ms | **2–3 ms** |
| `ORDER BY b LIMIT 100` | 2 ms | 22 ms | **1.4–1.8 ms** |
| `COUNT(DISTINCT a)` | 5 ms | 60 ms | **1.9–2.7 ms** |

**The 16–60× gap becomes ~1–3×.** DataFusion is *faster than DuckDB* on
`ORDER BY … LIMIT` and `COUNT(DISTINCT)` here, and within ~2–3× on `GROUP BY`.
The one place the interpreter still wins is `COUNT(*)`: ChakraDB answers it from
metadata (0.01 ms) while DataFusion scans the MemTable — a metadata `COUNT(*)`
fast path is worth preserving even after adopting DataFusion.

The earlier §8 projection ("2–5× behind DuckDB") was conservative; the measured
result is better. This is a single machine, single dataset, MemTable (not the
streaming provider) — treat it as a strong positive signal, not a final ClickBench
number.

## 3. The concurrency wedge survived the handoff

The decisive test. A snapshot is taken, then four writer threads start upserting.
DataFusion builds its executor over the snapshot and runs the query suite while
the writes land:

> DataFusion saw **500,000 rows** (exactly the snapshot) while **~37,000
> concurrent upserts** were applied to the same table during the queries. The
> snapshot never shifted; writers never blocked.

`assert_eq!(seen, n)` holds every run. This is the whole thesis: snapshot
isolation carried *across the executor boundary*. DuckDB cannot even open the
second writer (`Conflicting lock is held`), so this scenario has no DuckDB column
to compare against — it is a capability, not a speed.

## 4. SQL completeness — for free

Queries the ChakraDB interpreter parse-rejects, run unmodified under DataFusion:

| query | ChakraDB interpreter | DataFusion |
|---|---|---|
| self-join (`hits h1 JOIN hits h2 ON …`) | rejected | runs |
| window (`ROW_NUMBER() OVER (PARTITION BY …)`) | rejected | runs |
| correlated subquery (`WHERE a > (SELECT AVG(a) …)`) | rejected | runs |

This is arguably the larger prize than raw speed: the §9 "type system + function
library is 95% of compatibility" work arrives with the executor rather than being
hand-built.

## 5. What this does and does not settle

**Settles:** the §8 bet is sound. A vectorised executor closes the analytics gap
to the same order of magnitude as DuckDB, the concurrency wedge is preserved
across the boundary, and full SQL comes along. "Why use ChakraDB?" now has a
confident answer: **DuckDB-class analytics on data being written concurrently,
embedded, which DuckDB cannot do at all.**

**Does not settle:**
- MemTable materialises the whole snapshot. The real M3 is a **streaming
  `TableProvider`** with projection/filter pushdown and per-part parallelism, so
  large tables are not copied wholesale into Arrow per query.
- No ClickBench-scale or wide-schema run (still the fixed 4-column shape; see the
  dynamic-schema work).
- Memory hazards (spilling hash joins, unbounded windows — M2-5) are now reachable
  and must be bounded.
- The Arrow conversion cost is paid per query here; a provider that caches or
  streams Arrow would remove it.
- Single machine, single run. Numbers are a signal, not a benchmark submission.

## 6. Recommendation

Promote this from spike to **M3 proper**: replace the MemTable with a streaming
MVCC-aware `TableProvider`, keep the metadata `COUNT(*)` fast path, and re-run
against DuckDB at wider schema/scale. The interpreter stays as the zero-dependency
default; DataFusion is the opt-in performance/compatibility tier.
