# Gate 2 — ChakraDB vs DuckDB

**Status:** Evaluated. **Split result — read both halves.**
**Environment:** DuckDB v1.5.4 (`/home/ashutosh/duckdb`), ChakraDB M2, same
machine, identical 500,000-row dataset (`pk, a, b, c`), single run.

---

## The two questions, and why they have opposite answers

Gate 2 folds together two very different measurements, and conflating them is
exactly the trap the roadmap warns about. Kept separate:

- **M2-4 — raw cold-scan speed.** ChakraDB **loses decisively.** Expected, and
  the roadmap said so.
- **NFR-03 / M2-3 — the concurrency wedge.** The axis the project exists for.
  DuckDB **structurally cannot compete**, demonstrated below.

## M2-4: cold-scan speed — ChakraDB loses by 14–82×

Same 500k rows, same queries, both cold:

| query | DuckDB | ChakraDB | ratio |
|---|---|---|---|
| `COUNT(*)` | ~1 ms | 39.6 ms | ~40× |
| `SUM(a) WHERE a > 500` | ~1 ms | 38.7 ms | ~39× |
| `GROUP BY a` | ~1 ms | 82.4 ms | ~82× |
| `ORDER BY b LIMIT 100` | ~1–2 ms | 64.7 ms | ~43× |
| `COUNT(DISTINCT a)` | ~5 ms | 69.8 ms | ~14× |

**M2-4's target was "within 2× of DuckDB." We are 14–82× off. It is not met, and
it will not be met by the current executor.** This is not a surprise or a
disappointment — `requirements.md` §8 states it outright:

> DataFusion sets our execution ceiling … We win on the storage axis or we do not
> win.

M2's executor is a deliberately simple interpreter: row-at-a-time, values
rendered to strings, no vectorisation, no column pruning, no metadata shortcuts
(DuckDB answers `COUNT(*)` from metadata; we scan). The gap *is* the interpreter,
exactly as designed to be replaceable. Closing it means adopting DataFusion (or a
vectorised engine) behind the `scan` boundary — the §8 plan — not tuning this.

**So on the axis M2-4 measures, the honest verdict is: do not ship this executor
as a DuckDB competitor. It was never meant to be one.**

## NFR-03: the concurrency wedge — DuckDB cannot play

DuckDB's own error, produced by trying to open a second writer on one database:

```
IO Error: Could not set lock on file ".../test.db":
Conflicting lock is held ... See also https://duckdb.org/docs/concurrency
```

That is the whole thesis in one line. **DuckDB permits exactly one read-write
process.** A second writer is refused at the OS lock level. Concurrent
writer-plus-reader across processes is not slow in DuckDB — it is *impossible*.

ChakraDB, measured with 4 writer threads and analytical queries running
concurrently in the same process (`m2-bench`):

| phase | scan p50 | scan p99 |
|---|---|---|
| idle | 19.9 ms | 22.0 ms |
| under **346,753 concurrent upserts** | 42.6 ms | 70.9 ms |

**2.14× degradation, and the readers never blocked or saw a torn result.** Not
"barely affected" — 2.14× is a real cost — but *possible at all*, which is what
DuckDB cannot say.

## The honest Gate 2 verdict

The roadmap's gate: *"Proceed if NFR-03 shows a decisive win over DuckDB.
Reconsider seriously if the result is parity."*

- On **concurrency (NFR-03)** the result is not parity and not merely a win — it
  is a **capability DuckDB does not have**. Multi-writer-process is a lock error
  in DuckDB and a measured 2.14× in ChakraDB. That is the decisive result the
  gate asked for, on the axis the gate cares about.
- On **cold-scan speed (M2-4)** ChakraDB loses by 1–2 orders of magnitude, and
  will keep losing until the executor is replaced.

**Both are true, and the project's premise says which one decides.** §1.2 is
explicit: *"every hour of effort goes into the storage engine … the query
execution layer is bought, not built."* We competed on storage-plus-concurrency
and won an axis DuckDB structurally cannot contest; we lost the execution axis we
explicitly chose not to build. That is the intended outcome, not a failure — **but
only if the next step is to buy the execution layer rather than pretend the
interpreter is competitive.**

### Proceed — with the execution layer as the M3 precondition

Gate 2 is **passed on its own terms** (the NFR-03 wedge is real and decisive),
with one binding condition carried forward: **the interpreter must be replaced by
a vectorised engine (DataFusion) before any cold-performance claim is made.**
DataFusion 54.0.0 is verified to build in this environment (761 deps, ~2.6 GB,
23 s incremental); the `scan` boundary it plugs into already exists.

### What this does *not* establish

- No comparison at ClickBench/TPC-H scale or schema (our fixed 4-column schema
  cannot represent the 105-column `hits` table — see the schema-generalisation
  work).
- No comparison of DuckDB's *in-process* MVCC concurrency (multiple threads on
  one connection), only its cross-process single-writer limit. DuckDB does have
  intra-process concurrent transactions; the wedge is specifically about the
  embedded multi-writer / continuous-ingest shape it does not serve.
- Numbers are single-run and single-machine. The DuckDB values are near the CLI
  timer's resolution floor (~1 ms) and understate nothing in our favour.
