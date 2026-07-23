# ChakraDB Documentation

Start here. The docs split into two kinds: **current** references kept up to date
with the code, and **historical records** — point-in-time milestone reports and
spikes, preserved as written (each carries a banner scoping what has since
changed). Don't read a historical doc for the current state; read the current
ones.

## Current — kept up to date

| Doc | What it is |
|---|---|
| [`../README.md`](../README.md) | Project overview, the concurrency wedge, quick start (Rust + Python), and the **limitations & operating envelope**. |
| [`sql-reference.md`](sql-reference.md) | The **full SQL surface** — statements, data types, constraints, transactions, `COPY`, expressions, the query router, durability. The authoritative "what SQL works". |
| [`requirements.md`](requirements.md) | **Architecture & design specification** (v2.1). The engine's design and reasoning; §2.2 states the current operating envelope, §8 the dual-engine execution model, §9 the delivered SQL surface. Sections on lakehouse publication (§6) remain forward-looking. |
| [`roadmap.md`](roadmap.md) | Milestones, decision gates, and current status (M0–M2 done, M4 hardening in progress, lakehouse M3 not started). |
| [`clickbench-findings.md`](clickbench-findings.md) | Analytics **benchmarks vs DuckDB**, 100k–10M rows, with the harness that produced them. |
| [`graph-exploration.md`](graph-exploration.md) | Design proposal: using ChakraDB as a **graph database** (property-graph schema, clustered adjacency, baked-in algorithms). Forward-looking — not implemented. |

## Historical records — preserved snapshots

| Doc | Snapshot of |
|---|---|
| [`arrow-schema-migration.md`](arrow-schema-migration.md) | The rewrite from the fixed 4-column schema to Arrow-native storage with arbitrary schemas. Describes a real, current subsystem, but as a migration log. |
| [`m3-datafusion-spike.md`](m3-datafusion-spike.md) | The spike that adopted DataFusion (since shipped as the default analytical engine). |
| [`gate2-results.md`](gate2-results.md) | The Gate 2 evaluation vs DuckDB — the interpreter's cold-scan gap (since closed by DataFusion) and the still-current concurrency wedge. |
| [`m2-findings.md`](m2-findings.md) | The M2 interpreter-only SQL layer over the fixed schema. |
| [`m1-findings.md`](m1-findings.md) | The M1 durable engine (WAL, recovery, checkpointing). |
| [`m0-findings.md`](m0-findings.md) | The M0 storage risk spike (sorted-key index, MVCC). |
| [`archive/requirements-v1.0.md`](archive/requirements-v1.0.md) | The superseded v1.0 spec. |

## Where to look for…

- **"What SQL can I write?"** → [`sql-reference.md`](sql-reference.md).
- **"How does the storage/MVCC/WAL engine work?"** → [`requirements.md`](requirements.md) §4–§7.
- **"Why these design choices?"** → [`requirements.md`](requirements.md) §1–§3.
- **"What are the limits / how big / how many writers?"** → [`../README.md`](../README.md) *Limitations* + [`requirements.md`](requirements.md) §2.2.
- **"How fast vs DuckDB?"** → [`clickbench-findings.md`](clickbench-findings.md).
- **"What's done and what's next?"** → [`roadmap.md`](roadmap.md).
