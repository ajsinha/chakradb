# About This Book

```{=latex}
\epigraph{If I have seen further it is by standing on the shoulders of Giants.}{--- Isaac Newton}
```

This is the complete guide to **ChakraDB** — an embedded HTAP database with
built-in graph capabilities. It is written for three audiences at once, and it is
organized so each can find its level:

- **Users** who want to build applications — start at Part V (*Getting Started*)
  and Part VI (*Developer Guide & Tutorials*).
- **Architects** evaluating the engine — read Part I (*Introduction*), Part II
  (*Architecture*), and Part IX (*Comparative Studies*).
- **Contributors and the curious** who want to know *how* it works down to the
  algorithm — Part III (*Algorithms*) is the deep end.

## How the book is organized

| Part | What it covers |
|---|---|
| I — Introduction | What ChakraDB is, the problem it solves, its design principles and cost model. |
| II — Architecture | The engine's structure: storage, MVCC, durability, compaction, the query router. |
| III — Algorithms | Each core algorithm in detail, with pseudocode and complexity. |
| IV — Graph | The property-graph layer: adjacency, CSR snapshots, and graph algorithms. |
| V — Getting Started | Install, first database, Python, first graph. |
| VI — Developer Guide | SQL, types, constraints, transactions, COPY, backup, observability, tutorials. |
| VII — Operations | Durability tuning, limits, monitoring, disaster recovery. |
| VIII — Case Studies | Worked end-to-end applications. |
| IX — Comparative Studies | Head-to-head with DuckDB, SQLite, Neo4j, PostgreSQL, with methodology. |
| X — Reference | The Rust and Python APIs, configuration, errors, file formats. |
| Appendices | Glossary, design-decision log, bibliography. |

## Conventions

- **Diagrams** are rendered with [Mermaid](https://mermaid.js.org/). Every
  architecture and algorithm chapter has at least one.
- **Code** is real. Rust snippets compile against the public API; SQL runs on the
  shipped engine.
- **Callouts** flag the important asymmetries:
  > This is a *design commitment* — a place where ChakraDB deliberately pays a
  > cost so that it can be cheap somewhere that matters more.

## Relationship to the source docs

This book synthesizes and expands the design documents that live alongside the
code — `requirements.md` (the architecture specification), `sql-reference.md`,
`clickbench-findings.md`, and the milestone records. Where the book and a source
doc disagree, the code is the arbiter; the book aims to track it.

## Status

ChakraDB is a working engine — durable, crash-tested, benchmarked against DuckDB —
with an actively evolving surface. Chapters that describe not-yet-shipped work
(for example, parts of the graph roadmap) say so explicitly.
