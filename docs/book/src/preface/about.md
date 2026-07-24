# About This Book

```{=latex}
\epigraph{If I have seen further it is by standing on the shoulders of Giants.}{--- Isaac Newton}
```

This is the complete guide to **ChakraDB** — an embedded HTAP database with
built-in graph capabilities. It is written for three audiences at once, and it is
organized so each can find its level:

- **Users** who want to build applications — start at Part V (*Getting Started*)
  and the Part VI (*Case Studies*).
- **Architects** evaluating the engine — read Part I (*Introduction*), Part II
  (*The Engine*), and Part VII (*Perspective*).
- **Contributors and the curious** who want to know *how* it works down to the
  algorithm — Parts II–IV carry the pseudocode and complexity.

## How the book is organized

| Part | What it covers |
|---|---|
| I — Introduction | What ChakraDB is, the problem it solves, its design principles and cost model. |
| II — The Engine | Storage (parts, index, compaction, pruning), MVCC & transactions, durability, the HTAP query router, and exact types — the design *and* the algorithms, together. |
| III — The Graph Engine | The property-graph layer: clustered adjacency, CSR snapshots, and the full graph-algorithm library. |
| IV — Reactive | The change stream (CDC), materialized workers and the registry, and the sink transports — how applications react to committed data. |
| V — Getting Started | Install, first database, Python, and a first graph — Rust and Python. |
| VI — Case Studies | Worked end-to-end applications: real-time AML, counterparty & market risk, recommendations, and fraud. |
| VII — Perspective | Head-to-head with the alternatives, and where ChakraDB fits. |

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
