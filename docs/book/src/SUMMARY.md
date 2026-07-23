# Summary

[ChakraDB — The Definitive Guide](title.md)

---

# Preface

- [About This Book](preface/about.md)

# Part I — Introduction

- [Introduction to ChakraDB](introduction/introduction.md)

# Part II — Architecture

- [System Overview](architecture/overview.md)
- [The Storage Engine](architecture/storage-engine.md)
- [The Primary-Key Index](architecture/pk-index.md)
- [MVCC & Snapshot Isolation](architecture/mvcc.md)
- [Durability: WAL, Group Commit & Recovery](architecture/durability.md)
- [Checkpointing](architecture/checkpointing.md)
- [Compaction & Backpressure](architecture/compaction.md)
- [The Query Layer: The HTAP Router](architecture/query-router.md)
- [Zonemap Pruning & the Index Funnel](architecture/pruning.md)
- [Transactions](architecture/transactions.md)
- [The Arrow-Native Data Path](architecture/arrow.md)
- [The I/O Abstraction](architecture/io.md)

# Part III — Algorithms

- [How to Read This Part](algorithms/intro.md)
- [The Sorted-Part Key Index](algorithms/key-index.md)
- [MVCC Visibility](algorithms/visibility.md)
- [Write-Ahead Logging & Group Commit](algorithms/wal.md)
- [Crash Recovery](algorithms/recovery.md)
- [The Merge / Compaction Algorithm](algorithms/merge.md)
- [The GC Watermark & Live-Snapshot Registry](algorithms/gc-watermark.md)
- [Zonemap Part Pruning](algorithms/pruning.md)
- [Query Routing (Cost Model)](algorithms/routing.md)
- [Exact Decimal Arithmetic](algorithms/decimal.md)
- [Temporal Encoding](algorithms/temporal.md)
- [Bloom Filters & the Lookup Funnel](algorithms/bloom.md)

# Part IV — Graph

- [Graphs on ChakraDB](graph/overview.md)
- [Modeling a Property Graph](graph/modeling.md)
- [Clustered Adjacency](graph/adjacency.md)
- [The CSR Snapshot](graph/csr.md)
- [Graph Algorithms](graph/algorithms.md)
- [Live Graph Analytics](graph/live-analytics.md)

# Part V — Getting Started

- [Installation & Building](getting-started/install.md)
- [Your First Database (Rust)](getting-started/first-rust.md)
- [Python in Five Minutes](getting-started/python.md)
- [A Graph in Five Minutes](getting-started/graph.md)

# Part VI — Developer Guide & Tutorials

- [The SQL Reference](guide/sql-reference.md)
- [Data Types](guide/types.md)
- [Constraints](guide/constraints.md)
- [Transactions Tutorial](guide/transactions.md)
- [Bulk Ingest with COPY](guide/copy.md)
- [Backup & Restore](guide/backup.md)
- [Observability & Metrics](guide/observability.md)
- [Building a Graph Application](guide/graph-app.md)
- [The Python Driver (DB-API 2.0)](guide/python-driver.md)

# Part VII — Operations

- [Durability Modes & Tuning](operations/durability.md)
- [Limits & the Operating Envelope](operations/limits.md)
- [Monitoring in Production](operations/monitoring.md)
- [Backup & Disaster Recovery](operations/backup.md)

# Part VIII — Case Studies

- [Real-Time Fraud Detection (HTAP + Graph)](case-studies/fraud.md)
- [Live Recommendation Engine](case-studies/recommendations.md)
- [Streaming Analytics Dashboard](case-studies/streaming.md)
- [Exact-Money Ledger](case-studies/ledger.md)

# Part IX — Comparative Studies

- [ChakraDB vs. DuckDB](comparisons/duckdb.md)
- [ChakraDB vs. SQLite](comparisons/sqlite.md)
- [ChakraDB vs. Neo4j](comparisons/neo4j.md)
- [ChakraDB vs. PostgreSQL](comparisons/postgres.md)
- [Benchmark Methodology](comparisons/methodology.md)

# Part X — Reference

- [The Rust API](reference/rust-api.md)
- [The Python API](reference/python-api.md)
- [Configuration](reference/configuration.md)
- [Error Taxonomy](reference/errors.md)
- [File Formats & Versioning](reference/formats.md)

---

# Appendices

- [Glossary](appendix/glossary.md)
- [Design Decision Log](appendix/decisions.md)
- [Bibliography](appendix/bibliography.md)
