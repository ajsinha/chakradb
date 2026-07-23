<div style="text-align:center">

# ChakraDB
## The Definitive Guide

**The embedded HTAP database with built-in graph capabilities.**

*Write, analyze, and traverse live data — in one engine.*

</div>

---

ChakraDB is an embedded database that does three things most engines keep in
separate systems, over the same live data, in a single process:

- **Transactional (T)** — concurrent, durable, ACID writes.
- **Analytical (A)** — vectorized scans, joins, and aggregation that never block
  the writers.
- **Graph (G)** — clustered adjacency and built-in graph algorithms over a
  consistent snapshot.

That is what "HTAP with built-in graph" means: **one engine for the writes, the
analytics, and the traversals**, with no ETL between systems and no lock that
stops a reader when a writer is busy.

This book explains how it works — the architecture, every core algorithm, the
graph layer — and how to use it, with tutorials, case studies, and head-to-head
comparisons.

> **A note on honesty.** Every performance figure in this book ships with the
> harness that produced it (see Part IX). A number without a reproducible source
> is a hypothesis, not a measurement. The design reasoning stands on its own and
> does not depend on any single benchmark.
