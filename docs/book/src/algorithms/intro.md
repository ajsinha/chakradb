# How to Read This Part

This part is the engine at the level of its algorithms. Where Part II described the
*shape* of ChakraDB — the tiers, the clock, the log — this part gives the
*procedures*, each with pseudocode, a complexity, and, where correctness is
subtle, a **proposition and a proof sketch**.

## Conventions

- **Algorithms are numbered globally** — `ALGORITHM 1`, `ALGORITHM 2`, … — so they
  can be referenced across chapters. Each states its `Input`, its `Output`, and
  annotates lines with `▷` comments on the right.
- Assignment is written `←`. Comparison and set notation (`≤`, `∈`, `∩`, `∞`) mean
  the usual things.
- **Propositions** capture the guarantees that are easy to get wrong (visibility,
  crash-atomicity, GC safety). Proof *sketches* are arguments, not machine-checked
  proofs; where a full proof is future work, the text says so.
- `V`, `E`, `n` are sizes; `α` is the inverse-Ackermann function (effectively
  constant); CSN is a commit sequence number.

## The map of this part

| Chapter | Algorithms | The question it answers |
|---|---|---|
| [The Sorted-Part Key Index](key-index.md) | 1–2 | How is a row found in `O(log n)` with no key→location map? |
| [MVCC Visibility](visibility.md) | 3–4 | Which version does a snapshot see, and why is cold data free? |
| [Write-Ahead Logging & Group Commit](wal.md) | 5–6 | How is a write durable before it is acknowledged? |
| [Crash Recovery](recovery.md) | 7 | How is the exact acknowledged state rebuilt after a crash? |
| [The Merge / Compaction Algorithm](merge.md) | 8–9 | How are parts merged and dead versions reclaimed? |
| [The GC Watermark](gc-watermark.md) | 10 | How does compaction never reclaim a version a reader can see? |
| [Zonemap Part Pruning](pruning.md) | 11 | How does a scan skip parts that cannot match? |
| [Query Routing](routing.md) | 12 | How is each statement sent to the right engine? |
| [Exact Decimal Arithmetic](decimal.md) | 13 | How is `DECIMAL` exact, never `f64`? |
| [Temporal Encoding](temporal.md) | 14 | How are `DATE`/`TIMESTAMP` stored and rendered? |
| [Bloom Filters & the Lookup Funnel](bloom.md) | 15 | How is a part excluded without touching disk? |

## The recurring theme

Nearly every algorithm here spends structure at *write* time so that a *read* is
cheap and lock-free: parts are sorted so lookups binary-search; versions are
stamped so visibility is a comparison; zonemaps are precomputed so scans prune.
The [cost model](../introduction/cost-model.md) is the philosophy; this part is
where it is cashed out.
