# The Sorted-Part Key Index

This is the hardest and most consequential piece of the storage engine: how a row
is located by key without paying for a key→location map. The answer — following
Apache Doris / StarRocks — is that **the sorted key column *is* the index.**

## The idea

A sealed part is written **sorted by its primary key**. Therefore a row's *ordinal
position* in the part is its *offset*: to find where key `k` lives, binary-search
the sorted key column. There is no separate structure mapping keys to row numbers,
so the resident index cost is not the ~12 bytes/row an explicit map would need — it
is the Bloom filter plus min/max bounds, about **1.25 bytes/row, flat with table
size.**

```mermaid
flowchart LR
    K["lookup key = k"] --> B{"min ≤ k ≤ max?<br/>(part bounds)"}
    B -->|no| SKIP["skip part"]:::x
    B -->|yes| BL{"Bloom: might contain k?"}
    BL -->|no| SKIP
    BL -->|yes| BS["binary-search the<br/>sorted key column"]:::ok
    BS --> HIT["ordinal → row"]
    classDef x fill:#f5d6d6; classDef ok fill:#d6f5d6;
```

## The lookup funnel

A point lookup consults a cascade of cheap filters before it ever binary-searches,
newest tier first (a later write must win):

> **ALGORITHM 1 — Point lookup by key**
> ```text
> Input:  key k; snapshot S
> Output: the visible row for k, or NONE
> 1  if L0 has a version of k visible to S:            ▷ newest writes first
> 2      return that version
> 3  for each sealed part P, newest to oldest:
> 4      if k < P.min_key or k > P.max_key: continue    ▷ ALG 11: bounds skip
> 5      if not P.bloom.might_contain(k):    continue    ▷ ALG 15: Bloom skip
> 6      o ← BinarySearchKey(P, k)                       ▷ ALGORITHM 2
> 7      if o found and version at o is visible to S:
> 8          return the row at ordinal o
> 9  return NONE
> ```

Each part that certainly lacks `k` is dropped by a **bounds comparison** (min/max)
or a **Bloom probe** — neither touches the column data on disk. Only a part that
*might* hold `k` is searched, and the search is over the small resident key run.

## Binary search within a part

> **ALGORITHM 2 — Binary search the sorted key column**
> ```text
> Input:  part P (sorted by key), key k
> Output: an ordinal o with P.key(o) = k, or NOTFOUND
> 1  lo ← 0;  hi ← P.len − 1
> 2  while lo ≤ hi:
> 3      mid ← (lo + hi) / 2
> 4      c ← total_cmp(P.key(mid), k)                    ▷ total order over Values
> 5      if c < 0: lo ← mid + 1
> 6      elif c > 0: hi ← mid − 1
> 7      else: return mid                                ▷ found
> 8  return NOTFOUND
> ```

Keys are compared with `total_cmp`, a total order over all value types (integers
compared exactly — never routed through `f64`, which would conflate integers
beyond 2⁵³ and corrupt an integer key). For a duplicate-tolerant scan the search
extends left/right from `mid` to the run of equal keys.

## Any-type keys, and the hidden rowid

The key column may be any type — integer, text, float, boolean, date, decimal —
because `total_cmp` orders them all. A table with **no** declared key gets a hidden
auto-increment `_rowid`; it is still a single sorted key column, just invisible to
`SELECT *`. So "keyless" costs nothing extra: there is no second index for the
rowid — the sorted rowid column *is* the index, exactly as for a user key.

## Why this shape

> **Proposition 1 (Index cost is flat).** The resident per-row index cost of the
> sorted-part scheme is independent of the number of rows.
>
> *Proof sketch.* The resident structures are the Bloom filter (a fixed bits-per-key
> budget), the per-part min/max bounds (constant per part), the per-row version
> stamps, and the deletion vector. None grows super-linearly with row count, and
> crucially there is **no** key→location map — the map is replaced by "ordinal =
> offset," which stores nothing. Measured at ~1.25 B/row, it stays flat as tables
> grow (`m0-bench`). ∎

The consequence is the whole storage strategy: because the index is nearly free and
flat, ChakraDB can hold row *data* on disk and keep only the index resident, and
the resident index — not disk — becomes the scaling ceiling (see
[Limits](../operations/limits.md)).
