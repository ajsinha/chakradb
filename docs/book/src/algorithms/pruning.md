# Zonemap Part Pruning

A selective query should not read parts that cannot contain a match. ChakraDB skips
them using **zonemaps** — the per-column `(min, max)` bounds every part carries.
This is DuckDB-style rowgroup pruning, and it is also the mechanism behind [graph
adjacency](../graph/adjacency.md).

## The idea

Each sealed part stores, per column, the minimum and maximum value over its rows.
For a predicate on that column, if the part's `[min, max]` interval cannot satisfy
the predicate, no row in the part can — so the whole part is skipped without
touching its data.

```mermaid
flowchart LR
    Q["WHERE x >= 500"]
    P1["Part A<br/>x ∈ [1, 120]"]:::skip
    P2["Part B<br/>x ∈ [300, 900]"]:::hit
    P3["Part C<br/>x ∈ [950, 999]"]:::hit
    Q --> P1 & P2 & P3
    classDef skip fill:#f5d6d6,stroke:#c00; classDef hit fill:#d6f5d6,stroke:#0a0;
```

Part A (`max = 120 < 500`) is skipped; B and C are scanned.

## The exclusion test

The predicate is compiled to a conservative *excludes* test: given a part's bounds,
can this predicate be satisfied by *no* row in the part?

> **ALGORITHM 11 — Predicate excludes a part**
> ```text
> Input:  predicate φ; part bounds (per column) [min_c, max_c]
> Output: true if NO row in the part can satisfy φ (safe to skip)
> 1  match φ:
> 2    (col c) = v :  return v < min_c  or  v > max_c        ▷ value out of range
> 3    (col c) < v :  return min_c ≥ v                        ▷ all values ≥ v
> 4    (col c) ≤ v :  return min_c > v
> 5    (col c) > v :  return max_c ≤ v                        ▷ all values ≤ v
> 6    (col c) ≥ v :  return max_c < v
> 7    φ₁ AND φ₂ :  return excludes(φ₁) or excludes(φ₂)       ▷ either kills it
> 8    φ₁ OR  φ₂ :  return excludes(φ₁) and excludes(φ₂)      ▷ both must kill it
> 9    otherwise :  return false                              ▷ unsure ⇒ keep (safe)
> ```

The test is **conservative**: it returns `true` only when it can *prove* the part
holds no match, and `false` (keep the part) whenever it is unsure. So pruning never
changes an answer — it only avoids work.

The scan then drops every fully-materialised part the predicate excludes:

```text
segments ← scan_segments(snapshot)
segments.retain(seg → not (seg is a sealed part P and excludes(φ, P.bounds)))
```

## Two ways it is used

**As a SQL accelerator.** A selective `WHERE x = k` or `WHERE x BETWEEN a AND b`
prunes to the parts whose bounds overlap — the interpreter's second-stage router
sends such queries here rather than to a full vectorized scan (see
[Query Routing](routing.md)).

**As a graph adjacency index.** A graph edge key encodes `(src, dst)` src-major, so
"neighbors of X" is a key-range scan `key ∈ [(X,0), (X+1,0))`. Zonemap pruning on
the key column touches only the parts holding `src = X`. This is [clustered
adjacency](../graph/adjacency.md) — the graph traversal primitive falls directly
out of ALGORITHM 11.

## The scaling property

> **Proposition 7 (O(1) selective range scans).** For a key-range query over a
> table sorted by that key, the number of parts scanned depends on the range's
> selectivity, not the table size.
>
> *Proof sketch.* Parts are sorted by key, so each part's key-bounds interval is
> disjoint and ordered. A key range `[lo, hi)` overlaps only the contiguous run of
> parts whose intervals intersect it — a count that grows with `hi − lo`, not with
> the number of parts. Every other part is excluded by ALG 11 (lines 2–6). Measured:
> a needle range scan stays ~1 ms as the table grows 100× (ClickBench Q13, Part IX).
> ∎

## The correctness guarantee, restated

Pruning removes only parts it *proves* empty of matches, so the produced answer is
identical to a full scan. The unit tests exercise the boundaries directly —
equality at the min/max edges, swapped operands (`v = col`), `AND`/`OR`
combinations, and missing bounds — and an end-to-end suite scans a multi-part table
to confirm a pruned range returns exactly the matching rows.
