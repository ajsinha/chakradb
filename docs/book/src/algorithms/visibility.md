# MVCC Visibility

Visibility is the pure function at the heart of snapshot isolation. It decides,
for a snapshot `S` and a row version stamped `(created, deleted)`, whether that
version is the one `S` should see. Everything about ChakraDB's non-blocking reads
follows from how cheap this function is.

## The predicate

> **ALGORITHM 3 — Version visibility**
> ```text
> Input:  snapshot S (a CSN); version stamps created, deleted
> Output: true if the version is visible to S
> 1  return created ≤ S  and  S < deleted            ▷ deleted = ∞ if never deleted
> ```

That is the whole rule: a version is visible iff it was created at or before `S`
and not yet deleted as of `S`. It is three integer comparisons — no lock, no undo
log, no read-view to materialize.

An `UPDATE` is modeled as *delete-old + create-new*: the old version's `deleted` is
set to the new CSN, and the new version's `created` is that same CSN.

> **Proposition 2 (Exactly one live version).** For any live key and any snapshot
> `S`, exactly one version satisfies `created ≤ S < deleted`.
>
> *Proof sketch.* The versions of a key form a chain: `v₁(c₁, d₁), v₂(c₂, d₂), …`
> with `d_i = c_{i+1}` (each delete coincides with the next create), the last
> version having `deleted = ∞`. The intervals `[c_i, d_i)` therefore **partition**
> the CSN line from `c₁` to `∞`. Any `S ≥ c₁` lands in exactly one interval; any
> `S < c₁` lands in none (the key did not exist yet). ∎

```mermaid
flowchart LR
    subgraph chain["Version chain of one key — intervals partition the CSN line"]
      direction LR
      A["[1, 5)"] --> B["[5, 9)"] --> C["[9, ∞)"]
    end
    S3(["S=3 → [1,5)"]):::a --> A
    S7(["S=7 → [5,9)"]):::b --> B
    S12(["S=12 → [9,∞)"]):::c --> C
    classDef a fill:#bde0fe; classDef b fill:#a2d2ff; classDef c fill:#8ecae6;
```

## Why cold data is free

The predicate has a batched fast path that is the reason a large scan is cheap.
Instead of testing every row, a scan tests the *whole part* first:

> **ALGORITHM 4 — Full-part visibility fast path**
> ```text
> Input:  snapshot S; part P with uniform created stamp c_max
>         and minimum deletion m_del over its rows
> Output: how to scan P under S
> 1  if c_max ≤ S and S < m_del:                     ▷ every row created ≤ S,
> 2      scan ALL rows of P — no per-row check        ▷ none deleted ≤ S
> 3  elif S < c_min:                                  ▷ part entirely in the future
> 4      skip P                                        ▷ nothing visible
> 5  else:
> 6      for each row r in P:                          ▷ the slow path
> 7          if ALGORITHM 3 holds and r not tombstoned: emit r
> ```

Line 1 is the corollary that matters: a part whose newest creation is `≤ S` and
whose earliest deletion is `> S` is **fully visible** — every row passes, so the
scan emits the batch with a single comparison and *zero* per-row work.

> **Proposition 3 (Cold-scan cost).** A scan at `S` pays the per-row visibility
> check only on parts modified in the window the snapshot straddles; cold,
> unmodified parts pay `O(1)` per part.
>
> *Proof sketch.* A cold part has a uniform `created ≤ S` (it was written long ago)
> and no deletions after `S`, so it takes the fast path (ALG 4, line 1) — one
> comparison for the entire part. Only parts with a `created` or a `deleted` inside
> `(S_min, S_max]` fall to the slow path. Hence a billion-row table with a thousand
> recent mutations checks a thousand rows, not a billion — the "cost of concurrency
> is paid only by recently-modified data" principle, from Neumann et al. ∎

## Where the stamps live

Per-part, ChakraDB stores the version stamps compactly: a part whose rows all share
one CSN stores a single `Uniform(csn)` rather than a stamp per row (the common case
for a bulk-sealed part), and a **deletion vector** records which ordinals are
tombstoned and at which CSN. So the fast path's `c_max` and `m_del` are `O(1)` to
read, and the slow path consults the deletion vector rather than rewriting the part.

## The consequence for concurrency

Because visibility is a pure function of a snapshot number and immutable per-row
stamps, a reader needs **no lock**: it fixes `S`, grabs the (immutable) part list,
and scans. A concurrent writer advances the clock and appends new versions with
higher CSNs — invisible to `S`. This is the mechanism behind "readers never block
writers," and it is why the same snapshot feeds analytics, transactions, and the
[graph CSR](../graph/csr.md) consistently.
