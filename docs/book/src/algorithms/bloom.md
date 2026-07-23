# Bloom Filters & the Lookup Funnel

A point lookup should not read a part that does not contain the key. The min/max
bounds catch some parts; a **Bloom filter** catches the rest — it answers "could key
`k` be here?" from a few bits in memory, with no disk touch and no false negatives.

## What a Bloom filter guarantees

A Bloom filter is a bit array with `h` hash functions. To *insert* `k`, set the `h`
bits it hashes to; to *test* `k`, check those `h` bits.

- If any bit is `0`, `k` is **definitely absent** — a true negative.
- If all `h` bits are `1`, `k` is **probably present** — possibly a false positive.

Crucially there are **no false negatives**: a key that was inserted always tests
positive. That is exactly the property a lookup filter needs — it may occasionally
fail to skip a part, but it never wrongly skips one that holds the key.

> **ALGORITHM 15 — Bloom membership test**
> ```text
> Input:  key k; part Bloom filter B with h hash functions
> Output: DEFINITELY_ABSENT, or MAYBE_PRESENT
> 1  seed ← value_seed(k)                              ▷ reduce any Value type to 64 bits
> 2  for i in 0..h:
> 3      bit ← hash(seed, i) mod |B|
> 4      if B[bit] = 0: return DEFINITELY_ABSENT        ▷ a single 0 bit settles it
> 5  return MAYBE_PRESENT
> ```

`value_seed` maps a key of any type to a 64-bit seed: an integer maps to itself
(so the integer path is unchanged and collision-free in that dimension), and text,
float, boolean, and decimal keys get a deterministic reduction.

## Its place in the funnel

The Bloom test is the second filter in the [point-lookup funnel](key-index.md),
after the min/max bounds and before the binary search:

```mermaid
flowchart LR
    K["lookup k"] --> BND{"min ≤ k ≤ max?"}
    BND -->|no| S1["skip"]:::x
    BND -->|yes| BLM{"Bloom: maybe present?"}
    BLM -->|absent| S2["skip"]:::x
    BLM -->|maybe| BS["binary search<br/>(reads the key run)"]:::ok
    classDef x fill:#f5d6d6; classDef ok fill:#d6f5d6;
```

The ordering is deliberate — cheapest test first. The bounds check is two
comparisons; the Bloom probe is a handful of bit reads; only a part that passes both
pays for a binary search over its resident key run. A part that certainly lacks the
key is dropped before any of its data is examined.

## Sizing and cost

The filter trades a small, fixed number of bits per key for a low false-positive
rate. In ChakraDB it is part of the ~1.25 B/row resident index budget
([Proposition 1](key-index.md)): the Bloom bits, the min/max bounds, the version
stamps, and the deletion vector together stay flat with table size, which is what
lets the engine keep data on disk and the index in memory.

> **Proposition 11 (No false negatives ⇒ lookups are correct).** The Bloom skip in
> ALGORITHM 15 never causes a lookup to miss a key that is present.
>
> *Proof sketch.* If key `k` is in the part, it was inserted, so all `h` of its bits
> are `1`, so ALG 15 returns `MAYBE_PRESENT` and the funnel proceeds to the binary
> search that finds it. The filter can only err the other way — a `MAYBE_PRESENT`
> for an absent key — which costs a wasted binary search, not a wrong answer. This
> is asserted directly: over many keys, the filter has zero false negatives
> (`bloom` tests). ∎

## Why it matters for concurrency

Point lookups are the transactional read shape, and they run on the interpreter
precisely because this funnel makes them `O(log n)` with almost no I/O. That keeps
the transactional path fast and lock-free while analytical scans run on the same
snapshot — the HTAP split the [router](routing.md) exploits.
