# M0 Findings — Risk Reduction Spike

**Milestone:** M0 (see `roadmap.md`)
**Status:** Complete
**Verdict:** **PROCEED to M1**, with two design changes required before M1 starts.
**Artifact:** `src/` (~2,900 lines), 231 tests, `cargo run --release --bin m0-bench`

---

## 1. The question M0 existed to answer

> Can we sustain high-rate keyed updates while scanning, at an acceptable
> primary-key index memory footprint, without scan performance collapsing?

**Answer: yes, conditionally.** The index design works better than hoped. The
concurrency story works only when compaction runs — and the way M0 runs
compaction has a flaw serious enough that fixing it is now M1's first task.

### How to read the numbers

Single machine, single run, unaudited. Per `requirements.md` §10.2 the harness
is committed alongside the results so they can be re-run and disputed. Treat
them as **order-of-magnitude and directional**, not as benchmark claims. The
absolute scan figures in particular are inflated by an M0 shortcut (§6).

---

## 2. M0-2 — Index memory: **strong pass**

The most important result, and it validates `requirements.md` §5.2 decisively.

| rows | index (MB) | B/row fresh | B/row compacted | explicit map would cost |
|---|---|---|---|---|
| 100,000 | 0.13 | 9.25 | **1.25** | ~12 |
| 500,000 | 0.63 | 9.25 | **1.25** | ~12 |
| 2,000,000 | 2.50 | 9.25 | **1.25** | ~12 |

**Per-row index cost is flat at 1.25 bytes and does not grow with table size.**
That is the Bloom filter and nothing else — because parts are written PK-sorted,
the ordinal position in the key column *is* the row offset, so no key→location
map exists to pay for.

Extrapolating: **1 billion rows ≈ 1.25 GB of index.** The StarRocks-style
explicit map would be ~12 GB for the same table, which is what forced them to
build an L0/L1/L2 LSM for the index itself. We don't need one.

**The 9.25 vs 1.25 gap is worth understanding.** A freshly sealed part carries an
8-byte-per-row creation stamp, because its rows genuinely were created at
different CSNs. Compaction collapses those stamps once no live snapshot can
distinguish them, and the cost drops by 7.4×. So **version-metadata GC is not a
nicety — it is 86% of the index budget.**

> Consequence for M1: the compaction horizon must track the oldest active
> snapshot. A long-running query that pins the horizon doesn't just delay
> reclamation, it holds index memory at 7× its steady-state size.

---

## 3. M0-3 — Lookup fan-out: **pass, better than expected**

| parts | p50 (µs) | p99 | parts probed/lookup |
|---|---|---|---|
| 1 | 1.63 | 3.16 | 0.98 |
| 2 | 1.63 | 2.97 | 1.45 |
| 4 | 1.63 | 4.33 | 2.42 |
| 8 | 1.61 | 3.97 | 4.35 |
| 16 | 1.51 | 3.22 | 8.23 |
| 33 | 1.12 | 2.13 | 16.34 |

**Parts probed per lookup grows linearly with part count — and latency does
not.** From 1 to 33 parts the fan-out grows 16×, while p50 latency stays flat
(and the 33-part row is *faster*, because smaller parts mean smaller binary
searches).

This validates the four-stage funnel: bounds check and Bloom filter eliminate
almost every part before any data is touched, so a "probe" is a handful of
comparisons rather than a search.

**Caveat — the honest one:** this was measured with uniformly random keys over a
dense range, which is the friendliest case for min/max bounds. Skewed or
adversarial key distributions (e.g. every part spanning the full range) would
defeat stage 1 and force a Bloom probe per part. **M1 should re-run this with a
hostile distribution before treating the result as settled.**

---

## 4. M0-1 — Scan under write load: **pass, but it exposed a real defect**

| condition | p50 scan (µs) | ratio vs idle | upserts applied |
|---|---|---|---|
| idle | 13,064 | 1.00× | — |
| under write load, no compaction | 49,243 | **3.77×** | 48,918 |
| under write load + compaction | 17,980 | **1.38×** | **2,665** |

Two findings, and the second is the important one.

**Finding A — compaction is load-bearing, quantitatively.** Without it, sustained
keyed updates degrade scans 3.77×. With it, 1.38×. The §3 cost model is correct:
the price of fast writes is paid by compaction, and if compaction doesn't run,
readers pay it instead.

**Finding B — compaction and writers contend on the same lock, and it is
severe.** Enabling compaction cut applied upserts from 48,918 to 2,665 — an
**18× collapse in write throughput**. The scan number improved because the write
load largely stopped, not only because reclamation helped.

This is an M0 implementation flaw, not a design flaw, but it must be fixed
before any M1 number means anything:

> `Table::maybe_compact` takes the table write lock and holds it for the entire
> merge. Compaction should build the replacement part *outside* the lock and take
> it only for the final pointer swap.

The design already anticipates this — `requirements.md` §5.4 requires compaction
to have a resource budget and never starve foreground work, and §7.3.1 specifies
a priority-class I/O scheduler. M0 implemented neither. **This is M1's first
task, ahead of the WAL.**

---

## 5. M0-4 — Version-check cost on cold data: **partial pass**

| configuration | p50 (µs) | fast-path scans |
|---|---|---|
| cold (collapsed stamps, no tombstones) | 12,225 | **30/30** |
| warm (exactly one tombstone) | 14,934 | **0/30** |

The mechanism works exactly as designed: the fast path engages on every cold
scan and is disabled by a single tombstone anywhere in the part.

But the *saving* is only 1.22×, well below what §5.3's "zero per-row work" phrasing
implies. The reason is that M0's scan is dominated by cloning `String` values,
not by visibility checks — so eliminating the checks removes a small share of
total cost. **The claim is directionally validated but its magnitude is
unmeasured**, and will stay so until M2 puts a real columnar representation
behind it (see §6).

Note the sharpness of the cliff: **one tombstone in a 200,000-row part disables
the fast path for the entire part.** That makes compaction's DV-density trigger
more important than the default (0.3) suggests, and argues for a much lower
threshold. M1 should tune it against measurements rather than intuition.

---

## 6. Deviations from the specification

Three, all deliberate, all recorded here rather than hidden.

| Spec says | M0 does | Why |
|---|---|---|
| Sealed parts in Apache Arrow (§5.1) | Struct-of-vectors columnar | Arrow buys nothing for M0's four questions and costs build time. **This inflates all absolute scan figures** — string cloning dominates. M2 introduces Arrow at the DataFusion boundary where it earns its place |
| Deletion vectors as RoaringBitmap (§5.3, §6.2) | Sorted `Vec<(u32, Csn)>` | DV *encoding* is not what M0 measures; pure-Rust `roaring`'s SIMD path needs nightly. Swap is localised to one file |
| Multi-version retention across seal | Implemented in full | No deviation — sealing preserves every version and parts permit duplicate keys resolved by version stamps |

**Zero external dependencies.** This was not an aesthetic choice: it keeps
determinism fully under our control, makes the test loop instant, and forces the
`Io`/`Clock`/`Rng` seams to be real rather than inherited.

---

## 7. What was built that isn't strictly M0

Two things, both because they were cheap now and expensive later.

**The three unretrofittable seams** (`requirements.md` §11.1) — `trait Io`,
`trait Clock`, seeded `Rng` — exist despite M0 persisting nothing. `MemIo`
already supports write/sync/read fault injection *and* the silent lost-write
case (`drop_writes`), which is what M1's crash tests will drive.

**Multi-table support.** `Database` holds many `Table`s over one shared CSN
generator, so a snapshot is consistent across tables. Foreign keys remain an
explicit non-goal. This was added mid-M0 at the project owner's direction and
cost roughly 200 lines; retrofitting a catalog after M1's WAL would have been
considerably worse.

---

## 8. Test coverage

231 tests, all passing, ~1.3 s wall clock.

| Suite | Tests | Covers |
|---|---|---|
| unit (`src/`) | 195 | RNG determinism, clock virtualisation, I/O fault injection, Bloom FPP, DV visibility, part funnel, L0 versioning, compaction, catalog |
| `tests/mvcc.rs` | 10 | Snapshot isolation invariants, one-version-visible, seal/compact preservation |
| `tests/multi_table.rs` | 11 | Key-space independence, cross-table snapshot consistency, global CSN ordering |
| `tests/concurrency.rs` | 7 | Non-blocking reads, concurrent writers, contended keys, maintenance races |
| `tests/determinism.rs` | 7 | Seeded workload replay, get/scan agreement, virtual-time isolation |

Properties asserted rather than sampled: exactly one version of a key is visible
at every snapshot; `scan().len() == row_count()` at every snapshot; every visible
key is individually retrievable; identical seeds produce byte-identical state.

Two engine defects were found by tests during M0, both now fixed:
1. Compaction policy short-circuited on part count before checking tombstone
   density, so a single heavily-deleted part never compacted.
2. Compaction refused to run on a single part, leaving version stamps
   permanently uncollapsed for tables that never accumulated a second part.

---

## 9. Gate 0 assessment

Against the criteria in `roadmap.md`:

| Criterion | Result |
|---|---|
| Scans degrade *gracefully* under write load | ✅ 1.38× with compaction; 3.77× without |
| Index memory extrapolates to a supportable table size | ✅ 1.25 B/row flat → ~1.25 GB at 1B rows |
| Cold scans pay no measurable version cost | ⚠️ Mechanism confirmed; magnitude obscured by M0's string handling |
| Lookup latency acceptable as parts grow | ✅ Flat 1.1–1.7 µs from 1→33 parts |

**Verdict: proceed.** The index design — the piece flagged as most likely to kill
the project — is the strongest result. No finding suggests the architecture is
wrong.

### Required before M1 work begins

1. **Compaction must not hold the table write lock while merging.** Build the
   replacement outside the lock; take it only for the pointer swap. Without this,
   every M1 throughput number is measuring lock contention.
2. **Re-run M0-3 with a hostile key distribution.** The current result assumes
   min/max bounds are selective; prove it under keys that defeat them.

### Carried into M1 as open questions

- What DV-density threshold actually minimises total cost? The one-tombstone
  cliff suggests the 0.3 default is far too high.
- How should the compaction horizon track the oldest active snapshot, given that
  pinning it holds index memory at ~7× steady state?
- Does the single-writer-lock model survive once the WAL adds fsync latency
  inside the critical section? It very likely does not, and group commit will
  need to be designed around that.
