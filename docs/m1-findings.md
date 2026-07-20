# M1 Findings — Durable Single-Table Engine

**Milestone:** M1 (see `roadmap.md`)
**Status:** Complete
**Verdict:** **PROCEED to M2**, with one requirement formally reclassified and two
items carried forward.
**Artifacts:** `src/` (~6,000 lines), 335 tests, `cargo run --release --bin m1-bench`

---

## 1. What M1 was for

Make M0 survive power loss, and make compaction hold under sustained load. Both
were achieved. The interesting content of this report is in the three places
where measurement contradicted the plan.

### How to read the numbers

Single machine, single run, **in-memory I/O with a simulated 100 µs fsync**. That
last detail matters: with a zero-cost sync, group commit has no window in which
to batch and the measurement is meaningless. Absolute latencies understate a real
device; ratios are the meaningful part. The harness is committed.

---

## 2. The M0 blocker: fixed, and confirmed by measurement

M0's headline defect was that compaction held the table write lock for the entire
merge, collapsing applied upserts from 48,918 to 2,665 — an 18× loss.

Compaction is now **two-phase** (`compaction.rs`):

1. `plan_merge` builds the replacement part **holding no lock at all**.
2. `apply_plan` takes the write lock only to swap pointers — and to *replay*
   tombstones that writers produced while phase 1 was running, carried forward
   through an ordinal mapping.

Step 2 is what makes step 1 safe. Writers are free to keep deleting rows from
parts being merged, because those deletions are not lost.

| | upserts applied | ratio |
|---|---|---|
| M0, compaction off | 48,918 | — |
| M0, compaction on | 2,665 | **18.4× loss** |
| M1, compaction off | 7,834 | — |
| M1, compaction on | 5,029 | **1.56× loss** |

(The M0 and M1 absolute figures are not comparable — different durations and
durability settings. The *ratios* are.) A residual 1.5× cost is expected and
correct: compaction genuinely competes for CPU and for the write lock during the
swap. What it no longer does is serialise the whole merge against ingest.

A second defect was found and fixed during M1 — see §5.

---

## 2b. The second M0 blocker: fan-out under hostile keys

M0-3 measured lookup latency as flat from 1 to 33 parts, but used dense
sequential keys — the friendliest case, since each part then covers a narrow
disjoint `[min, max]` and stage 1 of the funnel eliminates almost everything for
free. `m0-findings.md` §3 flagged this and made a hostile re-run a blocker.

`tests/hostile_keys.rs` is that re-run. The adversarial case is **every part
spanning the full key range**, produced by shuffling keys before insertion so no
part has a narrow span. Results:

- **Bounds become useless and the Bloom filter carries the funnel alone** —
  Bloom skips exceed bounds skips, and eliminate >50% of probes on their own.
- **Absent keys are still refused cheaply**: >90% of probes for keys that exist
  nowhere are eliminated before touching data.
- Correctness holds at 50+ overlapping parts, with a hot-key workload where every
  part contains the same keys, and with `i64::MIN`/`i64::MAX` as live keys.

So §5.2's funnel does not depend on a friendly key distribution. What it does
depend on is the Bloom filter, which becomes the sole line of defence when spans
overlap — making its false-positive rate a first-class tuning parameter rather
than an implementation detail. Worth revisiting if fan-out ever looks expensive.

---

## 3. FR-06 must be reclassified, and this is the most important finding

The roadmap states M1-2 as *"recovery time bounded by WAL tail; demonstrated flat
as database size grows 10×."* Measured:

| base rows (checkpointed) | tail | **total recovery** | parts loaded | **replay only** |
|---|---|---|---|---|
| 10,000 | 5,000 | 9.7 ms | 1 | **1.4 ms** |
| 50,000 | 5,000 | 21.0 ms | 2 | **1.4 ms** |
| 200,000 | 5,000 | 69.1 ms | 8 | **1.4 ms** |

**Log replay is exactly flat — 1.4 ms across a 20× growth in base data.** That
half of FR-06 holds precisely, and it is the half the WAL design controls.

**Total recovery is not flat.** It grows roughly linearly with database size,
because M1 loads every part eagerly into memory at open. This is not a bug in the
WAL; it is inherent to an in-memory-resident engine. No amount of log-tail
discipline fixes it.

> **Recommended change to `requirements.md` FR-06.** Split it:
>
> * **FR-06a (met):** *log replay* time is bounded by WAL tail size, independent
>   of database size.
> * **FR-06b (deferred to M2):** *time to first query* is bounded independently
>   of database size. Requires the demand-paged buffer pool — parts must be
>   openable without being fully resident.
>
> Stating the original as satisfied would be false. Stating it as failed would be
> misleading, since the mechanism it was written to constrain works exactly as
> designed.

---

## 4. Group commit works, but only under concurrency

| writer threads | appends | syncs | syncs/append |
|---|---|---|---|
| 1 | 400 | 400 | **1.000** |
| 2 | 800 | 599 | 0.749 |
| 4 | 1,600 | 688 | 0.430 |
| 8 | 3,200 | 799 | 0.250 |
| 16 | 6,400 | 801 | **0.125** |

At 16 threads, **8× fewer fsyncs than commits** — and sync count is nearly flat
from 4 threads upward (688 → 801 while appends grow 4×), which is the signature
of a batch absorbing more members rather than more batches forming.

Single-writer latency by mode:

| mode | p50 | p99 | syncs/append | may lose data |
|---|---|---|---|---|
| sync | 153.6 µs | 169.1 µs | 1.000 | no |
| group | 153.7 µs | 171.4 µs | 1.000 | no |
| async | 0.8 µs | 1.3 µs | 0.000 | **yes** |

**With one writer, `group` is identical to `sync`** — there is nobody to batch
with, so it degenerates gracefully rather than cheating. That is the correct
behaviour and worth stating plainly: group commit is a *concurrency*
optimisation, not a latency one. A single-threaded workload pays full fsync cost
in any honest durability mode.

`async` is ~190× faster and loses data. It is named `Async` rather than "fast"
specifically so nobody selects it by accident.

---

## 5. The bug the crash suite found on its first run

**Symptom:** after a checkpoint, subsequent writes were silently lost on crash.

**Cause:** `GroupCommit::complete` is deliberately monotonic, so a slow sync can
never lower the durable watermark. But checkpointing *truncates* the WAL, which
genuinely does invalidate the watermark. The stale high value made every
subsequent `commit_to` conclude "already durable" and **skip its fsync entirely**.
Writes were acknowledged that had never reached the device.

**Fix:** `GroupCommit::reset_to`, which may lower the watermark, called only from
`Wal::truncate_before` under the append lock.

This is exactly the class of bug crash testing exists to catch: every unit test
passed, the data structure was individually correct, and the failure only appears
in the interaction between truncation and the next commit, after a power cut. It
is now covered by a named regression test.

---

## 6. Backpressure works, and is tuned too aggressively

| maintenance | rows | parts at end | backpressure events | stalled |
|---|---|---|---|---|
| none | 30,000 | 60 | 24,000 | 74.2 s |
| compaction thread | 30,000 | 4 | 0 | 0 ms |

The mechanism satisfies §5.4 — debt is bounded and the stall is **observable in
metrics** rather than silent. But 74 seconds of cumulative stall for 30,000 rows
is punishing, and the defaults (`soft_limit: 12`, `hard_limit: 48`) were chosen by
intuition rather than measurement.

**Carried to M2:** tune the ramp against a real workload. The current shape is
correct; the constants are guesses, and this document should not pretend
otherwise.

---

## 7. Sustained ingest reaches equilibrium

5-second proxy for the roadmap's 6-hour soak (that belongs in CI, not in an
interactive run):

- 77,000 upserts applied
- part count: first 1, **peak 8, final 8**
- 314 compactions, 53,598 rows live at end

Final equals peak rather than exceeding it — compaction keeps up. A monotonically
climbing part count would have meant it could not.

**Carried to M2:** the real multi-hour soak, in CI, with memory tracked across the
whole run.

---

## 8. What was built

| Component | File | Purpose |
|---|---|---|
| Binary codec + CRC-32 | `codec.rs` | Length-prefixed, checksummed framing. Torn writes are *rejected*, never misread |
| Durability modes | `durability.rs` | `sync`/`group`/`async` with honest semantics; group-commit coordinator |
| Write-ahead log | `wal.rs` | Append, batched fsync, replay, truncation |
| Part persistence | `persist.rs` | Immutable image + append-only tombstone records |
| Manifest | `manifest.rs` | Append-only catalog snapshots; last valid record wins |
| Durable database | `storage.rs` | Open/recover, checkpoint, generation-versioned parts |
| Backpressure | `backpressure.rs` | Compaction-debt ramp with observable stalls |
| Two-phase compaction | `compaction.rs` | Merge outside the lock, install under it |

**Crash safety of checkpointing** deserves a note. Part files are
**generation-versioned** (`part-<table>-<id>-g<csn>.dat`) and the manifest commit
is the atomic switch. Rewriting a part in place would not be crash-safe: a crash
between truncate and sync leaves a file the manifest still references but which no
longer decodes. Versioning means a torn write only ever damages a generation
nothing points at yet. The old generation is deleted only after the switch, and
failure to delete wastes space without threatening correctness.

---

## 9. Test coverage

**335 tests**, all passing, ~2 s wall clock.

| Suite | Tests | Covers |
|---|---|---|
| unit (`src/`) | 207 | Codec framing, CRC bit-flip detection, group-commit leadership, WAL framing, manifest scan, part encoding, backpressure ramp |
| `io_faults.rs` | 20 | Fault injection, silent lost writes, filesystem-wide crash |
| `wal_ops.rs` | 16 | Durability modes, truncation, concurrent appends, batching |
| `table_ops.rs` | 15 | Insert/update/delete/upsert, sealing, index-memory regimes |
| `part_behavior.rs` | 14 | Lookup funnel, duplicate-key version resolution |
| `storage_recovery.rs` | 12 | Clean reopen, checkpoint bounds the log, CSN never regresses |
| `multi_table.rs` | 11 | Key-space independence, cross-table snapshot consistency |
| `crash_consistency.rs` | 10 | **~700 seeded crash injections**; torn-tail-at-every-offset; idempotent recovery |
| `mvcc.rs` | 10 | Snapshot isolation invariants |
| `concurrency.rs` / `determinism.rs` | 14 | Non-blocking reads, seeded workload replay |
| `hostile_keys.rs` | 5 | Fan-out under distributions chosen to defeat min/max bounds |

The crash suite asserts the actual contract: **every write acknowledged before
the crash is present after recovery, with its correct value, and every deleted key
stays deleted.** Torn WAL tails are exercised by truncating at every byte offset
and requiring the surviving prefix to be intact.

---

## 10. Gate 1 assessment

| Criterion | Result |
|---|---|
| M1-1 · survives randomized crash injection | ✅ ~700 seeded trials, all modes, incl. mid-checkpoint |
| M1-2 · recovery bounded by WAL tail | ⚠️ **Split.** Replay flat at 1.4 ms (met); total recovery scales (deferred to M2) |
| M1-3 · sustained ingest at equilibrium | ✅ Part count peaks and holds at 8; proxy soak only |
| M1-4 · backpressure before degradation, observable | ✅ Mechanism correct; ⚠️ constants untuned |
| M1-5 · write latency per durability mode | ✅ Documented; group commit 8× batching at 16 threads |

**Verdict: proceed to M2.** No finding suggests the architecture is wrong. The one
reclassification (FR-06) is a case of the requirement having been written more
broadly than the mechanism it named could deliver — worth fixing in the spec now
rather than discovering at M2.

### Carried into M2

1. **Demand-paged buffer pool** — the prerequisite for FR-06b, and now the
   highest-value structural work remaining.
2. **Tune backpressure constants** against a real workload.
   Also revisit Bloom false-positive rate: under overlapping key spans it is the
   only thing standing between a lookup and a full fan-out scan.
3. **Multi-hour soak in CI**, with memory tracked.
4. **Real `PosixIo`** — everything so far runs on `MemIo`. The seam exists and is
   exercised, but no test has yet touched a real filesystem, so real fsync
   ordering and partial-write behaviour remain unverified.
5. **Revisit `Durability::Sync` vs `Group`** once a real device is in play; on
   `MemIo` they are indistinguishable single-threaded.
