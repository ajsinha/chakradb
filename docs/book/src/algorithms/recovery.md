# Crash Recovery

Recovery reconstructs the exact acknowledged state after a crash: the catalog from
the manifest, the data from the parts, and everything since the last checkpoint by
replaying the WAL — discarding any torn tail.

## The procedure

> **ALGORITHM 7 — Recovery**
> ```text
> Input:  a database directory (manifest, part files, wal.log)
> Output: an open database at the last acknowledged state
> 1  M ← read manifest                                 ▷ catalog + live parts + checkpoint_csn
> 2  for each table T in M:
> 3      rebuild T's schema; register its live parts    ▷ indexes resident, data lazy
> 4  max ← M.checkpoint_csn
> 5  for each frame F in wal.log, in order:
> 6      if not crc_ok(F): break                        ▷ torn tail — stop cleanly (ALG 7a)
> 7      if F.csn ≤ M.checkpoint_csn: continue           ▷ already durable in parts
> 8      apply F to the in-memory tables                 ▷ Insert/Delete/Txn; skip unknown tables
> 9      max ← builtin_max(max, F.csn)
> 10 set the CSN clock floor to max + 1                  ▷ never reissue a stamp
> 11 return the open database
> ```

Three things make this correct and fast.

**Only the tail is replayed.** A checkpoint has already written everything at or
below `checkpoint_csn` into parts (recorded in the manifest), so recovery skips
those frames (line 7) and replays only what came after — usually a small tail.

**Parts load lazily.** Line 3 registers each part's *index* (Bloom, bounds, version
stamps, deletion vector) resident, but leaves its *column data* on disk until first
touched. Reopening a large database is therefore near-instant — flat in the number
of parts, not their bytes.

**Unknown tables are skipped.** If a frame references a table not in the manifest
(it was dropped, or a `TRUNCATE` gave it a fresh id), line 8 ignores it. This is
what makes `DROP TABLE` and `TRUNCATE` durable without a special log record — see
[the SQL surface](../guide/sql-reference.md).

## The torn tail

A crash mid-append leaves a partial final frame. Recovery detects it by checksum
and stops — the log's valid prefix is always recoverable:

> **ALGORITHM 7a — Torn-tail detection (line 6, expanded)**
> ```text
> Input:  the byte at the current read position in the log
> Output: the next valid frame, or STOP
> 1  if fewer than 8 bytes remain: STOP                 ▷ no room for len+crc
> 2  len ← read u32;  crc ← read u32
> 3  if fewer than len bytes remain: STOP               ▷ payload truncated
> 4  payload ← read len bytes
> 5  if crc32(payload) ≠ crc: STOP                       ▷ torn or corrupt frame
> 6  return decode(payload)
> ```

Everything after the first bad frame is discarded — those writes were never
acknowledged (their covering `fsync` had not completed), so dropping them is
correct.

```mermaid
flowchart LR
    subgraph log["wal.log after a crash"]
      F1["frame 1 ✓"] --> F2["frame 2 ✓"] --> F3["frame 3 ✓"] --> T["torn frame ✗"]
    end
    F1 & F2 & F3 -->|replayed| DB[("recovered state")]
    T -.->|discarded (crc fails)| X["(dropped)"]
    style T fill:#f5d6d6
```

## Why it recovers the exact acknowledged state

> **Proposition 5 (Recovery completeness).** After recovery, the database contains
> every acknowledged write and no unacknowledged one.
>
> *Proof sketch.* An *acknowledged* write is, by the durability mode
> ([ALGORITHM 5](wal.md)), one whose frame and covering `fsync` completed — so its
> frame is intact and precedes the torn tail, and it is replayed (or already in a
> part below the checkpoint). An *unacknowledged* write either never reached the log
> or sits in the torn tail; the CRC rejects the latter, so it is dropped. Hence the
> recovered set is exactly the acknowledged set. This is stress-tested by tens of
> thousands of randomized crash trials across all durability modes and schemas
> (`crash_consistency`, `durable_sql_crash`). ∎

## Clock safety

Line 10 raises the CSN clock above the highest replayed stamp. Without it, a
recovered database could hand out a CSN a replayed row already used, and two rows
would collide on a version number — corrupting [visibility](visibility.md). The
floor makes recovered CSNs safe: the next allocation is strictly greater than
anything on disk.
