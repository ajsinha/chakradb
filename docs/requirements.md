# ChakraDB — Architecture & Design Specification

**Document Version:** 2.0
**Status:** Design proposal, pre-implementation
**Supersedes:** v1.0, archived at `archive/requirements-v1.0.md`
**Core language:** Rust
**v1 target platforms:** `x86_64-unknown-linux-gnu`, `aarch64-apple-darwin` (Windows deferred — see §12)

---

> ### On sourcing — read this before citing any number in this document
>
> Version numbers and third-party project states reflect research conducted **2026-07-19** and
> are marked ⚠️ where they must be re-verified before being relied upon. Architectural claims
> are sourced to papers and shipped systems and are not time-sensitive.
>
> **Confidence tiers.** Treat claims here as three distinct grades:
>
> 1. **Architectural reasoning** (§3, §5, §6.1, §7) — derived from published designs and papers.
>    Durable; disagree with the reasoning, not the sourcing.
> 2. **Source-verified facts** — read directly from repository source, specs, commit history, or
>    API output. Examples: Delta's deletion-vector format, Doris/StarRocks PK-index mechanics,
>    DuckDB containing zero SIMD intrinsics, DataFusion's per-operator spilling matrix. These
>    carry a named artifact and can be re-checked.
> 3. **Reported figures without a retrievable source** — benchmark numbers, timings, quotes that
>    arrived without a URL. **Verify before citing externally.**
>
> **This distinction is not theoretical.** During the research behind this document, one
> automated research pass produced a body of specific-looking findings — exact microsecond
> timings, exact quotes, exact CI matrices — labelled as verified, which were **fabricated** and
> subsequently retracted by the agent that produced them. None reached this document (checked),
> but the incident is why the tiering above exists and why §10.2 insists on publishing our own
> harness and raw numbers rather than trusting figures that cannot be re-run.
>
> Rule of thumb: **a number without a source in this document is a hypothesis, not a
> measurement.** The design reasoning stands on its own and does not depend on any of them.

---

## 1. What ChakraDB Is

ChakraDB is an embedded, single-process analytical database that accepts a continuous
high-rate write stream while serving analytical scans that never block, and whose on-disk
state is a **valid open-format lakehouse table** that other engines can read directly.

### 1.1 The one-sentence differentiator

> DuckDB gives you fast scans over data you loaded earlier, in a closed file format.
> ChakraDB gives you fast scans over data that is still arriving, in an open format.

That is the entire wedge. Everything in this document exists to serve it.

### 1.2 Why "beat DuckDB" is the wrong goal (and what to do instead)

v1.0 of this spec opened with "superior to DuckDB." That framing should be abandoned,
for a concrete reason: **DuckDB's advantage is its execution engine**, built over roughly a
decade — push-based vectorized pipelines, morsel-driven parallelism, out-of-core hash joins
and aggregation, a mature join-order optimizer, and an enormous accumulated correctness
surface. A new project does not out-execute that on general analytics, and any plan whose
first move is "wrap an existing query engine" concedes the execution axis by construction.

What DuckDB is genuinely weak at is narrower and real:

| Axis | DuckDB's position | Opportunity |
| :--- | :--- | :--- |
| Concurrent writes + reads | Single process holds the write lock; concurrent writers across processes are not supported | **Yes — the core wedge** |
| Sustained high-rate ingest | Optimized for bulk load, not continuous streaming mutation | **Yes** |
| Point updates/deletes by key at rate | Possible but not the design center | **Yes** |
| Open on-disk format | Native format is a closed single file; open formats are read via extensions | **Yes** |
| Scan throughput on static data | World-class | No — do not compete here |
| SQL surface / dialect coverage | Very broad, very mature | No — do not compete here |
| Join optimization | Strong | No — do not compete here |

**Design rule derived from this table:** every hour of effort goes into the storage engine,
the concurrency control, and the ingest path. The query execution layer is *bought, not
built*, until measurement proves it is the bottleneck.

### 1.3 Honest competitive context

These already exist and overlap. Know them before starting.

**Start with this one — someone is already executing a close variant of our plan.**
**Spice.ai's "Cayenne" accelerator** (announced Dec 2025, shipped in Spice 2.0, July 2026) is
DataFusion-based and was built explicitly to replace their DuckDB-backed accelerator. Their
stated reasons for leaving DuckDB are almost verbatim our §1.2 thesis:

> "Single-file architectures create bottlenecks for concurrency and updates."
> "Memory usage of embedded databases like DuckDB can be prohibitive."

Their self-reported numbers against DuckDB v1.4.2 (16 vCPU / 64 GiB / local NVMe): TPC-H
**1.4× faster** than DuckDB file mode with **~3× less memory**; ClickBench 14% faster with
3.4× less memory. Spice 2.0 additionally claims 1,046 QPH analytical *concurrently with* a
266,861 tpmC transactional load — i.e. they are already measuring and publishing our NFR-03.

**How to read this:** it is validation, not defeat. It confirms the wedge is real, that
DataFusion is a viable base for it, and that the DuckDB gap on concurrency/memory is
exploitable. But it also means we are not first, and the differentiator narrows to execution
quality and the embedded/local-first form factor — Spice is a data-plane *product*, not an
embeddable library. Note also that these are **self-run benchmarks not on the public
ClickBench leaderboard**; ClickHouse's team publicly invited them to submit. Treat the numbers
as directional.

The rest:

- **chDB — RESOLVED, and it is a weaker competitor than its benchmarks suggest.** Embedded
  ClickHouse, actively maintained under ClickHouse Inc (v4.2.1, 2026-07-13; crashes fixed in
  days), and it does edge DuckDB on ClickBench geomean. But it is **not a rival for the
  embedded-HTAP niche**, for reasons that are structural rather than incidental:
  - **It has never been officially declared production-ready** — no such claim in the README,
    docs, or clickhouse.com. No case studies, no adopters list.
  - **It cannot safely run in-process.** The one production adopter I could verify (PostHog,
    since 2025-04) runs it in a **subprocess behind a 30-second timeout with a
    ClickHouse-cluster fallback**, and says why in a source comment: *"chdb has no query
    timeout, and a stalled S3 read can wedge a web worker indefinitely (each request also
    pins ~300MB of RSS for the embedded ClickHouse)."* For an embedded engine, needing a
    subprocess to stay killable is close to a disqualifying property.
  - **No spill-to-disk.** Open issue #610: large `ORDER BY` fails with
    `MEMORY_LIMIT_EXCEEDED` rather than spilling, and because `max_server_memory_usage_to_ram_ratio=0`,
    OOM becomes a **kernel kill of the host process rather than a catchable error**.
  - **The Rust binding is self-described as "experimental, unstable, and subject to changes"**
    — literally a "Rust FFI *example* binding." Python on macOS/Linux is the only mature path;
    Windows is unsupported and Go still requires a separately installed `libchdb.so`.
  - No transactions or row-level MVCC — it inherits ClickHouse's append/merge model.

  **Read:** chDB competes on scan speed, which §1.2 already says we do not. It does not compete
  on transactional embedded concurrency, which is our wedge. Treat it as a benchmark reference,
  not a rival. (Note also that Bytebase, Tinybird, and RunReveal are **not** chDB adopters —
  those claims are substring artifacts, e.g. "co-ckroa-**chdb**".)
- **DuckLake — RESOLVED, and it is the most consequential finding in this document.**
  This was flagged as the top item to verify. The answer: **DuckLake v1.0 shipped
  2026-04-13 and is production-ready**, with the reference implementation in DuckDB 1.5.2.
  It puts catalog metadata in any ACID SQL database (SQLite/Postgres/DuckDB/Aurora) rather
  than in object-storage files. v1.0 includes deletion vectors as **Iceberg-v3-compatible
  Puffin files**, sorted tables, murmur3 bucket partitioning, GEOMETRY and VARIANT, and
  clients for DataFusion, Spark, Trino, and pandas. v1.1 is due Sept 2026.

  **Two implications, and the second is uncomfortable.**

  First, the open-format half of our wedge is materially weakened. DuckDB now has a
  production answer to "my data should be readable by other engines," which was half of
  §1.1's one-sentence differentiator.

  Second — and this is the part worth sitting with — **DuckLake v1.0 ships "data inlining,"
  where small DML is written into the catalog database and only later flushed to Parquet
  (default threshold 10 rows). That is the same insight as our two-level commit (§6.1),
  shipped and in production.** We independently arrived at a design DuckDB already
  productized. That does not make our design wrong; it does mean it is not novel, and we
  should stop treating it as a differentiator.

  Separately, DuckDB's **Quack** remote protocol (beta, ~1.5.2/1.5.3, stable targeted at
  v2.0 in fall 2026) is their answer to multi-writer, reporting ~5,400 txn/sec at 8 threads.
  It is beta and expects breaking changes, but it is a direct move onto the concurrency half
  of our wedge too.

  **Recommended action: re-read §1 against this before M0.** The wedge is now narrower than
  when this document was drafted — closer to "embedded HTAP with real transactions" than
  "fast scans over arriving data in an open format." That may still be a real gap (see the
  ClickBench concurrency finding in §10.2), but it should be stated accurately.
- **StarRocks / Apache Doris** — solved the exact primary-key-over-columnar problem we face,
  in the server (non-embedded) space. Their primary key index design is the single most
  relevant prior art for §5. Read their engineering blogs before writing any code.
- **Apache Hudi** — merge-on-read lakehouse with record-level indexes. Same problem, batch
  latency profile.
- **ArcticDB, LanceDB, Vortex** — adjacent, each strong on one axis (timeseries, vector,
  file format respectively).
- **SingleStore, Umbra/CedarDB, SAP HANA** — the HTAP designs worth studying. Not embedded.

If after reading these you conclude someone has already built this — that is a *successful*
outcome of the research, not a failure. Better to learn it now than in month nine.

---

## 2. Design Constraints (decided, not open)

| # | Constraint | Consequence |
| :--- | :--- | :--- |
| C-1 | **Single writer process** owns a database directory | No distributed consensus. No catalog service. Massive simplification — protect it fiercely. |
| C-2 | **On-disk layout is a valid open table format** readable by external engines without export | Table format is on the hot path. Commit overhead becomes a first-class design problem (§6). |
| C-3 | **Multiple reader processes** may read the published state concurrently | Published state must be immutable-once-written and atomically switched. |
| C-4 | **Rust-native API only in v1** | Python/Java deferred. The C ABI is *designed for* but not shipped. |
| C-5 | All three performance axes matter (ingest, scan, concurrency) | Requires an explicit cost model (§3) rather than blanket claims. |

### 2.1 Explicit non-goals for v1

Writing these down is as important as the requirements. **Not** in v1:

- Distributed execution, sharding, or replication.
- Multi-process *writers*. One writer, enforced by a directory lock.
- **Foreign keys and referential integrity.** ChakraDB holds many tables, each
  with its own primary-key space, and guarantees that a snapshot is consistent
  across all of them — so an application reading two tables observes one
  instant. It does **not** enforce relationships between them. Cascading
  deletes, referential constraints, and join-time integrity checks are the
  application's responsibility. Primary-key indexing (§5.2) is the mechanism
  this engine is built around; foreign keys are a different feature with a
  different cost model, and adding them would put a second index structure on
  the write path.
- Secondary indexes. Primary key only (also listed below).
- Full PostgreSQL dialect compatibility (see §9 — this was a serious misjudgment in v1.0).
- Windows support (deferred to v2; it doubles the I/O and filesystem test matrix).
- Python and Java bindings.
- User-defined functions, extensions, stored procedures.
- Larger-than-memory *working sets* beyond what the underlying engine provides.
- Secondary indexes. Primary key index only.

---

## 3. The Cost Model (read this before the architecture)

The three goals — fast writes, fast scans, high concurrency — are in genuine tension. Every
system claiming all three has chosen where to absorb the cost. Stating our choice explicitly:

| Goal | Costs paid elsewhere |
| :--- | :--- |
| Fast writes | Data accumulates as unmerged deltas → scans get slower until compaction catches up |
| Fast scans | Requires merged, sorted, compressed columnar data → costs write amplification |
| High concurrency | Requires versioning → readers may pay version-resolution cost on every row |
| Open format on disk | Requires committing to Parquet + a manifest protocol → costs commit latency and constrains layout |

**ChakraDB's chosen absorption points:**

1. **Cost of fast writes is paid by background compaction**, not by readers. Compaction is
   a first-class, resource-budgeted, backpressure-aware subsystem — not an afterthought.
   If compaction cannot keep up, we apply *explicit* ingest backpressure rather than
   silently degrading scan performance. This is a hard design commitment.

2. **Cost of concurrency is paid only by data that was recently modified.** This is the key
   trick, borrowed from Neumann et al.'s MVCC design (§5.3). Cold, unmodified data carries
   *zero* per-row version overhead on scan. A scan of a billion-row table with a thousand
   recent mutations pays version costs on a thousand rows, not a billion.

3. **Cost of open-format compatibility is paid in external visibility latency**, not in
   internal write latency. Internal transactions commit fast to a local log; the external
   lakehouse snapshot is published on a separate, slower cadence (§6). External readers see
   a consistent but slightly stale view. **This lag is a tunable knob, not zero**, and
   pretending otherwise would be the same mistake v1.0 made.

If any of these three trades is unacceptable, the architecture must change — so challenge
them now, not after implementation.

---

## 4. Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────┐
│  API LAYER — Rust native (sync + async).  C ABI designed, not shipped │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
┌───────────────────────────────────▼───────────────────────────────────┐
│  SQL LAYER — sqlparser-rs (PG dialect) → logical plan → optimizer      │
│  Bought, not built. Swappable behind a narrow interface.               │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
┌───────────────────────────────────▼───────────────────────────────────┐
│  EXECUTION — vectorized operators over Arrow RecordBatches             │
│  v1: DataFusion. Boundary kept narrow so it can be replaced. (§8)      │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │  scan(snapshot_csn, projection, filters)
┌───────────────────────────────────▼───────────────────────────────────┐
│  ★ STORAGE ENGINE — THIS IS THE PROJECT ★                             │
│                                                                       │
│   ┌─────────────┐  ┌──────────────┐  ┌───────────────────────────┐    │
│   │ L0: Write   │  │ L1: Sealed   │  │ L2: Published Parquet     │    │
│   │ buffer      │→ │ in-memory    │→ │ + deletion vectors        │    │
│   │ (row-major) │  │ Arrow parts  │  │ (open table format)       │    │
│   └─────────────┘  └──────────────┘  └───────────────────────────┘    │
│                                                                       │
│   Primary Key Index (§5.2)  │  MVCC visibility (§5.3)  │  Compaction   │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
┌───────────────────────────────────▼───────────────────────────────────┐
│  I/O — WAL (group commit) │ buffer pool + direct I/O │ NO mmap (§7.3)  │
└───────────────────────────────────────────────────────────────────────┘
```

The mass of the work is in the starred box. v1.0 of the spec allocated roughly one paragraph
to it and many pages to FFI plumbing; that ratio is inverted here deliberately.

---

## 5. Storage Engine (the core design)

### 5.1 Three tiers, not "dual storage"

v1.0 proposed an LSM key-value store *beside* a columnar store, with merge-on-read joining
them. That is workable but pays a permanent tax: every scan merges a row store against a
column store, and the row store's format is alien to the execution engine.

Instead, use **one tiered columnar store** where the tiers differ in mutability and location:

| Tier | Format | Location | Mutable? | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **L0** | Row-major append buffer | Memory + WAL | Yes | Absorb writes at memory speed |
| **L1** | Arrow, sealed, PK-sorted | Memory (bounded) | No (DV only) | Recent data, scannable at full vector speed |
| **L2** | Parquet + deletion vectors | Disk, open format | No (DV only) | Bulk of data, externally readable |

Writes land in L0. When L0 reaches a size or age threshold it is **sealed**: converted to
sorted Arrow columnar form as an immutable L1 part. L1 parts are periodically compacted and
published to L2 as Parquet. Deletes and updates never modify a sealed part in place — they
append to that part's **deletion vector** and, for updates, insert a new version in L0.

This gives one uniform scan interface: *every* tier hands the executor Arrow RecordBatches
plus a deletion vector. There is no row-store/column-store merge on the read path.

### 5.2 The Primary Key Index — the hardest piece

**This is the component that determines whether the project works.** v1.0 omitted it entirely.

An `UPDATE t SET x=1 WHERE pk=42` must locate which part and which row ordinal currently
holds the live version of `pk=42`. Without an index this is a full scan, and the write path
collapses.

Two production systems have solved exactly this problem, by opposite methods. The choice
between them is **the** architectural decision of this project.

#### Prior art, verified from source

| | **Apache Doris** (merge-on-write) | **StarRocks** (primary key table) |
| :--- | :--- | :--- |
| Address | `(rowset, segment, rowid)` where **rowid = ordinal position in the PK index** | `IndexValue` = one u64: `rssid << 32 \| rowid` |
| Index location | **Per-segment**, embedded in the segment file | **Per-tablet**, one global structure |
| Structure | Sorted, paginated, prefix-encoded + ZSTD (RocksDB partitioned-index style) | Static hash sharded by key length; LSM with L0 (memory+WAL) / L1 / L2 |
| Lookup | Ordered `seek_at_or_after` per candidate segment, **newest-first**, gated by segment min/max then a per-segment bloom filter (fpp 0.01) | One **batched hash probe**; L0 → L1 → L2 newest-first, bloom-gated, optionally parallel across levels |
| Cost | O(#candidate rowsets × log n) — degrades as rowset count grows | O(#LSM levels) — bounded by compaction |
| Memory | **Only bloom filters + hot index pages resident** | Whole index resident, or L0 + page cache when persistent |
| Requires PK-sorted segments? | **Yes** — that is precisely what makes ordinal == row offset | No |

**The key insight, and it is worth internalizing before designing anything:** Doris gets the
PK→rowid mapping *for free* by exploiting sort order. Because a merge-on-write segment is
written sorted by primary key, the i-th entry of the PK index is the i-th row of the segment —
so an ordinal lookup in the index *is* the row offset in every column. **No separate mapping
structure exists.** StarRocks instead pays 8 bytes per row to store the location explicitly,
which buys random-order segments and O(1) probes, and which is why they then needed an
L0/L1/L2 LSM to make the memory tractable.

StarRocks' own documented memory formula makes the cost concrete:
`(PK_length + 9) × row_count × replica_count × 1.5`. For 1B rows with 16-byte keys that is
**~37 GB** — untenable for an embedded engine, and exactly why their persistent index exists
(they claim it cuts resident memory to roughly 1/10).

#### Decision for ChakraDB: follow Doris

**§5.1 already specifies that L1 and L2 parts are sealed and PK-sorted. That makes the Doris
approach available to us, and it is decisively cheaper.** Concretely:

- **No global PK→location map.** Per-part, we store a sorted PK index whose ordinal *is* the
  row offset. Multi-column PKs encode into a single memcmp-ordered byte slice, so a composite
  key costs the same as a single one.
- **Resident memory is bloom filters plus hot index pages**, not one entry per row. This
  removes the binding constraint on table size that an explicit map would impose, and it
  changes NFR-07 from a hard ceiling into a cache-tuning question.
- **Lookup funnel, cheapest filter first** (copy this ordering exactly):
  1. Part-level PK min/max bounds — pure metadata, no I/O.
  2. Per-part bloom filter on the PK — target fpp 0.01, as Doris uses.
  3. Ordered seek within the part's PK index.
  4. Deletion-vector recheck — a hit that is already deleted must not win.
- **Search parts newest-first and stop at the first hit.** Cost then scales with the *recency*
  of the key, not with total data size — recently-written keys are found in L0/L1 immediately.
- **Persistence is nearly free**, because the index lives inside the immutable part file that
  already had to be written. Recovery replays only the WAL tail. This directly satisfies FR-06
  (recovery bounded by WAL tail, not database size) without a separate index-snapshot mechanism.

**The cost we accept, and it must be watched:** lookup fans out across candidate parts, so cost
grows with part count. **This makes compaction load-bearing for write performance, not just
scan performance** — §5.4's part-count trigger is what keeps the fan-out bounded. If part count
is allowed to grow unchecked, the write path degrades, not merely the read path.

**Where this breaks:** if we ever want parts sorted by something other than the primary key
(a clustering key for scan locality), ordinal ceases to equal row offset and we must store the
rowid explicitly — which is exactly what Doris does for cluster-key tables. Do not add
clustering keys in v1 without revisiting this section.

**M0 must measure:** lookup latency vs part count, bloom filter memory at 1M/10M/100M rows, and
the point at which fan-out forces compaction. These are the numbers that decide viability.

**Open question requiring a decision before implementation:** do we require a declared
primary key on every table? Options: (a) require it — simplest, restricts use; (b) allow
PK-less append-only tables that support INSERT and full-file DELETE but not point UPDATE —
recommended, since it makes the common streaming-ingest case cheap; (c) synthesize a hidden
row ID — costs an index on everything. **Recommendation: (b).** Tables opt into point
mutability by declaring a PK, and pay the index cost only then.

### 5.3 MVCC and visibility

**Model:** snapshot isolation via a monotonic **commit sequence number (CSN)**, allocated by
the single writer.

- Every transaction reads at a snapshot CSN. **Readers never block and never take locks.**
- Every row version carries a `created_csn`; deleted rows gain a `deleted_csn`.
- A row is visible to snapshot `S` iff `created_csn ≤ S < deleted_csn`.

**The critical optimization — why scans stay fast:** version metadata is *not* stored inline
per row in the columnar data. It lives in a **per-part side structure**:

- An immutable part created by compaction has a single uniform `created_csn`. If the reading
  snapshot is newer than it and the part has no deletions, **every row is visible and the
  scan performs zero per-row visibility work** — it hands the raw Arrow buffers straight to
  the executor.
- Deletions are a **roaring bitmap deletion vector**, versioned by CSN. A scan resolves the
  DV once per part, not once per row, producing a selection mask the vectorized operators
  already know how to consume.
- Only parts with mutations *in the reader's visibility window* pay any resolution cost.

This is the Neumann/Mühlbauer/Kemper insight (*Fast Serializable MVCC for Main-Memory
Database Systems*, SIGMOD 2015) applied to a columnar tiered store: **keep the version
machinery off the hot scan path and out of the common case.**

**Write–write conflicts:** with a single writer process, transactions serialize on the PK
index. Detect conflicts optimistically at commit by checking whether any key in the write set
gained a newer version since the transaction's snapshot; abort and surface a retryable error
if so. v1 provides **snapshot isolation**. Serializable is a later addition (and requires
read-set tracking or predicate locking — do not promise it in v1).

**Garbage collection:** old versions and superseded DV entries are reclaimable once no active
snapshot can see them. Track the oldest active reader CSN as a watermark. **A long-running
analytical query pins that watermark and blocks reclamation** — this is a real operational
failure mode (it is how Postgres gets bloat) and needs a documented mitigation: a configurable
maximum snapshot age after which queries are cancelled.

### 5.4 Compaction

Compaction is the mechanism that pays for fast writes, so it is a designed subsystem, not a
background thread that runs when convenient.

- **Trigger policy:** by DV density (a part whose rows are >N% deleted wastes scan bandwidth),
  by part count (too many small parts destroy scan performance), and by age.
- **Resource budget:** explicit caps on CPU threads and I/O bandwidth, configurable. Compaction
  must never starve foreground queries.
- **Backpressure:** when compaction debt exceeds a threshold, ingest is *explicitly slowed*
  and the condition is *observable*. Silent degradation is forbidden.
- **Correctness:** compaction produces new parts and atomically swaps references. Old parts
  are retained until no snapshot references them. Compaction must be crash-safe — a crash
  mid-compaction leaves the old parts authoritative.

---

## 6. Lakehouse Compatibility — resolving the central tension

Constraint C-2 (on-disk = open format) and the ingest goal are in direct conflict. Iceberg and
Delta commits involve writing manifests and atomically swapping a metadata pointer; per-commit
cost is on the order of milliseconds and involves multiple file operations. **You cannot do
that per transaction at high write rates.** Any design claiming both is wrong.

### 6.1 Two-level commit (the proposed resolution)

Separate the *transactional* commit from the *publication* commit:

```
Transaction commit (fast path, sub-millisecond)
   → append to local WAL, group-committed
   → advance CSN, update PK index and in-memory part references
   → visible immediately to in-process readers
                        │
                        │   (asynchronous, batched — seconds)
                        ▼
Publication commit (slow path)
   → compact L1 parts into Parquet files
   → write deletion vectors in the open format's DV encoding
   → append a table-format snapshot (Iceberg/Delta) referencing them
   → external engines now see the new data
```

**Consequences, stated plainly:**

- In-process readers see data with sub-millisecond freshness.
- External engines (Spark, Trino, DuckDB) see data with **publication-interval staleness** —
  seconds by default, tunable down at the cost of write throughput and small-file pressure.
- The WAL, not the table format, is the durability mechanism for recent writes. **A crash
  between publications is recovered from the WAL, not from the lakehouse snapshot.**
- This means the on-disk state is only a valid, complete table *as of the last publication*.
  That must be documented prominently — a user who copies the directory mid-run gets the last
  published snapshot, not the latest data.

### 6.2 Which table format

| Option | For | Against |
| :--- | :--- | :--- |
| **Delta Lake** | `delta-kernel-rs` is a maintained Rust implementation with deletion-vector support already implemented; DV design maps directly onto §5.3; protocol is fully specified in one document | Ecosystem narrower than Iceberg in some shops |
| **Iceberg** | Broadest engine support; v3 spec adds deletion vectors and row lineage | `iceberg-rust` maturity ⚠️ verify — ParadeDB cited "Iceberg support for DataFusion is in a nascent stage as part of the `iceberg-rust` project" as a reason to abandon it in 2024; heavier metadata layer; v4 discussions in flight ⚠️ |
| **DuckLake** | Metadata in SQL — a natural fit for an embedded engine that already has a catalog | Newer, narrower adoption ⚠️ verify; also a direct competitor |
| **Plain Parquet + own manifest** | Simplest, fastest | Fails C-2 — not readable as a *table* by other engines |

**Recommendation: abstract the table format behind a trait and implement Delta first.**

This is now a well-supported recommendation rather than a guess. Verified specifics that matter
to our design:

- Delta deletion vectors are **64-bit RoaringBitmaps in the "portable" format** (magic number
  `1681511377`), storing **physical row positions within a Parquet file, zero-based**. This is
  *exactly* the representation §5.3 proposes for our DVs — we can use one representation for
  both the internal engine and the published table, with no translation layer.
- DVs may be stored as separate `.bin` files (possibly several DVs per file, addressed by byte
  offset) or **inlined into the JSON log via Z85 encoding** for small vectors. The inline path
  is valuable for us: a small DV update need not create a file.
- The `DeletionVectorDescriptor` carries `cardinality`, and the protocol requires `numRecords`
  in stats to describe the *physical* file — so logical row count is `numRecords - cardinality`.
  Our statistics layer must follow this convention.
- Note `stats.tightBounds`: when DVs are present, min/max bounds become **wide** rather than
  tight. Data skipping still works, but our statistics must set this flag correctly or external
  readers will produce wrong results.
- Feature gating: DVs require **reader version 3 / writer version 7** with `deletionVectors` in
  both `readerFeatures` and `writerFeatures`. Readers must honor DVs whenever the table feature
  is present, regardless of the `delta.enableDeletionVectors` property.
- Maturity by operation (OSS Delta): DV **reads** from 2.3, **DELETE** from 2.4, **UPDATE** from
  3.0, **MERGE** from 3.1. All well past initial release.

The one caution: DV-based merge-on-read is **not universally faster**. Databricks' published 10×
MERGE figure is against Low-Shuffle MERGE on a 3 TB TPC-DS workload, and there are documented
cases where MERGE with DVs is *slower* than a full file rewrite. This is the §3 cost model
appearing in someone else's system: DVs trade write cost for read-time filtering plus eventual
purge. Our compaction policy (§5.4) is what decides whether that trade pays off, which is why
DV density is a first-class compaction trigger.

⚠️ Re-verify `iceberg-rust` maturity before scheduling the Iceberg implementation in M5.

### 6.3 File format for the hot tier

Parquet is required for L2 (interop is the point). It is not required for L1, where we control
both writer and reader. Arrow IPC is the obvious v1 choice — zero decode cost, direct scan.
Vortex is worth evaluating later for L2 if its random-access and scan claims hold up ⚠️, but
adopting a non-Parquet format for L2 would break C-2 and must not be done in v1.

---

## 7. Systems-Level Decisions

### 7.1 Concurrency model

- **Reads:** lock-free against an immutable snapshot. Scans of different parts are independent
  work units, distributed across a fixed worker pool. This is **morsel-driven parallelism**
  (Leis et al., SIGMOD 2014) and it is the right model — it gives NUMA-aware scheduling and
  natural load balancing across concurrent queries.

  **Part size is a parallelism decision, not just a storage one.** DuckDB's unit of scan
  parallelism is the row group, default **122,880 rows** — which means a table needs
  `k × 122,880` rows before it can saturate *k* threads. Our L1/L2 part sizing inherits the
  same constraint: parts too large starve parallelism, parts too small explode the PK-index
  fan-out described in §5.2. These two pressures bound part size from both sides, and M0
  should measure where the window is.

  Worth copying from DuckDB's scheduler while we are here: a task that blocks (on I/O, on a
  dependency) returns a `BLOCKED` result and is *descheduled* rather than parking a worker
  thread, then rescheduled by callback when its dependency completes. Any operator that can
  wait must therefore be a resumable state machine, not a function holding a stack. Velox
  reaches the same conclusion by the same route. Decide this at the operator-interface level
  early — it is not retrofittable.
- **Writes:** single writer thread owns the CSN counter and PK index mutation. Write batching
  and group commit make this fast enough; contention on a single writer is far cheaper than
  a concurrent index. **Revisit only if measurement shows the writer thread saturating.**
- **Thread pool:** a fixed pool sized to cores, not an unbounded async runtime. ⚠️ Note that
  building on DataFusion imports Tokio; keep query execution and I/O scheduling distinct.

### 7.2 Durability

Configurable per transaction, with honest names:

| Mode | Behavior | Approximate cost |
| :--- | :--- | :--- |
| `sync` | fsync before ack | Bounded by device fsync latency — hundreds to low thousands of commits/sec on typical NVMe |
| `group` (default) | Batched fsync, ack after the group's fsync | High throughput, sub-ms latency, no data loss |
| `async` | Ack before fsync | Fastest; **bounded data loss on power failure** — must be documented as such |

**v1.0's claim of 1M rows/sec with immediate ack was only achievable in `async` mode, while
simultaneously claiming ACID durability.** Those are incompatible. Group commit is the honest
answer: batching amortizes fsync across many transactions and reaches high throughput without
lying about durability.

### 7.3 No mmap on the durable path

v1.0 built the storage layer on memory-mapped files. This is a known trap, documented in
Crotty, Leis & Pavlo, *"Are You Sure You Want to Use MMAP in Your DBMS?"* (CIDR 2022). The
core problems: no control over eviction, transparent page-fault stalls that are invisible to
the query scheduler, no way to guarantee write ordering for crash consistency, and severe
TLB shootdown costs under concurrency.

**Decision:** a buffer pool with explicit I/O. **Default to a thread pool over `pread`/`pwrite`**
— this is a perfectly respectable v1 and should remain the assumption until io_uring is proven
to win on our specific workload. ⚠️ The Rust io_uring crate ecosystem has churned badly and
several once-promising crates are unmaintained; verify before depending on any of them.

If io_uring is later adopted, the kernel floor matters and is now precisely known:

| Feature | Min kernel | Relevance to us |
| :--- | :--- | :--- |
| io_uring core, `REGISTER_BUFFERS`, `READ_FIXED`/`WRITE_FIXED`, `SQPOLL`, `IOPOLL` | **5.1** | Baseline |
| `SQPOLL` without elevated privileges | 5.11 (CAP_SYS_NICE) / **5.13** (none) | Matters for an embedded library that cannot assume privileges |
| `IORING_SETUP_SINGLE_ISSUER` | **6.0** | Fits our single-writer model exactly; meaningful perf win |
| `IORING_SETUP_DEFER_TASKRUN` | **6.1** | Reduces scheduling overhead for a dedicated I/O thread |
| `IORING_OP_URING_CMD` (NVMe passthrough) | 5.19 | Only if we ever go direct-to-device |

Since we are a *library* embedded in someone else's process, we cannot dictate the kernel.
Any io_uring path must degrade cleanly to the thread pool at runtime, detected by feature
probe rather than by kernel version string.

**Two serious databases independently hedge away from io_uring, which is why the default above
is what it is.** ScyllaDB compiles io_uring support in and then *explicitly overrides* it for
the server — `main.cc` carries the comment `// We don't want ScyllaDB to run with the io_uring
backend` and forces linux-aio, using io_uring only for CLI tools. PostgreSQL 18 shipped
`io_method = io_uring` as **opt-in at build time and not the default** (`worker` is), and that
remains true in 19-devel; their own published benchmarks show the worker method *beating*
io_uring on buffered reads, because worker threads parallelize the page-cache memcpy. TigerBeetle
is the counterexample — io_uring only, no fallback, kernel 5.11 minimum — but it is a
single-purpose system that controls its own deployment, which we do not.

**Deployment reality — two independent runtime failure modes, neither caught by a version check:**

| Environment | io_uring state |
| :--- | :--- |
| **RHEL 9.8 / 10.2+** | **Fully supported** as of 2026-03 (graduated from Tech Preview, commit `3a972ceadda3`) — but still ships `kernel.io_uring_disabled = 2`, i.e. **off by default**. Red Hat's release notes don't mention the default |
| Ubuntu / Debian / Alpine / Amazon Linux 2023 | Enabled, upstream default (`=0`). Canonical never restricted it — they backported the sysctl so admins *could* |
| **Docker / containerd** | **Default seccomp profile blocks syscalls 425–427.** [moby#47532](https://github.com/moby/moby/issues/47532) requesting a change was **closed as "not planned"** |
| **GKE Container-Optimized OS** | Sets `unprivileged_bpf_disabled`, `kptr_restrict`, `ptrace_scope`, `perf_event_paranoid` — but **not** `io_uring_disabled`. GKE's block is purely the containerd seccomp allowlist |
| RHEL for Automotive | Compiled out entirely |

The kernel-side picture is *loosening* (Red Hat made it supported; Axboe landed per-opcode cBPF
filtering, and openSUSE Tumbleweed already ships `CONFIG_IO_URING_BPF=y`), while container
userspace defaults stay closed. ⚠️ The exact kernel version for the BPF filtering work is
unconfirmed — Axboe's `IORING_REGISTER_BPF_FILTER` and Begunkov's BPF struct_ops are two
distinct workstreams reported against 7.0 and 7.1 respectively.

**As an embedded library we control none of this.** The sysctl and the seccomp profile are two
independent ways io_uring fails at runtime on a correctly-configured modern kernel. Probe for
capability, never for version, and make the thread-pool path the one that gets the most testing
— because on RHEL and in Docker it *is* the path.

**Conclusion: treat io_uring as a measured optimization, never an architectural assumption.**

### 7.3.1 The userspace I/O scheduler — where the tail-latency wins actually are

This is the most valuable lesson available from prior art, and it corrects a hand-wave in §5.4
("explicit caps on I/O bandwidth"), which named a goal without a mechanism.

**ScyllaDB/Seastar is the strongest natural experiment for io_uring** — thread-per-core,
shared-nothing, direct-I/O-only, written by kernel people. There, io_uring delivered **−4%** on
first implementation (Avi Kivity's own patch, 2022-05, shipped non-default because of it), then
"on par" latency with modestly better throughput five months later, and **+6.7%** direct-I/O
IOPS in their own promotional benchmark. Single digits, after expert effort, in the most
favorable environment it will ever get.

What actually bought them their latency wins — **up to 55% p99.9 reduction** — was a *userspace
I/O scheduler*, built in 2016 and still sitting above the backend today, unchanged in role when
io_uring arrived. The lesson is direct: **io_uring gives you submission, not admission control.**

Four things we must build ourselves regardless of syscall interface:

1. **Priority classes.** Compaction vs. foreground scan vs. WAL flush is *our* semantics; the
   kernel has no channel to express it. Scylla assigns static shares per class (commitlog,
   memtable, compaction, reads, repair). This is the concrete mechanism §5.4 needs — not a
   thread cap, a share-based fair queue over tagged requests.
2. **Bounded in-flight depth.** Past a device's internal queue depth, more requests buy *zero*
   throughput and pure latency. Scylla's original motivation was starker still: exceed the
   Linux block layer's outstanding-request limit and submission goes **synchronous**, which for
   a thread-per-core design is fatal.
3. **Inferred disk queue depth.** This is the subtle one. The queue accumulating *inside the
   device* is what destroys tail latency, and it is **not observable from userspace by any
   syscall interface**. Seastar infers it statistically from the long-run
   dispatched-to-completed ratio. If we have no equivalent, we have no tail-latency control.
4. **Read/write cost asymmetry.** Their current model budgets four contending terms —
   read/write × IOPS/bandwidth — against a single normalized token bucket sized from an
   explicit latency goal. Writes cost more than reads and the ratio is device-specific.

**Ordering consequence for the roadmap:** build the accounting and priority layer *before*
reaching for io_uring. It is where the wins are, it is interface-independent, and it is what
makes §5.4's "compaction must never starve foreground queries" an enforceable property rather
than an aspiration. Keep the submission backend behind a trait — which §11.1 requires anyway
for simulation — and the io_uring question reduces to a swappable, measurable detail.

mmap remains acceptable for *read-only, already-published, immutable* files where a page fault
merely stalls one scan — but even there it must be measured, not assumed.

### 7.4 SIMD — do not hand-write it

v1.0 specified runtime SIMD dispatch across AVX-512 / AVX2 / NEON with hand-written kernels.
This is now the wrong instinct, and the evidence is unusually clear.

**arrow-rs deleted its entire `simd` feature** (PR #5184, merged 2023-12-08, shipped in arrow
50.0.0) after benchmarks showed autovectorized code matching or beating hand-written SIMD.
Today arrow-rs contains **zero uses of `core::arch`** — the only `std::arch` uses in the whole
repo are 128-bit division and a `_pext_u64` in parquet, neither a data kernel. The policy is
codified in `arrow/CONTRIBUTING.md`: *"This crate does not use SIMD intrinsics directly, but
instead relies on the Rust compiler's auto-vectorization capabilities."*

**Every serious columnar engine has independently reached the same conclusion. Verified by
source inspection, not reputation:**

| Engine | Hand-written SIMD |
| :--- | :--- |
| **DuckDB** | **Zero.** No file in `src/` (2,979 files, 577k LOC) matches `immintrin\|_mm[0-9]*_\|__m256\|__m128\|arm_neon`. The only intrinsics in the repo are vendored zstd/mbedtls/snappy. Its compression codecs — ALP, bitpacking, chimp, patas, FSST, roaring — are `__restrict` + templates and nothing else |
| **Velox** | **0.14% of lines** — 993 lines across 49 of 2,788 files, concentrated in one utility header pair plus filters, decoders, and hashing |
| **Photon** | Its own SIGMOD paper: *"often we rely on the compiler to auto-vectorize the kernel (and provide hints such as RESTRICT annotations)."* It publishes exactly **one** SIMD-attributed number (3× on `upper()`); the headline 3× average is attributed to native decimals and load parallelism |

Mühleisen stated DuckDB's position on the record: *"while the MonetDB/X100 system needed to use
explicit SIMD, DuckDB can rely on the auto-vectorization of our (carefully constructed) loops."*

**Two humbling corollaries.** First, **DuckDB ships baseline x86-64 binaries** — `-march=native`
is gated behind an opt-in `NATIVE_ARCH` flag, so the artifacts most people run are limited to
SSE2. DuckDB beats DataFusion at 16 cores *while compiled to SSE2*. The gap is architecture, not
instruction selection. Second, note where Velox's 0.14% actually lives: **selectivity vectors
and filters** — precisely the one exception carved out below. Independent convergence on the
same carve-out is the strongest signal available that the exception is the right one.

**Decision: inherit arrow-rs's kernels and its doctrine.** Where we write our own kernels
(deletion-vector application, visibility resolution, merge), copy the arrow-rs playbook:

- Independent accumulator arrays sized by `cfg!(target_feature)`, folded down at the end —
  this is what lets LLVM use vector registers despite float addition being non-reorderable.
- `chunks_exact(LANES)` loops with no conditionals in the body.
- Nulls and deletions as **shifted 64-bit validity words**, never per-row branches. Our
  deletion vectors are RoaringBitmaps; materializing them into u64 selection words is the
  fast path.
- `#[inline(always)]` on inner chunk functions, `#[inline(never)]` on the loop driver — the
  vectorizer gives up on over-inlined code.
- Verify on Godbolt before assuming vectorization happened.

Two consequences worth writing down:

1. **`std::simd` is not available and should not be planned for.** It remains nightly-only
   under `#![feature(portable_simd)]` five years on, and its tracking issue (rust-lang/rust
   #86656) still has open *design* questions — mask semantics, supported vector sizes — with
   16+ months of silence. Do not architect around it landing.
2. **There is exactly one place hand-written SIMD is likely to pay: selection-vector
   generation.** arrow-rs's `filter` kernel (`arrow-select/src/filter.rs`) contains **zero**
   SIMD — verified, no `target_feature`/`avx`/`neon` anywhere. Its own recent 3× wins came
   from removing allocation overhead, not vectorizing. Meanwhile Velox hand-writes exactly
   this primitive (`simd::filter`, an AVX2 permute-index table). Realistic gain from an
   AVX2 permute-table selection kernel: **2–3× at 25–90% selectivity, ~1.0× below ~6%.**

   Two disciplines before writing a line of it: (a) copy arrow-rs's *structure* first — they
   switch strategies at a `FILTER_SLICES_SELECTIVITY_THRESHOLD` of 0.8, using contiguous-run
   copies above it and per-index gather below. That selectivity-adaptive switch matters more
   than vector width. (b) AVX-512 must be **runtime-dispatched, never compile-time**: Intel
   fused AVX-512 off in consumer parts from Alder Lake onward, so it is a server-and-AMD-Zen4+
   feature. Use `is_x86_feature_detected!` + `#[target_feature]`.

   Conversely, **do not** hand-SIMD null-mask AND/OR (LLVM autovectorizes `&[u64]` loops;
   gain ≈ 1.0×) or popcount (`u64::count_ones()` is fine — the mask is 64× smaller than the
   data column it describes, so it will never be your bottleneck). And do not hand-write
   bit-unpacking: the FastLanes work (Afroozeh & Boncz, PVLDB 16(9), 2023) demonstrates
   scalar code with the right memory layout **auto-vectorizes to match explicit intrinsics**,
   and the authors recommend shipping the scalar path. Use the `fastlanes` crate if we need
   these kernels — noting it is *not* binary-compatible with reference FastLanes, which is
   fine only because we own our hot-tier format.

3. **Use `croaring`, not pure-Rust `roaring`, for deletion vectors.** This follows from §5.3
   and §6.2 making RoaringBitmaps load-bearing. `croaring` (CRoaring) does genuine **runtime**
   CPUID dispatch into AVX2 and AVX-512 (VBMI2/BITALG/VPOPCNTDQ) paths, enabled by default
   with no `target-cpu` required. Pure-Rust `roaring`'s `simd` feature **requires nightly**
   (`portable_simd`) and covers only sorted-array containers, not dense bitmaps — on a stable
   build you get scalar. The cost is a C dependency via `cc`, which conflicts with the DST
   ambition in §11; that tension needs an explicit decision, not a default.

4. **`-C target-cpu` matters enormously and is the user's choice, not ours.** arrow-rs's
   AVX/AVX-512 paths are selected by `cfg!(target_feature)` at compile time, meaning they are
   **dead code in a default `cargo build`**. Since we ship a library, we must document this
   prominently: a user who does not set `target-cpu` leaves large performance on the table.
   Runtime dispatch via `#[target_feature]` (usable on safe fns since Rust 1.86; AVX-512
   intrinsics stable since 1.89) is the escape hatch if we need one — but reach for it only
   with a benchmark proving autovectorization failed.

---

## 8. Query Execution — buy, don't build (for now)

**Recommendation: build on DataFusion for v1**, behind a deliberately narrow interface.

Rationale:

- Our differentiator is storage and concurrency, not execution. Spending year one rebuilding
  hash joins is spending it on the axis we already decided not to compete on (§1.2).
- It brings a large SQL surface and correctness baseline for free.
- **Spilling is real but incomplete — and the gap is in the worst possible operator.**
  Verified per-operator against DataFusion 54 (released 2026-06-08):

  | Operator | Spills? |
  | :--- | :--- |
  | `SortExec` (multi-level merge, since 50.0.0) | **Yes** — mature |
  | `AggregateExec` (grouped hash) | **Yes** |
  | `SortMergeJoinExec` (since 41.0.0) | **Yes** |
  | `NestedLoopJoinExec` (since 54.0.0, all join types) | **Yes** |
  | `RepartitionExec` (since 51.0.0) | **Yes** |
  | **`HashJoinExec`** | **NO** → `ResourcesExhausted` |
  | `CrossJoinExec` | **NO** → `ResourcesExhausted` |
  | `WindowAggExec` / `BoundedWindowAggExec` | **NO** — and no memory reservation at all, so they grow unbounded rather than erroring cleanly |

  **`HashJoinExec` not spilling is the single most important caveat in this document's
  execution story.** It is the default join operator for analytics. The build side collects
  fully into memory and returns `ResourcesExhausted` when it doesn't fit — the code comment
  says "Decide if we spill or not" but the line below it is `try_grow(...)?`. Issues #12952
  and #17267 are open with a hybrid-hash-join design and **no implementation PR**.

  Consequences we must accept and plan around: (a) larger-than-memory hash joins fail rather
  than degrade — this must be documented, not discovered by users; (b) the mitigation is
  either `SortMergeJoinExec` (which does spill) or contributing the hash-join spill upstream,
  which is a plausible way to spend fork budget productively; (c) window functions growing
  unbounded is arguably worse than failing, since it OOMs the *host process* we are embedded
  in. **Test window functions under memory pressure early.**

  What *is* solid, and it is a lot:
  - `MemoryPool` trait with `GreedyMemoryPool`, `FairSpillPool`, and `TrackConsumersPool`
    (which reports top consumers — useful for diagnosing our own operators).
  - `DiskManager` with configurable spill directories and `max_temp_directory_size`
    (default 100 GB, added 2025-04).
  - `SpillManager` with optional spill compression (`lz4_frame` / `zstd`, added in 49.0.0).
  - A `SpillPool` channel abstraction for rotating spill files (added 51.0.0).
  - **A pluggable `SpillFile` / `TempFileFactory` trait merged 2026-06-29** — this is the
    extension point if we ever want spill to route through our own I/O layer rather than
    local disk. Worth knowing it exists before designing around its absence.

**Plan for a fork. This is the observed industry norm, not the exception.** Verified: both
GreptimeDB and Spice.ai — two independent, well-resourced DataFusion-based products — carry
permanent forks pinned by git rev rather than consuming DataFusion from crates.io. Spice forks
`arrow-rs` as well, and names its branches per upstream version (`spiceai-54`), implying a
disciplined per-release rebase. ParadeDB likewise pins ~28 DataFusion crates to its own fork.

Budget for this explicitly: a fork, a rebase cadence tied to DataFusion's ~2.5-month major
release cycle, and the engineering time that implies. A plan that assumes clean crates.io
consumption is a plan that will be surprised.

### 8.1 The execution ceiling, measured

This was the last open ⚠️ in the document. It is now answered, and the answer is worse than the
2024-era optimism suggested — **but it contains a genuine surprise that changes how we position
the project.**

**DataFusion loses badly at low core counts and wins decisively at high ones.** ClickBench,
Parquet-partitioned, total hot time:

| Machine | DataFusion | DuckDB (native) | Winner |
| :--- | :--- | :--- | :--- |
| c6a.xlarge (4 vCPU) | 218.9s | **94.5s** | DuckDB by 2.3× |
| c6a.2xlarge (8) | 84.6s | **43.1s** | DuckDB by 2.0× |
| c6a.4xlarge (16) | 42.3s | **26.3s** | DuckDB by 1.6× |
| c6a.metal (192) | **11.4s** | 17.3s | **DataFusion by 1.5×** |
| c7a.metal-48xl (192) | **7.4s** | 15.7s | **DataFusion by 2.1×** |

⚠️ **Read that table with two caveats, both of which cut against over-reading it.**

*First, the comparison basis matters and flips results.* The table above is DataFusion-on-Parquet
vs DuckDB's **native format**, which is DuckDB's strongest configuration. Compared instead
against **DuckDB-on-Parquet**, DataFusion wins on more machines — including c6a.2xlarge, where
it loses to DuckDB-native. An independent recomputation of the ClickBench leaderboard from raw
result JSONs found DuckDB-Parquet-partitioned ahead only on c6a.4xlarge (7.63 vs 8.46), with
DataFusion ahead on c6a.2xlarge, c6a.metal, c7a.metal-48xl, and c8g.4xlarge. Always state which
DuckDB you mean.

*Second, neither engine is the top Parquet entry overall.* Polars leads on c6a.metal (3.96) and
chDB on c8g.4xlarge (3.53). **Any single-machine "X is fastest" claim — including DataFusion's
own 2024 headline — is cherry-picking a machine.** We should not make one either.

**DuckDB stops scaling past roughly 64 cores** (17.3s → 15.7s across those two 192-core
machines) while DataFusion keeps going (11.4s → 7.4s). The crossover sits somewhere between 16
and 64 cores. Ironically, the Tokio work-stealing model that costs DataFusion cache locality at
16 cores is what lets it keep scaling at 192.

Corroborated independently on h2o db-benchmark (DuckDB Labs, run 2026-04-29, DataFusion 52/53
vs DuckDB 1.5): groupby at 1e9 rows totals **60.3s vs 27.5s** — DataFusion 2.2× slower, with
the damage concentrated in high-cardinality grouping (4.9× on `sum v3 count by id1:id6`). At
1e9-row **joins, DataFusion produces no results at all** while DuckDB completes all five in
55.5s.

**Also note: the November 2024 "DataFusion is the fastest single-node Parquet engine" claim is
no longer true and should not be repeated.** DataFusion's own maintainers acknowledged the flip
in issue #14586; that epic is closed and DataFusion is still behind at 16 cores.

**Why it loses, from DataFusion's own open issues** — these are the things we cannot fix by
choosing better storage:

- **No late materialization.** ClickBench Q23 (`WHERE URL LIKE '%google%' ORDER BY EventTime
  LIMIT 10`) takes 9.2s vs DuckDB's 0.43s — **21×**, and ~140× behind the leaders. `EXPLAIN
  ANALYZE` shows it materializing **137.8 GB across 105 columns to return 10 rows**, with zero
  row groups pruned. Issue #23263 names the fix as "what DuckDB does" and notes it requires row
  tagging plus a self-join that nobody has built. This number has barely moved in three years.
- **Filter pushdown is off by default** (`pushdown_filters`), because enabling it naively
  regresses narrow-projection GROUP BY. PRs #23369/#23420 are open drafts. **This is why
  DataFusion's blog speedups don't appear on the leaderboard — the marketing numbers and the
  benchmark numbers are measuring different configurations.** Do not plan against blog figures.
- **No morsel-driven scheduler.** Issue #21719 (open, 2026-04): execution is "mostly a Volcano
  model" on Tokio, with no thread-locality control. A morsel scheduler was built in PR #2226
  and *removed* for lack of traction. §7.1's morsel-driven design is therefore **our storage
  layer's job, not something DataFusion provides.**
- **Weak out-of-core behavior.** Issue #18473 (open since 2025-11): `datafusion-cli` gets
  OOM-killed on eight ClickBench queries with 8 GB RAM against a 15 GB dataset. Compounds with
  the `HashJoinExec` gap above.

**What this means for ChakraDB — three consequences, and the third is the important one:**

1. **We do not compete on cold-scan analytics.** NFR-04's "within 2× of DuckDB is acceptable"
   is roughly where DataFusion actually sits at typical core counts. That was already the plan;
   it is now a measured expectation rather than a hope.
2. **DataFusion's wins are real but narrow** and mostly favor us: metadata-only queries (17–22×
   on `COUNT(*)` and `MIN/MAX`, answered from Parquet statistics), regex (2.4×), and selective
   narrow-projection filters — which is precisely the shape of the pruning stack our tiered
   storage feeds.
3. **The core-count crossover is a positioning decision we should make deliberately.** An
   embedded engine on a developer laptop is 8–16 cores, where DuckDB wins by ~2×. An embedded
   engine inside a server-side service is 64–192 cores, where DataFusion wins by ~2×. **These
   are different products, and the benchmark says we cannot be excellent at both.** Our wedge
   (§1) is streaming ingest with concurrent analytics — a server-side shape — which argues for
   targeting the high-core regime and saying so plainly. **This belongs in §15 as an open
   question.**

**What actually sits above DuckDB — and it is neither SIMD nor storage.** The top two entries on
ClickBench are **Umbra (×2.63) and CedarDB (×2.82)**, both from Thomas Neumann's TUM lineage,
and both **compiling** query engines. Not vectorized interpreters with better kernels —
engines that generate and compile machine code per query. Neither is embeddable (Umbra is a
closed-source research prototype; CedarDB is a PostgreSQL-wire-compatible server), so neither is
a competitor. But they establish where the real ceiling is, and it is a technique that neither
we nor DataFusion are pursuing.

The honest read: **vectorized interpretation has a ceiling that better kernels do not lift.** If
ChakraDB ever needs to go faster than DataFusion's execution model allows, the answer is
compilation, not intrinsics — and that is a rewrite, not an optimization. Do not plan for it;
just know that's what the next tier costs, so we don't mistake kernel-tuning for a path there.

Bauplan's production migration (DuckDB → DataFusion, ~2× faster p50) is worth reading for the
counter-case: they won because they needed custom optimizer rules, Arrow-native interop, and
Iceberg/S3 I/O that DuckDB's zero-dependency stance blocked. That is DataFusion's real value —
**it is a library for system builders, not a faster database.** Which is exactly why we are
using it. We still win on the storage axis or we do not win.

**The mitigation that must not be skipped:** keep the storage↔execution boundary narrow and
explicit — essentially `scan(snapshot, projection, filters) → Stream<RecordBatch>` plus
statistics. If the execution engine later needs replacing with a push-based morsel-driven
engine, that must be a contained change. Design for the swap; do not perform it in v1.

---

## 9. SQL Surface — a correction to v1.0

v1.0 required "complete PostgreSQL dialect compatibility" via `libpg_query`. This
significantly misjudges where the difficulty lies.

`libpg_query` gives a **parse tree**. Parsing is perhaps 5% of compatibility. The other 95% is
Postgres's type system, implicit coercion rules, collations, NULL semantics, `pg_catalog`
introspection, and thousands of built-in functions with exact edge-case behavior. Mapping a
Postgres AST onto a different engine's logical plan is a large, permanent, ongoing translation
burden — and DataFusion already ships its own parser, so libpg_query adds a heavy C dependency
that fights the static-linking goals in v1.0 §4.3.

**Decision for v1:**

- Use `sqlparser-rs` with the Postgres dialect (what DataFusion already uses).
- Define compatibility as **a documented subset with a conformance suite**, not a claim.
- Publish precisely which constructs are supported. An honest, tested subset beats an
  advertised superset that fails in surprising ways.
- Revisit `libpg_query` only if a concrete user need for wire-level Postgres compatibility
  emerges — and note that would also require the Postgres wire protocol, which is a separate
  project.

**Verified state of `sqlparser` (0.62.0, released 2026-05-07)**, and three things to plan for:

1. **Every release is a breaking change, by stated policy.** The CHANGELOG says any AST change
   is breaking and therefore bumps `0.(N+1)`, and crates.io confirms **zero patch releases
   across 15+ versions**. Measured AST churn is ~1,000–5,000 changed lines per release. The
   breakage is additive (new enum variants, new struct fields) and mechanical to fix, but
   **pin exactly and budget upgrade work every release.** Note this compounds with the
   DataFusion fork tax in §8 — the two rebase treadmills are separate.
2. **Use `PostgreSqlDialect`, never `GenericDialect`.** `GenericDialect` is a permissive *union*
   of every dialect's syntax — it accepts SQL no real database accepts, and it does not
   implement Postgres's operator-precedence table, so it can parse valid SQL into a *different
   tree*. It is also not a strict superset: `PostgreSqlDialect` is stricter about
   left-associative joins without parens. Using Generic would silently corrupt our semantics.
3. **The parser is syntax-only, by design.** No name resolution, no type checking, no catalog —
   `SELECT nonexistent FROM nowhere`, `SELECT 'abc' + 1`, and `INSERT INTO t (a) VALUES (1,2,3)`
   all parse cleanly. Binding and validation are entirely our responsibility (or DataFusion's
   `SqlToRel`). This is correct for our purposes but must not be mistaken for validation.

Known Postgres gaps confirmed against 0.62.0 (`VACUUM ANALYZE t`, `DO $$ ... $$` blocks,
`REFRESH MATERIALIZED VIEW`, `TABLE t`, expression indexes in `ON CONFLICT`) are all outside our
v1 surface, so none block us. Error messages are terse and single-error — acceptable for an
embedded engine, but if we ever want good developer-facing diagnostics we will be adding that
layer ourselves; it has been an open request upstream since 2020.

---

## 10. Requirements (measurable, and tied to the wedge)

v1.0's NFRs were partly unmeasurable ("0 ms"), partly contradictory (1M rows/sec + immediate
ack + ACID), and partly trivial ("compiles without warnings"). Restated:

### 10.1 Functional

| ID | Requirement | Priority |
| :--- | :--- | :--- |
| FR-01 | Tables support INSERT with declared schema; append-only tables need no primary key | P0 |
| FR-02 | Tables with a declared PK support point UPDATE and DELETE by key | P0 |
| FR-03 | Scans observe snapshot-isolated state; readers never block writers or each other | P0 |
| FR-04 | Transactions commit atomically with configurable durability (§7.2) | P0 |
| FR-05 | Published on-disk state is a valid open-format table readable by an external engine | P0 |
| FR-06 | Crash recovery restores the last committed state; recovery time is bounded by WAL tail size, **not** by database size | P0 |
| FR-07 | Compaction maintains scan performance under sustained write load without manual intervention | P0 |
| FR-08 | SELECT supports projection, filter, aggregation, sort, limit, and inner/outer joins | P0 |
| FR-09 | Results are exposed as Arrow RecordBatches over the native Rust API | P0 |
| FR-10 | Compaction debt and ingest backpressure are observable via a metrics interface | P1 |

### 10.2 Non-functional — with measurement methodology

Every number below is a **placeholder to be replaced by a measured baseline before it becomes
a target.** Setting performance targets before establishing a baseline is how v1.0 arrived at
numbers that could not coexist.

| ID | Metric | How measured | Target |
| :--- | :--- | :--- | :--- |
| NFR-01 | Sustained ingest, `group` durability | Continuous INSERT, 60 min, measure steady-state rows/sec **after** compaction reaches equilibrium | TBD from baseline |
| NFR-02 | Write p99 latency under concurrent scan load | Same workload, report full latency distribution, not mean | TBD |
| NFR-03 | **Scan throughput degradation under concurrent write load** | Run scan benchmark on idle DB, then repeat under sustained ingest; report the ratio | **The headline metric. This is the one where DuckDB should lose.** |
| NFR-04 | Cold scan throughput, no concurrent writes | ClickBench and TPC-H at SF10/SF100 | Within 2× of DuckDB is acceptable; we are not competing here |
| NFR-05 | Recovery time after `kill -9` | Crash injection at random points; measure time to first successful query | Bounded by WAL tail, target < 5s |
| NFR-06 | Idle RSS | Open a DB, run nothing, measure | TBD — v1.0's 50 MB was incompatible with its own component list |
| NFR-07 | PK index memory per row | Direct measurement at 1M/10M/100M rows | Must be documented; determines max table size |

**NFR-03 is the requirement that justifies the project's existence.** If ChakraDB cannot
demonstrate a decisive win there, the wedge is invalid and the design should be reconsidered
rather than defended.

### 10.3 There is no industry-standard benchmark for NFR-03 — and that is the point

ClickBench added a `concurrent_qps` field from a supplementary concurrent-query test. **Every
embedded engine's value is `null`** — DuckDB, chDB, Umbra, CedarDB, and every DataFusion entry.
This is not missing data; it is a structural exemption. ClickBench's README explains why:

> *"Single-process engines must set `BENCH_CONCURRENT_DURATION=0`… each query forks a fresh
> full-machine process with no shared scheduler, so concurrent connections only oversubscribe
> RAM (and can OOM the run) instead of measuring throughput."*

Two consequences, pulling in opposite directions:

1. **This is the strongest available evidence that the wedge is real.** The industry-standard
   analytical benchmark cannot measure concurrency on embedded engines *because they cannot
   meaningfully take the test*. "High-concurrency embedded" is an unsolved category, not merely
   an unoptimized one. That is a better argument for the project than anything in §1.
2. **We therefore have to build the benchmark ourselves, and we will be marking our own
   homework.** No neutral leaderboard will validate NFR-03 for us. The mitigation is
   methodological discipline, decided *before* we have results: publish the harness, publish
   the raw numbers, report full latency distributions rather than means, and — most
   importantly — **publish the configuration**, since DataFusion's own credibility gap here
   comes from blog numbers that used non-default settings the leaderboard didn't (§8.1).

Note also that the closest existing system to our goal, **ArcticDB**, reaches concurrent
writers-with-readers by **dropping transactions entirely** and partitioning concurrency by
symbol — their docs argue "why pay the cost of transactions when they are often not needed?"
That is a coherent answer to the same problem and a real alternative to §5.3's MVCC. If our
MVCC design proves too costly in M0, ArcticDB's trade is the fallback worth evaluating before
abandoning the project.

---

## 11. Correctness Strategy

For a database, correctness testing *is* the project. Budget more for this than for the engine.
v1.0's validation section tested that the code compiled and that queries parsed — neither of
which says anything about whether answers are right.

| Layer | Tool / method | Purpose |
| :--- | :--- | :--- |
| SQL correctness | `sqllogictest-rs`, using the Postgres/DuckDB-derived corpora | Answers match a reference implementation |
| Query fuzzing | SQLancer (TLP / PQS / NoREC oracles) | Finds wrong-answer bugs no hand-written test would |
| Storage engine | Seeded, reproducible testing of MVCC + compaction interleavings — but see the DST warning below, this is not free | The only practical way to find version-visibility and compaction-race bugs |
| Concurrency | **`shuttle`** (awslabs) — randomized PCT scheduling with deterministic replay | Actively maintained (0.9.1, 2026-04); scales to whole subsystems, unlike `loom` |
| Crash consistency | Injected power-loss at randomized points, verify recovery invariants | FR-06. Non-negotiable for a database |
| Concurrency | Long-running mixed read/write soak tests with invariant checking | Catches races DST may miss |
| Interop | External engine (Spark/DuckDB) reads our published tables and results must match ours | Validates C-2 — an untested interop claim is a false claim |
| Memory safety | Miri on unsafe code paths; ASan/TSan in CI | Any `unsafe` in the storage layer is a liability |
| Performance regression | Benchmarks in CI with tracked history | Prevents silent decay |

**A rule worth adopting:** no feature is "done" until it has sqllogictest coverage and, if it
touches storage, DST coverage.

### 11.1 Deterministic Simulation Testing — what it actually costs

DST is the highest-value testing technique available for this class of system, and it is also
the one most often adopted naively. The research here changes the recommendation.

**The pessimistic precedent.** FoundationDB's own SIGMOD 2021 paper is explicit that DST works
because they **co-designed a language (Flow) with the simulator** and, in their words, *"have
largely avoided taking dependencies on external systems"* — because simulation *"is unable to
test third-party libraries or dependencies, or even first-party code not implemented in Flow."*
In Rust, the equivalent is `madsim`, and its adoption cost is steep: single-threaded execution,
no `rayon`, no thread pools, no `std::collections::HashMap` iteration (`RandomState` is
nondeterministic — still an open madsim issue), no ambient clock, no direct entropy, and a
patched dependency graph. **No RocksDB or any unshimmed C library**, because a C library does
its own `pthread_create` and `clock_gettime` that the simulator cannot intercept. RisingWave —
the largest madsim user — maintains its own madsim fork, and upstream has been quiet since
February 2026.

**The optimistic precedent, and the one to follow.** Turso (the SQLite-in-Rust rewrite,
23k stars, v0.7.0 shipped 2026-07-13, beta warning dropped) built full DST **without** language
co-design. Their method is the one we should copy: a `trait IO` and `trait File` in the core,
with real backends (`unix`, `io_uring`, `windows`, `memory`) and a `SimulatorIO` that wraps any
of them, plus a `SimulatorClock` where virtual time advances by a seeded tick and a seeded
`ChaCha8Rng` threaded throughout. They inject `pread`/`pwrite`/`lock` faults and probabilistic
latency, shrink failing cases, replay by seed, and check MVCC snapshot isolation by emitting
**Jepsen Elle histories** to an external consistency checker. They run Antithesis on top to
catch bugs their own simulator misses — including, in their telling, bugs *in* the simulator.

**Three consequences for ChakraDB, in priority order:**

1. **Define `trait Io`, `trait Clock`, and a seeded RNG seam in M0.** This costs almost nothing
   at the start and is close to impossible to retrofit once compaction threads, a buffer pool,
   and a table-format layer all call ambient APIs directly. This is the single highest-leverage
   decision in the testing strategy, and it must be made before the first storage code is
   written — which is why it belongs in M0, not M1.
2. **This conflicts with the `croaring` recommendation in §7.4(3).** A C bitmap library is
   exactly the kind of dependency FDB warns about. Resolve it deliberately: either accept
   pure-Rust `roaring` (losing SIMD) for determinism, or keep `croaring` and accept that
   deletion-vector internals sit outside the simulator's reach. **Recommendation: start with
   pure-Rust `roaring`, measure, and only take the C dependency if DV operations prove to be a
   real bottleneck.** Do not pay the determinism cost speculatively.
3. **Do not attempt full FDB-grade DST across the whole engine.** Simulate the storage engine
   core — WAL, MVCC visibility, compaction, PK index — where the bugs are unfindable by other
   means. Use `shuttle` for concurrency testing of components that legitimately use threads,
   and conventional testing above the storage layer. Turso's own honest note on full-fidelity
   DST is worth keeping in view: *"it just takes way too much time."*

### 11.2 Crash-consistency testing — a concrete, ordered plan

The tooling landscape here is mostly dead, so the ordering matters more than the tool choice.

**Step 1 (do this before adopting any tool): write the `trait Io` abstraction and a redb-style
crash backend.** redb's [`tests/crash_consistency.rs`](https://github.com/cberner/redb) models
`live` / `durable` / `last_header` state with a freeze-at-Nth-sync crash point in **~100 lines
with no dependencies** — and it caught a real data-loss bug (header persisted but the file
extension didn't, leaving the DB permanently `Corrupted`). This is the template. It is portable,
dependency-free, and it is the same `trait Io` seam §11.1 already requires. **Every tool below
is worthless until we can answer "what does correct-after-crash mean for ChakraDB," and that
verifier is the expensive part we need regardless of tooling.**

**Step 2, cheap additions:**
- **`elle-cli`** to check our claimed snapshot isolation (§5.3) against an external consistency
  checker. It accepts **JSON**, and its reader keywordizes the fields we care about, so **plain
  `serde_json` output from Rust just works — no Clojure required.** Use `--model list-append`
  (most inference power). Build from HEAD: the prebuilt jar ships Elle 0.2.4 while HEAD pins
  0.2.7, and `project.clj` bakes in `-Xmx32g` that CI must override.
- **`fail` 0.5.1** for "crash at this exact line" sequencing. Note it is stale (no release since
  2022-10; two years of commits are "make clippy happy") but TiKV depends on it and it does one
  narrow job. It cannot produce a torn write or an `EIO` from fsync — it only runs an action
  where you placed it.
- **`turmoil`'s `unstable-fs`** for torn writes and probabilistic sync, per the appendix.

**Step 3, Linux-only nightly gate** (cannot be our durability story since it doesn't port, but
it is the only tier that records what the kernel *actually did* with our barriers):
- **`dm-flakey` with `drop_writes` first** — cheapest, catches the lost-write case that actually
  corrupts databases, and is io_uring-agnostic. Also `error_writes`, `corrupt_bio_byte`,
  `random_write_corrupt`; `dm-dust` adds deterministic bad sectors at known LBAs.
- **`dm-log-writes` + fstests second**, if we need true barrier fidelity. In mainline since 4.1
  and maintained. fstests' `common/dmlogwrites` is a ready-made harness worth lifting wholesale.
  Sobering caveat: **no example was found of anyone using it to test a database** — we would be
  off the beaten path.
- Note for io_uring, should we ever adopt it: LD_PRELOAD and `strace` are **structurally blind**
  to it, since submissions go through an mmap'd ring. The block layer is the answer —
  `fail_make_request` hooks `submit_bio_noacct()`, where every I/O arrives as a bio regardless
  of origin. This is a further argument for §7.3's thread-pool default.

**Skip entirely — all verified dead or wrong-layer:** ALICE (Python 2, vendors a frozen 2014
strace fork, last functional commit 2015, and explicitly does not support mmap), CrashMonkey/ACE
(filesystem-targeted kernel modules, kernel support stops at 5.6), charybdefs (archived by
ScyllaDB 2026-01-05), and the Jepsen *framework* — its value is partitions, clock skew and
split-brain, essentially none of which applies to a single-node embedded engine. Elle alone is
the transferable part. **Antithesis is technically ideal but sales-led with no free tier; their
`Hegel` libraries (2026-03, Rust-first, run on your own infra) are the $0 on-ramp.**

**One design consequence worth acting on now:** §7.3 already rejects mmap on the durable path.
This section independently reinforces it — mmap'd writes are invisible to nearly every tool
here. ALICE explicitly doesn't support them, LazyFS buffers at the write syscall so mmap stores
bypass it, and LD_PRELOAD/strace see page faults rather than stores. **Choosing mmap would mean
choosing untestable durability.**

---

## 12. Roadmap

Each milestone ends with a **decision point** where the project can be redirected or abandoned
cheaply. Deliberately, the riskiest question is answered first.

### M0 — Kill the biggest risk (target: weeks, not months)
Build a throwaway prototype that answers only: *can we sustain high-rate keyed updates while
scanning, with an acceptable PK index memory footprint?*
- In-memory only. No SQL. No persistence. No table format.
- Hardcoded schema, PK index, deletion vectors, tiered parts, MVCC visibility.
- Measure NFR-03 and NFR-07 directly.

> **Decision point:** if the PK index memory or the scan-under-write degradation is bad here,
> it will not improve with more layers on top. **This is the moment to change direction.**

### M1 — Durable single-table engine
WAL + group commit, crash recovery, persistent PK index, compaction with backpressure.
DST and crash-injection harness stood up *in this milestone*, not later.

> **Decision point:** recovery correct under injected crashes? Compaction stable under
> sustained load?

### M2 — Query layer
DataFusion integration via a custom `TableProvider`. SQL surface per §9. sqllogictest and
SQLancer in CI. Multi-table, joins, transactions across tables.

> **Decision point:** measure against DuckDB on NFR-03 and NFR-04. **This is the real
> go/no-go.** A decisive NFR-03 win validates the project; parity means reconsider.

### M3 — Lakehouse publication
Table format abstraction, Delta implementation, two-level commit, publication scheduling,
external-engine interop tests. Multi-process readers.

> **Decision point:** can Spark/DuckDB read our tables correctly, including deletion vectors?

### M4 — Hardening
Soak testing, memory-pressure behavior, operational metrics, documentation, error taxonomy,
file format versioning and compatibility policy.

### M5 — Reach (only after M4 is solid)
Python bindings (PyO3 + Arrow PyCapsule, zero-copy to Polars). Then Windows. Then Iceberg.
Then, if ever, Java.

**On v1.0's ordering:** it front-loaded FFI, multi-language bindings, and Postgres parsing —
all of which are reach items that depend on a core that does not exist yet. Building the
bindings before the engine produces an impressive demo that cannot become a product.

---

## 13. Principal Risks

| Risk | Severity | Mitigation |
| :--- | :--- | :--- |
| PK index memory limits practical table size | **High** | M0 measures it first; spill design; PK-less append-only tables as the cheap path |
| Compaction cannot keep up; scans degrade | **High** | Explicit backpressure and observability; compaction is a designed subsystem |
| DataFusion's ceiling makes us uncompetitive on NFR-04 | Medium | Accept it — we compete on NFR-03; keep the boundary swappable |
| Two-level commit staleness is unacceptable to users | Medium | Tunable interval; document honestly; validate with real users early |
| DuckLake/chDB/StarRocks already solve this | **High** | Do the competitive research in §1.3 **before M0** |
| MVCC + compaction interleaving bugs | **High** | DST from M1; this class of bug is not findable by conventional testing |
| Long-running queries pin the GC watermark | Medium | Max snapshot age with query cancellation; document the tradeoff |
| Scope creep back toward v1.0's ambitions | Medium | §2.1 non-goals are binding; revisit only at decision points |
| Single-maintainer dependency risk in the Rust storage ecosystem | Medium | Verified for `sled` — **do not use it**; `redb` and `fjall` are the credible options, but we likely write our own storage layer anyway |

---

## 14. Summary of Changes from v1.0

| Area | v1.0 | v2.0 | Why |
| :--- | :--- | :--- | :--- |
| Positioning | "Superior to DuckDB" | Fast scans over *arriving* data, in an open format | v1.0's plan conceded the execution axis while claiming to win on it |
| Storage | LSM beside columnar, merge-on-read | Three-tier columnar, one scan interface | Removes a permanent row/column merge tax |
| PK index | Absent | §5.2, the central component | Point updates are impossible without it |
| MVCC | "Snapshot isolation" asserted | Designed: CSN, per-part versioning, DVs, GC watermark | The mechanism *is* the design |
| Durability | "O(1), immediate ack" + ACID | Three explicit modes; group commit default | The original claims were mutually incompatible |
| mmap | Core of the design | Rejected for the durable path | CIDR 2022; well-documented trap |
| Table format | Iceberg, unexamined | Abstracted; Delta first; two-level commit | Per-transaction lakehouse commits cannot work at rate |
| SQL | "Complete PG compatibility" via libpg_query | Documented subset via sqlparser-rs | Parsing is 5% of compatibility |
| Execution | DataFusion, unexamined | DataFusion, explicitly as a bought component with a stated ceiling | Honesty about the tradeoff |
| Bindings | Python + Java, P0 | Deferred to M5 | They depend on an engine that does not exist yet |
| NFRs | "0 ms", 1M rows/sec, 50 MB | Measured baselines; NFR-03 as the headline | Unmeasurable targets cannot be engineered toward |
| Testing | Compiles + parses | sqllogictest, SQLancer, DST, crash injection, interop | Correctness testing is the project |
| Roadmap | Absent | M0–M5 with decision points | Risk-first ordering |

---

## 15. Open Questions Requiring Decisions

1. **Require a primary key on all tables, or allow PK-less append-only tables?**
   (§5.2 — recommendation: allow both, opt into mutability.)
2. **Delta or Iceberg first?** (§6.2 — recommendation: Delta, pending maturity re-verification.)
3. **Default publication interval**, and is seconds-level external staleness acceptable to the
   intended users? (§6.1 — needs real user input, not a guess.)
4. **Maximum practical table size** we commit to supporting, which follows directly from the PK
   index memory decision. (§5.2)
5. **Is the two-level commit model acceptable**, or does some user require the on-disk state to
   be continuously valid? If the latter, the architecture changes substantially.
6. **What is the actual target workload?** This document assumes streaming ingest plus
   analytics. A concrete first user would sharpen every decision above.
7. **`croaring` (SIMD, C dependency) vs pure-Rust `roaring` (no SIMD, simulatable)?**
   §7.4(3) and §11.1(2) pull in opposite directions. Recommendation: start pure-Rust.

---

## Appendix A: Dependency Verification Log

Verified against primary sources (crates.io API, GitHub API, repo source, specs) as of
**2026-07-19**. Re-verify before committing to any of these.

| Dependency | State | Verdict |
| :--- | :--- | :--- |
| **DataFusion** | 54.0.0 (2026-06-08); ~2.5-month major cadence; mature spilling (memory pool, DiskManager, SpillManager, spill compression, pluggable `SpillFile` since 2026-06-29) | **Adopt — and fork.** GreptimeDB, Spice.ai, ParadeDB, InfluxData all carry pinned forks |
| **arrow-rs** | 59.1.0 (2026-07-07); `simd` feature deleted in 50.0.0; pure autovectorization | Adopt; inherit the no-intrinsics doctrine (§7.4) |
| **sqlparser** | 0.62.0 (2026-05-07); every release breaking; ASF/DataFusion PMC governed | Adopt with `PostgreSqlDialect`; pin exactly |
| **delta-kernel-rs** | DV support implemented (portable 64-bit Roaring, magic `1681511377`) | **Adopt for §6.2** |
| **iceberg-rust** | ⚠️ Unverified; cited as "nascent" by ParadeDB in 2024 | Defer to M5; verify first |
| **sled** | **Dead.** No stable release since 0.34.7 (2021); 1.0-alpha since 2024-10; unanswered soundness bug (#1536), corruption report (#1533), build break on modern rustc | **Do not use** |
| **redb** | Actively maintained, stable file format with upgrade path | Viable if we need an embedded KV |
| **fjall** | 3.0 shipped with a longevity-oriented format; author states dev winds down into 2026 | Viable; note maintenance signal |
| **RocksDB** | Healthy — 11.6.0 (2026-07-02), release cadence *doubled* in 2026, Meta's internal pipeline fully active. But GitHub issues effectively untriaged (895 open, 33 opened / 5 closed in 90 days) and Java bindings neglected | Viable for C++ core; do not expect issue support |
| **Speedb** | **Dead as open source — verified.** Redis acquired it 2024-03-21; the last commit to `main` landed 2024-03-11, ten days *before* the announcement. Last release v2.8.0 (2024-01-31). Repo not archived but 146 open issues with zero maintainer replies; speedb.io is offline (TLS handshake fails) while the README still recruits contributors. The technology shipped as **Redis proprietary code** — Redis docs call Speedb "Redis proprietary storage engine," the default for Redis Flex. Rust bindings stuck at 0.0.5 (2024-02-24) against Speedb 2.7.0, never even reaching the final 2.8.0 | **Do not use.** v1.0 named this a P0 dependency; that alone would have sunk the project |
| **croaring** | 2.7.0; genuine runtime CPUID dispatch (AVX2 + AVX-512 VBMI2/BITALG/VPOPCNTDQ), enabled by default | Adopt only if DV perf demands it (§11.1) |
| **roaring** (pure Rust) | 0.11.4; `simd` feature **requires nightly**, covers only array containers | **Start here** for DST compatibility |
| **fastlanes** | 0.5.2 (2026-06-12); autovectorized bit-packing; **not** binary-compatible with reference FastLanes | Adopt if we need these kernels — we own the hot-tier format |
| **tokio** | 1.53.0 (2026-07-17); io_uring exists but **unstable, file-I/O only**, `--cfg tokio_unstable`; open use-after-free (#8255) | Use tokio; do not enable io_uring yet |
| **compio** | 0.19.1; healthiest thread-per-core runtime; IOCP/io_uring/polling | Only if we go thread-per-core |
| **tokio-uring** | **Abandoned** — no maintainer, last release 2024-05 | Do not use |
| **glommio** | **Dead** — author publicly endorsed a hard fork (2026-03); fork has 24 stars, no crates.io release | Do not use |
| **shuttle** (awslabs) | 0.9.1 (2026-04-21); active, architectural refactor landed 2026-05 | **Adopt** for concurrency testing |
| **loom** | Maintenance-only; 0.7.2 from 2024-04; 121 open issues, no release in 2 years | Small unsafe primitives only |
| **stateright** | **Stale** — last commit 2025-07-27 | Do not use |
| **madsim** | Thin — no commits since 2026-02; RisingWave runs its own fork | See §11.1; prefer the Turso pattern |
| **turmoil** (tokio-rs) | **Partially reversed — see below.** Its *network* simulation is irrelevant to us (C-1, single process), and its scheduler seeding is silently compiled out without `--cfg tokio_unstable`. **But `unstable-fs` (added 0.7.1, 2026-01) does exactly what we need**: `sync_probability` (writes randomly durable without fsync), `io_error_probability`, `corruption_probability`, `short_read_probability`, `block_size: Option<u64>` for **torn-write simulation**, and `Sim::crash()` which discards pending writes and deletes never-synced files | **Evaluate the FS half.** Caveats: `unstable-` is honest, you must use turmoil's `File`, it's tokio-coupled, and `main` has already split this into an unpublished `turmoil-fs` — expect churn |
| **madsim** (durability) | Separate from the §11.1 note on its concurrency story: **madsim does not simulate disk faults at all.** `madsim/src/sim/fs.rs` contains `pub fn power_fail(&self, _id: NodeId) { // TODO }` — and `reset_node` calls it, so a simulated restart currently loses nothing. It has an `fs` module, so a shallow check falsely says yes | **Do not use for durability.** Network, time, randomness only |
| **sqllogictest-rs** | 0.29.1 (2026-02-14); coasting but stable; DataFusion depends on it | **Adopt**; budget for vendoring a patch |
| **`std::simd`** | Nightly-only; tracking issue idle 16+ months with open *design* questions | **Do not plan around it** |

**Note on the research behind this table:** the session's web-search budget was exhausted
partway through, so several items were verified by direct source/API inspection rather than
search — which is generally stronger evidence. Items marked ⚠️ are genuinely unverified, not
negative findings. The most important unverified items are **DuckLake's maturity** (it bears
directly on the §1 wedge), **`iceberg-rust`**, and **current ClickBench/TPC-H standings**.
