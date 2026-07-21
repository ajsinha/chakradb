# Arrow-Native Storage & Dynamic Schema

**Status:** Done and green. 449 tests across 23 targets, 0 failures; `cargo build
--bins` and `--features datafusion` both succeed.
**What changed:** ChakraDB is no longer a fixed `(pk i64, a i64, b f64, c String)`
engine. Storage is Apache Arrow end to end, over an arbitrary [`Schema`] keyed by
one key column of **any type** — or a hidden `_rowid` for keyless tables. This is
the "be more like DuckDB" request, executed on the Arrow-native path chosen after
the M3 spike proved DataFusion out.

---

## 1. The decision

The dynamic-schema plan (`dynamic-schema-design.md`) originally chose a row-wise
`Vec<Value>` representation to keep the core dependency-free, accepting a
columnar→row-wise regression that DataFusion would later undo. The M3 spike
changed that calculus: Arrow/DataFusion is now the proven performance path, and
`requirements.md` always wanted open formats (Parquet/Arrow/Iceberg). So storage
went **Arrow-native** instead:

> Sealed parts hold Arrow arrays. Columnar throughout, zero-copy to DataFusion,
> and parts persist as the open Arrow IPC format.

The cost — `arrow` (~25 crates) is now a core dependency — was accepted
deliberately, ending the zero-dependency purity of M0/M1.

## 2. The one idea that kept it tractable

**Every table has exactly one key column.** It is either a user column declared
`PRIMARY KEY` (of any type) or a synthesised `_rowid`. The storage engine never
learns which; it sorts, seeks, and blooms on a key column of `Value`s. "PK-less"
is not a second code path — it is a table whose key is a hidden rowid. This is
what let the Doris-style "ordinal *is* row offset" index survive the move to
arbitrary types unchanged.

## 3. What was rebuilt

| Layer | Change |
|---|---|
| `value.rs` | `Value`/`DataType` is now the shared scalar (SQL + storage merged, §7); exact `Int/Int` order; `Key` wrapper for ordered map/set keys. |
| `batch.rs` (new) | `Batch` wraps an Arrow `RecordBatch` under a `Schema`; `from_rows`/`value`/`key`/`take`/`concat`/`to_ipc`/`from_ipc`. |
| `schema.rs` | `Schema` (columns + key index + synthetic-rowid flag), `Row = Vec<Value>`. |
| `bloom.rs` | `build_values`/`maybe_contains_value` over any `Value` (Int path bit-identical). |
| `part.rs` | `Value` key bounds; binary-search + equal-key-run via `total_cmp`; `take`-based partial scan. |
| `l0.rs` | Keyed by `Value` (`BTreeMap`); seals into Arrow via the schema. |
| `compaction.rs` | Merge sorts on the `Value` key; builds via `from_rows`. |
| `table.rs` / `database.rs` | Schema-carrying; key-generic ops; synthetic rowid assignment; `create_table_schema`. |
| `codec.rs` | Tagged `value`/`row`/`schema` encode+decode (self-describing). |
| `persist.rs` | `PART_VERSION 3` — part image is version stamps + embedded schema + **Arrow IPC** columns. |
| `pager.rs` | Summary bounds are `Value`; `definitely_excludes`/`lookup` take `&Value`. |
| `wal.rs` | `Delete` carries a `Value` key; `Insert` row is dynamic for free. |
| `sql/` | `plan_in(sql, db)` resolves columns against each table's live schema; `CREATE TABLE` builds a real `Schema`; `SELECT *` hides the rowid. |

MVCC (`csn.rs`, `delete_vector.rs`) was **not touched** — it is ordinal-based, so
it carried over unchanged. That was the load-bearing bet, and it held.

## 4. What users can now do (that the old engine rejected)

```sql
CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price FLOAT, qty INT);
CREATE TABLE users (email TEXT PRIMARY KEY, age INT);   -- text primary key
CREATE TABLE log   (msg TEXT, level INT);               -- keyless -> hidden _rowid
```

Covered by `tests/dynamic_schema.rs`: arbitrary columns/types, text PK (duplicate
rejected, keys order correctly), PK-less rowid table (`SELECT *` hides the rowid),
arity/type errors, and `GROUP BY` on a user-named column.

## 5. DataFusion, now zero-copy

Because a segment already *is* an Arrow `RecordBatch`, the bridge hands
DataFusion the parts' own columns by `Arc` clone — no rebuild, and it works for
any schema. Measured on the 500k-row `hits` set (median of 25, pinned):

| query | DuckDB | ChakraDB interpreter | ChakraDB + DataFusion |
|---|---|---|---|
| COUNT(*) | ~0 ms | 0.01 ms | 0.21 ms |
| SUM(a) WHERE a>500 | 1 ms | 16 ms | 0.81 ms |
| GROUP BY a | 1 ms | 50 ms | 2.0 ms |
| ORDER BY b LIMIT 100 | 2 ms | 22 ms | 1.36 ms |
| COUNT(DISTINCT a) | 5 ms | 60 ms | 1.90 ms |

The concurrency wedge still holds across the handoff (DataFusion sees exactly the
snapshot while writers commit), and joins/windows/subqueries run under DataFusion
that the interpreter rejects.

## 6. Honest residuals

- **Streaming `TableProvider`.** The bridge uses a `MemTable`, but it wraps
  Arc-shared Arrow buffers, so no column data is copied. A custom streaming
  provider would add filter/projection *pushdown into the scan*; deferred as a
  refinement, not a correctness gap.
- **Recovery schema.** Part files embed their schema, so a part rebuilds
  correctly. The table-level schema on recovery still comes from `create_table`
  (default) rather than the manifest — fine for default-schema data, but the
  manifest should persist each table's `Schema` before arbitrary-schema durability
  is claimed. This is the one carried-forward task.
- **Write-path cost.** Row-at-a-time `INSERT` now builds through `Vec<Value>` and
  seals to Arrow; bulk load is slower than the old struct-of-vectors path. The
  bottleneck is the SQL/insert front end, not the columnar store.
