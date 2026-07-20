# Dynamic Schema — Design & Migration Plan

**Goal:** make ChakraDB's schema arbitrary, like DuckDB — any number of columns,
any types, per-table — instead of the fixed `(pk i64, a i64, b f64, c String)`
of M0–M2.

**Status:** Foundation landed (`src/value.rs`, tested). The data-plane migration
is designed here but not yet executed — it is a large, mechanical change across
the whole storage stack, and this document is the plan to carry it out safely.

**Requested shape:** any-type primary key **and** PK-less tables (the fullest,
most DuckDB-like option).

---

## 1. The one idea that makes it tractable

ChakraDB's index (`requirements.md` §5.2) needs a **sortable key** — that is the
Doris-style "ordinal is row offset" trick the whole engine depends on. DuckDB
needs no key at all. To bridge that without two engines:

> **Every table has exactly one key column.** It is either a user column
> declared `PRIMARY KEY` (of any type), or a hidden auto-incrementing `_rowid`
> integer column synthesised when no PK is declared.

So "PK-less" is not a second code path — it is a table whose key is a hidden
rowid. The storage engine never learns which; it just sorts, searches, and blooms
on *a key column of `Value`s*. This is the single decision that keeps the change
mechanical instead of architectural.

---

## 2. Types (done)

`src/value.rs` is committed and tested (20 tests):

- `DataType { Int, Float, Text, Bool }` — with `parse()` accepting SQL aliases
  (`bigint`, `varchar`, …) and `type_char()` for sqllogictest.
- `Value { Null, Int, Float, Text, Bool }` — the scalar *and* the key type, with:
  - `total_cmp` — a deterministic total order, so **text and float keys sort**
    (this is what "any-type PK" needs); NULLs first.
  - `sql_cmp` — three-valued comparison (NULL → unknown).
  - `fits` / `coerce` — type checking against a `DataType`.

This is deliberately landed ahead of the migration: it is self-contained, it is
already what `src/sql/value.rs` should be (they will merge — see §7), and it lets
the rest of the plan reference concrete types.

---

## 3. The new schema and row (designed)

```rust
// schema.rs (rewrite)
struct ColumnDef { name: String, ty: DataType }
struct Schema { columns: Vec<ColumnDef>, key_index: usize, synthetic_key: bool }
struct Row { values: Vec<Value> }          // all columns, incl. hidden rowid
struct Batch { rows: Vec<Row> }            // row-wise; see §6 on why
```

- `Schema::default_schema()` reproduces `(pk, a, b, c)` so the ~450 existing tests
  keep passing against the new engine.
- `Row::new(pk, a, b, c)` stays as a convenience building the default-schema row;
  `Row::from_values(vec![...])` is the general constructor.
- Legacy accessors `row.pk()`, `.a()`, `.b()`, `.c()` become *methods* (they are
  fields today) — this is the one change that touches many call sites, and it is a
  mechanical `.pk` → `.pk()` sweep.

A prototyped version of this file (with 14 passing tests) was written during
scoping and is preserved in the branch history; it validated the shape above.

---

## 4. The engine becomes key-generic

Every place that today hard-codes an `i64` pk becomes a `Value` key:

| File | Change |
|---|---|
| `bloom.rs` | Add `hash_value(&Value)` and `build_values` / `maybe_contains_value`. Keep the `i64` path routing through `hash_value(Int)`. *(Prototyped and tested.)* |
| `part.rs` | Store `keys: Vec<Value>` + `key_index`; `min_key/max_key: Value`. Bounds via `total_cmp`, seek via `binary_search_by(total_cmp)`, `equal_key_run` over keys, bloom via `maybe_contains_value`. `lookup(key: &Value, …)`. |
| `l0.rs` | `L0Entry` keyed by `Value`; the `newest` index becomes `HashMap<Value, usize>` — needs `Value: Hash + Eq`, so add a manual `Hash` (float-bits, string bytes). |
| `compaction.rs` | Sort merged rows by `Value` key; the `(pk, created, …)` tuple's first element is a `Value`. |
| `pager.rs` | `PartSummary.min/max` become `Value`; `definitely_excludes(key: &Value)`. |
| `table.rs` | Holds a `SchemaRef`; `insert/get/delete(key: Value)`; validates rows via `Schema::check_row`; assigns `_rowid` when `synthetic_key`. |
| `database.rs` | `create_table(name, Schema)`. |

The MVCC layer (`csn.rs`, `delete_vector.rs`) is **untouched** — it works on
ordinals and CSNs, not keys. That is a meaningful fraction of the engine that
needs no change.

---

## 5. Persistence and the WAL

| File | Change |
|---|---|
| `codec.rs` | Tagged `value()` encode/decode and length-prefixed dynamic `row()`. *(Prototyped and tested — value + arity roundtrips.)* |
| `persist.rs` | Part file carries the schema (or a schema id) in its header; columns encoded as tagged values. Summary frame's bounds become `Value`. |
| `manifest.rs` | Each `TableMeta` stores its `Schema` (column names + types + key_index + synthetic flag), so recovery rebuilds tables with the right shape. |
| `wal.rs` | `WalRecord::Insert` already carries a `Row`; with dynamic `Row` it carries dynamic values for free. `Delete` carries a `Value` key instead of `i64`. |

**Format compatibility:** this is a breaking on-disk change. Bump the part/manifest
version bytes; old files are rejected with a clear error. There is no production
data to migrate.

---

## 6. One honest regression to accept

Row-wise `Batch` (a `Vec<Row>`) is simpler and correct, but it gives up the
columnar layout M0-2 measured. Two consequences, both acceptable and both to be
recorded in the findings:

1. **Memory.** Per-row `Vec<Value>` boxing costs more than parallel typed vectors.
   The M0-2 *index* result (1.25 B/row, Bloom-only) is unaffected — that is about
   the key index, not the payload. Payload memory grows; noted, not hidden.
2. **Scan speed.** Already 14–82× behind DuckDB (Gate 2); row-wise values do not
   help. Both are the interpreter/representation ceiling that `requirements.md` §8
   resolves with DataFusion, which brings the columnar `RecordBatch` back. The
   dynamic schema is in fact a *prerequisite* for DataFusion integration, since
   DataFusion needs a real schema to build a `TableProvider`.

So the dynamic schema and the eventual DataFusion adoption are the same arc: get
the schema right first, then let DataFusion supply the vectorised columnar engine
behind the `scan` boundary.

---

## 7. SQL layer

- **Merge `sql/value.rs` into the core `value.rs`** — today there are two `Value`
  types; the SQL one becomes a re-export. (Kept separate only to land the core
  type without touching SQL in the same step.)
- `CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT, bal DOUBLE)` parses
  column definitions into a `Schema`; no `PRIMARY KEY` → `Schema::rowid`.
- `INSERT`/`SELECT`/`UPDATE`/`DELETE` resolve columns by name against the table's
  schema instead of the fixed four. `SELECT *` expands to the schema's user
  columns. The three-valued-logic evaluator is already schema-agnostic.
- The sqllogictest corpus and SQLancer oracles then run over *arbitrary* schemas,
  which also lets us import a real slice of `apache/datafusion-testing` — the
  reason those files could not be used before (§ M2-1) was precisely the fixed
  schema.

---

## 8. Migration order (each step keeps the tree green)

1. **value.rs** — done.
2. **schema.rs** rewrite + `Row::new`/accessor compat; fix the `.pk`→`.pk()` sweep
   in engine + tests. *(Largest single step; ~9 source + ~14 test files.)*
3. **bloom.rs**, **codec.rs** — done in prototype; re-land.
4. **part.rs**, **l0.rs**, **compaction.rs**, **pager.rs** — key-generic engine.
5. **table.rs**, **database.rs** — schema-carrying, rowid assignment.
6. **persist.rs**, **manifest.rs**, **wal.rs**, **storage.rs** — dynamic
   persistence + recovery; version bump.
7. **sql/** — column defs, name resolution, `value.rs` merge.
8. New tests: arbitrary-schema round-trips, text-PK tables, PK-less/rowid tables,
   crash recovery of a dynamic schema, an arbitrary-schema `.slt` corpus.

Steps 3–6 are individually small and were validated in prototype; step 2 is the
one that must be done in a single sweep because `Row`'s shape changes. That is why
it was not rushed at the end of a long session — a half-applied step 2 leaves the
engine uncompilable, and a broken engine is worse than a clear plan.

---

## 9. What is committed now vs planned

| | Status |
|---|---|
| `Value` + `DataType` (core type system, 20 tests) | ✅ committed |
| Dynamic `Schema`/`Row`/`Batch` design | ✅ specified here; prototyped (14 tests) |
| `bloom`/`codec` key-generic changes | ✅ prototyped & tested; ready to re-land |
| Engine migration (part/l0/table/…) | ⬜ designed, not executed |
| Dynamic persistence + recovery | ⬜ designed, not executed |
| SQL `CREATE TABLE` with column defs | ⬜ designed, not executed |

**Recommendation.** Execute steps 2–7 as a dedicated branch of focused work, not
as the tail of an unrelated session — step 2's `Row` reshape is atomic and needs
room to land in one green sweep. The foundation and the plan de-risk it: the
hard design questions (any-type key ordering, PK-less-as-rowid, format
versioning, the columnar-vs-rowwise trade) are answered above.
