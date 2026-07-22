# ChakraDB SQL Reference

The SQL ChakraDB accepts, and exactly where the boundaries are. Parsing is done
with [`sqlparser`](https://crates.io/crates/sqlparser) under the **PostgreSQL
dialect**; identifiers keep their case (no lowercasing), so `AdvEngineID` stays
`AdvEngineID`.

> **Two engines, one surface.** Single-table reads and all writes run on the
> built-in **interpreter**. Analytical shapes the interpreter can't plan — joins,
> subqueries, window functions, and richer scalar functions — are handed to
> **DataFusion** (the default build). Without the `datafusion` feature, or
> **inside a transaction**, only the interpreter surface below is available; an
> unsupported construct returns a clear error rather than a wrong answer.

---

## Statements

| Statement | Notes |
|---|---|
| `CREATE TABLE t (...)` | Columns, one optional single-column `PRIMARY KEY`, constraints. |
| `INSERT INTO t [(cols)] VALUES (...), ...` | Positional or by column list; omitted columns take their `DEFAULT`. |
| `SELECT ...` | Projections, `WHERE`, `GROUP BY`, `HAVING`-less aggregates, `ORDER BY`, `LIMIT`, `DISTINCT`. |
| `UPDATE t SET c = expr [, ...] [WHERE ...]` | Every target row is constraint-checked *before* any is applied (statement-atomic). |
| `DELETE FROM t [WHERE ...]` | |
| `COPY t [(cols)] FROM '<file>' [WITH (...)]` | Bulk CSV import — see [COPY](#copy). |
| `BEGIN` / `COMMIT` / `ROLLBACK` | Transactions — see [Transactions](#transactions). |

Not supported: `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, `TRUNCATE`, views,
`COPY ... TO` (export), `MERGE`, CTEs (`WITH`).

---

## Data types

| Type | Aliases | Storage | Notes |
|---|---|---|---|
| `INT` | `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT` | 64-bit signed | Wrapping arithmetic. |
| `FLOAT` | `DOUBLE`, `REAL` | 64-bit IEEE-754 | |
| `TEXT` | `VARCHAR[(n)]`, `CHAR[(n)]`, `STRING` | UTF-8 | `(n)` bounds the length in **characters** (enforced). |
| `BOOLEAN` | `BOOL` | | |
| `DATE` | | days since 1970-01-01 (Arrow `Date32`) | Literals `'YYYY-MM-DD'` or `DATE '...'`; years ±262143. |
| `TIMESTAMP` | `DATETIME` | microseconds since epoch (Arrow `Timestamp`) | `'YYYY-MM-DD[ T]HH:MM:SS[.ffffff]'` or `TIMESTAMP '...'`. |
| `DECIMAL(p, s)` | `NUMERIC(p, s)`, `DEC` | exact `i128` mantissa (Arrow `Decimal128`) | Exact fixed-point — never `f64`. `p` ≤ 38. Precision enforced on write. |

**`DECIMAL` is exact.** `0.1 + 0.2` stored as `DECIMAL` is exactly `0.3`; values
are parsed from their source text, compared and summed in `i128`, and a value
exceeding the declared precision (e.g. `1000` into `DECIMAL(3,0)`) is rejected.
`SUM`/`MIN`/`MAX` stay exact; `AVG` of a decimal returns a float.

**`NULL`** is a value of any type, distinct under three-valued logic: a
comparison with `NULL` is *unknown* (not true), so it neither passes a `WHERE`
nor fails a `CHECK`.

---

## Constraints

Declared in `CREATE TABLE`, enforced at write time (INSERT at plan time, UPDATE
before applying any row):

```sql
CREATE TABLE accounts (
  id       INT PRIMARY KEY,              -- single column; implicitly NOT NULL
  email    VARCHAR(255) NOT NULL,        -- length + null enforced
  balance  DECIMAL(12,2) NOT NULL DEFAULT 0.00,
  status   TEXT DEFAULT 'active',
  age      INT CHECK (age >= 0),         -- column-level CHECK
  CHECK (balance >= 0)                   -- table-level CHECK
);
```

- **`PRIMARY KEY`** — exactly one, single column, any type (int, text, float,
  bool, date, decimal). Implicitly `NOT NULL`. A table with no `PRIMARY KEY` gets
  a hidden auto-increment `_rowid` key (a keyless table).
- **`NOT NULL`** / **`NULL`** — reject / allow `NULL` in the column.
- **`DEFAULT <literal>`** — a literal value used when `INSERT` omits the column.
  An explicit `NULL` in `VALUES` is kept, not replaced by the default.
- **`CHECK (<predicate>)`** — column- or table-level. Violated only by a definite
  **FALSE**; `NULL`/unknown passes (per SQL).
- **`VARCHAR(n)` / `CHAR(n)`** — the value's character count must be ≤ `n`.

Not supported: composite / multi-column `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`
(an explicit non-goal — referential integrity is the application's), and
generated columns.

---

## Expressions

The interpreter evaluates these (also valid inside DataFusion queries):

- **Columns** and **literals**: integers, floats, strings (`'...'`), booleans,
  `NULL`, `DATE '...'` / `TIMESTAMP '...'`, decimal numerals.
- **Arithmetic**: `+` `-` `*` `/` `%` (integer arithmetic wraps; `/` and `%` by
  zero yield `NULL`; decimal `+`/`-`/`*` are exact, `/` falls back to float).
- **Comparison**: `=` `<>` `<` `<=` `>` `>=` (type-aware — a literal compared to a
  typed column is coerced, so `d >= '2024-01-01'` and `price > 9.99` order
  correctly).
- **Logical**: `AND` `OR` `NOT`.
- **Null tests**: `IS NULL`, `IS NOT NULL`.
- **Parentheses** for grouping.

**Interpreter-only limitation:** `BETWEEN`, `LIKE`, `IN`, `CASE`, and scalar
function calls are **not** handled by the interpreter. In the default build a
`SELECT` using them is routed to DataFusion (which supports them); in the lean
build, or inside a transaction, they error. Rewrite `x BETWEEN a AND b` as
`x >= a AND x <= b` to stay on the interpreter.

### Aggregates

`COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`, with or
without `GROUP BY`. `COUNT(DISTINCT col)` is supported via DataFusion.

- `COUNT(*)` with no filter and bare `MIN(col)`/`MAX(col)` are answered from
  **metadata/zonemaps** without scanning.
- `SUM` of integers stays an integer; `SUM` of decimals stays an exact decimal.

---

## SELECT and the query router

```sql
SELECT [DISTINCT] <proj> [, ...] FROM t [WHERE <pred>]
  [GROUP BY <col> [, ...]] [ORDER BY <expr> [ASC|DESC] [, ...]] [LIMIT <n>]
```

Each query is routed to the engine that fits its shape:

| Shape | Engine | Why |
|---|---|---|
| `COUNT(*)` no filter | interpreter | metadata, no scan |
| bare `MIN`/`MAX` | interpreter | zonemaps, no scan |
| `WHERE key = <literal>` | interpreter | index funnel (bounds → bloom → seek) |
| selective `WHERE` range | interpreter | zonemap **part pruning** skips non-matching parts |
| writes (`INSERT`/`UPDATE`/`DELETE`/`COPY`) | interpreter | owns the WAL + snapshot clock |
| scans, `GROUP BY`, `ORDER BY`, joins, subqueries, windows | DataFusion | vectorised |

Joins, subqueries, window functions, and `LIKE`/`IN`/`CASE`/scalar functions
require the DataFusion feature (the default). They cannot run inside a
transaction (the transactional path is single-table interpreter-only).

---

## Transactions

```sql
BEGIN;
  UPDATE accounts SET balance = balance - 100 WHERE id = 1;
  UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;   -- or ROLLBACK to discard
```

- **Snapshot isolation.** Reads inside the transaction see the committed state as
  of `BEGIN` plus the transaction's own writes (read-your-writes); nothing
  uncommitted from other connections is visible.
- **Crash-atomic commit.** The whole transaction is logged as one WAL record —
  recovery applies all of it or none.
- **First-committer-wins.** If another transaction committed a change to a key
  this transaction also wrote, `COMMIT` fails with a conflict and the transaction
  is aborted; retry it.
- **Scope.** Statements in a transaction run on the single-table interpreter.
  Use joins/subqueries outside a transaction. DDL (`CREATE TABLE`) inside a
  transaction is applied immediately and not rolled back.

Outside an explicit transaction every statement autocommits.

---

## COPY

```sql
COPY t FROM '/path/data.csv';
COPY t (id, name) FROM '/path/data.csv'
  WITH (FORMAT CSV, HEADER true, DELIMITER '|', QUOTE '"', NULL '\N');
```

Bulk-loads a CSV file through the fast ingest path (skips the per-row duplicate
probe; durable loads are WAL-logged with one flush per chunk). The file is
streamed in chunks, so a multi-gigabyte file never fully materialises in memory.

- **Options:** `FORMAT` (CSV only), `HEADER`, `DELIMITER` (ASCII), `QUOTE`
  (ASCII), `NULL` (marker string). Defaults: `,` delimiter, `"` quote, no header,
  empty-string null marker.
- **CSV rules:** quoted fields may contain the delimiter and `""`-escaped quotes;
  an *unquoted* empty field is `NULL`, a quoted `""` is the empty string.
  Embedded newlines inside quoted fields are not supported.
- **Validation:** every row is type-coerced and constraint-checked (NOT NULL /
  DEFAULT / CHECK / VARCHAR length / DECIMAL precision) — a bad row fails the
  whole `COPY`.
- **Contract:** like the other bulk paths, `COPY` assumes **new keys** (seeding /
  restore); loading a key that already exists creates a second version.
- Only `COPY ... FROM <file>` (import) is supported; `COPY ... TO`, `STDIN`, and
  copy-from-query are rejected.

---

## Durability

A durable database (`Storage`) logs every write to a WAL before acknowledging.
Three durability modes trade latency for guarantees:

| Mode | Guarantee |
|---|---|
| `Sync` | every write `fsync`'d before ack (strongest) |
| `Group` | group commit — concurrent writers share one `fsync` |
| `Async` | acked before `fsync`; a crash may lose the last unflushed writes |

Recovery on reopen replays the WAL past the last checkpoint and rebuilds every
table — schema, constraints, and data — from the manifest.

---

## See also

- [`README.md`](../README.md) — overview, the concurrency wedge, quick start.
- [`requirements.md`](requirements.md) — architecture & design spec, including the
  operating envelope and limitations (§2.2).
- [`clickbench-findings.md`](clickbench-findings.md) — analytics benchmarks vs
  DuckDB.
