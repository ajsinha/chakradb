# Your First Database (Rust)

```{=latex}
\epigraph{What I cannot create, I do not understand.}{--- Richard Feynman}
```

This chapter walks through a complete Rust program: create a durable database,
define a table with real constraints and types, insert rows in a transaction,
and query them back. Every feature here is exercised by the test suite.

## An in-memory database

The smallest possible program:

```rust
use chakradb::{Database, SqlEngine};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::new());
    let sql = SqlEngine::new(db.clone());

    sql.run("CREATE TABLE accounts (
        id     INTEGER PRIMARY KEY,
        owner  VARCHAR(64) NOT NULL,
        balance DECIMAL(14,2) NOT NULL DEFAULT 0.00
    )")?;

    sql.run("INSERT INTO accounts VALUES (1, 'ada', 500.00)")?;
    sql.run("INSERT INTO accounts VALUES (2, 'grace', 1250.50)")?;

    let rows = sql.query("SELECT id, owner, balance FROM accounts ORDER BY id")?;
    for r in rows {
        println!("{r:?}");
    }
    Ok(())
}
```

`SqlEngine::run` executes a statement (DDL or DML) and returns an
[`Outcome`](../reference/rust-api.md); `SqlEngine::query` is a convenience that
returns rows already rendered as `Vec<Vec<String>>`.

## Types and constraints are enforced

ChakraDB is not a stringly-typed store. The declared types and constraints are
checked at write time:

```rust
// Rejected: NOT NULL violated.
assert!(sql.run("INSERT INTO accounts (id, owner) VALUES (3, NULL)").is_err());

// Rejected: DECIMAL(14,2) — an over-precise literal is a constraint error, not
// a silent rounding.
assert!(sql.run("INSERT INTO accounts VALUES (3, 'lin', 1.234)").is_err());

// Rejected: VARCHAR(64) length is enforced.
// Rejected: duplicate PRIMARY KEY.
assert!(sql.run("INSERT INTO accounts VALUES (1, 'dup', 0.00)").is_err());
```

Money is stored as **exact** fixed-point `DECIMAL`, never binary floating point —
see [Exact Decimal Arithmetic](../algorithms/decimal.md).

## Transactions

By default each statement autocommits. Wrap several in a transaction for
all-or-nothing semantics under snapshot isolation:

```rust
sql.begin()?;
sql.run("INSERT INTO accounts VALUES (10, 'noether', 300.00)")?;
sql.run("INSERT INTO accounts VALUES (11, 'hopper', 300.00)")?;
sql.commit()?;                       // both visible, atomically
// ... or sql.rollback()? to discard the whole unit.
```

Readers never block writers and writers never block readers: a transaction sees
a consistent [MVCC snapshot](../architecture/mvcc.md) taken at its start.

## A durable, on-disk database

Swap the in-memory `Database` for a `Storage` opened over a directory. Nothing
else in your code changes — the SQL surface is identical:

```rust
use chakradb::{PosixIo, SqlEngine};
use chakradb::storage::{Storage, StorageConfig};
use std::sync::Arc;

let io = PosixIo::new("/var/lib/myapp")?;
let storage = Storage::open(Arc::new(io), StorageConfig::default())?;
let sql = SqlEngine::durable(Arc::new(storage));
```

Writes are made durable through a write-ahead log with group commit, and the
engine recovers automatically on the next open — see
[Durability](../architecture/durability.md) and
[Crash Recovery](../algorithms/recovery.md).

## Where to go next

- [Python in Five Minutes](python.md) — the same engine from Python.
- [A Graph in Five Minutes](graph.md) — treat your tables as a graph.
- [The SQL Reference](../guide/sql-reference.md) — the full statement surface.
