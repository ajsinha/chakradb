# Installation & Building

```{=latex}
\epigraph{The beginning is the most important part of the work.}{--- Plato, \emph{The Republic}}
```

ChakraDB is an embedded database: there is no server to install, no daemon to
run. You add it to a Rust project as a crate, or install the Python package and
`import chakradb`. This chapter gets you from a clean checkout to a running
build.

## Prerequisites

- **Rust** 1.75 or newer (`rustup` recommended).
- A C toolchain (for a handful of transitive native dependencies).
- **Python** 3.9+ and [`maturin`](https://www.maturin.rs/) *only* if you want the
  Python bindings.

## Building the core (Rust)

The core builds in two flavours, selected by Cargo features:

| Build | Command | What you get |
|-------|---------|--------------|
| Interpreter only | `cargo build --no-default-features` | The full storage engine, SQL interpreter, MVCC, and the graph library — no analytical vectorized engine. Small and fast to compile. |
| With DataFusion | `cargo build --features datafusion` | Adds the vectorized analytical engine and the HTAP query router. |

Run the test suite the same way:

```bash
cargo test --no-default-features          # core + graph
cargo test --features datafusion          # + analytical path
```

> **Why two profiles?** The interpreter path is what powers the transactional
> workload and the graph engine; it has no heavy dependencies and compiles in
> seconds. DataFusion is bought in only when you want the analytical half of
> HTAP. Every algorithm in [the Graph Engine part](../graph/model.md) is available
> in *both* profiles.

## Adding ChakraDB to a Rust project

```toml
# Cargo.toml
[dependencies]
chakradb = { path = "../chakradb", default-features = false }
```

Then, in code:

```rust
use chakradb::{Database, SqlEngine};
use std::sync::Arc;

let db = Arc::new(Database::new());     // in-memory
let sql = SqlEngine::new(db.clone());
sql.run("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")?;
```

For a durable, on-disk database, open a `Storage` over a directory instead — see
Your First Database (Rust).

## Building the Python bindings

The Python package is a thin PyO3 extension over the same core. From
`bindings/python`:

```bash
cd bindings/python
maturin develop --release        # builds the extension into your venv
# or, for a wheel you can install elsewhere:
maturin build --release
```

Once built, the package is importable anywhere:

```python
import chakradb
conn = chakradb.connect(":memory:")
```

The Python driver implements PEP 249 (DB-API 2.0)
and exposes the graph engine via `conn.graph(name)` — see
A Graph in Five Minutes.

## Running the examples

Two end-to-end examples ship in the repository:

```bash
# Rust: a full real-time AML system over synthetic data
cargo run --release --example aml_realtime --no-default-features

# Python: the same application through the client bindings
python examples/aml_app.py
```

Both generate their own data, run the graph-analytics ensemble, and verify that
every planted laundering typology is detected. They are the reference for the
[Real-Time AML case study](../case-studies/aml.md).

## Your First Database (Rust)


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
`Outcome`; `SqlEngine::query` is a convenience that
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
see [Exact Decimal Arithmetic](../engine/data-types.md).

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
a consistent [MVCC snapshot](../engine/mvcc.md) taken at its start.

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
[Durability](../engine/durability.md) and
[Crash Recovery](../engine/durability.md).

## Where to go next

- Python in Five Minutes — the same engine from Python.
- A Graph in Five Minutes — treat your tables as a graph.
- The SQL Reference — the full statement surface.

## Python in Five Minutes


The Python bindings wrap the exact same engine as the Rust core. The SQL surface
is exposed through a standard PEP 249 (DB-API 2.0)
driver, so if you have used `sqlite3` you already know the shape of it.

## Connect, execute, fetch

```python
import chakradb

conn = chakradb.connect(":memory:")          # or a directory path for durability
cur = conn.execute("""
    CREATE TABLE accounts (
        id      INTEGER PRIMARY KEY,
        owner   VARCHAR(64) NOT NULL,
        balance DECIMAL(14,2) NOT NULL DEFAULT 0.00
    )""")

conn.execute("INSERT INTO accounts VALUES (1, 'ada', 500.00)")
conn.execute("INSERT INTO accounts VALUES (2, 'grace', 1250.50)")
conn.commit()

for row in conn.execute("SELECT id, owner, balance FROM accounts ORDER BY id"):
    print(row)          # (1, 'ada', '500.00')  ...
```

`connect`, `Connection.execute`, `Cursor.fetchone/fetchmany/fetchall`,
`commit`, `rollback`, and the full exception hierarchy
(`IntegrityError`, `ProgrammingError`, `OperationalError`, …) all behave exactly
as PEP 249 specifies.

## Parameters and transactions

Parameters use the `qmark` style; transactions are explicit around a unit of
work:

```python
conn.execute("INSERT INTO accounts VALUES (?, ?, ?)", (3, 'lin', 42.00))

try:
    conn.execute("BEGIN")  # or rely on autocommit-off via .execute + .commit
    conn.execute("INSERT INTO accounts VALUES (10, 'noether', 300.00)")
    conn.execute("INSERT INTO accounts VALUES (11, 'hopper', 300.00)")
    conn.commit()
except chakradb.IntegrityError as e:
    conn.rollback()
    print("rejected:", e)
```

Type and constraint violations surface as `IntegrityError`; malformed SQL as
`ProgrammingError`; a write–write conflict as `OperationalError`.

## The graph engine, from Python

Any table can be viewed as a graph. `conn.graph(name)` returns a handle backed
by table `name` in the same database; edges are ordinary transactional rows:

```python
g = conn.graph("payments")
g.add_edges([(1, 2, 100.0), (2, 3, 250.0), (3, 1, 90.0)])   # (src, dst, weight)

view = g.view()                        # one consistent snapshot for analytics
print(view.laundering_cycles())        # [[1, 2, 3]] — a round-trip ring
print(view.pagerank())                 # {1: 0.33, 2: 0.33, 3: 0.33}
print(view.personalized_pagerank(seeds=[1]))
```

Every algorithm returns plain Python `dict`/`list` values keyed by node id. The
full catalogue is in A Graph in Five Minutes and
[Graph Algorithms](../graph/algorithms.md).

## A complete application

`examples/aml_app.py` is a full real-time anti-money-laundering system in ~300
lines of Python: it builds a synthetic payment network, runs an ensemble of
graph detectors, and ranks suspicious accounts. Walk through it in the
[Real-Time AML case study](../case-studies/aml.md).

## A Graph in Five Minutes


ChakraDB has a graph engine built into the core — not a bolt-on, not a separate
store. A graph *is* a table of edges, clustered so that a node's neighbours sit
together on disk; analytics run over a consistent [MVCC snapshot](../engine/mvcc.md)
of that table. This chapter is the whirlwind tour.

## Open a graph and add edges

A graph is backed by a named table in an ordinary database. In Rust:

```rust
use chakradb::{Database, Graph};
use std::sync::Arc;

let db = Arc::new(Database::new());
let g = Graph::open(db.clone(), "transfers")?;

// (src, dst, weight) — inserts are upserts, and fully transactional.
g.add_edges([(1, 2, 100.0), (2, 3, 250.0), (3, 1, 90.0)])?;
```

In Python, through an existing connection:

```python
g = conn.graph("transfers")
g.add_edges([(1, 2, 100.0), (2, 3, 250.0), (3, 1, 90.0)])
```

Because edges are just rows, the *same* data is visible to SQL
(`SELECT ... FROM transfers`) and to the graph engine simultaneously — that is
the HTAP promise applied to graphs.

## Freeze a snapshot, then compute

All algorithms run against a `view()` — an immutable in-memory
[CSR](../graph/snapshot.md) snapshot. Take it once and a whole pipeline sees one
coherent graph, even while writers keep appending edges:

```rust
let view = g.view()?;
println!("{} nodes, {} edges", view.node_count(), view.edge_count());

let dist   = view.bfs(1);                       // hops from node 1
let path   = view.shortest_path(1, 3);          // Some([1, 2, 3])
let cost   = view.weighted_shortest_path(1, 3); // Dijkstra with weights
let rank   = view.pagerank(20, 0.85);           // {node: score}
let rings  = view.laundering_cycles();          // non-trivial SCCs
```

The Python surface is identical, returning dicts and lists:

```python
view = g.view()
view.bfs(1)                       # {1: 0, 2: 1, 3: 1}
view.shortest_path(1, 3)          # [1, 2, 3]
view.pagerank()                   # {1: 0.33, 2: 0.33, 3: 0.33}
view.laundering_cycles()          # [[1, 2, 3]]
view.personalized_pagerank([1])   # risk spread from a seed set
```

## The algorithm catalogue

Every one of these is built into the core and available from both Rust and
Python. See [Graph Algorithms](../graph/algorithms.md) for the derivations.

| Category | Algorithms |
|----------|-----------|
| **Traversal & paths** | `bfs`, `shortest_path`, `dijkstra`, `weighted_shortest_path`, `topological_order` |
| **Centrality** | `pagerank`, `personalized_pagerank`, `degree_centrality`, `closeness_centrality`, `betweenness_centrality` |
| **Community & structure** | `connected_components`, `strongly_connected_components`, `laundering_cycles`, `label_propagation`, `k_core`, `triangle_count` |
| **Systemic risk** | `eisenberg_noe` (clearing vector / default cascade) |
| **Similarity & link prediction** | `common_neighbors`, `jaccard_similarity`, `adamic_adar`, `recommend` |
| **Degrees** | `in_degree`, `out_degree`, `in_neighbors`, `out_neighbors` |

## Snapshots are stable under writes

The defining property — an analysis is reproducible because its view does not
move under it:

```rust
let v = g.view()?;
let before = v.edge_count();
g.add_edges([(9, 10, 1.0)])?;           // keep writing after the view is taken
assert_eq!(v.edge_count(), before);     // the snapshot did not change
assert_eq!(g.view()?.edge_count(), before + 1);  // a fresh view sees the write
```

## A real application

The [Real-Time AML case study](../case-studies/aml.md) composes these primitives
into a working fraud-detection system. The runnable code is
`examples/aml_realtime.rs` (Rust) and `examples/aml_app.py` (Python).
