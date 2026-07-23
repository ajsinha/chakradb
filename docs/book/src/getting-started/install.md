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
> HTAP. Every algorithm in [Part IV — Graph](../graph/overview.md) is available
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
[Your First Database (Rust)](first-rust.md).

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

The Python driver implements [PEP 249 (DB-API 2.0)](../guide/python-driver.md)
and exposes the graph engine via `conn.graph(name)` — see
[A Graph in Five Minutes](graph.md).

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
