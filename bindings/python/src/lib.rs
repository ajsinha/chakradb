//! Native core for the ChakraDB Python driver.
//!
//! This is deliberately thin: it opens a connection and executes one statement,
//! converting results to typed Python objects. The PEP 249 (DB-API 2.0) surface
//! — `Connection`, `Cursor`, `fetch*`, the exception hierarchy — lives in the
//! pure-Python `chakradb` package on top of this, where it is easiest to get the
//! standard semantics exactly right.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use chakradb::sql::Outcome;
use chakradb::storage::{Storage, StorageConfig};
use chakradb::cdc::{Cdc, CdcBackend, Change};
use chakradb::value::Value;
use chakradb::{Database, Graph as CoreGraph, GraphView as CoreGraphView, NodeId, PosixIo, SqlEngine};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

// Error categories the Python layer maps onto the DB-API exception hierarchy.
create_exception!(_core, IntegrityError, PyException);
create_exception!(_core, ProgrammingError, PyException);
create_exception!(_core, OperationalError, PyException);

fn map_err(e: chakradb::Error) -> PyErr {
    use chakradb::Error::*;
    let msg = e.to_string();
    match e {
        DuplicateKey(_) | KeyNotFound(_) | ConstraintViolation(_) => IntegrityError::new_err(msg),
        Sql(_) | SchemaMismatch(_) | TableNotFound(_) | TableExists(_) => {
            ProgrammingError::new_err(msg)
        }
        WriteConflict => OperationalError::new_err(msg),
    }
}

/// A native connection: a `SqlEngine` over either an in-memory database or a
/// durable, WAL-logged directory. The backend is CDC-wrapped so `on_change`
/// subscriptions receive committed writes.
#[pyclass]
struct Connection {
    engine: Option<SqlEngine>,
    cdc: Arc<Cdc>,
}

impl Connection {
    fn engine(&self) -> PyResult<&SqlEngine> {
        self.engine
            .as_ref()
            .ok_or_else(|| ProgrammingError::new_err("connection is closed"))
    }
}

#[pymethods]
impl Connection {
    /// `database` is `":memory:"` (or empty) for an ephemeral database, or a
    /// directory path for a durable, crash-safe one.
    #[new]
    fn new(database: &str) -> PyResult<Self> {
        let cdc = Cdc::new();
        let backend: Arc<dyn chakradb::sql::SqlBackend> =
            if database.is_empty() || database == ":memory:" {
                Arc::new(Database::new())
            } else {
                std::fs::create_dir_all(database).map_err(|e| {
                    OperationalError::new_err(format!("cannot open {database}: {e}"))
                })?;
                let io = PosixIo::open(database).map_err(|e| {
                    OperationalError::new_err(format!("cannot open {database}: {e}"))
                })?;
                let storage = Storage::open(Arc::new(io), StorageConfig::default())
                    .map_err(|e| OperationalError::new_err(format!("recovery failed: {e}")))?;
                Arc::new(storage)
            };
        let engine = SqlEngine::with_backend(CdcBackend::wrap(backend, cdc.clone()));
        Ok(Connection {
            engine: Some(engine),
            cdc,
        })
    }

    /// Execute one statement. Returns `(columns, types, rows, rowcount,
    /// is_query)`: `columns` a list of names, `types` their type chars
    /// (`I`/`R`/`T`), `rows` a list of typed tuples (empty for non-queries),
    /// `rowcount` the affected count for DML (`-1` for queries), and `is_query`
    /// whether a result set was produced.
    fn execute(&self, py: Python<'_>, sql: String) -> PyResult<PyObject> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| ProgrammingError::new_err("connection is closed"))?;
        // Release the GIL during query execution so other Python threads run —
        // this is the whole point of ChakraDB's concurrency.
        let outcome = py.allow_threads(|| engine.run(&sql)).map_err(map_err)?;
        match outcome {
            Outcome::Rows {
                columns,
                types,
                rows,
            } => {
                let py_rows = PyList::empty(py);
                for row in &rows {
                    let cells: Vec<PyObject> = row
                        .iter()
                        .enumerate()
                        .map(|(i, cell)| cell_to_py(py, cell, *types.get(i).unwrap_or(&'?')))
                        .collect();
                    py_rows.append(PyTuple::new(py, &cells)?)?;
                }
                let cols = PyList::new(py, &columns)?;
                let tys = PyList::new(py, types.iter().map(|c| c.to_string()))?;
                Ok((cols, tys, py_rows, -1i64, true)
                    .into_pyobject(py)?
                    .unbind()
                    .into())
            }
            Outcome::Affected(n) => Ok((
                PyList::empty(py),
                PyList::empty(py),
                PyList::empty(py),
                n as i64,
                false,
            )
                .into_pyobject(py)?
                .unbind()
                .into()),
        }
    }

    fn begin(&self) -> PyResult<()> {
        self.engine()?.begin().map_err(map_err)
    }
    fn commit_txn(&self) -> PyResult<()> {
        self.engine()?.commit().map_err(map_err)
    }
    fn rollback_txn(&self) -> PyResult<()> {
        self.engine()?.rollback().map_err(map_err)
    }
    fn in_transaction(&self) -> bool {
        self.engine.as_ref().map(|e| e.in_transaction()).unwrap_or(false)
    }

    fn close(&mut self) {
        self.engine = None;
    }

    /// Open a graph backed by table `name` in this same database. Edges written
    /// through the returned handle are ordinary MVCC rows: transactional, durable,
    /// and visible to SQL. Analytics run over a consistent snapshot via `view()`.
    fn graph(&self, name: &str) -> PyResult<Graph> {
        let backend = self.engine()?.backend().clone();
        CoreGraph::open(backend, name)
            .map(|inner| Graph { inner })
            .map_err(map_err)
    }

    /// Register a change hook: `callback(old, new)` is invoked for every
    /// committed INSERT / UPDATE / DELETE on `table`, from a background thread.
    /// `old` and `new` are dicts (column → value), or `None` for the absent side
    /// of an INSERT / DELETE. The hook fires after the write commits, so it never
    /// blocks the writer — the foundation of an event-driven pipeline (e.g. AML).
    ///
    /// Delivery is at-least-once, in commit order. Keep the callback quick, or
    /// hand work to a queue; a raised exception is printed and the stream
    /// continues. The subscription lives until the connection is closed.
    fn on_change(&self, table: String, callback: PyObject) -> PyResult<()> {
        let stream = self.cdc.subscribe(Some(&table));
        thread::spawn(move || {
            // Block for each committed batch; exit when the publisher is dropped.
            while let Some(batch) = stream.recv() {
                Python::with_gil(|py| {
                    for change in &batch {
                        let (old, new) = change_to_py(py, change);
                        if let Err(err) = callback.call1(py, (old, new)) {
                            err.print(py); // report and keep the stream alive
                        }
                    }
                });
            }
        });
        Ok(())
    }
}

/// Build `(old_dict|None, new_dict|None)` from a change, mapping column names to
/// typed Python values.
fn change_to_py(py: Python<'_>, change: &Change) -> (PyObject, PyObject) {
    let side = |vals: &Option<Vec<Value>>| -> PyObject {
        match vals {
            None => py.None(),
            Some(values) => {
                let d = pyo3::types::PyDict::new(py);
                for (name, v) in change.columns.iter().zip(values.iter()) {
                    let _ = d.set_item(name, value_to_py(py, v));
                }
                d.into_any().unbind()
            }
        }
    };
    (side(&change.old), side(&change.new))
}

/// Convert a ChakraDB `Value` to a native Python object.
fn value_to_py(py: Python<'_>, v: &Value) -> PyObject {
    use pyo3::IntoPyObjectExt;
    let obj: PyResult<PyObject> = match v {
        Value::Null => return py.None(),
        Value::Int(i) => i.into_py_any(py),
        Value::Float(f) => f.into_py_any(py),
        Value::Bool(b) => b.into_py_any(py),
        Value::Text(s) => s.into_py_any(py),
        // Exact fixed-point rendered to float for ergonomics; use SQL for the
        // exact decimal if you need full precision.
        Value::Decimal(mantissa, scale) => {
            (*mantissa as f64 / 10f64.powi(*scale as i32)).into_py_any(py)
        }
    };
    obj.unwrap_or_else(|_| py.None())
}

/// A directed, weighted graph stored as clustered adjacency rows in ChakraDB.
#[pyclass]
struct Graph {
    inner: CoreGraph,
}

#[pymethods]
impl Graph {
    /// Insert or update one edge `src -> dst` with `weight` (default 1.0).
    #[pyo3(signature = (src, dst, weight = 1.0))]
    fn add_edge(&self, src: NodeId, dst: NodeId, weight: f64) -> PyResult<()> {
        self.inner.add_edge(src, dst, weight).map_err(map_err)
    }

    /// Insert or update many edges in one transaction. Accepts an iterable of
    /// `(src, dst)` or `(src, dst, weight)` tuples.
    fn add_edges(&self, edges: Vec<(NodeId, NodeId, f64)>) -> PyResult<()> {
        self.inner.add_edges(edges).map_err(map_err)
    }

    /// Live out-neighbours of `node` (reads the latest committed state).
    fn out_neighbors(&self, node: NodeId) -> PyResult<Vec<NodeId>> {
        self.inner.out_neighbors(node).map_err(map_err)
    }

    /// Freeze a consistent CSR snapshot for read-only analytics. All algorithms
    /// run against the returned `GraphView`, so a whole pipeline sees one graph
    /// even while writers keep appending edges.
    fn view(&self) -> PyResult<GraphView> {
        self.inner.view().map(|inner| GraphView { inner }).map_err(map_err)
    }
}

/// An immutable, in-memory CSR snapshot of a graph. Every algorithm is a method
/// here; results are returned as plain Python `dict`/`list` values keyed by node id.
#[pyclass]
struct GraphView {
    inner: CoreGraphView,
}

#[pymethods]
impl GraphView {
    fn node_count(&self) -> usize {
        self.inner.node_count()
    }
    fn edge_count(&self) -> usize {
        self.inner.edge_count()
    }
    fn out_degree(&self, node: NodeId) -> usize {
        self.inner.out_degree(node)
    }
    fn in_degree(&self, node: NodeId) -> usize {
        self.inner.in_degree(node)
    }
    fn in_neighbors(&self, node: NodeId) -> Vec<NodeId> {
        self.inner.in_neighbors(node)
    }

    // --- Traversal & paths ---
    fn bfs(&self, start: NodeId) -> HashMap<NodeId, u32> {
        self.inner.bfs(start)
    }
    fn shortest_path(&self, from: NodeId, to: NodeId) -> Option<Vec<NodeId>> {
        self.inner.shortest_path(from, to)
    }
    fn dijkstra(&self, from: NodeId) -> HashMap<NodeId, f64> {
        self.inner.dijkstra(from)
    }
    fn weighted_shortest_path(&self, from: NodeId, to: NodeId) -> Option<(Vec<NodeId>, f64)> {
        self.inner.weighted_shortest_path(from, to)
    }
    fn topological_order(&self) -> Option<Vec<NodeId>> {
        self.inner.topological_order()
    }

    // --- Centrality & importance ---
    #[pyo3(signature = (iterations = 20, damping = 0.85))]
    fn pagerank(&self, iterations: usize, damping: f64) -> HashMap<NodeId, f64> {
        self.inner.pagerank(iterations, damping)
    }
    #[pyo3(signature = (seeds, iterations = 40, damping = 0.85))]
    fn personalized_pagerank(
        &self,
        seeds: Vec<NodeId>,
        iterations: usize,
        damping: f64,
    ) -> HashMap<NodeId, f64> {
        self.inner.personalized_pagerank(&seeds, iterations, damping)
    }
    fn degree_centrality(&self) -> HashMap<NodeId, f64> {
        self.inner.degree_centrality()
    }
    fn closeness_centrality(&self) -> HashMap<NodeId, f64> {
        self.inner.closeness_centrality()
    }
    fn betweenness_centrality(&self) -> HashMap<NodeId, f64> {
        self.inner.betweenness_centrality()
    }

    // --- Community & structure ---
    fn connected_components(&self) -> HashMap<NodeId, u32> {
        self.inner.connected_components()
    }
    fn strongly_connected_components(&self) -> Vec<Vec<NodeId>> {
        self.inner.strongly_connected_components()
    }
    /// Non-trivial SCCs — the money-laundering cycles (rings of size ≥ 2).
    fn laundering_cycles(&self) -> Vec<Vec<NodeId>> {
        self.inner.laundering_cycles()
    }
    #[pyo3(signature = (iterations = 10))]
    fn label_propagation(&self, iterations: usize) -> HashMap<NodeId, NodeId> {
        self.inner.label_propagation(iterations)
    }
    fn k_core(&self) -> HashMap<NodeId, u32> {
        self.inner.k_core()
    }
    fn triangle_count(&self) -> u64 {
        self.inner.triangle_count()
    }

    // --- Similarity ---
    fn common_neighbors(&self, a: NodeId, b: NodeId) -> Vec<NodeId> {
        self.inner.common_neighbors(a, b)
    }
    fn jaccard_similarity(&self, a: NodeId, b: NodeId) -> f64 {
        self.inner.jaccard_similarity(a, b)
    }
}

/// Convert one rendered cell to a typed Python object using its column type
/// char (`I`=int, `R`=float, `T`=text). The rendered NULL sentinel becomes
/// `None`; a genuine text value equal to "NULL" is the one known ambiguity.
fn cell_to_py(py: Python<'_>, cell: &str, ty: char) -> PyObject {
    if cell == "NULL" {
        return py.None();
    }
    let obj: PyResult<PyObject> = match ty {
        'I' => cell
            .parse::<i64>()
            .map(|v| v.into_pyobject(py).unwrap().into_any().unbind())
            .or_else(|_| Ok(cell.into_pyobject(py)?.into_any().unbind())),
        'R' => cell
            .parse::<f64>()
            .map(|v| v.into_pyobject(py).unwrap().into_any().unbind())
            .or_else(|_| Ok(cell.into_pyobject(py)?.into_any().unbind())),
        _ => Ok(cell.into_pyobject(py).unwrap().into_any().unbind()),
    };
    obj.unwrap_or_else(|_| py.None())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Connection>()?;
    m.add_class::<Graph>()?;
    m.add_class::<GraphView>()?;
    m.add("IntegrityError", m.py().get_type::<IntegrityError>())?;
    m.add("ProgrammingError", m.py().get_type::<ProgrammingError>())?;
    m.add("OperationalError", m.py().get_type::<OperationalError>())?;
    Ok(())
}
