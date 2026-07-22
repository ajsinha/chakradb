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
use chakradb::{Database, PosixIo, SqlEngine};
use std::sync::Arc;

// Error categories the Python layer maps onto the DB-API exception hierarchy.
create_exception!(_core, IntegrityError, PyException);
create_exception!(_core, ProgrammingError, PyException);
create_exception!(_core, OperationalError, PyException);

fn map_err(e: chakradb::Error) -> PyErr {
    use chakradb::Error::*;
    let msg = e.to_string();
    match e {
        DuplicateKey(_) | KeyNotFound(_) => IntegrityError::new_err(msg),
        Sql(_) | SchemaMismatch(_) | TableNotFound(_) | TableExists(_) => {
            ProgrammingError::new_err(msg)
        }
        WriteConflict => OperationalError::new_err(msg),
    }
}

/// A native connection: a `SqlEngine` over either an in-memory database or a
/// durable, WAL-logged directory.
#[pyclass]
struct Connection {
    engine: Option<SqlEngine>,
}

#[pymethods]
impl Connection {
    /// `database` is `":memory:"` (or empty) for an ephemeral database, or a
    /// directory path for a durable, crash-safe one.
    #[new]
    fn new(database: &str) -> PyResult<Self> {
        let engine = if database.is_empty() || database == ":memory:" {
            SqlEngine::new(Arc::new(Database::new()))
        } else {
            std::fs::create_dir_all(database)
                .map_err(|e| OperationalError::new_err(format!("cannot open {database}: {e}")))?;
            let io = PosixIo::open(database)
                .map_err(|e| OperationalError::new_err(format!("cannot open {database}: {e}")))?;
            let storage = Storage::open(Arc::new(io), StorageConfig::default())
                .map_err(|e| OperationalError::new_err(format!("recovery failed: {e}")))?;
            SqlEngine::durable(Arc::new(storage))
        };
        Ok(Connection {
            engine: Some(engine),
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

    fn close(&mut self) {
        self.engine = None;
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
    m.add("IntegrityError", m.py().get_type::<IntegrityError>())?;
    m.add("ProgrammingError", m.py().get_type::<ProgrammingError>())?;
    m.add("OperationalError", m.py().get_type::<OperationalError>())?;
    Ok(())
}
