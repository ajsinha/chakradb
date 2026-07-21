//! SQL query layer (M2).
//!
//! A real SQL surface over the storage engine: parse with `sqlparser` under the
//! PostgreSQL dialect (`requirements.md` §9), plan into a small logical form,
//! and interpret it against a [`Database`](crate::database::Database).
//!
//! # Scope, stated honestly
//!
//! This is a *documented subset*, not a compatibility claim. Supported:
//! `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, and single-table `SELECT` with
//! projection, `WHERE`, `GROUP BY`, aggregates (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`),
//! `ORDER BY`, `LIMIT`, and `DISTINCT`. Not supported (and rejected with a clear
//! error rather than mis-executed): joins, subqueries, and DDL beyond the fixed
//! M0 schema. Where compatibility is won or lost is the type system and function
//! library, which §9 explicitly defers.
//!
//! Execution is a plain interpreter. Per §8, if execution ever becomes the
//! bottleneck the plan is to adopt DataFusion behind the `scan` boundary — not
//! to hand-tune this. The value here is the correctness surface and the
//! conformance harness it enables.

pub mod backend;
pub mod exec;
pub mod expr;
pub mod plan;
pub mod value;

pub use backend::SqlBackend;
pub use exec::{execute, Outcome};
pub use plan::{plan, plan_in, Plan};
pub use plan::{AggFn, Projection};
pub use value::Value;

use crate::database::Database;
use crate::error::Error;
use crate::storage::Storage;
use std::sync::Arc;

/// A SQL front-end bound to a catalog. The catalog is either an in-memory
/// [`Database`] or a durable [`Storage`]; with the latter, SQL writes are logged
/// to the WAL and survive a crash.
pub struct SqlEngine {
    backend: Arc<dyn SqlBackend>,
}

impl std::fmt::Debug for SqlEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlEngine").finish_non_exhaustive()
    }
}

impl SqlEngine {
    /// Bind to an in-memory database (no durability).
    pub fn new(db: Arc<Database>) -> Self {
        SqlEngine { backend: db }
    }

    /// Bind to durable storage: SQL writes are WAL-logged and crash-safe.
    pub fn durable(storage: Arc<Storage>) -> Self {
        SqlEngine { backend: storage }
    }

    /// Parse, plan, and execute one statement. Column names resolve against the
    /// live catalog, so each table's declared schema is honoured.
    pub fn run(&self, sql: &str) -> Result<Outcome, Error> {
        let plan = plan_in(sql, &*self.backend).map_err(Error::Sql)?;
        execute(&*self.backend, plan)
    }

    /// Convenience: run a query and return its rows, or an error for
    /// non-queries.
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<String>>, Error> {
        match self.run(sql)? {
            Outcome::Rows { rows, .. } => Ok(rows),
            Outcome::Affected(_) => Err(Error::Sql("expected a query, got a statement".into())),
        }
    }

    /// The backend this engine writes through.
    pub fn backend(&self) -> &Arc<dyn SqlBackend> {
        &self.backend
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> SqlEngine {
        SqlEngine::new(Arc::new(Database::new()))
    }

    #[test]
    fn end_to_end_ddl_dml_query() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)").unwrap();
        e.run("INSERT INTO t VALUES (1, 100, 1.5, 'alice')").unwrap();
        e.run("INSERT INTO t VALUES (2, 200, 2.5, 'bob')").unwrap();

        let rows = e.query("SELECT c FROM t WHERE a > 150").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "bob");
    }

    #[test]
    fn update_then_query_reflects_change() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)").unwrap();
        e.run("INSERT INTO t VALUES (1, 1, 0, 'old')").unwrap();
        e.run("UPDATE t SET c = 'new' WHERE pk = 1").unwrap();
        assert_eq!(e.query("SELECT c FROM t").unwrap()[0][0], "new");
    }

    #[test]
    fn aggregate_over_inserts() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)").unwrap();
        for i in 1..=10 {
            e.run(&format!("INSERT INTO t VALUES ({i}, {}, 0, 'x')", i * 10))
                .unwrap();
        }
        let rows = e.query("SELECT SUM(a), COUNT(*) FROM t").unwrap();
        assert_eq!(rows[0][0], "550.0");
        assert_eq!(rows[0][1], "10");
    }

    #[test]
    fn parse_errors_surface_as_sql_errors() {
        let e = engine();
        assert!(matches!(e.run("NOT SQL AT ALL"), Err(Error::Sql(_))));
    }

    #[test]
    fn query_on_a_statement_is_an_error() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)").unwrap();
        assert!(e.query("INSERT INTO t VALUES (1,1,1,'x')").is_err());
    }

    #[test]
    fn snapshot_semantics_hold_through_sql() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)").unwrap();
        e.run("INSERT INTO t VALUES (1, 1, 0, 'a')").unwrap();
        e.run("INSERT INTO t VALUES (2, 2, 0, 'b')").unwrap();
        e.run("DELETE FROM t WHERE pk = 1").unwrap();
        assert_eq!(e.query("SELECT COUNT(*) FROM t").unwrap()[0][0], "1");
    }
}
