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
#[cfg(feature = "datafusion")]
pub mod df;
pub mod exec;
pub mod expr;
pub mod plan;
pub mod txn;
pub mod value;

pub use backend::SqlBackend;
pub use exec::{execute, Outcome};
pub use plan::{plan, plan_in, Plan};
pub use plan::{AggFn, Projection};
pub use txn::Transaction;
pub use value::Value;

use crate::database::Database;
use crate::error::Error;
use crate::storage::Storage;
use plan::{txn_control, TxnControl};
use std::sync::{Arc, Mutex};

/// A SQL front-end bound to a catalog. The catalog is either an in-memory
/// [`Database`] or a durable [`Storage`]; with the latter, SQL writes are logged
/// to the WAL and survive a crash.
pub struct SqlEngine {
    backend: Arc<dyn SqlBackend>,
    /// The open transaction, if any. `None` means autocommit.
    txn: Mutex<Option<Transaction>>,
}

impl std::fmt::Debug for SqlEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlEngine").finish_non_exhaustive()
    }
}

/// The key-column index of a `SELECT`'s table, for the point-lookup routing
/// check. Defaults to 0 for non-selects or unknown tables (harmless — those
/// don't reach the point-lookup branch).
#[cfg(feature = "datafusion")]
fn plan_key_index(backend: &Arc<dyn SqlBackend>, plan: &Plan) -> usize {
    if let Plan::Select { table, .. } = plan {
        if let Ok(t) = backend.table(table) {
            return t.schema().key_index();
        }
    }
    0
}

impl SqlEngine {
    /// Bind to an in-memory database (no durability).
    pub fn new(db: Arc<Database>) -> Self {
        SqlEngine {
            backend: db,
            txn: Mutex::new(None),
        }
    }

    /// Bind to durable storage: SQL writes are WAL-logged and crash-safe.
    pub fn durable(storage: Arc<Storage>) -> Self {
        SqlEngine {
            backend: storage,
            txn: Mutex::new(None),
        }
    }

    /// True if a transaction is currently open.
    pub fn in_transaction(&self) -> bool {
        self.txn.lock().unwrap().is_some()
    }

    /// Parse, plan, and execute one statement — the HTAP router.
    ///
    /// Each statement goes to whichever engine is faster for its shape (the point
    /// of an HTAP system):
    /// - Writes, DDL, metadata `COUNT(*)`, and key point lookups → the
    ///   **interpreter** (owns the WAL and snapshot clock; hits the index funnel).
    /// - Analytical reads — `GROUP BY`, aggregates, scans, `ORDER BY` — and
    ///   anything the interpreter can't plan (joins, windows, subqueries) →
    ///   **DataFusion**'s vectorised engine over an MVCC snapshot.
    ///
    /// Without the `datafusion` feature, everything runs on the interpreter (the
    /// analytical shapes just run slower, and joins/subqueries are rejected).
    pub fn run(&self, sql: &str) -> Result<Outcome, Error> {
        // Transaction control comes first.
        if let Some(ctl) = txn_control(sql) {
            return self.run_txn_control(ctl);
        }

        // Inside a transaction, every statement runs on the interpreter against
        // the private overlay (read-your-writes; nothing hits the real WAL until
        // COMMIT). Joins/subqueries are unsupported here — use them outside a
        // transaction.
        {
            let guard = self.txn.lock().unwrap();
            if let Some(txn) = guard.as_ref() {
                let plan = plan_in(sql, txn).map_err(Error::Sql)?;
                return execute(txn, plan);
            }
        }

        // Autocommit path.
        match plan_in(sql, &*self.backend) {
            Ok(plan) => {
                #[cfg(feature = "datafusion")]
                {
                    let key_index = plan_key_index(&self.backend, &plan);
                    if exec::prefers_vectorized(&plan, key_index) {
                        // A predicate whose zonemaps prune most parts runs faster
                        // on the pruning interpreter than a full columnar scan.
                        if let Plan::Select { table, .. } = &plan {
                            if let Ok(t) = self.backend.table(table) {
                                if exec::prune_favors_interpreter(&plan, &t) {
                                    return execute(&*self.backend, plan);
                                }
                            }
                        }
                        return df::execute_query(&*self.backend, sql);
                    }
                }
                execute(&*self.backend, plan)
            }
            // The interpreter can't plan this. With DataFusion, hand it the raw
            // SQL — but only for a SELECT (a join / subquery / window it doesn't
            // support). A write or DDL that failed to plan — e.g. a constraint
            // violation or an unsupported column option — is a real error to
            // surface, not something to retry on the read-only vectorised engine.
            Err(e) => {
                #[cfg(feature = "datafusion")]
                {
                    if plan::is_query(sql) {
                        return df::execute_query(&*self.backend, sql);
                    }
                    Err(Error::Sql(e))
                }
                #[cfg(not(feature = "datafusion"))]
                {
                    Err(Error::Sql(e))
                }
            }
        }
    }

    fn run_txn_control(&self, ctl: TxnControl) -> Result<Outcome, Error> {
        let mut guard = self.txn.lock().unwrap();
        match ctl {
            TxnControl::Begin => {
                if guard.is_some() {
                    return Err(Error::Sql("a transaction is already open".into()));
                }
                *guard = Some(Transaction::begin(self.backend.clone()));
            }
            TxnControl::Commit => match guard.take() {
                Some(t) => t.commit()?,
                None => return Err(Error::Sql("no transaction to commit".into())),
            },
            TxnControl::Rollback => {
                // Dropping the transaction discards the overlay and change-set.
                *guard = None;
            }
        }
        Ok(Outcome::Affected(0))
    }

    /// Begin/commit/rollback for drivers that manage transactions explicitly
    /// (e.g. the Python DB-API layer), without round-tripping SQL text.
    pub fn begin(&self) -> Result<(), Error> {
        self.run_txn_control(TxnControl::Begin).map(|_| ())
    }
    pub fn commit(&self) -> Result<(), Error> {
        self.run_txn_control(TxnControl::Commit).map(|_| ())
    }
    pub fn rollback(&self) -> Result<(), Error> {
        self.run_txn_control(TxnControl::Rollback).map(|_| ())
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
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
            .unwrap();
        e.run("INSERT INTO t VALUES (1, 100, 1.5, 'alice')")
            .unwrap();
        e.run("INSERT INTO t VALUES (2, 200, 2.5, 'bob')").unwrap();

        let rows = e.query("SELECT c FROM t WHERE a > 150").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "bob");
    }

    #[test]
    fn update_then_query_reflects_change() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
            .unwrap();
        e.run("INSERT INTO t VALUES (1, 1, 0, 'old')").unwrap();
        e.run("UPDATE t SET c = 'new' WHERE pk = 1").unwrap();
        assert_eq!(e.query("SELECT c FROM t").unwrap()[0][0], "new");
    }

    #[test]
    fn aggregate_over_inserts() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
            .unwrap();
        for i in 1..=10 {
            e.run(&format!("INSERT INTO t VALUES ({i}, {}, 0, 'x')", i * 10))
                .unwrap();
        }
        let rows = e.query("SELECT SUM(a), COUNT(*) FROM t").unwrap();
        assert_eq!(rows[0][0], "550");
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
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
            .unwrap();
        assert!(e.query("INSERT INTO t VALUES (1,1,1,'x')").is_err());
    }

    #[test]
    fn snapshot_semantics_hold_through_sql() {
        let e = engine();
        e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
            .unwrap();
        e.run("INSERT INTO t VALUES (1, 1, 0, 'a')").unwrap();
        e.run("INSERT INTO t VALUES (2, 2, 0, 'b')").unwrap();
        e.run("DELETE FROM t WHERE pk = 1").unwrap();
        assert_eq!(e.query("SELECT COUNT(*) FROM t").unwrap()[0][0], "1");
    }
}
