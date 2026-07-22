//! Transactions — `BEGIN` / `COMMIT` / `ROLLBACK`.
//!
//! A transaction is a private **overlay** catalog initialised from a committed
//! snapshot, plus a change-set to replay on commit. It implements
//! [`SqlBackend`], so the interpreter runs against it unchanged:
//!
//! - reads see the committed state at `BEGIN` plus the transaction's own writes
//!   (read-your-writes), and *nothing* uncommitted from other connections;
//! - writes go only into the overlay (and the change-set) — never the real
//!   engine or the WAL — so a crash or `ROLLBACK` simply discards them;
//! - `COMMIT` replays the change-set to the real backend, which durably logs it.
//!
//! Scope (v1): statements in a transaction run on the interpreter (single-table;
//! joins/subqueries belong outside a transaction). Referenced tables are
//! materialised into the overlay on first touch — fine for OLTP-sized working
//! sets. Commit replays writes op-by-op; crash-atomicity of a multi-statement
//! commit (WAL transaction markers) is a follow-up.

use super::backend::SqlBackend;
use crate::csn::{Csn, Snapshot};
use crate::database::Database;
use crate::error::Result;
use crate::schema::{Row, Schema};
use crate::table::Table;
use crate::value::{Key, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// The writes a transaction has made, per table: key → `Some(row)` (put) or
/// `None` (delete).
type ChangeSet = HashMap<String, BTreeMap<Key, Option<Row>>>;

pub struct Transaction {
    real: Arc<dyn SqlBackend>,
    snapshot: Snapshot,
    overlay: Database,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("snapshot", &self.snapshot)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Inner {
    materialized: HashSet<String>,
    writes: ChangeSet,
}

impl Transaction {
    /// Open a transaction over `real`, pinned to its current committed snapshot.
    pub fn begin(real: Arc<dyn SqlBackend>) -> Self {
        let snapshot = real.snapshot();
        Transaction {
            real,
            snapshot,
            overlay: Database::new(),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Materialise `name` into the overlay from committed state, once.
    fn materialize(&self, name: &str) -> Result<()> {
        if self.inner.lock().unwrap().materialized.contains(name) {
            return Ok(());
        }
        if let Ok(rt) = self.real.table(name) {
            let schema = rt.schema().clone();
            let rows: Vec<Row> = rt.scan(self.snapshot).iter().collect();
            let _ = self.overlay.create_table_schema(name, schema);
            self.overlay.table(name)?.bulk_load(rows);
        }
        self.inner
            .lock()
            .unwrap()
            .materialized
            .insert(name.to_string());
        Ok(())
    }

    /// Record a write into the change-set for commit replay.
    fn record(&self, table: &str, key: Key, op: Option<Row>) {
        self.inner
            .lock()
            .unwrap()
            .writes
            .entry(table.to_string())
            .or_default()
            .insert(key, op);
    }

    fn overlay_table(&self, name: &str) -> Result<Arc<Table>> {
        self.materialize(name)?;
        self.overlay.table(name)
    }

    /// Apply the change-set to the real backend. Consumes the transaction.
    pub fn commit(self) -> Result<()> {
        let writes = std::mem::take(&mut self.inner.lock().unwrap().writes);
        for (table, table_writes) in writes {
            let synthetic = self
                .real
                .table(&table)
                .map(|t| t.schema().synthetic_key())
                .unwrap_or(false);
            let key_index = self
                .real
                .table(&table)
                .map(|t| t.schema().key_index())
                .unwrap_or(0);
            for (_key, op) in table_writes {
                match op {
                    Some(mut row) => {
                        if synthetic {
                            // The overlay's rowid does not align with the real
                            // table's; let the real table assign a fresh one.
                            row.values[key_index] = Value::Null;
                            self.real.insert(&table, row)?;
                        } else {
                            self.real.upsert(&table, row)?;
                        }
                    }
                    None => {
                        let _ = self.real.delete(&table, &_key.0);
                    }
                }
            }
        }
        Ok(())
    }
}

impl SqlBackend for Transaction {
    fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        // DDL is applied immediately to the real backend (not rolled back in v1)
        // and mirrored into the overlay.
        self.real.create_table(name, schema.clone())?;
        self.overlay.create_table_schema(name, schema)?;
        self.inner
            .lock()
            .unwrap()
            .materialized
            .insert(name.to_string());
        Ok(())
    }

    fn table(&self, name: &str) -> Result<Arc<Table>> {
        self.overlay_table(name)
    }

    fn table_names(&self) -> Vec<String> {
        self.real.table_names()
    }

    fn snapshot(&self) -> Snapshot {
        // Reads run against the overlay, so its clock is the relevant one.
        self.overlay.snapshot()
    }

    fn insert(&self, table: &str, row: Row) -> Result<Csn> {
        let t = self.overlay_table(table)?;
        let ki = t.schema().key_index();
        let (csn, stored) = t.insert_returning(row)?;
        self.record(table, Key(stored.key(ki).clone()), Some(stored));
        Ok(csn)
    }

    fn upsert(&self, table: &str, row: Row) -> Result<Csn> {
        let t = self.overlay_table(table)?;
        let ki = t.schema().key_index();
        let (csn, stored) = t.upsert_returning(row)?;
        self.record(table, Key(stored.key(ki).clone()), Some(stored));
        Ok(csn)
    }

    fn update(&self, table: &str, row: Row) -> Result<Csn> {
        let t = self.overlay_table(table)?;
        let ki = t.schema().key_index();
        let key = Key(row.key(ki).clone());
        let csn = t.update(row.clone())?;
        self.record(table, key, Some(row));
        Ok(csn)
    }

    fn delete(&self, table: &str, key: &Value) -> Result<Csn> {
        let t = self.overlay_table(table)?;
        let csn = t.delete(key)?;
        self.record(table, Key(key.clone()), None);
        Ok(csn)
    }
}
