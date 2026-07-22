//! The target a SQL statement executes against.
//!
//! ChakraDB has two catalogs: the in-memory [`Database`] (fast, no durability)
//! and [`Storage`] (WAL-logged, crash-safe). Historically SQL ran only on the
//! former, so SQL writes were never durable. This trait unifies them: the
//! executor writes through a `SqlBackend`, and `SqlEngine` can be bound to either
//! — an in-memory database for tests, or `Storage` for a durable SQL database.

use crate::csn::{Csn, Snapshot, SnapshotPin};
use crate::database::Database;
use crate::error::Result;
use crate::schema::{Row, Schema};
use crate::storage::Storage;
use crate::table::Table;
use crate::value::Value;
use std::sync::Arc;

/// A catalog SQL can read and write. Writes through a durable backend are logged
/// to the WAL before they are acknowledged.
pub trait SqlBackend: Send + Sync {
    /// Create a table with an explicit schema.
    fn create_table(&self, name: &str, schema: Schema) -> Result<()>;
    /// Resolve a table for reads (and schema resolution during planning).
    fn table(&self, name: &str) -> Result<Arc<Table>>;
    /// Every table name in the catalog (for registering with an external
    /// executor such as DataFusion).
    fn table_names(&self) -> Vec<String>;
    /// A snapshot consistent across the catalog.
    fn snapshot(&self) -> Snapshot;
    /// Pin a read snapshot for the duration of a statement (or transaction), so
    /// concurrent compaction cannot reclaim a version this read may observe. The
    /// returned guard carries the snapshot; hold it for the whole read.
    fn pin(&self) -> SnapshotPin;
    fn insert(&self, table: &str, row: Row) -> Result<Csn>;
    /// Insert or replace — used to replay a committed transaction's writes.
    fn upsert(&self, table: &str, row: Row) -> Result<Csn>;
    fn update(&self, table: &str, row: Row) -> Result<Csn>;
    fn delete(&self, table: &str, key: &Value) -> Result<Csn>;

    /// Bulk-load rows whose keys are known to be new (the `COPY` fast path):
    /// skips the per-row duplicate probe. The default is a correct row-by-row
    /// fallback; `Database` and `Storage` override it with their bulk paths.
    /// Callers must validate/coerce rows (types, constraints) first.
    fn bulk_insert(&self, table: &str, rows: Vec<Row>) -> Result<usize> {
        let n = rows.len();
        for row in rows {
            self.insert(table, row)?;
        }
        Ok(n)
    }

    /// Apply a committed transaction's writes. A durable backend logs the whole
    /// batch as **one** WAL record, so it is crash-atomic (all-or-nothing).
    fn commit_batch(&self, writes: Vec<TxnWrite>) -> Result<()> {
        for w in writes {
            match w {
                TxnWrite::Put(table, row) => {
                    self.upsert(&table, row)?;
                }
                TxnWrite::Delete(table, key) => {
                    let _ = self.delete(&table, &key);
                }
            }
        }
        Ok(())
    }
}

/// A single write in a committed transaction's change-set.
#[derive(Debug, Clone)]
pub enum TxnWrite {
    Put(String, Row),
    Delete(String, Value),
}

impl SqlBackend for Database {
    fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        self.create_table_schema(name, schema).map(|_| ())
    }
    fn table(&self, name: &str) -> Result<Arc<Table>> {
        Database::table(self, name)
    }
    fn table_names(&self) -> Vec<String> {
        Database::table_names(self)
    }
    fn snapshot(&self) -> Snapshot {
        Database::snapshot(self)
    }
    fn pin(&self) -> SnapshotPin {
        Database::pin(self)
    }
    fn insert(&self, table: &str, row: Row) -> Result<Csn> {
        Database::table(self, table)?.insert(row)
    }
    fn upsert(&self, table: &str, row: Row) -> Result<Csn> {
        Database::table(self, table)?.upsert(row)
    }
    fn update(&self, table: &str, row: Row) -> Result<Csn> {
        Database::table(self, table)?.update(row)
    }
    fn delete(&self, table: &str, key: &Value) -> Result<Csn> {
        Database::table(self, table)?.delete(key)
    }
    fn bulk_insert(&self, table: &str, rows: Vec<Row>) -> Result<usize> {
        let n = rows.len();
        Database::table(self, table)?.bulk_load(rows);
        Ok(n)
    }
}

impl SqlBackend for Storage {
    fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        self.create_table_schema(name, schema)
    }
    fn table(&self, name: &str) -> Result<Arc<Table>> {
        // A read may touch lazily-opened parts; warm before handing back the
        // table so its part list is populated.
        self.warm();
        self.database().table(name)
    }
    fn table_names(&self) -> Vec<String> {
        self.warm();
        self.database().table_names()
    }
    fn snapshot(&self) -> Snapshot {
        self.database().snapshot()
    }
    fn pin(&self) -> SnapshotPin {
        self.warm();
        self.database().pin()
    }
    fn insert(&self, table: &str, row: Row) -> Result<Csn> {
        Storage::insert(self, table, row)
    }
    fn upsert(&self, table: &str, row: Row) -> Result<Csn> {
        Storage::upsert(self, table, row)
    }
    fn update(&self, table: &str, row: Row) -> Result<Csn> {
        Storage::update(self, table, row)
    }
    fn delete(&self, table: &str, key: &Value) -> Result<Csn> {
        Storage::delete(self, table, key)
    }
    fn commit_batch(&self, writes: Vec<TxnWrite>) -> Result<()> {
        Storage::commit_transaction(self, writes).map(|_| ())
    }
    fn bulk_insert(&self, table: &str, rows: Vec<Row>) -> Result<usize> {
        let n = rows.len();
        // Durable batch: WAL-logged, one flush for the whole chunk.
        Storage::load_batch(self, table, rows)?;
        Ok(n)
    }
}
