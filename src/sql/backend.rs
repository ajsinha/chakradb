//! The target a SQL statement executes against.
//!
//! ChakraDB has two catalogs: the in-memory [`Database`] (fast, no durability)
//! and [`Storage`] (WAL-logged, crash-safe). Historically SQL ran only on the
//! former, so SQL writes were never durable. This trait unifies them: the
//! executor writes through a `SqlBackend`, and `SqlEngine` can be bound to either
//! — an in-memory database for tests, or `Storage` for a durable SQL database.

use crate::csn::{Csn, Snapshot};
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
    fn insert(&self, table: &str, row: Row) -> Result<Csn>;
    /// Insert or replace — used to replay a committed transaction's writes.
    fn upsert(&self, table: &str, row: Row) -> Result<Csn>;
    fn update(&self, table: &str, row: Row) -> Result<Csn>;
    fn delete(&self, table: &str, key: &Value) -> Result<Csn>;
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
}
