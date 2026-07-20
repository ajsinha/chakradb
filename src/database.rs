//! The multi-table catalog.
//!
//! ChakraDB holds many primary-keyed tables. They share **one CSN generator**,
//! so a snapshot is consistent across every table — a scan of `orders` and a
//! scan of `customers` at the same snapshot observe the same instant.
//!
//! Foreign keys are an explicit non-goal (see `requirements.md` §2.1). Primary
//! key indexing is the mechanism this engine is built around (§5.2); referential
//! integrity between tables is left to the application.

use crate::csn::{Csn, CsnGenerator, Snapshot};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::table::{Table, TableConfig, TableStats};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// A collection of tables under one snapshot clock.
#[derive(Debug)]
pub struct Database {
    tables: RwLock<BTreeMap<String, Arc<Table>>>,
    csn: Arc<CsnGenerator>,
    metrics: Arc<Metrics>,
    default_config: TableConfig,
}

impl Database {
    pub fn new() -> Self {
        Self::with_config(TableConfig::default())
    }

    pub fn with_config(default_config: TableConfig) -> Self {
        Database {
            tables: RwLock::new(BTreeMap::new()),
            csn: Arc::new(CsnGenerator::new()),
            metrics: Arc::new(Metrics::new()),
            default_config,
        }
    }

    /// Create a table. Fails if the name is taken.
    pub fn create_table(&self, name: &str) -> Result<Arc<Table>> {
        self.create_table_with(name, self.default_config.clone())
    }

    pub fn create_table_with(&self, name: &str, config: TableConfig) -> Result<Arc<Table>> {
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(name) {
            return Err(Error::TableExists(name.to_string()));
        }
        let t = Arc::new(Table::new(
            name,
            self.csn.clone(),
            self.metrics.clone(),
            config,
        ));
        tables.insert(name.to_string(), t.clone());
        Ok(t)
    }

    /// Fetch a table by name.
    pub fn table(&self, name: &str) -> Result<Arc<Table>> {
        self.tables
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    /// Create the table if absent, otherwise return the existing one.
    pub fn table_or_create(&self, name: &str) -> Result<Arc<Table>> {
        match self.table(name) {
            Ok(t) => Ok(t),
            Err(_) => self.create_table(name),
        }
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        self.tables
            .write()
            .unwrap()
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap().keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.tables.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Raise the CSN floor so recovery never reissues a replayed stamp.
    pub fn set_csn_floor(&self, csn: Csn) {
        self.csn.set_floor(csn);
    }

    /// A snapshot consistent across every table.
    pub fn snapshot(&self) -> Snapshot {
        self.csn.snapshot()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Seal every table's write buffer.
    pub fn seal_all(&self) {
        for t in self.tables.read().unwrap().values() {
            t.seal();
        }
    }

    /// Run compaction across every table. Returns total parts merged.
    pub fn compact_all(&self, horizon: Csn) -> usize {
        self.tables
            .read()
            .unwrap()
            .values()
            .map(|t| t.maybe_compact(horizon))
            .sum()
    }

    pub fn stats(&self) -> Vec<TableStats> {
        self.tables
            .read()
            .unwrap()
            .values()
            .map(|t| t.stats())
            .collect()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Row;

    fn row(pk: i64) -> Row {
        Row::new(pk, pk, pk as f64, format!("v{pk}"))
    }

    #[test]
    fn new_database_is_empty() {
        let db = Database::new();
        assert!(db.is_empty());
        assert_eq!(db.table_names(), Vec::<String>::new());
    }

    #[test]
    fn create_and_fetch_table() {
        let db = Database::new();
        let t = db.create_table("users").unwrap();
        assert_eq!(t.name(), "users");
        assert_eq!(db.table("users").unwrap().name(), "users");
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn duplicate_table_is_rejected() {
        let db = Database::new();
        db.create_table("t").unwrap();
        assert!(matches!(db.create_table("t"), Err(Error::TableExists(_))));
    }

    #[test]
    fn missing_table_is_an_error() {
        let db = Database::new();
        assert!(matches!(db.table("nope"), Err(Error::TableNotFound(_))));
        assert!(matches!(db.drop_table("nope"), Err(Error::TableNotFound(_))));
    }

    #[test]
    fn table_or_create_is_idempotent() {
        let db = Database::new();
        let a = db.table_or_create("t").unwrap();
        let b = db.table_or_create("t").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn drop_removes_table() {
        let db = Database::new();
        db.create_table("t").unwrap();
        db.drop_table("t").unwrap();
        assert!(db.is_empty());
    }

    #[test]
    fn table_names_are_sorted() {
        let db = Database::new();
        for n in ["orders", "customers", "items"] {
            db.create_table(n).unwrap();
        }
        assert_eq!(db.table_names(), vec!["customers", "items", "orders"]);
    }

    #[test]
    fn tables_have_independent_key_spaces() {
        let db = Database::new();
        let a = db.create_table("a").unwrap();
        let b = db.create_table("b").unwrap();
        a.insert(Row::new(1, 100, 0.0, "in-a")).unwrap();
        b.insert(Row::new(1, 200, 0.0, "in-b")).unwrap();
        assert_eq!(a.get_latest(1).unwrap().c, "in-a");
        assert_eq!(b.get_latest(1).unwrap().c, "in-b");
    }

    #[test]
    fn snapshot_is_consistent_across_tables() {
        let db = Database::new();
        let a = db.create_table("a").unwrap();
        let b = db.create_table("b").unwrap();
        a.insert(row(1)).unwrap();
        b.insert(row(1)).unwrap();

        let snap = db.snapshot();
        // Writes after the snapshot must be invisible in *both* tables.
        a.insert(row(2)).unwrap();
        b.insert(row(2)).unwrap();

        assert_eq!(a.row_count(snap), 1);
        assert_eq!(b.row_count(snap), 1);
        assert_eq!(a.row_count(db.snapshot()), 2);
        assert_eq!(b.row_count(db.snapshot()), 2);
    }

    #[test]
    fn csn_ordering_is_global() {
        let db = Database::new();
        let a = db.create_table("a").unwrap();
        let b = db.create_table("b").unwrap();
        let c1 = a.insert(row(1)).unwrap();
        let c2 = b.insert(row(1)).unwrap();
        let c3 = a.insert(row(2)).unwrap();
        assert!(c1 < c2 && c2 < c3, "CSNs must be globally ordered");
    }

    #[test]
    fn seal_all_seals_every_table() {
        let db = Database::new();
        for n in ["a", "b"] {
            let t = db.create_table(n).unwrap();
            t.insert(row(1)).unwrap();
        }
        db.seal_all();
        for s in db.stats() {
            assert_eq!(s.l0_rows, 0, "{} not sealed", s.name);
            assert_eq!(s.num_parts, 1);
        }
    }

    #[test]
    fn compact_all_runs_across_tables() {
        let cfg = TableConfig {
            seal_threshold: 2,
            ..Default::default()
        };
        let db = Database::with_config(cfg);
        for n in ["a", "b"] {
            let t = db.create_table(n).unwrap();
            for pk in 0..20 {
                t.insert(row(pk)).unwrap();
            }
        }
        let merged = db.compact_all(db.snapshot().csn);
        assert!(merged > 0, "expected compaction across tables");
    }

    #[test]
    fn stats_cover_all_tables() {
        let db = Database::new();
        db.create_table("a").unwrap().insert(row(1)).unwrap();
        db.create_table("b").unwrap().insert(row(1)).unwrap();
        let stats = db.stats();
        assert_eq!(stats.len(), 2);
        assert!(stats.iter().all(|s| s.total_rows() == 1));
    }

    #[test]
    fn metrics_are_shared_across_tables() {
        let db = Database::new();
        db.create_table("a").unwrap().insert(row(1)).unwrap();
        db.create_table("b").unwrap().insert(row(1)).unwrap();
        assert_eq!(Metrics::get(&db.metrics().inserts), 2);
    }
}
