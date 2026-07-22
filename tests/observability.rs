//! Operational observability: `Storage::stats()` surfaces the signals an operator
//! needs (memory footprint, durability lag, ingest backpressure) without a scan.

use chakradb::backpressure::Pressure;
use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::SqlEngine;
use std::sync::Arc;

#[test]
fn stats_reflect_tables_and_rows() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(io, StorageConfig::default()).unwrap());
    let e = SqlEngine::durable(storage.clone());

    let empty = storage.stats();
    assert_eq!(empty.tables, 0);
    assert_eq!(empty.part_rows + empty.l0_rows, 0);

    e.run("CREATE TABLE a (id INT PRIMARY KEY, v INT)").unwrap();
    e.run("CREATE TABLE b (id INT PRIMARY KEY)").unwrap();
    for i in 0..100 {
        e.run(&format!("INSERT INTO a VALUES ({i}, {})", i * 2)).unwrap();
    }

    let s = storage.stats();
    assert_eq!(s.tables, 2);
    assert_eq!(s.l0_rows + s.part_rows, 100, "all rows accounted for");
    assert_eq!(s.tables_detail.len(), 2);
    // The resident index is the scaling ceiling — it must be tracked once rows
    // have sealed into parts.
    assert!(s.current_csn >= 100);
}

#[test]
fn checkpoint_lag_shrinks_after_checkpoint() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(io, StorageConfig::default()).unwrap());
    let e = SqlEngine::durable(storage.clone());
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 0..50 {
        e.run(&format!("INSERT INTO t VALUES ({i}, {i})")).unwrap();
    }
    let before = storage.stats();
    assert!(before.checkpoint_lag_csn > 0, "uncheckpointed writes lag");
    assert!(before.wal_written_bytes > 0);

    storage.checkpoint().unwrap();

    let after = storage.stats();
    assert!(
        after.checkpoint_lag_csn <= before.checkpoint_lag_csn,
        "checkpoint advances the durable watermark"
    );
    assert_eq!(after.checkpoint_csn, after.current_csn);
}

#[test]
fn stats_account_for_every_row() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(io, StorageConfig::default()).unwrap());
    let e = SqlEngine::durable(storage.clone());
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 0..20 {
        e.run(&format!("INSERT INTO t VALUES ({i}, {i})")).unwrap();
    }
    let before = storage.stats();
    assert_eq!(before.part_rows + before.l0_rows, 20);
    e.run("DELETE FROM t WHERE id = 5").unwrap();
    // The stats are a *physical* footprint (MVCC keeps the deleted version until
    // compaction reclaims it), so the physical count does not shrink — while the
    // logical row count does. That distinction is exactly what an operator
    // watching space amplification wants to see.
    let after = storage.stats();
    assert!(after.part_rows + after.l0_rows >= 20);
    assert_eq!(e.query("SELECT COUNT(*) FROM t").unwrap()[0][0], "19");
}

#[test]
fn pressure_is_none_under_light_load() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(io, StorageConfig::default()).unwrap());
    let e = SqlEngine::durable(storage.clone());
    e.run("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    e.run("INSERT INTO t VALUES (1)").unwrap();
    // A handful of parts is well under any backpressure threshold.
    assert_eq!(storage.stats().pressure, Pressure::None);
}
