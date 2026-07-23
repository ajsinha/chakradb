//! DROP TABLE and TRUNCATE — including durability across a reopen (old WAL
//! records must not resurrect dropped/truncated data).

use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::{Database, SqlEngine};
use std::sync::Arc;

fn mem() -> SqlEngine {
    SqlEngine::new(Arc::new(Database::new()))
}
fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}

#[test]
fn drop_table_removes_it() {
    let e = mem();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    e.run("INSERT INTO t VALUES (1, 10)").unwrap();
    e.run("DROP TABLE t").unwrap();
    assert!(e.run("SELECT * FROM t").is_err(), "table is gone");
    // The name is free again.
    e.run("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "0");
}

#[test]
fn drop_missing_table_errors() {
    let e = mem();
    assert!(e.run("DROP TABLE nope").is_err());
}

#[test]
fn truncate_empties_but_keeps_schema() {
    let e = mem();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 {
        e.run(&format!("INSERT INTO t VALUES ({i}, {})", i * 10)).unwrap();
    }
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "5");
    e.run("TRUNCATE t").unwrap();
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "0");
    // Same schema — and the keys are free again (no duplicate-key from old rows).
    e.run("INSERT INTO t VALUES (1, 999)").unwrap();
    assert_eq!(one(&e, "SELECT v FROM t WHERE id = 1"), "999");
}

#[test]
fn delete_still_works() {
    let e = mem();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    e.run("INSERT INTO t VALUES (1, 10)").unwrap();
    e.run("INSERT INTO t VALUES (2, 20)").unwrap();
    e.run("DELETE FROM t WHERE id = 1").unwrap();
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "1");
    assert_eq!(one(&e, "SELECT v FROM t WHERE id = 2"), "20");
}

#[test]
fn drop_table_is_durable() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = Arc::new(Storage::open(io.clone(), StorageConfig::default()).unwrap());
        let e = SqlEngine::durable(s.clone());
        e.run("CREATE TABLE keep (id INT PRIMARY KEY)").unwrap();
        e.run("CREATE TABLE gone (id INT PRIMARY KEY, v INT)").unwrap();
        for i in 1..=10 {
            e.run(&format!("INSERT INTO gone VALUES ({i}, {i})")).unwrap();
        }
        s.checkpoint().unwrap(); // gone's rows are now in parts + WAL
        e.run("INSERT INTO keep VALUES (1)").unwrap();
        e.run("DROP TABLE gone").unwrap();
    }
    // Reopen: the dropped table stays gone; its old WAL records are ignored
    // (the manifest no longer lists it).
    let e2 = SqlEngine::durable(Arc::new(
        Storage::open(io, StorageConfig::default()).unwrap(),
    ));
    assert!(e2.run("SELECT * FROM gone").is_err(), "dropped table not resurrected");
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM keep"), "1");
}

#[test]
fn truncate_is_durable_and_does_not_resurrect_rows() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = Arc::new(Storage::open(io.clone(), StorageConfig::default()).unwrap());
        let e = SqlEngine::durable(s.clone());
        e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        for i in 1..=20 {
            e.run(&format!("INSERT INTO t VALUES ({i}, {i})")).unwrap();
        }
        s.checkpoint().unwrap(); // rows persisted to parts + logged in the WAL
        e.run("TRUNCATE t").unwrap();
        // A fresh insert after truncate reuses an old key — must be fine.
        e.run("INSERT INTO t VALUES (1, 111)").unwrap();
    }
    // Reopen: only the post-truncate row survives; the 20 old rows (under the old
    // table id) are NOT replayed.
    let e2 = SqlEngine::durable(Arc::new(
        Storage::open(io, StorageConfig::default()).unwrap(),
    ));
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM t"), "1", "old rows not resurrected");
    assert_eq!(one(&e2, "SELECT v FROM t WHERE id = 1"), "111");
}
