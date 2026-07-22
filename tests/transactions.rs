//! Basic transactions: BEGIN / COMMIT / ROLLBACK.

use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::{Database, SqlEngine};
use std::sync::Arc;

fn eng(db: &Arc<Database>) -> SqlEngine {
    SqlEngine::new(db.clone())
}
fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}

#[test]
fn commit_persists() {
    let db = Arc::new(Database::new());
    let e = eng(&db);
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    e.run("BEGIN").unwrap();
    assert!(e.in_transaction());
    e.run("INSERT INTO t VALUES (1, 10)").unwrap();
    e.run("INSERT INTO t VALUES (2, 20)").unwrap();
    // Read-your-writes inside the transaction.
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "2");
    e.run("COMMIT").unwrap();
    assert!(!e.in_transaction());
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "2");
    assert_eq!(one(&e, "SELECT v FROM t WHERE id = 2"), "20");
}

#[test]
fn rollback_discards() {
    let db = Arc::new(Database::new());
    let e = eng(&db);
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    e.run("INSERT INTO t VALUES (1, 10)").unwrap();
    e.run("INSERT INTO t VALUES (2, 20)").unwrap();

    e.run("BEGIN").unwrap();
    e.run("INSERT INTO t VALUES (3, 30)").unwrap();
    e.run("DELETE FROM t WHERE id = 1").unwrap();
    // Inside the txn: -1 (deleted) +1 (inserted) = still 2.
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "2");
    e.run("ROLLBACK").unwrap();

    // Back to the committed state: {1, 2}.
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "2");
    assert!(e.query("SELECT v FROM t WHERE id = 3").unwrap().is_empty());
    assert_eq!(one(&e, "SELECT v FROM t WHERE id = 1"), "10");
}

#[test]
fn other_connection_does_not_see_uncommitted() {
    let db = Arc::new(Database::new());
    let a = eng(&db);
    let b = eng(&db);
    a.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();

    a.run("BEGIN").unwrap();
    a.run("INSERT INTO t VALUES (1, 10)").unwrap();
    // The other connection sees nothing uncommitted.
    assert_eq!(one(&b, "SELECT COUNT(*) FROM t"), "0");
    a.run("COMMIT").unwrap();
    assert_eq!(one(&b, "SELECT COUNT(*) FROM t"), "1");
}

#[test]
fn read_modify_write_within_txn() {
    let db = Arc::new(Database::new());
    let e = eng(&db);
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    e.run("INSERT INTO t VALUES (1, 5)").unwrap();
    e.run("BEGIN").unwrap();
    // The second increment must see the first (read-your-writes).
    e.run("UPDATE t SET v = v + 1 WHERE id = 1").unwrap();
    e.run("UPDATE t SET v = v + 1 WHERE id = 1").unwrap();
    assert_eq!(one(&e, "SELECT v FROM t WHERE id = 1"), "7");
    e.run("COMMIT").unwrap();
    assert_eq!(one(&e, "SELECT v FROM t WHERE id = 1"), "7");
}

#[test]
fn begin_and_commit_errors() {
    let db = Arc::new(Database::new());
    let e = eng(&db);
    e.run("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    assert!(e.run("COMMIT").is_err(), "commit with no transaction");
    e.run("BEGIN").unwrap();
    assert!(e.run("BEGIN").is_err(), "nested begin");
    e.run("ROLLBACK").unwrap();
}

#[test]
fn durable_transaction_is_crash_atomic() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let e = SqlEngine::durable(Arc::new(
            Storage::open(io.clone(), StorageConfig::default()).unwrap(),
        ));
        e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        // A committed transaction.
        e.run("BEGIN").unwrap();
        e.run("INSERT INTO t VALUES (1, 10)").unwrap();
        e.run("INSERT INTO t VALUES (2, 20)").unwrap();
        e.run("COMMIT").unwrap();
        // An uncommitted transaction, then a "crash" (drop without commit).
        e.run("BEGIN").unwrap();
        e.run("INSERT INTO t VALUES (3, 30)").unwrap();
    }
    // Reopen: only the committed transaction survived — the uncommitted writes
    // never reached the WAL.
    let e2 = SqlEngine::durable(Arc::new(
        Storage::open(io, StorageConfig::default()).unwrap(),
    ));
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM t"), "2");
    assert!(e2.query("SELECT v FROM t WHERE id = 3").unwrap().is_empty());
    assert_eq!(one(&e2, "SELECT v FROM t WHERE id = 1"), "10");
}
