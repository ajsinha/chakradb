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

#[test]
fn torn_commit_record_is_all_or_nothing() {
    // A committed multi-row transaction is logged as one WAL record. Truncating
    // the log at any byte must leave the transaction either fully applied or
    // fully absent after recovery — never a partial commit.
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    {
        let e = SqlEngine::durable(Arc::new(
            Storage::open(io.clone(), StorageConfig::default()).unwrap(),
        ));
        e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        e.run("INSERT INTO t VALUES (0, 0)").unwrap(); // autocommit baseline
        e.run("BEGIN").unwrap();
        for i in 1..=20 {
            e.run(&format!("INSERT INTO t VALUES ({i}, {i})")).unwrap();
        }
        e.run("COMMIT").unwrap();
    }

    let full = {
        let f = io.open("wal.log").unwrap();
        let mut b = vec![0u8; f.len().unwrap() as usize];
        f.pread(0, &mut b).unwrap();
        b
    };
    let manifest = {
        let f = io.open("MANIFEST").unwrap();
        let mut b = vec![0u8; f.len().unwrap() as usize];
        f.pread(0, &mut b).unwrap();
        b
    };

    for cut in (8..full.len()).step_by(9) {
        let io2: Arc<MemIo> = Arc::new(MemIo::new());
        {
            let m = io2.open("MANIFEST").unwrap();
            m.pwrite(0, &manifest).unwrap();
            m.sync().unwrap();
            let w = io2.open("wal.log").unwrap();
            w.pwrite(0, &full[..cut]).unwrap();
            w.sync().unwrap();
        }
        let e = SqlEngine::durable(Arc::new(
            Storage::open(io2, StorageConfig::default()).unwrap(),
        ));
        let n: i64 = e.query("SELECT COUNT(*) FROM t").unwrap()[0][0]
            .parse()
            .unwrap();
        // 0 = truncated inside the baseline record; 1 = baseline only (the txn's
        // record was torn and discarded); 21 = baseline + the whole txn. The
        // transaction contributes 0 or all 20 rows — never a partial count.
        assert!(
            n == 0 || n == 1 || n == 21,
            "cut {cut}: partial transaction recovered ({n} rows)"
        );
    }
}

#[test]
fn concurrent_write_conflict_is_detected() {
    let db = Arc::new(Database::new());
    let a = eng(&db);
    let b = eng(&db);
    a.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    a.run("INSERT INTO t VALUES (1, 0)").unwrap();

    // Both transactions begin at the same committed state and write the same key.
    a.run("BEGIN").unwrap();
    b.run("BEGIN").unwrap();
    a.run("UPDATE t SET v = 1 WHERE id = 1").unwrap();
    b.run("UPDATE t SET v = 2 WHERE id = 1").unwrap();

    a.run("COMMIT").unwrap(); // first committer wins
    assert!(b.run("COMMIT").is_err(), "second commit must conflict");
    assert!(
        !b.in_transaction(),
        "the conflicting transaction is aborted"
    );
    assert_eq!(one(&a, "SELECT v FROM t WHERE id = 1"), "1");
}

#[test]
fn non_conflicting_transactions_both_commit() {
    let db = Arc::new(Database::new());
    let a = eng(&db);
    let b = eng(&db);
    a.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    a.run("INSERT INTO t VALUES (1, 0)").unwrap();
    a.run("INSERT INTO t VALUES (2, 0)").unwrap();

    a.run("BEGIN").unwrap();
    b.run("BEGIN").unwrap();
    a.run("UPDATE t SET v = 1 WHERE id = 1").unwrap(); // different keys
    b.run("UPDATE t SET v = 2 WHERE id = 2").unwrap();
    a.run("COMMIT").unwrap();
    b.run("COMMIT").unwrap(); // no conflict
    assert_eq!(one(&a, "SELECT v FROM t WHERE id = 1"), "1");
    assert_eq!(one(&a, "SELECT v FROM t WHERE id = 2"), "2");
}
