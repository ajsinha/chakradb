//! Column and table constraints: NOT NULL, DEFAULT, CHECK.
//!
//! These are enforced in the SQL layer at write time — INSERT at plan time
//! (values are literals), UPDATE before any row is applied (statement-atomic).

use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::{Database, SqlEngine};
use std::sync::Arc;

fn eng() -> SqlEngine {
    SqlEngine::new(Arc::new(Database::new()))
}
fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}

#[test]
fn not_null_rejects_null_insert() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    assert!(e.run("INSERT INTO t VALUES (1, NULL)").is_err());
    assert!(e.run("INSERT INTO t (id) VALUES (2)").is_err(), "omitted NOT NULL");
    e.run("INSERT INTO t VALUES (3, 'ok')").unwrap();
    assert_eq!(one(&e, "SELECT name FROM t WHERE id = 3"), "ok");
}

#[test]
fn primary_key_is_implicitly_not_null() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    assert!(
        e.run("INSERT INTO t (v) VALUES (10)").is_err(),
        "a PRIMARY KEY may not be NULL"
    );
}

#[test]
fn default_fills_omitted_column() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, status TEXT DEFAULT 'new', n INT DEFAULT 0)")
        .unwrap();
    e.run("INSERT INTO t (id) VALUES (1)").unwrap();
    assert_eq!(one(&e, "SELECT status FROM t WHERE id = 1"), "new");
    assert_eq!(one(&e, "SELECT n FROM t WHERE id = 1"), "0");
    // An explicit value overrides the default.
    e.run("INSERT INTO t VALUES (2, 'live', 5)").unwrap();
    assert_eq!(one(&e, "SELECT status FROM t WHERE id = 2"), "live");
}

#[test]
fn explicit_null_is_not_replaced_by_default() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, status TEXT DEFAULT 'new')")
        .unwrap();
    // Providing NULL explicitly keeps NULL — the default only fills omitted cols.
    e.run("INSERT INTO t VALUES (1, NULL)").unwrap();
    assert!(e.query("SELECT status FROM t WHERE id = 1 AND status IS NULL").unwrap().len() == 1);
}

#[test]
fn default_plus_not_null_together() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, n INT NOT NULL DEFAULT 7)")
        .unwrap();
    e.run("INSERT INTO t (id) VALUES (1)").unwrap(); // default satisfies NOT NULL
    assert_eq!(one(&e, "SELECT n FROM t WHERE id = 1"), "7");
    assert!(e.run("INSERT INTO t VALUES (2, NULL)").is_err()); // explicit NULL fails
}

#[test]
fn check_rejects_violating_insert() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))")
        .unwrap();
    assert!(e.run("INSERT INTO t VALUES (1, -5)").is_err());
    e.run("INSERT INTO t VALUES (2, 30)").unwrap();
    assert_eq!(one(&e, "SELECT age FROM t WHERE id = 2"), "30");
}

#[test]
fn table_level_check_across_columns() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, lo INT, hi INT, CHECK (lo <= hi))")
        .unwrap();
    assert!(e.run("INSERT INTO t VALUES (1, 10, 5)").is_err());
    e.run("INSERT INTO t VALUES (2, 5, 10)").unwrap();
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "1");
}

#[test]
fn check_passes_on_null_operand() {
    // SQL: a CHECK fails only on a definite FALSE; NULL/UNKNOWN passes.
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, NULL)").unwrap(); // age NULL → CHECK unknown → allowed
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "1");
}

#[test]
fn check_enforced_on_update() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 10)").unwrap();
    assert!(e.run("UPDATE t SET age = -1 WHERE id = 1").is_err());
    // The failed UPDATE left the row unchanged.
    assert_eq!(one(&e, "SELECT age FROM t WHERE id = 1"), "10");
}

#[test]
fn not_null_enforced_on_update() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 'a')").unwrap();
    assert!(e.run("UPDATE t SET name = NULL WHERE id = 1").is_err());
    assert_eq!(one(&e, "SELECT name FROM t WHERE id = 1"), "a");
}

#[test]
fn constraints_survive_a_reopen() {
    // Constraints live in the persisted schema, so they must still be enforced
    // after the durable store is closed and reopened.
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let e = SqlEngine::durable(Arc::new(
            Storage::open(io.clone(), StorageConfig::default()).unwrap(),
        ));
        e.run("CREATE TABLE t (id INT PRIMARY KEY, age INT NOT NULL CHECK (age >= 0))")
            .unwrap();
        e.run("INSERT INTO t VALUES (1, 10)").unwrap();
    }
    let e2 = SqlEngine::durable(Arc::new(
        Storage::open(io, StorageConfig::default()).unwrap(),
    ));
    assert_eq!(one(&e2, "SELECT age FROM t WHERE id = 1"), "10");
    assert!(e2.run("INSERT INTO t VALUES (2, -1)").is_err(), "CHECK survives");
    assert!(e2.run("INSERT INTO t VALUES (3, NULL)").is_err(), "NOT NULL survives");
    e2.run("INSERT INTO t VALUES (4, 40)").unwrap();
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM t"), "2");
}

#[test]
fn update_is_statement_atomic_on_violation() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 10)").unwrap();
    e.run("INSERT INTO t VALUES (2, 20)").unwrap();
    // Setting every row's age to age-15 makes row 1 (10-15=-5) violate; row 2
    // (20-15=5) would pass. The whole statement must abort with NO row changed.
    assert!(e.run("UPDATE t SET age = age - 15").is_err());
    assert_eq!(one(&e, "SELECT age FROM t WHERE id = 1"), "10");
    assert_eq!(one(&e, "SELECT age FROM t WHERE id = 2"), "20");
}
