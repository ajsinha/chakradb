//! Exact DECIMAL(p, s): stored as an i128 mantissa (Arrow Decimal128), never
//! f64 — so money round-trips, compares, and aggregates exactly.

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
fn stores_and_renders_exactly() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, price DECIMAL(10,2))")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 9.99)").unwrap();
    e.run("INSERT INTO t VALUES (2, 1234.50)").unwrap();
    e.run("INSERT INTO t VALUES (3, -0.01)").unwrap();
    assert_eq!(one(&e, "SELECT price FROM t WHERE id = 1"), "9.99");
    assert_eq!(one(&e, "SELECT price FROM t WHERE id = 2"), "1234.50");
    assert_eq!(one(&e, "SELECT price FROM t WHERE id = 3"), "-0.01");
}

#[test]
fn value_famous_for_breaking_floats() {
    // 0.1 + 0.2 != 0.3 in f64. As stored DECIMAL(2,1) values they are exact.
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, x DECIMAL(2,1))")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 0.1)").unwrap();
    e.run("INSERT INTO t VALUES (2, 0.2)").unwrap();
    // Exact stored values, and an exact SUM (DataFusion aggregates Decimal128).
    assert_eq!(one(&e, "SELECT x FROM t WHERE id = 1"), "0.1");
    assert_eq!(one(&e, "SELECT SUM(x) FROM t"), "0.3");
}

#[test]
fn integer_widens_and_rescales() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, price DECIMAL(10,2))")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 5)").unwrap(); // integer into decimal
    e.run("INSERT INTO t VALUES (2, 3.5)").unwrap(); // fewer scale digits
    assert_eq!(one(&e, "SELECT price FROM t WHERE id = 1"), "5.00");
    assert_eq!(one(&e, "SELECT price FROM t WHERE id = 2"), "3.50");
}

#[test]
fn comparison_and_range_are_exact() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, price DECIMAL(10,2))")
        .unwrap();
    for (i, p) in [(1, "9.99"), (2, "10.00"), (3, "10.01")] {
        e.run(&format!("INSERT INTO t VALUES ({i}, {p})")).unwrap();
    }
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t WHERE price >= 10.00"), "2");
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t WHERE price < 10.00"), "1");
    assert_eq!(one(&e, "SELECT id FROM t WHERE price = 9.99"), "1");
}

#[test]
fn sum_and_minmax_exact() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, amt DECIMAL(12,2))")
        .unwrap();
    for (i, a) in [(1, "100.10"), (2, "200.20"), (3, "0.03")] {
        e.run(&format!("INSERT INTO t VALUES ({i}, {a})")).unwrap();
    }
    assert_eq!(one(&e, "SELECT SUM(amt) FROM t"), "300.33");
    assert_eq!(one(&e, "SELECT MIN(amt) FROM t"), "0.03");
    assert_eq!(one(&e, "SELECT MAX(amt) FROM t"), "200.20");
}

#[test]
fn precision_overflow_is_rejected() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, price DECIMAL(5,2))")
        .unwrap();
    // DECIMAL(5,2) holds up to 999.99 — larger magnitudes must be rejected, not
    // silently stored with the wrong value.
    assert!(e.run("INSERT INTO t VALUES (1, 1000.00)").is_err());
    assert!(e.run("INSERT INTO t VALUES (2, 9999.99)").is_err());
    e.run("INSERT INTO t VALUES (3, 999.99)").unwrap(); // the maximum fits
    e.run("INSERT INTO t VALUES (4, -999.99)").unwrap();
    assert_eq!(one(&e, "SELECT price FROM t WHERE id = 3"), "999.99");
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "2");
}

#[test]
fn arithmetic_is_exact() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, price DECIMAL(10,2), qty INT)")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 9.99, 3)").unwrap();
    // price * qty and price + price stay exact (interpreter point-lookup path).
    assert_eq!(one(&e, "SELECT price + price FROM t WHERE id = 1"), "19.98");
    assert_eq!(one(&e, "SELECT price * qty FROM t WHERE id = 1"), "29.97");
    assert_eq!(one(&e, "SELECT price - 0.01 FROM t WHERE id = 1"), "9.98");
}

#[test]
fn decimal_as_primary_key() {
    let e = eng();
    e.run("CREATE TABLE t (k DECIMAL(6,3) PRIMARY KEY, v INT)")
        .unwrap();
    e.run("INSERT INTO t VALUES (1.500, 42)").unwrap();
    assert_eq!(one(&e, "SELECT v FROM t WHERE k = 1.500"), "42");
}

#[test]
fn survives_durable_reopen() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let e = SqlEngine::durable(Arc::new(
            Storage::open(io.clone(), StorageConfig::default()).unwrap(),
        ));
        e.run("CREATE TABLE t (id INT PRIMARY KEY, price DECIMAL(10,2))")
            .unwrap();
        e.run("INSERT INTO t VALUES (1, 9.99)").unwrap();
    }
    let e2 = SqlEngine::durable(Arc::new(
        Storage::open(io, StorageConfig::default()).unwrap(),
    ));
    assert_eq!(one(&e2, "SELECT price FROM t WHERE id = 1"), "9.99");
    assert_eq!(one(&e2, "SELECT SUM(price) FROM t"), "9.99");
}
