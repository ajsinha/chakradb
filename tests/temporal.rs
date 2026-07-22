//! DATE and TIMESTAMP: logical temporal types over an integer physical column
//! (epoch days / microseconds), exposed to Arrow as Date32 / Timestamp so
//! DataFusion does real date work, and rendered back as date strings.

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
fn insert_and_read_back_date() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, d DATE)").unwrap();
    e.run("INSERT INTO t VALUES (1, '2024-01-15')").unwrap();
    e.run("INSERT INTO t VALUES (2, DATE '1999-12-31')").unwrap();
    // Point lookup (interpreter) renders the epoch integer back as a date.
    assert_eq!(one(&e, "SELECT d FROM t WHERE id = 1"), "2024-01-15");
    assert_eq!(one(&e, "SELECT d FROM t WHERE id = 2"), "1999-12-31");
}

#[test]
fn timestamp_round_trip() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, '2024-01-15 13:45:06')")
        .unwrap();
    assert_eq!(one(&e, "SELECT ts FROM t WHERE id = 1"), "2024-01-15 13:45:06");
}

#[test]
fn date_range_filter_selects_correctly() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, d DATE)").unwrap();
    for (i, d) in [(1, "2023-06-01"), (2, "2024-01-01"), (3, "2024-07-01")] {
        e.run(&format!("INSERT INTO t VALUES ({i}, '{d}')")).unwrap();
    }
    // The string literal is coerced to the DATE column's epoch integer, so the
    // comparison orders correctly (not lexically-as-string).
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t WHERE d >= '2024-01-01'"), "2");
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t WHERE d < '2024-01-01'"), "1");
}

#[test]
fn min_max_of_date_renders_as_date() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, d DATE)").unwrap();
    for (i, d) in [(1, "2024-03-05"), (2, "2020-01-01"), (3, "2025-12-31")] {
        e.run(&format!("INSERT INTO t VALUES ({i}, '{d}')")).unwrap();
    }
    // MIN/MAX go through the zonemap metadata path, then render as dates.
    assert_eq!(one(&e, "SELECT MIN(d) FROM t"), "2020-01-01");
    assert_eq!(one(&e, "SELECT MAX(d) FROM t"), "2025-12-31");
}

#[test]
fn order_by_date() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, d DATE)").unwrap();
    for (i, d) in [(1, "2024-03-05"), (2, "2020-01-01"), (3, "2025-12-31")] {
        e.run(&format!("INSERT INTO t VALUES ({i}, '{d}')")).unwrap();
    }
    let got = e.query("SELECT d FROM t ORDER BY d").unwrap();
    let dates: Vec<String> = got.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(dates, vec!["2020-01-01", "2024-03-05", "2025-12-31"]);
}

#[test]
fn date_survives_durable_reopen() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let e = SqlEngine::durable(Arc::new(
            Storage::open(io.clone(), StorageConfig::default()).unwrap(),
        ));
        e.run("CREATE TABLE t (id INT PRIMARY KEY, d DATE)").unwrap();
        e.run("INSERT INTO t VALUES (1, '2024-01-15')").unwrap();
    }
    let e2 = SqlEngine::durable(Arc::new(
        Storage::open(io, StorageConfig::default()).unwrap(),
    ));
    assert_eq!(one(&e2, "SELECT d FROM t WHERE id = 1"), "2024-01-15");
}

#[test]
fn hostile_date_literal_errors_not_panics() {
    // An out-of-range year must be rejected cleanly — never overflow (a debug
    // panic, or a silent wrap to a garbage epoch in release).
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, d DATE, ts TIMESTAMP)")
        .unwrap();
    assert!(e.run("INSERT INTO t VALUES (1, '300000-01-01', NULL)").is_err());
    assert!(e
        .run("INSERT INTO t VALUES (2, NULL, '300000-01-01 00:00:00')")
        .is_err());
    // Also via a WHERE comparison literal and a typed literal.
    e.run("INSERT INTO t VALUES (3, '2024-01-01', '2024-01-01 00:00:00')")
        .unwrap();
    assert!(e
        .run("SELECT * FROM t WHERE ts > TIMESTAMP '300000-01-01 00:00:00'")
        .is_err());
    // A sane far-future date still works.
    e.run("INSERT INTO t VALUES (4, '9999-12-31', NULL)").unwrap();
    assert_eq!(one(&e, "SELECT d FROM t WHERE id = 4"), "9999-12-31");
}

#[test]
fn date_as_primary_key() {
    let e = eng();
    e.run("CREATE TABLE t (d DATE PRIMARY KEY, v INT)").unwrap();
    e.run("INSERT INTO t VALUES ('2024-01-15', 100)").unwrap();
    // Point lookup on a DATE key: the literal coerces to the key's epoch integer.
    assert_eq!(one(&e, "SELECT v FROM t WHERE d = '2024-01-15'"), "100");
}
