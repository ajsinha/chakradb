//! DataFusion-executed SQL (only built with `--features datafusion`).
//!
//! Proves that with the feature on, `SqlEngine` routes SELECTs through
//! DataFusion over an MVCC snapshot: correct results, plus joins / window
//! functions / subqueries the interpreter rejects. Writes still go through the
//! interpreter/backend, so the two cooperate.
#![cfg(feature = "datafusion")]

use chakradb::{Database, SqlEngine};
use std::sync::Arc;

fn engine() -> SqlEngine {
    SqlEngine::new(Arc::new(Database::new()))
}

fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}

#[test]
fn basic_aggregates_are_correct_via_datafusion() {
    let e = engine();
    e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
        .unwrap();
    for i in 1..=10 {
        e.run(&format!(
            "INSERT INTO t VALUES ({i}, {}, {i}.0, 'x')",
            i * 10
        ))
        .unwrap();
    }
    // SUM of an integer column is an integer (DuckDB/DataFusion convention).
    assert_eq!(one(&e, "SELECT SUM(a) FROM t"), "550");
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "10");
    assert_eq!(one(&e, "SELECT MIN(a) FROM t"), "10");
    assert_eq!(one(&e, "SELECT MAX(a) FROM t"), "100");
    // Filter + aggregate.
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t WHERE a > 50"), "5");
}

#[test]
fn group_by_and_order_by() {
    let e = engine();
    e.run("CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)")
        .unwrap();
    for (i, (r, a)) in [("west", 10), ("east", 20), ("west", 5), ("east", 7)]
        .iter()
        .enumerate()
    {
        e.run(&format!("INSERT INTO sales VALUES ({}, '{r}', {a})", i + 1))
            .unwrap();
    }
    let rows = e
        .query("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["east".to_string(), "27".to_string()]);
    assert_eq!(rows[1], vec!["west".to_string(), "15".to_string()]);
}

#[test]
fn join_across_two_tables() {
    let e = engine();
    e.run("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    e.run("CREATE TABLE orders (id INT PRIMARY KEY, cust INT, total INT)")
        .unwrap();
    e.run("INSERT INTO customers VALUES (1, 'alice')").unwrap();
    e.run("INSERT INTO customers VALUES (2, 'bob')").unwrap();
    e.run("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
    e.run("INSERT INTO orders VALUES (11, 1, 50)").unwrap();
    e.run("INSERT INTO orders VALUES (12, 2, 70)").unwrap();

    // A join the single-table interpreter cannot express.
    let rows = e
        .query(
            "SELECT c.name, SUM(o.total) FROM orders o \
             JOIN customers c ON o.cust = c.id \
             GROUP BY c.name ORDER BY c.name",
        )
        .unwrap();
    assert_eq!(rows[0], vec!["alice".to_string(), "150".to_string()]);
    assert_eq!(rows[1], vec!["bob".to_string(), "70".to_string()]);
}

#[test]
fn window_function() {
    let e = engine();
    e.run("CREATE TABLE t (pk INT PRIMARY KEY, grp INT, v INT)")
        .unwrap();
    for (i, (g, v)) in [(1, 5), (1, 9), (2, 3)].iter().enumerate() {
        e.run(&format!("INSERT INTO t VALUES ({}, {g}, {v})", i + 1))
            .unwrap();
    }
    // ROW_NUMBER() over a partition — rejected by the interpreter, runs here.
    let rows = e
        .query(
            "SELECT pk, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v DESC) rn \
             FROM t ORDER BY pk",
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    // In group 1, pk=2 (v=9) ranks 1, pk=1 (v=5) ranks 2.
    assert_eq!(rows[0], vec!["1".to_string(), "2".to_string()]);
    assert_eq!(rows[1], vec!["2".to_string(), "1".to_string()]);
    assert_eq!(rows[2], vec!["3".to_string(), "1".to_string()]);
}

#[test]
fn correlated_subquery() {
    let e = engine();
    e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT)").unwrap();
    for i in 1..=5 {
        e.run(&format!("INSERT INTO t VALUES ({i}, {})", i * 10))
            .unwrap();
    }
    // Rows above the average — a subquery the interpreter rejects.
    assert_eq!(
        one(
            &e,
            "SELECT COUNT(*) FROM t WHERE a > (SELECT AVG(a) FROM t)"
        ),
        "2"
    );
}

#[test]
fn writes_still_go_through_the_interpreter_and_are_visible() {
    let e = engine();
    e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
        .unwrap();
    e.run("INSERT INTO t VALUES (1, 1, 0, 'a')").unwrap();
    e.run("INSERT INTO t VALUES (2, 2, 0, 'b')").unwrap();
    e.run("DELETE FROM t WHERE pk = 1").unwrap();
    e.run("UPDATE t SET a = 99 WHERE pk = 2").unwrap();
    // The DataFusion read sees the interpreter's committed writes.
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "1");
    assert_eq!(one(&e, "SELECT a FROM t WHERE pk = 2"), "99");
}
