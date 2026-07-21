//! Arbitrary-schema SQL — the "be more like DuckDB" capability.
//!
//! These exercise what the fixed `(pk, a, b, c)` engine could never do: tables
//! with any columns and types, a primary key of any type, and PK-less tables
//! backed by a hidden rowid. All through the SQL front door.

use chakradb::{Database, SqlEngine};
use std::sync::Arc;

fn engine() -> SqlEngine {
    SqlEngine::new(Arc::new(Database::new()))
}

fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}

#[test]
fn arbitrary_columns_and_types() {
    let e = engine();
    e.run("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price FLOAT, qty INT)")
        .unwrap();
    e.run("INSERT INTO items VALUES (1, 'apple', 0.50, 100)")
        .unwrap();
    e.run("INSERT INTO items VALUES (2, 'pear', 0.75, 40)")
        .unwrap();
    e.run("INSERT INTO items (id, name, price, qty) VALUES (3, 'kiwi', 1.25, 10)")
        .unwrap();

    assert_eq!(one(&e, "SELECT COUNT(*) FROM items"), "3");
    assert_eq!(one(&e, "SELECT name FROM items WHERE id = 2"), "pear");
    // Aggregate over a user-named float column.
    assert_eq!(
        one(&e, "SELECT SUM(qty) FROM items WHERE price < 1.0"),
        "140"
    );
    // Column resolution by declared name, any order.
    let rows = e
        .query("SELECT name, qty FROM items ORDER BY price DESC LIMIT 1")
        .unwrap();
    assert_eq!(rows[0], vec!["kiwi".to_string(), "10".to_string()]);
}

#[test]
fn text_primary_key() {
    let e = engine();
    e.run("CREATE TABLE users (email TEXT PRIMARY KEY, age INT)")
        .unwrap();
    e.run("INSERT INTO users VALUES ('carol@x.com', 30)")
        .unwrap();
    e.run("INSERT INTO users VALUES ('alice@x.com', 25)")
        .unwrap();
    e.run("INSERT INTO users VALUES ('bob@x.com', 41)").unwrap();

    // Duplicate text key is rejected.
    assert!(e
        .run("INSERT INTO users VALUES ('alice@x.com', 99)")
        .is_err());

    // Lookup by text key.
    assert_eq!(
        one(&e, "SELECT age FROM users WHERE email = 'bob@x.com'"),
        "41"
    );
    // Text keys order as keys (parts are sorted by the text column).
    let names = e.query("SELECT email FROM users ORDER BY email").unwrap();
    assert_eq!(names[0][0], "alice@x.com");
    assert_eq!(names[2][0], "carol@x.com");
    assert_eq!(one(&e, "SELECT COUNT(*) FROM users"), "3");
}

#[test]
fn pk_less_table_uses_hidden_rowid() {
    let e = engine();
    // No PRIMARY KEY -> a synthesised _rowid keys the table.
    e.run("CREATE TABLE log (msg TEXT, level INT)").unwrap();
    e.run("INSERT INTO log VALUES ('boot', 1)").unwrap();
    e.run("INSERT INTO log VALUES ('warn', 2)").unwrap();
    e.run("INSERT INTO log VALUES ('warn', 2)").unwrap(); // duplicate rows allowed

    assert_eq!(one(&e, "SELECT COUNT(*) FROM log"), "3");
    // SELECT * exposes only the user columns, not the hidden rowid.
    let rows = e.query("SELECT * FROM log").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].len(), 2, "rowid must stay hidden from SELECT *");
    assert_eq!(one(&e, "SELECT COUNT(*) FROM log WHERE level = 2"), "2");
}

#[test]
fn wrong_arity_and_unknown_column_are_errors() {
    let e = engine();
    e.run("CREATE TABLE t2 (k INT PRIMARY KEY, v TEXT)")
        .unwrap();
    // Too many values for a 2-column table.
    assert!(e.run("INSERT INTO t2 VALUES (1, 'a', 99)").is_err());
    // Unknown column name.
    assert!(e.query("SELECT nope FROM t2").is_err());
    // A type that does not fit the declared column.
    assert!(e.run("INSERT INTO t2 VALUES ('notint', 'a')").is_err());
}

#[test]
fn group_by_user_named_column() {
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
