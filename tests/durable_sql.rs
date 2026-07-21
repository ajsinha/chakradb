//! Durable SQL — the SQL front door wired to the WAL-logged `Storage` layer.
//!
//! Before this, SQL ran only on the in-memory `Database`, so no SQL write ever
//! survived a restart. These tests prove that `SqlEngine::durable(storage)`
//! WAL-logs every write and that an arbitrary schema created via SQL comes back
//! intact after a reopen — schema and all.

use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::SqlEngine;
use std::sync::Arc;

fn open(io: Arc<dyn Io>) -> Arc<Storage> {
    Arc::new(Storage::open(io, StorageConfig::default()).unwrap())
}

fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}

#[test]
fn durable_sql_survives_reopen_with_arbitrary_schema() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());

    {
        let e = SqlEngine::durable(open(io.clone()));
        e.run("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price FLOAT, qty INT)")
            .unwrap();
        e.run("INSERT INTO items VALUES (1, 'apple', 0.50, 100)")
            .unwrap();
        e.run("INSERT INTO items VALUES (2, 'pear', 0.75, 40)")
            .unwrap();
        e.run("INSERT INTO items VALUES (3, 'kiwi', 1.25, 10)")
            .unwrap();
        e.run("DELETE FROM items WHERE id = 2").unwrap();
        e.run("UPDATE items SET qty = 5 WHERE id = 3").unwrap();
        assert_eq!(one(&e, "SELECT COUNT(*) FROM items"), "2");
    } // simulate a crash: drop the engine and storage, keep the io (disk)

    // Reopen from the same durable medium. WAL replay + manifest schema must
    // reconstruct the arbitrary-schema table exactly.
    let e2 = SqlEngine::durable(open(io));
    assert_eq!(
        one(&e2, "SELECT COUNT(*) FROM items"),
        "2",
        "row count survived"
    );
    assert_eq!(one(&e2, "SELECT name FROM items WHERE id = 1"), "apple");
    // The deleted row stayed deleted; the update stuck.
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM items WHERE id = 2"), "0");
    assert_eq!(one(&e2, "SELECT qty FROM items WHERE id = 3"), "5");
    // The float column and its type recovered (aggregate over it works).
    assert_eq!(one(&e2, "SELECT SUM(price) FROM items"), "1.75");
    // New writes continue to work on the recovered schema.
    e2.run("INSERT INTO items VALUES (4, 'plum', 2.0, 3)")
        .unwrap();
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM items"), "3");
}

#[test]
fn durable_sql_text_primary_key_survives_reopen() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let e = SqlEngine::durable(open(io.clone()));
        e.run("CREATE TABLE users (email TEXT PRIMARY KEY, age INT)")
            .unwrap();
        e.run("INSERT INTO users VALUES ('alice@x.com', 25)")
            .unwrap();
        e.run("INSERT INTO users VALUES ('bob@x.com', 41)").unwrap();
    }
    let e2 = SqlEngine::durable(open(io));
    // The text key recovered and still looks up correctly.
    assert_eq!(
        one(&e2, "SELECT age FROM users WHERE email = 'bob@x.com'"),
        "41"
    );
    // The text key still rejects duplicates after recovery.
    assert!(e2
        .run("INSERT INTO users VALUES ('alice@x.com', 99)")
        .is_err());
}

#[test]
fn durable_sql_pk_less_rowid_table_survives_reopen() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let e = SqlEngine::durable(open(io.clone()));
        // No PRIMARY KEY: a hidden _rowid keys the table. Its assigned rowids
        // must be logged (not left null) so replay reproduces distinct rows.
        e.run("CREATE TABLE log (msg TEXT, level INT)").unwrap();
        e.run("INSERT INTO log VALUES ('boot', 1)").unwrap();
        e.run("INSERT INTO log VALUES ('warn', 2)").unwrap();
        e.run("INSERT INTO log VALUES ('warn', 2)").unwrap();
    }
    let e2 = SqlEngine::durable(open(io));
    assert_eq!(
        one(&e2, "SELECT COUNT(*) FROM log"),
        "3",
        "all three rows survived"
    );
    // SELECT * still hides the recovered rowid.
    let rows = e2.query("SELECT * FROM log").unwrap();
    assert_eq!(rows[0].len(), 2);
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM log WHERE level = 2"), "2");
}
