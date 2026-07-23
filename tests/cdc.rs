//! Change-Data-Capture: committed writes are published as a change stream.

use chakradb::cdc::{Cdc, CdcBackend, ChangeOp};
use chakradb::io::MemIo;
use chakradb::storage::{Storage, StorageConfig};
use chakradb::value::Value;
use chakradb::{Database, SqlEngine};
use std::sync::Arc;

fn mem_engine() -> (SqlEngine, Arc<Cdc>) {
    let db = Arc::new(Database::new());
    let cdc = Cdc::new();
    let engine = SqlEngine::with_backend(CdcBackend::wrap(db, cdc.clone()));
    (engine, cdc)
}

#[test]
fn insert_update_delete_are_published_in_order() {
    let (engine, cdc) = mem_engine();
    let stream = cdc.subscribe(Some("accounts"));

    engine
        .run("CREATE TABLE accounts (id INTEGER PRIMARY KEY, bal INTEGER)")
        .unwrap();
    engine.run("INSERT INTO accounts VALUES (1, 100)").unwrap();
    engine.run("UPDATE accounts SET bal = 250 WHERE id = 1").unwrap();
    engine.run("DELETE FROM accounts WHERE id = 1").unwrap();

    let changes = stream.drain();
    assert_eq!(changes.len(), 3, "one event per committed row change");

    // INSERT: no old image, new = (1, 100).
    assert_eq!(changes[0].op, ChangeOp::Insert);
    assert_eq!(changes[0].table, "accounts");
    assert!(changes[0].old.is_none());
    assert_eq!(changes[0].new.as_ref().unwrap()[1], Value::Int(100));
    assert_eq!(changes[0].columns.as_slice(), &["id", "bal"]);

    // UPDATE: old = (1, 100), new = (1, 250).
    assert_eq!(changes[1].op, ChangeOp::Update);
    assert_eq!(changes[1].old.as_ref().unwrap()[1], Value::Int(100));
    assert_eq!(changes[1].new.as_ref().unwrap()[1], Value::Int(250));

    // DELETE: old = (1, 250), no new image.
    assert_eq!(changes[2].op, ChangeOp::Delete);
    assert_eq!(changes[2].old.as_ref().unwrap()[1], Value::Int(250));
    assert!(changes[2].new.is_none());

    // CSNs are monotonic across the three changes.
    assert!(changes[0].csn < changes[1].csn && changes[1].csn < changes[2].csn);
}

#[test]
fn subscription_filters_by_table() {
    let (engine, cdc) = mem_engine();
    let only_txns = cdc.subscribe(Some("txns"));
    let everything = cdc.subscribe(None);

    engine
        .run("CREATE TABLE txns (id INTEGER PRIMARY KEY, amt INTEGER)")
        .unwrap();
    engine
        .run("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    engine.run("INSERT INTO txns VALUES (1, 10)").unwrap();
    engine.run("INSERT INTO notes VALUES (1, 'hi')").unwrap();
    engine.run("INSERT INTO txns VALUES (2, 20)").unwrap();

    let filtered = only_txns.drain();
    assert_eq!(filtered.len(), 2, "only txns changes");
    assert!(filtered.iter().all(|c| c.table == "txns"));

    let all = everything.drain();
    assert_eq!(all.len(), 3, "both tables");
}

#[test]
fn transaction_publishes_after_commit_not_on_rollback() {
    let (engine, cdc) = mem_engine();
    let stream = cdc.subscribe(Some("t"));
    engine
        .run("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    // A rolled-back transaction must publish nothing.
    engine.run("BEGIN").unwrap();
    engine.run("INSERT INTO t VALUES (1, 1)").unwrap();
    engine.run("ROLLBACK").unwrap();
    assert!(stream.drain().is_empty(), "rolled-back writes are not published");

    // A committed transaction publishes its writes.
    engine.run("BEGIN").unwrap();
    engine.run("INSERT INTO t VALUES (2, 2)").unwrap();
    engine.run("INSERT INTO t VALUES (3, 3)").unwrap();
    engine.run("COMMIT").unwrap();
    let changes = stream.drain();
    assert_eq!(changes.len(), 2, "both committed inserts");
    assert!(changes.iter().all(|c| c.op == ChangeOp::Insert));
}

#[test]
fn durable_backend_publishes_committed_writes() {
    let io = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(io, StorageConfig::default()).unwrap());
    let cdc = Cdc::new();
    let engine = SqlEngine::with_backend(CdcBackend::wrap(storage, cdc.clone()));
    let stream = cdc.subscribe(None);

    engine
        .run("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT)")
        .unwrap();
    engine
        .run("INSERT INTO events VALUES (1, 'login')")
        .unwrap();

    let changes = stream.drain();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].op, ChangeOp::Insert);
    assert_eq!(changes[0].new.as_ref().unwrap()[1], Value::Text("login".into()));
}
