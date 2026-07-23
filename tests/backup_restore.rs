//! Backup and restore of a durable store: a self-contained copy (manifest +
//! parts + WAL) that restores to the exact committed state, capturing both
//! checkpointed data and the uncheckpointed WAL tail.

use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::SqlEngine;
use std::sync::Arc;

fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}
fn cfg() -> StorageConfig {
    StorageConfig::default()
}

#[test]
fn backup_captures_checkpointed_and_wal_tail() {
    let src_io: Arc<dyn Io> = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(src_io, cfg()).unwrap());
    let e = SqlEngine::durable(storage.clone());
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    for i in 0..100 {
        e.run(&format!("INSERT INTO t VALUES ({i}, 'row{i}')")).unwrap();
    }
    storage.checkpoint().unwrap(); // 0..100 now on disk as parts
    for i in 100..150 {
        e.run(&format!("INSERT INTO t VALUES ({i}, 'row{i}')")).unwrap();
    } // 100..150 only in the WAL

    // Back it up while it is live.
    let backup_io: Arc<dyn Io> = Arc::new(MemIo::new());
    storage.backup_to(&backup_io).unwrap();

    // The source is untouched and still usable.
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "150");
    e.run("INSERT INTO t VALUES (999, 'after-backup')").unwrap();
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "151");

    // Restore into a fresh location and verify the exact backed-up state.
    let restore_io: Arc<dyn Io> = Arc::new(MemIo::new());
    let restored = Storage::restore(backup_io, restore_io, cfg()).unwrap();
    let r = SqlEngine::durable(Arc::new(restored));
    assert_eq!(one(&r, "SELECT COUNT(*) FROM t"), "150", "backup point, not after");
    assert_eq!(one(&r, "SELECT v FROM t WHERE id = 42"), "row42"); // checkpointed
    assert_eq!(one(&r, "SELECT v FROM t WHERE id = 137"), "row137"); // WAL tail
    assert!(
        r.query("SELECT v FROM t WHERE id = 999").unwrap().is_empty(),
        "writes after the backup are not in it"
    );
}

#[test]
fn restored_store_accepts_new_writes() {
    let src_io: Arc<dyn Io> = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(src_io, cfg()).unwrap());
    let e = SqlEngine::durable(storage.clone());
    e.run("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    e.run("INSERT INTO t VALUES (1)").unwrap();

    let backup_io: Arc<dyn Io> = Arc::new(MemIo::new());
    storage.backup_to(&backup_io).unwrap();

    let restore_io: Arc<dyn Io> = Arc::new(MemIo::new());
    let restored = Storage::restore(backup_io, restore_io, cfg()).unwrap();
    let r = SqlEngine::durable(Arc::new(restored));
    // The restored store is a normal, writable database.
    r.run("INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(one(&r, "SELECT COUNT(*) FROM t"), "2");
}

#[test]
fn backup_is_idempotent_and_survives_a_reopen() {
    let src_io: Arc<dyn Io> = Arc::new(MemIo::new());
    let storage = Arc::new(Storage::open(src_io, cfg()).unwrap());
    let e = SqlEngine::durable(storage.clone());
    e.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    e.run("INSERT INTO t VALUES (1, 10)").unwrap();

    let backup_io: Arc<dyn Io> = Arc::new(MemIo::new());
    storage.backup_to(&backup_io).unwrap();
    // A second backup over the same target must leave it clean and valid, even
    // after more data changed the source's file set (a checkpoint writes parts).
    e.run("INSERT INTO t VALUES (2, 20)").unwrap();
    storage.checkpoint().unwrap();
    storage.backup_to(&backup_io).unwrap();

    let restore_io: Arc<dyn Io> = Arc::new(MemIo::new());
    let restored = Storage::restore(backup_io, restore_io, cfg()).unwrap();
    let r = SqlEngine::durable(Arc::new(restored));
    assert_eq!(one(&r, "SELECT COUNT(*) FROM t"), "2");
    assert_eq!(one(&r, "SELECT v FROM t WHERE id = 2"), "20");
}
