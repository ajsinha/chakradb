//! Durable storage: WAL logging, checkpointing, and recovery.
//!
//! Crash-injection coverage lives in `crash_consistency.rs`; this suite covers
//! the non-crash paths — that data survives a clean reopen, that checkpointing
//! bounds the log, and that CSNs never regress across a restart.

use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::{Durability, Row, Value};
use std::sync::Arc;

fn open(io: Arc<dyn Io>) -> Storage {
    Storage::open(io, StorageConfig::default()).unwrap()
}

fn row(pk: i64, tag: &str) -> Row {
    Row::new(pk, pk * 2, pk as f64, tag)
}

#[test]
fn fresh_database_is_empty() {
    let io = Arc::new(MemIo::new());
    let s = open(io);
    assert_eq!(s.recovery().tables_loaded, 0);
    assert_eq!(s.recovery().recovered_csn, 0);
    assert!(s.database().is_empty());
}

#[test]
fn create_table_survives_reopen() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = open(io.clone());
        s.create_table("users").unwrap();
    }
    let s2 = open(io);
    assert_eq!(s2.recovery().tables_loaded, 1);
    assert!(s2.database().table("users").is_ok());
}

#[test]
fn writes_survive_reopen_via_wal() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = open(io.clone());
        s.create_table("t").unwrap();
        for pk in 0..50 {
            s.insert("t", row(pk, "v")).unwrap();
        }
    }
    let s2 = open(io);
    let t = s2.database().table("t").unwrap();
    assert_eq!(t.row_count(s2.database().snapshot()), 50);
    assert_eq!(s2.recovery().wal_records_replayed, 50);
}

#[test]
fn deletes_survive_reopen() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = open(io.clone());
        s.create_table("t").unwrap();
        for pk in 0..20 {
            s.insert("t", row(pk, "v")).unwrap();
        }
        for pk in 0..10 {
            s.delete("t", &Value::Int(pk)).unwrap();
        }
    }
    let s2 = open(io);
    let t = s2.database().table("t").unwrap();
    assert_eq!(t.row_count(s2.database().snapshot()), 10);
    assert!(t.get_latest(&Value::Int(0)).is_none());
    assert!(t.get_latest(&Value::Int(15)).is_some());
}

#[test]
fn updates_survive_with_latest_value() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = open(io.clone());
        s.create_table("t").unwrap();
        s.insert("t", row(1, "first")).unwrap();
        s.update("t", row(1, "second")).unwrap();
        s.update("t", row(1, "third")).unwrap();
    }
    let s2 = open(io);
    let t = s2.database().table("t").unwrap();
    assert_eq!(t.get_latest(&Value::Int(1)).unwrap().c(), "third");
    assert_eq!(t.row_count(s2.database().snapshot()), 1);
}

#[test]
fn checkpoint_persists_parts_and_shrinks_the_log() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let s = open(io.clone());
    s.create_table("t").unwrap();
    for pk in 0..200 {
        s.insert("t", row(pk, "v")).unwrap();
    }
    let before = s.wal().written_bytes();
    s.checkpoint().unwrap();
    assert!(
        s.wal().written_bytes() < before,
        "checkpoint did not truncate the log"
    );
    drop(s);

    let s2 = open(io);
    assert!(s2.recovery().parts_loaded > 0, "no parts restored");
    assert_eq!(s2.recovery().rows_from_parts, 200);
    assert_eq!(s2.recovery().wal_records_replayed, 0, "replayed after ckpt");
    let t = s2.database().table("t").unwrap();
    assert_eq!(t.row_count(s2.database().snapshot()), 200);
}

#[test]
fn writes_after_checkpoint_also_survive() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = open(io.clone());
        s.create_table("t").unwrap();
        for pk in 0..100 {
            s.insert("t", row(pk, "v")).unwrap();
        }
        s.checkpoint().unwrap();
        for pk in 100..150 {
            s.insert("t", row(pk, "v")).unwrap();
        }
    }
    let s2 = open(io);
    let t = s2.database().table("t").unwrap();
    assert_eq!(t.row_count(s2.database().snapshot()), 150);
    assert_eq!(s2.recovery().wal_records_replayed, 50);
}

#[test]
fn csn_does_not_regress_across_restart() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let last = {
        let s = open(io.clone());
        s.create_table("t").unwrap();
        let mut c = 0;
        for pk in 0..30 {
            c = s.insert("t", row(pk, "v")).unwrap();
        }
        c
    };
    let s2 = open(io);
    let next = s2.insert("t", row(999, "after")).unwrap();
    assert!(next > last, "CSN regressed: {next} <= {last}");
}

#[test]
fn multiple_tables_recover_independently() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = open(io.clone());
        s.create_table("a").unwrap();
        s.create_table("b").unwrap();
        for pk in 0..10 {
            s.insert("a", row(pk, "in-a")).unwrap();
        }
        for pk in 0..5 {
            s.insert("b", row(pk, "in-b")).unwrap();
        }
        s.checkpoint().unwrap();
    }
    let s2 = open(io);
    assert_eq!(s2.recovery().tables_loaded, 2);
    let snap = s2.database().snapshot();
    assert_eq!(s2.database().table("a").unwrap().row_count(snap), 10);
    assert_eq!(s2.database().table("b").unwrap().row_count(snap), 5);
    assert_eq!(
        s2.database().table("a").unwrap().get_latest(&Value::Int(0)).unwrap().c(),
        "in-a"
    );
}

#[test]
fn async_mode_needs_a_flush_to_be_durable() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let s = Storage::open(
        io.clone(),
        StorageConfig {
            durability: Durability::Async,
            ..Default::default()
        },
    )
    .unwrap();
    s.create_table("t").unwrap();
    s.insert("t", row(1, "v")).unwrap();
    assert_eq!(s.wal().sync_count(), 0);
    s.flush().unwrap();
    assert_eq!(s.wal().durable_bytes(), s.wal().written_bytes());
}

#[test]
fn checkpoint_due_tracks_log_growth() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let s = Storage::open(
        io,
        StorageConfig {
            checkpoint_wal_bytes: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    s.create_table("t").unwrap();
    assert!(!s.checkpoint_due());
    for pk in 0..200 {
        s.insert("t", row(pk, "padding padding padding")).unwrap();
    }
    assert!(s.checkpoint_due());
}

#[test]
fn compaction_result_survives_checkpoint() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    {
        let s = open(io.clone());
        s.create_table("t").unwrap();
        for pk in 0..300 {
            s.insert("t", row(pk, "v")).unwrap();
        }
        for pk in 0..150 {
            s.delete("t", &Value::Int(pk)).unwrap();
        }
        s.database().seal_all();
        s.compact_all();
        s.checkpoint().unwrap();
    }
    let s2 = open(io);
    let t = s2.database().table("t").unwrap();
    assert_eq!(t.row_count(s2.database().snapshot()), 150);
    assert_eq!(s2.recovery().rows_from_parts, 150, "reclaimed rows returned");
}
