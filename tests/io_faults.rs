//! The I/O seam: positional reads/writes, fault injection, and crash simulation.
//!
//! `MemIo` is what the crash-consistency suite drives. It deliberately supports
//! the failure modes that actually corrupt databases — errors on write/sync, and
//! the more insidious case of a write that reports success and stores nothing.

use chakradb::io::{FaultOp, File, Io, MemIo};

#[test]
fn write_then_read_roundtrips() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"hello world").unwrap();
    let mut buf = [0u8; 11];
    assert_eq!(f.pread(0, &mut buf).unwrap(), 11);
    assert_eq!(&buf, b"hello world");
}

#[test]
fn read_past_end_returns_zero() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"abc").unwrap();
    let mut buf = [0u8; 4];
    assert_eq!(f.pread(100, &mut buf).unwrap(), 0);
}

#[test]
fn partial_read_at_tail() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"abcdef").unwrap();
    let mut buf = [0u8; 10];
    assert_eq!(f.pread(4, &mut buf).unwrap(), 2);
    assert_eq!(&buf[..2], b"ef");
}

#[test]
fn sparse_write_zero_fills() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(4, b"xy").unwrap();
    assert_eq!(f.len().unwrap(), 6);
    let mut buf = [9u8; 6];
    f.pread(0, &mut buf).unwrap();
    assert_eq!(&buf, b"\0\0\0\0xy");
}

#[test]
fn truncate_shrinks_and_grows() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"abcdef").unwrap();
    f.truncate(3).unwrap();
    assert_eq!(f.len().unwrap(), 3);
    f.truncate(5).unwrap();
    assert_eq!(f.len().unwrap(), 5);
}

#[test]
fn same_path_returns_same_file() {
    let io = MemIo::new();
    let a = io.open("shared").unwrap();
    a.pwrite(0, b"z").unwrap();
    let b = io.open("shared").unwrap();
    let mut buf = [0u8; 1];
    b.pread(0, &mut buf).unwrap();
    assert_eq!(&buf, b"z");
}

#[test]
fn exists_list_and_remove() {
    let io = MemIo::new();
    assert!(!io.exists("x"));
    io.open("x").unwrap();
    io.open("y").unwrap();
    assert!(io.exists("x"));
    assert_eq!(io.list(), vec!["x".to_string(), "y".to_string()]);
    io.remove("x").unwrap();
    assert!(!io.exists("x"));
}

#[test]
fn injected_write_fault_fires_once() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    io.inject_fault(FaultOp::Write, 0);
    assert!(f.pwrite(0, b"x").is_err());
    // Second write succeeds: the fault was consumed.
    assert!(f.pwrite(0, b"x").is_ok());
}

#[test]
fn injected_fault_respects_countdown() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    io.inject_fault(FaultOp::Write, 2);
    assert!(f.pwrite(0, b"1").is_ok());
    assert!(f.pwrite(1, b"2").is_ok());
    assert!(f.pwrite(2, b"3").is_err());
}

#[test]
fn injected_sync_and_read_faults() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    io.inject_fault(FaultOp::Sync, 0);
    assert!(f.sync().is_err());
    io.inject_fault(FaultOp::Read, 0);
    let mut buf = [0u8; 1];
    assert!(f.pread(0, &mut buf).is_err());
}

#[test]
fn clear_faults_restores_normal_operation() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    io.inject_fault(FaultOp::Write, 0);
    io.clear_faults();
    assert!(f.pwrite(0, b"x").is_ok());
}

#[test]
fn crash_discards_unsynced_writes() {
    let io = MemIo::new();
    io.open("a").unwrap();
    let raw = io.file("a").unwrap();
    raw.pwrite(0, b"durable").unwrap();
    raw.sync().unwrap();
    raw.pwrite(7, b"-lost").unwrap();
    assert_eq!(raw.len().unwrap(), 12);
    raw.crash();
    assert_eq!(raw.len().unwrap(), 7);
    let mut buf = [0u8; 7];
    raw.pread(0, &mut buf).unwrap();
    assert_eq!(&buf, b"durable");
}

#[test]
fn drop_writes_loses_data_silently() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"real").unwrap();
    io.set_drop_writes(true);
    // Reports success...
    assert_eq!(f.pwrite(4, b"ghost").unwrap(), 5);
    // ...but nothing was stored.
    assert_eq!(f.len().unwrap(), 4);
}

#[test]
fn durable_len_tracks_sync() {
    let io = MemIo::new();
    io.open("a").unwrap();
    let raw = io.file("a").unwrap();
    raw.pwrite(0, b"12345").unwrap();
    assert_eq!(raw.durable_len(), 0);
    raw.sync().unwrap();
    assert_eq!(raw.durable_len(), 5);
}

#[test]
fn sync_delay_is_observable() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"x").unwrap();
    let t0 = std::time::Instant::now();
    f.sync().unwrap();
    assert!(t0.elapsed() < std::time::Duration::from_millis(2));

    io.set_sync_delay(std::time::Duration::from_millis(5));
    let t1 = std::time::Instant::now();
    f.sync().unwrap();
    assert!(t1.elapsed() >= std::time::Duration::from_millis(4));
}

#[test]
fn filesystem_wide_crash_reverts_every_file() {
    let io = MemIo::new();
    let a = io.open("a").unwrap();
    let b = io.open("b").unwrap();
    a.pwrite(0, b"durable-a").unwrap();
    b.pwrite(0, b"durable-b").unwrap();
    a.sync().unwrap();
    b.sync().unwrap();
    a.pwrite(9, b"-lost").unwrap();
    b.pwrite(9, b"-lost").unwrap();

    io.crash();

    assert_eq!(a.len().unwrap(), 9, "unsynced bytes survived in a");
    assert_eq!(b.len().unwrap(), 9, "unsynced bytes survived in b");
}

#[test]
fn crash_on_never_synced_file_empties_it() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"nothing was ever synced").unwrap();
    io.crash();
    assert_eq!(f.len().unwrap(), 0);
}

#[test]
fn durable_bytes_tracks_syncs() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    f.pwrite(0, b"12345").unwrap();
    assert_eq!(io.durable_bytes(), 0);
    f.sync().unwrap();
    assert_eq!(io.durable_bytes(), 5);
}

#[test]
fn io_trait_is_object_safe() {
    let io: Box<dyn Io> = Box::new(MemIo::new());
    io.open("a").unwrap();
    assert!(io.exists("a"));
}

#[test]
fn is_empty_reflects_length() {
    let io = MemIo::new();
    let f = io.open("a").unwrap();
    assert!(f.is_empty().unwrap());
    f.pwrite(0, b"x").unwrap();
    assert!(!f.is_empty().unwrap());
}
