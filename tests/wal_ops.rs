//! Write-ahead log behaviour, via the public API.
//!
//! Framing, durability modes, group-commit batching, truncation, and the
//! torn-tail contract that makes crash recovery safe.

use chakradb::codec::{frame, unframe};
use chakradb::io::{Io, MemIo};
use chakradb::wal::{Wal, WalRecord};
use chakradb::{Csn, Durability, Row};
use std::sync::Arc;

fn rec_insert(pk: i64, csn: Csn) -> WalRecord {
    WalRecord::Insert {
        table: 1,
        csn,
        row: Row::new(pk, pk * 2, pk as f64, format!("v{pk}")),
    }
}

#[test]
fn every_record_type_roundtrips() {
    let recs = vec![
        rec_insert(5, 10),
        WalRecord::Delete {
            table: 2,
            csn: 11,
            pk: -7,
        },
        WalRecord::Seal {
            table: 3,
            csn: 12,
            part_id: 99,
        },
        WalRecord::Checkpoint { csn: 13 },
    ];
    for r in recs {
        let bytes = r.encode();
        let (payload, _) = unframe(&bytes, 0).unwrap();
        assert_eq!(WalRecord::decode(payload).unwrap(), r);
    }
}

#[test]
fn unknown_opcode_is_rejected() {
    let bad = frame(&[0xEE, 0, 0, 0, 0]);
    let (payload, _) = unframe(&bad, 0).unwrap();
    assert!(WalRecord::decode(payload).is_err());
}

#[test]
fn append_then_replay() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
    for i in 1..=100 {
        wal.append(&rec_insert(i, i as u64)).unwrap();
    }
    let r = Wal::replay(&io, "wal").unwrap();
    assert_eq!(r.records.len(), 100);
    assert!(!r.truncated_tail);
    assert_eq!(r.max_csn(), 100);
}

#[test]
fn replay_of_empty_log_is_empty() {
    let io = MemIo::new();
    io.open("wal").unwrap();
    let r = Wal::replay(&io, "wal").unwrap();
    assert!(r.records.is_empty());
    assert!(!r.truncated_tail);
    assert_eq!(r.max_csn(), 0);
}

#[test]
fn torn_tail_stops_replay_without_losing_prefix() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
    for i in 1..=10 {
        wal.append(&rec_insert(i, i as u64)).unwrap();
    }
    let full = {
        let f = io.open("wal").unwrap();
        let mut b = vec![0u8; f.len().unwrap() as usize];
        f.pread(0, &mut b).unwrap();
        b
    };
    // Cut at every possible point; the prefix must always survive intact.
    for cut in 1..full.len() {
        let r = Wal::replay_bytes(&full[..cut]);
        assert!(r.records.len() <= 10);
        for (i, rec) in r.records.iter().enumerate() {
            assert_eq!(rec.csn(), (i + 1) as u64, "record {i} garbled at cut {cut}");
        }
    }
}

#[test]
fn corrupted_middle_truncates_at_that_point() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
    for i in 1..=5 {
        wal.append(&rec_insert(i, i as u64)).unwrap();
    }
    let f = io.open("wal").unwrap();
    let mut b = vec![0u8; f.len().unwrap() as usize];
    f.pread(0, &mut b).unwrap();
    let mid = b.len() / 2;
    b[mid] ^= 0xFF;
    let r = Wal::replay_bytes(&b);
    assert!(r.truncated_tail);
    assert!(r.records.len() < 5);
}

#[test]
fn async_mode_does_not_sync_on_append() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Async).unwrap();
    for i in 1..=50 {
        wal.append(&rec_insert(i, i as u64)).unwrap();
    }
    assert_eq!(wal.sync_count(), 0, "async mode synced");
    assert!(wal.written_bytes() > 0);
    assert_eq!(wal.durable_bytes(), 0);
}

#[test]
fn sync_mode_makes_every_append_durable() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Sync).unwrap();
    for i in 1..=20 {
        let end = wal.append(&rec_insert(i, i as u64)).unwrap();
        assert!(wal.durable_bytes() >= end, "append returned before durable");
    }
}

#[test]
fn flush_makes_async_writes_durable() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Async).unwrap();
    wal.append(&rec_insert(1, 1)).unwrap();
    assert_eq!(wal.durable_bytes(), 0);
    wal.flush().unwrap();
    assert_eq!(wal.durable_bytes(), wal.written_bytes());
}

#[test]
fn reopening_treats_existing_bytes_as_durable() {
    let io = MemIo::new();
    {
        let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
        wal.append(&rec_insert(1, 1)).unwrap();
    }
    let wal2 = Wal::open(&io, "wal", Durability::Group).unwrap();
    assert!(wal2.written_bytes() > 0);
    assert_eq!(wal2.durable_bytes(), wal2.written_bytes());
}

#[test]
fn truncate_keeps_the_tail_and_drops_the_head() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
    let mut offsets = vec![];
    for i in 1..=10 {
        offsets.push(wal.append(&rec_insert(i, i as u64)).unwrap());
    }
    // Drop the first five records.
    wal.truncate_before(offsets[4]).unwrap();
    let r = Wal::replay(&io, "wal").unwrap();
    assert_eq!(r.records.len(), 5);
    assert_eq!(r.records[0].csn(), 6, "wrong record survived truncation");
    assert!(!r.truncated_tail);
}

#[test]
fn writes_after_truncation_are_still_synced() {
    // Regression: truncation used to leave a stale high watermark, so the
    // next append skipped its fsync and vanished on crash.
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
    for i in 1..=10 {
        wal.append(&rec_insert(i, i as u64)).unwrap();
    }
    wal.truncate_before(wal.written_bytes()).unwrap();
    assert_eq!(wal.written_bytes(), 0);
    assert_eq!(wal.durable_bytes(), 0, "watermark not reset");

    let syncs_before = wal.sync_count();
    wal.append(&rec_insert(99, 99)).unwrap();
    assert!(wal.sync_count() > syncs_before, "append skipped its fsync");
    assert_eq!(wal.durable_bytes(), wal.written_bytes());

    // And it survives a power cut.
    io.crash();
    let r = Wal::replay(&io, "wal").unwrap();
    assert_eq!(r.records.len(), 1);
    assert_eq!(r.records[0].csn(), 99);
}

#[test]
fn truncate_is_a_noop_at_boundaries() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
    wal.append(&rec_insert(1, 1)).unwrap();
    let before = wal.written_bytes();
    wal.truncate_before(0).unwrap();
    wal.truncate_before(before + 1000).unwrap();
    assert_eq!(wal.written_bytes(), before);
}

#[test]
fn last_checkpoint_is_reported() {
    let io = MemIo::new();
    let wal = Wal::open(&io, "wal", Durability::Group).unwrap();
    wal.append(&rec_insert(1, 1)).unwrap();
    wal.append(&WalRecord::Checkpoint { csn: 42 }).unwrap();
    wal.append(&rec_insert(2, 50)).unwrap();
    let r = Wal::replay(&io, "wal").unwrap();
    assert_eq!(r.last_checkpoint(), 42);
    assert_eq!(r.max_csn(), 50);
}

#[test]
fn concurrent_appends_are_all_recorded() {
    use std::thread;
    let io = MemIo::new();
    let wal = Arc::new(Wal::open(&io, "wal", Durability::Group).unwrap());
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let wal = wal.clone();
            thread::spawn(move || {
                for i in 0..100 {
                    let csn = (t * 100 + i + 1) as u64;
                    wal.append(&rec_insert(csn as i64, csn)).unwrap();
                }
            })
        })
        .collect();
    for th in threads {
        th.join().unwrap();
    }
    let r = Wal::replay(&io, "wal").unwrap();
    assert_eq!(r.records.len(), 800, "records lost under concurrency");
    assert!(!r.truncated_tail, "concurrent appends corrupted the log");

    let mut csns: Vec<u64> = r.records.iter().map(|x| x.csn()).collect();
    csns.sort_unstable();
    csns.dedup();
    assert_eq!(csns.len(), 800, "duplicate or lost CSNs");
}

#[test]
fn group_commit_amortises_syncs() {
    use std::thread;
    let io = MemIo::new();
    let wal = Arc::new(Wal::open(&io, "wal", Durability::Group).unwrap());
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let wal = wal.clone();
            thread::spawn(move || {
                for i in 0..200 {
                    let csn = (t * 200 + i + 1) as u64;
                    wal.append(&rec_insert(csn as i64, csn)).unwrap();
                }
            })
        })
        .collect();
    for th in threads {
        th.join().unwrap();
    }
    assert_eq!(wal.append_count(), 1600);
    assert!(
        wal.syncs_per_append() < 1.0,
        "group commit did no batching: {} syncs for {} appends",
        wal.sync_count(),
        wal.append_count()
    );
}
