//! FR-06b: time to open must not scale with database size.
//!
//! M1 loaded every part eagerly, so total recovery grew linearly with data even
//! though log replay was flat (`m1-findings.md` §3). That is why FR-06 had to be
//! split. This suite covers the mechanism that closes FR-06b: parts are
//! registered from a small summary frame and their columns are faulted in on
//! first touch.
//!
//! The property holds **after a clean checkpoint**. If the WAL tail contains
//! mutations, opening must materialise the parts those mutations touch, because
//! a logged DELETE of a key living in a sealed part has to tombstone that part's
//! row and cannot do so through a summary. That trade is deliberate and is
//! asserted here rather than left implicit.

use chakradb::io::MemIo;
use chakradb::{Row, Storage, StorageConfig, TableConfig, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn cfg(seal: usize) -> StorageConfig {
    StorageConfig {
        table: TableConfig {
            seal_threshold: seal,
            ..Default::default()
        },
        checkpoint_wal_bytes: u64::MAX,
        ..Default::default()
    }
}

fn row(pk: i64) -> Row {
    Row::new(pk, pk * 2, pk as f64, format!("v{pk}"))
}

/// Build a checkpointed database of `n` rows and return its filesystem.
fn seeded(n: i64, seal: usize) -> Arc<MemIo> {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let s = Storage::open(io.clone(), cfg(seal)).unwrap();
    s.create_table("t").unwrap();
    s.load_batch("t", (0..n).map(row).collect()).unwrap();
    s.checkpoint().unwrap();
    io
}

#[test]
fn clean_open_does_not_read_column_data() {
    let io = seeded(20_000, 2_000);
    let s = Storage::open(io, cfg(2_000)).unwrap();

    assert!(s.recovery().parts_registered_lazily > 0, "nothing registered");
    assert_eq!(
        s.pager_metrics().parts_faulted.load(Ordering::Relaxed),
        0,
        "open faulted parts in despite a clean checkpoint"
    );
    // The row count is known from summaries alone.
    assert_eq!(s.recovery().rows_from_parts, 20_000);
}

#[test]
fn open_cost_is_independent_of_database_size() {
    // The FR-06b property, expressed as bytes read rather than wall time, which
    // is the stable thing to assert in a test.
    let mut faulted = vec![];
    for n in [5_000i64, 20_000, 80_000] {
        let io = seeded(n, 2_000);
        let s = Storage::open(io, cfg(2_000)).unwrap();
        faulted.push(s.pager_metrics().bytes_faulted.load(Ordering::Relaxed));
    }
    assert!(
        faulted.iter().all(|&b| b == 0),
        "open read column data: {faulted:?}"
    );
}

#[test]
fn bounds_reject_absent_keys_without_faulting() {
    let io = seeded(10_000, 1_000);
    let s = Storage::open(io, cfg(1_000)).unwrap();

    // Keys outside every part's range are refused from summaries alone.
    assert!(!s.may_contain_key("t", &Value::Int(-1)));
    assert!(!s.may_contain_key("t", &Value::Int(999_999)));
    assert_eq!(
        s.pager_metrics().parts_faulted.load(Ordering::Relaxed),
        0,
        "out-of-range probe faulted a part in"
    );
    assert!(s.may_contain_key("t", &Value::Int(5_000)), "in-range key was excluded");
}

#[test]
fn warming_materialises_everything_correctly() {
    let io = seeded(10_000, 1_000);
    let s = Storage::open(io, cfg(1_000)).unwrap();
    assert_eq!(s.pager_metrics().parts_faulted.load(Ordering::Relaxed), 0);

    // Touching the database warms it.
    let db = s.database();
    let t = db.table("t").unwrap();
    assert_eq!(t.row_count(db.snapshot()), 10_000);
    assert!(s.pager_metrics().parts_faulted.load(Ordering::Relaxed) > 0);
    assert!(s.pager_metrics().resident_fraction() > 0.99);

    // And the data is right.
    for pk in (0..10_000).step_by(311) {
        assert_eq!(t.get_latest(&Value::Int(pk)).unwrap().c(), format!("v{pk}"));
    }
}

#[test]
fn warming_is_idempotent() {
    let io = seeded(5_000, 1_000);
    let s = Storage::open(io, cfg(1_000)).unwrap();
    s.warm();
    let after_first = s.pager_metrics().parts_faulted.load(Ordering::Relaxed);
    s.warm();
    s.warm();
    assert_eq!(
        s.pager_metrics().parts_faulted.load(Ordering::Relaxed),
        after_first,
        "repeated warming re-read parts"
    );
}

#[test]
fn a_wal_tail_forces_warming_and_stays_correct() {
    // The documented trade: mutations beyond the checkpoint need parts resident,
    // because a delete has to tombstone a row inside one.
    let io = seeded(8_000, 1_000);
    {
        let s = Storage::open(io.clone(), cfg(1_000)).unwrap();
        for pk in 0..200 {
            s.delete("t", &Value::Int(pk)).unwrap();
        }
        for pk in 8_000..8_100 {
            s.insert("t", row(pk)).unwrap();
        }
    }
    let s2 = Storage::open(io, cfg(1_000)).unwrap();
    assert!(
        s2.pager_metrics().parts_faulted.load(Ordering::Relaxed) > 0,
        "a mutating tail should have forced warming"
    );

    let db = s2.database();
    let t = db.table("t").unwrap();
    let snap = db.snapshot();
    assert_eq!(t.row_count(snap), 8_000 - 200 + 100);
    for pk in 0..200 {
        assert!(t.get(&Value::Int(pk), snap).is_none(), "deleted pk={pk} came back");
    }
    for pk in 8_000..8_100 {
        assert!(t.get(&Value::Int(pk), snap).is_some(), "tail insert pk={pk} lost");
    }
}

#[test]
fn multi_table_lazy_open() {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    {
        let s = Storage::open(io.clone(), cfg(1_000)).unwrap();
        for name in ["a", "b", "c"] {
            s.create_table(name).unwrap();
            s.load_batch(name, (0..4_000).map(row).collect()).unwrap();
        }
        s.checkpoint().unwrap();
    }
    let s2 = Storage::open(io, cfg(1_000)).unwrap();
    assert_eq!(s2.recovery().tables_loaded, 3);
    assert_eq!(s2.pager_metrics().parts_faulted.load(Ordering::Relaxed), 0);

    let db = s2.database();
    let snap = db.snapshot();
    for name in ["a", "b", "c"] {
        assert_eq!(db.table(name).unwrap().row_count(snap), 4_000);
    }
}

#[test]
fn lazy_open_survives_a_second_round_trip() {
    let io = seeded(6_000, 1_000);
    for _ in 0..3 {
        let s = Storage::open(io.clone(), cfg(1_000)).unwrap();
        let db = s.database();
        assert_eq!(db.table("t").unwrap().row_count(db.snapshot()), 6_000);
        s.checkpoint().unwrap();
    }
}
