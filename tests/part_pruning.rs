//! Zonemap part pruning: a selective range predicate must return exactly the
//! matching rows even when most parts are skipped by their min/max bounds.
//!
//! Correctness is the invariant under test — pruning may only skip parts it can
//! *prove* hold no matching row, so the answer must be identical to a full scan.
//! Inserting well past the 10k seal threshold builds several sealed parts, each
//! with its own zonemap, so the pruning path is genuinely exercised.

use chakradb::{Database, SqlEngine};
use std::sync::Arc;

const N: i64 = 40_000; // > 3 seal thresholds → several sealed parts

fn engine() -> SqlEngine {
    let e = SqlEngine::new(Arc::new(Database::new()));
    e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT)").unwrap();
    for pk in 0..N {
        e.run(&format!("INSERT INTO t VALUES ({pk}, {})", pk * 10))
            .unwrap();
    }
    e
}

fn rows(e: &SqlEngine, sql: &str) -> Vec<Vec<String>> {
    e.query(sql).unwrap()
}

#[test]
fn selective_key_range_returns_exact_rows() {
    let e = engine();
    // A tail range that lives in the last part only.
    let got = rows(&e, "SELECT pk FROM t WHERE pk >= 39995 ORDER BY pk");
    let pks: Vec<i64> = got.iter().map(|r| r[0].parse().unwrap()).collect();
    assert_eq!(pks, vec![39995, 39996, 39997, 39998, 39999]);
}

#[test]
fn selective_key_equality_is_a_single_row() {
    let e = engine();
    let got = rows(&e, "SELECT pk, a FROM t WHERE pk = 20000");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0][0], "20000");
    assert_eq!(got[0][1], "200000");
}

#[test]
fn range_on_non_key_column_prunes_correctly() {
    let e = engine();
    // a = pk*10, so a in [399950, 399990] ⇒ pk in {39995..39999}.
    let got = rows(&e, "SELECT pk FROM t WHERE a >= 399950 ORDER BY pk");
    let pks: Vec<i64> = got.iter().map(|r| r[0].parse().unwrap()).collect();
    assert_eq!(pks, vec![39995, 39996, 39997, 39998, 39999]);
}

#[test]
fn pruned_answer_matches_full_scan_count() {
    let e = engine();
    // NoREC-style: the pruned range count agrees with an arithmetic ground truth.
    let n: i64 = e.query("SELECT COUNT(*) FROM t WHERE pk >= 30000").unwrap()[0][0]
        .parse()
        .unwrap();
    assert_eq!(n, N - 30000);
}

#[test]
fn empty_range_below_min_returns_nothing() {
    let e = engine();
    // Every part's min pk is >= 0, so this prunes everything.
    assert!(rows(&e, "SELECT pk FROM t WHERE pk < 0").is_empty());
    assert!(rows(&e, "SELECT pk FROM t WHERE pk > 1000000").is_empty());
}
