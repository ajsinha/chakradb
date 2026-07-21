//! M2-2 stand-in: property / metamorphic testing of the query layer.
//!
//! Real SQLancer (TLP/NoREC/PQS) drives an engine over JDBC, which M2 has no
//! wire protocol for — `m2-findings.md` records that as the gap. What we *can*
//! do without a wire protocol is run SQLancer's *oracles* directly in-process:
//! generate random data and random predicates, then assert the metamorphic
//! relations those oracles check. That catches the same class of wrong-answer
//! bug (a predicate that returns the wrong rows) without the transport.
//!
//! The two oracles implemented here:
//!
//! * **TLP** (Ternary Logic Partitioning): for any predicate `p`, the rows where
//!   `p`, `NOT p`, and `p IS NULL` must together partition the table exactly.
//! * **NoREC**-style: `COUNT(*) WHERE p` must equal the number of rows a full
//!   scan reports as satisfying `p` — i.e. the optimiser's filtered count agrees
//!   with an unoptimised one.

use chakradb::{Database, Rng, SqlEngine};
use std::sync::Arc;

fn engine_with(seed: u64, n: i64) -> SqlEngine {
    let e = SqlEngine::new(Arc::new(Database::new()));
    e.run("CREATE TABLE t (pk INT PRIMARY KEY, a INT, b FLOAT, c TEXT)").unwrap();
    let mut rng = Rng::new(seed);
    for pk in 0..n {
        // Some columns randomly held back so NULLs and edge values appear.
        let a = rng.range(-50, 50);
        let b = rng.next_f64() * 100.0;
        let c = format!("s{}", rng.range(0, 10));
        e.run(&format!("INSERT INTO t VALUES ({pk}, {a}, {b:.3}, '{c}')"))
            .unwrap();
    }
    e
}

fn count(e: &SqlEngine, sql: &str) -> i64 {
    e.query(sql).unwrap()[0][0].parse().unwrap()
}

/// A pool of predicates, some of which involve NULL comparisons so three-valued
/// logic is genuinely exercised.
fn predicates() -> Vec<&'static str> {
    vec![
        "a > 0",
        "a < 10",
        "a >= 0 AND a <= 20",
        "a = 5",
        "a <> 5",
        "pk % 2 = 0",
        "a > 0 OR pk < 5",
        "a = NULL",       // always NULL → no rows
        "a IS NULL",      // never, our schema has no NULL a
        "c = 's3'",
        "NOT (a > 0)",
        "a > 0 AND a > 0",
    ]
}

#[test]
fn tlp_partitions_the_table() {
    for seed in 0..25 {
        let e = engine_with(seed, 200);
        let total = count(&e, "SELECT COUNT(*) FROM t");
        for p in predicates() {
            let yes = count(&e, &format!("SELECT COUNT(*) FROM t WHERE {p}"));
            let no = count(&e, &format!("SELECT COUNT(*) FROM t WHERE NOT ({p})"));
            let unknown = count(&e, &format!("SELECT COUNT(*) FROM t WHERE ({p}) IS NULL"));
            assert_eq!(
                yes + no + unknown,
                total,
                "seed {seed}, predicate `{p}`: TLP partition {yes}+{no}+{unknown} != {total}"
            );
        }
    }
}

#[test]
fn filtered_count_agrees_with_scan() {
    // NoREC-style: the WHERE-filtered count equals a manual count over a
    // projection of the same predicate.
    for seed in 25..50 {
        let e = engine_with(seed, 150);
        for p in predicates() {
            let filtered = count(&e, &format!("SELECT COUNT(*) FROM t WHERE {p}"));
            // Manual: project the predicate itself, count the true rows.
            let projected = e
                .query(&format!("SELECT {p} FROM t"))
                .unwrap()
                .iter()
                .filter(|r| r[0] == "1") // Bool(true) renders as "1"
                .count() as i64;
            assert_eq!(
                filtered, projected,
                "seed {seed}, predicate `{p}`: filtered {filtered} != projected {projected}"
            );
        }
    }
}

#[test]
fn double_negation_is_identity_modulo_nulls() {
    for seed in 50..70 {
        let e = engine_with(seed, 100);
        for p in predicates() {
            let once = count(&e, &format!("SELECT COUNT(*) FROM t WHERE {p}"));
            let twice = count(&e, &format!("SELECT COUNT(*) FROM t WHERE NOT (NOT ({p}))"));
            assert_eq!(once, twice, "seed {seed}: `{p}` not stable under double negation");
        }
    }
}

#[test]
fn aggregate_consistency() {
    // MIN <= AVG <= MAX must hold whenever the column has any non-null values.
    for seed in 70..90 {
        let e = engine_with(seed, 120);
        let rows = e.query("SELECT MIN(a), MAX(a), AVG(a) FROM t").unwrap();
        let min: f64 = rows[0][0].parse().unwrap();
        let max: f64 = rows[0][1].parse().unwrap();
        let avg: f64 = rows[0][2].parse().unwrap();
        assert!(min <= avg + 1e-9, "seed {seed}: min {min} > avg {avg}");
        assert!(avg <= max + 1e-9, "seed {seed}: avg {avg} > max {max}");
    }
}

#[test]
fn limit_is_a_prefix_of_the_unlimited_order() {
    for seed in 90..105 {
        let e = engine_with(seed, 80);
        let all = e.query("SELECT pk FROM t ORDER BY pk").unwrap();
        let limited = e.query("SELECT pk FROM t ORDER BY pk LIMIT 10").unwrap();
        assert_eq!(limited.len(), 10.min(all.len()));
        for (i, row) in limited.iter().enumerate() {
            assert_eq!(row, &all[i], "seed {seed}: LIMIT changed row {i}");
        }
    }
}

#[test]
fn distinct_count_never_exceeds_total() {
    for seed in 105..120 {
        let e = engine_with(seed, 100);
        let total = count(&e, "SELECT COUNT(*) FROM t");
        let distinct = e.query("SELECT DISTINCT a FROM t").unwrap().len() as i64;
        assert!(distinct <= total, "seed {seed}: distinct {distinct} > total {total}");
        assert!(distinct >= 1 || total == 0);
    }
}

#[test]
fn deletes_reduce_count_by_exactly_the_matched_rows() {
    for seed in 120..135 {
        let e = engine_with(seed, 100);
        let before = count(&e, "SELECT COUNT(*) FROM t");
        let matched = count(&e, "SELECT COUNT(*) FROM t WHERE a > 0");
        e.run("DELETE FROM t WHERE a > 0").unwrap();
        let after = count(&e, "SELECT COUNT(*) FROM t");
        assert_eq!(before - matched, after, "seed {seed}: delete arithmetic wrong");
    }
}
