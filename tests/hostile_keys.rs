//! Lookup fan-out under key distributions chosen to defeat the funnel.
//!
//! M0-3 measured point-lookup latency as flat from 1 to 33 parts, but did so
//! with dense sequential keys — the friendliest possible case, because each part
//! then covers a narrow, disjoint `[min, max]` range and stage 1 of the funnel
//! eliminates almost everything for free.
//!
//! `m0-findings.md` §3 recorded that as a caveat and made re-running it under a
//! hostile distribution a blocker for M1. This suite is that re-run.
//!
//! The adversarial case is **every part spanning the full key range**, which
//! makes bounds useless and forces a Bloom probe on every part. If correctness
//! or the Bloom filter fails here, §5.2's funnel does not hold in general.

use chakradb::{Database, Metrics, Row, Rng, TableConfig};

fn row(pk: i64, tag: &str) -> Row {
    // wrapping_mul: this suite deliberately uses i64::MIN / i64::MAX as keys.
    Row::new(pk, pk.wrapping_mul(2), pk as f64, tag)
}

fn table(seal: usize) -> (Database, std::sync::Arc<chakradb::Table>) {
    let db = Database::new();
    let t = db
        .create_table_with(
            "t",
            TableConfig {
                seal_threshold: seal,
                // Compaction off: we want to observe raw fan-out.
                compaction: chakradb::compaction::CompactionPolicy {
                    max_parts: usize::MAX,
                    max_dv_density: 1.1,
                    max_merge_width: 1,
                },
            },
        )
        .unwrap();
    (db, t)
}

/// Interleave keys so consecutive parts cover overlapping full-range spans.
#[test]
fn overlapping_ranges_defeat_bounds_but_bloom_holds() {
    let (db, t) = table(1_000);
    let n = 20_000i64;
    // Round-robin assignment: every part ends up spanning ~the whole range.
    let mut rng = Rng::new(1);
    let mut keys: Vec<i64> = (0..n).collect();
    rng.shuffle(&mut keys);
    for &pk in &keys {
        t.insert(row(pk, "v")).unwrap();
    }
    t.seal();

    let stats = t.stats();
    assert!(stats.num_parts >= 15, "expected many parts, got {}", stats.num_parts);

    // Every part should now span nearly the entire key space.
    let before = t.metrics().snapshot();
    let snap = db.snapshot();
    for pk in (0..n).step_by(37) {
        assert!(t.get(pk, snap).is_some(), "lost key {pk}");
    }
    let after = t.metrics().snapshot();

    let probes = (after.parts_probed - before.parts_probed) as f64;
    let lookups = (after.lookups - before.lookups).max(1) as f64;
    let bloom_skips = (after.bloom_skips - before.bloom_skips) as f64;
    let bounds_skips = (after.bounds_skips - before.bounds_skips) as f64;

    // The point of the test: bounds are now useless, and the Bloom filter is
    // carrying the funnel on its own.
    assert!(
        bloom_skips > bounds_skips,
        "expected Bloom to dominate under overlap: bloom={bloom_skips} bounds={bounds_skips}"
    );
    assert!(
        bloom_skips / probes.max(1.0) > 0.5,
        "Bloom eliminated only {:.1}% of probes",
        100.0 * bloom_skips / probes.max(1.0)
    );
    assert!(probes / lookups > 1.0, "fan-out should be real here");
}

#[test]
fn absent_keys_are_rejected_cheaply_under_overlap() {
    // The worst case for a fan-out design: a key that exists nowhere still has
    // to be refused by every part.
    let (db, t) = table(500);
    let mut rng = Rng::new(2);
    let mut keys: Vec<i64> = (0..10_000).map(|i| i * 2).collect(); // evens only
    rng.shuffle(&mut keys);
    for &pk in &keys {
        t.insert(row(pk, "v")).unwrap();
    }
    t.seal();

    let before = t.metrics().snapshot();
    let snap = db.snapshot();
    // Odd keys only — every one is guaranteed absent. (Stepping by an odd
    // stride from an odd start would alternate parity and hit real keys.)
    for pk in (1..20_000).step_by(2).filter(|k| k % 202 == 1) {
        assert!(t.get(pk, snap).is_none(), "phantom key {pk}");
    }
    let after = t.metrics().snapshot();

    let probes = (after.parts_probed - before.parts_probed) as f64;
    let skips = (after.bloom_skips - before.bloom_skips + after.bounds_skips - before.bounds_skips)
        as f64;
    assert!(
        skips / probes.max(1.0) > 0.9,
        "absent-key probes should be eliminated before data: {:.1}%",
        100.0 * skips / probes.max(1.0)
    );
}

#[test]
fn clustered_hot_keys_still_resolve_correctly() {
    // Repeated updates to a tiny hot set: every part contains the same keys,
    // so bounds overlap completely and version resolution does the work.
    let (db, t) = table(200);
    for pk in 0..50i64 {
        t.insert(row(pk, "v0")).unwrap();
    }
    for round in 1..40 {
        for pk in 0..50i64 {
            t.upsert(row(pk, &format!("v{round}"))).unwrap();
        }
    }
    t.seal();

    let snap = db.snapshot();
    assert_eq!(t.row_count(snap), 50, "hot-set updates changed row count");
    for pk in 0..50i64 {
        assert_eq!(
            t.get(pk, snap).unwrap().c,
            "v39",
            "stale version survived for {pk}"
        );
    }
}

#[test]
fn extreme_key_values_do_not_break_bounds() {
    let (db, t) = table(100);
    let keys = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    for (i, &pk) in keys.iter().enumerate() {
        t.insert(row(pk, &format!("k{i}"))).unwrap();
    }
    // Pad so the extremes land in a part alongside ordinary keys.
    for pk in 100..500i64 {
        t.insert(row(pk, "pad")).unwrap();
    }
    t.seal();

    let snap = db.snapshot();
    for (i, &pk) in keys.iter().enumerate() {
        assert_eq!(t.get(pk, snap).unwrap().c, format!("k{i}"), "lost {pk}");
    }
    assert!(t.get(12_345, snap).is_none());
}

#[test]
fn fanout_grows_but_lookups_stay_correct_at_scale() {
    // 60 overlapping parts — well past the point where bounds help at all.
    let (db, t) = table(400);
    let mut rng = Rng::new(3);
    let n = 24_000i64;
    let mut keys: Vec<i64> = (0..n).collect();
    rng.shuffle(&mut keys);
    for &pk in &keys {
        t.insert(row(pk, "v")).unwrap();
    }
    t.seal();
    assert!(t.stats().num_parts >= 50, "want deep fan-out for this test");

    let snap = db.snapshot();
    let mut checked = 0;
    for pk in (0..n).step_by(13) {
        assert!(t.get(pk, snap).is_some(), "lost {pk} at depth");
        checked += 1;
    }
    assert!(checked > 1_000);
    assert_eq!(t.row_count(snap), n as usize);

    let m = t.metrics();
    assert!(
        m.skip_ratio() > 0.5,
        "funnel collapsed under overlap: only {:.1}% of probes skipped",
        m.skip_ratio() * 100.0
    );
    let _ = Metrics::get(&m.lookups);
}
