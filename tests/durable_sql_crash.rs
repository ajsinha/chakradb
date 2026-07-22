//! Crash consistency for **durable SQL** — the newest, least-battle-tested path.
//!
//! `crash_consistency.rs` hammers the storage layer directly. This does the same
//! to `SqlEngine::durable(Storage)`: every write is a SQL statement (WAL-logged
//! through the engine), a crash is injected at a seeded point, and after reopen
//! every acknowledged write must survive — across arbitrary schemas (int PK,
//! text PK, keyless rowid) the storage-layer suite never exercised.
//!
//! Writes go through SQL; verification reads the table directly, so the loop is
//! fast and isolates the durable-write path.
//!
//! ```sh
//! CHAKRA_CRASH_TRIALS=10000 cargo test --release --test durable_sql_crash
//! ```

use chakradb::io::MemIo;
use chakradb::{Durability, Rng, SqlEngine, Storage, StorageConfig, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn config() -> StorageConfig {
    StorageConfig {
        durability: Durability::Group,
        checkpoint_wal_bytes: u64::MAX,
        ..Default::default()
    }
}

fn trial_count(default: u64) -> u64 {
    std::env::var("CHAKRA_CRASH_TRIALS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A durable-SQL workload over an int-PK table, crashed at a seeded point.
fn int_pk_trial(seed: u64, ops: usize) -> usize {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let mut expected: HashMap<i64, (String, i64)> = HashMap::new();
    let mut rng = Rng::new(seed);
    let mut acked = 0usize;

    {
        let s = Arc::new(Storage::open(io.clone(), config()).unwrap());
        let e = SqlEngine::durable(s);
        e.run("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, n INT)")
            .unwrap();

        let crash_at = rng.below(ops as u64) as usize;
        for i in 0..ops {
            if i == crash_at {
                break;
            }
            let pk = rng.range(0, 200);
            if rng.chance(0.7) {
                let tag = format!("v{i}");
                let n = i as i64;
                let sql = if expected.contains_key(&pk) {
                    format!("UPDATE t SET tag = '{tag}', n = {n} WHERE id = {pk}")
                } else {
                    format!("INSERT INTO t VALUES ({pk}, '{tag}', {n})")
                };
                if e.run(&sql).is_ok() {
                    expected.insert(pk, (tag, n));
                    acked += 1;
                }
            } else if e.run(&format!("DELETE FROM t WHERE id = {pk}")).is_ok()
                && expected.remove(&pk).is_some()
            {
                acked += 1;
            }
        }
        io.crash(); // power cut
    }

    // Reopen and verify every acknowledged write survived exactly.
    let s2 = Storage::open(io, config()).unwrap();
    let t = s2.database().table("t").unwrap();
    let snap = s2.database().snapshot();
    for (pk, (tag, n)) in &expected {
        let got = t
            .get(&Value::Int(*pk), snap)
            .unwrap_or_else(|| panic!("seed {seed}: acked id={pk} lost after crash"));
        assert_eq!(
            got.get(1),
            &Value::Text(tag.clone()),
            "seed {seed}: id={pk} stale tag"
        );
        assert_eq!(got.get(2), &Value::Int(*n), "seed {seed}: id={pk} stale n");
    }
    for pk in 0..200i64 {
        if !expected.contains_key(&pk) {
            assert!(
                t.get(&Value::Int(pk), snap).is_none(),
                "seed {seed}: deleted/never-written id={pk} came back"
            );
        }
    }
    acked
}

/// A durable-SQL workload over a **text-PK** table.
fn text_pk_trial(seed: u64, ops: usize) -> usize {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let mut expected: HashMap<String, i64> = HashMap::new();
    let mut rng = Rng::new(seed);
    let mut acked = 0usize;

    {
        let s = Arc::new(Storage::open(io.clone(), config()).unwrap());
        let e = SqlEngine::durable(s);
        e.run("CREATE TABLE users (name TEXT PRIMARY KEY, n INT)")
            .unwrap();

        let crash_at = rng.below(ops as u64) as usize;
        for i in 0..ops {
            if i == crash_at {
                break;
            }
            let name = format!("k{}", rng.range(0, 150));
            if rng.chance(0.7) {
                let n = i as i64;
                let sql = if expected.contains_key(&name) {
                    format!("UPDATE users SET n = {n} WHERE name = '{name}'")
                } else {
                    format!("INSERT INTO users VALUES ('{name}', {n})")
                };
                if e.run(&sql).is_ok() {
                    expected.insert(name, n);
                    acked += 1;
                }
            } else if e
                .run(&format!("DELETE FROM users WHERE name = '{name}'"))
                .is_ok()
                && expected.remove(&name).is_some()
            {
                acked += 1;
            }
        }
        io.crash();
    }

    let s2 = Storage::open(io, config()).unwrap();
    let t = s2.database().table("users").unwrap();
    let snap = s2.database().snapshot();
    for (name, n) in &expected {
        let got = t
            .get(&Value::Text(name.clone()), snap)
            .unwrap_or_else(|| panic!("seed {seed}: acked name={name} lost"));
        assert_eq!(
            got.get(1),
            &Value::Int(*n),
            "seed {seed}: name={name} stale"
        );
    }
    acked
}

/// A durable-SQL insert-only workload over a **keyless (rowid)** table: rows have
/// no client key, so verify by count and internal consistency.
fn rowid_trial(seed: u64, ops: usize) -> usize {
    let io: Arc<MemIo> = Arc::new(MemIo::new());
    let mut rng = Rng::new(seed);
    let mut acked = 0usize;

    {
        let s = Arc::new(Storage::open(io.clone(), config()).unwrap());
        let e = SqlEngine::durable(s);
        e.run("CREATE TABLE log (msg TEXT, level INT)").unwrap();

        let crash_at = rng.below(ops as u64) as usize;
        for i in 0..ops {
            if i == crash_at {
                break;
            }
            let sql = format!("INSERT INTO log VALUES ('m{i}', {})", rng.range(0, 5));
            if e.run(&sql).is_ok() {
                acked += 1;
            }
        }
        io.crash();
    }

    let s2 = Storage::open(io, config()).unwrap();
    let t = s2.database().table("log").unwrap();
    let snap = s2.database().snapshot();
    let n = t.row_count(snap);
    // Every acknowledged insert survived, and the state is internally consistent.
    assert_eq!(n, acked, "seed {seed}: rowid table lost/gained rows");
    assert_eq!(
        t.scan(snap).len(),
        n,
        "seed {seed}: inconsistent after recovery"
    );
    acked
}

#[test]
fn durable_sql_int_pk_survives_crashes() {
    let n = trial_count(200);
    let mut total = 0;
    for seed in 0..n {
        total += int_pk_trial(seed, 400);
    }
    assert!(total > 0, "workload never acknowledged anything");
    eprintln!("durable-SQL int-PK: {n} crash trials, {total} acked writes verified");
}

#[test]
fn durable_sql_text_pk_survives_crashes() {
    let n = trial_count(200);
    let mut total = 0;
    for seed in 1_000_000..1_000_000 + n {
        total += text_pk_trial(seed, 300);
    }
    assert!(total > 0);
    eprintln!("durable-SQL text-PK: {n} crash trials, {total} acked writes verified");
}

#[test]
fn durable_sql_rowid_table_survives_crashes() {
    let n = trial_count(200);
    let mut total = 0;
    for seed in 2_000_000..2_000_000 + n {
        total += rowid_trial(seed, 300);
    }
    assert!(total > 0);
    eprintln!("durable-SQL rowid: {n} crash trials, {total} acked inserts verified");
}
