//! COPY <table> FROM '<file>' — bulk CSV ingest.

use chakradb::io::{Io, MemIo};
use chakradb::storage::{Storage, StorageConfig};
use chakradb::{Database, SqlEngine};
use std::io::Write;
use std::sync::Arc;

fn eng() -> SqlEngine {
    SqlEngine::new(Arc::new(Database::new()))
}
fn one(e: &SqlEngine, sql: &str) -> String {
    e.query(sql).unwrap()[0][0].clone()
}

/// Write `contents` to a fresh temp file and return its path.
fn tmp_csv(name: &str, contents: &str) -> String {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("chakra-copy-{}-{}.csv", std::process::id(), name));
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn copy_loads_a_csv() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)")
        .unwrap();
    let csv = tmp_csv("basic", "1,alice,9.5\n2,bob,7.25\n3,carol,10.0\n");
    let n = e.run(&format!("COPY t FROM '{csv}'")).unwrap();
    assert_eq!(n.row_count(), 3);
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "3");
    assert_eq!(one(&e, "SELECT name FROM t WHERE id = 2"), "bob");
    assert_eq!(one(&e, "SELECT SUM(score) FROM t"), "26.75");
    std::fs::remove_file(csv).ok();
}

#[test]
fn copy_with_header_and_delimiter() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, city TEXT)").unwrap();
    let csv = tmp_csv("hdr", "id|city\n1|Paris\n2|Berlin\n");
    e.run(&format!(
        "COPY t FROM '{csv}' WITH (FORMAT CSV, HEADER true, DELIMITER '|')"
    ))
    .unwrap();
    assert_eq!(one(&e, "SELECT city FROM t WHERE id = 1"), "Paris");
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), "2");
    std::fs::remove_file(csv).ok();
}

#[test]
fn copy_handles_quotes_and_nulls() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, note TEXT, tag TEXT)")
        .unwrap();
    // Row 1: a quoted field containing the delimiter and an escaped quote.
    // Row 2: an unquoted empty field → NULL; a quoted empty field → "".
    let csv = tmp_csv("q", "1,\"a,b \"\"c\"\"\",x\n2,,\"\"\n");
    e.run(&format!("COPY t FROM '{csv}'")).unwrap();
    assert_eq!(one(&e, "SELECT note FROM t WHERE id = 1"), r#"a,b "c""#);
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t WHERE id = 2 AND note IS NULL"), "1");
    assert_eq!(one(&e, "SELECT tag FROM t WHERE id = 2"), "");
    std::fs::remove_file(csv).ok();
}

#[test]
fn copy_column_list_and_defaults() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, status TEXT DEFAULT 'new')")
        .unwrap();
    let csv = tmp_csv("cols", "1,alice\n2,bob\n");
    e.run(&format!("COPY t (id, name) FROM '{csv}'")).unwrap();
    // The uncovered column takes its DEFAULT.
    assert_eq!(one(&e, "SELECT status FROM t WHERE id = 1"), "new");
    std::fs::remove_file(csv).ok();
}

#[test]
fn copy_enforces_constraints_and_types() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))")
        .unwrap();
    let bad = tmp_csv("bad", "1,-5\n");
    assert!(e.run(&format!("COPY t FROM '{bad}'")).is_err(), "CHECK enforced");
    let bad_type = tmp_csv("badty", "notanint,10\n");
    assert!(
        e.run(&format!("COPY t FROM '{bad_type}'")).is_err(),
        "type mismatch rejected"
    );
    std::fs::remove_file(bad).ok();
    std::fs::remove_file(bad_type).ok();
}

#[test]
fn copy_is_durable() {
    let io: Arc<dyn Io> = Arc::new(MemIo::new());
    let csv = tmp_csv("durable", "1,apple\n2,pear\n3,kiwi\n");
    {
        let e = SqlEngine::durable(Arc::new(
            Storage::open(io.clone(), StorageConfig::default()).unwrap(),
        ));
        e.run("CREATE TABLE fruit (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        e.run(&format!("COPY fruit FROM '{csv}'")).unwrap();
        assert_eq!(one(&e, "SELECT COUNT(*) FROM fruit"), "3");
    }
    // Reopen: the bulk-loaded rows were WAL-logged and survive.
    let e2 = SqlEngine::durable(Arc::new(
        Storage::open(io, StorageConfig::default()).unwrap(),
    ));
    assert_eq!(one(&e2, "SELECT COUNT(*) FROM fruit"), "3");
    assert_eq!(one(&e2, "SELECT name FROM fruit WHERE id = 3"), "kiwi");
    std::fs::remove_file(csv).ok();
}

#[test]
fn copy_to_is_rejected() {
    let e = eng();
    e.run("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    assert!(e.run("COPY t TO '/tmp/out.csv'").is_err());
}
