//! M2-1: a sqllogictest-style conformance harness.
//!
//! The roadmap points at `apache/datafusion-testing`'s 595 pre-converted `.slt`
//! files. Those assume a general schema (`CREATE TABLE t(a,b,c,...)`), which is
//! outside M2's fixed four-column subset, so importing them wholesale would test
//! nothing but our `CREATE TABLE` rejection. Instead this runs the *same file
//! format* — the SQLite sqllogictest grammar — over a corpus written against our
//! documented subset. The parser and directives are real; only the corpus is
//! ours, and it is version-controlled so results can be re-run (`§10.2`).
//!
//! Directives supported: `statement ok`, `statement error`, `query <types>` with
//! an expected result block, `halt`, and `#` comments — the subset of the
//! grammar our corpus needs.

use chakradb::{Database, SqlEngine};
use std::sync::Arc;

/// One parsed record from a `.slt` script.
#[derive(Debug)]
enum Record {
    StatementOk(String),
    StatementError(String),
    Query {
        sql: String,
        sort: bool,
        expected: Vec<String>,
    },
}

fn parse(script: &str) -> Vec<Record> {
    let mut records = Vec::new();
    let mut lines = script.lines().peekable();
    while let Some(line) = lines.next() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line == "halt" {
            break;
        }
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("statement") => {
                let ok = parts.next() == Some("ok");
                let sql = collect_sql(&mut lines);
                records.push(if ok {
                    Record::StatementOk(sql)
                } else {
                    Record::StatementError(sql)
                });
            }
            Some("query") => {
                let sort = line.contains("rowsort");
                let sql = collect_sql(&mut lines);
                // Expected rows follow until a blank line or `----` already
                // consumed; read until blank.
                let mut expected = Vec::new();
                for l in lines.by_ref() {
                    if l.trim().is_empty() {
                        break;
                    }
                    expected.push(l.trim().to_string());
                }
                records.push(Record::Query {
                    sql,
                    sort,
                    expected,
                });
            }
            _ => {}
        }
    }
    records
}

/// Collect SQL lines up to the `----` separator (or a blank line for a
/// statement with no result block).
fn collect_sql<'a>(lines: &mut std::iter::Peekable<impl Iterator<Item = &'a str>>) -> String {
    let mut sql = Vec::new();
    for l in lines.by_ref() {
        let t = l.trim();
        if t == "----" || t.is_empty() {
            break;
        }
        sql.push(t.to_string());
    }
    sql.join(" ")
}

fn run_script(script: &str) {
    let engine = SqlEngine::new(Arc::new(Database::new()));
    for (i, rec) in parse(script).into_iter().enumerate() {
        match rec {
            Record::StatementOk(sql) => {
                engine
                    .run(&sql)
                    .unwrap_or_else(|e| panic!("record {i}: `{sql}` should succeed: {e}"));
            }
            Record::StatementError(sql) => {
                assert!(
                    engine.run(&sql).is_err(),
                    "record {i}: `{sql}` should have failed"
                );
            }
            Record::Query {
                sql,
                sort,
                expected,
            } => {
                let rows = engine
                    .query(&sql)
                    .unwrap_or_else(|e| panic!("record {i}: `{sql}`: {e}"));
                let mut got: Vec<String> =
                    rows.iter().map(|r| r.join(" ")).collect();
                let mut want = expected.clone();
                if sort {
                    got.sort();
                    want.sort();
                }
                assert_eq!(got, want, "record {i}: `{sql}` produced wrong rows");
            }
        }
    }
}

const BASICS: &str = "
# Basic DDL/DML/query round-trip over the fixed schema.
statement ok
CREATE TABLE t (pk INT)

statement ok
INSERT INTO t VALUES (1, 10, 1.5, 'alice')

statement ok
INSERT INTO t VALUES (2, 20, 2.5, 'bob')

statement ok
INSERT INTO t VALUES (3, 30, 3.5, 'carol')

query I
SELECT COUNT(*) FROM t
----
3

query T rowsort
SELECT c FROM t WHERE a >= 20
----
bob
carol

query I
SELECT pk FROM t ORDER BY pk DESC LIMIT 1
----
3
";

const AGGREGATES: &str = "
statement ok
CREATE TABLE t (pk INT)

statement ok
INSERT INTO t VALUES (1, 10, 1.0, 'x')

statement ok
INSERT INTO t VALUES (2, 20, 2.0, 'y')

statement ok
INSERT INTO t VALUES (3, 30, 3.0, 'z')

query R
SELECT SUM(b) FROM t
----
6.0

query I
SELECT MIN(a) FROM t
----
10

query I
SELECT MAX(a) FROM t
----
30

query R
SELECT AVG(a) FROM t
----
20.0
";

const THREE_VALUED_LOGIC: &str = "
statement ok
CREATE TABLE t (pk INT)

statement ok
INSERT INTO t VALUES (1, 10, 0.0, 'a')

statement ok
INSERT INTO t VALUES (2, 20, 0.0, 'b')

# a = NULL is NULL, so nothing matches.
query I
SELECT COUNT(*) FROM t WHERE a = NULL
----
0

# a IS NOT NULL matches everything.
query I
SELECT COUNT(*) FROM t WHERE a IS NOT NULL
----
2
";

const MUTATIONS: &str = "
statement ok
CREATE TABLE t (pk INT)

statement ok
INSERT INTO t VALUES (1, 10, 0.0, 'old')

statement ok
UPDATE t SET c = 'new' WHERE pk = 1

query T
SELECT c FROM t
----
new

statement ok
INSERT INTO t VALUES (2, 20, 0.0, 'gone')

statement ok
DELETE FROM t WHERE pk = 2

query I
SELECT COUNT(*) FROM t
----
1
";

const ERRORS: &str = "
statement ok
CREATE TABLE t (pk INT)

# Duplicate primary key.
statement ok
INSERT INTO t VALUES (1, 1, 1.0, 'x')

statement error
INSERT INTO t VALUES (1, 2, 2.0, 'y')

# Unknown table.
statement error
SELECT pk FROM nope

# Unsupported: joins.
statement error
SELECT pk FROM t JOIN t AS u ON t.pk = u.pk

# Syntax error.
statement error
SELCT WHATEVER
";

const GROUPING: &str = "
statement ok
CREATE TABLE t (pk INT)

statement ok
INSERT INTO t VALUES (1, 5, 0.0, 'x')

statement ok
INSERT INTO t VALUES (2, 5, 0.0, 'y')

statement ok
INSERT INTO t VALUES (3, 9, 0.0, 'z')

query II rowsort
SELECT a, COUNT(*) FROM t GROUP BY a
----
5 2
9 1

query I
SELECT DISTINCT a FROM t ORDER BY a
----
5
9
";

#[test]
fn slt_basics() {
    run_script(BASICS);
}

#[test]
fn slt_aggregates() {
    run_script(AGGREGATES);
}

#[test]
fn slt_three_valued_logic() {
    run_script(THREE_VALUED_LOGIC);
}

#[test]
fn slt_mutations() {
    run_script(MUTATIONS);
}

#[test]
fn slt_errors() {
    run_script(ERRORS);
}

#[test]
fn slt_grouping() {
    run_script(GROUPING);
}

#[test]
fn parser_handles_comments_and_blank_lines() {
    let script = "
# a comment
statement ok
CREATE TABLE t (pk INT)


# another comment after blanks
query I
SELECT COUNT(*) FROM t
----
0
";
    run_script(script);
}

#[test]
fn halt_stops_processing() {
    // Everything after `halt` is ignored, so the bad statement never runs.
    let script = "
statement ok
CREATE TABLE t (pk INT)

halt

statement ok
THIS WOULD FAIL
";
    run_script(script);
}
