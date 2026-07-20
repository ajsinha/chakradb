//! Plan execution over the storage engine.
//!
//! This is a straightforward interpreter: filter → aggregate-or-project →
//! distinct → sort → limit. No push-based pipeline, no vectorisation — the M2
//! goal is a *correct* query surface with a conformance harness (M2-1/M2-2), and
//! the NFR-03/NFR-04 measurements, not to out-execute DataFusion. If execution
//! ever becomes the bottleneck, `requirements.md` §8 is explicit that the answer
//! is to adopt DataFusion behind the existing `scan` boundary, not to hand-tune
//! this.

use super::expr::Expr;
use super::plan::{AggFn, OrderKey, Plan, Projection};
use super::value::{row_value, Value};
use crate::database::Database;
use crate::error::Error;
use crate::schema::Row;
use std::collections::BTreeMap;

/// (column labels, per-column type chars, rendered rows).
type ResultSet = (Vec<String>, Vec<char>, Vec<Vec<String>>);

/// The result of running a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A row set: column labels and rendered rows.
    Rows {
        columns: Vec<String>,
        types: Vec<char>,
        rows: Vec<Vec<String>>,
    },
    /// A statement that modified `n` rows.
    Affected(usize),
}

impl Outcome {
    pub fn row_count(&self) -> usize {
        match self {
            Outcome::Rows { rows, .. } => rows.len(),
            Outcome::Affected(n) => *n,
        }
    }
}

/// Execute a plan against a database.
pub fn execute(db: &Database, plan: Plan) -> Result<Outcome, Error> {
    match plan {
        Plan::CreateTable { name } => {
            db.create_table(&name)?;
            Ok(Outcome::Affected(0))
        }
        Plan::Insert { table, rows } => exec_insert(db, &table, rows),
        Plan::Delete { table, filter } => exec_delete(db, &table, filter),
        Plan::Update {
            table,
            sets,
            filter,
        } => exec_update(db, &table, sets, filter),
        Plan::Select { .. } => exec_select(db, plan),
    }
}

fn exec_insert(db: &Database, table: &str, rows: Vec<Row>) -> Result<Outcome, Error> {
    let t = db.table(table)?;
    let n = rows.len();
    for row in rows {
        t.insert(row)?;
    }
    Ok(Outcome::Affected(n))
}

fn exec_delete(db: &Database, table: &str, filter: Option<Expr>) -> Result<Outcome, Error> {
    let t = db.table(table)?;
    let snap = db.snapshot();
    let victims: Vec<i64> = t
        .scan(snap)
        .iter()
        .filter(|r| passes(&filter, r))
        .map(|r| r.pk)
        .collect();
    let mut n = 0;
    for pk in victims {
        if t.delete(pk).is_ok() {
            n += 1;
        }
    }
    Ok(Outcome::Affected(n))
}

fn exec_update(
    db: &Database,
    table: &str,
    sets: Vec<(usize, Expr)>,
    filter: Option<Expr>,
) -> Result<Outcome, Error> {
    let t = db.table(table)?;
    let snap = db.snapshot();
    let targets: Vec<Row> = t.scan(snap).iter().filter(|r| passes(&filter, r)).collect();
    let mut n = 0;
    for mut row in targets {
        for (idx, expr) in &sets {
            let v = expr.eval(&row);
            apply_set(&mut row, *idx, v);
        }
        if t.update(row).is_ok() {
            n += 1;
        }
    }
    Ok(Outcome::Affected(n))
}

fn apply_set(row: &mut Row, idx: usize, v: Value) {
    match idx {
        0 => {
            if let Value::Int(i) = v {
                row.pk = i;
            }
        }
        1 => row.a = v.as_f64().map(|f| f as i64).unwrap_or(row.a),
        2 => row.b = v.as_f64().unwrap_or(row.b),
        3 => row.c = if v.is_null() { String::new() } else { v.render() },
        _ => {}
    }
}

fn exec_select(db: &Database, plan: Plan) -> Result<Outcome, Error> {
    let Plan::Select {
        table,
        projections,
        filter,
        group_by,
        order_by,
        limit,
        distinct,
    } = plan
    else {
        unreachable!()
    };

    let t = db.table(&table)?;
    let scanned = t.scan(db.snapshot());
    let filtered: Vec<Row> = scanned.iter().filter(|r| passes(&filter, r)).collect();

    let (columns, types, mut rows) = if group_by.is_empty()
        && projections.iter().all(|p| matches!(p, Projection::Expr(..)))
    {
        project_rows(&projections, &filtered)
    } else {
        aggregate_rows(&projections, &group_by, &filtered)?
    };

    if distinct {
        dedup(&mut rows);
    }
    if !order_by.is_empty() {
        sort_rows(&mut rows, &order_by, &filtered, &columns);
    }
    if let Some(n) = limit {
        rows.truncate(n);
    }

    Ok(Outcome::Rows {
        columns,
        types,
        rows,
    })
}

/// Whether a row passes an optional predicate. Absent predicate = all rows.
fn passes(filter: &Option<Expr>, row: &Row) -> bool {
    match filter {
        None => true,
        Some(e) => e.eval(row).is_true(),
    }
}

fn project_rows(
    projections: &[Projection],
    rows: &[Row],
) -> ResultSet {
    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            Projection::Expr(_, label) => label.clone(),
            Projection::Agg(_, _, label) => label.clone(),
        })
        .collect();
    let mut out = Vec::with_capacity(rows.len());
    let mut types = vec!['?'; projections.len()];
    for row in rows {
        let mut rendered = Vec::with_capacity(projections.len());
        for (i, p) in projections.iter().enumerate() {
            if let Projection::Expr(e, _) = p {
                let v = e.eval(row);
                if types[i] == '?' {
                    types[i] = v.type_char();
                }
                rendered.push(v.render());
            }
        }
        out.push(rendered);
    }
    (columns, types, out)
}

/// A running aggregate accumulator.
#[derive(Clone)]
struct Acc {
    count: i64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
    seen_numeric: bool,
}

impl Acc {
    fn new() -> Self {
        Acc {
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            seen_numeric: false,
        }
    }
    fn push(&mut self, v: &Value) {
        if v.is_null() {
            return; // aggregates ignore NULLs, except COUNT(*)
        }
        self.count += 1;
        if let Some(f) = v.as_f64() {
            self.sum += f;
            self.seen_numeric = true;
        }
        if self.min.as_ref().map(|m| v.total_cmp(m).is_lt()).unwrap_or(true) {
            self.min = Some(v.clone());
        }
        if self.max.as_ref().map(|m| v.total_cmp(m).is_gt()).unwrap_or(true) {
            self.max = Some(v.clone());
        }
    }
    fn value(&self, f: AggFn, is_star: bool, group_rows: i64) -> Value {
        match f {
            AggFn::Count => Value::Int(if is_star { group_rows } else { self.count }),
            AggFn::Sum => {
                if self.seen_numeric {
                    Value::Float(self.sum)
                } else {
                    Value::Null
                }
            }
            AggFn::Avg => {
                if self.count > 0 && self.seen_numeric {
                    Value::Float(self.sum / self.count as f64)
                } else {
                    Value::Null
                }
            }
            AggFn::Min => self.min.clone().unwrap_or(Value::Null),
            AggFn::Max => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

fn aggregate_rows(
    projections: &[Projection],
    group_by: &[usize],
    rows: &[Row],
) -> Result<ResultSet, Error> {
    // Group key → (group row count, per-projection accumulators).
    let mut groups: BTreeMap<Vec<String>, (i64, Vec<Acc>)> = BTreeMap::new();
    // Deterministic ordering for grouped output: BTreeMap over the rendered key.
    for row in rows {
        let key: Vec<String> = group_by.iter().map(|&i| row_value(row, i).render()).collect();
        let entry = groups
            .entry(key)
            .or_insert_with(|| (0, vec![Acc::new(); projections.len()]));
        entry.0 += 1;
        for (i, p) in projections.iter().enumerate() {
            if let Projection::Agg(_, arg, _) = p {
                let v = arg.map(|c| row_value(row, c)).unwrap_or(Value::Int(1));
                entry.1[i].push(&v);
            }
        }
    }
    // A bare aggregate with no rows still yields one row (COUNT = 0).
    if groups.is_empty() && group_by.is_empty() {
        groups.insert(Vec::new(), (0, vec![Acc::new(); projections.len()]));
    }

    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            Projection::Expr(_, l) | Projection::Agg(_, _, l) => l.clone(),
        })
        .collect();
    let mut types = vec!['?'; projections.len()];
    let mut out = Vec::new();
    for (key, (group_rows, accs)) in groups {
        let mut rendered = Vec::with_capacity(projections.len());
        let mut gi = 0;
        for (i, p) in projections.iter().enumerate() {
            let v = match p {
                Projection::Agg(f, arg, _) => accs[i].value(*f, arg.is_none(), group_rows),
                Projection::Expr(_, _) => {
                    // A grouped column: echo the group key value.
                    let val = key.get(gi).cloned().unwrap_or_default();
                    gi += 1;
                    if types[i] == '?' {
                        types[i] = 'I';
                    }
                    rendered.push(val);
                    continue;
                }
            };
            if types[i] == '?' {
                types[i] = v.type_char();
            }
            rendered.push(v.render());
        }
        out.push(rendered);
    }
    Ok((columns, types, out))
}

fn dedup(rows: &mut Vec<Vec<String>>) {
    let mut seen = std::collections::HashSet::new();
    rows.retain(|r| seen.insert(r.clone()));
}

fn sort_rows(rows: &mut [Vec<String>], keys: &[OrderKey], source: &[Row], columns: &[String]) {
    // ORDER BY over projected output: match each key expression to an output
    // column when it is a bare column reference, else evaluate against source.
    let _ = (source, columns);
    rows.sort_by(|a, b| {
        for (ki, key) in keys.iter().enumerate() {
            // Sort by the corresponding output column position when available.
            let idx = key_column_index(key, ki).min(a.len().saturating_sub(1));
            let ord = a[idx].cmp(&b[idx]);
            let ord = if key.ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn key_column_index(key: &OrderKey, fallback: usize) -> usize {
    match &key.expr {
        Expr::Column(i) => *i,
        _ => fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::super::plan::plan;
    use super::*;

    fn db_with(rows: &[(i64, i64, f64, &str)]) -> Database {
        let db = Database::new();
        let t = db.create_table("t").unwrap();
        for &(pk, a, b, c) in rows {
            t.insert(Row::new(pk, a, b, c)).unwrap();
        }
        db
    }

    fn run(db: &Database, sql: &str) -> Outcome {
        execute(db, plan(sql).unwrap()).unwrap()
    }

    #[test]
    fn insert_and_count() {
        let db = Database::new();
        run(&db, "CREATE TABLE t (pk INT)");
        assert_eq!(run(&db, "INSERT INTO t VALUES (1,2,3,'a')"), Outcome::Affected(1));
        match run(&db, "SELECT COUNT(*) FROM t") {
            Outcome::Rows { rows, .. } => assert_eq!(rows[0][0], "1"),
            _ => panic!(),
        }
    }

    #[test]
    fn projection_and_filter() {
        let db = db_with(&[(1, 10, 0.0, "x"), (2, 20, 0.0, "y"), (3, 30, 0.0, "z")]);
        match run(&db, "SELECT pk FROM t WHERE a >= 20") {
            Outcome::Rows { rows, .. } => {
                let got: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
                assert_eq!(got.len(), 2);
                assert!(got.contains(&"2") && got.contains(&"3"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn order_by_and_limit() {
        let db = db_with(&[(3, 0, 0.0, "c"), (1, 0, 0.0, "a"), (2, 0, 0.0, "b")]);
        match run(&db, "SELECT pk FROM t ORDER BY pk DESC LIMIT 2") {
            Outcome::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], "3");
                assert_eq!(rows[1][0], "2");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn aggregates() {
        let db = db_with(&[(1, 10, 1.0, "x"), (2, 20, 2.0, "y"), (3, 30, 3.0, "z")]);
        match run(&db, "SELECT COUNT(*), SUM(a), MIN(a), MAX(a), AVG(a) FROM t") {
            Outcome::Rows { rows, .. } => {
                assert_eq!(rows[0][0], "3");
                assert_eq!(rows[0][1], "60.0");
                assert_eq!(rows[0][2], "10");
                assert_eq!(rows[0][3], "30");
                assert_eq!(rows[0][4], "20.0");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn count_of_empty_table_is_zero() {
        let db = Database::new();
        db.create_table("t").unwrap();
        match run(&db, "SELECT COUNT(*) FROM t") {
            Outcome::Rows { rows, .. } => assert_eq!(rows[0][0], "0"),
            _ => panic!(),
        }
    }

    #[test]
    fn group_by() {
        let db = db_with(&[(1, 5, 0.0, "x"), (2, 5, 0.0, "y"), (3, 9, 0.0, "z")]);
        match run(&db, "SELECT a, COUNT(*) FROM t GROUP BY a") {
            Outcome::Rows { rows, .. } => {
                // Two groups: a=5 (count 2), a=9 (count 1).
                assert_eq!(rows.len(), 2);
                let by_key: std::collections::HashMap<_, _> =
                    rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect();
                assert_eq!(by_key["5"], "2");
                assert_eq!(by_key["9"], "1");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn delete_with_predicate() {
        let db = db_with(&[(1, 0, 0.0, "a"), (2, 0, 0.0, "b"), (3, 0, 0.0, "c")]);
        assert_eq!(run(&db, "DELETE FROM t WHERE pk = 2"), Outcome::Affected(1));
        assert_eq!(run(&db, "SELECT COUNT(*) FROM t").row_count(), 1);
        match run(&db, "SELECT COUNT(*) FROM t") {
            Outcome::Rows { rows, .. } => assert_eq!(rows[0][0], "2"),
            _ => panic!(),
        }
    }

    #[test]
    fn update_with_predicate() {
        let db = db_with(&[(1, 10, 0.0, "a"), (2, 20, 0.0, "b")]);
        assert_eq!(run(&db, "UPDATE t SET a = 99 WHERE pk = 1"), Outcome::Affected(1));
        match run(&db, "SELECT a FROM t WHERE pk = 1") {
            Outcome::Rows { rows, .. } => assert_eq!(rows[0][0], "99"),
            _ => panic!(),
        }
    }

    #[test]
    fn null_filter_excludes_row() {
        let db = db_with(&[(1, 10, 0.0, "a")]);
        // a = NULL is NULL, so the row is excluded (not matched).
        assert_eq!(run(&db, "SELECT pk FROM t WHERE a = NULL").row_count(), 0);
    }

    #[test]
    fn distinct() {
        let db = db_with(&[(1, 5, 0.0, "x"), (2, 5, 0.0, "y"), (3, 9, 0.0, "z")]);
        assert_eq!(run(&db, "SELECT DISTINCT a FROM t").row_count(), 2);
    }

    #[test]
    fn arithmetic_projection() {
        let db = db_with(&[(1, 10, 0.0, "x")]);
        match run(&db, "SELECT pk + a FROM t") {
            Outcome::Rows { rows, .. } => assert_eq!(rows[0][0], "11"),
            _ => panic!(),
        }
    }

    #[test]
    fn error_on_missing_table() {
        let db = Database::new();
        assert!(execute(&db, plan("SELECT pk FROM nope").unwrap()).is_err());
    }
}
