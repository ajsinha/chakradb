//! Plan execution over the storage engine.
//!
//! This is a straightforward interpreter: filter → aggregate-or-project →
//! distinct → sort → limit. No push-based pipeline, no vectorisation — the M2
//! goal is a *correct* query surface with a conformance harness (M2-1/M2-2), and
//! the NFR-03/NFR-04 measurements, not to out-execute DataFusion. If execution
//! ever becomes the bottleneck, `requirements.md` §8 is explicit that the answer
//! is to adopt DataFusion behind the existing `scan` boundary, not to hand-tune
//! this.

use super::backend::SqlBackend;
use super::expr::{BinaryOp, Expr};
use super::plan::{AggFn, OrderKey, Plan, Projection};
use super::value::{batch_value, Value};
use crate::batch::Batch;
use crate::error::Error;
use crate::schema::{Row, Schema};
use crate::value::DataType;
use crate::table::Segment;
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

/// Execute a plan against a catalog (in-memory or durable).
pub fn execute(be: &dyn SqlBackend, plan: Plan) -> Result<Outcome, Error> {
    match plan {
        Plan::CreateTable { name, schema } => {
            be.create_table(&name, schema)?;
            Ok(Outcome::Affected(0))
        }
        Plan::Insert { table, rows } => exec_insert(be, &table, rows),
        Plan::Delete { table, filter } => exec_delete(be, &table, filter),
        Plan::Update {
            table,
            sets,
            filter,
        } => exec_update(be, &table, sets, filter),
        Plan::Select { .. } => exec_select(be, plan),
        Plan::Copy { .. } => exec_copy(be, plan),
    }
}

fn exec_insert(be: &dyn SqlBackend, table: &str, rows: Vec<Row>) -> Result<Outcome, Error> {
    let n = rows.len();
    for row in rows {
        be.insert(table, row)?;
    }
    Ok(Outcome::Affected(n))
}

/// One parsed CSV field, tracking whether it was quoted — an *unquoted* empty
/// field is a NULL, a quoted `""` is the empty string.
struct CsvField {
    text: String,
    quoted: bool,
}

/// Split a CSV line into fields, honouring double-quoted fields (with `""` as an
/// escaped quote). `delim`/`quote` are ASCII; parsing is over `char`s so UTF-8
/// content survives. Embedded newlines inside quotes are not supported.
fn parse_csv_line(line: &str, delim: char, quote: char) -> Vec<CsvField> {
    let mut out = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        let mut buf = String::new();
        let quoted = chars.peek() == Some(&quote);
        if quoted {
            chars.next();
            while let Some(c) = chars.next() {
                if c == quote {
                    if chars.peek() == Some(&quote) {
                        buf.push(quote);
                        chars.next();
                    } else {
                        break; // closing quote
                    }
                } else {
                    buf.push(c);
                }
            }
            // Ignore anything between the closing quote and the next delimiter.
            while let Some(&c) = chars.peek() {
                if c == delim {
                    break;
                }
                chars.next();
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c == delim {
                    break;
                }
                buf.push(c);
                chars.next();
            }
        }
        out.push(CsvField { text: buf, quoted });
        match chars.peek() {
            Some(&c) if c == delim => {
                chars.next();
            }
            _ => break,
        }
    }
    out
}

/// Parse a CSV text field into a value of the target column type. Returns `None`
/// if the text is not a valid value for that type.
fn parse_field(text: &str, ty: DataType) -> Option<Value> {
    match ty {
        DataType::Int => text.trim().parse::<i64>().ok().map(Value::Int),
        DataType::Float => text.trim().parse::<f64>().ok().map(Value::Float),
        DataType::Bool => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" | "yes" | "y" => Some(Value::Bool(true)),
            "false" | "f" | "0" | "no" | "n" => Some(Value::Bool(false)),
            _ => None,
        },
        DataType::Text => Some(Value::Text(text.to_string())),
        // Date/Timestamp/Decimal reuse the value coercions (which parse exactly
        // and enforce range/precision).
        DataType::Date | DataType::Timestamp | DataType::Decimal(..) => {
            Value::Text(text.to_string()).coerce(ty)
        }
    }
}

/// `COPY <table> FROM '<path>'` — stream a CSV file, coerce and constraint-check
/// each row, and bulk-load it in chunks (so a huge file never fully materialises
/// in memory). Rows must have new keys, like the other bulk paths.
fn exec_copy(be: &dyn SqlBackend, plan: Plan) -> Result<Outcome, Error> {
    use std::io::BufRead;
    let Plan::Copy {
        table,
        columns,
        path,
        delimiter,
        quote,
        header,
        null_marker,
    } = plan
    else {
        unreachable!()
    };
    let schema = be.table(&table)?.schema().clone();
    let checks = super::plan::planned_checks(&schema).map_err(Error::Sql)?;
    // Defaults for columns not covered by the COPY column list, applied once.
    let default_slots: Vec<(usize, Value)> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, c)| !columns.contains(i) && c.default.is_some())
        .map(|(i, c)| (i, c.default.clone().unwrap()))
        .collect();

    let file = std::fs::File::open(&path)
        .map_err(|e| Error::Sql(format!("COPY cannot open {path}: {e}")))?;
    let mut reader = std::io::BufReader::new(file);
    let (delim, quot) = (delimiter as char, quote as char);

    const CHUNK: usize = 256 * 1024;
    let mut batch: Vec<Row> = Vec::new();
    let mut total = 0usize;
    let mut line = String::new();
    let mut lineno = 0usize;
    let mut skip_header = header;
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| Error::Sql(format!("COPY read error: {e}")))?;
        if read == 0 {
            break;
        }
        lineno += 1;
        let raw = line.trim_end_matches(['\n', '\r']);
        if skip_header {
            skip_header = false;
            continue;
        }
        if raw.is_empty() {
            continue;
        }
        let fields = parse_csv_line(raw, delim, quot);
        if fields.len() != columns.len() {
            return Err(Error::Sql(format!(
                "COPY line {lineno}: expected {} fields, got {}",
                columns.len(),
                fields.len()
            )));
        }
        let mut values = vec![Value::Null; schema.arity()];
        for (i, v) in &default_slots {
            values[*i] = v.clone();
        }
        for (field, &slot) in fields.iter().zip(&columns) {
            let c = schema.column(slot);
            values[slot] = if !field.quoted && field.text == null_marker {
                Value::Null
            } else {
                parse_field(&field.text, c.ty).ok_or_else(|| {
                    Error::Sql(format!(
                        "COPY line {lineno}: {:?} is not a valid {} for column {}",
                        field.text,
                        c.ty.name(),
                        c.name
                    ))
                })?
            };
        }
        let row = Row::from_values(values);
        super::plan::enforce_constraints(&schema, &checks, &row)?;
        batch.push(row);
        if batch.len() >= CHUNK {
            total += be.bulk_insert(&table, std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        total += be.bulk_insert(&table, batch)?;
    }
    Ok(Outcome::Affected(total))
}

fn exec_delete(be: &dyn SqlBackend, table: &str, filter: Option<Expr>) -> Result<Outcome, Error> {
    let t = be.table(table)?;
    let _snap_pin = be.pin();
    let snap = _snap_pin.snapshot();
    let ki = t.schema().key_index();
    let victims: Vec<Value> = t
        .scan(snap)
        .iter()
        .filter(|r| passes(&filter, r))
        .map(|r| r.key(ki).clone())
        .collect();
    let mut n = 0;
    for key in victims {
        if be.delete(table, &key).is_ok() {
            n += 1;
        }
    }
    Ok(Outcome::Affected(n))
}

fn exec_update(
    be: &dyn SqlBackend,
    table: &str,
    sets: Vec<(usize, Expr)>,
    filter: Option<Expr>,
) -> Result<Outcome, Error> {
    let t = be.table(table)?;
    let _snap_pin = be.pin();
    let snap = _snap_pin.snapshot();
    let schema = t.schema().clone();
    let checks = super::plan::planned_checks(&schema).map_err(Error::Sql)?;
    let targets: Vec<Row> = t.scan(snap).iter().filter(|r| passes(&filter, r)).collect();
    // Compute and constraint-check every updated row *before* applying any, so a
    // violation aborts the whole statement instead of leaving a partial update.
    let mut updated = Vec::with_capacity(targets.len());
    for mut row in targets {
        for (idx, expr) in &sets {
            let v = expr.eval(&row);
            let ty = schema.column(*idx).ty;
            row.values[*idx] = v.coerce(ty).unwrap_or(Value::Null);
        }
        super::plan::enforce_constraints(&schema, &checks, &row)?;
        updated.push(row);
    }
    let mut n = 0;
    for row in updated {
        if be.update(table, row).is_ok() {
            n += 1;
        }
    }
    Ok(Outcome::Affected(n))
}

fn exec_select(be: &dyn SqlBackend, plan: Plan) -> Result<Outcome, Error> {
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

    let t = be.table(&table)?;
    let _snap_pin = be.pin();
    let snap = _snap_pin.snapshot();
    // Output type per projection, so DATE/TIMESTAMP columns render as date
    // strings rather than their epoch integers. `render_as` is a no-op for every
    // other type, so a neutral default is safe for computed expressions.
    let out_types = projection_types(&projections, t.schema());

    // Fast path: `SELECT COUNT(*) FROM t` with no filter answers from metadata,
    // the way DuckDB does — no scan at all. This is the single most common
    // analytical probe, and scanning millions of rows to count them is waste.
    if is_bare_count_star(&projections, &filter, &group_by, &order_by, distinct) {
        let label = match &projections[0] {
            Projection::Agg(_, _, l) => l.clone(),
            _ => "count".to_string(),
        };
        let n = t.row_count(snap);
        return Ok(Outcome::Rows {
            columns: vec![label],
            types: vec!['I'],
            rows: vec![vec![n.to_string()]],
        });
    }

    // Metadata fast path: bare `MIN(col)` / `MAX(col)` with no filter or grouping
    // is answered from per-part column zonemaps — no scan for clean parts. This
    // is the DuckDB-style min/max shortcut.
    if is_bare_minmax(&projections, &filter, &group_by, distinct) {
        let columns: Vec<String> = projections
            .iter()
            .map(|p| match p {
                Projection::Expr(_, l) | Projection::Agg(_, _, l) => l.clone(),
            })
            .collect();
        let mut types = Vec::with_capacity(projections.len());
        let mut rendered = Vec::with_capacity(projections.len());
        for p in &projections {
            let Projection::Agg(f, Some(col), _) = p else {
                unreachable!()
            };
            let v = match (f, t.column_minmax(*col, snap)) {
                (AggFn::Min, Some((mn, _))) => mn,
                (AggFn::Max, Some((_, mx))) => mx,
                _ => Value::Null, // empty / all-NULL column
            };
            types.push(v.type_char());
            rendered.push(v.render_as(out_types[rendered.len()]));
        }
        return Ok(Outcome::Rows {
            columns,
            types,
            rows: vec![rendered],
        });
    }

    // Point-lookup fast path: `WHERE key = literal` with no grouping/aggregation
    // resolves through the index funnel (bounds → bloom → binary search) instead
    // of scanning — O(log n), the transactional read path.
    if let Some(key) = point_lookup_key(
        &filter,
        &group_by,
        distinct,
        &projections,
        t.schema().key_index(),
    ) {
        let columns: Vec<String> = projections
            .iter()
            .map(|p| match p {
                Projection::Expr(_, l) | Projection::Agg(_, _, l) => l.clone(),
            })
            .collect();
        let mut types = vec!['?'; projections.len()];
        let mut rows = Vec::new();
        if let Some(row) = t.get(&key, snap) {
            let mut rendered = Vec::with_capacity(projections.len());
            for (i, p) in projections.iter().enumerate() {
                if let Projection::Expr(e, _) = p {
                    let v = e.eval(&row);
                    if types[i] == '?' {
                        types[i] = v.type_char();
                    }
                    rendered.push(v.render_as(out_types[i]));
                }
            }
            rows.push(rendered);
        }
        return Ok(Outcome::Rows {
            columns,
            types,
            rows,
        });
    }

    // Zero-copy segment scan: fully-visible parts are read in place, never
    // concatenated into a giant batch. The filter runs against each segment's
    // own columns.
    let mut segments = t.scan_segments(snap);

    // Zonemap part pruning (DuckDB-style): drop any fully-materialised part
    // whose per-column min/max bounds prove it holds no row matching the
    // `WHERE` predicate — so a selective range scan never touches it. Only
    // `Segment::Part` carries exact bounds; owned/partial segments always stay.
    if let Some(f) = &filter {
        segments.retain(|s| match s {
            Segment::Part(p) => !f.excludes(p.col_bounds_all()),
            _ => true,
        });
    }

    let non_grouped = group_by.is_empty()
        && projections
            .iter()
            .all(|p| matches!(p, Projection::Expr(..)));

    // Non-grouped `ORDER BY` (without DISTINCT) sorts on keys evaluated from the
    // *source* row — so `ORDER BY b` is honoured even when `b` is not projected —
    // and with a `LIMIT` it renders only the surviving top-K. DISTINCT and
    // grouped queries order by output columns, per SQL, and take the plain path.
    if non_grouped && !order_by.is_empty() && !distinct {
        return Ok(project_ordered(
            &projections,
            &out_types,
            &segments,
            &filter,
            &order_by,
            limit,
        ));
    }

    // `SELECT DISTINCT <cols>` with no ORDER BY is a de-duplication over the
    // projected values — the same shape as GROUP BY. Dedup on typed keys during
    // the scan (holding only the distinct set, never 500k rendered strings).
    if non_grouped && distinct && order_by.is_empty() {
        return Ok(project_distinct(
            &projections,
            &out_types,
            &segments,
            &filter,
            limit,
        ));
    }

    let (columns, types, mut rows) = if non_grouped {
        project_rows(&projections, &out_types, &segments, &filter)
    } else {
        aggregate_rows(&projections, &out_types, &group_by, &segments, &filter)?
    };

    if distinct {
        dedup(&mut rows);
    }
    if !order_by.is_empty() {
        sort_rows(&mut rows, &order_by, &projections);
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

/// The non-grouped `SELECT DISTINCT` path. Dedups on typed projected values
/// during the scan via a `BTreeSet`, so it holds only the distinct set and
/// renders each survivor once — instead of rendering every input row.
fn project_distinct(
    projections: &[Projection],
    out_types: &[DataType],
    segments: &[Segment],
    filter: &Option<Expr>,
    limit: Option<usize>,
) -> Outcome {
    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            Projection::Expr(_, l) | Projection::Agg(_, _, l) => l.clone(),
        })
        .collect();
    let single = projections.len() == 1;

    let mut seen: std::collections::BTreeSet<GroupKey> = std::collections::BTreeSet::new();
    for seg in segments {
        let batch = seg.batch();
        for i in 0..batch.len() {
            if !passes_at(filter, batch, i) {
                continue;
            }
            let key = if single {
                if let Projection::Expr(e, _) = &projections[0] {
                    GroupKey::One(OrdVal(e.eval_at(batch, i)))
                } else {
                    continue;
                }
            } else {
                GroupKey::Many(
                    projections
                        .iter()
                        .filter_map(|p| match p {
                            Projection::Expr(e, _) => Some(OrdVal(e.eval_at(batch, i))),
                            _ => None,
                        })
                        .collect(),
                )
            };
            seen.insert(key);
        }
    }

    let mut types = vec!['?'; projections.len()];
    let mut rows = Vec::with_capacity(seen.len());
    for key in &seen {
        let mut rendered = Vec::with_capacity(projections.len());
        for (col, _) in projections.iter().enumerate() {
            if let Some(v) = key.component(col) {
                if types[col] == '?' {
                    types[col] = v.0.type_char();
                }
                rendered.push(v.0.render_as(out_types[col]));
            }
        }
        rows.push(rendered);
        if limit.is_some_and(|n| rows.len() >= n) {
            break;
        }
    }

    Outcome::Rows {
        columns,
        types,
        rows,
    }
}

/// The non-grouped `ORDER BY` path. Evaluates sort keys from the source row,
/// selects the top-K when a `LIMIT` is present (so only K rows are rendered),
/// then renders the survivors in order.
fn project_ordered(
    projections: &[Projection],
    out_types: &[DataType],
    segments: &[Segment],
    filter: &Option<Expr>,
    order_by: &[OrderKey],
    limit: Option<usize>,
) -> Outcome {
    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            Projection::Expr(_, l) | Projection::Agg(_, _, l) => l.clone(),
        })
        .collect();

    // (sort key, segment index, row index) for every row that passes the filter.
    // The single-key case (by far the common one) stores the key inline, with no
    // per-row heap allocation — critical when sorting hundreds of thousands of
    // rows.
    let mut keyed: Vec<(SortKey, usize, usize)> = Vec::new();
    for (si, seg) in segments.iter().enumerate() {
        let batch = seg.batch();
        for i in 0..batch.len() {
            if !passes_at(filter, batch, i) {
                continue;
            }
            let key = if order_by.len() == 1 {
                SortKey::One(OrdVal(order_by[0].expr.eval_at(batch, i)))
            } else {
                SortKey::Many(
                    order_by
                        .iter()
                        .map(|o| OrdVal(o.expr.eval_at(batch, i)))
                        .collect(),
                )
            };
            keyed.push((key, si, i));
        }
    }

    let cmp = |a: &(SortKey, usize, usize), b: &(SortKey, usize, usize)| {
        for (ki, o) in order_by.iter().enumerate() {
            let ord = a.0.at(ki).cmp(b.0.at(ki));
            let ord = if o.ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    };

    // Top-K: partition so the K best are first, then sort only those.
    match limit {
        Some(k) if k < keyed.len() => {
            keyed.select_nth_unstable_by(k, &cmp);
            keyed.truncate(k);
            keyed.sort_by(&cmp);
        }
        _ => keyed.sort_by(&cmp),
    }

    // Render only the survivors.
    let mut types = vec!['?'; projections.len()];
    let mut rows = Vec::with_capacity(keyed.len());
    for (_, si, i) in &keyed {
        let batch = segments[*si].batch();
        let mut rendered = Vec::with_capacity(projections.len());
        for (col, p) in projections.iter().enumerate() {
            if let Projection::Expr(e, _) = p {
                let v = e.eval_at(batch, *i);
                if types[col] == '?' {
                    types[col] = v.type_char();
                }
                rendered.push(v.render_as(out_types[col]));
            }
        }
        rows.push(rendered);
    }

    Outcome::Rows {
        columns,
        types,
        rows,
    }
}

/// Whether a row passes an optional predicate. Absent predicate = all rows.
fn passes(filter: &Option<Expr>, row: &Row) -> bool {
    match filter {
        None => true,
        Some(e) => e.eval(row).is_true(),
    }
}

/// The query is exactly `SELECT COUNT(*) FROM t` with nothing else — the case
/// the metadata fast path can answer without scanning.
fn is_bare_count_star(
    projections: &[Projection],
    filter: &Option<Expr>,
    group_by: &[usize],
    order_by: &[OrderKey],
    distinct: bool,
) -> bool {
    filter.is_none()
        && group_by.is_empty()
        && order_by.is_empty()
        && !distinct
        && projections.len() == 1
        && matches!(projections[0], Projection::Agg(AggFn::Count, None, _))
}

/// Every projection is a bare `MIN(col)` / `MAX(col)`, with no filter, grouping,
/// or DISTINCT — the case the zonemap metadata path answers without scanning.
fn is_bare_minmax(
    projections: &[Projection],
    filter: &Option<Expr>,
    group_by: &[usize],
    distinct: bool,
) -> bool {
    filter.is_none()
        && group_by.is_empty()
        && !distinct
        && !projections.is_empty()
        && projections
            .iter()
            .all(|p| matches!(p, Projection::Agg(AggFn::Min | AggFn::Max, Some(_), _)))
}

/// If this query is a point lookup on the key column — `WHERE key = <literal>`
/// with no grouping, aggregation, or DISTINCT — return the key value. Such a
/// query is answered through the index funnel, not a scan.
fn point_lookup_key(
    filter: &Option<Expr>,
    group_by: &[usize],
    distinct: bool,
    projections: &[Projection],
    key_index: usize,
) -> Option<Value> {
    if !group_by.is_empty()
        || distinct
        || projections.iter().any(|p| matches!(p, Projection::Agg(..)))
    {
        return None;
    }
    match filter {
        Some(Expr::Binary(BinaryOp::Eq, l, r)) => match (l.as_ref(), r.as_ref()) {
            (Expr::Column(i), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(i))
                if *i == key_index =>
            {
                Some(v.clone())
            }
            _ => None,
        },
        _ => None,
    }
}

/// The HTAP router's verdict: does this plan want the vectorised (DataFusion)
/// engine? Cheap transactional shapes — metadata `COUNT(*)` and key point
/// lookups — stay on the interpreter; everything else analytical goes vectorised.
/// Non-`SELECT` plans (writes, DDL) are never routed away from the interpreter.
#[cfg(feature = "datafusion")]
pub(crate) fn prefers_vectorized(plan: &Plan, key_index: usize) -> bool {
    let Plan::Select {
        projections,
        filter,
        group_by,
        order_by,
        distinct,
        ..
    } = plan
    else {
        return false;
    };
    if is_bare_count_star(projections, filter, group_by, order_by, *distinct) {
        return false;
    }
    if is_bare_minmax(projections, filter, group_by, *distinct) {
        return false; // answered from zonemaps by the interpreter
    }
    if point_lookup_key(filter, group_by, *distinct, projections, key_index).is_some() {
        return false;
    }
    true
}

/// Second-stage router check: given a plan the first stage sent to the vectorised
/// engine, decide whether zonemap part pruning makes it *more* selective than a
/// columnar scan — in which case the pruning interpreter wins. Cheap: consults
/// only per-part min/max bounds, never scans a row.
///
/// Conservative by construction. It fires only for simple row-returning shapes
/// (no aggregate / `GROUP BY` / `DISTINCT`) over a table with enough sealed parts
/// to matter, and only when the predicate's zonemaps prune the large majority of
/// rows — otherwise the interpreter's per-row rendering would lose to DataFusion.
#[cfg(feature = "datafusion")]
pub(crate) fn prune_favors_interpreter(plan: &Plan, table: &crate::table::Table) -> bool {
    let Plan::Select {
        projections,
        filter,
        group_by,
        distinct,
        ..
    } = plan
    else {
        return false;
    };
    if !group_by.is_empty()
        || *distinct
        || projections
            .iter()
            .any(|p| !matches!(p, Projection::Expr(..)))
    {
        return false;
    }
    let Some(f) = filter else { return false };
    let (parts, _) = table.parts_snapshot();
    if parts.len() < 4 {
        return false; // too few parts for pruning to pay off
    }
    let mut surviving = 0usize;
    let mut total = 0usize;
    for p in &parts {
        let n = p.num_rows();
        total += n;
        if !f.excludes(p.col_bounds_all()) {
            surviving += n;
        }
    }
    // Only when ≥80% of rows are provably skipped.
    total > 0 && surviving.saturating_mul(5) <= total
}

/// The rendered output type of each projection. A bare column (or `MIN`/`MAX`
/// of one) carries that column's declared type — so a `DATE`/`TIMESTAMP` renders
/// as a date string; a computed expression defaults to `Int`, for which
/// [`Value::render_as`] is identical to [`Value::render`].
fn projection_types(projs: &[Projection], schema: &Schema) -> Vec<DataType> {
    projs
        .iter()
        .map(|p| match p {
            Projection::Expr(Expr::Column(i), _) => schema.column(*i).ty,
            Projection::Agg(AggFn::Min | AggFn::Max, Some(i), _) => schema.column(*i).ty,
            _ => DataType::Int,
        })
        .collect()
}

/// Whether batch row `i` passes an optional predicate, evaluated columnar.
#[inline]
fn passes_at(filter: &Option<Expr>, batch: &Batch, i: usize) -> bool {
    match filter {
        None => true,
        Some(e) => e.eval_at(batch, i).is_true(),
    }
}

fn project_rows(
    projections: &[Projection],
    out_types: &[DataType],
    segments: &[Segment],
    filter: &Option<Expr>,
) -> ResultSet {
    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            Projection::Expr(_, label) => label.clone(),
            Projection::Agg(_, _, label) => label.clone(),
        })
        .collect();
    let mut out = Vec::new();
    let mut types = vec!['?'; projections.len()];
    for seg in segments {
        let batch = seg.batch();
        for row_i in 0..batch.len() {
            if !passes_at(filter, batch, row_i) {
                continue;
            }
            let mut rendered = Vec::with_capacity(projections.len());
            for (i, p) in projections.iter().enumerate() {
                if let Projection::Expr(e, _) = p {
                    let v = e.eval_at(batch, row_i);
                    if types[i] == '?' {
                        types[i] = v.type_char();
                    }
                    rendered.push(v.render_as(out_types[i]));
                }
            }
            out.push(rendered);
        }
    }
    (columns, types, out)
}

/// A group key component that orders by `Value::total_cmp` (a total order over
/// all SQL values, NULLs first). Wrapping lets a `BTreeMap` key on typed values
/// directly — so grouping never renders a `String` per row, only per group at
/// the end. On a 500k-row `GROUP BY`, that removes 500k heap allocations.
#[derive(Clone)]
struct OrdVal(Value);
impl PartialEq for OrdVal {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0).is_eq()
    }
}
impl Eq for OrdVal {}
impl PartialOrd for OrdVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdVal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// A sort key that avoids a heap allocation for the common single-column
/// `ORDER BY`. `at(ki)` returns the `ki`-th component.
enum SortKey {
    One(OrdVal),
    Many(Vec<OrdVal>),
}
impl SortKey {
    #[inline]
    fn at(&self, ki: usize) -> &OrdVal {
        match self {
            SortKey::One(v) => v,
            SortKey::Many(v) => &v[ki],
        }
    }
}

/// A `GROUP BY` key ordered by `Value::total_cmp`, with the single-column case
/// stored inline — so grouping 500k rows on one column allocates no per-row
/// `Vec`. All keys in one query share a variant, so `One`/`Many` never mix.
#[derive(PartialEq, Eq)]
enum GroupKey {
    One(OrdVal),
    Many(Vec<OrdVal>),
}
impl GroupKey {
    #[inline]
    fn component(&self, i: usize) -> Option<&OrdVal> {
        match self {
            GroupKey::One(v) => (i == 0).then_some(v),
            GroupKey::Many(v) => v.get(i),
        }
    }
}
impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (GroupKey::One(a), GroupKey::One(b)) => a.cmp(b),
            (GroupKey::Many(a), GroupKey::Many(b)) => a.cmp(b),
            // Mixed variants never occur within one query; order deterministically.
            (GroupKey::One(_), GroupKey::Many(_)) => std::cmp::Ordering::Less,
            (GroupKey::Many(_), GroupKey::One(_)) => std::cmp::Ordering::Greater,
        }
    }
}

/// A running aggregate accumulator.
#[derive(Clone)]
struct Acc {
    count: i64,
    sum: f64,
    int_sum: i64,
    /// True while every summed value has been an integer — then SUM stays an
    /// integer, matching DataFusion/DuckDB (SUM of integers is not a float).
    all_int: bool,
    /// Exact SUM of decimals: while every summed value is a `Decimal` of one
    /// consistent scale, SUM accumulates the i128 mantissa and stays exact.
    dec_sum: i128,
    dec_scale: Option<u32>,
    all_dec: bool,
    min: Option<Value>,
    max: Option<Value>,
    seen_numeric: bool,
}

impl Acc {
    fn new() -> Self {
        Acc {
            count: 0,
            sum: 0.0,
            int_sum: 0,
            all_int: true,
            dec_sum: 0,
            dec_scale: None,
            all_dec: true,
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
        match v {
            Value::Int(i) => {
                self.int_sum = self.int_sum.wrapping_add(*i);
                self.all_dec = false;
            }
            Value::Bool(_) => {
                self.all_dec = false;
            }
            Value::Decimal(m, s) => {
                self.all_int = false;
                match self.dec_scale {
                    None => {
                        self.dec_scale = Some(*s);
                        self.dec_sum = *m;
                    }
                    Some(scale) if scale == *s => self.dec_sum = self.dec_sum.wrapping_add(*m),
                    // Mixed scales — give up on the exact path, keep the f64 sum.
                    Some(_) => self.all_dec = false,
                }
            }
            // Float / Text: not exactly summable as int or decimal.
            _ => {
                self.all_int = false;
                self.all_dec = false;
            }
        }
        if self
            .min
            .as_ref()
            .map(|m| v.total_cmp(m).is_lt())
            .unwrap_or(true)
        {
            self.min = Some(v.clone());
        }
        if self
            .max
            .as_ref()
            .map(|m| v.total_cmp(m).is_gt())
            .unwrap_or(true)
        {
            self.max = Some(v.clone());
        }
    }
    fn value(&self, f: AggFn, is_star: bool, group_rows: i64) -> Value {
        match f {
            AggFn::Count => Value::Int(if is_star { group_rows } else { self.count }),
            AggFn::Sum => {
                if !self.seen_numeric {
                    Value::Null
                } else if self.all_int {
                    Value::Int(self.int_sum)
                } else if self.all_dec {
                    // Exact decimal sum at the shared scale.
                    Value::Decimal(self.dec_sum, self.dec_scale.unwrap_or(0))
                } else {
                    Value::Float(self.sum)
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
    out_types: &[DataType],
    group_by: &[usize],
    segments: &[Segment],
    filter: &Option<Expr>,
) -> Result<ResultSet, Error> {
    // Group key (typed, not rendered) → (group row count, per-projection accs).
    // BTreeMap over the typed key gives deterministic sorted output without a
    // separate sort, and — via GroupKey::One — without a per-row allocation on
    // the common single-column GROUP BY.
    let mut groups: BTreeMap<GroupKey, (i64, Vec<Acc>)> = BTreeMap::new();
    for seg in segments {
        let batch = seg.batch();
        for row_i in 0..batch.len() {
            if !passes_at(filter, batch, row_i) {
                continue;
            }
            let key = if group_by.len() == 1 {
                GroupKey::One(OrdVal(batch_value(batch, group_by[0], row_i)))
            } else {
                GroupKey::Many(
                    group_by
                        .iter()
                        .map(|&i| OrdVal(batch_value(batch, i, row_i)))
                        .collect(),
                )
            };
            let entry = groups
                .entry(key)
                .or_insert_with(|| (0, vec![Acc::new(); projections.len()]));
            entry.0 += 1;
            for (i, p) in projections.iter().enumerate() {
                if let Projection::Agg(_, arg, _) = p {
                    let v = arg
                        .map(|c| batch_value(batch, c, row_i))
                        .unwrap_or(Value::Int(1));
                    entry.1[i].push(&v);
                }
            }
        }
    }
    // A bare aggregate with no rows still yields one row (COUNT = 0).
    if groups.is_empty() && group_by.is_empty() {
        groups.insert(
            GroupKey::Many(Vec::new()),
            (0, vec![Acc::new(); projections.len()]),
        );
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
                    let val = key.component(gi).map(|k| &k.0);
                    gi += 1;
                    if types[i] == '?' {
                        types[i] = val.map(|v| v.type_char()).unwrap_or('?');
                    }
                    rendered.push(val.map(|v| v.render_as(out_types[i])).unwrap_or_default());
                    continue;
                }
            };
            if types[i] == '?' {
                types[i] = v.type_char();
            }
            rendered.push(v.render_as(out_types[i]));
        }
        out.push(rendered);
    }
    Ok((columns, types, out))
}

fn dedup(rows: &mut Vec<Vec<String>>) {
    let mut seen = std::collections::HashSet::new();
    rows.retain(|r| seen.insert(r.clone()));
}

fn sort_rows(rows: &mut [Vec<String>], keys: &[OrderKey], projections: &[Projection]) {
    // ORDER BY over projected output. A key that names a column is matched to the
    // *output* position that projects it — not its schema index, which differs
    // once a query groups or reorders columns.
    rows.sort_by(|a, b| {
        for (ki, key) in keys.iter().enumerate() {
            let idx = output_index(key, projections, ki).min(a.len().saturating_sub(1));
            let ord = a[idx].cmp(&b[idx]);
            let ord = if key.ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// The output column position an ORDER BY key sorts by: the projection that
/// emits that column, else `fallback`.
fn output_index(key: &OrderKey, projections: &[Projection], fallback: usize) -> usize {
    if let Expr::Column(ci) = &key.expr {
        for (j, p) in projections.iter().enumerate() {
            if let Projection::Expr(Expr::Column(pj), _) = p {
                if pj == ci {
                    return j;
                }
            }
        }
    }
    fallback
}

#[cfg(test)]
mod tests {
    use super::super::plan::plan;
    use super::*;
    use crate::database::Database;

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
        assert_eq!(
            run(&db, "INSERT INTO t VALUES (1,2,3,'a')"),
            Outcome::Affected(1)
        );
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
        match run(
            &db,
            "SELECT COUNT(*), SUM(a), MIN(a), MAX(a), AVG(a) FROM t",
        ) {
            Outcome::Rows { rows, .. } => {
                assert_eq!(rows[0][0], "3");
                assert_eq!(rows[0][1], "60");
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
        assert_eq!(
            run(&db, "UPDATE t SET a = 99 WHERE pk = 1"),
            Outcome::Affected(1)
        );
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
