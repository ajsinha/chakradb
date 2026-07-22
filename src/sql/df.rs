//! DataFusion-backed query execution (feature `datafusion`).
//!
//! This is the §8 "buy execution, don't build it" path wired into the SQL front
//! end. When the feature is on, `SqlEngine` routes `SELECT` queries here: every
//! table is registered with DataFusion at one MVCC snapshot (zero-copy — the
//! parts already hold Arrow arrays), DataFusion plans and runs the query, and the
//! result is rendered back into the engine's row form.
//!
//! Writes and DDL never come here — they stay on the interpreter/`SqlBackend`,
//! which owns the snapshot clock and the WAL. So DataFusion sees a consistent
//! read snapshot while writers keep committing underneath it: the concurrency
//! wedge, preserved across the executor boundary.

use super::backend::SqlBackend;
use super::exec::Outcome;
use crate::datafusion_bridge::snapshot_memtable;
use crate::error::Error;
use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType as ArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

fn sql_err<E: std::fmt::Display>(e: E) -> Error {
    Error::Sql(e.to_string())
}

/// One process-wide Tokio runtime shared by every `SqlEngine`, so binding an
/// engine does not spawn worker threads and queries reuse a warm pool.
fn runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
    })
}

/// Execute a read-only query through DataFusion over the catalog's current
/// snapshot.
pub fn execute_query(be: &dyn SqlBackend, sql: &str) -> Result<Outcome, Error> {
    // Pin the snapshot for the whole query so concurrent compaction cannot
    // reclaim a version this scan will read.
    let pin = be.pin();
    let snap = pin.snapshot();
    // Preserve identifier case: ChakraDB columns can be CamelCase (e.g. the
    // ClickBench schema), and DataFusion otherwise lowercases unquoted names,
    // which would fail to resolve `AdvEngineID`. DuckDB matches case-insensitively;
    // disabling normalization keeps the two consistent for exact-case queries.
    let mut config = datafusion::prelude::SessionConfig::new();
    config.options_mut().sql_parser.enable_ident_normalization = false;
    let ctx = SessionContext::new_with_config(config);
    for name in be.table_names() {
        if let Ok(t) = be.table(&name) {
            let mem = snapshot_memtable(t.as_ref(), snap);
            ctx.register_table(&name, Arc::new(mem)).map_err(sql_err)?;
        }
    }

    let sql = sql.to_string();
    let batches = runtime().block_on(async move {
        let df = ctx.sql(&sql).await.map_err(sql_err)?;
        df.collect().await.map_err(sql_err)
    })?;

    let columns = column_names(&batches);
    let types = column_types(&batches);
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                row.push(render_cell(batch.column(c), r));
            }
            rows.push(row);
        }
    }
    Ok(Outcome::Rows {
        columns,
        types,
        rows,
    })
}

fn column_names(batches: &[RecordBatch]) -> Vec<String> {
    match batches.first() {
        Some(b) => b
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect(),
        None => Vec::new(),
    }
}

/// sqllogictest-style type chars, from the result's Arrow types.
fn column_types(batches: &[RecordBatch]) -> Vec<char> {
    match batches.first() {
        Some(b) => b
            .schema()
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                ArrowType::Utf8 | ArrowType::LargeUtf8 => 'T',
                ArrowType::Float16 | ArrowType::Float32 | ArrowType::Float64 => 'R',
                _ => 'I',
            })
            .collect(),
        None => Vec::new(),
    }
}

/// Render one Arrow cell to a string, matching the interpreter's conventions
/// (NULL -> "NULL", integer floats -> "N.0", bool -> "1"/"0") so output is
/// consistent whichever executor ran the query.
fn render_cell(arr: &dyn Array, i: usize) -> String {
    if arr.is_null(i) {
        return "NULL".to_string();
    }
    macro_rules! as_arr {
        ($ty:ty) => {
            arr.as_any().downcast_ref::<$ty>().unwrap()
        };
    }
    match arr.data_type() {
        ArrowType::Int64 => as_arr!(Int64Array).value(i).to_string(),
        ArrowType::Int32 => as_arr!(Int32Array).value(i).to_string(),
        ArrowType::UInt64 => as_arr!(UInt64Array).value(i).to_string(),
        ArrowType::UInt32 => as_arr!(UInt32Array).value(i).to_string(),
        ArrowType::Float64 => render_float(as_arr!(Float64Array).value(i)),
        ArrowType::Float32 => render_float(as_arr!(Float32Array).value(i) as f64),
        ArrowType::Utf8 => as_arr!(StringArray).value(i).to_string(),
        ArrowType::LargeUtf8 => as_arr!(LargeStringArray).value(i).to_string(),
        ArrowType::Boolean => {
            if as_arr!(BooleanArray).value(i) {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        // Anything else: fall back to Arrow's own display.
        _ => datafusion::arrow::util::display::array_value_to_string(arr, i)
            .unwrap_or_else(|_| "NULL".to_string()),
    }
}

fn render_float(f: f64) -> String {
    if f.fract() == 0.0 && f.is_finite() {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}
