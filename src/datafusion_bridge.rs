//! The DataFusion bridge — ChakraDB's storage under a bought executor (§8).
//!
//! This is the crux of M3. DataFusion does *execution*; ChakraDB keeps owning
//! *storage and MVCC*. The bridge is the seam: it turns a **consistent MVCC
//! snapshot** of a table into Arrow record batches that DataFusion can plan and
//! run over — with no idea that snapshots, deletion vectors, or concurrent
//! writers exist.
//!
//! That is the whole thesis of the spike. If a query planned by DataFusion sees
//! exactly the rows visible at one snapshot, and writers keep mutating the table
//! the entire time without blocking or corrupting that view, then the
//! concurrency wedge DuckDB structurally cannot offer survives the handoff to a
//! vectorised engine. Speed is the easy half; snapshot-consistency under load is
//! the half worth proving.
//!
//! The spike uses a `MemTable`: it materialises the snapshot's visible rows into
//! Arrow up front. A production M3 would implement a streaming `TableProvider`
//! with projection/filter pushdown so nothing is copied that the query prunes —
//! but MemTable is enough to measure execution speed and prove the snapshot
//! handoff, which is what this experiment is for.

use crate::csn::Snapshot;
use crate::schema::Batch;
use crate::table::Table;
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;

/// The fixed M0/M2 schema, in Arrow terms: `(pk i64, a i64, b f64, c text)`.
pub fn hits_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Utf8, false),
    ]))
}

/// One ChakraDB batch → one Arrow record batch (a column-wise copy).
fn batch_to_record(b: &Batch, schema: &SchemaRef) -> RecordBatch {
    let cs: Vec<&str> = b.c.iter().map(|s| s.as_str()).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(b.pk.clone())),
            Arc::new(Int64Array::from(b.a.clone())),
            Arc::new(Float64Array::from(b.b.clone())),
            Arc::new(StringArray::from(cs)),
        ],
    )
    .expect("record batch shape matches schema")
}

/// Convert a consistent MVCC snapshot of `table` into a DataFusion `MemTable`.
///
/// Each visible segment (a fully-visible sealed part read in place, or a
/// materialised L0 / partial-visibility batch) becomes one Arrow record batch,
/// so DataFusion can parallelise scans across them. Every row in the result is
/// visible to `snap`; concurrent writers after `snap` are simply not seen — that
/// is snapshot isolation carried across the executor boundary.
pub fn snapshot_memtable(table: &Table, snap: Snapshot) -> MemTable {
    let schema = hits_schema();
    let batches: Vec<RecordBatch> = table
        .scan_segments(snap)
        .iter()
        .map(|seg| batch_to_record(seg.batch(), &schema))
        .filter(|rb| rb.num_rows() > 0)
        .collect();
    MemTable::try_new(schema, vec![batches]).expect("memtable from snapshot")
}
