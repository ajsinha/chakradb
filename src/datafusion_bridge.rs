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
use crate::table::Table;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;

/// Convert a consistent MVCC snapshot of `table` into a DataFusion `MemTable`.
///
/// Now that ChakraDB stores Arrow arrays, this is **zero-copy**: each visible
/// segment already *is* an Arrow `RecordBatch`, so we hand DataFusion the parts'
/// own columns (an `Arc` clone), never rebuilding them. It also works for *any*
/// schema — the table's Arrow schema drives it — not just the fixed hits shape.
///
/// Every row in the result is visible to `snap`; writers committing after `snap`
/// are simply not seen. That is snapshot isolation carried across the executor
/// boundary — the property the M3 spike exists to prove.
pub fn snapshot_memtable(table: &Table, snap: Snapshot) -> MemTable {
    let schema = table.schema();
    // Expose only the user columns — a synthesised `_rowid` key stays hidden, so
    // DataFusion's `SELECT *` and column resolution match the interpreter.
    let keep = schema.star_indices();
    let arrow_schema = std::sync::Arc::new(
        schema
            .arrow()
            .project(&keep)
            .expect("project to user columns"),
    );
    let batches: Vec<RecordBatch> = table
        .scan_segments(snap)
        .iter()
        .map(|seg| seg.batch().record_batch().project(&keep).expect("project batch"))
        .filter(|rb| rb.num_rows() > 0)
        .collect();
    MemTable::try_new(arrow_schema, vec![batches]).expect("memtable from snapshot")
}
