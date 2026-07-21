//! # ChakraDB — M0 risk-reduction prototype
//!
//! This crate is **not a database**. It is the M0 milestone from
//! `docs/roadmap.md`: a deliberately throwaway spike built to answer one
//! question before any real system is written.
//!
//! > Can we sustain high-rate keyed updates while scanning, at an acceptable
//! > primary-key index memory footprint, without scan performance collapsing?
//!
//! ## What is here
//!
//! * [`Database`] / [`Table`] — a multi-table catalog over a shared snapshot
//!   clock. Each table owns one primary-key space.
//! * Three-tier storage (`requirements.md` §5.1): [`l0::L0Buffer`] absorbs
//!   writes, [`part::Part`] holds sealed PK-sorted runs.
//! * The Doris-style index (§5.2): parts are written sorted by key, so the
//!   **ordinal position in the index is the row offset**. There is no separate
//!   key→location map, and therefore no per-row index cost.
//! * Snapshot-isolation MVCC (§5.3) with per-part version stamps and
//!   [`delete_vector::DeleteVector`] tombstones. Cold, unmodified parts pay
//!   **zero per-row visibility cost** on scan.
//! * [`compaction`] — reclaims tombstoned rows, bounds lookup fan-out, and
//!   garbage-collects version metadata.
//!
//! ## What is deliberately absent
//!
//! No durability, no WAL, no recovery, no SQL, no Arrow, no table format, no
//! spilling, no bindings. Those are M1 and later. M0 is in-memory by design:
//! adding persistence would not change the four numbers it exists to produce.
//!
//! ## The seams that had to exist from day one
//!
//! [`io::Io`], [`clock::Clock`] and [`rng::Rng`] are present even though M0
//! never persists anything. Per `requirements.md` §11.1 they cannot be
//! retrofitted once compaction threads and a buffer pool call ambient APIs
//! directly — so they are built now, when it is free.
//!
//! ## Example
//!
//! ```
//! use chakradb::{Database, Row, Value};
//!
//! let db = Database::new();
//! let users = db.create_table("users").unwrap();
//!
//! users.insert(Row::new(1, 100, 1.5, "alice")).unwrap();
//! users.insert(Row::new(2, 200, 2.5, "bob")).unwrap();
//!
//! // Snapshots are stable across concurrent writes.
//! let before = db.snapshot();
//! users.update(Row::new(1, 999, 1.5, "alice-v2")).unwrap();
//!
//! assert_eq!(users.get(&Value::Int(1), before).unwrap().c(), "alice");
//! assert_eq!(users.get_latest(&Value::Int(1)).unwrap().c(), "alice-v2");
//! assert_eq!(users.row_count(db.snapshot()), 2);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod backpressure;
pub mod batch;
pub mod bloom;
pub mod clock;
pub mod codec;
pub mod compaction;
pub mod csn;
pub mod database;
pub mod delete_vector;
pub mod durability;
pub mod error;
pub mod io;
pub mod l0;
pub mod manifest;
pub mod metrics;
pub mod pager;
pub mod part;
#[cfg(unix)]
pub mod posix;
pub mod persist;
#[cfg(feature = "datafusion")]
pub mod datafusion_bridge;
pub mod rng;
pub mod schema;
pub mod sql;
pub mod storage;
pub mod storage_config;
pub mod table;
pub mod value;
pub mod wal;

pub use backpressure::{Backpressure, BackpressureConfig};
pub use clock::{Clock, RealClock, SimClock};
pub use durability::Durability;
pub use csn::{Csn, CsnGenerator, Snapshot};
pub use database::Database;
pub use error::{Error, Result};
pub use metrics::{Metrics, MetricsSnapshot};
#[cfg(unix)]
pub use posix::{PosixIo, TempDir};
pub use rng::Rng;
pub use batch::Batch;
pub use schema::{ColumnDef, Row, Schema};
pub use sql::SqlEngine;
pub use value::{DataType, Key, Value};
pub use storage::{Storage, StorageConfig};
pub use table::{Table, TableConfig, TableStats};

/// Crate version, for benchmark reports.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_to_end_smoke() {
        let db = Database::new();
        let t = db.create_table("t").unwrap();
        for pk in 0..100 {
            t.insert(Row::new(pk, pk, pk as f64, format!("v{pk}")))
                .unwrap();
        }
        assert_eq!(t.row_count(db.snapshot()), 100);

        t.seal();
        for pk in (0..100).step_by(3) {
            t.delete(&Value::Int(pk)).unwrap();
        }
        let expected = 100 - (0..100).step_by(3).count();
        assert_eq!(t.row_count(db.snapshot()), expected);

        t.force_compact(db.snapshot().csn);
        assert_eq!(t.row_count(db.snapshot()), expected);
    }

    #[test]
    fn version_is_exported() {
        assert!(!VERSION.is_empty());
    }
}
