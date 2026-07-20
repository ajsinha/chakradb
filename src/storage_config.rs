//! Configuration and recovery reporting for [`Storage`](crate::storage::Storage).

use crate::backpressure::BackpressureConfig;
use crate::csn::Csn;
use crate::durability::Durability;
use crate::table::TableConfig;

/// Tunables for the durable layer.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub durability: Durability,
    pub table: TableConfig,
    pub backpressure: BackpressureConfig,
    /// Checkpoint once the log exceeds this many bytes.
    pub checkpoint_wal_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            durability: Durability::default(),
            table: TableConfig::default(),
            backpressure: BackpressureConfig::default(),
            checkpoint_wal_bytes: 4 * 1024 * 1024,
        }
    }
}

/// What recovery found on startup.
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub tables_loaded: usize,
    pub parts_loaded: usize,
    pub rows_from_parts: usize,
    pub wal_records_replayed: usize,
    pub wal_bytes_scanned: u64,
    /// A torn record was found and discarded — normal after a crash.
    pub truncated_tail: bool,
    pub recovered_csn: Csn,
    /// Parts registered from their summaries without reading column data.
    pub parts_registered_lazily: usize,
}

