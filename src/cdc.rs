//! Change-Data-Capture: a committed-change stream for reacting to writes.
//!
//! ChakraDB has no in-SQL triggers. Instead it exposes the more scalable model an
//! event-driven application actually wants: a **stream of committed row changes**.
//! Register a subscription, and every INSERT / UPDATE / DELETE that commits is
//! delivered — after the write has been applied (and, on the durable backend,
//! after it is WAL-logged) — as a [`Change`] carrying the operation, the commit
//! sequence number (CSN), and the old and new row images.
//!
//! This is the substrate for the real-time AML pipeline: the payment table is
//! written at ingest speed, and a perpetually-running worker consumes the change
//! stream to update its graph and fire detectors — never blocking the writer.
//!
//! # Design
//!
//! The publisher is a **decorator** over any [`SqlBackend`]: [`CdcBackend::wrap`]
//! wraps the `Database` or `Storage` the [`SqlEngine`](crate::SqlEngine) is bound
//! to, forwards every method to the inner backend, and — only for the four
//! mutating methods plus the bulk/transaction paths — publishes a [`Change`] once
//! the inner write returns successfully. Because the wrap sits *above* the write
//! path, it adds nothing to the engine's hot locks; an INSERT pays only a channel
//! send, and the old-row read for UPDATE/DELETE happens outside any core lock.
//!
//! Delivery is **at-least-once** and **in commit order** per subscriber, over a
//! `std` MPSC channel. Batches preserve transaction grouping: a multi-statement
//! transaction's writes arrive as one `Vec<Change>`. A future durable-subscription
//! layer can add resume-by-CSN and a Kafka [`sink`](ChangeSink); the in-process
//! channel is the default, lowest-latency transport.

use crate::csn::{Csn, Snapshot, SnapshotPin};
use crate::error::Result;
use crate::schema::{Row, Schema};
use crate::sql::backend::{SqlBackend, TxnWrite};
use crate::table::Table;
use crate::value::Value;
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// The kind of row change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl ChangeOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeOp::Insert => "insert",
            ChangeOp::Update => "update",
            ChangeOp::Delete => "delete",
        }
    }
}

/// One committed row change. `columns` names line up positionally with the values
/// in `old` and `new`.
#[derive(Clone, Debug)]
pub struct Change {
    pub table: String,
    pub op: ChangeOp,
    pub csn: Csn,
    /// Column names for this table (shared, positional with `old`/`new`).
    pub columns: Arc<Vec<String>>,
    /// Pre-image: `None` for an INSERT.
    pub old: Option<Vec<Value>>,
    /// Post-image: `None` for a DELETE.
    pub new: Option<Vec<Value>>,
}

impl Change {
    /// The new row as `(column, value)` pairs, or `None` for a DELETE.
    pub fn new_pairs(&self) -> Option<Vec<(&str, &Value)>> {
        self.new.as_ref().map(|vals| self.pairs(vals))
    }
    /// The old row as `(column, value)` pairs, or `None` for an INSERT.
    pub fn old_pairs(&self) -> Option<Vec<(&str, &Value)>> {
        self.old.as_ref().map(|vals| self.pairs(vals))
    }
    fn pairs<'a>(&'a self, vals: &'a [Value]) -> Vec<(&'a str, &'a Value)> {
        self.columns
            .iter()
            .map(String::as_str)
            .zip(vals.iter())
            .collect()
    }
}

/// A transport for published change batches. The in-process channel is the
/// default; an external sink (e.g. Kafka) implements this to fan changes out to
/// other processes for horizontal scale-out.
pub trait ChangeSink: Send + Sync {
    fn emit(&self, batch: &[Change]);
}

struct Subscriber {
    /// `None` subscribes to every table.
    table: Option<String>,
    tx: Sender<Vec<Change>>,
}

/// A captured, not-yet-published change: `(table, op, old, new)`.
type PendingChange = (String, ChangeOp, Option<Vec<Value>>, Option<Vec<Value>>);

/// The change publisher. Hand the same `Arc<Cdc>` to [`CdcBackend::wrap`] and to
/// [`Cdc::subscribe`]; the backend publishes, subscribers receive.
#[derive(Default)]
pub struct Cdc {
    subs: RwLock<Vec<Subscriber>>,
    sinks: RwLock<Vec<Arc<dyn ChangeSink>>>,
}

impl std::fmt::Debug for Cdc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cdc")
            .field("subscribers", &self.subs.read().map(|s| s.len()).unwrap_or(0))
            .finish_non_exhaustive()
    }
}

impl Cdc {
    pub fn new() -> Arc<Cdc> {
        Arc::new(Cdc::default())
    }

    /// Subscribe to committed changes. `table = Some(name)` filters to one table;
    /// `None` receives changes for every table. Returns a pull-based stream.
    pub fn subscribe(&self, table: Option<&str>) -> ChangeStream {
        let (tx, rx) = mpsc::channel();
        self.subs.write().unwrap().push(Subscriber {
            table: table.map(str::to_string),
            tx,
        });
        ChangeStream { rx }
    }

    /// Attach an external sink (e.g. Kafka). Every published batch is also
    /// `emit`ted here, on the writer's thread — keep it cheap or hand off.
    pub fn add_sink(&self, sink: Arc<dyn ChangeSink>) {
        self.sinks.write().unwrap().push(sink);
    }

    /// Publish a batch of changes (one commit's worth). Filters per subscriber by
    /// table and drops any whose receiver has hung up.
    fn publish(&self, batch: Vec<Change>) {
        if batch.is_empty() {
            return;
        }
        let subs = self.subs.read().unwrap();
        for s in subs.iter() {
            let filtered: Vec<Change> = match &s.table {
                None => batch.clone(),
                Some(t) => batch.iter().filter(|c| &c.table == t).cloned().collect(),
            };
            if !filtered.is_empty() {
                let _ = s.tx.send(filtered); // receiver gone → drop silently
            }
        }
        let sinks = self.sinks.read().unwrap();
        for sink in sinks.iter() {
            sink.emit(&batch);
        }
    }

    fn publish_one(&self, change: Change) {
        self.publish(vec![change]);
    }
}

/// A pull-based stream of committed change batches. Each item is one commit's
/// worth of changes, in commit order. Poll it from your own thread; the consumer
/// controls its own cadence and backpressure.
pub struct ChangeStream {
    rx: Receiver<Vec<Change>>,
}

impl std::fmt::Debug for ChangeStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeStream").finish_non_exhaustive()
    }
}

impl ChangeStream {
    /// Non-blocking: the next batch if one is ready, else `None`.
    pub fn poll(&self) -> Option<Vec<Change>> {
        self.rx.try_recv().ok()
    }
    /// Block until the next batch (or `None` if the publisher is gone).
    pub fn recv(&self) -> Option<Vec<Change>> {
        self.rx.recv().ok()
    }
    /// Block up to `timeout` for the next batch.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<Vec<Change>> {
        self.rx.recv_timeout(timeout).ok()
    }
    /// Drain every currently-available change into one flat vector.
    pub fn drain(&self) -> Vec<Change> {
        let mut out = Vec::new();
        while let Ok(batch) = self.rx.try_recv() {
            out.extend(batch);
        }
        out
    }
}

/// A [`SqlBackend`] decorator that publishes a [`Change`] for every committed
/// write. Wrap the backend the engine is bound to:
///
/// ```
/// use chakradb::{Database, SqlEngine};
/// use chakradb::cdc::{Cdc, CdcBackend};
/// use std::sync::Arc;
///
/// let db = Arc::new(Database::new());
/// let cdc = Cdc::new();
/// let engine = SqlEngine::with_backend(CdcBackend::wrap(db, cdc.clone()));
/// let stream = cdc.subscribe(Some("transactions"));
///
/// engine.run("CREATE TABLE transactions (id INTEGER PRIMARY KEY, amt INTEGER)").unwrap();
/// engine.run("INSERT INTO transactions VALUES (1, 500)").unwrap();
///
/// let batch = stream.poll().expect("one change");
/// assert_eq!(batch[0].table, "transactions");
/// ```
pub struct CdcBackend {
    inner: Arc<dyn SqlBackend>,
    cdc: Arc<Cdc>,
    cols: Mutex<HashMap<String, Arc<Vec<String>>>>,
}

impl std::fmt::Debug for CdcBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcBackend").finish_non_exhaustive()
    }
}

impl CdcBackend {
    /// Wrap `inner` so writes through it publish to `cdc`.
    pub fn wrap(inner: Arc<dyn SqlBackend>, cdc: Arc<Cdc>) -> Arc<dyn SqlBackend> {
        Arc::new(CdcBackend {
            inner,
            cdc,
            cols: Mutex::new(HashMap::new()),
        })
    }

    /// Column names for `table`, cached (schema is fixed after DDL).
    fn columns(&self, table: &str) -> Arc<Vec<String>> {
        if let Some(c) = self.cols.lock().unwrap().get(table) {
            return c.clone();
        }
        let names: Vec<String> = self
            .inner
            .table(table)
            .map(|t| t.schema().columns().iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default();
        let arc = Arc::new(names);
        self.cols
            .lock()
            .unwrap()
            .insert(table.to_string(), arc.clone());
        arc
    }

    fn key_index(&self, table: &str) -> usize {
        self.inner
            .table(table)
            .map(|t| t.schema().key_index())
            .unwrap_or(0)
    }

    /// The latest committed image of the row keyed `key`, for a pre-image.
    fn old_row(&self, table: &str, key: &Value) -> Option<Vec<Value>> {
        self.inner
            .table(table)
            .ok()
            .and_then(|t| t.get_latest(key))
            .map(|r| r.values)
    }

    fn change(
        &self,
        table: &str,
        op: ChangeOp,
        csn: Csn,
        old: Option<Vec<Value>>,
        new: Option<Vec<Value>>,
    ) -> Change {
        Change {
            table: table.to_string(),
            op,
            csn,
            columns: self.columns(table),
            old,
            new,
        }
    }
}

impl SqlBackend for CdcBackend {
    // --- Pure forwards (no change events) ---
    fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        self.inner.create_table(name, schema)
    }
    fn drop_table(&self, name: &str) -> Result<()> {
        self.inner.drop_table(name)
    }
    fn truncate(&self, name: &str) -> Result<()> {
        self.inner.truncate(name)
    }
    fn table(&self, name: &str) -> Result<Arc<Table>> {
        self.inner.table(name)
    }
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }
    fn snapshot(&self) -> Snapshot {
        self.inner.snapshot()
    }
    fn pin(&self) -> SnapshotPin {
        self.inner.pin()
    }

    // --- Mutations: forward, then publish ---
    fn insert(&self, table: &str, row: Row) -> Result<Csn> {
        let new = row.values.clone();
        let csn = self.inner.insert(table, row)?;
        self.cdc
            .publish_one(self.change(table, ChangeOp::Insert, csn, None, Some(new)));
        Ok(csn)
    }

    fn upsert(&self, table: &str, row: Row) -> Result<Csn> {
        let key = row.values.get(self.key_index(table)).cloned();
        let old = key.as_ref().and_then(|k| self.old_row(table, k));
        let new = row.values.clone();
        let csn = self.inner.upsert(table, row)?;
        let op = if old.is_some() {
            ChangeOp::Update
        } else {
            ChangeOp::Insert
        };
        self.cdc
            .publish_one(self.change(table, op, csn, old, Some(new)));
        Ok(csn)
    }

    fn update(&self, table: &str, row: Row) -> Result<Csn> {
        let key = row.values.get(self.key_index(table)).cloned();
        let old = key.as_ref().and_then(|k| self.old_row(table, k));
        let new = row.values.clone();
        let csn = self.inner.update(table, row)?;
        self.cdc
            .publish_one(self.change(table, ChangeOp::Update, csn, old, Some(new)));
        Ok(csn)
    }

    fn delete(&self, table: &str, key: &Value) -> Result<Csn> {
        let old = self.old_row(table, key);
        let csn = self.inner.delete(table, key)?;
        self.cdc
            .publish_one(self.change(table, ChangeOp::Delete, csn, old, None));
        Ok(csn)
    }

    fn bulk_insert(&self, table: &str, rows: Vec<Row>) -> Result<usize> {
        // COPY fast path: capture new images, apply, then publish one batch. The
        // bulk path does not surface per-row CSNs, so all share the post-load CSN.
        let images: Vec<Vec<Value>> = rows.iter().map(|r| r.values.clone()).collect();
        let n = self.inner.bulk_insert(table, rows)?;
        let csn = self.inner.snapshot().csn;
        let batch = images
            .into_iter()
            .map(|new| self.change(table, ChangeOp::Insert, csn, None, Some(new)))
            .collect();
        self.cdc.publish(batch);
        Ok(n)
    }

    fn commit_batch(&self, writes: Vec<TxnWrite>) -> Result<()> {
        // Capture pre-images BEFORE the batch applies, then publish the whole
        // transaction as one ordered batch after it commits.
        let mut pending: Vec<PendingChange> = Vec::with_capacity(writes.len());
        for w in &writes {
            match w {
                TxnWrite::Put(t, row) => {
                    let key = row.values.get(self.key_index(t)).cloned();
                    let old = key.as_ref().and_then(|k| self.old_row(t, k));
                    let op = if old.is_some() {
                        ChangeOp::Update
                    } else {
                        ChangeOp::Insert
                    };
                    pending.push((t.clone(), op, old, Some(row.values.clone())));
                }
                TxnWrite::Delete(t, key) => {
                    let old = self.old_row(t, key);
                    pending.push((t.clone(), ChangeOp::Delete, old, None));
                }
            }
        }
        self.inner.commit_batch(writes)?;
        let csn = self.inner.snapshot().csn;
        let batch = pending
            .into_iter()
            .map(|(t, op, old, new)| self.change(&t, op, csn, old, new))
            .collect();
        self.cdc.publish(batch);
        Ok(())
    }
}
