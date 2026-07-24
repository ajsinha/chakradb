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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
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

/// The observable status of a registered materialized worker.
#[derive(Clone, Debug)]
pub struct WorkerStatus {
    pub name: String,
    /// The table this worker derives from (`None` = all tables).
    pub table: Option<String>,
    /// The highest CSN it has consumed — its resume point.
    pub cursor: Csn,
    /// Whether it is still maintaining its derivation.
    pub running: bool,
}

/// A worker registered by name for observability and stop-by-name.
struct Registered {
    name: String,
    table: Option<String>,
    cursor: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
}

/// The change publisher. Hand the same `Arc<Cdc>` to [`CdcBackend::wrap`] and to
/// [`Cdc::subscribe`]; the backend publishes, subscribers receive.
#[derive(Default)]
pub struct Cdc {
    subs: RwLock<Vec<Subscriber>>,
    sinks: RwLock<Vec<Arc<dyn ChangeSink>>>,
    registry: RwLock<Vec<Registered>>,
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

    /// Register a **materialized worker** over `table`: a named, incrementally-
    /// maintained derivation of the data. The returned [`Materialized`] runs a
    /// background thread that folds every committed change into `worker` in commit
    /// order, tracks the CSN cursor, and lets you query the derived state or stop
    /// the worker. This is the disciplined "worker" primitive — a *function of the
    /// data*, not an application host.
    ///
    /// Seeding: register the worker before the table is populated to see every
    /// change, or fold the current rows in yourself first (a snapshot read) and
    /// then let the stream take over — the cursor tells you where the stream is.
    pub fn materialize<W: MaterializedWorker>(
        &self,
        table: Option<&str>,
        worker: W,
    ) -> Materialized<W> {
        Materialized::spawn(self.subscribe(table), worker)
    }

    /// Like [`materialize`](Cdc::materialize), but **named and tracked** in the
    /// registry, so the worker is observable ([`workers`](Cdc::workers)) and can
    /// be stopped by name ([`stop_worker`](Cdc::stop_worker)). Seed the worker's
    /// initial state from a snapshot *before* registering to resume/rebuild; the
    /// [`cursor`](Materialized::cursor) then tracks how far the live stream has
    /// advanced.
    pub fn register<W: MaterializedWorker>(
        &self,
        name: &str,
        table: Option<&str>,
        worker: W,
    ) -> Materialized<W> {
        let m = Materialized::spawn(self.subscribe(table), worker);
        self.registry.write().unwrap().push(Registered {
            name: name.to_string(),
            table: table.map(str::to_string),
            cursor: m.cursor.clone(),
            stop: m.stop.clone(),
            done: m.done.clone(),
        });
        m
    }

    /// A snapshot of every registered worker — name, source table, CSN cursor,
    /// and whether it is still running. The observability surface for the worker
    /// fleet.
    pub fn workers(&self) -> Vec<WorkerStatus> {
        self.registry
            .read()
            .unwrap()
            .iter()
            .map(|r| WorkerStatus {
                name: r.name.clone(),
                table: r.table.clone(),
                cursor: r.cursor.load(Ordering::Acquire),
                running: !r.stop.load(Ordering::Acquire) && !r.done.load(Ordering::Acquire),
            })
            .collect()
    }

    /// The status of one registered worker by name.
    pub fn worker(&self, name: &str) -> Option<WorkerStatus> {
        self.workers().into_iter().find(|w| w.name == name)
    }

    /// Stop a registered worker by name (idempotent). Returns `false` if no such
    /// worker is registered.
    pub fn stop_worker(&self, name: &str) -> bool {
        match self.registry.read().unwrap().iter().find(|r| r.name == name) {
            Some(r) => {
                r.stop.store(true, Ordering::Release);
                true
            }
            None => false,
        }
    }

    /// Publish a batch of changes (one commit's worth). Filters per subscriber by
    /// table and drops any whose receiver has hung up.
    fn publish(&self, batch: Vec<Change>) {
        if batch.is_empty() {
            return;
        }
        // Prune subscribers whose receiver has been dropped (a closed
        // subscription), so a started-then-closed worker frees its slot.
        let mut subs = self.subs.write().unwrap();
        subs.retain(|s| {
            let filtered: Vec<Change> = match &s.table {
                None => batch.clone(),
                Some(t) => batch.iter().filter(|c| &c.table == t).cloned().collect(),
            };
            if filtered.is_empty() {
                return true; // nothing for this subscriber, but it's still live
            }
            s.tx.send(filtered).is_ok() // false ⇒ receiver gone ⇒ drop it
        });
        drop(subs);
        let sinks = self.sinks.read().unwrap();
        for sink in sinks.iter() {
            sink.emit(&batch);
        }
    }

    fn publish_one(&self, change: Change) {
        self.publish(vec![change]);
    }
}

/// The outcome of a bounded [`ChangeStream::next_timeout`] wait.
#[derive(Debug)]
pub enum Recv {
    /// One commit's worth of changes.
    Batch(Vec<Change>),
    /// No batch arrived within the timeout (the publisher is still live).
    Timeout,
    /// The publisher is gone; no further batches will ever arrive.
    Closed,
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
    /// Block up to `timeout`, distinguishing a delivered batch from a plain
    /// timeout and from a permanently-closed stream. A worker loop uses this to
    /// poll a stop flag on timeout and exit cleanly when the publisher is gone.
    pub fn next_timeout(&self, timeout: Duration) -> Recv {
        match self.rx.recv_timeout(timeout) {
            Ok(batch) => Recv::Batch(batch),
            Err(RecvTimeoutError::Timeout) => Recv::Timeout,
            Err(RecvTimeoutError::Disconnected) => Recv::Closed,
        }
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

// ---------------------------------------------------------------------------
// Materialized workers — named, incrementally-maintained derivations.
// ---------------------------------------------------------------------------

/// A stateful derivation maintained incrementally from a table's change stream.
///
/// Implement this to define *what* is maintained (a running aggregate, a graph
/// projection, a set of alerts); the [`Materialized`] runtime owns the loop,
/// commit ordering, cursor tracking, and lifecycle. A worker is deliberately a
/// **function of the data** — it folds changes into state and nothing more.
///
/// ```
/// use chakradb::cdc::{Change, ChangeOp, MaterializedWorker};
///
/// /// Maintains the number of live rows in a table.
/// #[derive(Default)]
/// struct RowCount(i64);
/// impl MaterializedWorker for RowCount {
///     fn apply(&mut self, change: &Change) {
///         match change.op {
///             ChangeOp::Insert => self.0 += 1,
///             ChangeOp::Delete => self.0 -= 1,
///             ChangeOp::Update => {}
///         }
///     }
/// }
/// ```
pub trait MaterializedWorker: Send + 'static {
    /// Fold one committed change into the derived state.
    fn apply(&mut self, change: &Change);
    /// Called once after each commit batch is applied, with its highest CSN.
    /// Override for periodic work (e.g. a heavier recompute every N commits).
    fn on_commit(&mut self, _csn: Csn) {}
}

/// A running materialized worker: query its derived state, read its CSN cursor,
/// or stop it. The derivation is maintained on a background thread; the client
/// owns the lifecycle (`stop`, or drop).
pub struct Materialized<W> {
    state: Arc<Mutex<W>>,
    cursor: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl<W> std::fmt::Debug for Materialized<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Materialized")
            .field("cursor", &self.cursor.load(Ordering::Acquire))
            .field("running", &self.handle.lock().map(|h| h.is_some()).unwrap_or(false))
            .finish_non_exhaustive()
    }
}

impl<W: MaterializedWorker> Materialized<W> {
    fn spawn(stream: ChangeStream, worker: W) -> Self {
        let state = Arc::new(Mutex::new(worker));
        let cursor = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let (s, c, st, dn) = (state.clone(), cursor.clone(), stop.clone(), done.clone());
        let handle = thread::spawn(move || {
            while !st.load(Ordering::Acquire) {
                match stream.next_timeout(Duration::from_millis(100)) {
                    Recv::Batch(batch) => {
                        let mut w = s.lock().unwrap();
                        let mut last = c.load(Ordering::Acquire);
                        for change in &batch {
                            w.apply(change);
                            last = last.max(change.csn);
                        }
                        w.on_commit(last);
                        drop(w);
                        c.store(last, Ordering::Release);
                    }
                    Recv::Timeout => continue,
                    Recv::Closed => break,
                }
            }
            dn.store(true, Ordering::Release); // publisher gone or stopped
        });
        Materialized {
            state,
            cursor,
            stop,
            done,
            handle: Mutex::new(Some(handle)),
        }
    }

    /// Read the derived state under the lock. Use for point queries into the
    /// materialized result: `m.query(|s| s.total)`.
    pub fn query<R>(&self, f: impl FnOnce(&W) -> R) -> R {
        f(&self.state.lock().unwrap())
    }

    /// Mutate the derived state under the lock (e.g. to reset or seed it).
    pub fn update<R>(&self, f: impl FnOnce(&mut W) -> R) -> R {
        f(&mut self.state.lock().unwrap())
    }

    /// The highest CSN applied so far — how far the derivation has consumed the
    /// change stream. Persist it to resume after a restart.
    pub fn cursor(&self) -> Csn {
        self.cursor.load(Ordering::Acquire)
    }

    /// True while the background thread is still maintaining the derivation
    /// (not stopped and the publisher is still live).
    pub fn is_running(&self) -> bool {
        !self.stop.load(Ordering::Acquire) && !self.done.load(Ordering::Acquire)
    }

    /// Stop maintaining the derivation and join the worker thread. Idempotent;
    /// the last-computed state remains queryable.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl<W> Drop for Materialized<W> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.lock().unwrap().take() {
            let _ = handle.join();
        }
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
