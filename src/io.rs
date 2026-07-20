//! I/O abstraction — the third unretrofittable seam.
//!
//! M0 is an in-memory prototype and does not persist anything. This module
//! exists anyway, because the design (`requirements.md` §11.1) is explicit that
//! the `Io` seam cannot be retrofitted once compaction threads, a buffer pool
//! and a table-format layer all call ambient filesystem APIs directly.
//!
//! Shape borrowed from Turso's `trait IO` / `trait File`, which is the only
//! demonstrated route to deterministic simulation testing in Rust without
//! language co-design.
//!
//! M1 adds `PosixIo`; the fault-injecting `MemIo` here is what the crash tests
//! will drive.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

/// A file handle supporting positional reads and writes.
pub trait File: Send + Sync + std::fmt::Debug {
    fn pread(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn pwrite(&self, offset: u64, buf: &[u8]) -> io::Result<usize>;
    /// Make previously written data durable.
    fn sync(&self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn truncate(&self, len: u64) -> io::Result<()>;

    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
}

/// A filesystem namespace.
pub trait Io: Send + Sync + std::fmt::Debug + 'static {
    fn open(&self, path: &str) -> io::Result<Arc<dyn File>>;
    fn remove(&self, path: &str) -> io::Result<()>;
    fn exists(&self, path: &str) -> bool;
    fn list(&self) -> Vec<String>;
}

/// Which operation a fault should target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    Read,
    Write,
    Sync,
}

/// A scheduled fault: fail the Nth occurrence of `op`.
#[derive(Debug, Clone, Copy)]
struct Fault {
    op: FaultOp,
    /// Countdown; the fault fires when this reaches zero.
    after: u64,
}

#[derive(Default, Debug)]
struct MemState {
    files: BTreeMap<String, Arc<MemFile>>,
    faults: Vec<Fault>,
}

/// In-memory filesystem with fault injection.
///
/// Deliberately supports the failure modes that actually corrupt databases:
/// an error on write/sync, and — more insidiously — *silently dropping* a
/// write that reports success (`drop_writes`), which is the lost-write case.
#[derive(Clone, Debug)]
pub struct MemIo {
    state: Arc<Mutex<MemState>>,
    drop_writes: Arc<Mutex<bool>>,
    /// Simulated device fsync latency.
    ///
    /// Zero by default, which makes tests fast — but a zero-cost sync also
    /// makes group commit look useless, because no batch has time to form.
    /// Benchmarks set a realistic value so the batching is measurable.
    sync_delay: Arc<Mutex<std::time::Duration>>,
}

impl MemIo {
    pub fn new() -> Self {
        MemIo {
            state: Arc::new(Mutex::new(MemState::default())),
            drop_writes: Arc::new(Mutex::new(false)),
            sync_delay: Arc::new(Mutex::new(std::time::Duration::ZERO)),
        }
    }

    /// Fail the `after`-th occurrence (0 = the very next one) of `op`.
    pub fn inject_fault(&self, op: FaultOp, after: u64) {
        self.state.lock().unwrap().faults.push(Fault { op, after });
    }

    /// Silently discard all subsequent writes while reporting success.
    ///
    /// Models the lost-write case — the one that produces corruption rather
    /// than a clean error.
    pub fn set_drop_writes(&self, on: bool) {
        *self.drop_writes.lock().unwrap() = on;
    }

    /// Borrow the concrete file at `path`, if it exists.
    ///
    /// Exposed for tests that need `MemFile::crash` / `durable_len` on a single
    /// file rather than the whole filesystem.
    pub fn file(&self, path: &str) -> Option<Arc<MemFile>> {
        self.state.lock().unwrap().files.get(path).cloned()
    }

    /// Model a device where fsync costs real time.
    pub fn set_sync_delay(&self, d: std::time::Duration) {
        *self.sync_delay.lock().unwrap() = d;
    }

    fn sync_cost(&self) -> std::time::Duration {
        *self.sync_delay.lock().unwrap()
    }

    /// Simulate power loss across the whole filesystem: every file reverts to
    /// its last synced image.
    ///
    /// This is the primitive the crash-consistency suite drives. Recovery must
    /// produce a database containing every write that was acknowledged before
    /// the crash, and no torn record.
    pub fn crash(&self) {
        let files: Vec<Arc<MemFile>> = self.state.lock().unwrap().files.values().cloned().collect();
        for f in files {
            f.crash();
        }
    }

    /// Total bytes that would survive a crash right now.
    pub fn durable_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .files
            .values()
            .map(|f| f.durable_len())
            .sum()
    }

    pub fn clear_faults(&self) {
        self.state.lock().unwrap().faults.clear();
        self.set_drop_writes(false);
    }

    /// Consume a fault for `op` if one is due. Returns true if it fired.
    fn check_fault(&self, op: FaultOp) -> bool {
        let mut st = self.state.lock().unwrap();
        for f in st.faults.iter_mut() {
            if f.op == op {
                if f.after == 0 {
                    // Fire once, then remove.
                    let idx = st.faults.iter().position(|x| x.op == op && x.after == 0);
                    if let Some(i) = idx {
                        st.faults.remove(i);
                    }
                    return true;
                }
                f.after -= 1;
                return false;
            }
        }
        false
    }
}

impl Default for MemIo {
    fn default() -> Self {
        Self::new()
    }
}

impl Io for MemIo {
    fn open(&self, path: &str) -> io::Result<Arc<dyn File>> {
        let mut st = self.state.lock().unwrap();
        let f = st
            .files
            .entry(path.to_string())
            .or_insert_with(|| Arc::new(MemFile::new(self.clone())))
            .clone();
        Ok(f as Arc<dyn File>)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.state.lock().unwrap().files.remove(path);
        Ok(())
    }

    fn exists(&self, path: &str) -> bool {
        self.state.lock().unwrap().files.contains_key(path)
    }

    fn list(&self) -> Vec<String> {
        self.state.lock().unwrap().files.keys().cloned().collect()
    }
}

/// A file living in memory, with separate "written" and "durable" images so
/// that a simulated crash can discard unsynced data.
#[derive(Debug)]
pub struct MemFile {
    io: MemIo,
    inner: Mutex<MemFileInner>,
}

#[derive(Default, Debug)]
struct MemFileInner {
    live: Vec<u8>,
    durable: Vec<u8>,
}

impl MemFile {
    fn new(io: MemIo) -> Self {
        MemFile {
            io,
            inner: Mutex::new(MemFileInner::default()),
        }
    }

    /// Discard everything not yet synced — i.e. simulate power loss.
    pub fn crash(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.live = inner.durable.clone();
    }

    /// Bytes that would survive a crash right now.
    pub fn durable_len(&self) -> usize {
        self.inner.lock().unwrap().durable.len()
    }
}

impl File for MemFile {
    fn pread(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if self.io.check_fault(FaultOp::Read) {
            return Err(io::Error::other("injected read fault"));
        }
        let inner = self.inner.lock().unwrap();
        let start = offset as usize;
        if start >= inner.live.len() {
            return Ok(0);
        }
        let n = buf.len().min(inner.live.len() - start);
        buf[..n].copy_from_slice(&inner.live[start..start + n]);
        Ok(n)
    }

    fn pwrite(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        if self.io.check_fault(FaultOp::Write) {
            return Err(io::Error::other("injected write fault"));
        }
        if *self.io.drop_writes.lock().unwrap() {
            // Report success, write nothing. The dangerous case.
            return Ok(buf.len());
        }
        let mut inner = self.inner.lock().unwrap();
        let end = offset as usize + buf.len();
        if inner.live.len() < end {
            inner.live.resize(end, 0);
        }
        inner.live[offset as usize..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn sync(&self) -> io::Result<()> {
        if self.io.check_fault(FaultOp::Sync) {
            return Err(io::Error::other("injected sync fault"));
        }
        let delay = self.io.sync_cost();
        if !delay.is_zero() {
            // Sleep *outside* the file lock, so concurrent appends can keep
            // landing and join the batch — which is what a real device allows.
            std::thread::sleep(delay);
        }
        let mut inner = self.inner.lock().unwrap();
        inner.durable = inner.live.clone();
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.inner.lock().unwrap().live.len() as u64)
    }

    fn truncate(&self, len: u64) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.live.resize(len as usize, 0);
        Ok(())
    }
}
