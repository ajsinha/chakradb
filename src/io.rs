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
pub trait File: Send + Sync {
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
pub trait Io: Send + Sync + 'static {
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
}

impl MemIo {
    pub fn new() -> Self {
        MemIo {
            state: Arc::new(Mutex::new(MemState::default())),
            drop_writes: Arc::new(Mutex::new(false)),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_read_roundtrips() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        f.pwrite(0, b"hello world").unwrap();
        let mut buf = [0u8; 11];
        assert_eq!(f.pread(0, &mut buf).unwrap(), 11);
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn read_past_end_returns_zero() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        f.pwrite(0, b"abc").unwrap();
        let mut buf = [0u8; 4];
        assert_eq!(f.pread(100, &mut buf).unwrap(), 0);
    }

    #[test]
    fn partial_read_at_tail() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        f.pwrite(0, b"abcdef").unwrap();
        let mut buf = [0u8; 10];
        assert_eq!(f.pread(4, &mut buf).unwrap(), 2);
        assert_eq!(&buf[..2], b"ef");
    }

    #[test]
    fn sparse_write_zero_fills() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        f.pwrite(4, b"xy").unwrap();
        assert_eq!(f.len().unwrap(), 6);
        let mut buf = [9u8; 6];
        f.pread(0, &mut buf).unwrap();
        assert_eq!(&buf, b"\0\0\0\0xy");
    }

    #[test]
    fn truncate_shrinks_and_grows() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        f.pwrite(0, b"abcdef").unwrap();
        f.truncate(3).unwrap();
        assert_eq!(f.len().unwrap(), 3);
        f.truncate(5).unwrap();
        assert_eq!(f.len().unwrap(), 5);
    }

    #[test]
    fn same_path_returns_same_file() {
        let io = MemIo::new();
        let a = io.open("shared").unwrap();
        a.pwrite(0, b"z").unwrap();
        let b = io.open("shared").unwrap();
        let mut buf = [0u8; 1];
        b.pread(0, &mut buf).unwrap();
        assert_eq!(&buf, b"z");
    }

    #[test]
    fn exists_list_and_remove() {
        let io = MemIo::new();
        assert!(!io.exists("x"));
        io.open("x").unwrap();
        io.open("y").unwrap();
        assert!(io.exists("x"));
        assert_eq!(io.list(), vec!["x".to_string(), "y".to_string()]);
        io.remove("x").unwrap();
        assert!(!io.exists("x"));
    }

    #[test]
    fn injected_write_fault_fires_once() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        io.inject_fault(FaultOp::Write, 0);
        assert!(f.pwrite(0, b"x").is_err());
        // Second write succeeds: the fault was consumed.
        assert!(f.pwrite(0, b"x").is_ok());
    }

    #[test]
    fn injected_fault_respects_countdown() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        io.inject_fault(FaultOp::Write, 2);
        assert!(f.pwrite(0, b"1").is_ok());
        assert!(f.pwrite(1, b"2").is_ok());
        assert!(f.pwrite(2, b"3").is_err());
    }

    #[test]
    fn injected_sync_and_read_faults() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        io.inject_fault(FaultOp::Sync, 0);
        assert!(f.sync().is_err());
        io.inject_fault(FaultOp::Read, 0);
        let mut buf = [0u8; 1];
        assert!(f.pread(0, &mut buf).is_err());
    }

    #[test]
    fn clear_faults_restores_normal_operation() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        io.inject_fault(FaultOp::Write, 0);
        io.clear_faults();
        assert!(f.pwrite(0, b"x").is_ok());
    }

    #[test]
    fn crash_discards_unsynced_writes() {
        let io = MemIo::new();
        io.open("a").unwrap();
        let raw = {
            let st = io.state.lock().unwrap();
            st.files.get("a").unwrap().clone()
        };
        raw.pwrite(0, b"durable").unwrap();
        raw.sync().unwrap();
        raw.pwrite(7, b"-lost").unwrap();
        assert_eq!(raw.len().unwrap(), 12);
        raw.crash();
        assert_eq!(raw.len().unwrap(), 7);
        let mut buf = [0u8; 7];
        raw.pread(0, &mut buf).unwrap();
        assert_eq!(&buf, b"durable");
    }

    #[test]
    fn drop_writes_loses_data_silently() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        f.pwrite(0, b"real").unwrap();
        io.set_drop_writes(true);
        // Reports success...
        assert_eq!(f.pwrite(4, b"ghost").unwrap(), 5);
        // ...but nothing was stored.
        assert_eq!(f.len().unwrap(), 4);
    }

    #[test]
    fn durable_len_tracks_sync() {
        let io = MemIo::new();
        io.open("a").unwrap();
        let raw = {
            let st = io.state.lock().unwrap();
            st.files.get("a").unwrap().clone()
        };
        raw.pwrite(0, b"12345").unwrap();
        assert_eq!(raw.durable_len(), 0);
        raw.sync().unwrap();
        assert_eq!(raw.durable_len(), 5);
    }

    #[test]
    fn io_trait_is_object_safe() {
        let io: Box<dyn Io> = Box::new(MemIo::new());
        io.open("a").unwrap();
        assert!(io.exists("a"));
    }

    #[test]
    fn is_empty_reflects_length() {
        let io = MemIo::new();
        let f = io.open("a").unwrap();
        assert!(f.is_empty().unwrap());
        f.pwrite(0, b"x").unwrap();
        assert!(!f.is_empty().unwrap());
    }
}
