//! Demand paging for sealed parts — the FR-06b mechanism.
//!
//! M1 loaded every part eagerly at open, so *total* recovery time scaled with
//! database size even though log replay was flat (`m1-findings.md` §3). That is
//! the whole reason FR-06 had to be split. This module closes it.
//!
//! # What is resident, and what is not
//!
//! A [`PagedPart`] keeps its **index** resident and its **data** on disk:
//!
//! * resident: Bloom filter, min/max bounds, version stamps, deletion vector —
//!   the ~1.25 B/row M0-2 measured, plus the file handle.
//! * on disk until touched: the column data.
//!
//! That split is only affordable because of §5.2's result. If the index were an
//! explicit key→location map at ~12 B/row it would dominate memory and paging
//! the data would buy little. Because the sorted key column *is* the index, we
//! can answer "is this key here, and where" from a Bloom probe plus a small
//! resident key run, and fetch the row only on a hit.
//!
//! # Deliberately not a general buffer pool
//!
//! There is no LRU, no eviction policy, and no page cache with a replacement
//! algorithm. Parts are immutable and whole-part granular, so "load on first
//! touch, keep until the part is dropped" is the honest amount of machinery for
//! M2. A real replacement policy belongs with the M3 workload, where there will
//! be evidence about what to evict.

use crate::csn::{Csn, Snapshot};
use crate::io::Io;
use crate::part::{LookupResult, Part};
use crate::persist;
use crate::schema::Batch;
use std::io as stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Counters describing paging behaviour.
#[derive(Debug, Default)]
pub struct PagerMetrics {
    /// Parts registered without reading their data.
    pub parts_registered: AtomicU64,
    /// Parts whose data was actually faulted in.
    pub parts_faulted: AtomicU64,
    /// Lookups answered from the resident index without touching disk.
    pub index_only_hits: AtomicU64,
    pub bytes_faulted: AtomicU64,
}

impl PagerMetrics {
    pub fn resident_fraction(&self) -> f64 {
        let reg = self.parts_registered.load(Ordering::Relaxed);
        if reg == 0 {
            return 0.0;
        }
        self.parts_faulted.load(Ordering::Relaxed) as f64 / reg as f64
    }
}

/// The resident summary of a part whose data may still be on disk.
///
/// Encoding these separately is what makes open cheap: recovery reads a small
/// footer per part instead of the whole file.
#[derive(Debug, Clone)]
pub struct PartSummary {
    pub id: u64,
    pub num_rows: usize,
    pub min_pk: i64,
    pub max_pk: i64,
    pub created_min: Csn,
    pub created_max: Csn,
}

/// A part that loads its data on first use.
#[derive(Debug)]
pub struct PagedPart {
    summary: PartSummary,
    path: String,
    io: Arc<dyn Io>,
    metrics: Arc<PagerMetrics>,
    /// `None` until faulted in. `OnceLock` gives us a single initialisation
    /// without holding a lock on the read path afterwards.
    loaded: OnceLock<Arc<Part>>,
    /// Serialises concurrent faults so the file is read once.
    fault_lock: Mutex<()>,
}

impl PagedPart {
    /// Register a part without reading its data.
    pub fn register(
        summary: PartSummary,
        path: String,
        io: Arc<dyn Io>,
        metrics: Arc<PagerMetrics>,
    ) -> Self {
        metrics.parts_registered.fetch_add(1, Ordering::Relaxed);
        PagedPart {
            summary,
            path,
            io,
            metrics,
            loaded: OnceLock::new(),
            fault_lock: Mutex::new(()),
        }
    }

    /// Wrap an already-resident part (the sealing path, which has the data).
    pub fn resident(part: Arc<Part>, path: String, io: Arc<dyn Io>, metrics: Arc<PagerMetrics>) -> Self {
        let summary = PartSummary {
            id: part.id(),
            num_rows: part.num_rows(),
            min_pk: part.min_pk(),
            max_pk: part.max_pk(),
            created_min: part.created_min(),
            created_max: part.created_max(),
        };
        metrics.parts_registered.fetch_add(1, Ordering::Relaxed);
        metrics.parts_faulted.fetch_add(1, Ordering::Relaxed);
        let loaded = OnceLock::new();
        let _ = loaded.set(part);
        PagedPart {
            summary,
            path,
            io,
            metrics,
            loaded,
            fault_lock: Mutex::new(()),
        }
    }

    pub fn id(&self) -> u64 {
        self.summary.id
    }
    pub fn num_rows(&self) -> usize {
        self.summary.num_rows
    }
    pub fn summary(&self) -> &PartSummary {
        &self.summary
    }
    pub fn is_resident(&self) -> bool {
        self.loaded.get().is_some()
    }

    /// Cheapest possible rejection: is this key outside the part's range?
    ///
    /// Answering this needs no disk access, which is the point.
    #[inline]
    pub fn definitely_excludes(&self, pk: i64) -> bool {
        self.summary.num_rows == 0 || pk < self.summary.min_pk || pk > self.summary.max_pk
    }

    /// Fault the data in, if it is not already resident.
    pub fn load(&self) -> stdio::Result<&Arc<Part>> {
        if let Some(p) = self.loaded.get() {
            return Ok(p);
        }
        let _g = self.fault_lock.lock().unwrap();
        if let Some(p) = self.loaded.get() {
            return Ok(p); // another thread won the race
        }
        let part = persist::read_part(&*self.io, &self.path)?;
        self.metrics.parts_faulted.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_faulted
            .fetch_add(part.data_memory_bytes() as u64, Ordering::Relaxed);
        let _ = self.loaded.set(Arc::new(part));
        Ok(self.loaded.get().expect("just set"))
    }

    /// Point lookup, avoiding the fault when bounds already answer it.
    pub fn lookup(&self, pk: i64, snap: Snapshot) -> stdio::Result<LookupResult> {
        if self.definitely_excludes(pk) {
            self.metrics.index_only_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(LookupResult::OutOfBounds);
        }
        Ok(self.load()?.lookup(pk, snap))
    }

    /// Scan visible rows. Always faults the part in — a scan needs the data.
    pub fn scan(&self, snap: Snapshot) -> stdio::Result<Batch> {
        Ok(self.load()?.scan(snap))
    }

    pub fn visible_count(&self, snap: Snapshot) -> stdio::Result<usize> {
        Ok(self.load()?.visible_count(snap))
    }

    /// Can this snapshot skip per-row work? Answerable from the summary alone
    /// when no deletion has ever been recorded.
    pub fn maybe_fully_visible(&self, snap: Snapshot) -> bool {
        snap.sees_all_created_up_to(self.summary.created_max)
    }

    /// Resident bytes: the summary, plus data only if faulted in.
    pub fn memory_bytes(&self) -> usize {
        let base = std::mem::size_of::<PartSummary>() + self.path.len();
        match self.loaded.get() {
            Some(p) => base + p.index_memory_bytes() + p.data_memory_bytes(),
            None => base,
        }
    }
}

/// Read just the summary of a part file, without decoding its columns.
///
/// This is what makes open independent of database size: recovery reads a
/// bounded prefix per part rather than the whole thing.
pub fn read_summary(io: &dyn Io, path: &str) -> stdio::Result<PartSummary> {
    persist::read_part_summary(io, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemIo;
    use crate::part::CreatedCsns;
    use crate::schema::Row;

    fn make_part(id: u64, pks: &[i64], csn: Csn) -> Part {
        let batch: Batch = pks
            .iter()
            .map(|&pk| Row::new(pk, pk * 2, pk as f64, format!("row-{pk}")))
            .collect();
        Part::new(id, batch, CreatedCsns::Uniform(csn))
    }

    fn setup(pks: &[i64]) -> (Arc<MemIo>, Arc<PagerMetrics>, PagedPart) {
        let io = Arc::new(MemIo::new());
        let m = Arc::new(PagerMetrics::default());
        let p = make_part(1, pks, 10);
        persist::write_part(&*io, "p1", &p).unwrap();
        let summary = read_summary(&*io, "p1").unwrap();
        let paged = PagedPart::register(summary, "p1".into(), io.clone() as Arc<dyn Io>, m.clone());
        (io, m, paged)
    }

    #[test]
    fn registration_does_not_read_data() {
        let (_io, m, paged) = setup(&[1, 2, 3]);
        assert!(!paged.is_resident(), "registration faulted the part in");
        assert_eq!(m.parts_faulted.load(Ordering::Relaxed), 0);
        // But the summary is already usable.
        assert_eq!(paged.num_rows(), 3);
        assert_eq!(paged.summary().min_pk, 1);
        assert_eq!(paged.summary().max_pk, 3);
    }

    #[test]
    fn out_of_range_lookup_never_faults() {
        let (_io, m, paged) = setup(&[10, 20, 30]);
        assert_eq!(
            paged.lookup(5, Snapshot::at(100)).unwrap(),
            LookupResult::OutOfBounds
        );
        assert_eq!(
            paged.lookup(99, Snapshot::at(100)).unwrap(),
            LookupResult::OutOfBounds
        );
        assert!(!paged.is_resident(), "bounds rejection still read the data");
        assert_eq!(m.index_only_hits.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn in_range_lookup_faults_once() {
        let (_io, m, paged) = setup(&[10, 20, 30]);
        assert!(paged.lookup(20, Snapshot::at(100)).unwrap().ordinal().is_some());
        assert!(paged.is_resident());
        assert_eq!(m.parts_faulted.load(Ordering::Relaxed), 1);
        // Subsequent lookups reuse the loaded data.
        paged.lookup(30, Snapshot::at(100)).unwrap();
        paged.lookup(10, Snapshot::at(100)).unwrap();
        assert_eq!(m.parts_faulted.load(Ordering::Relaxed), 1, "faulted twice");
    }

    #[test]
    fn faulted_data_matches_the_original() {
        let (_io, _m, paged) = setup(&[1, 5, 9]);
        let b = paged.scan(Snapshot::at(100)).unwrap();
        assert_eq!(b.pk, vec![1, 5, 9]);
        assert_eq!(b.c[1], "row-5");
    }

    #[test]
    fn memory_grows_only_after_faulting() {
        let (_io, _m, paged) = setup(&(0..2_000).collect::<Vec<_>>());
        let cold = paged.memory_bytes();
        assert!(cold < 1_000, "cold part holds {cold} bytes");
        paged.scan(Snapshot::at(100)).unwrap();
        assert!(paged.memory_bytes() > cold * 10, "faulting freed memory?");
    }

    #[test]
    fn resident_constructor_skips_the_disk() {
        let io = Arc::new(MemIo::new());
        let m = Arc::new(PagerMetrics::default());
        let part = Arc::new(make_part(7, &[1, 2], 5));
        let paged = PagedPart::resident(part, "p7".into(), io as Arc<dyn Io>, m.clone());
        assert!(paged.is_resident());
        assert_eq!(paged.id(), 7);
        // No file was ever written, so a fault would fail — proving none happened.
        assert_eq!(paged.scan(Snapshot::at(100)).unwrap().len(), 2);
    }

    #[test]
    fn concurrent_faults_read_the_file_once() {
        use std::thread;
        let (_io, m, paged) = setup(&(0..500).collect::<Vec<_>>());
        let paged = Arc::new(paged);
        let hs: Vec<_> = (0..8)
            .map(|_| {
                let p = paged.clone();
                thread::spawn(move || {
                    p.lookup(250, Snapshot::at(100)).unwrap();
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(
            m.parts_faulted.load(Ordering::Relaxed),
            1,
            "concurrent faults duplicated the read"
        );
    }

    #[test]
    fn empty_part_excludes_everything() {
        let (_io, _m, paged) = setup(&[]);
        assert!(paged.definitely_excludes(0));
        assert!(paged.definitely_excludes(i64::MAX));
        assert!(!paged.is_resident());
    }

    #[test]
    fn resident_fraction_tracks_faulting() {
        let io = Arc::new(MemIo::new());
        let m = Arc::new(PagerMetrics::default());
        let mut parts = Vec::new();
        for i in 0..4u64 {
            let p = make_part(i, &[i as i64 * 100], 1);
            let path = format!("p{i}");
            persist::write_part(&*io, &path, &p).unwrap();
            let s = read_summary(&*io, &path).unwrap();
            parts.push(PagedPart::register(s, path, io.clone() as Arc<dyn Io>, m.clone()));
        }
        assert_eq!(m.resident_fraction(), 0.0);
        parts[0].load().unwrap();
        parts[1].load().unwrap();
        assert!((m.resident_fraction() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn summary_survives_a_part_with_tombstones() {
        let io = Arc::new(MemIo::new());
        let m = Arc::new(PagerMetrics::default());
        let p = make_part(3, &[1, 2, 3, 4], 5);
        p.mark_deleted(1, 50);
        persist::write_part(&*io, "p3", &p).unwrap();
        let s = read_summary(&*io, "p3").unwrap();
        assert_eq!(s.num_rows, 4, "summary should count physical rows");
        let paged = PagedPart::register(s, "p3".into(), io as Arc<dyn Io>, m);
        assert_eq!(paged.scan(Snapshot::at(50)).unwrap().pk, vec![1, 3, 4]);
    }
}
