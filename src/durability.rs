//! Durability modes and group commit.
//!
//! `requirements.md` §7.2 insists these be named honestly, because v1.0 of the
//! spec claimed 1M rows/sec with immediate acknowledgement *and* ACID
//! durability — which are incompatible. Group commit is the honest answer:
//! batching amortises fsync across many transactions and reaches high
//! throughput without lying about what survives a power cut.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// What a successful commit promises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// fsync before acknowledging. Bounded by device fsync latency.
    Sync,
    /// Batched fsync; acknowledge after the group's fsync completes.
    /// High throughput, sub-millisecond latency, **no data loss**.
    #[default]
    Group,
    /// Acknowledge before fsync. Fastest, and **loses bounded data on power
    /// failure**. Named `Async` rather than "fast" so nobody chooses it by
    /// accident.
    Async,
}

impl Durability {
    /// Does a commit in this mode wait for durability before returning?
    pub fn waits_for_disk(self) -> bool {
        matches!(self, Durability::Sync | Durability::Group)
    }

    /// Can this mode lose acknowledged writes on power loss?
    pub fn may_lose_data(self) -> bool {
        matches!(self, Durability::Async)
    }

    pub fn name(self) -> &'static str {
        match self {
            Durability::Sync => "sync",
            Durability::Group => "group",
            Durability::Async => "async",
        }
    }
}

/// Coordinates batched fsync across concurrent committers.
///
/// The contract: a writer appends its record, then calls
/// [`GroupCommit::wait_for`] with the offset it needs durable. One thread
/// performs the sync on behalf of everyone waiting, and the rest sleep until
/// the durable watermark passes them. That turns N fsyncs into one.
#[derive(Debug)]
pub struct GroupCommit {
    /// Highest byte offset known durable.
    durable: AtomicU64,
    /// Generation counter, bumped on every sync, so waiters can detect progress.
    state: Mutex<GroupState>,
    cv: Condvar,
}

#[derive(Debug, Default)]
struct GroupState {
    /// A sync is currently in flight.
    syncing: bool,
    /// Number of completed syncs — the thing waiters watch.
    generation: u64,
}

/// What a caller should do after registering interest in an offset.
#[derive(Debug, PartialEq, Eq)]
pub enum CommitAction {
    /// Already durable; proceed.
    Done,
    /// You are the leader: perform the sync, then call [`GroupCommit::complete`].
    Sync,
}

impl GroupCommit {
    pub fn new() -> Self {
        GroupCommit {
            durable: AtomicU64::new(0),
            state: Mutex::new(GroupState::default()),
            cv: Condvar::new(),
        }
    }

    pub fn durable_offset(&self) -> u64 {
        self.durable.load(Ordering::SeqCst)
    }

    /// Claim leadership of a sync, or wait for one already in flight.
    ///
    /// Returns [`CommitAction::Sync`] to exactly one caller per batch.
    pub fn begin(&self, needed: u64) -> CommitAction {
        if self.durable_offset() >= needed {
            return CommitAction::Done;
        }
        let mut st = self.state.lock().unwrap();
        loop {
            if self.durable_offset() >= needed {
                return CommitAction::Done;
            }
            if !st.syncing {
                st.syncing = true;
                return CommitAction::Sync;
            }
            // Someone else is syncing; their sync may cover us.
            let gen = st.generation;
            st = self.cv.wait(st).unwrap();
            if st.generation > gen && self.durable_offset() >= needed {
                return CommitAction::Done;
            }
        }
    }

    /// Publish a completed sync up to `offset` and wake every waiter.
    pub fn complete(&self, offset: u64) {
        let mut st = self.state.lock().unwrap();
        // Monotonic: a slow sync must never lower the watermark.
        let prev = self.durable.load(Ordering::SeqCst);
        if offset > prev {
            self.durable.store(offset, Ordering::SeqCst);
        }
        st.syncing = false;
        st.generation += 1;
        drop(st);
        self.cv.notify_all();
    }

    /// Forcibly set the watermark, including *downwards*.
    ///
    /// Only valid when the underlying file has been truncated, and only under
    /// whatever lock serialises appends. [`complete`](Self::complete) is
    /// deliberately monotonic so a slow sync cannot lower the watermark — but
    /// truncation genuinely does invalidate it, and without this the next
    /// append would see a stale high watermark, skip its fsync, and be lost on
    /// a crash.
    pub fn reset_to(&self, offset: u64) {
        let mut st = self.state.lock().unwrap();
        self.durable.store(offset, Ordering::SeqCst);
        st.syncing = false;
        st.generation += 1;
        drop(st);
        self.cv.notify_all();
    }

    /// Abandon leadership without publishing progress (the sync failed).
    pub fn abort(&self) {
        let mut st = self.state.lock().unwrap();
        st.syncing = false;
        st.generation += 1;
        drop(st);
        self.cv.notify_all();
    }

    /// Block until `needed` is durable, up to `timeout`. Returns whether it is.
    pub fn wait_until(&self, needed: u64, timeout: Duration) -> bool {
        let mut st = self.state.lock().unwrap();
        loop {
            if self.durable_offset() >= needed {
                return true;
            }
            let (next, res) = self.cv.wait_timeout(st, timeout).unwrap();
            st = next;
            if res.timed_out() {
                return self.durable_offset() >= needed;
            }
        }
    }
}

impl Default for GroupCommit {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn mode_semantics_are_explicit() {
        assert!(Durability::Sync.waits_for_disk());
        assert!(Durability::Group.waits_for_disk());
        assert!(!Durability::Async.waits_for_disk());

        assert!(!Durability::Sync.may_lose_data());
        assert!(!Durability::Group.may_lose_data());
        assert!(Durability::Async.may_lose_data(), "async must admit loss");
    }

    #[test]
    fn default_is_group() {
        assert_eq!(Durability::default(), Durability::Group);
        assert!(!Durability::default().may_lose_data());
    }

    #[test]
    fn names_are_stable() {
        assert_eq!(Durability::Sync.name(), "sync");
        assert_eq!(Durability::Group.name(), "group");
        assert_eq!(Durability::Async.name(), "async");
    }

    #[test]
    fn starts_with_nothing_durable() {
        assert_eq!(GroupCommit::new().durable_offset(), 0);
    }

    #[test]
    fn already_durable_needs_no_sync() {
        let g = GroupCommit::new();
        g.complete(100);
        assert_eq!(g.begin(50), CommitAction::Done);
        assert_eq!(g.begin(100), CommitAction::Done);
    }

    #[test]
    fn first_caller_becomes_leader() {
        let g = GroupCommit::new();
        assert_eq!(g.begin(10), CommitAction::Sync);
        g.complete(10);
        assert_eq!(g.durable_offset(), 10);
    }

    #[test]
    fn watermark_is_monotonic() {
        let g = GroupCommit::new();
        g.complete(100);
        g.complete(50);
        assert_eq!(g.durable_offset(), 100, "watermark went backwards");
    }

    #[test]
    fn abort_releases_leadership() {
        let g = GroupCommit::new();
        assert_eq!(g.begin(10), CommitAction::Sync);
        g.abort();
        assert_eq!(g.durable_offset(), 0);
        // A later caller can now lead.
        assert_eq!(g.begin(10), CommitAction::Sync);
    }

    #[test]
    fn reset_can_lower_the_watermark_after_truncation() {
        let g = GroupCommit::new();
        g.complete(1000);
        assert_eq!(g.durable_offset(), 1000);
        g.reset_to(0);
        assert_eq!(g.durable_offset(), 0, "reset must be able to lower");
        // And the next committer is forced to actually sync.
        assert_eq!(g.begin(10), CommitAction::Sync);
    }

    #[test]
    fn one_sync_serves_many_waiters() {
        // The whole point: N committers, far fewer than N syncs.
        let g = Arc::new(GroupCommit::new());
        let syncs = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..16)
            .map(|i| {
                let g = g.clone();
                let syncs = syncs.clone();
                thread::spawn(move || {
                    let needed = (i as u64 % 4) + 1;
                    match g.begin(needed) {
                        CommitAction::Sync => {
                            syncs.fetch_add(1, Ordering::SeqCst);
                            // Pretend to fsync everything written so far.
                            g.complete(100);
                        }
                        CommitAction::Done => {}
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(g.durable_offset() >= 4);
        let n = syncs.load(Ordering::SeqCst);
        assert!((1..=16).contains(&n));
        assert!(n < 16, "group commit degenerated to one sync per writer");
    }

    #[test]
    fn waiters_wake_when_watermark_passes_them() {
        let g = Arc::new(GroupCommit::new());
        let waiter = {
            let g = g.clone();
            thread::spawn(move || g.wait_until(500, Duration::from_secs(5)))
        };
        thread::sleep(Duration::from_millis(20));
        g.complete(500);
        assert!(waiter.join().unwrap());
    }

    #[test]
    fn wait_times_out_when_no_progress() {
        let g = GroupCommit::new();
        assert!(!g.wait_until(1000, Duration::from_millis(30)));
    }

    #[test]
    fn concurrent_leaders_never_overlap() {
        // At most one thread may hold sync leadership at a time.
        let g = Arc::new(GroupCommit::new());
        let concurrent = Arc::new(AtomicU64::new(0));
        let max_seen = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let g = g.clone();
                let concurrent = concurrent.clone();
                let max_seen = max_seen.clone();
                thread::spawn(move || {
                    for round in 1..50u64 {
                        if let CommitAction::Sync = g.begin(round * 10) {
                            let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                            max_seen.fetch_max(now, Ordering::SeqCst);
                            concurrent.fetch_sub(1, Ordering::SeqCst);
                            g.complete(round * 10);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(max_seen.load(Ordering::SeqCst), 1, "two leaders at once");
    }
}
