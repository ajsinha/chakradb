//! Time abstraction — the second unretrofittable seam.
//!
//! Nothing in the engine may call `Instant::now()` directly. All time flows
//! through a `Clock`, so that a simulation can advance time deterministically
//! and a test can make a timeout fire without sleeping.
//!
//! See `requirements.md` §11.1 for why this must exist from M0 rather than
//! being added later.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A source of monotonic time.
///
/// Implementations must be cheap to call: the engine consults the clock on
/// hot paths such as compaction scheduling.
pub trait Clock: Send + Sync + 'static {
    /// Nanoseconds since an arbitrary but fixed epoch. Monotonic.
    fn now_nanos(&self) -> u64;

    /// Convenience wrapper.
    fn now_micros(&self) -> u64 {
        self.now_nanos() / 1_000
    }

    /// Convenience wrapper.
    fn now_millis(&self) -> u64 {
        self.now_nanos() / 1_000_000
    }
}

/// Wall-clock time, backed by `std::time::Instant`.
#[derive(Debug)]
pub struct RealClock {
    origin: Instant,
}

impl RealClock {
    pub fn new() -> Self {
        RealClock {
            origin: Instant::now(),
        }
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RealClock {
    fn now_nanos(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }
}

/// Virtual time. Advances only when explicitly told to.
///
/// This is what makes deterministic simulation possible: a test can advance
/// an hour instantly, and two runs with the same schedule observe identical
/// timestamps.
#[derive(Clone, Debug)]
pub struct SimClock {
    nanos: Arc<AtomicU64>,
}

impl SimClock {
    pub fn new() -> Self {
        SimClock {
            nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start at a specific virtual instant.
    pub fn starting_at(nanos: u64) -> Self {
        SimClock {
            nanos: Arc::new(AtomicU64::new(nanos)),
        }
    }

    /// Move virtual time forward. Returns the new value.
    pub fn advance(&self, by: Duration) -> u64 {
        self.advance_nanos(by.as_nanos() as u64)
    }

    /// Move virtual time forward by raw nanoseconds. Returns the new value.
    pub fn advance_nanos(&self, by: u64) -> u64 {
        self.nanos.fetch_add(by, Ordering::SeqCst) + by
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SimClock {
    fn now_nanos(&self) -> u64 {
        self.nanos.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn sim_clock_starts_at_zero() {
        let c = SimClock::new();
        assert_eq!(c.now_nanos(), 0);
    }

    #[test]
    fn sim_clock_only_moves_when_told() {
        let c = SimClock::new();
        let t0 = c.now_nanos();
        // Real time passes here; virtual time must not.
        for _ in 0..100_000 {
            std::hint::black_box(0);
        }
        assert_eq!(c.now_nanos(), t0);
    }

    #[test]
    fn sim_clock_advances_exactly() {
        let c = SimClock::new();
        c.advance(Duration::from_secs(1));
        assert_eq!(c.now_nanos(), 1_000_000_000);
        assert_eq!(c.now_micros(), 1_000_000);
        assert_eq!(c.now_millis(), 1_000);
    }

    #[test]
    fn sim_clock_advance_returns_new_value() {
        let c = SimClock::new();
        assert_eq!(c.advance_nanos(500), 500);
        assert_eq!(c.advance_nanos(250), 750);
    }

    #[test]
    fn sim_clock_starting_at_offset() {
        let c = SimClock::starting_at(9_000);
        assert_eq!(c.now_nanos(), 9_000);
        c.advance_nanos(1_000);
        assert_eq!(c.now_nanos(), 10_000);
    }

    #[test]
    fn sim_clock_shares_time_across_clones() {
        let a = SimClock::new();
        let b = a.clone();
        a.advance_nanos(42);
        assert_eq!(b.now_nanos(), 42);
    }

    #[test]
    fn sim_clock_is_monotonic_under_concurrent_advance() {
        let c = SimClock::new();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let c = c.clone();
                thread::spawn(move || {
                    for _ in 0..1000 {
                        c.advance_nanos(1);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(c.now_nanos(), 8_000);
    }

    #[test]
    fn real_clock_is_monotonic() {
        let c = RealClock::new();
        let mut last = c.now_nanos();
        for _ in 0..1000 {
            let t = c.now_nanos();
            assert!(t >= last, "clock went backwards: {t} < {last}");
            last = t;
        }
    }

    #[test]
    fn real_clock_actually_advances() {
        let c = RealClock::new();
        let t0 = c.now_nanos();
        thread::sleep(Duration::from_millis(5));
        assert!(c.now_nanos() > t0);
    }

    #[test]
    fn clock_trait_is_object_safe() {
        let clocks: Vec<Box<dyn Clock>> =
            vec![Box::new(RealClock::new()), Box::new(SimClock::new())];
        for c in &clocks {
            let _ = c.now_millis();
        }
    }
}
