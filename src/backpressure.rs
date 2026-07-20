//! Ingest backpressure.
//!
//! `requirements.md` §5.4: *"when compaction debt exceeds a threshold, ingest is
//! explicitly slowed and the condition is observable. Silent degradation is
//! forbidden."*
//!
//! M0 measured why this matters. Without compaction, sustained keyed updates
//! degraded scans 3.77×; the debt has to be bounded, and the only honest way to
//! bound it is to slow the producer rather than let readers absorb the cost.
//!
//! Debt here is **part count**, because M0-3 showed that is what drives lookup
//! fan-out, which degrades the *write* path as well as the read path.

use crate::clock::Clock;
use crate::metrics::Metrics;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Thresholds governing when ingest is slowed.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Below this many parts, ingest runs at full speed.
    pub soft_limit: usize,
    /// At or above this, ingest stalls until compaction catches up.
    pub hard_limit: usize,
    /// Delay applied at the soft limit; scales linearly to `max_delay` at hard.
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Cap on any single stall, so a stuck compactor cannot hang a writer
    /// forever. Exceeding it is a bug worth surfacing, not tolerating.
    pub stall_cap: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        BackpressureConfig {
            soft_limit: 12,
            hard_limit: 48,
            base_delay: Duration::from_micros(50),
            max_delay: Duration::from_millis(5),
            stall_cap: Duration::from_millis(100),
        }
    }
}

/// What ingest should do right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pressure {
    /// No debt; proceed immediately.
    None,
    /// Debt is building; delay by this much.
    Throttle(Duration),
    /// Debt is at the limit; stall for this long, then re-check.
    Stall(Duration),
}

impl Pressure {
    pub fn delay(self) -> Duration {
        match self {
            Pressure::None => Duration::ZERO,
            Pressure::Throttle(d) | Pressure::Stall(d) => d,
        }
    }
    pub fn is_engaged(self) -> bool {
        !matches!(self, Pressure::None)
    }
}

/// Computes and applies ingest delay from compaction debt.
#[derive(Debug)]
pub struct Backpressure {
    config: BackpressureConfig,
}

impl Backpressure {
    pub fn new(config: BackpressureConfig) -> Self {
        Backpressure { config }
    }

    pub fn config(&self) -> &BackpressureConfig {
        &self.config
    }

    /// Decide what a writer should do given the current part count.
    pub fn evaluate(&self, parts: usize) -> Pressure {
        let c = &self.config;
        if parts < c.soft_limit {
            return Pressure::None;
        }
        if parts >= c.hard_limit {
            return Pressure::Stall(c.max_delay.min(c.stall_cap));
        }
        // Linear ramp between the limits, so pressure builds smoothly rather
        // than as a cliff the workload can oscillate across.
        let span = (c.hard_limit - c.soft_limit).max(1) as f64;
        let over = (parts - c.soft_limit) as f64;
        let frac = (over / span).clamp(0.0, 1.0);
        let base = c.base_delay.as_nanos() as f64;
        let max = c.max_delay.as_nanos() as f64;
        let nanos = base + (max - base) * frac;
        Pressure::Throttle(Duration::from_nanos(nanos as u64).min(c.stall_cap))
    }

    /// Apply the delay, recording it so the stall is observable.
    ///
    /// Returns the delay actually applied.
    pub fn apply(&self, parts: usize, clock: &dyn Clock, metrics: &Metrics) -> Duration {
        let p = self.evaluate(parts);
        if !p.is_engaged() {
            return Duration::ZERO;
        }
        let d = p.delay();
        Metrics::bump(&metrics.backpressure_events);
        metrics
            .backpressure_nanos
            .fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
        let _ = clock; // virtual clocks do not sleep; real ingest does
        std::thread::sleep(d);
        d
    }

    /// Evaluate without sleeping — for simulation and tests.
    pub fn record_only(&self, parts: usize, metrics: &Metrics) -> Duration {
        let p = self.evaluate(parts);
        if !p.is_engaged() {
            return Duration::ZERO;
        }
        let d = p.delay();
        Metrics::bump(&metrics.backpressure_events);
        metrics
            .backpressure_nanos
            .fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
        d
    }
}

impl Default for Backpressure {
    fn default() -> Self {
        Self::new(BackpressureConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bp() -> Backpressure {
        Backpressure::new(BackpressureConfig {
            soft_limit: 10,
            hard_limit: 20,
            base_delay: Duration::from_micros(100),
            max_delay: Duration::from_millis(1),
            stall_cap: Duration::from_millis(50),
        })
    }

    #[test]
    fn no_pressure_below_soft_limit() {
        let b = bp();
        for parts in 0..10 {
            assert_eq!(b.evaluate(parts), Pressure::None, "at {parts} parts");
        }
    }

    #[test]
    fn throttle_begins_at_soft_limit() {
        let b = bp();
        match b.evaluate(10) {
            Pressure::Throttle(d) => assert_eq!(d, Duration::from_micros(100)),
            other => panic!("expected throttle, got {other:?}"),
        }
    }

    #[test]
    fn throttle_ramps_monotonically() {
        let b = bp();
        let mut last = Duration::ZERO;
        for parts in 10..20 {
            let d = b.evaluate(parts).delay();
            assert!(d >= last, "delay decreased at {parts} parts");
            last = d;
        }
    }

    #[test]
    fn stall_at_hard_limit() {
        let b = bp();
        assert!(matches!(b.evaluate(20), Pressure::Stall(_)));
        assert!(matches!(b.evaluate(1000), Pressure::Stall(_)));
    }

    #[test]
    fn stall_never_exceeds_cap() {
        let b = Backpressure::new(BackpressureConfig {
            soft_limit: 1,
            hard_limit: 2,
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(60),
            stall_cap: Duration::from_millis(5),
            });
        assert!(b.evaluate(100).delay() <= Duration::from_millis(5));
        assert!(b.evaluate(1).delay() <= Duration::from_millis(5));
    }

    #[test]
    fn pressure_reports_engagement() {
        assert!(!Pressure::None.is_engaged());
        assert!(Pressure::Throttle(Duration::from_millis(1)).is_engaged());
        assert!(Pressure::Stall(Duration::from_millis(1)).is_engaged());
        assert_eq!(Pressure::None.delay(), Duration::ZERO);
    }

    #[test]
    fn engagement_is_observable_in_metrics() {
        // The §5.4 requirement: never degrade silently.
        let b = bp();
        let m = Metrics::new();
        assert_eq!(b.record_only(0, &m), Duration::ZERO);
        assert_eq!(Metrics::get(&m.backpressure_events), 0);

        b.record_only(15, &m);
        assert_eq!(Metrics::get(&m.backpressure_events), 1);
        assert!(Metrics::get(&m.backpressure_nanos) > 0);

        b.record_only(50, &m);
        assert_eq!(Metrics::get(&m.backpressure_events), 2);
    }

    #[test]
    fn apply_actually_sleeps_when_engaged() {
        let b = Backpressure::new(BackpressureConfig {
            soft_limit: 1,
            hard_limit: 2,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(5),
            stall_cap: Duration::from_millis(50),
        });
        let clock = crate::clock::RealClock::new();
        let m = Metrics::new();
        let start = std::time::Instant::now();
        let applied = b.apply(5, &clock, &m);
        assert!(applied >= Duration::from_millis(5));
        assert!(start.elapsed() >= Duration::from_millis(4));
    }

    #[test]
    fn apply_is_free_when_not_engaged() {
        let b = bp();
        let clock = crate::clock::RealClock::new();
        let m = Metrics::new();
        let start = std::time::Instant::now();
        assert_eq!(b.apply(0, &clock, &m), Duration::ZERO);
        assert!(start.elapsed() < Duration::from_millis(2));
    }

    #[test]
    fn degenerate_config_does_not_divide_by_zero() {
        let b = Backpressure::new(BackpressureConfig {
            soft_limit: 5,
            hard_limit: 5,
            ..Default::default()
        });
        // soft == hard: everything at or above is a stall.
        assert!(matches!(b.evaluate(5), Pressure::Stall(_)));
        assert_eq!(b.evaluate(4), Pressure::None);
    }
}
