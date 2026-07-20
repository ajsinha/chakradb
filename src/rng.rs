//! Seeded, deterministic pseudo-random number generation.
//!
//! This is one of the three "unretrofittable seams" identified in the design
//! (`requirements.md` §11.1): every source of non-determinism in the engine must
//! flow through an explicit, seeded object rather than through ambient APIs.
//!
//! We implement PCG-XSH-RR 64/32 rather than pulling in `rand` so that the
//! generated sequence is stable across dependency updates. A simulation whose
//! replay depends on a third-party crate's version is not reproducible.
//!
//! Reference: O'Neill, "PCG: A Family of Simple Fast Space-Efficient
//! Statistically Good Algorithms for Random Number Generation" (2014).

/// A small, fast, fully deterministic PRNG.
///
/// Two `Rng`s created with the same seed produce identical sequences forever.
/// This property is asserted by the test suite and is load-bearing for
/// simulation replay.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
    inc: u64,
}

const PCG_MULT: u64 = 6_364_136_223_846_793_005;

impl Rng {
    /// Create a generator from a seed. Distinct seeds give distinct streams.
    pub fn new(seed: u64) -> Self {
        let mut rng = Rng {
            state: 0,
            // Stream selector must be odd.
            inc: (seed << 1) | 1,
        };
        rng.next_u32();
        rng.state = rng.state.wrapping_add(seed);
        rng.next_u32();
        rng
    }

    /// Raw 32-bit output.
    pub fn next_u32(&mut self) -> u32 {
        let old = self.state;
        self.state = old.wrapping_mul(PCG_MULT).wrapping_add(self.inc);
        let xorshifted = (((old >> 18) ^ old) >> 27) as u32;
        let rot = (old >> 59) as u32;
        xorshifted.rotate_right(rot)
    }

    /// Raw 64-bit output, built from two 32-bit draws.
    pub fn next_u64(&mut self) -> u64 {
        ((self.next_u32() as u64) << 32) | (self.next_u32() as u64)
    }

    /// Uniform in `[0, n)`. Uses rejection sampling to avoid modulo bias.
    ///
    /// Panics if `n == 0`.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "Rng::below requires n > 0");
        if n.is_power_of_two() {
            return self.next_u64() & (n - 1);
        }
        // Reject the tail that would skew the distribution.
        let zone = u64::MAX - (u64::MAX % n) - 1;
        loop {
            let v = self.next_u64();
            if v <= zone {
                return v % n;
            }
        }
    }

    /// Uniform in `[lo, hi)`. Panics if `lo >= hi`.
    pub fn range(&mut self, lo: i64, hi: i64) -> i64 {
        assert!(lo < hi, "Rng::range requires lo < hi");
        let span = (hi - lo) as u64;
        lo + self.below(span) as i64
    }

    /// Returns `true` with probability `p` (clamped to `[0, 1]`).
    pub fn chance(&mut self, p: f64) -> bool {
        let p = p.clamp(0.0, 1.0);
        // 2^53 keeps us inside f64's exact integer range.
        const SCALE: u64 = 1 << 53;
        (self.below(SCALE) as f64) < p * (SCALE as f64)
    }

    /// Uniform float in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        const SCALE: u64 = 1 << 53;
        (self.below(SCALE) as f64) / (SCALE as f64)
    }

    /// Fisher-Yates shuffle.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        if items.len() < 2 {
            return;
        }
        for i in (1..items.len()).rev() {
            let j = self.below((i + 1) as u64) as usize;
            items.swap(i, j);
        }
    }

    /// Fork a child generator deterministically derived from this one.
    ///
    /// Used to give each simulated thread its own stream without the threads'
    /// interleaving affecting each other's sequences.
    pub fn fork(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn same_seed_gives_identical_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..10_000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        let diffs = (0..1000).filter(|_| a.next_u64() != b.next_u64()).count();
        // Overwhelmingly likely to differ on essentially every draw.
        assert!(diffs > 990, "streams too similar: {diffs}/1000 differed");
    }

    #[test]
    fn below_respects_bound() {
        let mut r = Rng::new(7);
        for n in [1u64, 2, 3, 7, 16, 1000, 1 << 20] {
            for _ in 0..1000 {
                assert!(r.below(n) < n);
            }
        }
    }

    #[test]
    fn below_one_is_always_zero() {
        let mut r = Rng::new(99);
        for _ in 0..100 {
            assert_eq!(r.below(1), 0);
        }
    }

    #[test]
    #[should_panic(expected = "requires n > 0")]
    fn below_zero_panics() {
        Rng::new(1).below(0);
    }

    #[test]
    fn range_respects_bounds() {
        let mut r = Rng::new(11);
        for _ in 0..10_000 {
            let v = r.range(-50, 50);
            assert!((-50..50).contains(&v));
        }
    }

    #[test]
    fn chance_extremes_are_absolute() {
        let mut r = Rng::new(3);
        for _ in 0..500 {
            assert!(!r.chance(0.0));
            assert!(r.chance(1.0));
        }
    }

    #[test]
    fn chance_is_roughly_calibrated() {
        let mut r = Rng::new(5);
        let n = 100_000;
        let hits = (0..n).filter(|_| r.chance(0.25)).count();
        let frac = hits as f64 / n as f64;
        assert!((frac - 0.25).abs() < 0.01, "p=0.25 gave {frac}");
    }

    #[test]
    fn next_f64_in_unit_interval() {
        let mut r = Rng::new(13);
        for _ in 0..10_000 {
            let v = r.next_f64();
            assert!((0.0..1.0).contains(&v));
        }
    }

    #[test]
    fn shuffle_is_a_permutation() {
        let mut r = Rng::new(17);
        let mut v: Vec<u32> = (0..1000).collect();
        r.shuffle(&mut v);
        let set: HashSet<u32> = v.iter().copied().collect();
        assert_eq!(set.len(), 1000);
        assert_ne!(v, (0..1000).collect::<Vec<_>>(), "shuffle was a no-op");
    }

    #[test]
    fn shuffle_handles_degenerate_lengths() {
        let mut r = Rng::new(19);
        let mut empty: Vec<u8> = vec![];
        r.shuffle(&mut empty);
        let mut single = vec![1u8];
        r.shuffle(&mut single);
        assert_eq!(single, vec![1]);
    }

    #[test]
    fn shuffle_is_deterministic_for_seed() {
        let mut a = Rng::new(23);
        let mut b = Rng::new(23);
        let mut va: Vec<u32> = (0..100).collect();
        let mut vb: Vec<u32> = (0..100).collect();
        a.shuffle(&mut va);
        b.shuffle(&mut vb);
        assert_eq!(va, vb);
    }

    #[test]
    fn fork_is_deterministic_and_independent() {
        let mut parent_a = Rng::new(31);
        let mut parent_b = Rng::new(31);
        let mut ca = parent_a.fork();
        let mut cb = parent_b.fork();
        for _ in 0..1000 {
            assert_eq!(ca.next_u64(), cb.next_u64());
        }
        // A second fork yields a different stream from the first.
        let mut c2 = parent_a.fork();
        assert_ne!(ca.next_u64(), c2.next_u64());
    }

    #[test]
    fn output_is_not_trivially_biased() {
        // Crude smoke test: bit 0 should be roughly balanced.
        let mut r = Rng::new(37);
        let n = 100_000;
        let ones = (0..n).filter(|_| r.next_u64() & 1 == 1).count();
        let frac = ones as f64 / n as f64;
        assert!((frac - 0.5).abs() < 0.01, "bit0 bias: {frac}");
    }
}
