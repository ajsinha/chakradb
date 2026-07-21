//! A small blocked Bloom filter for primary-key lookups.
//!
//! Stage 2 of the four-stage lookup funnel (`requirements.md` §5.2):
//! bounds check → **bloom** → ordered seek → deletion-vector recheck.
//!
//! Doris uses fpp = 0.01 for exactly this purpose, and we match it. The filter
//! is the *only* per-row index overhead ChakraDB pays: because parts are
//! PK-sorted, the sorted key column doubles as the index (ordinal == row
//! offset), so there is no separate key→location map. M0-2 measures this.

/// Bits per key for a target false-positive rate of ~1%.
/// m/n = -log2(p) / ln2 ≈ 9.585 for p = 0.01.
const BITS_PER_KEY: usize = 10;
/// Optimal k = (m/n) * ln2 ≈ 6.93 → 7.
const NUM_HASHES: u32 = 7;

/// Immutable Bloom filter over `i64` keys.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Build a filter sized for `expected_keys`.
    pub fn with_capacity(expected_keys: usize) -> Self {
        // Always keep at least one word so the modulo is well-defined.
        let num_bits = (expected_keys * BITS_PER_KEY).max(64);
        let words = num_bits.div_ceil(64);
        BloomFilter {
            bits: vec![0u64; words],
            num_bits: words * 64,
            num_hashes: NUM_HASHES,
        }
    }

    /// Build directly from a key set.
    pub fn build(keys: &[i64]) -> Self {
        let mut f = Self::with_capacity(keys.len());
        for &k in keys {
            f.insert(k);
        }
        f
    }

    /// Build from any-type keys. Each [`Value`] is reduced to a 64-bit seed and
    /// then hashed by the same machinery as integer keys, so an integer key
    /// column behaves bit-for-bit as before. A key column has one type, so a
    /// value and its probe always reduce the same way — no false negatives.
    pub fn build_values(keys: &[crate::value::Value]) -> Self {
        let mut f = Self::with_capacity(keys.len());
        for k in keys {
            f.insert(value_seed(k));
        }
        f
    }

    /// Insert an any-type key.
    pub fn insert_value(&mut self, key: &crate::value::Value) {
        self.insert(value_seed(key));
    }

    /// Probe an any-type key. `false` means definitely absent.
    #[inline]
    pub fn maybe_contains_value(&self, key: &crate::value::Value) -> bool {
        self.maybe_contains(value_seed(key))
    }

    pub fn insert(&mut self, key: i64) {
        let (h1, h2) = Self::hashes(key);
        for i in 0..self.num_hashes {
            let bit = self.bit_index(h1, h2, i);
            self.bits[bit / 64] |= 1u64 << (bit % 64);
        }
    }

    /// `false` means definitely absent. `true` means possibly present.
    #[inline]
    pub fn maybe_contains(&self, key: i64) -> bool {
        let (h1, h2) = Self::hashes(key);
        for i in 0..self.num_hashes {
            let bit = self.bit_index(h1, h2, i);
            if self.bits[bit / 64] & (1u64 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    #[inline]
    fn bit_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        // Kirsch-Mitzenmacher: g_i(x) = h1 + i*h2 gives k hashes from two.
        let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (combined % self.num_bits as u64) as usize
    }

    /// Two independent 64-bit hashes via SplitMix64 finalisation.
    #[inline]
    fn hashes(key: i64) -> (u64, u64) {
        let mut z = (key as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        let h1 = z ^ (z >> 31);

        let mut w = h1.wrapping_add(0x9E37_79B9_7F4A_7C15);
        w = (w ^ (w >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        w = (w ^ (w >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        // h2 must be odd so that i*h2 cycles through the space.
        let h2 = (w ^ (w >> 31)) | 1;
        (h1, h2)
    }

    /// Resident bytes — the number M0-2 reports as index overhead.
    pub fn memory_bytes(&self) -> usize {
        self.bits.capacity() * 8
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Fraction of bits set. A saturated filter (→1.0) has degraded to
    /// "always maybe", which is a signal the part is oversized.
    pub fn fill_ratio(&self) -> f64 {
        let set: u32 = self.bits.iter().map(|w| w.count_ones()).sum();
        set as f64 / self.num_bits as f64
    }
}

/// Reduce a key value to a 64-bit seed for hashing. `Int` maps to itself so the
/// integer path is unchanged; other types get a deterministic reduction.
fn value_seed(v: &crate::value::Value) -> i64 {
    use crate::value::Value;
    match v {
        Value::Int(i) => *i,
        Value::Bool(b) => *b as i64,
        Value::Float(f) => f.to_bits() as i64,
        Value::Text(s) => {
            // FNV-1a over the bytes.
            let mut h = 0xcbf2_9ce4_8422_2325u64;
            for &byte in s.as_bytes() {
                h ^= byte as u64;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            h as i64
        }
        Value::Null => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn value_keys_have_no_false_negatives() {
        let keys: Vec<Value> = (0..2000)
            .map(|i| Value::Text(format!("user-{i}")))
            .collect();
        let f = BloomFilter::build_values(&keys);
        for k in &keys {
            assert!(f.maybe_contains_value(k), "false negative on {k:?}");
        }
    }

    #[test]
    fn int_value_path_matches_i64_path() {
        let a = BloomFilter::build(&[1, 2, 3]);
        let b = BloomFilter::build_values(&[Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert_eq!(a.bits, b.bits, "Int keys must hash identically to i64 keys");
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let f = BloomFilter::build(&[]);
        // A few probes should all miss on an all-zero filter.
        for k in 0..100i64 {
            assert!(!f.maybe_contains(k));
        }
    }

    #[test]
    fn no_false_negatives_dense_range() {
        let keys: Vec<i64> = (0..10_000).collect();
        let f = BloomFilter::build(&keys);
        for &k in &keys {
            assert!(f.maybe_contains(k), "false negative on {k}");
        }
    }

    #[test]
    fn no_false_negatives_sparse_and_negative() {
        let keys: Vec<i64> = (0..5_000).map(|i| i * 7919 - 2_000_000).collect();
        let f = BloomFilter::build(&keys);
        for &k in &keys {
            assert!(f.maybe_contains(k), "false negative on {k}");
        }
    }

    #[test]
    fn extreme_keys_are_handled() {
        let keys = vec![i64::MIN, i64::MAX, 0, -1, 1];
        let f = BloomFilter::build(&keys);
        for &k in &keys {
            assert!(f.maybe_contains(k));
        }
    }

    #[test]
    fn false_positive_rate_is_near_one_percent() {
        let n = 20_000i64;
        let keys: Vec<i64> = (0..n).collect();
        let f = BloomFilter::build(&keys);

        let probes = 100_000i64;
        let fp = (n..n + probes).filter(|&k| f.maybe_contains(k)).count();
        let rate = fp as f64 / probes as f64;
        // Target 1%; allow generous slack for hash quality variation.
        assert!(rate < 0.03, "fpp too high: {rate}");
    }

    #[test]
    fn duplicate_inserts_are_idempotent() {
        let mut a = BloomFilter::with_capacity(100);
        let mut b = BloomFilter::with_capacity(100);
        a.insert(42);
        b.insert(42);
        b.insert(42);
        b.insert(42);
        assert_eq!(a.bits, b.bits);
    }

    #[test]
    fn capacity_scales_memory() {
        let small = BloomFilter::with_capacity(100);
        let big = BloomFilter::with_capacity(100_000);
        assert!(big.memory_bytes() > small.memory_bytes());
        // ~10 bits/key => ~1.25 bytes/key.
        assert!(big.memory_bytes() >= 100_000 * 10 / 8);
    }

    #[test]
    fn tiny_capacity_still_valid() {
        let f = BloomFilter::build(&[7]);
        assert!(f.maybe_contains(7));
        assert!(f.num_bits() >= 64);
    }

    #[test]
    fn fill_ratio_grows_with_load() {
        let empty = BloomFilter::with_capacity(1000);
        assert_eq!(empty.fill_ratio(), 0.0);
        let loaded = BloomFilter::build(&(0..1000).collect::<Vec<_>>());
        let r = loaded.fill_ratio();
        assert!(r > 0.0 && r < 1.0, "fill ratio {r} implausible");
    }

    #[test]
    fn memory_per_key_is_about_ten_bits() {
        let n = 100_000;
        let f = BloomFilter::with_capacity(n);
        let bits_per_key = f.num_bits() as f64 / n as f64;
        assert!(
            (bits_per_key - BITS_PER_KEY as f64).abs() < 1.0,
            "got {bits_per_key} bits/key"
        );
    }

    #[test]
    fn hashes_are_stable() {
        // Determinism: the filter must not depend on ambient state.
        let a = BloomFilter::build(&[1, 2, 3]);
        let b = BloomFilter::build(&[1, 2, 3]);
        assert_eq!(a.bits, b.bits);
    }
}
