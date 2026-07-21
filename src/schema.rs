//! The M0 schema — deliberately hardcoded.
//!
//! M0 answers questions about index memory and scan-under-write behaviour, not
//! about type systems. A fixed four-column shape keeps the prototype honest and
//! small: `(pk i64, a i64, b f64, c string)`.
//!
//! Note the deviation from `requirements.md` §5.1, which specifies Arrow for
//! sealed parts. M0 uses a plain struct-of-vectors columnar layout instead.
//! The rationale is recorded in `docs/m0-findings.md`: Arrow buys us nothing
//! for M0's measurements and costs build time, and the sealed-part layout is
//! not what M0 is testing. M2 introduces Arrow at the DataFusion boundary,
//! where it actually earns its place.

/// A single logical row.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub pk: i64,
    pub a: i64,
    pub b: f64,
    pub c: String,
}

impl Row {
    pub fn new(pk: i64, a: i64, b: f64, c: impl Into<String>) -> Self {
        Row {
            pk,
            a,
            b,
            c: c.into(),
        }
    }

    /// Heap bytes owned by this row beyond its own size.
    pub fn heap_bytes(&self) -> usize {
        self.c.capacity()
    }
}

/// A columnar batch — the unit handed to a scan consumer.
///
/// Column vectors are parallel: index `i` across all four is one row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Batch {
    pub pk: Vec<i64>,
    pub a: Vec<i64>,
    pub b: Vec<f64>,
    pub c: Vec<String>,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Batch {
            pk: Vec::with_capacity(n),
            a: Vec::with_capacity(n),
            b: Vec::with_capacity(n),
            c: Vec::with_capacity(n),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pk.is_empty()
    }

    pub fn push(&mut self, row: &Row) {
        self.pk.push(row.pk);
        self.a.push(row.a);
        self.b.push(row.b);
        self.c.push(row.c.clone());
    }

    pub fn push_owned(&mut self, row: Row) {
        self.pk.push(row.pk);
        self.a.push(row.a);
        self.b.push(row.b);
        self.c.push(row.c);
    }

    /// Materialise row `i`. Panics if out of bounds.
    pub fn row(&self, i: usize) -> Row {
        Row {
            pk: self.pk[i],
            a: self.a[i],
            b: self.b[i],
            c: self.c[i].clone(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        (0..self.len()).map(move |i| self.row(i))
    }

    /// Append every row of `other`.
    pub fn extend(&mut self, other: &Batch) {
        self.pk.extend_from_slice(&other.pk);
        self.a.extend_from_slice(&other.a);
        self.b.extend_from_slice(&other.b);
        self.c.extend(other.c.iter().cloned());
    }

    /// Append `other`, cloning only the columns `mask` marks as needed.
    ///
    /// Unneeded columns are filled with cheap placeholders (0 / 0.0 / empty
    /// String — none of which allocate), preserving the all-columns-same-length
    /// invariant while never touching a heap the query will not read. A
    /// `SUM(a) WHERE a > 500` scan clones one column instead of four; a
    /// `COUNT(*)` clones none. This is the single biggest interpreter win —
    /// see `sql/exec.rs`.
    pub fn extend_masked(&mut self, other: &Batch, mask: [bool; 4]) {
        let n = other.len();
        if mask[0] {
            self.pk.extend_from_slice(&other.pk);
        } else {
            self.pk.resize(self.pk.len() + n, 0);
        }
        if mask[1] {
            self.a.extend_from_slice(&other.a);
        } else {
            self.a.resize(self.a.len() + n, 0);
        }
        if mask[2] {
            self.b.extend_from_slice(&other.b);
        } else {
            self.b.resize(self.b.len() + n, 0.0);
        }
        if mask[3] {
            self.c.extend(other.c.iter().cloned());
        } else {
            self.c.resize(self.c.len() + n, String::new());
        }
    }

    /// Approximate resident bytes, including string heap.
    pub fn memory_bytes(&self) -> usize {
        let fixed = self.pk.capacity() * 8 + self.a.capacity() * 8 + self.b.capacity() * 8;
        let strings: usize = self.c.iter().map(|s| s.capacity()).sum();
        let string_headers = self.c.capacity() * std::mem::size_of::<String>();
        fixed + strings + string_headers
    }

    /// True if `pk` is non-decreasing across the batch.
    pub fn is_sorted_by_pk(&self) -> bool {
        self.pk.windows(2).all(|w| w[0] <= w[1])
    }

    /// Invariant check: all columns the same length.
    pub fn is_well_formed(&self) -> bool {
        let n = self.pk.len();
        self.a.len() == n && self.b.len() == n && self.c.len() == n
    }
}

impl FromIterator<Row> for Batch {
    fn from_iter<T: IntoIterator<Item = Row>>(iter: T) -> Self {
        let mut b = Batch::new();
        for r in iter {
            b.push_owned(r);
        }
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(pk: i64) -> Row {
        Row::new(pk, pk * 2, pk as f64 / 2.0, format!("v{pk}"))
    }

    #[test]
    fn row_constructs_and_compares() {
        let a = Row::new(1, 2, 3.0, "x");
        let b = Row::new(1, 2, 3.0, "x".to_string());
        assert_eq!(a, b);
    }

    #[test]
    fn empty_batch_is_empty() {
        let b = Batch::new();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        assert!(b.is_well_formed());
    }

    #[test]
    fn push_and_read_back() {
        let mut b = Batch::new();
        b.push(&r(1));
        b.push(&r(2));
        assert_eq!(b.len(), 2);
        assert_eq!(b.row(0), r(1));
        assert_eq!(b.row(1), r(2));
        assert!(b.is_well_formed());
    }

    #[test]
    fn push_owned_avoids_clone() {
        let mut b = Batch::new();
        b.push_owned(r(7));
        assert_eq!(b.row(0), r(7));
    }

    #[test]
    fn iter_yields_all_rows_in_order() {
        let b: Batch = (0..5).map(r).collect();
        let got: Vec<i64> = b.iter().map(|row| row.pk).collect();
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn extend_concatenates() {
        let mut a: Batch = (0..3).map(r).collect();
        let b: Batch = (3..6).map(r).collect();
        a.extend(&b);
        assert_eq!(a.len(), 6);
        assert_eq!(a.pk, vec![0, 1, 2, 3, 4, 5]);
        assert!(a.is_well_formed());
    }

    #[test]
    fn extend_with_empty_is_noop() {
        let mut a: Batch = (0..3).map(r).collect();
        let before = a.clone();
        a.extend(&Batch::new());
        assert_eq!(a, before);
    }

    #[test]
    fn from_iter_builds_batch() {
        let b: Batch = vec![r(1), r(2), r(3)].into_iter().collect();
        assert_eq!(b.len(), 3);
        assert!(b.is_well_formed());
    }

    #[test]
    fn with_capacity_reserves_without_length() {
        let b = Batch::with_capacity(100);
        assert_eq!(b.len(), 0);
        assert!(b.pk.capacity() >= 100);
    }

    #[test]
    fn sorted_detection() {
        let sorted: Batch = vec![r(1), r(2), r(2), r(5)].into_iter().collect();
        assert!(sorted.is_sorted_by_pk());
        let unsorted: Batch = vec![r(3), r(1)].into_iter().collect();
        assert!(!unsorted.is_sorted_by_pk());
    }

    #[test]
    fn single_row_and_empty_are_sorted() {
        assert!(Batch::new().is_sorted_by_pk());
        let one: Batch = vec![r(9)].into_iter().collect();
        assert!(one.is_sorted_by_pk());
    }

    #[test]
    fn memory_bytes_grows_with_content() {
        let small: Batch = (0..10).map(r).collect();
        let big: Batch = (0..1000).map(r).collect();
        assert!(big.memory_bytes() > small.memory_bytes());
        // Fixed-width columns alone account for 24 bytes/row.
        assert!(big.memory_bytes() >= 1000 * 24);
    }

    #[test]
    fn heap_bytes_reflects_string() {
        let row = Row::new(1, 1, 1.0, "hello");
        assert!(row.heap_bytes() >= 5);
    }

    #[test]
    fn well_formed_detects_corruption() {
        let mut b: Batch = (0..3).map(r).collect();
        b.a.pop();
        assert!(!b.is_well_formed());
    }

    #[test]
    #[should_panic]
    fn row_out_of_bounds_panics() {
        let b = Batch::new();
        let _ = b.row(0);
    }
}
