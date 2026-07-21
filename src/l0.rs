//! L0 — the row-major write buffer.
//!
//! Writes land here at memory speed (`requirements.md` §5.1). When L0 reaches a
//! size threshold it is *sealed*: sorted by primary key and converted into an
//! immutable columnar part.
//!
//! Entries carry the same `(created, deleted)` version stamps as sealed parts,
//! so that sealing is a pure reorganisation and never loses a version an active
//! snapshot might still need.

use crate::batch::Batch;
use crate::csn::{Csn, Snapshot, NEVER_DELETED};
use crate::schema::{Row, Schema};
use crate::value::{Key, Value};
use std::collections::BTreeMap;

/// One buffered row version.
#[derive(Debug, Clone)]
pub struct L0Entry {
    pub row: Row,
    pub created: Csn,
    pub deleted: Csn,
}

impl L0Entry {
    #[inline]
    pub fn visible_to(&self, snap: Snapshot) -> bool {
        snap.sees(self.created, self.deleted)
    }
}

/// The sealed output of an L0 buffer: a PK-sorted batch plus its version
/// stamps and pre-populated tombstones.
#[derive(Debug)]
pub struct SealedL0 {
    pub batch: Batch,
    pub created: Vec<Csn>,
    /// `(ordinal, deleted_csn)` pairs, ordinal-ascending.
    pub deletions: Vec<(u32, Csn)>,
}

/// Append-only write buffer with a point-lookup index over the newest version
/// of each key. Keyed by an arbitrary-type key column (`schema.key_index`).
#[derive(Debug)]
pub struct L0Buffer {
    schema: Schema,
    entries: Vec<L0Entry>,
    /// key → index of the newest entry for that key.
    newest: BTreeMap<Key, usize>,
    string_bytes: usize,
}

impl L0Buffer {
    pub fn new(schema: Schema) -> Self {
        L0Buffer {
            schema,
            entries: Vec::new(),
            newest: BTreeMap::new(),
            string_bytes: 0,
        }
    }

    #[inline]
    fn key_index(&self) -> usize {
        self.schema.key_index()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[L0Entry] {
        &self.entries
    }

    /// Distinct keys currently buffered.
    pub fn distinct_keys(&self) -> usize {
        self.newest.len()
    }

    /// Append a new row version. The row must already carry its key value (the
    /// table assigns a rowid before calling this for synthetic-key tables).
    pub fn insert(&mut self, row: Row, csn: Csn) {
        self.string_bytes += row.heap_bytes();
        let key = Key(row.key(self.key_index()).clone());
        self.entries.push(L0Entry {
            row,
            created: csn,
            deleted: NEVER_DELETED,
        });
        self.newest.insert(key, self.entries.len() - 1);
    }

    /// Index of the newest entry for `key` that is live at `snap`.
    pub fn lookup(&self, key: &Value, snap: Snapshot) -> Option<usize> {
        // Fast path: the newest version is usually the answer.
        if let Some(&i) = self.newest.get(&Key(key.clone())) {
            if self.entries[i].visible_to(snap) {
                return Some(i);
            }
        }
        // Older snapshot: walk backwards for the version it should see.
        let ki = self.key_index();
        self.entries
            .iter()
            .enumerate()
            .rev()
            .find(|(_, e)| e.row.key(ki).total_cmp(key).is_eq() && e.visible_to(snap))
            .map(|(i, _)| i)
    }

    /// Tombstone the entry at `index`. Returns false if already deleted.
    pub fn mark_deleted(&mut self, index: usize, csn: Csn) -> bool {
        let e = &mut self.entries[index];
        if e.deleted != NEVER_DELETED {
            return false;
        }
        e.deleted = csn;
        true
    }

    /// Rows visible to `snap`, in insertion order.
    pub fn scan(&self, snap: Snapshot) -> Batch {
        let rows: Vec<Row> = self
            .entries
            .iter()
            .filter(|e| e.visible_to(snap))
            .map(|e| e.row.clone())
            .collect();
        Batch::from_rows(&self.schema, &rows)
    }

    pub fn visible_count(&self, snap: Snapshot) -> usize {
        self.entries.iter().filter(|e| e.visible_to(snap)).count()
    }

    /// Approximate resident bytes.
    pub fn memory_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<L0Entry>()
            + self.string_bytes
            + self.newest.len() * (std::mem::size_of::<Value>() + std::mem::size_of::<usize>())
    }

    /// Sort by `(key, created)` and emit a sealed representation.
    ///
    /// Every version is preserved — sealing must not change what any snapshot
    /// can see. Duplicate keys are permitted in the output and are resolved by
    /// version stamps at lookup time.
    pub fn seal(&mut self) -> SealedL0 {
        let mut entries = std::mem::take(&mut self.entries);
        self.newest.clear();
        self.string_bytes = 0;

        let ki = self.key_index();
        entries.sort_by(|a, b| {
            a.row
                .key(ki)
                .total_cmp(b.row.key(ki))
                .then(a.created.cmp(&b.created))
        });

        let mut rows = Vec::with_capacity(entries.len());
        let mut created = Vec::with_capacity(entries.len());
        let mut deletions = Vec::new();

        for (ordinal, e) in entries.into_iter().enumerate() {
            created.push(e.created);
            if e.deleted != NEVER_DELETED {
                deletions.push((ordinal as u32, e.deleted));
            }
            rows.push(e.row);
        }

        SealedL0 {
            batch: Batch::from_rows(&self.schema, &rows),
            created,
            deletions,
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.newest.clear();
        self.string_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buf() -> L0Buffer {
        L0Buffer::new(Schema::default_schema())
    }
    fn row(pk: i64) -> Row {
        Row::new(pk, pk * 2, pk as f64, format!("v{pk}"))
    }
    fn k(pk: i64) -> Value {
        Value::Int(pk)
    }
    /// The pk column of a batch as i64s.
    fn pks(b: &Batch) -> Vec<i64> {
        (0..b.len()).map(|i| b.key(i).as_int().unwrap()).collect()
    }
    /// The text column (index 3) of a batch.
    fn texts(b: &Batch) -> Vec<String> {
        (0..b.len()).map(|i| b.value(3, i).render()).collect()
    }

    #[test]
    fn new_buffer_is_empty() {
        let b = buf();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        assert_eq!(b.distinct_keys(), 0);
    }

    #[test]
    fn insert_then_lookup() {
        let mut b = buf();
        b.insert(row(5), 10);
        let i = b.lookup(&k(5), Snapshot::at(10)).expect("should find");
        assert_eq!(b.entries()[i].row.pk(), 5);
    }

    #[test]
    fn lookup_misses_unknown_key() {
        let mut b = buf();
        b.insert(row(5), 10);
        assert!(b.lookup(&k(6), Snapshot::at(10)).is_none());
    }

    #[test]
    fn lookup_respects_creation_csn() {
        let mut b = buf();
        b.insert(row(5), 10);
        assert!(b.lookup(&k(5), Snapshot::at(9)).is_none());
        assert!(b.lookup(&k(5), Snapshot::at(10)).is_some());
    }

    #[test]
    fn tombstone_hides_row() {
        let mut b = buf();
        b.insert(row(5), 10);
        let i = b.lookup(&k(5), Snapshot::at(10)).unwrap();
        assert!(b.mark_deleted(i, 20));
        assert!(b.lookup(&k(5), Snapshot::at(20)).is_none());
        // Older snapshot still sees it.
        assert!(b.lookup(&k(5), Snapshot::at(15)).is_some());
    }

    #[test]
    fn double_tombstone_rejected() {
        let mut b = buf();
        b.insert(row(5), 10);
        assert!(b.mark_deleted(0, 20));
        assert!(!b.mark_deleted(0, 30));
    }

    #[test]
    fn multiple_versions_resolve_by_snapshot() {
        let mut b = buf();
        b.insert(Row::new(5, 1, 1.0, "first"), 10);
        b.mark_deleted(0, 20);
        b.insert(Row::new(5, 2, 2.0, "second"), 20);

        let old = b.lookup(&k(5), Snapshot::at(15)).unwrap();
        assert_eq!(b.entries()[old].row.c(), "first");

        let new = b.lookup(&k(5), Snapshot::at(20)).unwrap();
        assert_eq!(b.entries()[new].row.c(), "second");
    }

    #[test]
    fn exactly_one_version_visible_at_any_snapshot() {
        let mut b = buf();
        b.insert(Row::new(1, 0, 0.0, "a"), 5);
        b.mark_deleted(0, 10);
        b.insert(Row::new(1, 0, 0.0, "b"), 10);
        b.mark_deleted(1, 15);
        b.insert(Row::new(1, 0, 0.0, "c"), 15);

        for csn in 5..20u64 {
            let snap = Snapshot::at(csn);
            let n = b.entries().iter().filter(|e| e.visible_to(snap)).count();
            assert_eq!(n, 1, "at csn={csn} expected exactly one live version");
        }
    }

    #[test]
    fn scan_returns_visible_rows_only() {
        let mut b = buf();
        b.insert(row(1), 5);
        b.insert(row(2), 6);
        b.insert(row(3), 7);
        b.mark_deleted(1, 8);
        let got = b.scan(Snapshot::at(8));
        assert_eq!(pks(&got), vec![1, 3]);
        assert_eq!(b.visible_count(Snapshot::at(8)), 2);
    }

    #[test]
    fn scan_at_early_snapshot_sees_nothing() {
        let mut b = buf();
        b.insert(row(1), 10);
        assert!(b.scan(Snapshot::at(5)).is_empty());
    }

    #[test]
    fn distinct_keys_counts_keys_not_versions() {
        let mut b = buf();
        b.insert(row(1), 1);
        b.insert(row(1), 2);
        b.insert(row(2), 3);
        assert_eq!(b.len(), 3);
        assert_eq!(b.distinct_keys(), 2);
    }

    #[test]
    fn seal_sorts_by_key() {
        let mut b = buf();
        for pk in [5, 1, 9, 3] {
            b.insert(row(pk), pk as Csn);
        }
        let sealed = b.seal();
        assert_eq!(pks(&sealed.batch), vec![1, 3, 5, 9]);
        assert!(sealed.batch.is_sorted_by_key());
        assert!(b.is_empty(), "seal must drain the buffer");
    }

    #[test]
    fn seal_preserves_every_version() {
        let mut b = buf();
        b.insert(Row::new(1, 0, 0.0, "a"), 5);
        b.mark_deleted(0, 10);
        b.insert(Row::new(1, 0, 0.0, "b"), 10);
        let sealed = b.seal();
        assert_eq!(sealed.batch.len(), 2, "both versions must survive sealing");
        assert_eq!(sealed.created, vec![5, 10]);
        assert_eq!(sealed.deletions, vec![(0, 10)]);
    }

    #[test]
    fn seal_orders_versions_of_same_key_by_creation() {
        let mut b = buf();
        b.insert(Row::new(7, 0, 0.0, "new"), 30);
        b.insert(Row::new(7, 0, 0.0, "old"), 10);
        let sealed = b.seal();
        assert_eq!(sealed.created, vec![10, 30]);
        assert_eq!(
            texts(&sealed.batch),
            vec!["old".to_string(), "new".to_string()]
        );
    }

    #[test]
    fn seal_of_empty_buffer_is_empty() {
        let mut b = buf();
        let sealed = b.seal();
        assert!(sealed.batch.is_empty());
        assert!(sealed.created.is_empty());
        assert!(sealed.deletions.is_empty());
    }

    #[test]
    fn seal_output_length_matches_stamps() {
        let mut b = buf();
        for pk in 0..100 {
            b.insert(row(pk), pk as Csn + 1);
        }
        let sealed = b.seal();
        assert_eq!(sealed.created.len(), sealed.batch.len());
    }

    #[test]
    fn clear_empties_everything() {
        let mut b = buf();
        b.insert(row(1), 1);
        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.distinct_keys(), 0);
    }

    #[test]
    fn memory_grows_with_entries() {
        let mut b = buf();
        let empty = b.memory_bytes();
        for pk in 0..1000 {
            b.insert(row(pk), pk as Csn + 1);
        }
        assert!(b.memory_bytes() > empty);
    }
}
