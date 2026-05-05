//! Map-shaped CRDTs.
//!
//! Phase 8 of `docs/full-port-plan.md`. Three flavours of CRDT map:
//!
//! * [`ORMap`] — keys can be added & removed concurrently; per-key
//!   value is itself a CRDT (`V: CrdtMerge`).
//! * [`LWWMap`] — keys map to last-write-wins-registered values; the
//!   highest timestamp per key wins.
//! * [`PNCounterMap`] — keys map to `PNCounter`s; merge is per-key
//!   PNCounter merge.

use std::collections::HashMap;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::counters::PNCounter;
use crate::sets::OrSet;
use crate::traits::CrdtMerge;

// -- ORMap ---------------------------------------------------------

/// Observed-remove map of K → V (V itself a CRDT).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ORMap<K, V>
where
    K: Eq + Hash + Clone,
    V: CrdtMerge,
{
    entries: HashMap<K, (u64, V)>, // (add-tag, value)
    tombstones: HashMap<K, u64>,
    counter: u64,
}

impl<K: Eq + Hash + Clone, V: CrdtMerge> Default for ORMap<K, V> {
    fn default() -> Self {
        Self { entries: HashMap::new(), tombstones: HashMap::new(), counter: 0 }
    }
}

impl<K: Eq + Hash + Clone, V: CrdtMerge> ORMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update an entry. Bumps the per-key add-tag so a
    /// concurrent `remove` (with an older tag) can be merged
    /// correctly.
    pub fn put(&mut self, key: K, value: V) {
        self.counter += 1;
        self.entries.insert(key, (self.counter, value));
    }

    /// Update the value for `key` in-place (CRDT merge).
    pub fn update(&mut self, key: K, value: V) {
        self.counter += 1;
        match self.entries.get_mut(&key) {
            Some((tag, existing)) => {
                existing.merge(&value);
                *tag = self.counter;
            }
            None => {
                self.entries.insert(key, (self.counter, value));
            }
        }
    }

    pub fn remove(&mut self, key: &K) {
        if let Some((tag, _)) = self.entries.get(key) {
            self.tombstones.insert(key.clone(), *tag);
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let (add_tag, v) = self.entries.get(key)?;
        match self.tombstones.get(key) {
            Some(tomb) if tomb >= add_tag => None,
            _ => Some(v),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.entries.iter().filter_map(|(k, (add, v))| match self.tombstones.get(k) {
            Some(tomb) if tomb >= add => None,
            _ => Some((k, v)),
        })
    }
}

impl<K: Eq + Hash + Clone, V: CrdtMerge> CrdtMerge for ORMap<K, V> {
    fn merge(&mut self, other: &Self) {
        for (k, (other_tag, other_v)) in &other.entries {
            match self.entries.get_mut(k) {
                Some((tag, existing)) => {
                    existing.merge(other_v);
                    *tag = (*tag).max(*other_tag);
                }
                None => {
                    self.entries.insert(k.clone(), (*other_tag, other_v.clone()));
                }
            }
        }
        for (k, t) in &other.tombstones {
            let cur = self.tombstones.entry(k.clone()).or_insert(0);
            *cur = (*cur).max(*t);
        }
        self.counter = self.counter.max(other.counter);
    }
}

// -- LWWMap --------------------------------------------------------

/// Last-write-wins map of K → V.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    entries: HashMap<K, (u128, V)>, // (timestamp, value)
}

impl<K: Eq + Hash + Clone, V: Clone> Default for LWWMap<K, V> {
    fn default() -> Self {
        Self { entries: HashMap::new() }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> LWWMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: K, value: V, timestamp: u128) {
        match self.entries.get(&key) {
            Some((ts, _)) if *ts >= timestamp => {} // older write — drop
            _ => {
                self.entries.insert(key, (timestamp, value));
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|(_, v)| v)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.entries.iter().map(|(k, (_, v))| (k, v))
    }
}

impl<K: Eq + Hash + Clone, V: Clone> CrdtMerge for LWWMap<K, V> {
    fn merge(&mut self, other: &Self) {
        for (k, (ts, v)) in &other.entries {
            match self.entries.get(k) {
                Some((my_ts, _)) if my_ts >= ts => {}
                _ => {
                    self.entries.insert(k.clone(), (*ts, v.clone()));
                }
            }
        }
    }
}

// -- PNCounterMap --------------------------------------------------

/// Map of K → `PNCounter`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PNCounterMap<K>
where
    K: Eq + Hash + Clone,
{
    entries: HashMap<K, PNCounter>,
}

impl<K: Eq + Hash + Clone> Default for PNCounterMap<K> {
    fn default() -> Self {
        Self { entries: HashMap::new() }
    }
}

impl<K: Eq + Hash + Clone> PNCounterMap<K> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, key: K, node: &str, delta: u64) {
        self.entries.entry(key).or_default().increment(node, delta);
    }

    pub fn decrement(&mut self, key: K, node: &str, delta: u64) {
        self.entries.entry(key).or_default().decrement(node, delta);
    }

    pub fn value(&self, key: &K) -> i64 {
        self.entries.get(key).map(|c| c.value()).unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, i64)> {
        self.entries.iter().map(|(k, c)| (k, c.value()))
    }
}

impl<K: Eq + Hash + Clone> CrdtMerge for PNCounterMap<K> {
    fn merge(&mut self, other: &Self) {
        for (k, v) in &other.entries {
            self.entries.entry(k.clone()).or_default().merge(v);
        }
    }
}

// -- ORMultiMap --------------------------------------------------

/// Map of K → set-of-V, where the set is itself an `OrSet<V>`. Phase 8.B.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ORMultiMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Eq + Hash + Clone,
{
    entries: HashMap<K, OrSet<V>>,
}

impl<K: Eq + Hash + Clone, V: Eq + Hash + Clone> Default for ORMultiMap<K, V> {
    fn default() -> Self {
        Self { entries: HashMap::new() }
    }
}

impl<K: Eq + Hash + Clone, V: Eq + Hash + Clone> ORMultiMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, key: K, value: V) {
        self.entries.entry(key).or_default().add(value);
    }

    pub fn remove(&mut self, key: &K, value: &V) {
        if let Some(set) = self.entries.get_mut(key) {
            set.remove(value);
        }
    }

    pub fn contains(&self, key: &K, value: &V) -> bool {
        self.entries.get(key).map(|s| s.contains(value)).unwrap_or(false)
    }

    pub fn key_count(&self) -> usize {
        self.entries.len()
    }
}

impl<K: Eq + Hash + Clone, V: Eq + Hash + Clone> CrdtMerge for ORMultiMap<K, V> {
    fn merge(&mut self, other: &Self) {
        for (k, set) in &other.entries {
            self.entries.entry(k.clone()).or_default().merge(set);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ormap_concurrent_put_and_remove_resolves_to_remove() {
        let mut a = ORMap::<&'static str, crate::counters::GCounter>::new();
        a.put("k", crate::counters::GCounter::new());
        let mut b = a.clone();
        b.remove(&"k");
        a.merge(&b);
        assert!(a.get(&"k").is_none());
    }

    #[test]
    fn ormap_concurrent_re_add_after_remove() {
        let mut a = ORMap::<&'static str, crate::counters::GCounter>::new();
        a.put("k", crate::counters::GCounter::new());
        let mut b = a.clone();
        b.remove(&"k");
        // Concurrent re-add on a wins because its tag is newer.
        a.put("k", crate::counters::GCounter::new());
        a.merge(&b);
        assert!(a.get(&"k").is_some());
    }

    #[test]
    fn lwwmap_higher_timestamp_wins() {
        let mut a = LWWMap::<&'static str, i32>::new();
        let mut b = LWWMap::<&'static str, i32>::new();
        a.put("k", 1, 100);
        b.put("k", 2, 200);
        a.merge(&b);
        assert_eq!(a.get(&"k"), Some(&2));
        // Reverse direction: older write must not displace.
        let mut a = LWWMap::<&'static str, i32>::new();
        let mut b = LWWMap::<&'static str, i32>::new();
        a.put("k", 1, 200);
        b.put("k", 2, 100);
        a.merge(&b);
        assert_eq!(a.get(&"k"), Some(&1));
    }

    #[test]
    fn pncounter_map_per_key_counts() {
        let mut m: PNCounterMap<&'static str> = PNCounterMap::new();
        m.increment("alice", "n1", 5);
        m.increment("bob", "n1", 3);
        m.decrement("alice", "n1", 2);
        assert_eq!(m.value(&"alice"), 3);
        assert_eq!(m.value(&"bob"), 3);

        let mut m2: PNCounterMap<&'static str> = PNCounterMap::new();
        m2.increment("alice", "n2", 7);
        m.merge(&m2);
        assert_eq!(m.value(&"alice"), 10);
        assert_eq!(m.value(&"bob"), 3);
    }
}
