//! Grow-only and observed-remove sets. akka.net: `GSet`, `ORSet`.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::traits::CrdtMerge;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GSet<T>
where
    T: Eq + Hash + Clone,
{
    items: HashSet<T>,
}

impl<T: Eq + Hash + Clone> Default for GSet<T> {
    fn default() -> Self {
        Self { items: HashSet::new() }
    }
}

impl<T: Eq + Hash + Clone> GSet<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, item: T) {
        self.items.insert(item);
    }

    pub fn contains(&self, item: &T) -> bool {
        self.items.contains(item)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<T: Eq + Hash + Clone> CrdtMerge for GSet<T> {
    fn merge(&mut self, other: &Self) {
        for item in &other.items {
            self.items.insert(item.clone());
        }
    }
}

/// Observed-remove set. Each addition gets a unique tag; a removal retains
/// all tags seen at that moment. Merge takes the union of (item, tag) pairs
/// minus tombstones.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSet<T>
where
    T: Eq + Hash + Clone,
{
    adds: HashMap<T, HashSet<u64>>,
    removes: HashMap<T, HashSet<u64>>,
    counter: u64,
}

impl<T: Eq + Hash + Clone> Default for OrSet<T> {
    fn default() -> Self {
        Self { adds: HashMap::new(), removes: HashMap::new(), counter: 0 }
    }
}

impl<T: Eq + Hash + Clone> OrSet<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, item: T) {
        self.counter += 1;
        self.adds.entry(item).or_default().insert(self.counter);
    }

    pub fn remove(&mut self, item: &T) {
        if let Some(tags) = self.adds.get(item).cloned() {
            self.removes.entry(item.clone()).or_default().extend(tags);
        }
    }

    pub fn contains(&self, item: &T) -> bool {
        match (self.adds.get(item), self.removes.get(item)) {
            (Some(a), Some(r)) => a.difference(r).next().is_some(),
            (Some(a), None) => !a.is_empty(),
            _ => false,
        }
    }
}

impl<T: Eq + Hash + Clone> CrdtMerge for OrSet<T> {
    fn merge(&mut self, other: &Self) {
        for (k, v) in &other.adds {
            self.adds.entry(k.clone()).or_default().extend(v.iter().copied());
        }
        for (k, v) in &other.removes {
            self.removes.entry(k.clone()).or_default().extend(v.iter().copied());
        }
        self.counter = self.counter.max(other.counter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gset_merges_union() {
        let mut a = GSet::<i32>::new();
        let mut b = GSet::<i32>::new();
        a.add(1);
        b.add(2);
        a.merge(&b);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn orset_add_then_remove() {
        let mut s = OrSet::<&'static str>::new();
        s.add("x");
        assert!(s.contains(&"x"));
        s.remove(&"x");
        assert!(!s.contains(&"x"));
    }

    #[test]
    fn orset_merge_preserves_re_add_after_concurrent_remove() {
        let mut a = OrSet::<&'static str>::new();
        a.add("x");

        let mut b = a.clone();
        b.remove(&"x");

        a.add("x");
        a.merge(&b);
        assert!(a.contains(&"x"));
    }
}
