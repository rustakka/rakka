//! Vector clock. akka.net: `Cluster/VectorClock.cs`.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VectorClock {
    pub versions: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum VectorRelation {
    Before,
    After,
    Same,
    Concurrent,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tick(&mut self, node: &str) {
        *self.versions.entry(node.to_string()).or_insert(0) += 1;
    }

    pub fn merge(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for (k, v) in &other.versions {
            let entry = out.versions.entry(k.clone()).or_insert(0);
            if *v > *entry {
                *entry = *v;
            }
        }
        out
    }

    pub fn compare(&self, other: &Self) -> VectorRelation {
        let keys: std::collections::BTreeSet<_> = self.versions.keys().chain(other.versions.keys()).collect();
        let mut a_le = true;
        let mut b_le = true;
        for k in keys {
            let a = self.versions.get(k.as_str()).copied().unwrap_or(0);
            let b = other.versions.get(k.as_str()).copied().unwrap_or(0);
            match a.cmp(&b) {
                Ordering::Less => {} // a still ≤
                Ordering::Greater => a_le = false,
                Ordering::Equal => {}
            }
            match a.cmp(&b) {
                Ordering::Greater => {} // b still ≤
                Ordering::Less => b_le = false,
                Ordering::Equal => {}
            }
        }
        match (a_le, b_le) {
            (true, true) => VectorRelation::Same,
            (true, false) => VectorRelation::Before,
            (false, true) => VectorRelation::After,
            (false, false) => VectorRelation::Concurrent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_and_compare() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.tick("A");
        b.tick("A");
        assert_eq!(a.compare(&b), VectorRelation::Same);
        a.tick("A");
        assert_eq!(b.compare(&a), VectorRelation::Before);
        b.tick("B");
        assert_eq!(a.compare(&b), VectorRelation::Concurrent);
    }

    #[test]
    fn merge_is_pointwise_max() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.tick("A");
        a.tick("A");
        b.tick("A");
        b.tick("B");
        let m = a.merge(&b);
        assert_eq!(m.versions["A"], 2);
        assert_eq!(m.versions["B"], 1);
    }
}
