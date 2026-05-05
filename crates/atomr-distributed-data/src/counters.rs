//! Grow-only counter and positive/negative counter.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::traits::{CrdtMerge, DeltaCrdt};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GCounter {
    state: HashMap<String, u64>,
    /// Accumulated since the last `take_delta`. Skipped on
    /// serialization so peers never see another node's pending
    /// delta — they receive deltas through the explicit
    /// `Replicator::propagate_delta` path.
    #[serde(skip)]
    pending_delta: HashMap<String, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, node: &str, delta: u64) {
        let key = node.to_string();
        *self.state.entry(key.clone()).or_default() += delta;
        *self.pending_delta.entry(key).or_default() += delta;
    }

    pub fn value(&self) -> u64 {
        self.state.values().copied().sum()
    }
}

impl CrdtMerge for GCounter {
    fn merge(&mut self, other: &Self) {
        for (k, v) in &other.state {
            let slot = self.state.entry(k.clone()).or_default();
            *slot = (*slot).max(*v);
        }
    }
}

impl DeltaCrdt for GCounter {
    /// Delta is just the per-node increments accumulated since the
    /// last take. Merging adds to the recipient's per-node count.
    type Delta = HashMap<String, u64>;

    fn take_delta(&mut self) -> Option<Self::Delta> {
        if self.pending_delta.is_empty() {
            return None;
        }
        Some(std::mem::take(&mut self.pending_delta))
    }

    fn merge_delta(&mut self, delta: &Self::Delta) {
        for (k, v) in delta {
            let slot = self.state.entry(k.clone()).or_default();
            *slot += *v;
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PNCounter {
    inc: GCounter,
    dec: GCounter,
}

impl PNCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, node: &str, delta: u64) {
        self.inc.increment(node, delta);
    }

    pub fn decrement(&mut self, node: &str, delta: u64) {
        self.dec.increment(node, delta);
    }

    pub fn value(&self) -> i64 {
        self.inc.value() as i64 - self.dec.value() as i64
    }
}

impl CrdtMerge for PNCounter {
    fn merge(&mut self, other: &Self) {
        self.inc.merge(&other.inc);
        self.dec.merge(&other.dec);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gcounter_merges_take_max_per_node() {
        let mut a = GCounter::new();
        let mut b = GCounter::new();
        a.increment("n1", 5);
        b.increment("n1", 3);
        b.increment("n2", 7);
        a.merge(&b);
        assert_eq!(a.value(), 5 + 7);
    }

    #[test]
    fn pncounter_supports_positive_negative() {
        let mut c = PNCounter::new();
        c.increment("n1", 10);
        c.decrement("n1", 3);
        assert_eq!(c.value(), 7);
    }

    #[test]
    fn delta_take_and_clear() {
        let mut c = GCounter::new();
        c.increment("a", 3);
        c.increment("b", 2);
        let delta = c.take_delta().expect("non-empty");
        assert_eq!(delta.get("a"), Some(&3));
        assert_eq!(delta.get("b"), Some(&2));
        // Cleared on take.
        assert!(c.take_delta().is_none());
    }

    #[test]
    fn delta_merge_adds_to_remote() {
        let mut local = GCounter::new();
        local.increment("a", 5);
        let _ = local.take_delta();

        let mut remote = GCounter::new();
        remote.increment("a", 1); // remote saw 1 from "a"
        let _ = remote.take_delta();

        // Local emits an additional +3 delta; remote applies it.
        local.increment("a", 3);
        let delta = local.take_delta().unwrap();
        remote.merge_delta(&delta);
        assert_eq!(remote.value(), 1 + 3);
    }
}
