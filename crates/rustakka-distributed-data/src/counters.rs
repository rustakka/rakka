//! Grow-only counter and positive/negative counter. akka.net: `GCounter`, `PNCounter`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::traits::CrdtMerge;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GCounter {
    state: HashMap<String, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, node: &str, delta: u64) {
        *self.state.entry(node.to_string()).or_default() += delta;
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
}
