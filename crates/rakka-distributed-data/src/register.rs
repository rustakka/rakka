//! Last-writer-wins register. akka.net: `LWWRegister`.

use serde::{Deserialize, Serialize};

use crate::traits::CrdtMerge;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LwwRegister<T: Clone> {
    value: T,
    timestamp: u64,
    node: String,
}

impl<T: Clone> LwwRegister<T> {
    pub fn new(node: impl Into<String>, value: T, timestamp: u64) -> Self {
        Self { value, timestamp, node: node.into() }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn set(&mut self, value: T, timestamp: u64, node: impl Into<String>) {
        if timestamp > self.timestamp {
            self.value = value;
            self.timestamp = timestamp;
            self.node = node.into();
        }
    }
}

impl<T: Clone> CrdtMerge for LwwRegister<T> {
    fn merge(&mut self, other: &Self) {
        if other.timestamp > self.timestamp
            || (other.timestamp == self.timestamp && other.node > self.node)
        {
            *self = other.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn later_wins() {
        let mut a = LwwRegister::new("n1", "alpha", 1);
        let b = LwwRegister::new("n2", "beta", 2);
        a.merge(&b);
        assert_eq!(*a.value(), "beta");
    }
}
