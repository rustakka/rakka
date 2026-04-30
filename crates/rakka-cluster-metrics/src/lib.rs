//! rakka-cluster-metrics. akka.net: `Akka.Cluster.Metrics`.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub address: String,
    pub timestamp: u64,
    pub cpu_load: f64,
    pub memory_used: u64,
    pub memory_max: u64,
}

#[derive(Default)]
pub struct ClusterMetrics {
    entries: RwLock<HashMap<String, NodeMetrics>>,
}

impl ClusterMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&self, m: NodeMetrics) {
        self.entries.write().insert(m.address.clone(), m);
    }

    pub fn snapshot(&self) -> Vec<NodeMetrics> {
        self.entries.read().values().cloned().collect()
    }

    pub fn get(&self, address: &str) -> Option<NodeMetrics> {
        self.entries.read().get(address).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_and_fetch() {
        let m = ClusterMetrics::new();
        m.publish(NodeMetrics {
            address: "a".into(),
            timestamp: 1,
            cpu_load: 0.5,
            memory_used: 100,
            memory_max: 1000,
        });
        assert_eq!(m.snapshot().len(), 1);
        assert_eq!(m.get("a").unwrap().cpu_load, 0.5);
    }
}
