//! rakka-cluster-metrics. akka.net: `Akka.Cluster.Metrics`.
//!
//! Phase 10 of `docs/full-port-plan.md`. Three layers:
//!
//! * [`ClusterMetrics`] — the per-node snapshot store (unchanged
//!   from prior version).
//! * [`MetricsProbe`] — pluggable trait that produces a
//!   [`NodeMetrics`] sample per call. The default implementation
//!   ([`StaticProbe`]) is for tests; production callers ship a probe
//!   that reads `/proc/loadavg` or calls `sysinfo` themselves
//!   (kept dep-free here so the metrics crate stays slim).
//! * [`AdaptiveLoadBalancer`] — picks a node weighted by inverse
//!   CPU load. Used by `RemoteRouterConfig` once the metrics gossip
//!   wiring lands (Phase 10.B).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeMetrics {
    pub address: String,
    pub timestamp: u64,
    pub cpu_load: f64,
    pub memory_used: u64,
    pub memory_max: u64,
}

impl NodeMetrics {
    /// Used memory as a fraction of max [0.0, 1.0]. Returns 0.0 if
    /// `memory_max` is zero.
    pub fn memory_usage(&self) -> f64 {
        if self.memory_max == 0 {
            0.0
        } else {
            self.memory_used as f64 / self.memory_max as f64
        }
    }
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

    pub fn node_count(&self) -> usize {
        self.entries.read().len()
    }
}

// -- Probe -----------------------------------------------------------

/// Sample local CPU/memory stats. Implementors decide how — `sysinfo`,
/// `/proc/loadavg`, or a hand-rolled JNI-style call. Deliberately
/// dep-free here.
pub trait MetricsProbe: Send + Sync + 'static {
    fn sample(&self, address: &str, timestamp: u64) -> NodeMetrics;
}

/// Static probe — useful for tests and as a baseline when no real
/// probe is wired. Returns the supplied values.
pub struct StaticProbe {
    pub cpu_load: f64,
    pub memory_used: u64,
    pub memory_max: u64,
}

impl MetricsProbe for StaticProbe {
    fn sample(&self, address: &str, timestamp: u64) -> NodeMetrics {
        NodeMetrics {
            address: address.into(),
            timestamp,
            cpu_load: self.cpu_load,
            memory_used: self.memory_used,
            memory_max: self.memory_max,
        }
    }
}

// -- Adaptive routing ------------------------------------------------

/// Router that picks the node with the lowest `cpu_load` from a
/// [`ClusterMetrics`] snapshot. Falls back to deterministic-by-address
/// order when there are no metrics.
pub struct AdaptiveLoadBalancer {
    metrics: Arc<ClusterMetrics>,
}

impl AdaptiveLoadBalancer {
    pub fn new(metrics: Arc<ClusterMetrics>) -> Self {
        Self { metrics }
    }

    /// Pick a candidate from `candidates` weighted by inverse load.
    /// Ties broken by address.
    pub fn pick<'a>(&self, candidates: &'a [&'a str]) -> Option<&'a str> {
        if candidates.is_empty() {
            return None;
        }
        let snapshot = self.metrics.snapshot();
        let lookup: HashMap<&str, &NodeMetrics> = snapshot
            .iter()
            .map(|m| (m.address.as_str(), m))
            .collect();
        let mut sorted: Vec<&&str> = candidates.iter().collect();
        sorted.sort_by(|a, b| {
            let load_a = lookup.get(*a).map(|m| m.cpu_load).unwrap_or(f64::INFINITY);
            let load_b = lookup.get(*b).map(|m| m.cpu_load).unwrap_or(f64::INFINITY);
            load_a
                .partial_cmp(&load_b)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.cmp(b))
        });
        sorted.first().copied().copied().map(|s| s)
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

    #[test]
    fn memory_usage_ratio() {
        let m = NodeMetrics {
            address: "a".into(),
            timestamp: 0,
            cpu_load: 0.0,
            memory_used: 250,
            memory_max: 1000,
        };
        assert_eq!(m.memory_usage(), 0.25);
    }

    #[test]
    fn memory_usage_handles_zero_max() {
        let m = NodeMetrics {
            address: "a".into(),
            timestamp: 0,
            cpu_load: 0.0,
            memory_used: 0,
            memory_max: 0,
        };
        assert_eq!(m.memory_usage(), 0.0);
    }

    #[test]
    fn static_probe_returns_configured_values() {
        let probe = StaticProbe {
            cpu_load: 0.7,
            memory_used: 5,
            memory_max: 10,
        };
        let m = probe.sample("nodeA", 42);
        assert_eq!(m.address, "nodeA");
        assert_eq!(m.timestamp, 42);
        assert_eq!(m.cpu_load, 0.7);
        assert_eq!(m.memory_used, 5);
    }

    #[test]
    fn adaptive_picks_lowest_load() {
        let m = Arc::new(ClusterMetrics::new());
        m.publish(NodeMetrics {
            address: "a".into(), timestamp: 0, cpu_load: 0.9,
            memory_used: 0, memory_max: 1,
        });
        m.publish(NodeMetrics {
            address: "b".into(), timestamp: 0, cpu_load: 0.1,
            memory_used: 0, memory_max: 1,
        });
        m.publish(NodeMetrics {
            address: "c".into(), timestamp: 0, cpu_load: 0.5,
            memory_used: 0, memory_max: 1,
        });
        let lb = AdaptiveLoadBalancer::new(m);
        assert_eq!(lb.pick(&["a", "b", "c"]), Some("b"));
    }

    #[test]
    fn adaptive_falls_back_to_address_order_when_no_metrics() {
        let m = Arc::new(ClusterMetrics::new());
        let lb = AdaptiveLoadBalancer::new(m);
        assert_eq!(lb.pick(&["c", "a", "b"]), Some("a"));
    }

    #[test]
    fn adaptive_returns_none_for_empty_candidates() {
        let m = Arc::new(ClusterMetrics::new());
        let lb = AdaptiveLoadBalancer::new(m);
        assert_eq!(lb.pick(&[]), None);
    }
}
