//! atomr-cluster-metrics.
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
        let lookup: HashMap<&str, &NodeMetrics> = snapshot.iter().map(|m| (m.address.as_str(), m)).collect();
        let mut sorted: Vec<&&str> = candidates.iter().collect();
        sorted.sort_by(|a, b| {
            let load_a = lookup.get(*a).map(|m| m.cpu_load).unwrap_or(f64::INFINITY);
            let load_b = lookup.get(*b).map(|m| m.cpu_load).unwrap_or(f64::INFINITY);
            load_a.partial_cmp(&load_b).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.cmp(b))
        });
        sorted.first().copied().copied()
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
        let m =
            NodeMetrics { address: "a".into(), timestamp: 0, cpu_load: 0.0, memory_used: 0, memory_max: 0 };
        assert_eq!(m.memory_usage(), 0.0);
    }

    #[test]
    fn static_probe_returns_configured_values() {
        let probe = StaticProbe { cpu_load: 0.7, memory_used: 5, memory_max: 10 };
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
            address: "a".into(),
            timestamp: 0,
            cpu_load: 0.9,
            memory_used: 0,
            memory_max: 1,
        });
        m.publish(NodeMetrics {
            address: "b".into(),
            timestamp: 0,
            cpu_load: 0.1,
            memory_used: 0,
            memory_max: 1,
        });
        m.publish(NodeMetrics {
            address: "c".into(),
            timestamp: 0,
            cpu_load: 0.5,
            memory_used: 0,
            memory_max: 1,
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

// -- EWMA smoothing --------------------------------------------------

/// Exponentially-weighted moving average.
///
/// `alpha` is the smoothing factor in `(0.0, 1.0]`; larger `alpha`
/// follows the new sample more aggressively.
#[derive(Debug, Clone, Copy)]
pub struct Ewma {
    pub alpha: f64,
    pub value: f64,
}

impl Ewma {
    /// Construct with an initial value and smoothing factor.
    /// Panics if `alpha` is outside `(0.0, 1.0]`.
    pub fn new(initial: f64, alpha: f64) -> Self {
        assert!(alpha > 0.0 && alpha <= 1.0, "alpha must be in (0.0, 1.0]");
        Self { alpha, value: initial }
    }

    /// Pick `alpha` from a half-life. After `half_life` samples the
    /// previous value contributes 50% of the EWMA. Useful when the
    /// sample interval is fixed.
    pub fn from_half_life(initial: f64, half_life_samples: f64) -> Self {
        assert!(half_life_samples > 0.0);
        // alpha = 1 - 2^(-1/half_life)
        let alpha = 1.0 - (2.0_f64).powf(-1.0 / half_life_samples);
        Self::new(initial, alpha)
    }

    /// Fold a new sample into the EWMA and return the new smoothed value.
    pub fn update(&mut self, sample: f64) -> f64 {
        self.value = self.alpha * sample + (1.0 - self.alpha) * self.value;
        self.value
    }
}

// -- Metrics selectors ------------------------------------------------

/// What dimension drives `WeightedRoutees`.
/// `MetricsSelector` / `CpuMetricsSelector` / `HeapMetricsSelector` /
/// `MixMetricsSelector`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MetricsSelector {
    /// Higher weight for lower CPU load. Returns `1 - cpu_load`.
    Cpu,
    /// Higher weight for lower memory usage. Returns `1 - memory_usage`.
    Heap,
    /// Average of CPU and Heap selectors.
    Mix,
}

impl MetricsSelector {
    /// Compute a weight in `[0.0, 1.0]` for `m`. Larger == more
    /// preferable as a routing target.
    pub fn weight(&self, m: &NodeMetrics) -> f64 {
        let cpu = (1.0 - m.cpu_load).clamp(0.0, 1.0);
        let heap = (1.0 - m.memory_usage()).clamp(0.0, 1.0);
        match self {
            Self::Cpu => cpu,
            Self::Heap => heap,
            Self::Mix => 0.5 * (cpu + heap),
        }
    }
}

// -- Weighted routees -------------------------------------------------

/// Pick a routee with probability proportional to its
/// [`MetricsSelector::weight`].
pub struct WeightedRoutees {
    metrics: Arc<ClusterMetrics>,
    selector: MetricsSelector,
}

impl WeightedRoutees {
    pub fn new(metrics: Arc<ClusterMetrics>, selector: MetricsSelector) -> Self {
        Self { metrics, selector }
    }

    /// Pick a routee using `seed` as the random draw in `[0.0, 1.0)`.
    /// Splitting the RNG out of the call lets tests be deterministic.
    /// The returned slice index corresponds to `candidates`.
    pub fn pick<'a>(&self, candidates: &'a [&'a str], seed: f64) -> Option<&'a str> {
        if candidates.is_empty() {
            return None;
        }
        let snap = self.metrics.snapshot();
        let by_addr: HashMap<&str, &NodeMetrics> = snap.iter().map(|m| (m.address.as_str(), m)).collect();
        let weights: Vec<f64> = candidates
            .iter()
            .map(|c| by_addr.get(c).map(|m| self.selector.weight(m)).unwrap_or(0.5))
            .collect();
        let total: f64 = weights.iter().sum();
        if total <= 0.0 {
            return Some(candidates[0]);
        }
        let target = (seed.clamp(0.0, 1.0) * total).min(total);
        let mut acc = 0.0;
        for (i, w) in weights.iter().enumerate() {
            acc += *w;
            if target <= acc {
                return Some(candidates[i]);
            }
        }
        candidates.last().copied()
    }
}

#[cfg(test)]
mod ewma_tests {
    use super::*;

    #[test]
    fn ewma_initial_value_unchanged_until_update() {
        let e = Ewma::new(0.5, 0.3);
        assert_eq!(e.value, 0.5);
    }

    #[test]
    fn ewma_converges_to_steady_signal() {
        let mut e = Ewma::new(0.0, 0.5);
        for _ in 0..30 {
            e.update(1.0);
        }
        assert!(e.value > 0.99, "expected ≈1.0, got {}", e.value);
    }

    #[test]
    fn ewma_rejects_invalid_alpha() {
        let r = std::panic::catch_unwind(|| Ewma::new(0.0, 0.0));
        assert!(r.is_err());
    }

    #[test]
    fn ewma_from_half_life_yields_50pct_weight_after_half_life() {
        let mut e = Ewma::from_half_life(0.0, 4.0);
        // after 4 samples of `1.0`, value ≥ 0.5
        for _ in 0..4 {
            e.update(1.0);
        }
        assert!(e.value >= 0.5);
    }

    #[test]
    fn cpu_selector_prefers_lower_load() {
        let m =
            NodeMetrics { address: "a".into(), timestamp: 0, cpu_load: 0.2, memory_used: 0, memory_max: 1 };
        let n =
            NodeMetrics { address: "b".into(), timestamp: 0, cpu_load: 0.9, memory_used: 0, memory_max: 1 };
        assert!(MetricsSelector::Cpu.weight(&m) > MetricsSelector::Cpu.weight(&n));
    }

    #[test]
    fn mix_selector_averages_cpu_and_heap() {
        let m = NodeMetrics {
            address: "a".into(),
            timestamp: 0,
            cpu_load: 0.0,
            memory_used: 100,
            memory_max: 200,
        };
        let w = MetricsSelector::Mix.weight(&m);
        // cpu weight 1.0, heap weight 0.5 -> mix 0.75
        assert!((w - 0.75).abs() < 1e-6, "mix weight {w}");
    }

    #[test]
    fn weighted_routees_picks_higher_weight_node_more_often() {
        let m = Arc::new(ClusterMetrics::new());
        m.publish(NodeMetrics {
            address: "fast".into(),
            timestamp: 0,
            cpu_load: 0.1,
            memory_used: 0,
            memory_max: 1,
        });
        m.publish(NodeMetrics {
            address: "slow".into(),
            timestamp: 0,
            cpu_load: 0.9,
            memory_used: 0,
            memory_max: 1,
        });
        let r = WeightedRoutees::new(m, MetricsSelector::Cpu);
        let cands = ["fast", "slow"];
        let mut fast = 0;
        // 100 deterministic seeds across [0.0, 1.0)
        for i in 0..100 {
            let seed = i as f64 / 100.0;
            if r.pick(&cands, seed) == Some("fast") {
                fast += 1;
            }
        }
        assert!(fast > 60, "expected >60 fast picks, got {fast}");
    }

    #[test]
    fn weighted_routees_returns_first_when_all_zero() {
        let m = Arc::new(ClusterMetrics::new());
        m.publish(NodeMetrics {
            address: "a".into(),
            timestamp: 0,
            cpu_load: 1.0,
            memory_used: 1,
            memory_max: 1,
        });
        m.publish(NodeMetrics {
            address: "b".into(),
            timestamp: 0,
            cpu_load: 1.0,
            memory_used: 1,
            memory_max: 1,
        });
        let r = WeightedRoutees::new(m, MetricsSelector::Mix);
        assert_eq!(r.pick(&["a", "b"], 0.5), Some("a"));
    }
}

// -- Phase 10.B: optional sysinfo-backed probe -----------------------

#[cfg(feature = "sysinfo-probe")]
pub mod sys {
    //! `sysinfo`-backed [`super::MetricsProbe`]. Enabled with the
    //! `sysinfo-probe` feature.
    use super::{MetricsProbe, NodeMetrics};
    use std::sync::Mutex;
    use sysinfo::System;

    pub struct SysinfoProbe {
        sys: Mutex<System>,
    }

    impl Default for SysinfoProbe {
        fn default() -> Self {
            Self::new()
        }
    }

    impl SysinfoProbe {
        pub fn new() -> Self {
            Self { sys: Mutex::new(System::new_all()) }
        }
    }

    impl MetricsProbe for SysinfoProbe {
        fn sample(&self, address: &str, timestamp: u64) -> NodeMetrics {
            let mut sys = self.sys.lock().unwrap();
            sys.refresh_cpu_usage();
            sys.refresh_memory();
            // global_cpu_usage() is in [0..100]; normalize to [0..1].
            let cpu_load = (sys.global_cpu_usage() as f64 / 100.0).clamp(0.0, 1.0);
            let memory_max = sys.total_memory();
            let memory_used = sys.used_memory();
            NodeMetrics { address: address.into(), timestamp, cpu_load, memory_used, memory_max }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn sysinfo_probe_returns_finite_load() {
            let p = SysinfoProbe::new();
            let m = p.sample("a", 1);
            assert!(m.cpu_load.is_finite());
            assert!(m.memory_max >= m.memory_used);
        }
    }
}

// -- Phase 10.C: metrics gossip --------------------------------------

/// Wire shape for cross-node metric exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetricsPdu {
    /// Push the sender's latest sample.
    Push(NodeMetrics),
    /// Push a batch of samples (e.g. for catch-up sync).
    PushBatch(Vec<NodeMetrics>),
}

/// Pluggable transport for metrics gossip. Mirrors
/// [`atomr_cluster::GossipTransport`] in spirit but works on raw addresses.
pub trait MetricsTransport: Send + Sync + 'static {
    fn send(&self, target_node: &str, pdu: MetricsPdu);
}

/// Apply an inbound `MetricsPdu` into a [`ClusterMetrics`].
pub fn apply_metrics_pdu(metrics: &ClusterMetrics, pdu: MetricsPdu) {
    match pdu {
        MetricsPdu::Push(m) => metrics.publish(m),
        MetricsPdu::PushBatch(v) => {
            for m in v {
                metrics.publish(m);
            }
        }
    }
}

/// Push the local probe sample to a peer. Caller drives this on a tick.
pub fn gossip_local_metrics<P: MetricsProbe + ?Sized>(
    probe: &P,
    self_address: &str,
    target_node: &str,
    transport: &dyn MetricsTransport,
    now: u64,
) {
    let m = probe.sample(self_address, now);
    transport.send(target_node, MetricsPdu::Push(m));
}

#[cfg(test)]
mod gossip_tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CaptureTransport {
        sent: Mutex<Vec<(String, MetricsPdu)>>,
    }
    impl MetricsTransport for CaptureTransport {
        fn send(&self, target: &str, pdu: MetricsPdu) {
            self.sent.lock().unwrap().push((target.to_string(), pdu));
        }
    }

    #[test]
    fn gossip_pushes_local_sample_to_target() {
        let probe = StaticProbe { cpu_load: 0.3, memory_used: 1, memory_max: 4 };
        let net = CaptureTransport::default();
        gossip_local_metrics(&probe, "self", "peer", &net, 1);
        let sent = net.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        match &sent[0].1 {
            MetricsPdu::Push(m) => assert_eq!(m.address, "self"),
            _ => panic!("expected Push"),
        }
    }

    #[test]
    fn apply_pdu_merges_into_metrics() {
        let m = ClusterMetrics::new();
        let pdu = MetricsPdu::Push(NodeMetrics {
            address: "x".into(),
            timestamp: 7,
            cpu_load: 0.1,
            memory_used: 1,
            memory_max: 2,
        });
        apply_metrics_pdu(&m, pdu);
        assert_eq!(m.node_count(), 1);
        assert_eq!(m.get("x").unwrap().timestamp, 7);
    }

    #[test]
    fn adaptive_balancer_can_be_used_as_picker_closure() {
        let m = Arc::new(ClusterMetrics::new());
        m.publish(NodeMetrics {
            address: "akka.tcp://Sys@a:1".into(),
            timestamp: 0,
            cpu_load: 0.9,
            memory_used: 0,
            memory_max: 1,
        });
        m.publish(NodeMetrics {
            address: "akka.tcp://Sys@b:1".into(),
            timestamp: 0,
            cpu_load: 0.1,
            memory_used: 0,
            memory_max: 1,
        });
        let lb = Arc::new(AdaptiveLoadBalancer::new(m));
        type Picker = Arc<dyn Fn(&[String]) -> Option<String> + Send + Sync>;
        let picker: Picker = {
            let lb = lb.clone();
            Arc::new(move |cands| {
                let refs: Vec<&str> = cands.iter().map(String::as_str).collect();
                lb.pick(&refs).map(|s| s.to_string())
            })
        };
        let chosen = picker(&["akka.tcp://Sys@a:1".to_string(), "akka.tcp://Sys@b:1".to_string()]).unwrap();
        assert_eq!(chosen, "akka.tcp://Sys@b:1");
    }

    #[test]
    fn batch_pdu_merges_each() {
        let m = ClusterMetrics::new();
        let pdu = MetricsPdu::PushBatch(vec![
            NodeMetrics { address: "a".into(), timestamp: 1, cpu_load: 0.0, memory_used: 0, memory_max: 0 },
            NodeMetrics { address: "b".into(), timestamp: 2, cpu_load: 0.0, memory_used: 0, memory_max: 0 },
        ]);
        apply_metrics_pdu(&m, pdu);
        assert_eq!(m.node_count(), 2);
    }
}
