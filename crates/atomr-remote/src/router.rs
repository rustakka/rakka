//! Remote router config.
//!
//! Wraps a local routing strategy so that the routees can be deployed
//! across a list of remote `Address`es. The local pool decides *which*
//! routee gets the next message; the `RemoteRouterConfig` decides *where*
//! that routee lives.

use std::sync::Arc;

use atomr_core::actor::{ActorPath, Address};

use crate::endpoint_manager::EndpointManager;

/// Strategy for distributing routees across the configured `nodes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RemoteRouterStrategy {
    /// Round-robin across nodes in declaration order.
    RoundRobin,
    /// Hash an arbitrary key onto a node.
    ConsistentHash,
    /// Delegate to the configured adaptive picker (e.g. lowest-CPU
    /// from cluster-metrics' `AdaptiveLoadBalancer`).
    Adaptive,
}

/// Pluggable picker for [`RemoteRouterStrategy::Adaptive`]. Receives
/// the candidate addresses (as strings) and returns the chosen one.
pub type AdaptivePicker = Arc<dyn Fn(&[String]) -> Option<String> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct RemoteRouterConfig {
    pub nodes: Arc<Vec<Address>>,
    pub strategy: RemoteRouterStrategy,
    pub endpoint_manager: EndpointManager,
    counter: Arc<std::sync::atomic::AtomicUsize>,
    adaptive: Option<AdaptivePicker>,
}

impl RemoteRouterConfig {
    pub fn new(
        nodes: Vec<Address>,
        strategy: RemoteRouterStrategy,
        endpoint_manager: EndpointManager,
    ) -> Self {
        Self {
            nodes: Arc::new(nodes),
            strategy,
            endpoint_manager,
            counter: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            adaptive: None,
        }
    }

    /// Install an adaptive picker for [`RemoteRouterStrategy::Adaptive`].
    /// Cluster-metrics callers wire `AdaptiveLoadBalancer` here:
    ///
    /// ```ignore
    /// router.with_adaptive_picker(Arc::new(move |cands| {
    ///     let refs: Vec<&str> = cands.iter().map(String::as_str).collect();
    ///     lb.pick(&refs).map(str::to_string)
    /// }));
    /// ```
    pub fn with_adaptive_picker(mut self, picker: AdaptivePicker) -> Self {
        self.adaptive = Some(picker);
        self
    }

    /// Pick the next remote node for the message identified by `key`
    /// (use `None` for round-robin / counter-driven selection).
    pub fn pick(&self, key: Option<&str>) -> Option<&Address> {
        if self.nodes.is_empty() {
            return None;
        }
        let i = match (self.strategy, key) {
            (RemoteRouterStrategy::RoundRobin, _) => {
                self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.nodes.len()
            }
            (RemoteRouterStrategy::ConsistentHash, Some(k)) => fxhash(k) as usize % self.nodes.len(),
            (RemoteRouterStrategy::ConsistentHash, None) => 0,
            (RemoteRouterStrategy::Adaptive, _) => {
                // Delegate to the picker; fall back to round-robin if
                // no picker is installed or the picker returns None.
                if let Some(p) = &self.adaptive {
                    let cands: Vec<String> = self.nodes.iter().map(|a| a.to_string()).collect();
                    if let Some(chosen) = p(&cands) {
                        if let Some(idx) = self.nodes.iter().position(|a| a.to_string() == chosen) {
                            return Some(&self.nodes[idx]);
                        }
                    }
                }
                self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.nodes.len()
            }
        };
        Some(&self.nodes[i])
    }

    /// Build a fully-qualified routee path on the picked remote node.
    pub fn routee_path(&self, base: &str, key: Option<&str>) -> Option<ActorPath> {
        let addr = self.pick(key)?.clone();
        let mut path = ActorPath::root(addr).child("user");
        for seg in base.split('/').filter(|s| !s.is_empty()) {
            path = path.child(seg);
        }
        Some(path)
    }
}

/// Cheap non-cryptographic hash used for `ConsistentHash`.
fn fxhash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}
