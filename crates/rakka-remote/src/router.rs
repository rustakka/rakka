//! Remote router config. akka.net: `Remote/Routing/RemoteRouterConfig.cs`.
//!
//! Wraps a local routing strategy so that the routees can be deployed
//! across a list of remote `Address`es. The local pool decides *which*
//! routee gets the next message; the `RemoteRouterConfig` decides *where*
//! that routee lives.

use std::sync::Arc;

use rakka_core::actor::{ActorPath, Address};

use crate::endpoint_manager::EndpointManager;

/// Strategy for distributing routees across the configured `nodes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteRouterStrategy {
    /// Round-robin across nodes in declaration order.
    RoundRobin,
    /// Hash an arbitrary key onto a node.
    ConsistentHash,
}

#[derive(Clone)]
pub struct RemoteRouterConfig {
    pub nodes: Arc<Vec<Address>>,
    pub strategy: RemoteRouterStrategy,
    pub endpoint_manager: EndpointManager,
    counter: Arc<std::sync::atomic::AtomicUsize>,
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
        }
    }

    /// Pick the next remote node for the message identified by `key`
    /// (use `None` for round-robin / counter-driven selection).
    pub fn pick(&self, key: Option<&str>) -> Option<&Address> {
        if self.nodes.is_empty() {
            return None;
        }
        let i = match (self.strategy, key) {
            (RemoteRouterStrategy::RoundRobin, _) => {
                self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    % self.nodes.len()
            }
            (RemoteRouterStrategy::ConsistentHash, Some(k)) => {
                fxhash(k) as usize % self.nodes.len()
            }
            (RemoteRouterStrategy::ConsistentHash, None) => 0,
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
