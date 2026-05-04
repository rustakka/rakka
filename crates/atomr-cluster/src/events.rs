//! Cluster events bus.
//!
//! Phase 6 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Cluster/ClusterEvent.cs`. Events are published when membership
//! transitions, leader changes, or reachability flips. Subscribers
//! register a per-event-class callback (or a multi-class one via the
//! [`ClusterEvent`] enum) and receive each event in publish order.
//!
//! The bus is a thin `RwLock<Vec<callback>>` rather than an actor
//! because subscribers are typically a handful of long-lived objects
//! (telemetry probes, sharding region, pubsub mediator) and the
//! contention model is "rare write, rare read." Phase 13 may move it
//! behind a real actor if profiling justifies it.

use std::sync::Arc;

use atomr_core::actor::Address;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::member::{Member, MemberStatus};

/// Event variants published on [`ClusterEventBus`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClusterEvent {
    MemberJoined(Member),
    MemberUp(Member),
    MemberLeft(Member),
    MemberExited(Member),
    MemberRemoved(Member, MemberStatus),
    UnreachableMember(Member),
    ReachableMember(Member),
    LeaderChanged { from: Option<Address>, to: Option<Address> },
    ClusterShuttingDown,
    Convergence(bool),
}

type Subscriber = Arc<dyn Fn(&ClusterEvent) + Send + Sync + 'static>;

/// In-process cluster events bus.
#[derive(Default, Clone)]
pub struct ClusterEventBus {
    inner: Arc<RwLock<Vec<Subscriber>>>,
}

impl ClusterEventBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a subscriber that fires on every event. Returns a
    /// handle whose `Drop` removes the subscription.
    pub fn subscribe<F>(&self, callback: F) -> SubscriptionHandle
    where
        F: Fn(&ClusterEvent) + Send + Sync + 'static,
    {
        let cb: Subscriber = Arc::new(callback);
        let mut subs = self.inner.write();
        subs.push(cb.clone());
        SubscriptionHandle {
            bus: self.inner.clone(),
            // Use the Arc pointer identity to find this subscription on drop.
            id: Arc::as_ptr(&cb) as *const () as usize,
            anchor: cb,
        }
    }

    /// Publish an event to all current subscribers, synchronously,
    /// in registration order. Subscribers must not block.
    pub fn publish(&self, event: ClusterEvent) {
        let subs = self.inner.read().clone();
        for s in &subs {
            s(&event);
        }
    }

    /// Number of currently registered subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.inner.read().len()
    }
}

/// RAII handle returned by [`ClusterEventBus::subscribe`]. Dropping it
/// removes the corresponding subscriber.
pub struct SubscriptionHandle {
    bus: Arc<RwLock<Vec<Subscriber>>>,
    id: usize,
    /// Keeps the `Arc` alive so the pointer identity matches on drop.
    anchor: Subscriber,
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        let mut subs = self.bus.write();
        subs.retain(|s| Arc::as_ptr(s) as *const () as usize != self.id);
        // anchor goes out of scope after retain, so the Arc count drops.
        let _ = &self.anchor;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn publish_delivers_to_subscribers() {
        let bus = ClusterEventBus::new();
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let _h = bus.subscribe(move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        });
        bus.publish(ClusterEvent::ClusterShuttingDown);
        bus.publish(ClusterEvent::Convergence(true));
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn drop_unsubscribes() {
        let bus = ClusterEventBus::new();
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let h = bus.subscribe(move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(bus.subscriber_count(), 1);
        drop(h);
        assert_eq!(bus.subscriber_count(), 0);
        bus.publish(ClusterEvent::Convergence(false));
        assert_eq!(n.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn leader_changed_carries_old_and_new() {
        let bus = ClusterEventBus::new();
        let captured = Arc::new(parking_lot::Mutex::new(None));
        let c2 = captured.clone();
        let _h = bus.subscribe(move |e| {
            *c2.lock() = Some(e.clone());
        });
        bus.publish(ClusterEvent::LeaderChanged { from: None, to: Some(Address::local("a")) });
        let got = captured.lock().clone();
        assert!(matches!(got, Some(ClusterEvent::LeaderChanged { from: None, to: Some(_) })));
    }
}
