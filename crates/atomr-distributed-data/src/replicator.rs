//! In-memory Replicator. akka.net: `Akka.DistributedData/Replicator.cs`.
//!
//! The full akka.net replicator gossips deltas to other nodes; this port
//! implements the local storage/merge aspects. Remote replication plugs
//! into `atomr-cluster` in a later phase.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use crate::traits::CrdtMerge;

/// Phase 8.D — typed consistency levels with timeouts. The current
/// in-process Replicator runs every operation as `Local` (single-node
/// store); cross-node `All`/`Majority`/`From(n)` semantics activate
/// once Phase 6 gossip lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteConsistency {
    Local,
    All { timeout: Duration },
    Majority { timeout: Duration },
    From { n: usize, timeout: Duration },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadConsistency {
    Local,
    All { timeout: Duration },
    Majority { timeout: Duration },
    From { n: usize, timeout: Duration },
}

impl WriteConsistency {
    /// Number of nodes that must acknowledge before the write is
    /// considered complete (1 for `Local`).
    pub fn required_acks(self, cluster_size: usize) -> usize {
        match self {
            Self::Local => 1,
            Self::All { .. } => cluster_size.max(1),
            Self::Majority { .. } => (cluster_size / 2) + 1,
            Self::From { n, .. } => n.min(cluster_size.max(1)),
        }
    }

    pub fn timeout(self) -> Option<Duration> {
        match self {
            Self::Local => None,
            Self::All { timeout } | Self::Majority { timeout } | Self::From { timeout, .. } => Some(timeout),
        }
    }
}

impl ReadConsistency {
    pub fn required_replies(self, cluster_size: usize) -> usize {
        match self {
            Self::Local => 1,
            Self::All { .. } => cluster_size.max(1),
            Self::Majority { .. } => (cluster_size / 2) + 1,
            Self::From { n, .. } => n.min(cluster_size.max(1)),
        }
    }

    pub fn timeout(self) -> Option<Duration> {
        match self {
            Self::Local => None,
            Self::All { timeout } | Self::Majority { timeout } | Self::From { timeout, .. } => Some(timeout),
        }
    }
}

type Entry = Box<dyn Any + Send + Sync>;
type SubscriberId = u64;
type Notifier = Box<dyn Fn(&str) + Send + Sync + 'static>;

pub struct Replicator {
    store: RwLock<HashMap<String, Entry>>,
    subscribers: RwLock<HashMap<String, Vec<(SubscriberId, Notifier)>>>,
    next_sub_id: AtomicU64,
}

impl Default for Replicator {
    fn default() -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
            next_sub_id: AtomicU64::new(0),
        }
    }
}

impl Replicator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn update<T>(&self, key: &str, value: T)
    where
        T: CrdtMerge + Send + Sync + 'static,
    {
        {
            let mut map = self.store.write();
            match map.get_mut(key) {
                Some(existing) => {
                    if let Some(current) = existing.downcast_mut::<T>() {
                        current.merge(&value);
                    } else {
                        map.insert(key.to_string(), Box::new(value));
                    }
                }
                None => {
                    map.insert(key.to_string(), Box::new(value));
                }
            }
        }
        self.notify(key);
    }

    /// Register `notifier` to fire on every successful
    /// `update(key, _)` or `delete(key)`. Returns a
    /// [`SubscriptionToken`] whose `Drop` removes the subscription.
    /// Phase 8.E.
    pub fn subscribe<F>(self: &Arc<Self>, key: impl Into<String>, notifier: F) -> SubscriptionToken
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        let key = key.into();
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        self.subscribers.write().entry(key.clone()).or_default().push((id, Box::new(notifier)));
        SubscriptionToken { id, key, replicator: Arc::downgrade(self) }
    }

    /// Internal: deliver notifications. Public so the cluster
    /// adapter can re-fire after a remote merge.
    pub fn notify(&self, key: &str) {
        let subs = self.subscribers.read();
        if let Some(list) = subs.get(key) {
            for (_, cb) in list {
                cb(key);
            }
        }
    }

    /// Drop the subscription identified by `token`. Called from
    /// `SubscriptionToken::drop`; safe to invoke multiple times.
    pub(crate) fn unsubscribe_by_id(&self, key: &str, id: SubscriberId) {
        let mut g = self.subscribers.write();
        if let Some(list) = g.get_mut(key) {
            list.retain(|(i, _)| *i != id);
            if list.is_empty() {
                g.remove(key);
            }
        }
    }

    /// Number of subscribers for a key (debug / telemetry).
    pub fn subscriber_count(&self, key: &str) -> usize {
        self.subscribers.read().get(key).map(|v| v.len()).unwrap_or(0)
    }

    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        T: CrdtMerge + Clone + Send + Sync + 'static,
    {
        self.store.read().get(key).and_then(|e| e.downcast_ref::<T>().cloned())
    }

    pub fn delete(&self, key: &str) {
        self.store.write().remove(key);
        self.notify(key);
    }

    /// Snapshot of all keys currently held by this replicator. Useful for
    /// telemetry / dashboards.
    pub fn keys(&self) -> Vec<String> {
        let mut ks: Vec<String> = self.store.read().keys().cloned().collect();
        ks.sort();
        ks
    }
}

/// RAII handle returned by [`Replicator::subscribe`].
pub struct SubscriptionToken {
    id: SubscriberId,
    key: String,
    replicator: std::sync::Weak<Replicator>,
}

impl Drop for SubscriptionToken {
    fn drop(&mut self) {
        if let Some(r) = self.replicator.upgrade() {
            r.unsubscribe_by_id(&self.key, self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GCounter;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn update_merges_into_existing_value() {
        let r = Replicator::new();
        let mut c1 = GCounter::new();
        c1.increment("n1", 1);
        r.update("count", c1);
        let mut c2 = GCounter::new();
        c2.increment("n2", 5);
        r.update("count", c2);
        let got: GCounter = r.get("count").unwrap();
        assert_eq!(got.value(), 6);
    }

    #[test]
    fn subscribe_fires_on_update() {
        let r = Replicator::new();
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let _t = r.subscribe("k", move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        });
        let mut c = GCounter::new();
        c.increment("a", 1);
        r.update("k", c.clone());
        r.update("k", c.clone());
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn subscribe_fires_on_delete() {
        let r = Replicator::new();
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let _t = r.subscribe("k", move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        });
        r.update("k", GCounter::new());
        r.delete("k");
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn drop_token_unsubscribes() {
        let r = Replicator::new();
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let t = r.subscribe("k", move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(r.subscriber_count("k"), 1);
        drop(t);
        assert_eq!(r.subscriber_count("k"), 0);
        r.update("k", GCounter::new());
        assert_eq!(n.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn write_consistency_majority_math() {
        let w = WriteConsistency::Majority { timeout: Duration::from_secs(1) };
        assert_eq!(w.required_acks(1), 1);
        assert_eq!(w.required_acks(3), 2);
        assert_eq!(w.required_acks(5), 3);
        assert_eq!(w.required_acks(6), 4);
    }

    #[test]
    fn write_consistency_all_uses_cluster_size() {
        let w = WriteConsistency::All { timeout: Duration::from_secs(1) };
        assert_eq!(w.required_acks(7), 7);
        assert_eq!(w.required_acks(0), 1); // floor at 1
    }

    #[test]
    fn read_consistency_from_clamps_to_cluster_size() {
        let r = ReadConsistency::From { n: 99, timeout: Duration::from_secs(1) };
        assert_eq!(r.required_replies(3), 3);
    }

    #[test]
    fn local_consistency_has_no_timeout() {
        assert!(WriteConsistency::Local.timeout().is_none());
        assert!(ReadConsistency::Local.timeout().is_none());
    }

    #[test]
    fn subscribe_only_fires_for_matching_key() {
        let r = Replicator::new();
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let _t = r.subscribe("a", move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        });
        r.update("a", GCounter::new());
        r.update("b", GCounter::new());
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }
}
