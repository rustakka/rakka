//! In-memory Replicator. akka.net: `Akka.DistributedData/Replicator.cs`.
//!
//! The full akka.net replicator gossips deltas to other nodes; this port
//! implements the local storage/merge aspects. Remote replication plugs
//! into `rakka-cluster` in a later phase.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::traits::CrdtMerge;

#[derive(Debug, Clone, Copy)]
pub enum WriteConsistency {
    Local,
    All,
    Majority,
}

#[derive(Debug, Clone, Copy)]
pub enum ReadConsistency {
    Local,
    All,
    Majority,
}

type Entry = Box<dyn Any + Send + Sync>;

pub struct Replicator {
    store: RwLock<HashMap<String, Entry>>,
}

impl Default for Replicator {
    fn default() -> Self {
        Self { store: RwLock::new(HashMap::new()) }
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

    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        T: CrdtMerge + Clone + Send + Sync + 'static,
    {
        self.store.read().get(key).and_then(|e| e.downcast_ref::<T>().cloned())
    }

    pub fn delete(&self, key: &str) {
        self.store.write().remove(key);
    }

    /// Snapshot of all keys currently held by this replicator. Useful for
    /// telemetry / dashboards.
    pub fn keys(&self) -> Vec<String> {
        let mut ks: Vec<String> = self.store.read().keys().cloned().collect();
        ks.sort();
        ks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GCounter;

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
}
