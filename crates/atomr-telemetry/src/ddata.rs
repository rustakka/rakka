//! Distributed-data probe — tracks replicator key updates.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashSet;

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::DDataSnapshot;

pub struct DDataProbe {
    bus: TelemetryBus,
    keys: DashSet<String>,
    updates: AtomicU64,
}

impl DDataProbe {
    pub fn new(bus: TelemetryBus) -> Self {
        Self { bus, keys: DashSet::new(), updates: AtomicU64::new(0) }
    }

    pub fn record_update(&self, key: &str) {
        self.keys.insert(key.to_string());
        self.updates.fetch_add(1, Ordering::Relaxed);
        self.bus.publish(TelemetryEvent::DDataUpdated { key: key.to_string() });
    }

    pub fn record_delete(&self, key: &str) {
        self.keys.remove(key);
    }

    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    pub fn snapshot(&self) -> DDataSnapshot {
        let mut keys: Vec<String> = self.keys.iter().map(|k| k.clone()).collect();
        keys.sort();
        DDataSnapshot { keys, total_updates: self.updates.load(Ordering::Relaxed) }
    }

    /// Refresh key set from a live replicator. Feature-gated.
    #[cfg(feature = "ddata")]
    pub fn refresh_from(&self, replicator: &atomr_distributed_data::Replicator) {
        let current: std::collections::HashSet<String> = self.keys.iter().map(|k| k.clone()).collect();
        let fresh: std::collections::HashSet<String> = replicator.keys().into_iter().collect();
        for gone in current.difference(&fresh) {
            self.keys.remove(gone);
        }
        for new in fresh.difference(&current) {
            self.keys.insert(new.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_keys_and_counts() {
        let bus = TelemetryBus::new(8);
        let p = DDataProbe::new(bus);
        p.record_update("counter");
        p.record_update("set");
        p.record_update("counter");
        assert_eq!(p.key_count(), 2);
        let s = p.snapshot();
        assert_eq!(s.total_updates, 3);
        assert_eq!(s.keys.len(), 2);
    }
}
