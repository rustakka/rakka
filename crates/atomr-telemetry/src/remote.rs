//! Remote probe — snapshots the remote `EndpointRegistry` plus inbound /
//! outbound byte counters. Cooperates with the (optional)
//! `atomr-remote` crate.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::{RemoteAssociationInfo, RemoteSnapshot};

struct AssociationCounters {
    state: parking_lot::RwLock<String>,
    inbound_bytes: AtomicU64,
    outbound_bytes: AtomicU64,
}

pub struct RemoteProbe {
    bus: TelemetryBus,
    associations: DashMap<String, AssociationCounters>,
}

impl RemoteProbe {
    pub fn new(bus: TelemetryBus) -> Self {
        Self { bus, associations: DashMap::new() }
    }

    pub fn record_association(&self, remote_address: &str, state: &str) {
        self.associations.insert(
            remote_address.to_string(),
            AssociationCounters {
                state: parking_lot::RwLock::new(state.to_string()),
                inbound_bytes: AtomicU64::new(0),
                outbound_bytes: AtomicU64::new(0),
            },
        );
        self.bus.publish(TelemetryEvent::RemoteAssociation(RemoteAssociationInfo {
            remote_address: remote_address.to_string(),
            state: state.to_string(),
            inbound_bytes: 0,
            outbound_bytes: 0,
        }));
    }

    pub fn set_state(&self, remote_address: &str, state: &str) {
        if let Some(entry) = self.associations.get(remote_address) {
            *entry.state.write() = state.to_string();
        }
    }

    pub fn record_inbound_bytes(&self, remote_address: &str, bytes: u64) {
        if let Some(entry) = self.associations.get(remote_address) {
            entry.inbound_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn record_outbound_bytes(&self, remote_address: &str, bytes: u64) {
        if let Some(entry) = self.associations.get(remote_address) {
            entry.outbound_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn remove(&self, remote_address: &str) {
        self.associations.remove(remote_address);
    }

    pub fn association_count(&self) -> usize {
        self.associations.len()
    }

    pub fn snapshot(&self) -> RemoteSnapshot {
        let associations: Vec<RemoteAssociationInfo> = self
            .associations
            .iter()
            .map(|e| RemoteAssociationInfo {
                remote_address: e.key().clone(),
                state: e.value().state.read().clone(),
                inbound_bytes: e.value().inbound_bytes.load(Ordering::Relaxed),
                outbound_bytes: e.value().outbound_bytes.load(Ordering::Relaxed),
            })
            .collect();
        RemoteSnapshot { associations }
    }
}

/// Populate the probe from a live [`atomr_remote::EndpointManager`].
/// Creates entries for every known remote with the manager's reported
/// association state, and pulls byte counters from
/// [`atomr_remote::RemoteMetrics`].
#[cfg(feature = "remote")]
pub fn refresh_from_endpoint_manager(probe: &RemoteProbe, manager: &atomr_remote::EndpointManager) {
    use std::collections::HashSet;
    let states = manager.peer_states();
    let live: HashSet<String> = states.iter().map(|(k, _, _)| k.clone()).collect();
    for (addr, state, _attempt) in &states {
        probe.associations.entry(addr.clone()).or_insert_with(|| AssociationCounters {
            state: parking_lot::RwLock::new((*state).to_string()),
            inbound_bytes: AtomicU64::new(0),
            outbound_bytes: AtomicU64::new(0),
        });
        probe.set_state(addr, state);
    }
    let snap = manager.metrics().snapshot();
    for row in snap.per_address {
        if let Some(entry) = probe.associations.get(&row.address) {
            entry.inbound_bytes.store(row.received_bytes, Ordering::Relaxed);
            entry.outbound_bytes.store(row.sent_bytes, Ordering::Relaxed);
        }
    }
    let stale: Vec<String> =
        probe.associations.iter().map(|e| e.key().clone()).filter(|k| !live.contains(k)).collect();
    for k in stale {
        probe.remove(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_associations_and_bytes() {
        let bus = TelemetryBus::new(8);
        let p = RemoteProbe::new(bus);
        p.record_association("akka://A@host:1", "active");
        p.record_inbound_bytes("akka://A@host:1", 100);
        p.record_outbound_bytes("akka://A@host:1", 200);
        let snap = p.snapshot();
        assert_eq!(snap.associations.len(), 1);
        assert_eq!(snap.associations[0].inbound_bytes, 100);
        assert_eq!(snap.associations[0].outbound_bytes, 200);
    }
}
