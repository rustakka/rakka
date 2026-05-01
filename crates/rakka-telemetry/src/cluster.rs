//! Cluster state probe — wraps `rakka-cluster`'s `MembershipState` /
//! `Gossip` data structures with an owning snapshot that the dashboard
//! can poll and an event publisher that emits diff events on updates.

use parking_lot::RwLock;

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::{ClusterMemberInfo, ClusterMembershipDiff, ClusterStateInfo};
#[cfg(feature = "cluster")]
use crate::dto::ReachabilityRecord;

pub struct ClusterProbe {
    bus: TelemetryBus,
    state: RwLock<ClusterStateInfo>,
}

impl ClusterProbe {
    pub fn new(bus: TelemetryBus) -> Self {
        Self { bus, state: RwLock::new(ClusterStateInfo::default()) }
    }

    pub fn set_self_address(&self, addr: impl Into<String>) {
        self.state.write().self_address = Some(addr.into());
    }

    pub fn set_leader(&self, leader: Option<String>) {
        self.state.write().leader = leader;
    }

    /// Replace the current snapshot and emit a diff event describing the
    /// change. Consumers that already track a baseline can use the diff
    /// directly; dashboards that just want the latest value can poll
    /// [`Self::snapshot`].
    pub fn update(&self, next: ClusterStateInfo) {
        let prev = std::mem::replace(&mut *self.state.write(), next.clone());
        let diff = compute_diff(&prev, &next);
        if !diff.is_empty() {
            self.bus.publish(TelemetryEvent::ClusterChanged(diff));
        }
    }

    pub fn snapshot(&self) -> ClusterStateInfo {
        self.state.read().clone()
    }

    pub fn member_count(&self) -> usize {
        self.state.read().members.len()
    }

    pub fn unreachable_count(&self) -> usize {
        self.state.read().unreachable.len()
    }
}

fn compute_diff(prev: &ClusterStateInfo, next: &ClusterStateInfo) -> ClusterMembershipDiff {
    let prev_by_addr: std::collections::HashMap<&str, &ClusterMemberInfo> =
        prev.members.iter().map(|m| (m.address.as_str(), m)).collect();
    let next_by_addr: std::collections::HashMap<&str, &ClusterMemberInfo> =
        next.members.iter().map(|m| (m.address.as_str(), m)).collect();

    let mut added = Vec::new();
    let mut updated = Vec::new();
    for m in &next.members {
        match prev_by_addr.get(m.address.as_str()) {
            None => added.push(m.clone()),
            Some(old) if old.status != m.status || old.reachable != m.reachable => {
                updated.push(m.clone())
            }
            _ => {}
        }
    }
    let removed: Vec<String> =
        prev.members.iter().filter(|m| !next_by_addr.contains_key(m.address.as_str())).map(|m| m.address.clone()).collect();

    let prev_unreach: std::collections::HashSet<&str> =
        prev.unreachable.iter().map(|s| s.as_str()).collect();
    let next_unreach: std::collections::HashSet<&str> =
        next.unreachable.iter().map(|s| s.as_str()).collect();
    let became_unreachable: Vec<String> =
        next_unreach.difference(&prev_unreach).map(|s| s.to_string()).collect();
    let became_reachable: Vec<String> =
        prev_unreach.difference(&next_unreach).map(|s| s.to_string()).collect();

    ClusterMembershipDiff { added, updated, removed, became_unreachable, became_reachable }
}

impl ClusterMembershipDiff {
    fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.updated.is_empty()
            && self.removed.is_empty()
            && self.became_unreachable.is_empty()
            && self.became_reachable.is_empty()
    }
}

/// Convert a `rakka-cluster` `MembershipState` into our serializable
/// `ClusterStateInfo`. Feature-gated because the cluster crate is optional.
#[cfg(feature = "cluster")]
pub fn from_cluster_state(state: &rakka_cluster::MembershipState) -> ClusterStateInfo {
    use rakka_cluster::ReachabilityStatus;

    let members: Vec<ClusterMemberInfo> = state
        .members
        .iter()
        .map(|m| ClusterMemberInfo {
            address: m.address.to_string(),
            status: format!("{:?}", m.status),
            roles: m.roles.clone(),
            reachable: state.reachability.is_reachable(&m.address),
            up_number: m.up_number,
        })
        .collect();

    let unreachable: Vec<String> =
        members.iter().filter(|m| !m.reachable).map(|m| m.address.clone()).collect();

    let reachability_records: Vec<ReachabilityRecord> = state
        .reachability
        .records
        .iter()
        .map(|((observer, subject), status)| ReachabilityRecord {
            observer: observer.to_string(),
            subject: subject.to_string(),
            status: match status {
                ReachabilityStatus::Reachable => "reachable".into(),
                ReachabilityStatus::Unreachable => "unreachable".into(),
                ReachabilityStatus::Terminated => "terminated".into(),
                _ => "unknown".into(),
            },
        })
        .collect();

    ClusterStateInfo {
        self_address: None,
        leader: None,
        members,
        unreachable,
        reachability_records,
        gossip_version: Vec::new(),
    }
}

/// Convert a full `Gossip` into a serializable `ClusterStateInfo` that
/// also carries the vector-clock version vector.
#[cfg(feature = "cluster")]
pub fn from_gossip(gossip: &rakka_cluster::Gossip) -> ClusterStateInfo {
    let mut state = from_cluster_state(&gossip.state);
    state.gossip_version =
        gossip.version.versions.iter().map(|(k, v)| (k.clone(), *v)).collect();
    state
}

impl ClusterProbe {
    /// Convenience: update from a live `rakka-cluster::Gossip`.
    #[cfg(feature = "cluster")]
    pub fn update_from_gossip(&self, gossip: &rakka_cluster::Gossip) {
        self.update(from_gossip(gossip));
    }

    /// Convenience: update from a `rakka-cluster::MembershipState`.
    #[cfg(feature = "cluster")]
    pub fn update_from_state(&self, state: &rakka_cluster::MembershipState) {
        self.update(from_cluster_state(state));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(addr: &str, status: &str, reachable: bool) -> ClusterMemberInfo {
        ClusterMemberInfo {
            address: addr.into(),
            status: status.into(),
            roles: vec![],
            reachable,
            up_number: 1,
        }
    }

    #[test]
    fn diffs_added_updated_removed() {
        let prev = ClusterStateInfo {
            members: vec![member("a", "Up", true), member("b", "Up", true)],
            unreachable: vec![],
            ..Default::default()
        };
        let next = ClusterStateInfo {
            members: vec![member("a", "Leaving", true), member("c", "Joining", true)],
            unreachable: vec![],
            ..Default::default()
        };
        let d = compute_diff(&prev, &next);
        assert_eq!(d.added.len(), 1);
        assert_eq!(d.updated.len(), 1);
        assert_eq!(d.removed.len(), 1);
    }

    #[tokio::test]
    async fn emits_change_event() {
        let bus = TelemetryBus::new(8);
        let mut rx = bus.subscribe();
        let probe = ClusterProbe::new(bus);
        probe.update(ClusterStateInfo {
            members: vec![member("a", "Up", true)],
            ..Default::default()
        });
        let e = rx.recv().await.unwrap();
        assert_eq!(e.topic(), "cluster");
    }
}
