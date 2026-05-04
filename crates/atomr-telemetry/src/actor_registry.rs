//! Actor registry probe — tracks path/parent/mailbox-depth for every live
//! actor in an `ActorSystem`.
//!
//! Populated by the `atomr-core` spawn/stop hooks when a
//! [`crate::TelemetryExtension`] is registered on the actor system.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use atomr_core::actor::{ActorPath, SpawnObserver};

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::{ActorSnapshot, ActorStatus, ActorTreeNode};

pub struct ActorRegistry {
    entries: DashMap<String, ActorStatus>,
    bus: TelemetryBus,
    spawned: AtomicU64,
    stopped: AtomicU64,
}

impl ActorRegistry {
    pub fn new(bus: TelemetryBus) -> Self {
        Self { entries: DashMap::new(), bus, spawned: AtomicU64::new(0), stopped: AtomicU64::new(0) }
    }

    pub fn record_spawn(&self, status: ActorStatus) {
        self.spawned.fetch_add(1, Ordering::Relaxed);
        self.entries.insert(status.path.clone(), status.clone());
        self.bus.publish(TelemetryEvent::ActorSpawned(status));
    }

    pub fn record_stop(&self, path: &str) {
        if self.entries.remove(path).is_some() {
            self.stopped.fetch_add(1, Ordering::Relaxed);
            self.bus.publish(TelemetryEvent::ActorStopped { path: path.to_string() });
        }
    }

    pub fn record_mailbox_depth(&self, path: &str, depth: u64) {
        if let Some(mut e) = self.entries.get_mut(path) {
            e.mailbox_depth = depth;
        }
        self.bus.publish(TelemetryEvent::MailboxSampled { path: path.to_string(), depth });
    }

    pub fn total_spawned(&self) -> u64 {
        self.spawned.load(Ordering::Relaxed)
    }

    pub fn total_stopped(&self) -> u64 {
        self.stopped.load(Ordering::Relaxed)
    }

    pub fn live_count(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn snapshot(&self) -> ActorSnapshot {
        let flat: Vec<ActorStatus> = self.entries.iter().map(|e| e.value().clone()).collect();
        let roots = build_tree(&flat);
        ActorSnapshot { total: flat.len() as u64, roots, flat }
    }
}

impl SpawnObserver for ActorRegistry {
    fn on_spawn(&self, path: &ActorPath, parent: Option<&ActorPath>, actor_type: &'static str) {
        self.record_spawn(ActorStatus {
            path: path.to_string(),
            parent: parent.map(|p| p.to_string()),
            actor_type: actor_type.to_string(),
            mailbox_depth: 0,
            spawned_at: chrono::Utc::now().to_rfc3339(),
        });
    }

    fn on_stop(&self, path: &ActorPath) {
        self.record_stop(&path.to_string());
    }

    fn on_mailbox_depth(&self, path: &ActorPath, depth: u64) {
        self.record_mailbox_depth(&path.to_string(), depth);
    }
}

fn build_tree(flat: &[ActorStatus]) -> Vec<ActorTreeNode> {
    use std::collections::HashMap;
    let mut children_of: HashMap<String, Vec<&ActorStatus>> = HashMap::new();
    let mut roots: Vec<&ActorStatus> = Vec::new();

    for s in flat {
        match &s.parent {
            Some(p) if flat.iter().any(|x| &x.path == p) => {
                children_of.entry(p.clone()).or_default().push(s);
            }
            _ => roots.push(s),
        }
    }

    fn to_node(
        s: &ActorStatus,
        children_of: &std::collections::HashMap<String, Vec<&ActorStatus>>,
    ) -> ActorTreeNode {
        let name = s.path.rsplit('/').next().unwrap_or(&s.path).to_string();
        let children = children_of
            .get(&s.path)
            .map(|v| v.iter().map(|c| to_node(c, children_of)).collect())
            .unwrap_or_default();
        ActorTreeNode {
            path: s.path.clone(),
            name,
            actor_type: s.actor_type.clone(),
            mailbox_depth: s.mailbox_depth,
            children,
        }
    }

    roots.iter().map(|r| to_node(r, &children_of)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(path: &str, parent: Option<&str>) -> ActorStatus {
        ActorStatus {
            path: path.into(),
            parent: parent.map(|s| s.into()),
            actor_type: "Test".into(),
            mailbox_depth: 0,
            spawned_at: "now".into(),
        }
    }

    #[test]
    fn tracks_spawn_and_stop() {
        let bus = TelemetryBus::new(8);
        let reg = ActorRegistry::new(bus);
        reg.record_spawn(status("/user/a", Some("/user")));
        reg.record_spawn(status("/user/b", Some("/user")));
        assert_eq!(reg.live_count(), 2);
        reg.record_stop("/user/a");
        assert_eq!(reg.live_count(), 1);
        assert_eq!(reg.total_spawned(), 2);
        assert_eq!(reg.total_stopped(), 1);
    }

    #[test]
    fn builds_tree_with_known_parents() {
        let bus = TelemetryBus::new(8);
        let reg = ActorRegistry::new(bus);
        reg.record_spawn(status("/user", None));
        reg.record_spawn(status("/user/a", Some("/user")));
        reg.record_spawn(status("/user/a/aa", Some("/user/a")));
        let snap = reg.snapshot();
        assert_eq!(snap.total, 3);
        assert_eq!(snap.roots.len(), 1);
        assert_eq!(snap.roots[0].children.len(), 1);
        assert_eq!(snap.roots[0].children[0].children.len(), 1);
    }

    #[test]
    fn mailbox_depth_updates_status() {
        let bus = TelemetryBus::new(8);
        let reg = ActorRegistry::new(bus);
        reg.record_spawn(status("/user/a", Some("/user")));
        reg.record_mailbox_depth("/user/a", 42);
        let snap = reg.snapshot();
        assert_eq!(snap.flat[0].mailbox_depth, 42);
    }
}
