//! Passivation — stop idle entities to bound memory.
//!
//! Phase 9 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Cluster.Sharding.Shard` passivation logic with
//! `entity_recovery_strategy` knobs.
//!
//! [`PassivationTracker`] holds a `last_seen` timestamp per entity
//! and exposes [`PassivationTracker::idle_since`] to surface entities
//! that haven't received traffic past the configured TTL. The shard
//! actor calls `record_activity(entity_id)` on every inbound message
//! and runs a periodic sweep that calls `idle_since(threshold)` to
//! decide what to passivate. Passivation itself (sending the
//! configured stop message + buffering replies) is the shard's
//! responsibility.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

#[derive(Default)]
pub struct PassivationTracker {
    /// `entity_id → last_active`.
    inner: RwLock<HashMap<String, Instant>>,
}

impl PassivationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump the activity timestamp for `entity_id`.
    pub fn record_activity(&self, entity_id: impl Into<String>) {
        self.inner.write().insert(entity_id.into(), Instant::now());
    }

    /// Drop the entry for `entity_id` (call when the entity actually
    /// stops, so it doesn't loiter as a stale "idle since forever").
    pub fn drop_entity(&self, entity_id: &str) {
        self.inner.write().remove(entity_id);
    }

    /// Entity ids whose `last_seen` age exceeds `idle_for`.
    pub fn idle_since(&self, idle_for: Duration) -> Vec<String> {
        let g = self.inner.read();
        let now = Instant::now();
        g.iter()
            .filter_map(|(id, t)| if now.duration_since(*t) >= idle_for { Some(id.clone()) } else { None })
            .collect()
    }

    pub fn entity_count(&self) -> usize {
        self.inner.read().len()
    }

    /// Snapshot for telemetry.
    pub fn snapshot(&self) -> Vec<(String, Duration)> {
        let g = self.inner.read();
        let now = Instant::now();
        g.iter().map(|(id, t)| (id.clone(), now.duration_since(*t))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn freshly_active_is_not_idle() {
        let p = PassivationTracker::new();
        p.record_activity("e1");
        assert!(p.idle_since(Duration::from_millis(50)).is_empty());
    }

    #[test]
    fn old_entries_show_up_as_idle() {
        let p = PassivationTracker::new();
        p.record_activity("e1");
        sleep(Duration::from_millis(30));
        let idle = p.idle_since(Duration::from_millis(20));
        assert_eq!(idle, vec!["e1"]);
    }

    #[test]
    fn drop_entity_removes_from_tracker() {
        let p = PassivationTracker::new();
        p.record_activity("e1");
        p.record_activity("e2");
        assert_eq!(p.entity_count(), 2);
        p.drop_entity("e1");
        assert_eq!(p.entity_count(), 1);
    }

    #[test]
    fn record_activity_resets_idle_clock() {
        let p = PassivationTracker::new();
        p.record_activity("e1");
        sleep(Duration::from_millis(30));
        p.record_activity("e1"); // refresh
        assert!(p.idle_since(Duration::from_millis(20)).is_empty());
    }

    #[test]
    fn snapshot_returns_per_entity_age() {
        let p = PassivationTracker::new();
        p.record_activity("e1");
        sleep(Duration::from_millis(10));
        let snap = p.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap[0].1 >= Duration::from_millis(5));
    }
}
