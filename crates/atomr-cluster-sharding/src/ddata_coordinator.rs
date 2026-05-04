//! `DDataShardCoordinator` — DistributedData-backed allocation table.
//!
//! Phase 9.E of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Cluster.Sharding.DDataShardCoordinator`. The persistent
//! variant (Phase 9.D) journals every allocation; the DData variant
//! stores the table as a CRDT in `atomr-distributed-data` so the
//! coordinator state converges across the cluster without an
//! event-sourced log.
//!
//! The CRDT used here is `LWWMap<String, String>` (`shard_id →
//! region`), which mirrors akka.net's choice. Concurrent writes are
//! resolved by timestamp; the higher timestamp wins. The
//! coordinator is responsible for using monotonic timestamps so that
//! it doesn't accidentally lose a real allocation to a stale write.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use atomr_distributed_data::{CrdtMerge, LWWMap};

/// DData-backed allocation coordinator.
pub struct DDataShardCoordinator {
    /// LWW map of shard_id → region.
    state: RwLock<LWWMap<String, String>>,
    /// Strictly-monotonic local clock so concurrent local writes
    /// produce distinct timestamps.
    next_ts: AtomicU64,
}

impl Default for DDataShardCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl DDataShardCoordinator {
    pub fn new() -> Self {
        let bootstrap =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1);
        Self { state: RwLock::new(LWWMap::new()), next_ts: AtomicU64::new(bootstrap) }
    }

    /// Issue a fresh, strictly-increasing timestamp for the next
    /// allocation. Wall-clock skew is bounded by the atomic counter.
    fn tick(&self) -> u128 {
        self.next_ts.fetch_add(1, Ordering::Relaxed) as u128
    }

    /// Allocate `shard_id` to `region`, overwriting any older
    /// allocation.
    pub fn allocate(&self, shard_id: impl Into<String>, region: impl Into<String>) {
        let ts = self.tick();
        self.state.write().put(shard_id.into(), region.into(), ts);
    }

    /// Look up the region currently hosting `shard_id`.
    pub fn region_for(&self, shard_id: &str) -> Option<String> {
        self.state.read().get(&shard_id.to_string()).cloned()
    }

    /// Number of distinct shards currently allocated.
    pub fn shard_count(&self) -> usize {
        self.state.read().iter().count()
    }

    /// Snapshot of the full allocation table.
    pub fn allocations(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> =
            self.state.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Merge a remote DData snapshot in. Used by the gossip layer.
    pub fn merge_remote(&self, remote: &LWWMap<String, String>) {
        self.state.write().merge(remote);
    }

    /// Take a snapshot suitable for gossiping to peers.
    pub fn snapshot(&self) -> LWWMap<String, String> {
        self.state.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_and_lookup() {
        let c = DDataShardCoordinator::new();
        c.allocate("s1", "r1");
        c.allocate("s2", "r2");
        assert_eq!(c.region_for("s1"), Some("r1".into()));
        assert_eq!(c.region_for("s2"), Some("r2".into()));
        assert_eq!(c.shard_count(), 2);
    }

    #[test]
    fn later_allocate_overwrites_earlier() {
        let c = DDataShardCoordinator::new();
        c.allocate("s1", "r1");
        c.allocate("s1", "r2");
        assert_eq!(c.region_for("s1"), Some("r2".into()));
    }

    #[test]
    fn merge_remote_takes_higher_timestamp() {
        let local = DDataShardCoordinator::new();
        local.allocate("s1", "r1");

        // Build a remote snapshot with a higher timestamp.
        let mut remote = LWWMap::new();
        remote.put("s1".to_string(), "r-remote".to_string(), u128::MAX);
        local.merge_remote(&remote);
        assert_eq!(local.region_for("s1"), Some("r-remote".into()));
    }

    #[test]
    fn merge_remote_keeps_local_when_local_is_newer() {
        let local = DDataShardCoordinator::new();
        local.allocate("s1", "r-local"); // gets a fresh ts

        // Remote has an older write.
        let mut remote = LWWMap::new();
        remote.put("s1".to_string(), "r-stale".to_string(), 1);
        local.merge_remote(&remote);
        assert_eq!(local.region_for("s1"), Some("r-local".into()));
    }

    #[test]
    fn allocations_sorted_for_telemetry() {
        let c = DDataShardCoordinator::new();
        c.allocate("zebra", "r2");
        c.allocate("alpha", "r1");
        c.allocate("middle", "r3");
        let snap = c.allocations();
        assert_eq!(
            snap,
            vec![
                ("alpha".into(), "r1".into()),
                ("middle".into(), "r3".into()),
                ("zebra".into(), "r2".into()),
            ]
        );
    }

    #[test]
    fn snapshot_is_independent_of_subsequent_writes() {
        let c = DDataShardCoordinator::new();
        c.allocate("s1", "r1");
        let snap = c.snapshot();
        c.allocate("s1", "r2"); // changes local
                                // Snapshot still reflects the earlier state.
        assert_eq!(snap.get(&"s1".to_string()), Some(&"r1".to_string()));
    }

    /// Replicating Akka.NET's "DData coordinator joins late and
    /// converges to the cluster's view" property: empty local +
    /// merge-in a populated remote should adopt every allocation.
    #[test]
    fn empty_coordinator_adopts_remote_state() {
        let local = DDataShardCoordinator::new();
        let mut remote = LWWMap::new();
        remote.put("s1".to_string(), "rA".to_string(), 100);
        remote.put("s2".to_string(), "rB".to_string(), 100);
        local.merge_remote(&remote);
        assert_eq!(local.region_for("s1"), Some("rA".into()));
        assert_eq!(local.region_for("s2"), Some("rB".into()));
    }
}
