//! Owns the shard→region allocation table.
//!
//! Phase 9 of `docs/full-port-plan.md`. The `allocate_with_strategy`
//! and `rebalance_with_strategy` methods plug a
//! [`crate::ShardAllocationStrategy`] in front of the table so the
//! caller doesn't have to hand-pick a region. Persistent
//! event-sourced coordinator + handoff state machine remain Phase 9
//! follow-ons.

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::allocation::ShardAllocationStrategy;

#[derive(Default)]
pub struct ShardCoordinator {
    allocation: RwLock<HashMap<String, String>>,
}

impl ShardCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the region hosting `shard_id`, allocating it to `default_region`
    /// on first mention. Mirrors the "least-shards" allocation of
    /// where the caller supplies candidate regions.
    pub fn allocate(&self, shard_id: &str, default_region: &str) -> String {
        let mut map = self.allocation.write();
        map.entry(shard_id.to_string()).or_insert_with(|| default_region.to_string()).clone()
    }

    /// Allocate `shard_id` using `strategy`, given the current
    /// per-region shard counts. Returns the chosen region; updates
    /// the allocation table.
    pub fn allocate_with_strategy<S: ShardAllocationStrategy>(
        &self,
        shard_id: &str,
        strategy: &S,
    ) -> Option<String> {
        // Snapshot current counts.
        let counts = self.region_shard_counts();
        let chosen = strategy.allocate_shard(shard_id, &counts)?;
        self.allocation.write().insert(shard_id.to_string(), chosen.clone());
        Some(chosen)
    }

    /// Run `strategy.rebalance` on the current table; returns the
    /// shard ids the caller should hand off (the caller picks the
    /// destination via `allocate_with_strategy` after each handoff
    /// completes).
    pub fn rebalance_with_strategy<S: ShardAllocationStrategy>(&self, strategy: &S) -> Vec<String> {
        let allocations = self.allocation.read().clone();
        let counts = region_shard_counts(&allocations);
        strategy.rebalance(&allocations, &counts)
    }

    pub fn region_for(&self, shard_id: &str) -> Option<String> {
        self.allocation.read().get(shard_id).cloned()
    }

    pub fn rebalance(&self, shard_id: &str, to_region: impl Into<String>) {
        self.allocation.write().insert(shard_id.to_string(), to_region.into());
    }

    pub fn shard_count(&self) -> usize {
        self.allocation.read().len()
    }

    /// Snapshot of the full shard → region allocation table. Useful for
    /// telemetry / dashboards.
    pub fn allocations(&self) -> Vec<(String, String)> {
        self.allocation.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Per-region shard counts. Computed from the current allocation
    /// table. Regions with zero shards are omitted; callers that
    /// want to include known-but-empty regions should merge in their
    /// own region list.
    pub fn region_shard_counts(&self) -> HashMap<String, usize> {
        region_shard_counts(&self.allocation.read())
    }
}

fn region_shard_counts(allocations: &HashMap<String, String>) -> HashMap<String, usize> {
    let mut out: HashMap<String, usize> = HashMap::new();
    for region in allocations.values() {
        *out.entry(region.clone()).or_insert(0) += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocation::{LeastShardAllocationStrategy, PinnedAllocationStrategy};

    #[test]
    fn allocate_remembers_first_assignment() {
        let c = ShardCoordinator::new();
        assert_eq!(c.allocate("s1", "r1"), "r1");
        assert_eq!(c.allocate("s1", "r2"), "r1"); // already pinned
    }

    #[test]
    fn allocate_with_strategy_uses_least_loaded() {
        let c = ShardCoordinator::new();
        // Pre-populate so r1 has 2 shards, r2 has 1.
        c.allocate("s1", "r1");
        c.allocate("s2", "r1");
        c.allocate("s3", "r2");
        let s = LeastShardAllocationStrategy::default();
        let r = c.allocate_with_strategy("s4", &s).unwrap();
        assert_eq!(r, "r2");
    }

    #[test]
    fn allocate_with_strategy_no_regions_returns_none() {
        let c = ShardCoordinator::new();
        let s = LeastShardAllocationStrategy::default();
        assert!(c.allocate_with_strategy("s1", &s).is_none());
    }

    #[test]
    fn pinned_strategy_creates_target_region_immediately() {
        let c = ShardCoordinator::new();
        let s = PinnedAllocationStrategy { region: "primary".into() };
        assert_eq!(c.allocate_with_strategy("s1", &s), Some("primary".to_string()));
        assert_eq!(c.region_for("s1"), Some("primary".to_string()));
    }

    #[test]
    fn rebalance_with_strategy_returns_overloaded_shards() {
        let c = ShardCoordinator::new();
        for s in &["s1", "s2", "s3", "s4", "s5"] {
            c.allocate(s, "r1");
        }
        c.allocate("s6", "r2");
        let strat = LeastShardAllocationStrategy { max_simultaneous_rebalance: 2, rebalance_threshold: 2 };
        let to_move = c.rebalance_with_strategy(&strat);
        assert_eq!(to_move.len(), 2);
        for shard in &to_move {
            assert_eq!(c.region_for(shard), Some("r1".to_string()));
        }
    }

    #[test]
    fn region_shard_counts_aggregate_correctly() {
        let c = ShardCoordinator::new();
        c.allocate("s1", "r1");
        c.allocate("s2", "r1");
        c.allocate("s3", "r2");
        let counts = c.region_shard_counts();
        assert_eq!(counts.get("r1"), Some(&2));
        assert_eq!(counts.get("r2"), Some(&1));
    }
}
