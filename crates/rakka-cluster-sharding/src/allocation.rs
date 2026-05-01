//! Shard allocation strategies.
//!
//! Phase 9 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Cluster.Sharding.IShardAllocationStrategy` and its
//! built-in implementations.
//!
//! A strategy answers two questions:
//!
//! 1. **Where to place a new shard?** [`ShardAllocationStrategy::
//!    allocate_shard`] picks one of the currently-known regions to
//!    host a freshly-requested shard.
//! 2. **What to rebalance?** [`ShardAllocationStrategy::rebalance`]
//!    surfaces a list of shard ids that should migrate, given the
//!    current allocation table and the per-region shard counts.

use std::collections::HashMap;

/// Pluggable shard allocation policy.
pub trait ShardAllocationStrategy: Send + Sync + 'static {
    /// Pick a region to host `shard_id`. `regions` lists known
    /// candidates (region path → current shard count). Returns the
    /// chosen region's path, or `None` if `regions` is empty.
    fn allocate_shard(
        &self,
        shard_id: &str,
        regions: &HashMap<String, usize>,
    ) -> Option<String>;

    /// Decide which shards should migrate. `current_allocations` is
    /// the shard → region mapping; `regions` lists region shard
    /// counts. Returns shard ids to hand off (the coordinator picks
    /// the destination).
    fn rebalance(
        &self,
        current_allocations: &HashMap<String, String>,
        regions: &HashMap<String, usize>,
    ) -> Vec<String>;
}

/// Place new shards on the region with the fewest shards, breaking
/// ties lexicographically. Rebalances if the difference between most-
/// and least-loaded regions exceeds `rebalance_threshold`.
pub struct LeastShardAllocationStrategy {
    /// Migrate at most this many shards per rebalance call.
    pub max_simultaneous_rebalance: usize,
    /// Rebalance only if `max_count - min_count >= rebalance_threshold`.
    pub rebalance_threshold: usize,
}

impl Default for LeastShardAllocationStrategy {
    fn default() -> Self {
        Self {
            max_simultaneous_rebalance: 3,
            rebalance_threshold: 1,
        }
    }
}

impl ShardAllocationStrategy for LeastShardAllocationStrategy {
    fn allocate_shard(
        &self,
        _shard_id: &str,
        regions: &HashMap<String, usize>,
    ) -> Option<String> {
        let mut entries: Vec<(&String, &usize)> = regions.iter().collect();
        entries.sort_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)));
        entries.first().map(|(k, _)| (*k).clone())
    }

    fn rebalance(
        &self,
        current: &HashMap<String, String>,
        regions: &HashMap<String, usize>,
    ) -> Vec<String> {
        if regions.len() < 2 {
            return Vec::new();
        }
        let max = regions.values().max().copied().unwrap_or(0);
        let min = regions.values().min().copied().unwrap_or(0);
        if max.saturating_sub(min) < self.rebalance_threshold {
            return Vec::new();
        }
        // Pick shard ids that live on the most-loaded region(s).
        let mut max_regions: Vec<&String> = regions
            .iter()
            .filter(|(_, c)| **c == max)
            .map(|(k, _)| k)
            .collect();
        max_regions.sort();
        let mut out: Vec<String> = current
            .iter()
            .filter(|(_, region)| max_regions.iter().any(|r| **r == **region))
            .map(|(shard, _)| shard.clone())
            .collect();
        out.sort();
        out.truncate(self.max_simultaneous_rebalance);
        out
    }
}

/// Pin every shard to a specific region (useful for tests / static
/// allocation). akka.net analog: a custom strategy returning a
/// constant region.
pub struct PinnedAllocationStrategy {
    pub region: String,
}

impl ShardAllocationStrategy for PinnedAllocationStrategy {
    fn allocate_shard(
        &self,
        _shard_id: &str,
        _regions: &HashMap<String, usize>,
    ) -> Option<String> {
        Some(self.region.clone())
    }

    fn rebalance(
        &self,
        _current: &HashMap<String, String>,
        _regions: &HashMap<String, usize>,
    ) -> Vec<String> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regions(pairs: &[(&str, usize)]) -> HashMap<String, usize> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn allocs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn least_shard_picks_emptiest_region() {
        let s = LeastShardAllocationStrategy::default();
        let r = regions(&[("r1", 5), ("r2", 1), ("r3", 3)]);
        assert_eq!(s.allocate_shard("x", &r), Some("r2".into()));
    }

    #[test]
    fn least_shard_picks_no_region_when_empty() {
        let s = LeastShardAllocationStrategy::default();
        let r = regions(&[]);
        assert!(s.allocate_shard("x", &r).is_none());
    }

    #[test]
    fn least_shard_breaks_ties_lexicographically() {
        let s = LeastShardAllocationStrategy::default();
        let r = regions(&[("r2", 1), ("r1", 1)]);
        assert_eq!(s.allocate_shard("x", &r), Some("r1".into()));
    }

    #[test]
    fn rebalance_returns_empty_when_balanced() {
        let s = LeastShardAllocationStrategy::default();
        let r = regions(&[("r1", 3), ("r2", 3)]);
        let a = allocs(&[]);
        assert!(s.rebalance(&a, &r).is_empty());
    }

    #[test]
    fn rebalance_returns_shards_from_loaded_region() {
        let s = LeastShardAllocationStrategy {
            max_simultaneous_rebalance: 2,
            rebalance_threshold: 2,
        };
        let r = regions(&[("r1", 5), ("r2", 1)]);
        let a = allocs(&[
            ("s1", "r1"), ("s2", "r1"), ("s3", "r1"), ("s4", "r1"), ("s5", "r1"),
            ("s6", "r2"),
        ]);
        let out = s.rebalance(&a, &r);
        assert_eq!(out.len(), 2);
        for shard in &out {
            assert_eq!(a.get(shard), Some(&"r1".to_string()));
        }
    }

    #[test]
    fn pinned_always_picks_same_region() {
        let s = PinnedAllocationStrategy { region: "fixed".into() };
        let r = regions(&[("r1", 0), ("r2", 0)]);
        assert_eq!(s.allocate_shard("a", &r), Some("fixed".into()));
        assert_eq!(s.allocate_shard("b", &r), Some("fixed".into()));
    }
}
