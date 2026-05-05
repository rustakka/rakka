//! Cluster-sharding spec parity. akka.net:
//! `LeastShardAllocationStrategySpec`,
//! `LeastShardAllocationStrategyRandomizedSpec`,
//! `ShardRegionSpec` (handoff invariants).

use std::collections::HashMap;

use atomr_cluster_sharding::{
    HandoffCoordinator, HandoffState, LeastShardAllocationStrategy, ShardAllocationStrategy,
};

fn regions(pairs: &[(&str, usize)]) -> HashMap<String, usize> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

fn current_allocs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn least_shard_picks_emptiest_region_with_lex_tie_break() {
    let s = LeastShardAllocationStrategy::default();
    let r = regions(&[("r1", 5), ("r2", 1), ("r3", 1), ("r4", 3)]);
    assert_eq!(s.allocate_shard("any", &r), Some("r2".into()));
}

#[test]
fn least_shard_returns_none_for_no_regions() {
    let s = LeastShardAllocationStrategy::default();
    let r: HashMap<String, usize> = HashMap::new();
    assert!(s.allocate_shard("any", &r).is_none());
}

#[test]
fn rebalance_below_threshold_yields_empty() {
    let s = LeastShardAllocationStrategy { rebalance_threshold: 5, max_simultaneous_rebalance: 10 };
    let r = regions(&[("r1", 4), ("r2", 1)]);
    let cur = current_allocs(&[("s1", "r1"), ("s2", "r1")]);
    assert!(s.rebalance(&cur, &r).is_empty(), "diff 3 < threshold 5");
}

#[test]
fn rebalance_at_threshold_picks_shards_from_max_region() {
    let s = LeastShardAllocationStrategy { rebalance_threshold: 2, max_simultaneous_rebalance: 10 };
    let r = regions(&[("r1", 4), ("r2", 1)]);
    let cur = current_allocs(&[("s1", "r1"), ("s2", "r1"), ("s3", "r1"), ("s4", "r1"), ("s5", "r2")]);
    let chosen = s.rebalance(&cur, &r);
    assert_eq!(chosen.len(), 4, "all r1 shards available for migration");
    for shard in &chosen {
        assert_eq!(cur[shard], "r1");
    }
}

#[test]
fn rebalance_caps_at_max_simultaneous() {
    let s = LeastShardAllocationStrategy { rebalance_threshold: 1, max_simultaneous_rebalance: 2 };
    let r = regions(&[("r1", 5), ("r2", 0)]);
    let cur =
        current_allocs(&[("s1", "r1"), ("s2", "r1"), ("s3", "r1"), ("s4", "r1"), ("s5", "r1")]);
    assert_eq!(s.rebalance(&cur, &r).len(), 2);
}

#[test]
fn rebalance_with_one_region_is_noop() {
    let s = LeastShardAllocationStrategy::default();
    let r = regions(&[("r1", 99)]);
    let cur = current_allocs(&[("s", "r1")]);
    assert!(s.rebalance(&cur, &r).is_empty());
}

// -- Handoff state machine ------------------------------------------

#[test]
fn handoff_lifecycle_progresses_through_phases() {
    let h = HandoffCoordinator::new();
    assert!(matches!(h.state("S"), HandoffState::Idle));
    h.begin("S", "src").unwrap();
    assert!(matches!(h.state("S"), HandoffState::Beginning { .. }));
}

#[test]
fn handoff_begin_twice_errors() {
    let h = HandoffCoordinator::new();
    h.begin("S", "src").unwrap();
    let again = h.begin("S", "src");
    assert!(again.is_err(), "second begin without progress should be rejected");
}

#[test]
fn handoff_state_per_shard_isolated() {
    let h = HandoffCoordinator::new();
    h.begin("A", "r1").unwrap();
    assert!(matches!(h.state("B"), HandoffState::Idle));
    h.begin("B", "r2").unwrap();
    assert!(matches!(h.state("A"), HandoffState::Beginning { .. }));
    assert!(matches!(h.state("B"), HandoffState::Beginning { .. }));
}
