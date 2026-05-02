//! Rebalance algorithm runner.
//!
//! Phase 9.F of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Cluster.Sharding.ShardCoordinator.Rebalance` — runs the
//! configured [`crate::ShardAllocationStrategy`] every `tick`, picks
//! shards to move, and drives them through the
//! [`crate::HandoffCoordinator`] state machine in lock-step with the
//! [`crate::ShardCoordinator`].
//!
//! This runner is *pure scheduling*: it does not own any actors. The
//! caller drives it with `step()` and gets back a list of
//! [`RebalanceAction`]s to execute.

use crate::allocation::ShardAllocationStrategy;
use crate::coordinator::ShardCoordinator;
use crate::handoff::{HandoffCoordinator, HandoffState};

/// Action emitted by `RebalanceRunner::step`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RebalanceAction {
    /// Tell the source region to begin draining `shard_id`.
    BeginHandoff { shard_id: String, source_region: String },
    /// Allocate `shard_id` to `target_region` (handoff complete).
    Allocate { shard_id: String, target_region: String },
}

pub struct RebalanceRunner<'a, S: ShardAllocationStrategy> {
    coordinator: &'a ShardCoordinator,
    handoff: &'a HandoffCoordinator,
    strategy: &'a S,
}

impl<'a, S: ShardAllocationStrategy> RebalanceRunner<'a, S> {
    pub fn new(coordinator: &'a ShardCoordinator, handoff: &'a HandoffCoordinator, strategy: &'a S) -> Self {
        Self { coordinator, handoff, strategy }
    }

    /// One scheduling tick. Returns:
    /// 1. `BeginHandoff` for any newly-rebalanceable shard whose
    ///    handoff state is `Idle` or `Started`.
    /// 2. `Allocate` for any shard whose handoff state is `Stopped`
    ///    (the source region drained successfully — pick a new
    ///    target via the strategy).
    pub fn step(&self) -> Vec<RebalanceAction> {
        let mut actions = Vec::new();

        // (1) Promote `Stopped` shards into `Allocate(target)`.
        for (shard_id, state) in self.handoff.snapshot() {
            if matches!(state, HandoffState::Stopped { .. }) {
                let counts = self.coordinator.region_shard_counts();
                if let Some(target) = self.strategy.allocate_shard(&shard_id, &counts) {
                    actions.push(RebalanceAction::Allocate {
                        shard_id: shard_id.clone(),
                        target_region: target,
                    });
                }
            }
        }

        // (2) Pick fresh shards to begin handing off.
        for shard_id in self.coordinator.rebalance_with_strategy(self.strategy) {
            let cur_state = self.handoff.state(&shard_id);
            if !matches!(cur_state, HandoffState::Idle | HandoffState::Started { .. }) {
                continue; // already in flight
            }
            let Some(source_region) = self.coordinator.region_for(&shard_id) else {
                continue;
            };
            actions.push(RebalanceAction::BeginHandoff { shard_id, source_region });
        }

        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LeastShardAllocationStrategy, ShardCoordinator};

    #[test]
    fn step_emits_begin_handoff_for_overloaded_region() {
        let coord = ShardCoordinator::new();
        for s in &["s1", "s2", "s3", "s4", "s5"] {
            coord.allocate(s, "r1");
        }
        coord.allocate("s6", "r2");
        let handoff = HandoffCoordinator::new();
        let strat = LeastShardAllocationStrategy { max_simultaneous_rebalance: 2, rebalance_threshold: 2 };
        let runner = RebalanceRunner::new(&coord, &handoff, &strat);
        let actions = runner.step();
        // 2 shards from r1 should be flagged to begin handoff.
        let begins: Vec<_> =
            actions.iter().filter(|a| matches!(a, RebalanceAction::BeginHandoff { .. })).collect();
        assert_eq!(begins.len(), 2);
        for a in begins {
            if let RebalanceAction::BeginHandoff { source_region, .. } = a {
                assert_eq!(source_region, "r1");
            }
        }
    }

    #[test]
    fn step_does_not_double_begin_in_flight_shards() {
        let coord = ShardCoordinator::new();
        for s in &["s1", "s2", "s3", "s4", "s5"] {
            coord.allocate(s, "r1");
        }
        coord.allocate("s6", "r2");
        let handoff = HandoffCoordinator::new();
        let strat = LeastShardAllocationStrategy { max_simultaneous_rebalance: 2, rebalance_threshold: 2 };
        let runner = RebalanceRunner::new(&coord, &handoff, &strat);
        // First tick begins handoff for two shards.
        let first = runner.step();
        assert_eq!(first.len(), 2);
        // Apply Beginning state in handoff so subsequent ticks see them as in-flight.
        for a in &first {
            if let RebalanceAction::BeginHandoff { shard_id, source_region } = a {
                handoff.begin(shard_id, source_region).unwrap();
            }
        }
        // Second tick should NOT re-emit them.
        let second = runner.step();
        assert_eq!(second.iter().filter(|a| matches!(a, RebalanceAction::BeginHandoff { .. })).count(), 0);
    }

    #[test]
    fn stopped_shard_gets_allocate_action() {
        let coord = ShardCoordinator::new();
        coord.allocate("s1", "r1");
        coord.allocate("s2", "r2");
        let handoff = HandoffCoordinator::new();
        // Drive s1 to Stopped manually.
        handoff.begin("s1", "r1").unwrap();
        handoff.ack_begin("s1", 0).unwrap();
        handoff.entity_stopped("s1").ok(); // count was 0 → already Stopped
                                           // After ack_begin(0), state is HandingOff{remaining:0}; force to Stopped.
        if let HandoffState::HandingOff { source_region, .. } = handoff.state("s1") {
            // entity_stopped at 0 won't fire; simulate by re-driving via
            // a 1-entity round so we exercise the runner predicate.
            handoff.forget("s1");
            handoff.begin("s1", &source_region).unwrap();
            handoff.ack_begin("s1", 1).unwrap();
            handoff.entity_stopped("s1").unwrap();
        }
        assert!(matches!(handoff.state("s1"), HandoffState::Stopped { .. }));
        let strat = LeastShardAllocationStrategy::default();
        let runner = RebalanceRunner::new(&coord, &handoff, &strat);
        let actions = runner.step();
        let allocates: Vec<_> =
            actions.iter().filter(|a| matches!(a, RebalanceAction::Allocate { .. })).collect();
        assert_eq!(allocates.len(), 1);
    }
}
