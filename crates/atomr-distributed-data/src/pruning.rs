//! Pruning state for replicated CRDTs.
//!
//! When a cluster member is permanently removed, its contributions to
//! a CRDT (vector entries, set elements added by it, etc.) must be
//! transferred to a still-alive "seen-by" node so causal ordering
//! is preserved. The transfer is two-phase:
//!
//! 1. **Initialized**: a still-alive `owner` is chosen to take over
//!    the removed node's contributions. The pruning is announced to
//!    every replica.
//! 2. **Performed**: every replica has applied the pruning. The
//!    pruning marker can then be garbage-collected once the
//!    `obsolete_at` round has fully propagated.
//!
//! This module ships the type and the state-machine helpers; the
//! actual rewriting of CRDT internals happens in the per-CRDT
//! `prune` implementation (added per-CRDT as needed).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Per-(removed-node, owner) pruning state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PruningPhase {
    /// Pruning announced; not yet observed by every replica.
    Initialized { owner: String },
    /// Pruning observed by every replica. The marker can be
    /// garbage-collected after `obsolete_at` rounds have elapsed
    /// since `Performed` was set.
    Performed { owner: String, obsolete_at: u64 },
}

/// State carried alongside a CRDT entry — maps each *removed* node
/// to its pruning phase. Per, the map's keys
/// are the addresses that have left the cluster.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruningState {
    pub markers: BTreeMap<String, PruningPhase>,
}

impl PruningState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Announce that `removed_node` is being pruned, with `owner`
    /// taking over. No-op if a marker for `removed_node` already
    /// exists in any phase.
    pub fn initialize(&mut self, removed_node: String, owner: String) {
        self.markers.entry(removed_node).or_insert(PruningPhase::Initialized { owner });
    }

    /// Mark that pruning of `removed_node` has been observed
    /// everywhere; the marker can be aged out at `obsolete_at`.
    /// Returns `true` if the phase advanced from Initialized →
    /// Performed.
    pub fn mark_performed(&mut self, removed_node: &str, obsolete_at: u64) -> bool {
        match self.markers.get_mut(removed_node) {
            Some(PruningPhase::Initialized { owner }) => {
                let owner = std::mem::take(owner);
                self.markers.insert(removed_node.to_string(), PruningPhase::Performed { owner, obsolete_at });
                true
            }
            _ => false,
        }
    }

    /// True if `removed_node` is currently being pruned.
    pub fn is_pruned(&self, removed_node: &str) -> bool {
        self.markers.contains_key(removed_node)
    }

    /// Return the owner that has taken over `removed_node`, if any.
    pub fn owner(&self, removed_node: &str) -> Option<&str> {
        match self.markers.get(removed_node)? {
            PruningPhase::Initialized { owner } | PruningPhase::Performed { owner, .. } => Some(owner),
        }
    }

    /// Discard pruning markers whose `obsolete_at` is ≤ `current_round`.
    /// Returns the number of markers removed.
    pub fn gc(&mut self, current_round: u64) -> usize {
        let before = self.markers.len();
        self.markers.retain(|_, phase| match phase {
            PruningPhase::Initialized { .. } => true,
            PruningPhase::Performed { obsolete_at, .. } => *obsolete_at > current_round,
        });
        before - self.markers.len()
    }

    /// Merge `other` into self. Performed wins over Initialized when
    /// the same node is referenced; latest `obsolete_at` wins among
    /// two Performed entries.
    pub fn merge(&mut self, other: &Self) {
        for (k, v) in &other.markers {
            match (self.markers.get(k), v) {
                (None, _) => {
                    self.markers.insert(k.clone(), v.clone());
                }
                (Some(PruningPhase::Initialized { .. }), PruningPhase::Performed { .. }) => {
                    self.markers.insert(k.clone(), v.clone());
                }
                (
                    Some(PruningPhase::Performed { obsolete_at: lhs, .. }),
                    PruningPhase::Performed { obsolete_at: rhs, .. },
                ) if rhs > lhs => {
                    self.markers.insert(k.clone(), v.clone());
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_records_owner() {
        let mut p = PruningState::new();
        p.initialize("dead".into(), "alive".into());
        assert!(p.is_pruned("dead"));
        assert_eq!(p.owner("dead"), Some("alive"));
    }

    #[test]
    fn double_initialize_is_idempotent() {
        let mut p = PruningState::new();
        p.initialize("dead".into(), "alive1".into());
        p.initialize("dead".into(), "alive2".into());
        // Owner does not change once recorded.
        assert_eq!(p.owner("dead"), Some("alive1"));
    }

    #[test]
    fn perform_advances_phase() {
        let mut p = PruningState::new();
        p.initialize("dead".into(), "alive".into());
        assert!(p.mark_performed("dead", 100));
        // Second attempt is a no-op (already Performed).
        assert!(!p.mark_performed("dead", 200));
    }

    #[test]
    fn gc_drops_obsolete_markers() {
        let mut p = PruningState::new();
        p.initialize("dead".into(), "alive".into());
        p.mark_performed("dead", 5);
        let removed = p.gc(10);
        assert_eq!(removed, 1);
        assert!(!p.is_pruned("dead"));
    }

    #[test]
    fn gc_keeps_initialized_markers() {
        let mut p = PruningState::new();
        p.initialize("dead".into(), "alive".into());
        let removed = p.gc(10_000);
        assert_eq!(removed, 0);
        assert!(p.is_pruned("dead"));
    }

    #[test]
    fn merge_promotes_initialized_to_performed() {
        let mut a = PruningState::new();
        a.initialize("dead".into(), "alive".into());

        let mut b = PruningState::new();
        b.initialize("dead".into(), "alive".into());
        b.mark_performed("dead", 50);

        a.merge(&b);
        assert!(matches!(a.markers["dead"], PruningPhase::Performed { obsolete_at: 50, .. }));
    }

    #[test]
    fn merge_picks_latest_obsolete_at() {
        let mut a = PruningState::new();
        a.initialize("dead".into(), "alive".into());
        a.mark_performed("dead", 10);

        let mut b = PruningState::new();
        b.initialize("dead".into(), "alive".into());
        b.mark_performed("dead", 50);

        a.merge(&b);
        assert!(matches!(a.markers["dead"], PruningPhase::Performed { obsolete_at: 50, .. }));
    }
}

// -- WriteAggregator / ReadAggregator -------------------------------

/// Counts acks against a target derived from a [`crate::WriteConsistency`]
/// and `cluster_size`.
#[derive(Debug)]
pub struct WriteAggregator {
    target: usize,
    received: usize,
    nacks: usize,
}

impl WriteAggregator {
    pub fn new(target: usize) -> Self {
        Self { target: target.max(1), received: 0, nacks: 0 }
    }

    pub fn ack(&mut self) {
        self.received += 1;
    }

    pub fn nack(&mut self) {
        self.nacks += 1;
    }

    /// True when enough positive acks have arrived.
    pub fn is_satisfied(&self) -> bool {
        self.received >= self.target
    }

    /// True when so many negative acks have arrived that the target
    /// can no longer be reached.
    pub fn is_failed(&self, cluster_size: usize) -> bool {
        self.nacks > cluster_size.saturating_sub(self.target)
    }

    pub fn received(&self) -> usize {
        self.received
    }

    pub fn target(&self) -> usize {
        self.target
    }
}

/// Counts replies against a target derived from a [`crate::ReadConsistency`]
/// and `cluster_size`. Identical shape to
/// `WriteAggregator` but distinct so call sites cannot mix them up.
#[derive(Debug)]
pub struct ReadAggregator {
    target: usize,
    received: usize,
}

impl ReadAggregator {
    pub fn new(target: usize) -> Self {
        Self { target: target.max(1), received: 0 }
    }

    pub fn reply(&mut self) {
        self.received += 1;
    }

    pub fn is_satisfied(&self) -> bool {
        self.received >= self.target
    }

    pub fn target(&self) -> usize {
        self.target
    }
}

#[cfg(test)]
mod aggregator_tests {
    use super::*;

    #[test]
    fn write_satisfied_after_target_acks() {
        let mut a = WriteAggregator::new(3);
        a.ack();
        a.ack();
        assert!(!a.is_satisfied());
        a.ack();
        assert!(a.is_satisfied());
    }

    #[test]
    fn write_fails_when_too_many_nacks() {
        let mut a = WriteAggregator::new(3);
        // cluster_size=4, target=3 → can tolerate 1 nack; 2 fails.
        a.nack();
        assert!(!a.is_failed(4));
        a.nack();
        assert!(a.is_failed(4));
    }

    #[test]
    fn read_satisfied_after_target_replies() {
        let mut a = ReadAggregator::new(2);
        a.reply();
        assert!(!a.is_satisfied());
        a.reply();
        assert!(a.is_satisfied());
    }
}
