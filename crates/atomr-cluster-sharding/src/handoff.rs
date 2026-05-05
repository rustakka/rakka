//! 3-phase shard handoff state machine.
//!
//! Three phases:
//!
//! ```text
//! BeginHandoff(shard) ── source region acks ──► HandingOff
//! HandingOff          ── all entities stopped ─► Stopped
//! Stopped             ── coordinator allocates ─► StartElsewhere(shard, new_region)
//! ```
//!
//! [`HandoffCoordinator`] tracks a per-shard state machine and
//! exposes pure transition helpers; the runtime driver wires it into
//! the shard region.

use std::collections::HashMap;

use parking_lot::RwLock;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandoffState {
    /// No handoff in progress.
    Idle,
    /// Source region has been told to begin draining.
    Beginning { source_region: String },
    /// Entities are stopping; new messages buffer at the source.
    HandingOff { source_region: String, remaining_entities: usize },
    /// All entities stopped; awaiting reassignment.
    Stopped { source_region: String },
    /// Shard re-allocated to `target_region`.
    Started { source_region: String, target_region: String },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandoffError {
    #[error("invalid transition for shard `{0}` (current state does not allow it)")]
    InvalidTransition(String),
}

/// Per-shard handoff state machine.
#[derive(Default)]
pub struct HandoffCoordinator {
    states: RwLock<HashMap<String, HandoffState>>,
}

impl HandoffCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self, shard_id: &str) -> HandoffState {
        self.states.read().get(shard_id).cloned().unwrap_or(HandoffState::Idle)
    }

    /// Phase 1: tell `source_region` to start draining `shard_id`.
    pub fn begin(&self, shard_id: &str, source_region: &str) -> Result<(), HandoffError> {
        let mut g = self.states.write();
        let cur = g.entry(shard_id.into()).or_insert(HandoffState::Idle).clone();
        if !matches!(cur, HandoffState::Idle | HandoffState::Started { .. }) {
            return Err(HandoffError::InvalidTransition(shard_id.into()));
        }
        g.insert(shard_id.into(), HandoffState::Beginning { source_region: source_region.into() });
        Ok(())
    }

    /// Phase 2a: source region has acknowledged the begin and is now
    /// stopping `entity_count` entities.
    pub fn ack_begin(&self, shard_id: &str, entity_count: usize) -> Result<(), HandoffError> {
        let mut g = self.states.write();
        let cur = g.get(shard_id).cloned().unwrap_or(HandoffState::Idle);
        let HandoffState::Beginning { source_region } = cur else {
            return Err(HandoffError::InvalidTransition(shard_id.into()));
        };
        g.insert(
            shard_id.into(),
            HandoffState::HandingOff { source_region, remaining_entities: entity_count },
        );
        Ok(())
    }

    /// Phase 2b: one more entity finished stopping. Auto-transitions
    /// to `Stopped` when the count reaches zero.
    pub fn entity_stopped(&self, shard_id: &str) -> Result<(), HandoffError> {
        let mut g = self.states.write();
        let cur = g.get(shard_id).cloned().unwrap_or(HandoffState::Idle);
        let HandoffState::HandingOff { source_region, remaining_entities } = cur else {
            return Err(HandoffError::InvalidTransition(shard_id.into()));
        };
        let next = if remaining_entities <= 1 {
            HandoffState::Stopped { source_region }
        } else {
            HandoffState::HandingOff { source_region, remaining_entities: remaining_entities - 1 }
        };
        g.insert(shard_id.into(), next);
        Ok(())
    }

    /// Phase 3: coordinator allocated the shard to `target_region`.
    pub fn start_elsewhere(&self, shard_id: &str, target_region: &str) -> Result<(), HandoffError> {
        let mut g = self.states.write();
        let cur = g.get(shard_id).cloned().unwrap_or(HandoffState::Idle);
        let HandoffState::Stopped { source_region } = cur else {
            return Err(HandoffError::InvalidTransition(shard_id.into()));
        };
        g.insert(
            shard_id.into(),
            HandoffState::Started { source_region, target_region: target_region.into() },
        );
        Ok(())
    }

    /// Forget a shard (e.g. it was removed entirely).
    pub fn forget(&self, shard_id: &str) {
        self.states.write().remove(shard_id);
    }

    /// Snapshot for telemetry — `(shard_id, state)` pairs.
    pub fn snapshot(&self) -> Vec<(String, HandoffState)> {
        let mut v: Vec<(String, HandoffState)> =
            self.states.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_three_phase_handoff() {
        let h = HandoffCoordinator::new();
        h.begin("s1", "r1").unwrap();
        assert!(matches!(h.state("s1"), HandoffState::Beginning { .. }));
        h.ack_begin("s1", 3).unwrap();
        h.entity_stopped("s1").unwrap();
        h.entity_stopped("s1").unwrap();
        assert!(matches!(h.state("s1"), HandoffState::HandingOff { remaining_entities: 1, .. }));
        h.entity_stopped("s1").unwrap();
        assert!(matches!(h.state("s1"), HandoffState::Stopped { .. }));
        h.start_elsewhere("s1", "r2").unwrap();
        assert!(matches!(h.state("s1"), HandoffState::Started { .. }));
    }

    #[test]
    fn ack_without_begin_errors() {
        let h = HandoffCoordinator::new();
        let r = h.ack_begin("s1", 5);
        assert!(matches!(r, Err(HandoffError::InvalidTransition(_))));
    }

    #[test]
    fn entity_stopped_without_handing_off_errors() {
        let h = HandoffCoordinator::new();
        let r = h.entity_stopped("s1");
        assert!(matches!(r, Err(HandoffError::InvalidTransition(_))));
    }

    #[test]
    fn start_elsewhere_without_stopped_errors() {
        let h = HandoffCoordinator::new();
        let r = h.start_elsewhere("s1", "r2");
        assert!(matches!(r, Err(HandoffError::InvalidTransition(_))));
    }

    #[test]
    fn re_handoff_after_started_is_allowed() {
        let h = HandoffCoordinator::new();
        h.begin("s1", "r1").unwrap();
        h.ack_begin("s1", 1).unwrap();
        h.entity_stopped("s1").unwrap();
        h.start_elsewhere("s1", "r2").unwrap();
        // Now start a new handoff cycle — `r2 → r3`.
        h.begin("s1", "r2").unwrap();
        assert!(matches!(h.state("s1"), HandoffState::Beginning { .. }));
    }

    #[test]
    fn forget_drops_state() {
        let h = HandoffCoordinator::new();
        h.begin("s1", "r1").unwrap();
        h.forget("s1");
        assert!(matches!(h.state("s1"), HandoffState::Idle));
    }
}
