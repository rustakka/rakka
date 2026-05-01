//! `PersistentShardCoordinator` — event-sourced allocation table.
//!
//! Phase 9.D of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Cluster.Sharding.PersistentShardCoordinator`. Wraps a
//! [`ShardCoordinator`] and persists every allocation / rebalance
//! decision through `rakka_persistence::Eventsourced` so a
//! coordinator restart on a different node restores the exact same
//! allocation table.
//!
//! Events:
//! ```text
//! ShardAllocated  { shard_id, region }
//! ShardRebalanced { shard_id, from_region, to_region }
//! ShardRemoved    { shard_id }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use rakka_persistence::{Eventsourced, EventsourcedError, Journal, RecoveryPermitter};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::coordinator::ShardCoordinator;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CoordinatorEvent {
    ShardAllocated { shard_id: String, region: String },
    ShardRebalanced { shard_id: String, from_region: String, to_region: String },
    ShardRemoved { shard_id: String },
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CoordinatorCommand {
    Allocate { shard_id: String, region: String },
    Rebalance { shard_id: String, to_region: String },
    Remove { shard_id: String },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CoordinatorError {
    #[error("shard `{0}` is unknown")]
    UnknownShard(String),
}

/// Eventsourced coordinator state — kept separate from
/// [`ShardCoordinator`] so callers can rebuild it from journal
/// replay. The in-memory `ShardCoordinator` is the local
/// projection; this struct mirrors it through the persistence layer.
#[derive(Default, Debug, Clone)]
pub struct CoordinatorState {
    pub allocations: std::collections::HashMap<String, String>,
}

/// Wraps a [`ShardCoordinator`] with `Eventsourced` plumbing. Use
/// `recover` on boot, then `command` for every allocation /
/// rebalance / removal.
pub struct PersistentShardCoordinator {
    persistence_id: String,
}

impl PersistentShardCoordinator {
    pub fn new(persistence_id: impl Into<String>) -> Self {
        Self { persistence_id: persistence_id.into() }
    }
}

#[async_trait]
impl Eventsourced for PersistentShardCoordinator {
    type Command = CoordinatorCommand;
    type Event = CoordinatorEvent;
    type State = CoordinatorState;
    type Error = CoordinatorError;

    fn persistence_id(&self) -> String {
        self.persistence_id.clone()
    }

    fn command_to_events(
        &self,
        state: &Self::State,
        cmd: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match cmd {
            CoordinatorCommand::Allocate { shard_id, region } => {
                Ok(vec![CoordinatorEvent::ShardAllocated { shard_id, region }])
            }
            CoordinatorCommand::Rebalance { shard_id, to_region } => {
                let Some(from) = state.allocations.get(&shard_id).cloned() else {
                    return Err(CoordinatorError::UnknownShard(shard_id));
                };
                Ok(vec![CoordinatorEvent::ShardRebalanced {
                    shard_id,
                    from_region: from,
                    to_region,
                }])
            }
            CoordinatorCommand::Remove { shard_id } => {
                if !state.allocations.contains_key(&shard_id) {
                    return Err(CoordinatorError::UnknownShard(shard_id));
                }
                Ok(vec![CoordinatorEvent::ShardRemoved { shard_id }])
            }
        }
    }

    fn apply_event(state: &mut Self::State, event: &Self::Event) {
        match event {
            CoordinatorEvent::ShardAllocated { shard_id, region } => {
                state.allocations.insert(shard_id.clone(), region.clone());
            }
            CoordinatorEvent::ShardRebalanced { shard_id, to_region, .. } => {
                state.allocations.insert(shard_id.clone(), to_region.clone());
            }
            CoordinatorEvent::ShardRemoved { shard_id } => {
                state.allocations.remove(shard_id);
            }
        }
    }

    fn encode_event(event: &Self::Event) -> Result<Vec<u8>, String> {
        let cfg = bincode::config::standard();
        bincode::serde::encode_to_vec(event, cfg).map_err(|e| e.to_string())
    }

    fn decode_event(bytes: &[u8]) -> Result<Self::Event, String> {
        let cfg = bincode::config::standard();
        bincode::serde::decode_from_slice::<Self::Event, _>(bytes, cfg)
            .map(|(v, _)| v)
            .map_err(|e| e.to_string())
    }
}

/// Project a [`CoordinatorState`] (rebuilt from journal replay) onto
/// a fresh [`ShardCoordinator`]. Useful right after `recover`.
pub fn project_into(state: &CoordinatorState, target: &ShardCoordinator) {
    for (shard, region) in &state.allocations {
        // `allocate` is the right primitive — first-mention sets the
        // region; we then overwrite via `rebalance` if the journal
        // shows a later move (which `apply_event` already collapsed
        // into the final allocation).
        target.rebalance(shard, region.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_persistence::InMemoryJournal;

    fn cfg() -> (Arc<InMemoryJournal>, RecoveryPermitter) {
        (
            Arc::new(InMemoryJournal::default()),
            RecoveryPermitter::new(2),
        )
    }

    #[tokio::test]
    async fn allocate_then_rebalance_round_trips() {
        let (journal, permits) = cfg();
        let mut coord = PersistentShardCoordinator::new("coord-1");
        let mut state = CoordinatorState::default();
        let mut seq = 0u64;

        coord
            .handle_command(
                journal.clone(),
                &mut state,
                &mut seq,
                "w",
                CoordinatorCommand::Allocate {
                    shard_id: "s1".into(),
                    region: "r1".into(),
                },
            )
            .await
            .unwrap();
        coord
            .handle_command(
                journal.clone(),
                &mut state,
                &mut seq,
                "w",
                CoordinatorCommand::Rebalance {
                    shard_id: "s1".into(),
                    to_region: "r2".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(state.allocations.get("s1"), Some(&"r2".to_string()));

        // Replay → identical state.
        let mut coord2 = PersistentShardCoordinator::new("coord-1");
        let mut state2 = CoordinatorState::default();
        coord2.recover(journal.clone(), &mut state2, &permits).await.unwrap();
        assert_eq!(state2.allocations.get("s1"), Some(&"r2".to_string()));
    }

    #[tokio::test]
    async fn rebalance_unknown_shard_errors() {
        let (journal, _) = cfg();
        let coord = PersistentShardCoordinator::new("coord-2");
        let mut state = CoordinatorState::default();
        let mut seq = 0u64;
        let r = coord
            .handle_command(
                journal,
                &mut state,
                &mut seq,
                "w",
                CoordinatorCommand::Rebalance {
                    shard_id: "missing".into(),
                    to_region: "r2".into(),
                },
            )
            .await;
        assert!(matches!(
            r,
            Err(EventsourcedError::Domain(CoordinatorError::UnknownShard(_)))
        ));
    }

    #[tokio::test]
    async fn project_into_in_memory_coordinator() {
        let (journal, permits) = cfg();
        let mut coord = PersistentShardCoordinator::new("coord-3");
        let mut state = CoordinatorState::default();
        let mut seq = 0u64;
        for (sid, region) in [("s1", "r1"), ("s2", "r2"), ("s3", "r1")] {
            coord
                .handle_command(
                    journal.clone(),
                    &mut state,
                    &mut seq,
                    "w",
                    CoordinatorCommand::Allocate {
                        shard_id: sid.into(),
                        region: region.into(),
                    },
                )
                .await
                .unwrap();
        }

        // Replay into a brand-new in-memory coordinator.
        let mut state2 = CoordinatorState::default();
        let mut coord2 = PersistentShardCoordinator::new("coord-3");
        coord2.recover(journal.clone(), &mut state2, &permits).await.unwrap();
        let local = ShardCoordinator::new();
        project_into(&state2, &local);
        assert_eq!(local.region_for("s1"), Some("r1".to_string()));
        assert_eq!(local.region_for("s2"), Some("r2".to_string()));
        assert_eq!(local.region_for("s3"), Some("r1".to_string()));
    }

    #[tokio::test]
    async fn remove_shard_drops_from_state() {
        let (journal, _) = cfg();
        let mut coord = PersistentShardCoordinator::new("coord-4");
        let mut state = CoordinatorState::default();
        let mut seq = 0u64;
        coord
            .handle_command(
                journal.clone(),
                &mut state,
                &mut seq,
                "w",
                CoordinatorCommand::Allocate {
                    shard_id: "s1".into(),
                    region: "r1".into(),
                },
            )
            .await
            .unwrap();
        coord
            .handle_command(
                journal.clone(),
                &mut state,
                &mut seq,
                "w",
                CoordinatorCommand::Remove { shard_id: "s1".into() },
            )
            .await
            .unwrap();
        assert!(!state.allocations.contains_key("s1"));
    }
}
