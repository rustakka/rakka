//! Persistent actor trait. akka.net: `UntypedPersistentActor`, `ReceivePersistentActor`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::journal::{Journal, JournalError, PersistentRepr};
use crate::snapshot::SnapshotStore;

#[async_trait]
pub trait PersistentActor: Send + 'static {
    type Command: Send + 'static;
    type Event: Send + Clone + 'static;
    type State: Default + Send + 'static;

    fn persistence_id(&self) -> String;

    fn command_to_events(&self, state: &Self::State, cmd: Self::Command) -> Vec<Self::Event>;

    fn apply_event(state: &mut Self::State, event: &Self::Event);

    fn encode_event(event: &Self::Event) -> Vec<u8>;
    fn decode_event(bytes: &[u8]) -> Self::Event;

    async fn recover<J: Journal>(
        &self,
        journal: Arc<J>,
        state: &mut Self::State,
        writer_uuid: &str,
    ) -> Result<u64, JournalError> {
        let highest = journal.highest_sequence_nr(&self.persistence_id(), 0).await?;
        let events = journal.replay_messages(&self.persistence_id(), 1, highest, u64::MAX).await?;
        for e in &events {
            let evt = Self::decode_event(&e.payload);
            Self::apply_event(state, &evt);
        }
        let _ = writer_uuid;
        Ok(highest)
    }

    async fn handle_command<J: Journal>(
        &self,
        journal: Arc<J>,
        state: &mut Self::State,
        next_seq: &mut u64,
        writer_uuid: &str,
        cmd: Self::Command,
    ) -> Result<(), JournalError> {
        let events = self.command_to_events(state, cmd);
        let mut reprs = Vec::with_capacity(events.len());
        for e in &events {
            *next_seq += 1;
            reprs.push(PersistentRepr {
                persistence_id: self.persistence_id(),
                sequence_nr: *next_seq,
                payload: Self::encode_event(e),
                manifest: "evt".into(),
                writer_uuid: writer_uuid.into(),
                deleted: false,
                tags: Vec::new(),
            });
        }
        journal.write_messages(reprs).await?;
        for e in &events {
            Self::apply_event(state, e);
        }
        Ok(())
    }

    async fn save_snapshot<S: SnapshotStore>(&self, store: Arc<S>, sequence_nr: u64, payload: Vec<u8>) {
        store
            .save(
                crate::snapshot::SnapshotMetadata {
                    persistence_id: self.persistence_id(),
                    sequence_nr,
                    timestamp: 0,
                },
                payload,
            )
            .await;
    }
}
