//! rakka-persistence-query. akka.net: `Akka.Persistence.Query`.

use async_trait::async_trait;
use rakka_persistence::{Journal, JournalError, PersistentRepr};

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub payload: Vec<u8>,
    pub offset: u64,
}

impl From<PersistentRepr> for EventEnvelope {
    fn from(r: PersistentRepr) -> Self {
        Self {
            persistence_id: r.persistence_id,
            sequence_nr: r.sequence_nr,
            payload: r.payload,
            offset: r.sequence_nr,
        }
    }
}

#[async_trait]
pub trait ReadJournal: Send + Sync + 'static {
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError>;
}

pub struct SimpleReadJournal<J: Journal> {
    journal: std::sync::Arc<J>,
}

impl<J: Journal> SimpleReadJournal<J> {
    pub fn new(journal: std::sync::Arc<J>) -> Self {
        Self { journal }
    }
}

#[async_trait]
impl<J: Journal> ReadJournal for SimpleReadJournal<J> {
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs =
            self.journal.replay_messages(persistence_id, from, to, u64::MAX).await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }
}
