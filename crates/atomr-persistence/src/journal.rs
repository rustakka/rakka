//! Journal plugin trait and an in-memory implementation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use thiserror::Error;

#[derive(Debug, Clone, Default)]
pub struct PersistentRepr {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub payload: Vec<u8>,
    pub manifest: String,
    pub writer_uuid: String,
    pub deleted: bool,
    pub tags: Vec<String>,
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("sequence nr {expected} expected, got {got}")]
    SequenceOutOfOrder { expected: u64, got: u64 },
    #[error("persistence id not found: {0}")]
    NotFound(String),
    #[error("backend error: {0}")]
    Backend(String),
}

impl JournalError {
    pub fn backend(err: impl std::fmt::Display) -> Self {
        Self::Backend(err.to_string())
    }
}

#[async_trait]
pub trait Journal: Send + Sync + 'static {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError>;

    async fn delete_messages_to(&self, persistence_id: &str, to_sequence_nr: u64)
        -> Result<(), JournalError>;

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError>;

    async fn highest_sequence_nr(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
    ) -> Result<u64, JournalError>;

    async fn events_by_tag(
        &self,
        _tag: &str,
        _from_offset: u64,
        _max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        Ok(Vec::new())
    }

    /// Distinct persistence ids known to the backend. Default impl
    /// returns empty so backends without an id index opt in.
    async fn all_persistence_ids(&self) -> Result<Vec<String>, JournalError> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
pub struct InMemoryJournal {
    streams: RwLock<HashMap<String, Vec<PersistentRepr>>>,
}

impl InMemoryJournal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl Journal for InMemoryJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError> {
        let mut map = self.streams.write();
        for msg in messages {
            let entry = map.entry(msg.persistence_id.clone()).or_default();
            let expected = entry.last().map(|r| r.sequence_nr + 1).unwrap_or(1);
            if msg.sequence_nr != expected {
                return Err(JournalError::SequenceOutOfOrder { expected, got: msg.sequence_nr });
            }
            entry.push(msg);
        }
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        let mut map = self.streams.write();
        if let Some(stream) = map.get_mut(persistence_id) {
            for r in stream.iter_mut() {
                if r.sequence_nr <= to_sequence_nr {
                    r.deleted = true;
                }
            }
        }
        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let map = self.streams.read();
        Ok(map
            .get(persistence_id)
            .map(|v| {
                v.iter()
                    .filter(|r| !r.deleted && r.sequence_nr >= from && r.sequence_nr <= to)
                    .take(max as usize)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn highest_sequence_nr(&self, pid: &str, _from: u64) -> Result<u64, JournalError> {
        Ok(self.streams.read().get(pid).and_then(|v| v.last()).map(|r| r.sequence_nr).unwrap_or(0))
    }

    async fn all_persistence_ids(&self) -> Result<Vec<String>, JournalError> {
        Ok(self.streams.read().keys().cloned().collect())
    }

    async fn events_by_tag(
        &self,
        tag: &str,
        from_offset: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let map = self.streams.read();
        let mut out = Vec::new();
        for (_pid, stream) in map.iter() {
            for r in stream {
                if r.deleted {
                    continue;
                }
                if r.sequence_nr < from_offset {
                    continue;
                }
                if r.tags.iter().any(|t| t == tag) {
                    out.push(r.clone());
                    if out.len() as u64 >= max {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }
}

impl InMemoryJournal {
    /// List all persistence ids currently stored. Used by the telemetry
    /// `JournalAdmin` impl.
    pub fn list_persistence_ids(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    /// Number of non-deleted events stored for `persistence_id`.
    pub fn event_count(&self, persistence_id: &str) -> u64 {
        self.streams
            .read()
            .get(persistence_id)
            .map(|v| v.iter().filter(|r| !r.deleted).count() as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repr(pid: &str, nr: u64) -> PersistentRepr {
        PersistentRepr {
            persistence_id: pid.into(),
            sequence_nr: nr,
            payload: vec![nr as u8],
            manifest: "m".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: Vec::new(),
        }
    }

    #[tokio::test]
    async fn write_and_replay() {
        let j = InMemoryJournal::new();
        j.write_messages(vec![repr("p", 1), repr("p", 2), repr("p", 3)]).await.unwrap();
        let got = j.replay_messages("p", 1, 3, 10).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(j.highest_sequence_nr("p", 0).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn out_of_order_rejected() {
        let j = InMemoryJournal::new();
        let err = j.write_messages(vec![repr("p", 2)]).await.unwrap_err();
        matches!(err, JournalError::SequenceOutOfOrder { .. });
    }
}
