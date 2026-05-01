//! rakka-persistence-query. akka.net: `Akka.Persistence.Query`.
//!
//! Phase 11 of `docs/full-port-plan.md` extends the read-journal
//! surface to match upstream: `events_by_persistence_id`,
//! `events_by_tag`, `current_*` variants, `all_persistence_ids`, and
//! a typed [`Offset`] type.

use async_trait::async_trait;
use rakka_persistence::{Journal, JournalError, PersistentRepr};

/// Typed read-journal offset. The in-memory backend uses `Sequence`
/// numbers; a SQL backend might emit `TimeBased` UUIDs. `NoOffset`
/// means "from the start."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Offset {
    NoOffset,
    Sequence(u64),
    TimeBased(u128),
}

impl Default for Offset {
    fn default() -> Self {
        Self::NoOffset
    }
}

impl Offset {
    pub fn as_sequence(self) -> Option<u64> {
        match self {
            Self::NoOffset => Some(0),
            Self::Sequence(n) => Some(n),
            Self::TimeBased(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub payload: Vec<u8>,
    pub offset: u64,
    pub tags: Vec<String>,
}

impl From<PersistentRepr> for EventEnvelope {
    fn from(r: PersistentRepr) -> Self {
        Self {
            persistence_id: r.persistence_id,
            sequence_nr: r.sequence_nr,
            payload: r.payload,
            offset: r.sequence_nr,
            tags: r.tags,
        }
    }
}

/// Read-journal surface. `current_*` variants take a snapshot at call
/// time; the non-current variants are tail-following (live) — backends
/// that only support snapshots return the snapshot and let callers
/// re-poll.
#[async_trait]
pub trait ReadJournal: Send + Sync + 'static {
    /// Replay events for a single persistence id, sequence-number
    /// bounded.
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError>;

    /// Snapshot variant of [`Self::events_by_persistence_id`] —
    /// default impl is identical (in-memory journals don't tail).
    async fn current_events_by_persistence_id(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        self.events_by_persistence_id(persistence_id, from, to).await
    }

    /// All events with a given tag, returned in offset order.
    /// Default impl is empty so backends without tag indexing don't
    /// silently mis-behave.
    async fn events_by_tag(
        &self,
        _tag: &str,
        _offset: Offset,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        Ok(Vec::new())
    }

    async fn current_events_by_tag(
        &self,
        tag: &str,
        offset: Offset,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        self.events_by_tag(tag, offset).await
    }

    /// Distinct persistence ids known to the backend. Default impl
    /// returns empty (backends without an id index opt in).
    async fn all_persistence_ids(&self) -> Result<Vec<String>, JournalError> {
        Ok(Vec::new())
    }

    async fn current_persistence_ids(&self) -> Result<Vec<String>, JournalError> {
        self.all_persistence_ids().await
    }
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

    async fn events_by_tag(
        &self,
        tag: &str,
        offset: Offset,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let from_seq = offset.as_sequence().unwrap_or(0);
        // For backends that don't have a tag index, we have to fall
        // back to scanning known persistence ids. The Journal trait
        // doesn't expose enumeration, so we ask the backend for the
        // list via a downcast-free path: we use `current_persistence_ids`
        // (default impl returns empty for in-memory). Production
        // backends override `events_by_tag` directly with an indexed
        // query.
        let ids = self.current_persistence_ids().await?;
        let mut out = Vec::new();
        for id in ids {
            let reprs = self.journal.replay_messages(&id, from_seq, u64::MAX, u64::MAX).await?;
            for r in reprs {
                if r.tags.iter().any(|t| t == tag) {
                    out.push(r.into());
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_persistence::{InMemoryJournal, Journal, PersistentRepr};
    use std::sync::Arc;

    fn repr(pid: &str, seq: u64, tags: &[&str]) -> PersistentRepr {
        PersistentRepr {
            persistence_id: pid.into(),
            sequence_nr: seq,
            payload: vec![seq as u8],
            manifest: "evt".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: tags.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn events_by_persistence_id_replays_range() {
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![repr("a", 1, &[]), repr("a", 2, &[]), repr("a", 3, &[])])
            .await
            .unwrap();
        let q = SimpleReadJournal::new(j);
        let evs = q.events_by_persistence_id("a", 1, 2).await.unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].sequence_nr, 1);
        assert_eq!(evs[1].sequence_nr, 2);
    }

    #[tokio::test]
    async fn current_variant_matches_live() {
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![repr("a", 1, &[])]).await.unwrap();
        let q = SimpleReadJournal::new(j);
        let live = q.events_by_persistence_id("a", 1, 99).await.unwrap();
        let snap = q.current_events_by_persistence_id("a", 1, 99).await.unwrap();
        assert_eq!(live.len(), snap.len());
    }

    #[tokio::test]
    async fn offset_sequence_round_trips() {
        assert_eq!(Offset::Sequence(7).as_sequence(), Some(7));
        assert_eq!(Offset::NoOffset.as_sequence(), Some(0));
        assert_eq!(Offset::TimeBased(123).as_sequence(), None);
    }
}
