//! rustakka-persistence-tck. akka.net: `Akka.Persistence.TCK`.
//!
//! Provides reusable spec functions plugin authors can call against their
//! [`Journal`](rustakka_persistence::Journal) and
//! [`SnapshotStore`](rustakka_persistence::SnapshotStore) implementations.

use std::sync::Arc;

use rustakka_persistence::{Journal, PersistentRepr, SnapshotMetadata, SnapshotStore};

pub async fn journal_round_trip<J: Journal>(journal: Arc<J>, pid: &str) -> bool {
    let mut batch = Vec::new();
    for i in 1..=5u64 {
        batch.push(PersistentRepr {
            persistence_id: pid.into(),
            sequence_nr: i,
            payload: vec![i as u8],
            manifest: "m".into(),
            writer_uuid: "tck".into(),
            deleted: false,
        });
    }
    journal.write_messages(batch).await.unwrap();
    let replay = journal.replay_messages(pid, 1, 5, 100).await.unwrap();
    replay.len() == 5 && journal.highest_sequence_nr(pid, 0).await.unwrap() == 5
}

pub async fn snapshot_round_trip<S: SnapshotStore>(store: Arc<S>, pid: &str) -> bool {
    store
        .save(
            SnapshotMetadata { persistence_id: pid.into(), sequence_nr: 42, timestamp: 0 },
            b"state".to_vec(),
        )
        .await;
    let loaded = store.load(pid).await;
    matches!(loaded, Some((m, p)) if m.sequence_nr == 42 && p == b"state")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustakka_persistence::{InMemoryJournal, InMemorySnapshotStore};

    #[tokio::test]
    async fn in_memory_journal_passes_tck() {
        assert!(journal_round_trip(InMemoryJournal::new(), "tck-j").await);
    }

    #[tokio::test]
    async fn in_memory_snapshot_passes_tck() {
        assert!(snapshot_round_trip(InMemorySnapshotStore::new(), "tck-s").await);
    }
}
