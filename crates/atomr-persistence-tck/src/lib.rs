//! atomr-persistence-tck. akka.net: `Akka.Persistence.TCK`.
//!
//! Provides reusable spec functions plugin authors can call against their
//! [`Journal`](atomr_persistence::Journal) and
//! [`SnapshotStore`](atomr_persistence::SnapshotStore) implementations.
//!
//! The suite is split into [`journal_suite`] and [`snapshot_suite`] modules
//! (plus the lightweight round-trip helpers retained for historical usage).

mod journal_suite;
mod snapshot_suite;

pub use journal_suite::{
    journal_concurrent_suite, journal_extended_suite, journal_replay_edge_cases, journal_round_trip,
    journal_suite, journal_tag_suite,
};
pub use snapshot_suite::{snapshot_round_trip, snapshot_suite};

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_persistence::{InMemoryJournal, InMemorySnapshotStore};

    #[tokio::test]
    async fn in_memory_journal_round_trip() {
        assert!(journal_round_trip(InMemoryJournal::new(), "tck-j").await);
    }

    #[tokio::test]
    async fn in_memory_journal_full_suite() {
        journal_suite(InMemoryJournal::new(), "tck-full").await;
    }

    #[tokio::test]
    async fn in_memory_snapshot_round_trip() {
        assert!(snapshot_round_trip(InMemorySnapshotStore::new(), "tck-s").await);
    }

    #[tokio::test]
    async fn in_memory_snapshot_full_suite() {
        snapshot_suite(InMemorySnapshotStore::new(), "tck-s-full").await;
    }

    #[tokio::test]
    async fn in_memory_journal_extended_suite() {
        journal_extended_suite(InMemoryJournal::new(), "tck-j-ext").await;
    }

    #[tokio::test]
    async fn in_memory_journal_concurrent_suite() {
        journal_concurrent_suite(InMemoryJournal::new(), "tck-j-conc").await;
    }

    #[tokio::test]
    async fn in_memory_journal_edge_cases() {
        journal_replay_edge_cases(InMemoryJournal::new(), "tck-j-edge").await;
    }
}
