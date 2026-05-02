//! SQL provider conformance. Runs the shared TCK against an in-memory
//! SQLite database so `cargo test --workspace` covers this crate without
//! any external services.

#![cfg(feature = "sqlite")]

use std::sync::Arc;

use rakka_persistence::SnapshotStore as _;
use rakka_persistence_sql::{SqlConfig, SqlJournal, SqlReadJournal, SqlSnapshotStore};
use rakka_persistence_tck::{
    journal_concurrent_suite, journal_extended_suite, journal_suite, journal_tag_suite, snapshot_round_trip,
    snapshot_suite,
};

async fn new_journal() -> Arc<SqlJournal> {
    let cfg = SqlConfig::new("sqlite::memory:");
    SqlJournal::connect(cfg).await.expect("sqlite journal")
}

#[tokio::test]
async fn sqlite_journal_passes_tck() {
    let j = new_journal().await;
    journal_suite(j.clone(), "sql-j").await;
    journal_tag_suite(j.clone(), "sql-j").await;
    journal_extended_suite(j.clone(), "sql-j").await;
    journal_concurrent_suite(j, "sql-j").await;
}

#[tokio::test]
async fn sqlite_snapshot_passes_tck() {
    let cfg = SqlConfig::new("sqlite::memory:");
    let s = SqlSnapshotStore::connect(cfg).await.expect("sqlite snapshot");
    assert!(snapshot_round_trip(s.clone(), "sql-s").await);
    snapshot_suite(s, "sql-s-full").await;
}

#[tokio::test]
async fn sqlite_read_journal_events_by_tag() {
    let j = new_journal().await;
    use rakka_persistence::{Journal, PersistentRepr};
    let reprs = vec![
        PersistentRepr {
            persistence_id: "q".into(),
            sequence_nr: 1,
            payload: b"1".to_vec(),
            manifest: "m".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: vec!["alpha".into()],
        },
        PersistentRepr {
            persistence_id: "q".into(),
            sequence_nr: 2,
            payload: b"2".to_vec(),
            manifest: "m".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: vec!["beta".into()],
        },
    ];
    j.write_messages(reprs).await.unwrap();
    let rj = SqlReadJournal::new(j);
    let envelopes = rj.events_by_tag("alpha", 0, 100).await.unwrap();
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].sequence_nr, 1);
}

#[tokio::test]
async fn config_from_env_defaults_to_memory() {
    // With no env vars set this should degrade gracefully.
    std::env::remove_var("RAKKA_PERSISTENCE_SQL_URL");
    std::env::remove_var("RAKKA_IT_SQL_URL");
    std::env::remove_var("DATABASE_URL");
    let cfg = SqlConfig::from_env();
    assert!(cfg.url.starts_with("sqlite"));
    let store = SqlSnapshotStore::connect(cfg).await.expect("env snapshot");
    assert!(store.load("never").await.is_none());
}
