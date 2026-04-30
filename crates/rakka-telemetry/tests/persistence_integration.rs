//! Integration test wiring the persistence probe to a live
//! `InMemoryJournal`.

#![cfg(feature = "persistence")]

use std::sync::Arc;

use rakka_persistence::{InMemoryJournal, Journal, PersistentRepr};
use rakka_telemetry::bus::TelemetryBus;
use rakka_telemetry::persistence::{InMemoryJournalAdmin, PersistenceProbe};

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
async fn snapshot_async_reports_live_journal_state() {
    let journal = InMemoryJournal::new();
    journal
        .write_messages(vec![repr("orders", 1), repr("orders", 2), repr("orders", 3)])
        .await
        .unwrap();
    journal.write_messages(vec![repr("payments", 1)]).await.unwrap();

    let probe = PersistenceProbe::new(TelemetryBus::new(8));
    probe.register_journal(Arc::new(InMemoryJournalAdmin::new("inmem", journal.clone())));
    probe.record_write("inmem", "orders", 3);
    probe.record_write("inmem", "payments", 1);

    let snap = probe.snapshot_async().await;
    assert_eq!(snap.journals.len(), 1);
    let pids: std::collections::HashMap<_, _> = snap.journals[0]
        .persistence_ids
        .iter()
        .map(|p| (p.persistence_id.clone(), p.highest_sequence_nr))
        .collect();
    assert_eq!(pids.get("orders"), Some(&3));
    assert_eq!(pids.get("payments"), Some(&1));
    assert_eq!(snap.total_events, 2);
}
