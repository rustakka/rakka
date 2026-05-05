//! Azure Table Storage conformance. Requires
//! `ATOMR_IT_AZURE_CONNECTION_STRING` (e.g. the Azurite emulator) to
//! be set; tests skip otherwise.

use std::env;

use atomr_persistence_azure::{AzureConfig, AzureJournal, AzureSnapshotStore};
use atomr_persistence_tck::{
    journal_concurrent_suite, journal_extended_suite, journal_replay_edge_cases, journal_suite,
    snapshot_extended_suite, snapshot_round_trip, snapshot_suite,
};

fn it_cfg() -> Option<AzureConfig> {
    let cs = env::var("ATOMR_IT_AZURE_CONNECTION_STRING").ok()?;
    AzureConfig::from_connection_string(&cs).ok()
}

fn unique_tables() -> (String, String) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    (format!("eventjournal{nanos:x}"), format!("snapshotstore{nanos:x}"))
}

#[tokio::test]
async fn azure_journal_passes_tck() {
    let Some(mut cfg) = it_cfg() else {
        eprintln!("skip: ATOMR_IT_AZURE_CONNECTION_STRING not set");
        return;
    };
    let (j, s) = unique_tables();
    cfg.journal_table = j;
    cfg.snapshot_table = s;
    let journal = AzureJournal::connect(cfg).await.expect("azure journal");
    journal_suite(journal.clone(), "azure-j").await;
    journal_extended_suite(journal.clone(), "azure-j").await;
    journal_replay_edge_cases(journal.clone(), "azure-j").await;
    journal_concurrent_suite(journal, "azure-j").await;
}

#[tokio::test]
async fn azure_snapshot_passes_tck() {
    let Some(mut cfg) = it_cfg() else {
        eprintln!("skip: ATOMR_IT_AZURE_CONNECTION_STRING not set");
        return;
    };
    let (j, s) = unique_tables();
    cfg.journal_table = j;
    cfg.snapshot_table = s;
    let store = AzureSnapshotStore::connect(cfg).await.expect("azure snapshot");
    assert!(snapshot_round_trip(store.clone(), "azure-s").await);
    snapshot_suite(store.clone(), "azure-s-full").await;
    snapshot_extended_suite(store, "azure-s-ext").await;
}
