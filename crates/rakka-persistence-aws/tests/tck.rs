//! DynamoDB conformance. Requires `RAKKA_IT_DYNAMO_ENDPOINT`
//! (e.g. pointing at dynamodb-local) to be set; tests skip otherwise.

use std::env;

use rakka_persistence_aws::{DynamoConfig, DynamoJournal, DynamoSnapshotStore};
use rakka_persistence_tck::{journal_suite, snapshot_round_trip, snapshot_suite};

fn it_endpoint() -> Option<String> {
    env::var("RAKKA_IT_DYNAMO_ENDPOINT").ok()
}

fn unique_table() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("rakka_tck_{nanos:x}")
}

#[tokio::test]
async fn dynamo_journal_passes_tck() {
    let Some(endpoint) = it_endpoint() else {
        eprintln!("skip: RAKKA_IT_DYNAMO_ENDPOINT not set");
        return;
    };
    let cfg = DynamoConfig::new(unique_table())
        .with_endpoint(endpoint)
        .with_region("us-east-1");
    let j = DynamoJournal::connect(cfg).await.expect("dynamo journal");
    journal_suite(j, "dynamo-j").await;
}

#[tokio::test]
async fn dynamo_snapshot_passes_tck() {
    let Some(endpoint) = it_endpoint() else {
        eprintln!("skip: RAKKA_IT_DYNAMO_ENDPOINT not set");
        return;
    };
    let cfg = DynamoConfig::new(unique_table())
        .with_endpoint(endpoint)
        .with_region("us-east-1");
    let s = DynamoSnapshotStore::connect(cfg).await.expect("dynamo snapshot");
    assert!(snapshot_round_trip(s.clone(), "dynamo-s").await);
    snapshot_suite(s, "dynamo-s-full").await;
}
