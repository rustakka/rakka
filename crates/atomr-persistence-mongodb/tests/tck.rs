//! MongoDB conformance. Requires `ATOMR_IT_MONGO_URL` to be set,
//! otherwise the tests skip.

use std::env;

use atomr_persistence_mongodb::{MongoConfig, MongoJournal, MongoSnapshotStore};
use atomr_persistence_tck::{
    journal_concurrent_suite, journal_extended_suite, journal_replay_edge_cases, journal_suite,
    snapshot_extended_suite, snapshot_round_trip, snapshot_suite,
};

fn it_url() -> Option<String> {
    env::var("ATOMR_IT_MONGO_URL").ok()
}

fn unique_db() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("atomr_tck_{nanos:x}")
}

#[tokio::test]
async fn mongo_journal_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: ATOMR_IT_MONGO_URL not set");
        return;
    };
    let cfg = MongoConfig::new(url, unique_db());
    let j = MongoJournal::connect(cfg).await.expect("mongo journal");
    journal_suite(j.clone(), "mongo-j").await;
    journal_extended_suite(j.clone(), "mongo-j").await;
    journal_replay_edge_cases(j.clone(), "mongo-j").await;
    journal_concurrent_suite(j, "mongo-j").await;
}

#[tokio::test]
async fn mongo_snapshot_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: ATOMR_IT_MONGO_URL not set");
        return;
    };
    let cfg = MongoConfig::new(url, unique_db());
    let s = MongoSnapshotStore::connect(cfg).await.expect("mongo snapshot");
    assert!(snapshot_round_trip(s.clone(), "mongo-s").await);
    snapshot_suite(s.clone(), "mongo-s-full").await;
    snapshot_extended_suite(s, "mongo-s-ext").await;
}
