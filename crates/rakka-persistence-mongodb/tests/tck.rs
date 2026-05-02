//! MongoDB conformance. Requires `RAKKA_IT_MONGO_URL` to be set,
//! otherwise the tests skip.

use std::env;

use rakka_persistence_mongodb::{MongoConfig, MongoJournal, MongoSnapshotStore};
use rakka_persistence_tck::{
    journal_concurrent_suite, journal_extended_suite, journal_suite, snapshot_round_trip, snapshot_suite,
};

fn it_url() -> Option<String> {
    env::var("RAKKA_IT_MONGO_URL").ok()
}

fn unique_db() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("rakka_tck_{nanos:x}")
}

#[tokio::test]
async fn mongo_journal_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: RAKKA_IT_MONGO_URL not set");
        return;
    };
    let cfg = MongoConfig::new(url, unique_db());
    let j = MongoJournal::connect(cfg).await.expect("mongo journal");
    journal_suite(j.clone(), "mongo-j").await;
    journal_extended_suite(j.clone(), "mongo-j").await;
    journal_concurrent_suite(j, "mongo-j").await;
}

#[tokio::test]
async fn mongo_snapshot_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: RAKKA_IT_MONGO_URL not set");
        return;
    };
    let cfg = MongoConfig::new(url, unique_db());
    let s = MongoSnapshotStore::connect(cfg).await.expect("mongo snapshot");
    assert!(snapshot_round_trip(s.clone(), "mongo-s").await);
    snapshot_suite(s, "mongo-s-full").await;
}
