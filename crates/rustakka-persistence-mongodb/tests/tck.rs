//! MongoDB conformance. Requires `RUSTAKKA_IT_MONGO_URL` to be set,
//! otherwise the tests skip.

use std::env;

use rustakka_persistence_mongodb::{MongoConfig, MongoJournal, MongoSnapshotStore};
use rustakka_persistence_tck::{journal_suite, snapshot_round_trip, snapshot_suite};

fn it_url() -> Option<String> {
    env::var("RUSTAKKA_IT_MONGO_URL").ok()
}

fn unique_db() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("rustakka_tck_{nanos:x}")
}

#[tokio::test]
async fn mongo_journal_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: RUSTAKKA_IT_MONGO_URL not set");
        return;
    };
    let cfg = MongoConfig::new(url, unique_db());
    let j = MongoJournal::connect(cfg).await.expect("mongo journal");
    journal_suite(j, "mongo-j").await;
}

#[tokio::test]
async fn mongo_snapshot_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: RUSTAKKA_IT_MONGO_URL not set");
        return;
    };
    let cfg = MongoConfig::new(url, unique_db());
    let s = MongoSnapshotStore::connect(cfg).await.expect("mongo snapshot");
    assert!(snapshot_round_trip(s.clone(), "mongo-s").await);
    snapshot_suite(s, "mongo-s-full").await;
}
