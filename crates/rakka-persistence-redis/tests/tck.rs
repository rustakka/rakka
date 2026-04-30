//! Redis provider conformance. Runs when a `RAKKA_IT_REDIS_URL` is set,
//! otherwise the tests are skipped so `cargo test --workspace` remains
//! hermetic.

use std::env;

use rakka_persistence_redis::{RedisConfig, RedisJournal, RedisSnapshotStore};
use rakka_persistence_tck::{journal_suite, snapshot_round_trip, snapshot_suite};

fn it_url() -> Option<String> {
    env::var("RAKKA_IT_REDIS_URL").ok()
}

#[tokio::test]
async fn redis_journal_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: RAKKA_IT_REDIS_URL not set");
        return;
    };
    let cfg = RedisConfig::new(url).with_key_prefix(format!(
        "tck:{}",
        uuid_like()
    ));
    let j = RedisJournal::connect(cfg).await.expect("redis journal");
    journal_suite(j, "redis-j").await;
}

#[tokio::test]
async fn redis_snapshot_passes_tck() {
    let Some(url) = it_url() else {
        eprintln!("skip: RAKKA_IT_REDIS_URL not set");
        return;
    };
    let cfg = RedisConfig::new(url).with_key_prefix(format!(
        "tck:{}",
        uuid_like()
    ));
    let s = RedisSnapshotStore::connect(cfg).await.expect("redis snapshot");
    assert!(snapshot_round_trip(s.clone(), "redis-s").await);
    snapshot_suite(s, "redis-s-full").await;
}

fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{nanos:x}")
}
