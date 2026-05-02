//! Cassandra conformance. Requires `RAKKA_IT_CASSANDRA_NODES` (comma
//! separated host:port list) to be set; tests skip otherwise.

use std::env;

use rakka_persistence_cassandra::{CassandraConfig, CassandraJournal, CassandraSnapshotStore};
use rakka_persistence_tck::{
    journal_concurrent_suite, journal_extended_suite, journal_suite, snapshot_round_trip, snapshot_suite,
};

fn it_nodes() -> Option<Vec<String>> {
    env::var("RAKKA_IT_CASSANDRA_NODES").ok().map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
}

fn unique_keyspace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("rakka_tck_{nanos:x}")
}

#[tokio::test]
async fn cassandra_journal_passes_tck() {
    let Some(nodes) = it_nodes() else {
        eprintln!("skip: RAKKA_IT_CASSANDRA_NODES not set");
        return;
    };
    let cfg = CassandraConfig::new(nodes, unique_keyspace()).with_partition_size(100);
    let j = CassandraJournal::connect(cfg).await.expect("cassandra journal");
    journal_suite(j.clone(), "cassandra-j").await;
    journal_extended_suite(j.clone(), "cassandra-j").await;
    journal_concurrent_suite(j, "cassandra-j").await;
}

#[tokio::test]
async fn cassandra_snapshot_passes_tck() {
    let Some(nodes) = it_nodes() else {
        eprintln!("skip: RAKKA_IT_CASSANDRA_NODES not set");
        return;
    };
    let cfg = CassandraConfig::new(nodes, unique_keyspace());
    let s = CassandraSnapshotStore::connect(cfg).await.expect("cassandra snapshot");
    assert!(snapshot_round_trip(s.clone(), "cassandra-s").await);
    snapshot_suite(s, "cassandra-s-full").await;
}
