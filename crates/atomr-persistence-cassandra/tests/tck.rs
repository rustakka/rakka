//! Cassandra conformance. Requires `ATOMR_IT_CASSANDRA_NODES` (comma
//! separated host:port list) to be set; tests skip otherwise.

use std::env;

use atomr_persistence_cassandra::{CassandraConfig, CassandraJournal, CassandraSnapshotStore};
use atomr_persistence_tck::{
    journal_concurrent_suite, journal_extended_suite, journal_replay_edge_cases, journal_suite,
    snapshot_extended_suite, snapshot_round_trip, snapshot_suite,
};

fn it_nodes() -> Option<Vec<String>> {
    env::var("ATOMR_IT_CASSANDRA_NODES").ok().map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
}

fn unique_keyspace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("atomr_tck_{nanos:x}")
}

#[tokio::test]
async fn cassandra_journal_passes_tck() {
    let Some(nodes) = it_nodes() else {
        eprintln!("skip: ATOMR_IT_CASSANDRA_NODES not set");
        return;
    };
    let cfg = CassandraConfig::new(nodes, unique_keyspace()).with_partition_size(100);
    let j = CassandraJournal::connect(cfg).await.expect("cassandra journal");
    journal_suite(j.clone(), "cassandra-j").await;
    journal_extended_suite(j.clone(), "cassandra-j").await;
    journal_replay_edge_cases(j.clone(), "cassandra-j").await;
    journal_concurrent_suite(j, "cassandra-j").await;
}

#[tokio::test]
async fn cassandra_snapshot_passes_tck() {
    let Some(nodes) = it_nodes() else {
        eprintln!("skip: ATOMR_IT_CASSANDRA_NODES not set");
        return;
    };
    let cfg = CassandraConfig::new(nodes, unique_keyspace());
    let s = CassandraSnapshotStore::connect(cfg).await.expect("cassandra snapshot");
    assert!(snapshot_round_trip(s.clone(), "cassandra-s").await);
    snapshot_suite(s.clone(), "cassandra-s-full").await;
    snapshot_extended_suite(s, "cassandra-s-ext").await;
}
