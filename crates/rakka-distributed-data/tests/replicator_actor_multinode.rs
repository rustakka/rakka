//! Phase 15.C — multinode-style integration test for the actor-based
//! Replicator. Two `ReplicatorActor`s race CRDT updates against the
//! same key; merging after gossip-style cross-replication preserves
//! `GCounter` semantics.

use std::sync::Arc;

use rakka_distributed_data::{
    DurableStore, FileDurableStore, GCounter, ReadConsistency, ReplicatorActor, WriteConsistency,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_actor_replicators_merge_disjoint_increments() {
    let r1 = ReplicatorActor::spawn();
    let r2 = ReplicatorActor::spawn();

    let mut a = GCounter::new();
    a.increment("n1", 7);
    let mut b = GCounter::new();
    b.increment("n2", 5);

    r1.update("k", a.clone(), WriteConsistency::Local).await;
    r2.update("k", b.clone(), WriteConsistency::Local).await;

    // "Gossip" the two snapshots into each other.
    let from1: GCounter = r1.get("k", ReadConsistency::Local).await.unwrap();
    let from2: GCounter = r2.get("k", ReadConsistency::Local).await.unwrap();
    r2.update("k", from1, WriteConsistency::Local).await;
    r1.update("k", from2, WriteConsistency::Local).await;

    let m1: GCounter = r1.get("k", ReadConsistency::Local).await.unwrap();
    let m2: GCounter = r2.get("k", ReadConsistency::Local).await.unwrap();
    assert_eq!(m1.value(), 12);
    assert_eq!(m2.value(), 12);

    r1.shutdown().await;
    r2.shutdown().await;
}

#[tokio::test]
async fn replicator_actor_persists_through_durable_store() {
    let store = Arc::new(FileDurableStore::tmp().unwrap());
    let r = ReplicatorActor::spawn_with(store.clone());
    let mut c = GCounter::new();
    c.increment("only", 4);
    r.update("a", c, WriteConsistency::Local).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let keys = store.keys().unwrap();
    assert_eq!(keys, vec!["a".to_string()]);
    r.shutdown().await;
}
