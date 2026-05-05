//! 3-node convergence spec for the Replicator.
//! (subset that
//! does not require remoting).
//!
//! Asserts that three independent ReplicatorActors with disjoint
//! GCounter increments converge to the same merged value once each
//! has observed every other's snapshot via cross-write (the in-process
//! analog of gossip). The properties asserted match
//! "eventual consistency" invariant for GCounter.

use atomr_distributed_data::{
    GCounter, OrSet, PNCounter, ReadConsistency, ReplicatorActor, WriteConsistency,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_replicators_converge_on_gcounter_sum() {
    let r1 = ReplicatorActor::spawn();
    let r2 = ReplicatorActor::spawn();
    let r3 = ReplicatorActor::spawn();

    let mut a = GCounter::new();
    a.increment("n1", 4);
    let mut b = GCounter::new();
    b.increment("n2", 9);
    let mut c = GCounter::new();
    c.increment("n3", 2);

    let key = "shared";
    r1.update(key, a, WriteConsistency::Local).await;
    r2.update(key, b, WriteConsistency::Local).await;
    r3.update(key, c, WriteConsistency::Local).await;

    // Gossip snapshots in a star pattern: r1 ← all, r2 ← all, r3 ← all.
    for src in [&r1, &r2, &r3] {
        let snap: GCounter = src.get(key, ReadConsistency::Local).await.unwrap();
        for sink in [&r1, &r2, &r3] {
            sink.update(key, snap.clone(), WriteConsistency::Local).await;
        }
    }

    let v1: GCounter = r1.get(key, ReadConsistency::Local).await.unwrap();
    let v2: GCounter = r2.get(key, ReadConsistency::Local).await.unwrap();
    let v3: GCounter = r3.get(key, ReadConsistency::Local).await.unwrap();
    assert_eq!(v1.value(), 15);
    assert_eq!(v2.value(), 15);
    assert_eq!(v3.value(), 15);

    r1.shutdown().await;
    r2.shutdown().await;
    r3.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_replicators_converge_on_pncounter() {
    let r1 = ReplicatorActor::spawn();
    let r2 = ReplicatorActor::spawn();
    let r3 = ReplicatorActor::spawn();

    let mut a = PNCounter::new();
    a.increment("n1", 10);
    let mut b = PNCounter::new();
    b.increment("n2", 5);
    b.decrement("n2", 2);
    let mut c = PNCounter::new();
    c.increment("n3", 1);

    let key = "pn";
    r1.update(key, a, WriteConsistency::Local).await;
    r2.update(key, b, WriteConsistency::Local).await;
    r3.update(key, c, WriteConsistency::Local).await;

    for src in [&r1, &r2, &r3] {
        let snap: PNCounter = src.get(key, ReadConsistency::Local).await.unwrap();
        for sink in [&r1, &r2, &r3] {
            sink.update(key, snap.clone(), WriteConsistency::Local).await;
        }
    }

    let v1: PNCounter = r1.get(key, ReadConsistency::Local).await.unwrap();
    // 10 (n1+) + 5 (n2+) - 2 (n2-) + 1 (n3+) = 14
    assert_eq!(v1.value(), 14);

    r1.shutdown().await;
    r2.shutdown().await;
    r3.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_replicators_or_set_add_remove_converges() {
    let r1 = ReplicatorActor::spawn();
    let r2 = ReplicatorActor::spawn();
    let r3 = ReplicatorActor::spawn();

    let mut a: OrSet<String> = OrSet::new();
    a.add("x".to_string());
    a.add("y".to_string());
    let mut b: OrSet<String> = OrSet::new();
    b.add("y".to_string());
    b.add("z".to_string());
    let mut c: OrSet<String> = OrSet::new();
    c.add("w".to_string());

    let key = "set";
    r1.update(key, a, WriteConsistency::Local).await;
    r2.update(key, b, WriteConsistency::Local).await;
    r3.update(key, c, WriteConsistency::Local).await;

    for src in [&r1, &r2, &r3] {
        let snap: OrSet<String> = src.get(key, ReadConsistency::Local).await.unwrap();
        for sink in [&r1, &r2, &r3] {
            sink.update(key, snap.clone(), WriteConsistency::Local).await;
        }
    }

    let merged: OrSet<String> = r1.get(key, ReadConsistency::Local).await.unwrap();
    let mut items: Vec<&String> = merged.iter().collect();
    items.sort();
    assert_eq!(items, vec![&"w".to_string(), &"x".to_string(), &"y".to_string(), &"z".to_string()]);

    r1.shutdown().await;
    r2.shutdown().await;
    r3.shutdown().await;
}
