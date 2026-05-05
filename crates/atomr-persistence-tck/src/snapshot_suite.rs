//! Snapshot conformance suite.

use std::sync::Arc;

use atomr_persistence::{SnapshotMetadata, SnapshotStore};

fn meta(pid: &str, nr: u64) -> SnapshotMetadata {
    SnapshotMetadata { persistence_id: pid.into(), sequence_nr: nr, timestamp: nr * 100 }
}

/// Smoke test: save and load a single snapshot.
pub async fn snapshot_round_trip<S: SnapshotStore>(store: Arc<S>, pid: &str) -> bool {
    store.save(meta(pid, 42), b"state".to_vec()).await;
    let loaded = store.load(pid).await;
    matches!(loaded, Some((m, p)) if m.sequence_nr == 42 && p == b"state")
}

/// Full snapshot conformance suite.
pub async fn snapshot_suite<S: SnapshotStore>(store: Arc<S>, pid_prefix: &str) {
    let a = format!("{pid_prefix}-a");
    let b = format!("{pid_prefix}-b");

    store.save(meta(&a, 1), b"v1".to_vec()).await;
    store.save(meta(&a, 2), b"v2".to_vec()).await;
    store.save(meta(&a, 3), b"v3".to_vec()).await;
    store.save(meta(&b, 7), b"bv7".to_vec()).await;

    let latest_a = store.load(&a).await;
    let (m, payload) = latest_a.expect("latest snapshot");
    assert_eq!(m.sequence_nr, 3, "latest snapshot should be sequence 3");
    assert_eq!(payload, b"v3");

    let latest_b = store.load(&b).await.expect("latest b");
    assert_eq!(latest_b.0.sequence_nr, 7);

    store.delete(&a, 2).await;
    let after = store.load(&a).await.expect("remaining snapshot after delete");
    assert!(after.0.sequence_nr > 2, "delete_to must drop all <= bound");

    store.delete(&b, 100).await;
    assert!(store.load(&b).await.is_none(), "delete_to(MAX) should clear all");
}

/// Extended snapshot conformance suite. Mirrors a subset of upstream's
/// `Akka.Persistence.TCK.Snapshot.SnapshotStoreSpec`, covering: latest-wins
/// across multiple saves, cross-pid isolation, partial deletes, no-op deletes
/// against unknown pids, and concurrent saves for the same pid.
pub async fn snapshot_extended_suite<S: SnapshotStore>(store: Arc<S>, pid_prefix: &str) {
    let a = format!("{pid_prefix}-xA");
    let b = format!("{pid_prefix}-xB");

    // 1. Multiple saves for the same pid: load() returns the latest.
    store.save(meta(&a, 1), b"a-v1".to_vec()).await;
    store.save(meta(&a, 2), b"a-v2".to_vec()).await;
    store.save(meta(&a, 3), b"a-v3".to_vec()).await;
    let latest = store.load(&a).await.expect("latest snapshot for a");
    assert_eq!(latest.0.sequence_nr, 3, "expected latest seq=3, got {}", latest.0.sequence_nr);
    assert_eq!(latest.1, b"a-v3", "latest payload mismatch");

    // 2. Saving for a different pid does not affect existing pid.
    store.save(meta(&b, 50), b"b-v50".to_vec()).await;
    let latest_a = store.load(&a).await.expect("a still present");
    assert_eq!(latest_a.0.sequence_nr, 3, "saving pid b leaked into pid a");
    let latest_b = store.load(&b).await.expect("b present");
    assert_eq!(latest_b.0.sequence_nr, 50);
    assert_eq!(latest_b.1, b"b-v50");

    // 3. delete(pid, max_seq) only removes entries <= max_seq.
    store.delete(&a, 2).await;
    let after = store.load(&a).await.expect("seq 3 should remain after delete<=2");
    assert!(after.0.sequence_nr > 2, "delete(<=2) leaked seq {}", after.0.sequence_nr);
    assert_eq!(after.0.sequence_nr, 3);
    // pid b must be untouched by delete on pid a.
    let b_unaffected = store.load(&b).await.expect("b unaffected by delete on a");
    assert_eq!(b_unaffected.0.sequence_nr, 50);

    // 4. delete() on an unknown pid is a no-op (no panic, no side effects).
    let unknown = format!("{pid_prefix}-xUnknown");
    store.delete(&unknown, u64::MAX).await;
    assert!(store.load(&unknown).await.is_none(), "unknown pid load must be None");
    // Existing pids still intact.
    assert_eq!(store.load(&a).await.expect("a intact").0.sequence_nr, 3);
    assert_eq!(store.load(&b).await.expect("b intact").0.sequence_nr, 50);

    // 5. Concurrent saves for the same pid must all succeed (no panic).
    let conc = format!("{pid_prefix}-xConc");
    let s1 = store.clone();
    let s2 = store.clone();
    let c1 = conc.clone();
    let c2 = conc.clone();
    let h1 = tokio::spawn(async move {
        for i in 1..=10u64 {
            s1.save(meta(&c1, i), vec![b'1', i as u8]).await;
        }
    });
    let h2 = tokio::spawn(async move {
        for i in 11..=20u64 {
            s2.save(meta(&c2, i), vec![b'2', i as u8]).await;
        }
    });
    h1.await.unwrap();
    h2.await.unwrap();
    // Per backend semantics, *some* snapshot must be visible after concurrent
    // saves. We don't assert which one wins (ordering is unspecified) only that
    // load returns a value whose seq came from one of the writers.
    let loaded = store.load(&conc).await.expect("a snapshot must be visible after concurrent saves");
    assert!(
        (1..=20u64).contains(&loaded.0.sequence_nr),
        "concurrent save produced unexpected seq {}",
        loaded.0.sequence_nr,
    );
}
