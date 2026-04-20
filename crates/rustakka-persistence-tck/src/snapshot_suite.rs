//! Snapshot conformance suite.

use std::sync::Arc;

use rustakka_persistence::{SnapshotMetadata, SnapshotStore};

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
