//! Snapshot store retention spec parity.
//!
//! Maps the retention invariants onto the
//! [`atomr_persistence::SnapshotStore`] surface (`save` / `load` / `delete`)
//! using the in-memory store. Where exposes a fine-grained
//! `SnapshotSelectionCriteria { max_sequence_nr }` /
//! `LoadSnapshot(persistenceId, criteria, toSequenceNr)`, the rust trait
//! collapses retention onto a single
//! `delete(persistence_id, to_sequence_nr)` (deletes all snapshots whose
//! `sequence_nr <= to_sequence_nr`) and `load` always returns the most
//! recent snapshot — so the upper-bounded `LoadSnapshot` invariant is
//! re-expressed as: prune snapshots above the bound, then `load` returns
//! the highest-seq snapshot at-or-below it.
//!
//! Asserted invariants:
//!
//! * `save` then `load` round-trips the latest snapshot for a persistence id.
//! * After saving multiple snapshots, `load` returns the highest-seq snapshot.
//! * Snapshots for distinct persistence ids are isolated.
//! * `delete(pid, seq)` with `seq` equal to a single snapshot's sequence
//!   number removes that snapshot (and any older ones, which is a noop in
//!   the single-snapshot case).
//! * `delete(pid, max)` removes every snapshot whose `sequence_nr <= max`,
//!   leaving strictly newer snapshots intact and reachable via `load`.
//! * A "keep last N" pattern (caller-driven): save `N + k` snapshots, then
//!   `delete(pid, highest_seq - N)` — `load` returns the newest snapshot
//!   and the older ones are gone.
//! * `delete` against a never-saved persistence id is a no-op.
//! * `load` on an empty store returns `None`.
//! * `delete` past the highest snapshot empties the store for that pid.

use std::sync::Arc;

use atomr_persistence::{InMemorySnapshotStore, SnapshotMetadata, SnapshotStore};

fn meta(pid: &str, seq: u64) -> SnapshotMetadata {
    SnapshotMetadata { persistence_id: pid.into(), sequence_nr: seq, timestamp: seq }
}

async fn save(store: &Arc<InMemorySnapshotStore>, pid: &str, seq: u64, payload: &[u8]) {
    store.save(meta(pid, seq), payload.to_vec()).await;
}

#[tokio::test]
async fn save_then_load_round_trips_latest() {
    let store = InMemorySnapshotStore::new();
    save(&store, "p-1", 10, b"snap-10").await;

    let (m, payload) = store.load("p-1").await.expect("snapshot present");
    assert_eq!(m.persistence_id, "p-1");
    assert_eq!(m.sequence_nr, 10);
    assert_eq!(payload, b"snap-10");
}

#[tokio::test]
async fn load_on_empty_store_returns_none() {
    let store = InMemorySnapshotStore::new();
    assert!(store.load("ghost").await.is_none());
}

#[tokio::test]
async fn load_returns_highest_sequence_snapshot() {
    let store = InMemorySnapshotStore::new();
    save(&store, "p", 1, b"v1").await;
    save(&store, "p", 2, b"v2").await;
    save(&store, "p", 3, b"v3").await;

    let (m, payload) = store.load("p").await.unwrap();
    assert_eq!(m.sequence_nr, 3);
    assert_eq!(payload, b"v3");
}

#[tokio::test]
async fn snapshots_are_isolated_by_persistence_id() {
    let store = InMemorySnapshotStore::new();
    save(&store, "alpha", 5, b"alpha-5").await;
    save(&store, "beta", 7, b"beta-7").await;

    let (am, ap) = store.load("alpha").await.unwrap();
    assert_eq!(am.sequence_nr, 5);
    assert_eq!(ap, b"alpha-5");

    let (bm, bp) = store.load("beta").await.unwrap();
    assert_eq!(bm.sequence_nr, 7);
    assert_eq!(bp, b"beta-7");
}

#[tokio::test]
async fn delete_single_snapshot_removes_only_that_one() {
    // With one snapshot saved, `delete(pid, seq)` removes it; `load` is empty.
    let store = InMemorySnapshotStore::new();
    save(&store, "p", 4, b"only").await;

    store.delete("p", 4).await;

    assert!(store.load("p").await.is_none());
}

#[tokio::test]
async fn delete_with_bound_removes_everything_at_or_below_and_keeps_newer() {
    // Mirrors.
    let store = InMemorySnapshotStore::new();
    for seq in [1u64, 2, 3, 4, 5] {
        save(&store, "p", seq, format!("v{seq}").as_bytes()).await;
    }

    // Drop everything <= 3.
    store.delete("p", 3).await;

    // The remaining tip must be seq 5.
    let (m, payload) = store.load("p").await.unwrap();
    assert_eq!(m.sequence_nr, 5);
    assert_eq!(payload, b"v5");

    // Re-deleting at the same bound is a no-op (4 and 5 are still > 3).
    store.delete("p", 3).await;
    let (m2, _) = store.load("p").await.unwrap();
    assert_eq!(m2.sequence_nr, 5);
}

#[tokio::test]
async fn keep_last_n_pattern_via_caller_driven_delete() {
    // callers prune by `delete_snapshots(criteria { max_sequence_nr = highest - N })`
    // to retain the last N snapshots. Mapping onto the rust trait:
    // save N+1 snapshots, then `delete(pid, highest - N)` — that drops
    // everything at-or-below `highest - N` and keeps strictly newer ones.
    let store = InMemorySnapshotStore::new();
    let n: u64 = 3;
    let total: u64 = n + 2; // save 5 (sequences 1..=5), keep last 3 (3, 4, 5).

    for seq in 1..=total {
        save(&store, "p", seq, format!("v{seq}").as_bytes()).await;
    }

    // Delete everything <= total - n = 2 → keeps sequences 3, 4, 5.
    store.delete("p", total - n).await;

    // `load` always returns the latest → seq 5.
    let (m, payload) = store.load("p").await.unwrap();
    assert_eq!(m.sequence_nr, 5);
    assert_eq!(payload, b"v5");

    // Pruning further down to leave only the tip: delete <= 4.
    store.delete("p", 4).await;
    let (m2, _) = store.load("p").await.unwrap();
    assert_eq!(m2.sequence_nr, 5, "only the latest snapshot should remain");

    // Nuke the tip too.
    store.delete("p", 5).await;
    assert!(store.load("p").await.is_none());
}

#[tokio::test]
async fn delete_on_unknown_persistence_id_is_noop() {
    let store = InMemorySnapshotStore::new();
    save(&store, "real", 1, b"x").await;

    // Deleting against a non-existent pid must not affect other pids.
    store.delete("ghost", u64::MAX).await;

    let (m, _) = store.load("real").await.unwrap();
    assert_eq!(m.sequence_nr, 1);
    assert!(store.load("ghost").await.is_none());
}

#[tokio::test]
async fn delete_past_highest_clears_persistence_id() {
    let store = InMemorySnapshotStore::new();
    save(&store, "p", 2, b"a").await;
    save(&store, "p", 7, b"b").await;

    store.delete("p", u64::MAX).await;

    assert!(store.load("p").await.is_none());
}

#[tokio::test]
async fn save_after_delete_resumes_normally() {
    // Retention should not poison the slot — saving again is fine.
    let store = InMemorySnapshotStore::new();
    save(&store, "p", 1, b"v1").await;
    store.delete("p", 1).await;
    assert!(store.load("p").await.is_none());

    save(&store, "p", 9, b"v9").await;
    let (m, payload) = store.load("p").await.unwrap();
    assert_eq!(m.sequence_nr, 9);
    assert_eq!(payload, b"v9");
}
