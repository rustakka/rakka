//! Subscribe-surface + delta-CRDT propagation spec for the Replicator.
//! akka.net: `Akka.DistributedData.Tests.ReplicatorSpec` (subset of
//! invariants that exercise `Subscribe` / `Unsubscribe` and delta
//! merging on a single node).
//!
//! Each test runs in well under 2s on the in-process `Replicator`; no
//! actor system or remoting is required.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use atomr_distributed_data::{GCounter, Replicator};

/// `subscribe(key, callback)` invokes the callback when the key is
/// updated. Mirrors akka.net `Subscribe(key, subscriber)` →
/// `Changed(key)` delivery.
#[test]
fn subscribe_callback_fires_on_update() {
    let r = Replicator::new();
    let hits = Arc::new(AtomicU32::new(0));
    let hits_cb = hits.clone();
    let _token = r.subscribe("counter", move |key| {
        assert_eq!(key, "counter");
        hits_cb.fetch_add(1, Ordering::SeqCst);
    });

    let mut c = GCounter::new();
    c.increment("n1", 1);
    r.update("counter", c.clone());
    r.update("counter", c);

    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

/// The returned `SubscriptionToken` is RAII — dropping it removes the
/// subscription so subsequent updates are silent. Mirrors akka.net's
/// `Unsubscribe` semantics, but driven by Rust's ownership model.
#[test]
fn drop_token_silences_subsequent_updates() {
    let r = Replicator::new();
    let hits = Arc::new(AtomicU32::new(0));
    let hits_cb = hits.clone();
    let token = r.subscribe("k", move |_| {
        hits_cb.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(r.subscriber_count("k"), 1);
    r.update("k", GCounter::new());
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    drop(token);
    assert_eq!(r.subscriber_count("k"), 0);

    r.update("k", GCounter::new());
    r.update("k", GCounter::new());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "callback must not fire after token drop"
    );
}

/// Multiple subscribers on the same key all see every update — akka.net
/// fans out `Changed` to every registered subscriber.
#[test]
fn multiple_subscribers_all_see_update() {
    let r = Replicator::new();
    let a = Arc::new(AtomicU32::new(0));
    let b = Arc::new(AtomicU32::new(0));
    let c = Arc::new(AtomicU32::new(0));

    let a_cb = a.clone();
    let b_cb = b.clone();
    let c_cb = c.clone();

    let _t1 = r.subscribe("k", move |_| {
        a_cb.fetch_add(1, Ordering::SeqCst);
    });
    let _t2 = r.subscribe("k", move |_| {
        b_cb.fetch_add(1, Ordering::SeqCst);
    });
    let _t3 = r.subscribe("k", move |_| {
        c_cb.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(r.subscriber_count("k"), 3);
    r.update("k", GCounter::new());

    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);
    assert_eq!(c.load(Ordering::SeqCst), 1);
}

/// Subscribers for one key never see updates for a different key.
/// akka.net registers subscriptions per-key; cross-key delivery would
/// be a leak.
#[test]
fn subscribers_are_scoped_to_their_key() {
    let r = Replicator::new();
    let on_a = Arc::new(AtomicU32::new(0));
    let on_b = Arc::new(AtomicU32::new(0));

    let on_a_cb = on_a.clone();
    let on_b_cb = on_b.clone();
    let _ta = r.subscribe("a", move |key| {
        assert_eq!(key, "a");
        on_a_cb.fetch_add(1, Ordering::SeqCst);
    });
    let _tb = r.subscribe("b", move |key| {
        assert_eq!(key, "b");
        on_b_cb.fetch_add(1, Ordering::SeqCst);
    });

    r.update("a", GCounter::new());
    r.update("a", GCounter::new());
    r.update("b", GCounter::new());
    r.update("c", GCounter::new()); // no subscriber — silent.

    assert_eq!(on_a.load(Ordering::SeqCst), 2);
    assert_eq!(on_b.load(Ordering::SeqCst), 1);
}

/// `delete(key)` notifies subscribers and the post-delete `get` returns
/// `None`. akka.net's replicator emits `Deleted` to subscribers; we
/// model both: the callback is invoked, and the entry is gone.
#[test]
fn delete_notifies_subscribers_and_clears_value() {
    let r = Replicator::new();
    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = received.clone();

    let _t = r.subscribe("k", move |key| {
        received_cb.lock().unwrap().push(key.to_string());
    });

    let mut c = GCounter::new();
    c.increment("n1", 7);
    r.update("k", c);
    assert_eq!(r.get::<GCounter>("k").map(|g| g.value()), Some(7));

    r.delete("k");

    // Subscriber was notified for the update and the delete.
    let log = received.lock().unwrap().clone();
    assert_eq!(log, vec!["k".to_string(), "k".to_string()]);

    // Post-delete read returns None — entry is removed.
    assert!(r.get::<GCounter>("k").is_none());
}

/// Delta-CRDT propagation: two `GCounter`s with disjoint per-node
/// deltas merge into the replicator and `get` returns the merged sum.
/// Matches akka.net's invariant that delta-propagated GCounters
/// converge to the join of their per-node states.
#[test]
fn delta_crdt_disjoint_increments_merge_to_sum() {
    let r = Replicator::new();

    // Node "n1" emits +3 on its own counter.
    let mut from_n1 = GCounter::new();
    from_n1.increment("n1", 3);

    // Node "n2" emits +5 on its own counter (disjoint dot from "n1").
    let mut from_n2 = GCounter::new();
    from_n2.increment("n2", 5);

    let key = "shared";
    r.update(key, from_n1);
    let after_first: GCounter = r.get(key).unwrap();
    assert_eq!(after_first.value(), 3);

    r.update(key, from_n2);
    let merged: GCounter = r.get(key).unwrap();
    assert_eq!(
        merged.value(),
        3 + 5,
        "disjoint per-node deltas must merge to the sum"
    );
}

/// Reordered delta application converges to the same merged sum, and
/// re-applying an already-merged delta is idempotent (per-node max
/// semantics for full-state merge).
#[test]
fn delta_merge_is_order_independent_and_idempotent() {
    let r1 = Replicator::new();
    let r2 = Replicator::new();

    let mut a = GCounter::new();
    a.increment("n1", 2);
    let mut b = GCounter::new();
    b.increment("n2", 4);
    let mut c = GCounter::new();
    c.increment("n3", 1);

    // r1: a then b then c.
    r1.update("k", a.clone());
    r1.update("k", b.clone());
    r1.update("k", c.clone());

    // r2: c then a then b — different order.
    r2.update("k", c);
    r2.update("k", a.clone());
    r2.update("k", b.clone());

    // Re-apply `a` to r2 — full-state merge takes per-node max, so
    // value is unchanged.
    r2.update("k", a);

    let v1: GCounter = r1.get("k").unwrap();
    let v2: GCounter = r2.get("k").unwrap();
    assert_eq!(v1.value(), v2.value());
    assert_eq!(v1.value(), 2 + 4 + 1);
}
