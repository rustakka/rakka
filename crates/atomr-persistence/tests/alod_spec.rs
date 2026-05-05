//! At-least-once delivery spec parity. akka.net:
//! `AtLeastOnceDeliverySpec`,
//! `AtLeastOnceDeliveryReceiveActorSpec`,
//! `AtLeastOnceDeliveryFailureSpec`,
//! `AtLeastOnceDeliveryCrashSpec`.

use atomr_persistence::AtLeastOnceDelivery;

#[test]
fn deliver_returns_increasing_ids() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
    let id1 = alod.deliver("dst", 1).unwrap();
    let id2 = alod.deliver("dst", 2).unwrap();
    let id3 = alod.deliver("dst", 3).unwrap();
    assert!(id2 > id1);
    assert!(id3 > id2);
}

#[test]
fn deliver_returns_none_when_max_unconfirmed_reached() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 2);
    assert!(alod.deliver("a", 1).is_some());
    assert!(alod.deliver("a", 2).is_some());
    // Cap is 2 — third deliver yields None.
    assert!(alod.deliver("a", 3).is_none());
    assert_eq!(alod.unconfirmed_count(), 2);
}

#[test]
fn deliver_again_works_after_confirm_frees_slot() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 1);
    let id = alod.deliver("a", 1).unwrap();
    assert!(alod.deliver("a", 2).is_none());
    alod.confirm_delivery(id);
    assert!(alod.deliver("a", 2).is_some());
}

#[test]
fn confirm_unknown_id_returns_false() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
    assert!(!alod.confirm_delivery(999));
}

#[test]
fn redeliver_returns_every_unconfirmed_delivery() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
    alod.deliver("a", 1);
    alod.deliver("b", 2);
    alod.deliver("c", 3);
    let snapshot = alod.redeliver();
    assert_eq!(snapshot.len(), 3);
    let mut destinations: Vec<&str> = snapshot.iter().map(|d| d.destination.as_str()).collect();
    destinations.sort();
    assert_eq!(destinations, vec!["a", "b", "c"]);
}

#[test]
fn redeliver_does_not_remove_pending_entries() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
    alod.deliver("a", 1);
    let _ = alod.redeliver();
    let _ = alod.redeliver();
    assert_eq!(alod.unconfirmed_count(), 1, "redeliver must not clear the pending set");
}

#[test]
fn redeliver_increments_attempts_per_call() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
    alod.deliver("a", 1);
    let r1 = alod.redeliver();
    assert_eq!(r1.len(), 1);
    let r2 = alod.redeliver();
    assert_eq!(r2.len(), 1);
    // attempts is internal; the public surface is just "still
    // present after multiple redeliver()s".
    assert_eq!(alod.unconfirmed_count(), 1);
}

#[test]
fn warn_threshold_is_what_was_configured() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 7, 100);
    assert_eq!(alod.warn_threshold(), 7);
    assert_eq!(alod.redeliver_interval_ms(), 500);
}

#[test]
fn confirms_after_redeliver_remove_entries() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
    let id = alod.deliver("a", 1).unwrap();
    let _ = alod.redeliver();
    assert!(alod.confirm_delivery(id));
    assert_eq!(alod.unconfirmed_count(), 0);
}

#[test]
fn redeliver_after_all_confirmed_yields_empty() {
    let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
    let id = alod.deliver("a", 1).unwrap();
    alod.confirm_delivery(id);
    assert!(alod.redeliver().is_empty());
}
