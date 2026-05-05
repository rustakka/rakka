//! Phase Y — `DistributedPubSubMediatorSpec` parity sweep.
//!
//! Single-node parity check for the spec.
//! Covers the local-mediator invariants:
//!
//! * Subscribe + `publish_msg::<M>` delivers to a typed `ActorRef<M>`.
//! * Multiple subscribers on a topic each receive every published message.
//! * `subscribe_to_group` + `send_to_group` is point-to-point: only one
//!   group member receives each message, distributed round-robin.
//! * `unsubscribe(topic, path)` removes the subscriber from future
//!   delivery without affecting peers on the same topic.
//! * Publishing to an unknown topic returns 0 (no panic, no error).
//! * `topic_count` / `group_count` snapshot the mediator's bookkeeping.
//!
//! These tests use `Inbox` (synthetic actor) rather than the multi-node
//! barrier harness so each case completes in milliseconds.

use std::time::Duration;

use atomr_cluster_tools::DistributedPubSub;
use atomr_core::actor::Inbox;

/// Cap on how long any individual `Inbox::receive` is allowed to wait.
/// All deliveries here happen in-process and synchronously, so the timeout
/// only exists to bound the test if delivery is broken.
const RECV_DEADLINE: Duration = Duration::from_millis(100);

#[tokio::test]
async fn subscriber_receives_published_message() {
    let bus = DistributedPubSub::new();
    let mut inbox = Inbox::<u32>::new("solo");
    bus.subscribe("topic", inbox.actor_ref().clone());

    let delivered = bus.publish_msg("topic", 99u32);
    assert_eq!(delivered, 1, "single subscriber receives the publish");

    let got = inbox.receive(RECV_DEADLINE).await.expect("subscriber missed publish");
    assert_eq!(got, 99);
}

#[tokio::test]
async fn multiple_subscribers_all_receive_publish() {
    let bus = DistributedPubSub::new();
    let mut a = Inbox::<&'static str>::new("a");
    let mut b = Inbox::<&'static str>::new("b");
    let mut c = Inbox::<&'static str>::new("c");
    bus.subscribe("room", a.actor_ref().clone());
    bus.subscribe("room", b.actor_ref().clone());
    bus.subscribe("room", c.actor_ref().clone());

    let delivered = bus.publish_msg("room", "hi");
    assert_eq!(delivered, 3, "every subscriber gets the broadcast");

    assert_eq!(a.receive(RECV_DEADLINE).await.expect("a missed"), "hi");
    assert_eq!(b.receive(RECV_DEADLINE).await.expect("b missed"), "hi");
    assert_eq!(c.receive(RECV_DEADLINE).await.expect("c missed"), "hi");
}

#[tokio::test]
async fn group_subscribers_get_point_to_point_semantics() {
    // DistributedPubSubMediatorSpec — `Send` (group-routed) goes
    // to exactly one bucket member per call, not all of them.
    let bus = DistributedPubSub::new();
    let mut a = Inbox::<u32>::new("ga");
    let mut b = Inbox::<u32>::new("gb");
    bus.subscribe_to_group("work", "G1", a.actor_ref().clone());
    bus.subscribe_to_group("work", "G1", b.actor_ref().clone());

    // Single send → exactly one of them gets it; the other gets nothing.
    assert!(bus.send_to_group("work", "G1", 1u32));
    let a_first = a.receive(Duration::from_millis(20)).await.ok();
    let b_first = b.receive(Duration::from_millis(20)).await.ok();
    let total_first = a_first.is_some() as u32 + b_first.is_some() as u32;
    assert_eq!(total_first, 1, "exactly one group member receives a single send");

    // Round-robin: 4 sends → each member gets 2.
    for v in 0..4u32 {
        assert!(bus.send_to_group("work", "G1", v));
    }
    let mut a_count = 0;
    let mut b_count = 0;
    for _ in 0..4 {
        if a.receive(Duration::from_millis(20)).await.is_ok() {
            a_count += 1;
        }
        if b.receive(Duration::from_millis(20)).await.is_ok() {
            b_count += 1;
        }
    }
    assert_eq!(a_count, 2, "round-robin distributes evenly to a");
    assert_eq!(b_count, 2, "round-robin distributes evenly to b");
}

#[tokio::test]
async fn unsubscribe_removes_subscriber_from_future_delivery() {
    let bus = DistributedPubSub::new();
    let mut keep = Inbox::<u32>::new("keep");
    let drop_inbox = Inbox::<u32>::new("drop");
    bus.subscribe("chan", keep.actor_ref().clone());
    bus.subscribe("chan", drop_inbox.actor_ref().clone());

    // Both receive the first publish.
    assert_eq!(bus.publish_msg("chan", 1u32), 2);
    assert_eq!(keep.receive(RECV_DEADLINE).await.expect("keep missed first"), 1);
    // Drain the dropped subscriber's first delivery via a fresh inbox view.
    let mut drop_inbox = drop_inbox;
    assert_eq!(drop_inbox.receive(RECV_DEADLINE).await.expect("drop missed first"), 1);

    // Unsubscribe the second subscriber by its path.
    let drop_path = drop_inbox.actor_ref().path().clone();
    bus.unsubscribe("chan", &drop_path);

    // Next publish reaches only the remaining subscriber.
    assert_eq!(bus.publish_msg("chan", 2u32), 1, "only the surviving subscriber is counted");
    assert_eq!(keep.receive(RECV_DEADLINE).await.expect("keep missed second"), 2);
    assert!(
        drop_inbox.receive(Duration::from_millis(50)).await.is_err(),
        "unsubscribed actor must not receive further messages",
    );
}

#[tokio::test]
async fn publish_to_unknown_topic_is_a_noop() {
    let bus = DistributedPubSub::new();
    // No subscribers, no panic, zero deliveries.
    assert_eq!(bus.publish_msg("ghost", 0u32), 0);

    // Even with subscribers on *other* topics, the unknown topic stays empty.
    let inbox = Inbox::<u32>::new("other");
    bus.subscribe("real", inbox.actor_ref().clone());
    assert_eq!(bus.publish_msg("ghost", 1u32), 0);
    // And `send_to_group` against an unknown (topic, group) returns false.
    assert!(!bus.send_to_group("ghost", "anywhere", 1u32));
}

#[test]
fn topic_and_group_snapshots_track_bookkeeping() {
    let bus = DistributedPubSub::new();
    assert_eq!(bus.topic_count(), 0);
    assert_eq!(bus.group_count(), 0);

    let inbox = Inbox::<u32>::new("snap");
    bus.subscribe("t1", inbox.actor_ref().clone());
    bus.subscribe("t1", inbox.actor_ref().clone()); // same topic, two refs
    bus.subscribe("t2", inbox.actor_ref().clone());
    assert_eq!(bus.topic_count(), 2, "topic_count counts distinct topics");

    bus.subscribe_to_group("t1", "G1", inbox.actor_ref().clone());
    bus.subscribe_to_group("t1", "G2", inbox.actor_ref().clone());
    bus.subscribe_to_group("t2", "G1", inbox.actor_ref().clone());
    assert_eq!(bus.group_count(), 3, "group_count counts distinct (topic,group) buckets");

    // After dropping every subscriber under a topic, the topic key remains
    // (mirrors which keeps the topic actor alive); the snapshot
    // count is the number of `topic` keys, not non-empty subscriber lists.
    let path = inbox.actor_ref().path().clone();
    bus.unsubscribe("t1", &path);
    bus.unsubscribe("t1", &path);
    assert_eq!(
        bus.topic_count(),
        2,
        "topic_count is the count of registered topic keys, regardless of subscriber count",
    );
}
