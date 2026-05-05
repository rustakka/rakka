//! BroadcastHub / MergeHub spec parity.

use atomr_streams::{BroadcastHub, MergeHub, Sink, Source};
use std::time::Duration;
use tokio::sync::mpsc;

// -- BroadcastHub --------------------------------------------------

#[tokio::test]
async fn broadcast_hub_every_consumer_sees_every_post_subscribe_element() {
    let hub = BroadcastHub::<i32>::new(16);
    // Three consumers all subscribed BEFORE attach.
    let c1 = hub.consumer();
    let c2 = hub.consumer();
    let c3 = hub.consumer();

    hub.attach(Source::from_iter(vec![1, 2, 3, 4]));

    // Drop the hub so consumers observe completion.
    drop(hub);

    let (a, b, c) = tokio::join!(Sink::collect(c1), Sink::collect(c2), Sink::collect(c3));
    assert_eq!(a, vec![1, 2, 3, 4]);
    assert_eq!(b, vec![1, 2, 3, 4]);
    assert_eq!(c, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn broadcast_hub_late_subscriber_misses_pre_subscribe_elements() {
    // Use an mpsc-driven source so we can interleave: send 1,2 → late
    // consumer subscribes → send 3,4 → close. The late consumer must
    // see only [3, 4].
    let hub = BroadcastHub::<i32>::new(16);
    let early = hub.consumer();

    let (tx, rx) = mpsc::unbounded_channel::<i32>();
    hub.attach(Source::from_receiver(rx));

    // Push first batch and give the pump task a chance to forward it.
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Late subscriber attaches AFTER 1,2 were broadcast.
    let late = hub.consumer();

    tx.send(3).unwrap();
    tx.send(4).unwrap();
    drop(tx);
    drop(hub);

    let (e, l) = tokio::join!(Sink::collect(early), Sink::collect(late));
    assert_eq!(e, vec![1, 2, 3, 4]);
    assert_eq!(l, vec![3, 4]);
}

#[tokio::test]
async fn broadcast_hub_upstream_completion_completes_all_consumers() {
    let hub = BroadcastHub::<i32>::new(16);
    let c1 = hub.consumer();
    let c2 = hub.consumer();
    hub.attach(Source::from_iter(vec![7, 8, 9]));

    // Drop hub so the only retained sender side is the spawned pump
    // task; once it finishes, both consumers must complete.
    drop(hub);

    let collect_both = async {
        let (a, b) = tokio::join!(Sink::collect(c1), Sink::collect(c2));
        (a, b)
    };
    let (a, b) = tokio::time::timeout(Duration::from_millis(200), collect_both)
        .await
        .expect("consumers should complete after upstream finishes");
    assert_eq!(a, vec![7, 8, 9]);
    assert_eq!(b, vec![7, 8, 9]);
}

#[tokio::test]
async fn broadcast_hub_supports_multiple_consumer_calls_independently() {
    // Calling .consumer() twice yields two independent views: dropping
    // one does not impact the other.
    let hub = BroadcastHub::<i32>::new(16);
    let keep = hub.consumer();
    {
        let _drop_me = hub.consumer();
        // _drop_me dropped at end of block.
    }
    assert_eq!(hub.consumer_count(), 1);

    hub.attach(Source::from_iter(vec![100, 200]));
    drop(hub);

    let got = Sink::collect(keep).await;
    assert_eq!(got, vec![100, 200]);
}

// -- MergeHub ------------------------------------------------------

#[tokio::test]
async fn merge_hub_aggregates_n_producers_exactly_once() {
    let hub = MergeHub::<i32>::new();
    hub.attach(Source::from_iter(vec![1, 2, 3]));
    hub.attach(Source::from_iter(vec![10, 20, 30]));
    hub.attach(Source::from_iter(vec![100, 200, 300]));

    let merged = hub.source();
    drop(hub);

    let mut got = Sink::collect(merged).await;
    got.sort();
    assert_eq!(got, vec![1, 2, 3, 10, 20, 30, 100, 200, 300]);
}

#[tokio::test]
async fn merge_hub_late_attached_producer_is_picked_up() {
    // Drive the merged source through an mpsc so we can interleave a
    // late attach() and confirm its elements still appear downstream.
    let hub = MergeHub::<i32>::new();

    let (tx_early, rx_early) = mpsc::unbounded_channel::<i32>();
    hub.attach(Source::from_receiver(rx_early));

    let merged = hub.source();
    let collector = tokio::spawn(async move { Sink::collect(merged).await });

    // Push from early producer.
    tx_early.send(1).unwrap();
    tx_early.send(2).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Late attach AFTER source() was already taken. This producer's
    // elements should still reach the merged source.
    hub.attach(Source::from_iter(vec![777]));
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(tx_early);
    drop(hub);

    let mut got = tokio::time::timeout(Duration::from_millis(500), collector)
        .await
        .expect("merged source must complete")
        .unwrap();
    got.sort();
    assert_eq!(got, vec![1, 2, 777]);
}

#[tokio::test]
async fn merge_hub_supports_multiple_attach_calls() {
    // Five attaches must all flow into the single merged source.
    let hub = MergeHub::<i32>::new();
    for i in 0..5 {
        hub.attach(Source::single(i));
    }
    let merged = hub.source();
    drop(hub);

    let mut got = Sink::collect(merged).await;
    got.sort();
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn merge_hub_second_source_call_yields_empty() {
    // The receiver only exists once: a second .source() call yields
    // an empty source (no panics, no double-take).
    let hub = MergeHub::<i32>::new();
    hub.attach(Source::from_iter(vec![1, 2, 3]));

    let _first = hub.source();
    let second = hub.source();

    let v = tokio::time::timeout(Duration::from_millis(50), Sink::collect(second)).await.unwrap_or_default();
    assert!(v.is_empty(), "second source() must be empty, got {:?}", v);
}
