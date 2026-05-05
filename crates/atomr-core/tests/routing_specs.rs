//! Routing spec parity. RoundRobinSpec, RandomSpec,
//! ConsistentHashingRouterSpec, ScatterGatherFirstCompletedSpec,
//! TailChoppingSpec.

use std::time::Duration;

use atomr_core::actor::{ActorRef, Inbox};
use atomr_core::routing::{
    BroadcastRouter, ConsistentHashRouter, RandomRouter, RoundRobinRouter, ScatterGatherFirstCompletedRouter,
    TailChoppingRouter,
};

fn refs<const N: usize>(prefix: &str) -> ([Inbox<u32>; N], Vec<ActorRef<u32>>) {
    let inboxes: [Inbox<u32>; N] = std::array::from_fn(|i| Inbox::new(&format!("{prefix}-{i}")));
    let refs: Vec<ActorRef<u32>> = inboxes.iter().map(|i| i.actor_ref().clone()).collect();
    (inboxes, refs)
}

#[tokio::test]
async fn round_robin_distributes_evenly_in_order() {
    let (mut inboxes, refs) = refs::<3>("rr");
    let r = RoundRobinRouter::new(refs);
    for i in 0..6u32 {
        r.route(i);
    }
    // Each inbox receives 2 messages in order.
    for (i, inbox) in inboxes.iter_mut().enumerate() {
        let v1 = inbox.receive(Duration::from_millis(50)).await.unwrap();
        let v2 = inbox.receive(Duration::from_millis(50)).await.unwrap();
        assert_eq!(v1, i as u32);
        assert_eq!(v2, (i + 3) as u32);
    }
}

#[tokio::test]
async fn round_robin_with_no_routees_drops() {
    let r: RoundRobinRouter<u32> = RoundRobinRouter::new(Vec::new());
    r.route(1); // must not panic
}

#[tokio::test]
async fn random_router_delivers_to_some_routee() {
    let (mut inboxes, refs) = refs::<3>("rand");
    let r = RandomRouter::new(refs);
    for i in 0..30u32 {
        r.route(i);
    }
    // At least one inbox got something.
    let mut any = false;
    for inbox in inboxes.iter_mut() {
        if inbox.receive(Duration::from_millis(10)).await.is_ok() {
            any = true;
        }
    }
    assert!(any);
}

#[tokio::test]
async fn consistent_hash_routes_same_key_to_same_routee() {
    let (mut inboxes, refs) = refs::<4>("ch");
    let r = ConsistentHashRouter::new(refs, 32);
    for _ in 0..10u32 {
        r.route_by_key("key-A", 1);
    }
    // Find which inbox got the messages.
    let mut counts = vec![0u32; inboxes.len()];
    for (i, inbox) in inboxes.iter_mut().enumerate() {
        while inbox.receive(Duration::from_millis(5)).await.is_ok() {
            counts[i] += 1;
        }
    }
    // Exactly one inbox should hold all 10 messages.
    let nonzero: Vec<&u32> = counts.iter().filter(|c| **c > 0).collect();
    assert_eq!(nonzero.len(), 1);
    assert_eq!(*nonzero[0], 10);
}

#[tokio::test]
async fn consistent_hash_virtual_nodes_factor_visible() {
    let (_inboxes, refs) = refs::<3>("ch-vn");
    let r = ConsistentHashRouter::new(refs, 64);
    assert_eq!(r.virtual_nodes(), 64);
}

#[tokio::test]
async fn scatter_gather_constructs() {
    let (_inboxes, refs) = refs::<3>("sg");
    let _r = ScatterGatherFirstCompletedRouter::new(refs, Duration::from_millis(50));
}

#[tokio::test]
async fn tail_chopping_routee_count_matches_input() {
    let (_inboxes, refs) = refs::<5>("tc");
    let r = TailChoppingRouter::new(refs, Duration::from_millis(10), Duration::from_millis(50));
    assert_eq!(r.routee_count(), 5);
    assert!(r.max_attempts() >= 1);
}

#[tokio::test]
async fn tail_chopping_empty_router_has_no_next_attempt() {
    let r: TailChoppingRouter<u32> =
        TailChoppingRouter::new(Vec::new(), Duration::from_millis(10), Duration::from_millis(50));
    assert!(r.next_attempt().is_none());
    assert_eq!(r.routee_count(), 0);
}

#[tokio::test]
async fn broadcast_router_delivers_to_every_routee() {
    let (mut inboxes, refs) = refs::<3>("bc");
    let r = BroadcastRouter::new(refs);
    r.route(7);
    for inbox in inboxes.iter_mut() {
        let v = inbox.receive(Duration::from_millis(50)).await.unwrap();
        assert_eq!(v, 7);
    }
}
