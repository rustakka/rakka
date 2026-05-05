//! Cluster event spec parity.
//! `ClusterDomainEventSpec`, `ClusterDomainEventPublisherSpec`.
//!
//! Walks a member through Joining → WeaklyUp → Up → Leaving → Exited
//! → Removed and asserts the event stream the bus would publish.

use atomr_cluster::{ClusterEvent, ClusterEventBus, Member, MemberStatus};
use atomr_core::actor::Address;
use parking_lot::Mutex;
use std::sync::Arc;

fn addr(host: &str) -> Address {
    Address { protocol: "atomr".into(), system: "S".into(), host: Some(host.into()), port: Some(2552) }
}

fn member(host: &str, status: MemberStatus) -> Member {
    let mut m = Member::new(addr(host), Vec::new());
    m.status = status;
    m
}

#[test]
fn full_lifecycle_emits_expected_events() {
    let transitions = [
        (MemberStatus::Joining, MemberStatus::WeaklyUp),
        (MemberStatus::WeaklyUp, MemberStatus::Up),
        (MemberStatus::Up, MemberStatus::Leaving),
        (MemberStatus::Leaving, MemberStatus::Exiting),
        (MemberStatus::Exiting, MemberStatus::Removed),
    ];
    let m = member("a", MemberStatus::Joining);
    let mut events = Vec::new();
    for (old, new) in transitions {
        let mut after = m.clone();
        after.status = new;
        let _ = old;
        if let Some(e) = ClusterEvent::from_status_transition(after, old) {
            events.push(e);
        }
    }
    assert!(matches!(events[0], ClusterEvent::MemberWeaklyUp(_)));
    assert!(matches!(events[1], ClusterEvent::MemberUp(_)));
    assert!(matches!(events[2], ClusterEvent::MemberLeft(_)));
    assert!(matches!(events[3], ClusterEvent::MemberExited(_)));
    assert!(matches!(events[4], ClusterEvent::MemberRemoved(_, MemberStatus::Exiting)));
}

#[test]
fn no_event_when_status_unchanged() {
    let m = member("a", MemberStatus::Up);
    assert!(ClusterEvent::from_status_transition(m, MemberStatus::Up).is_none());
}

#[test]
fn bus_delivers_in_publish_order() {
    let bus = ClusterEventBus::new();
    let captured: Arc<Mutex<Vec<ClusterEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let c = captured.clone();
    let _h = bus.subscribe(move |e| c.lock().push(e.clone()));
    let a = member("a", MemberStatus::Up);
    let b = member("b", MemberStatus::Up);
    bus.publish(ClusterEvent::MemberUp(a.clone()));
    bus.publish(ClusterEvent::Convergence(true));
    bus.publish(ClusterEvent::MemberLeft(b.clone()));
    let got = captured.lock().clone();
    assert_eq!(got.len(), 3);
    assert!(matches!(got[0], ClusterEvent::MemberUp(_)));
    assert!(matches!(got[1], ClusterEvent::Convergence(true)));
    assert!(matches!(got[2], ClusterEvent::MemberLeft(_)));
}

#[test]
fn drop_handle_unsubscribes() {
    let bus = ClusterEventBus::new();
    let n = Arc::new(Mutex::new(0u32));
    let n2 = n.clone();
    let h = bus.subscribe(move |_| *n2.lock() += 1);
    bus.publish(ClusterEvent::Convergence(true));
    drop(h);
    bus.publish(ClusterEvent::Convergence(false));
    assert_eq!(*n.lock(), 1);
}
