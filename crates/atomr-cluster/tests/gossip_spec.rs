//! Gossip + decision spec parity. akka.net: `GossipSpec`,
//! `HeartbeatNodeRingSpec` (subset of decision logic).

use atomr_cluster::{
    gossip_decide, pick_gossip_target, Gossip, GossipDecision, Member, VectorClock,
};
use atomr_core::actor::Address;

fn vc(pairs: &[(&str, u64)]) -> VectorClock {
    let mut v = VectorClock::new();
    for (k, n) in pairs {
        for _ in 0..*n {
            v.tick(k);
        }
    }
    v
}

#[test]
fn decide_same_clocks_emit_same() {
    let a = vc(&[("n1", 2), ("n2", 1)]);
    let b = vc(&[("n1", 2), ("n2", 1)]);
    assert_eq!(gossip_decide(&a, &b), GossipDecision::Same);
}

#[test]
fn decide_local_strictly_after_sends_envelope() {
    let local = vc(&[("n1", 5), ("n2", 3)]);
    let remote = vc(&[("n1", 4), ("n2", 3)]);
    assert_eq!(gossip_decide(&local, &remote), GossipDecision::SendEnvelope);
}

#[test]
fn decide_local_strictly_before_requests_merge() {
    let local = vc(&[("n1", 1)]);
    let remote = vc(&[("n1", 5), ("n2", 7)]);
    assert_eq!(gossip_decide(&local, &remote), GossipDecision::RequestMerge);
}

#[test]
fn decide_concurrent_clocks_merge_both() {
    let local = vc(&[("n1", 5), ("n2", 1)]);
    let remote = vc(&[("n1", 1), ("n2", 5)]);
    assert_eq!(gossip_decide(&local, &remote), GossipDecision::MergeBoth);
}

#[test]
fn merge_is_commutative_on_members() {
    let mut a = Gossip::new();
    a.tick("A");
    a.state.add_or_update(Member::new(Address::local("A"), vec![]));
    let mut b = Gossip::new();
    b.tick("B");
    b.state.add_or_update(Member::new(Address::local("B"), vec![]));
    let ab = a.merge(&b);
    let ba = b.merge(&a);
    assert_eq!(ab.state.member_count(), ba.state.member_count());
    assert_eq!(ab.state.member_count(), 2);
}

#[test]
fn merge_is_idempotent() {
    let mut a = Gossip::new();
    a.tick("A");
    a.state.add_or_update(Member::new(Address::local("A"), vec![]));
    let aa = a.merge(&a);
    assert_eq!(aa.state.member_count(), 1);
}

#[test]
fn pick_gossip_target_returns_none_when_pool_empty() {
    let pool: Vec<Address> = vec![];
    let me = Address::local("self");
    let pick = pick_gossip_target(&pool, &me, 0);
    assert!(pick.is_none());
}

#[test]
fn pick_gossip_target_excludes_self() {
    let me = Address::local("self");
    let pool = vec![me.clone(), Address::local("peer-a"), Address::local("peer-b")];
    let pick = pick_gossip_target(&pool, &me, 0);
    assert!(matches!(pick, Some(p) if p != &me));
}

#[test]
fn pick_gossip_target_deterministic_with_same_cursor() {
    let me = Address::local("self");
    let pool = vec![Address::local("a"), Address::local("b"), Address::local("c")];
    let p1 = pick_gossip_target(&pool, &me, 1);
    let p2 = pick_gossip_target(&pool, &me, 1);
    assert_eq!(p1, p2);
}

#[test]
fn pick_gossip_target_returns_none_when_only_self() {
    let me = Address::local("only");
    let pool = vec![me.clone()];
    assert!(pick_gossip_target(&pool, &me, 0).is_none());
}
