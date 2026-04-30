//! Integration test wiring the cluster probe to live `rakka-cluster`
//! `Gossip` / `MembershipState` values.

#![cfg(feature = "cluster")]

use rakka_cluster::{Gossip, Member, MemberStatus};
use rakka_core::actor::Address;
use rakka_telemetry::cluster::{from_cluster_state, from_gossip, ClusterProbe};
use rakka_telemetry::bus::TelemetryBus;

#[test]
fn converts_membership_state_and_gossip() {
    let mut gossip = Gossip::new();
    gossip.tick("A");
    gossip.tick("B");
    let mut m1 = Member::new(Address::local("A"), vec!["worker".into()]);
    m1.status = MemberStatus::Up;
    gossip.state.add_or_update(m1);
    gossip
        .state
        .add_or_update(Member::new(Address::local("B"), vec!["metrics".into()]));

    let info = from_cluster_state(&gossip.state);
    assert_eq!(info.members.len(), 2);

    let full = from_gossip(&gossip);
    assert_eq!(full.gossip_version.len(), 2);
}

#[tokio::test]
async fn probe_emits_diff_on_update() {
    let bus = TelemetryBus::new(8);
    let mut rx = bus.subscribe();
    let probe = ClusterProbe::new(bus);

    let mut gossip = Gossip::new();
    gossip.tick("A");
    let mut m1 = Member::new(Address::local("A"), vec![]);
    m1.status = MemberStatus::Up;
    gossip.state.add_or_update(m1);

    probe.update_from_gossip(&gossip);
    let e = rx.recv().await.unwrap();
    assert_eq!(e.topic(), "cluster");

    let snap = probe.snapshot();
    assert_eq!(snap.members.len(), 1);
    assert_eq!(snap.members[0].roles.len(), 0);
}
