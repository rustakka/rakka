//! MembershipState parity spec. akka.net: `Cluster.Tests.ClusterSpec`
//! and the `MembershipState` invariants under `Cluster/MembershipState.cs`.
//!
//! Focuses on the [`MembershipState`] public surface:
//! `add_or_update`, `remove`, `up_members`, `member_count`,
//! and the `Removed`-status invariant enforced by
//! `apply_leader_actions`.
//!
//! Notes on parity gaps (see Phase PP brief):
//! * akka.net's `MembersByStatus(MemberStatus)` lookup is currently
//!   only exposed for `Up` (via [`MembershipState::up_members`]).
//!   The general-status variant is not part of atomr's public API yet,
//!   so the spec only asserts the `Up` filter.
//! * akka.net's `Cluster.State.LeaderCandidates` does not have a
//!   matching free helper on [`MembershipState`]; atomr exposes
//!   single-leader election via the [`atomr_cluster::elect_leader`]
//!   function. The "leader candidates" assertion is therefore skipped
//!   here (documented in the Phase PP report).

use atomr_cluster::{Member, MemberStatus, MembershipState};
use atomr_core::actor::Address;

fn member(name: &str, status: MemberStatus) -> Member {
    let mut m = Member::new(Address::local(name), Vec::new());
    m.status = status;
    m
}

#[test]
fn add_or_update_inserts_new_member() {
    let mut s = MembershipState::new();
    assert_eq!(s.member_count(), 0);
    s.add_or_update(member("a", MemberStatus::Joining));
    assert_eq!(s.member_count(), 1);
    assert_eq!(s.members[0].address, Address::local("a"));
    assert_eq!(s.members[0].status, MemberStatus::Joining);
}

#[test]
fn add_or_update_replaces_existing_entry_for_same_address() {
    let mut s = MembershipState::new();
    s.add_or_update(member("a", MemberStatus::Joining));
    // Second add_or_update with the same address but a newer status
    // should replace the entry, not append a duplicate.
    s.add_or_update(member("a", MemberStatus::Up));
    assert_eq!(s.member_count(), 1, "duplicate address must not create a second entry");
    assert_eq!(s.members[0].status, MemberStatus::Up);
}

#[test]
fn member_count_reflects_visible_members() {
    let mut s = MembershipState::new();
    s.add_or_update(member("a", MemberStatus::Up));
    s.add_or_update(member("b", MemberStatus::Joining));
    s.add_or_update(member("c", MemberStatus::Leaving));
    assert_eq!(s.member_count(), 3);
}

#[test]
fn up_members_returns_only_up_status() {
    let mut s = MembershipState::new();
    s.add_or_update(member("a", MemberStatus::Up));
    s.add_or_update(member("b", MemberStatus::Joining));
    s.add_or_update(member("c", MemberStatus::Up));
    s.add_or_update(member("d", MemberStatus::Leaving));
    s.add_or_update(member("e", MemberStatus::Down));

    let mut up: Vec<String> = s.up_members().iter().map(|m| m.address.to_string()).collect();
    up.sort();
    assert_eq!(up, vec!["akka://a".to_string(), "akka://c".to_string()]);
    // And every returned member is actually Up.
    assert!(s.up_members().iter().all(|m| matches!(m.status, MemberStatus::Up)));
}

#[test]
fn up_members_is_empty_when_no_one_is_up() {
    let mut s = MembershipState::new();
    s.add_or_update(member("a", MemberStatus::Joining));
    s.add_or_update(member("b", MemberStatus::Down));
    assert!(s.up_members().is_empty());
}

#[test]
fn remove_drops_member_from_count() {
    let mut s = MembershipState::new();
    s.add_or_update(member("a", MemberStatus::Up));
    s.add_or_update(member("b", MemberStatus::Up));
    assert_eq!(s.member_count(), 2);
    s.remove(&Address::local("a"));
    assert_eq!(s.member_count(), 1);
    assert_eq!(s.members[0].address, Address::local("b"));
}

#[test]
fn remove_is_noop_for_unknown_address() {
    let mut s = MembershipState::new();
    s.add_or_update(member("a", MemberStatus::Up));
    s.remove(&Address::local("nope"));
    assert_eq!(s.member_count(), 1);
}

#[test]
fn member_transitioning_to_removed_disappears_from_count() {
    // akka.net invariant: members in `Removed` status are purged on
    // the next leader tick. atomr enforces this in
    // `MembershipState::apply_leader_actions`, which both transitions
    // `Down -> Removed` and drops `Removed` rows from `members`.
    let mut s = MembershipState::new();
    s.add_or_update(member("survivor", MemberStatus::Up));
    s.add_or_update(member("doomed", MemberStatus::Down));
    assert_eq!(s.member_count(), 2);

    let _events = s.apply_leader_actions();

    assert_eq!(s.member_count(), 1, "Removed members must not appear in member_count");
    assert_eq!(s.members[0].address, Address::local("survivor"));
}
