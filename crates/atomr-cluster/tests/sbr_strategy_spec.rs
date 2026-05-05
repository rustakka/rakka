//! Split-brain resolver strategy parity.
//!
//! Each scenario constructs deterministic `Member` slices for the
//! reachable / unreachable sides of a hypothetical partition and
//! asserts the [`DowningDecision`] each strategy returns. These tests
//! exercise only public items in `atomr_cluster::sbr`.

use atomr_cluster::{
    DowningDecision, DowningStrategy, KeepMajorityStrategy, KeepOldestStrategy, KeepReferee,
    LeaseMajorityStrategy, Member, MemberStatus, SplitBrainResolver, StaticQuorumStrategy,
};
use atomr_core::actor::Address;

/// Build an `Up` member with a deterministic name and `up_number`.
fn up(name: &str, up_number: i32) -> Member {
    let mut m = Member::new(Address::local(name), vec![]);
    m.status = MemberStatus::Up;
    m.up_number = up_number;
    m
}

fn up_with_roles(name: &str, up_number: i32, roles: &[&str]) -> Member {
    let mut m = Member::new(Address::local(name), roles.iter().map(|s| s.to_string()).collect());
    m.status = MemberStatus::Up;
    m.up_number = up_number;
    m
}

/// Borrow a `&[Member]` as `Vec<&Member>` — the shape `decide` accepts.
fn refs(ms: &[Member]) -> Vec<&Member> {
    ms.iter().collect()
}

// ---------- KeepMajority ----------

#[test]
fn keep_majority_larger_reachable_side_survives() {
    let r = [up("a", 1), up("b", 2), up("c", 3)];
    let u = [up("d", 4), up("e", 5)];
    assert_eq!(KeepMajorityStrategy.decide(&refs(&r), &refs(&u)), DowningDecision::DownUnreachable);
}

#[test]
fn keep_majority_smaller_reachable_side_self_downs() {
    let r = [up("a", 1)];
    let u = [up("b", 2), up("c", 3)];
    assert_eq!(KeepMajorityStrategy.decide(&refs(&r), &refs(&u)), DowningDecision::DownSelf);
}

#[test]
fn keep_majority_equal_sides_down_all_as_tiebreak() {
    // atomr's deterministic tie-break: when both sides have equal `Up`
    // count we cannot pick a side, so `DownAll`. ships an
    // optional role-based tiebreak; atomr's stable rule is `DownAll`.
    let r = [up("a", 1), up("b", 2)];
    let u = [up("c", 3), up("d", 4)];
    assert_eq!(KeepMajorityStrategy.decide(&refs(&r), &refs(&u)), DowningDecision::DownAll);
}

#[test]
fn keep_majority_only_counts_up_status() {
    // Joining/Leaving on either side should not contribute to majority.
    let mut r = [up("a", 1), up("b", 2), up("c", 3)];
    r[2].status = MemberStatus::Joining;
    let u = [up("d", 4), up("e", 5)];
    // Up on r side: 2; Up on u side: 2 → DownAll.
    assert_eq!(KeepMajorityStrategy.decide(&refs(&r), &refs(&u)), DowningDecision::DownAll);
}

// ---------- StaticQuorum ----------

#[test]
fn static_quorum_at_quorum_size_survives() {
    let r = [up("a", 1), up("b", 2), up("c", 3)];
    let u = [up("d", 4), up("e", 5)];
    assert_eq!(
        StaticQuorumStrategy { quorum_size: 3 }.decide(&refs(&r), &refs(&u)),
        DowningDecision::DownUnreachable
    );
}

#[test]
fn static_quorum_below_quorum_size_self_downs() {
    let r = [up("a", 1), up("b", 2)];
    let u = [up("d", 4), up("e", 5), up("f", 6)];
    assert_eq!(
        StaticQuorumStrategy { quorum_size: 3 }.decide(&refs(&r), &refs(&u)),
        DowningDecision::DownSelf
    );
}

#[test]
fn static_quorum_both_sides_below_quorum_each_self_downs() {
    // The strategy is evaluated independently from each side's POV; if
    // both sides fail their quorum check, both choose `DownSelf`.
    let strat = StaticQuorumStrategy { quorum_size: 4 };
    let left = [up("a", 1), up("b", 2)];
    let right = [up("c", 3), up("d", 4)];

    // Side A perspective: A reachable, B unreachable.
    assert_eq!(strat.decide(&refs(&left), &refs(&right)), DowningDecision::DownSelf);
    // Side B perspective: B reachable, A unreachable.
    assert_eq!(strat.decide(&refs(&right), &refs(&left)), DowningDecision::DownSelf);
}

// ---------- KeepOldest ----------

#[test]
fn keep_oldest_side_with_lowest_up_number_survives() {
    // Oldest (up_number = 1) is in the reachable side.
    let r = [up("a", 1), up("b", 5)];
    let u = [up("c", 2), up("d", 3)];
    assert_eq!(KeepOldestStrategy::default().decide(&refs(&r), &refs(&u)), DowningDecision::DownUnreachable);
}

#[test]
fn keep_oldest_other_side_holds_oldest_self_downs() {
    let r = [up("a", 9), up("b", 7)];
    let u = [up("c", 1), up("d", 8)];
    assert_eq!(KeepOldestStrategy::default().decide(&refs(&r), &refs(&u)), DowningDecision::DownSelf);
}

#[test]
fn keep_oldest_only_unreachable_side_self_downs() {
    let r: [Member; 0] = [];
    let u = [up("c", 1)];
    assert_eq!(KeepOldestStrategy::default().decide(&refs(&r), &refs(&u)), DowningDecision::DownSelf);
}

#[test]
fn keep_oldest_alone_with_down_if_alone_downs_all() {
    // The oldest is on this side, but we are alone; with `down_if_alone`
    // the strategy refuses to keep a singleton oldest survivor.
    let r = [up("oldest", 1)];
    let u = [up("b", 2), up("c", 3)];
    let strat = KeepOldestStrategy { down_if_alone: true };
    assert_eq!(strat.decide(&refs(&r), &refs(&u)), DowningDecision::DownAll);
}

// ---------- KeepReferee ----------

#[test]
fn keep_referee_present_side_survives() {
    let r = [up("ref", 1), up("b", 2)];
    let u = [up("c", 3)];
    let strat = KeepReferee { referee: Address::local("ref").to_string(), down_all_if_less_than: 0 };
    assert_eq!(strat.decide(&refs(&r), &refs(&u)), DowningDecision::DownUnreachable);
}

#[test]
fn keep_referee_absent_side_self_downs() {
    let r = [up("a", 1), up("b", 2)];
    let u = [up("ref", 3)];
    let strat = KeepReferee { referee: Address::local("ref").to_string(), down_all_if_less_than: 0 };
    assert_eq!(strat.decide(&refs(&r), &refs(&u)), DowningDecision::DownSelf);
}

#[test]
fn keep_referee_below_minimum_size_downs_all() {
    // Referee is here, but we don't meet the configured minimum cluster
    // size, so the strategy escalates to `DownAll`.
    let r = [up("ref", 1)];
    let u = [up("b", 2)];
    let strat = KeepReferee { referee: Address::local("ref").to_string(), down_all_if_less_than: 3 };
    assert_eq!(strat.decide(&refs(&r), &refs(&u)), DowningDecision::DownAll);
}

// ---------- LeaseMajority ----------

#[test]
fn lease_majority_majority_decides_when_unambiguous() {
    let r = [up("a", 1), up("b", 2), up("c", 3)];
    let u = [up("d", 4)];
    // Lease state shouldn't matter: KeepMajority already picks a winner.
    let acquired = LeaseMajorityStrategy { lease_acquired: true };
    let denied = LeaseMajorityStrategy { lease_acquired: false };
    assert_eq!(acquired.decide(&refs(&r), &refs(&u)), DowningDecision::DownUnreachable);
    assert_eq!(denied.decide(&refs(&r), &refs(&u)), DowningDecision::DownUnreachable);
}

#[test]
fn lease_majority_tie_with_lease_acquired_survives() {
    // Equal sides — `KeepMajority` would `DownAll`. With the lease
    // acquired this side wins instead and downs the other side.
    let r = [up("a", 1), up("b", 2)];
    let u = [up("c", 3), up("d", 4)];
    let strat = LeaseMajorityStrategy { lease_acquired: true };
    assert_eq!(strat.decide(&refs(&r), &refs(&u)), DowningDecision::DownUnreachable);
}

#[test]
fn lease_majority_tie_without_lease_falls_back_to_down_all() {
    let r = [up("a", 1), up("b", 2)];
    let u = [up("c", 3), up("d", 4)];
    let strat = LeaseMajorityStrategy { lease_acquired: false };
    assert_eq!(strat.decide(&refs(&r), &refs(&u)), DowningDecision::DownAll);
}

#[test]
fn lease_majority_minority_self_downs_regardless_of_lease() {
    // Holding a lease cannot rescue a strict minority — the parity rule
    // is that the lease only breaks ties.
    let r = [up("a", 1)];
    let u = [up("c", 3), up("d", 4), up("e", 5)];
    let acquired = LeaseMajorityStrategy { lease_acquired: true };
    assert_eq!(acquired.decide(&refs(&r), &refs(&u)), DowningDecision::DownSelf);
}

// ---------- SplitBrainResolver facade ----------

#[test]
fn resolver_facade_delegates_to_inner_strategy() {
    let r = [up_with_roles("a", 1, &["frontend"]), up_with_roles("b", 2, &["frontend"])];
    let u = [up_with_roles("c", 3, &["backend"])];
    let resolver = SplitBrainResolver::new(Box::new(KeepMajorityStrategy));
    assert_eq!(resolver.decide(&refs(&r), &refs(&u)), DowningDecision::DownUnreachable);
}
