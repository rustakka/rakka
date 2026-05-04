//! Leader election.
//!
//! Phase 6 of `docs/full-port-plan.md`. Akka.NET's leader is the
//! lowest-address `Up`/`Leaving` member that's reachable from the
//! current node — deterministic given the gossip-converged
//! membership state. This module implements that pure function plus
//! the transition rules that fire on each gossip tick.
//!
//! The full state machine (`Joining → Up → Leaving → Exiting →
//! Removed`) lives here as helpers; the active driver that wires
//! these into the gossip loop is Phase 6.B.

use atomr_core::actor::Address;

use crate::member::{Member, MemberStatus};
use crate::membership::MembershipState;

/// Pick the deterministic leader from a [`MembershipState`].
///
/// Algorithm: among reachable members in the `Up` or `Leaving`
/// status, return the one with the lowest `Address` (lexicographic
/// over the `Display` form). Returns `None` if no eligible member
/// exists.
pub fn elect_leader(state: &MembershipState) -> Option<Address> {
    let mut eligible: Vec<&Member> = state
        .members
        .iter()
        .filter(|m| matches!(m.status, MemberStatus::Up | MemberStatus::Leaving))
        .filter(|m| state.reachability.is_reachable(&m.address))
        .collect();
    eligible.sort_by_key(|a| a.address.to_string());
    eligible.first().map(|m| m.address.clone())
}

/// Compute the next status for a member given the current convergence
/// state. Returns `None` if no transition applies.
///
/// * `Joining` → `Up` once convergence is reached and this member is
///   reachable from the leader.
/// * `Leaving` → `Exiting` once the leader sees the leave intent.
/// * `Exiting` → `Removed` once the leader-side cleanup completes.
/// * `Down` → `Removed` once the gossip purge interval elapses.
pub fn next_status(current: MemberStatus, converged: bool) -> Option<MemberStatus> {
    match (current, converged) {
        (MemberStatus::Joining, true) => Some(MemberStatus::Up),
        (MemberStatus::Leaving, true) => Some(MemberStatus::Exiting),
        (MemberStatus::Exiting, true) => Some(MemberStatus::Removed),
        (MemberStatus::Down, _) => Some(MemberStatus::Removed),
        _ => None,
    }
}

/// Convergence holds when every member that this node believes is
/// alive is also reachable. Akka.NET's gossip uses convergence as a
/// pre-condition for the leader's status-transition tick (the
/// leader won't move members from `Joining → Up` while a partition
/// is in flight).
///
/// Simplified vs. upstream: we don't track per-node "seen" sets yet,
/// so this is the local-view variant. A partitioned member shows up
/// as unreachable; once SBR (or a heartbeat recovery) resolves it,
/// convergence holds again. `Down` members don't block convergence
/// because they're already on the way out.
pub fn is_converged(state: &MembershipState) -> bool {
    state.members.iter().all(|m| {
        if matches!(m.status, MemberStatus::Down | MemberStatus::Removed) {
            return true;
        }
        state.reachability.is_reachable(&m.address)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(addr: &str, status: MemberStatus) -> Member {
        let mut m = Member::new(Address::local(addr), vec![]);
        m.status = status;
        m
    }

    #[test]
    fn leader_is_lowest_address_up_member() {
        let mut s = MembershipState::new();
        s.add_or_update(member("c", MemberStatus::Up));
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        assert_eq!(elect_leader(&s), Some(Address::local("a")));
    }

    #[test]
    fn leader_skips_non_up_members() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Joining));
        s.add_or_update(member("b", MemberStatus::Up));
        assert_eq!(elect_leader(&s), Some(Address::local("b")));
    }

    #[test]
    fn leader_skips_unreachable_members() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        // Mark "a" unreachable from "b".
        s.reachability.unreachable(Address::local("b"), Address::local("a"));
        assert_eq!(elect_leader(&s), Some(Address::local("b")));
    }

    #[test]
    fn no_leader_when_no_eligible_members() {
        let s = MembershipState::new();
        assert_eq!(elect_leader(&s), None);
    }

    #[test]
    fn next_status_transitions() {
        assert_eq!(next_status(MemberStatus::Joining, true), Some(MemberStatus::Up));
        assert_eq!(next_status(MemberStatus::Joining, false), None);
        assert_eq!(next_status(MemberStatus::Leaving, true), Some(MemberStatus::Exiting));
        assert_eq!(next_status(MemberStatus::Exiting, true), Some(MemberStatus::Removed));
        assert_eq!(next_status(MemberStatus::Down, false), Some(MemberStatus::Removed));
        assert_eq!(next_status(MemberStatus::Up, true), None);
    }

    #[test]
    fn convergence_holds_when_everyone_reachable() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Joining));
        assert!(is_converged(&s));
    }

    #[test]
    fn convergence_fails_when_a_member_is_unreachable() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        s.reachability.unreachable(Address::local("a"), Address::local("b"));
        assert!(!is_converged(&s));
    }

    #[test]
    fn down_members_do_not_block_convergence() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Down));
        // b is Down so its reachability doesn't matter for convergence.
        s.reachability.unreachable(Address::local("a"), Address::local("b"));
        assert!(is_converged(&s));
    }
}
