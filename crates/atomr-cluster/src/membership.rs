//! Membership state.

use std::collections::BTreeSet;

use atomr_core::actor::Address;
use serde::{Deserialize, Serialize};

use crate::events::ClusterEvent;
use crate::leader::{is_converged, next_status};
use crate::member::{Member, MemberStatus};
use crate::reachability::Reachability;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MembershipState {
    pub members: Vec<Member>,
    pub reachability: Reachability,
}

impl MembershipState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_or_update(&mut self, m: Member) {
        if let Some(existing) = self.members.iter_mut().find(|x| x.address == m.address) {
            *existing = m;
        } else {
            self.members.push(m);
        }
    }

    pub fn remove(&mut self, addr: &Address) {
        self.members.retain(|m| &m.address != addr);
    }

    pub fn up_members(&self) -> Vec<&Member> {
        self.members.iter().filter(|m| matches!(m.status, MemberStatus::Up)).collect()
    }

    pub fn unreachable_addresses(&self) -> BTreeSet<String> {
        self.members
            .iter()
            .filter(|m| !self.reachability.is_reachable(&m.address))
            .map(|m| m.address.to_string())
            .collect()
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Run the leader's per-tick transition logic against the current
    /// state. Returns the [`ClusterEvent`]s the daemon should publish
    /// (membership status flips, removals).
    ///
    /// Phase 6.C of `docs/full-port-plan.md`. Pure function — keeps
    /// the daemon actor itself trivial: collect events, then publish
    /// onto [`crate::events::ClusterEventBus`].
    pub fn apply_leader_actions(&mut self) -> Vec<ClusterEvent> {
        let converged = is_converged(self);
        let mut events = Vec::new();
        // First pass: compute transitions.
        let mut transitions: Vec<(Address, MemberStatus)> = Vec::new();
        for m in &self.members {
            if let Some(next) = next_status(m.status, converged) {
                transitions.push((m.address.clone(), next));
            }
        }
        // Second pass: apply + emit.
        for (addr, next) in transitions {
            if let Some(m) = self.members.iter_mut().find(|x| x.address == addr) {
                let prev = m.status;
                m.status = next;
                let updated = m.clone();
                let evt = match next {
                    MemberStatus::Up => ClusterEvent::MemberUp(updated.clone()),
                    MemberStatus::Exiting => ClusterEvent::MemberExited(updated.clone()),
                    MemberStatus::Removed => ClusterEvent::MemberRemoved(updated.clone(), prev),
                    _ => continue,
                };
                events.push(evt);
            }
        }
        // Drop members in `Removed` status (clean-up).
        self.members.retain(|m| m.status != MemberStatus::Removed);
        if converged {
            events.push(ClusterEvent::Convergence(true));
        }
        events
    }

    /// Insert `m` as a `Joining` member and emit the `MemberJoined`
    /// event for the daemon to publish.
    pub fn join(&mut self, m: Member) -> ClusterEvent {
        self.add_or_update(m.clone());
        ClusterEvent::MemberJoined(m)
    }

    /// Mark `addr` as leaving. Returns the published event if the
    /// transition was valid, `None` if no such member exists.
    pub fn leave(&mut self, addr: &Address) -> Option<ClusterEvent> {
        let m = self.members.iter_mut().find(|x| &x.address == addr)?;
        if matches!(m.status, MemberStatus::Up | MemberStatus::WeaklyUp) {
            m.status = MemberStatus::Leaving;
            return Some(ClusterEvent::MemberLeft(m.clone()));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove() {
        let mut s = MembershipState::new();
        let m = Member::new(Address::local("a"), vec![]);
        s.add_or_update(m.clone());
        assert_eq!(s.member_count(), 1);
        s.remove(&m.address);
        assert_eq!(s.member_count(), 0);
    }

    #[test]
    fn join_emits_member_joined() {
        let mut s = MembershipState::new();
        let evt = s.join(Member::new(Address::local("a"), vec![]));
        assert!(matches!(evt, ClusterEvent::MemberJoined(_)));
        assert_eq!(s.member_count(), 1);
    }

    #[test]
    fn leader_actions_promote_joining_to_up_when_converged() {
        let mut s = MembershipState::new();
        s.join(Member::new(Address::local("a"), vec![]));
        // Converged because every member is reachable; Joining→Up transitions.
        let events = s.apply_leader_actions();
        let names: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ClusterEvent::MemberUp(m) => Some(m.address.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(names, vec!["akka://a".to_string()]);
    }

    #[test]
    fn leader_actions_remove_down_members() {
        let mut s = MembershipState::new();
        let mut m = Member::new(Address::local("a"), vec![]);
        m.status = MemberStatus::Down;
        s.add_or_update(m);
        let _ = s.apply_leader_actions();
        assert_eq!(s.member_count(), 0);
    }

    #[test]
    fn leave_marks_up_member_as_leaving() {
        let mut s = MembershipState::new();
        let mut m = Member::new(Address::local("a"), vec![]);
        m.status = MemberStatus::Up;
        s.add_or_update(m);
        let evt = s.leave(&Address::local("a"));
        assert!(matches!(evt, Some(ClusterEvent::MemberLeft(_))));
    }

    #[test]
    fn leave_is_noop_for_unknown_member() {
        let mut s = MembershipState::new();
        let evt = s.leave(&Address::local("nope"));
        assert!(evt.is_none());
    }
}
