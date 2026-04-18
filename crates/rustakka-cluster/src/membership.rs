//! Membership state. akka.net: `Cluster/MembershipState.cs`.

use std::collections::BTreeSet;

use rustakka_core::actor::Address;
use serde::{Deserialize, Serialize};

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
}
