//! Gossip envelope.

use serde::{Deserialize, Serialize};

use crate::membership::MembershipState;
use crate::vector_clock::VectorClock;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Gossip {
    pub version: VectorClock,
    pub state: MembershipState,
}

impl Gossip {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment our local clock entry for `node`.
    pub fn tick(&mut self, node: &str) {
        self.version.tick(node);
    }

    /// Merge with another gossip, taking vector-clock max and union of members.
    pub fn merge(&self, other: &Self) -> Self {
        let mut merged = Self { version: self.version.merge(&other.version), ..Self::default() };
        for m in self.state.members.iter().chain(other.state.members.iter()) {
            merged.state.add_or_update(m.clone());
        }
        for ((a, b), st) in
            self.state.reachability.records.iter().chain(other.state.reachability.records.iter())
        {
            merged.state.reachability.records.insert((a.clone(), b.clone()), *st);
        }
        merged
    }
}

/// Snapshot of seen-by + unreachable info.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GossipOverview {
    pub seen_by: Vec<String>,
    pub reachability: crate::reachability::Reachability,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::member::Member;
    use atomr_core::actor::Address;

    #[test]
    fn merge_is_commutative_for_members() {
        let mut a = Gossip::new();
        a.tick("A");
        a.state.add_or_update(Member::new(Address::local("A"), vec![]));
        let mut b = Gossip::new();
        b.tick("B");
        b.state.add_or_update(Member::new(Address::local("B"), vec![]));
        let ab = a.merge(&b);
        let ba = b.merge(&a);
        assert_eq!(ab.state.member_count(), 2);
        assert_eq!(ab.state.member_count(), ba.state.member_count());
    }
}
