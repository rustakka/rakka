//! Cluster membership types.

use std::cmp::Ordering;

use atomr_core::actor::Address;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemberStatus {
    Joining,
    WeaklyUp,
    Up,
    Leaving,
    Exiting,
    Down,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub address: Address,
    pub up_number: i32,
    pub status: MemberStatus,
    pub roles: Vec<String>,
}

impl Member {
    pub fn new(address: Address, roles: Vec<String>) -> Self {
        Self { address, up_number: 0, status: MemberStatus::Joining, roles }
    }

    pub fn copy_with_status(&self, status: MemberStatus) -> Self {
        Self { status, ..self.clone() }
    }

    /// oldest first, then address.
    /// "Oldest" == lowest `up_number` (joined earliest).
    pub fn age_ordering(a: &Member, b: &Member) -> Ordering {
        a.up_number.cmp(&b.up_number).then_with(|| {
            // Address tie-break: protocol → host → port.
            a.address
                .protocol
                .cmp(&b.address.protocol)
                .then_with(|| a.address.host.cmp(&b.address.host))
                .then_with(|| a.address.port.cmp(&b.address.port))
        })
    }

    /// Sort `members` in age order in place.
    pub fn sort_by_age(members: &mut [Member]) {
        members.sort_by(Self::age_ordering);
    }

    /// Convenience: the oldest member of a slice (lowest up_number).
    pub fn oldest(members: &[Member]) -> Option<&Member> {
        members.iter().min_by(|a, b| Self::age_ordering(a, b))
    }
}
