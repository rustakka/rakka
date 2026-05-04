//! Cluster membership types. akka.net: `Cluster/Member.cs`.

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
}
