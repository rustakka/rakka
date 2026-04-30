//! Reachability. akka.net: `Cluster/Reachability.cs`.
//!
//! Records which observers think which subjects are reachable. A node is
//! considered unreachable if any observer reports it as such.

use std::collections::HashMap;

use rakka_core::actor::Address;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReachabilityStatus {
    Reachable,
    Unreachable,
    Terminated,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Reachability {
    pub records: HashMap<(Address, Address), ReachabilityStatus>,
}

impl Reachability {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn unreachable(&mut self, observer: Address, subject: Address) {
        self.records.insert((observer, subject), ReachabilityStatus::Unreachable);
    }

    pub fn reachable(&mut self, observer: Address, subject: Address) {
        self.records.insert((observer, subject), ReachabilityStatus::Reachable);
    }

    pub fn terminated(&mut self, observer: Address, subject: Address) {
        self.records.insert((observer, subject), ReachabilityStatus::Terminated);
    }

    pub fn status(&self, subject: &Address) -> ReachabilityStatus {
        let mut any_unreachable = false;
        for ((_, s), st) in &self.records {
            if s == subject {
                match st {
                    ReachabilityStatus::Terminated => return ReachabilityStatus::Terminated,
                    ReachabilityStatus::Unreachable => any_unreachable = true,
                    ReachabilityStatus::Reachable => {}
                }
            }
        }
        if any_unreachable { ReachabilityStatus::Unreachable } else { ReachabilityStatus::Reachable }
    }

    pub fn is_reachable(&self, subject: &Address) -> bool {
        matches!(self.status(subject), ReachabilityStatus::Reachable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marks_unreachable_then_reachable() {
        let mut r = Reachability::new();
        let a = Address::local("A");
        let b = Address::local("B");
        r.unreachable(a.clone(), b.clone());
        assert!(!r.is_reachable(&b));
        r.reachable(a.clone(), b.clone());
        assert!(r.is_reachable(&b));
    }
}
