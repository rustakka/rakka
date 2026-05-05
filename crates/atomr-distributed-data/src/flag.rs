//! `Flag` CRDT — monotonic boolean (false → true; once true, stays true).

use serde::{Deserialize, Serialize};

use crate::traits::CrdtMerge;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Flag {
    enabled: bool,
}

impl Flag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn switch_on(&mut self) {
        self.enabled = true;
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }
}

impl CrdtMerge for Flag {
    fn merge(&mut self, other: &Self) {
        self.enabled = self.enabled || other.enabled;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_is_logical_or() {
        let mut a = Flag::new();
        let mut b = Flag::new();
        b.switch_on();
        a.merge(&b);
        assert!(a.enabled());
    }

    #[test]
    fn flag_is_monotonic() {
        let mut a = Flag::new();
        a.switch_on();
        let b = Flag::new(); // false
        a.merge(&b);
        // Already-on stays on regardless of merge order.
        assert!(a.enabled());
    }
}
