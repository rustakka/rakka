//! `atomr-pycluster` — placeholder sub-crate held for future per-wheel
//! carve-outs. The active Python bindings for the cluster control
//! plane (Phase 5 of the Python expansion plan) live in
//! [`atomr-pycore`](../atomr_pycore/index.html), exported under the
//! `atomr._native.cluster` submodule.
//!
//! This crate provides:
//!   * [`SbrStrategyName`] — string-form enum mirroring the
//!     `cluster.sbr.strategy` config keys recognized by `pycore`.
//!   * [`is_known_strategy`] — helper used by tests / config validators.
//!
//! Keeping these here means the `pycore` binding can re-export them and
//! a future `atomr_pycluster` wheel can pick them up directly without a
//! rename when sharded sub-wheels are introduced.

#![cfg_attr(not(test), allow(dead_code))]

/// Canonical SBR strategy names. Matches Akka's split-brain-resolver
/// `active-strategy` keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SbrStrategyName {
    KeepMajority,
    StaticQuorum,
    KeepOldest,
    DownAll,
    LeaseMajority,
}

impl SbrStrategyName {
    pub const ALL: &'static [SbrStrategyName] = &[
        SbrStrategyName::KeepMajority,
        SbrStrategyName::StaticQuorum,
        SbrStrategyName::KeepOldest,
        SbrStrategyName::DownAll,
        SbrStrategyName::LeaseMajority,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SbrStrategyName::KeepMajority => "keep-majority",
            SbrStrategyName::StaticQuorum => "static-quorum",
            SbrStrategyName::KeepOldest => "keep-oldest",
            SbrStrategyName::DownAll => "down-all",
            SbrStrategyName::LeaseMajority => "lease-majority",
        }
    }

    pub fn parse(s: &str) -> Option<SbrStrategyName> {
        match s {
            "keep-majority" => Some(SbrStrategyName::KeepMajority),
            "static-quorum" => Some(SbrStrategyName::StaticQuorum),
            "keep-oldest" => Some(SbrStrategyName::KeepOldest),
            "down-all" | "down-all-when-unstable" => Some(SbrStrategyName::DownAll),
            "lease-majority" => Some(SbrStrategyName::LeaseMajority),
            _ => None,
        }
    }
}

/// Convenience predicate used by tests / config validators.
pub fn is_known_strategy(s: &str) -> bool {
    SbrStrategyName::parse(s).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for name in SbrStrategyName::ALL {
            assert_eq!(SbrStrategyName::parse(name.as_str()), Some(*name));
        }
    }

    #[test]
    fn down_all_alias() {
        assert_eq!(SbrStrategyName::parse("down-all-when-unstable"), Some(SbrStrategyName::DownAll));
    }

    #[test]
    fn unknown_returns_none() {
        assert!(!is_known_strategy("not-a-strategy"));
    }
}
