//! Multi-data-center awareness.
//!
//! Nodes carry `dc-default` / `dc-<name>` cluster roles. A node belongs to
//! exactly one data-center (or `default`); cross-DC heartbeats use a slow
//! path (longer interval, larger phi-accrual threshold) so transient WAN
//! latency doesn't trigger spurious downing.
//!
//! This module ships the pure helpers — DC extraction from
//! `Member.roles`, peer classification, and slow-path interval
//! selection. The wiring into `HeartbeatSender` (Phase 6.E) +
//! gossip dissemination (Phase 6.D) is a follow-on.

use std::time::Duration;

use crate::member::Member;

/// Convention: a member's data-center is encoded as a role of the
/// form `"dc-<name>"`. uses the same prefix.
pub const DC_ROLE_PREFIX: &str = "dc-";

/// Default DC name used when no `dc-*` role is present.
pub const DEFAULT_DC: &str = "default";

/// Extract the data-center name from a member's role list. Returns
/// [`DEFAULT_DC`] when no `dc-*` role is set.
pub fn member_dc(m: &Member) -> &str {
    for role in &m.roles {
        if let Some(rest) = role.strip_prefix(DC_ROLE_PREFIX) {
            return rest;
        }
    }
    DEFAULT_DC
}

/// `true` if `a` and `b` belong to the same data-center.
pub fn same_dc(a: &Member, b: &Member) -> bool {
    member_dc(a) == member_dc(b)
}

/// Slow-path settings used for cross-DC heartbeats / gossip.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct CrossDcSettings {
    /// Heartbeat interval for peers in a different DC.
    pub heartbeat_interval: Duration,
    /// Acceptable pause window for cross-DC peers (bigger than the
    /// in-DC default to absorb WAN jitter).
    pub acceptable_pause: Duration,
    /// Threshold to keep at most `n` cross-DC peers actively
    /// monitored.
    pub max_monitored_peers: usize,
}

impl Default for CrossDcSettings {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            acceptable_pause: Duration::from_secs(30),
            max_monitored_peers: 5,
        }
    }
}

/// Pick the heartbeat interval to use against `peer` from the
/// perspective of `local`: in-DC peers get `local_interval`,
/// cross-DC peers get `cross.heartbeat_interval`.
pub fn heartbeat_interval_for(
    local: &Member,
    peer: &Member,
    local_interval: Duration,
    cross: &CrossDcSettings,
) -> Duration {
    if same_dc(local, peer) {
        local_interval
    } else {
        cross.heartbeat_interval
    }
}

/// Partition a peer list into `(in_dc, cross_dc)` from `local`'s
/// perspective.
pub fn partition_by_dc<'a>(local: &Member, peers: &'a [Member]) -> (Vec<&'a Member>, Vec<&'a Member>) {
    let mut in_dc = Vec::new();
    let mut cross = Vec::new();
    for p in peers {
        if same_dc(local, p) {
            in_dc.push(p);
        } else {
            cross.push(p);
        }
    }
    (in_dc, cross)
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_core::actor::Address;

    fn member(addr: &str, roles: &[&str]) -> Member {
        Member::new(Address::local(addr), roles.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn member_dc_uses_dc_role_when_present() {
        let m = member("a", &["dc-eu-west", "compute"]);
        assert_eq!(member_dc(&m), "eu-west");
    }

    #[test]
    fn member_dc_defaults_when_missing() {
        let m = member("a", &["compute"]);
        assert_eq!(member_dc(&m), DEFAULT_DC);
    }

    #[test]
    fn same_dc_compares_correctly() {
        let a = member("a", &["dc-us"]);
        let b = member("b", &["dc-us"]);
        let c = member("c", &["dc-eu"]);
        assert!(same_dc(&a, &b));
        assert!(!same_dc(&a, &c));
    }

    #[test]
    fn heartbeat_interval_picks_cross_for_other_dc() {
        let a = member("a", &["dc-us"]);
        let b = member("b", &["dc-eu"]);
        let cross = CrossDcSettings::default();
        let interval = heartbeat_interval_for(&a, &b, Duration::from_secs(1), &cross);
        assert_eq!(interval, cross.heartbeat_interval);
    }

    #[test]
    fn heartbeat_interval_picks_local_for_same_dc() {
        let a = member("a", &["dc-us"]);
        let b = member("b", &["dc-us"]);
        let local = Duration::from_secs(1);
        let cross = CrossDcSettings::default();
        let interval = heartbeat_interval_for(&a, &b, local, &cross);
        assert_eq!(interval, local);
    }

    #[test]
    fn partition_splits_peers_correctly() {
        let local = member("self", &["dc-us"]);
        let peers = vec![
            member("a", &["dc-us"]),
            member("b", &["dc-eu"]),
            member("c", &["dc-us"]),
            member("d", &["dc-ap"]),
        ];
        let (same, cross) = partition_by_dc(&local, &peers);
        assert_eq!(same.len(), 2);
        assert_eq!(cross.len(), 2);
    }
}
