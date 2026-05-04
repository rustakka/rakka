//! Gossip dissemination PDUs.
//!
//! Phase 6.D of `docs/full-port-plan.md`. Akka.NET parity:
//! `Cluster/ClusterMessages.cs` — the `GossipStatus` /
//! `GossipEnvelope` / `GossipMerge` PDUs the leader exchanges with
//! peers on each tick. The actual transport plug-in (over
//! atomr-remote) wires up once Phase 5.D ships; this module
//! contains the typed shapes.

use atomr_core::actor::Address;
use serde::{Deserialize, Serialize};

use crate::membership::MembershipState;
use crate::vector_clock::{VectorClock, VectorRelation};

/// One gossip exchange. Sender hands a `GossipStatus` to the peer;
/// peer responds with either a full `GossipEnvelope` (if it has
/// newer state) or a `GossipMerge` request (if its state is older).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GossipPdu {
    /// "Here is my version vector — do you have newer state?"
    Status { from: String, version: VectorClock },
    /// "Here's my whole state, merge it in."
    Envelope { from: String, version: VectorClock, state: MembershipState },
    /// "I'm older than you — please send me your envelope."
    Merge { from: String, version: VectorClock },
}

/// Decision the receiver makes after comparing version vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GossipDecision {
    /// We're newer — send our envelope.
    SendEnvelope,
    /// We're older — request theirs.
    RequestMerge,
    /// Concurrent — merge both.
    MergeBoth,
    /// Identical — nothing to do.
    Same,
}

/// Pure decision function: given the local + remote version vectors,
/// what should we do?
pub fn decide(local: &VectorClock, remote: &VectorClock) -> GossipDecision {
    match local.compare(remote) {
        VectorRelation::Same => GossipDecision::Same,
        VectorRelation::After => GossipDecision::SendEnvelope,
        VectorRelation::Before => GossipDecision::RequestMerge,
        VectorRelation::Concurrent => GossipDecision::MergeBoth,
    }
}

/// Pick the next peer to gossip with, given a list of known peer
/// addresses and a per-tick cursor. Skips `self_addr`.
pub fn pick_gossip_target<'a>(
    peers: &'a [Address],
    self_addr: &Address,
    cursor: usize,
) -> Option<&'a Address> {
    if peers.is_empty() {
        return None;
    }
    let total = peers.len();
    for offset in 0..total {
        let p = &peers[(cursor + offset) % total];
        if p != self_addr {
            return Some(p);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vc(entries: &[(&str, u64)]) -> VectorClock {
        let mut v = VectorClock::new();
        for (node, n) in entries {
            for _ in 0..*n {
                v.tick(node);
            }
        }
        v
    }

    #[test]
    fn decide_same_when_equal() {
        let a = vc(&[("n1", 1), ("n2", 2)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(decide(&a, &b), GossipDecision::Same);
    }

    #[test]
    fn decide_send_envelope_when_local_is_newer() {
        let a = vc(&[("n1", 3), ("n2", 2)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(decide(&a, &b), GossipDecision::SendEnvelope);
    }

    #[test]
    fn decide_request_merge_when_local_is_older() {
        let a = vc(&[("n1", 1), ("n2", 2)]);
        let b = vc(&[("n1", 3), ("n2", 2)]);
        assert_eq!(decide(&a, &b), GossipDecision::RequestMerge);
    }

    #[test]
    fn decide_merge_when_concurrent() {
        let a = vc(&[("n1", 2), ("n2", 0)]);
        let b = vc(&[("n1", 0), ("n2", 2)]);
        assert_eq!(decide(&a, &b), GossipDecision::MergeBoth);
    }

    #[test]
    fn pick_gossip_target_skips_self() {
        let peers = vec![Address::local("a"), Address::local("b"), Address::local("c")];
        let self_addr = Address::local("b");
        let pick = pick_gossip_target(&peers, &self_addr, 1);
        // cursor=1 → peers[1]="b" → skipped → peers[2]="c"
        assert_eq!(pick, Some(&peers[2]));
    }

    #[test]
    fn pick_gossip_target_returns_none_when_only_self() {
        let peers = vec![Address::local("a")];
        let self_addr = Address::local("a");
        assert!(pick_gossip_target(&peers, &self_addr, 0).is_none());
    }

    #[test]
    fn pick_gossip_target_handles_empty() {
        let pick = pick_gossip_target(&[], &Address::local("x"), 0);
        assert!(pick.is_none());
    }

    #[test]
    fn pdus_serialize_round_trip() {
        let pdu = GossipPdu::Status { from: "node-1".into(), version: vc(&[("a", 1)]) };
        let cfg = bincode::config::standard();
        let bytes = bincode::serde::encode_to_vec(&pdu, cfg).unwrap();
        let (back, _): (GossipPdu, _) = bincode::serde::decode_from_slice(&bytes, cfg).unwrap();
        match back {
            GossipPdu::Status { from, .. } => assert_eq!(from, "node-1"),
            _ => panic!("expected Status"),
        }
    }
}
