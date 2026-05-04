//! Heartbeat sender — periodically updates the failure detector
//! with a synthetic heartbeat per peer.
//!
//! Phase 6.E of `docs/full-port-plan.md`. Akka.NET parity:
//! `Cluster/ClusterHeartbeatSender.cs`. The sender owns the per-peer
//! interval timer and feeds the local
//! [`crate::HeartbeatState`] book-keeping. The actual cross-node
//! heartbeat PDU exchange wires in once Phase 5.D's reader/writer
//! split + Phase 6.D's gossip transport land.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use atomr_core::actor::Address;
use parking_lot::RwLock;

/// Per-peer heartbeat record kept by the sender.
#[derive(Debug, Clone)]
pub struct PeerHeartbeat {
    /// Last time the local sender ticked for this peer.
    pub last_tick: Instant,
    /// Number of ticks emitted since the peer was added.
    pub ticks: u64,
}

/// In-memory heartbeat-sender state.
#[derive(Default)]
pub struct HeartbeatSender {
    interval: Duration,
    peers: RwLock<HashMap<String, PeerHeartbeat>>,
}

impl HeartbeatSender {
    pub fn new(interval: Duration) -> Arc<Self> {
        assert!(!interval.is_zero(), "heartbeat interval must be > 0");
        Arc::new(Self { interval, peers: RwLock::new(HashMap::new()) })
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Add a peer to the rotation.
    pub fn add_peer(&self, addr: &Address) {
        self.peers.write().insert(
            addr.to_string(),
            PeerHeartbeat {
                last_tick: Instant::now() - self.interval, // tick on first poll
                ticks: 0,
            },
        );
    }

    pub fn remove_peer(&self, addr: &Address) {
        self.peers.write().remove(&addr.to_string());
    }

    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// Run one tick — return the addresses whose `last_tick` is older
    /// than `interval`. The caller emits a heartbeat PDU to each and
    /// then calls [`Self::record_tick`].
    pub fn due_peers(&self, now: Instant) -> Vec<Address> {
        let g = self.peers.read();
        g.values()
            .filter_map(|hb| {
                if now.duration_since(hb.last_tick) >= self.interval {
                    Address::parse(&_addr_round_trip(&g, hb))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Bump the per-peer last-tick to `now` and increment the
    /// counter. No-op if the peer is unknown.
    pub fn record_tick(&self, addr: &Address, now: Instant) {
        let mut g = self.peers.write();
        if let Some(hb) = g.get_mut(&addr.to_string()) {
            hb.last_tick = now;
            hb.ticks += 1;
        }
    }

    /// Snapshot of (peer-address-string, ticks-emitted).
    pub fn ticks_per_peer(&self) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> =
            self.peers.read().iter().map(|(k, hb)| (k.clone(), hb.ticks)).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
}

// helper — recover the address string from a record by scanning the
// map. We don't carry the address inside `PeerHeartbeat` to keep that
// struct small; the map key is the canonical form.
fn _addr_round_trip(map: &HashMap<String, PeerHeartbeat>, target: &PeerHeartbeat) -> String {
    for (k, v) in map {
        if std::ptr::eq(v as *const PeerHeartbeat, target as *const PeerHeartbeat) {
            return k.clone();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_peer() {
        let s = HeartbeatSender::new(Duration::from_secs(1));
        let a = Address::local("a");
        s.add_peer(&a);
        assert_eq!(s.peer_count(), 1);
        s.remove_peer(&a);
        assert_eq!(s.peer_count(), 0);
    }

    #[test]
    fn record_tick_increments_count() {
        let s = HeartbeatSender::new(Duration::from_millis(10));
        let a = Address::local("a");
        s.add_peer(&a);
        let now = Instant::now();
        s.record_tick(&a, now);
        s.record_tick(&a, now);
        let snap = s.ticks_per_peer();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].1, 2);
    }

    #[test]
    fn due_peers_respects_interval() {
        let s = HeartbeatSender::new(Duration::from_secs(60));
        let a = Address::local("a");
        s.add_peer(&a);
        // Just-added peers tick on first poll (last_tick is in the past).
        let now = Instant::now();
        let due = s.due_peers(now);
        assert_eq!(due.len(), 1);
        // After recording a tick, they're not due again until interval passes.
        s.record_tick(&a, now);
        assert!(s.due_peers(now).is_empty());
    }
}
