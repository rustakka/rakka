//! Heartbeat sender + state spec parity.
//! `ClusterHeartbeatSenderSpec`, `ClusterHeartBeatSenderStateSpec`,
//! and `HeartbeatNodeRingSpec`.
//!
//! These fixtures pin down the user-visible invariants of the
//! [`HeartbeatSender`] tick loop and the [`HeartbeatState`]
//! per-peer book-keeping. The atomr public API exposes
//! `due_peers` + `record_tick` (rather than a single `tick`
//! that mutates) and `HeartbeatState::heartbeat(addr)` (rather
//! than `record(peer, now)`); the assertions below mirror the
//! invariants by composing those primitives.
//!
//! Note: `HeartbeatNodeRingSpec` in pins the
//! deterministic peer-ring used to pick *which* peers a node
//! sends to. atomr's current sender simply heartbeats every
//! known peer (the ring filter is a TODO once larger clusters
//! land); we still assert that adding/removing peers
//! deterministically expands and contracts the target set.

use std::time::{Duration, Instant};

use atomr_cluster::{HeartbeatSender, HeartbeatState, PeerHeartbeat};
use atomr_core::actor::Address;

fn addr(host: &str, port: u16) -> Address {
    Address::remote("atomr.tcp", "Sys", host, port)
}

// -- HeartbeatSender / HeartbeatNodeRingSpec ---------------------------

#[test]
fn tick_targets_all_currently_known_peers() {
    let s = HeartbeatSender::new(Duration::from_millis(50));
    let a = addr("a", 1);
    let b = addr("b", 2);
    let c = addr("c", 3);
    s.add_peer(&a);
    s.add_peer(&b);
    s.add_peer(&c);

    // Newly-added peers tick on the very first poll because their
    // synthetic last-tick is set to `now - interval`.
    let due = s.due_peers(Instant::now());
    let mut got: Vec<String> = due.iter().map(|d| d.to_string()).collect();
    got.sort();
    assert_eq!(
        got,
        vec![a.to_string(), b.to_string(), c.to_string()],
        "tick should target every known peer on first poll"
    );
}

#[test]
fn adding_a_peer_expands_the_target_ring() {
    let s = HeartbeatSender::new(Duration::from_millis(50));
    let a = addr("a", 1);
    s.add_peer(&a);
    assert_eq!(s.peer_count(), 1);
    assert_eq!(s.due_peers(Instant::now()).len(), 1);

    let b = addr("b", 2);
    s.add_peer(&b);
    assert_eq!(s.peer_count(), 2);
    assert_eq!(s.due_peers(Instant::now()).len(), 2);
}

#[test]
fn removing_a_peer_contracts_the_target_ring() {
    let s = HeartbeatSender::new(Duration::from_millis(50));
    let a = addr("a", 1);
    let b = addr("b", 2);
    s.add_peer(&a);
    s.add_peer(&b);
    assert_eq!(s.peer_count(), 2);

    s.remove_peer(&a);
    assert_eq!(s.peer_count(), 1);
    let due = s.due_peers(Instant::now());
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].to_string(), b.to_string());
}

#[test]
fn record_tick_marks_peer_not_due_until_interval_elapses() {
    let interval = Duration::from_millis(200);
    let s = HeartbeatSender::new(interval);
    let a = addr("a", 1);
    s.add_peer(&a);

    let t0 = Instant::now();
    assert_eq!(s.due_peers(t0).len(), 1, "fresh peer is due immediately");
    s.record_tick(&a, t0);
    assert!(s.due_peers(t0).is_empty(), "just-recorded peer is not due");

    let later = t0 + interval / 2;
    assert!(s.due_peers(later).is_empty(), "still not due before interval");

    let after = t0 + interval;
    assert_eq!(s.due_peers(after).len(), 1, "due once interval elapses");
}

#[test]
fn record_tick_for_unknown_peer_is_a_noop() {
    let s = HeartbeatSender::new(Duration::from_millis(50));
    let ghost = addr("ghost", 9);
    // No add_peer — recording must not panic, must not create state.
    s.record_tick(&ghost, Instant::now());
    assert_eq!(s.peer_count(), 0);
    assert!(s.ticks_per_peer().is_empty());
}

#[test]
fn ticks_per_peer_counts_emitted_heartbeats() {
    let s = HeartbeatSender::new(Duration::from_millis(50));
    let a = addr("a", 1);
    let b = addr("b", 2);
    s.add_peer(&a);
    s.add_peer(&b);

    let now = Instant::now();
    s.record_tick(&a, now);
    s.record_tick(&a, now);
    s.record_tick(&b, now);

    let snap = s.ticks_per_peer();
    assert_eq!(snap.len(), 2);
    let by_key: std::collections::HashMap<_, _> = snap.into_iter().collect();
    assert_eq!(by_key[&a.to_string()], 2);
    assert_eq!(by_key[&b.to_string()], 1);
}

// -- ClusterHeartbeatSenderSpec — PeerHeartbeat carries the address ----
//
// In the wire-level `Heartbeat` PDU carries the sender's
// `UniqueAddress`. atomr's sender keeps the address as the map key
// (canonical string form) and the per-peer record holds the
// last-tick + count. We assert that, given a `due_peers` result, the
// caller can reconstruct the matching `PeerHeartbeat` and that the
// address round-trips cleanly through `Address::parse`.

#[test]
fn due_peers_addresses_round_trip_through_parse() {
    let s = HeartbeatSender::new(Duration::from_millis(50));
    let a = addr("a", 1);
    s.add_peer(&a);

    let due = s.due_peers(Instant::now());
    assert_eq!(due.len(), 1);
    assert_eq!(due[0], a, "tick payload addresses the sender's peer");
    // And the canonical string form parses back to the same address.
    let reparsed = Address::parse(&due[0].to_string()).expect("parse");
    assert_eq!(reparsed, a);
}

#[test]
fn peer_heartbeat_record_is_constructible_and_clonable() {
    // Smoke: the public PeerHeartbeat struct is part of the API so
    // downstream cluster wiring can build/inspect records.
    let now = Instant::now();
    let hb = PeerHeartbeat { last_tick: now, ticks: 7 };
    let cloned = hb.clone();
    assert_eq!(cloned.ticks, 7);
    assert_eq!(cloned.last_tick, now);
}

// -- ClusterHeartBeatSenderStateSpec — HeartbeatState ------------------

#[test]
fn heartbeat_state_records_per_peer_last_seen() {
    let mut state = HeartbeatState::new();
    let a = addr("a", 1);
    state.heartbeat(a.clone());
    assert!(state.detectors.contains_key(&a), "heartbeat creates a per-peer failure-detector entry");
}

#[test]
fn unknown_peer_has_no_detector_entry() {
    let mut state = HeartbeatState::new();
    let a = addr("a", 1);
    let b = addr("b", 2);
    state.heartbeat(a.clone());
    assert!(state.detectors.contains_key(&a));
    assert!(
        !state.detectors.contains_key(&b),
        "a peer with no recorded heartbeat is treated as unknown / not-yet-seen"
    );
}

#[test]
fn heartbeat_state_is_idempotent_per_address() {
    let mut state = HeartbeatState::new();
    let a = addr("a", 1);
    state.heartbeat(a.clone());
    state.heartbeat(a.clone());
    state.heartbeat(a.clone());
    assert_eq!(state.detectors.len(), 1, "repeated heartbeats from the same address share one detector");
}

#[test]
fn heartbeat_state_default_is_empty() {
    let state = HeartbeatState::default();
    assert!(state.detectors.is_empty());
}
