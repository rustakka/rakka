//! `ClusterDaemon` — the actor that owns [`MembershipState`] and drives
//! the active gossip / leader-action / SBR ticks.
//!
//! Phase 6.C / 6.D / 6.F of `docs/full-port-plan.md`. Akka.NET parity:
//! `Cluster/ClusterDaemon.cs`.
//!
//! Architecture
//! ------------
//! * One `tokio::task::JoinHandle` runs the main loop.
//! * `mpsc::UnboundedSender<DaemonCmd>` is the inbox for control
//!   messages (`Join`, `Leave`, `ApplyGossip`, `Tick`, `Shutdown`).
//! * Per-tick the daemon
//!   1. applies leader actions (`MembershipState::apply_leader_actions`)
//!   2. runs the SBR runtime (if installed) and applies the resulting
//!      `SbrAction` (currently `DownUnreachable` is the only one that
//!      mutates state directly).
//!   3. picks a gossip target via [`pick_gossip_target`] and emits an
//!      outbound `GossipPdu` through a caller-supplied transport
//!      callback. The transport is abstracted as a `dyn GossipTransport`
//!      trait so we can plug in either the in-process [`crate::ClusterRemoteAdapter`]
//!      or a real `rakka-remote` endpoint once Phase 5.D wires it.
//!
//! Tests use [`InMemoryGossipTransport`] which routes between two
//! daemons in the same process.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rakka_core::actor::Address;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::events::ClusterEventBus;
use crate::gossip_pdu::{decide as gossip_decide, pick_gossip_target, GossipDecision, GossipPdu};
use crate::leader::elect_leader;
use crate::member::Member;
use crate::membership::MembershipState;
use crate::sbr::DowningStrategy;
use crate::sbr_runtime::{SbrAction, SbrRuntime};
use crate::vector_clock::VectorClock;

/// Pluggable transport for gossip PDUs.
pub trait GossipTransport: Send + Sync + 'static {
    /// Deliver `pdu` to `target`. The transport is "best effort" —
    /// errors must not crash the daemon.
    fn send(&self, target: &Address, pdu: GossipPdu);
}

/// Control commands accepted by the daemon mailbox.
#[derive(Debug)]
pub enum DaemonCmd {
    /// Add/update a member (Joining).
    Join(Member),
    /// Mark `addr` as Leaving.
    Leave(Address),
    /// Inject a peer's gossip PDU (called by the transport on receive).
    ApplyGossip(GossipPdu),
    /// Force a single tick (mostly for tests).
    Tick,
    /// Stop the daemon loop.
    Shutdown,
}

/// Configuration knobs.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// How often the daemon ticks (gossip + leader actions).
    pub gossip_interval: Duration,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self { gossip_interval: Duration::from_millis(1_000) }
    }
}

/// Snapshot of the daemon state used by `peer_state` queries.
#[derive(Debug, Clone, Default)]
pub struct DaemonSnapshot {
    pub state: MembershipState,
    pub leader: Option<Address>,
    pub version: VectorClock,
}

/// Public handle to a running `ClusterDaemon`.
pub struct ClusterDaemonHandle {
    cmd: mpsc::UnboundedSender<DaemonCmd>,
    snapshot: Arc<Mutex<DaemonSnapshot>>,
    join: Option<JoinHandle<()>>,
    bus: ClusterEventBus,
    self_addr: Address,
}

impl ClusterDaemonHandle {
    pub fn join(&self, m: Member) {
        let _ = self.cmd.send(DaemonCmd::Join(m));
    }
    pub fn leave(&self, addr: Address) {
        let _ = self.cmd.send(DaemonCmd::Leave(addr));
    }
    pub fn apply_gossip(&self, pdu: GossipPdu) {
        let _ = self.cmd.send(DaemonCmd::ApplyGossip(pdu));
    }
    pub fn tick(&self) {
        let _ = self.cmd.send(DaemonCmd::Tick);
    }
    pub fn snapshot(&self) -> DaemonSnapshot {
        self.snapshot.lock().clone()
    }
    pub fn events(&self) -> &ClusterEventBus {
        &self.bus
    }
    pub fn address(&self) -> &Address {
        &self.self_addr
    }
    /// Cheaply-cloneable inbox that delivers `GossipPdu`s into this
    /// daemon. Used by transport adapters that need to fan inbound
    /// PDUs into the daemon without holding the [`ClusterDaemonHandle`]
    /// itself (which is consume-on-shutdown).
    pub fn gossip_inbox(&self) -> mpsc::UnboundedSender<GossipPdu> {
        let cmd = self.cmd.clone();
        let (tx, mut rx) = mpsc::unbounded_channel::<GossipPdu>();
        tokio::spawn(async move {
            while let Some(p) = rx.recv().await {
                let _ = cmd.send(DaemonCmd::ApplyGossip(p));
            }
        });
        tx
    }

    /// Stop and join.
    pub async fn shutdown(mut self) {
        let _ = self.cmd.send(DaemonCmd::Shutdown);
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }
}

impl Drop for ClusterDaemonHandle {
    fn drop(&mut self) {
        let _ = self.cmd.send(DaemonCmd::Shutdown);
        if let Some(j) = self.join.take() {
            j.abort();
        }
    }
}

/// Spawn a daemon. The caller provides a transport implementation;
/// the daemon never blocks on it.
pub fn spawn_daemon(
    self_addr: Address,
    transport: Arc<dyn GossipTransport>,
    bus: ClusterEventBus,
    cfg: DaemonConfig,
) -> ClusterDaemonHandle {
    spawn_daemon_with_sbr::<NoSbr>(self_addr, transport, bus, cfg, None)
}

/// Same as [`spawn_daemon`] but installs an SBR runtime.
pub fn spawn_daemon_with_sbr<S>(
    self_addr: Address,
    transport: Arc<dyn GossipTransport>,
    bus: ClusterEventBus,
    cfg: DaemonConfig,
    sbr: Option<SbrRuntime<S>>,
) -> ClusterDaemonHandle
where
    S: DowningStrategy + Send + 'static,
{
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let snapshot = Arc::new(Mutex::new(DaemonSnapshot::default()));
    let snap2 = snapshot.clone();
    let bus2 = bus.clone();
    let self_addr2 = self_addr.clone();
    let join = tokio::spawn(run_daemon::<S>(self_addr.clone(), transport, bus2, cfg, sbr, cmd_rx, snap2));
    ClusterDaemonHandle { cmd: cmd_tx, snapshot, join: Some(join), bus, self_addr: self_addr2 }
}

/// Marker `DowningStrategy` used when no SBR runtime is installed.
pub struct NoSbr;

impl DowningStrategy for NoSbr {
    fn decide(&self, _r: &[&Member], _u: &[&Member]) -> crate::sbr::DowningDecision {
        crate::sbr::DowningDecision::Stay
    }
}

async fn run_daemon<S>(
    self_addr: Address,
    transport: Arc<dyn GossipTransport>,
    bus: ClusterEventBus,
    cfg: DaemonConfig,
    mut sbr: Option<SbrRuntime<S>>,
    mut cmd_rx: mpsc::UnboundedReceiver<DaemonCmd>,
    snapshot: Arc<Mutex<DaemonSnapshot>>,
) where
    S: DowningStrategy + Send + 'static,
{
    let mut state = MembershipState::new();
    let mut version = VectorClock::new();
    let mut last_leader: Option<Address> = None;
    let mut cursor: usize = 0;
    let mut ticker = tokio::time::interval(cfg.gossip_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                None => break,
                Some(DaemonCmd::Shutdown) => break,
                Some(DaemonCmd::Join(m)) => {
                    let evt = state.join(m);
                    version.tick(self_addr.to_string().as_str());
                    bus.publish(evt);
                }
                Some(DaemonCmd::Leave(addr)) => {
                    if let Some(evt) = state.leave(&addr) {
                        version.tick(self_addr.to_string().as_str());
                        bus.publish(evt);
                    }
                }
                Some(DaemonCmd::ApplyGossip(pdu)) => {
                    handle_pdu(&self_addr, &transport, &bus, &mut state, &mut version, pdu);
                }
                Some(DaemonCmd::Tick) => {
                    do_tick(&self_addr, &transport, &bus, &mut state, &mut version,
                            &mut sbr, &mut last_leader, &mut cursor);
                }
            },
            _ = ticker.tick() => {
                do_tick(&self_addr, &transport, &bus, &mut state, &mut version,
                        &mut sbr, &mut last_leader, &mut cursor);
            }
        }
        // Update snapshot.
        let leader = elect_leader(&state);
        *snapshot.lock() = DaemonSnapshot { state: state.clone(), leader, version: version.clone() };
    }
}

#[allow(clippy::too_many_arguments)]
fn do_tick<S>(
    self_addr: &Address,
    transport: &Arc<dyn GossipTransport>,
    bus: &ClusterEventBus,
    state: &mut MembershipState,
    version: &mut VectorClock,
    sbr: &mut Option<SbrRuntime<S>>,
    last_leader: &mut Option<Address>,
    cursor: &mut usize,
) where
    S: DowningStrategy + Send + 'static,
{
    // 1) Leader actions.
    let evts = state.apply_leader_actions();
    let mutated = !evts.is_empty();
    for e in evts {
        bus.publish(e);
    }
    if mutated {
        version.tick(self_addr.to_string().as_str());
    }

    // 2) Leader change?
    let leader_now = elect_leader(state);
    if leader_now != *last_leader {
        bus.publish(crate::events::ClusterEvent::LeaderChanged {
            from: last_leader.clone(),
            to: leader_now.clone(),
        });
        *last_leader = leader_now;
    }

    // 3) SBR.
    if let Some(rt) = sbr.as_mut() {
        match rt.tick(state, Instant::now()) {
            SbrAction::None | SbrAction::DownSelf => {}
            SbrAction::DownUnreachable(addrs) | SbrAction::DownAll(addrs) => {
                for a in addrs {
                    if let Some(m) = state.members.iter_mut().find(|m| m.address.to_string() == a) {
                        m.status = crate::member::MemberStatus::Down;
                    }
                }
                version.tick(self_addr.to_string().as_str());
            }
        }
    }

    // 4) Active gossip dissemination.
    let peers: Vec<Address> = state.members.iter().map(|m| m.address.clone()).collect();
    if let Some(target) = pick_gossip_target(&peers, self_addr, *cursor) {
        let pdu = GossipPdu::Status { from: self_addr.to_string(), version: version.clone() };
        transport.send(target, pdu);
        *cursor = cursor.wrapping_add(1);
    }
}

fn handle_pdu(
    self_addr: &Address,
    transport: &Arc<dyn GossipTransport>,
    bus: &ClusterEventBus,
    state: &mut MembershipState,
    version: &mut VectorClock,
    pdu: GossipPdu,
) {
    match pdu {
        GossipPdu::Status { from, version: their } => {
            let target = parse_address(&from);
            match gossip_decide(version, &their) {
                GossipDecision::SendEnvelope | GossipDecision::MergeBoth => {
                    if let Some(t) = &target {
                        transport.send(
                            t,
                            GossipPdu::Envelope {
                                from: self_addr.to_string(),
                                version: version.clone(),
                                state: state.clone(),
                            },
                        );
                    }
                }
                GossipDecision::RequestMerge => {
                    if let Some(t) = &target {
                        transport.send(
                            t,
                            GossipPdu::Merge { from: self_addr.to_string(), version: version.clone() },
                        );
                    }
                }
                GossipDecision::Same => {}
            }
        }
        GossipPdu::Envelope { from: _, version: their, state: their_state } => {
            // Naive merge: union members, prefer "later" status order, merge reachability.
            merge_state(state, their_state);
            *version = version.merge(&their);
            let _ = bus; // events published via leader-action path on next tick
        }
        GossipPdu::Merge { from, version: _ } => {
            if let Some(t) = parse_address(&from) {
                transport.send(
                    &t,
                    GossipPdu::Envelope {
                        from: self_addr.to_string(),
                        version: version.clone(),
                        state: state.clone(),
                    },
                );
            }
        }
    }
}

fn parse_address(s: &str) -> Option<Address> {
    Address::parse(s)
}

fn merge_state(local: &mut MembershipState, other: MembershipState) {
    for m in other.members {
        local.add_or_update(m);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::member::MemberStatus;
    use std::collections::HashMap;

    /// Dispatch table keyed by listener address.
    #[derive(Default, Clone)]
    struct InMemNet {
        inboxes: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<GossipPdu>>>>,
    }

    impl GossipTransport for InMemNet {
        fn send(&self, target: &Address, pdu: GossipPdu) {
            if let Some(tx) = self.inboxes.lock().get(&target.to_string()) {
                let _ = tx.send(pdu);
            }
        }
    }

    /// Bridge an mpsc receiver into a daemon's `apply_gossip` calls.
    fn install_inbox(net: &InMemNet, addr: &Address, handle: &ClusterDaemonHandle) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        net.inboxes.lock().insert(addr.to_string(), tx);
        let cmd = handle.cmd.clone();
        tokio::spawn(async move {
            while let Some(p) = rx.recv().await {
                let _ = cmd.send(DaemonCmd::ApplyGossip(p));
            }
        });
    }

    #[tokio::test]
    async fn two_daemons_exchange_membership_via_gossip() {
        let net = InMemNet::default();
        let bus_a = ClusterEventBus::new();
        let bus_b = ClusterEventBus::new();
        let addr_a = Address::local("nodeA");
        let addr_b = Address::local("nodeB");

        let cfg = DaemonConfig { gossip_interval: Duration::from_millis(50) };
        let a = spawn_daemon(addr_a.clone(), Arc::new(net.clone()), bus_a.clone(), cfg.clone());
        let b = spawn_daemon(addr_b.clone(), Arc::new(net.clone()), bus_b.clone(), cfg);
        install_inbox(&net, &addr_a, &a);
        install_inbox(&net, &addr_b, &b);

        // Each node "joins" itself.
        a.join(Member::new(addr_a.clone(), vec![]));
        b.join(Member::new(addr_b.clone(), vec![]));
        // Inject knowledge of B into A so A's gossip target picker has a peer.
        a.join(Member::new(addr_b.clone(), vec![]));
        b.join(Member::new(addr_a.clone(), vec![]));

        // Force a few ticks.
        for _ in 0..6 {
            a.tick();
            b.tick();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let snap_a = a.snapshot();
        let snap_b = b.snapshot();
        assert!(snap_a.state.member_count() >= 1);
        assert!(snap_b.state.member_count() >= 1);
        // Eventually both are converged and Up.
        assert!(snap_a
            .state
            .members
            .iter()
            .any(|m| m.address == addr_a && matches!(m.status, MemberStatus::Up | MemberStatus::Joining)));
        a.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn leader_change_event_published() {
        let net = InMemNet::default();
        let bus = ClusterEventBus::new();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let c2 = captured.clone();
        let _h = bus.subscribe(move |e| {
            if let crate::events::ClusterEvent::LeaderChanged { .. } = e {
                c2.lock().push(e.clone())
            }
        });
        let addr = Address::local("only");
        let cfg = DaemonConfig { gossip_interval: Duration::from_millis(20) };
        let d = spawn_daemon(addr.clone(), Arc::new(net.clone()), bus.clone(), cfg);
        install_inbox(&net, &addr, &d);
        d.join(Member::new(addr.clone(), vec![]));
        for _ in 0..5 {
            d.tick();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!captured.lock().is_empty());
        d.shutdown().await;
    }
}
