//! `EndpointManager`. akka.net: `Remote/EndpointManager.cs`.
//!
//! Owns the per-peer association state machine, dispatches inbound
//! `ProtocolEvent`s to the right [`EndpointHandle`], and re-establishes
//! associations after a transport failure (with exponential backoff and
//! a quarantine table).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use rakka_core::actor::Address;

use crate::endpoint::{spawn_endpoint, EndpointHandle, InboundEnvelope, InboundPdu};
use crate::failure_detector_registry::FailureDetectorRegistry;
use crate::metrics::RemoteMetrics;
use crate::pdu::DisassociateReason;
use crate::settings::RemoteSettings;
use crate::transport::akka_protocol::{AkkaProtocolTransport, ProtocolEvent};
use crate::transport::{Transport, TransportError};

/// Per-peer association state.
///
/// State transitions follow akka.net's `EndpointManager`:
/// `Idle → Pending → Connected → (Quarantined → Tombstoned)`.
/// `Quarantined` is time-bounded by [`RemoteSettings::
/// quarantine_duration`]; `Tombstoned` is permanent until
/// `EndpointManager::purge_tombstones` sweeps the entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AssociationState {
    Idle,
    Pending,
    Connected,
    Quarantined,
    Tombstoned,
}

#[derive(Debug, Clone)]
struct PeerEntry {
    state: AssociationState,
    /// When did we enter the current state?
    state_since: Instant,
    /// Number of consecutive reconnect attempts.
    attempt: u32,
}

impl PeerEntry {
    fn new() -> Self {
        Self { state: AssociationState::Idle, state_since: Instant::now(), attempt: 0 }
    }

    fn transition(&mut self, next: AssociationState) {
        self.state = next;
        self.state_since = Instant::now();
        if next == AssociationState::Connected {
            self.attempt = 0;
        }
    }
}

#[derive(Clone)]
pub struct EndpointManager {
    inner: Arc<EndpointManagerInner>,
}

struct EndpointManagerInner {
    protocol: Arc<AkkaProtocolTransport>,
    settings: RemoteSettings,
    local_address: RwLock<Option<Address>>,
    endpoints: DashMap<String, EndpointHandle>,
    peers: RwLock<HashMap<String, PeerEntry>>,
    inbound_sink: mpsc::UnboundedSender<InboundEnvelope>,
    inbound_rx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<InboundEnvelope>>>,
    failure_detectors: FailureDetectorRegistry,
    metrics: RemoteMetrics,
}

impl EndpointManager {
    pub fn new(protocol: Arc<AkkaProtocolTransport>, settings: RemoteSettings) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(EndpointManagerInner {
                protocol,
                settings,
                local_address: RwLock::new(None),
                endpoints: DashMap::new(),
                peers: RwLock::new(HashMap::new()),
                inbound_sink: inbound_tx,
                inbound_rx: parking_lot::Mutex::new(Some(inbound_rx)),
                failure_detectors: FailureDetectorRegistry::default_phi(),
                metrics: RemoteMetrics::new(),
            }),
        }
    }

    pub fn metrics(&self) -> RemoteMetrics {
        self.inner.metrics.clone()
    }

    pub fn failure_detectors(&self) -> FailureDetectorRegistry {
        self.inner.failure_detectors.clone()
    }

    pub fn settings(&self) -> &RemoteSettings {
        &self.inner.settings
    }

    pub fn protocol(&self) -> Arc<AkkaProtocolTransport> {
        self.inner.protocol.clone()
    }

    pub fn local_address(&self) -> Option<Address> {
        self.inner.local_address.read().clone()
    }

    /// Bind the underlying transport, store the local address, and start
    /// the dispatcher pump.
    pub async fn start(&self) -> Result<Address, TransportError> {
        let address = self.inner.protocol.start().await?;
        *self.inner.local_address.write() = Some(address.clone());
        self.start_dispatch();
        Ok(address)
    }

    fn start_dispatch(&self) {
        let mgr = self.clone();
        let mut events = self.inner.protocol.events();
        tokio::spawn(async move {
            while let Some(ev) = events.recv().await {
                mgr.dispatch_event(ev).await;
            }
        });
    }

    async fn dispatch_event(&self, ev: ProtocolEvent) {
        match ev {
            ProtocolEvent::Associated(assoc) => {
                self.inner.failure_detectors.heartbeat(&assoc.address);
                let key = assoc.address.to_string();
                let mut peers = self.inner.peers.write();
                let entry = peers.entry(key.clone()).or_insert_with(PeerEntry::new);
                entry.transition(AssociationState::Connected);
                drop(peers);
                if !self.inner.endpoints.contains_key(&key) {
                    let handle = spawn_endpoint(
                        self.inner.protocol.clone(),
                        self.inner.settings.clone(),
                        assoc.address.clone(),
                        assoc.uid,
                        self.inner.inbound_sink.clone(),
                    );
                    self.inner.endpoints.insert(key, handle);
                } else {
                    // Reused association: replay any unacked window.
                    if let Some(h) = self.inner.endpoints.get(&key) {
                        h.resend();
                    }
                }
            }
            ProtocolEvent::Disassociated { peer, reason } => {
                self.inner.failure_detectors.remove(&peer);
                let key = peer.to_string();
                if let Some((_, h)) = self.inner.endpoints.remove(&key) {
                    h.shutdown(reason.clone());
                }
                let mut peers = self.inner.peers.write();
                let entry = peers.entry(key.clone()).or_insert_with(PeerEntry::new);
                match reason {
                    DisassociateReason::Quarantined => {
                        entry.transition(AssociationState::Quarantined);
                    }
                    _ => {
                        entry.transition(AssociationState::Idle);
                    }
                }
            }
            ProtocolEvent::Payload { from, pdu } => {
                use crate::pdu::AkkaPdu;
                self.inner.failure_detectors.heartbeat(&from);
                let key = from.to_string();
                let bytes = match crate::codec::encode_pdu(&pdu) {
                    Ok(b) => b.len(),
                    Err(_) => 0,
                };
                self.inner.metrics.record_receive(&from, bytes);
                let inbound = match pdu {
                    AkkaPdu::Payload(env) => Some(InboundPdu::Payload(env)),
                    AkkaPdu::Ack(ack) => Some(InboundPdu::Ack(ack)),
                    _ => None,
                };
                if let Some(p) = inbound {
                    if let Some(h) = self.inner.endpoints.get(&key) {
                        h.deliver(p);
                    }
                }
            }
        }
    }

    /// Get (or create) an outbound endpoint to `target`. Initiates the
    /// handshake if we are not yet associated.
    pub async fn endpoint_for(&self, target: &Address) -> Result<EndpointHandle, TransportError> {
        let key = target.to_string();
        if let Some(h) = self.inner.endpoints.get(&key) {
            return Ok(h.clone());
        }
        // Quarantine guard.
        {
            let peers = self.inner.peers.read();
            if let Some(p) = peers.get(&key) {
                if p.state == AssociationState::Quarantined
                    && p.state_since.elapsed() < self.inner.settings.quarantine_duration
                {
                    return Err(TransportError::HandshakeRejected(format!("{key} is quarantined")));
                }
                if p.state == AssociationState::Tombstoned {
                    return Err(TransportError::HandshakeRejected(format!("{key} is tombstoned")));
                }
            }
        }
        // Mark Pending and start the handshake.
        {
            let mut peers = self.inner.peers.write();
            let e = peers.entry(key.clone()).or_insert_with(PeerEntry::new);
            e.transition(AssociationState::Pending);
            e.attempt = e.attempt.saturating_add(1);
        }
        let local = self.inner.local_address.read().clone().ok_or(TransportError::Closed)?;
        self.inner.protocol.associate(target, &local).await?;

        // Wait briefly for the protocol pump to flip to Connected. If it
        // doesn't, return a synthetic handle that will become real on the
        // next Associated event.
        let deadline = Instant::now() + self.inner.settings.handshake_timeout;
        loop {
            if let Some(h) = self.inner.endpoints.get(&key) {
                return Ok(h.clone());
            }
            if Instant::now() > deadline {
                let mut peers = self.inner.peers.write();
                if let Some(e) = peers.get_mut(&key) {
                    e.transition(AssociationState::Idle);
                }
                return Err(TransportError::HandshakeRejected(format!("handshake timeout to {target}")));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Quarantine a peer for the configured duration. Drops any current
    /// endpoint and refuses reconnect attempts until the window expires.
    pub async fn quarantine(&self, target: &Address) {
        let key = target.to_string();
        if let Some((_, h)) = self.inner.endpoints.remove(&key) {
            h.shutdown(DisassociateReason::Quarantined);
        }
        let _ = self.inner.protocol.disassociate(target, DisassociateReason::Quarantined).await;
        let mut peers = self.inner.peers.write();
        let e = peers.entry(key).or_insert_with(PeerEntry::new);
        e.transition(AssociationState::Quarantined);
    }

    /// Permanent gate — peer will never be readmitted.
    pub fn tombstone(&self, target: &Address) {
        let key = target.to_string();
        if let Some((_, h)) = self.inner.endpoints.remove(&key) {
            h.shutdown(DisassociateReason::Other("tombstoned".into()));
        }
        let mut peers = self.inner.peers.write();
        let e = peers.entry(key).or_insert_with(PeerEntry::new);
        e.transition(AssociationState::Tombstoned);
    }

    /// Drop tombstoned peers whose `Tombstoned`-since age exceeds
    /// `older_than`, so the peer table doesn't grow unbounded across
    /// long-running clusters. Returns the number of entries removed.
    /// Phase 5 — quarantine lifecycle.
    pub fn purge_tombstones(&self, older_than: Duration) -> usize {
        let mut peers = self.inner.peers.write();
        let before = peers.len();
        peers.retain(|_, e| {
            !(e.state == AssociationState::Tombstoned && e.state_since.elapsed() >= older_than)
        });
        before - peers.len()
    }

    /// Current state for a single peer (`None` if no association
    /// has ever been attempted).
    pub fn peer_state(&self, target: &Address) -> Option<AssociationState> {
        self.inner.peers.read().get(&target.to_string()).map(|e| e.state)
    }

    /// Take the inbound stream of decoded user/system envelopes. Calling
    /// more than once returns an empty channel — the first taker is
    /// responsible for fan-out (typically the `provider::InboundDispatcher`).
    pub fn take_inbound(&self) -> mpsc::UnboundedReceiver<InboundEnvelope> {
        self.inner.inbound_rx.lock().take().unwrap_or_else(|| {
            let (_t, r) = mpsc::unbounded_channel();
            r
        })
    }

    /// Snapshot of all known peers and their states (for telemetry).
    pub fn peer_states(&self) -> Vec<(String, &'static str, u32)> {
        self.inner.peers.read().iter().map(|(k, p)| (k.clone(), state_name(p.state), p.attempt)).collect()
    }

    pub async fn shutdown(&self) -> Result<(), TransportError> {
        for kv in self.inner.endpoints.iter() {
            kv.value().shutdown(DisassociateReason::Normal);
        }
        self.inner.endpoints.clear();
        self.inner.protocol.shutdown().await
    }
}

fn state_name(s: AssociationState) -> &'static str {
    match s {
        AssociationState::Idle => "idle",
        AssociationState::Pending => "pending",
        AssociationState::Connected => "connected",
        AssociationState::Quarantined => "quarantined",
        AssociationState::Tombstoned => "tombstoned",
    }
}
