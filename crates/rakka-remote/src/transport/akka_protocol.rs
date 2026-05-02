//! Akka-protocol layer atop a raw [`Transport`].
//! akka.net: `Remote/Transport/AkkaProtocolTransport.cs`.
//!
//! This wrapper handles handshake (Associate / Associate reply),
//! validates the protocol version + cookie, attributes inbound frames to
//! peer UIDs, and exposes `send_payload` / `send_system` helpers that the
//! Endpoint pair calls directly.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use rakka_core::actor::Address;

use crate::address_uid::AddressUid;
use crate::pdu::{AkkaPdu, AssociateInfo, DisassociateReason, PROTOCOL_VERSION};
use crate::settings::RemoteSettings;

use super::{InboundFrame, Transport, TransportError};

/// Outcome of a peer's `Associate` PDU.
#[derive(Debug, Clone)]
pub struct PeerAssociation {
    pub address: Address,
    pub uid: u64,
}

/// Wraps an inner `Transport` to enforce the Akka handshake.
pub struct AkkaProtocolTransport {
    inner: Arc<dyn Transport>,
    settings: RemoteSettings,
    local_uid: AddressUid,
    /// Local address — captured at `start()`. Used to populate the
    /// `origin` field in inbound-handshake replies.
    local_address: Mutex<Option<Address>>,
    /// Outbound peer state: `target Address -> last UID we observed`. We
    /// use this to detect a peer restart.
    peer_uids: DashMap<String, u64>,
    /// Set of peers we have already finished handshake with.
    associated: DashMap<String, ()>,
    /// Set of peers we have already replied to with our Associate.
    associate_replied: DashMap<String, ()>,
    inbound_tx: mpsc::UnboundedSender<ProtocolEvent>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<ProtocolEvent>>>,
    pump_started: Mutex<bool>,
}

#[derive(Debug)]
pub enum ProtocolEvent {
    /// Handshake completed with this peer.
    Associated(PeerAssociation),
    /// Peer disassociated (graceful or quarantine).
    Disassociated { peer: Address, reason: DisassociateReason },
    /// Inbound payload PDU.
    Payload { from: Address, pdu: AkkaPdu },
}

impl AkkaProtocolTransport {
    pub fn new(inner: Arc<dyn Transport>, settings: RemoteSettings, local_uid: AddressUid) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            inner,
            settings,
            local_uid,
            local_address: Mutex::new(None),
            peer_uids: DashMap::new(),
            associated: DashMap::new(),
            associate_replied: DashMap::new(),
            inbound_tx: tx,
            inbound_rx: Mutex::new(Some(rx)),
            pump_started: Mutex::new(false),
        })
    }

    pub fn local_address(&self) -> Option<Address> {
        self.local_address.lock().clone()
    }

    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    pub fn local_uid(&self) -> u64 {
        self.local_uid.get()
    }

    pub fn raw_transport(&self) -> Arc<dyn Transport> {
        self.inner.clone()
    }

    /// Start listening on the underlying transport and begin pumping
    /// inbound PDUs. Returns the local `Address`.
    pub async fn start(self: &Arc<Self>) -> Result<Address, TransportError> {
        let address = self.inner.listen().await?;
        *self.local_address.lock() = Some(address.clone());
        self.start_pump();
        Ok(address)
    }

    fn start_pump(self: &Arc<Self>) {
        let mut started = self.pump_started.lock();
        if *started {
            return;
        }
        *started = true;
        drop(started);

        let this = self.clone();
        let mut inbound = self.inner.inbound();
        tokio::spawn(async move {
            while let Some(frame) = inbound.recv().await {
                this.dispatch_frame(frame).await;
            }
        });
    }

    async fn dispatch_frame(&self, frame: InboundFrame) {
        match frame.pdu {
            AkkaPdu::Associate(info) => {
                if info.protocol_version != PROTOCOL_VERSION {
                    let _ = self
                        .inner
                        .send(
                            &info.origin,
                            AkkaPdu::Disassociate(DisassociateReason::HandshakeFailure(format!(
                                "protocol version mismatch: peer={}, local={}",
                                info.protocol_version, PROTOCOL_VERSION
                            ))),
                        )
                        .await;
                    return;
                }
                if self.settings.require_cookie.is_some() && self.settings.require_cookie != info.cookie {
                    let _ = self
                        .inner
                        .send(
                            &info.origin,
                            AkkaPdu::Disassociate(DisassociateReason::HandshakeFailure(
                                "cookie mismatch".into(),
                            )),
                        )
                        .await;
                    return;
                }
                let key = info.origin.to_string();
                if let Some(prev) = self.peer_uids.insert(key.clone(), info.uid) {
                    if prev != info.uid && info.uid != 0 {
                        let _ = self.inbound_tx.send(ProtocolEvent::Disassociated {
                            peer: info.origin.clone(),
                            reason: DisassociateReason::Quarantined,
                        });
                    }
                }
                self.associated.insert(key.clone(), ());

                // Reply with our own Associate so the initiator's pump
                // can also flip to Connected. The reply travels back
                // over the same TCP socket pair (the underlying
                // transport keys peers by Address).
                if self.associate_replied.insert(key.clone(), ()).is_none() {
                    let local = self.local_address.lock().clone();
                    if let Some(local) = local {
                        let reply = AkkaPdu::Associate(AssociateInfo {
                            origin: local,
                            uid: self.local_uid.get(),
                            cookie: self.settings.require_cookie.clone(),
                            protocol_version: PROTOCOL_VERSION,
                        });
                        let _ = self.inner.send(&info.origin, reply).await;
                    }
                }

                let _ = self.inbound_tx.send(ProtocolEvent::Associated(PeerAssociation {
                    address: info.origin.clone(),
                    uid: info.uid,
                }));
            }
            AkkaPdu::Disassociate(reason) => {
                let key = frame.from.to_string();
                self.associated.remove(&key);
                self.peer_uids.remove(&key);
                let _ = self.inbound_tx.send(ProtocolEvent::Disassociated { peer: frame.from, reason });
            }
            AkkaPdu::Heartbeat => {
                // Liveness only; nothing to do at protocol layer.
            }
            other => {
                let _ = self.inbound_tx.send(ProtocolEvent::Payload { from: frame.from, pdu: other });
            }
        }
    }

    /// Initiate an outbound association: open the underlying transport,
    /// send our `Associate` PDU, and let the inbound pump complete the
    /// handshake.
    pub async fn associate(
        self: &Arc<Self>,
        target: &Address,
        local_address: &Address,
    ) -> Result<(), TransportError> {
        self.start_pump();
        self.inner.associate(target).await?;
        let pdu = AkkaPdu::Associate(AssociateInfo {
            origin: local_address.clone(),
            uid: self.local_uid.get(),
            cookie: self.settings.require_cookie.clone(),
            protocol_version: PROTOCOL_VERSION,
        });
        self.inner.send(target, pdu).await?;
        Ok(())
    }

    pub async fn send_pdu(&self, target: &Address, pdu: AkkaPdu) -> Result<(), TransportError> {
        self.inner.send(target, pdu).await
    }

    pub async fn disassociate(
        &self,
        target: &Address,
        reason: DisassociateReason,
    ) -> Result<(), TransportError> {
        let _ = self.inner.send(target, AkkaPdu::Disassociate(reason)).await;
        let _ = self.inner.disassociate(target).await;
        self.associated.remove(&target.to_string());
        self.peer_uids.remove(&target.to_string());
        Ok(())
    }

    pub fn events(&self) -> mpsc::UnboundedReceiver<ProtocolEvent> {
        self.inbound_rx.lock().take().unwrap_or_else(|| {
            let (_t, r) = mpsc::unbounded_channel();
            r
        })
    }

    pub fn is_associated(&self, address: &Address) -> bool {
        self.associated.contains_key(&address.to_string())
    }
}

#[async_trait]
impl Transport for AkkaProtocolTransport {
    async fn listen(&self) -> Result<Address, TransportError> {
        self.inner.listen().await
    }

    async fn associate(&self, target: &Address) -> Result<(), TransportError> {
        self.inner.associate(target).await
    }

    async fn send(&self, target: &Address, pdu: AkkaPdu) -> Result<(), TransportError> {
        self.inner.send(target, pdu).await
    }

    fn inbound(&self) -> mpsc::UnboundedReceiver<InboundFrame> {
        self.inner.inbound()
    }

    async fn disassociate(&self, target: &Address) -> Result<(), TransportError> {
        self.inner.disassociate(target).await
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.inner.shutdown().await
    }
}
