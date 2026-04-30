//! Default TCP transport.
//! akka.net: `Remote/Transport/DotNetty/TcpTransport.cs`.
//!
//! Each association is one TCP connection carrying length-prefixed
//! [`AkkaPdu`] frames in both directions.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};

use rakka_core::actor::Address;

use crate::codec::{read_frame, write_frame};
use crate::pdu::{AkkaPdu, AssociateInfo, PROTOCOL_VERSION};

use super::{InboundFrame, Transport, TransportError};

pub struct TcpTransport {
    system_name: String,
    bind: SocketAddr,
    advertised_host: Option<String>,
    max_frame_size: usize,
    peers: Arc<DashMap<String, PeerLink>>,
    inbound_tx: mpsc::UnboundedSender<InboundFrame>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<InboundFrame>>>,
    shutdown: Arc<Notify>,
    /// `Address` we advertise to peers in `Associate` PDUs. Filled in by
    /// [`Self::listen`].
    local_address: Mutex<Option<Address>>,
}

#[derive(Clone)]
struct PeerLink {
    sender: mpsc::UnboundedSender<AkkaPdu>,
}

impl TcpTransport {
    pub fn new(system_name: impl Into<String>, bind: SocketAddr) -> Self {
        Self::with_advertised(system_name, bind, None, 4 * 1024 * 1024)
    }

    pub fn with_advertised(
        system_name: impl Into<String>,
        bind: SocketAddr,
        advertised_host: Option<String>,
        max_frame_size: usize,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            system_name: system_name.into(),
            bind,
            advertised_host,
            max_frame_size,
            peers: Arc::new(DashMap::new()),
            inbound_tx: tx,
            inbound_rx: Mutex::new(Some(rx)),
            shutdown: Arc::new(Notify::new()),
            local_address: Mutex::new(None),
        }
    }

    pub fn local_address(&self) -> Option<Address> {
        self.local_address.lock().clone()
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn listen(&self) -> Result<Address, TransportError> {
        let listener = TcpListener::bind(self.bind).await?;
        let bound = listener.local_addr()?;
        let host = self
            .advertised_host
            .clone()
            .unwrap_or_else(|| bound.ip().to_string());
        let address = Address::remote("akka.tcp", &self.system_name, host, bound.port());
        *self.local_address.lock() = Some(address.clone());

        let inbound = self.inbound_tx.clone();
        let shutdown = self.shutdown.clone();
        let peers = self.peers.clone();
        let max_frame = self.max_frame_size;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    accept = listener.accept() => {
                        let Ok((sock, peer_addr)) = accept else { continue };
                        let _ = sock.set_nodelay(true);
                        let inb = inbound.clone();
                        let peers = peers.clone();
                        tokio::spawn(handle_inbound_socket(sock, peer_addr, inb, peers, max_frame));
                    }
                }
            }
        });
        Ok(address)
    }

    async fn associate(&self, target: &Address) -> Result<(), TransportError> {
        let key = target.to_string();
        if self.peers.contains_key(&key) {
            return Ok(());
        }
        let host = target
            .host
            .clone()
            .ok_or_else(|| TransportError::NotAssociated(key.clone()))?;
        let port = target
            .port
            .ok_or_else(|| TransportError::NotAssociated(key.clone()))?;
        let stream = TcpStream::connect((host.as_str(), port)).await?;
        let _ = stream.set_nodelay(true);
        let (mut reader, mut writer) = stream.into_split();

        let (tx, mut rx) = mpsc::unbounded_channel::<AkkaPdu>();
        let max_frame = self.max_frame_size;
        let target_addr = target.clone();

        // Outbound writer task.
        tokio::spawn(async move {
            while let Some(pdu) = rx.recv().await {
                if write_frame(&mut writer, &pdu, max_frame).await.is_err() {
                    break;
                }
                if matches!(pdu, AkkaPdu::Disassociate(_)) {
                    let _ = writer.shutdown().await;
                    break;
                }
            }
        });

        // Outbound reader task: peer's replies arrive on the same socket.
        let inbound = self.inbound_tx.clone();
        let peers_for_reader = self.peers.clone();
        let key_for_reader = key.clone();
        tokio::spawn(async move {
            loop {
                match read_frame(&mut reader, max_frame).await {
                    Ok(pdu) => {
                        let _ = inbound
                            .send(InboundFrame { from: target_addr.clone(), pdu });
                    }
                    Err(_) => {
                        peers_for_reader.remove(&key_for_reader);
                        break;
                    }
                }
            }
        });

        self.peers.insert(key, PeerLink { sender: tx });
        Ok(())
    }

    async fn send(&self, target: &Address, pdu: AkkaPdu) -> Result<(), TransportError> {
        let key = target.to_string();
        if !self.peers.contains_key(&key) {
            self.associate(target).await?;
        }
        let peer = self
            .peers
            .get(&key)
            .ok_or(TransportError::Closed)?
            .clone();
        peer.sender.send(pdu).map_err(|_| TransportError::Closed)
    }

    fn inbound(&self) -> mpsc::UnboundedReceiver<InboundFrame> {
        self.inbound_rx.lock().take().unwrap_or_else(|| {
            let (_tx, rx) = mpsc::unbounded_channel();
            rx
        })
    }

    async fn disassociate(&self, target: &Address) -> Result<(), TransportError> {
        let key = target.to_string();
        if let Some((_, peer)) = self.peers.remove(&key) {
            let _ = peer.sender.send(AkkaPdu::Disassociate(
                crate::pdu::DisassociateReason::Normal,
            ));
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.shutdown.notify_waiters();
        self.peers.clear();
        Ok(())
    }
}

async fn handle_inbound_socket(
    sock: TcpStream,
    _peer_addr: SocketAddr,
    inbound: mpsc::UnboundedSender<InboundFrame>,
    peers: Arc<DashMap<String, PeerLink>>,
    max_frame: usize,
) {
    let (mut reader, mut writer) = sock.into_split();
    // First frame must be `Associate`. We *don't* echo it back here —
    // the higher-level protocol layer (`AkkaProtocolTransport`) is
    // responsible for the reply Associate. We just attribute frames
    // and register an outbound channel keyed by the peer's origin so
    // the protocol layer's `send(target, ...)` flows back over this
    // socket pair.
    let first = match read_frame(&mut reader, max_frame).await {
        Ok(pdu) => pdu,
        Err(_) => return,
    };
    let origin = match &first {
        AkkaPdu::Associate(AssociateInfo { origin, .. }) => origin.clone(),
        _ => return,
    };

    let (tx, mut rx) = mpsc::unbounded_channel::<AkkaPdu>();
    let key = origin.to_string();
    peers.insert(key.clone(), PeerLink { sender: tx });

    let _ = inbound.send(InboundFrame { from: origin.clone(), pdu: first });

    let writer_task = tokio::spawn(async move {
        while let Some(pdu) = rx.recv().await {
            if write_frame(&mut writer, &pdu, max_frame).await.is_err() {
                break;
            }
            if matches!(pdu, AkkaPdu::Disassociate(_)) {
                let _ = writer.shutdown().await;
                break;
            }
        }
    });

    let reader_origin = origin.clone();
    let inbound_for_reader = inbound.clone();
    let reader_task = tokio::spawn(async move {
        loop {
            match read_frame(&mut reader, max_frame).await {
                Ok(pdu) => {
                    let _ = inbound_for_reader
                        .send(InboundFrame { from: reader_origin.clone(), pdu });
                }
                Err(_) => break,
            }
        }
    });

    let _ = tokio::join!(writer_task, reader_task);
    peers.remove(&key);
}

// Silence a stale unused-import lint in case `PROTOCOL_VERSION` is not
// referenced elsewhere in this module.
const _PV: u32 = PROTOCOL_VERSION;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdu::{AkkaPdu, AssociateInfo, PROTOCOL_VERSION};
    use std::time::Duration;

    fn associate_pdu(origin: Address, uid: u64) -> AkkaPdu {
        AkkaPdu::Associate(AssociateInfo {
            origin,
            uid,
            cookie: None,
            protocol_version: PROTOCOL_VERSION,
        })
    }

    #[tokio::test]
    async fn handshake_and_payload_roundtrip() {
        let a = TcpTransport::new("A", "127.0.0.1:0".parse().unwrap());
        let b = TcpTransport::new("B", "127.0.0.1:0".parse().unwrap());
        let addr_a = a.listen().await.unwrap();
        let addr_b = b.listen().await.unwrap();
        let mut inbound_a = a.inbound();

        b.associate(&addr_a).await.unwrap();
        b.send(&addr_a, associate_pdu(addr_b.clone(), 7)).await.unwrap();

        let frame = tokio::time::timeout(Duration::from_millis(500), inbound_a.recv())
            .await
            .unwrap()
            .unwrap();
        match frame.pdu {
            AkkaPdu::Associate(info) => {
                assert_eq!(info.origin, addr_b);
                assert_eq!(info.uid, 7);
            }
            other => panic!("unexpected pdu {other:?}"),
        }
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
