//! Concrete `GossipTransport` implementations and a unified cluster
//! frame that carries both gossip PDUs and Python-level remote-tells
//! over the same underlying connection.
//!
//! Two transports ship here:
//!
//! * [`InProcessClusterTransport`] — channel-based, deterministic;
//!   suitable for unit tests that need multiple `ActorSystem`s in one
//!   process.
//! * [`TcpClusterTransport`] — opens a TCP listener and connects out to
//!   peers on demand. Frames are length-prefixed bincode-encoded
//!   [`ClusterFrame`]s.
//!
//! The transports are not Python-aware. The [`RemoteMessageSink`] trait
//! is implemented by the caller (e.g. the pycore binding) to receive
//! `RemoteTell` frames and route them to the right local actor after
//! decoding the payload through the codec registry.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bincode::config::standard as bincode_cfg;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};

use atomr_core::actor::Address;

use crate::cluster_daemon::GossipTransport;
use crate::gossip_pdu::GossipPdu;

/// Wire-level frame used by both [`InProcessClusterTransport`] and
/// [`TcpClusterTransport`]. The two variants are multiplexed over the
/// same connection so that gossip and Python-level remote-tells share
/// the same association.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterFrame {
    /// Cluster-membership gossip PDU.
    Gossip(GossipPdu),
    /// Type-erased Python actor-message envelope. The receiving side
    /// looks up `manifest` in the codec registry to decode `payload`.
    RemoteTell {
        target_path: String,
        manifest: String,
        payload: Vec<u8>,
        sender_path: Option<String>,
    },
}

/// Sink for inbound `RemoteTell` frames. The pycore binding implements
/// this — typically by decoding the payload via the codec registry and
/// invoking `tell` on the matching local actor.
pub trait RemoteMessageSink: Send + Sync + 'static {
    /// Deliver a `RemoteTell` frame. Errors must not crash the
    /// transport — the implementor is responsible for logging or
    /// dead-lettering.
    fn deliver(
        &self,
        target_path: &str,
        manifest: &str,
        payload: &[u8],
        sender_path: Option<&str>,
    );
}

// ---------------------------------------------------------------------------
// In-process transport. Useful for deterministic multi-node tests.
// ---------------------------------------------------------------------------

/// Shared registry that wires up [`InProcessClusterTransport`] siblings
/// in the same process. A single registry is created once per "logical
/// network" and handed to every transport that should be able to reach
/// every other.
#[derive(Default)]
pub struct InProcessRegistry {
    peers: DashMap<String, mpsc::UnboundedSender<ClusterFrame>>,
}

impl InProcessRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn register(&self, addr: &Address, tx: mpsc::UnboundedSender<ClusterFrame>) {
        self.peers.insert(addr.to_string(), tx);
    }

    fn unregister(&self, addr: &Address) {
        self.peers.remove(&addr.to_string());
    }

    fn send(&self, target: &Address, frame: ClusterFrame) {
        if let Some(tx) = self.peers.get(&target.to_string()) {
            let _ = tx.send(frame);
        }
    }
}

/// Channel-backed cluster transport. Discovers peers through a shared
/// [`InProcessRegistry`]. Construct one per node, register the daemon's
/// gossip inbox + a [`RemoteMessageSink`] via [`Self::start`], and call
/// [`Self::send_remote`] to push remote-tells.
pub struct InProcessClusterTransport {
    self_addr: Address,
    registry: Arc<InProcessRegistry>,
    #[allow(dead_code)]
    inbound_tx: mpsc::UnboundedSender<ClusterFrame>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<ClusterFrame>>>,
}

impl InProcessClusterTransport {
    pub fn new(self_addr: Address, registry: Arc<InProcessRegistry>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        registry.register(&self_addr, tx.clone());
        Self {
            self_addr,
            registry,
            inbound_tx: tx,
            inbound_rx: Mutex::new(Some(rx)),
        }
    }

    pub fn self_address(&self) -> &Address {
        &self.self_addr
    }

    /// Send a `RemoteTell` frame to `target`. Drops silently if the
    /// peer is not registered — matches the best-effort semantics of
    /// [`GossipTransport::send`].
    pub fn send_remote(
        &self,
        target: &Address,
        target_path: String,
        manifest: String,
        payload: Vec<u8>,
        sender_path: Option<String>,
    ) {
        self.registry.send(
            target,
            ClusterFrame::RemoteTell { target_path, manifest, payload, sender_path },
        );
    }

    /// Spawn the inbound demultiplex task. `gossip_inbox` is the
    /// daemon's [`crate::ClusterDaemonHandle::gossip_inbox`] sender;
    /// `sink` receives `RemoteTell` frames.
    pub fn start(&self, gossip_inbox: mpsc::UnboundedSender<GossipPdu>, sink: Arc<dyn RemoteMessageSink>) {
        let mut rx = match self.inbound_rx.lock().take() {
            Some(rx) => rx,
            None => return,
        };
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                match frame {
                    ClusterFrame::Gossip(p) => {
                        let _ = gossip_inbox.send(p);
                    }
                    ClusterFrame::RemoteTell { target_path, manifest, payload, sender_path } => {
                        sink.deliver(&target_path, &manifest, &payload, sender_path.as_deref());
                    }
                }
            }
        });
    }
}

impl GossipTransport for InProcessClusterTransport {
    fn send(&self, target: &Address, pdu: GossipPdu) {
        // Self-send is a no-op (consistent with the existing in-mem test
        // network). The daemon never picks itself as a gossip target, but
        // be defensive.
        if target == &self.self_addr {
            return;
        }
        self.registry.send(target, ClusterFrame::Gossip(pdu));
    }
}

impl Drop for InProcessClusterTransport {
    fn drop(&mut self) {
        self.registry.unregister(&self.self_addr);
    }
}

// ---------------------------------------------------------------------------
// TCP transport.
// ---------------------------------------------------------------------------

/// TCP-based cluster transport. One listener per node accepts inbound
/// connections; outbound connections are opened on demand and reused
/// per peer address. Frames are length-prefixed (4-byte big-endian)
/// bincode-encoded [`ClusterFrame`]s.
pub struct TcpClusterTransport {
    self_addr: Address,
    bind: SocketAddr,
    advertised_host: Option<String>,
    peers: Arc<DashMap<String, mpsc::UnboundedSender<ClusterFrame>>>,
    inbound_tx: mpsc::UnboundedSender<ClusterFrame>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<ClusterFrame>>>,
    shutdown: Arc<Notify>,
    listen_addr: Mutex<Option<SocketAddr>>,
}

impl TcpClusterTransport {
    /// Build a new TCP transport. The system name is taken from
    /// `self_addr.system`; the bind socket is given separately because
    /// `Address` doesn't carry a port until `listen` resolves it.
    pub fn new(self_addr: Address, bind: SocketAddr) -> Self {
        Self::with_advertised(self_addr, bind, None)
    }

    pub fn with_advertised(
        self_addr: Address,
        bind: SocketAddr,
        advertised_host: Option<String>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            self_addr,
            bind,
            advertised_host,
            peers: Arc::new(DashMap::new()),
            inbound_tx: tx,
            inbound_rx: Mutex::new(Some(rx)),
            shutdown: Arc::new(Notify::new()),
            listen_addr: Mutex::new(None),
        }
    }

    /// Listen on the configured bind address. The returned `Address`
    /// reflects the actually-bound socket (so callers that pass
    /// `0.0.0.0:0` learn the auto-allocated port). The protocol
    /// scheme is forced to `akka.tcp` since the resolved address
    /// represents a real TCP listener.
    pub async fn listen(&self) -> std::io::Result<Address> {
        let listener = TcpListener::bind(self.bind).await?;
        let bound = listener.local_addr()?;
        *self.listen_addr.lock() = Some(bound);
        let host = self.advertised_host.clone().unwrap_or_else(|| bound.ip().to_string());
        let resolved = Address::remote(
            "akka.tcp",
            self.self_addr.system.clone(),
            host,
            bound.port(),
        );

        let inbound = self.inbound_tx.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    accept = listener.accept() => {
                        let Ok((sock, _)) = accept else { continue };
                        let _ = sock.set_nodelay(true);
                        let inbound = inbound.clone();
                        tokio::spawn(handle_inbound_socket(sock, inbound));
                    }
                }
            }
        });
        Ok(resolved)
    }

    pub fn local_address(&self) -> Option<SocketAddr> {
        *self.listen_addr.lock()
    }

    /// Hand the inbound receiver out (call once). Subsequent calls
    /// return an empty channel.
    pub fn take_inbound(&self) -> mpsc::UnboundedReceiver<ClusterFrame> {
        self.inbound_rx.lock().take().unwrap_or_else(|| mpsc::unbounded_channel().1)
    }

    /// Spawn the inbound demultiplex task. Mirrors
    /// [`InProcessClusterTransport::start`].
    pub fn start(
        &self,
        gossip_inbox: mpsc::UnboundedSender<GossipPdu>,
        sink: Arc<dyn RemoteMessageSink>,
    ) {
        let mut rx = self.take_inbound();
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                match frame {
                    ClusterFrame::Gossip(p) => {
                        let _ = gossip_inbox.send(p);
                    }
                    ClusterFrame::RemoteTell { target_path, manifest, payload, sender_path } => {
                        sink.deliver(&target_path, &manifest, &payload, sender_path.as_deref());
                    }
                }
            }
        });
    }

    /// Send a `RemoteTell` frame to `target`. Best-effort.
    pub fn send_remote(
        &self,
        target: &Address,
        target_path: String,
        manifest: String,
        payload: Vec<u8>,
        sender_path: Option<String>,
    ) {
        let frame = ClusterFrame::RemoteTell { target_path, manifest, payload, sender_path };
        let target = target.clone();
        let peers = self.peers.clone();
        tokio::spawn(async move {
            send_via_tcp(target, frame, peers).await;
        });
    }

    pub async fn shutdown(&self) {
        self.shutdown.notify_waiters();
        self.peers.clear();
    }
}

impl GossipTransport for TcpClusterTransport {
    fn send(&self, target: &Address, pdu: GossipPdu) {
        if target == &self.self_addr {
            return;
        }
        let frame = ClusterFrame::Gossip(pdu);
        let target = target.clone();
        let peers = self.peers.clone();
        tokio::spawn(async move {
            send_via_tcp(target, frame, peers).await;
        });
    }
}

async fn send_via_tcp(
    target: Address,
    frame: ClusterFrame,
    peers: Arc<DashMap<String, mpsc::UnboundedSender<ClusterFrame>>>,
) {
    let key = target.to_string();
    if let Some(tx) = peers.get(&key) {
        let _ = tx.send(frame);
        return;
    }
    // Otherwise open a new connection and remember it.
    let host = match target.host.as_deref() {
        Some(h) => h.to_string(),
        None => return,
    };
    let port = match target.port {
        Some(p) => p,
        None => return,
    };
    let stream = match TcpStream::connect((host.as_str(), port)).await {
        Ok(s) => s,
        Err(_) => return,
    };
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ClusterFrame>();
    peers.insert(key.clone(), tx.clone());

    let key_w = key.clone();
    let peers_w = peers.clone();
    tokio::spawn(async move {
        while let Some(f) = rx.recv().await {
            if write_frame(&mut writer, &f).await.is_err() {
                break;
            }
        }
        peers_w.remove(&key_w);
    });

    // Reader: outbound TCP also receives any reply frames the peer
    // might choose to send back over the same socket. (We don't use
    // this in practice — the peer's listener accepts a separate
    // connection — but draining the half-open socket prevents weird
    // EOF artefacts.)
    tokio::spawn(async move {
        let mut buf = Vec::new();
        loop {
            buf.clear();
            if read_frame_into(&mut reader, &mut buf).await.is_err() {
                break;
            }
        }
    });

    let _ = tx.send(frame);
}

async fn handle_inbound_socket(
    sock: TcpStream,
    inbound: mpsc::UnboundedSender<ClusterFrame>,
) {
    let (mut reader, mut _writer) = sock.into_split();
    let mut buf = Vec::new();
    loop {
        buf.clear();
        if read_frame_into(&mut reader, &mut buf).await.is_err() {
            break;
        }
        match bincode::serde::decode_from_slice::<ClusterFrame, _>(&buf, bincode_cfg()) {
            Ok((frame, _)) => {
                if inbound.send(frame).is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    frame: &ClusterFrame,
) -> std::io::Result<()> {
    let bytes = bincode::serde::encode_to_vec(frame, bincode_cfg())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let len = (bytes.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_frame_into<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"));
    }
    buf.resize(len, 0);
    reader.read_exact(buf).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Convenience: in-memory sink that records frames for assertions.
// ---------------------------------------------------------------------------

/// Test-only sink that buffers every received `RemoteTell` for later
/// inspection. Public so binding-side tests can use it.
#[derive(Default)]
pub struct RecordingSink {
    pub records: Mutex<Vec<RemoteTellRecord>>,
}

#[derive(Debug, Clone)]
pub struct RemoteTellRecord {
    pub target_path: String,
    pub manifest: String,
    pub payload: Vec<u8>,
    pub sender_path: Option<String>,
}

impl RemoteMessageSink for RecordingSink {
    fn deliver(
        &self,
        target_path: &str,
        manifest: &str,
        payload: &[u8],
        sender_path: Option<&str>,
    ) {
        self.records.lock().push(RemoteTellRecord {
            target_path: target_path.to_string(),
            manifest: manifest.to_string(),
            payload: payload.to_vec(),
            sender_path: sender_path.map(|s| s.to_string()),
        });
    }
}

// Keep one-import linter happy.
#[allow(dead_code)]
type _Hm = HashMap<(), ()>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_clock::VectorClock;
    use std::time::Duration;

    fn local(name: &str) -> Address {
        Address::local(name)
    }

    #[tokio::test]
    async fn in_process_gossip_round_trip() {
        let net = InProcessRegistry::new();
        let a = Arc::new(InProcessClusterTransport::new(local("A"), net.clone()));
        let b = Arc::new(InProcessClusterTransport::new(local("B"), net.clone()));

        let (gossip_tx_b, mut gossip_rx_b) = mpsc::unbounded_channel();
        let sink: Arc<dyn RemoteMessageSink> = Arc::new(RecordingSink::default());
        b.start(gossip_tx_b, sink);

        a.send(
            &local("B"),
            GossipPdu::Status { from: "A".into(), version: VectorClock::new() },
        );
        let pdu = tokio::time::timeout(Duration::from_millis(200), gossip_rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(pdu, GossipPdu::Status { .. }));
    }

    #[tokio::test]
    async fn in_process_remote_tell_delivered_to_sink() {
        let net = InProcessRegistry::new();
        let a = Arc::new(InProcessClusterTransport::new(local("A"), net.clone()));
        let b = Arc::new(InProcessClusterTransport::new(local("B"), net.clone()));

        let (gossip_tx, _gossip_rx) = mpsc::unbounded_channel();
        let sink = Arc::new(RecordingSink::default());
        let sink_dyn: Arc<dyn RemoteMessageSink> = sink.clone();
        b.start(gossip_tx, sink_dyn);

        a.send_remote(
            &local("B"),
            "akka://B/user/echo".into(),
            "json:Hello".into(),
            b"{\"name\":\"Ada\"}".to_vec(),
            None,
        );
        // Allow the channel deliver tick.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let recs = sink.records.lock().clone();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].target_path, "akka://B/user/echo");
        assert_eq!(recs[0].manifest, "json:Hello");
        assert_eq!(recs[0].payload, b"{\"name\":\"Ada\"}");
    }

    #[tokio::test]
    async fn tcp_round_trip_remote_tell() {
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let a_addr = Address::remote("akka.tcp", "A", "127.0.0.1", 0);
        let b_addr_seed = Address::remote("akka.tcp", "B", "127.0.0.1", 0);
        let a = Arc::new(TcpClusterTransport::new(a_addr, bind));
        let b = Arc::new(TcpClusterTransport::new(b_addr_seed, bind));

        let resolved_b = b.listen().await.unwrap();
        let _resolved_a = a.listen().await.unwrap();

        let (gossip_tx, _gossip_rx) = mpsc::unbounded_channel();
        let sink = Arc::new(RecordingSink::default());
        let sink_dyn: Arc<dyn RemoteMessageSink> = sink.clone();
        b.start(gossip_tx, sink_dyn);

        a.send_remote(
            &resolved_b,
            format!("{}/user/echo", resolved_b),
            "json:Hello".into(),
            b"hi".to_vec(),
            None,
        );

        // Poll for delivery.
        for _ in 0..50 {
            if !sink.records.lock().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let recs = sink.records.lock().clone();
        assert_eq!(recs.len(), 1, "expected one frame, got {recs:?}");
        assert_eq!(recs[0].manifest, "json:Hello");
        assert_eq!(recs[0].payload, b"hi");

        a.shutdown().await;
        b.shutdown().await;
    }
}
