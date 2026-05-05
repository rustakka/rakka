//! `TcpManager` / `UdpManager` actor-style wrappers.
//!
//! is an actor that mediates `Bind`/`Connect`
//! commands and dispatches per-connection child actors. Our equivalent is
//! a small state machine driven by mpsc channels — callers get an
//! [`IoEvent`] stream of inbound connections / read bytes / disconnects.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex};

/// Stable identifier for an inbound TCP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub u64);

/// Events emitted by [`TcpManager`] / [`UdpManager`].
#[derive(Debug)]
pub enum IoEvent {
    Connected { id: ConnId, peer: SocketAddr },
    Received { id: ConnId, bytes: Vec<u8> },
    Closed { id: ConnId },
    Bound { addr: SocketAddr },
    Datagram { from: SocketAddr, bytes: Vec<u8> },
    Error { reason: String },
}

/// Commands sent into the [`TcpManager`].
#[derive(Debug)]
pub enum TcpCommand {
    /// Listen on `addr`. The kernel-assigned port flows back as
    /// `IoEvent::Bound { addr }`.
    Bind {
        addr: SocketAddr,
    },
    /// Initiate an outbound connection. On success a
    /// `IoEvent::Connected { id, peer }` is published; subsequent
    /// reads / writes use the same `ConnId` API as inbound.
    Connect {
        addr: SocketAddr,
    },
    Send {
        id: ConnId,
        bytes: Vec<u8>,
    },
    Close {
        id: ConnId,
    },
    Shutdown,
}

type Conns = Arc<Mutex<HashMap<ConnId, mpsc::UnboundedSender<Vec<u8>>>>>;

/// Actor-style TCP manager. Drop the handle (or call [`Self::shutdown`])
/// to stop it.
pub struct TcpManager {
    cmd: mpsc::UnboundedSender<TcpCommand>,
}

impl TcpManager {
    /// Spawn the manager and return the command handle + event stream.
    pub fn spawn() -> (Self, mpsc::UnboundedReceiver<IoEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (evt_tx, evt_rx) = mpsc::unbounded_channel();
        let conns: Conns = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(run_tcp(cmd_rx, evt_tx, conns));
        (Self { cmd: cmd_tx }, evt_rx)
    }

    pub fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        self.cmd
            .send(TcpCommand::Bind { addr })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manager stopped"))
    }
    /// Initiate an outbound connection. On success the manager
    /// publishes `IoEvent::Connected { id, peer }`; on failure it
    /// publishes `IoEvent::Error { reason }`.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.cmd
            .send(TcpCommand::Connect { addr })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manager stopped"))
    }
    pub fn send_bytes(&self, id: ConnId, bytes: Vec<u8>) -> io::Result<()> {
        self.cmd
            .send(TcpCommand::Send { id, bytes })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manager stopped"))
    }
    pub fn close(&self, id: ConnId) -> io::Result<()> {
        self.cmd
            .send(TcpCommand::Close { id })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manager stopped"))
    }
    pub fn shutdown(&self) {
        let _ = self.cmd.send(TcpCommand::Shutdown);
    }
}

static SEQ: AtomicU64 = AtomicU64::new(1);

async fn run_tcp(
    mut cmd: mpsc::UnboundedReceiver<TcpCommand>,
    evt: mpsc::UnboundedSender<IoEvent>,
    conns: Conns,
) {
    while let Some(c) = cmd.recv().await {
        match c {
            TcpCommand::Bind { addr } => {
                let evt_tx = evt.clone();
                let conns = conns.clone();
                tokio::spawn(async move {
                    let listener = match TcpListener::bind(addr).await {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = evt_tx.send(IoEvent::Error { reason: e.to_string() });
                            return;
                        }
                    };
                    let bound = listener.local_addr().unwrap_or(addr);
                    let _ = evt_tx.send(IoEvent::Bound { addr: bound });
                    loop {
                        let stream = match listener.accept().await {
                            Ok((s, _)) => s,
                            Err(e) => {
                                let _ = evt_tx.send(IoEvent::Error { reason: e.to_string() });
                                break;
                            }
                        };
                        let peer = stream.peer_addr().unwrap_or(bound);
                        let id = ConnId(SEQ.fetch_add(1, Ordering::Relaxed));
                        let _ = evt_tx.send(IoEvent::Connected { id, peer });
                        spawn_conn(id, stream, evt_tx.clone(), conns.clone()).await;
                    }
                });
            }
            TcpCommand::Connect { addr } => {
                let evt_tx = evt.clone();
                let conns = conns.clone();
                tokio::spawn(async move {
                    let stream = match TcpStream::connect(addr).await {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = evt_tx.send(IoEvent::Error { reason: e.to_string() });
                            return;
                        }
                    };
                    let peer = stream.peer_addr().unwrap_or(addr);
                    let id = ConnId(SEQ.fetch_add(1, Ordering::Relaxed));
                    let _ = evt_tx.send(IoEvent::Connected { id, peer });
                    spawn_conn(id, stream, evt_tx, conns).await;
                });
            }
            TcpCommand::Send { id, bytes } => {
                let g = conns.lock().await;
                if let Some(tx) = g.get(&id) {
                    let _ = tx.send(bytes);
                }
            }
            TcpCommand::Close { id } => {
                conns.lock().await.remove(&id);
            }
            TcpCommand::Shutdown => break,
        }
    }
}

async fn spawn_conn(id: ConnId, stream: TcpStream, evt: mpsc::UnboundedSender<IoEvent>, conns: Conns) {
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    conns.lock().await.insert(id, write_tx);
    let (mut rh, mut wh) = stream.into_split();
    tokio::spawn(async move {
        while let Some(bytes) = write_rx.recv().await {
            if wh.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = wh.shutdown().await;
    });
    let evt2 = evt.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 8 * 1024];
        loop {
            match rh.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let _ = evt2.send(IoEvent::Closed { id });
                    break;
                }
                Ok(n) => {
                    let _ = evt2.send(IoEvent::Received { id, bytes: buf[..n].to_vec() });
                }
            }
        }
    });
}

#[derive(Debug)]
pub enum UdpCommand {
    Send { to: SocketAddr, bytes: Vec<u8> },
    Shutdown,
}

/// Actor-style UDP manager bound to a single socket.
pub struct UdpManager {
    cmd: mpsc::UnboundedSender<UdpCommand>,
    local: SocketAddr,
}

impl UdpManager {
    pub async fn bind(addr: SocketAddr) -> io::Result<(Self, mpsc::UnboundedReceiver<IoEvent>)> {
        let socket = UdpSocket::bind(addr).await?;
        let local = socket.local_addr()?;
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let (evt_tx, evt_rx) = mpsc::unbounded_channel();
        let socket = Arc::new(socket);
        let s_recv = socket.clone();
        let etx = evt_tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match s_recv.recv_from(&mut buf).await {
                    Ok((n, from)) => {
                        let _ = etx.send(IoEvent::Datagram { from, bytes: buf[..n].to_vec() });
                    }
                    Err(e) => {
                        let _ = etx.send(IoEvent::Error { reason: e.to_string() });
                        break;
                    }
                }
            }
        });
        let s_send = socket.clone();
        tokio::spawn(async move {
            while let Some(c) = cmd_rx.recv().await {
                match c {
                    UdpCommand::Send { to, bytes } => {
                        if let Err(e) = s_send.send_to(&bytes, to).await {
                            let _ = evt_tx.send(IoEvent::Error { reason: e.to_string() });
                        }
                    }
                    UdpCommand::Shutdown => break,
                }
            }
        });
        Ok((Self { cmd: cmd_tx, local }, evt_rx))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    pub fn send_to(&self, to: SocketAddr, bytes: Vec<u8>) -> io::Result<()> {
        self.cmd
            .send(UdpCommand::Send { to, bytes })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manager stopped"))
    }
    pub fn shutdown(&self) {
        let _ = self.cmd.send(UdpCommand::Shutdown);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn udp_manager_round_trip() {
        let (a, mut a_rx) = UdpManager::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let (b, _b_rx) = UdpManager::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        b.send_to(a.local_addr(), b"hi".to_vec()).unwrap();
        let evt =
            tokio::time::timeout(std::time::Duration::from_millis(500), a_rx.recv()).await.unwrap().unwrap();
        match evt {
            IoEvent::Datagram { bytes, .. } => assert_eq!(bytes, b"hi"),
            other => panic!("unexpected event: {other:?}"),
        }
        a.shutdown();
        b.shutdown();
    }

    #[tokio::test]
    async fn tcp_manager_accept_and_echo() {
        let (mgr, mut events) = TcpManager::spawn();
        mgr.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let bound = match tokio::time::timeout(std::time::Duration::from_millis(500), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            IoEvent::Bound { addr } => addr,
            other => panic!("expected Bound, got {other:?}"),
        };
        let mut client = TcpStream::connect(bound).await.unwrap();
        let id = match tokio::time::timeout(std::time::Duration::from_millis(500), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            IoEvent::Connected { id, .. } => id,
            other => panic!("expected Connected, got {other:?}"),
        };
        client.write_all(b"ping").await.unwrap();
        match tokio::time::timeout(std::time::Duration::from_millis(500), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            IoEvent::Received { bytes, .. } => assert_eq!(bytes, b"ping"),
            other => panic!("expected Received, got {other:?}"),
        }
        mgr.send_bytes(id, b"pong".to_vec()).unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
        mgr.shutdown();
    }
}
