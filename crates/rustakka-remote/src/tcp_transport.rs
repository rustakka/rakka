//! Default TCP transport with length-prefixed JSON frames.
//! akka.net: `Remote/Transport/DotNetty/TcpTransport.cs`.
//!
//! Frame format: `u32` big-endian length, followed by JSON of `RemoteEnvelope`.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use rustakka_core::actor::Address;

use crate::envelope::RemoteEnvelope;
use crate::transport::{Transport, TransportError};

pub struct TcpTransport {
    system_name: String,
    bind: SocketAddr,
    peers: Arc<DashMap<String, mpsc::UnboundedSender<RemoteEnvelope>>>,
    inbound_tx: mpsc::UnboundedSender<RemoteEnvelope>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<RemoteEnvelope>>>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl TcpTransport {
    pub fn new(system_name: impl Into<String>, bind: SocketAddr) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            system_name: system_name.into(),
            bind,
            peers: Arc::new(DashMap::new()),
            inbound_tx: tx,
            inbound_rx: Mutex::new(Some(rx)),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

async fn read_frame<R: tokio::io::AsyncRead + Unpin>(mut r: R) -> Result<Vec<u8>, std::io::Error> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    mut w: W,
    bytes: &[u8],
) -> Result<(), std::io::Error> {
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(bytes).await?;
    w.flush().await
}

#[async_trait]
impl Transport for TcpTransport {
    async fn listen(&self) -> Result<Address, TransportError> {
        let listener = TcpListener::bind(self.bind).await?;
        let inbound = self.inbound_tx.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    accept = listener.accept() => {
                        let Ok((sock, _)) = accept else { continue };
                        let inb = inbound.clone();
                        tokio::spawn(async move {
                            let (mut r, _w) = sock.into_split();
                            loop {
                                match read_frame(&mut r).await {
                                    Ok(buf) => {
                                        if let Ok(env) = serde_json::from_slice::<RemoteEnvelope>(&buf) {
                                            let _ = inb.send(env);
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        });
                    }
                }
            }
        });
        Ok(Address::remote("akka.tcp", self.system_name.clone(), self.bind.ip().to_string(), self.bind.port()))
    }

    async fn associate(&self, target: &Address) -> Result<(), TransportError> {
        let key = target.to_string();
        if self.peers.contains_key(&key) {
            return Ok(());
        }
        let host = target.host.clone().ok_or_else(|| TransportError::NotAssociated(key.clone()))?;
        let port = target.port.ok_or_else(|| TransportError::NotAssociated(key.clone()))?;
        let socket = format!("{host}:{port}");
        let addr: SocketAddr = socket.parse().map_err(|e: std::net::AddrParseError| {
            TransportError::Ser(e.to_string())
        })?;
        let stream = TcpStream::connect(addr).await?;
        let (_r, mut w) = stream.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<RemoteEnvelope>();
        tokio::spawn(async move {
            while let Some(env) = rx.recv().await {
                let Ok(bytes) = serde_json::to_vec(&env) else { continue };
                if write_frame(&mut w, &bytes).await.is_err() {
                    break;
                }
            }
        });
        self.peers.insert(key, tx);
        Ok(())
    }

    async fn send(&self, target: &Address, env: RemoteEnvelope) -> Result<(), TransportError> {
        let key = target.to_string();
        if !self.peers.contains_key(&key) {
            self.associate(target).await?;
        }
        let peer = self.peers.get(&key).ok_or(TransportError::Closed)?;
        peer.send(env).map_err(|_| TransportError::Closed)
    }

    fn inbound(&self) -> mpsc::UnboundedReceiver<RemoteEnvelope> {
        self.inbound_rx.lock().take().unwrap_or_else(|| {
            let (_tx, rx) = mpsc::unbounded_channel();
            rx
        })
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.shutdown.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn roundtrip_message_over_tcp() {
        let a = TcpTransport::new("A", "127.0.0.1:14411".parse().unwrap());
        let b = TcpTransport::new("B", "127.0.0.1:14412".parse().unwrap());
        let addr_a = a.listen().await.unwrap();
        let _ = b.listen().await.unwrap();
        let mut inbound = a.inbound();
        b.associate(&addr_a).await.unwrap();
        b.send(
            &addr_a,
            RemoteEnvelope::new("akka://A/user/echo", None, 1, "u32", b"\"hi\"".to_vec()),
        )
        .await
        .unwrap();
        let env = tokio::time::timeout(Duration::from_millis(500), inbound.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(env.recipient_path, "akka://A/user/echo");
        assert_eq!(env.payload, b"\"hi\"");
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
