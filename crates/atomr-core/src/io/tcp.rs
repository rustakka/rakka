//! Minimal Tcp bind/connect helpers.
//! etc.

use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

pub async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    TcpStream::connect(addr).await
}

/// Read exactly `len` bytes into a new buffer.
pub async fn read_exact<R: AsyncRead + Unpin>(mut r: R, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

pub async fn write_all<W: AsyncWrite + Unpin>(mut w: W, bytes: &[u8]) -> io::Result<()> {
    w.write_all(bytes).await
}
