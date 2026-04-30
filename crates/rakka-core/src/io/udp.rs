//! UDP helpers. akka.net: `IO/UdpManager.cs`.

use std::io;
use std::net::SocketAddr;

use tokio::net::UdpSocket;

pub async fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
    UdpSocket::bind(addr).await
}
