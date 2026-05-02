//! Network IO — small TCP/UDP helpers mirroring akka.net's `IO.Tcp` / `IO.Udp`
//! but exposed as simple functions returning `tokio::net` primitives wrapped
//! in channel-driven actors. akka.net: `src/core/Akka/IO`.

pub mod manager;
pub mod tcp;
pub mod udp;

pub use manager::{ConnId, IoEvent, TcpCommand, TcpManager, UdpCommand, UdpManager};
