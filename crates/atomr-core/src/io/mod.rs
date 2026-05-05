//! Network IO — small TCP/UDP helpers mirroring / `IO.Udp`
//! but exposed as simple functions returning `tokio::net` primitives wrapped
//! in channel-driven actors.

pub mod manager;
pub mod tcp;
pub mod udp;

pub use manager::{ConnId, IoEvent, TcpCommand, TcpManager, UdpCommand, UdpManager};
