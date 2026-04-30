//! Network IO — small TCP/UDP helpers mirroring akka.net's `IO.Tcp` / `IO.Udp`
//! but exposed as simple functions returning `tokio::net` primitives wrapped
//! in channel-driven actors. akka.net: `src/core/Akka/IO`.

pub mod tcp;
pub mod udp;
