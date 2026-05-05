//! Transport abstraction.
//!
//! A `Transport` is a bidirectional, addressable, frame-oriented channel
//! between two `ActorSystem`s. The Akka protocol layer
//! ([`AkkaProtocolTransport`]) sits on top of the raw `Transport` and
//! handles handshake, heartbeat, ack, and disassociate PDUs.

pub mod akka_protocol;
mod failure_injector;
mod tcp;
mod test_transport;
mod throttle;

pub use akka_protocol::AkkaProtocolTransport;
pub use failure_injector::{FailureInjectorTransport, InjectionMode};
pub use tcp::TcpTransport;
pub use test_transport::TestTransport;
pub use throttle::{ThrottleMode, ThrottleTransport};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;

use atomr_core::actor::Address;

use crate::pdu::AkkaPdu;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(#[from] crate::codec::CodecError),
    #[error("not associated with `{0}`")]
    NotAssociated(String),
    #[error("transport closed")]
    Closed,
    #[error("handshake rejected: {0}")]
    HandshakeRejected(String),
    #[error("transport-specific: {0}")]
    Other(String),
}

/// A frame received from a remote peer.
#[derive(Debug)]
pub struct InboundFrame {
    pub from: Address,
    pub pdu: AkkaPdu,
}

/// Bidirectional, frame-oriented connectivity to other `ActorSystem`s.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Bind a listener and return the local `Address`.
    async fn listen(&self) -> Result<Address, TransportError>;

    /// Open (or reuse) an outbound association to `target`.
    async fn associate(&self, target: &Address) -> Result<(), TransportError>;

    /// Send a single PDU to the peer at `target`. Implementations are
    /// expected to associate lazily if needed.
    async fn send(&self, target: &Address, pdu: AkkaPdu) -> Result<(), TransportError>;

    /// Take ownership of the inbound stream. Calling more than once
    /// returns an empty channel.
    fn inbound(&self) -> mpsc::UnboundedReceiver<InboundFrame>;

    /// Drop a specific association (used by quarantine).
    async fn disassociate(&self, target: &Address) -> Result<(), TransportError>;

    /// Stop listening and close all associations.
    async fn shutdown(&self) -> Result<(), TransportError>;
}
