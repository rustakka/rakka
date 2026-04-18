//! Transport abstraction. akka.net: `Remote/Transport/Transport.cs`.

use async_trait::async_trait;
use thiserror::Error;

use rustakka_core::actor::Address;

use crate::envelope::RemoteEnvelope;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Ser(String),
    #[error("not associated with `{0}`")]
    NotAssociated(String),
    #[error("transport closed")]
    Closed,
}

#[async_trait]
pub trait Transport: Send + Sync {
    /// Bind and start listening. Returns the local `Address`.
    async fn listen(&self) -> Result<Address, TransportError>;

    /// Establish (or reuse) an association to the remote address.
    async fn associate(&self, target: &Address) -> Result<(), TransportError>;

    /// Send an already-serialized remote envelope.
    async fn send(&self, target: &Address, env: RemoteEnvelope) -> Result<(), TransportError>;

    /// Subscribe to inbound messages; returns a channel receiver.
    fn inbound(&self) -> tokio::sync::mpsc::UnboundedReceiver<RemoteEnvelope>;

    async fn shutdown(&self) -> Result<(), TransportError>;
}
