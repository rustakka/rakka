//! Endpoints. akka.net: `Remote/EndpointManager.cs`, `EndpointReader.cs`,
//! `EndpointWriter.cs`. Our simplified version reuses the transport's
//! per-peer channel for the writer and the shared inbound channel for the
//! reader.

use std::sync::Arc;

use rustakka_core::actor::Address;

use crate::envelope::RemoteEnvelope;
use crate::transport::{Transport, TransportError};

pub struct Endpoint {
    pub remote: Address,
    pub transport: Arc<dyn Transport>,
}

impl Endpoint {
    pub fn new(remote: Address, transport: Arc<dyn Transport>) -> Self {
        Self { remote, transport }
    }

    pub async fn send(&self, env: RemoteEnvelope) -> Result<(), TransportError> {
        self.transport.send(&self.remote, env).await
    }
}

/// Oversees all endpoints for a system. akka.net: `EndpointManager.cs`.
pub struct EndpointManager {
    transport: Arc<dyn Transport>,
}

impl EndpointManager {
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self { transport }
    }

    pub async fn start(&self) -> Result<Address, TransportError> {
        self.transport.listen().await
    }

    pub async fn endpoint(&self, remote: Address) -> Endpoint {
        let _ = self.transport.associate(&remote).await;
        Endpoint::new(remote, self.transport.clone())
    }

    pub async fn shutdown(&self) -> Result<(), TransportError> {
        self.transport.shutdown().await
    }
}
