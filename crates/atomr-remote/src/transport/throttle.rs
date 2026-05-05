//! Rate-limiting / blackholing transport adapter.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use atomr_core::actor::Address;

use crate::pdu::AkkaPdu;

use super::{InboundFrame, Transport, TransportError};

#[derive(Debug, Clone, Copy)]
pub enum ThrottleMode {
    /// Pass through unchanged.
    Unthrottled,
    /// Inject this many ms of latency on every PDU.
    Latency(Duration),
    /// Drop every PDU silently (used for partition simulation).
    Blackhole,
}

pub struct ThrottleTransport {
    inner: Arc<dyn Transport>,
    mode: Arc<RwLock<ThrottleMode>>,
}

impl ThrottleTransport {
    pub fn new(inner: Arc<dyn Transport>, mode: ThrottleMode) -> Arc<Self> {
        Arc::new(Self { inner, mode: Arc::new(RwLock::new(mode)) })
    }

    pub fn set_mode(&self, mode: ThrottleMode) {
        *self.mode.write() = mode;
    }

    pub fn mode(&self) -> ThrottleMode {
        *self.mode.read()
    }
}

#[async_trait]
impl Transport for ThrottleTransport {
    async fn listen(&self) -> Result<Address, TransportError> {
        self.inner.listen().await
    }

    async fn associate(&self, target: &Address) -> Result<(), TransportError> {
        self.inner.associate(target).await
    }

    async fn send(&self, target: &Address, pdu: AkkaPdu) -> Result<(), TransportError> {
        match self.mode() {
            ThrottleMode::Unthrottled => self.inner.send(target, pdu).await,
            ThrottleMode::Latency(d) => {
                tokio::time::sleep(d).await;
                self.inner.send(target, pdu).await
            }
            ThrottleMode::Blackhole => Ok(()),
        }
    }

    fn inbound(&self) -> mpsc::UnboundedReceiver<InboundFrame> {
        self.inner.inbound()
    }

    async fn disassociate(&self, target: &Address) -> Result<(), TransportError> {
        self.inner.disassociate(target).await
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.inner.shutdown().await
    }
}
