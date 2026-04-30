//! Failure-injecting transport adapter.
//! akka.net: `Remote/Transport/FailureInjectorTransportAdapter.cs`.
//!
//! Useful in tests for verifying timeout / retry / quarantine paths.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use rakka_core::actor::Address;

use crate::pdu::AkkaPdu;

use super::{InboundFrame, Transport, TransportError};

#[derive(Debug, Clone)]
pub enum InjectionMode {
    /// Pass through unchanged.
    Pass,
    /// Drop every nth send (n>=1; 1 drops everything).
    DropEvery(u32),
    /// Reply with a fixed [`TransportError`] on every send.
    Fail(String),
}

pub struct FailureInjectorTransport {
    inner: Arc<dyn Transport>,
    mode: Arc<RwLock<InjectionMode>>,
    counter: Arc<std::sync::atomic::AtomicU32>,
}

impl FailureInjectorTransport {
    pub fn new(inner: Arc<dyn Transport>, mode: InjectionMode) -> Arc<Self> {
        Arc::new(Self {
            inner,
            mode: Arc::new(RwLock::new(mode)),
            counter: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        })
    }

    pub fn set_mode(&self, mode: InjectionMode) {
        *self.mode.write() = mode;
    }
}

#[async_trait]
impl Transport for FailureInjectorTransport {
    async fn listen(&self) -> Result<Address, TransportError> {
        self.inner.listen().await
    }

    async fn associate(&self, target: &Address) -> Result<(), TransportError> {
        self.inner.associate(target).await
    }

    async fn send(&self, target: &Address, pdu: AkkaPdu) -> Result<(), TransportError> {
        let mode = self.mode.read().clone();
        match mode {
            InjectionMode::Pass => self.inner.send(target, pdu).await,
            InjectionMode::DropEvery(n) if n >= 1 => {
                let i = self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i % n == 0 {
                    Ok(())
                } else {
                    self.inner.send(target, pdu).await
                }
            }
            InjectionMode::DropEvery(_) => self.inner.send(target, pdu).await,
            InjectionMode::Fail(msg) => Err(TransportError::Other(msg)),
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
