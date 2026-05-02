//! In-memory deterministic transport for tests.
//! akka.net: `Remote/Transport/TestTransport.cs`.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use rakka_core::actor::Address;

use crate::pdu::AkkaPdu;

use super::{InboundFrame, Transport, TransportError};

/// A `TestTransport` lets multiple `Address` participants exchange
/// `AkkaPdu` frames without going through the network.
#[derive(Clone)]
pub struct TestTransport {
    pub local_address: Address,
    #[allow(dead_code)]
    inbound_tx: mpsc::UnboundedSender<InboundFrame>,
    inbound_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<InboundFrame>>>>,
    pub registry: Arc<TestRegistry>,
}

#[derive(Default)]
pub struct TestRegistry {
    /// Address → outbound channel that delivers to that peer's inbound.
    peers: DashMap<String, mpsc::UnboundedSender<InboundFrame>>,
}

impl TestRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, address: &Address, sink: mpsc::UnboundedSender<InboundFrame>) {
        self.peers.insert(address.to_string(), sink);
    }

    pub fn unregister(&self, address: &Address) {
        self.peers.remove(&address.to_string());
    }
}

impl TestTransport {
    pub fn new(address: Address, registry: Arc<TestRegistry>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        registry.register(&address, tx.clone());
        Self { local_address: address, inbound_tx: tx, inbound_rx: Arc::new(Mutex::new(Some(rx))), registry }
    }
}

#[async_trait]
impl Transport for TestTransport {
    async fn listen(&self) -> Result<Address, TransportError> {
        Ok(self.local_address.clone())
    }

    async fn associate(&self, target: &Address) -> Result<(), TransportError> {
        if self.registry.peers.contains_key(&target.to_string()) {
            Ok(())
        } else {
            Err(TransportError::NotAssociated(target.to_string()))
        }
    }

    async fn send(&self, target: &Address, pdu: AkkaPdu) -> Result<(), TransportError> {
        let sink = self
            .registry
            .peers
            .get(&target.to_string())
            .ok_or_else(|| TransportError::NotAssociated(target.to_string()))?
            .clone();
        sink.send(InboundFrame { from: self.local_address.clone(), pdu }).map_err(|_| TransportError::Closed)
    }

    fn inbound(&self) -> mpsc::UnboundedReceiver<InboundFrame> {
        self.inbound_rx.lock().take().unwrap_or_else(|| {
            let (_t, r) = mpsc::unbounded_channel();
            r
        })
    }

    async fn disassociate(&self, _target: &Address) -> Result<(), TransportError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.registry.unregister(&self.local_address);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdu::{AkkaPdu, AssociateInfo, PROTOCOL_VERSION};
    use std::time::Duration;

    #[tokio::test]
    async fn loopback_send() {
        let reg = TestRegistry::new();
        let a = TestTransport::new(Address::remote("test", "A", "h", 1), reg.clone());
        let b = TestTransport::new(Address::remote("test", "B", "h", 2), reg.clone());
        let mut inbound_a = a.inbound();
        let _addr_a = a.listen().await.unwrap();
        let _addr_b = b.listen().await.unwrap();
        b.associate(&a.local_address).await.unwrap();
        let pdu = AkkaPdu::Associate(AssociateInfo {
            origin: b.local_address.clone(),
            uid: 1,
            cookie: None,
            protocol_version: PROTOCOL_VERSION,
        });
        b.send(&a.local_address, pdu).await.unwrap();
        let frame =
            tokio::time::timeout(Duration::from_millis(100), inbound_a.recv()).await.unwrap().unwrap();
        assert_eq!(frame.from, b.local_address);
    }
}
