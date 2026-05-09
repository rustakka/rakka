//! Domain Event Bus pattern.
//!
//! In-process broadcast of domain events to interested subscribers.
//! Typically used between a write-side [`crate::cqrs::CqrsPattern`]
//! and downstream readers / sagas / external integrations.
//!
//! v1 is local-only. The `bus-cluster` feature (gated on
//! `atomr-cluster-tools`) will add a cluster-wide variant.

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use atomr_core::actor::ActorSystem;
use parking_lot::RwLock;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::topology::Topology;
use crate::PatternError;

/// Public handle to the bus pattern.
pub struct DomainEventBus<E>(PhantomData<E>);

impl<E: Clone + Send + 'static> DomainEventBus<E> {
    pub fn builder() -> BusBuilder<E> {
        BusBuilder { name: None, _ev: PhantomData }
    }
}

pub struct BusBuilder<E> {
    name: Option<String>,
    _ev: PhantomData<E>,
}

impl<E: Clone + Send + 'static> BusBuilder<E> {
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    pub fn build(self) -> BusTopology<E> {
        BusTopology { name: self.name.unwrap_or_else(|| "bus".into()), _ev: PhantomData }
    }
}

pub struct BusTopology<E> {
    #[allow(dead_code)]
    name: String,
    _ev: PhantomData<E>,
}

/// Bus handles. Use [`BusHandles::publish`] to push events; call
/// [`BusHandles::subscribe`] to obtain a fresh receiver.
pub struct BusHandles<E: Clone + Send + 'static> {
    inner: Arc<BusInner<E>>,
}

impl<E: Clone + Send + 'static> Clone for BusHandles<E> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

struct BusInner<E: Clone + Send + 'static> {
    subscribers: RwLock<Vec<UnboundedSender<E>>>,
}

impl<E: Clone + Send + 'static> BusHandles<E> {
    /// Broadcast `event` to every live subscriber. Closed receivers
    /// are pruned in-line.
    pub fn publish(&self, event: E) {
        let mut guard = self.inner.subscribers.write();
        guard.retain(|tx| tx.send(event.clone()).is_ok());
    }

    /// Subscribe and receive a fresh channel. The returned
    /// [`UnboundedReceiver`] is closed when the bus drops or the
    /// receiver is dropped.
    pub fn subscribe(&self) -> UnboundedReceiver<E> {
        let (tx, rx) = unbounded_channel();
        self.inner.subscribers.write().push(tx);
        rx
    }
}

#[async_trait]
impl<E: Clone + Send + 'static> Topology for BusTopology<E> {
    type Handles = BusHandles<E>;

    async fn materialize(self, _system: &ActorSystem) -> Result<Self::Handles, PatternError<()>> {
        Ok(BusHandles {
            inner: Arc::new(BusInner { subscribers: RwLock::new(Vec::new()) }),
        })
    }
}
