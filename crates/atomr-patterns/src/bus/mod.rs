//! Domain Event Bus pattern.
//!
//! In-process broadcast of domain events to interested subscribers.
//! Typically used between a write-side [`crate::cqrs::CqrsPattern`]
//! and downstream readers / sagas / external integrations.
//!
//! v2 also ships a cluster-wide variant behind the `bus-cluster`
//! Cargo feature. Configure it via
//! [`BusBuilder::cluster`] / [`BusBuilder::topic`] /
//! [`BusBuilder::codec`].

#[cfg(feature = "bus-cluster")]
mod cluster;

#[cfg(feature = "bus-cluster")]
use cluster::ClusterConfig;

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "bus-cluster")]
use atomr_core::actor::{Actor, Context, Props};
use atomr_core::actor::ActorSystem;
use parking_lot::RwLock;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::topology::Topology;
use crate::PatternError;

/// Public handle to the bus pattern.
pub struct DomainEventBus<E>(PhantomData<E>);

impl<E: Clone + Send + 'static> DomainEventBus<E> {
    pub fn builder() -> BusBuilder<E> {
        BusBuilder {
            name: None,
            #[cfg(feature = "bus-cluster")]
            cluster: None,
            _ev: PhantomData,
        }
    }
}

pub struct BusBuilder<E: Clone + Send + 'static> {
    name: Option<String>,
    #[cfg(feature = "bus-cluster")]
    cluster: Option<ClusterConfig<E>>,
    _ev: PhantomData<E>,
}

impl<E: Clone + Send + 'static> BusBuilder<E> {
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    /// Enable cluster fan-out via
    /// [`atomr_cluster_tools::ClusterPubSub`]. The caller supplies a
    /// `local` [`atomr_cluster_tools::DistributedPubSub`] and a
    /// `cluster` wrapper already constructed against a transport.
    /// Combine with [`Self::topic`], [`Self::type_id`], and
    /// [`Self::codec`] to finish the wiring.
    #[cfg(feature = "bus-cluster")]
    pub fn cluster(
        mut self,
        local: Arc<atomr_cluster_tools::DistributedPubSub>,
        cluster: Arc<atomr_cluster_tools::ClusterPubSub>,
    ) -> Self {
        let topic = self.name.clone().unwrap_or_else(|| "bus".into());
        let cfg = ClusterConfig {
            local,
            cluster,
            topic: topic.clone(),
            type_id: topic,
            encode: Arc::new(|_e: &E| Vec::new()),
            decode: Arc::new(|_b: &[u8]| Err("codec not configured".into())),
        };
        self.cluster = Some(cfg);
        self
    }

    /// Set the cluster-wide topic name. Defaults to the bus name.
    /// No-op unless `cluster()` was called first.
    #[cfg(feature = "bus-cluster")]
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        if let Some(c) = self.cluster.as_mut() {
            c.topic = topic.into();
        }
        self
    }

    /// Set the cross-node type tag used to dispatch incoming PDUs.
    /// Defaults to the topic.
    #[cfg(feature = "bus-cluster")]
    pub fn type_id(mut self, id: impl Into<String>) -> Self {
        if let Some(c) = self.cluster.as_mut() {
            c.type_id = id.into();
        }
        self
    }

    /// Provide encode/decode closures for cross-node delivery.
    /// `encode` is called for each `publish`; `decode` runs on
    /// inbound PDUs from peer nodes.
    #[cfg(feature = "bus-cluster")]
    pub fn codec<EncFn, DecFn>(mut self, encode: EncFn, decode: DecFn) -> Self
    where
        EncFn: Fn(&E) -> Vec<u8> + Send + Sync + 'static,
        DecFn: Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static,
    {
        if let Some(c) = self.cluster.as_mut() {
            c.encode = Arc::new(encode);
            c.decode = Arc::new(decode);
        }
        self
    }

    pub fn build(self) -> BusTopology<E> {
        BusTopology {
            name: self.name.unwrap_or_else(|| "bus".into()),
            #[cfg(feature = "bus-cluster")]
            cluster: self.cluster,
            _ev: PhantomData,
        }
    }
}

pub struct BusTopology<E: Clone + Send + 'static> {
    #[allow(dead_code)]
    name: String,
    #[cfg(feature = "bus-cluster")]
    cluster: Option<ClusterConfig<E>>,
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
    #[cfg(feature = "bus-cluster")]
    cluster: Option<ClusterConfig<E>>,
}

impl<E: Clone + Send + 'static> BusHandles<E> {
    /// Broadcast `event` to every live subscriber. Closed receivers
    /// are pruned in-line. When the bus is configured with
    /// `cluster()`, also forwards to remote nodes via
    /// [`atomr_cluster_tools::ClusterPubSub`].
    pub fn publish(&self, event: E) {
        #[cfg(feature = "bus-cluster")]
        {
            if let Some(cfg) = &self.inner.cluster {
                // publish_remote fans out locally via DistributedPubSub
                // (delivers to our internal BusRouter actor, which
                // forwards to subscribers) AND forwards to peer nodes.
                let encode = cfg.encode.clone();
                cfg.cluster.publish_remote::<E, _>(&cfg.topic, event, &cfg.type_id, |e| {
                    encode(e)
                });
                return;
            }
        }
        // Local-only path.
        let mut guard = self.inner.subscribers.write();
        guard.retain(|tx| tx.send(event.clone()).is_ok());
    }

    /// Subscribe and receive a fresh channel. The returned
    /// [`UnboundedReceiver`] is closed when the bus drops or the
    /// receiver is dropped. Subscribers receive both locally-published
    /// events *and* events forwarded from peer nodes (when
    /// clustered).
    pub fn subscribe(&self) -> UnboundedReceiver<E> {
        let (tx, rx) = unbounded_channel();
        self.inner.subscribers.write().push(tx);
        rx
    }

}

/// Internal actor that bridges DistributedPubSub deliveries (typed
/// `ActorRef<E>`) into the bus's mpsc subscriber list.
#[cfg(feature = "bus-cluster")]
struct BusRouter<E: Clone + Send + 'static> {
    inner: Arc<BusInner<E>>,
}

#[cfg(feature = "bus-cluster")]
#[async_trait]
impl<E: Clone + Send + 'static> Actor for BusRouter<E> {
    type Msg = E;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: E) {
        let mut guard = self.inner.subscribers.write();
        guard.retain(|tx| tx.send(msg.clone()).is_ok());
    }
}

#[async_trait]
impl<E: Clone + Send + 'static> Topology for BusTopology<E> {
    type Handles = BusHandles<E>;

    #[cfg_attr(not(feature = "bus-cluster"), allow(unused_variables))]
    async fn materialize(self, system: &ActorSystem) -> Result<Self::Handles, PatternError<()>> {
        let inner = Arc::new(BusInner {
            subscribers: RwLock::new(Vec::new()),
            #[cfg(feature = "bus-cluster")]
            cluster: self.cluster,
        });
        let handles = BusHandles { inner: inner.clone() };

        // Cluster wiring: spawn a router actor, subscribe it to the
        // local DistributedPubSub for the topic (so announce_to sees
        // it), and register a decoder that re-publishes inbound peer
        // PDUs into the same local pubsub. The router forwards
        // received events into our subscribers list.
        #[cfg(feature = "bus-cluster")]
        if let Some(cfg) = inner.cluster.as_ref() {
            let router_inner = inner.clone();
            let router_name = format!("bus-router-{}", self.name);
            let router_ref = system
                .actor_of(
                    Props::create(move || BusRouter::<E> { inner: router_inner.clone() }),
                    &router_name,
                )
                .map_err(|e| PatternError::Invariant(format!("spawn bus router: {e}")))?;

            cfg.local.subscribe(cfg.topic.clone(), router_ref);

            let local_for_decoder = cfg.local.clone();
            let topic_for_decoder = cfg.topic.clone();
            let decode = cfg.decode.clone();
            cfg.cluster.register_decoder(cfg.type_id.clone(), move |bytes| {
                match decode(bytes) {
                    Ok(event) => {
                        local_for_decoder.publish_msg::<E>(&topic_for_decoder, event) > 0
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "cluster bus decode failed");
                        false
                    }
                }
            });
        }

        Ok(handles)
    }
}
