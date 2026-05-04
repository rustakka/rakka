//! ShardRegion — routes messages to the correct local or remote shard.
//! akka.net: `ShardRegion.cs`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::coordinator::ShardCoordinator;
use crate::extractor::MessageExtractor;
use crate::shard::{EntityHandler, Shard};

/// Closure used by the region to forward a message to a remote shard
/// owner. Wired up by `AtomrSharding::with_remote(...)` once a remote
/// system is available; absent otherwise (in which case messages routed
/// to a non-local shard are dropped with a debug log).
pub type RemoteForwarder<M> = Arc<dyn Fn(&str, M) + Send + Sync>;

pub struct ShardRegion<E: MessageExtractor> {
    region_id: String,
    extractor: Arc<E>,
    coordinator: Arc<ShardCoordinator>,
    shards: RwLock<HashMap<String, Arc<Shard<E::Message>>>>,
    handler_factory: Arc<dyn Fn() -> EntityHandler<E::Message> + Send + Sync>,
    remote_forwarder: RwLock<Option<RemoteForwarder<E::Message>>>,
}

impl<E: MessageExtractor> ShardRegion<E> {
    pub fn new(
        region_id: impl Into<String>,
        extractor: Arc<E>,
        coordinator: Arc<ShardCoordinator>,
        handler_factory: Arc<dyn Fn() -> EntityHandler<E::Message> + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            region_id: region_id.into(),
            extractor,
            coordinator,
            shards: RwLock::new(HashMap::new()),
            handler_factory,
            remote_forwarder: RwLock::new(None),
        })
    }

    pub fn region_id(&self) -> &str {
        &self.region_id
    }

    /// Install a forwarder that ships messages addressed to a remote
    /// shard owner to that owner's `ShardRegion` over `atomr-remote`.
    pub fn set_remote_forwarder(&self, forwarder: RemoteForwarder<E::Message>) {
        *self.remote_forwarder.write() = Some(forwarder);
    }

    pub fn deliver(&self, message: E::Message) {
        let shard_id = self.extractor.shard_id(&message);
        let entity_id = self.extractor.entity_id(&message);
        let owner = self.coordinator.allocate(&shard_id, &self.region_id);

        if owner != self.region_id {
            if let Some(fwd) = self.remote_forwarder.read().clone() {
                fwd(&owner, message);
            } else {
                tracing::debug!(
                    shard = %shard_id,
                    owner = %owner,
                    "no remote forwarder installed; dropping"
                );
            }
            return;
        }

        let shard = {
            let mut map = self.shards.write();
            map.entry(shard_id.clone())
                .or_insert_with(|| Arc::new(Shard::new(shard_id.clone(), (self.handler_factory)())))
                .clone()
        };

        shard.deliver(&entity_id, message);
    }

    pub fn shard_count(&self) -> usize {
        self.shards.read().len()
    }

    /// Names of the shards currently owned by this region.
    pub fn shard_ids(&self) -> Vec<String> {
        self.shards.read().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Ex;
    impl MessageExtractor for Ex {
        type Message = (String, u32);
        fn entity_id(&self, m: &Self::Message) -> String {
            m.0.clone()
        }
        fn shard_id(&self, m: &Self::Message) -> String {
            format!("shard-{}", (m.0.len() % 4))
        }
    }

    #[test]
    fn region_routes_to_shard_and_invokes_handler() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        let coord = Arc::new(ShardCoordinator::new());
        let region = ShardRegion::new(
            "r1",
            Arc::new(Ex),
            coord,
            Arc::new(|| {
                Box::new(|_id: &str, _msg: (String, u32)| {
                    CALLS.fetch_add(1, Ordering::SeqCst);
                })
            }),
        );

        region.deliver(("alice".into(), 1));
        region.deliver(("bob".into(), 2));
        region.deliver(("alice".into(), 3));
        assert_eq!(CALLS.load(Ordering::SeqCst), 3);
    }
}
