//! A typed handle to an entity inside a shard region. akka.net: `EntityRef<T>`.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::shard_region::ShardRegion;

#[derive(Clone)]
pub struct EntityRef<E: crate::extractor::MessageExtractor> {
    pub(crate) region: Arc<ShardRegion<E>>,
    pub(crate) entity_id: String,
    _marker: PhantomData<E>,
}

impl<E: crate::extractor::MessageExtractor> EntityRef<E> {
    pub fn new(region: Arc<ShardRegion<E>>, entity_id: String) -> Self {
        Self { region, entity_id, _marker: PhantomData }
    }

    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub fn tell(&self, msg: E::Message) {
        self.region.deliver(msg);
    }
}
