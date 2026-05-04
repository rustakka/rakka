//! A Shard owns entities (by entity_id). akka.net: `Shard.cs`.

use std::collections::HashMap;

use parking_lot::RwLock;

/// Entity handler — a hook invoked when a message arrives for an entity.
///
/// Replaces the `IActorRef` per entity in akka.net; the `tell` hook can
/// forward into whatever actor runtime the caller wires up.
pub type EntityHandler<M> = Box<dyn Fn(&str, M) + Send + Sync + 'static>;

pub struct Shard<M: Send + 'static> {
    shard_id: String,
    entities: RwLock<HashMap<String, ()>>,
    handler: EntityHandler<M>,
}

impl<M: Send + 'static> Shard<M> {
    pub fn new(shard_id: impl Into<String>, handler: EntityHandler<M>) -> Self {
        Self { shard_id: shard_id.into(), entities: RwLock::new(HashMap::new()), handler }
    }

    pub fn shard_id(&self) -> &str {
        &self.shard_id
    }

    pub fn entity_count(&self) -> usize {
        self.entities.read().len()
    }

    pub fn deliver(&self, entity_id: &str, msg: M) {
        self.entities.write().entry(entity_id.to_string()).or_insert(());
        (self.handler)(entity_id, msg);
    }

    pub fn passivate(&self, entity_id: &str) {
        self.entities.write().remove(entity_id);
    }
}
