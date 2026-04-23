//! Endpoint registry. akka.net: `Remote/EndpointRegistry.cs`.

use std::sync::Arc;

use dashmap::DashMap;

use rustakka_core::actor::Address;

use crate::endpoint::Endpoint;

#[derive(Default)]
pub struct EndpointRegistry {
    eps: DashMap<String, Arc<Endpoint>>,
}

impl EndpointRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, ep: Arc<Endpoint>) {
        self.eps.insert(ep.remote.to_string(), ep);
    }

    pub fn get(&self, addr: &Address) -> Option<Arc<Endpoint>> {
        self.eps.get(&addr.to_string()).map(|e| e.clone())
    }

    pub fn remove(&self, addr: &Address) {
        self.eps.remove(&addr.to_string());
    }

    pub fn len(&self) -> usize {
        self.eps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.eps.is_empty()
    }

    /// Snapshot of all remote addresses currently associated.
    pub fn addresses(&self) -> Vec<String> {
        self.eps.iter().map(|e| e.key().clone()).collect()
    }
}
