//! `RemoteActorRefProvider`. akka.net: `Remote/RemoteActorRefProvider.cs`.
//!
//! Plug-in for `atomr_core::ActorSystem` that resolves
//! `akka.tcp://...`-style paths into `RemoteActorRefImpl` handles.

use std::sync::Arc;

use atomr_core::actor::{ActorPath, ActorSystem, Address, RemoteProvider, RemoteRef};

use crate::endpoint_manager::EndpointManager;
use crate::remote_ref::RemoteActorRefImpl;
use crate::serialization::SerializerRegistry;
use crate::system_daemon::RemoteSystemDaemon;

/// Per-system remote provider. Created by `enable_remote`.
pub struct RemoteActorRefProvider {
    local_address: Address,
    local_uid: u64,
    endpoint_manager: EndpointManager,
    registry: SerializerRegistry,
    pub system_daemon: Arc<RemoteSystemDaemon>,
}

impl RemoteActorRefProvider {
    pub fn new(
        local_address: Address,
        local_uid: u64,
        endpoint_manager: EndpointManager,
        registry: SerializerRegistry,
        system_daemon: Arc<RemoteSystemDaemon>,
    ) -> Arc<Self> {
        Arc::new(Self { local_address, local_uid, endpoint_manager, registry, system_daemon })
    }

    pub fn endpoint_manager(&self) -> &EndpointManager {
        &self.endpoint_manager
    }

    pub fn registry(&self) -> &SerializerRegistry {
        &self.registry
    }

    /// Install on an `ActorSystem` so `actor_selection` resolves remote paths.
    pub fn install(self: &Arc<Self>, system: &ActorSystem) {
        let p: Arc<dyn RemoteProvider> = self.clone();
        system.set_remote_provider(p);
    }
}

impl RemoteProvider for RemoteActorRefProvider {
    fn local_address(&self) -> &Address {
        &self.local_address
    }

    fn resolve(&self, path: &ActorPath) -> Option<Arc<dyn RemoteRef>> {
        if path.address == self.local_address {
            return None;
        }
        let r = RemoteActorRefImpl::new(
            path.clone(),
            self.endpoint_manager.clone(),
            self.registry.clone(),
            self.local_uid,
        );
        Some(Arc::new(r))
    }
}
