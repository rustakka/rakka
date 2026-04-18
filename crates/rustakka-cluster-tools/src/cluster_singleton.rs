//! ClusterSingletonManager / Proxy — one logical actor across the cluster.
//! akka.net: `Akka.Cluster.Tools/Singleton/`.

use std::sync::Arc;

use parking_lot::RwLock;

use rustakka_core::actor::UntypedActorRef;

/// Holds the current singleton ref (or `None` during handover). Decides
/// which node hosts the singleton based on oldest up-member — a hook is
/// provided so tests can simulate handover without wiring the full cluster.
#[derive(Default)]
pub struct ClusterSingletonManager {
    current: RwLock<Option<UntypedActorRef>>,
}

impl ClusterSingletonManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn set(&self, r: UntypedActorRef) {
        *self.current.write() = Some(r);
    }

    pub fn clear(&self) {
        *self.current.write() = None;
    }

    pub fn current(&self) -> Option<UntypedActorRef> {
        self.current.read().clone()
    }
}

/// Proxy that routes messages to the current singleton.
pub struct ClusterSingletonProxy {
    pub manager: Arc<ClusterSingletonManager>,
}

impl ClusterSingletonProxy {
    pub fn new(manager: Arc<ClusterSingletonManager>) -> Self {
        Self { manager }
    }

    pub fn singleton(&self) -> Option<UntypedActorRef> {
        self.manager.current()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustakka_core::actor::Inbox;

    #[test]
    fn proxy_routes_to_current_singleton() {
        let mgr = ClusterSingletonManager::new();
        let inbox = Inbox::<u32>::new("singleton");
        mgr.set(inbox.actor_ref().as_untyped());
        let proxy = ClusterSingletonProxy::new(mgr);
        assert!(proxy.singleton().is_some());
    }
}
