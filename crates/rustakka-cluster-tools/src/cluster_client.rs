//! ClusterClient / ClusterReceptionist — dispatching to actors addressable by name.
//! akka.net: `Akka.Cluster.Tools/Client/ClusterClient.cs`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use rustakka_core::actor::UntypedActorRef;

#[derive(Default)]
pub struct ClusterReceptionist {
    services: RwLock<HashMap<String, UntypedActorRef>>,
}

impl ClusterReceptionist {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, name: impl Into<String>, r: UntypedActorRef) {
        self.services.write().insert(name.into(), r);
    }

    pub fn lookup(&self, name: &str) -> Option<UntypedActorRef> {
        self.services.read().get(name).cloned()
    }

    pub fn unregister(&self, name: &str) {
        self.services.write().remove(name);
    }
}

pub struct ClusterClient {
    pub receptionist: Arc<ClusterReceptionist>,
}

impl ClusterClient {
    pub fn new(receptionist: Arc<ClusterReceptionist>) -> Self {
        Self { receptionist }
    }

    pub fn send(&self, name: &str) -> Option<UntypedActorRef> {
        self.receptionist.lookup(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustakka_core::actor::Inbox;

    #[test]
    fn receptionist_register_lookup() {
        let rec = ClusterReceptionist::new();
        let inbox = Inbox::<u32>::new("svc");
        rec.register("svc", inbox.actor_ref().as_untyped());
        let c = ClusterClient::new(rec);
        assert!(c.send("svc").is_some());
    }
}
