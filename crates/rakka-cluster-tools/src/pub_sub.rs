//! DistributedPubSub (local-topic subset).
//! akka.net: `Akka.Cluster.Tools/PublishSubscribe/DistributedPubSubMediator.cs`.
//!
//! This implementation is single-node today; the mediator pattern is in
//! place so cross-node routing is plugged in during full cluster wiring.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use rakka_core::actor::UntypedActorRef;

#[derive(Default)]
pub struct DistributedPubSub {
    topics: RwLock<HashMap<String, Vec<UntypedActorRef>>>,
}

impl DistributedPubSub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn subscribe(&self, topic: impl Into<String>, subscriber: UntypedActorRef) {
        self.topics.write().entry(topic.into()).or_default().push(subscriber);
    }

    pub fn unsubscribe(&self, topic: &str, subscriber_path: &rakka_core::actor::ActorPath) {
        if let Some(v) = self.topics.write().get_mut(topic) {
            v.retain(|s| s.path() != subscriber_path);
        }
    }

    pub fn publish(&self, topic: &str) -> Vec<UntypedActorRef> {
        self.topics.read().get(topic).cloned().unwrap_or_default()
    }

    pub fn topic_count(&self) -> usize {
        self.topics.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_core::actor::Inbox;

    #[test]
    fn subscribe_and_publish() {
        let bus = DistributedPubSub::new();
        let inbox = Inbox::<u32>::new("s");
        bus.subscribe("greetings", inbox.actor_ref().as_untyped());
        let subs = bus.publish("greetings");
        assert_eq!(subs.len(), 1);
    }
}
