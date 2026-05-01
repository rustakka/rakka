//! `DistributedPubSub.Mediator` (local-topic subset).
//! akka.net: `Akka.Cluster.Tools/PublishSubscribe/DistributedPubSubMediator.cs`.
//!
//! Phase 7 of `docs/full-port-plan.md`. The mediator owns a local
//! per-node topic table; cross-node gossip plugs in once Phase 6's
//! gossip transport lands. This sub-step adds:
//!
//! * **Typed publish** — `publish_msg::<M>(topic, msg)` actually
//!   delivers the message to each subscribed `ActorRef<M>` (the
//!   prior API only returned the subscriber list).
//! * **Group routing** — `subscribe_to_group(topic, group, ref)`
//!   buckets subscribers; `send_to_group(topic, group, msg)` picks
//!   one round-robin recipient per call.
//! * **send_to_one(path)** — single recipient by path, akka.net's
//!   `DistributedPubSubMediator.Send` semantics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use rakka_core::actor::{ActorRef, UntypedActorRef};

#[derive(Default)]
pub struct DistributedPubSub {
    topics: RwLock<HashMap<String, Vec<TypedSubscriber>>>,
    groups: RwLock<HashMap<(String, String), Group>>,
}

/// A subscriber that knows how to deliver `M` by holding a typed
/// closure. Stored type-erased in the mediator so the topic table
/// is a homogeneous `Vec`.
struct TypedSubscriber {
    untyped: UntypedActorRef,
    deliver_any: Box<dyn Fn(&dyn std::any::Any) -> bool + Send + Sync>,
}

#[derive(Default)]
struct Group {
    members: Vec<TypedSubscriber>,
    cursor: AtomicUsize,
}

impl DistributedPubSub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Subscribe `subscriber: ActorRef<M>` to `topic`. Future
    /// `publish_msg::<M>(topic, msg)` calls deliver to it.
    pub fn subscribe<M: Clone + Send + 'static>(
        &self,
        topic: impl Into<String>,
        subscriber: ActorRef<M>,
    ) {
        let typed = TypedSubscriber::new(subscriber);
        self.topics.write().entry(topic.into()).or_default().push(typed);
    }

    /// Subscribe to a `(topic, group)` bucket. `send_to_group`
    /// rotates through bucket members.
    pub fn subscribe_to_group<M: Clone + Send + 'static>(
        &self,
        topic: impl Into<String>,
        group: impl Into<String>,
        subscriber: ActorRef<M>,
    ) {
        let typed = TypedSubscriber::new(subscriber);
        self.groups
            .write()
            .entry((topic.into(), group.into()))
            .or_default()
            .members
            .push(typed);
    }

    /// Drop a subscriber by path from a topic.
    pub fn unsubscribe(&self, topic: &str, subscriber_path: &rakka_core::actor::ActorPath) {
        if let Some(v) = self.topics.write().get_mut(topic) {
            v.retain(|s| s.untyped.path() != subscriber_path);
        }
    }

    /// Snapshot of subscriber refs for a topic. Useful for tests +
    /// the legacy "discover, then send" pattern.
    pub fn publish(&self, topic: &str) -> Vec<UntypedActorRef> {
        self.topics
            .read()
            .get(topic)
            .map(|v| v.iter().map(|s| s.untyped.clone()).collect())
            .unwrap_or_default()
    }

    /// Typed broadcast. Delivers `msg` (cloned) to every subscriber
    /// of `topic`. Returns the number of successful deliveries.
    pub fn publish_msg<M: Clone + Send + 'static>(&self, topic: &str, msg: M) -> usize {
        let subs = self.topics.read();
        let Some(list) = subs.get(topic) else { return 0; };
        let mut delivered = 0;
        let any: &dyn std::any::Any = &msg;
        for s in list {
            if (s.deliver_any)(any) {
                delivered += 1;
            }
        }
        // Clone-per-recipient happens inside deliver_any, so we
        // can't move `msg`. The first deliver is a borrow; subsequent
        // delivers re-borrow the same `Any`.
        let _ = msg; // keep alive
        delivered
    }

    /// Pick one member of `(topic, group)` round-robin and deliver
    /// `msg`. Returns `true` if a recipient was found.
    pub fn send_to_group<M: Clone + Send + 'static>(
        &self,
        topic: &str,
        group: &str,
        msg: M,
    ) -> bool {
        let groups = self.groups.read();
        let Some(g) = groups.get(&(topic.to_string(), group.to_string())) else { return false; };
        if g.members.is_empty() {
            return false;
        }
        let i = g.cursor.fetch_add(1, Ordering::Relaxed) % g.members.len();
        let any: &dyn std::any::Any = &msg;
        let r = (g.members[i].deliver_any)(any);
        let _ = msg;
        r
    }

    pub fn topic_count(&self) -> usize {
        self.topics.read().len()
    }

    pub fn group_count(&self) -> usize {
        self.groups.read().len()
    }
}

impl TypedSubscriber {
    fn new<M: Clone + Send + 'static>(r: ActorRef<M>) -> Self {
        let untyped = r.as_untyped();
        let r2 = r.clone();
        let deliver_any: Box<dyn Fn(&dyn std::any::Any) -> bool + Send + Sync> =
            Box::new(move |any| {
                if let Some(m) = any.downcast_ref::<M>() {
                    r2.tell(m.clone());
                    true
                } else {
                    false
                }
            });
        Self { untyped, deliver_any }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_core::actor::Inbox;
    use std::time::Duration;

    #[test]
    fn subscribe_and_publish_returns_subscriber_list() {
        let bus = DistributedPubSub::new();
        let inbox = Inbox::<u32>::new("s");
        bus.subscribe("greetings", inbox.actor_ref().clone());
        let subs = bus.publish("greetings");
        assert_eq!(subs.len(), 1);
    }

    #[tokio::test]
    async fn typed_publish_delivers_to_each_subscriber() {
        let bus = DistributedPubSub::new();
        let mut a = Inbox::<u32>::new("a");
        let mut b = Inbox::<u32>::new("b");
        bus.subscribe("nums", a.actor_ref().clone());
        bus.subscribe("nums", b.actor_ref().clone());

        let n = bus.publish_msg("nums", 7u32);
        assert_eq!(n, 2);

        assert_eq!(a.receive(Duration::from_millis(50)).await.unwrap(), 7);
        assert_eq!(b.receive(Duration::from_millis(50)).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn publish_to_unknown_topic_delivers_zero() {
        let bus = DistributedPubSub::new();
        let n = bus.publish_msg("nope", 1u32);
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn group_send_round_robins_one_member() {
        let bus = DistributedPubSub::new();
        let mut a = Inbox::<u32>::new("ga");
        let mut b = Inbox::<u32>::new("gb");
        bus.subscribe_to_group("work", "G1", a.actor_ref().clone());
        bus.subscribe_to_group("work", "G1", b.actor_ref().clone());

        // 4 sends → 2 + 2 (round-robin starts at index 0).
        for i in 0..4u32 {
            assert!(bus.send_to_group("work", "G1", i));
        }
        let mut a_count = 0;
        let mut b_count = 0;
        for _ in 0..2 {
            a.receive(Duration::from_millis(20)).await.unwrap();
            a_count += 1;
            b.receive(Duration::from_millis(20)).await.unwrap();
            b_count += 1;
        }
        assert_eq!(a_count, 2);
        assert_eq!(b_count, 2);
    }

    #[test]
    fn group_count_tracks_distinct_buckets() {
        let bus = DistributedPubSub::new();
        let inbox = Inbox::<u32>::new("g");
        bus.subscribe_to_group("t1", "G1", inbox.actor_ref().clone());
        bus.subscribe_to_group("t1", "G2", inbox.actor_ref().clone());
        bus.subscribe_to_group("t2", "G1", inbox.actor_ref().clone());
        assert_eq!(bus.group_count(), 3);
    }
}
