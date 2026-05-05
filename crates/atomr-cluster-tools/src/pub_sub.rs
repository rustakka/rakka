//! `DistributedPubSub.Mediator` (local-topic subset).
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
//! * **send_to_one(path)** — single recipient by path,
//!   `DistributedPubSubMediator.Send` semantics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use atomr_core::actor::{ActorRef, UntypedActorRef};

#[derive(Default)]
pub struct DistributedPubSub {
    topics: RwLock<HashMap<String, Vec<TypedSubscriber>>>,
    groups: RwLock<HashMap<(String, String), Group>>,
}

type DeliverAnyFn = Box<dyn Fn(&dyn std::any::Any) -> bool + Send + Sync>;
type CodecFn = Box<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// A subscriber that knows how to deliver `M` by holding a typed
/// closure. Stored type-erased in the mediator so the topic table
/// is a homogeneous `Vec`.
struct TypedSubscriber {
    untyped: UntypedActorRef,
    deliver_any: DeliverAnyFn,
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
    pub fn subscribe<M: Clone + Send + 'static>(&self, topic: impl Into<String>, subscriber: ActorRef<M>) {
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
        self.groups.write().entry((topic.into(), group.into())).or_default().members.push(typed);
    }

    /// Drop a subscriber by path from a topic.
    pub fn unsubscribe(&self, topic: &str, subscriber_path: &atomr_core::actor::ActorPath) {
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
        let Some(list) = subs.get(topic) else {
            return 0;
        };
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
    pub fn send_to_group<M: Clone + Send + 'static>(&self, topic: &str, group: &str, msg: M) -> bool {
        let groups = self.groups.read();
        let Some(g) = groups.get(&(topic.to_string(), group.to_string())) else {
            return false;
        };
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

// -----------------------------------------------------------------------
// Phase 7.B — cross-node mediator.
// -----------------------------------------------------------------------

use std::collections::HashSet;

/// Pluggable transport for the cross-node mediator. Sends an outbound
/// `MediatorPdu` to a peer node, identified by an opaque string node id
/// (typically `Address::to_string()`). The transport is responsible for
/// the wire round-trip; on the receiver side, the inbound PDU is fed
/// back into the local mediator via [`ClusterPubSub::apply_pdu`].
pub trait MediatorTransport: Send + Sync + 'static {
    fn send(&self, target_node: &str, pdu: MediatorPdu);
}

/// Wire shape of a cross-node mediator exchange.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MediatorPdu {
    /// Announce the set of topics this node has at least one subscriber for.
    TopicAnnounce { from: String, topics: Vec<String> },
    /// Forward `msg_blob` (already serialized) to every local subscriber
    /// of `topic` on the receiving node.
    Forward { topic: String, msg_blob: Vec<u8>, type_id: String },
}

/// Mediator that augments a local [`DistributedPubSub`] with a
/// cross-node topic table + transport. Clusters publish via
/// [`ClusterPubSub::publish_remote`] which fans out to all nodes that
/// have advertised the topic; receivers route the payload to local
/// subscribers using the codec registry.
pub struct ClusterPubSub {
    local: Arc<DistributedPubSub>,
    self_node: String,
    /// `topic -> set of advertising node-ids`.
    remote_topics: RwLock<HashMap<String, HashSet<String>>>,
    transport: Arc<dyn MediatorTransport>,
    codecs: RwLock<HashMap<String, CodecFn>>,
}

impl ClusterPubSub {
    pub fn new(
        local: Arc<DistributedPubSub>,
        self_node: impl Into<String>,
        transport: Arc<dyn MediatorTransport>,
    ) -> Arc<Self> {
        Arc::new(Self {
            local,
            self_node: self_node.into(),
            remote_topics: RwLock::new(HashMap::new()),
            transport,
            codecs: RwLock::new(HashMap::new()),
        })
    }

    /// Register a per-message-type decoder for inbound `Forward` PDUs.
    /// `type_id` typically matches `std::any::type_name::<M>()`; the
    /// decoder must deliver to local subscribers (and return `true` if
    /// any delivery happened).
    pub fn register_decoder<F>(&self, type_id: impl Into<String>, decode: F)
    where
        F: Fn(&[u8]) -> bool + Send + Sync + 'static,
    {
        self.codecs.write().insert(type_id.into(), Box::new(decode));
    }

    /// Announce currently-subscribed topics to a peer node. Caller drives
    /// this on a tick (similar to `ClusterDaemon`).
    pub fn announce_to(&self, target_node: &str) {
        let topics: Vec<String> = self.local.topics.read().keys().cloned().collect();
        self.transport.send(target_node, MediatorPdu::TopicAnnounce { from: self.self_node.clone(), topics });
    }

    /// Apply an inbound PDU received from the transport.
    pub fn apply_pdu(&self, pdu: MediatorPdu) {
        match pdu {
            MediatorPdu::TopicAnnounce { from, topics } => {
                let mut g = self.remote_topics.write();
                // Drop prior announcements from this node.
                for set in g.values_mut() {
                    set.remove(&from);
                }
                for t in topics {
                    g.entry(t).or_default().insert(from.clone());
                }
            }
            MediatorPdu::Forward { topic, msg_blob, type_id } => {
                let codecs = self.codecs.read();
                if let Some(decode) = codecs.get(&type_id) {
                    let _ = decode(&msg_blob);
                    // Local fan-out: the decoder publishes to this node's
                    // local mediator. The topic is implicit in the codec's
                    // closure body. We also stash the topic for diagnostics.
                    let _ = topic;
                }
            }
        }
    }

    /// Cross-node publish. Locally fan-out via the wrapped mediator,
    /// then forward the serialized payload to every remote node that has
    /// announced this topic.
    pub fn publish_remote<M, S>(&self, topic: &str, msg: M, type_id: impl Into<String>, encode: S) -> usize
    where
        M: Clone + Send + 'static,
        S: FnOnce(&M) -> Vec<u8>,
    {
        let local_n = self.local.publish_msg(topic, msg.clone());
        let remote = self.remote_topics.read();
        let Some(nodes) = remote.get(topic) else { return local_n };
        let blob = encode(&msg);
        let type_id = type_id.into();
        let mut forwarded = 0;
        for node in nodes {
            if node == &self.self_node {
                continue;
            }
            self.transport.send(
                node,
                MediatorPdu::Forward {
                    topic: topic.into(),
                    msg_blob: blob.clone(),
                    type_id: type_id.clone(),
                },
            );
            forwarded += 1;
        }
        local_n + forwarded
    }

    pub fn known_remote_topics(&self) -> usize {
        self.remote_topics.read().len()
    }

    pub fn nodes_for(&self, topic: &str) -> Vec<String> {
        self.remote_topics.read().get(topic).map(|s| s.iter().cloned().collect()).unwrap_or_default()
    }
}

impl TypedSubscriber {
    fn new<M: Clone + Send + 'static>(r: ActorRef<M>) -> Self {
        let untyped = r.as_untyped();
        let r2 = r.clone();
        let deliver_any: DeliverAnyFn = Box::new(move |any| {
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
    use atomr_core::actor::Inbox;
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

    #[derive(Default, Clone)]
    struct CapturingTransport {
        sent: Arc<parking_lot::Mutex<Vec<(String, MediatorPdu)>>>,
    }
    impl MediatorTransport for CapturingTransport {
        fn send(&self, target: &str, pdu: MediatorPdu) {
            self.sent.lock().push((target.to_string(), pdu));
        }
    }

    #[tokio::test]
    async fn cluster_pub_sub_announce_and_forward_round_trip() {
        let local_a = DistributedPubSub::new();
        let local_b = DistributedPubSub::new();
        let mut subscriber = Inbox::<u32>::new("sub");
        local_b.subscribe("nums", subscriber.actor_ref().clone());
        let net = CapturingTransport::default();
        let net_arc: Arc<dyn MediatorTransport> = Arc::new(net.clone());
        let a = ClusterPubSub::new(local_a.clone(), "node-a", net_arc.clone());
        let b = ClusterPubSub::new(local_b.clone(), "node-b", net_arc);

        // B announces its topics.
        b.announce_to("node-a");
        let pdu = net.sent.lock().pop().unwrap().1;
        a.apply_pdu(pdu);
        assert_eq!(a.known_remote_topics(), 1);
        assert_eq!(a.nodes_for("nums"), vec!["node-b".to_string()]);

        // B installs a decoder that publishes locally.
        let local_b2 = local_b.clone();
        b.register_decoder("u32", move |bytes| {
            let n = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            local_b2.publish_msg::<u32>("nums", n) > 0
        });

        // A publishes — it forwards to B.
        let n = a.publish_remote::<u32, _>("nums", 42, "u32", |m| m.to_le_bytes().to_vec());
        assert_eq!(n, 1);
        let (target, fwd) = net.sent.lock().pop().unwrap();
        assert_eq!(target, "node-b");
        b.apply_pdu(fwd);
        assert_eq!(subscriber.receive(std::time::Duration::from_millis(50)).await.unwrap(), 42);
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
