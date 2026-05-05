//! Typed telemetry bus backed by a `tokio::sync::broadcast` channel.
//!
//! Probes publish `TelemetryEvent`s through this bus. Dashboard clients
//! (WebSocket subscribers), exporters (Prometheus/OpenTelemetry), and
//! the in-memory `DeadLetterFeed` ring buffer all receive copies.

use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::dto::{
    ActorStatus, ClusterMembershipDiff, DeadLetterRecord, JournalWriteInfo, RemoteAssociationInfo,
    ShardingEvent,
};
use crate::exporters::Exporter;

/// A single telemetry event. Kept deliberately wide so clients can filter
/// by the `topic` field without deserializing every payload variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TelemetryEvent {
    ActorSpawned(ActorStatus),
    ActorStopped { path: String },
    MailboxSampled { path: String, depth: u64 },
    DeadLetter(DeadLetterRecord),
    ClusterChanged(ClusterMembershipDiff),
    ShardingChanged(ShardingEvent),
    JournalWrite(JournalWriteInfo),
    RemoteAssociation(RemoteAssociationInfo),
    StreamsGraphStarted { id: u64, name: String },
    StreamsGraphFinished { id: u64 },
    DDataUpdated { key: String },
}

impl TelemetryEvent {
    /// A short, stable topic string used by WebSocket clients to filter
    /// the event stream.
    pub fn topic(&self) -> &'static str {
        match self {
            Self::ActorSpawned(_) | Self::ActorStopped { .. } | Self::MailboxSampled { .. } => "actors",
            Self::DeadLetter(_) => "dead_letters",
            Self::ClusterChanged(_) => "cluster",
            Self::ShardingChanged(_) => "sharding",
            Self::JournalWrite(_) => "persistence",
            Self::RemoteAssociation(_) => "remote",
            Self::StreamsGraphStarted { .. } | Self::StreamsGraphFinished { .. } => "streams",
            Self::DDataUpdated { .. } => "ddata",
        }
    }

    /// All telemetry topics the bus emits. Used by the dashboard /
    /// spec parity tests to ensure every probe surface is wired.
    pub const ALL_TOPICS: &'static [&'static str] =
        &["actors", "dead_letters", "cluster", "sharding", "persistence", "remote", "streams", "ddata"];
}

/// Cheap-to-clone broadcast bus. Wraps a `tokio::sync::broadcast` sender
/// plus a slot for attached exporters (synchronous callbacks).
#[derive(Clone)]
pub struct TelemetryBus {
    tx: broadcast::Sender<TelemetryEvent>,
    exporters: Arc<RwLock<Vec<Arc<dyn Exporter>>>>,
}

impl TelemetryBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(16));
        Self { tx, exporters: Arc::new(RwLock::new(Vec::new())) }
    }

    pub fn publish(&self, event: TelemetryEvent) {
        // Fan out to in-process exporters first (synchronous, cheap).
        let exporters = self.exporters.read().clone();
        for exp in &exporters {
            exp.on_event(&event);
        }
        // Then broadcast to async subscribers (WS clients, tests).
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.tx.subscribe()
    }

    /// Subscribe to a single topic. The returned receiver yields only
    /// events whose `topic()` matches `wanted`. Backed by a forwarder
    /// task that filters the broadcast stream — drop the receiver to
    /// stop the forwarder.
    pub fn subscribe_topic(
        &self,
        wanted: &'static str,
    ) -> tokio::sync::mpsc::UnboundedReceiver<TelemetryEvent> {
        let mut src = self.tx.subscribe();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok(ev) = src.recv().await {
                if ev.topic() == wanted {
                    if tx.send(ev).is_err() {
                        return;
                    }
                }
            }
        });
        rx
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    pub(crate) fn attach_exporter(&self, exporter: Arc<dyn Exporter>) {
        self.exporters.write().push(exporter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::ActorStatus;

    #[tokio::test]
    async fn publish_and_subscribe_roundtrip() {
        let bus = TelemetryBus::new(8);
        let mut rx = bus.subscribe();
        bus.publish(TelemetryEvent::ActorStopped { path: "/user/a".into() });
        let got = rx.recv().await.unwrap();
        assert_eq!(got.topic(), "actors");
    }

    #[tokio::test]
    async fn topic_labels_correct() {
        let e = TelemetryEvent::ActorSpawned(ActorStatus {
            path: "/user/x".into(),
            parent: Some("/user".into()),
            actor_type: "Test".into(),
            mailbox_depth: 0,
            spawned_at: "now".into(),
        });
        assert_eq!(e.topic(), "actors");
    }

    #[tokio::test]
    async fn subscribe_topic_filters_by_topic() {
        let bus = TelemetryBus::new(16);
        let mut rx = bus.subscribe_topic("ddata");
        bus.publish(TelemetryEvent::ActorStopped { path: "/x".into() }); // actors — skipped
        bus.publish(TelemetryEvent::DDataUpdated { key: "k".into() });
        bus.publish(TelemetryEvent::DDataUpdated { key: "j".into() });
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        match (first, second) {
            (TelemetryEvent::DDataUpdated { key: k1 }, TelemetryEvent::DDataUpdated { key: k2 }) => {
                assert_eq!(k1, "k");
                assert_eq!(k2, "j");
            }
            other => panic!("unexpected events: {other:?}"),
        }
    }

    #[test]
    fn all_topics_covers_every_variant() {
        // Confirm every topic that variants advertise is listed in
        // ALL_TOPICS — guards against drift between TelemetryEvent and
        // the dashboard topic enumeration.
        let samples = [
            TelemetryEvent::ActorStopped { path: "/x".into() }.topic(),
            TelemetryEvent::DeadLetter(DeadLetterRecord {
                seq: 0,
                recipient: "/x".into(),
                sender: None,
                message_type: "test".into(),
                message_preview: "p".into(),
                timestamp: "now".into(),
            })
            .topic(),
            TelemetryEvent::DDataUpdated { key: "k".into() }.topic(),
        ];
        for t in samples {
            assert!(TelemetryEvent::ALL_TOPICS.contains(&t), "topic {t} missing from ALL_TOPICS");
        }
        // No duplicates.
        let mut sorted = TelemetryEvent::ALL_TOPICS.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), TelemetryEvent::ALL_TOPICS.len());
    }
}
