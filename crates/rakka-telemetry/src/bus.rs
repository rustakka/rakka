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
            Self::ActorSpawned(_) | Self::ActorStopped { .. } | Self::MailboxSampled { .. } => {
                "actors"
            }
            Self::DeadLetter(_) => "dead_letters",
            Self::ClusterChanged(_) => "cluster",
            Self::ShardingChanged(_) => "sharding",
            Self::JournalWrite(_) => "persistence",
            Self::RemoteAssociation(_) => "remote",
            Self::StreamsGraphStarted { .. } | Self::StreamsGraphFinished { .. } => "streams",
            Self::DDataUpdated { .. } => "ddata",
        }
    }
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
}
