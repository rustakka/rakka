//! Prometheus exporter for `rakka-telemetry`.
//!
//! Maps incoming [`TelemetryEvent`]s onto a `prometheus::Registry` of
//! counters/gauges/histograms. Cardinality is bounded by design: we label
//! only with low-cardinality tags (`node`, `topic`, journal name, etc.).
//! Per-actor-path mailbox depth is exposed under a dedicated gauge with
//! the `actor_path` label, kept bounded by the actor-registry lifecycle.

use std::sync::Arc;

use parking_lot::RwLock;
use prometheus::{
    Encoder, Gauge, GaugeVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

use super::Exporter;
use crate::bus::TelemetryEvent;

/// Handle returned when installing the exporter. Call [`Self::render`] to
/// produce the `text/plain; version=0.0.4` body served under `/metrics`.
pub struct PrometheusExporter {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Registry,
    node: String,

    actors_spawned_total: IntCounter,
    actors_stopped_total: IntCounter,
    actors_live: IntGauge,
    mailbox_depth: IntGaugeVec,

    dead_letters_total: IntCounter,

    cluster_members_up: IntGauge,
    cluster_unreachable: IntGauge,
    cluster_member_events_total: IntCounterVec,

    sharding_events_total: IntCounterVec,
    sharding_allocations: IntGaugeVec,

    persistence_events_written_total: IntCounterVec,
    persistence_last_seq: IntGaugeVec,

    remote_endpoints: IntGauge,
    remote_association_events_total: IntCounterVec,
    remote_bytes: GaugeVec,

    streams_running: IntGauge,
    streams_started_total: IntCounter,
    streams_finished_total: IntCounter,

    ddata_updates_total: IntCounterVec,

    live_actors: Arc<RwLock<std::collections::HashSet<String>>>,
}

impl PrometheusExporter {
    /// Build a new exporter from a [`super::config::PrometheusConfig`].
    pub fn new(cfg: super::config::PrometheusConfig) -> Result<Self, prometheus::Error> {
        let ns = cfg.namespace.as_deref().unwrap_or("rakka").to_string();
        Self::with_namespace("", Some(&ns))
    }

    /// Build a new exporter with an optional metric namespace (e.g.
    /// `"rakka"` produces `rakka_actors_spawned_total`).
    pub fn with_namespace(node: impl Into<String>, namespace: Option<&str>) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let ns = namespace.unwrap_or("rakka");
        let node = node.into();

        let actors_spawned_total = IntCounter::with_opts(
            Opts::new("actors_spawned_total", "Total number of actors spawned")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let actors_stopped_total = IntCounter::with_opts(
            Opts::new("actors_stopped_total", "Total number of actors stopped")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let actors_live = IntGauge::with_opts(
            Opts::new("actors_live", "Currently-live actor count")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let mailbox_depth = IntGaugeVec::new(
            Opts::new("mailbox_depth", "Last-sampled mailbox depth per actor")
                .namespace(ns)
                .const_label("node", &node),
            &["actor_path"],
        )?;

        let dead_letters_total = IntCounter::with_opts(
            Opts::new("dead_letters_total", "Total dead-lettered messages")
                .namespace(ns)
                .const_label("node", &node),
        )?;

        let cluster_members_up = IntGauge::with_opts(
            Opts::new("cluster_members_up", "Cluster members currently Up")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let cluster_unreachable = IntGauge::with_opts(
            Opts::new("cluster_unreachable", "Cluster members currently unreachable")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let cluster_member_events_total = IntCounterVec::new(
            Opts::new("cluster_member_events_total", "Cluster membership events")
                .namespace(ns)
                .const_label("node", &node),
            &["kind"],
        )?;

        let sharding_events_total = IntCounterVec::new(
            Opts::new("sharding_events_total", "Sharding events")
                .namespace(ns)
                .const_label("node", &node),
            &["region", "event"],
        )?;
        let sharding_allocations = IntGaugeVec::new(
            Opts::new("sharding_allocations", "Allocated shards per region")
                .namespace(ns)
                .const_label("node", &node),
            &["region"],
        )?;

        let persistence_events_written_total = IntCounterVec::new(
            Opts::new(
                "persistence_events_written_total",
                "Events written to the journal",
            )
            .namespace(ns)
            .const_label("node", &node),
            &["journal"],
        )?;
        let persistence_last_seq = IntGaugeVec::new(
            Opts::new(
                "persistence_last_sequence_nr",
                "Highest observed sequence_nr per journal",
            )
            .namespace(ns)
            .const_label("node", &node),
            &["journal"],
        )?;

        let remote_endpoints = IntGauge::with_opts(
            Opts::new("remote_endpoints", "Active remote endpoint associations")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let remote_association_events_total = IntCounterVec::new(
            Opts::new(
                "remote_association_events_total",
                "Remote association state changes",
            )
            .namespace(ns)
            .const_label("node", &node),
            &["state"],
        )?;
        let remote_bytes = GaugeVec::new(
            Opts::new("remote_bytes", "Bytes per remote association direction")
                .namespace(ns)
                .const_label("node", &node),
            &["remote", "direction"],
        )?;

        let streams_running = IntGauge::with_opts(
            Opts::new("streams_running", "Currently-running stream graphs")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let streams_started_total = IntCounter::with_opts(
            Opts::new("streams_started_total", "Stream graphs started")
                .namespace(ns)
                .const_label("node", &node),
        )?;
        let streams_finished_total = IntCounter::with_opts(
            Opts::new("streams_finished_total", "Stream graphs finished")
                .namespace(ns)
                .const_label("node", &node),
        )?;

        let ddata_updates_total = IntCounterVec::new(
            Opts::new("ddata_updates_total", "Distributed-data updates")
                .namespace(ns)
                .const_label("node", &node),
            &["key"],
        )?;

        registry.register(Box::new(actors_spawned_total.clone()))?;
        registry.register(Box::new(actors_stopped_total.clone()))?;
        registry.register(Box::new(actors_live.clone()))?;
        registry.register(Box::new(mailbox_depth.clone()))?;
        registry.register(Box::new(dead_letters_total.clone()))?;
        registry.register(Box::new(cluster_members_up.clone()))?;
        registry.register(Box::new(cluster_unreachable.clone()))?;
        registry.register(Box::new(cluster_member_events_total.clone()))?;
        registry.register(Box::new(sharding_events_total.clone()))?;
        registry.register(Box::new(sharding_allocations.clone()))?;
        registry.register(Box::new(persistence_events_written_total.clone()))?;
        registry.register(Box::new(persistence_last_seq.clone()))?;
        registry.register(Box::new(remote_endpoints.clone()))?;
        registry.register(Box::new(remote_association_events_total.clone()))?;
        registry.register(Box::new(remote_bytes.clone()))?;
        registry.register(Box::new(streams_running.clone()))?;
        registry.register(Box::new(streams_started_total.clone()))?;
        registry.register(Box::new(streams_finished_total.clone()))?;
        registry.register(Box::new(ddata_updates_total.clone()))?;
        let _ = Gauge::new("_noop", "unused").ok();

        Ok(Self {
            inner: Arc::new(Inner {
                registry,
                node,
                actors_spawned_total,
                actors_stopped_total,
                actors_live,
                mailbox_depth,
                dead_letters_total,
                cluster_members_up,
                cluster_unreachable,
                cluster_member_events_total,
                sharding_events_total,
                sharding_allocations,
                persistence_events_written_total,
                persistence_last_seq,
                remote_endpoints,
                remote_association_events_total,
                remote_bytes,
                streams_running,
                streams_started_total,
                streams_finished_total,
                ddata_updates_total,
                live_actors: Arc::new(RwLock::new(Default::default())),
            }),
        })
    }

    /// Render the registry in the Prometheus text exposition format.
    pub fn render(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        encoder.encode(&self.inner.registry.gather(), &mut buf)?;
        Ok(String::from_utf8(buf).unwrap_or_default())
    }

    /// Access the underlying registry (e.g. to merge with a shared one).
    pub fn registry(&self) -> &Registry {
        &self.inner.registry
    }

    /// Node label this exporter was built with.
    pub fn node(&self) -> &str {
        &self.inner.node
    }

    /// Share the exporter as an `Arc<dyn Exporter>` suitable for
    /// `TelemetryExtension::add_exporter`.
    pub fn into_dyn(self) -> Arc<dyn Exporter> {
        Arc::new(self)
    }
}

impl Exporter for PrometheusExporter {
    fn on_event(&self, event: &TelemetryEvent) {
        match event {
            TelemetryEvent::ActorSpawned(a) => {
                self.inner.actors_spawned_total.inc();
                let mut live = self.inner.live_actors.write();
                if live.insert(a.path.clone()) {
                    self.inner.actors_live.set(live.len() as i64);
                }
            }
            TelemetryEvent::ActorStopped { path } => {
                self.inner.actors_stopped_total.inc();
                let mut live = self.inner.live_actors.write();
                if live.remove(path) {
                    self.inner.actors_live.set(live.len() as i64);
                }
                let _ = self.inner.mailbox_depth.remove_label_values(&[path]);
            }
            TelemetryEvent::MailboxSampled { path, depth } => {
                if let Ok(g) = self.inner.mailbox_depth.get_metric_with_label_values(&[path]) {
                    g.set(*depth as i64);
                }
            }
            TelemetryEvent::DeadLetter(_) => {
                self.inner.dead_letters_total.inc();
            }
            TelemetryEvent::ClusterChanged(diff) => {
                self.inner
                    .cluster_member_events_total
                    .with_label_values(&["added"])
                    .inc_by(diff.added.len() as u64);
                self.inner
                    .cluster_member_events_total
                    .with_label_values(&["updated"])
                    .inc_by(diff.updated.len() as u64);
                self.inner
                    .cluster_member_events_total
                    .with_label_values(&["removed"])
                    .inc_by(diff.removed.len() as u64);
                self.inner
                    .cluster_member_events_total
                    .with_label_values(&["unreachable"])
                    .inc_by(diff.became_unreachable.len() as u64);
                self.inner
                    .cluster_member_events_total
                    .with_label_values(&["reachable"])
                    .inc_by(diff.became_reachable.len() as u64);
            }
            TelemetryEvent::ShardingChanged(ev) => {
                self.inner
                    .sharding_events_total
                    .with_label_values(&[&ev.region_id, &ev.event])
                    .inc();
            }
            TelemetryEvent::JournalWrite(info) => {
                self.inner
                    .persistence_events_written_total
                    .with_label_values(&[&info.journal])
                    .inc();
                if let Ok(g) = self
                    .inner
                    .persistence_last_seq
                    .get_metric_with_label_values(&[&info.journal])
                {
                    if info.sequence_nr as i64 > g.get() {
                        g.set(info.sequence_nr as i64);
                    }
                }
            }
            TelemetryEvent::RemoteAssociation(info) => {
                self.inner
                    .remote_association_events_total
                    .with_label_values(&[&info.state])
                    .inc();
                if let Ok(g) = self
                    .inner
                    .remote_bytes
                    .get_metric_with_label_values(&[&info.remote_address, "inbound"])
                {
                    g.set(info.inbound_bytes as f64);
                }
                if let Ok(g) = self
                    .inner
                    .remote_bytes
                    .get_metric_with_label_values(&[&info.remote_address, "outbound"])
                {
                    g.set(info.outbound_bytes as f64);
                }
            }
            TelemetryEvent::StreamsGraphStarted { .. } => {
                self.inner.streams_started_total.inc();
                self.inner.streams_running.inc();
            }
            TelemetryEvent::StreamsGraphFinished { .. } => {
                self.inner.streams_finished_total.inc();
                let cur = self.inner.streams_running.get();
                if cur > 0 {
                    self.inner.streams_running.set(cur - 1);
                }
            }
            TelemetryEvent::DDataUpdated { key } => {
                self.inner.ddata_updates_total.with_label_values(&[key]).inc();
            }
        }
    }
}

impl PrometheusExporter {
    /// Seed gauges from a full snapshot. Call after building probes (e.g.
    /// after the dashboard starts) to avoid zero-reads on scrape.
    pub fn seed_from_snapshot(&self, snap: &crate::dto::NodeSnapshot) {
        self.inner.cluster_members_up.set(
            snap.cluster
                .members
                .iter()
                .filter(|m| m.status.eq_ignore_ascii_case("up"))
                .count() as i64,
        );
        self.inner
            .cluster_unreachable
            .set(snap.cluster.unreachable.len() as i64);
        self.inner.remote_endpoints.set(snap.remote.associations.len() as i64);
        self.inner.streams_running.set(snap.streams.running_graphs as i64);
        for reg in &snap.sharding.regions {
            self.inner
                .sharding_allocations
                .with_label_values(&[&reg.region_id])
                .set(reg.shard_count as i64);
        }
        self.inner.actors_live.set(snap.actors.total as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{ActorStatus, ClusterMembershipDiff, DeadLetterRecord, JournalWriteInfo};

    #[test]
    fn emits_expected_metrics_after_events() {
        let exp = PrometheusExporter::with_namespace("node-1", Some("rakka")).unwrap();
        exp.on_event(&TelemetryEvent::ActorSpawned(ActorStatus {
            path: "/user/a".into(),
            parent: Some("/user".into()),
            actor_type: "T".into(),
            mailbox_depth: 0,
            spawned_at: "now".into(),
        }));
        exp.on_event(&TelemetryEvent::MailboxSampled {
            path: "/user/a".into(),
            depth: 5,
        });
        exp.on_event(&TelemetryEvent::DeadLetter(DeadLetterRecord {
            seq: 1,
            recipient: "/user/x".into(),
            sender: None,
            message_type: "Ping".into(),
            message_preview: "".into(),
            timestamp: "now".into(),
        }));
        exp.on_event(&TelemetryEvent::JournalWrite(JournalWriteInfo {
            journal: "inmem".into(),
            persistence_id: "p1".into(),
            sequence_nr: 7,
            timestamp: "now".into(),
        }));
        exp.on_event(&TelemetryEvent::ClusterChanged(ClusterMembershipDiff {
            added: Vec::new(),
            updated: Vec::new(),
            removed: Vec::new(),
            became_unreachable: vec!["akka://n/1".into()],
            became_reachable: Vec::new(),
        }));
        let body = exp.render().unwrap();
        assert!(body.contains("rakka_actors_spawned_total"));
        assert!(body.contains("rakka_mailbox_depth"));
        assert!(body.contains("rakka_dead_letters_total"));
        assert!(body.contains("rakka_persistence_events_written_total"));
        assert!(body.contains("rakka_persistence_last_sequence_nr"));
        assert!(body.contains("rakka_cluster_member_events_total"));
        assert!(body.contains("node=\"node-1\""));
    }

    #[test]
    fn live_actors_tracks_spawn_stop() {
        let exp = PrometheusExporter::with_namespace("n", None).unwrap();
        let spawn = |p: &str| {
            TelemetryEvent::ActorSpawned(ActorStatus {
                path: p.into(),
                parent: None,
                actor_type: "T".into(),
                mailbox_depth: 0,
                spawned_at: "now".into(),
            })
        };
        exp.on_event(&spawn("/user/a"));
        exp.on_event(&spawn("/user/b"));
        assert_eq!(exp.inner.actors_live.get(), 2);
        exp.on_event(&TelemetryEvent::ActorStopped { path: "/user/a".into() });
        assert_eq!(exp.inner.actors_live.get(), 1);
    }
}
