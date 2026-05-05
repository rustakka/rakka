//! Telemetry probe spec parity. akka.net's telemetry analogues are
//! distributed across `Akka.Diagnostics`, `Akka.Cluster.Metrics`, and
//! per-subsystem health endpoints; atomr unifies them under a typed
//! `TelemetryBus` + per-subsystem registries. These tests assert each
//! probe's record/snapshot contract.

use std::time::Duration;

use atomr_telemetry::actor_registry::ActorRegistry;
use atomr_telemetry::bus::{TelemetryBus, TelemetryEvent};
use atomr_telemetry::dead_letters::DeadLetterFeed;
use atomr_telemetry::dto::ActorStatus;

fn status(path: &str) -> ActorStatus {
    ActorStatus {
        path: path.into(),
        parent: Some("/user".into()),
        actor_type: "T".into(),
        mailbox_depth: 0,
        spawned_at: "now".into(),
    }
}

#[tokio::test]
async fn actor_registry_tracks_spawn_and_stop() {
    let bus = TelemetryBus::new(16);
    let reg = ActorRegistry::new(bus);
    assert_eq!(reg.live_count(), 0);
    reg.record_spawn(status("/user/a"));
    reg.record_spawn(status("/user/b"));
    assert_eq!(reg.total_spawned(), 2);
    assert_eq!(reg.live_count(), 2);
    reg.record_stop("/user/a");
    assert_eq!(reg.total_stopped(), 1);
    assert_eq!(reg.live_count(), 1);
}

#[tokio::test]
async fn actor_registry_publishes_events_on_bus() {
    let bus = TelemetryBus::new(16);
    let mut rx = bus.subscribe();
    let reg = ActorRegistry::new(bus);
    reg.record_spawn(status("/user/x"));
    reg.record_stop("/user/x");
    let mut topics = Vec::new();
    while let Ok(Ok(ev)) = tokio::time::timeout(Duration::from_millis(20), rx.recv()).await {
        topics.push(ev.topic());
    }
    assert!(topics.contains(&"actors"));
}

#[tokio::test]
async fn actor_registry_snapshot_includes_recorded_actors() {
    let bus = TelemetryBus::new(16);
    let reg = ActorRegistry::new(bus);
    reg.record_spawn(status("/user/a"));
    reg.record_spawn(status("/user/b"));
    let snap = reg.snapshot();
    let names: Vec<&String> = snap.flat.iter().map(|s| &s.path).collect();
    assert!(names.iter().any(|p| p.as_str() == "/user/a"));
    assert!(names.iter().any(|p| p.as_str() == "/user/b"));
}

#[tokio::test]
async fn dead_letter_feed_caps_buffer() {
    let bus = TelemetryBus::new(16);
    let feed = DeadLetterFeed::new(bus, 2);
    feed.record("/x".into(), None, "T".into(), "p".into());
    feed.record("/y".into(), None, "T".into(), "p".into());
    feed.record("/z".into(), None, "T".into(), "p".into());
    assert_eq!(feed.total_count(), 3);
    assert_eq!(feed.buffered(), 2, "ring buffer caps at capacity");
    let recent = feed.recent(10);
    assert_eq!(recent.len(), 2);
}

#[tokio::test]
async fn dead_letter_feed_recent_returns_at_most_limit() {
    let bus = TelemetryBus::new(16);
    let feed = DeadLetterFeed::new(bus, 100);
    for i in 0..5 {
        feed.record(format!("/x{i}"), None, "T".into(), "p".into());
    }
    let recent = feed.recent(2);
    assert_eq!(recent.len(), 2);
}

#[tokio::test]
async fn dead_letter_feed_publishes_on_bus() {
    let bus = TelemetryBus::new(16);
    let mut rx = bus.subscribe_topic("dead_letters");
    let feed = DeadLetterFeed::new(bus, 10);
    feed.record("/x".into(), None, "T".into(), "p".into());
    let ev = tokio::time::timeout(Duration::from_millis(50), rx.recv())
        .await
        .expect("event timeout")
        .expect("rx closed");
    assert!(matches!(ev, TelemetryEvent::DeadLetter(_)));
}

#[tokio::test]
async fn topic_subscriber_isolates_topics() {
    let bus = TelemetryBus::new(16);
    let mut actors_rx = bus.subscribe_topic("actors");
    let mut letters_rx = bus.subscribe_topic("dead_letters");
    let reg = ActorRegistry::new(bus.clone());
    let feed = DeadLetterFeed::new(bus, 10);
    reg.record_spawn(status("/user/m"));
    feed.record("/dl".into(), None, "T".into(), "p".into());
    let actors_ev = tokio::time::timeout(Duration::from_millis(50), actors_rx.recv()).await.unwrap().unwrap();
    let letters_ev =
        tokio::time::timeout(Duration::from_millis(50), letters_rx.recv()).await.unwrap().unwrap();
    assert_eq!(actors_ev.topic(), "actors");
    assert_eq!(letters_ev.topic(), "dead_letters");
}
