//! Integration test: attach an in-memory exporter to the telemetry bus
//! and verify that probe activity (spawn, dead letter, etc.) drives the
//! exporter. This covers the contract both the Prometheus and OpenTelemetry
//! exporters rely on.

#![cfg(feature = "otel-stdout")]

use std::sync::Arc;

use rustakka_telemetry::bus::TelemetryEvent;
use rustakka_telemetry::dto::ActorStatus;
use rustakka_telemetry::exporters::otel::CapturingExporter;
use rustakka_telemetry::TelemetryExtension;

#[tokio::test]
async fn capturing_exporter_sees_probe_activity() {
    let telemetry = TelemetryExtension::new("n1", 64);
    let capture = Arc::new(CapturingExporter::new());
    telemetry.add_exporter(capture.clone());

    telemetry.actors.record_spawn(ActorStatus {
        path: "/user/a".into(),
        parent: Some("/user".into()),
        actor_type: "Demo".into(),
        mailbox_depth: 0,
        spawned_at: "now".into(),
    });
    telemetry.dead_letters.record(
        "/user/a".into(),
        None,
        "String".into(),
        "hi".into(),
    );

    let events = capture.events.lock().clone();
    assert!(events.iter().any(|e| matches!(e, TelemetryEvent::ActorSpawned(_))));
    assert!(events.iter().any(|e| matches!(e, TelemetryEvent::DeadLetter(_))));
}
