//! Integration test for the remote / streams / ddata probes.

use rakka_telemetry::bus::TelemetryBus;
use rakka_telemetry::ddata::DDataProbe;
use rakka_telemetry::remote::RemoteProbe;
use rakka_telemetry::streams::StreamsProbe;

#[cfg(feature = "ddata")]
#[test]
fn ddata_probe_refreshes_from_replicator() {
    use rakka_distributed_data::{GCounter, Replicator};

    let rep = Replicator::new();
    let mut c = GCounter::new();
    c.increment("n1", 1);
    rep.update("counter", c);
    let mut s = rakka_distributed_data::GSet::<String>::default();
    s.add("a".into());
    rep.update("items", s);

    let probe = DDataProbe::new(TelemetryBus::new(8));
    probe.refresh_from(rep.as_ref());
    let snap = probe.snapshot();
    assert_eq!(snap.keys.len(), 2);
    assert!(snap.keys.contains(&"counter".to_string()));
    assert!(snap.keys.contains(&"items".to_string()));
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn remote_probe_refreshes_from_endpoint_manager() {
    use rakka_remote::{RemoteSettings, RemoteSystem};
    use rakka_telemetry::remote::refresh_from_endpoint_manager;

    let sys_a = rakka_core::actor::ActorSystem::create("A", rakka_config::Config::reference())
        .await
        .unwrap();
    let sys_b = rakka_core::actor::ActorSystem::create("B", rakka_config::Config::reference())
        .await
        .unwrap();
    let remote_a = RemoteSystem::start(sys_a, "127.0.0.1:0".parse().unwrap(), RemoteSettings::default())
        .await
        .unwrap();
    let remote_b = RemoteSystem::start(sys_b, "127.0.0.1:0".parse().unwrap(), RemoteSettings::default())
        .await
        .unwrap();
    remote_a.register_bincode::<u32>();
    remote_b.register_bincode::<u32>();

    // Trigger an association A → B so the EndpointManager learns about B.
    let _ = remote_a
        .endpoint_manager()
        .endpoint_for(&remote_b.local_address)
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let probe = RemoteProbe::new(TelemetryBus::new(8));
    refresh_from_endpoint_manager(&probe, remote_a.endpoint_manager());
    let snap = probe.snapshot();
    assert!(!snap.associations.is_empty(), "expected at least one association");
}

#[test]
fn streams_probe_tracks_running_graphs() {
    let probe = StreamsProbe::new(TelemetryBus::new(8));
    let a = probe.start_graph("a");
    probe.start_graph("b");
    probe.finish_graph(a);
    let snap = probe.snapshot();
    assert_eq!(snap.running_graphs, 1);
    assert_eq!(snap.active[0].name, "b");
}
