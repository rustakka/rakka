//! Integration test for the remote / streams / ddata probes.

use rustakka_telemetry::bus::TelemetryBus;
use rustakka_telemetry::ddata::DDataProbe;
use rustakka_telemetry::remote::RemoteProbe;
use rustakka_telemetry::streams::StreamsProbe;

#[cfg(feature = "ddata")]
#[test]
fn ddata_probe_refreshes_from_replicator() {
    use rustakka_distributed_data::{GCounter, Replicator};

    let rep = Replicator::new();
    let mut c = GCounter::new();
    c.increment("n1", 1);
    rep.update("counter", c);
    let mut s = rustakka_distributed_data::GSet::<String>::default();
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
#[test]
fn remote_probe_refreshes_from_registry() {
    use std::sync::Arc;

    use rustakka_core::actor::Address;
    use rustakka_remote::{Endpoint, EndpointRegistry, Transport, TransportError};
    use rustakka_telemetry::remote::refresh_from_registry;

    struct NullTransport {
        tx: tokio::sync::Mutex<
            Option<tokio::sync::mpsc::UnboundedSender<rustakka_remote::RemoteEnvelope>>,
        >,
    }
    #[async_trait::async_trait]
    impl Transport for NullTransport {
        async fn listen(&self) -> Result<Address, TransportError> {
            Ok(Address::local("A"))
        }
        async fn associate(&self, _addr: &Address) -> Result<(), TransportError> {
            Ok(())
        }
        async fn send(
            &self,
            _to: &Address,
            _env: rustakka_remote::RemoteEnvelope,
        ) -> Result<(), TransportError> {
            Ok(())
        }
        fn inbound(&self) -> tokio::sync::mpsc::UnboundedReceiver<rustakka_remote::RemoteEnvelope> {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            *self.tx.blocking_lock() = Some(tx);
            rx
        }
        async fn shutdown(&self) -> Result<(), TransportError> {
            Ok(())
        }
    }

    let reg = EndpointRegistry::new();
    let t: Arc<dyn Transport> = Arc::new(NullTransport { tx: tokio::sync::Mutex::new(None) });
    reg.insert(Arc::new(Endpoint::new(Address::local("A"), t.clone())));
    reg.insert(Arc::new(Endpoint::new(Address::local("B"), t.clone())));

    let probe = RemoteProbe::new(TelemetryBus::new(8));
    refresh_from_registry(&probe, &reg);
    let snap = probe.snapshot();
    assert_eq!(snap.associations.len(), 2);
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
