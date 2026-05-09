//! Event upcasting: aggregate writes events with manifest "v2"; an
//! older v1-encoded payload (written manually) is decoded through the
//! codec registry's v1 entry.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::cqrs::{EventCodecRegistry, ProjectionHandle};
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal, Journal, PersistentRepr};
use atomr_persistence_query_inmemory::read_journal;

#[derive(Debug, thiserror::Error)]
#[error("e")]
struct E;

#[derive(Default, Debug)]
struct State(i64);

#[derive(Clone, Debug)]
struct Tick(i64);
impl DomainEvent for Tick {}

#[derive(Debug)]
struct Add(i64);
impl Command for Add {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        "x".into()
    }
}

struct Agg;

#[async_trait]
impl Eventsourced for Agg {
    type Command = Add;
    type Event = Tick;
    type State = State;
    type Error = E;
    fn persistence_id(&self) -> String {
        "x".into()
    }
    fn event_manifest(&self) -> &'static str {
        "v2"
    } // new writes use v2
    fn command_to_events(&self, _: &State, c: Add) -> Result<Vec<Tick>, E> {
        Ok(vec![Tick(c.0)])
    }
    fn apply_event(s: &mut State, e: &Tick) {
        s.0 += e.0;
    }
    // v2 encoding: 8-byte little-endian.
    fn encode_event(e: &Tick) -> Result<Vec<u8>, String> {
        Ok(e.0.to_le_bytes().to_vec())
    }
    fn decode_event(b: &[u8]) -> Result<Tick, String> {
        let arr: [u8; 8] = b.try_into().map_err(|_| "len".to_string())?;
        Ok(Tick(i64::from_le_bytes(arr)))
    }
}

impl AggregateRoot for Agg {
    type Id = String;
    fn aggregate_id(&self) -> &Self::Id {
        static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        ID.get_or_init(|| "x".into())
    }
}

#[derive(Default)]
struct Total {
    n: i64,
}

struct R;
#[async_trait]
impl Reader for R {
    type Event = Tick;
    type Projection = Total;
    type Error = std::io::Error;
    fn name(&self) -> &str {
        "totals"
    }
    fn decode(b: &[u8]) -> Result<Tick, String> {
        Agg::decode_event(b)
    }
    async fn apply(&mut self, p: &mut Total, e: Tick) -> Result<(), std::io::Error> {
        p.n += e.0;
        Ok(())
    }
}

#[tokio::test]
async fn registry_decodes_old_v1_events_through_compat_decoder() {
    let system = ActorSystem::create("upcast", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());
    let rj = Arc::new(read_journal(journal.clone()));

    // Pre-seed a "v1" event under a *separate* persistence id so the
    // gateway's recovery path for "x" doesn't try to decode it. The
    // reader scans all pids and will pick it up via the registry.
    // v1 used 4-byte i32 little-endian.
    let v1_payload: Vec<u8> = 7i32.to_le_bytes().to_vec();
    journal
        .write_messages(vec![PersistentRepr {
            persistence_id: "legacy-x".into(),
            sequence_nr: 1,
            payload: v1_payload,
            manifest: "v1".into(),
            writer_uuid: "legacy".into(),
            deleted: false,
            tags: vec![],
        }])
        .await
        .unwrap();

    let registry = EventCodecRegistry::<Tick>::new()
        .register("v1", |b: &[u8]| {
            let arr: [u8; 4] = b.try_into().map_err(|_| "v1 len".to_string())?;
            Ok(Tick(i32::from_le_bytes(arr) as i64))
        })
        .register("v2", |b: &[u8]| Agg::decode_event(b));

    let (builder, totals): (_, ProjectionHandle<Total>) = CqrsPattern::<Agg>::builder(journal.clone())
        .factory(|_| Agg)
        .read_journal(rj)
        .with_event_codecs(registry)
        .poll_interval(Duration::from_millis(20))
        .with_reader(R);

    let h = builder.build().unwrap().materialize(&system).await.unwrap();

    // Now also write a v2 event through the gateway.
    h.repository().send(Add(10)).await.unwrap();

    // Wait for projection to catch up (sees v1=7 + v2=10 = 17).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let n = totals.read(|t| t.n).await;
        if n == 17 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("projection stuck at {n}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    system.terminate().await;
}
