//! Projection rebuild: reset state, replay, end up at the same value.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::cqrs::EventCodecRegistry;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal};
use atomr_persistence_query_inmemory::read_journal;

#[derive(Debug, thiserror::Error)]
#[error("e")]
struct E;

#[derive(Default)]
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
struct A;
#[async_trait]
impl Eventsourced for A {
    type Command = Add;
    type Event = Tick;
    type State = State;
    type Error = E;
    fn persistence_id(&self) -> String {
        "x".into()
    }
    fn command_to_events(&self, _: &State, c: Add) -> Result<Vec<Tick>, E> {
        Ok(vec![Tick(c.0)])
    }
    fn apply_event(s: &mut State, e: &Tick) {
        s.0 += e.0;
    }
    fn encode_event(e: &Tick) -> Result<Vec<u8>, String> {
        Ok(e.0.to_le_bytes().to_vec())
    }
    fn decode_event(b: &[u8]) -> Result<Tick, String> {
        Ok(Tick(i64::from_le_bytes(b.try_into().map_err(|_| "len")?)))
    }
}
impl AggregateRoot for A {
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
        A::decode_event(b)
    }
    async fn apply(&mut self, p: &mut Total, e: Tick) -> Result<(), std::io::Error> {
        p.n += e.0;
        Ok(())
    }
}

#[tokio::test]
async fn rebuild_resets_and_replays_to_same_total() {
    let system = ActorSystem::create("rebuild", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());
    let rj = Arc::new(read_journal(journal.clone()));
    let codecs =
        EventCodecRegistry::<Tick>::new().with_default(|b: &[u8]| A::decode_event(b));

    let (builder, totals) = CqrsPattern::<A>::builder(journal.clone())
        .factory(|_| A)
        .read_journal(rj)
        .with_event_codecs(codecs)
        .poll_interval(Duration::from_millis(20))
        .with_reader(R);
    let h = builder.build().unwrap().materialize(&system).await.unwrap();
    let repo = h.repository();

    for n in [1, 2, 3, 4i64] {
        repo.send(Add(n)).await.unwrap();
    }

    // Wait for projection to catch up to 10.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while totals.read(|t| t.n).await != 10 {
        if tokio::time::Instant::now() >= deadline {
            panic!("projection stuck at {}", totals.read(|t| t.n).await);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Trigger rebuild — state resets to default, replays all events.
    h.rebuild_projection("totals").await.unwrap();
    assert_eq!(totals.read(|t| t.n).await, 10, "rebuild yields the same total");

    system.terminate().await;
}
