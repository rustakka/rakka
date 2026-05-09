//! End-to-end CQRS test: Counter aggregate, OrderTotalsReader projection.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal};
use atomr_persistence_query_inmemory::read_journal;

#[derive(Debug, thiserror::Error)]
enum CounterErr {
    #[error("would underflow")]
    Underflow,
}

#[derive(Default, Debug)]
struct CounterState {
    n: i64,
}

#[derive(Clone, Debug)]
enum CounterEvent {
    Adjusted(i64),
}

impl DomainEvent for CounterEvent {
    fn tags(&self) -> Vec<String> {
        vec!["counter".into()]
    }
}

#[derive(Debug)]
enum CounterCmd {
    Add(i64),
    Sub(i64),
}

impl Command for CounterCmd {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        "the-counter".into()
    }
}

struct Counter {
    id: String,
}

#[async_trait]
impl Eventsourced for Counter {
    type Command = CounterCmd;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = CounterErr;

    fn persistence_id(&self) -> String {
        self.id.clone()
    }

    fn command_to_events(
        &self,
        state: &Self::State,
        cmd: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let delta = match cmd {
            CounterCmd::Add(n) => n,
            CounterCmd::Sub(n) => -n,
        };
        if state.n + delta < 0 {
            return Err(CounterErr::Underflow);
        }
        Ok(vec![CounterEvent::Adjusted(delta)])
    }

    fn apply_event(state: &mut Self::State, event: &Self::Event) {
        match event {
            CounterEvent::Adjusted(d) => state.n += d,
        }
    }

    fn encode_event(event: &Self::Event) -> Result<Vec<u8>, String> {
        match event {
            CounterEvent::Adjusted(d) => Ok(d.to_le_bytes().to_vec()),
        }
    }

    fn decode_event(bytes: &[u8]) -> Result<Self::Event, String> {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| "bad len".to_string())?;
        Ok(CounterEvent::Adjusted(i64::from_le_bytes(arr)))
    }
}

impl AggregateRoot for Counter {
    type Id = String;
    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }
}

#[derive(Default, Debug)]
struct TotalsProjection {
    total: i64,
    event_count: u64,
}

struct TotalsReader;

#[async_trait]
impl Reader for TotalsReader {
    type Event = CounterEvent;
    type Projection = TotalsProjection;
    type Error = std::io::Error;

    fn name(&self) -> &str {
        "totals"
    }

    fn tag(&self) -> Option<String> {
        Some("counter".into())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Event, String> {
        Counter::decode_event(bytes)
    }

    async fn apply(
        &mut self,
        projection: &mut Self::Projection,
        event: Self::Event,
    ) -> Result<(), Self::Error> {
        match event {
            CounterEvent::Adjusted(d) => {
                projection.total += d;
                projection.event_count += 1;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn aggregate_persists_and_projection_catches_up() {
    let system = ActorSystem::create("test-cqrs", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());
    let rj = Arc::new(read_journal(journal.clone()));

    let (builder, totals_handle) = CqrsPattern::<Counter>::builder(journal.clone())
        .name("counter-cqrs")
        .factory(|id| Counter { id })
        .read_journal(rj.clone())
        .poll_interval(Duration::from_millis(20))
        .with_reader(TotalsReader);

    let topology = builder.build().expect("build");
    let handles = topology.materialize(&system).await.expect("materialize");
    let repo = handles.repository();

    repo.send(CounterCmd::Add(5)).await.unwrap();
    repo.send(CounterCmd::Add(10)).await.unwrap();
    let err = repo.send(CounterCmd::Sub(100)).await;
    assert!(matches!(err, Err(PatternError::Domain(CounterErr::Underflow))));
    repo.send(CounterCmd::Sub(3)).await.unwrap();

    // Wait for projection to catch up. Three successful events -> total=12, count=3.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let snap = totals_handle.read(|p| (p.total, p.event_count)).await;
        if snap == (12, 3) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("projection never caught up: {:?}", snap);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    system.terminate().await;
}

#[tokio::test]
async fn missing_factory_errors_on_build() {
    let journal = Arc::new(InMemoryJournal::default());
    let rj = Arc::new(read_journal(journal.clone()));
    let result: Result<_, PatternError<CounterErr>> = CqrsPattern::<Counter>::builder(journal)
        .read_journal(rj)
        .build();
    assert!(matches!(result, Err(PatternError::NotConfigured("factory"))));
}
