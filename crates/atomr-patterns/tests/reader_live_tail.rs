//! Live-tail readers via DomainEventBus: persisted events flow into
//! the bus and out to subscribed readers without journal polling.
//! Also exercises `with_reader_retry` by injecting transient apply
//! failures.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_core::pattern::RetrySchedule;
use atomr_patterns::bus::DomainEventBus;
use atomr_patterns::prelude::*;
use atomr_patterns::topology::Topology;
use atomr_persistence::{Eventsourced, InMemoryJournal};

#[derive(Debug, thiserror::Error)]
#[error("e")]
struct E;

#[derive(Default)]
struct S(i64);

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
    type State = S;
    type Error = E;
    fn persistence_id(&self) -> String {
        "x".into()
    }
    fn command_to_events(&self, _: &S, c: Add) -> Result<Vec<Tick>, E> {
        Ok(vec![Tick(c.0)])
    }
    fn apply_event(s: &mut S, e: &Tick) {
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

#[derive(Debug, thiserror::Error)]
#[error("transient")]
struct Transient;

struct FlakyReader {
    fail_n_times: Arc<AtomicU32>,
}

#[async_trait]
impl Reader for FlakyReader {
    type Event = Tick;
    type Projection = Total;
    type Error = Transient;
    fn name(&self) -> &str {
        "totals"
    }
    fn decode(b: &[u8]) -> Result<Tick, String> {
        A::decode_event(b)
    }
    async fn apply(&mut self, p: &mut Total, e: Tick) -> Result<(), Transient> {
        if self.fail_n_times.load(Ordering::SeqCst) > 0 {
            self.fail_n_times.fetch_sub(1, Ordering::SeqCst);
            return Err(Transient);
        }
        p.n += e.0;
        Ok(())
    }
}

#[tokio::test]
async fn live_tail_reader_with_retry_recovers_from_transient_failures() {
    let system = ActorSystem::create("live", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());

    let bus = DomainEventBus::<Tick>::builder()
        .name("ticks")
        .build()
        .materialize(&system)
        .await
        .unwrap();

    let fail_count = Arc::new(AtomicU32::new(2));
    let reader = FlakyReader { fail_n_times: fail_count.clone() };

    let (builder, totals) = CqrsPattern::<A>::builder(journal.clone())
        .factory(|_| A)
        .with_event_bus(bus.clone())
        .with_reader_retry(5, RetrySchedule::fixed(Duration::from_millis(10)))
        .with_reader(reader);

    let h = builder.build().unwrap().materialize(&system).await.unwrap();

    // First command fails twice then succeeds (2 retries baked in).
    h.repository().send(Add(5)).await.unwrap();
    // Second command goes through cleanly.
    h.repository().send(Add(7)).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let n = totals.read(|t| t.n).await;
        if n == 12 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("projection stuck at {n}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(fail_count.load(Ordering::SeqCst), 0, "all transient failures consumed");
    system.terminate().await;
}
