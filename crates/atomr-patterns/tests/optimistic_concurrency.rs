//! `expected_version` rejects out-of-order commands.

use std::sync::Arc;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal};

#[derive(Debug, thiserror::Error)]
#[error("e")]
struct E;

#[derive(Default)]
struct State(i64);

#[derive(Clone, Debug)]
struct Tick(i64);
impl DomainEvent for Tick {}

#[derive(Debug)]
struct Add {
    n: i64,
    expect: Option<u64>,
}

impl Command for Add {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        "x".into()
    }
    fn expected_version(&self) -> Option<u64> {
        self.expect
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
        Ok(vec![Tick(c.n)])
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

#[tokio::test]
async fn expected_version_mismatch_returns_concurrency_conflict() {
    let system = ActorSystem::create("oc", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());

    let h = CqrsPattern::<A>::builder(journal)
        .factory(|_| A)
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();
    let repo = h.repository();

    // Initial entity has seq=0. expected_version=Some(0) succeeds and bumps seq to 1.
    repo.send(Add { n: 5, expect: Some(0) }).await.unwrap();

    // Stale write: expects 0 but actual is 1.
    let stale = repo.send(Add { n: 999, expect: Some(0) }).await;
    assert!(matches!(
        stale,
        Err(PatternError::ConcurrencyConflict { expected: 0, actual: 1 })
    ));

    // Fresh write: expects 1, succeeds, bumps to 2.
    repo.send(Add { n: 3, expect: Some(1) }).await.unwrap();

    // No expected_version: always allowed.
    repo.send(Add { n: 1, expect: None }).await.unwrap();

    system.terminate().await;
}
