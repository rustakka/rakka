//! Command-id dedupe: same command_id is only handled once per
//! aggregate.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal};

#[derive(Debug, thiserror::Error)]
#[error("e")]
struct E;

static HANDLES: AtomicU32 = AtomicU32::new(0);

#[derive(Default)]
struct State(i64);

#[derive(Clone, Debug)]
struct Tick(i64);
impl DomainEvent for Tick {}

#[derive(Debug)]
struct Add {
    id: String,
    n: i64,
    cmd_id: String,
}

impl Command for Add {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        self.id.clone()
    }
    fn command_id(&self) -> Option<&str> {
        Some(&self.cmd_id)
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
        HANDLES.fetch_add(1, Ordering::SeqCst);
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
async fn duplicate_command_ids_are_deduped() {
    HANDLES.store(0, Ordering::SeqCst);
    let system = ActorSystem::create("dedupe", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());

    let h = CqrsPattern::<A>::builder(journal)
        .factory(|_| A)
        .dedupe_window(8)
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();
    let repo = h.repository();

    // First send: handler runs.
    let r1 = repo.send(Add { id: "x".into(), n: 5, cmd_id: "c-1".into() }).await.unwrap();
    // Second send (same cmd_id): cached.
    let r2 = repo.send(Add { id: "x".into(), n: 999, cmd_id: "c-1".into() }).await.unwrap();
    // Third send (new cmd_id): handler runs.
    let r3 = repo.send(Add { id: "x".into(), n: 7, cmd_id: "c-2".into() }).await.unwrap();

    let inner_r1: i64 = match &r1[0] {
        Tick(v) => *v,
    };
    let inner_r2: i64 = match &r2[0] {
        Tick(v) => *v,
    };
    let inner_r3: i64 = match &r3[0] {
        Tick(v) => *v,
    };
    assert_eq!(inner_r1, 5);
    assert_eq!(inner_r2, 5, "second call returned the cached event, not the new one");
    assert_eq!(inner_r3, 7);
    assert_eq!(HANDLES.load(Ordering::SeqCst), 2, "exactly 2 handler invocations");

    system.terminate().await;
}
