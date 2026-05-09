//! Exercises the named extension slots (`on_command`, `on_event`) and
//! generic event taps.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal};

#[derive(Debug, thiserror::Error)]
#[error("nothing")]
struct E;

#[derive(Default)]
struct S;

#[derive(Clone, Debug)]
struct Ev(i64);
impl DomainEvent for Ev {}

#[derive(Debug)]
enum C {
    Yes(i64),
    No,
}
impl Command for C {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        "x".into()
    }
}

struct A;
#[async_trait]
impl Eventsourced for A {
    type Command = C;
    type Event = Ev;
    type State = S;
    type Error = E;
    fn persistence_id(&self) -> String {
        "x".into()
    }
    fn command_to_events(&self, _: &S, c: C) -> Result<Vec<Ev>, E> {
        match c {
            C::Yes(n) => Ok(vec![Ev(n)]),
            C::No => Ok(vec![]),
        }
    }
    fn apply_event(_: &mut S, _: &Ev) {}
    fn encode_event(e: &Ev) -> Result<Vec<u8>, String> {
        Ok(e.0.to_le_bytes().to_vec())
    }
    fn decode_event(b: &[u8]) -> Result<Ev, String> {
        Ok(Ev(i64::from_le_bytes(b.try_into().unwrap())))
    }
}
impl AggregateRoot for A {
    type Id = String;
    fn aggregate_id(&self) -> &Self::Id {
        // dummy - never used because Command::aggregate_id is what matters
        static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        ID.get_or_init(|| "x".into())
    }
}

#[tokio::test]
async fn interceptor_can_reject_and_listeners_fire_on_success() {
    let system = ActorSystem::create("ext", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());

    let intercept_calls = Arc::new(AtomicUsize::new(0));
    let listener_calls = Arc::new(AtomicUsize::new(0));

    let intercept_clone = intercept_calls.clone();
    let listener_clone = listener_calls.clone();

    let (tap_tx, mut tap_rx) = tokio::sync::mpsc::unbounded_channel::<Ev>();

    let topology = CqrsPattern::<A>::builder(journal.clone())
        .factory(|_id| A)
        .on_command(move |c| {
            intercept_clone.fetch_add(1, Ordering::SeqCst);
            match c {
                C::No => Err(PatternError::Intercepted("nope".into())),
                _ => Ok(()),
            }
        })
        .on_event(move |_e: &Ev| {
            listener_clone.fetch_add(1, Ordering::SeqCst);
        })
        .tap_events(tap_tx)
        .build()
        .unwrap();

    let h = topology.materialize(&system).await.unwrap();
    let repo = h.repository();

    // accepted command -> listener+tap fire
    repo.send(C::Yes(7)).await.unwrap();
    // rejected command -> persist aborts; listener does NOT fire
    let err = repo.send(C::No).await;
    assert!(matches!(err, Err(PatternError::Intercepted(_))));

    // We saw 2 commands at the interceptor (one accepted, one rejected).
    assert_eq!(intercept_calls.load(Ordering::SeqCst), 2);
    // We saw 1 event at the listener (only the accepted command persisted).
    assert_eq!(listener_calls.load(Ordering::SeqCst), 1);

    // Tap received the accepted event.
    let received = tokio::time::timeout(Duration::from_millis(200), tap_rx.recv()).await.unwrap();
    let received = received.expect("tap closed");
    assert_eq!(received.0, 7);

    system.terminate().await;
}
