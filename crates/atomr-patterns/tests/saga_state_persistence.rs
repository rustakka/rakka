//! Saga state survives runner restarts when wired to a state store.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_patterns::saga::{InMemorySagaStateStore, SagaStateStore};

#[derive(Clone, Debug)]
struct Step(#[allow(dead_code)] u8);

#[derive(Default, Debug)]
struct Counter {
    seen: u32,
}

#[derive(Debug, thiserror::Error)]
#[error("nope")]
struct Err_;

struct CountingSaga;

#[async_trait]
impl Saga for CountingSaga {
    type Event = Step;
    type Command = ();
    type State = Counter;
    type Error = Err_;

    fn correlation_id(_: &Step) -> Option<String> {
        Some("the-only-saga".into())
    }

    async fn handle(
        &mut self,
        state: &mut Counter,
        _e: Step,
    ) -> Result<Vec<SagaAction<()>>, Err_> {
        state.seen += 1;
        Ok(vec![])
    }

    fn encode_state(state: &Counter) -> Option<Result<Vec<u8>, String>> {
        Some(Ok(state.seen.to_le_bytes().to_vec()))
    }

    fn decode_state(b: &[u8]) -> Result<Counter, String> {
        let arr: [u8; 4] = b.try_into().map_err(|_| "len".to_string())?;
        Ok(Counter { seen: u32::from_le_bytes(arr) })
    }
}

#[tokio::test]
async fn saga_state_rehydrates_from_store_on_restart() {
    let store: Arc<InMemorySagaStateStore> = Arc::new(InMemorySagaStateStore::new());
    let system = ActorSystem::create("saga-persist", Config::reference()).await.unwrap();

    // Run 1: send 3 events, observe state in store.
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<Step>();
    SagaPattern::<CountingSaga>::builder()
        .saga(CountingSaga)
        .events(rx1)
        .dispatcher(|()| async { true })
        .state_store(store.clone())
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();
    for i in 0..3 {
        tx1.send(Step(i)).unwrap();
    }
    drop(tx1);
    // Wait for the runner task to process and persist.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Some(payload) = store.load("the-only-saga").await {
            if u32::from_le_bytes(payload.try_into().unwrap()) == 3 {
                break;
            }
        }
    }
    let payload = store.load("the-only-saga").await.expect("state persisted");
    assert_eq!(u32::from_le_bytes(payload.try_into().unwrap()), 3);

    // Run 2: fresh runner, same store. Should rehydrate from 3.
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<Step>();
    SagaPattern::<CountingSaga>::builder()
        .saga(CountingSaga)
        .events(rx2)
        .dispatcher(|()| async { true })
        .state_store(store.clone())
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();
    tx2.send(Step(99)).unwrap();
    drop(tx2);
    // Final state should be 4 (rehydrated 3 + 1 new event).
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Some(payload) = store.load("the-only-saga").await {
            if u32::from_le_bytes(payload.try_into().unwrap()) == 4 {
                break;
            }
        }
    }
    let payload = store.load("the-only-saga").await.expect("state persisted");
    assert_eq!(
        u32::from_le_bytes(payload.try_into().unwrap()),
        4,
        "rehydrated state was incremented"
    );

    system.terminate().await;
}
