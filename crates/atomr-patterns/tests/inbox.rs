//! Inbox pattern: duplicate keys suppressed.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::inbox::{InMemoryInboxStore, InboxPattern};
use atomr_patterns::topology::Topology;

#[derive(Clone, Debug)]
struct Msg {
    id: String,
    #[allow(dead_code)]
    payload: i32,
}

#[tokio::test]
async fn inbox_suppresses_duplicate_keys() {
    let system = ActorSystem::create("inbox", Config::reference()).await.unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let calls_for_handler = calls.clone();

    let store = Arc::new(InMemoryInboxStore::new());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();

    InboxPattern::<Msg>::builder()
        .name("test-inbox")
        .key(|m: &Msg| m.id.clone())
        .source(rx)
        .store(store)
        .handler(move |m: Msg| {
            let calls = calls_for_handler.clone();
            async move {
                let _ = m;
                calls.fetch_add(1, Ordering::SeqCst);
                true
            }
        })
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();

    // 5 unique keys, then 5 duplicates.
    for i in 0..5 {
        tx.send(Msg { id: format!("k-{i}"), payload: i }).unwrap();
    }
    for i in 0..5 {
        tx.send(Msg { id: format!("k-{i}"), payload: 999 }).unwrap();
    }
    drop(tx);

    // Wait for the runner to drain.
    for _ in 0..50 {
        if calls.load(Ordering::SeqCst) >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 5, "handler ran exactly 5 times (no duplicates)");

    system.terminate().await;
}
