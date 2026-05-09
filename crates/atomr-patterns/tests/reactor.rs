//! Reactor: every event runs through the side-effect closure.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::reactor::ReactorPattern;
use atomr_patterns::topology::Topology;

#[tokio::test]
async fn reactor_runs_handler_per_event() {
    let system = ActorSystem::create("reactor", Config::reference()).await.unwrap();
    let count = Arc::new(AtomicU32::new(0));
    let count_for_handler = count.clone();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    ReactorPattern::<u32>::builder()
        .name("counter")
        .events(rx)
        .reaction(move |n: u32| {
            let count = count_for_handler.clone();
            async move {
                count.fetch_add(n, Ordering::SeqCst);
            }
        })
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();

    for i in 1..=5u32 {
        tx.send(i).unwrap();
    }
    drop(tx);

    for _ in 0..50 {
        if count.load(Ordering::SeqCst) >= 15 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(count.load(Ordering::SeqCst), 15, "1+2+3+4+5 = 15");
    system.terminate().await;
}
