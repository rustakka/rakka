//! Process manager state-machine: paid -> shipped -> done.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::process_manager::{
    ProcessManager, ProcessManagerPattern, Transition,
};
use atomr_patterns::topology::Topology;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum St {
    #[default]
    Pending,
    Paid,
    Shipped,
}

#[derive(Clone, Debug)]
enum Event {
    Pay { order: String },
    Ship { order: String },
    Deliver { order: String },
}

#[derive(Debug)]
enum Cmd {
    Notify(#[allow(dead_code)] String),
}

#[derive(Debug, thiserror::Error)]
#[error("pm err")]
struct PmErr;

struct OrderProcess;

impl ProcessManager for OrderProcess {
    type Event = Event;
    type Command = Cmd;
    type State = St;
    type Error = PmErr;

    fn correlation_id(e: &Event) -> Option<String> {
        Some(match e {
            Event::Pay { order } | Event::Ship { order } | Event::Deliver { order } => order.clone(),
        })
    }

    fn transition(
        state: &St,
        event: Event,
    ) -> Result<Transition<St, Cmd>, PmErr> {
        Ok(match (state, &event) {
            (St::Pending, Event::Pay { order }) => Transition::Goto {
                next: St::Paid,
                commands: vec![Cmd::Notify(format!("paid:{order}"))],
            },
            (St::Paid, Event::Ship { order }) => Transition::Goto {
                next: St::Shipped,
                commands: vec![Cmd::Notify(format!("shipped:{order}"))],
            },
            (St::Shipped, Event::Deliver { order }) => Transition::Complete {
                commands: vec![Cmd::Notify(format!("delivered:{order}"))],
            },
            _ => Transition::Stay,
        })
    }
}

#[tokio::test]
async fn process_manager_walks_state_machine() {
    let system = ActorSystem::create("pm", Config::reference()).await.unwrap();
    let dispatched = Arc::new(AtomicU32::new(0));
    let dispatched_for = dispatched.clone();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Event>();

    ProcessManagerPattern::<OrderProcess>::builder()
        .events(rx)
        .dispatcher(move |c: Cmd| {
            let dispatched = dispatched_for.clone();
            async move {
                let _ = c;
                dispatched.fetch_add(1, Ordering::SeqCst);
                true
            }
        })
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();

    tx.send(Event::Pay { order: "o-1".into() }).unwrap();
    tx.send(Event::Ship { order: "o-1".into() }).unwrap();
    tx.send(Event::Deliver { order: "o-1".into() }).unwrap();
    // Out-of-order event for a stale completed correlation: ignored.
    tx.send(Event::Pay { order: "o-1".into() }).unwrap();
    drop(tx);

    for _ in 0..50 {
        if dispatched.load(Ordering::SeqCst) >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Pay -> 1 cmd, Ship -> 1, Deliver -> 1, then re-Pay starts a fresh
    // correlation (default state) and dispatches another Notify.
    assert_eq!(dispatched.load(Ordering::SeqCst), 4);
    system.terminate().await;
}
