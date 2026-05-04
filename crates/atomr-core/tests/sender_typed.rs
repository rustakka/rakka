//! Phase 1 — typed `Sender` end-to-end.
//!
//! Verifies that `ActorRef::tell_from(msg, Sender::Local(ref))` makes
//! the recipient see the typed sender via `Context::sender_typed()`,
//! without any runtime downcast.

use std::sync::Arc;
use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::{Actor, ActorSystem, Context, Props, Sender, UntypedActorRef};
use tokio::sync::{oneshot, Mutex};

struct Recorder {
    last_sender_path: Arc<Mutex<Option<String>>>,
    notify: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[async_trait::async_trait]
impl Actor for Recorder {
    type Msg = ();
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: ()) {
        let path = ctx.sender_typed().path().map(|p| p.to_string());
        *self.last_sender_path.lock().await = path;
        if let Some(tx) = self.notify.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::test]
async fn tell_from_sets_typed_sender() {
    let sys = ActorSystem::create("TypedSenderTest", Config::reference()).await.unwrap();

    let recorded: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let (notify_tx, notify_rx) = oneshot::channel();
    let notify = Arc::new(Mutex::new(Some(notify_tx)));

    let recorded_for_actor = recorded.clone();
    let notify_for_actor = notify.clone();
    let recorder = sys
        .actor_of(
            Props::create(move || Recorder {
                last_sender_path: recorded_for_actor.clone(),
                notify: notify_for_actor.clone(),
            }),
            "recorder",
        )
        .unwrap();

    // A second actor we'll cast as the sender for tell_from.
    let other = sys
        .actor_of(
            Props::create(|| Recorder {
                last_sender_path: Arc::new(Mutex::new(None)),
                notify: Arc::new(Mutex::new(None)),
            }),
            "other",
        )
        .unwrap();
    let sender_ref: UntypedActorRef = other.as_untyped();
    let sender_path_str = sender_ref.path().to_string();

    recorder.tell_from((), Sender::Local(sender_ref));

    tokio::time::timeout(Duration::from_millis(500), notify_rx)
        .await
        .expect("recorder did not receive message in time")
        .unwrap();

    let recorded_path = recorded.lock().await.clone();
    assert_eq!(recorded_path, Some(sender_path_str));

    sys.terminate().await;
}

#[tokio::test]
async fn tell_yields_sender_none() {
    let sys = ActorSystem::create("NoSenderTest", Config::reference()).await.unwrap();

    let recorded: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let (notify_tx, notify_rx) = oneshot::channel();
    let notify = Arc::new(Mutex::new(Some(notify_tx)));

    let recorded_for_actor = recorded.clone();
    let notify_for_actor = notify.clone();
    let recorder = sys
        .actor_of(
            Props::create(move || Recorder {
                last_sender_path: recorded_for_actor.clone(),
                notify: notify_for_actor.clone(),
            }),
            "recorder",
        )
        .unwrap();

    recorder.tell(()); // Plain tell — Sender::None.

    tokio::time::timeout(Duration::from_millis(500), notify_rx)
        .await
        .expect("recorder did not receive message in time")
        .unwrap();

    assert!(recorded.lock().await.is_none());

    sys.terminate().await;
}
