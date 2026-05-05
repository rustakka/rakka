//! Actor lifecycle parity spec. akka.net: `Tests/ActorLifeCycleSpec.cs`.
//!
//! Asserts the contract of pre_start / post_stop / pre_restart /
//! post_restart hook ordering, supervision-driven restart semantics
//! (one-for-one default), self-stop ergonomics, and dead-letter
//! observation for sends after stop.
//!
//! Each test pairs hook firings with shared `Arc<AtomicU32>` counters
//! so we can observe progress without coupling test code to actor
//! internals.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::{Actor, ActorPath, ActorSystem, Context, DeadLetterObserver, Props};
use parking_lot::Mutex;
use tokio::sync::oneshot;

/// Shared counters wired into the spec actors so the harness can
/// observe lifecycle progress.
#[derive(Clone, Default)]
struct LifeCounters {
    pre_start: Arc<AtomicU32>,
    post_stop: Arc<AtomicU32>,
    pre_restart: Arc<AtomicU32>,
    post_restart: Arc<AtomicU32>,
    handled: Arc<AtomicU32>,
}

impl LifeCounters {
    fn new() -> Self {
        Self::default()
    }
    fn snapshot(&self) -> (u32, u32, u32, u32, u32) {
        (
            self.pre_start.load(Ordering::SeqCst),
            self.post_stop.load(Ordering::SeqCst),
            self.pre_restart.load(Ordering::SeqCst),
            self.post_restart.load(Ordering::SeqCst),
            self.handled.load(Ordering::SeqCst),
        )
    }
}

/// Counter actor whose handler can optionally panic, and that exposes
/// its current count via an oneshot reply.
struct LifeActor {
    counters: LifeCounters,
    /// Reset on restart by the props factory.
    state: u32,
}

enum LifeMsg {
    Inc,
    Get(oneshot::Sender<u32>),
    Boom,
    StopSelf,
}

#[async_trait]
impl Actor for LifeActor {
    type Msg = LifeMsg;

    async fn pre_start(&mut self, _ctx: &mut Context<Self>) {
        self.counters.pre_start.fetch_add(1, Ordering::SeqCst);
    }
    async fn post_stop(&mut self, _ctx: &mut Context<Self>) {
        self.counters.post_stop.fetch_add(1, Ordering::SeqCst);
    }
    async fn pre_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {
        self.counters.pre_restart.fetch_add(1, Ordering::SeqCst);
    }
    async fn post_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {
        self.counters.post_restart.fetch_add(1, Ordering::SeqCst);
    }

    async fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Msg) {
        self.counters.handled.fetch_add(1, Ordering::SeqCst);
        match msg {
            LifeMsg::Inc => self.state += 1,
            LifeMsg::Get(reply) => {
                let _ = reply.send(self.state);
            }
            LifeMsg::Boom => panic!("life-actor boom"),
            LifeMsg::StopSelf => {
                ctx.stop_self();
            }
        }
    }
}

fn life_props(counters: LifeCounters) -> Props<LifeActor> {
    Props::create(move || LifeActor { counters: counters.clone(), state: 0 })
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[tokio::test]
async fn pre_start_runs_once_before_first_message() {
    let counters = LifeCounters::new();
    let sys = ActorSystem::create("LifeStart", Config::reference()).await.unwrap();
    let r = sys.actor_of(life_props(counters.clone()), "a").unwrap();

    // Allow pre_start to run before we send anything.
    settle().await;
    assert_eq!(counters.pre_start.load(Ordering::SeqCst), 1, "pre_start fires before any message");
    assert_eq!(counters.handled.load(Ordering::SeqCst), 0, "no handle yet");

    r.tell(LifeMsg::Inc);
    r.tell(LifeMsg::Inc);
    let v = r.ask_with(LifeMsg::Get, Duration::from_millis(200)).await.unwrap();
    assert_eq!(v, 2);

    let (p, s, _pr, _po, _h) = counters.snapshot();
    assert_eq!(p, 1, "pre_start still 1 after handles");
    assert_eq!(s, 0, "post_stop has not fired while running");

    sys.terminate().await;
}

#[tokio::test]
async fn post_stop_runs_after_graceful_stop() {
    let counters = LifeCounters::new();
    let sys = ActorSystem::create("LifeStop", Config::reference()).await.unwrap();
    let r = sys.actor_of(life_props(counters.clone()), "a").unwrap();

    settle().await;
    r.tell(LifeMsg::Inc);
    settle().await;
    assert_eq!(counters.handled.load(Ordering::SeqCst), 1);

    r.stop();
    settle().await;

    let (pre, post, pre_r, post_r, _) = counters.snapshot();
    assert_eq!(pre, 1);
    assert_eq!(post, 1, "post_stop fired exactly once on graceful stop");
    assert_eq!(pre_r, 0);
    assert_eq!(post_r, 0);

    sys.terminate().await;
}

#[tokio::test]
async fn panic_triggers_restart_with_state_reset() {
    let counters = LifeCounters::new();
    let sys = ActorSystem::create("LifeRestart", Config::reference()).await.unwrap();
    let r = sys.actor_of(life_props(counters.clone()), "a").unwrap();

    settle().await;
    r.tell(LifeMsg::Inc);
    r.tell(LifeMsg::Inc);
    r.tell(LifeMsg::Inc);
    let pre_panic = r.ask_with(LifeMsg::Get, Duration::from_millis(200)).await.unwrap();
    assert_eq!(pre_panic, 3);

    // Default supervisor strategy is OneForOne / Restart.
    r.tell(LifeMsg::Boom);
    settle().await;

    // After restart, state must be reset.
    let post_panic = r.ask_with(LifeMsg::Get, Duration::from_millis(200)).await.unwrap();
    assert_eq!(post_panic, 0, "fresh actor instance after Restart");

    let (pre, post, pre_r, post_r, _) = counters.snapshot();
    assert_eq!(pre, 1, "pre_start fires only on the original start");
    assert_eq!(pre_r, 1, "pre_restart fired once on the panic");
    assert_eq!(post_r, 1, "post_restart fired once after the panic");
    assert_eq!(post, 0, "post_stop has not fired (actor is still alive)");

    sys.terminate().await;
}

#[tokio::test]
async fn stop_self_finishes_message_and_runs_post_stop() {
    let counters = LifeCounters::new();
    let sys = ActorSystem::create("LifeStopSelf", Config::reference()).await.unwrap();
    let r = sys.actor_of(life_props(counters.clone()), "a").unwrap();

    settle().await;
    r.tell(LifeMsg::Inc);
    r.tell(LifeMsg::StopSelf);
    settle().await;

    let (pre, post, pre_r, post_r, handled) = counters.snapshot();
    assert!(handled >= 2, "both Inc and StopSelf were handled, got {handled}");
    assert_eq!(pre, 1);
    assert_eq!(post, 1, "post_stop fires after self-stop drains the current message");
    assert_eq!(pre_r, 0);
    assert_eq!(post_r, 0);

    sys.terminate().await;
}

/// Captures dead-letter notifications from the `ActorSystem` for spec
/// assertions.
#[derive(Default)]
struct CapturingDeadLetters {
    seen: Mutex<Vec<ActorPath>>,
}
impl CapturingDeadLetters {
    fn count_for(&self, path: &ActorPath) -> usize {
        self.seen.lock().iter().filter(|p| *p == path).count()
    }
}
impl DeadLetterObserver for CapturingDeadLetters {
    fn on_dead_letter(
        &self,
        recipient: &ActorPath,
        _sender: Option<&ActorPath>,
        _message_type: &'static str,
    ) {
        self.seen.lock().push(recipient.clone());
    }
}

#[tokio::test]
async fn sends_after_post_stop_route_to_dead_letters() {
    let counters = LifeCounters::new();
    let sys = ActorSystem::create("LifeDead", Config::reference()).await.unwrap();
    let dl = Arc::new(CapturingDeadLetters::default());
    sys.set_dead_letter_observer(dl.clone());

    let r = sys.actor_of(life_props(counters.clone()), "a").unwrap();
    let path = r.path().clone();

    settle().await;
    r.tell(LifeMsg::Inc);
    settle().await;

    r.stop();
    // Wait long enough for the cell task to drop the user receiver so
    // the `tell` below cannot find a live mailbox.
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(counters.post_stop.load(Ordering::SeqCst), 1, "post_stop ran");

    // Send several messages after stop — they must not reach the
    // actor and must surface as dead letters.
    let pre_count = counters.handled.load(Ordering::SeqCst);
    for _ in 0..3 {
        r.tell(LifeMsg::Inc);
    }
    settle().await;

    assert_eq!(counters.handled.load(Ordering::SeqCst), pre_count, "no further handle calls after post_stop");
    assert!(
        dl.count_for(&path) >= 3,
        "dead-letter observer saw at least the 3 post-stop sends, got {}",
        dl.count_for(&path)
    );

    sys.terminate().await;
}
