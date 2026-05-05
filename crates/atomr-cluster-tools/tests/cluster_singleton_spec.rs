//! Cluster singleton spec parity. akka.net:
//! `ClusterSingletonProxySpec`, `ClusterSingletonRestartSpec`,
//! `ClusterSingletonLeavingSpeedSpec`.
//!
//! Drives the manager / proxy state machine through the canonical
//! lifecycle: Inactive → Starting → Active → HandingOver → Active
//! (new node) and asserts that the proxy buffers correctly across
//! every transition. We test the buffer/flush logic without typed
//! message envelopes by counting invocations of the deliver closure
//! itself.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use atomr_cluster_tools::{ClusterSingletonManager, ClusterSingletonProxy, SingletonState};
use atomr_config::Config;
use atomr_core::actor::{Actor, ActorSystem, Context, Props};

#[derive(Default)]
struct Sink;

#[async_trait::async_trait]
impl Actor for Sink {
    type Msg = ();
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
}

fn fresh_actor(sys: &ActorSystem, name: &str) -> atomr_core::actor::UntypedActorRef {
    sys.actor_of(Props::create(Sink::default), name).unwrap().as_untyped()
}

#[tokio::test]
async fn proxy_buffers_until_singleton_becomes_active() {
    let sys = ActorSystem::create("singleton-spec", Config::reference()).await.unwrap();
    let actor = fresh_actor(&sys, "single");

    let mgr = ClusterSingletonManager::new();
    let proxy = ClusterSingletonProxy::new(mgr.clone());
    assert_eq!(mgr.state(), SingletonState::Inactive);

    let calls = Arc::new(AtomicU32::new(0));
    for _ in 0..3 {
        let c = calls.clone();
        let ok = proxy.send(move |_target| {
            c.fetch_add(1, Ordering::SeqCst);
        });
        assert!(ok);
    }
    assert_eq!(mgr.buffered(), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // Activation flushes — every deferred deliver fires.
    mgr.set_active_here(actor);
    assert_eq!(mgr.buffered(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    sys.terminate().await;
}

#[tokio::test]
async fn handover_re_buffers_then_flushes_on_new_active() {
    let sys = ActorSystem::create("singleton-handover", Config::reference()).await.unwrap();
    let old = fresh_actor(&sys, "old");
    let new = fresh_actor(&sys, "new");

    let mgr = ClusterSingletonManager::new();
    let proxy = ClusterSingletonProxy::new(mgr.clone());

    mgr.set_active_here(old);
    assert!(matches!(mgr.state(), SingletonState::Active { here: true, .. }));
    let post_active = Arc::new(AtomicU32::new(0));
    let p = post_active.clone();
    proxy.send(move |_| {
        p.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(post_active.load(Ordering::SeqCst), 1, "send during Active should run immediately");

    // Begin handover — proxy buffers again.
    mgr.begin_handover();
    let during = Arc::new(AtomicU32::new(0));
    for _ in 0..2 {
        let d = during.clone();
        proxy.send(move |_| {
            d.fetch_add(1, Ordering::SeqCst);
        });
    }
    assert_eq!(mgr.buffered(), 2);
    assert_eq!(during.load(Ordering::SeqCst), 0);

    // New oldest takes over.
    mgr.set_active_remote(new);
    assert_eq!(mgr.buffered(), 0);
    assert_eq!(during.load(Ordering::SeqCst), 2);

    sys.terminate().await;
}

#[tokio::test]
async fn buffer_overflow_increments_drop_counter() {
    let sys = ActorSystem::create("singleton-drops", Config::reference()).await.unwrap();
    let mgr = ClusterSingletonManager::with_buffer_size(2);
    let proxy = ClusterSingletonProxy::new(mgr.clone());

    assert!(proxy.send(|_| {}));
    assert!(proxy.send(|_| {}));
    // Buffer full now → next send should be rejected.
    assert!(!proxy.send(|_| {}));
    assert_eq!(mgr.buffered(), 2);
    assert_eq!(mgr.drops(), 1);

    sys.terminate().await;
}

#[tokio::test]
async fn clear_drops_back_to_inactive() {
    let sys = ActorSystem::create("singleton-clear", Config::reference()).await.unwrap();
    let mgr = ClusterSingletonManager::new();
    let actor = fresh_actor(&sys, "c");
    mgr.set_active_here(actor);
    mgr.clear();
    assert_eq!(mgr.state(), SingletonState::Inactive);
    assert!(mgr.current().is_none());
    sys.terminate().await;
}

#[tokio::test]
async fn begin_starting_marks_state_starting() {
    let sys = ActorSystem::create("singleton-starting", Config::reference()).await.unwrap();
    let mgr = ClusterSingletonManager::new();
    mgr.begin_starting();
    assert_eq!(mgr.state(), SingletonState::Starting);
    // Sends during Starting still buffer.
    let proxy = ClusterSingletonProxy::new(mgr.clone());
    let counted = Arc::new(AtomicU32::new(0));
    let c = counted.clone();
    proxy.send(move |_| {
        c.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(mgr.buffered(), 1);
    assert_eq!(counted.load(Ordering::SeqCst), 0);
    sys.terminate().await;
}
