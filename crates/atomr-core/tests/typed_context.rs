//! Phase 1.C — phantom-typed `TypedContext<'a, A, P>` view smoke tests.

use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::{Actor, ActorSystem, Context, LifecyclePhase, Props, Running, Starting, Stopping};
use tokio::sync::oneshot;

enum Cmd {
    Probe(oneshot::Sender<LifecyclePhase>),
    SetTimeout,
    Stash,
    DrainCount(oneshot::Sender<usize>),
}

struct PhaseProbe;

#[async_trait::async_trait]
impl Actor for PhaseProbe {
    type Msg = Cmd;
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Msg) {
        match msg {
            Cmd::Probe(tx) => {
                let _ = tx.send(ctx.phase());
            }
            Cmd::SetTimeout => {
                let mut view = ctx.running().expect("Running view");
                view.set_receive_timeout(Some(Duration::from_millis(50)));
            }
            Cmd::Stash => {
                ctx.stash(Cmd::SetTimeout);
            }
            Cmd::DrainCount(tx) => {
                let mut view = ctx.running().expect("Running view");
                let drained = view.unstash_all();
                let _ = tx.send(drained.len());
            }
        }
    }
}

#[tokio::test]
async fn typed_context_running_view_works() {
    let sys = ActorSystem::create("typed-ctx", Config::reference()).await.unwrap();
    let r = sys.actor_of(Props::create(|| PhaseProbe), "probe").unwrap();

    let (tx, rx) = oneshot::channel();
    r.tell(Cmd::Probe(tx));
    assert_eq!(rx.await.unwrap(), LifecyclePhase::Running);

    r.tell(Cmd::SetTimeout);

    r.tell(Cmd::Stash);
    r.tell(Cmd::Stash);

    let (tx, rx) = oneshot::channel();
    r.tell(Cmd::DrainCount(tx));
    assert_eq!(rx.await.unwrap(), 2);

    sys.terminate().await;
}

#[test]
fn phase_marker_compile_time_witnesses() {
    fn _assert<A: Actor>(c: &mut Context<A>) {
        let _: Option<atomr_core::actor::TypedContext<'_, A, Starting>> = c.starting();
        let _: Option<atomr_core::actor::TypedContext<'_, A, Running>> = c.running();
        let _: Option<atomr_core::actor::TypedContext<'_, A, Stopping>> = c.stopping_view();
    }
}
