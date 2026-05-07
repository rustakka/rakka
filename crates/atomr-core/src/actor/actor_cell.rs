//! `ActorCell` — the per-actor runtime.
//! and its partial classes (`.Children.cs`, `.DeathWatch.cs`,
//! `.DefaultMessages.cs`, `.FaultHandling.cs`, `.ReceiveTimeout.cs`).
//!
//! Responsibilities:
//! * Own the user actor instance `A`
//! * Poll mailbox (system priority over user)
//! * Invoke lifecycle hooks (pre_start, post_stop, pre/post_restart)
//! * Handle supervision decisions on panic
//! * Track children, watchers, and receive timeout

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::actor_ref::{ActorRef, UntypedActorRef};
use super::context::Context;
use super::path::ActorPath;
use super::props::Props;
use super::traits::{Actor, MessageEnvelope};
use crate::supervision::{Directive, PanicPayload};

/// Messages on the actor's system channel.
#[derive(Debug)]
pub enum SystemMsg {
    Stop,
    Restart(String),
    Terminated(ActorPath),
    Watch(UntypedActorRef),
    Unwatch(ActorPath),
    ReceiveTimeout,
    ChildFailed { name: String, error: String },
}

/// Bookkeeping entry for a child on the parent's side.
#[derive(Debug)]
pub struct ChildEntry {
    /// Reserved for future child introspection APIs.
    #[allow(dead_code)]
    pub path: ActorPath,
    #[allow(dead_code)]
    pub untyped: UntypedActorRef,
    pub system_tx: mpsc::UnboundedSender<SystemMsg>,
}

/// Marker used only for public type references.
pub struct ActorCell<A: Actor> {
    _marker: std::marker::PhantomData<A>,
}

pub(crate) fn spawn_cell<A: Actor>(
    system: Arc<super::actor_system::ActorSystemInner>,
    props: Props<A>,
    path: ActorPath,
) -> Result<ActorRef<A::Msg>, super::context::SpawnError> {
    let (user_tx, user_rx) = mpsc::unbounded_channel::<MessageEnvelope<A::Msg>>();
    let (sys_tx, sys_rx) = mpsc::unbounded_channel::<SystemMsg>();
    let actor_ref = ActorRef::new(path.clone(), user_tx, sys_tx, Arc::downgrade(&system));

    let cell_ref = actor_ref.clone();
    let cell_system = Arc::downgrade(&system);
    let props_clone = props.clone();
    tokio::spawn(async move {
        let mut actor = props_clone.new_actor();
        let mut ctx = Context::<A>::new(cell_ref.clone(), path.clone(), cell_system);
        run_cell(&mut actor, &mut ctx, user_rx, sys_rx, &props_clone).await;
    });

    Ok(actor_ref)
}

async fn run_cell<A: Actor>(
    actor: &mut A,
    ctx: &mut Context<A>,
    mut user_rx: mpsc::UnboundedReceiver<MessageEnvelope<A::Msg>>,
    mut sys_rx: mpsc::UnboundedReceiver<SystemMsg>,
    props: &Props<A>,
) {
    ctx.phase = super::context::LifecyclePhase::Starting;
    actor.pre_start(ctx).await;
    ctx.phase = super::context::LifecyclePhase::Running;

    let supervisor_ref = props.supervisor_strategy.clone();

    loop {
        while let Ok(sys) = sys_rx.try_recv() {
            if handle_system(actor, ctx, sys).await {
                finalize(actor, ctx).await;
                return;
            }
        }
        if ctx.stopping {
            finalize(actor, ctx).await;
            return;
        }

        let timeout = ctx.receive_timeout;
        let next: Either<A> = tokio::select! {
            biased;
            sys = sys_rx.recv() => Either::<A>::Sys(sys),
            user = user_rx.recv() => Either::<A>::User(user),
            _ = maybe_sleep(timeout), if timeout.is_some() => Either::<A>::Timeout,
        };

        match next {
            Either::Sys(Some(s)) => {
                if handle_system(actor, ctx, s).await {
                    finalize(actor, ctx).await;
                    return;
                }
            }
            Either::User(Some(env)) => {
                ctx.current_sender = env.sender;
                if let Err(panic_msg) = run_handle(actor, ctx, env.message).await {
                    let directive =
                        supervisor_ref.as_ref().map(|s| s.decide(&panic_msg)).unwrap_or(Directive::Restart);
                    match directive {
                        Directive::Resume => {}
                        Directive::Restart => {
                            actor.pre_restart(ctx, &panic_msg).await;
                            *actor = props.new_actor();
                            actor.post_restart(ctx, &panic_msg).await;
                        }
                        Directive::Stop | Directive::Escalate => {
                            finalize(actor, ctx).await;
                            return;
                        }
                    }
                }
                ctx.current_sender = super::sender::Sender::None;
            }
            Either::Timeout => {
                if handle_system(actor, ctx, SystemMsg::ReceiveTimeout).await {
                    finalize(actor, ctx).await;
                    return;
                }
            }
            Either::Sys(None) | Either::User(None) => {
                finalize(actor, ctx).await;
                return;
            }
        }
    }
}

enum Either<A: Actor> {
    User(Option<MessageEnvelope<A::Msg>>),
    Sys(Option<SystemMsg>),
    Timeout,
}

async fn maybe_sleep(d: Option<Duration>) {
    if let Some(d) = d {
        tokio::time::sleep(d).await;
    } else {
        futures_util::future::pending::<()>().await;
    }
}

async fn handle_system<A: Actor>(actor: &mut A, ctx: &mut Context<A>, msg: SystemMsg) -> bool {
    match msg {
        SystemMsg::Stop => true,
        SystemMsg::Restart(err) => {
            actor.pre_restart(ctx, &err).await;
            actor.post_restart(ctx, &err).await;
            false
        }
        SystemMsg::Terminated(path) => {
            tracing::debug!(self_path = %ctx.path, watched = %path, "watched actor terminated");
            ctx.watching.remove(&path);
            actor.on_terminated(ctx, &path).await;
            false
        }
        SystemMsg::Watch(subscriber) => {
            ctx.watched_by.insert(subscriber);
            false
        }
        SystemMsg::Unwatch(path) => {
            ctx.watched_by.retain(|w| w.path() != &path);
            false
        }
        SystemMsg::ReceiveTimeout => false,
        SystemMsg::ChildFailed { name, error } => {
            tracing::warn!(path = %ctx.path, child = %name, "child failed: {error}");
            false
        }
    }
}

async fn run_handle<A: Actor>(actor: &mut A, ctx: &mut Context<A>, msg: A::Msg) -> Result<(), String> {
    use futures_util::FutureExt;
    let fut = actor.handle(ctx, msg);
    match std::panic::AssertUnwindSafe(fut).catch_unwind().await {
        Ok(()) => Ok(()),
        Err(p) => {
            let s = panic_payload_to_string(p);
            tracing::error!(path = %ctx.path, "handle panic: {s}");
            Err(s)
        }
    }
}

fn panic_payload_to_string(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(payload) = p.downcast_ref::<PanicPayload>() {
        payload.to_wire()
    } else if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "actor panic".to_string()
    }
}

async fn finalize<A: Actor>(actor: &mut A, ctx: &mut Context<A>) {
    ctx.phase = super::context::LifecyclePhase::Stopping;
    actor.post_stop(ctx).await;
    for w in ctx.watched_by.drain().collect::<Vec<_>>() {
        w.notify_watchers(ctx.path.clone());
    }
    for (_, child) in std::mem::take(&mut ctx.children) {
        let _ = child.system_tx.send(SystemMsg::Stop);
    }
    if let Some(system) = ctx.system.upgrade() {
        if let Some(obs) = system.spawn_observer.read().as_ref() {
            obs.on_stop(&ctx.path);
        }
    }
}
