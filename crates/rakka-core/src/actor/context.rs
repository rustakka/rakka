//! `Context<A>` — the actor's window into the system.
//! akka.net: `Actor/IActorContext.cs`, `ActorCell.cs` (partial).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Weak;
use std::time::Duration;

use super::actor_cell::{ChildEntry, SystemMsg};
use super::actor_ref::{ActorRef, UntypedActorRef};
use super::path::ActorPath;
use super::props::Props;
use super::sender::Sender;
use super::traits::Actor;

/// Lifecycle phase exposed via [`Context::phase`]. Phase 1.C of
/// `docs/full-port-plan.md` — runtime precursor to the phantom-typed
/// `Context<A, Phase>` (kept additive so it doesn't break existing
/// signatures). Stage-only APIs assert against this in debug builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecyclePhase {
    Starting,
    Running,
    Stopping,
}

/// Passed to every `Actor::handle` call.
pub struct Context<A: Actor> {
    pub(crate) self_ref: ActorRef<A::Msg>,
    pub(crate) path: ActorPath,
    pub(crate) system: Weak<super::actor_system::ActorSystemInner>,
    pub(crate) children: HashMap<String, ChildEntry>,
    pub(crate) watching: HashSet<ActorPath>,
    pub(crate) watched_by: HashSet<UntypedActorRef>,
    pub(crate) stash: VecDeque<A::Msg>,
    pub(crate) receive_timeout: Option<Duration>,
    pub(crate) current_sender: Sender,
    pub(crate) stopping: bool,
    pub(crate) phase: LifecyclePhase,
}

impl<A: Actor> Context<A> {
    pub(crate) fn new(
        self_ref: ActorRef<A::Msg>,
        path: ActorPath,
        system: Weak<super::actor_system::ActorSystemInner>,
    ) -> Self {
        Self {
            self_ref,
            path,
            system,
            children: HashMap::new(),
            watching: HashSet::new(),
            watched_by: HashSet::new(),
            stash: VecDeque::new(),
            receive_timeout: None,
            current_sender: Sender::None,
            stopping: false,
            phase: LifecyclePhase::Starting,
        }
    }

    /// Current lifecycle phase. Phase 1.C marker — useful in
    /// generic helpers that need to gate calls (e.g. `become`,
    /// `unstash_all`) without taking a typed-`Context<A, P>`
    /// parameter.
    pub fn phase(&self) -> LifecyclePhase {
        self.phase
    }

    pub fn self_ref(&self) -> &ActorRef<A::Msg> {
        &self.self_ref
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Spawn a child actor under this context.
    pub fn spawn<B: Actor>(&mut self, props: Props<B>, name: &str) -> Result<ActorRef<B::Msg>, SpawnError> {
        if self.children.contains_key(name) {
            return Err(SpawnError::NameTaken(name.into()));
        }
        let system = self.system.upgrade().ok_or(SpawnError::SystemTerminated)?;
        let child_path = self.path.child(name);
        let r = super::actor_cell::spawn_cell::<B>(system.clone(), props, child_path.clone())?;
        if let Some(obs) = system.spawn_observer.read().as_ref() {
            obs.on_spawn(&child_path, Some(&self.path), std::any::type_name::<B>());
        }
        self.children.insert(
            name.to_string(),
            ChildEntry { path: child_path, untyped: r.as_untyped(), system_tx: r.system_sender() },
        );
        Ok(r)
    }

    /// Stop a specific child.
    pub fn stop_child(&mut self, name: &str) {
        if let Some(c) = self.children.get(name) {
            let _ = c.system_tx.send(SystemMsg::Stop);
        }
    }

    /// Watch another actor. The sender is notified with a `SystemMsg::Terminated`
    /// when the watched actor stops. akka.net: `Context.Watch`.
    pub fn watch<M: Send + 'static>(&mut self, target: &ActorRef<M>) {
        if self.watching.insert(target.path().clone()) {
            let _ = target.system_sender().send(SystemMsg::Watch(self.self_ref.as_untyped()));
        }
    }

    /// Stop watching.
    pub fn unwatch<M: Send + 'static>(&mut self, target: &ActorRef<M>) {
        if self.watching.remove(target.path()) {
            let _ = target.system_sender().send(SystemMsg::Unwatch(self.path.clone()));
        }
    }

    /// Stash the currently-processed message for later unstash.
    /// akka.net: `IStash.Stash()`.
    pub fn stash(&mut self, msg: A::Msg) {
        self.stash.push_back(msg);
    }

    /// Put all stashed messages back at the front of the mailbox.
    pub fn unstash_all(&mut self) -> Vec<A::Msg> {
        let mut out = Vec::with_capacity(self.stash.len());
        while let Some(m) = self.stash.pop_front() {
            out.push(m);
        }
        out
    }

    /// Stop self. akka.net: `Context.Stop(Self)`.
    pub fn stop_self(&mut self) {
        self.stopping = true;
    }

    /// Set idle-receive timeout (like akka.net `SetReceiveTimeout`).
    pub fn set_receive_timeout(&mut self, d: Option<Duration>) {
        self.receive_timeout = d;
    }

    /// Typed sender of the message currently being processed.
    ///
    /// Returns [`Sender::None`] if the sender slot was empty (the
    /// akka.net analog of `Sender == NoSender`).
    pub fn sender(&self) -> &Sender {
        &self.current_sender
    }

    /// Backwards-compatible alias for [`Context::sender`].
    #[doc(hidden)]
    pub fn sender_typed(&self) -> &Sender {
        &self.current_sender
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("child name `{0}` already taken")]
    NameTaken(String),
    #[error("actor system has terminated")]
    SystemTerminated,
}
