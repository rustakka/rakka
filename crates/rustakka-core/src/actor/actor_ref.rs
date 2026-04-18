//! `ActorRef` — typed handle to an actor. akka.net: `Actor/ActorRef.cs`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use super::actor_cell::SystemMsg;
use super::path::ActorPath;
use super::traits::MessageEnvelope;

/// Typed handle to an actor.
///
/// Cheap to clone (internally `Arc`). `tell` sends without waiting; `ask`
/// uses a helper pattern (`ask_with`) to avoid reflection.
pub struct ActorRef<M: Send + 'static> {
    inner: Arc<ActorRefInner<M>>,
}

struct ActorRefInner<M: Send + 'static> {
    path: ActorPath,
    user: mpsc::UnboundedSender<MessageEnvelope<M>>,
    system: mpsc::UnboundedSender<SystemMsg>,
}

impl<M: Send + 'static> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<M: Send + 'static> fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef").field("path", &self.inner.path.to_string()).finish()
    }
}

impl<M: Send + 'static> ActorRef<M> {
    pub(crate) fn new(
        path: ActorPath,
        user: mpsc::UnboundedSender<MessageEnvelope<M>>,
        system: mpsc::UnboundedSender<SystemMsg>,
    ) -> Self {
        Self { inner: Arc::new(ActorRefInner { path, user, system }) }
    }

    pub fn path(&self) -> &ActorPath {
        &self.inner.path
    }

    /// Fire-and-forget send. akka.net: `Tell`.
    pub fn tell(&self, msg: M) {
        let _ = self.inner.user.send(MessageEnvelope::new(msg));
    }

    pub fn tell_with_sender<S: Any + Send>(&self, msg: M, sender: S) {
        let _ = self.inner.user.send(MessageEnvelope::with_sender(msg, sender));
    }

    /// Stop the actor. akka.net: `Stop(ActorRef)`.
    pub fn stop(&self) {
        let _ = self.inner.system.send(SystemMsg::Stop);
    }

    /// Ask pattern: callers supply a closure that embeds a `oneshot::Sender<R>`
    /// in the message. The future resolves when the actor replies, or errors
    /// out on timeout/actor-stop. akka.net: `Ask`.
    pub async fn ask_with<R, F>(&self, build: F, timeout: Duration) -> Result<R, AskError>
    where
        R: Send + 'static,
        F: FnOnce(oneshot::Sender<R>) -> M,
    {
        let (tx, rx) = oneshot::channel();
        let msg = build(tx);
        self.tell(msg);
        tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| AskError::Timeout)?
            .map_err(|_| AskError::TargetDropped)
    }

    /// Downgrade into an untyped ref for use with DeadLetters / EventStream.
    pub fn as_untyped(&self) -> UntypedActorRef {
        UntypedActorRef {
            path: self.inner.path.clone(),
            system: self.inner.system.clone(),
        }
    }

    /// System-message channel exposed internally for DeathWatch.
    pub(crate) fn system_sender(&self) -> mpsc::UnboundedSender<SystemMsg> {
        self.inner.system.clone()
    }
}

/// Untyped ref used where the message type is not statically known
/// (e.g. death-watch notifications across actor types, event stream).
#[derive(Clone, Debug)]
pub struct UntypedActorRef {
    pub path: ActorPath,
    pub(crate) system: mpsc::UnboundedSender<SystemMsg>,
}

impl PartialEq for UntypedActorRef {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for UntypedActorRef {}

impl std::hash::Hash for UntypedActorRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl UntypedActorRef {
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub fn stop(&self) {
        let _ = self.system.send(SystemMsg::Stop);
    }

    pub(crate) fn notify_watchers(&self, sender: ActorPath) {
        let _ = self.system.send(SystemMsg::Terminated(sender));
    }
}

#[derive(Debug, Error)]
pub enum AskError {
    #[error("ask timed out")]
    Timeout,
    #[error("target actor was dropped before replying")]
    TargetDropped,
}
