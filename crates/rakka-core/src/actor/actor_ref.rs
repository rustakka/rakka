//! `ActorRef` — typed handle to an actor. akka.net: `Actor/ActorRef.cs`.
//!
//! Refs are polymorphic: a [`ActorRef<M>`] is either backed by a local
//! mailbox (cheap, in-process, the common case) or by a remote handle that
//! serializes `M` and ships it to another `ActorSystem`.

use std::any::Any;
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use super::actor_cell::SystemMsg;
use super::actor_system::ActorSystemInner;
use super::path::ActorPath;
use super::remote::{RemoteRef, RemoteSystemMsg, SerializedMessage};
use super::traits::MessageEnvelope;

/// Type-erased serializer used by the Remote variant of `ActorRef<M>`.
type RemoteSerializerFn<M> =
    Arc<dyn Fn(M, Option<ActorPath>) -> SerializedMessage + Send + Sync>;

enum RefImpl<M: Send + 'static> {
    Local {
        path: ActorPath,
        user: mpsc::UnboundedSender<MessageEnvelope<M>>,
        system: mpsc::UnboundedSender<SystemMsg>,
        system_ref: Weak<ActorSystemInner>,
    },
    Remote {
        path: ActorPath,
        handle: Arc<dyn RemoteRef>,
        serialize: RemoteSerializerFn<M>,
    },
}

/// Typed handle to an actor.
///
/// Cheap to clone (internally `Arc`). `tell` sends without waiting; `ask`
/// uses a helper pattern (`ask_with`) to avoid reflection.
pub struct ActorRef<M: Send + 'static> {
    inner: Arc<RefImpl<M>>,
}

impl<M: Send + 'static> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<M: Send + 'static> fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef").field("path", &self.path().to_string()).finish()
    }
}

impl<M: Send + 'static> ActorRef<M> {
    pub(crate) fn new(
        path: ActorPath,
        user: mpsc::UnboundedSender<MessageEnvelope<M>>,
        system: mpsc::UnboundedSender<SystemMsg>,
        system_ref: Weak<ActorSystemInner>,
    ) -> Self {
        Self {
            inner: Arc::new(RefImpl::Local { path, user, system, system_ref }),
        }
    }

    /// Construct a typed remote ref given a (type-erased) `RemoteRef` handle
    /// and a serializer for `M`. Used by `rakka-remote::RemoteActorRefProvider`.
    pub fn from_remote(
        handle: Arc<dyn RemoteRef>,
        serialize: RemoteSerializerFn<M>,
    ) -> Self {
        let path = handle.path().clone();
        Self {
            inner: Arc::new(RefImpl::Remote { path, handle, serialize }),
        }
    }

    pub fn path(&self) -> &ActorPath {
        match &*self.inner {
            RefImpl::Local { path, .. } => path,
            RefImpl::Remote { path, .. } => path,
        }
    }

    /// True if this ref points at an actor in a different `ActorSystem`.
    pub fn is_remote(&self) -> bool {
        matches!(&*self.inner, RefImpl::Remote { .. })
    }

    /// Fire-and-forget send. akka.net: `Tell`.
    pub fn tell(&self, msg: M) {
        match &*self.inner {
            RefImpl::Local { user, path, system_ref, .. } => {
                if user.send(MessageEnvelope::new(msg)).is_err() {
                    notify_dead_letter::<M>(path, system_ref);
                }
            }
            RefImpl::Remote { handle, serialize, .. } => {
                handle.tell_serialized(serialize(msg, None));
            }
        }
    }

    pub fn tell_with_sender<S: Any + Send>(&self, msg: M, sender: S) {
        match &*self.inner {
            RefImpl::Local { user, path, system_ref, .. } => {
                if user.send(MessageEnvelope::with_sender(msg, sender)).is_err() {
                    notify_dead_letter::<M>(path, system_ref);
                }
            }
            RefImpl::Remote { handle, serialize, .. } => {
                let sender_path = (&sender as &dyn Any)
                    .downcast_ref::<ActorPath>()
                    .cloned()
                    .or_else(|| {
                        (&sender as &dyn Any)
                            .downcast_ref::<UntypedActorRef>()
                            .map(|u| u.path().clone())
                    });
                handle.tell_serialized(serialize(msg, sender_path));
            }
        }
    }

    /// Stop the actor. akka.net: `Stop(ActorRef)`.
    pub fn stop(&self) {
        match &*self.inner {
            RefImpl::Local { system, .. } => {
                let _ = system.send(SystemMsg::Stop);
            }
            RefImpl::Remote { handle, .. } => {
                handle.tell_system(RemoteSystemMsg::Stop);
            }
        }
    }

    /// Ask pattern: callers supply a closure that embeds a `oneshot::Sender<R>`
    /// in the message. The future resolves when the actor replies, or errors
    /// out on timeout/actor-stop. akka.net: `Ask`.
    ///
    /// Note: `ask_with` only works on local refs. For remote ask, use the
    /// dedicated `rakka-remote::ask_remote` helper which routes the reply
    /// through a temporary local responder actor.
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
        match &*self.inner {
            RefImpl::Local { path, system, .. } => UntypedActorRef {
                inner: Arc::new(UntypedImpl::Local {
                    path: path.clone(),
                    system: system.clone(),
                }),
            },
            RefImpl::Remote { path, handle, .. } => UntypedActorRef {
                inner: Arc::new(UntypedImpl::Remote {
                    path: path.clone(),
                    handle: handle.clone(),
                }),
            },
        }
    }

    /// System-message channel exposed internally for DeathWatch (local only).
    pub(crate) fn system_sender(&self) -> mpsc::UnboundedSender<SystemMsg> {
        match &*self.inner {
            RefImpl::Local { system, .. } => system.clone(),
            RefImpl::Remote { .. } => {
                let (tx, _rx) = mpsc::unbounded_channel();
                tx
            }
        }
    }
}

fn notify_dead_letter<M: 'static>(path: &ActorPath, system_ref: &Weak<ActorSystemInner>) {
    if let Some(system) = system_ref.upgrade() {
        if let Some(obs) = system.dead_letter_observer.read().as_ref() {
            obs.on_dead_letter(path, None, std::any::type_name::<M>());
        }
    }
}

enum UntypedImpl {
    Local {
        path: ActorPath,
        system: mpsc::UnboundedSender<SystemMsg>,
    },
    Remote {
        path: ActorPath,
        handle: Arc<dyn RemoteRef>,
    },
}

/// Untyped ref used where the message type is not statically known
/// (e.g. death-watch notifications across actor types, event stream).
#[derive(Clone)]
pub struct UntypedActorRef {
    inner: Arc<UntypedImpl>,
}

impl fmt::Debug for UntypedActorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UntypedActorRef").field("path", &self.path().to_string()).finish()
    }
}

impl UntypedActorRef {
    pub fn from_remote(handle: Arc<dyn RemoteRef>) -> Self {
        let path = handle.path().clone();
        Self { inner: Arc::new(UntypedImpl::Remote { path, handle }) }
    }

    pub fn path(&self) -> &ActorPath {
        match &*self.inner {
            UntypedImpl::Local { path, .. } => path,
            UntypedImpl::Remote { path, .. } => path,
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(&*self.inner, UntypedImpl::Remote { .. })
    }

    pub fn stop(&self) {
        match &*self.inner {
            UntypedImpl::Local { system, .. } => {
                let _ = system.send(SystemMsg::Stop);
            }
            UntypedImpl::Remote { handle, .. } => {
                handle.tell_system(RemoteSystemMsg::Stop);
            }
        }
    }

    /// Surface termination to this ref. For local refs this delivers
    /// `SystemMsg::Terminated(sender)` to the actor's system mailbox;
    /// for remote refs it ships a `RemoteSystemMsg::Terminated` PDU.
    /// Used by `actor_cell::finalize` and by `rakka-remote::RemoteWatcher`.
    pub fn notify_watchers(&self, sender: ActorPath) {
        match &*self.inner {
            UntypedImpl::Local { system, .. } => {
                let _ = system.send(SystemMsg::Terminated(sender));
            }
            UntypedImpl::Remote { handle, .. } => {
                handle.tell_system(RemoteSystemMsg::Terminated { actor: sender });
            }
        }
    }
}

impl PartialEq for UntypedActorRef {
    fn eq(&self, other: &Self) -> bool {
        self.path() == other.path()
    }
}

impl Eq for UntypedActorRef {}

impl std::hash::Hash for UntypedActorRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path().hash(state);
    }
}

#[derive(Debug, Error)]
pub enum AskError {
    #[error("ask timed out")]
    Timeout,
    #[error("target actor was dropped before replying")]
    TargetDropped,
}
