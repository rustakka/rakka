//! Typed `Sender` value passed alongside every message.
//!
//! Phase 1 of the full-port plan replaces the legacy
//! `Box<dyn Any + Send>` sender erasure with this enum so that every
//! reply path keeps compile-time type information about its origin.
//! See `docs/idiomatic-rust.md` (P-1) and `docs/full-port-plan.md`
//! Phase 1.
//!
//! The legacy `MessageEnvelope::sender: Option<Box<dyn Any + Send>>`
//! and `Context::current_sender` fields are retained during the
//! transition to keep existing call sites compiling. New code should
//! use [`ActorRef::tell_from`] / [`Context::sender_typed`] /
//! [`MessageEnvelope::with_typed_sender`].

use std::sync::Arc;

use super::actor_ref::UntypedActorRef;
use super::path::ActorPath;
use super::remote::RemoteRef;

/// Typed identity of a message's sender.
///
/// Three variants:
///
/// * [`Sender::Local`] — the sender is an actor in this `ActorSystem`.
/// * [`Sender::Remote`] — the sender lives in another `ActorSystem`,
///   reached via remoting. Carries a path + a remote handle so replies
///   can be serialized back without a `downcast`.
/// * [`Sender::None`] — no sender attached (the akka.net analog of
///   `IActorRef.NoSender`).
#[derive(Clone)]
#[non_exhaustive]
pub enum Sender {
    Local(UntypedActorRef),
    Remote {
        path: ActorPath,
        handle: Arc<dyn RemoteRef>,
    },
    None,
}

impl Sender {
    /// Path of the sender, if any.
    pub fn path(&self) -> Option<&ActorPath> {
        match self {
            Sender::Local(r) => Some(r.path()),
            Sender::Remote { path, .. } => Some(path),
            Sender::None => None,
        }
    }

    /// `true` iff the sender lives in another actor system.
    pub fn is_remote(&self) -> bool {
        matches!(self, Sender::Remote { .. })
    }

    /// `true` iff the sender slot is empty.
    pub fn is_none(&self) -> bool {
        matches!(self, Sender::None)
    }

    /// Borrow the local untyped ref, if this is a local sender.
    pub fn local(&self) -> Option<&UntypedActorRef> {
        if let Sender::Local(r) = self {
            Some(r)
        } else {
            None
        }
    }
}

impl std::fmt::Debug for Sender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Sender::Local(r) => f.debug_tuple("Local").field(&r.path().to_string()).finish(),
            Sender::Remote { path, .. } => {
                f.debug_struct("Remote").field("path", &path.to_string()).finish()
            }
            Sender::None => f.write_str("None"),
        }
    }
}

impl Default for Sender {
    fn default() -> Self {
        Sender::None
    }
}

impl From<UntypedActorRef> for Sender {
    fn from(r: UntypedActorRef) -> Self {
        Sender::Local(r)
    }
}

impl<M: Send + 'static> From<&super::actor_ref::ActorRef<M>> for Sender {
    fn from(r: &super::actor_ref::ActorRef<M>) -> Self {
        Sender::Local(r.as_untyped())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_is_default() {
        let s = Sender::default();
        assert!(s.is_none());
        assert!(!s.is_remote());
        assert!(s.path().is_none());
        assert!(s.local().is_none());
    }
}
