//! Remoting extension points exposed by `rakka-core`.
//!
//! The actual transport, endpoint manager, and remote-ref implementation
//! live in `rakka-remote`. This module declares the *trait surface* that
//! `rakka-remote` plugs into so that `rakka-core` does not need a build
//! dependency on remoting.
//!
//! akka.net: `Akka.Remote/RemoteActorRef.cs`,
//! `Akka.Remote/RemoteActorRefProvider.cs`,
//! `Akka.Remote/MessageSerializer.cs` collectively.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::address::Address;
use super::path::ActorPath;

/// A serialized user message destined for a remote actor.
///
/// `manifest` is the type identifier (`std::any::type_name::<M>()` by default)
/// and `serializer_id` keys into the per-system serializer registry on the
/// receiving side.
#[derive(Clone, Debug)]
pub struct SerializedMessage {
    pub serializer_id: u32,
    pub manifest: String,
    pub payload: Vec<u8>,
    pub sender: Option<ActorPath>,
}

/// System-level controls that travel across the wire.
///
/// akka.net: `Akka.Remote/RemoteWatcher.cs` + the system-message serializer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RemoteSystemMsg {
    Stop,
    Watch { watcher: ActorPath },
    Unwatch { watcher: ActorPath },
    Terminated { actor: ActorPath },
}

/// A reference to an actor on a *different* `ActorSystem`.
///
/// Implementations live in `rakka-remote::RemoteActorRefImpl`. The trait is
/// object-safe so `ActorRef<M>` can carry an `Arc<dyn RemoteRef>` regardless
/// of the user message type.
pub trait RemoteRef: Send + Sync + std::fmt::Debug {
    fn path(&self) -> &ActorPath;
    fn tell_serialized(&self, msg: SerializedMessage);
    fn tell_system(&self, msg: RemoteSystemMsg);
}

/// Pluggable resolver: given a fully-qualified `ActorPath`, return a remote
/// handle that can deliver to it. Installed on the `ActorSystem` by
/// `rakka-remote::RemoteActorRefProvider::install`.
pub trait RemoteProvider: Send + Sync {
    fn local_address(&self) -> &Address;
    fn resolve(&self, path: &ActorPath) -> Option<Arc<dyn RemoteRef>>;
}
