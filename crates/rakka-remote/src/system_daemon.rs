//! `RemoteSystemDaemon` + `RemoteDeployer`.
//! akka.net: `Remote/RemoteSystemDaemon.cs`, `Remote/RemoteDeployer.cs`,
//! `Remote/RemoteDeploymentWatcher.cs`.
//!
//! On the receiving side every inbound envelope addressed at
//! `/remote/<system>@<host>:<port>/...` is dispatched here. The daemon
//! resolves local actor paths under `/user`, decodes the payload via the
//! [`SerializerRegistry`], and hands it to the appropriate user actor's
//! mailbox.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use rakka_core::actor::{ActorPath, ActorSystem, RemoteSystemMsg, UntypedActorRef};

use crate::endpoint_manager::EndpointManager;
use crate::serialization::{SerializeError, SerializerRegistry};

/// Function that dispatches a decoded user-message payload to a local actor.
pub type LocalDispatch =
    Arc<dyn Fn(&ActorPath, &str, Box<dyn std::any::Any + Send>) + Send + Sync>;

#[derive(Clone)]
pub struct RemoteSystemDaemon {
    inner: Arc<RemoteSystemDaemonInner>,
}

struct RemoteSystemDaemonInner {
    system: ActorSystem,
    registry: SerializerRegistry,
    endpoint_manager: EndpointManager,
    local_uid: u64,
    routes: RwLock<HashMap<String, LocalDispatch>>,
    /// Path → list of remote watchers that should receive `Terminated`.
    remote_watchers: RwLock<HashMap<String, Vec<UntypedActorRef>>>,
}

impl RemoteSystemDaemon {
    pub fn new(
        system: ActorSystem,
        registry: SerializerRegistry,
        endpoint_manager: EndpointManager,
        local_uid: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(RemoteSystemDaemonInner {
                system,
                registry,
                endpoint_manager,
                local_uid,
                routes: RwLock::new(HashMap::new()),
                remote_watchers: RwLock::new(HashMap::new()),
            }),
        })
    }

    pub fn registry(&self) -> &SerializerRegistry {
        &self.inner.registry
    }

    pub fn system(&self) -> &ActorSystem {
        &self.inner.system
    }

    /// Register a dispatcher for inbound messages addressed to `path`.
    pub fn register(&self, path: ActorPath, dispatch: LocalDispatch) {
        self.inner
            .routes
            .write()
            .insert(path.to_string_without_address(), dispatch);
    }

    pub fn unregister(&self, path: &ActorPath) {
        self.inner
            .routes
            .write()
            .remove(&path.to_string_without_address());
    }

    pub fn clear(&self) {
        self.inner.routes.write().clear();
    }

    pub fn dispatch_user(
        &self,
        path: &ActorPath,
        manifest: &str,
        serializer_id: u32,
        bytes: &[u8],
    ) -> Result<(), SerializeError> {
        let routes = self.inner.routes.read();
        let key = path.to_string_without_address();
        let Some(dispatch) = routes.get(&key).cloned() else {
            tracing::debug!(path = %path, "no remote route registered");
            return Ok(());
        };
        drop(routes);
        let (value, _codec) = self.inner.registry.decode_dyn(manifest, serializer_id, bytes)?;
        dispatch(path, manifest, value);
        Ok(())
    }

    pub fn dispatch_system(&self, path: &ActorPath, msg: RemoteSystemMsg) {
        match msg {
            RemoteSystemMsg::Stop => {
                if let Some(untyped) = self.inner.system.actor_selection(&path.to_string()) {
                    untyped.stop();
                }
            }
            RemoteSystemMsg::Watch { watcher } => {
                let stub = crate::remote_watcher::WatcherStub::new(
                    watcher.clone(),
                    self.inner.endpoint_manager.clone(),
                    self.inner.registry.clone(),
                    self.inner.local_uid,
                );
                self.inner
                    .remote_watchers
                    .write()
                    .entry(path.to_string_without_address())
                    .or_default()
                    .push(UntypedActorRef::from_remote(Arc::new(stub)));
            }
            RemoteSystemMsg::Unwatch { watcher } => {
                let mut g = self.inner.remote_watchers.write();
                if let Some(list) = g.get_mut(&path.to_string_without_address()) {
                    list.retain(|w| w.path() != &watcher);
                }
            }
            RemoteSystemMsg::Terminated { actor: _ } => {
                // Surfaced to the local watching actor by the dispatcher
                // path that delivered this PDU; nothing extra here.
            }
        }
    }

    /// Notify all remote watchers of `path` that the actor has stopped.
    pub fn notify_terminated(&self, path: &ActorPath) {
        let mut g = self.inner.remote_watchers.write();
        let key = path.to_string_without_address();
        let Some(watchers) = g.remove(&key) else { return };
        drop(g);
        for w in watchers {
            w.notify_watchers(path.clone());
        }
    }
}

/// `RemoteDeployer` ships a `Props`-equivalent payload (manifest+bytes)
/// to a remote peer's daemon for remote actor creation.
pub struct RemoteDeployer {
    pub endpoint_manager: EndpointManager,
}

impl RemoteDeployer {
    pub fn new(endpoint_manager: EndpointManager) -> Self {
        Self { endpoint_manager }
    }

    pub async fn deploy(
        &self,
        target_address: rakka_core::actor::Address,
        path: ActorPath,
        manifest: String,
        bytes: Vec<u8>,
    ) -> Result<ActorPath, crate::transport::TransportError> {
        let env = crate::envelope::RemoteEnvelope::user(
            format!("{}/remote/__deploy__", target_address),
            None,
            0,
            0,
            0,
            crate::serialization::BINCODE_SERIALIZER_ID,
            manifest,
            bytes,
        );
        let handle = self.endpoint_manager.endpoint_for(&target_address).await?;
        handle.send(env);
        Ok(path)
    }
}
