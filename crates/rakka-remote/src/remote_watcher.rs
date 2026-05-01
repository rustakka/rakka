//! `RemoteWatcher`. akka.net: `Remote/RemoteWatcher.cs`.
//!
//! Tracks local actors that are watching remote ones and surfaces
//! `Terminated` when:
//!
//! * The watched actor's `ActorSystem` disassociates (graceful or quarantine).
//! * The watched actor's UID changes (peer crash/restart).
//! * The peer's failure detector trips.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use rakka_core::actor::{ActorPath, RemoteRef, RemoteSystemMsg, UntypedActorRef};

use crate::endpoint_manager::EndpointManager;
use crate::failure_detector_registry::FailureDetectorRegistry;
use crate::remote_ref::RemoteActorRefImpl;
use crate::serialization::SerializerRegistry;

#[derive(Clone, Debug)]
struct Watch {
    watcher: UntypedActorRef,
    watchee: ActorPath,
}

#[derive(Clone)]
pub struct RemoteWatcher {
    inner: Arc<RemoteWatcherInner>,
}

struct RemoteWatcherInner {
    endpoint_manager: EndpointManager,
    detectors: FailureDetectorRegistry,
    registry: SerializerRegistry,
    local_uid: u64,
    watches: RwLock<Vec<Watch>>,
    terminated_addresses: RwLock<HashSet<String>>,
    started: std::sync::OnceLock<()>,
}

impl RemoteWatcher {
    pub fn new(
        endpoint_manager: EndpointManager,
        registry: SerializerRegistry,
        local_uid: u64,
    ) -> Arc<Self> {
        let detectors = endpoint_manager.failure_detectors();
        Arc::new(Self {
            inner: Arc::new(RemoteWatcherInner {
                endpoint_manager,
                detectors,
                registry,
                local_uid,
                watches: RwLock::new(Vec::new()),
                terminated_addresses: RwLock::new(HashSet::new()),
                started: std::sync::OnceLock::new(),
            }),
        })
    }

    /// Begin watching `watchee`. The local watcher receives
    /// `SystemMsg::Terminated` if the watchee's host disassociates.
    pub async fn watch(
        self: &Arc<Self>,
        watcher: UntypedActorRef,
        watchee: ActorPath,
    ) -> Result<(), crate::transport::TransportError> {
        let target = watchee.address.clone();
        // Inform the peer via a system PDU so it can echo Terminated
        // when the actor stops there.
        let _ = self.inner.endpoint_manager.endpoint_for(&target).await?;
        let remote_ref = RemoteActorRefImpl::new(
            watchee.clone(),
            self.inner.endpoint_manager.clone(),
            self.inner.registry.clone(),
            self.inner.local_uid,
        );
        remote_ref.tell_system(RemoteSystemMsg::Watch {
            watcher: watcher.path().clone(),
        });
        self.inner.watches.write().push(Watch { watcher, watchee });
        self.start_supervisor();
        Ok(())
    }

    pub async fn unwatch(self: &Arc<Self>, watcher: &UntypedActorRef, watchee: &ActorPath) {
        self.inner
            .watches
            .write()
            .retain(|w| !(w.watcher.path() == watcher.path() && &w.watchee == watchee));
        let target = watchee.address.clone();
        if self
            .inner
            .endpoint_manager
            .endpoint_for(&target)
            .await
            .is_ok()
        {
            let remote_ref = RemoteActorRefImpl::new(
                watchee.clone(),
                self.inner.endpoint_manager.clone(),
                self.inner.registry.clone(),
                self.inner.local_uid,
            );
            remote_ref.tell_system(RemoteSystemMsg::Unwatch {
                watcher: watcher.path().clone(),
            });
        }
    }

    /// Driven by the periodic supervisor task. Surfaces `Terminated` for
    /// any actor whose host has gone unavailable.
    pub fn check(&self) {
        let mut bad: Vec<String> = Vec::new();
        for addr_str in self.inner.detectors.addresses() {
            if let Some(addr) = rakka_core::actor::Address::parse(&addr_str) {
                if !self.inner.detectors.is_available(&addr) {
                    bad.push(addr_str);
                }
            }
        }
        if bad.is_empty() {
            return;
        }
        let mut terminated = self.inner.terminated_addresses.write();
        let watches = self.inner.watches.read();
        for addr in bad {
            if !terminated.insert(addr.clone()) {
                continue;
            }
            for w in watches.iter() {
                if w.watchee.address.to_string() == addr {
                    w.watcher.notify_watchers(w.watchee.clone());
                }
            }
        }
    }

    fn start_supervisor(self: &Arc<Self>) {
        if self.inner.started.set(()).is_err() {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                this.check();
            }
        });
    }

    pub fn watch_count(&self) -> usize {
        self.inner.watches.read().len()
    }
}

/// Outbound `RemoteRef` proxy used by the daemon's death-watch
/// book-keeping. Carries the `EndpointManager` + `SerializerRegistry`
/// so the proxy can serialize and ship `RemoteSystemMsg::Terminated`
/// over the wire without going through the local mailbox path.
/// Wraps cheap clones of those handles; constructing the proxy
/// itself is cheap.
pub(crate) struct RemoteWatcherProxy {
    pub path: ActorPath,
    pub endpoint_manager: Option<EndpointManager>,
    pub registry: Option<SerializerRegistry>,
    pub local_uid: u64,
}

impl std::fmt::Debug for RemoteWatcherProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteWatcherProxy").field("path", &self.path).finish()
    }
}

impl RemoteWatcherProxy {
    pub fn new(
        path: ActorPath,
        endpoint_manager: EndpointManager,
        registry: SerializerRegistry,
        local_uid: u64,
    ) -> Self {
        Self { path, endpoint_manager: Some(endpoint_manager), registry: Some(registry), local_uid }
    }
}

impl RemoteRef for RemoteWatcherProxy {
    fn path(&self) -> &ActorPath {
        &self.path
    }

    fn tell_serialized(&self, _msg: rakka_core::actor::SerializedMessage) {
        // Watcher proxies only forward Terminated; user payloads flow
        // through the regular RemoteActorRef path instead.
    }

    fn tell_system(&self, msg: RemoteSystemMsg) {
        let (Some(mgr), Some(reg)) =
            (self.endpoint_manager.clone(), self.registry.clone())
        else {
            return;
        };
        let target = self.path.clone();
        let local_uid = self.local_uid;
        let r = RemoteActorRefImpl::new(target, mgr, reg, local_uid);
        r.tell_system(msg);
    }
}
