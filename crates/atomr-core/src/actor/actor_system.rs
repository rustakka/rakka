//! `ActorSystem` — root of the actor hierarchy.

use std::collections::HashMap;
use std::sync::Arc;

use atomr_config::Config;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{mpsc, Notify};

use super::actor_cell::{spawn_cell, ChildEntry, SystemMsg};
use super::actor_ref::{ActorRef, UntypedActorRef};
use super::address::Address;
use super::extensions::Extensions;
use super::observer::{DeadLetterObserver, SpawnObserver};
use super::path::ActorPath;
use super::props::Props;
use super::remote::RemoteProvider;
use super::scheduler::{Scheduler, TokioScheduler};
use super::traits::Actor;

pub(crate) struct ActorSystemInner {
    pub name: String,
    pub config: Config,
    pub address: Address,
    pub scheduler: Arc<dyn Scheduler>,
    pub extensions: Extensions,
    pub user_guardian: Mutex<HashMap<String, ChildEntry>>,
    pub(crate) spawn_observer: parking_lot::RwLock<Option<Arc<dyn SpawnObserver>>>,
    pub(crate) dead_letter_observer: parking_lot::RwLock<Option<Arc<dyn DeadLetterObserver>>>,
    pub(crate) remote_provider: parking_lot::RwLock<Option<Arc<dyn RemoteProvider>>>,
    terminated: Notify,
    terminated_flag: std::sync::atomic::AtomicBool,
}

/// Public handle to the actor system.
#[derive(Clone)]
pub struct ActorSystem {
    pub(crate) inner: Arc<ActorSystemInner>,
}

impl ActorSystem {
    /// Create an actor system with the given name and configuration.
    pub async fn create(name: impl Into<String>, config: Config) -> Result<Self, ActorSystemError> {
        let name = name.into();
        let address = Address::local(&name);
        let inner = Arc::new(ActorSystemInner {
            name,
            config,
            address,
            scheduler: Arc::new(TokioScheduler::new()),
            extensions: Extensions::default(),
            user_guardian: Mutex::new(HashMap::new()),
            spawn_observer: parking_lot::RwLock::new(None),
            dead_letter_observer: parking_lot::RwLock::new(None),
            remote_provider: parking_lot::RwLock::new(None),
            terminated: Notify::new(),
            terminated_flag: std::sync::atomic::AtomicBool::new(false),
        });
        Ok(Self { inner })
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn address(&self) -> &Address {
        &self.inner.address
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn scheduler(&self) -> Arc<dyn Scheduler> {
        self.inner.scheduler.clone()
    }

    pub fn extensions(&self) -> &Extensions {
        &self.inner.extensions
    }

    /// Install a [`SpawnObserver`]. Only one observer may be installed;
    /// subsequent calls replace the previous one. This is the hook used by
    /// `atomr-telemetry` to populate its actor registry.
    pub fn set_spawn_observer(&self, obs: Arc<dyn SpawnObserver>) {
        *self.inner.spawn_observer.write() = Some(obs);
    }

    /// Install a [`DeadLetterObserver`] that is notified when a `tell`
    /// fails because the target has stopped.
    pub fn set_dead_letter_observer(&self, obs: Arc<dyn DeadLetterObserver>) {
        *self.inner.dead_letter_observer.write() = Some(obs);
    }

    /// Install the remote provider. Done by `atomr-remote::RemoteSystemExt::enable_remote`.
    /// Replaces any previous provider.
    pub fn set_remote_provider(&self, provider: Arc<dyn RemoteProvider>) {
        *self.inner.remote_provider.write() = Some(provider);
    }

    /// True if a remote provider is installed and the address has global scope.
    pub fn is_remote_path(&self, path: &ActorPath) -> bool {
        path.address.has_global_scope() && self.inner.remote_provider.read().is_some()
    }

    /// Look up an actor by full path string. Local paths return the local
    /// child if it exists; remote paths consult the installed remote provider.
    pub fn actor_selection(&self, path_str: &str) -> Option<UntypedActorRef> {
        let path = parse_actor_path(path_str)?;
        if path.address.has_local_scope() || path.address == self.inner.address {
            // Local: best-effort look-up among top-level user actors.
            if path.elements.len() >= 2 && path.elements[0].as_str() == "user" {
                let name = path.elements[1].as_str();
                let g = self.inner.user_guardian.lock();
                return g.get(name).map(|c| c.untyped.clone());
            }
            return None;
        }
        let provider = self.inner.remote_provider.read().clone()?;
        let handle = provider.resolve(&path)?;
        Some(UntypedActorRef::from_remote(handle))
    }

    /// Resolve a remote path and produce a *typed* `ActorRef<M>`. The caller
    /// supplies a serializer closure for `M`; `atomr-remote::RemoteSystem`
    /// provides a default that uses bincode + `type_name::<M>()`.
    pub fn actor_selection_with<M>(
        &self,
        path_str: &str,
        serialize: Arc<dyn Fn(M, Option<ActorPath>) -> super::remote::SerializedMessage + Send + Sync>,
    ) -> Option<ActorRef<M>>
    where
        M: Send + 'static,
    {
        let path = parse_actor_path(path_str)?;
        if path.address.has_local_scope() || path.address == self.inner.address {
            return None;
        }
        let provider = self.inner.remote_provider.read().clone()?;
        let handle = provider.resolve(&path)?;
        Some(ActorRef::from_remote(handle, serialize))
    }

    /// Spawn a top-level actor under `/user`.
    pub fn actor_of<A: Actor>(
        &self,
        props: Props<A>,
        name: &str,
    ) -> Result<ActorRef<A::Msg>, ActorSystemError> {
        let root = ActorPath::root(self.inner.address.clone());
        let parent = root.child("user");
        let path = parent.child(name);
        let mut guardian = self.inner.user_guardian.lock();
        if guardian.contains_key(name) {
            return Err(ActorSystemError::NameTaken(name.into()));
        }
        let r = spawn_cell::<A>(self.inner.clone(), props, path.clone())
            .map_err(|e| ActorSystemError::Spawn(e.to_string()))?;
        if let Some(obs) = self.inner.spawn_observer.read().as_ref() {
            obs.on_spawn(&path, Some(&parent), std::any::type_name::<A>());
        }
        guardian.insert(
            name.to_string(),
            ChildEntry { path, untyped: r.as_untyped(), system_tx: r.system_sender() },
        );
        Ok(r)
    }

    /// Stop a top-level actor by name.
    pub fn stop(&self, name: &str) {
        if let Some(c) = self.inner.user_guardian.lock().get(name) {
            let _ = c.system_tx.send(SystemMsg::Stop);
        }
    }

    /// Initiate orderly shutdown. Awaits actor termination best-effort.
    pub async fn terminate(&self) {
        {
            let guardian = self.inner.user_guardian.lock();
            for (_, c) in guardian.iter() {
                let _ = c.system_tx.send(SystemMsg::Stop);
            }
        }
        self.inner.terminated_flag.store(true, std::sync::atomic::Ordering::Release);
        self.inner.terminated.notify_waiters();
        // Give in-flight tasks a moment to unwind.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    pub async fn when_terminated(&self) {
        if self.inner.terminated_flag.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        self.inner.terminated.notified().await;
    }
}

#[derive(Debug, Error)]
pub enum ActorSystemError {
    #[error("top-level actor name `{0}` already taken")]
    NameTaken(String),
    #[error("failed to spawn actor: {0}")]
    Spawn(String),
    #[error("system already terminated")]
    Terminated,
}

// Keep channel import referenced to avoid unused imports in stub paths.
#[allow(dead_code)]
type _SysChan = mpsc::UnboundedSender<SystemMsg>;

/// Parse a string like `akka.tcp://Sys@host:port/user/foo/bar` into an
/// `ActorPath`. Returns `None` on malformed input.
fn parse_actor_path(s: &str) -> Option<ActorPath> {
    let (addr_part, path_part) = split_addr_path(s)?;
    let address = Address::parse(addr_part)?;
    let mut path = ActorPath::root(address);
    for seg in path_part.split('/').filter(|s| !s.is_empty()) {
        // Strip optional `#uid` suffix on the leaf segment.
        if let Some((name, uid)) = seg.split_once('#') {
            let uid_n: u64 = uid.parse().ok()?;
            path = path.child(name).with_uid(uid_n);
        } else {
            path = path.child(seg);
        }
    }
    Some(path)
}

fn split_addr_path(s: &str) -> Option<(&str, &str)> {
    // Address always contains `://`. The path starts at the next `/` *after*
    // the host:port section.
    let scheme_end = s.find("://")?;
    let after_scheme = &s[scheme_end + 3..];
    // The address ends at the first `/` in the after-scheme section.
    if let Some(slash) = after_scheme.find('/') {
        let split = scheme_end + 3 + slash;
        Some((&s[..split], &s[split..]))
    } else {
        Some((s, ""))
    }
}
