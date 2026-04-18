//! `ActorSystem` — root of the actor hierarchy. akka.net: `Actor/ActorSystem.cs`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rustakka_config::Config;
use thiserror::Error;
use tokio::sync::{mpsc, Notify};

use super::actor_cell::{spawn_cell, ChildEntry, SystemMsg};
use super::actor_ref::ActorRef;
use super::address::Address;
use super::extensions::Extensions;
use super::path::ActorPath;
use super::props::Props;
use super::scheduler::{Scheduler, TokioScheduler};
use super::traits::Actor;

pub(crate) struct ActorSystemInner {
    pub name: String,
    pub config: Config,
    pub address: Address,
    pub scheduler: Arc<dyn Scheduler>,
    pub extensions: Extensions,
    pub user_guardian: Mutex<HashMap<String, ChildEntry>>,
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

    /// Spawn a top-level actor under `/user`. akka.net: `ActorOf`.
    pub fn actor_of<A: Actor>(
        &self,
        props: Props<A>,
        name: &str,
    ) -> Result<ActorRef<A::Msg>, ActorSystemError> {
        let root = ActorPath::root(self.inner.address.clone());
        let path = root.child("user").child(name);
        let mut guardian = self.inner.user_guardian.lock();
        if guardian.contains_key(name) {
            return Err(ActorSystemError::NameTaken(name.into()));
        }
        let r = spawn_cell::<A>(self.inner.clone(), props, path.clone())
            .map_err(|e| ActorSystemError::Spawn(e.to_string()))?;
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
