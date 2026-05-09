//! Inbox pattern — idempotent receiver for messages arriving from
//! external sources.
//!
//! The mirror image of [`crate::outbox::OutboxPattern`]: outbox
//! guarantees at-least-once delivery *out* of a system; inbox makes
//! the *receiving* side idempotent so duplicates are silently
//! suppressed.
//!
//! ```ignore
//! let inbox = InboxPattern::<OrderEvent>::builder()
//!     .name("orders-inbox")
//!     .key(|e: &OrderEvent| e.id().to_string())
//!     .source(rx)
//!     .handler(|e: OrderEvent| async move { process(e).await; true })
//!     .store(Arc::new(InMemoryInboxStore::new()))
//!     .build()?
//!     .materialize(&system)
//!     .await?;
//! ```

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use atomr_core::actor::ActorSystem;
use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::topology::Topology;
use crate::PatternError;

/// Persistent record of which idempotency keys have been processed.
#[async_trait]
pub trait InboxStore: Send + Sync + 'static {
    /// True if `key` has already been recorded as processed.
    async fn was_seen(&self, key: &str) -> bool;
    /// Record `key` as processed. Idempotent — repeat calls are
    /// no-ops.
    async fn mark_seen(&self, key: &str);
}

/// In-memory reference implementation. Survives runner restarts
/// within the same process; loses everything on process restart.
#[derive(Default)]
pub struct InMemoryInboxStore {
    inner: Arc<Mutex<HashSet<String>>>,
}

impl InMemoryInboxStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl InboxStore for InMemoryInboxStore {
    async fn was_seen(&self, key: &str) -> bool {
        self.inner.lock().contains(key)
    }
    async fn mark_seen(&self, key: &str) {
        self.inner.lock().insert(key.into());
    }
}

type KeyFn<E> = Arc<dyn Fn(&E) -> String + Send + Sync + 'static>;
type Handler<E> = Arc<dyn Fn(E) -> futures::future::BoxFuture<'static, bool> + Send + Sync>;

/// Public, zero-sized handle to the inbox pattern.
pub struct InboxPattern<E>(PhantomData<E>);

impl<E: Send + 'static> InboxPattern<E> {
    pub fn builder() -> InboxBuilder<E> {
        InboxBuilder { name: None, key: None, source: None, handler: None, store: None }
    }
}

pub struct InboxBuilder<E: Send + 'static> {
    name: Option<String>,
    key: Option<KeyFn<E>>,
    source: Option<UnboundedReceiver<E>>,
    handler: Option<Handler<E>>,
    store: Option<Arc<dyn InboxStore>>,
}

impl<E: Send + 'static> InboxBuilder<E> {
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    /// Closure that derives the idempotency key from a message.
    pub fn key<F>(mut self, f: F) -> Self
    where
        F: Fn(&E) -> String + Send + Sync + 'static,
    {
        self.key = Some(Arc::new(f));
        self
    }

    /// Inbound message source.
    pub fn source(mut self, rx: UnboundedReceiver<E>) -> Self {
        self.source = Some(rx);
        self
    }

    /// Handler for not-yet-seen messages. Returning `true` marks the
    /// message as processed; `false` leaves it unmarked so a
    /// retry can re-attempt processing.
    pub fn handler<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = bool> + Send + 'static,
    {
        let f = Arc::new(f);
        self.handler = Some(Arc::new(move |e| {
            let f = f.clone();
            Box::pin(async move { f(e).await })
        }));
        self
    }

    pub fn store<S: InboxStore>(mut self, store: Arc<S>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn build(self) -> Result<InboxTopology<E>, PatternError<()>> {
        Ok(InboxTopology {
            name: self.name.unwrap_or_else(|| "inbox".into()),
            key: self.key.ok_or(PatternError::NotConfigured("key"))?,
            source: self.source.ok_or(PatternError::NotConfigured("source"))?,
            handler: self.handler.ok_or(PatternError::NotConfigured("handler"))?,
            store: self.store.unwrap_or_else(|| Arc::new(InMemoryInboxStore::new())),
        })
    }
}

pub struct InboxTopology<E: Send + 'static> {
    name: String,
    key: KeyFn<E>,
    source: UnboundedReceiver<E>,
    handler: Handler<E>,
    store: Arc<dyn InboxStore>,
}

pub struct InboxHandles {
    pub name: String,
}

#[async_trait]
impl<E: Send + 'static> Topology for InboxTopology<E> {
    type Handles = InboxHandles;

    async fn materialize(self, _system: &ActorSystem) -> Result<InboxHandles, PatternError<()>> {
        let InboxTopology { name, key, mut source, handler, store } = self;
        let task_name = name.clone();
        tokio::spawn(async move {
            while let Some(msg) = source.recv().await {
                let k = (key)(&msg);
                if store.was_seen(&k).await {
                    tracing::trace!(inbox = %task_name, key = %k, "duplicate suppressed");
                    continue;
                }
                if (handler)(msg).await {
                    store.mark_seen(&k).await;
                } else {
                    tracing::warn!(inbox = %task_name, key = %k, "handler returned false; key not marked");
                }
            }
        });
        Ok(InboxHandles { name })
    }
}
