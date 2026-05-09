//! Transactional Outbox pattern.
//!
//! Tail-follows the [`atomr_persistence_query::ReadJournal`] and
//! re-emits events into a publish callback, persisting the offset so
//! restarts don't double-publish. Use this when you have a side-effect
//! (e.g. publishing to Kafka, hitting a webhook) that must occur
//! "at-least-once after every successful aggregate write."

mod journal_offset_store;

pub use journal_offset_store::JournalOffsetStore;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_core::actor::ActorSystem;
use atomr_persistence_query::ReadJournal;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::topology::Topology;
use crate::PatternError;

/// Public, zero-sized handle to the outbox pattern.
pub struct OutboxPattern<E>(PhantomData<E>);

impl<E: Clone + Send + 'static> OutboxPattern<E> {
    pub fn builder() -> OutboxBuilder<E> {
        OutboxBuilder {
            name: None,
            read_journal: None,
            poll_interval: Duration::from_millis(50),
            decode: None,
            publish: None,
            offset_store: None,
        }
    }
}

type Decoder<E> = Arc<dyn Fn(&[u8]) -> Result<E, String> + Send + Sync>;
type Publisher<E> = Arc<dyn Fn(E) -> futures::future::BoxFuture<'static, bool> + Send + Sync>;

/// Pluggable per-pid offset persistence. `load`/`save` return offsets
/// keyed by persistence_id.
pub trait OutboxOffsetStore: Send + Sync + 'static {
    fn load(&self) -> HashMap<String, u64>;
    fn save(&self, offsets: &HashMap<String, u64>);
}

/// In-memory offset store — useful for tests. State is kept in a
/// `Mutex<HashMap>`; survives restarts of the publisher loop, but not
/// process restarts.
#[derive(Default)]
pub struct InMemoryOffsetStore {
    inner: Arc<Mutex<HashMap<String, u64>>>,
}

impl InMemoryOffsetStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> HashMap<String, u64> {
        self.inner.lock().clone()
    }
}

impl OutboxOffsetStore for InMemoryOffsetStore {
    fn load(&self) -> HashMap<String, u64> {
        self.inner.lock().clone()
    }
    fn save(&self, offsets: &HashMap<String, u64>) {
        let mut guard = self.inner.lock();
        for (k, v) in offsets {
            guard.insert(k.clone(), *v);
        }
    }
}

pub struct OutboxBuilder<E> {
    name: Option<String>,
    read_journal: Option<Arc<dyn ReadJournal>>,
    poll_interval: Duration,
    decode: Option<Decoder<E>>,
    publish: Option<Publisher<E>>,
    offset_store: Option<Arc<dyn OutboxOffsetStore>>,
}

impl<E: Clone + Send + 'static> OutboxBuilder<E> {
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    pub fn read_journal<R: ReadJournal>(mut self, rj: Arc<R>) -> Self {
        self.read_journal = Some(rj);
        self
    }

    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    pub fn decode<F>(mut self, f: F) -> Self
    where
        F: Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static,
    {
        self.decode = Some(Arc::new(f));
        self
    }

    pub fn publish<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = bool> + Send + 'static,
    {
        let f = Arc::new(f);
        self.publish = Some(Arc::new(move |e| {
            let f = f.clone();
            Box::pin(async move { f(e).await })
        }));
        self
    }

    pub fn offset_store<S: OutboxOffsetStore>(mut self, s: Arc<S>) -> Self {
        self.offset_store = Some(s);
        self
    }

    pub fn build(self) -> Result<OutboxTopology<E>, PatternError<()>> {
        Ok(OutboxTopology {
            name: self.name.unwrap_or_else(|| "outbox".into()),
            read_journal: self.read_journal.ok_or(PatternError::NotConfigured("read_journal"))?,
            poll_interval: self.poll_interval,
            decode: self.decode.ok_or(PatternError::NotConfigured("decode"))?,
            publish: self.publish.ok_or(PatternError::NotConfigured("publish"))?,
            offset_store: self.offset_store.unwrap_or_else(|| Arc::new(InMemoryOffsetStore::new())),
        })
    }
}

pub struct OutboxTopology<E> {
    #[allow(dead_code)]
    name: String,
    read_journal: Arc<dyn ReadJournal>,
    poll_interval: Duration,
    decode: Decoder<E>,
    publish: Publisher<E>,
    offset_store: Arc<dyn OutboxOffsetStore>,
}

pub struct OutboxHandles {
    pub published: Arc<AtomicU64>,
    stopper: oneshot::Sender<()>,
}

impl OutboxHandles {
    /// Stop the publisher loop. Idempotent — second call is a no-op.
    pub fn stop(self) {
        let _ = self.stopper.send(());
    }

    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Acquire)
    }
}

#[async_trait]
impl<E: Clone + Send + 'static> Topology for OutboxTopology<E> {
    type Handles = OutboxHandles;

    async fn materialize(self, _system: &ActorSystem) -> Result<OutboxHandles, PatternError<()>> {
        let OutboxTopology { name, read_journal, poll_interval, decode, publish, offset_store } = self;
        let published = Arc::new(AtomicU64::new(0));
        let published_clone = published.clone();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut pid_offsets = offset_store.load();
            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                let pids = match read_journal.all_persistence_ids().await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(outbox = %name, error = ?e, "list pids failed");
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                };
                for pid in pids {
                    let from = pid_offsets.get(&pid).copied().unwrap_or(0).saturating_add(1);
                    let events = match read_journal.events_by_persistence_id(&pid, from, u64::MAX).await {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!(outbox = %name, pid = %pid, error = ?e, "read failed");
                            continue;
                        }
                    };
                    for env in events {
                        match (decode)(&env.payload) {
                            Ok(event) => {
                                let ok = (publish)(event).await;
                                if ok {
                                    pid_offsets.insert(pid.clone(), env.sequence_nr);
                                    published_clone.fetch_add(1, Ordering::AcqRel);
                                } else {
                                    // Stop advancing; retry next tick.
                                    break;
                                }
                            }
                            Err(s) => {
                                tracing::warn!(outbox = %name, error = %s, "decode failed");
                                pid_offsets.insert(pid.clone(), env.sequence_nr);
                            }
                        }
                    }
                }
                offset_store.save(&pid_offsets);
                tokio::time::sleep(poll_interval).await;
            }
        });
        Ok(OutboxHandles { published, stopper: stop_tx })
    }
}
