//! [`SagaStateStore`] — pluggable per-correlation state storage.
//!
//! The saga runner keeps state keyed by correlation id. The default
//! [`InMemorySagaStateStore`] is fine for tests and single-process
//! workloads but loses state on restart. Implement this trait against
//! a durable backend (or build on top of [`atomr_persistence::Journal`]
//! via [`JournalSagaStateStore`]) for production sagas.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use atomr_persistence::{Journal, PersistentRepr};
use parking_lot::RwLock;

/// Per-correlation state storage. Saga state is opaque (`Vec<u8>`) at
/// this layer; the saga supplies the codec via
/// [`crate::saga::Saga::encode_state`] / [`crate::saga::Saga::decode_state`].
#[async_trait]
pub trait SagaStateStore: Send + Sync + 'static {
    /// Load the persisted state for `correlation_id`. `None` means no
    /// state exists yet (treat as fresh / `Default`).
    async fn load(&self, correlation_id: &str) -> Option<Vec<u8>>;

    /// Persist `payload` as the latest state for `correlation_id`.
    async fn save(&self, correlation_id: &str, payload: Vec<u8>);

    /// Drop the state for `correlation_id` (called on `SagaAction::Complete`).
    async fn delete(&self, correlation_id: &str);

    /// Every correlation id with persisted state — used at startup to
    /// rehydrate in-flight sagas.
    async fn keys(&self) -> Vec<String>;
}

/// Reference in-memory implementation. Survives runner restarts within
/// the same process; loses everything on process restart.
pub struct InMemorySagaStateStore {
    inner: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl Default for InMemorySagaStateStore {
    fn default() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }
}

impl InMemorySagaStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SagaStateStore for InMemorySagaStateStore {
    async fn load(&self, correlation_id: &str) -> Option<Vec<u8>> {
        self.inner.read().get(correlation_id).cloned()
    }
    async fn save(&self, correlation_id: &str, payload: Vec<u8>) {
        self.inner.write().insert(correlation_id.into(), payload);
    }
    async fn delete(&self, correlation_id: &str) {
        self.inner.write().remove(correlation_id);
    }
    async fn keys(&self) -> Vec<String> {
        self.inner.read().keys().cloned().collect()
    }
}

/// Journal-backed saga state store.
///
/// Each `(saga_name, correlation_id)` pair is treated as a single
/// persistence id (`saga::<saga_name>::<correlation_id>`). Saves
/// append a new event; loads replay the stream and return the most
/// recent state payload. `keys()` is best-effort — it consults
/// [`Journal::all_persistence_ids`].
pub struct JournalSagaStateStore<J: Journal> {
    journal: Arc<J>,
    saga_name: String,
    writer_uuid: String,
    _marker: PhantomData<J>,
}

impl<J: Journal> JournalSagaStateStore<J> {
    pub fn new(journal: Arc<J>, saga_name: impl Into<String>) -> Self {
        Self {
            journal,
            saga_name: saga_name.into(),
            writer_uuid: format!("saga-{}", rand_id()),
            _marker: PhantomData,
        }
    }

    fn pid(&self, correlation_id: &str) -> String {
        format!("saga::{}::{}", self.saga_name, correlation_id)
    }

    fn pid_prefix(&self) -> String {
        format!("saga::{}::", self.saga_name)
    }
}

#[async_trait]
impl<J: Journal> SagaStateStore for JournalSagaStateStore<J> {
    async fn load(&self, correlation_id: &str) -> Option<Vec<u8>> {
        let pid = self.pid(correlation_id);
        let highest = self.journal.highest_sequence_nr(&pid, 0).await.ok()?;
        if highest == 0 {
            return None;
        }
        let reprs = self
            .journal
            .replay_messages(&pid, highest, highest, 1)
            .await
            .ok()?;
        reprs.into_iter().last().filter(|r| !r.deleted).map(|r| r.payload)
    }

    async fn save(&self, correlation_id: &str, payload: Vec<u8>) {
        let pid = self.pid(correlation_id);
        let next_seq = self
            .journal
            .highest_sequence_nr(&pid, 0)
            .await
            .unwrap_or(0)
            + 1;
        let _ = self
            .journal
            .write_messages(vec![PersistentRepr {
                persistence_id: pid,
                sequence_nr: next_seq,
                payload,
                manifest: "saga-state".into(),
                writer_uuid: self.writer_uuid.clone(),
                deleted: false,
                tags: vec![format!("saga::{}", self.saga_name)],
            }])
            .await;
    }

    async fn delete(&self, correlation_id: &str) {
        let pid = self.pid(correlation_id);
        if let Ok(highest) = self.journal.highest_sequence_nr(&pid, 0).await {
            if highest > 0 {
                let _ = self.journal.delete_messages_to(&pid, highest).await;
            }
        }
    }

    async fn keys(&self) -> Vec<String> {
        let prefix = self.pid_prefix();
        self.journal
            .all_persistence_ids()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|pid| pid.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }
}

fn rand_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}
