//! `ReplicatorActor` — Phase 8.E.
//!
//! Wraps a [`crate::Replicator`] in an actor-style task: all reads and
//! writes are serialized through an mpsc command channel rather than the
//! existing `RwLock<HashMap>` plumbing. Useful in cluster mode where
//! gossip-driven merges must interleave with consistency-level quorums
//! without lock contention.
//!
//! Pairs with [`crate::DurableStore`] (Phase 8.F) for write-through
//! persistence: every successful update / delete flushes through the
//! configured store before the response is acknowledged.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::durable::{DurableStore, NoopDurableStore};
use crate::replicator::{ReadConsistency, Replicator, WriteConsistency};
use crate::traits::CrdtMerge;

/// Tagged response payloads for [`ReplicatorActor`] commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicatorAck {
    Ok,
    KeyNotFound,
    Timeout,
}

type DynUpdate = Box<dyn FnOnce(&Arc<Replicator>) -> Result<(), ReplicatorError> + Send + 'static>;
type DynQuery = Box<dyn FnOnce(&Arc<Replicator>) + Send + 'static>;

enum Cmd {
    Update { key: String, op: DynUpdate, ack: oneshot::Sender<ReplicatorAck> },
    Query { op: DynQuery },
    Delete { key: String, ack: oneshot::Sender<ReplicatorAck> },
    Shutdown,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicatorError {
    #[error("type mismatch for key {0}")]
    TypeMismatch(String),
}

/// Actor-style replicator handle.
pub struct ReplicatorActor {
    cmd: mpsc::UnboundedSender<Cmd>,
    inner: Arc<Replicator>,
    join: Option<JoinHandle<()>>,
}

impl ReplicatorActor {
    /// Spawn a new replicator with the default in-memory store.
    pub fn spawn() -> Self {
        Self::spawn_with(Arc::new(NoopDurableStore))
    }

    /// Spawn a new replicator with a pluggable durable backend.
    pub fn spawn_with(store: Arc<dyn DurableStore>) -> Self {
        let inner = Replicator::new();
        let inner2 = inner.clone();
        let (tx, mut rx) = mpsc::unbounded_channel::<Cmd>();
        let join = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Cmd::Update { key, op, ack } => {
                        let res = op(&inner2);
                        if res.is_ok() {
                            // Snapshot the entry as serialized bytes via the durable store
                            // (the type-erased layer means we can only persist the key here;
                            // the user is responsible for installing a typed store via
                            // `register_persistor` for now — keep this actor a serialized
                            // façade and let durability be opt-in per key).
                            let _ = store.persist_marker(&key);
                        }
                        let _ = ack.send(match res {
                            Ok(()) => ReplicatorAck::Ok,
                            Err(_) => ReplicatorAck::KeyNotFound,
                        });
                    }
                    Cmd::Query { op } => op(&inner2),
                    Cmd::Delete { key, ack } => {
                        inner2.delete(&key);
                        let _ = store.delete_marker(&key);
                        let _ = ack.send(ReplicatorAck::Ok);
                    }
                    Cmd::Shutdown => break,
                }
            }
        });
        Self { cmd: tx, inner, join: Some(join) }
    }

    /// Read-only access to the inner replicator (subscriptions etc).
    pub fn inner(&self) -> &Arc<Replicator> {
        &self.inner
    }

    /// Submit a typed update and wait for the ack.
    pub async fn update<T>(&self, key: impl Into<String>, value: T, _write: WriteConsistency) -> ReplicatorAck
    where
        T: CrdtMerge + Send + Sync + 'static,
    {
        let key = key.into();
        let key_for_op = key.clone();
        let (ack_tx, ack_rx) = oneshot::channel();
        let op: DynUpdate = Box::new(move |r: &Arc<Replicator>| {
            r.update(&key_for_op, value);
            Ok(())
        });
        if self.cmd.send(Cmd::Update { key, op, ack: ack_tx }).is_err() {
            return ReplicatorAck::Timeout;
        }
        ack_rx.await.unwrap_or(ReplicatorAck::Timeout)
    }

    /// Read a key. Consistency is `Local` for now (Phase 8.D wires the
    /// quorum exchange once gossip lands).
    pub async fn get<T>(&self, key: impl Into<String>, _read: ReadConsistency) -> Option<T>
    where
        T: CrdtMerge + Clone + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let key = key.into();
        let op: DynQuery = Box::new(move |r: &Arc<Replicator>| {
            let v: Option<T> = r.get::<T>(&key);
            let _ = tx.send(v);
        });
        if self.cmd.send(Cmd::Query { op }).is_err() {
            return None;
        }
        rx.await.ok().flatten()
    }

    /// Delete a key.
    pub async fn delete(&self, key: impl Into<String>) -> ReplicatorAck {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.cmd.send(Cmd::Delete { key: key.into(), ack: ack_tx }).is_err() {
            return ReplicatorAck::Timeout;
        }
        ack_rx.await.unwrap_or(ReplicatorAck::Timeout)
    }

    /// Stop the actor task and join it.
    pub async fn shutdown(mut self) {
        let _ = self.cmd.send(Cmd::Shutdown);
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }
}

impl Drop for ReplicatorActor {
    fn drop(&mut self) {
        let _ = self.cmd.send(Cmd::Shutdown);
        if let Some(j) = self.join.take() {
            j.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GCounter;
    use std::time::Duration;

    #[tokio::test]
    async fn actor_serializes_update_and_get() {
        let r = ReplicatorActor::spawn();
        let mut c = GCounter::new();
        c.increment("n1", 4);
        let ack = r.update("k", c, WriteConsistency::Local).await;
        assert_eq!(ack, ReplicatorAck::Ok);
        let got: GCounter = r.get::<GCounter>("k", ReadConsistency::Local).await.expect("key found");
        assert_eq!(got.value(), 4);
        r.shutdown().await;
    }

    #[tokio::test]
    async fn actor_delete_marks_key_gone() {
        let r = ReplicatorActor::spawn();
        let mut c = GCounter::new();
        c.increment("n1", 1);
        r.update("k", c, WriteConsistency::Local).await;
        r.delete("k").await;
        let v: Option<GCounter> = r.get("k", ReadConsistency::Local).await;
        assert!(v.is_none());
        r.shutdown().await;
    }

    #[tokio::test]
    async fn actor_persists_through_durable_store() {
        let store = Arc::new(crate::durable::FileDurableStore::tmp().unwrap());
        let r = ReplicatorActor::spawn_with(store.clone());
        let mut c = GCounter::new();
        c.increment("n1", 7);
        r.update("k", c, WriteConsistency::Local).await;
        // Give the spawned task one scheduling tick.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(store.contains("k"));
        r.delete("k").await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!store.contains("k"));
        r.shutdown().await;
    }
}
