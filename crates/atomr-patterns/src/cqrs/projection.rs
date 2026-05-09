//! [`ProjectionHandle`] — typed handle to a reader's projection state.
//!
//! Returned from [`super::CqrsBuilder::with_reader`]. Hold on to it; the
//! reader runner spawned by `materialize` updates the same `Arc` you
//! get here.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;

/// Read-only access to a projection's current state and offset.
///
/// `P` is the user's projection type (the read model). Cloning a handle
/// is cheap — the underlying state is shared.
pub struct ProjectionHandle<P> {
    pub(crate) state: Arc<RwLock<P>>,
    pub(crate) offset: Arc<AtomicU64>,
}

impl<P> Clone for ProjectionHandle<P> {
    fn clone(&self) -> Self {
        Self { state: self.state.clone(), offset: self.offset.clone() }
    }
}

impl<P: Send + Sync + 'static> ProjectionHandle<P> {
    /// Highest journal sequence number the runner has applied.
    /// Useful for tests that wait until the projection has caught up.
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::Acquire)
    }

    /// Take a read lock on the projection state.
    pub async fn snapshot(&self) -> tokio::sync::RwLockReadGuard<'_, P> {
        self.state.read().await
    }

    /// Apply a closure to the projection state under a read lock and
    /// return the result. Convenience wrapper around [`Self::snapshot`].
    pub async fn read<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        let guard = self.state.read().await;
        f(&*guard)
    }
}
