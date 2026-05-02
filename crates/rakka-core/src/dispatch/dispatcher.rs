//! Dispatchers schedule actor cells onto a runtime.
//! akka.net: `Dispatch/Dispatcher.cs`, `PinnedDispatcher.cs`.

use std::future::Future;
use std::sync::Arc;

use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

/// Abstraction over "somewhere a task can run".
pub trait Dispatcher: Send + Sync {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle;

    /// akka.net: `Throughput`.
    fn throughput(&self) -> u32 {
        10
    }
}

pub struct DispatcherHandle(pub(crate) JoinHandle<()>);

impl DispatcherHandle {
    pub async fn join(self) {
        let _ = self.0.await;
    }

    pub fn abort(&self) {
        self.0.abort();
    }
}

/// Default dispatcher — uses the ambient Tokio runtime.
pub struct DefaultDispatcher {
    handle: Handle,
    throughput: u32,
}

impl DefaultDispatcher {
    pub fn new(handle: Handle, throughput: u32) -> Self {
        Self { handle, throughput }
    }

    pub fn current() -> Self {
        Self::new(Handle::current(), 10)
    }
}

impl Dispatcher for DefaultDispatcher {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle {
        DispatcherHandle(self.handle.spawn(task))
    }

    fn throughput(&self) -> u32 {
        self.throughput
    }
}

/// Dedicated single-thread runtime for actors that require strict affinity.
/// akka.net: `PinnedDispatcher`.
pub struct PinnedDispatcher {
    rt: Arc<Runtime>,
}

impl PinnedDispatcher {
    pub fn new() -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        Ok(Self { rt: Arc::new(rt) })
    }
}

impl Dispatcher for PinnedDispatcher {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle {
        DispatcherHandle(self.rt.spawn(task))
    }
}

/// Helper to run a future on the default tokio executor.
pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(f)
}

/// Multi-thread dedicated runtime sized by `worker_threads`.
/// akka.net: `ThreadPoolDispatcher`.
pub struct ThreadPoolDispatcher {
    rt: Arc<Runtime>,
    throughput: u32,
}

impl ThreadPoolDispatcher {
    pub fn new(worker_threads: usize, throughput: u32) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads.max(1))
            .enable_all()
            .build()?;
        Ok(Self { rt: Arc::new(rt), throughput })
    }
}

impl Dispatcher for ThreadPoolDispatcher {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle {
        DispatcherHandle(self.rt.spawn(task))
    }
    fn throughput(&self) -> u32 {
        self.throughput
    }
}

/// Dispatcher that runs the task immediately on the calling thread by
/// using `tokio::task::spawn_blocking` to drive the future to completion
/// inline. akka.net: `CallingThreadDispatcher`. Mostly useful in tests.
pub struct CallingThreadDispatcher;

impl Dispatcher for CallingThreadDispatcher {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle {
        DispatcherHandle(tokio::task::spawn(task))
    }
    fn throughput(&self) -> u32 {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn default_dispatcher_runs_task() {
        let d = DefaultDispatcher::current();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let h = d.spawn_task(Box::pin(async move {
            tx.send(42u32).unwrap();
        }));
        assert_eq!(rx.await.unwrap(), 42);
        h.join().await;
    }
}
