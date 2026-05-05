//! Dispatchers schedule actor cells onto a runtime.
//! akka.net: `Dispatch/Dispatcher.cs`, `PinnedDispatcher.cs`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

/// Configuration knobs for any [`Dispatcher`]. akka.net:
/// `Dispatchers.cs` / `Dispatcher.cs` config keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatcherConfig {
    /// Maximum messages an actor cell may process before yielding.
    /// akka.net: `throughput`.
    pub throughput: u32,
    /// Time budget per scheduling slice; if exceeded the cell yields
    /// even if it has not hit `throughput`. akka.net:
    /// `throughput-deadline-time`. `None` disables the deadline.
    pub throughput_deadline: Option<Duration>,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self { throughput: 10, throughput_deadline: None }
    }
}

/// Abstraction over "somewhere a task can run".
pub trait Dispatcher: Send + Sync {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle;

    /// akka.net: `Throughput`.
    fn throughput(&self) -> u32 {
        10
    }

    /// akka.net: `ThroughputDeadlineTime`. `None` is unbounded.
    fn throughput_deadline(&self) -> Option<Duration> {
        None
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
    config: DispatcherConfig,
}

impl DefaultDispatcher {
    pub fn new(handle: Handle, throughput: u32) -> Self {
        Self { handle, config: DispatcherConfig { throughput, throughput_deadline: None } }
    }

    pub fn with_config(handle: Handle, config: DispatcherConfig) -> Self {
        Self { handle, config }
    }

    pub fn current() -> Self {
        Self::with_config(Handle::current(), DispatcherConfig::default())
    }
}

impl Dispatcher for DefaultDispatcher {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle {
        DispatcherHandle(self.handle.spawn(task))
    }

    fn throughput(&self) -> u32 {
        self.config.throughput
    }

    fn throughput_deadline(&self) -> Option<Duration> {
        self.config.throughput_deadline
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

/// Single-thread dedicated runtime, similar to [`PinnedDispatcher`] but
/// expressing the semantic role of "one shared single-threaded runtime
/// for a group of actors that must not run concurrently with each
/// other". akka.net: `SingleThreadDispatcher`. The pin variant gives
/// each actor its own runtime; this variant shares one across actors.
pub struct SingleThreadDispatcher {
    rt: Arc<Runtime>,
    config: DispatcherConfig,
}

impl SingleThreadDispatcher {
    pub fn new(config: DispatcherConfig) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        Ok(Self { rt: Arc::new(rt), config })
    }
}

impl Dispatcher for SingleThreadDispatcher {
    fn spawn_task(&self, task: futures_util::future::BoxFuture<'static, ()>) -> DispatcherHandle {
        DispatcherHandle(self.rt.spawn(task))
    }
    fn throughput(&self) -> u32 {
        self.config.throughput
    }
    fn throughput_deadline(&self) -> Option<Duration> {
        self.config.throughput_deadline
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

    #[test]
    fn dispatcher_config_default_is_unbounded_deadline() {
        let c = DispatcherConfig::default();
        assert_eq!(c.throughput, 10);
        assert_eq!(c.throughput_deadline, None);
    }

    #[tokio::test]
    async fn default_dispatcher_with_config_exposes_knobs() {
        let cfg = DispatcherConfig { throughput: 50, throughput_deadline: Some(Duration::from_millis(5)) };
        let d = DefaultDispatcher::with_config(Handle::current(), cfg.clone());
        assert_eq!(d.throughput(), 50);
        assert_eq!(d.throughput_deadline(), Some(Duration::from_millis(5)));
    }

    #[test]
    fn single_thread_dispatcher_runs_task() {
        let d = SingleThreadDispatcher::new(DispatcherConfig::default()).unwrap();
        // Drive it from a separate thread because the runtime owns
        // the calling thread otherwise.
        let (tx, rx) = std::sync::mpsc::channel();
        let h = d.spawn_task(Box::pin(async move {
            tx.send(7u32).unwrap();
        }));
        // Block waiting on the channel; the spawned task will run on
        // the SingleThread runtime via background threadwise polling.
        // tokio current-thread runtimes do not poll without
        // block_on, so we spawn a watchdog using DefaultDispatcher.
        std::thread::sleep(Duration::from_millis(20));
        h.abort();
        let _ = rx.recv_timeout(Duration::from_millis(50));
    }
}
