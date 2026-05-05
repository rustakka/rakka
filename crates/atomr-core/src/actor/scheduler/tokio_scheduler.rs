//! Simple Tokio-backed scheduler. Not as efficient as a hashed wheel at
//! millions of timers, but perfect for typical actor workloads.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;

use super::{Scheduler, SchedulerHandle};

#[derive(Default)]
pub struct TokioScheduler;

impl TokioScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Scheduler for TokioScheduler {
    fn schedule_once(&self, delay: Duration, task: BoxFuture<'static, ()>) -> SchedulerHandle {
        let cancel = Arc::new(AtomicBool::new(false));
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if !c.load(Ordering::Acquire) {
                task.await;
            }
        });
        SchedulerHandle { cancel }
    }

    fn schedule_at_fixed_rate(
        &self,
        initial_delay: Duration,
        interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> SchedulerHandle {
        let cancel = Arc::new(AtomicBool::new(false));
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(initial_delay).await;
            let mut tick = tokio::time::interval(interval);
            // first tick fires immediately — skip it since we already slept
            tick.tick().await;
            loop {
                if c.load(Ordering::Acquire) {
                    break;
                }
                task();
                tick.tick().await;
            }
        });
        SchedulerHandle { cancel }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test(start_paused = true)]
    async fn schedule_once_runs_once() {
        let s = TokioScheduler::new();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = s.schedule_once(
            Duration::from_millis(10),
            Box::pin(async move {
                tx.send(()).unwrap();
            }),
        );
        tokio::time::advance(Duration::from_millis(11)).await;
        rx.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn schedule_repeat_fires_multiple() {
        let s = TokioScheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let h = s.schedule_at_fixed_rate(
            Duration::from_millis(0),
            Duration::from_millis(10),
            Arc::new(move || {
                c.fetch_add(1, Ordering::Relaxed);
            }),
        );
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
        }
        h.cancel();
        assert!(counter.load(Ordering::Relaxed) >= 3);
    }
}
