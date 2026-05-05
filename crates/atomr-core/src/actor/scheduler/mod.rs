//! Scheduler — timers backing `schedule_once` / `schedule_at_fixed_rate`.

mod hashed_wheel;
mod tokio_scheduler;

pub use hashed_wheel::HashedWheelTimerScheduler;
pub use tokio_scheduler::TokioScheduler;

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;

/// A handle that can cancel a pending scheduled action.
pub struct SchedulerHandle {
    pub(crate) cancel: Arc<std::sync::atomic::AtomicBool>,
}

impl SchedulerHandle {
    pub fn cancel(&self) {
        self.cancel.store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// The scheduler trait.
pub trait Scheduler: Send + Sync {
    fn schedule_once(&self, delay: Duration, task: BoxFuture<'static, ()>) -> SchedulerHandle;

    fn schedule_at_fixed_rate(
        &self,
        initial_delay: Duration,
        interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> SchedulerHandle;
}
