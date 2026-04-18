//! Hashed wheel timer — port of akka.net's `HashedWheelTimerScheduler.cs`.
//!
//! A hashed wheel is a cyclic array of "buckets"; scheduling a timeout
//! places the deadline in the bucket that corresponds to
//! `(now + delay) / tick_duration`. A background task ticks through the
//! buckets and fires anything whose "rounds remaining" has reached zero.
//!
//! This port exposes the same `Scheduler` trait as the simple
//! [`super::TokioScheduler`]; users can pick via
//! `akka.scheduler.implementation`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use parking_lot::Mutex;

use super::{Scheduler, SchedulerHandle};

type Callback = Box<dyn FnOnce() + Send>;

struct Slot {
    items: VecDeque<Entry>,
}

struct Entry {
    rounds: u64,
    cancel: Arc<AtomicBool>,
    cb: Callback,
}

pub struct HashedWheelTimerScheduler {
    inner: Arc<Inner>,
}

struct Inner {
    tick: Duration,
    mask: usize,
    slots: Mutex<Vec<Slot>>,
    cursor: Mutex<usize>,
    shutdown: AtomicBool,
}

impl HashedWheelTimerScheduler {
    pub fn new(tick: Duration, ticks_per_wheel: usize) -> Self {
        assert!(ticks_per_wheel.is_power_of_two(), "ticks_per_wheel must be power of two");
        let slots = (0..ticks_per_wheel).map(|_| Slot { items: VecDeque::new() }).collect();
        let inner = Arc::new(Inner {
            tick,
            mask: ticks_per_wheel - 1,
            slots: Mutex::new(slots),
            cursor: Mutex::new(0),
            shutdown: AtomicBool::new(false),
        });
        let i2 = inner.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tick);
            loop {
                ticker.tick().await;
                if i2.shutdown.load(Ordering::Acquire) {
                    break;
                }
                i2.tick();
            }
        });
        Self { inner }
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
    }

    fn schedule(&self, delay: Duration, cancel: Arc<AtomicBool>, cb: Callback) {
        let ticks = (delay.as_nanos() / self.inner.tick.as_nanos().max(1)) as u64;
        let slots_len = (self.inner.mask + 1) as u64;
        let rounds = ticks / slots_len;
        let offset = (ticks % slots_len) as usize;
        let mut cursor = self.inner.cursor.lock();
        let idx = (*cursor + offset) & self.inner.mask;
        drop(cursor);
        let mut slots = self.inner.slots.lock();
        slots[idx].items.push_back(Entry { rounds, cancel, cb });
    }
}

impl Inner {
    fn tick(&self) {
        let mut cursor = self.cursor.lock();
        let idx = *cursor & self.mask;
        *cursor = cursor.wrapping_add(1);
        drop(cursor);

        let mut due: Vec<Callback> = Vec::new();
        {
            let mut slots = self.slots.lock();
            let slot = &mut slots[idx];
            let mut kept: VecDeque<Entry> = VecDeque::with_capacity(slot.items.len());
            while let Some(mut e) = slot.items.pop_front() {
                if e.cancel.load(Ordering::Acquire) {
                    continue;
                }
                if e.rounds == 0 {
                    due.push(e.cb);
                } else {
                    e.rounds -= 1;
                    kept.push_back(e);
                }
            }
            slot.items = kept;
        }
        for cb in due {
            cb();
        }
    }
}

impl Scheduler for HashedWheelTimerScheduler {
    fn schedule_once(&self, delay: Duration, task: BoxFuture<'static, ()>) -> SchedulerHandle {
        let cancel = Arc::new(AtomicBool::new(false));
        let c = cancel.clone();
        let cb: Callback = Box::new(move || {
            tokio::spawn(task);
        });
        self.schedule(delay, c, cb);
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
        // Delegate recurring timers to Tokio for simplicity: a full wheel
        // implementation would self-reschedule inside the fired callback.
        let t = task.clone();
        tokio::spawn(async move {
            tokio::time::sleep(initial_delay).await;
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                if c.load(Ordering::Acquire) {
                    break;
                }
                t();
                tick.tick().await;
            }
        });
        SchedulerHandle { cancel }
    }
}

impl Drop for HashedWheelTimerScheduler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fires_after_delay() {
        let s = HashedWheelTimerScheduler::new(Duration::from_millis(2), 64);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = s.schedule_once(
            Duration::from_millis(10),
            Box::pin(async move {
                let _ = tx.send(());
            }),
        );
        tokio::time::timeout(Duration::from_millis(200), rx).await.expect("timer fired").unwrap();
    }
}
