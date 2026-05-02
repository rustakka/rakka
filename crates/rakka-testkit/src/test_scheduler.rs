//! `TestScheduler` — virtual-time scheduler for deterministic tests.
//!
//! Phase 4 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.TestKit.TestScheduler`. Differs in API shape because we
//! lean on Tokio's `time::pause` for suspension and provide a
//! lightweight `advance_by`/`advance_to` helper that drives both
//! Tokio's clock and a list of registered callbacks.
//!
//! Typical pattern:
//!
//! ```no_run
//! # use std::time::Duration;
//! # use rakka_testkit::TestScheduler;
//! # async fn ex() {
//! let mut sched = TestScheduler::new();
//! let token = sched.schedule_after(Duration::from_secs(60), || println!("fired"));
//! // No real time elapses; callback runs once we advance.
//! sched.advance_by(Duration::from_secs(60)).await;
//! assert!(sched.fired(token));
//! # }
//! ```

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Callback = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ScheduledToken(u64);

struct Entry {
    fire_at: Instant,
    cb: Option<Callback>,
    fired: bool,
    cancelled: bool,
}

struct Inner {
    now: Instant,
    next_token: u64,
    entries: Vec<(ScheduledToken, Entry)>,
}

/// Virtual-time scheduler. Time only advances when [`advance_by`] /
/// [`advance_to`] is called.
///
/// [`advance_by`]: TestScheduler::advance_by
/// [`advance_to`]: TestScheduler::advance_to
#[derive(Clone)]
pub struct TestScheduler {
    inner: Arc<Mutex<Inner>>,
}

impl Default for TestScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl TestScheduler {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner { now: Instant::now(), next_token: 0, entries: Vec::new() })),
        }
    }

    /// Current virtual time.
    pub fn now(&self) -> Instant {
        self.inner.lock().unwrap().now
    }

    /// Schedule `cb` to fire `delay` after the current virtual time.
    pub fn schedule_after<F>(&self, delay: Duration, cb: F) -> ScheduledToken
    where
        F: FnOnce() + Send + 'static,
    {
        let mut g = self.inner.lock().unwrap();
        let token = ScheduledToken(g.next_token);
        g.next_token += 1;
        let fire_at = g.now + delay;
        g.entries.push((token, Entry { fire_at, cb: Some(Box::new(cb)), fired: false, cancelled: false }));
        token
    }

    /// Cancel a scheduled callback if it hasn't fired yet.
    pub fn cancel(&self, token: ScheduledToken) -> bool {
        let mut g = self.inner.lock().unwrap();
        for (tok, entry) in g.entries.iter_mut() {
            if *tok == token && !entry.fired {
                entry.cancelled = true;
                return true;
            }
        }
        false
    }

    /// Advance virtual time by `delta`, firing all callbacks whose
    /// fire-at falls in the new range. Callbacks fire in fire-at order.
    pub async fn advance_by(&self, delta: Duration) {
        let target = {
            let g = self.inner.lock().unwrap();
            g.now + delta
        };
        self.advance_to(target).await;
    }

    /// Advance virtual time to `target` (must be ≥ current time).
    pub async fn advance_to(&self, target: Instant) {
        loop {
            // Find the next due entry.
            let next = {
                let g = self.inner.lock().unwrap();
                let mut due: Vec<(usize, Instant)> = g
                    .entries
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, e))| !e.fired && !e.cancelled && e.fire_at <= target)
                    .map(|(i, (_, e))| (i, e.fire_at))
                    .collect();
                due.sort_by_key(|(_, t)| *t);
                due.first().copied()
            };
            match next {
                Some((idx, t)) => {
                    let cb = {
                        let mut g = self.inner.lock().unwrap();
                        g.now = t;
                        let entry = &mut g.entries[idx].1;
                        entry.fired = true;
                        entry.cb.take()
                    };
                    if let Some(cb) = cb {
                        cb();
                    }
                    // Yield so any spawned tasks can observe the call.
                    tokio::task::yield_now().await;
                }
                None => {
                    let mut g = self.inner.lock().unwrap();
                    if g.now < target {
                        g.now = target;
                    }
                    return;
                }
            }
        }
    }

    /// Has the scheduled callback fired?
    pub fn fired(&self, token: ScheduledToken) -> bool {
        self.inner.lock().unwrap().entries.iter().any(|(t, e)| *t == token && e.fired)
    }

    /// How many scheduled entries are still pending (not fired,
    /// not cancelled)?
    pub fn pending(&self) -> usize {
        self.inner.lock().unwrap().entries.iter().filter(|(_, e)| !e.fired && !e.cancelled).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn fires_after_advance() {
        let s = TestScheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c2 = counter.clone();
        let token = s.schedule_after(Duration::from_secs(5), move || {
            c2.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        s.advance_by(Duration::from_secs(5)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(s.fired(token));
        assert_eq!(s.pending(), 0);
    }

    #[tokio::test]
    async fn does_not_fire_before_delay() {
        let s = TestScheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c2 = counter.clone();
        let _ = s.schedule_after(Duration::from_secs(10), move || {
            c2.fetch_add(1, Ordering::SeqCst);
        });
        s.advance_by(Duration::from_secs(9)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(s.pending(), 1);
    }

    #[tokio::test]
    async fn cancel_prevents_fire() {
        let s = TestScheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c2 = counter.clone();
        let t = s.schedule_after(Duration::from_secs(1), move || {
            c2.fetch_add(1, Ordering::SeqCst);
        });
        assert!(s.cancel(t));
        s.advance_by(Duration::from_secs(2)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert!(!s.fired(t));
    }

    #[tokio::test]
    async fn fires_in_order() {
        let s = TestScheduler::new();
        let order = Arc::new(Mutex::new(Vec::<u32>::new()));
        for (delay, id) in [(3u64, 1u32), (1, 2), (2, 3)] {
            let order = order.clone();
            s.schedule_after(Duration::from_secs(delay), move || {
                order.lock().unwrap().push(id);
            });
        }
        s.advance_by(Duration::from_secs(5)).await;
        assert_eq!(*order.lock().unwrap(), vec![2, 3, 1]);
    }
}
