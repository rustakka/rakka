//! Tail-chopping router. akka.net: `Routing/TailChoppingPool.cs`.
//!
//! Phase 3.3 of `docs/full-port-plan.md`. Akka.NET semantics:
//! a request is sent to a randomly-picked routee; if no reply
//! arrives within `interval`, a second routee is tried; and so on
//! until either a reply arrives or `within` is exceeded. Useful for
//! latency-sensitive workloads where a stuck or slow node would
//! otherwise tail-latency the whole call.
//!
//! The actual reply path is the caller's responsibility (akka.net
//! relies on the `Ask` machinery to pick a winner); this router
//! exposes the per-attempt fan-out + interval policy as a typed
//! schedule.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::actor::ActorRef;

pub struct TailChoppingRouter<M: Send + Clone + 'static> {
    routees: Vec<ActorRef<M>>,
    cursor: AtomicUsize,
    /// How long to wait for the previous attempt's reply before
    /// firing the next. A `Duration::ZERO` interval makes this
    /// equivalent to scatter-gather.
    pub interval: Duration,
    /// Hard ceiling on how long the caller will wait overall.
    pub within: Duration,
}

impl<M: Send + Clone + 'static> TailChoppingRouter<M> {
    pub fn new(routees: Vec<ActorRef<M>>, interval: Duration, within: Duration) -> Self {
        Self { routees, cursor: AtomicUsize::new(0), interval, within }
    }

    /// Number of routees currently registered.
    pub fn routee_count(&self) -> usize {
        self.routees.len()
    }

    /// Pick the next attempt's recipient; returns `None` when the
    /// router is empty.
    pub fn next_attempt(&self) -> Option<&ActorRef<M>> {
        if self.routees.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.routees.len();
        Some(&self.routees[idx])
    }

    /// Maximum number of distinct attempts within `within`.
    pub fn max_attempts(&self) -> usize {
        if self.interval.is_zero() {
            self.routees.len()
        } else {
            // ceil(within / interval) capped at routee count.
            let nanos = self.within.as_nanos();
            let step = self.interval.as_nanos().max(1);
            nanos.div_ceil(step) as usize
        }
        .min(self.routees.len().max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Inbox;

    #[test]
    fn next_attempt_round_robins() {
        let r1 = Inbox::<u32>::new("a").actor_ref().clone();
        let r2 = Inbox::<u32>::new("b").actor_ref().clone();
        let r3 = Inbox::<u32>::new("c").actor_ref().clone();
        let router = TailChoppingRouter::new(
            vec![r1.clone(), r2.clone(), r3.clone()],
            Duration::from_millis(10),
            Duration::from_millis(50),
        );
        let p1 = router.next_attempt().unwrap().path().clone();
        let p2 = router.next_attempt().unwrap().path().clone();
        let p3 = router.next_attempt().unwrap().path().clone();
        let p4 = router.next_attempt().unwrap().path().clone();
        assert_ne!(p1, p2);
        assert_ne!(p2, p3);
        assert_eq!(p1, p4); // wrap-around
    }

    #[test]
    fn empty_router_has_no_next_attempt() {
        let router =
            TailChoppingRouter::<u32>::new(Vec::new(), Duration::from_millis(10), Duration::from_millis(50));
        assert!(router.next_attempt().is_none());
        assert_eq!(router.routee_count(), 0);
    }

    #[test]
    fn max_attempts_respects_interval_and_within() {
        let r = Inbox::<u32>::new("x").actor_ref().clone();
        let routees = vec![r.clone(); 10];
        // 100ms / 20ms = 5 attempts, capped at routee count (10).
        let router = TailChoppingRouter::new(routees, Duration::from_millis(20), Duration::from_millis(100));
        assert_eq!(router.max_attempts(), 5);
    }

    #[test]
    fn zero_interval_is_scatter_gather() {
        let r = Inbox::<u32>::new("x").actor_ref().clone();
        let routees = vec![r; 4];
        let router = TailChoppingRouter::new(routees, Duration::ZERO, Duration::from_millis(50));
        assert_eq!(router.max_attempts(), 4);
    }
}
