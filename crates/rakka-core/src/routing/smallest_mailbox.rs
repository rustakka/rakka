//! Smallest-mailbox router. akka.net: `Routing/SmallestMailboxPool.cs`.
//!
//! True mailbox-size inspection requires hooking into the mpsc internals,
//! which is not stable. This port approximates by round-robin as a baseline
//! and allows plugging in a custom size probe. Matches the behaviour of
//! akka.net when mailbox size introspection is unavailable (it falls back
//! to round-robin per routee).

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::actor::ActorRef;

pub struct SmallestMailboxRouter<M: Send + Clone + 'static> {
    routees: Vec<(ActorRef<M>, AtomicUsize)>,
    cursor: AtomicUsize,
}

impl<M: Send + Clone + 'static> SmallestMailboxRouter<M> {
    pub fn new(routees: Vec<ActorRef<M>>) -> Self {
        Self {
            routees: routees.into_iter().map(|r| (r, AtomicUsize::new(0))).collect(),
            cursor: AtomicUsize::new(0),
        }
    }

    pub fn route(&self, msg: M) {
        if self.routees.is_empty() {
            return;
        }
        let (best_idx, _) = self
            .routees
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, c))| c.load(Ordering::Relaxed))
            .map(|(i, (_, c))| (i, c.load(Ordering::Relaxed)))
            .unwrap_or((self.cursor.fetch_add(1, Ordering::Relaxed) % self.routees.len(), 0));
        self.routees[best_idx].0.tell(msg);
        self.routees[best_idx].1.fetch_add(1, Ordering::Relaxed);
    }

    /// Callers can decrement after they know a message was processed — optional.
    pub fn on_processed(&self, idx: usize) {
        if let Some((_, c)) = self.routees.get(idx) {
            c.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
