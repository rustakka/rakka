//! Stash — buffers messages that should be deferred until `unstash_all`.
//! akka.net: `Actor/Stash/*`.
//!
//! Phase 3.6 of `docs/full-port-plan.md`. Two layers:
//!
//! * The unbounded stash storage on [`crate::actor::Context`] is the
//!   default and matches the legacy API.
//! * [`BoundedStash`] is a free-standing bounded buffer with a
//!   pluggable [`StashOverflow`] policy. Actor authors can hold one
//!   per actor instance for back-pressure-aware stashing.
//!
//! `Stash` (marker trait) stays for symmetry with akka.net's
//! `IWithStash`.

use std::collections::VecDeque;

/// Marker — any actor may opt in to document stash usage.
/// Stash storage itself is provided unconditionally by `Context`.
pub trait Stash {}

/// What to do when the stash is full and a new message arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StashOverflow {
    /// Drop the oldest stashed message; queue the new one.
    DropOldest,
    /// Drop the new message.
    DropNewest,
    /// Drop the new message AND surface it to the caller as an error
    /// (so the runtime can route it to DeadLetters).
    Reject,
}

/// Result of [`BoundedStash::stash`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StashResult<M> {
    /// Stashed successfully; depth is the new buffer length.
    Stashed { depth: usize },
    /// `Reject` policy refused the message; caller must route it
    /// (e.g. publish on DeadLetters).
    Rejected(M),
    /// `DropOldest` displaced an existing message; caller may want
    /// to surface it on DeadLetters.
    DroppedOldest(M),
    /// `DropNewest` discarded the incoming message — depth unchanged.
    DroppedNewest,
}

/// Bounded stash with a configurable overflow policy.
pub struct BoundedStash<M> {
    capacity: usize,
    policy: StashOverflow,
    buf: VecDeque<M>,
}

impl<M> BoundedStash<M> {
    pub fn new(capacity: usize, policy: StashOverflow) -> Self {
        assert!(capacity >= 1, "capacity must be >= 1");
        Self { capacity, policy, buf: VecDeque::with_capacity(capacity) }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.buf.len() >= self.capacity
    }

    /// Stash `msg`, applying the configured overflow policy if
    /// the buffer is full.
    pub fn stash(&mut self, msg: M) -> StashResult<M> {
        if !self.is_full() {
            self.buf.push_back(msg);
            return StashResult::Stashed { depth: self.buf.len() };
        }
        match self.policy {
            StashOverflow::DropOldest => {
                let dropped = self.buf.pop_front();
                self.buf.push_back(msg);
                match dropped {
                    Some(old) => StashResult::DroppedOldest(old),
                    None => StashResult::Stashed { depth: self.buf.len() },
                }
            }
            StashOverflow::DropNewest => StashResult::DroppedNewest,
            StashOverflow::Reject => StashResult::Rejected(msg),
        }
    }

    /// Drain the stash front-to-back. Maintains akka.net's
    /// "messages prepended in order" semantic — caller front-prepends
    /// to the mailbox.
    pub fn unstash_all(&mut self) -> Vec<M> {
        let mut out = Vec::with_capacity(self.buf.len());
        while let Some(m) = self.buf.pop_front() {
            out.push(m);
        }
        out
    }

    /// Pop a single stashed message (oldest first).
    pub fn pop(&mut self) -> Option<M> {
        self.buf.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stash_until_capacity() {
        let mut s = BoundedStash::<u32>::new(3, StashOverflow::Reject);
        assert!(matches!(s.stash(1), StashResult::Stashed { depth: 1 }));
        assert!(matches!(s.stash(2), StashResult::Stashed { depth: 2 }));
        assert!(matches!(s.stash(3), StashResult::Stashed { depth: 3 }));
        assert!(s.is_full());
    }

    #[test]
    fn reject_returns_message_back() {
        let mut s = BoundedStash::<u32>::new(1, StashOverflow::Reject);
        s.stash(1);
        let r = s.stash(99);
        assert_eq!(r, StashResult::Rejected(99));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn drop_oldest_displaces() {
        let mut s = BoundedStash::<u32>::new(2, StashOverflow::DropOldest);
        s.stash(1);
        s.stash(2);
        let r = s.stash(3);
        assert_eq!(r, StashResult::DroppedOldest(1));
        let drained = s.unstash_all();
        assert_eq!(drained, vec![2, 3]);
    }

    #[test]
    fn drop_newest_keeps_old() {
        let mut s = BoundedStash::<u32>::new(2, StashOverflow::DropNewest);
        s.stash(1);
        s.stash(2);
        let r = s.stash(3);
        assert_eq!(r, StashResult::DroppedNewest);
        let drained = s.unstash_all();
        assert_eq!(drained, vec![1, 2]);
    }

    #[test]
    fn unstash_all_drains_in_order() {
        let mut s = BoundedStash::<u32>::new(4, StashOverflow::Reject);
        for i in 1..=4 {
            s.stash(i);
        }
        let drained = s.unstash_all();
        assert_eq!(drained, vec![1, 2, 3, 4]);
        assert!(s.is_empty());
    }

    #[test]
    #[should_panic]
    fn zero_capacity_panics() {
        let _: BoundedStash<u32> = BoundedStash::new(0, StashOverflow::Reject);
    }
}
