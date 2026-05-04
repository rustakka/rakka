//! Bounded outbound send-queue with `OverflowStrategy`. Phase 5.G of
//! `docs/full-port-plan.md`.
//!
//! The endpoint writer used to drain an unbounded mpsc which silently
//! grew under sustained back-pressure. This module wraps that channel in
//! a small VecDeque-backed queue with a bounded capacity and a
//! configurable [`SendQueueOverflow`] policy.

use std::collections::VecDeque;

use crate::error::{RemoteError, RemoteErrorKind};
use crate::settings::SendQueueOverflow;

/// Result of a `try_push`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// Message accepted; queue length is the returned value.
    Enqueued(usize),
    /// Overflow handled by dropping the new message.
    DroppedNew,
    /// Overflow handled by dropping an older message; queue still len capacity.
    DroppedOld,
}

/// Bounded send queue for outbound endpoint envelopes (typed via `T`).
pub struct BoundedSendQueue<T> {
    inner: VecDeque<T>,
    capacity: usize,
    policy: SendQueueOverflow,
}

impl<T> BoundedSendQueue<T> {
    pub fn new(capacity: usize, policy: SendQueueOverflow) -> Self {
        Self { inner: VecDeque::with_capacity(capacity), capacity: capacity.max(1), policy }
    }

    /// Try to enqueue `item`. Honours the configured overflow policy.
    pub fn try_push(&mut self, item: T) -> Result<SendOutcome, RemoteError> {
        if self.inner.len() < self.capacity {
            self.inner.push_back(item);
            return Ok(SendOutcome::Enqueued(self.inner.len()));
        }
        match self.policy {
            SendQueueOverflow::DropNew => Ok(SendOutcome::DroppedNew),
            SendQueueOverflow::DropOld => {
                let _ = self.inner.pop_front();
                self.inner.push_back(item);
                Ok(SendOutcome::DroppedOld)
            }
            SendQueueOverflow::Fail => {
                Err(RemoteError::new(RemoteErrorKind::BackPressure, "send queue full"))
            }
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop_front()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_until_full() {
        let mut q = BoundedSendQueue::<u32>::new(2, SendQueueOverflow::Fail);
        assert!(matches!(q.try_push(1), Ok(SendOutcome::Enqueued(1))));
        assert!(matches!(q.try_push(2), Ok(SendOutcome::Enqueued(2))));
        assert!(q.try_push(3).is_err());
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn drop_new_keeps_oldest() {
        let mut q = BoundedSendQueue::<u32>::new(1, SendQueueOverflow::DropNew);
        q.try_push(1).unwrap();
        assert_eq!(q.try_push(2).unwrap(), SendOutcome::DroppedNew);
        assert_eq!(q.pop(), Some(1));
    }

    #[test]
    fn drop_old_evicts_oldest() {
        let mut q = BoundedSendQueue::<u32>::new(1, SendQueueOverflow::DropOld);
        q.try_push(1).unwrap();
        assert_eq!(q.try_push(2).unwrap(), SendOutcome::DroppedOld);
        assert_eq!(q.pop(), Some(2));
    }

    #[test]
    fn capacity_floor_is_one() {
        let mut q = BoundedSendQueue::<u32>::new(0, SendQueueOverflow::DropNew);
        assert!(matches!(q.try_push(1), Ok(SendOutcome::Enqueued(1))));
        assert_eq!(q.try_push(2).unwrap(), SendOutcome::DroppedNew);
    }
}
