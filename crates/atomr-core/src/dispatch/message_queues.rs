//! Message queue implementations.
//!
//! These are in-memory data structures used by the mailbox. They are
//! `!Send` outside their owning `ActorCell` — all external sending goes
//! through the typed channel held in [`crate::actor::ActorRef`].

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};

use crate::dispatch::mailbox::OverflowStrategy;
use crate::util::BoundedQueue;

/// Envelope trait used by priority queues.
pub trait Prioritized {
    fn priority(&self) -> i32;
}

/// Unbounded FIFO queue.
#[derive(Debug, Default)]
pub struct UnboundedQueue<T> {
    inner: VecDeque<T>,
}

impl<T> UnboundedQueue<T> {
    pub fn new() -> Self {
        Self { inner: VecDeque::new() }
    }

    pub fn push(&mut self, msg: T) {
        self.inner.push_back(msg);
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
}

/// Outcome of a bounded `push` once an [`OverflowStrategy`] has been
/// applied. `Accepted` means the message was enqueued (possibly after
/// dropping another); `Rejected(msg)` means the configured strategy
/// refused the push and returns the original message.
#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome<T> {
    Accepted,
    Dropped { dropped: T },
    Rejected(T),
}

/// Bounded FIFO queue.
#[derive(Debug)]
pub struct BoundedMsgQueue<T> {
    inner: BoundedQueue<T>,
    overflow: OverflowStrategy,
}

impl<T> BoundedMsgQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self::with_overflow(capacity, OverflowStrategy::Fail)
    }

    pub fn with_overflow(capacity: usize, overflow: OverflowStrategy) -> Self {
        Self { inner: BoundedQueue::new(capacity), overflow }
    }

    /// Legacy `push` that mirrors the original signature: returns the
    /// original message if the queue is full. Equivalent to using
    /// [`OverflowStrategy::Fail`].
    pub fn push(&mut self, msg: T) -> Result<(), T> {
        match self.push_with_strategy(msg) {
            PushOutcome::Accepted => Ok(()),
            PushOutcome::Dropped { dropped } => Err(dropped),
            PushOutcome::Rejected(msg) => Err(msg),
        }
    }

    /// Push with the configured overflow strategy applied. Returns
    /// [`PushOutcome::Accepted`] if the message was enqueued (possibly
    /// after dropping another), [`PushOutcome::Dropped`] giving back the
    /// dropped message when DropHead/DropTail kicked in, or
    /// [`PushOutcome::Rejected`] when DropNew/Fail refused the push.
    pub fn push_with_strategy(&mut self, msg: T) -> PushOutcome<T> {
        if !self.inner.is_full() {
            return match self.inner.push(msg) {
                Ok(()) => PushOutcome::Accepted,
                Err(m) => PushOutcome::Rejected(m),
            };
        }
        match self.overflow {
            OverflowStrategy::Fail | OverflowStrategy::DropNew => PushOutcome::Rejected(msg),
            OverflowStrategy::DropHead => match self.inner.pop() {
                Some(dropped) => match self.inner.push(msg) {
                    Ok(()) => PushOutcome::Dropped { dropped },
                    Err(m) => PushOutcome::Rejected(m),
                },
                None => PushOutcome::Rejected(msg),
            },
            OverflowStrategy::DropTail => match self.inner.pop_back() {
                Some(dropped) => match self.inner.push(msg) {
                    Ok(()) => PushOutcome::Dropped { dropped },
                    Err(m) => PushOutcome::Rejected(m),
                },
                None => PushOutcome::Rejected(msg),
            },
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop()
    }

    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    pub fn overflow(&self) -> OverflowStrategy {
        self.overflow
    }
}

/// Control-aware queue. Control messages are drained before user
/// messages regardless of insertion order.
/// `UnboundedControlAwareMessageQueue`. Use the typed wrapper
/// [`ControlAware::Control`] / [`ControlAware::User`] to tag a message.
#[derive(Debug)]
pub enum ControlAware<T> {
    Control(T),
    User(T),
}

#[derive(Debug, Default)]
pub struct ControlAwareQueue<T> {
    control: VecDeque<T>,
    user: VecDeque<T>,
}

impl<T> ControlAwareQueue<T> {
    pub fn new() -> Self {
        Self { control: VecDeque::new(), user: VecDeque::new() }
    }

    pub fn push(&mut self, msg: ControlAware<T>) {
        match msg {
            ControlAware::Control(m) => self.control.push_back(m),
            ControlAware::User(m) => self.user.push_back(m),
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        self.control.pop_front().or_else(|| self.user.pop_front())
    }

    pub fn len(&self) -> usize {
        self.control.len() + self.user.len()
    }

    pub fn is_empty(&self) -> bool {
        self.control.is_empty() && self.user.is_empty()
    }
}

/// Deque-like queue permitting front insertion (for stash/unstash).
#[derive(Debug)]
pub struct DequeQueue<T> {
    inner: VecDeque<T>,
}

impl<T> Default for DequeQueue<T> {
    fn default() -> Self {
        Self { inner: VecDeque::new() }
    }
}

impl<T> DequeQueue<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_back(&mut self, msg: T) {
        self.inner.push_back(msg);
    }

    pub fn push_front(&mut self, msg: T) {
        self.inner.push_front(msg);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop_front()
    }
}

/// Priority queue.
///
/// `T` must implement [`Prioritized`].
pub struct PriorityQueue<T: Prioritized> {
    heap: BinaryHeap<PriItem<T>>,
}

impl<T: Prioritized> std::fmt::Debug for PriorityQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PriorityQueue").field("len", &self.heap.len()).finish()
    }
}

impl<T: Prioritized> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self { heap: BinaryHeap::new() }
    }
}

impl<T: Prioritized> PriorityQueue<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, msg: T) {
        let p = msg.priority();
        self.heap.push(PriItem { prio: p, inner: msg });
    }

    pub fn pop(&mut self) -> Option<T> {
        self.heap.pop().map(|i| i.inner)
    }
}

struct PriItem<T: Prioritized> {
    prio: i32,
    inner: T,
}

impl<T: Prioritized> PartialEq for PriItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.prio == other.prio
    }
}
impl<T: Prioritized> Eq for PriItem<T> {}
impl<T: Prioritized> PartialOrd for PriItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Prioritized> Ord for PriItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.prio.cmp(&other.prio)
    }
}

/// Stable priority queue (FIFO among equal priorities).
pub struct StablePriorityQueue<T: Prioritized> {
    heap: BinaryHeap<StableItem<T>>,
    seq: u64,
}

impl<T: Prioritized> std::fmt::Debug for StablePriorityQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StablePriorityQueue").field("len", &self.heap.len()).finish()
    }
}

impl<T: Prioritized> Default for StablePriorityQueue<T> {
    fn default() -> Self {
        Self { heap: BinaryHeap::new(), seq: 0 }
    }
}

impl<T: Prioritized> StablePriorityQueue<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, msg: T) {
        let p = msg.priority();
        let s = self.seq;
        self.seq = self.seq.wrapping_add(1);
        self.heap.push(StableItem { prio: p, seq: s, inner: msg });
    }

    pub fn pop(&mut self) -> Option<T> {
        self.heap.pop().map(|i| i.inner)
    }
}

struct StableItem<T: Prioritized> {
    prio: i32,
    seq: u64,
    inner: T,
}

impl<T: Prioritized> PartialEq for StableItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.prio == other.prio && self.seq == other.seq
    }
}
impl<T: Prioritized> Eq for StableItem<T> {}
impl<T: Prioritized> PartialOrd for StableItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Prioritized> Ord for StableItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.prio.cmp(&other.prio).then_with(|| other.seq.cmp(&self.seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct M(i32);
    impl Prioritized for M {
        fn priority(&self) -> i32 {
            self.0
        }
    }

    #[test]
    fn unbounded_fifo() {
        let mut q = UnboundedQueue::new();
        q.push(1);
        q.push(2);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
    }

    #[test]
    fn bounded_rejects_when_full() {
        let mut q = BoundedMsgQueue::new(1);
        q.push(1).unwrap();
        assert!(q.push(2).is_err());
    }

    #[test]
    fn bounded_drop_head_removes_oldest() {
        let mut q = BoundedMsgQueue::with_overflow(2, OverflowStrategy::DropHead);
        assert_eq!(q.push_with_strategy(1), PushOutcome::Accepted);
        assert_eq!(q.push_with_strategy(2), PushOutcome::Accepted);
        assert_eq!(q.push_with_strategy(3), PushOutcome::Dropped { dropped: 1 });
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
    }

    #[test]
    fn bounded_drop_tail_removes_newest() {
        let mut q = BoundedMsgQueue::with_overflow(2, OverflowStrategy::DropTail);
        q.push_with_strategy(1);
        q.push_with_strategy(2);
        assert_eq!(q.push_with_strategy(3), PushOutcome::Dropped { dropped: 2 });
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(3));
    }

    #[test]
    fn bounded_drop_new_rejects_incoming() {
        let mut q = BoundedMsgQueue::with_overflow(1, OverflowStrategy::DropNew);
        q.push_with_strategy(1);
        assert_eq!(q.push_with_strategy(2), PushOutcome::Rejected(2));
        assert_eq!(q.pop(), Some(1));
    }

    #[test]
    fn bounded_fail_rejects_incoming() {
        let mut q = BoundedMsgQueue::with_overflow(1, OverflowStrategy::Fail);
        q.push_with_strategy(1);
        assert_eq!(q.push_with_strategy(2), PushOutcome::Rejected(2));
    }

    #[test]
    fn control_aware_drains_control_first() {
        let mut q = ControlAwareQueue::new();
        q.push(ControlAware::User(1));
        q.push(ControlAware::User(2));
        q.push(ControlAware::Control(99));
        assert_eq!(q.pop(), Some(99));
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert!(q.is_empty());
    }

    #[test]
    fn control_aware_preserves_within_class_fifo() {
        let mut q = ControlAwareQueue::new();
        q.push(ControlAware::Control(1));
        q.push(ControlAware::Control(2));
        q.push(ControlAware::User(10));
        q.push(ControlAware::User(11));
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(10));
        assert_eq!(q.pop(), Some(11));
    }

    #[test]
    fn priority_highest_first() {
        let mut q = PriorityQueue::new();
        q.push(M(1));
        q.push(M(5));
        q.push(M(3));
        assert_eq!(q.pop().unwrap().0, 5);
        assert_eq!(q.pop().unwrap().0, 3);
    }

    #[test]
    fn stable_priority_preserves_fifo_for_ties() {
        let mut q = StablePriorityQueue::new();
        q.push(M(1));
        q.push(M(2));
        q.push(M(1));
        assert_eq!(q.pop().unwrap().0, 2);
        // both remaining priorities are 1 — FIFO
        assert_eq!(q.pop().unwrap().0, 1);
        assert_eq!(q.pop().unwrap().0, 1);
    }
}
