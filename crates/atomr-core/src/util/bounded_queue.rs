//! A simple bounded FIFO queue used by bounded mailboxes.
//! akka.net: `Util/BoundedQueue.cs`.

use std::collections::VecDeque;

#[derive(Debug)]
pub struct BoundedQueue<T> {
    cap: usize,
    inner: VecDeque<T>,
}

impl<T> BoundedQueue<T> {
    pub fn new(cap: usize) -> Self {
        Self { cap, inner: VecDeque::with_capacity(cap) }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.inner.len() >= self.cap
    }

    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.is_full() {
            Err(item)
        } else {
            self.inner.push_back(item);
            Ok(())
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop_front()
    }

    /// Remove and return the most-recently-enqueued element, if any.
    /// Used by [`OverflowStrategy::DropTail`].
    pub fn pop_back(&mut self) -> Option<T> {
        self.inner.pop_back()
    }

    pub fn push_front(&mut self, item: T) -> Result<(), T> {
        if self.is_full() {
            Err(item)
        } else {
            self.inner.push_front(item);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_order() {
        let mut q = BoundedQueue::new(3);
        q.push(1).unwrap();
        q.push(2).unwrap();
        q.push(3).unwrap();
        assert!(q.is_full());
        assert!(q.push(4).is_err());
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
        assert!(q.is_empty());
    }
}
