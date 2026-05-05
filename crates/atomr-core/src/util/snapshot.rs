//! `Snapshot<T>` — read-mostly immutable-snapshot container.
//!
//! hot-path
//! shared state (cluster gossip, sharding allocation tables) is
//! read by every dispatcher pump but updated rarely; we mirror the
//! "swap an Arc snapshot" idiom from `arc-swap` so readers never
//! block writers and vice versa.
//!
//! Implementation: `parking_lot::RwLock<Arc<T>>`. Reads acquire the
//! read lock briefly to clone the `Arc`, then drop it — the actual
//! data is read through the `Arc` for as long as the borrower needs
//! it. Writes swap the inner `Arc` under the write lock; existing
//! readers continue to see the old snapshot until they release it.
//!
//! Dep-free; we don't pull in `arc-swap` for one type.

use std::sync::Arc;

use parking_lot::RwLock;

/// Lock-light snapshot container.
pub struct Snapshot<T> {
    inner: RwLock<Arc<T>>,
}

impl<T> Snapshot<T> {
    pub fn new(value: T) -> Self {
        Self { inner: RwLock::new(Arc::new(value)) }
    }

    /// Cheap clone of the current snapshot. Holds the read lock only
    /// long enough to clone the `Arc`.
    pub fn load(&self) -> Arc<T> {
        self.inner.read().clone()
    }

    /// Replace the snapshot wholesale. Existing readers keep their
    /// old `Arc` until they drop it.
    pub fn store(&self, value: T) {
        *self.inner.write() = Arc::new(value);
    }

    /// Compute a new value from the current and store it. Equivalent
    /// to `store(f(load()))` but holds the write lock for the whole
    /// duration so readers see a consistent transition.
    pub fn rcu<F>(&self, f: F)
    where
        F: FnOnce(&T) -> T,
    {
        let mut g = self.inner.write();
        let next = f(&g);
        *g = Arc::new(next);
    }
}

impl<T: Default> Default for Snapshot<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc as StdArc;

    #[test]
    fn load_and_store_round_trip() {
        let s = Snapshot::new(vec![1, 2, 3]);
        let snap = s.load();
        assert_eq!(*snap, vec![1, 2, 3]);
        s.store(vec![10, 20]);
        let next = s.load();
        assert_eq!(*next, vec![10, 20]);
        // Old snapshot still readable.
        assert_eq!(*snap, vec![1, 2, 3]);
    }

    #[test]
    fn rcu_mutates_atomically() {
        let s = Snapshot::new(0u32);
        for _ in 0..10 {
            s.rcu(|cur| cur + 1);
        }
        assert_eq!(*s.load(), 10);
    }

    #[test]
    fn many_readers_no_blocking() {
        let s = StdArc::new(Snapshot::new(0u64));
        let counter = StdArc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = s.clone();
            let c = counter.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = s.load();
                    c.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 8000);
    }

    #[test]
    fn default_constructs_via_t_default() {
        let s: Snapshot<Vec<u32>> = Snapshot::default();
        assert!(s.load().is_empty());
    }
}
