//! `RecoveryPermitter` — bounded concurrent recoveries.
//!
//! Phase 11 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Persistence.RecoveryPermitter` (config:
//! `akka.persistence.max-concurrent-recoveries`).
//!
//! Without a permitter, every actor that starts up triggers a journal
//! replay; thousands of restart-storming actors can DoS the journal
//! backend. The permitter bounds the in-flight recovery count so
//! late arrivers wait their turn.
//!
//! Implementation note: a thin wrapper around
//! [`tokio::sync::Semaphore`] so it integrates cleanly with the
//! `Eventsourced::recover` driver. Held permits use the standard
//! `OwnedSemaphorePermit` so callers can `drop` them mid-method
//! (e.g. before running a long `recovery_completed` user hook).

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Bounded in-flight recovery counter.
#[derive(Clone)]
pub struct RecoveryPermitter {
    sem: Arc<Semaphore>,
    capacity: usize,
}

impl RecoveryPermitter {
    /// Create a permitter that allows up to `max_concurrent` parallel
    /// recoveries.
    pub fn new(max_concurrent: usize) -> Self {
        assert!(max_concurrent >= 1, "max_concurrent must be ≥ 1");
        Self { sem: Arc::new(Semaphore::new(max_concurrent)), capacity: max_concurrent }
    }

    /// Maximum permits ever issued (i.e. construction-time capacity).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Permits currently available — i.e. how many *more* recoveries
    /// could begin right now without blocking.
    pub fn available(&self) -> usize {
        self.sem.available_permits()
    }

    /// Permits currently held by callers (waiting on or driving a
    /// recovery).
    pub fn in_flight(&self) -> usize {
        self.capacity - self.available()
    }

    /// Block until a permit is available.
    ///
    /// Returns `None` if the permitter has been
    /// [`close`d](RecoveryPermitter::close), so callers can map the
    /// result onto `EventsourcedError::PermitDenied` cleanly.
    pub async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.sem.clone().acquire_owned().await.ok()
    }

    /// Try to acquire a permit without waiting. Returns `Err(_)` if
    /// no permit is available *right now*.
    pub fn try_acquire(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.sem.clone().try_acquire_owned()
    }

    /// Drain the permitter — pending and future
    /// [`acquire`s](RecoveryPermitter::acquire) return `None` so
    /// shutdown can short-circuit.
    pub fn close(&self) {
        self.sem.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn capacity_bounds_concurrent_acquires() {
        let p = RecoveryPermitter::new(2);
        assert_eq!(p.capacity(), 2);
        assert_eq!(p.available(), 2);

        let permit_a = p.acquire().await.unwrap();
        let permit_b = p.acquire().await.unwrap();
        assert_eq!(p.available(), 0);
        assert_eq!(p.in_flight(), 2);

        // Third acquire must wait; a parallel task drops permit_a after
        // a tick to release it.
        let p2 = p.clone();
        let h = tokio::spawn(async move { p2.acquire().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!h.is_finished()); // still waiting
        drop(permit_a);
        let permit_c = h.await.unwrap().unwrap();
        assert_eq!(p.in_flight(), 2);

        drop(permit_b);
        drop(permit_c);
        assert_eq!(p.in_flight(), 0);
    }

    #[tokio::test]
    async fn try_acquire_returns_immediately() {
        let p = RecoveryPermitter::new(1);
        let _held = p.try_acquire().unwrap();
        assert!(p.try_acquire().is_err());
    }

    #[tokio::test]
    async fn close_returns_none_for_pending() {
        let p = RecoveryPermitter::new(1);
        let _held = p.acquire().await.unwrap();
        let p2 = p.clone();
        let h = tokio::spawn(async move { p2.acquire().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        p.close();
        let r = h.await.unwrap();
        assert!(r.is_none());
    }

    #[test]
    #[should_panic]
    fn zero_capacity_panics() {
        let _ = RecoveryPermitter::new(0);
    }
}
