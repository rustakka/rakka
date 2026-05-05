//! atomr-coordination./ `Lease` API.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::Mutex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LeaseError {
    #[error("lease already held by {0}")]
    AlreadyHeld(String),
    #[error("lease not held")]
    NotHeld,
}

#[async_trait]
pub trait Lease: Send + Sync + 'static {
    async fn acquire(&self, owner: &str) -> Result<bool, LeaseError>;
    async fn release(&self, owner: &str) -> Result<(), LeaseError>;
    async fn check_lease(&self) -> Option<String>;
}

#[derive(Default)]
pub struct InMemoryLease {
    inner: Mutex<Option<(String, Instant)>>,
}

impl InMemoryLease {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl Lease for InMemoryLease {
    async fn acquire(&self, owner: &str) -> Result<bool, LeaseError> {
        let mut guard = self.inner.lock();
        match guard.as_ref() {
            Some((current, _)) if current == owner => Ok(true),
            Some((current, _)) => Err(LeaseError::AlreadyHeld(current.clone())),
            None => {
                *guard = Some((owner.to_string(), Instant::now()));
                Ok(true)
            }
        }
    }

    async fn release(&self, owner: &str) -> Result<(), LeaseError> {
        let mut guard = self.inner.lock();
        match guard.as_ref() {
            Some((current, _)) if current == owner => {
                *guard = None;
                Ok(())
            }
            _ => Err(LeaseError::NotHeld),
        }
    }

    async fn check_lease(&self) -> Option<String> {
        self.inner.lock().as_ref().map(|(s, _)| s.clone())
    }
}

#[derive(Default)]
pub struct LeaseRegistry {
    leases: Mutex<HashMap<String, Arc<InMemoryLease>>>,
}

impl LeaseRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&self, name: &str) -> Arc<InMemoryLease> {
        self.leases.lock().entry(name.to_string()).or_default().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_release_cycle() {
        let l = InMemoryLease::new();
        assert!(l.acquire("me").await.unwrap());
        assert_eq!(l.check_lease().await.as_deref(), Some("me"));
        l.release("me").await.unwrap();
        assert!(l.check_lease().await.is_none());
    }

    #[tokio::test]
    async fn second_owner_rejected() {
        let l = InMemoryLease::new();
        l.acquire("a").await.unwrap();
        matches!(l.acquire("b").await.unwrap_err(), LeaseError::AlreadyHeld(_));
    }
}
