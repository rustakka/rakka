//! [`AuditLog`] — built-in [`super::Reader`] that retains a ring of
//! every event it sees, useful for compliance views and "what
//! happened recently" UIs.

use std::collections::VecDeque;

use async_trait::async_trait;

use crate::cqrs::reader::Reader;
use crate::DomainEvent;

/// Bounded ring buffer of events.
pub struct AuditProjection<E> {
    pub capacity: usize,
    pub entries: VecDeque<E>,
}

impl<E> Default for AuditProjection<E> {
    fn default() -> Self {
        Self { capacity: 1024, entries: VecDeque::new() }
    }
}

impl<E: Clone> AuditProjection<E> {
    /// Most recent `n` events in chronological order.
    pub fn recent(&self, n: usize) -> Vec<E> {
        let n = n.min(self.entries.len());
        self.entries.iter().rev().take(n).rev().cloned().collect()
    }
}

/// A reader that records every event it sees into an
/// [`AuditProjection`]. Construct via [`AuditLog::with_capacity`].
pub struct AuditLog<E: DomainEvent> {
    name: String,
    capacity: usize,
    _ev: std::marker::PhantomData<E>,
}

impl<E: DomainEvent> AuditLog<E> {
    /// Construct an audit log with the given ring capacity. Wire
    /// decoding via [`crate::cqrs::CqrsBuilder::with_event_codecs`]
    /// — the registry takes priority over [`Reader::decode`].
    pub fn with_capacity(capacity: usize) -> Self {
        Self { name: "audit".into(), capacity, _ev: std::marker::PhantomData }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
}

#[async_trait]
impl<E: DomainEvent + Sync> Reader for AuditLog<E> {
    type Event = E;
    type Projection = AuditProjection<E>;
    type Error = std::io::Error;

    fn name(&self) -> &str {
        &self.name
    }

    fn decode(_bytes: &[u8]) -> Result<Self::Event, String> {
        // Static `decode` doesn't have access to `self.decoder`, so
        // we fall through and rely on a configured codec registry.
        // Users who want a fixed-codec audit log should pass the
        // decoder closure they used at the aggregate level.
        Err("AuditLog: configure an EventCodecRegistry on the CQRS pattern".into())
    }

    async fn apply(&mut self, p: &mut AuditProjection<E>, event: E) -> Result<(), std::io::Error> {
        if p.capacity == 0 {
            p.capacity = self.capacity;
        }
        if p.entries.len() >= p.capacity {
            p.entries.pop_front();
        }
        p.entries.push_back(event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct E(i32);
    impl crate::DomainEvent for E {}

    #[test]
    fn ring_truncates_to_capacity() {
        let mut p = AuditProjection::<E> { capacity: 3, entries: VecDeque::new() };
        let mut audit = AuditLog::<E>::with_capacity(3);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            for i in 0..5 {
                audit.apply(&mut p, E(i)).await.unwrap();
            }
        });
        assert_eq!(p.entries.len(), 3);
        assert_eq!(p.recent(3), vec![E(2), E(3), E(4)]);
    }
}
