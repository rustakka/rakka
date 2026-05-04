//! Remote metrics extension.
//! akka.net: `Remote/RemoteMetricsExtension.cs`.
//!
//! Lightweight per-`Address` counters for sent/received messages and
//! bytes. The dashboard / `atomr-telemetry` consume this via
//! [`RemoteMetrics::snapshot`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use atomr_core::actor::Address;

#[derive(Default, Debug)]
struct PerAddress {
    sent_messages: AtomicU64,
    sent_bytes: AtomicU64,
    received_messages: AtomicU64,
    received_bytes: AtomicU64,
    errors: AtomicU64,
}

#[derive(Default, Clone)]
pub struct RemoteMetrics {
    inner: Arc<DashMap<String, PerAddress>>,
}

#[derive(Debug, Clone, Default)]
pub struct RemoteMetricsSnapshot {
    pub per_address: Vec<RemoteMetricsRow>,
}

#[derive(Debug, Clone)]
pub struct RemoteMetricsRow {
    pub address: String,
    pub sent_messages: u64,
    pub sent_bytes: u64,
    pub received_messages: u64,
    pub received_bytes: u64,
    pub errors: u64,
}

impl RemoteMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_send(&self, address: &Address, bytes: usize) {
        let e = self.inner.entry(address.to_string()).or_default();
        e.sent_messages.fetch_add(1, Ordering::Relaxed);
        e.sent_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_receive(&self, address: &Address, bytes: usize) {
        let e = self.inner.entry(address.to_string()).or_default();
        e.received_messages.fetch_add(1, Ordering::Relaxed);
        e.received_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_error(&self, address: &Address) {
        let e = self.inner.entry(address.to_string()).or_default();
        e.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RemoteMetricsSnapshot {
        let per_address = self
            .inner
            .iter()
            .map(|kv| RemoteMetricsRow {
                address: kv.key().clone(),
                sent_messages: kv.value().sent_messages.load(Ordering::Relaxed),
                sent_bytes: kv.value().sent_bytes.load(Ordering::Relaxed),
                received_messages: kv.value().received_messages.load(Ordering::Relaxed),
                received_bytes: kv.value().received_bytes.load(Ordering::Relaxed),
                errors: kv.value().errors.load(Ordering::Relaxed),
            })
            .collect();
        RemoteMetricsSnapshot { per_address }
    }
}
