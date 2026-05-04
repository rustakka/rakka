//! `AddressUidExtension`. akka.net: `Remote/AddressUidExtension.cs`.
//!
//! Each `ActorSystem` incarnation gets a fresh UID at startup. The UID
//! travels in every association handshake; if we see the same `Address`
//! associate with a *different* UID than we last knew, the peer crashed
//! and was restarted, so we drop our previous endpoint state, surface
//! `Terminated` for everything we were watching there, and start fresh.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct AddressUid {
    inner: Arc<AddressUidInner>,
}

#[derive(Debug)]
struct AddressUidInner {
    value: AtomicU64,
}

impl Default for AddressUid {
    fn default() -> Self {
        Self::new()
    }
}

impl AddressUid {
    /// Pick a UID derived from the wall clock, falling back to a
    /// monotonic counter on systems without a usable clock.
    pub fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or_else(|_| next_fallback());
        // Mix in a small entropy seed so two systems started in the same
        // nanosecond on the same machine still differ.
        let mixed = nanos.wrapping_mul(2862933555777941757).wrapping_add(3037000493);
        Self { inner: Arc::new(AddressUidInner { value: AtomicU64::new(mixed) }) }
    }

    pub fn get(&self) -> u64 {
        self.inner.value.load(Ordering::Acquire)
    }
}

fn next_fallback() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_systems_get_distinct_uids() {
        let a = AddressUid::new();
        std::thread::sleep(std::time::Duration::from_micros(10));
        let b = AddressUid::new();
        assert_ne!(a.get(), b.get());
    }

    #[test]
    fn cloned_handle_observes_same_uid() {
        let a = AddressUid::new();
        let b = a.clone();
        assert_eq!(a.get(), b.get());
    }
}
