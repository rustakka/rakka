//! ClusterSingletonManager / Proxy — one logical actor across the cluster.
//! akka.net: `Akka.Cluster.Tools/Singleton/`.
//!
//! Phase 7.C of `docs/full-port-plan.md`. Handover protocol:
//!
//! ```text
//! Active(here) ── oldest changed ──► HandingOver ── handover ack ──► Inactive
//! Inactive ────── elected oldest ──► Starting     ── started   ──► Active(here)
//! Inactive ────── observed remote ──► Active(remote)
//! ```
//!
//! While in `HandingOver` or `Starting`, messages submitted via the
//! [`ClusterSingletonProxy`] are buffered (up to `buffer_size`) and
//! flushed once the singleton is `Active` again.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;

use rakka_core::actor::UntypedActorRef;

/// Singleton lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SingletonState {
    /// No singleton known yet.
    Inactive,
    /// We are about to become the singleton, but haven't started it.
    Starting,
    /// The singleton lives at this ref.
    Active { ref_: UntypedActorRef, here: bool },
    /// We were the singleton; new oldest member is taking over.
    HandingOver,
}

/// Buffered envelope used during handover. The payload is type-erased
/// (Box<dyn Any>) because the proxy doesn't know the concrete message
/// type at the API boundary; recipients downcast on flush. Phase 13
/// will replace this with typed-per-(M) buffering.
type BufferedMsg = Box<dyn FnOnce(&UntypedActorRef) + Send + 'static>;

/// Decides which node hosts the singleton based on oldest up-member —
/// a hook is provided so tests can simulate handover without wiring
/// the full cluster.
pub struct ClusterSingletonManager {
    state: RwLock<SingletonState>,
    buffer: parking_lot::Mutex<VecDeque<BufferedMsg>>,
    buffer_size: usize,
    /// Count of messages dropped because the buffer was full.
    drops: parking_lot::Mutex<u64>,
}

impl Default for ClusterSingletonManager {
    fn default() -> Self {
        Self {
            state: RwLock::new(SingletonState::Inactive),
            buffer: parking_lot::Mutex::new(VecDeque::new()),
            buffer_size: 1_000,
            drops: parking_lot::Mutex::new(0),
        }
    }
}

impl ClusterSingletonManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Construct with a custom proxy buffer size.
    pub fn with_buffer_size(size: usize) -> Arc<Self> {
        Arc::new(Self { buffer_size: size, ..Self::default() })
    }

    pub fn state(&self) -> SingletonState {
        self.state.read().clone()
    }

    /// Mark `r` as the local singleton (we won the election).
    /// Flushes any messages that were buffered during handover.
    pub fn set_active_here(&self, r: UntypedActorRef) {
        *self.state.write() = SingletonState::Active { ref_: r.clone(), here: true };
        self.flush(&r);
    }

    /// Mark `r` as the remote singleton (some other node is hosting it).
    pub fn set_active_remote(&self, r: UntypedActorRef) {
        *self.state.write() = SingletonState::Active { ref_: r.clone(), here: false };
        self.flush(&r);
    }

    /// Begin handover (we used to be Active(here)).
    pub fn begin_handover(&self) {
        *self.state.write() = SingletonState::HandingOver;
    }

    /// Begin starting (we were elected as the new oldest).
    pub fn begin_starting(&self) {
        *self.state.write() = SingletonState::Starting;
    }

    /// Forget the current singleton entirely.
    pub fn clear(&self) {
        *self.state.write() = SingletonState::Inactive;
    }

    pub fn current(&self) -> Option<UntypedActorRef> {
        match &*self.state.read() {
            SingletonState::Active { ref_, .. } => Some(ref_.clone()),
            _ => None,
        }
    }

    /// Buffer `deliver` for replay once the singleton becomes
    /// `Active`. Used by the proxy when the singleton isn't yet
    /// reachable. Returns `true` if buffered, `false` if the buffer
    /// was full (in which case the caller can route to DeadLetters).
    fn buffer_or_deliver<F>(&self, deliver: F) -> bool
    where
        F: FnOnce(&UntypedActorRef) + Send + 'static,
    {
        if let Some(r) = self.current() {
            deliver(&r);
            return true;
        }
        let mut q = self.buffer.lock();
        if q.len() >= self.buffer_size {
            *self.drops.lock() += 1;
            return false;
        }
        q.push_back(Box::new(deliver));
        true
    }

    fn flush(&self, target: &UntypedActorRef) {
        let mut q = self.buffer.lock();
        while let Some(deliver) = q.pop_front() {
            deliver(target);
        }
    }

    /// Number of currently-buffered messages (waiting for handover to
    /// complete).
    pub fn buffered(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Total number of messages dropped due to buffer-full overflow.
    pub fn drops(&self) -> u64 {
        *self.drops.lock()
    }
}

/// Proxy that routes messages to the current singleton, buffering
/// during handover.
pub struct ClusterSingletonProxy {
    pub manager: Arc<ClusterSingletonManager>,
}

impl ClusterSingletonProxy {
    pub fn new(manager: Arc<ClusterSingletonManager>) -> Self {
        Self { manager }
    }

    pub fn singleton(&self) -> Option<UntypedActorRef> {
        self.manager.current()
    }

    /// Schedule `deliver` against the singleton. If `Active`, runs
    /// immediately; if `Inactive`/`Starting`/`HandingOver`, buffers
    /// for replay. Returns `false` if the buffer was full.
    pub fn send<F>(&self, deliver: F) -> bool
    where
        F: FnOnce(&UntypedActorRef) + Send + 'static,
    {
        self.manager.buffer_or_deliver(deliver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_core::actor::Inbox;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn proxy_routes_to_current_singleton() {
        let mgr = ClusterSingletonManager::new();
        let inbox = Inbox::<u32>::new("singleton");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        let proxy = ClusterSingletonProxy::new(mgr);
        assert!(proxy.singleton().is_some());
    }

    #[test]
    fn handover_state_transitions() {
        let mgr = ClusterSingletonManager::new();
        assert!(matches!(mgr.state(), SingletonState::Inactive));
        mgr.begin_starting();
        assert!(matches!(mgr.state(), SingletonState::Starting));
        let inbox = Inbox::<u32>::new("s");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        assert!(matches!(mgr.state(), SingletonState::Active { here: true, .. }));
        mgr.begin_handover();
        assert!(matches!(mgr.state(), SingletonState::HandingOver));
    }

    #[tokio::test]
    async fn proxy_buffers_during_handover_and_flushes_after() {
        let mgr = ClusterSingletonManager::new();
        let proxy = ClusterSingletonProxy::new(mgr.clone());

        let calls = Arc::new(AtomicU32::new(0));
        // Send 3 messages while inactive — all buffered.
        for _ in 0..3 {
            let c = calls.clone();
            assert!(proxy.send(move |_r| {
                c.fetch_add(1, Ordering::SeqCst);
            }));
        }
        assert_eq!(mgr.buffered(), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        // Become active → buffer flushes.
        let inbox = Inbox::<u32>::new("s");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        assert_eq!(mgr.buffered(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // After active, send delivers immediately.
        let c2 = calls.clone();
        proxy.send(move |_| {
            c2.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn full_buffer_drops_and_counts_overflow() {
        let mgr = ClusterSingletonManager::with_buffer_size(2);
        let proxy = ClusterSingletonProxy::new(mgr.clone());
        assert!(proxy.send(|_| {}));
        assert!(proxy.send(|_| {}));
        // Third should overflow.
        assert!(!proxy.send(|_| {}));
        assert_eq!(mgr.drops(), 1);
        assert_eq!(mgr.buffered(), 2);
    }

    #[test]
    fn set_active_remote_marks_here_false() {
        let mgr = ClusterSingletonManager::new();
        let inbox = Inbox::<u32>::new("remote-host");
        mgr.set_active_remote(inbox.actor_ref().as_untyped());
        match mgr.state() {
            SingletonState::Active { here, .. } => assert!(!here),
            _ => panic!("expected active-remote"),
        }
    }
}
