//! Listener router.
//!
//! A pub/sub registry: any number of subscriber `ActorRef<M>` may
//! `subscribe`; messages routed via [`ListenerRouter::publish`] are
//! delivered to every current subscriber. Subscribers can `unsubscribe`
//! at any time.
//!
//! The router stores subscribers behind an `Arc<Mutex<...>>` so that the
//! same handle can be shared across tasks. We use `parking_lot::Mutex`
//! for the same reason `EventStream` does — sub/unsub is rare and
//! contention is low.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::actor::ActorRef;

/// Pub/sub router with dynamic subscriber set.
pub struct ListenerRouter<M: Clone + Send + 'static> {
    subs: Arc<Mutex<Vec<ActorRef<M>>>>,
}

impl<M: Clone + Send + 'static> Default for ListenerRouter<M> {
    fn default() -> Self {
        Self { subs: Arc::new(Mutex::new(Vec::new())) }
    }
}

impl<M: Clone + Send + 'static> Clone for ListenerRouter<M> {
    fn clone(&self) -> Self {
        Self { subs: Arc::clone(&self.subs) }
    }
}

impl<M: Clone + Send + 'static> ListenerRouter<M> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a subscriber. Idempotent: a path that is already subscribed
    /// is not added twice.
    pub fn subscribe(&self, r: ActorRef<M>) {
        let mut g = self.subs.lock();
        if !g.iter().any(|s| s.path() == r.path()) {
            g.push(r);
        }
    }

    /// Remove a subscriber by path. Returns `true` if a subscriber was
    /// removed.
    pub fn unsubscribe(&self, r: &ActorRef<M>) -> bool {
        let mut g = self.subs.lock();
        let len_before = g.len();
        g.retain(|s| s.path() != r.path());
        g.len() != len_before
    }

    /// Number of currently-registered subscribers.
    pub fn len(&self) -> usize {
        self.subs.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Send `msg` to every current subscriber. The message is cloned
    /// per subscriber.
    pub fn publish(&self, msg: M) {
        for s in self.subs.lock().iter() {
            s.tell(msg.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, ActorSystem, Context, Props};
    use atomr_config::Config;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static SEEN: AtomicUsize = AtomicUsize::new(0);

    #[derive(Default)]
    struct Counter;

    #[async_trait::async_trait]
    impl Actor for Counter {
        type Msg = u32;
        async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: u32) {
            SEEN.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn publish_fans_out_to_subscribers() {
        SEEN.store(0, Ordering::SeqCst);
        let sys = ActorSystem::create("listener", Config::reference()).await.unwrap();
        let a = sys.actor_of(Props::create(Counter::default), "a").unwrap();
        let b = sys.actor_of(Props::create(Counter::default), "b").unwrap();
        let r: ListenerRouter<u32> = ListenerRouter::new();
        r.subscribe(a);
        r.subscribe(b);
        assert_eq!(r.len(), 2);
        r.publish(1);
        r.publish(2);
        // Drain — give tokio a tick.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(SEEN.load(Ordering::SeqCst), 4);
        sys.terminate().await;
    }

    #[tokio::test]
    async fn unsubscribe_removes_subscriber() {
        let sys = ActorSystem::create("listener2", Config::reference()).await.unwrap();
        let a = sys.actor_of(Props::create(Counter::default), "a").unwrap();
        let r: ListenerRouter<u32> = ListenerRouter::new();
        r.subscribe(a.clone());
        assert!(r.unsubscribe(&a));
        assert!(r.is_empty());
        assert!(!r.unsubscribe(&a));
        sys.terminate().await;
    }

    #[tokio::test]
    async fn subscribe_is_idempotent() {
        let sys = ActorSystem::create("listener3", Config::reference()).await.unwrap();
        let a = sys.actor_of(Props::create(Counter::default), "a").unwrap();
        let r: ListenerRouter<u32> = ListenerRouter::new();
        r.subscribe(a.clone());
        r.subscribe(a);
        assert_eq!(r.len(), 1);
        sys.terminate().await;
    }
}
