//! Round-robin router. akka.net: `Routing/RoundRobinPool.cs`.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::actor::ActorRef;

pub struct RoundRobinRouter<M: Send + Clone + 'static> {
    routees: Vec<ActorRef<M>>,
    cursor: AtomicUsize,
}

impl<M: Send + Clone + 'static> RoundRobinRouter<M> {
    pub fn new(routees: Vec<ActorRef<M>>) -> Self {
        Self { routees, cursor: AtomicUsize::new(0) }
    }

    pub fn route(&self, msg: M) {
        if self.routees.is_empty() {
            return;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.routees.len();
        self.routees[idx].tell(msg);
    }
}
