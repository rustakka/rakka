//! Broadcast router. akka.net: `Routing/BroadcastPool.cs`.

use crate::actor::ActorRef;

pub struct BroadcastRouter<M: Send + Clone + 'static> {
    routees: Vec<ActorRef<M>>,
}

impl<M: Send + Clone + 'static> BroadcastRouter<M> {
    pub fn new(routees: Vec<ActorRef<M>>) -> Self {
        Self { routees }
    }

    pub fn route(&self, msg: M) {
        for r in &self.routees {
            r.tell(msg.clone());
        }
    }
}
