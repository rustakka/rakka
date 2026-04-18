//! Random router. akka.net: `Routing/RandomPool.cs`.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::actor::ActorRef;

pub struct RandomRouter<M: Send + Clone + 'static> {
    routees: Vec<ActorRef<M>>,
    seed: AtomicU64,
}

impl<M: Send + Clone + 'static> RandomRouter<M> {
    pub fn new(routees: Vec<ActorRef<M>>) -> Self {
        Self { routees, seed: AtomicU64::new(0xDEADBEEF) }
    }

    pub fn route(&self, msg: M) {
        if self.routees.is_empty() {
            return;
        }
        let s = self.seed.fetch_add(1, Ordering::Relaxed);
        let idx = (splitmix64(s) as usize) % self.routees.len();
        self.routees[idx].tell(msg);
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
