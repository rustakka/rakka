//! Consistent-hash router.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use crate::actor::ActorRef;

pub struct ConsistentHashRouter<M: Send + Clone + 'static> {
    ring: BTreeMap<u64, usize>,
    routees: Vec<ActorRef<M>>,
    vnodes: u32,
}

impl<M: Send + Clone + 'static> ConsistentHashRouter<M> {
    pub fn new(routees: Vec<ActorRef<M>>, virtual_nodes_factor: u32) -> Self {
        let mut ring = BTreeMap::new();
        for (i, r) in routees.iter().enumerate() {
            for v in 0..virtual_nodes_factor {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                r.path().to_string().hash(&mut h);
                v.hash(&mut h);
                ring.insert(h.finish(), i);
            }
        }
        Self { ring, routees, vnodes: virtual_nodes_factor }
    }

    pub fn route_by_key<K: Hash>(&self, key: K, msg: M) {
        if self.routees.is_empty() {
            return;
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        let k = h.finish();
        let idx = self.ring.range(k..).next().or_else(|| self.ring.iter().next()).map(|(_, i)| *i);
        if let Some(i) = idx {
            self.routees[i].tell(msg);
        }
    }

    pub fn virtual_nodes(&self) -> u32 {
        self.vnodes
    }
}
