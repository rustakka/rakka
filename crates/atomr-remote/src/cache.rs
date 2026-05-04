//! Bounded LRU caches used by the remoting hot paths.
//!
//! Phase 5.H of `docs/full-port-plan.md`. Akka.NET parity:
//! `RemoteActorRefProvider` keeps an LRU of `ActorPath ↔ RemoteRef`
//! and `SerializerRegistry` keeps an LRU of serializer-id ↔
//! manifest. Both speed up repeat lookups on the inbound dispatcher.
//!
//! We hand-roll a small LRU instead of pulling in `lru` so the
//! crate stays dep-free.

use std::collections::HashMap;
use std::hash::Hash;

/// Bounded LRU cache. Eviction is O(N) per insert in the worst case
/// (we scan the access order), but N is the cache capacity — small
/// in practice (≤4096 for the remoting use case).
pub struct LruCache<K: Eq + Hash + Clone, V: Clone> {
    capacity: usize,
    map: HashMap<K, (V, u64)>,
    /// Monotonically-increasing access counter.
    tick: u64,
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity >= 1, "capacity must be >= 1");
        Self { capacity, map: HashMap::with_capacity(capacity), tick: 0 }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    /// Look up `key`, bumping its recency. Returns `None` on miss.
    pub fn get(&mut self, key: &K) -> Option<V> {
        let v = self.map.get_mut(key).map(|(v, last)| {
            self.tick += 1;
            *last = self.tick;
            v.clone()
        });
        v
    }

    /// Insert `(key, value)`. Evicts the least-recently-used entry
    /// when at capacity. Returns the evicted value if any.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        self.tick += 1;
        if self.map.contains_key(&key) {
            let (slot, last) = self.map.get_mut(&key).expect("checked above");
            *slot = value;
            *last = self.tick;
            return None;
        }
        if self.map.len() >= self.capacity {
            // Evict the entry with the smallest `last`.
            if let Some((evict_k, _)) =
                self.map.iter().min_by_key(|(_, (_, last))| *last).map(|(k, _)| (k.clone(), ()))
            {
                let (evicted, _) = self.map.remove(&evict_k).expect("just found");
                self.map.insert(key, (value, self.tick));
                return Some(evicted);
            }
        }
        self.map.insert(key, (value, self.tick));
        None
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key).map(|(v, _)| v)
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.tick = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let mut c = LruCache::<&'static str, i32>::new(3);
        assert!(c.put("a", 1).is_none());
        assert!(c.put("b", 2).is_none());
        assert_eq!(c.get(&"a"), Some(1));
        assert_eq!(c.get(&"b"), Some(2));
        assert_eq!(c.get(&"c"), None);
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn lru_eviction_drops_least_recently_used() {
        let mut c = LruCache::<&'static str, i32>::new(2);
        c.put("a", 1);
        c.put("b", 2);
        let _ = c.get(&"a"); // bump a
        let evicted = c.put("c", 3); // b should evict
        assert_eq!(evicted, Some(2));
        assert!(!c.contains(&"b"));
        assert!(c.contains(&"a"));
        assert!(c.contains(&"c"));
    }

    #[test]
    fn put_existing_key_updates_value_no_evict() {
        let mut c = LruCache::<&'static str, i32>::new(2);
        c.put("a", 1);
        c.put("b", 2);
        let evicted = c.put("a", 99);
        assert!(evicted.is_none());
        assert_eq!(c.get(&"a"), Some(99));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn remove_drops_entry() {
        let mut c = LruCache::<&'static str, i32>::new(2);
        c.put("a", 1);
        let r = c.remove(&"a");
        assert_eq!(r, Some(1));
        assert!(c.is_empty());
    }

    #[test]
    fn clear_resets_state() {
        let mut c = LruCache::<&'static str, i32>::new(2);
        c.put("a", 1);
        c.put("b", 2);
        c.clear();
        assert!(c.is_empty());
    }

    #[test]
    #[should_panic]
    fn zero_capacity_panics() {
        let _: LruCache<&'static str, i32> = LruCache::new(0);
    }
}
