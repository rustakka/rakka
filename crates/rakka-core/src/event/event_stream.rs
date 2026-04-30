//! Typed pub/sub. akka.net: `Event/EventStream.cs`.

use std::any::{Any, TypeId};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

type SubFn = Arc<dyn Fn(&(dyn Any + Send + Sync)) + Send + Sync>;

#[derive(Clone)]
pub struct Subscription {
    pub id: u64,
    type_id: TypeId,
    map: Arc<DashMap<TypeId, Mutex<Vec<(u64, SubFn)>>>>,
}

impl Subscription {
    pub fn unsubscribe(&self) {
        if let Some(e) = self.map.get(&self.type_id) {
            e.lock().retain(|(id, _)| *id != self.id);
        }
    }
}

#[derive(Default)]
pub struct EventStream {
    map: Arc<DashMap<TypeId, Mutex<Vec<(u64, SubFn)>>>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl EventStream {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe<T: Any + Send + Sync>(
        &self,
        f: impl Fn(&T) + Send + Sync + 'static,
    ) -> Subscription {
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let type_id = TypeId::of::<T>();
        let cb: SubFn = Arc::new(move |any: &(dyn Any + Send + Sync)| {
            if let Some(t) = any.downcast_ref::<T>() {
                f(t);
            }
        });
        self.map.entry(type_id).or_default().lock().push((id, cb));
        Subscription { id, type_id, map: self.map.clone() }
    }

    pub fn publish<T: Any + Send + Sync>(&self, value: T) {
        let type_id = TypeId::of::<T>();
        let value_arc: Arc<dyn Any + Send + Sync> = Arc::new(value);
        if let Some(entry) = self.map.get(&type_id) {
            let callbacks: Vec<SubFn> = entry.lock().iter().map(|(_, f)| f.clone()).collect();
            for f in callbacks {
                f(&*value_arc);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn publishes_to_typed_subscribers() {
        let bus = EventStream::new();
        let n = Arc::new(AtomicU32::new(0));
        let n1 = n.clone();
        let sub = bus.subscribe(move |v: &u32| {
            n1.fetch_add(*v, Ordering::SeqCst);
        });
        bus.publish(10u32);
        bus.publish(20u32);
        bus.publish("ignored".to_string());
        assert_eq!(n.load(Ordering::SeqCst), 30);
        sub.unsubscribe();
        bus.publish(100u32);
        assert_eq!(n.load(Ordering::SeqCst), 30);
    }
}
