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

    /// Subscribe with a predicate filter — only events matching
    /// `pred(t)` are delivered to `f`. Phase 3.5 of
    /// `docs/full-port-plan.md`. Akka.NET's
    /// `EventStream.Subscribe(IActorRef, predicate)` analog.
    pub fn subscribe_filtered<T, P>(
        &self,
        pred: P,
        f: impl Fn(&T) + Send + Sync + 'static,
    ) -> Subscription
    where
        T: Any + Send + Sync,
        P: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let type_id = TypeId::of::<T>();
        let cb: SubFn = Arc::new(move |any: &(dyn Any + Send + Sync)| {
            if let Some(t) = any.downcast_ref::<T>() {
                if pred(t) {
                    f(t);
                }
            }
        });
        self.map.entry(type_id).or_default().lock().push((id, cb));
        Subscription { id, type_id, map: self.map.clone() }
    }

    /// Number of subscribers registered for events of type `T`.
    pub fn subscriber_count<T: Any>(&self) -> usize {
        self.map
            .get(&TypeId::of::<T>())
            .map(|e| e.lock().len())
            .unwrap_or(0)
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

    #[test]
    fn subscribe_filtered_delivers_only_matches() {
        let bus = EventStream::new();
        let count = Arc::new(AtomicU32::new(0));
        let c2 = count.clone();
        let _sub = bus.subscribe_filtered(
            |v: &u32| *v > 5,
            move |_| { c2.fetch_add(1, Ordering::SeqCst); },
        );
        bus.publish(1u32);
        bus.publish(7u32);
        bus.publish(2u32);
        bus.publish(99u32);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn subscriber_count_reflects_registered_subscribers() {
        let bus = EventStream::new();
        assert_eq!(bus.subscriber_count::<u32>(), 0);
        let _s1 = bus.subscribe(|_v: &u32| {});
        let _s2 = bus.subscribe_filtered(|_v: &u32| true, |_| {});
        assert_eq!(bus.subscriber_count::<u32>(), 2);
        assert_eq!(bus.subscriber_count::<String>(), 0);
    }
}
