//! Service-container spec parity.,
//! `DependencyResolverSpec`. Asserts the registry's resolution
//! semantics under common usage patterns.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use atomr_di::ServiceContainer;

#[derive(Debug, PartialEq)]
struct Hello(&'static str);

#[derive(Debug)]
struct Counter(AtomicU32);

#[test]
fn unregistered_type_resolves_to_none() {
    let c = ServiceContainer::new();
    assert!(c.resolve::<Hello>().is_none());
}

#[test]
fn each_resolve_calls_factory_again() {
    let c = ServiceContainer::new();
    let invocations = Arc::new(AtomicU32::new(0));
    let i = invocations.clone();
    c.register::<Hello, _>(move || {
        i.fetch_add(1, Ordering::SeqCst);
        Arc::new(Hello("world"))
    });
    let _a = c.resolve::<Hello>().unwrap();
    let _b = c.resolve::<Hello>().unwrap();
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

#[test]
fn re_register_overrides_factory() {
    let c = ServiceContainer::new();
    c.register::<Hello, _>(|| Arc::new(Hello("first")));
    c.register::<Hello, _>(|| Arc::new(Hello("second")));
    let h = c.resolve::<Hello>().unwrap();
    assert_eq!(h.0, "second");
}

#[test]
fn distinct_types_have_independent_registrations() {
    let c = ServiceContainer::new();
    c.register::<Hello, _>(|| Arc::new(Hello("hi")));
    c.register::<Counter, _>(|| Arc::new(Counter(AtomicU32::new(0))));
    assert_eq!(c.resolve::<Hello>().unwrap().0, "hi");
    let cnt = c.resolve::<Counter>().unwrap();
    cnt.0.fetch_add(5, Ordering::SeqCst);
    assert_eq!(cnt.0.load(Ordering::SeqCst), 5);
}

#[test]
fn arc_clone_preserves_shared_state_when_factory_returns_same_arc() {
    let shared = Arc::new(Counter(AtomicU32::new(0)));
    let c = ServiceContainer::new();
    let s = shared.clone();
    c.register::<Counter, _>(move || s.clone());
    let a = c.resolve::<Counter>().unwrap();
    a.0.fetch_add(1, Ordering::SeqCst);
    let b = c.resolve::<Counter>().unwrap();
    b.0.fetch_add(1, Ordering::SeqCst);
    assert_eq!(shared.0.load(Ordering::SeqCst), 2);
}

#[test]
fn container_is_send_sync_through_arc() {
    let c = Arc::new(ServiceContainer::new());
    c.register::<Hello, _>(|| Arc::new(Hello("threaded")));
    let cc = c.clone();
    let h = std::thread::spawn(move || cc.resolve::<Hello>().unwrap().0);
    let result = h.join().unwrap();
    assert_eq!(result, "threaded");
}
