//! Extensions registry spec parity. akka.net:
//! `Actor.Extensions` + ExtensionsSpec.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use atomr_core::actor::{ExtensionId, Extensions};

struct CallCounter(Arc<AtomicU32>);

struct CallCounterId(Arc<AtomicU32>);

impl ExtensionId<CallCounter> for CallCounterId {
    fn create(&self) -> CallCounter {
        self.0.fetch_add(1, Ordering::SeqCst);
        CallCounter(self.0.clone())
    }
}

struct OtherExt(&'static str);
struct OtherId;
impl ExtensionId<OtherExt> for OtherId {
    fn create(&self) -> OtherExt {
        OtherExt("other")
    }
}

#[test]
fn unknown_extension_returns_none() {
    let e = Extensions::new();
    assert!(e.get::<CallCounter>().is_none());
}

#[test]
fn get_or_create_caches_first_call() {
    let e = Extensions::new();
    let n = Arc::new(AtomicU32::new(0));
    let id = CallCounterId(n.clone());
    let _a = e.get_or_create::<CallCounter, _>(&id);
    let _b = e.get_or_create::<CallCounter, _>(&id);
    let _c = e.get_or_create::<CallCounter, _>(&id);
    assert_eq!(n.load(Ordering::SeqCst), 1, "create() must run only once per type");
}

#[test]
fn distinct_types_have_separate_slots() {
    let e = Extensions::new();
    let n = Arc::new(AtomicU32::new(0));
    let cid = CallCounterId(n.clone());
    let oid = OtherId;
    e.get_or_create::<CallCounter, _>(&cid);
    e.get_or_create::<OtherExt, _>(&oid);
    assert!(e.get::<CallCounter>().is_some());
    assert!(e.get::<OtherExt>().is_some());
}

#[test]
fn register_overrides_prior_registration() {
    let e = Extensions::new();
    let first = OtherExt("first");
    e.register::<OtherExt>(first);
    assert_eq!(e.get::<OtherExt>().unwrap().0, "first");
    e.register::<OtherExt>(OtherExt("second"));
    assert_eq!(e.get::<OtherExt>().unwrap().0, "second");
}

#[test]
fn extensions_are_send_sync_via_arc() {
    let e: Arc<Extensions> = Arc::new(Extensions::new());
    e.register::<OtherExt>(OtherExt("threaded"));
    let ec = e.clone();
    let h = std::thread::spawn(move || ec.get::<OtherExt>().unwrap().0);
    assert_eq!(h.join().unwrap(), "threaded");
}
