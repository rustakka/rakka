//! Extensions — per-`ActorSystem` singletons keyed by type.
//! akka.net: `Actor/Extensions.cs`.

use std::any::{Any, TypeId};
use std::sync::Arc;

use dashmap::DashMap;

/// Marker trait for types stored in `Extensions`.
pub trait Extension: Any + Send + Sync {}

impl<T: Any + Send + Sync> Extension for T {}

/// Identifier trait mirroring akka.net's `IExtensionId<T>`.
pub trait ExtensionId<E: Extension>: Send + Sync {
    fn create(&self) -> E;
}

#[derive(Debug, Default)]
pub struct Extensions {
    inner: DashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl Extensions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<E: Extension>(&self, ext: E) {
        self.inner.insert(TypeId::of::<E>(), Arc::new(ext));
    }

    pub fn get<E: Extension>(&self) -> Option<Arc<E>> {
        self.inner.get(&TypeId::of::<E>()).and_then(|e| e.clone().downcast::<E>().ok())
    }

    pub fn get_or_create<E: Extension, I: ExtensionId<E>>(&self, id: &I) -> Arc<E> {
        if let Some(e) = self.get::<E>() {
            return e;
        }
        let ext = id.create();
        self.register(ext);
        self.get::<E>().expect("just inserted")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Metrics(u32);
    struct MetricsId;
    impl ExtensionId<Metrics> for MetricsId {
        fn create(&self) -> Metrics {
            Metrics(99)
        }
    }

    #[test]
    fn create_and_get() {
        let e = Extensions::new();
        let m = e.get_or_create::<Metrics, _>(&MetricsId);
        assert_eq!(m.0, 99);
        assert!(e.get::<Metrics>().is_some());
    }
}
