//! TypeId-keyed registry used by the `Extensions` subsystem.
//! akka.net: `Util/Reflection.cs` (partial — we do not need runtime reflection).

use std::any::{Any, TypeId};
use std::sync::Arc;

use dashmap::DashMap;

#[derive(Debug, Default)]
pub struct TypeRegistry {
    inner: DashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl TypeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<T: Any + Send + Sync>(&self, value: T) {
        self.inner.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.inner.get(&TypeId::of::<T>()).and_then(|entry| entry.clone().downcast::<T>().ok())
    }

    pub fn contains<T: Any + Send + Sync>(&self) -> bool {
        self.inner.contains_key(&TypeId::of::<T>())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Foo(u32);

    #[test]
    fn inserts_and_fetches() {
        let r = TypeRegistry::new();
        r.insert(Foo(7));
        let got = r.get::<Foo>().expect("present");
        assert_eq!(*got, Foo(7));
        assert!(r.contains::<Foo>());
        assert_eq!(r.len(), 1);
    }
}
