//! atomr-di. akka.net: `Akka.DI.Core` / `DependencyResolver`.
//!
//! Minimal type-keyed service container. Registered providers are `Arc`'d
//! factory functions producing `Arc<T>` on demand.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

type Factory = Arc<dyn Fn() -> Arc<dyn Any + Send + Sync> + Send + Sync>;

#[derive(Default)]
pub struct ServiceContainer {
    providers: RwLock<HashMap<TypeId, Factory>>,
}

impl ServiceContainer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T, F>(&self, factory: F)
    where
        T: Send + Sync + 'static,
        F: Fn() -> Arc<T> + Send + Sync + 'static,
    {
        let wrapper: Factory = Arc::new(move || factory() as Arc<dyn Any + Send + Sync>);
        self.providers.write().insert(TypeId::of::<T>(), wrapper);
    }

    pub fn resolve<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let providers = self.providers.read();
        let factory = providers.get(&TypeId::of::<T>())?;
        let any = factory();
        any.downcast::<T>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Hello(&'static str);

    #[test]
    fn resolves_registered_factory() {
        let c = ServiceContainer::new();
        c.register::<Hello, _>(|| Arc::new(Hello("world")));
        let h = c.resolve::<Hello>().unwrap();
        assert_eq!(h.0, "world");
    }
}
