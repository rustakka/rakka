//! atomr-discovery.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub host: String,
    pub port: Option<u16>,
}

#[derive(Debug, Clone)]
pub struct Resolved {
    pub service_name: String,
    pub addresses: Vec<ResolvedTarget>,
}

#[async_trait]
pub trait ServiceDiscovery: Send + Sync + 'static {
    async fn lookup(&self, service_name: &str) -> Resolved;
}

#[derive(Default)]
pub struct StaticDiscovery {
    services: RwLock<HashMap<String, Vec<ResolvedTarget>>>,
}

impl StaticDiscovery {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, name: impl Into<String>, target: ResolvedTarget) {
        self.services.write().entry(name.into()).or_default().push(target);
    }
}

#[async_trait]
impl ServiceDiscovery for StaticDiscovery {
    async fn lookup(&self, service_name: &str) -> Resolved {
        Resolved {
            service_name: service_name.into(),
            addresses: self.services.read().get(service_name).cloned().unwrap_or_default(),
        }
    }
}

/// Chain of discovery backends. `lookup` walks providers in order and
/// returns the first non-empty resolution.
pub struct AggregateDiscovery {
    providers: Vec<Arc<dyn ServiceDiscovery>>,
}

impl AggregateDiscovery {
    pub fn new(providers: Vec<Arc<dyn ServiceDiscovery>>) -> Arc<Self> {
        Arc::new(Self { providers })
    }

    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }
}

#[async_trait]
impl ServiceDiscovery for AggregateDiscovery {
    async fn lookup(&self, service_name: &str) -> Resolved {
        for p in &self.providers {
            let r = p.lookup(service_name).await;
            if !r.addresses.is_empty() {
                return r;
            }
        }
        Resolved { service_name: service_name.into(), addresses: Vec::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_discovery_resolves() {
        let d = StaticDiscovery::new();
        d.register("svc", ResolvedTarget { host: "1.2.3.4".into(), port: Some(8080) });
        let r = d.lookup("svc").await;
        assert_eq!(r.addresses.len(), 1);
    }

    #[tokio::test]
    async fn aggregate_falls_through_to_second_provider_when_first_empty() {
        let empty = StaticDiscovery::new();
        let full = StaticDiscovery::new();
        full.register("svc", ResolvedTarget { host: "10.0.0.1".into(), port: None });
        let agg = AggregateDiscovery::new(vec![empty, full]);
        let r = agg.lookup("svc").await;
        assert_eq!(r.addresses.len(), 1);
        assert_eq!(r.addresses[0].host, "10.0.0.1");
    }

    #[tokio::test]
    async fn aggregate_returns_first_nonempty_provider() {
        let a = StaticDiscovery::new();
        a.register("svc", ResolvedTarget { host: "first".into(), port: None });
        let b = StaticDiscovery::new();
        b.register("svc", ResolvedTarget { host: "second".into(), port: None });
        let agg = AggregateDiscovery::new(vec![a, b]);
        let r = agg.lookup("svc").await;
        assert_eq!(r.addresses.len(), 1);
        assert_eq!(r.addresses[0].host, "first");
    }

    #[tokio::test]
    async fn aggregate_empty_when_no_providers_resolve() {
        let a = StaticDiscovery::new();
        let b = StaticDiscovery::new();
        let agg = AggregateDiscovery::new(vec![a, b]);
        let r = agg.lookup("svc").await;
        assert!(r.addresses.is_empty());
        assert_eq!(r.service_name, "svc");
    }

    #[tokio::test]
    async fn aggregate_with_no_providers_resolves_empty() {
        let agg = AggregateDiscovery::new(Vec::new());
        assert_eq!(agg.provider_count(), 0);
        let r = agg.lookup("svc").await;
        assert!(r.addresses.is_empty());
    }
}
