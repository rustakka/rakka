//! rakka-discovery. akka.net: `Akka.Discovery`.

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
}
