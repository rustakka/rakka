//! atomr-hosting. akka.net: `Akka.Hosting`.
//!
//! Builder for wiring together `ActorSystem`, config, and DI container.

use std::sync::Arc;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_di::ServiceContainer;

type SetupHook = Box<dyn FnOnce(&ActorSystem) + Send + 'static>;

pub struct ActorSystemBuilder {
    name: String,
    config: Option<Config>,
    container: Arc<ServiceContainer>,
    setup_hooks: Vec<SetupHook>,
}

impl ActorSystemBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: None,
            container: Arc::new(ServiceContainer::new()),
            setup_hooks: Vec::new(),
        }
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_services<F>(self, f: F) -> Self
    where
        F: FnOnce(&ServiceContainer),
    {
        f(&self.container);
        self
    }

    pub fn on_start<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&ActorSystem) + Send + 'static,
    {
        self.setup_hooks.push(Box::new(f));
        self
    }

    pub async fn build(self) -> Result<HostedActorSystem, Box<dyn std::error::Error>> {
        let config = self.config.unwrap_or_else(Config::empty);
        let system = ActorSystem::create(self.name, config).await?;
        for hook in self.setup_hooks {
            hook(&system);
        }
        Ok(HostedActorSystem { system, container: self.container })
    }
}

pub struct HostedActorSystem {
    pub system: ActorSystem,
    pub container: Arc<ServiceContainer>,
}

impl HostedActorSystem {
    pub async fn terminate(&self) {
        self.system.terminate().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builder_constructs_system() {
        let hosted = ActorSystemBuilder::new("hosted").build().await.unwrap();
        hosted.terminate().await;
    }
}
