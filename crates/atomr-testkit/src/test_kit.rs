//! `TestKit` — a thin wrapper around an `ActorSystem` that tests use for
//! defaults and lifetime management. akka.net: `Akka.TestKit/TestKitBase.cs`.

use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;

use crate::probe::TestProbe;

pub struct TestKit {
    pub system: ActorSystem,
    pub default_timeout: Duration,
}

impl TestKit {
    pub async fn new(name: &str) -> Self {
        let system = ActorSystem::create(name, Config::reference()).await.expect("create system");
        Self { system, default_timeout: Duration::from_secs(3) }
    }

    pub fn probe<M: Send + 'static>(&self, name: &str) -> TestProbe<M> {
        TestProbe::new(name)
    }

    pub async fn shutdown(self) {
        self.system.terminate().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn kit_boots_and_shuts_down() {
        let kit = TestKit::new("kit").await;
        assert_eq!(kit.system.name(), "kit");
        kit.shutdown().await;
    }
}
