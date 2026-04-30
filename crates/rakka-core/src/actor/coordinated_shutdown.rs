//! Coordinated shutdown — phase-ordered cleanup hooks.
//! akka.net: `Actor/CoordinatedShutdown.cs`.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use parking_lot::Mutex;

/// Phases modeled on akka.net's defaults (subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    BeforeServiceUnbind = 0,
    ServiceUnbind = 1,
    ServiceRequestsDone = 2,
    ServiceStop = 3,
    BeforeClusterShutdown = 4,
    ClusterShardingShutdownRegion = 5,
    ClusterLeave = 6,
    ClusterExiting = 7,
    ClusterExitingDone = 8,
    ClusterShutdown = 9,
    BeforeActorSystemTerminate = 10,
    ActorSystemTerminate = 11,
}

type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Default, Clone)]
pub struct CoordinatedShutdown {
    inner: Arc<Mutex<BTreeMap<Phase, Vec<(String, Hook)>>>>,
}

impl CoordinatedShutdown {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_task<F>(&self, phase: Phase, name: impl Into<String>, hook: F)
    where
        F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        self.inner.lock().entry(phase).or_default().push((name.into(), Arc::new(hook)));
    }

    /// Run every phase's hooks sequentially within a phase and phases in
    /// ascending order.
    pub async fn run(&self) {
        let ordered: Vec<(Phase, Vec<(String, Hook)>)> = {
            let g = self.inner.lock();
            g.iter().map(|(k, v)| (*k, v.clone())).collect()
        };
        for (phase, hooks) in ordered {
            for (name, h) in hooks {
                tracing::debug!("running shutdown hook {name} in {phase:?}");
                h().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn runs_hooks_in_phase_order() {
        let cs = CoordinatedShutdown::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let c1 = counter.clone();
        cs.add_task(Phase::ServiceStop, "a", move || {
            let c = c1.clone();
            Box::pin(async move {
                assert_eq!(c.load(Ordering::SeqCst), 0);
                c.fetch_add(1, Ordering::SeqCst);
            })
        });
        let c2 = counter.clone();
        cs.add_task(Phase::ActorSystemTerminate, "b", move || {
            let c = c2.clone();
            Box::pin(async move {
                assert_eq!(c.load(Ordering::SeqCst), 1);
                c.fetch_add(1, Ordering::SeqCst);
            })
        });
        cs.run().await;
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
