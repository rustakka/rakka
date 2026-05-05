//! Coordinated shutdown — phase-ordered cleanup hooks.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use parking_lot::Mutex;

/// Phases modeled on defaults.
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

impl Phase {
    /// All phases in ascending execution order.
    pub const ALL: &'static [Phase] = &[
        Phase::BeforeServiceUnbind,
        Phase::ServiceUnbind,
        Phase::ServiceRequestsDone,
        Phase::ServiceStop,
        Phase::BeforeClusterShutdown,
        Phase::ClusterShardingShutdownRegion,
        Phase::ClusterLeave,
        Phase::ClusterExiting,
        Phase::ClusterExitingDone,
        Phase::ClusterShutdown,
        Phase::BeforeActorSystemTerminate,
        Phase::ActorSystemTerminate,
    ];
}

/// Per-phase timeout — if any hook exceeds this duration the phase
/// is abandoned and (if `recover`) the next phase begins.
/// `phase.timeout` and `recover`.
#[derive(Debug, Clone)]
pub struct PhaseConfig {
    pub timeout: Duration,
    /// If true, run the next phase even if this one timed out.
    pub recover: bool,
}

impl Default for PhaseConfig {
    fn default() -> Self {
        Self { timeout: Duration::from_secs(5), recover: true }
    }
}

type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;
type PhaseHooks = BTreeMap<Phase, Vec<(String, Hook)>>;

#[derive(Default, Clone)]
pub struct CoordinatedShutdown {
    inner: Arc<Mutex<PhaseHooks>>,
    phase_configs: Arc<Mutex<BTreeMap<Phase, PhaseConfig>>>,
    started: Arc<Mutex<bool>>,
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

    /// Configure the timeout / recovery policy for a single phase.
    pub fn configure_phase(&self, phase: Phase, config: PhaseConfig) {
        self.phase_configs.lock().insert(phase, config);
    }

    /// Has shutdown already been initiated?
    /// `CoordinatedShutdown.IsRunning`.
    pub fn is_running(&self) -> bool {
        *self.started.lock()
    }

    /// Run every phase's hooks sequentially within a phase and phases
    /// in ascending order. Idempotent: repeated calls are no-ops.
    pub async fn run(&self) {
        self.run_from(Phase::BeforeServiceUnbind).await
    }

    /// Run only phases at or after `start`.
    pub async fn run_from(&self, start: Phase) {
        {
            let mut g = self.started.lock();
            if *g {
                return;
            }
            *g = true;
        }
        let ordered: Vec<(Phase, Vec<(String, Hook)>)> = {
            let g = self.inner.lock();
            g.iter().filter(|(p, _)| **p >= start).map(|(k, v)| (*k, v.clone())).collect()
        };
        let configs = self.phase_configs.lock().clone();
        for (phase, hooks) in ordered {
            let cfg = configs.get(&phase).cloned().unwrap_or_default();
            let phase_run = async {
                for (name, h) in &hooks {
                    tracing::debug!("running shutdown hook {name} in {phase:?}");
                    h().await;
                }
            };
            match tokio::time::timeout(cfg.timeout, phase_run).await {
                Ok(()) => {}
                Err(_) => {
                    tracing::warn!(?phase, ?cfg, "coordinated-shutdown phase timed out");
                    if !cfg.recover {
                        return;
                    }
                }
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

    #[tokio::test]
    async fn run_is_idempotent() {
        let cs = CoordinatedShutdown::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        cs.add_task(Phase::ServiceStop, "once", move || {
            let c = c.clone();
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
            })
        });
        cs.run().await;
        assert!(cs.is_running());
        cs.run().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1, "second run() should be a no-op");
    }

    #[tokio::test]
    async fn run_from_skips_earlier_phases() {
        let cs = CoordinatedShutdown::new();
        let early = Arc::new(AtomicUsize::new(0));
        let late = Arc::new(AtomicUsize::new(0));
        let e = early.clone();
        cs.add_task(Phase::ServiceUnbind, "early", move || {
            let e = e.clone();
            Box::pin(async move {
                e.fetch_add(1, Ordering::SeqCst);
            })
        });
        let l = late.clone();
        cs.add_task(Phase::ClusterShutdown, "late", move || {
            let l = l.clone();
            Box::pin(async move {
                l.fetch_add(1, Ordering::SeqCst);
            })
        });
        cs.run_from(Phase::ClusterLeave).await;
        assert_eq!(early.load(Ordering::SeqCst), 0, "earlier phase should have been skipped");
        assert_eq!(late.load(Ordering::SeqCst), 1, "later phase should have run");
    }

    #[tokio::test]
    async fn phase_timeout_aborts_and_recovers() {
        let cs = CoordinatedShutdown::new();
        cs.configure_phase(
            Phase::ServiceStop,
            PhaseConfig { timeout: Duration::from_millis(20), recover: true },
        );
        cs.add_task(Phase::ServiceStop, "slow", || {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
        });
        let next_ran = Arc::new(AtomicUsize::new(0));
        let n = next_ran.clone();
        cs.add_task(Phase::ActorSystemTerminate, "next", move || {
            let n = n.clone();
            Box::pin(async move {
                n.fetch_add(1, Ordering::SeqCst);
            })
        });
        cs.run().await;
        assert_eq!(next_ran.load(Ordering::SeqCst), 1, "recover=true should run later phases");
    }

    #[tokio::test]
    async fn phase_timeout_without_recover_halts() {
        let cs = CoordinatedShutdown::new();
        cs.configure_phase(
            Phase::ServiceStop,
            PhaseConfig { timeout: Duration::from_millis(20), recover: false },
        );
        cs.add_task(Phase::ServiceStop, "slow", || {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
        });
        let later = Arc::new(AtomicUsize::new(0));
        let l = later.clone();
        cs.add_task(Phase::ActorSystemTerminate, "later", move || {
            let l = l.clone();
            Box::pin(async move {
                l.fetch_add(1, Ordering::SeqCst);
            })
        });
        cs.run().await;
        assert_eq!(later.load(Ordering::SeqCst), 0, "recover=false should halt");
    }

    #[test]
    fn phase_all_is_ascending() {
        let mut prev = Phase::BeforeServiceUnbind;
        for &p in Phase::ALL.iter().skip(1) {
            assert!(p > prev, "Phase::ALL not ascending: {prev:?} → {p:?}");
            prev = p;
        }
    }
}
