//! Supervision. akka.net: `Actor/SupervisorStrategy.cs`.

use std::sync::Arc;
use std::time::Duration;

/// What the supervisor decides when a child fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Directive {
    Resume,
    Restart,
    Stop,
    Escalate,
}

pub type Decider = Arc<dyn Fn(&str) -> Directive + Send + Sync>;

/// Strategy applied to children of a supervising actor. Mirrors
/// akka.net's `OneForOneStrategy`/`AllForOneStrategy` split.
#[derive(Clone)]
pub struct SupervisorStrategy {
    pub kind: StrategyKind,
    pub max_retries: Option<u32>,
    pub within: Option<Duration>,
    pub decider: Decider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StrategyKind {
    OneForOne,
    AllForOne,
}

impl std::fmt::Debug for SupervisorStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorStrategy")
            .field("kind", &self.kind)
            .field("max_retries", &self.max_retries)
            .field("within", &self.within)
            .finish_non_exhaustive()
    }
}

impl Default for SupervisorStrategy {
    fn default() -> Self {
        OneForOneStrategy::default().into()
    }
}

impl SupervisorStrategy {
    pub fn decide(&self, err: &str) -> Directive {
        (self.decider)(err)
    }
}

/// Builder for `OneForOne` — the akka.net default.
pub struct OneForOneStrategy {
    pub max_retries: Option<u32>,
    pub within: Option<Duration>,
    pub decider: Decider,
}

impl Default for OneForOneStrategy {
    fn default() -> Self {
        Self {
            max_retries: Some(10),
            within: Some(Duration::from_secs(60)),
            decider: Arc::new(|_| Directive::Restart),
        }
    }
}

impl OneForOneStrategy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = Some(n);
        self
    }

    pub fn with_within(mut self, d: Duration) -> Self {
        self.within = Some(d);
        self
    }

    pub fn with_decider(mut self, f: impl Fn(&str) -> Directive + Send + Sync + 'static) -> Self {
        self.decider = Arc::new(f);
        self
    }
}

impl From<OneForOneStrategy> for SupervisorStrategy {
    fn from(o: OneForOneStrategy) -> Self {
        Self {
            kind: StrategyKind::OneForOne,
            max_retries: o.max_retries,
            within: o.within,
            decider: o.decider,
        }
    }
}

/// Builder for `AllForOne`.
pub struct AllForOneStrategy {
    pub max_retries: Option<u32>,
    pub within: Option<Duration>,
    pub decider: Decider,
}

impl Default for AllForOneStrategy {
    fn default() -> Self {
        Self {
            max_retries: Some(10),
            within: Some(Duration::from_secs(60)),
            decider: Arc::new(|_| Directive::Restart),
        }
    }
}

impl From<AllForOneStrategy> for SupervisorStrategy {
    fn from(o: AllForOneStrategy) -> Self {
        Self {
            kind: StrategyKind::AllForOne,
            max_retries: o.max_retries,
            within: o.within,
            decider: o.decider,
        }
    }
}

// -- Phase 1.D: typed `SupervisorOf<C>` trait ---------------------------
//
// Compile-time supervision contract. The legacy `SupervisorStrategy`
// value above (a closure-based decider held on `Props`) stays in
// place as the default runtime policy. `SupervisorOf<C>` layers an
// **opt-in** per-(parent, child) typed policy on top — see P-8 of
// `docs/idiomatic-rust.md` and Phase 1.D of
// `docs/full-port-plan.md`.
//
// Rust's coherence rules forbid a blanket-with-override pattern (no
// stable specialization), so `SupervisorOf<C>` is **not** auto-impl'd
// for every `(P, C)` pair. Actors that want compile-time-typed child
// supervision implement it explicitly:
//
// ```ignore
// impl SupervisorOf<Worker> for Boss {
//     type ChildError = WorkerError;
//     fn decide(&self, err: &WorkerError) -> Directive { … }
// }
// ```
//
// Actors without an explicit impl fall through to the legacy
// closure-based `Props::supervisor_strategy` at runtime — exactly
// the pre-Phase-1 behaviour. The forthcoming `Context::
// spawn_supervised::<C>(…)` (Phase 1.D follow-on) will require
// `Self: SupervisorOf<C>` so that opting into typed supervision is
// enforced at the call site.

use crate::actor::Actor;

/// A parent actor's typed supervision policy for a specific child
/// type `C`. Opt-in only — see module docs.
pub trait SupervisorOf<C: Actor> {
    /// The child's error type. Implementations choose this; the
    /// recommended pattern is one error enum per supervised child
    /// type.
    type ChildError: std::error::Error + Send + 'static;

    /// Decide what to do when the child fails with `err`. Defaults
    /// to `Restart`, mirroring akka.net's `OneForOneStrategy`
    /// default.
    fn decide(&self, _err: &Self::ChildError) -> Directive {
        Directive::Restart
    }
}

/// Generic boxed-string error suitable for `SupervisorOf` impls that
/// don't yet have a typed error story (e.g. wrapping a panic
/// payload). New code should prefer a domain-specific
/// `#[derive(thiserror::Error)]` enum instead.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct SupervisionError {
    pub message: String,
}

impl SupervisionError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, Context};

    #[test]
    fn default_is_one_for_one_restart() {
        let s = SupervisorStrategy::default();
        assert_eq!(s.kind, StrategyKind::OneForOne);
        assert_eq!(s.decide("boom"), Directive::Restart);
    }

    #[test]
    fn custom_decider_runs() {
        let s: SupervisorStrategy =
            OneForOneStrategy::new().with_decider(|e| if e == "stop" { Directive::Stop } else { Directive::Resume }).into();
        assert_eq!(s.decide("stop"), Directive::Stop);
        assert_eq!(s.decide("keep"), Directive::Resume);
    }

    // ---- SupervisorOf<C> --------------------------------------

    #[derive(Default)]
    struct Boss;
    #[derive(Default)]
    struct Worker;

    #[async_trait::async_trait]
    impl Actor for Boss {
        type Msg = ();
        async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
    }

    #[async_trait::async_trait]
    impl Actor for Worker {
        type Msg = ();
        async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
    }

    #[derive(Debug, thiserror::Error)]
    #[error("worker died: {0}")]
    struct WorkerError(String);

    impl SupervisorOf<Worker> for Boss {
        type ChildError = WorkerError;
        fn decide(&self, _err: &WorkerError) -> Directive {
            Directive::Stop
        }
    }

    #[test]
    fn explicit_impl_is_resolvable_with_typed_error() {
        fn assert_typed_decider<P: SupervisorOf<C, ChildError = WorkerError>, C: Actor>() {}
        assert_typed_decider::<Boss, Worker>();
    }

    #[test]
    fn typed_decider_runs() {
        let boss = Boss;
        let err = WorkerError("oops".into());
        let d = SupervisorOf::<Worker>::decide(&boss, &err);
        assert_eq!(d, Directive::Stop);
    }

    /// Demonstrates the recommended pattern of using
    /// [`SupervisionError`] for actors that don't yet have a typed
    /// child-error enum. Future PRs replace this with the domain
    /// error.
    #[test]
    fn supervision_error_works_as_default_child_error() {
        struct Default42;
        #[async_trait::async_trait]
        impl Actor for Default42 {
            type Msg = ();
            async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
        }
        struct AnyParent;
        #[async_trait::async_trait]
        impl Actor for AnyParent {
            type Msg = ();
            async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
        }
        impl SupervisorOf<Default42> for AnyParent {
            type ChildError = SupervisionError;
        }
        let p = AnyParent;
        let err = SupervisionError::new("crash");
        assert_eq!(SupervisorOf::<Default42>::decide(&p, &err), Directive::Restart);
    }

    #[test]
    fn supervision_error_displays_message() {
        let e = SupervisionError::new("halt");
        assert_eq!(e.to_string(), "halt");
    }
}
