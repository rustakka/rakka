//! Supervision. akka.net: `Actor/SupervisorStrategy.cs`.

use std::sync::Arc;
use std::time::Duration;

/// What the supervisor decides when a child fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
