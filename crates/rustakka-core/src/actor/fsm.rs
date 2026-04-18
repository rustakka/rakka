//! Finite state machine DSL. akka.net: `Actor/FSM.cs`.

use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsmTransition<S, D> {
    pub next: S,
    pub data: D,
    pub timeout: Option<Duration>,
}

/// Simple trait-based FSM. Actors implementing this trait are driven by
/// `ctx.become(...)` inside their cell.
pub trait FiniteStateMachine {
    type State: Clone + Eq + 'static;
    type Data: Clone + 'static;
    type Msg: Send + 'static;

    fn initial_state(&self) -> Self::State;
    fn initial_data(&self) -> Self::Data;

    fn transition(
        &mut self,
        current: &Self::State,
        data: &Self::Data,
        msg: Self::Msg,
    ) -> Option<FsmTransition<Self::State, Self::Data>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Eq, PartialEq, Debug)]
    enum S {
        Idle,
        Running,
    }

    struct TrafficLight;
    enum M {
        Go,
        Stop,
    }

    impl FiniteStateMachine for TrafficLight {
        type State = S;
        type Data = u32;
        type Msg = M;

        fn initial_state(&self) -> S {
            S::Idle
        }
        fn initial_data(&self) -> u32 {
            0
        }

        fn transition(&mut self, s: &S, d: &u32, m: M) -> Option<FsmTransition<S, u32>> {
            match (s, m) {
                (S::Idle, M::Go) => Some(FsmTransition { next: S::Running, data: d + 1, timeout: None }),
                (S::Running, M::Stop) => Some(FsmTransition { next: S::Idle, data: *d, timeout: None }),
                _ => None,
            }
        }
    }

    #[test]
    fn transitions_idle_to_running() {
        let mut fsm = TrafficLight;
        let t = fsm.transition(&S::Idle, &0, M::Go).unwrap();
        assert_eq!(t.next, S::Running);
        assert_eq!(t.data, 1);
    }
}
