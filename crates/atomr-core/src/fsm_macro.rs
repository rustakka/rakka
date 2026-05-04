//! `fsm!` declarative macro — terse `FiniteStateMachine` impls.
//!
//! ```ignore
//! use atomr_core::fsm;
//!
//! #[derive(Clone, Eq, PartialEq, Debug)]
//! enum Light { Idle, Running }
//! enum Cmd { Go, Stop }
//!
//! struct Traffic;
//!
//! fsm! {
//!     Traffic, state = Light, data = u32, msg = Cmd;
//!     initial state = Light::Idle, data = 0;
//!     (Light::Idle, Cmd::Go) => |s, d| (Light::Running, *d + 1, None);
//!     (Light::Running, Cmd::Stop) => |s, d| (Light::Idle, *d, None);
//! }
//! ```
//!
//! Each arm receives `(state: &State, data: &Data)` and returns
//! `(next_state, next_data, Option<Duration>)`.

#[macro_export]
macro_rules! fsm {
    (
        $self_ty:ty,
        state = $state:ty,
        data = $data:ty,
        msg = $msg:ty;
        initial state = $init_state:expr, data = $init_data:expr;
        $(
            ($pat_state:pat, $pat_msg:pat) => |$s:ident, $d:ident| ($next:expr, $ndata:expr, $timeout:expr);
        )+
    ) => {
        impl $crate::actor::FiniteStateMachine for $self_ty {
            type State = $state;
            type Data = $data;
            type Msg = $msg;

            fn initial_state(&self) -> Self::State { $init_state }
            fn initial_data(&self) -> Self::Data { $init_data }

            fn transition(
                &mut self,
                current: &Self::State,
                data: &Self::Data,
                msg: Self::Msg,
            ) -> ::core::option::Option<$crate::actor::FsmTransition<Self::State, Self::Data>> {
                match (current, msg) {
                    $(
                        ($pat_state, $pat_msg) => {
                            let $s = current;
                            let $d = data;
                            ::core::option::Option::Some($crate::actor::FsmTransition {
                                next: $next,
                                data: $ndata,
                                timeout: $timeout,
                            })
                        }
                    )+
                    _ => ::core::option::Option::None,
                }
            }
        }
    };
}
