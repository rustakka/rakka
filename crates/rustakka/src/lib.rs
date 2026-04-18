//! # rustakka
//!
//! Umbrella re-export facade for the rustakka workspace.
//! Mirrors the role of `Akka.csproj` in akka.net.
//!
//! Most users should use the [`prelude`] module.

pub use rustakka_core as core;
pub use rustakka_config as config;

#[cfg(feature = "macros")]
pub use rustakka_macros as macros;

pub mod prelude {
    //! Commonly used types.
    pub use rustakka_config::Config;
    pub use rustakka_core::actor::{Actor, ActorRef, ActorSystem, Context, Props};
    pub use rustakka_core::pattern::{ask, pipe_to};
    pub use rustakka_core::supervision::{Directive, OneForOneStrategy, SupervisorStrategy};
    #[cfg(feature = "macros")]
    pub use rustakka_macros::actor_msg;
}
