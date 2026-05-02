//! # rakka
//!
//! Umbrella re-export facade for the rakka workspace.
//! Mirrors the role of `Akka.csproj` in akka.net.
//!
//! Most users should use the [`prelude`] module.

pub use rakka_config as config;
pub use rakka_core as core;

#[cfg(feature = "macros")]
pub use rakka_macros as macros;

pub mod prelude {
    //! Commonly used types.
    pub use rakka_config::Config;
    pub use rakka_core::actor::{Actor, ActorRef, ActorSystem, Context, Props};
    pub use rakka_core::pattern::{ask, pipe_to};
    pub use rakka_core::supervision::{Directive, OneForOneStrategy, SupervisorStrategy};
    #[cfg(feature = "macros")]
    pub use rakka_macros::actor_msg;
}
