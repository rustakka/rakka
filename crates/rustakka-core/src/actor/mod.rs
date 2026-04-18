//! The `actor` subsystem. akka.net: `src/core/Akka/Actor`.
//!
//! Everything user-visible about actors lives here: [`Actor`], [`ActorRef`],
//! [`ActorSystem`], [`Props`], [`Context`], plus paths/addresses and the
//! internal `ActorCell` machinery.

mod actor_cell;
mod actor_ref;
mod actor_system;
mod address;
mod context;
mod coordinated_shutdown;
mod deploy;
mod extensions;
mod fsm;
mod inbox;
mod path;
mod props;
mod provider;
pub mod scheduler;
mod stash;
mod traits;

pub use actor_cell::{ActorCell, SystemMsg};
pub use actor_ref::{ActorRef, AskError, UntypedActorRef};
pub use actor_system::{ActorSystem, ActorSystemError};
pub use address::Address;
pub use context::Context;
pub use coordinated_shutdown::{CoordinatedShutdown, Phase};
pub use deploy::{Deploy, Scope};
pub use extensions::{Extension, ExtensionId};
pub use fsm::{FiniteStateMachine, FsmTransition};
pub use inbox::Inbox;
pub use path::{ActorPath, PathElement};
pub use props::{BoxedProps, Props};
pub use provider::{ActorRefProvider, LocalActorRefProvider};
pub use stash::Stash;
pub use traits::{Actor, MessageEnvelope};
