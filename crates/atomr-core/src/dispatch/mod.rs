//! Dispatcher + mailbox subsystem. akka.net: `src/core/Akka/Dispatch`.

pub mod dispatcher;
pub mod mailbox;
pub mod message_queues;

pub use dispatcher::{
    CallingThreadDispatcher, DefaultDispatcher, Dispatcher, DispatcherHandle, PinnedDispatcher,
    ThreadPoolDispatcher,
};
pub use mailbox::{Mailbox, MailboxConfig, MailboxKind};
