//! Dispatcher + mailbox subsystem. akka.net: `src/core/Akka/Dispatch`.

pub mod dispatcher;
pub mod mailbox;
pub mod message_queues;

pub use dispatcher::{DefaultDispatcher, Dispatcher, DispatcherHandle, PinnedDispatcher};
pub use mailbox::{Mailbox, MailboxConfig, MailboxKind};
