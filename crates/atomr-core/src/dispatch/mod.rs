//! Dispatcher + mailbox subsystem.

pub mod dispatcher;
pub mod mailbox;
pub mod message_queues;

pub use dispatcher::{
    CallingThreadDispatcher, DefaultDispatcher, Dispatcher, DispatcherConfig, DispatcherHandle,
    PinnedDispatcher, SingleThreadDispatcher, ThreadPoolDispatcher,
};
pub use mailbox::{Mailbox, MailboxConfig, MailboxKind, OverflowStrategy};
pub use message_queues::{
    BoundedMsgQueue, ControlAware, ControlAwareQueue, DequeQueue, Prioritized, PriorityQueue, PushOutcome,
    StablePriorityQueue, UnboundedQueue,
};
