//! Mailbox configuration. akka.net: `Dispatch/Mailbox.cs`, `Mailboxes.cs`.
//!
//! The mailbox holds queued messages for a single actor. Our implementation
//! uses `tokio::mpsc::UnboundedReceiver` as the user queue and a separate
//! unbounded queue for system messages; `ActorCell` polls with system priority.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailboxKind {
    Unbounded,
    Bounded,
    UnboundedDeque,
    UnboundedPriority,
    UnboundedStablePriority,
}

impl Default for MailboxKind {
    fn default() -> Self {
        Self::Unbounded
    }
}

#[derive(Debug, Clone)]
pub struct MailboxConfig {
    pub kind: MailboxKind,
    pub capacity: usize,
    pub push_timeout: Duration,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self { kind: MailboxKind::Unbounded, capacity: 1_000, push_timeout: Duration::from_secs(10) }
    }
}

/// Marker type exposed publicly — the concrete storage lives inside
/// `ActorCell` (tokio mpsc channels and an auxiliary priority heap when needed).
#[derive(Debug, Clone, Default)]
pub struct Mailbox {
    pub config: MailboxConfig,
}

impl Mailbox {
    pub fn new(config: MailboxConfig) -> Self {
        Self { config }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_mailbox_kind_unbounded() {
        assert_eq!(Mailbox::default().config.kind, MailboxKind::Unbounded);
    }
}
