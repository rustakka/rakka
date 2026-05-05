//! Mailbox configuration.
//!
//! The mailbox holds queued messages for a single actor. Our implementation
//! uses `tokio::mpsc::UnboundedReceiver` as the user queue and a separate
//! unbounded queue for system messages; `ActorCell` polls with system priority.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MailboxKind {
    #[default]
    Unbounded,
    Bounded,
    UnboundedDeque,
    UnboundedPriority,
    UnboundedStablePriority,
    /// Control messages bypass user messages.
    /// `UnboundedControlAwareMessageQueue`.
    UnboundedControlAware,
    /// Bounded variant with control-priority bypass.
    BoundedControlAware,
}

/// What a bounded mailbox does when its capacity is reached.
/// overflow policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverflowStrategy {
    /// Drop the new (incoming) message and log it as a dead letter.
    #[default]
    DropNew,
    /// Drop the oldest enqueued message and accept the new one.
    DropHead,
    /// Drop the most-recently-enqueued message and accept the new one.
    DropTail,
    /// Reject the push and signal failure to the sender.
    Fail,
}

#[derive(Debug, Clone)]
pub struct MailboxConfig {
    pub kind: MailboxKind,
    pub capacity: usize,
    pub push_timeout: Duration,
    /// Overflow policy used for bounded kinds. Ignored for unbounded.
    pub overflow: OverflowStrategy,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            kind: MailboxKind::Unbounded,
            capacity: 1_000,
            push_timeout: Duration::from_secs(10),
            overflow: OverflowStrategy::DropNew,
        }
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

    #[test]
    fn default_overflow_drops_new() {
        assert_eq!(MailboxConfig::default().overflow, OverflowStrategy::DropNew);
    }

    #[test]
    fn config_for_each_kind_is_constructible() {
        for k in [
            MailboxKind::Unbounded,
            MailboxKind::Bounded,
            MailboxKind::UnboundedDeque,
            MailboxKind::UnboundedPriority,
            MailboxKind::UnboundedStablePriority,
            MailboxKind::UnboundedControlAware,
            MailboxKind::BoundedControlAware,
        ] {
            let c = MailboxConfig { kind: k, ..Default::default() };
            assert_eq!(c.kind, k);
        }
    }
}
