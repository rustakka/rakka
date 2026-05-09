//! Pattern-level errors.

use atomr_core::actor::AskError;
use atomr_persistence::JournalError;

/// Errors raised by pattern-level operations.
///
/// Generic over the user's domain error `E`. Mirrors the shape of
/// [`atomr_persistence::EventsourcedError`] so downstream code can
/// pattern-match consistently.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PatternError<E> {
    #[error(transparent)]
    Domain(E),

    #[error("journal error: {0}")]
    Journal(#[from] JournalError),

    #[error("ask: {0}")]
    Ask(#[from] AskError),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("invariant violation: {0}")]
    Invariant(String),

    #[error("pattern not configured: {0}")]
    NotConfigured(&'static str),

    #[error("intercepted: {0}")]
    Intercepted(String),

    #[error("reply channel dropped")]
    ReplyDropped,
}
