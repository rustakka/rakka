//! [`DomainEvent`] — a fact about something that happened in the domain.

/// A persisted fact. Supplies optional metadata the read side needs:
///
/// * [`DomainEvent::tags`] — categorization keys for `events_by_tag`
///   subscriptions in [`atomr_persistence_query::ReadJournal`].
/// * [`DomainEvent::correlation_id`] — threads related events across
///   aggregates so a [`crate::saga::Saga`] can correlate them.
pub trait DomainEvent: Clone + Send + 'static {
    /// Tags applied to this event in the journal. Default: none.
    /// Tags are how readers subscribe to *categories* of events
    /// instead of specific persistence ids.
    fn tags(&self) -> Vec<String> {
        Vec::new()
    }

    /// Correlation id used by sagas / process managers to thread
    /// related events together. Default: none — no correlation.
    fn correlation_id(&self) -> Option<&str> {
        None
    }
}
