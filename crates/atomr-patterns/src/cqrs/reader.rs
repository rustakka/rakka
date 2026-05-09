//! [`Reader`] — read-side fold from journal events into a projection.
//!
//! Users implement this trait once per read model. `decode` deserializes
//! the journal payload into the event type; `apply` folds events into
//! the projection. The framework runs an async loop that polls the
//! configured [`atomr_persistence_query::ReadJournal`] and drives this
//! trait.

use async_trait::async_trait;

/// What stream of events the reader subscribes to. Selected via
/// [`Reader::filter`]; defaults to [`ReaderFilter::All`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReaderFilter {
    /// Every event from every persistence id.
    All,
    /// Only events whose [`crate::DomainEvent::tags`] contains this tag.
    Tag(String),
    /// Only events from the given persistence id.
    PersistenceId(String),
    /// Only events whose persistence id is in the given set.
    PersistenceIds(Vec<String>),
}

/// Fold journal events into a projection.
///
/// The runner polls the configured read journal, decodes each
/// [`atomr_persistence_query::EventEnvelope`]'s payload into
/// `Self::Event` via [`Reader::decode`], optionally filters by
/// [`Reader::tag`], and calls [`Reader::apply`] per event. Per-pid
/// offsets are tracked internally so each event is applied exactly
/// once per process lifetime.
#[async_trait]
pub trait Reader: Send + 'static {
    /// The event type this reader projects. Must match the aggregate's
    /// event type when wired into a [`super::CqrsPattern`].
    type Event: Send + Clone + 'static;

    /// The read-model state this reader builds.
    type Projection: Default + Send + Sync + 'static;

    /// Domain error type for projection failures. Failures are logged
    /// at `warn` level; the runner advances past the offending event so
    /// it doesn't get stuck.
    type Error: std::error::Error + Send + 'static;

    /// Stable name of this reader. Used for tracing spans and
    /// dashboard child-actor naming. Must be unique per CQRS instance.
    fn name(&self) -> &str;

    /// Legacy tag filter. Default `None`. Implemented in terms of
    /// [`Self::filter`] so existing v1 readers keep working unchanged.
    /// Prefer [`Self::filter`] in new code.
    fn tag(&self) -> Option<String> {
        None
    }

    /// What stream of events this reader follows. Default returns
    /// [`ReaderFilter::Tag`] when [`Self::tag`] is `Some`, else
    /// [`ReaderFilter::All`].
    fn filter(&self) -> ReaderFilter {
        match self.tag() {
            Some(t) => ReaderFilter::Tag(t),
            None => ReaderFilter::All,
        }
    }

    /// Decode a journal payload back into the event type. The codec
    /// must be the inverse of the aggregate's `encode_event`. Used
    /// as the fallback when no [`crate::cqrs::EventCodecRegistry`] is
    /// configured for the relevant `manifest`.
    fn decode(bytes: &[u8]) -> Result<Self::Event, String>;

    /// Apply one event to the projection.
    async fn apply(
        &mut self,
        projection: &mut Self::Projection,
        event: Self::Event,
    ) -> Result<(), Self::Error>;
}
