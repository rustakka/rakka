//! [`EventCodecRegistry`] — manifest-keyed event decoder dispatch.
//!
//! Use it when an aggregate's event schema evolves. For each
//! historical manifest your journal might contain, register a decoder
//! that returns the *current* `Event` type:
//!
//! ```ignore
//! let registry = EventCodecRegistry::<OrderEvent>::new()
//!     .register("order-evt-v1", |b| OrderEventV1::decode(b).map(Into::into))
//!     .register("order-evt-v2", |b| OrderEventV2::decode(b))
//!     .with_default(|b| OrderEventV2::decode(b));
//!
//! CqrsPattern::<Order>::builder(journal)
//!     .factory(...)
//!     .with_event_codecs(registry)
//!     .with_reader(MyReader)
//!     .build()?
//! ```
//!
//! On replay, the runner inspects each
//! [`atomr_persistence_query::EventEnvelope::manifest`] and dispatches
//! to the matching registered decoder. When no entry matches, the
//! reader's [`crate::cqrs::Reader::decode`] is used as a final
//! fallback.

use std::collections::HashMap;
use std::sync::Arc;

type Decoder<E> = Arc<dyn Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static>;

/// Manifest -> decoder map plus an optional catch-all decoder.
pub struct EventCodecRegistry<E: Send + 'static> {
    pub(crate) decoders: HashMap<String, Decoder<E>>,
    pub(crate) default: Option<Decoder<E>>,
}

impl<E: Send + 'static> Default for EventCodecRegistry<E> {
    fn default() -> Self {
        Self { decoders: HashMap::new(), default: None }
    }
}

impl<E: Send + 'static> EventCodecRegistry<E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `decode` as the decoder for events written with
    /// `manifest`.
    pub fn register<F>(mut self, manifest: impl Into<String>, decode: F) -> Self
    where
        F: Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static,
    {
        self.decoders.insert(manifest.into(), Arc::new(decode));
        self
    }

    /// Set a catch-all decoder used when no manifest matches.
    pub fn with_default<F>(mut self, decode: F) -> Self
    where
        F: Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static,
    {
        self.default = Some(Arc::new(decode));
        self
    }

    /// Look up a decoder for `manifest` or fall back to the default.
    pub fn decode(&self, manifest: &str, bytes: &[u8]) -> Option<Result<E, String>> {
        if let Some(decoder) = self.decoders.get(manifest) {
            return Some(decoder(bytes));
        }
        self.default.as_ref().map(|d| d(bytes))
    }
}
