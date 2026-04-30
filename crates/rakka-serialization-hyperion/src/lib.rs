//! rakka-serialization-hyperion.
//!
//! Hyperion is a CLR-specific binary serializer tightly coupled to
//! System.Reflection. A line-by-line port is impractical in Rust. To keep
//! the crate name reserved (matching Akka.NET layout) while providing
//! useful functionality, this crate exposes a Serde/bincode-based
//! `HyperionSerializer<T>` that implements the same
//! [`rakka_core::serialization::Serializer`] trait used everywhere
//! else. Wire format is **not** compatible with CLR Hyperion and is only
//! meant to be pluggable as the default binary serializer for a pure-Rust
//! rakka deployment.

use std::marker::PhantomData;

use bincode::config;
use rakka_core::serialization::{Serializer, SerializerError};
use serde::{de::DeserializeOwned, Serialize};

/// Default identifier for the Hyperion-compat slot. akka.net reserves
/// serializer id 7 for Hyperion; we keep the same number for parity.
pub const HYPERION_SERIALIZER_ID: u32 = 7;

pub struct HyperionSerializer<T> {
    id: u32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> HyperionSerializer<T> {
    pub fn new() -> Self {
        Self { id: HYPERION_SERIALIZER_ID, _marker: PhantomData }
    }

    pub fn with_id(id: u32) -> Self {
        Self { id, _marker: PhantomData }
    }
}

impl<T> Default for HyperionSerializer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Serializer<T> for HyperionSerializer<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn identifier(&self) -> u32 {
        self.id
    }

    fn manifest(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    fn to_bytes(&self, value: &T) -> Result<Vec<u8>, SerializerError> {
        bincode::serde::encode_to_vec(value, config::standard())
            .map_err(|e| SerializerError::Encode(e.to_string()))
    }

    fn from_bytes(&self, bytes: &[u8]) -> Result<T, SerializerError> {
        let (v, _) = bincode::serde::decode_from_slice::<T, _>(bytes, config::standard())
            .map_err(|e| SerializerError::Decode(e.to_string()))?;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Payload {
        id: u32,
        name: String,
        tags: Vec<String>,
    }

    #[test]
    fn round_trip_struct() {
        let s = HyperionSerializer::<Payload>::new();
        let p = Payload {
            id: 42,
            name: "rakka".into(),
            tags: vec!["cluster".into(), "streams".into()],
        };
        let bytes = s.to_bytes(&p).unwrap();
        let back = s.from_bytes(&bytes).unwrap();
        assert_eq!(back, p);
        assert_eq!(s.identifier(), HYPERION_SERIALIZER_ID);
    }

    #[test]
    fn identifier_is_overridable() {
        let s = HyperionSerializer::<u32>::with_id(42);
        assert_eq!(s.identifier(), 42);
    }
}
