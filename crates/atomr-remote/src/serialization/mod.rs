//! Pluggable serializer registry for remote payloads.
//! akka.net: `Remote/Serialization/MessageSerializer.cs` +
//! `Akka.Serialization` core.
//!
//! Every payload that crosses the wire carries a `serializer_id` and a
//! `manifest` (type name). On the receiving side those two fields key
//! into the [`SerializerRegistry`] to recover the original Rust type.
//!
//! Built-in serializers:
//!
//! | id | name      | wire format        |
//! |----|-----------|--------------------|
//! | 0  | system    | bincode (control)  |
//! | 1  | bincode   | bincode v2 + serde |
//! | 2  | json      | serde_json         |

mod bincode_codec;
mod json_codec;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;

pub use bincode_codec::{bincode_decode, bincode_encode, system_decode, system_encode};
pub use json_codec::{json_decode, json_encode};

pub const SYSTEM_SERIALIZER_ID: u32 = 0;
pub const BINCODE_SERIALIZER_ID: u32 = 1;
pub const JSON_SERIALIZER_ID: u32 = 2;

#[derive(Debug, Error)]
pub enum SerializeError {
    #[error("serialization failed: {0}")]
    Encode(String),
    #[error("deserialization failed: {0}")]
    Decode(String),
    #[error("no serializer registered for manifest={0}")]
    UnknownManifest(String),
    #[error("downcast failed for manifest={0}")]
    Downcast(String),
}

type EncodeFn = Arc<dyn Fn(&dyn Any) -> Result<Vec<u8>, SerializeError> + Send + Sync>;
type DecodeFn = Arc<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send>, SerializeError> + Send + Sync>;

/// Closure pair that knows how to encode/decode one Rust type to/from
/// wire bytes. The registry maps a `manifest` (type name) to one of these.
#[derive(Clone)]
pub struct TypeCodec {
    pub serializer_id: u32,
    pub manifest: String,
    pub type_id: TypeId,
    pub encode: EncodeFn,
    pub decode: DecodeFn,
}

/// Per-system registry mapping a `manifest` to a [`TypeCodec`].
#[derive(Default, Clone)]
pub struct SerializerRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

#[derive(Default)]
struct RegistryInner {
    by_manifest: HashMap<String, TypeCodec>,
    by_type_id: HashMap<TypeId, TypeCodec>,
}

impl SerializerRegistry {
    /// Empty registry. Most callers want [`SerializerRegistry::standard`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Registry pre-populated with codecs for the system control payloads
    /// (see `bincode_codec::register_system_payloads`).
    pub fn standard() -> Self {
        let r = Self::new();
        bincode_codec::register_system_payloads(&r);
        r
    }

    /// Register a type with its serialization closures. The `manifest`
    /// must match the Rust `std::any::type_name::<T>()` if you want
    /// `encode_typed::<T>` to find it without an explicit manifest.
    pub fn register_codec(&self, codec: TypeCodec) {
        let mut g = self.inner.write();
        g.by_type_id.insert(codec.type_id, codec.clone());
        g.by_manifest.insert(codec.manifest.clone(), codec);
    }

    /// Convenience: register `T` with the bincode codec (id=1).
    pub fn register_bincode<T>(&self)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    {
        let manifest = std::any::type_name::<T>().to_string();
        self.register_codec(TypeCodec {
            serializer_id: BINCODE_SERIALIZER_ID,
            manifest: manifest.clone(),
            type_id: TypeId::of::<T>(),
            encode: Arc::new(|v: &dyn Any| {
                let v = v
                    .downcast_ref::<T>()
                    .ok_or_else(|| SerializeError::Downcast(std::any::type_name::<T>().to_string()))?;
                bincode_encode(v)
            }),
            decode: Arc::new(|b: &[u8]| {
                let v: T = bincode_decode(b)?;
                Ok(Box::new(v) as Box<dyn Any + Send>)
            }),
        });
    }

    /// Convenience: register `T` with the JSON codec (id=2).
    pub fn register_json<T>(&self)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    {
        let manifest = std::any::type_name::<T>().to_string();
        self.register_codec(TypeCodec {
            serializer_id: JSON_SERIALIZER_ID,
            manifest: manifest.clone(),
            type_id: TypeId::of::<T>(),
            encode: Arc::new(|v: &dyn Any| {
                let v = v
                    .downcast_ref::<T>()
                    .ok_or_else(|| SerializeError::Downcast(std::any::type_name::<T>().to_string()))?;
                json_encode(v)
            }),
            decode: Arc::new(|b: &[u8]| {
                let v: T = json_decode(b)?;
                Ok(Box::new(v) as Box<dyn Any + Send>)
            }),
        });
    }

    pub fn codec_for_manifest(&self, manifest: &str) -> Option<TypeCodec> {
        self.inner.read().by_manifest.get(manifest).cloned()
    }

    pub fn codec_for_type<T: Any>(&self) -> Option<TypeCodec> {
        self.inner.read().by_type_id.get(&TypeId::of::<T>()).cloned()
    }

    /// Encode a typed value, looking up its codec by `TypeId`. Returns
    /// `(serializer_id, manifest, bytes)` so the caller can fill the
    /// envelope.
    pub fn encode_typed<T: Any + Send>(&self, value: &T) -> Result<(u32, String, Vec<u8>), SerializeError> {
        let codec = self
            .codec_for_type::<T>()
            .ok_or_else(|| SerializeError::UnknownManifest(std::any::type_name::<T>().to_string()))?;
        let bytes = (codec.encode)(value as &dyn Any)?;
        Ok((codec.serializer_id, codec.manifest.clone(), bytes))
    }

    /// Decode an inbound payload. Returns the type-erased value plus the
    /// codec used (so the caller can downcast).
    pub fn decode_dyn(
        &self,
        manifest: &str,
        _serializer_id: u32,
        bytes: &[u8],
    ) -> Result<(Box<dyn Any + Send>, TypeCodec), SerializeError> {
        let codec = self
            .codec_for_manifest(manifest)
            .ok_or_else(|| SerializeError::UnknownManifest(manifest.to_string()))?;
        let value = (codec.decode)(bytes)?;
        Ok((value, codec))
    }
}
