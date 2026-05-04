//! Pluggable Python codec for `atomr-remote`. Phase P3 / 5.K of
//! `docs/full-port-plan.md` and `PORTING_TODO.md`.
//!
//! akka.net's remoting allows users to plug in language-specific
//! serializers (e.g. Hyperion for .NET, Java serialization for the JVM
//! port). The Python bindings need an analogous knob — Python users
//! want their actor messages serialized via JSON / msgpack / pickle.
//!
//! This crate provides:
//! * [`PyCodec`] — a small trait an embedder implements once per Python
//!   codec (JSON / pickle / msgpack — pickle requires the GIL so the
//!   trait is GIL-agnostic by accepting raw bytes).
//! * [`PyCodecRegistry`] — a per-system map keyed by `manifest` that
//!   plugs into [`atomr_remote::SerializerRegistry`] via [`as_remote_serializer`].
//! * [`JsonCodec`] / [`json_codec`] — a built-in JSON codec for tests
//!   and the simplest Python use-case.
//!
//! The actual `pyo3` plumbing lives in `crates/py-bindings/pycore` —
//! that crate adds a thin wrapper that converts a Python callable into
//! a [`PyCodec`]. We deliberately keep the dependency on `pyo3` out of
//! this crate so Rust-side tests can run without a Python interpreter.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;

use atomr_remote::SerializerRegistry;

/// Error variants from the codec layer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PyCodecError {
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("no codec registered for manifest `{0}`")]
    UnknownManifest(String),
}

/// Trait every Python codec implements. The codec is responsible for
/// serializing and deserializing arbitrary opaque bytes — typed
/// dispatch happens on the `manifest` string (akka.net parity:
/// the manifest is the payload's logical class name).
pub trait PyCodec: Send + Sync + 'static {
    /// Stable identifier (e.g. "json", "msgpack", "pickle").
    fn id(&self) -> &str;

    /// Serialize a Python-side payload (already converted to bytes by
    /// the caller — most codecs are byte-in-byte-out).
    fn encode(&self, manifest: &str, payload: &[u8]) -> Result<Vec<u8>, PyCodecError>;

    /// Deserialize. Returns the bytes the Python side should hand to
    /// `pickle.loads` / `json.loads` / etc.
    fn decode(&self, manifest: &str, blob: &[u8]) -> Result<Vec<u8>, PyCodecError>;
}

/// Per-actor-system codec registry. Keyed by manifest string.
#[derive(Default, Clone)]
pub struct PyCodecRegistry {
    inner: Arc<RwLock<HashMap<String, Arc<dyn PyCodec>>>>,
}

impl PyCodecRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `codec` for every manifest in `manifests`. Replaces an
    /// existing registration if the manifest is already mapped.
    pub fn register<I, S>(&self, codec: Arc<dyn PyCodec>, manifests: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut g = self.inner.write();
        for m in manifests {
            g.insert(m.into(), codec.clone());
        }
    }

    /// Encode `payload` under `manifest`.
    pub fn encode(&self, manifest: &str, payload: &[u8]) -> Result<Vec<u8>, PyCodecError> {
        let g = self.inner.read();
        let codec = g.get(manifest).ok_or_else(|| PyCodecError::UnknownManifest(manifest.into()))?;
        codec.encode(manifest, payload)
    }

    /// Decode `blob` under `manifest`.
    pub fn decode(&self, manifest: &str, blob: &[u8]) -> Result<Vec<u8>, PyCodecError> {
        let g = self.inner.read();
        let codec = g.get(manifest).ok_or_else(|| PyCodecError::UnknownManifest(manifest.into()))?;
        codec.decode(manifest, blob)
    }

    pub fn manifests(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.inner.read().keys().cloned().collect();
        keys.sort();
        keys
    }
}

/// Convert a [`PyCodecRegistry`] into a [`atomr_remote::SerializerRegistry`].
///
/// The actual `SerializerRegistry` API in `atomr-remote` registers
/// typed serializers via `register_bincode::<T>()` / `register_json::<T>()`
/// — both of which require a typed `T`. The Python side is type-erased
/// (everything is `Vec<u8>`), so the integration point is a thin
/// adapter type-erased to `Vec<u8>` payloads. We surface that adapter
/// as part of pycore's pyo3 layer (it needs `Py<PyAny>` to call back
/// into Python). Here we hand back a fresh `SerializerRegistry`; the
/// `pycore` adapter installs the typed wrapper at boot.
pub fn as_remote_serializer(_registry: &PyCodecRegistry) -> SerializerRegistry {
    SerializerRegistry::new()
}

// -- Built-in JSON codec ---------------------------------------------

/// Reference JSON codec — round-trips any UTF-8 JSON blob unchanged.
/// The Python side typically wraps `json.dumps(obj).encode()` /
/// `json.loads(blob.decode())` around this.
pub struct JsonCodec;

impl PyCodec for JsonCodec {
    fn id(&self) -> &str {
        "json"
    }
    fn encode(&self, _manifest: &str, payload: &[u8]) -> Result<Vec<u8>, PyCodecError> {
        let s = std::str::from_utf8(payload).map_err(|e| PyCodecError::Encode(e.to_string()))?;
        let _: serde_json::Value =
            serde_json::from_str(s).map_err(|e| PyCodecError::Encode(e.to_string()))?;
        Ok(payload.to_vec())
    }
    fn decode(&self, _manifest: &str, blob: &[u8]) -> Result<Vec<u8>, PyCodecError> {
        let s = std::str::from_utf8(blob).map_err(|e| PyCodecError::Decode(e.to_string()))?;
        let _: serde_json::Value =
            serde_json::from_str(s).map_err(|e| PyCodecError::Decode(e.to_string()))?;
        Ok(blob.to_vec())
    }
}

/// Convenience: a [`PyCodecRegistry`] preloaded with the JSON codec
/// for the supplied manifests.
pub fn json_codec(manifests: impl IntoIterator<Item = impl Into<String>>) -> PyCodecRegistry {
    let reg = PyCodecRegistry::new();
    reg.register(Arc::new(JsonCodec), manifests);
    reg
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedCodec(&'static str);
    impl PyCodec for FixedCodec {
        fn id(&self) -> &str {
            self.0
        }
        fn encode(&self, _manifest: &str, payload: &[u8]) -> Result<Vec<u8>, PyCodecError> {
            Ok([&[0xAB], payload].concat())
        }
        fn decode(&self, _manifest: &str, blob: &[u8]) -> Result<Vec<u8>, PyCodecError> {
            if blob.first() != Some(&0xAB) {
                return Err(PyCodecError::Decode("missing magic".into()));
            }
            Ok(blob[1..].to_vec())
        }
    }

    #[test]
    fn registry_routes_by_manifest() {
        let reg = PyCodecRegistry::new();
        reg.register(Arc::new(FixedCodec("a")), ["A.Msg"]);
        reg.register(Arc::new(FixedCodec("b")), ["B.Msg"]);
        let a = reg.encode("A.Msg", b"hello").unwrap();
        assert_eq!(a, vec![0xAB, b'h', b'e', b'l', b'l', b'o']);
        let back = reg.decode("A.Msg", &a).unwrap();
        assert_eq!(back, b"hello");
    }

    #[test]
    fn unknown_manifest_is_an_error() {
        let reg = PyCodecRegistry::new();
        let err = reg.encode("nope", b"x").unwrap_err();
        assert!(matches!(err, PyCodecError::UnknownManifest(_)));
    }

    #[test]
    fn json_codec_round_trips_valid_json() {
        let codec = JsonCodec;
        let blob = codec.encode("Cmd", b"{\"k\":1}").unwrap();
        let back = codec.decode("Cmd", &blob).unwrap();
        assert_eq!(back, b"{\"k\":1}");
    }

    #[test]
    fn json_codec_rejects_invalid_json() {
        let codec = JsonCodec;
        assert!(codec.encode("Cmd", b"not json").is_err());
    }

    #[test]
    fn json_codec_factory_seeds_manifests() {
        let reg = json_codec(["Foo", "Bar"]);
        let mut ms = reg.manifests();
        ms.sort();
        assert_eq!(ms, vec!["Bar", "Foo"]);
    }
}
