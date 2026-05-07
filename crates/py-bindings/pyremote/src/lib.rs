//! Pluggable Python codec for `atomr-remote`. Phase P3 / 5.K of
//! `docs/full-port-plan.md` and `PORTING_TODO.md`.
//!
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

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;

use atomr_remote::{SerializeError, SerializerRegistry, TypeCodec, BINCODE_SERIALIZER_ID};

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
/// dispatch happens on the `manifest` string ( parity:
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

    /// Snapshot the `(manifest, codec)` pairs currently registered.
    /// Used by [`as_remote_serializer`] to mirror entries into the
    /// upstream [`atomr_remote::SerializerRegistry`].
    pub fn snapshot(&self) -> Vec<(String, Arc<dyn PyCodec>)> {
        self.inner.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Look up the codec for `manifest` without holding any locks
    /// across the call.
    pub fn get(&self, manifest: &str) -> Option<Arc<dyn PyCodec>> {
        self.inner.read().get(manifest).cloned()
    }
}

/// Type-tag for the type-erased Python payload. Every Python message
/// shows up as `PyBytes` on the wire; the `manifest` discriminates
/// classes within that one Rust type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PyBytes(pub Vec<u8>);

/// Convert a [`PyCodecRegistry`] into a [`atomr_remote::SerializerRegistry`].
///
/// The actual `SerializerRegistry` API in `atomr-remote` registers
/// typed serializers via `register_bincode::<T>()` / `register_json::<T>()`
/// — both of which require a typed `T`. The Python side is type-erased
/// (everything ends up as raw bytes), so we register one `TypeCodec`
/// per Python manifest. All of them share `TypeId::of::<PyBytes>()`
/// for the by-type lookup; senders should encode by manifest, not by
/// `T`.
///
/// Python ↔ Python only — the manifest namespace is the Python class
/// path (`module.qualname`). Cross-language interop is out of scope for
/// this adapter; bridging would require a wire-level translation
/// outside the codec layer.
pub fn as_remote_serializer(registry: &PyCodecRegistry) -> SerializerRegistry {
    let out = SerializerRegistry::new();
    let snapshot = registry.snapshot();
    for (manifest, codec) in snapshot {
        install_codec(&out, codec, manifest);
    }
    out
}

fn install_codec(target: &SerializerRegistry, codec: Arc<dyn PyCodec>, manifest: String) {
    let manifest_for_encode = manifest.clone();
    let manifest_for_decode = manifest.clone();
    let codec_for_encode = codec.clone();
    let codec_for_decode = codec;
    target.register_codec(TypeCodec {
        serializer_id: BINCODE_SERIALIZER_ID,
        manifest: manifest.clone(),
        type_id: TypeId::of::<PyBytes>(),
        encode: Arc::new(move |v| {
            let bytes = v
                .downcast_ref::<PyBytes>()
                .ok_or_else(|| SerializeError::Downcast("PyBytes".into()))?;
            codec_for_encode
                .encode(&manifest_for_encode, &bytes.0)
                .map_err(|e| SerializeError::Encode(e.to_string()))
        }),
        decode: Arc::new(move |b| {
            let payload = codec_for_decode
                .decode(&manifest_for_decode, b)
                .map_err(|e| SerializeError::Decode(e.to_string()))?;
            Ok(Box::new(PyBytes(payload)) as Box<dyn std::any::Any + Send>)
        }),
    });
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

    #[test]
    fn as_remote_serializer_round_trip_via_manifest() {
        // Two different Python "classes" — both encode through the same
        // `FixedCodec`. The mirror should hand back distinct entries
        // keyed by manifest, all pointing at `TypeId::of::<PyBytes>()`.
        let reg = PyCodecRegistry::new();
        reg.register(Arc::new(FixedCodec("a")), ["my.module.Cmd", "my.module.Reply"]);
        let serializer = as_remote_serializer(&reg);
        let codec_cmd = serializer.codec_for_manifest("my.module.Cmd").unwrap();
        let blob = (codec_cmd.encode)(&PyBytes(b"hi".to_vec()) as &dyn std::any::Any).unwrap();
        let (any, _) =
            serializer.decode_dyn("my.module.Cmd", BINCODE_SERIALIZER_ID, &blob).unwrap();
        let pb = any.downcast::<PyBytes>().unwrap();
        assert_eq!(pb.0, b"hi");
    }

    #[test]
    fn as_remote_serializer_unknown_manifest_returns_none() {
        let reg = PyCodecRegistry::new();
        let serializer = as_remote_serializer(&reg);
        assert!(serializer.codec_for_manifest("nope").is_none());
    }
}
