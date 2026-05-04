//! `RemoteProps` — typed Props serialization for `Deploy::remote`.
//!
//! Phase 5.I of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Remote.RemoteDeploymentWatcher` + Hyperion-serialized
//! Props. Without a portable Props codec the remote deployer can
//! only ship `(manifest, bytes)` pairs — this module gives users an
//! opt-in registry where each manifest maps to a typed factory that
//! reconstructs the actor on the receiving node.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RemotePropsError {
    #[error("no factory registered for manifest `{0}`")]
    UnknownManifest(String),
    #[error("codec error: {0}")]
    Codec(String),
}

/// Boxed factory closure: given the serialized payload, produce the
/// reconstructed actor handle as `Arc<dyn std::any::Any + Send + Sync>`
/// (downcast on the receiving side).
type Factory =
    Arc<dyn Fn(&[u8]) -> Result<Arc<dyn std::any::Any + Send + Sync>, RemotePropsError> + Send + Sync>;

/// Per-system registry of `(manifest, factory)` pairs.
#[derive(Default, Clone)]
pub struct RemotePropsRegistry {
    inner: Arc<RwLock<HashMap<String, Factory>>>,
}

impl RemotePropsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a factory for `manifest`. The factory receives the
    /// serialized payload and returns a reconstructed type-erased
    /// value the receiver can downcast.
    pub fn register<F>(&self, manifest: impl Into<String>, factory: F)
    where
        F: Fn(&[u8]) -> Result<Arc<dyn std::any::Any + Send + Sync>, RemotePropsError>
            + Send
            + Sync
            + 'static,
    {
        self.inner.write().insert(manifest.into(), Arc::new(factory));
    }

    /// Reconstruct an actor from a `(manifest, bytes)` pair.
    pub fn instantiate(
        &self,
        manifest: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, RemotePropsError> {
        let factory = self
            .inner
            .read()
            .get(manifest)
            .cloned()
            .ok_or_else(|| RemotePropsError::UnknownManifest(manifest.into()))?;
        factory(bytes)
    }

    pub fn manifests(&self) -> Vec<String> {
        let mut v: Vec<String> = self.inner.read().keys().cloned().collect();
        v.sort();
        v
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

/// Convenience: register a factory that decodes a `serde::Deserialize`
/// type via bincode. Eliminates the per-manifest boilerplate.
pub fn register_bincode<T>(reg: &RemotePropsRegistry, manifest: impl Into<String>)
where
    T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
{
    reg.register(manifest, |bytes: &[u8]| {
        let cfg = bincode::config::standard();
        let (v, _): (T, _) = bincode::serde::decode_from_slice(bytes, cfg)
            .map_err(|e| RemotePropsError::Codec(e.to_string()))?;
        Ok(Arc::new(v) as Arc<dyn std::any::Any + Send + Sync>)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Greeter {
        prefix: String,
    }

    #[test]
    fn unknown_manifest_errors() {
        let reg = RemotePropsRegistry::new();
        let r = reg.instantiate("nope", &[]);
        assert!(matches!(r, Err(RemotePropsError::UnknownManifest(_))));
    }

    #[test]
    fn register_bincode_round_trip() {
        let reg = RemotePropsRegistry::new();
        register_bincode::<Greeter>(&reg, "Greeter");
        let cfg = bincode::config::standard();
        let bytes = bincode::serde::encode_to_vec(&Greeter { prefix: "hi".into() }, cfg).unwrap();
        let any = reg.instantiate("Greeter", &bytes).unwrap();
        let g: &Greeter = any.downcast_ref().unwrap();
        assert_eq!(g.prefix, "hi");
    }

    #[test]
    fn manifests_listed_sorted() {
        let reg = RemotePropsRegistry::new();
        register_bincode::<Greeter>(&reg, "ZGreeter");
        register_bincode::<Greeter>(&reg, "AGreeter");
        register_bincode::<Greeter>(&reg, "MGreeter");
        assert_eq!(reg.manifests(), vec!["AGreeter", "MGreeter", "ZGreeter"]);
        assert_eq!(reg.len(), 3);
    }

    #[test]
    fn codec_failure_is_typed() {
        let reg = RemotePropsRegistry::new();
        register_bincode::<Greeter>(&reg, "G");
        let r = reg.instantiate("G", &[0xff, 0xff]);
        assert!(matches!(r, Err(RemotePropsError::Codec(_))));
    }
}
