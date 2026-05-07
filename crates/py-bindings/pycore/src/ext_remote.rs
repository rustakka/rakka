//! `remote` submodule — Python codec registry that plugs into
//! `atomr_pyremote::PyCodecRegistry` and supports cross-node Python
//! actors. Phase 9 of the Python expansion plan.
//!
//! The Rust crate `atomr-pyremote` defines the trait and a built-in
//! JSON codec, but deliberately keeps `pyo3` out so its tests can run
//! without an interpreter. This module supplies the `pyo3` glue:
//!
//! * `PyCodecRegistry` — Python-facing per-system registry. Holds two
//!   parallel maps: a `manifest -> (encoder, decoder)` of Python
//!   callables (used for object <-> bytes), and an
//!   `atomr_pyremote::PyCodecRegistry` (used for the remote-bytes
//!   pipeline so the rest of the framework can introspect / install
//!   typed serialisers).
//! * `BuiltinJsonCodec` — convenience that wires `json.dumps` /
//!   `json.loads` for any number of manifests.
//! * `validate_manifest` — round-trips a `module.qualname` string
//!   through `importlib`; raises `ValueError` on failure.
//!
//! The actor-system glue (`PyActorSystem.register_codec`,
//! `use_json_codec`) lives in `actor_system.rs` so it can hold the
//! registry alongside system state.

use std::sync::Arc;

use dashmap::DashMap;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use atomr_pyremote::{JsonCodec, PyCodec, PyCodecError, PyCodecRegistry as RustRegistry};

use crate::errors;

/// Stored entry — encoder/decoder callables plus the codec name.
pub(crate) struct CodecEntry {
    name: String,
    encoder: Py<PyAny>,
    decoder: Py<PyAny>,
}

/// Returned by [`PyCodecRegistry::insert`] when `force=false` and a
/// manifest is already registered.
pub(crate) struct CodecCollision {
    pub manifest: String,
    pub existing_name: String,
}

/// Validate that `manifest` is a fully-qualified Python class path
/// (`module.qualname`) by importing the module and walking the
/// qualname segments. Raises `ValueError` on failure.
///
/// When `strict` is `false`, the importlib round-trip is skipped and
/// `warnings.warn(...)` is emitted to flag that the receiver is
/// trusted to recognise the manifest. The dotted-path syntax check
/// (`module.qualname` must contain at least one dot) is still enforced.
pub(crate) fn validate_manifest(py: Python<'_>, manifest: &str) -> PyResult<()> {
    validate_manifest_with_mode(py, manifest, true)
}

/// Strict (default) and lax (`strict=False`) variants of
/// [`validate_manifest`]. See the public function's docstring.
pub(crate) fn validate_manifest_with_mode(
    py: Python<'_>,
    manifest: &str,
    strict: bool,
) -> PyResult<()> {
    let (module_path, qualname) = manifest.rsplit_once('.').ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "manifest `{manifest}` must be `module.qualname` (no dot found)"
        ))
    })?;
    if !strict {
        // Lax mode: only enforce the dotted-path syntax. Emit a
        // WARNING so production users notice if they accidentally
        // rely on it.
        let warnings = py.import_bound("warnings")?;
        let msg = format!(
            "manifest `{manifest}` not strictly validated; relying on receiver to recognize it"
        );
        warnings.call_method1("warn", (msg,))?;
        return Ok(());
    }
    let importlib = py.import_bound("importlib")?;
    let module = importlib.call_method1("import_module", (module_path,)).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "manifest `{manifest}`: cannot import `{module_path}`: {e}"
        ))
    })?;
    let mut cur: Bound<'_, PyAny> = module.into_any();
    for segment in qualname.split('.') {
        cur = cur.getattr(segment).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "manifest `{manifest}`: attribute `{segment}` missing on resolved object: {e}"
            ))
        })?;
    }
    Ok(())
}

/// Default-codec marker. When set, any unknown manifest falls back to
/// the JSON codec.
pub(crate) struct DefaultCodec {
    encoder: Py<PyAny>,
    decoder: Py<PyAny>,
}

/// Python-facing codec registry. Each `PyActorSystem` owns one of
/// these. Internally tracks Python callables per manifest plus an
/// `atomr_pyremote::PyCodecRegistry` mirror for downstream Rust
/// consumers (e.g. the eventual remote serializer registry plumbing).
#[pyclass(name = "PyCodecRegistry", module = "atomr._native.remote")]
#[derive(Clone)]
pub struct PyCodecRegistry {
    pub(crate) entries: Arc<DashMap<String, CodecEntry>>,
    pub(crate) rust_mirror: Arc<RustRegistry>,
    pub(crate) default: Arc<parking_lot::RwLock<Option<DefaultCodec>>>,
}

impl Default for PyCodecRegistry {
    fn default() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            rust_mirror: Arc::new(RustRegistry::new()),
            default: Arc::new(parking_lot::RwLock::new(None)),
        }
    }
}

impl PyCodecRegistry {
    pub(crate) fn lookup(&self, manifest: &str) -> Option<(Py<PyAny>, Py<PyAny>)> {
        if let Some(e) = self.entries.get(manifest) {
            return Python::with_gil(|py| Some((e.encoder.clone_ref(py), e.decoder.clone_ref(py))));
        }
        let g = self.default.read();
        g.as_ref().map(|d| Python::with_gil(|py| (d.encoder.clone_ref(py), d.decoder.clone_ref(py))))
    }

    /// Insert a codec entry. Used by the pymethod and by callers in
    /// `actor_system.rs`.
    ///
    /// When `force` is false, this returns an error containing the
    /// (manifest, existing-codec-name) for the **first** collision
    /// encountered without mutating any entries. When `force` is true,
    /// existing entries are silently replaced.
    pub(crate) fn insert(
        &self,
        name: String,
        encoder: Py<PyAny>,
        decoder: Py<PyAny>,
        manifests: &[String],
        force: bool,
    ) -> Result<(), CodecCollision> {
        if !force {
            for manifest in manifests {
                if let Some(existing) = self.entries.get(manifest) {
                    return Err(CodecCollision {
                        manifest: manifest.clone(),
                        existing_name: existing.name.clone(),
                    });
                }
            }
        }
        Python::with_gil(|py| {
            for manifest in manifests {
                self.entries.insert(
                    manifest.clone(),
                    CodecEntry {
                        name: name.clone(),
                        encoder: encoder.clone_ref(py),
                        decoder: decoder.clone_ref(py),
                    },
                );
            }
        });
        self.rust_mirror.register(Arc::new(JsonCodec), manifests.iter().cloned());
        Ok(())
    }

    /// Install a default fallback codec.
    pub(crate) fn install_default(&self, encoder: Py<PyAny>, decoder: Py<PyAny>) {
        let mut g = self.default.write();
        *g = Some(DefaultCodec { encoder, decoder });
    }

    /// Register the JSON codec for `manifests`, building the encoder
    /// and decoder lambdas from `json.dumps` / `json.loads`. Returns
    /// `Ok(Ok(()))` on success, `Ok(Err(collision))` on collision when
    /// `force=false`, and `Err(...)` on Python-level errors.
    pub(crate) fn install_json(
        &self,
        py: Python<'_>,
        manifests: &[String],
        force: bool,
    ) -> PyResult<Result<(), CodecCollision>> {
        let (encoder, decoder) = build_json_pair(py)?;
        Ok(self.insert("json".to_string(), encoder, decoder, manifests, force))
    }
}

/// Build `(encoder, decoder)` lambdas backed by `json.dumps` /
/// `json.loads`.
pub(crate) fn build_json_pair(py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let json = py.import_bound("json")?;
    let dumps = json.getattr("dumps")?.unbind();
    let loads = json.getattr("loads")?.unbind();
    let dumps_dict = pyo3::types::PyDict::new_bound(py);
    dumps_dict.set_item("__d", dumps)?;
    let encoder = py
        .eval_bound("lambda o, _d=__d: _d(o).encode('utf-8')", Some(&dumps_dict), None)?
        .unbind();
    let loads_dict = pyo3::types::PyDict::new_bound(py);
    loads_dict.set_item("__l", loads)?;
    let decoder = py
        .eval_bound("lambda b, _l=__l: _l(b.decode('utf-8'))", Some(&loads_dict), None)?
        .unbind();
    Ok((encoder, decoder))
}

#[pymethods]
impl PyCodecRegistry {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// Register a codec under `name` for one or more manifests.
    ///
    /// `encoder(obj) -> bytes` and `decoder(bytes) -> obj`. Manifests
    /// must be `module.qualname` strings; validation is the caller's
    /// responsibility (see `PyActorSystem.register_codec`).
    ///
    /// On collision (manifest already registered) and `force=false`,
    /// raises `ValueError` listing the existing codec name. With
    /// `force=true`, the existing entry is silently replaced.
    #[pyo3(name = "register", signature = (name, encoder, decoder, manifests, force=false))]
    fn py_register(
        &self,
        name: String,
        encoder: Py<PyAny>,
        decoder: Py<PyAny>,
        manifests: Vec<String>,
        force: bool,
    ) -> PyResult<()> {
        if manifests.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "register: manifests must not be empty",
            ));
        }
        self.insert(name, encoder, decoder, &manifests, force)
            .map_err(collision_to_pyerr)
    }

    /// Convenience: register the built-in JSON codec for the given
    /// manifests. Honors the same `force` flag as `register`.
    #[pyo3(name = "register_json", signature = (manifests, force=false))]
    fn py_register_json(
        &self,
        py: Python<'_>,
        manifests: Vec<String>,
        force: bool,
    ) -> PyResult<()> {
        if manifests.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "register_json: manifests must not be empty",
            ));
        }
        self.install_json(py, &manifests, force)?
            .map_err(collision_to_pyerr)
    }

    /// Mark the JSON codec (or any user-supplied codec) as the
    /// fallback used for any manifest that has no explicit
    /// registration. Use sparingly — opaque manifests skip
    /// validation.
    #[pyo3(name = "set_default")]
    fn py_set_default(&self, encoder: Py<PyAny>, decoder: Py<PyAny>) -> PyResult<()> {
        self.install_default(encoder, decoder);
        Ok(())
    }

    /// Encode a Python object under `manifest`. Returns the codec's
    /// bytes output.
    fn encode<'py>(
        &self,
        py: Python<'py>,
        manifest: String,
        obj: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let (encoder, _decoder) = self.lookup(&manifest).ok_or_else(|| {
            PyErr::new::<errors::AtomrError, _>(format!(
                "no codec registered for manifest `{manifest}`"
            ))
        })?;
        let bytes = call_encoder(py, &encoder, &obj)?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Decode bytes under `manifest` back into a Python object.
    fn decode<'py>(
        &self,
        py: Python<'py>,
        manifest: String,
        blob: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (_encoder, decoder) = self.lookup(&manifest).ok_or_else(|| {
            PyErr::new::<errors::AtomrError, _>(format!(
                "no codec registered for manifest `{manifest}`"
            ))
        })?;
        let res = call_decoder(py, &decoder, &blob)?;
        Ok(res.into_bound(py))
    }

    /// Round-trip a Python object through encoder + decoder. Exposed
    /// for tests and for the in-process "remote" path used until full
    /// transport wiring is in place.
    fn roundtrip<'py>(
        &self,
        py: Python<'py>,
        manifest: String,
        obj: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = {
            let (encoder, _) = self.lookup(&manifest).ok_or_else(|| {
                PyErr::new::<errors::AtomrError, _>(format!(
                    "no codec registered for manifest `{manifest}`"
                ))
            })?;
            call_encoder(py, &encoder, &obj)?
        };
        let (_, decoder) = self.lookup(&manifest).ok_or_else(|| {
            PyErr::new::<errors::AtomrError, _>(format!(
                "no codec registered for manifest `{manifest}`"
            ))
        })?;
        let res = call_decoder(py, &decoder, &bytes)?;
        Ok(res.into_bound(py))
    }

    fn manifests(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.entries.iter().map(|e| e.key().clone()).collect();
        keys.sort();
        keys
    }

    fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.iter().map(|e| e.value().name.clone()).collect();
        names.sort();
        names.dedup();
        names
    }

    fn __contains__(&self, manifest: String) -> bool {
        self.entries.contains_key(&manifest)
    }
}

/// Encode an object via the user-supplied callable.
pub(crate) fn call_encoder(
    py: Python<'_>,
    encoder: &Py<PyAny>,
    obj: &Py<PyAny>,
) -> PyResult<Vec<u8>> {
    let res = encoder.call1(py, PyTuple::new_bound(py, &[obj.clone_ref(py)]))?;
    let bound = res.bind(py);
    if let Ok(b) = bound.downcast::<PyBytes>() {
        Ok(b.as_bytes().to_vec())
    } else {
        Err(PyErr::new::<errors::AtomrError, _>(
            "codec encoder must return bytes",
        ))
    }
}

/// Decode bytes via the user-supplied callable.
pub(crate) fn call_decoder(
    py: Python<'_>,
    decoder: &Py<PyAny>,
    bytes: &[u8],
) -> PyResult<Py<PyAny>> {
    let arg = PyBytes::new_bound(py, bytes);
    let res = decoder.call1(py, (arg,))?;
    Ok(res)
}

/// Compute `obj.__class__.__module__ + "." + obj.__class__.__qualname__`.
pub(crate) fn manifest_for(py: Python<'_>, obj: &Py<PyAny>) -> PyResult<String> {
    let bound = obj.bind(py);
    let cls = bound.get_type();
    let module: String = cls.getattr("__module__")?.extract()?;
    let qualname: String = cls.getattr("__qualname__")?.extract()?;
    Ok(format!("{module}.{qualname}"))
}

/// Free-function variant of validate_manifest, exposed to Python.
///
/// Pass `strict=False` to skip the importlib round-trip; useful for
/// `__main__`-scoped classes and inline test fixtures. Lax mode emits
/// `warnings.warn(...)` so production code does not silently rely on
/// it.
#[pyfunction]
#[pyo3(signature = (manifest, strict=true))]
fn validate_manifest_py(py: Python<'_>, manifest: String, strict: bool) -> PyResult<()> {
    validate_manifest_with_mode(py, &manifest, strict)
}

/// Translate a `CodecCollision` into a Python `ValueError`.
pub(crate) fn collision_to_pyerr(c: CodecCollision) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
        "manifest '{}' already registered as codec '{}'; pass force=True to override",
        c.manifest, c.existing_name
    ))
}

/// Compute `module.qualname` for a Python object. Mirrors the manifest
/// derivation used at remote-tell time.
#[pyfunction]
fn manifest_of(py: Python<'_>, obj: Py<PyAny>) -> PyResult<String> {
    manifest_for(py, &obj)
}

/// Built-in `JsonCodec` exposed for tests and Rust-side instrumentation.
/// Wraps a single instance of `atomr_pyremote::JsonCodec`.
#[pyclass(name = "BuiltinJsonCodec", module = "atomr._native.remote")]
pub struct PyBuiltinJsonCodec {
    inner: JsonCodec,
}

#[pymethods]
impl PyBuiltinJsonCodec {
    #[new]
    fn new() -> Self {
        Self { inner: JsonCodec }
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn encode<'py>(
        &self,
        py: Python<'py>,
        manifest: String,
        payload: Vec<u8>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let out = self
            .inner
            .encode(&manifest, &payload)
            .map_err(map_codec_err)?;
        Ok(PyBytes::new_bound(py, &out))
    }

    fn decode<'py>(
        &self,
        py: Python<'py>,
        manifest: String,
        blob: Vec<u8>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let out = self.inner.decode(&manifest, &blob).map_err(map_codec_err)?;
        Ok(PyBytes::new_bound(py, &out))
    }
}

fn map_codec_err(e: PyCodecError) -> PyErr {
    PyErr::new::<errors::AtomrError, _>(format!("{e}"))
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "remote")?;
    sub.add_class::<PyCodecRegistry>()?;
    sub.add_class::<PyBuiltinJsonCodec>()?;
    sub.add_function(wrap_pyfunction!(validate_manifest_py, &sub)?)?;
    sub.add_function(wrap_pyfunction!(manifest_of, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
