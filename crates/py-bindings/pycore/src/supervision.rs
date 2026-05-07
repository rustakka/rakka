//! Phase 2 — supervision strategies bound for Python.
//!
//! The Rust [`SupervisorStrategy`] holds an `Arc<dyn Fn(&str) ->
//! Directive + Send + Sync>` decider. Supervision runs synchronously
//! on the actor's dispatcher *without* the GIL, so we cannot call back
//! into Python from inside the decider. The compromise is to compile a
//! Python-supplied `[(class_path, directive)]` list down to a closure
//! that matches the panic payload's `module + "." + qualname` prefix.
//!
//! Panic payloads are produced by `PyActor::handle` via
//! `std::panic::panic_any(PanicPayload { ... })`. The core
//! `actor_cell::panic_payload_to_string` recognizes that struct and
//! formats the wire string as `"<class_path>: <repr>"`. Our compiled
//! decider splits on the first `": "`, looks up the class path, and
//! falls back to the configured default.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use atomr_core::supervision::{
    AllForOneStrategy, Decider, Directive, OneForOneStrategy, StrategyKind,
    SupervisorStrategy as RustStrategy,
};

/// Python-facing directive enum. Mirrors
/// [`atomr_core::supervision::Directive`] but encoded as a string for
/// binding ergonomics. Only the four canonical directives are
/// supported.
fn parse_directive(s: &str) -> PyResult<Directive> {
    match s {
        "resume" => Ok(Directive::Resume),
        "restart" => Ok(Directive::Restart),
        "stop" => Ok(Directive::Stop),
        "escalate" => Ok(Directive::Escalate),
        other => Err(PyValueError::new_err(format!(
            "unknown directive `{other}`; expected one of \
             resume | restart | stop | escalate"
        ))),
    }
}

/// Compile a `[(class_path, directive_str)]` table plus an optional
/// default into a `Decider` closure that inspects the wire-format
/// panic message produced by `PanicPayload::to_wire` (i.e.
/// `"<module>.<qualname>: <repr>"`). For non-`PanicPayload` panics
/// (which carry just the panic-msg string), we still match against
/// the prefix in case an upstream caller named the class explicitly.
fn compile_decider(rules: HashMap<String, Directive>, default: Directive) -> Decider {
    Arc::new(move |panic_msg: &str| -> Directive {
        // Wire format from `PanicPayload::to_wire`:
        //     "<module>.<qualname>: <repr>"
        // For legacy string panics, just try the whole message.
        let class_path = match panic_msg.split_once(": ") {
            Some((cp, _rest)) => cp.trim(),
            None => panic_msg.trim(),
        };
        if let Some(d) = rules.get(class_path) {
            return *d;
        }
        // Allow specifying a bare class name (no module) e.g.
        // `"ValueError"` — rare but useful for builtins.
        if let Some(idx) = class_path.rfind('.') {
            let bare = &class_path[idx + 1..];
            if let Some(d) = rules.get(bare) {
                return *d;
            }
        }
        default
    })
}

/// Python-facing wrapper around [`atomr_core::supervision::SupervisorStrategy`].
///
/// The wrapped strategy is `Clone` (Decider is `Arc`) so we can hand
/// it to multiple `Props` without rebuilding the decider closure.
#[pyclass(name = "SupervisorStrategy", module = "atomr._native")]
#[derive(Clone)]
pub struct PySupervisorStrategy {
    pub(crate) inner: RustStrategy,
}

impl PySupervisorStrategy {
    pub(crate) fn into_inner(self) -> RustStrategy {
        self.inner
    }

    pub(crate) fn rust(&self) -> &RustStrategy {
        &self.inner
    }

    /// Round-2 Epic B convenience: default `OneForOne` decider
    /// (restart-on-anything) with the given retry budget. Used by
    /// `Props.with_supervisor_budget`.
    pub(crate) fn default_with_budget(max_retries: u32, within_seconds: f64) -> Self {
        let strat = OneForOneStrategy::new()
            .with_max_retries(max_retries)
            .with_within(Duration::from_secs_f64(within_seconds));
        Self { inner: strat.into() }
    }
}

#[pymethods]
impl PySupervisorStrategy {
    /// Build a `OneForOne` strategy.
    ///
    /// `decider` is a list of `(class_path, directive)` pairs where
    /// `class_path` is `<module>.<qualname>` and `directive` is one of
    /// `"resume" | "restart" | "stop" | "escalate"`. `default` (also
    /// a directive string) is used when no rule matches; if omitted,
    /// `"restart"` is the default — matching Akka and the Rust
    /// `OneForOneStrategy::default` decider.
    #[staticmethod]
    #[pyo3(signature = (decider=None, default=None, max_retries=None, within_seconds=None))]
    pub fn one_for_one(
        decider: Option<Vec<(String, String)>>,
        default: Option<String>,
        max_retries: Option<u32>,
        within_seconds: Option<f64>,
    ) -> PyResult<Self> {
        let mut rules: HashMap<String, Directive> = HashMap::new();
        if let Some(rs) = decider {
            for (k, v) in rs {
                rules.insert(k, parse_directive(&v)?);
            }
        }
        let default_directive = match default.as_deref() {
            Some(s) => parse_directive(s)?,
            None => Directive::Restart,
        };
        let dec = compile_decider(rules, default_directive);
        let mut builder = OneForOneStrategy::new().with_decider(move |s| dec(s));
        if let Some(n) = max_retries {
            builder = builder.with_max_retries(n);
        }
        if let Some(secs) = within_seconds {
            builder = builder.with_within(Duration::from_secs_f64(secs));
        }
        Ok(Self { inner: builder.into() })
    }

    /// Build an `AllForOne` strategy.
    #[staticmethod]
    #[pyo3(signature = (decider=None, default=None, max_retries=None, within_seconds=None))]
    pub fn all_for_one(
        decider: Option<Vec<(String, String)>>,
        default: Option<String>,
        max_retries: Option<u32>,
        within_seconds: Option<f64>,
    ) -> PyResult<Self> {
        let mut rules: HashMap<String, Directive> = HashMap::new();
        if let Some(rs) = decider {
            for (k, v) in rs {
                rules.insert(k, parse_directive(&v)?);
            }
        }
        let default_directive = match default.as_deref() {
            Some(s) => parse_directive(s)?,
            None => Directive::Restart,
        };
        let dec = compile_decider(rules, default_directive);
        let strat = AllForOneStrategy {
            max_retries,
            within: within_seconds.map(Duration::from_secs_f64),
            decider: Arc::new(move |s| dec(s)),
        };
        Ok(Self { inner: strat.into() })
    }

    /// Convenience: stop on every failure, no decider table.
    #[staticmethod]
    pub fn stopping() -> Self {
        let dec: Decider = Arc::new(|_| Directive::Stop);
        let strat = OneForOneStrategy { max_retries: None, within: None, decider: dec };
        Self { inner: strat.into() }
    }

    /// Convenience: escalate on every failure (parent decides).
    #[staticmethod]
    pub fn escalating() -> Self {
        let dec: Decider = Arc::new(|_| Directive::Escalate);
        let strat = OneForOneStrategy { max_retries: None, within: None, decider: dec };
        Self { inner: strat.into() }
    }

    #[getter]
    fn kind(&self) -> &'static str {
        match self.inner.kind {
            StrategyKind::OneForOne => "one_for_one",
            StrategyKind::AllForOne => "all_for_one",
            _ => "unknown",
        }
    }

    #[getter]
    fn max_retries(&self) -> Option<u32> {
        self.inner.max_retries
    }

    #[getter]
    fn within_seconds(&self) -> Option<f64> {
        self.inner.within.map(|d| d.as_secs_f64())
    }

    /// Inspect the compiled decider for a given class path. Useful for
    /// tests and debugging.
    fn decide(&self, class_path: String) -> &'static str {
        match self.inner.decide(&class_path) {
            Directive::Resume => "resume",
            Directive::Restart => "restart",
            Directive::Stop => "stop",
            Directive::Escalate => "escalate",
            _ => "unknown",
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "<SupervisorStrategy kind={} max_retries={:?} within={:?}>",
            self.kind(),
            self.inner.max_retries,
            self.inner.within,
        )
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySupervisorStrategy>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_core::supervision::Directive;

    #[test]
    fn decider_matches_module_qualname() {
        let mut rules = HashMap::new();
        rules.insert("builtins.ValueError".into(), Directive::Restart);
        let d = compile_decider(rules, Directive::Stop);
        assert_eq!(d("builtins.ValueError: oops"), Directive::Restart);
        assert_eq!(d("builtins.RuntimeError: bad"), Directive::Stop);
    }

    #[test]
    fn decider_falls_back_to_bare_classname() {
        let mut rules = HashMap::new();
        rules.insert("ValueError".into(), Directive::Resume);
        let d = compile_decider(rules, Directive::Restart);
        assert_eq!(d("builtins.ValueError: oops"), Directive::Resume);
    }

    #[test]
    fn decider_uses_default_for_unknown() {
        let d = compile_decider(HashMap::new(), Directive::Escalate);
        assert_eq!(d("foo.Bar: x"), Directive::Escalate);
    }
}
