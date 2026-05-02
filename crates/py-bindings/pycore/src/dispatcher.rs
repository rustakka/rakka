//! Dispatcher-name → `InterpreterKind` decoding. Config lives in TOML;
//! we merely translate when props hit `ActorSystem.actor_of`.

use pyo3::prelude::*;

use crate::interpreter::InterpreterKind;

pub fn parse(name: &str, count: usize) -> InterpreterKind {
    match name {
        "python-pinned" => InterpreterKind::Pinned,
        "python-subinterpreter-pool" => InterpreterKind::SubinterpreterPool { count: count.max(1) },
        "python-nogil" => InterpreterKind::NoGil { threads: count.max(1) },
        "python-subprocess" => InterpreterKind::Subprocess { count: count.max(1) },
        _ => InterpreterKind::Pinned,
    }
}

pub fn register(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
