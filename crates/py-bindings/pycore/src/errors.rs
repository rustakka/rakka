//! Python exception types exposed by the native extension.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(atomr, AtomrError, PyException, "Base atomr error.");
create_exception!(atomr, ActorSystemError, AtomrError, "ActorSystem error.");
create_exception!(atomr, SpawnError, AtomrError, "Spawn error.");
create_exception!(atomr, AskError, AtomrError, "Ask timed out or target stopped.");
create_exception!(atomr, InterpreterOverloaded, AtomrError, "Interpreter mailbox full.");
create_exception!(
    atomr,
    InterpreterCompatError,
    AtomrError,
    "C extension not safe for the selected dispatcher."
);

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("AtomrError", m.py().get_type_bound::<AtomrError>())?;
    m.add("ActorSystemError", m.py().get_type_bound::<ActorSystemError>())?;
    m.add("SpawnError", m.py().get_type_bound::<SpawnError>())?;
    m.add("AskError", m.py().get_type_bound::<AskError>())?;
    m.add("InterpreterOverloaded", m.py().get_type_bound::<InterpreterOverloaded>())?;
    m.add("InterpreterCompatError", m.py().get_type_bound::<InterpreterCompatError>())?;
    Ok(())
}

pub fn map<E: std::fmt::Display>(e: E) -> PyErr {
    PyErr::new::<AtomrError, _>(e.to_string())
}
