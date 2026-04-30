//! Python exception types exposed by the native extension.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(rakka, RakkaError, PyException, "Base rakka error.");
create_exception!(rakka, ActorSystemError, RakkaError, "ActorSystem error.");
create_exception!(rakka, SpawnError, RakkaError, "Spawn error.");
create_exception!(rakka, AskError, RakkaError, "Ask timed out or target stopped.");
create_exception!(rakka, InterpreterOverloaded, RakkaError, "Interpreter mailbox full.");
create_exception!(
    rakka,
    InterpreterCompatError,
    RakkaError,
    "C extension not safe for the selected dispatcher."
);

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("RakkaError", m.py().get_type_bound::<RakkaError>())?;
    m.add("ActorSystemError", m.py().get_type_bound::<ActorSystemError>())?;
    m.add("SpawnError", m.py().get_type_bound::<SpawnError>())?;
    m.add("AskError", m.py().get_type_bound::<AskError>())?;
    m.add("InterpreterOverloaded", m.py().get_type_bound::<InterpreterOverloaded>())?;
    m.add("InterpreterCompatError", m.py().get_type_bound::<InterpreterCompatError>())?;
    Ok(())
}

pub fn map<E: std::fmt::Display>(e: E) -> PyErr {
    PyErr::new::<RakkaError, _>(e.to_string())
}
