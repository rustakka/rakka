//! Python exception types exposed by the native extension.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(rustakka, RustakkaError, PyException, "Base rustakka error.");
create_exception!(rustakka, ActorSystemError, RustakkaError, "ActorSystem error.");
create_exception!(rustakka, SpawnError, RustakkaError, "Spawn error.");
create_exception!(rustakka, AskError, RustakkaError, "Ask timed out or target stopped.");
create_exception!(rustakka, InterpreterOverloaded, RustakkaError, "Interpreter mailbox full.");
create_exception!(
    rustakka,
    InterpreterCompatError,
    RustakkaError,
    "C extension not safe for the selected dispatcher."
);

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("RustakkaError", m.py().get_type_bound::<RustakkaError>())?;
    m.add("ActorSystemError", m.py().get_type_bound::<ActorSystemError>())?;
    m.add("SpawnError", m.py().get_type_bound::<SpawnError>())?;
    m.add("AskError", m.py().get_type_bound::<AskError>())?;
    m.add("InterpreterOverloaded", m.py().get_type_bound::<InterpreterOverloaded>())?;
    m.add("InterpreterCompatError", m.py().get_type_bound::<InterpreterCompatError>())?;
    Ok(())
}

pub fn map<E: std::fmt::Display>(e: E) -> PyErr {
    PyErr::new::<RustakkaError, _>(e.to_string())
}
