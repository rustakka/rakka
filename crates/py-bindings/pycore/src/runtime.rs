//! Shared Tokio runtime. pyo3-async-runtimes bridges Python `asyncio`
//! futures to Tokio; we install a single process-wide runtime here the
//! first time anyone creates an `ActorSystem`.

use std::sync::Once;

use tokio::runtime::Builder;

static INIT: Once = Once::new();

pub fn ensure_initialized() {
    INIT.call_once(|| {
        let mut b = Builder::new_multi_thread();
        b.enable_all().thread_name("atomr-py");
        pyo3_async_runtimes::tokio::init(b);
    });
}

pub fn runtime() -> &'static tokio::runtime::Runtime {
    ensure_initialized();
    pyo3_async_runtimes::tokio::get_runtime()
}
