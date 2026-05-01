//! # rakka-config
//!
//! Akka.NET-compatible configuration. Mirrors keys under `akka.*` but is
//! layered over [`toml`] + `serde`. HOCON has no idiomatic equivalent in Rust;
//! a small `hocon` feature shim can be added later for migration tooling.
//!
//! akka.net source: `src/core/Akka/Configuration/`.

mod error;
pub mod hocon;
mod path;
mod reference;
mod value;

pub use error::ConfigError;
pub use hocon::HoconError;
pub use path::ConfigPath;
pub use reference::reference_config;
pub use value::{Config, ConfigValue};
