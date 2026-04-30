//! Config errors. akka.net: `Configuration/ConfigurationException.cs`.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config path `{0}` not found")]
    NotFound(String),

    #[error("config path `{path}` has wrong type (expected {expected})")]
    WrongType { path: String, expected: &'static str },

    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid key `{0}`")]
    InvalidKey(String),
}
