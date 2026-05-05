//! Serializer traits.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SerializerError {
    #[error("no serializer registered for type")]
    NotRegistered,
    #[error("serializer id {0} not known")]
    UnknownId(u32),
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
}

pub trait Serializer<T>: Send + Sync {
    fn identifier(&self) -> u32;
    fn manifest(&self) -> &'static str;
    fn to_bytes(&self, value: &T) -> Result<Vec<u8>, SerializerError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_bytes(&self, bytes: &[u8]) -> Result<T, SerializerError>;
}
