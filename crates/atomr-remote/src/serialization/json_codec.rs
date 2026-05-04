//! JSON codec helpers (debug-friendly fallback).

use serde::{de::DeserializeOwned, Serialize};

use super::SerializeError;

pub fn json_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SerializeError> {
    serde_json::to_vec(value).map_err(|e| SerializeError::Encode(e.to_string()))
}

pub fn json_decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SerializeError> {
    serde_json::from_slice(bytes).map_err(|e| SerializeError::Decode(e.to_string()))
}
