//! JSON serializer using `serde_json`. akka.net: `Serialization/NewtonSoftJsonSerializer.cs`.

use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};

use super::traits::{Serializer, SerializerError};

pub struct JsonSerializer<T> {
    id: u32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> JsonSerializer<T> {
    pub fn new(id: u32) -> Self {
        Self { id, _marker: PhantomData }
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Serializer<T> for JsonSerializer<T> {
    fn identifier(&self) -> u32 {
        self.id
    }

    fn manifest(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    fn to_bytes(&self, value: &T) -> Result<Vec<u8>, SerializerError> {
        serde_json::to_vec(value).map_err(|e| SerializerError::Encode(e.to_string()))
    }

    fn from_bytes(&self, bytes: &[u8]) -> Result<T, SerializerError> {
        serde_json::from_slice(bytes).map_err(|e| SerializerError::Decode(e.to_string()))
    }
}
