//! Registry mapping types to serializers.
//!
//! The default impl only supports `JsonSerializer` to keep the public API
//! simple. Callers who need additional codecs add an enum-based
//! `Serializer` trait variant; the registry stays `TypeId`-keyed.

use std::any::{Any, TypeId};
use std::sync::Arc;

use dashmap::DashMap;

use super::json::JsonSerializer;
use super::traits::SerializerError;

#[derive(Default)]
pub struct SerializationRegistry {
    by_type: DashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl SerializationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T>(&self, serializer: JsonSerializer<T>)
    where
        T: Any + Send + Sync + 'static + serde::Serialize + serde::de::DeserializeOwned,
    {
        self.by_type.insert(TypeId::of::<T>(), Arc::new(serializer));
    }

    pub fn to_bytes<T>(&self, value: &T) -> Result<Vec<u8>, SerializerError>
    where
        T: Any + Send + Sync + 'static + serde::Serialize + serde::de::DeserializeOwned,
    {
        let e = self.by_type.get(&TypeId::of::<T>()).ok_or(SerializerError::NotRegistered)?;
        let s = e.clone().downcast::<JsonSerializer<T>>().map_err(|_| SerializerError::NotRegistered)?;
        super::traits::Serializer::to_bytes(&*s, value)
    }

    pub fn from_bytes<T>(&self, bytes: &[u8]) -> Result<T, SerializerError>
    where
        T: Any + Send + Sync + 'static + serde::Serialize + serde::de::DeserializeOwned,
    {
        let e = self.by_type.get(&TypeId::of::<T>()).ok_or(SerializerError::NotRegistered)?;
        let s = e.clone().downcast::<JsonSerializer<T>>().map_err(|_| SerializerError::NotRegistered)?;
        super::traits::Serializer::from_bytes(&*s, bytes)
    }
}
