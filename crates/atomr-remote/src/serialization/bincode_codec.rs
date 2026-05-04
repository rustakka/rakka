//! Bincode v2 + serde codec helpers.

use serde::{de::DeserializeOwned, Serialize};

use super::{SerializeError, SerializerRegistry, BINCODE_SERIALIZER_ID, SYSTEM_SERIALIZER_ID};
use crate::pdu::{AckInfo, AssociateInfo, DisassociateReason};

pub fn bincode_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SerializeError> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| SerializeError::Encode(e.to_string()))
}

pub fn bincode_decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SerializeError> {
    let (v, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| SerializeError::Decode(e.to_string()))?;
    Ok(v)
}

/// System control payloads use the same bincode codec but a reserved
/// `serializer_id` of [`SYSTEM_SERIALIZER_ID`] so receivers can dispatch
/// them on the system path without consulting the user manifest table.
pub fn system_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SerializeError> {
    bincode_encode(value)
}

pub fn system_decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SerializeError> {
    bincode_decode(bytes)
}

/// Pre-register codecs for the protocol-level system payloads
/// (`AssociateInfo`, `DisassociateReason`, `AckInfo`, plus the
/// `RemoteSystemMsg` variants that travel as Payload PDUs).
pub fn register_system_payloads(reg: &SerializerRegistry) {
    use atomr_core::actor::RemoteSystemMsg;
    use std::any::TypeId;
    use std::sync::Arc;

    use super::TypeCodec;

    fn codec<T: Serialize + DeserializeOwned + Send + 'static>(id: u32) -> TypeCodec {
        TypeCodec {
            serializer_id: id,
            manifest: std::any::type_name::<T>().to_string(),
            type_id: TypeId::of::<T>(),
            encode: Arc::new(|v: &dyn std::any::Any| {
                let v = v
                    .downcast_ref::<T>()
                    .ok_or_else(|| SerializeError::Downcast(std::any::type_name::<T>().to_string()))?;
                bincode_encode(v)
            }),
            decode: Arc::new(|b: &[u8]| {
                let v: T = bincode_decode(b)?;
                Ok(Box::new(v) as Box<dyn std::any::Any + Send>)
            }),
        }
    }

    reg.register_codec(codec::<AssociateInfo>(SYSTEM_SERIALIZER_ID));
    reg.register_codec(codec::<DisassociateReason>(SYSTEM_SERIALIZER_ID));
    reg.register_codec(codec::<AckInfo>(SYSTEM_SERIALIZER_ID));
    reg.register_codec(codec::<RemoteSystemMsg>(SYSTEM_SERIALIZER_ID));

    // Register a few common user types with bincode by default so trivial
    // examples work without manual registration.
    reg.register_codec(codec::<String>(BINCODE_SERIALIZER_ID));
    reg.register_codec(codec::<Vec<u8>>(BINCODE_SERIALIZER_ID));
    reg.register_codec(codec::<i64>(BINCODE_SERIALIZER_ID));
    reg.register_codec(codec::<u64>(BINCODE_SERIALIZER_ID));
    reg.register_codec(codec::<i32>(BINCODE_SERIALIZER_ID));
    reg.register_codec(codec::<u32>(BINCODE_SERIALIZER_ID));
    reg.register_codec(codec::<bool>(BINCODE_SERIALIZER_ID));
}
