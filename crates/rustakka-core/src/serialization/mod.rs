//! Serialization framework. akka.net: `src/core/Akka/Serialization/`.
//!
//! A `Serializer` is a pluggable codec identified by a numeric id. The
//! registry maps both rust `TypeId` and serializer id to concrete codecs.

mod json;
mod registry;
mod traits;

pub use json::JsonSerializer;
pub use registry::SerializationRegistry;
pub use traits::{Serializer, SerializerError};

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Greeting {
        who: String,
    }

    #[test]
    fn json_roundtrip() {
        let reg = SerializationRegistry::default();
        reg.register(JsonSerializer::<Greeting>::new(1));
        let out = reg
            .to_bytes(&Greeting { who: "world".into() })
            .expect("serialize");
        let back: Greeting = reg.from_bytes(&out).expect("deserialize");
        assert_eq!(back.who, "world");
    }
}
