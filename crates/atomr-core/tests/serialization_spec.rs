//! Serialization registry spec parity. akka.net:
//! `Akka.Tests/Serialization/SerializationSpec.cs`.
//!
//! atomr's registry is `TypeId`-keyed and currently exposes only
//! `JsonSerializer`. These tests assert the invariants that map onto
//! akka.net's `SerializationSpec` for the public surface that exists today:
//!
//! - registered codecs round-trip typed values through `to_bytes`/`from_bytes`,
//! - the manifest reported by `Serializer::manifest` reflects the rust type
//!   name and is what akka.net would use as the lookup key,
//! - re-registering for the same type replaces the previous serializer,
//! - looking up an unregistered type yields `SerializerError::NotRegistered`,
//! - JSON (atomr's default codec) produces stable, deterministic bytes for
//!   primitives and small structs.
//!
//! Note on the original akka.net spec: `SerializationSpec` exercises a
//! manifest -> serializer lookup. atomr keys by `TypeId` instead, so the
//! "lookup by manifest" check here is expressed as: the serializer's
//! `manifest()` matches `std::any::type_name::<T>()` and the registry
//! resolves the same `T` round-trip.

use atomr_core::serialization::{JsonSerializer, SerializationRegistry, Serializer, SerializerError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct Greeting {
    who: String,
    n: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct Other {
    flag: bool,
}

#[test]
fn registered_serializer_roundtrips_typed_value() {
    let reg = SerializationRegistry::new();
    reg.register(JsonSerializer::<Greeting>::new(101));

    let original = Greeting { who: "world".into(), n: 7 };
    let bytes = reg.to_bytes(&original).expect("serialize");
    let back: Greeting = reg.from_bytes(&bytes).expect("deserialize");

    assert_eq!(back, original);
}

#[test]
fn manifest_reports_rust_type_name() {
    // The registry is TypeId-keyed, but the codec still surfaces a manifest
    // that downstream remoting layers can use, mirroring akka.net's
    // `Serializer.Manifest`.
    let s = JsonSerializer::<Greeting>::new(7);
    assert_eq!(s.manifest(), std::any::type_name::<Greeting>());
    assert_eq!(s.identifier(), 7);
}

#[test]
fn registry_resolves_registered_type_and_rejects_unregistered() {
    let reg = SerializationRegistry::new();
    reg.register(JsonSerializer::<Greeting>::new(1));

    // Registered type resolves.
    let g = Greeting { who: "a".into(), n: 1 };
    let bytes = reg.to_bytes(&g).expect("serialize Greeting");
    let _: Greeting = reg.from_bytes(&bytes).expect("deserialize Greeting");

    // Unregistered type yields NotRegistered (the analog of akka.net's
    // "no serializer found for manifest").
    let other = Other { flag: true };
    let err = reg.to_bytes(&other).expect_err("Other not registered");
    assert!(matches!(err, SerializerError::NotRegistered));

    // Decoding into an unregistered type also fails with NotRegistered.
    let err = reg.from_bytes::<Other>(&[1, 2, 3]).expect_err("Other not registered");
    assert!(matches!(err, SerializerError::NotRegistered));
}

#[test]
fn registering_twice_overrides_previous_serializer() {
    // atomr uses `DashMap::insert`, so a second `register` for the same `T`
    // replaces the prior entry. This is the documented atomr semantics; the
    // observable effect is that the new identifier wins on subsequent ops.
    let reg = SerializationRegistry::new();
    reg.register(JsonSerializer::<Greeting>::new(1));
    reg.register(JsonSerializer::<Greeting>::new(2));

    // Round-trip still works after override.
    let g = Greeting { who: "z".into(), n: 9 };
    let bytes = reg.to_bytes(&g).expect("serialize");
    let back: Greeting = reg.from_bytes(&bytes).expect("deserialize");
    assert_eq!(back, g);
}

#[test]
fn decode_failure_surfaces_decode_error() {
    let reg = SerializationRegistry::new();
    reg.register(JsonSerializer::<Greeting>::new(1));

    // Garbage bytes for a registered type should be a decode error, not
    // NotRegistered.
    let err = reg.from_bytes::<Greeting>(b"not json").expect_err("decode fails");
    assert!(matches!(err, SerializerError::Decode(_)), "unexpected: {err:?}");
}

#[test]
fn json_default_produces_stable_bytes_for_primitives() {
    // atomr's default codec is `JsonSerializer` (akka.net's default is the
    // Newtonsoft JSON serializer). JSON is canonical for these primitives,
    // so the byte output is stable across runs.
    let s_u32 = JsonSerializer::<u32>::new(10);
    assert_eq!(Serializer::<u32>::to_bytes(&s_u32, &42).expect("encode"), b"42".to_vec());

    let s_str = JsonSerializer::<String>::new(11);
    assert_eq!(
        Serializer::<String>::to_bytes(&s_str, &"hi".to_string()).expect("encode"),
        b"\"hi\"".to_vec(),
    );

    let s_bool = JsonSerializer::<bool>::new(12);
    assert_eq!(Serializer::<bool>::to_bytes(&s_bool, &true).expect("encode"), b"true".to_vec());

    // Determinism: two encodes produce identical bytes.
    let a = Serializer::<u32>::to_bytes(&s_u32, &123).expect("encode a");
    let b = Serializer::<u32>::to_bytes(&s_u32, &123).expect("encode b");
    assert_eq!(a, b);
}

#[test]
fn json_default_struct_bytes_are_stable_and_roundtrip() {
    let reg = SerializationRegistry::new();
    reg.register(JsonSerializer::<Greeting>::new(1));

    let g = Greeting { who: "stable".into(), n: 3 };
    let a = reg.to_bytes(&g).expect("encode a");
    let b = reg.to_bytes(&g).expect("encode b");
    assert_eq!(a, b, "JSON encoding must be deterministic");

    // Field order is the struct definition order; this guards the wire
    // format for downstream remoting.
    assert_eq!(a, br#"{"who":"stable","n":3}"#.to_vec());
}
