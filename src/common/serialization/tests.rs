// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Translation of `org.apache.kafka.common.serialization.SerializationTest`.
//!
//! Java's `SerializationTest` is structured as a single `allSerdesShouldRoundtripInput`
//! method iterating over a heterogeneous `Map<Class<?>, List<Object>>`. Rust does
//! not have heterogeneous maps keyed on type, so the loop is split into one
//! `#[test]` per primitive type per scenario. This is the test-reorganization
//! noted in CLAUDE.md DoD #3 — same coverage, one test per (type, scenario).

#![cfg(test)]

use std::collections::HashMap;

use bytes::Bytes;
use uuid::Uuid;

use crate::common::serialization::list_serializer::InnerKind;
use crate::common::serialization::serdes::{
    self, Boolean as BooleanSerde, ByteArray as ByteArraySerde, ByteBuffer as ByteBufferSerde, Bytes as BytesSerde,
    Double as DoubleSerde, Float as FloatSerde, Integer as IntegerSerde, Long as LongSerde, Short as ShortSerde,
    String as StringSerde, UUID as UUIDSerde, Void as VoidSerde,
};
use crate::common::serialization::{
    Deserializer, FloatSerializer, IntegerSerializer, ListDeserializer, ListSerializer, LongSerializer, Serde,
    SerializationStrategy, Serializer, ShortSerializer, StringDeserializer, StringSerializer, UUIDDeserializer,
    UUIDSerializer,
};

const TOPIC: &str = "testTopic";

// -----------------------------------------------------------------------------
// allSerdesShouldRoundtripInput — split per type
// -----------------------------------------------------------------------------

/// Java: `allSerdesShouldRoundtripInput` for `String.class`.
#[test]
fn string_round_trip() {
    let serde = StringSerde();
    let cases: [Option<&str>; 2] = [None, Some("my string")];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.map(str::to_string).as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round.as_deref(), case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `Short.class`.
#[test]
fn short_round_trip() {
    let serde = ShortSerde();
    let cases: [Option<i16>; 3] = [None, Some(32767), Some(-32768)];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `Integer.class`.
#[test]
fn integer_round_trip() {
    let serde = IntegerSerde();
    let cases: [Option<i32>; 3] = [None, Some(423_412_424), Some(-41_243_432)];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `Long.class`.
#[test]
fn long_round_trip() {
    let serde = LongSerde();
    let cases: [Option<i64>; 3] = [None, Some(922_337_203_685_477_580), Some(-922_337_203_685_477_581)];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `Float.class`.
#[test]
fn float_round_trip() {
    let serde = FloatSerde();
    let cases: [Option<f32>; 3] = [None, Some(5_678_567.123_12), Some(-5_678_567.123_41)];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `Double.class`.
#[test]
fn double_round_trip() {
    let serde = DoubleSerde();
    let cases: [Option<f64>; 3] = [None, Some(5_678_567.123_12), Some(-5_678_567.123_41)];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `byte[].class`.
#[test]
fn byte_array_round_trip() {
    let serde = ByteArraySerde();
    let cases: [Option<Vec<u8>>; 2] = [None, Some(b"my string".to_vec())];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `ByteBuffer.class`.
#[test]
fn byte_buffer_round_trip() {
    let serde = ByteBufferSerde();
    let cases: [Option<Vec<u8>>; 2] = [None, Some(b"my string".to_vec())];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `Bytes.class`.
#[test]
fn bytes_round_trip() {
    let serde = BytesSerde();
    let cases: [Option<Bytes>; 2] = [None, Some(Bytes::copy_from_slice(b"my string"))];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

/// Java: `allSerdesShouldRoundtripInput` for `UUID.class`.
#[test]
fn uuid_round_trip() {
    let serde = UUIDSerde();
    let cases: [Option<Uuid>; 2] = [None, Some(Uuid::new_v4())];
    for case in cases {
        let bytes = serde.serializer().serialize(TOPIC, case.as_ref()).unwrap();
        let round = serde.deserializer().deserialize(TOPIC, bytes.as_deref()).unwrap();
        assert_eq!(round, case);
    }
}

// -----------------------------------------------------------------------------
// allSerdesShouldSupportNull — split per type
// -----------------------------------------------------------------------------

/// Java: `allSerdesShouldSupportNull` for String.
#[test]
fn string_supports_null() {
    let serde = StringSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&String>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for Short.
#[test]
fn short_supports_null() {
    let serde = ShortSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&i16>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for Integer.
#[test]
fn integer_supports_null() {
    let serde = IntegerSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&i32>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for Long.
#[test]
fn long_supports_null() {
    let serde = LongSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&i64>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for Float.
#[test]
fn float_supports_null() {
    let serde = FloatSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&f32>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for Double.
#[test]
fn double_supports_null() {
    let serde = DoubleSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&f64>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for byte[].
#[test]
fn byte_array_supports_null() {
    let serde = ByteArraySerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&Vec<u8>>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for ByteBuffer.
#[test]
fn byte_buffer_supports_null() {
    let serde = ByteBufferSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&Vec<u8>>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for Bytes.
#[test]
fn bytes_supports_null() {
    let serde = BytesSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&Bytes>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `allSerdesShouldSupportNull` for UUID.
#[test]
fn uuid_supports_null() {
    let serde = UUIDSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&Uuid>).unwrap().is_none());
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

// -----------------------------------------------------------------------------
// String-encoding tests
// -----------------------------------------------------------------------------

/// Java: `stringSerdeShouldSupportDifferentEncodings`.
#[test]
fn string_serde_should_support_different_encodings() {
    for encoding in ["UTF-8", "UTF-16"] {
        let mut serializer = StringSerializer::new();
        let mut deserializer = StringDeserializer::new();
        let mut configs = HashMap::new();
        configs.insert("key.serializer.encoding".to_string(), encoding.to_string());
        serializer.configure(&configs, true).unwrap();

        let mut configs2 = HashMap::new();
        configs2.insert("key.deserializer.encoding".to_string(), encoding.to_string());
        deserializer.configure(&configs2, true).unwrap();

        let str = "my string";
        let bytes = serializer.serialize(TOPIC, Some(str)).unwrap().unwrap();
        let round = deserializer.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
        assert_eq!(str, round, "encoding {encoding}");
    }
}

/// Java: `stringSerdeConfigureThrowsOnUnknownEncoding`.
#[test]
fn string_serde_configure_throws_on_unknown_encoding() {
    let encoding = "encoding-does-not-exist";
    let mut serializer = StringSerializer::new();
    let mut deserializer = StringDeserializer::new();

    let mut configs = HashMap::new();
    configs.insert("key.serializer.encoding".to_string(), encoding.to_string());
    let err = serializer.configure(&configs, true).unwrap_err();
    assert!(err.to_string().contains("Unsupported encoding"), "got: {err}");

    let mut configs2 = HashMap::new();
    configs2.insert("key.deserializer.encoding".to_string(), encoding.to_string());
    let err = deserializer.configure(&configs2, true).unwrap_err();
    assert!(err.to_string().contains("Unsupported encoding"), "got: {err}");
}

/// Java: `stringDeserializerSupportByteBuffer`. Rust simplification: we
/// don't have a separate ByteBuffer overload — the `&[u8]` API is the
/// canonical entry point.
#[test]
fn string_deserializer_support_bytes() {
    let data = "Hello, ByteBuffer!";
    let serde = StringSerde();
    let bytes = serde.serializer().serialize(TOPIC, Some(&data.to_string())).unwrap().unwrap();
    let round = serde.deserializer().deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(data, round);
}

// -----------------------------------------------------------------------------
// Float NaN, fixed-length error paths
// -----------------------------------------------------------------------------

/// Java: `floatDeserializerShouldThrowSerializationExceptionOnZeroBytes`.
#[test]
fn float_deserializer_throws_on_zero_bytes() {
    let serde = FloatSerde();
    let err = serde.deserializer().deserialize(TOPIC, Some(&[])).unwrap_err();
    assert!(
        err.to_string().contains("Size of data received by Deserializer is not 4"),
        "got: {err}"
    );
}

/// Java: `floatDeserializerShouldThrowSerializationExceptionOnTooFewBytes`.
#[test]
fn float_deserializer_throws_on_too_few_bytes() {
    let serde = FloatSerde();
    let err = serde.deserializer().deserialize(TOPIC, Some(&[0u8; 3])).unwrap_err();
    assert!(
        err.to_string().contains("Size of data received by Deserializer is not 4"),
        "got: {err}"
    );
}

/// Java: `floatDeserializerShouldThrowSerializationExceptionOnTooManyBytes`.
#[test]
fn float_deserializer_throws_on_too_many_bytes() {
    let serde = FloatSerde();
    let err = serde.deserializer().deserialize(TOPIC, Some(&[0u8; 5])).unwrap_err();
    assert!(
        err.to_string().contains("Size of data received by Deserializer is not 4"),
        "got: {err}"
    );
}

/// Java: `floatSerdeShouldPreserveNaNValues`.
#[test]
fn float_serde_should_preserve_nan_values() {
    let serde = FloatSerde();

    let some_nan_as_int_bits: u32 = 0x7f80_0001;
    let some_nan = f32::from_bits(some_nan_as_int_bits);
    let bytes = serde.serializer().serialize(TOPIC, Some(&some_nan)).unwrap().unwrap();
    let round = serde.deserializer().deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(some_nan_as_int_bits, round.to_bits());

    let another_nan_as_int_bits: u32 = 0x7f80_0002;
    let another_nan = f32::from_bits(another_nan_as_int_bits);
    let bytes = serde.serializer().serialize(TOPIC, Some(&another_nan)).unwrap().unwrap();
    let round = serde.deserializer().deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(another_nan_as_int_bits, round.to_bits());
}

// -----------------------------------------------------------------------------
// Void
// -----------------------------------------------------------------------------

/// Java: `testSerializeVoid`.
#[test]
fn void_serialize() {
    let serde = VoidSerde();
    assert!(serde.serializer().serialize(TOPIC, None::<&()>).unwrap().is_none());
}

/// Java: `testDeserializeVoid`.
#[test]
fn void_deserialize() {
    let serde = VoidSerde();
    assert!(serde.deserializer().deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `voidDeserializerShouldThrowOnNotNullValues`.
#[test]
fn void_deserializer_throws_on_not_null_values() {
    let serde = VoidSerde();
    let err = serde.deserializer().deserialize(TOPIC, Some(&[0u8; 5])).unwrap_err();
    assert!(
        err.to_string().contains("Data should be null for a VoidDeserializer."),
        "got: {err}"
    );
}

// -----------------------------------------------------------------------------
// Boolean (parameterized over true/false in Java)
// -----------------------------------------------------------------------------

/// Java: `testBooleanSerializer` parameterized with `{true, false}`.
#[test]
fn boolean_serializer_true() {
    let serde = BooleanSerde();
    let test_data = vec![1u8];
    let bytes = serde.serializer().serialize(TOPIC, Some(&true)).unwrap().unwrap();
    assert_eq!(bytes, test_data);
}
#[test]
fn boolean_serializer_false() {
    let serde = BooleanSerde();
    let test_data = vec![0u8];
    let bytes = serde.serializer().serialize(TOPIC, Some(&false)).unwrap().unwrap();
    assert_eq!(bytes, test_data);
}

/// Java: `testBooleanDeserializer` parameterized with `{true, false}`.
#[test]
fn boolean_deserializer_true() {
    let serde = BooleanSerde();
    let v = serde.deserializer().deserialize(TOPIC, Some(&[1u8])).unwrap().unwrap();
    assert!(v);
}
#[test]
fn boolean_deserializer_false() {
    let serde = BooleanSerde();
    let v = serde.deserializer().deserialize(TOPIC, Some(&[0u8])).unwrap().unwrap();
    assert!(!v);
}

/// Java: `booleanDeserializerShouldThrowOnEmptyInput`.
#[test]
fn boolean_deserializer_should_throw_on_empty_input() {
    let serde = BooleanSerde();
    let err = serde.deserializer().deserialize(TOPIC, Some(&[])).unwrap_err();
    assert!(
        err.to_string()
            .contains("Size of data received by BooleanDeserializer is not 1"),
        "got: {err}"
    );
}

// -----------------------------------------------------------------------------
// `Serdes::serdeFrom` factory
// -----------------------------------------------------------------------------

/// Java: `testSerdeFromNotNull`. Java throws when the serializer arg is
/// null. Rust types are non-null by construction so the precondition is
/// enforced at the type system level — verify the factory works with
/// non-null inputs (compile-time guarantee).
#[test]
fn serde_from_constructs_non_null() {
    let serializer = LongSerializer;
    let deserializer = crate::common::serialization::LongDeserializer;
    let serde = serdes::serde_from::<i64, _, _>(serializer, deserializer);
    let bytes = serde.serializer().serialize(TOPIC, Some(&42i64)).unwrap().unwrap();
    let round = serde.deserializer().deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, 42);
}

// -----------------------------------------------------------------------------
// Wire-byte fixtures (CLAUDE.md DoD #3 — byte-level encoding tests against
// Java-equivalent vectors. Hand-computed from Java's serializers since the
// formats are documented BE.)
// -----------------------------------------------------------------------------

/// Verify Integer wire bytes match Java's `IntegerSerializer` (BE 4-byte).
#[test]
fn integer_wire_bytes() {
    let serde = IntegerSerde();
    // 0x01020304 (16909060)
    let bytes = serde.serializer().serialize(TOPIC, Some(&16_909_060_i32)).unwrap().unwrap();
    assert_eq!(bytes, vec![0x01, 0x02, 0x03, 0x04]);
    // -1 (0xFFFFFFFF)
    let bytes = serde.serializer().serialize(TOPIC, Some(&-1_i32)).unwrap().unwrap();
    assert_eq!(bytes, vec![0xFF, 0xFF, 0xFF, 0xFF]);
}

/// Verify Long wire bytes match Java's `LongSerializer` (BE 8-byte).
#[test]
fn long_wire_bytes() {
    let serde = LongSerde();
    // 0x0102030405060708
    let bytes = serde
        .serializer()
        .serialize(TOPIC, Some(&72_623_859_790_382_856_i64))
        .unwrap()
        .unwrap();
    assert_eq!(bytes, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
}

/// Verify Short wire bytes match Java's `ShortSerializer` (BE 2-byte).
#[test]
fn short_wire_bytes() {
    let serde = ShortSerde();
    let bytes = serde.serializer().serialize(TOPIC, Some(&0x0102_i16)).unwrap().unwrap();
    assert_eq!(bytes, vec![0x01, 0x02]);
    let bytes = serde.serializer().serialize(TOPIC, Some(&-1_i16)).unwrap().unwrap();
    assert_eq!(bytes, vec![0xFF, 0xFF]);
}

/// Verify Boolean wire bytes match Java's `BooleanSerializer` (1-byte 0x01/0x00).
#[test]
fn boolean_wire_bytes() {
    let serde = BooleanSerde();
    let t = serde.serializer().serialize(TOPIC, Some(&true)).unwrap().unwrap();
    let f = serde.serializer().serialize(TOPIC, Some(&false)).unwrap().unwrap();
    assert_eq!(t, vec![0x01]);
    assert_eq!(f, vec![0x00]);
}

/// Verify Float wire bytes — `0.0_f32` → 0x00000000.
#[test]
fn float_wire_bytes() {
    let serde = FloatSerde();
    let bytes = serde.serializer().serialize(TOPIC, Some(&0.0_f32)).unwrap().unwrap();
    assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x00]);
    let bytes = serde.serializer().serialize(TOPIC, Some(&1.0_f32)).unwrap().unwrap();
    assert_eq!(bytes, vec![0x3F, 0x80, 0x00, 0x00]);
}

/// Verify Double wire bytes — `1.0_f64` → 0x3FF0000000000000.
#[test]
fn double_wire_bytes() {
    let serde = DoubleSerde();
    let bytes = serde.serializer().serialize(TOPIC, Some(&1.0_f64)).unwrap().unwrap();
    assert_eq!(bytes, vec![0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
}

/// Verify ByteArray identity wire bytes.
#[test]
fn byte_array_wire_bytes() {
    let serde = ByteArraySerde();
    let bytes = serde.serializer().serialize(TOPIC, Some(&b"abcd".to_vec())).unwrap().unwrap();
    assert_eq!(bytes, b"abcd");
}

// -----------------------------------------------------------------------------
// `serialize_to` zero-copy path
// -----------------------------------------------------------------------------

/// Verify the `serialize_to` API of `IntegerSerializer` writes 4 BE bytes
/// directly into the buffer (no intermediate Vec allocation).
#[test]
fn integer_serialize_to_writes_directly() {
    let mut out = Vec::with_capacity(4);
    let written = IntegerSerializer.serialize_to(TOPIC, Some(&0x01020304_i32), &mut out).unwrap();
    assert!(written);
    assert_eq!(out, vec![0x01, 0x02, 0x03, 0x04]);
}

/// `serialize_to` with `None` writes nothing and returns `Ok(false)`.
#[test]
fn integer_serialize_to_null_writes_nothing() {
    let mut out = Vec::new();
    let written = IntegerSerializer.serialize_to(TOPIC, None, &mut out).unwrap();
    assert!(!written);
    assert!(out.is_empty());
}

/// Multi-record `serialize_to` keeps the buffer single-pass — appending two
/// records does not require any reset/clone.
#[test]
fn long_serialize_to_appends() {
    let mut out = Vec::new();
    LongSerializer.serialize_to(TOPIC, Some(&1_i64), &mut out).unwrap();
    LongSerializer.serialize_to(TOPIC, Some(&2_i64), &mut out).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&1_i64.to_be_bytes());
    expected.extend_from_slice(&2_i64.to_be_bytes());
    assert_eq!(out, expected);
}

// -----------------------------------------------------------------------------
// UUID via ListDeserializer-like shape check (36-byte text form)
// -----------------------------------------------------------------------------

/// Verify UUIDSerializer produces 36-byte UTF-8 text form (matching Java's
/// `UUID.toString()`). Phase 3b List support depends on this length.
#[test]
fn uuid_serializer_produces_36_bytes_utf8() {
    let serializer = UUIDSerializer::new();
    let id = Uuid::new_v4();
    let bytes = serializer.serialize(TOPIC, Some(&id)).unwrap().unwrap();
    assert_eq!(bytes.len(), 36);
    // Round-trip
    let deserializer = UUIDDeserializer::new();
    let round = deserializer.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, id);
}

// -----------------------------------------------------------------------------
// ListSerde tests (translated from SerializationTest list*Tests)
// -----------------------------------------------------------------------------

fn list_int_serde() -> (
    ListSerializer<i32, IntegerSerializer>,
    ListDeserializer<i32, crate::common::serialization::IntegerDeserializer>,
) {
    (
        ListSerializer::new(IntegerSerializer, InnerKind::FixedSize(4)),
        ListDeserializer::new(crate::common::serialization::IntegerDeserializer, InnerKind::FixedSize(4)),
    )
}

fn list_short_serde() -> (
    ListSerializer<i16, ShortSerializer>,
    ListDeserializer<i16, crate::common::serialization::ShortDeserializer>,
) {
    (
        ListSerializer::new(ShortSerializer, InnerKind::FixedSize(2)),
        ListDeserializer::new(crate::common::serialization::ShortDeserializer, InnerKind::FixedSize(2)),
    )
}

fn list_long_serde() -> (
    ListSerializer<i64, LongSerializer>,
    ListDeserializer<i64, crate::common::serialization::LongDeserializer>,
) {
    (
        ListSerializer::new(LongSerializer, InnerKind::FixedSize(8)),
        ListDeserializer::new(crate::common::serialization::LongDeserializer, InnerKind::FixedSize(8)),
    )
}

fn list_float_serde() -> (
    ListSerializer<f32, FloatSerializer>,
    ListDeserializer<f32, crate::common::serialization::FloatDeserializer>,
) {
    (
        ListSerializer::new(FloatSerializer, InnerKind::FixedSize(4)),
        ListDeserializer::new(crate::common::serialization::FloatDeserializer, InnerKind::FixedSize(4)),
    )
}

fn list_double_serde() -> (
    ListSerializer<f64, crate::common::serialization::DoubleSerializer>,
    ListDeserializer<f64, crate::common::serialization::DoubleDeserializer>,
) {
    (
        ListSerializer::new(crate::common::serialization::DoubleSerializer, InnerKind::FixedSize(8)),
        ListDeserializer::new(crate::common::serialization::DoubleDeserializer, InnerKind::FixedSize(8)),
    )
}

fn list_uuid_serde() -> (ListSerializer<Uuid, UUIDSerializer>, ListDeserializer<Uuid, UUIDDeserializer>) {
    (
        ListSerializer::new(UUIDSerializer::default(), InnerKind::FixedSize(36)),
        ListDeserializer::new(UUIDDeserializer::default(), InnerKind::FixedSize(36)),
    )
}

fn list_string_serde() -> (
    ListSerializer<String, crate::common::serialization::serdes::StringOwnedSerializer>,
    ListDeserializer<String, StringDeserializer>,
) {
    (
        ListSerializer::new(
            crate::common::serialization::serdes::StringOwnedSerializer::default(),
            InnerKind::VariableSize,
        ),
        ListDeserializer::new(StringDeserializer::default(), InnerKind::VariableSize),
    )
}

/// Java: `listSerdeShouldReturnEmptyCollection`.
#[test]
fn list_serde_should_return_empty_collection() {
    let (ser, des) = list_int_serde();
    let test_data: Vec<Option<i32>> = Vec::new();
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeShouldReturnNull`.
#[test]
fn list_serde_should_return_null() {
    let (ser, des) = list_int_serde();
    assert!(ser.serialize(TOPIC, None).unwrap().is_none());
    assert!(des.deserialize(TOPIC, None).unwrap().is_none());
}

/// Java: `listSerdeShouldRoundtripIntPrimitiveInput`.
#[test]
fn list_serde_should_round_trip_int_primitive_input() {
    let (ser, des) = list_int_serde();
    let test_data: Vec<Option<i32>> = vec![Some(1), Some(2), Some(3)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeSerializerShouldReturnByteArrayOfFixedSizeForIntPrimitiveInput`.
/// 1 (strategy) + 4 (null-count=0) + 4 (size=3) + 3*4 = 21
#[test]
fn list_serde_int_primitive_byte_count_is_21() {
    let (ser, _) = list_int_serde();
    let test_data: Vec<Option<i32>> = vec![Some(1), Some(2), Some(3)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    assert_eq!(bytes.len(), 21);
}

/// Java: `listSerdeShouldRoundtripShortPrimitiveInput`.
#[test]
fn list_serde_should_round_trip_short_primitive_input() {
    let (ser, des) = list_short_serde();
    let test_data: Vec<Option<i16>> = vec![Some(1), Some(2), Some(3)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeSerializerShouldReturnByteArrayOfFixedSizeForShortPrimitiveInput`.
/// 1 + 4 + 4 + 3*2 = 15
#[test]
fn list_serde_short_primitive_byte_count_is_15() {
    let (ser, _) = list_short_serde();
    let test_data: Vec<Option<i16>> = vec![Some(1), Some(2), Some(3)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    assert_eq!(bytes.len(), 15);
}

/// Java: `listSerdeShouldRoundtripFloatPrimitiveInput`.
#[test]
fn list_serde_should_round_trip_float_primitive_input() {
    let (ser, des) = list_float_serde();
    let test_data: Vec<Option<f32>> = vec![Some(1.0), Some(2.0), Some(3.0)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeSerializerShouldReturnByteArrayOfFixedSizeForFloatPrimitiveInput`. 21 bytes.
#[test]
fn list_serde_float_primitive_byte_count_is_21() {
    let (ser, _) = list_float_serde();
    let test_data: Vec<Option<f32>> = vec![Some(1.0), Some(2.0), Some(3.0)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    assert_eq!(bytes.len(), 21);
}

/// Java: `listSerdeShouldRoundtripLongPrimitiveInput`.
#[test]
fn list_serde_should_round_trip_long_primitive_input() {
    let (ser, des) = list_long_serde();
    let test_data: Vec<Option<i64>> = vec![Some(1), Some(2), Some(3)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeSerializerShouldReturnByteArrayOfFixedSizeForLongPrimitiveInput`. 33 bytes.
#[test]
fn list_serde_long_primitive_byte_count_is_33() {
    let (ser, _) = list_long_serde();
    let test_data: Vec<Option<i64>> = vec![Some(1), Some(2), Some(3)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    assert_eq!(bytes.len(), 33);
}

/// Java: `listSerdeShouldRoundtripDoublePrimitiveInput`.
#[test]
fn list_serde_should_round_trip_double_primitive_input() {
    let (ser, des) = list_double_serde();
    let test_data: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeSerializerShouldReturnByteArrayOfFixedSizeForDoublePrimitiveInput`. 33 bytes.
#[test]
fn list_serde_double_primitive_byte_count_is_33() {
    let (ser, _) = list_double_serde();
    let test_data: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    assert_eq!(bytes.len(), 33);
}

/// Java: `listSerdeShouldRoundtripUUIDInput`.
#[test]
fn list_serde_should_round_trip_uuid_input() {
    let (ser, des) = list_uuid_serde();
    let test_data: Vec<Option<Uuid>> = vec![Some(Uuid::new_v4()), Some(Uuid::new_v4()), Some(Uuid::new_v4())];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeSerializerShouldReturnByteArrayOfFixedSizeForUUIDInput`. 117 bytes.
/// 1 (strategy) + 4 (null-count) + 4 (size) + 3*36 = 117
#[test]
fn list_serde_uuid_byte_count_is_117() {
    let (ser, _) = list_uuid_serde();
    let test_data: Vec<Option<Uuid>> = vec![Some(Uuid::new_v4()), Some(Uuid::new_v4()), Some(Uuid::new_v4())];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    assert_eq!(bytes.len(), 117);
}

/// Java: `listSerdeShouldRoundtripNonPrimitiveInput`.
#[test]
fn list_serde_should_round_trip_non_primitive_input() {
    let (ser, des) = list_string_serde();
    let test_data: Vec<Option<String>> = vec![Some("A".to_string()), Some("B".to_string()), Some("C".to_string())];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeShouldRoundtripPrimitiveInputWithNullEntries`.
#[test]
fn list_serde_should_round_trip_primitive_input_with_null_entries() {
    let (ser, des) = list_int_serde();
    let test_data: Vec<Option<i32>> = vec![Some(1), None, Some(3)];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Java: `listSerdeShouldRoundtripNonPrimitiveInputWithNullEntries`.
#[test]
fn list_serde_should_round_trip_non_primitive_input_with_null_entries() {
    let (ser, des) = list_string_serde();
    let test_data: Vec<Option<String>> = vec![Some("A".to_string()), None, Some("C".to_string())];
    let bytes = ser.serialize(TOPIC, Some(&test_data)).unwrap().unwrap();
    let round = des.deserialize(TOPIC, Some(&bytes)).unwrap().unwrap();
    assert_eq!(round, test_data);
}

/// Verify the `SerializationStrategy` choice flows from `InnerKind`.
#[test]
fn list_serializer_strategy_is_constant_for_fixed_inner() {
    let (ser, _) = list_int_serde();
    assert_eq!(ser.strategy(), SerializationStrategy::ConstantSize);
}

#[test]
fn list_serializer_strategy_is_variable_for_variable_inner() {
    let (ser, _) = list_string_serde();
    assert_eq!(ser.strategy(), SerializationStrategy::VariableSize);
}

// -----------------------------------------------------------------------------
// Skipped Java tests (with rationale)
// -----------------------------------------------------------------------------
// - `listSerdeShouldReturnLinkedList`/`listSerdeShouldReturnStack` (Java
//   `ListDeserializerTest`-adjacent): the Rust `ListDeserializer` always
//   returns `Vec<Option<T>>`. Java preserves the concrete `List` subclass
//   via reflection-based instantiation; Rust does not have list-class
//   reflection, so the test is not applicable. Documented in
//   `phase3b_list_serde_gap.md`.
// - `testSerdeFromUnknown`: Java's `Serdes.serdeFrom(Class<T>)` factory has
//   no Rust analog (no class-keyed factory). Per-type factory functions
//   (`serdes::Long()`, etc.) replace it; the unknown-type case is
//   compile-time-impossible.
// - `ListSerializerTest` and `ListDeserializerTest`: those Java tests
//   exercise runtime class-loading paths (`Utils.newInstance(class_name)`)
//   that do not exist in Rust. The list-serde tests above cover the
//   round-trip surface that *does* translate. See
//   `phase3b_list_serde_gap.md` for the full deferral rationale.
