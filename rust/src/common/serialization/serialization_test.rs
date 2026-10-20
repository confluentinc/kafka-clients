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

//! Translation of Java's
//! `org.apache.kafka.common.serialization.SerializationTest`, for the serdes
//! the Rust client has: `StringSerializer` / `StringDeserializer`,
//! `ByteArraySerializer` / `ByteArrayDeserializer` and `BytesDeserializer`
//! (Java's `Bytes` serde; the Rust `Bytes` value is serialized by
//! `ByteArraySerializer`, as Java's `BytesSerializer` writes `Bytes.get()`).
//!
//! Java's `testData` map lists one entry per serde class; only the `String`,
//! `byte[]` and `Bytes` entries have a Rust serde. Java's `byte[]` and `Bytes`
//! `null` entries become `None` on the serializer side.
//!
//! # Tests not translated, and why
//!
//! - The `Short`, `Integer`, `Long`, `Float`, `Double`, `ByteBuffer` and
//!   `UUID` entries of `allSerdesShouldRoundtripInput` /
//!   `allSerdesShouldSupportNull`: the Rust client has no serde for those
//!   types.
//! - The deserializer half of `allSerdesShouldSupportNull`
//!   (`deserialize(topic, null)` and `deserialize(topic, null, (ByteBuffer)
//!   null)`): a Rust deserializer takes `&[u8]` / `&Bytes`, which cannot be
//!   null. The consumer never calls the deserializer for a null key or value
//!   (the record's field is `None`), as Java's `CompletedFetch` passes the null
//!   through without decoding it.
//! - `testSerdeFromUnknown`, `testSerdeFromNotNull`: Rust has no `Serdes`
//!   class-based factory (no reflection, CLAUDE.md §2).
//! - The UTF-16 case of `stringSerdeShouldSupportDifferentEncodings`, and
//!   `stringSerdeConfigureThrowsOnUnknownEncoding`: the Rust String serdes are
//!   UTF-8 only and keep the trait's no-op `configure`, so they read no
//!   `*.encoding` config. Java rejects an unknown charset by throwing from
//!   `configure` (`StringSerializer.java:47`, `StringDeserializer.java:50`);
//!   the Rust `configure` returns `()`, so it has no way to report it. The
//!   UTF-8 case is translated below.
//! - Every `listSerde*` test: there is no `ListSerializer` /
//!   `ListDeserializer`.
//! - `floatDeserializerShouldThrow*`, `floatSerdeShouldPreserveNaNValues`: no
//!   `Float` serde.
//! - `testSerializeVoid`, `testDeserializeVoid`,
//!   `voidDeserializerShouldThrowOnNotNullValues`: no `Void` serde.
//! - `testBooleanSerializer`, `testBooleanDeserializer`,
//!   `booleanDeserializerShouldThrowOnEmptyInput`: no `Boolean` serde.
//! - The direct-buffer half of `stringDeserializerSupportByteBuffer`: a
//!   `Bytes` has no heap/direct distinction; the heap case, a slice that does
//!   not span its whole buffer, is translated below.

use bytes::Bytes;

use crate::common::header::RecordHeaders;
use crate::common::serialization::{
    ByteArrayDeserializer, ByteArraySerializer, BytesDeserializer, Deserializer, Serializer, StringDeserializer,
    StringSerializer,
};

const TOPIC: &str = "testTopic";

/// Java's `wrapNullable(serialized)`: the serialized bytes as the buffer the
/// KIP-863 overload receives.
fn wrap(serialized: &[u8]) -> Bytes {
    Bytes::copy_from_slice(serialized)
}

/// The `String` entry of Java's `allSerdesShouldRoundtripInput`
/// (`SerializationTest.java:72-91`), through both deserializer overloads.
#[tokio::test]
#[doc(alias = "org.apache.kafka.common.serialization.SerializationTest#allSerdesShouldRoundtripInput")]
async fn all_serdes_should_roundtrip_input_with_string() {
    let serializer = StringSerializer::new();
    let deserializer = StringDeserializer::new();
    let value = "my string";
    let serialized = Serializer::<str>::serialize(&serializer, TOPIC, Some(value))
        .await
        .expect("serialize")
        .expect("a non-null value serializes to bytes");
    assert_eq!(
        value,
        deserializer.deserialize(TOPIC, &serialized).await.expect("deserialize"),
        "Should get the original String after serialization and deserialization"
    );
    let headers = RecordHeaders::new();
    assert_eq!(
        value,
        deserializer
            .deserialize_with_headers_bytes(TOPIC, &headers, &wrap(&serialized))
            .await
            .expect("deserialize"),
        "Should get the original String after serialization and deserialization"
    );
}

/// The `byte[]` entry of Java's `allSerdesShouldRoundtripInput`, which uses
/// the `byte[]` overload `deserialize(topic, null, (byte[]) value)` for its
/// second check, plus the KIP-863 overload the consumer calls.
#[tokio::test]
#[doc(alias = "org.apache.kafka.common.serialization.SerializationTest#allSerdesShouldRoundtripInput")]
async fn all_serdes_should_roundtrip_input_with_byte_array() {
    let serializer = ByteArraySerializer::new();
    let deserializer = ByteArrayDeserializer::new();
    let value: &[u8] = b"my string";
    let serialized = Serializer::<[u8]>::serialize(&serializer, TOPIC, Some(value))
        .await
        .expect("serialize")
        .expect("a non-null value serializes to bytes");
    assert_eq!(
        value,
        deserializer.deserialize(TOPIC, &serialized).await.expect("deserialize"),
        "Should get the original byte[] after serialization and deserialization"
    );
    let headers = RecordHeaders::new();
    assert_eq!(
        value,
        deserializer
            .deserialize_with_headers(TOPIC, &headers, value)
            .await
            .expect("deserialize"),
        "Should get the original byte[] after serialization and deserialization"
    );
    assert_eq!(
        value,
        deserializer
            .deserialize_with_headers_bytes(TOPIC, &headers, &wrap(&serialized))
            .await
            .expect("deserialize"),
        "Should get the original byte[] after serialization and deserialization"
    );
}

/// The `Bytes` entry of Java's `allSerdesShouldRoundtripInput`.
#[tokio::test]
#[doc(alias = "org.apache.kafka.common.serialization.SerializationTest#allSerdesShouldRoundtripInput")]
async fn all_serdes_should_roundtrip_input_with_bytes() {
    let serializer = ByteArraySerializer::new();
    let deserializer = BytesDeserializer::new();
    let value = Bytes::from_static(b"my string");
    let serialized = Serializer::<[u8]>::serialize(&serializer, TOPIC, Some(&value[..]))
        .await
        .expect("serialize")
        .expect("a non-null value serializes to bytes");
    assert_eq!(
        value,
        deserializer.deserialize(TOPIC, &serialized).await.expect("deserialize"),
        "Should get the original Bytes after serialization and deserialization"
    );
    let headers = RecordHeaders::new();
    assert_eq!(
        value,
        deserializer
            .deserialize_with_headers_bytes(TOPIC, &headers, &wrap(&serialized))
            .await
            .expect("deserialize"),
        "Should get the original Bytes after serialization and deserialization"
    );
}

/// The serializer half of Java's `allSerdesShouldSupportNull`
/// (`SerializationTest.java:93-105`) for the String and byte[] serdes (the
/// latter also serializes Java's `Bytes`).
#[tokio::test]
#[doc(alias = "org.apache.kafka.common.serialization.SerializationTest#allSerdesShouldSupportNull")]
async fn all_serdes_should_support_null() {
    assert!(
        Serializer::<str>::serialize(&StringSerializer::new(), TOPIC, None)
            .await
            .expect("serialize")
            .is_none(),
        "Should support null in String serialization"
    );
    assert!(
        Serializer::<String>::serialize(&StringSerializer::new(), TOPIC, None)
            .await
            .expect("serialize")
            .is_none(),
        "Should support null in String serialization"
    );
    assert!(
        Serializer::<[u8]>::serialize(&ByteArraySerializer::new(), TOPIC, None)
            .await
            .expect("serialize")
            .is_none(),
        "Should support null in byte[] serialization"
    );
    assert!(
        Serializer::<Vec<u8>>::serialize(&ByteArraySerializer::new(), TOPIC, None)
            .await
            .expect("serialize")
            .is_none(),
        "Should support null in byte[] serialization"
    );
}

/// The UTF-8 case of Java's `stringSerdeShouldSupportDifferentEncodings`
/// (`SerializationTest.java:119-132`), configured through `key.*.encoding` as
/// Java's `getStringSerde` does.
#[tokio::test]
#[doc(alias = "org.apache.kafka.common.serialization.SerializationTest#stringSerdeShouldSupportDifferentEncodings")]
async fn string_serde_should_support_different_encodings() {
    let value = "my string";
    let mut serializer = StringSerializer::new();
    let serializer_configs = [("key.serializer.encoding".to_string(), "UTF-8".to_string())].into();
    Serializer::<str>::configure(&mut serializer, &serializer_configs, true).await;
    let mut deserializer = StringDeserializer::new();
    let deserializer_configs = [("key.deserializer.encoding".to_string(), "UTF-8".to_string())].into();
    deserializer.configure(&deserializer_configs, true).await;
    let serialized = Serializer::<str>::serialize(&serializer, TOPIC, Some(value))
        .await
        .expect("serialize")
        .expect("a non-null value serializes to bytes");
    assert_eq!(
        value,
        deserializer.deserialize(TOPIC, &serialized).await.expect("deserialize"),
        "Should get the original string after serialization and deserialization with encoding UTF-8"
    );
}

/// Java's `stringDeserializerSupportByteBuffer` (`SerializationTest.java:393-407`):
/// the KIP-863 overload decodes only the buffer's readable part, here a slice
/// of a buffer twice its size, as Java's heap buffer of
/// `serializedBytes.length << 1`.
#[tokio::test]
#[doc(alias = "org.apache.kafka.common.serialization.SerializationTest#stringDeserializerSupportByteBuffer")]
async fn string_deserializer_support_byte_buffer() {
    let data = "Hello, ByteBuffer!";
    let serializer = StringSerializer::new();
    let serialized = Serializer::<str>::serialize(&serializer, TOPIC, Some(data))
        .await
        .expect("serialize")
        .expect("a non-null value serializes to bytes");
    let mut backing = vec![0u8; serialized.len() << 1];
    backing[..serialized.len()].copy_from_slice(&serialized);
    let heap_buff = Bytes::from(backing).slice(..serialized.len());
    let headers = RecordHeaders::new();
    assert_eq!(
        data,
        StringDeserializer::new()
            .deserialize_with_headers_bytes(TOPIC, &headers, &heap_buff)
            .await
            .expect("deserialize")
    );
}

/// The KIP-863 overload on each deserializer decodes the slice it is given, a
/// window into a larger shared buffer as the consumer passes it, and
/// `BytesDeserializer` hands that window back without copying.
#[tokio::test]
async fn test_deserialize_with_headers_bytes_reads_only_the_record_slice() {
    let buffer = Bytes::from_static(b"<<my string>>");
    let record = buffer.slice(2..11);
    let headers = RecordHeaders::new();
    assert_eq!(
        "my string",
        StringDeserializer::new()
            .deserialize_with_headers_bytes(TOPIC, &headers, &record)
            .await
            .expect("deserialize")
    );
    assert_eq!(
        b"my string".to_vec(),
        ByteArrayDeserializer::new()
            .deserialize_with_headers_bytes(TOPIC, &headers, &record)
            .await
            .expect("deserialize")
    );
    let bytes = BytesDeserializer::new()
        .deserialize_with_headers_bytes(TOPIC, &headers, &record)
        .await
        .expect("deserialize");
    assert_eq!(&b"my string"[..], &bytes[..]);
    assert_eq!(record.as_ptr(), bytes.as_ptr(), "BytesDeserializer shares the buffer");
}
