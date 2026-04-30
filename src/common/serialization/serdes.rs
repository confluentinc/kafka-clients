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

//! Translation of `org.apache.kafka.common.serialization.Serdes`.
//!
//! Factory for creating serializers / deserializers.

use std::collections::HashMap;

use bytes::Bytes;
use uuid::Uuid;

use crate::common::KafkaError;
use crate::common::serialization::{
    BooleanDeserializer, BooleanSerializer, ByteArrayDeserializer, ByteBufferDeserializer, ByteBufferSerializer,
    BytesDeserializer, BytesSerializer, Deserializer, DoubleDeserializer, DoubleSerializer, FloatDeserializer,
    FloatSerializer, IntegerDeserializer, IntegerSerializer, LongDeserializer, LongSerializer, Serde, Serializer,
    ShortDeserializer, ShortSerializer, StringDeserializer, StringSerializer, UUIDDeserializer, UUIDSerializer,
    VoidDeserializer, VoidSerializer,
};

/// Generic wrapper that implements [`Serde`] in terms of a separate
/// serializer + deserializer pair. Mirrors Java's `Serdes.WrapperSerde`.
///
/// `S` and `D` are owned concrete types; the [`Serde`] trait returns
/// `&dyn Serializer<T>` / `&dyn Deserializer<T>` so callers can route
/// without naming the concrete types.
pub struct WrapperSerde<T, S, D>
where
    S: Serializer<T> + 'static,
    D: Deserializer<T> + 'static,
{
    serializer: S,
    deserializer: D,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T, S, D> WrapperSerde<T, S, D>
where
    S: Serializer<T> + 'static,
    D: Deserializer<T> + 'static,
{
    /// Construct a wrapper from a serializer and deserializer.
    pub fn new(serializer: S, deserializer: D) -> Self {
        WrapperSerde { serializer, deserializer, _marker: std::marker::PhantomData }
    }

    /// Mutable access to the underlying serializer (for tests / configure
    /// flow). Java exposes this implicitly via `serializer().configure(...)`
    /// because the field is mutable; in Rust the `&dyn Serializer<T>` from
    /// [`Serde::serializer`] returns an immutable reference, so callers that
    /// want to configure mid-flight take this method instead.
    pub fn serializer_mut(&mut self) -> &mut S {
        &mut self.serializer
    }

    /// Mutable access to the underlying deserializer.
    pub fn deserializer_mut(&mut self) -> &mut D {
        &mut self.deserializer
    }
}

impl<T, S, D> Serde<T> for WrapperSerde<T, S, D>
where
    T: Send + Sync + 'static,
    S: Serializer<T> + 'static,
    D: Deserializer<T> + 'static,
{
    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), KafkaError> {
        self.serializer.configure(configs, is_key)?;
        self.deserializer.configure(configs, is_key)?;
        Ok(())
    }

    fn close(&mut self) {
        self.serializer.close();
        self.deserializer.close();
    }

    fn serializer(&self) -> &dyn Serializer<T> {
        &self.serializer
    }

    fn deserializer(&self) -> &dyn Deserializer<T> {
        &self.deserializer
    }
}

// ---- per-type Serde aliases (one per Java `Serdes.XxxSerde`) ----

/// Serde for [`Vec<u8>`] (Java's `byte[]`).
pub type ByteArraySerde = WrapperSerde<Vec<u8>, ByteArrayOwnedSerializer, ByteArrayDeserializer>;

/// Owned-Vec wrapper around [`ByteArraySerializer`] so the serde lines up on
/// `Vec<u8>` as the value type. The producer hot path should use
/// [`ByteArraySerializer`] directly (which targets `[u8]`); the serde
/// wrapper is for symmetry with the Java factory.
#[derive(Default, Debug, Clone, Copy)]
pub struct ByteArrayOwnedSerializer;

impl Serializer<Vec<u8>> for ByteArrayOwnedSerializer {
    fn serialize(&self, _topic: &str, data: Option<&Vec<u8>>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.cloned())
    }

    fn serialize_to(&self, _topic: &str, data: Option<&Vec<u8>>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(v) => {
                out.extend_from_slice(v);
                Ok(true)
            },
            None => Ok(false),
        }
    }
}

/// Owned-`String` wrapper around [`StringSerializer`] so the serde lines up
/// on `String` as the value type. The hot-path serializer targets `&str`.
#[derive(Default, Debug, Clone, Copy)]
pub struct StringOwnedSerializer(StringSerializer);

impl Serializer<String> for StringOwnedSerializer {
    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), KafkaError> {
        self.0.configure(configs, is_key)
    }

    fn serialize(&self, topic: &str, data: Option<&String>) -> Result<Option<Vec<u8>>, KafkaError> {
        self.0.serialize(topic, data.map(String::as_str))
    }

    fn serialize_to(&self, topic: &str, data: Option<&String>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        self.0.serialize_to(topic, data.map(String::as_str), out)
    }
}

/// Serde for [`String`].
pub type StringSerde = WrapperSerde<String, StringOwnedSerializer, StringDeserializer>;

/// Serde for [`i16`] (Java's `Short`).
pub type ShortSerde = WrapperSerde<i16, ShortSerializer, ShortDeserializer>;

/// Serde for [`i32`] (Java's `Integer`).
pub type IntegerSerde = WrapperSerde<i32, IntegerSerializer, IntegerDeserializer>;

/// Serde for [`i64`] (Java's `Long`).
pub type LongSerde = WrapperSerde<i64, LongSerializer, LongDeserializer>;

/// Serde for [`f32`] (Java's `Float`).
pub type FloatSerde = WrapperSerde<f32, FloatSerializer, FloatDeserializer>;

/// Serde for [`f64`] (Java's `Double`).
pub type DoubleSerde = WrapperSerde<f64, DoubleSerializer, DoubleDeserializer>;

/// Serde for [`bool`] (Java's `Boolean`).
pub type BooleanSerde = WrapperSerde<bool, BooleanSerializer, BooleanDeserializer>;

/// Serde for [`bytes::Bytes`] (Java's `org.apache.kafka.common.utils.Bytes`).
pub type BytesSerde = WrapperSerde<Bytes, BytesSerializer, BytesDeserializer>;

/// Serde for [`Vec<u8>`] used as a `ByteBuffer` analog (see
/// [`ByteBufferSerializer`] for why we map `ByteBuffer` to `Vec<u8>`).
pub type ByteBufferSerde = WrapperSerde<Vec<u8>, ByteBufferSerializer, ByteBufferDeserializer>;

/// Serde for [`uuid::Uuid`] (Java's `java.util.UUID`).
pub type UUIDSerde = WrapperSerde<Uuid, UUIDSerializer, UUIDDeserializer>;

/// Serde for `()` (Java's `Void`).
pub type VoidSerde = WrapperSerde<(), VoidSerializer, VoidDeserializer>;

// ---- per-type factory functions (one per Java `Serdes.Xyz()`) ----

/// A serde for nullable `Long` (i64) type. Mirrors Java's `Serdes.Long()`.
#[allow(non_snake_case)]
pub fn Long() -> LongSerde {
    LongSerde::new(LongSerializer, LongDeserializer)
}

/// A serde for nullable `Integer` (i32) type. Mirrors Java's `Serdes.Integer()`.
#[allow(non_snake_case)]
pub fn Integer() -> IntegerSerde {
    IntegerSerde::new(IntegerSerializer, IntegerDeserializer)
}

/// A serde for nullable `Short` (i16) type. Mirrors Java's `Serdes.Short()`.
#[allow(non_snake_case)]
pub fn Short() -> ShortSerde {
    ShortSerde::new(ShortSerializer, ShortDeserializer)
}

/// A serde for nullable `Float` (f32) type. Mirrors Java's `Serdes.Float()`.
#[allow(non_snake_case)]
pub fn Float() -> FloatSerde {
    FloatSerde::new(FloatSerializer, FloatDeserializer)
}

/// A serde for nullable `Double` (f64) type. Mirrors Java's `Serdes.Double()`.
#[allow(non_snake_case)]
pub fn Double() -> DoubleSerde {
    DoubleSerde::new(DoubleSerializer, DoubleDeserializer)
}

/// A serde for nullable `String` type. Mirrors Java's `Serdes.String()`.
#[allow(non_snake_case)]
pub fn String() -> StringSerde {
    StringSerde::new(StringOwnedSerializer::default(), StringDeserializer::default())
}

/// A serde for nullable `ByteBuffer` (Vec<u8>) type. Mirrors Java's `Serdes.ByteBuffer()`.
#[allow(non_snake_case)]
pub fn ByteBuffer() -> ByteBufferSerde {
    ByteBufferSerde::new(ByteBufferSerializer, ByteBufferDeserializer)
}

/// A serde for nullable `Bytes` (`bytes::Bytes`) type. Mirrors Java's `Serdes.Bytes()`.
#[allow(non_snake_case)]
pub fn Bytes() -> BytesSerde {
    BytesSerde::new(BytesSerializer, BytesDeserializer)
}

/// A serde for nullable `UUID` (`uuid::Uuid`) type. Mirrors Java's `Serdes.UUID()`.
#[allow(non_snake_case)]
pub fn UUID() -> UUIDSerde {
    UUIDSerde::new(UUIDSerializer::default(), UUIDDeserializer::default())
}

/// A serde for nullable `Boolean` type. Mirrors Java's `Serdes.Boolean()`.
#[allow(non_snake_case)]
pub fn Boolean() -> BooleanSerde {
    BooleanSerde::new(BooleanSerializer, BooleanDeserializer)
}

/// A serde for nullable `byte[]` (Vec<u8>) type. Mirrors Java's `Serdes.ByteArray()`.
#[allow(non_snake_case)]
pub fn ByteArray() -> ByteArraySerde {
    ByteArraySerde::new(ByteArrayOwnedSerializer, ByteArrayDeserializer)
}

/// A serde for `Void` (`()`) type. Mirrors Java's `Serdes.Void()`.
#[allow(non_snake_case)]
pub fn Void() -> VoidSerde {
    VoidSerde::new(VoidSerializer, VoidDeserializer)
}

/// Construct a serde from a separate serializer and deserializer. Mirrors
/// Java's `Serdes.serdeFrom(Serializer, Deserializer)`.
///
/// Java throws `IllegalArgumentException` when either argument is null;
/// Rust types are non-null by construction so the precondition is enforced
/// at compile time.
pub fn serde_from<T, S, D>(serializer: S, deserializer: D) -> WrapperSerde<T, S, D>
where
    T: Send + Sync + 'static,
    S: Serializer<T> + 'static,
    D: Deserializer<T> + 'static,
{
    WrapperSerde::new(serializer, deserializer)
}
