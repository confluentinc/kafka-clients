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

//! Translation of `org.apache.kafka.common.serialization`.
//!
//! Phase 3b — serializer / deserializer trait surface and per-type impls
//! for the producer/consumer payload-conversion path.

pub mod boolean_deserializer;
pub mod boolean_serializer;
pub mod byte_array_deserializer;
pub mod byte_array_serializer;
pub mod byte_buffer_deserializer;
pub mod byte_buffer_serializer;
pub mod bytes_deserializer;
pub mod bytes_serializer;
pub mod deserializer;
pub mod double_deserializer;
pub mod double_serializer;
pub mod float_deserializer;
pub mod float_serializer;
pub mod integer_deserializer;
pub mod integer_serializer;
pub mod list_deserializer;
pub mod list_serializer;
pub mod long_deserializer;
pub mod long_serializer;
pub mod serde;
pub mod serdes;
pub mod serializer;
pub mod short_deserializer;
pub mod short_serializer;
pub mod string_deserializer;
pub mod string_serializer;
pub mod uuid_deserializer;
pub mod uuid_serializer;
pub mod void_deserializer;
pub mod void_serializer;

#[cfg(test)]
mod tests;

pub use boolean_deserializer::BooleanDeserializer;
pub use boolean_serializer::BooleanSerializer;
pub use byte_array_deserializer::ByteArrayDeserializer;
pub use byte_array_serializer::ByteArraySerializer;
pub use byte_buffer_deserializer::ByteBufferDeserializer;
pub use byte_buffer_serializer::ByteBufferSerializer;
pub use bytes_deserializer::BytesDeserializer;
pub use bytes_serializer::BytesSerializer;
pub use deserializer::Deserializer;
pub use double_deserializer::DoubleDeserializer;
pub use double_serializer::DoubleSerializer;
pub use float_deserializer::FloatDeserializer;
pub use float_serializer::FloatSerializer;
pub use integer_deserializer::IntegerDeserializer;
pub use integer_serializer::IntegerSerializer;
pub use list_deserializer::ListDeserializer;
pub use list_serializer::{InnerKind, ListSerializer, NULL_ENTRY_VALUE, SerializationStrategy};
pub use long_deserializer::LongDeserializer;
pub use long_serializer::LongSerializer;
pub use serde::Serde;
pub use serializer::Serializer;
pub use short_deserializer::ShortDeserializer;
pub use short_serializer::ShortSerializer;
pub use string_deserializer::StringDeserializer;
pub use string_serializer::{StringEncoding, StringSerializer};
pub use uuid_deserializer::UUIDDeserializer;
pub use uuid_serializer::UUIDSerializer;
pub use void_deserializer::VoidDeserializer;
pub use void_serializer::VoidSerializer;
