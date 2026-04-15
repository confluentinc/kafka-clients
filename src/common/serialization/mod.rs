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

//! Serialization interfaces and implementations (org.apache.kafka.common.serialization).
//!
//! Provides the [`Serializer`] trait and common implementations for strings
//! and byte arrays.

pub mod byte_array_serializer;
pub mod string_serializer;

pub use byte_array_serializer::ByteArraySerializer;
pub use string_serializer::StringSerializer;

use crate::common::KafkaError;

/// An interface for converting objects to bytes.
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.Serializer<T>`.
///
/// # Type Parameters
///
/// * `T` - Type to be serialized from. Use `?Sized` bounds to accept both
///   owned and borrowed forms (e.g., `Serializer<str>` accepts `&str`).
pub trait Serializer<T: ?Sized> {
    /// Convert `data` into a byte array.
    ///
    /// It is recommended to serialize `None` data to `None` byte array.
    ///
    /// # Arguments
    ///
    /// * `topic` - topic associated with data
    /// * `data` - typed data; may be `None`
    ///
    /// # Returns
    ///
    /// Serialized bytes; may be `None`.
    fn serialize(&self, topic: &str, data: Option<&T>) -> Result<Option<Vec<u8>>, KafkaError>;
}
