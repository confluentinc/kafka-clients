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
//! Provides the [`Serializer`] and [`Deserializer`] traits and common
//! implementations for strings and byte arrays.

pub mod byte_array_deserializer;
pub mod byte_array_serializer;
pub mod bytes_deserializer;
pub mod deserializer;
pub mod string_deserializer;
pub mod string_serializer;

pub use byte_array_deserializer::ByteArrayDeserializer;
pub use byte_array_serializer::ByteArraySerializer;
pub use bytes_deserializer::BytesDeserializer;
pub use deserializer::Deserializer;
pub use string_deserializer::StringDeserializer;
pub use string_serializer::StringSerializer;

use crate::common::Error;
use crate::common::header::internals::RecordHeaders;

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
    fn serialize(&self, topic: &str, data: Option<&T>) -> Result<Option<Vec<u8>>, Error>;

    /// Convert `data` into a byte array, with access to the record headers.
    ///
    /// The default implementation ignores the headers and delegates to
    /// [`serialize`](Serializer::serialize). Override this method in custom
    /// serializer implementations that need to inspect or modify headers during
    /// serialization (e.g., for schema registry integration).
    ///
    /// Corresponds to Java's `Serializer.serialize(String topic, Headers headers, T data)`.
    ///
    /// # Arguments
    ///
    /// * `topic` - topic associated with data
    /// * `headers` - record headers
    /// * `data` - typed data; may be `None`
    ///
    /// # Returns
    ///
    /// Serialized bytes; may be `None`.
    fn serialize_with_headers(
        &self,
        topic: &str,
        _headers: &RecordHeaders,
        data: Option<&T>,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.serialize(topic, data)
    }

    /// Serialize owned data, avoiding a clone when the input is already bytes.
    ///
    /// The default implementation borrows `data` and delegates to
    /// [`serialize_with_headers`](Serializer::serialize_with_headers).
    /// Implementations for types that are already byte buffers (e.g. `Vec<u8>`)
    /// can override this to pass ownership through without copying.
    fn serialize_owned_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: Option<T>,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        T: Sized,
    {
        self.serialize_with_headers(topic, headers, data.as_ref())
    }
}
