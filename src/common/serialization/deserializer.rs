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

//! Deserialization interface.
//!
//! Translated from `org.apache.kafka.common.serialization.Deserializer<T>`.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::header::internals::RecordHeaders;

/// An interface for converting bytes to objects.
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.Deserializer<T>`.
///
/// # Sync, not async
///
/// `deserialize` is intentionally a synchronous `fn` (not `async`) so that the
/// receive path can call it inline against borrowed slices of the
/// `CompletedFetch` buffer without allocating a per-record future. See
/// `consumer-threading.md` §27 — every record allocates at most the
/// user-deserialized `T`; the trait itself adds zero heap allocation per
/// call.
///
/// **External lookups inside `deserialize`:** deserializers that depend on
/// external state (most notably a schema registry) should pre-populate an
/// in-memory cache before the consumer starts polling. For rare blocking
/// calls inside `deserialize`, callers on the multi-thread runtime can wrap
/// with `tokio::task::block_in_place`; this is not free and should not be
/// the per-record default.
///
/// # Bounds
///
/// `Send + Sync + 'static` is required because deserializer instances are
/// stored as `Box<dyn Deserializer<T>>` inside `Deserializers<K, V>`, which
/// is shared across tasks via `Arc<Deserializers<K, V>>` (the consumer's app
/// side and its background task hold the same `Arc`). `Arc<T>: Send`
/// requires `T: Send + Sync`; that requirement transits the `Box<dyn>`
/// boundary to the trait.
pub trait Deserializer<T>: Send + Sync + 'static {
    /// Deserialize a record value from a byte slice.
    ///
    /// Corresponds to Java's `T deserialize(String topic, byte[] data)`.
    ///
    /// # Arguments
    ///
    /// * `topic` - topic associated with the data
    /// * `data` - serialized bytes; the receive path slices these from the
    ///   underlying `CompletedFetch` buffer (`consumer-threading.md` §27).
    ///
    /// # Returns
    ///
    /// The deserialized typed object, or a [`KafkaError`] if deserialization
    /// fails.
    ///
    /// Java returns `T` directly and accepts a null `byte[]` returning a
    /// null `T`. In Rust the receive path never passes a null slice — it
    /// passes an empty slice or skips the call entirely — so the trait
    /// surface only models the happy path. Deserialization errors are
    /// reported via the `Result`.
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<T, KafkaError>;

    /// Deserialize a record value with access to its headers.
    ///
    /// Corresponds to Java's
    /// `default T deserialize(String topic, Headers headers, byte[] data)`.
    /// The default implementation ignores the headers and delegates to
    /// [`deserialize`](Deserializer::deserialize).
    ///
    /// Override this method in custom deserializer implementations that need
    /// to inspect headers during deserialization (for example, schema
    /// registry integration).
    fn deserialize_with_headers(
        &self,
        topic: &str,
        _headers: &RecordHeaders,
        data: &[u8],
    ) -> Result<T, KafkaError> {
        self.deserialize(topic, data)
    }

    /// Configure this deserializer. The default implementation is a no-op.
    ///
    /// Corresponds to Java's
    /// `default void configure(Map<String, ?> configs, boolean isKey)`.
    fn configure(&mut self, _configs: &HashMap<String, String>, _is_key: bool) {
        // intentionally left blank — Java default
    }

    /// Close this deserializer. The default implementation is a no-op.
    ///
    /// Corresponds to Java's `default void close()`.
    fn close(&mut self) {
        // intentionally left blank — Java default
    }
}
