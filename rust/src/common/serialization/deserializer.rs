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

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::RecordHeaders;

/// An interface for converting bytes to objects.
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.Deserializer<T>`.
///
/// # Asynchronous methods
///
/// Every method returns a `Send` future, so a deserializer can await I/O (a
/// schema-registry lookup, for instance) without blocking the runtime.
/// Implementations may write them as `async fn`. The trait is not
/// dyn-compatible; [`DynDeserializer`], which every `Deserializer` implements,
/// is its dyn-compatible companion, and the consumer stores its deserializers
/// as `Box<dyn DynDeserializer<K>>`. A call through `dyn` returns an
/// [`InlineFuture`], which holds the future without allocating as long as it
/// is small: a deserializer that awaits a large operation only on a cache miss
/// should `Box::pin` that operation alone. The receive path then adds no heap
/// allocation per record beyond the deserialized `T` (`consumer-threading.md`
/// §27).
///
/// A consumer's `poll()` may be cancelled while a record is being
/// deserialized, for instance by `tokio::time::timeout`; the record is then
/// deserialized again by the next `poll()`, so a deserializer should not rely
/// on being called exactly once per record.
///
/// # Bounds
///
/// `Send + Sync + 'static` is required because the consumer's app side and its
/// background task share the deserializers across tasks.
///
/// [`DynDeserializer`]: crate::common::serialization::DynDeserializer
/// [`InlineFuture`]: crate::common::serialization::InlineFuture
#[doc(alias = "org.apache.kafka.common.serialization.Deserializer")]
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
    /// The deserialized typed object.
    ///
    /// Java returns `T` directly and accepts a null `byte[]` returning a
    /// null `T`. In Rust the receive path never passes a null slice — it
    /// skips the call for a null key or value — so the trait surface only
    /// models the happy path.
    ///
    /// # Errors
    ///
    /// Whatever error the deserializer reports; the consumer returns it from
    /// `poll`.
    #[doc(alias = "org.apache.kafka.common.serialization.Deserializer#deserialize")]
    fn deserialize<'a>(&'a self, topic: &'a str, data: &'a [u8]) -> impl Future<Output = Result<T, Error>> + Send + 'a;

    /// Deserialize a record value with access to its headers.
    ///
    /// Corresponds to Java's
    /// `default T deserialize(String topic, Headers headers, byte[] data)`
    /// (`Deserializer.java:84`). The default implementation ignores the headers
    /// and delegates to [`deserialize`](Deserializer::deserialize).
    ///
    /// The translated Java overloads intersect on `{topic, data}`, which is
    /// exactly `deserialize(String, byte[])` (`:64`) — so that one keeps the
    /// plain name and this one is suffixed with the parameter that distinguishes
    /// it (CLAUDE.md §2).
    ///
    /// Override this method in custom deserializer implementations that need
    /// to inspect headers during deserialization (for example, schema
    /// registry integration).
    ///
    /// Takes the concrete [`RecordHeaders`] rather than a `&dyn Headers`
    /// trait object, as [`Serializer::serialize_with_headers`] does: Java's
    /// parameter type is the `Headers` interface, but the client only ever
    /// passes its one implementation, `RecordHeaders`, which is also what
    /// `ConsumerRecord` and `ProducerRecord` hold. The concrete type keeps the
    /// two serde traits symmetric and lets the C binding hand the headers over
    /// as a `RecordHeaders_t` handle instead of an interface view.
    ///
    /// # Errors
    ///
    /// As [`deserialize`](Deserializer::deserialize).
    ///
    /// [`Serializer::serialize_with_headers`]: crate::common::serialization::Serializer::serialize_with_headers
    #[doc(alias = "org.apache.kafka.common.serialization.Deserializer#deserialize")]
    fn deserialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: &'a [u8],
    ) -> impl Future<Output = Result<T, Error>> + Send + 'a {
        let _ = headers;
        self.deserialize(topic, data)
    }

    /// Deserialize a record value from a shared [`Bytes`] buffer, with access
    /// to its headers.
    ///
    /// Corresponds to Java's
    /// `default T deserialize(String topic, Headers headers, ByteBuffer data)`
    /// (`Deserializer.java:115`, KIP-863), the overload Java's `CompletedFetch`
    /// calls (`CompletedFetch.java:323,329`); the Rust `CompletedFetch` calls
    /// this method at the same point. `data` is the record's key or value: a
    /// refcounted slice of the buffer that owns the fetched bytes (the fetched
    /// records buffer for an uncompressed batch, the buffer a compressed batch
    /// was decompressed into), so a deserializer may keep it — or a
    /// [`Bytes::slice`] of it — without copying (`consumer-threading.md` §27).
    ///
    /// The default implementation delegates to
    /// [`deserialize_with_headers`](Deserializer::deserialize_with_headers) with
    /// `&data[..]`, which, unlike Java's default (`Utils.toNullableArray`),
    /// copies nothing. [`BytesDeserializer`] overrides it to return `data`
    /// itself.
    ///
    /// Java's `byte[]` and `ByteBuffer` overloads share the parameter name
    /// `data`, so the Rust type name `bytes` tells this one apart (CLAUDE.md
    /// §2).
    ///
    /// # Errors
    ///
    /// As [`deserialize`](Deserializer::deserialize).
    ///
    /// [`BytesDeserializer`]: crate::common::serialization::BytesDeserializer
    #[doc(alias = "org.apache.kafka.common.serialization.Deserializer#deserialize")]
    fn deserialize_with_headers_bytes<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: &'a Bytes,
    ) -> impl Future<Output = Result<T, Error>> + Send + 'a {
        self.deserialize_with_headers(topic, headers, &data[..])
    }

    /// Configure this deserializer. The default implementation is a no-op.
    ///
    /// Corresponds to Java's
    /// `default void configure(Map<String, ?> configs, boolean isKey)`.
    #[doc(alias = "org.apache.kafka.common.serialization.Deserializer#configure")]
    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> impl Future<Output = ()> + Send + 'a {
        // intentionally left blank — Java default
        let _ = (configs, is_key);
        async {}
    }

    /// Close this deserializer. The default implementation is a no-op.
    ///
    /// Corresponds to Java's `default void close()`. As in Java, the method
    /// must be idempotent, as it may be called multiple times.
    #[doc(alias = "org.apache.kafka.common.serialization.Deserializer#close")]
    fn close(&mut self) -> impl Future<Output = ()> + Send {
        // intentionally left blank — Java default
        async {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::Headers;

    /// Reports where the slice it was handed starts, and whether it went
    /// through `deserialize_with_headers`.
    struct Address;

    impl Deserializer<(usize, bool)> for Address {
        async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<(usize, bool), Error> {
            Ok((data.as_ptr() as usize, false))
        }

        async fn deserialize_with_headers(
            &self,
            _topic: &str,
            headers: &RecordHeaders,
            data: &[u8],
        ) -> Result<(usize, bool), Error> {
            assert!(headers.last_header("k").is_some(), "the headers are forwarded");
            Ok((data.as_ptr() as usize, true))
        }
    }

    #[tokio::test]
    async fn test_default_bytes_overload_forwards_the_slice_without_copying() {
        let source = Bytes::from_static(b"hello world");
        let data = source.slice(6..);
        let mut headers = RecordHeaders::new();
        headers.add_with_key_value("k", Some(b"v")).expect("add");
        let (address, through_headers) = Deserializer::deserialize_with_headers_bytes(&Address, "t", &headers, &data)
            .await
            .expect("deserialize");
        assert!(through_headers, "the default forwards to deserialize_with_headers");
        assert_eq!(
            data.as_ptr() as usize,
            address,
            "the default hands over the buffer itself, not a copy"
        );
    }
}
