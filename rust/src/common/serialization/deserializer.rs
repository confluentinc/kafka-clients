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
/// This interface can be combined with `ClusterResourceListener` to receive
/// cluster metadata once it's available, as well as `Monitorable` to enable the
/// deserializer to register metrics. For the latter, the following tags are
/// automatically added to all metrics registered: `config` set to either
/// `key.deserializer` or `value.deserializer`, and `class` set to the
/// deserializer class name.
///
/// # Type Parameters
///
/// - `T` - Type to be deserialized into.
///
/// # Translation notes
///
/// WARNING: combining with `ClusterResourceListener` or `Monitorable` has no effect: no client registers either.
///
/// Methods return a `Send` future, which the consumer stores inline: a deserializer
/// awaiting a large operation only on a cache miss should `Box::pin` it alone.
///
/// A `poll()` cancelled mid-record deserializes that record again on the next
/// `poll()`.
#[doc(alias = "org.apache.kafka.common.serialization.Deserializer")]
pub trait Deserializer<T>: Send + Sync + 'static {
    /// Deserialize a record value from a byte array into a value or object.
    ///
    /// # Arguments
    ///
    /// - `topic` - topic associated with the data
    /// - `data` - serialized bytes
    ///
    /// # Returns
    ///
    /// The deserialized typed object.
    ///
    /// # Errors
    ///
    /// Whatever error the deserializer reports; the consumer returns it from
    /// `poll`.
    ///
    /// # Translation notes
    ///
    /// The consumer skips the call for a null key or value, so `data` and the
    /// result are never `None`.
    #[doc(alias = "org.apache.kafka.common.serialization.Deserializer#deserialize")]
    fn deserialize<'a>(&'a self, topic: &'a str, data: &'a [u8]) -> impl Future<Output = Result<T, Error>> + Send + 'a;

    /// Deserialize a record value from a byte array into a value or object.
    ///
    /// Note that the passed in headers may be empty. The implementation is allowed to
    /// modify the passed in headers, as a side effect of deserialization. It is
    /// considered best practice to not delete or modify existing headers, but rather
    /// only add new ones.
    ///
    /// # Arguments
    ///
    /// - `topic` - topic associated with the data
    /// - `headers` - headers associated with the record
    /// - `data` - serialized bytes
    ///
    /// # Returns
    ///
    /// The deserialized typed object.
    ///
    /// # Errors
    ///
    /// As [`deserialize`](Deserializer::deserialize).
    ///
    /// # Translation notes
    ///
    /// The default implementation ignores the headers and delegates to
    /// [`deserialize`](Deserializer::deserialize). The consumer skips the call for a
    /// null key or value.
    ///
    /// WARNING: the headers are borrowed immutably, so the implementation
    /// cannot modify them.
    // Takes the concrete `RecordHeaders` rather than a `&dyn Headers` trait
    // object, as `Serializer::serialize_with_headers` does: the client only ever
    // passes its one implementation, which keeps the two serde traits symmetric
    // and lets the C binding hand the headers over as a `RecordHeaders_t` handle.
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

    /// Deserialize a record value from a [`Bytes`] buffer into a value or object.
    ///
    /// If this method is overridden, the implementation cannot make any assumptions
    /// about the passed in buffer.
    ///
    /// Note that the passed in headers may be empty. The implementation is allowed to
    /// modify the passed in headers, as a side effect of deserialization. It is
    /// considered best practice to not delete or modify existing headers, but rather
    /// only add new ones.
    ///
    /// # Arguments
    ///
    /// - `topic` - topic associated with the data
    /// - `headers` - headers associated with the record
    /// - `data` - serialized bytes
    ///
    /// # Returns
    ///
    /// The deserialized typed object.
    ///
    /// # Errors
    ///
    /// As [`deserialize`](Deserializer::deserialize).
    ///
    /// # Translation notes
    ///
    /// The consumer calls this overload. `data` is a refcounted slice of the fetched
    /// bytes, so a deserializer may keep it without copying. The default delegates to
    /// [`deserialize_with_headers`](Deserializer::deserialize_with_headers) with
    /// `&data[..]`, copying nothing. The consumer skips the call for a null key or
    /// value.
    ///
    /// WARNING: the headers are borrowed immutably, so the implementation
    /// cannot modify them.
    #[doc(alias = "org.apache.kafka.common.serialization.Deserializer#deserialize")]
    fn deserialize_with_headers_bytes<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: &'a Bytes,
    ) -> impl Future<Output = Result<T, Error>> + Send + 'a {
        self.deserialize_with_headers(topic, headers, &data[..])
    }

    /// Configure this class. The default implementation is a no-op.
    ///
    /// # Arguments
    ///
    /// - `configs` - configs in key/value pairs
    /// - `is_key` - whether the deserializer is used for the key or the value
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

    /// Close this deserializer.
    ///
    /// This method must be idempotent as it may be called multiple times.
    ///
    /// # Translation notes
    ///
    /// A failure is reported by panicking.
    // The consumer closes its deserializers as Java's `Utils.closeQuietly` does:
    // it catches the panic, logs it and closes the other deserializer; then the
    // consumer's `close` returns `Failed to close kafka consumer`, caused by
    // `Failed to close deserializers` with the panic's message as its source,
    // while a failed constructor only logs it.
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
