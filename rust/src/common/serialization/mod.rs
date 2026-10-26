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

mod byte_array_deserializer;
mod byte_array_serializer;
mod bytes_deserializer;
mod deserializer;
mod dyn_deserializer;
mod dyn_serializer;
mod inline_future;
#[cfg(test)]
mod serialization_test;
mod string_deserializer;
mod string_serializer;

pub use byte_array_deserializer::ByteArrayDeserializer;
pub use byte_array_serializer::ByteArraySerializer;
pub use bytes_deserializer::BytesDeserializer;
pub use deserializer::Deserializer;
// The dyn-compatible companions and the future they return are the crate's own
// dispatch mechanism: the clients take serdes as `impl Serializer<K>` /
// `impl Deserializer<K>` and box them once, and never hand a serde out, so no
// caller needs to name them (`check-dyn-compatible`).
pub(crate) use dyn_deserializer::DynDeserializer;
pub(crate) use dyn_serializer::DynSerializer;
pub(crate) use inline_future::InlineFuture;
pub use string_deserializer::StringDeserializer;
pub use string_serializer::StringSerializer;

use std::borrow::Cow;
use std::collections::HashMap;

use crate::common::Error;
use crate::common::header::RecordHeaders;

/// An interface for converting objects to bytes.
///
/// This interface can be combined with `ClusterResourceListener` to receive
/// cluster metadata once it's available, as well as `Monitorable` to enable the
/// serializer to register metrics. For the latter, the following tags are
/// automatically added to all metrics registered: `config` set to either
/// `key.serializer` or `value.serializer`, and `class` set to the serializer
/// class name.
///
/// # Type Parameters
///
/// - `T` - Type to be serialized from. It may be unsized (`Serializer<str>`).
///
/// # Translation notes
///
/// WARNING: combining with `ClusterResourceListener` or `Monitorable` has no effect: no client registers either.
///
/// Methods return a `Send` future, which the producer stores inline: a serializer
/// awaiting a large operation only on a cache miss should `Box::pin` it alone.
#[doc(alias = "org.apache.kafka.common.serialization.Serializer")]
pub trait Serializer<T: ?Sized>: Send + Sync {
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
    /// Serialized bytes, which may borrow from `data`; may be `None`.
    ///
    /// # Errors
    ///
    /// Whatever error the serializer reports; the producer returns it from
    /// `send`, or fails the record's future.
    #[doc(alias = "org.apache.kafka.common.serialization.Serializer#serialize")]
    fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a T>,
    ) -> impl Future<Output = Result<Option<Cow<'a, [u8]>>, Error>> + Send + 'a;

    /// Convert `data` into a byte array, with access to the record headers.
    ///
    /// It is recommended to serialize `None` data to `None` byte array.
    ///
    /// Note that the passed in headers may be empty. The implementation is allowed
    /// to modify them, as a side effect of serialization. It is considered best
    /// practice to not delete or modify existing headers, but rather only add new
    /// ones.
    ///
    /// # Arguments
    ///
    /// - `topic` - topic associated with data
    /// - `headers` - headers associated with the record
    /// - `data` - typed data; may be `None`
    ///
    /// # Returns
    ///
    /// Serialized bytes, which may borrow from `data`; may be `None`.
    ///
    /// # Errors
    ///
    /// As [`serialize`](Serializer::serialize).
    ///
    /// # Translation notes
    ///
    /// The default implementation ignores the headers and delegates to `serialize`.
    /// The producer makes the headers read-only once both serializers ran.
    // The headers are borrowed for `'h`, a lifetime of their own, so the
    // serialized bytes (which live for `'a`) do not keep them borrowed: the
    // producer serializes the key, then lends the same headers to the value
    // serializer while it still holds the key's bytes.
    #[doc(alias = "org.apache.kafka.common.serialization.Serializer#serialize")]
    fn serialize_with_headers<'a, 'h>(
        &'a self,
        topic: &'a str,
        headers: &'h mut RecordHeaders,
        data: Option<&'a T>,
    ) -> impl Future<Output = Result<Option<Cow<'a, [u8]>>, Error>> + Send + use<'a, 'h, Self, T>
    where
        'a: 'h,
    {
        let _ = headers;
        self.serialize(topic, data)
    }

    /// Configure this class. The default implementation is a no-op.
    ///
    /// # Arguments
    ///
    /// - `configs` - configs in key/value pairs
    /// - `is_key` - whether the serializer is used for the key or the value
    // Declared after the `serialize` methods, as on `Deserializer`, so the C
    // constructor takes the methods in the same order for both.
    #[doc(alias = "org.apache.kafka.common.serialization.Serializer#configure")]
    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> impl Future<Output = ()> + Send + 'a {
        // intentionally left blank — Java default
        let _ = (configs, is_key);
        async {}
    }

    /// Close this serializer.
    ///
    /// This method must be idempotent as it may be called multiple times.
    ///
    /// # Translation notes
    ///
    /// A failure is reported by panicking.
    // The producer calls it once: from its `close`, after its I/O task has
    // stopped, or from a constructor that fails, as Java's catch does. It closes
    // its serializers as Java's `Utils.closeQuietly` does: it catches the panic,
    // logs it and closes the other serializer; then the producer's `close` returns
    // `Failed to close kafka producer`, with the panic's message as its source,
    // while a failed constructor only logs it. It takes `&self`, unlike
    // `Deserializer::close`: the producer shares its serializers with every
    // concurrent `send`, so an exclusive borrow would need a lock on every send.
    #[doc(alias = "org.apache.kafka.common.serialization.Serializer#close")]
    fn close(&self) -> impl Future<Output = ()> + Send + '_ {
        // intentionally left blank — Java default
        async {}
    }
}
