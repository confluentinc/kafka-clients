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
pub use dyn_deserializer::DynDeserializer;
pub use dyn_serializer::DynSerializer;
pub use inline_future::InlineFuture;
pub use string_deserializer::StringDeserializer;
pub use string_serializer::StringSerializer;

use std::borrow::Cow;
use std::collections::HashMap;

use crate::common::Error;
use crate::common::header::RecordHeaders;

/// An interface for converting objects to bytes.
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.Serializer<T>`.
///
/// # Asynchronous methods
///
/// Every method returns a `Send` future, so a serializer can await I/O (a
/// schema-registry lookup, for instance) without blocking the runtime.
/// Implementations may write them as `async fn`. The trait is not
/// dyn-compatible; [`DynSerializer`], which every `Serializer` implements, is
/// its dyn-compatible companion, and the producer stores its serializers as
/// `Box<dyn DynSerializer<K>>`. A call through `dyn` returns an
/// [`InlineFuture`], which holds the future without allocating as long as it
/// is small: a serializer that awaits a large operation only on a cache miss
/// should `Box::pin` that operation alone.
///
/// # Borrowed output
///
/// The serialized bytes are a [`Cow`] that may borrow from `data`: a
/// serializer whose input already is the bytes (`ByteArraySerializer`,
/// `StringSerializer`) returns [`Cow::Borrowed`] and neither allocates nor
/// copies (CLAUDE.md §14); one that builds the bytes returns [`Cow::Owned`].
/// Either is Java's `byte[]` return.
///
/// # Type Parameters
///
/// * `T` - Type to be serialized from. Use `?Sized` bounds to accept both
///   owned and borrowed forms (e.g., `Serializer<str>` accepts `&str`).
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
    /// `send`, or fails the record's future, as Java's `doSend` does.
    #[doc(alias = "org.apache.kafka.common.serialization.Serializer#serialize")]
    fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a T>,
    ) -> impl Future<Output = Result<Option<Cow<'a, [u8]>>, Error>> + Send + 'a;

    /// Convert `data` into a byte array, with access to the record headers.
    ///
    /// The default implementation ignores the headers and delegates to
    /// [`serialize`](Serializer::serialize). Override this method in custom
    /// serializer implementations that need to inspect or modify headers during
    /// serialization (e.g., for schema registry integration).
    ///
    /// The headers may be empty. The implementation is allowed to modify them,
    /// as a side effect of serialization: it is considered best practice not to
    /// delete or modify existing headers, but only to add new ones. The producer
    /// sends the headers as they are after both the key and the value are
    /// serialized, and makes them read-only from then on.
    ///
    /// The headers are borrowed for `'h`, a lifetime of their own, so the
    /// serialized bytes (which live for `'a`) do not keep them borrowed: the
    /// producer serializes the key, then lends the same headers to the value
    /// serializer while it still holds the key's bytes.
    ///
    /// Corresponds to Java's `Serializer.serialize(String topic, Headers headers, T data)`
    /// (`Serializer.java:84`). The two Java overloads intersect on
    /// `{topic, data}`, which is exactly `serialize(String, T)` (`:62`) — so that
    /// one keeps the plain name and this one is suffixed with the parameter that
    /// distinguishes it (CLAUDE.md §2).
    ///
    /// # Arguments
    ///
    /// * `topic` - topic associated with data
    /// * `headers` - headers associated with the record; may be modified
    /// * `data` - typed data; may be `None`
    ///
    /// # Returns
    ///
    /// Serialized bytes, which may borrow from `data`; may be `None`.
    ///
    /// # Errors
    ///
    /// As [`serialize`](Serializer::serialize).
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
    /// Declared after the `serialize` methods, as on [`Deserializer`], so the
    /// C constructor takes the methods in the same order for both. Corresponds
    /// to Java's
    /// `default void configure(Map<String, ?> configs, boolean isKey)`.
    ///
    /// # Arguments
    ///
    /// * `configs` - configs in key/value pairs
    /// * `is_key` - whether the serializer is used for the key or the value
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

    /// Close this serializer. The default implementation is a no-op.
    ///
    /// Corresponds to Java's `default void close()`. As in Java, the method
    /// must be idempotent, as it may be called multiple times. The producer
    /// calls it once, from its `close`, after its I/O task has stopped.
    ///
    /// It takes `&self`, unlike [`Deserializer::close`]: the producer shares
    /// its serializers with every concurrent `send` and closes through a shared
    /// reference, so an exclusive borrow would need a lock on every send.
    #[doc(alias = "org.apache.kafka.common.serialization.Serializer#close")]
    fn close(&self) -> impl Future<Output = ()> + Send + '_ {
        // intentionally left blank — Java default
        async {}
    }
}
