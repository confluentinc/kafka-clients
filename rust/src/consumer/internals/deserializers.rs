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

//! Container holding the key + value deserializer for a consumer.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.Deserializers`.
//!
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;

use crate::common::Error;
use crate::common::serialization::DynDeserializer;
use crate::consumer::internals::panic_payload_message;

/// A container that holds the key + value deserializers as
/// [`DynDeserializer`] trait objects.
///
/// Translates `org.apache.kafka.clients.consumer.internals.Deserializers`.
///
/// # Shape B: one struct, shared via `Arc<Deserializers<K, V>>`
///
/// The consumer, `Fetcher`, and `FetchCollector` all need to call the same
/// deserializers. In Rust this is modeled as **one struct shared via
/// `Arc<Deserializers<K, V>>`** — NOT as a `Clone`-able struct over inner
/// `Arc<dyn Deserializer<T>>`.
///
/// Rationale (Phase 2 PLAN.md): making `Deserializers` itself `Clone` by
/// storing `Arc<dyn Deserializer<K>>` inside forces every owner to hold
/// their own `Deserializers` instance that happens to point at the same
/// inner `Arc`s. Shape B has exactly one `Deserializers` struct per
/// consumer; sharing happens at the outer `Arc`. One concept instead of
/// two.
///
/// # Allocation cost (CLAUDE.md §13 / DoD §10)
///
/// The receive path calls a deserializer once per key and once per value per
/// record. With `Box<dyn DynDeserializer<T>>` inside `Arc<Deserializers>`:
///
/// - One pointer-indirection per call (vtable lookup through `Box<dyn>`):
///   ~1 ns
/// - Zero allocations per call: the deserializer's future is returned as an
///   [`InlineFuture`](crate::common::serialization::InlineFuture), stored in
///   the caller's own future
/// - Zero atomic ops per record — the outer `Arc` is dereferenced once
///   per poll (or once at fetcher construction), not per record.
///
/// This is acceptable because (a) actual deserializer bodies are 100ns+
/// (parsing UTF-8, parsing protobuf, etc.) and (b) the alternative —
/// threading `KD: Deserializer<K>` and `VD: Deserializer<V>` through
/// every type that touches the fetch path — would propagate two type
/// parameters through `Fetcher`, `FetchCollector`, `CompletedFetch`,
/// `ApplicationEventProcessor`, and ultimately
/// `AsyncKafkaConsumer<K, V, KD, VD>`, and from there into the public
/// `KafkaConsumer<K, V>`, whose only type parameters are the record types.
///
/// Note: deliberately NOT `Clone`. Sharing happens at the outer
/// `Arc<Deserializers<K, V>>` boundary, not on this struct.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.Deserializers")]
pub(crate) struct Deserializers<K: 'static, V: 'static> {
    key: Box<dyn DynDeserializer<K>>,
    value: Box<dyn DynDeserializer<V>>,
}

impl<K, V> Deserializers<K, V> {
    /// Construct a `Deserializers` from owned key + value deserializer
    /// trait objects.
    ///
    /// Translates Java's
    /// `Deserializers(Deserializer<K> keyDeserializer,
    ///                Deserializer<V> valueDeserializer,
    ///                Metrics metrics)`.
    ///
    /// The `Metrics` parameter is dropped per Phase 2 PLAN.md (no metrics
    /// framework in this milestone). Java's reflection-style constructor
    /// `Deserializers(ConsumerConfig, ...)` is also not translated — Phase
    /// 11 wires the deserializers explicitly via the consumer config
    /// builder.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Deserializers#Deserializers")]
    pub(crate) fn new(key: Box<dyn DynDeserializer<K>>, value: Box<dyn DynDeserializer<V>>) -> Self {
        Self { key, value }
    }

    /// Returns a borrowed reference to the key deserializer.
    ///
    /// Translates Java's `Deserializer<K> keyDeserializer()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Deserializers#keyDeserializer")]
    pub(crate) fn key_deserializer(&self) -> &dyn DynDeserializer<K> {
        &*self.key
    }

    /// Returns a borrowed reference to the value deserializer.
    ///
    /// Translates Java's `Deserializer<V> valueDeserializer()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Deserializers#valueDeserializer")]
    pub(crate) fn value_deserializer(&self) -> &dyn DynDeserializer<V> {
        &*self.value
    }
}

impl<K, V> Deserializers<K, V> {
    /// Closes both deserializers, the key one first.
    ///
    /// Translates Java's `void close()`, which closes each deserializer through
    /// `Utils.closeQuietly`, keeps the first failure, and after both have been
    /// closed throws it as `KafkaException("Failed to close deserializers",
    /// cause)`. [`Deserializer::close`](crate::common::serialization::Deserializer::close)
    /// returns nothing, so the only failure a Rust deserializer can report is a
    /// panic: it is caught, logged as `closeQuietly` logs, and turned into the
    /// cause, so one panicking deserializer does not keep the other from
    /// closing. Java's `InterruptException` branch has no Rust counterpart (no
    /// thread interruption).
    ///
    /// Async because a deserializer's `close` is (it may release a resource
    /// over the network). Nothing closes the deserializers on drop: the consumer
    /// awaits this on its close path, as Java's `close` calls it.
    ///
    /// # Errors
    ///
    /// `KafkaError("Failed to close deserializers")`, its source the first
    /// deserializer's failure, when either `close` panicked.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Deserializers#close")]
    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        let mut first_error = None;
        if let Err(payload) = AssertUnwindSafe(DynDeserializer::close(&mut *self.key)).catch_unwind().await {
            first_error.get_or_insert(close_failure("key deserializer", &*payload));
        }
        if let Err(payload) = AssertUnwindSafe(DynDeserializer::close(&mut *self.value)).catch_unwind().await {
            first_error.get_or_insert(close_failure("value deserializer", &*payload));
        }
        match first_error {
            Some(error) => Err(Error::kafka_message_source("Failed to close deserializers", error)),
            None => Ok(()),
        }
    }
}

/// The error `Utils.closeQuietly` records for a deserializer whose `close`
/// panicked, logged as `closeQuietly` logs it (`Utils#closeQuietly(AutoCloseable,String,AtomicReference)`).
fn close_failure(name: &str, payload: &(dyn std::any::Any + Send)) -> Error {
    let message = panic_payload_message(payload);
    log::error!("Failed to close {name}: {message}");
    Error::kafka_message(message.to_string())
}

impl<K, V> std::fmt::Debug for Deserializers<K, V> {
    /// Matches Java's `toString()`:
    /// `Deserializers{keyDeserializer=..., valueDeserializer=...}`.
    ///
    /// The Rust version omits the inner deserializer details — trait
    /// objects do not implement `Debug` by default, and adding a `Debug`
    /// bound on the `Deserializer` trait would force every user impl to
    /// provide one.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deserializers")
            .field("key_deserializer", &"<dyn DynDeserializer>")
            .field("value_deserializer", &"<dyn DynDeserializer>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::serialization::Deserializer;
    use crate::common::serialization::StringDeserializer;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A deserializer whose `close` counts the call, then panics with the given
    /// message.
    struct PanickingOnClose(&'static str, Arc<AtomicUsize>);

    impl PanickingOnClose {
        fn new(message: &'static str) -> Self {
            Self(message, Arc::new(AtomicUsize::new(0)))
        }
    }

    impl Deserializer<String> for PanickingOnClose {
        async fn deserialize(&self, _topic: &str, _data: &[u8]) -> Result<String, Error> {
            Ok(String::new())
        }

        async fn close(&mut self) {
            self.1.fetch_add(1, Ordering::SeqCst);
            panic!("{}", self.0);
        }
    }

    #[tokio::test]
    async fn test_close_without_failure() {
        let mut deserializers: Deserializers<String, String> =
            Deserializers::new(Box::new(StringDeserializer::new()), Box::new(StringDeserializer::new()));
        deserializers.close().await.expect("close");
    }

    #[tokio::test]
    async fn test_close_reports_the_first_failure_after_closing_both() {
        let key = PanickingOnClose::new("key close failed");
        let value = PanickingOnClose::new("value close failed");
        let (key_closes, value_closes) = (Arc::clone(&key.1), Arc::clone(&value.1));
        let mut deserializers: Deserializers<String, String> = Deserializers::new(Box::new(key), Box::new(value));
        let error = deserializers.close().await.expect_err("both closes panicked");
        // Java's `Utils.closeQuietly` keeps going after the key failure: both
        // deserializers are closed exactly once.
        assert_eq!(1, key_closes.load(Ordering::SeqCst));
        assert_eq!(1, value_closes.load(Ordering::SeqCst));
        assert!(error.is_kafka_error());
        assert_eq!("Failed to close deserializers", error.message());
        let source = std::error::Error::source(&error).expect("the first failure is the source");
        assert_eq!("key close failed", source.to_string());
    }

    #[tokio::test]
    async fn test_close_reports_a_value_failure() {
        let mut deserializers: Deserializers<String, String> = Deserializers::new(
            Box::new(StringDeserializer::new()),
            Box::new(PanickingOnClose::new("value close failed")),
        );
        let error = deserializers.close().await.expect_err("the value close panicked");
        assert_eq!("Failed to close deserializers", error.message());
        let source = std::error::Error::source(&error).expect("the failure is the source");
        assert_eq!("value close failed", source.to_string());
    }
}
