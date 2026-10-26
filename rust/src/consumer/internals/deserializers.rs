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
    /// cause)`. A deserializer's failure is the `Err` its
    /// [`close`](crate::common::serialization::Deserializer::close) returns, or a
    /// panic, which is caught so one panicking deserializer does not keep the
    /// other from closing; either is logged as `closeQuietly` logs it and the
    /// first becomes the cause. Java's `InterruptException` branch has no Rust
    /// counterpart (no thread interruption).
    ///
    /// Async because a deserializer's `close` is (it may release a resource
    /// over the network). Nothing closes the deserializers on drop: the consumer
    /// awaits this on its close path, as Java's `close` calls it.
    ///
    /// # Errors
    ///
    /// `KafkaError("Failed to close deserializers")`, its source the first
    /// deserializer's failure, when either `close` failed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Deserializers#close")]
    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        let mut first_error = None;
        let key = AssertUnwindSafe(DynDeserializer::close(&mut *self.key)).catch_unwind().await;
        record_close_failure(&mut first_error, "key deserializer", key);
        let value = AssertUnwindSafe(DynDeserializer::close(&mut *self.value)).catch_unwind().await;
        record_close_failure(&mut first_error, "value deserializer", value);
        match first_error {
            Some(error) => Err(Error::kafka_message_source("Failed to close deserializers", error)),
            None => Ok(()),
        }
    }
}

/// Records a deserializer's `close` failure, an `Err` or a caught panic, as
/// `Utils.closeQuietly` records a throw: logged, and kept in `first_error` when
/// it is the first (`Utils#closeQuietly(AutoCloseable,String,AtomicReference)`).
fn record_close_failure(
    first_error: &mut Option<Error>,
    name: &str,
    outcome: Result<Result<(), Error>, Box<dyn std::any::Any + Send>>,
) {
    let error = match outcome {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(payload) => Error::kafka_message(panic_payload_message(&*payload).to_string()),
    };
    log::error!("Failed to close {name}: {error}");
    first_error.get_or_insert(error);
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

        async fn close(&mut self) -> Result<(), Error> {
            self.1.fetch_add(1, Ordering::SeqCst);
            panic!("{}", self.0);
        }
    }

    /// A deserializer whose `close` counts the call, then returns an error
    /// with the given message.
    struct FailingOnClose(&'static str, Arc<AtomicUsize>);

    impl Deserializer<String> for FailingOnClose {
        async fn deserialize(&self, _topic: &str, _data: &[u8]) -> Result<String, Error> {
            Ok(String::new())
        }

        async fn close(&mut self) -> Result<(), Error> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Err(Error::serialization(self.0))
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

    #[tokio::test]
    async fn test_close_reports_the_first_returned_error_after_closing_both() {
        let key = FailingOnClose("key close failed", Arc::new(AtomicUsize::new(0)));
        let value = PanickingOnClose::new("value close failed");
        let (key_closes, value_closes) = (Arc::clone(&key.1), Arc::clone(&value.1));
        let mut deserializers: Deserializers<String, String> = Deserializers::new(Box::new(key), Box::new(value));
        let error = deserializers.close().await.expect_err("both closes failed");
        assert_eq!(1, key_closes.load(Ordering::SeqCst));
        assert_eq!(1, value_closes.load(Ordering::SeqCst));
        assert!(error.is_kafka_error());
        assert_eq!("Failed to close deserializers", error.message());
        let source = Error::source(&error).expect("the first failure is the source");
        assert!(source.is_serialization_error(), "the key's own error: {source:?}");
        assert_eq!("key close failed", source.message());
    }

    #[tokio::test]
    async fn test_close_reports_a_returned_value_error_as_it_is() {
        let mut deserializers: Deserializers<String, String> = Deserializers::new(
            Box::new(StringDeserializer::new()),
            Box::new(FailingOnClose("value close failed", Arc::new(AtomicUsize::new(0)))),
        );
        let error = deserializers.close().await.expect_err("the value close failed");
        assert_eq!("Failed to close deserializers", error.message());
        let source = Error::source(&error).expect("the failure is the source");
        assert!(source.is_serialization_error(), "the deserializer's own error: {source:?}");
        assert_eq!("value close failed", source.message());
    }
}
