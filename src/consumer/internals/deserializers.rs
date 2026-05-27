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
//! The container is not yet referenced by `MockConsumer` (Phase 3) or
//! `AsyncKafkaConsumer` (Phase 11); `dead_code` is allowed at the module
//! level to keep the public-API surface frozen ahead of consumer wiring.

#![allow(dead_code)]

use crate::common::serialization::Deserializer;

/// A container that holds the key + value [`Deserializer`] instances as
/// concrete trait objects.
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
/// # Allocation cost (CLAUDE.md §11 / DoD §10)
///
/// The receive path calls `Deserializer::deserialize` once per key and
/// once per value per record. With `Box<dyn Deserializer<T>>` inside
/// `Arc<Deserializers>`:
///
/// - One pointer-indirection per call (vtable lookup through `Box<dyn>`):
///   ~1 ns
/// - Zero atomic ops per record — the outer `Arc` is dereferenced once
///   per poll (or once at fetcher construction), not per record.
///
/// This is acceptable because (a) actual deserializer bodies are 100ns+
/// (parsing UTF-8, parsing protobuf, etc.) and (b) the alternative —
/// threading `KD: Deserializer<K>` and `VD: Deserializer<V>` through
/// every type that touches the fetch path — would propagate two type
/// parameters through `Fetcher`, `FetchCollector`, `CompletedFetch`,
/// `ApplicationEventProcessor`, and ultimately
/// `AsyncKafkaConsumer<K, V, KD, VD>`, defeating the
/// `Box<dyn Consumer<K, V>>` factory return.
///
/// Note: deliberately NOT `Clone`. Sharing happens at the outer
/// `Arc<Deserializers<K, V>>` boundary, not on this struct.
pub(crate) struct Deserializers<K: 'static, V: 'static> {
    key: Box<dyn Deserializer<K>>,
    value: Box<dyn Deserializer<V>>,
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
    pub(crate) fn new(key: Box<dyn Deserializer<K>>, value: Box<dyn Deserializer<V>>) -> Self {
        Self { key, value }
    }

    /// Returns a borrowed reference to the key deserializer.
    ///
    /// Translates Java's `Deserializer<K> keyDeserializer()`.
    pub(crate) fn key_deserializer(&self) -> &dyn Deserializer<K> {
        &*self.key
    }

    /// Returns a borrowed reference to the value deserializer.
    ///
    /// Translates Java's `Deserializer<V> valueDeserializer()`.
    pub(crate) fn value_deserializer(&self) -> &dyn Deserializer<V> {
        &*self.value
    }
}

impl<K, V> Drop for Deserializers<K, V> {
    /// Closes both deserializers. Translates Java's `void close()`.
    ///
    /// Java's `close()` collects the first exception from the key and
    /// value deserializer and rethrows it after both have been closed. In
    /// Rust there is no `close()` failure mode visible from
    /// `Deserializer::close` (the trait's default returns `()`); errors
    /// surface only via panics. We catch panics so that one panicking
    /// deserializer does not prevent the other from closing.
    fn drop(&mut self) {
        let key_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.key.close();
        }));
        let value_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.value.close();
        }));
        if key_result.is_err() {
            log::error!("Failed to close key deserializer");
        }
        if value_result.is_err() {
            log::error!("Failed to close value deserializer");
        }
    }
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
            .field("key_deserializer", &"<dyn Deserializer>")
            .field("value_deserializer", &"<dyn Deserializer>")
            .finish()
    }
}
