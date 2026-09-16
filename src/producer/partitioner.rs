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

//! Pluggable partitioner interface.
//!
//! Translated from `org.apache.kafka.clients.producer.Partitioner`
//! (`interface Partitioner extends Configurable, Closeable`).

use std::collections::HashMap;

use crate::common::Cluster;

/// Partitioner interface: compute the partition for a record.
///
/// A `Partitioner` maps each produced record to a partition of its topic. The
/// producer holds a single shared instance and consults it on the send path
/// (see [`KafkaProducer`](crate::producer::KafkaProducer)); the built-in
/// key-hash / sticky logic is used only when no partitioner is configured.
///
/// # Translation notes (Definition-of-Done §7 — justified deviations)
///
/// Java's interface `extends Configurable, Closeable` and takes `Object key` /
/// `Object value`. Rust has neither `Configurable` nor `Closeable`, and the
/// producer is generic rather than erased, so the shape differs deliberately:
///
/// - **`K, V` generics** translate Java's `Object key` / `Object value`. In the
///   generic Rust producer the key and value have concrete types, so the
///   partitioner receives typed borrows instead of `Object` — no downcasts.
/// - **`Send + Sync` supertraits**: the producer is shared across tasks, so the
///   boxed partitioner it owns must be shareable. Because these are supertrait
///   bounds, `Box<dyn Partitioner<K, V>>` is itself `Send + Sync`.
/// - **`partition(&self)`**, not `&mut self`: Java's `KafkaProducer` is
///   thread-safe and calls the *one* shared partitioner instance concurrently,
///   so Java implementations must already be internally synchronized
///   (`RoundRobinPartitioner` uses a `ConcurrentMap` + `AtomicInteger`). `&self`
///   is the faithful translation and keeps the send path lock-free (DoD §10):
///   any per-record state is the implementer's job via interior mutability
///   (atomics / a concurrent map), exactly as in Java.
/// - **`close(&self)`**, not `&mut self`: [`Producer::close`](crate::producer::Producer::close)
///   takes `&self`, so the partitioner must be closable through a shared
///   reference. Wrapping it in a `Mutex` to obtain `&mut` would add a lock to
///   every send for no behavioral gain.
/// - **`configure(&mut self)`** with a default no-op mirrors the consumer
///   interceptor precedent (`ConsumerInterceptor::configure`) and is called
///   exactly once, before the instance is boxed and shared with the producer,
///   so `&mut self` is available then.
/// - Java's `Plugin<Partitioner>` / `Monitorable` metrics wrapper (the javadoc's
///   "implement `Monitorable` to register metrics") has **no Rust counterpart**
///   and is not translated.
pub trait Partitioner<K, V>: Send + Sync {
    /// Configure this partitioner.
    ///
    /// Corresponds to Java's `Configurable.configure(Map<String, ?> configs)`.
    /// Called once, before the instance is shared with the producer, with the
    /// producer's original configuration plus the (possibly generated)
    /// `client.id`. The default implementation is a no-op.
    fn configure(&mut self, _configs: &HashMap<String, String>) {}

    /// Compute the partition for the given record.
    ///
    /// Corresponds to Java's `int partition(String topic, Object key,
    /// byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)`.
    ///
    /// - `topic` — the topic name.
    /// - `key` — the typed key to partition on, or `None` if there is no key.
    /// - `key_bytes` — the serialized key to partition on, or `None` if there is
    ///   no key.
    /// - `value` — the typed value to partition on, or `None`.
    /// - `value_bytes` — the serialized value to partition on, or `None`.
    /// - `cluster` — the current cluster metadata.
    ///
    /// The returned partition must be non-negative; the producer rejects a
    /// negative result with an `IllegalArgumentError`.
    ///
    /// Note: on the borrowed zero-copy send path
    /// (`KafkaProducer<Vec<u8>, Vec<u8>>::send(ProducerRecord<&[u8], &[u8]>)`)
    /// the typed `key` / `value` are `None` even when `key_bytes` / `value_bytes`
    /// are present, because on that path Java's `record.key()` *is* the same
    /// `byte[]` as `keyBytes`; materializing a typed `&Vec<u8>` from the borrowed
    /// bytes would allocate, violating the zero-copy contract. Partitioners that
    /// need the key/value should read the `*_bytes` parameters, which are always
    /// supplied when a key/value exists.
    fn partition(
        &self,
        topic: &str,
        key: Option<&K>,
        key_bytes: Option<&[u8]>,
        value: Option<&V>,
        value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32;

    /// Close this partitioner.
    ///
    /// Corresponds to Java's `Closeable.close()` — "This is called when
    /// partitioner is closed." The default implementation is a no-op.
    fn close(&self) {}
}
