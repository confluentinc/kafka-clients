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

/// Partitioner Interface
///
/// # Translation notes
///
/// WARNING: `Monitorable` is not translated, so a partitioner cannot register
/// metrics.
///
/// `partition` and `close` take `&self`: the producer shares one instance
/// across tasks, as Java's shares it across threads.
//
// Java's interface `extends Configurable, Closeable` and takes `Object key` /
// `Object value`. Rust has neither `Configurable` nor `Closeable`, and the
// producer is generic rather than erased, so the shape differs deliberately
// (justified deviations, Definition of Done §7):
//
// - `K, V` generics translate Java's `Object key` / `Object value`: the
//   partitioner receives typed borrows instead of `Object`, no downcasts.
// - `Send + Sync` supertraits: the producer is shared across tasks, so the
//   boxed partitioner it owns must be shareable.
// - `partition(&self)`, not `&mut self`: Java's `KafkaProducer` calls the one
//   shared partitioner concurrently, so Java implementations are already
//   internally synchronized (`RoundRobinPartitioner` uses a `ConcurrentMap` +
//   `AtomicInteger`). `&self` keeps the send path lock-free; per-record state
//   is the implementer's job via interior mutability, as in Java.
// - `close(&self)`, not `&mut self`: `Producer::close` takes `&self`, and a
//   `Mutex` to obtain `&mut` would add a lock to every send.
// - `configure(&mut self)` with a default no-op mirrors
//   `ConsumerInterceptor::configure` and is called exactly once, before the
//   instance is boxed and shared with the producer.
// - Java's `Plugin<Partitioner>` / `Monitorable` metrics wrapper has no Rust
//   counterpart.
#[doc(alias = "org.apache.kafka.clients.producer.Partitioner")]
pub trait Partitioner<K, V>: Send + Sync {
    /// Configure this class with the given key-value pairs
    ///
    /// # Arguments
    ///
    /// - `_configs` - the pairs
    // Java's `Configurable.configure(Map<String, ?> configs)`. Called once,
    // before the instance is shared with the producer, with the producer's
    // original configuration plus the (possibly generated) `client.id`. The
    // default implementation is a no-op.
    #[doc(alias = "org.apache.kafka.common.Configurable#configure")]
    fn configure(&mut self, _configs: &HashMap<String, String>) {}

    /// Compute the partition for the given record.
    ///
    /// # Arguments
    ///
    /// - `topic` - The topic name
    /// - `key` - The key to partition on (or None if no key)
    /// - `key_bytes` - The serialized key to partition on( or None if no key)
    /// - `value` - The value to partition on or None
    /// - `value_bytes` - The serialized value to partition on or None
    /// - `cluster` - The current cluster metadata
    ///
    /// # Returns
    ///
    /// The partition, non-negative: the send fails on a negative one.
    ///
    /// # Translation notes
    ///
    /// WARNING: `key` and `value` are `None` on the borrowed send path of
    /// `KafkaProducer<Vec<u8>, Vec<u8>>`.
    // On the borrowed zero-copy send path
    // (`KafkaProducer<Vec<u8>, Vec<u8>>::send(ProducerRecord<&[u8], &[u8]>)`)
    // Java's `record.key()` is the same `byte[]` as `keyBytes`; materializing a
    // typed `&Vec<u8>` from the borrowed bytes would allocate, violating the
    // zero-copy contract. Partitioners that need the key/value read the
    // `*_bytes` parameters, which are always supplied when a key/value exists.
    #[doc(alias = "org.apache.kafka.clients.producer.Partitioner#partition")]
    fn partition(
        &self,
        topic: &str,
        key: Option<&K>,
        key_bytes: Option<&[u8]>,
        value: Option<&V>,
        value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32;

    /// This is called when partitioner is closed.
    // Java's `Closeable.close()`. The default implementation is a no-op.
    #[doc(alias = "org.apache.kafka.clients.producer.Partitioner#close")]
    fn close(&self) {}
}
