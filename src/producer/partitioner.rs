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

//! Translation of `org.apache.kafka.clients.producer.Partitioner`.

use std::any::Any;
use std::collections::HashMap;

use crate::common::cluster::Cluster;

/// Partitioner Interface.
///
/// Implementations select the partition for a given record on the producer
/// hot path. The trait corresponds 1:1 to Java's
/// `org.apache.kafka.clients.producer.Partitioner` (which extends
/// `Configurable` and `Closeable`).
///
/// # Translation notes
///
/// * Java's `Object key` / `Object value` parameters become
///   `Option<&dyn Any>` so an implementation may downcast if needed.
///   The hot path is the byte-slice arguments; the typed values are
///   primarily metadata for custom partitioners.
/// * Java's `byte[] keyBytes` / `byte[] valueBytes` become
///   `Option<&[u8]>`, which is the most general borrowed form for
///   serialized payload bytes (CLAUDE.md rule 12) — implementations
///   never own these slices.
/// * Java's `Configurable.configure(Map<String, ?> configs)` becomes a
///   `configure` default method taking a borrowed
///   `&HashMap<String, String>`. Custom partitioners that need
///   non-string config can downcast through the `Any` payload — for
///   the Phase 6 milestone we do not have any custom partitioners that
///   need richer config, and `RoundRobinPartitioner::configure` is a
///   no-op.
/// * Java's `close()` becomes a `close(&mut self)` default no-op method.
///   `Closeable` in Java may throw `IOException`; partitioners in
///   practice never do. We model `close` as infallible.
pub trait Partitioner: Send + Sync {
    /// Compute the partition for the given record.
    ///
    /// * `topic` — The topic name.
    /// * `key` — The key to partition on (or `None` if no key).
    /// * `key_bytes` — The serialized key to partition on (or `None`
    ///   if no key).
    /// * `value` — The value to partition on or `None`.
    /// * `value_bytes` — The serialized value to partition on or `None`.
    /// * `cluster` — The current cluster metadata.
    ///
    /// # Panics
    ///
    /// May panic if `cluster` reports zero partitions for `topic`. This
    /// mirrors Java's behavior: the Java implementations all reduce to
    /// `random % numPartitions`, which throws `ArithmeticException` on
    /// division by zero. Per CLAUDE.md rule 10, panicking on
    /// `ArithmeticException`-like conditions is acceptable. In practice
    /// the producer's metadata pipeline guards against this earlier; a
    /// panic here indicates a programmer error or stale metadata.
    fn partition(
        &self,
        topic: &str,
        key: Option<&dyn Any>,
        key_bytes: Option<&[u8]>,
        value: Option<&dyn Any>,
        value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32;

    /// Configure this partitioner. Mirrors Java's
    /// `Configurable.configure(Map<String, ?>)`. Default implementation
    /// is a no-op.
    fn configure(&mut self, _configs: &HashMap<String, String>) {}

    /// Called when the partitioner is closed. Default implementation is
    /// a no-op.
    fn close(&mut self) {}
}
