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

//! Translation of `org.apache.kafka.clients.producer.ProducerInterceptor`.

use std::collections::HashMap;

use crate::common::errors::KafkaError;
use crate::common::header::RecordHeaders;
use crate::producer::ProducerRecord;
use crate::producer::RecordMetadata;

/// A plugin interface that allows you to intercept (and possibly mutate)
/// the records received by the producer before they are published to the
/// Kafka cluster.
///
/// This trait will get producer config properties via the `configure()`
/// method, including the client id assigned by `KafkaProducer` if not
/// specified in the producer config. The interceptor implementation needs
/// to be aware that it will be sharing producer config namespace with
/// other interceptors and serializers, and ensure that there are no
/// conflicts.
///
/// Exceptions thrown by `ProducerInterceptor` methods will be caught,
/// logged, but not propagated further. As a result, if the user
/// configures the interceptor with the wrong key and value type
/// parameters, the producer will not throw an exception, just log the
/// errors.
///
/// `ProducerInterceptor` callbacks may be called from multiple threads.
/// Interceptor implementation must ensure thread-safety, if needed —
/// hence the `Send + Sync` bounds.
///
/// # Translation notes
///
/// * `on_send` accepts an owned `ProducerRecord<K, V>` and returns an
///   owned `ProducerRecord<K, V>` (CLAUDE.md DoD: returns the
///   "(possibly modified) record by **value**, not boxed"). Java mutates
///   in place via the return value; the Rust mirror is owned-record
///   in / owned-record out. Implementations may return the same record
///   unchanged.
/// * `on_acknowledgement` borrows the metadata and the optional error
///   (Java: `RecordMetadata metadata, Exception exception`). Borrowing
///   matches CLAUDE.md rule 12 — the dispatcher already owns the
///   metadata.
/// * The Java overload that adds `Headers headers` is folded into the
///   single `on_acknowledgement` signature. The Rust translation
///   exposes only the richer (3-arg) form because the Java 2-arg
///   default delegates to the 3-arg form anyway.
/// * Java's `AutoCloseable.close()` may throw; we model it as
///   infallible.
pub trait ProducerInterceptor<K, V>: Send + Sync {
    /// Called from the producer's `send` method, before key and value
    /// get serialized and a partition is assigned (if a partition is
    /// not specified in `ProducerRecord`).
    ///
    /// This method is allowed to modify the record, in which case the
    /// new record will be returned. The implication of modifying
    /// key/value is that partition assignment (if not specified in
    /// `ProducerRecord`) will be done based on the modified key/value,
    /// not the original. Consequently, key/value transformation done
    /// in `on_send` needs to be consistent: the same key and value
    /// should mutate to the same (modified) key and value. Otherwise,
    /// log compaction would not work as expected.
    ///
    /// Similarly, it is up to the interceptor implementation to ensure
    /// that the correct topic/partition is returned in
    /// `ProducerRecord`. Most often, it should be the same
    /// topic/partition from `record`.
    ///
    /// Any panic propagated from this method will be caught by the
    /// caller and logged, but not propagated further (matching Java's
    /// "exception caught, logged, ignored" behaviour).
    ///
    /// Since the producer may run multiple interceptors, a particular
    /// interceptor's `on_send` callback will be called in the order
    /// specified by `ProducerConfig::INTERCEPTOR_CLASSES_CONFIG`. The
    /// first interceptor in the list gets the record passed from the
    /// client, the following interceptor will be passed the record
    /// returned by the previous interceptor, and so on.
    fn on_send(&self, record: ProducerRecord<K, V>) -> ProducerRecord<K, V>;

    /// Called when the record sent to the server has been acknowledged,
    /// or when sending the record fails before it gets sent to the
    /// server.
    ///
    /// This method is generally called just before the user callback
    /// is called, and in additional cases when `KafkaProducer.send()`
    /// returns an error.
    ///
    /// Any panic propagated from this method will be ignored by the
    /// caller.
    ///
    /// This method will generally execute in the background I/O
    /// task, so the implementation should be reasonably fast.
    /// Otherwise, sending of messages from other threads could be
    /// delayed.
    ///
    /// * `metadata` — The metadata for the record that was sent (i.e.
    ///   the partition and offset). If an error occurred, metadata
    ///   will only contain a valid topic and maybe a partition. If a
    ///   partition is not given in `ProducerRecord` and an error
    ///   occurs before partition gets assigned, then partition will
    ///   be set to [`RecordMetadata::UNKNOWN_PARTITION`]. The metadata
    ///   may be `None` if the client passed a `null` record to send.
    /// * `exception` — The error encountered during processing of this
    ///   record. `None` if no error occurred.
    /// * `headers` — The headers for the record that was sent. It is
    ///   read-only.
    fn on_acknowledgement(
        &self,
        metadata: Option<&RecordMetadata>,
        exception: Option<&KafkaError>,
        headers: &RecordHeaders,
    ) {
        let _ = (metadata, exception, headers);
    }

    /// Configure this interceptor. Mirrors Java's
    /// `Configurable.configure(Map<String, ?>)`. Default implementation
    /// is a no-op.
    fn configure(&mut self, _configs: &HashMap<String, String>) {}

    /// Called when the interceptor is closed.
    fn close(&mut self) {}
}
