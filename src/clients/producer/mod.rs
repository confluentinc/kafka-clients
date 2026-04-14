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

//! Producer types (org.apache.kafka.clients.producer).

use std::time::Duration;

use crate::common::kafka_error::KafkaError;
use crate::common::partition_info::PartitionInfo;

pub mod future_record_metadata;
pub mod record;
pub mod record_metadata;

pub use future_record_metadata::FutureRecordMetadata;
pub use record::{Header, ProducerRecord};
pub use record_metadata::RecordMetadata;

/// The interface for the KafkaProducer.
///
/// Translated from `org.apache.kafka.clients.producer.Producer`.
///
/// # Design Notes
///
/// - `&self` is used for `send` and `flush` because Java uses `synchronized`
///   blocks inside the implementation. In Rust, interior mutability via `Mutex`
///   allows `&self` methods, which also enables `Arc<dyn Producer>` sharing.
/// - `&self` is used for `close` because Java's `KafkaProducer.close()` uses
///   internal synchronization (not exclusive ownership). In Rust, implementors
///   use interior mutability (e.g., `AtomicBool` or `Mutex`) to manage the
///   `closed` flag, which keeps `close` callable through `Arc<dyn Producer>`.
/// - `Result` return types match Java's unchecked exceptions
///   (`IllegalStateException`, `KafkaException`). Per CLAUDE.md rule 10.2,
///   we return `Result` for recoverable errors even when Java uses unchecked
///   exceptions.
/// - `ProducerRecord` is taken by value since ownership transfers to the
///   producer. The caller constructs a record and hands it off.
/// - No generics — without serializers, the trait works with raw bytes. If
///   serializers are added later, the trait can become `Producer<K, V>`.
///
/// ## Methods Excluded (with rationale)
///
/// | Java Method | Reason |
/// |-------------|--------|
/// | `initTransactions()` | Transactional — out of scope |
/// | `beginTransaction()` | Transactional — out of scope |
/// | `commitTransaction()` | Transactional — out of scope |
/// | `abortTransaction()` | Transactional — out of scope |
/// | `sendOffsetsToTransaction(...)` | Transactional — out of scope |
/// | `send(record, callback)` | Callback — out of scope |
/// | `metrics()` | Metrics — out of scope |
/// | `clientInstanceId(timeout)` | Telemetry — out of scope |
/// | `registerMetricForSubscription(metric)` | Metrics — out of scope |
/// | `unregisterMetricFromSubscription(metric)` | Metrics — out of scope |
pub trait Producer: Send + Sync {
    /// Send a record to Kafka.
    ///
    /// Returns a future that will eventually contain the record metadata
    /// (offset, timestamp, partition) assigned by the broker.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if the producer is closed or a send error
    /// has been injected (in the case of `MockProducer`).
    fn send(&self, record: ProducerRecord) -> Result<FutureRecordMetadata, KafkaError>;

    /// Flush all accumulated records, blocking until all sends are complete.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if the producer is closed or a flush error
    /// has been injected.
    fn flush(&self) -> Result<(), KafkaError>;

    /// Get the partition metadata for a topic.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if partition metadata cannot be retrieved.
    fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError>;

    /// Close this producer. Blocks until all previously sent records are
    /// acknowledged.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if the close operation fails (e.g., due to
    /// an interrupt or an internal error while flushing pending records).
    fn close(&self) -> Result<(), KafkaError>;

    /// Close this producer with a timeout.
    ///
    /// If the close does not complete within the given timeout, any pending
    /// sends may be aborted.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if the close operation fails (e.g., due to
    /// an interrupt or an internal error while flushing pending records).
    fn close_with_timeout(&self, timeout: Duration) -> Result<(), KafkaError>;
}
