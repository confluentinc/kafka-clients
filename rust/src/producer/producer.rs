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

//! The Producer trait — the interface for the KafkaProducer.
//!
//! Translated from `org.apache.kafka.clients.producer.Producer`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::common::Error;
use crate::common::KafkaFuture;
use crate::common::MetricName;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::metrics::KafkaMetric;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::OffsetAndMetadata;
use crate::producer::Callback;
use crate::producer::ProducerRecord;
use crate::producer::RecordMetadata;

/// The interface for the [`KafkaProducer`](super::KafkaProducer).
///
/// Translated from `org.apache.kafka.clients.producer.Producer`.
///
/// Every asynchronous method returns `impl Future + Send` rather than being
/// declared `async fn`, so that generic code can move the returned futures
/// across tasks and box them as `dyn Future + Send`. That guarantee is what lets
/// [`DynProducer`](super::DynProducer) be blanket-implemented for every
/// `Producer`. Implementations may still be written with `async fn`.
#[doc(alias = "org.apache.kafka.clients.producer.Producer")]
pub trait Producer<K, V>: Send + Sync {
    /// Needs to be called before any other method when the `transactional.id` is
    /// set in the configuration.
    ///
    /// See [`KafkaProducer::init_transactions`](super::KafkaProducer::init_transactions).
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - No `transactional.id` has been configured
    ///   ([`LocalIllegalState`](Error::LocalIllegalState))
    /// - The broker does not support transactions
    ///   ([`UnsupportedVersion`](Error::UnsupportedVersion))
    /// - The configured `transactional.id` is not authorized, or the idempotent
    ///   producer id is unavailable
    /// - The producer has encountered a previous fatal error
    /// - Initialization does not complete within `max.block.ms`
    ///   ([`Timeout`](Error::Timeout))
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#initTransactions")]
    fn init_transactions(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Should be called before the start of each new transaction.
    ///
    /// See [`KafkaProducer::begin_transaction`](super::KafkaProducer::begin_transaction).
    ///
    /// Stays synchronous because Java's `beginTransaction`
    /// (`KafkaProducer.java:734-740`) is a pure state transition and never
    /// blocks.
    ///
    /// # Errors
    ///
    /// Returns `Err` if no `transactional.id` has been configured, if
    /// `init_transactions` has not yet been invoked, if another producer with
    /// the same `transactional.id` has fenced this one, or if the producer has
    /// encountered a previous fatal error.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#beginTransaction")]
    fn begin_transaction(&self) -> Result<(), Error>;

    /// Sends a list of specified offsets to the consumer group coordinator, and
    /// also marks those offsets as part of the current transaction.
    ///
    /// See [`KafkaProducer::send_offsets_to_transaction`](super::KafkaProducer::send_offsets_to_transaction).
    ///
    /// # Errors
    ///
    /// Returns `Err` if no `transactional.id` has been configured or no
    /// transaction has been started, if `group_metadata` is invalid, if the
    /// commit failed and cannot be retried, or if the offsets are not sent
    /// within `max.block.ms` ([`Timeout`](Error::Timeout)).
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#sendOffsetsToTransaction")]
    fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &dyn ConsumerGroupMetadata,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commits the ongoing transaction.
    ///
    /// See [`KafkaProducer::commit_transaction`](super::KafkaProducer::commit_transaction).
    ///
    /// # Errors
    ///
    /// Returns `Err` if no `transactional.id` has been configured or no
    /// transaction has been started, if the producer has encountered a previous
    /// fatal or abortable error, or if the commit does not complete within
    /// `max.block.ms` ([`Timeout`](Error::Timeout)).
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#commitTransaction")]
    fn commit_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Aborts the ongoing transaction.
    ///
    /// See [`KafkaProducer::abort_transaction`](super::KafkaProducer::abort_transaction).
    ///
    /// # Errors
    ///
    /// Returns `Err` if no `transactional.id` has been configured or no
    /// transaction has been started, if the producer has encountered a previous
    /// fatal error, or if the abort does not complete within `max.block.ms`
    /// ([`Timeout`](Error::Timeout)).
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#abortTransaction")]
    fn abort_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Asynchronously send a record to a topic. Equivalent to
    /// `send_with_callback(record, None)`.
    ///
    /// See [`send_with_callback`](Producer::send_with_callback) for details.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#send")]
    fn send(
        &self,
        record: ProducerRecord<K, V>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send;

    /// Asynchronously send a record to a topic and invoke the provided callback
    /// when the send has been acknowledged.
    ///
    /// The send is asynchronous and this method will return immediately once the record
    /// has been stored in the buffer of records waiting to be sent. It may block
    /// waiting for metadata or buffer space.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to send
    /// * `callback` - A user-supplied callback to execute when the record has been
    ///   acknowledged by the server (`None` indicates no callback)
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - The producer has already been closed ([`LocalIllegalState`](Error::LocalIllegalState))
    /// - The key or value cannot be serialized ([`Serialization`](Error::Serialization))
    /// - DNS resolution of the bootstrap servers fails within
    ///   `bootstrap.resolve.timeout.ms` ([`BootstrapResolution`](Error::BootstrapResolution), KIP-909)
    /// - A Kafka-related error occurs
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#send")]
    fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send;

    /// Invoking this method makes all buffered records immediately available to send
    /// and awaits the completion of the requests associated with these records.
    ///
    /// # Errors
    ///
    /// Returns `Err` if an error occurs during flushing.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#flush")]
    fn flush(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Get the partition metadata for the given topic.
    ///
    /// This can be used for custom partitioning.
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - The topic cannot be found within `max.block.ms` ([`Timeout`](Error::Timeout))
    /// - DNS resolution of the bootstrap servers fails within
    ///   `bootstrap.resolve.timeout.ms` ([`BootstrapResolution`](Error::BootstrapResolution), KIP-909)
    /// - The producer has been closed
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#partitionsFor")]
    fn partitions_for(&self, topic: &str) -> impl Future<Output = Result<Vec<PartitionInfo>, Error>> + Send;

    /// Get the full set of producer metrics maintained by this producer.
    ///
    /// Translated from `Producer.metrics()`. The returned map is keyed by
    /// [`MetricName`]; the value type is `Arc<KafkaMetric>` — [`KafkaMetric`]
    /// is the concrete registry entry (Java's `Metric` interface). This method
    /// does not block in Java, so it stays a synchronous `fn`.
    ///
    /// The returned `HashMap` is a snapshot clone of `Arc<KafkaMetric>`
    /// handles; mutating it does not affect the registry (Java's
    /// `Collections.unmodifiableMap` analog). Metrics registered or removed
    /// afterwards are not reflected in it, but each [`KafkaMetric`] is the
    /// registry's own shared entry, so reading its value returns the current
    /// value. (Java's javadoc, KAFKA-20341, documents its map as an
    /// unmodifiable *live* view of the metrics; the snapshot of the key set is
    /// the one difference.)
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#metrics")]
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>;

    /// Close this producer. This method awaits until all previously sent requests
    /// complete.
    ///
    /// # Errors
    ///
    /// Returns `Err` if an error occurs during closing.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#close")]
    fn close(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Close this producer, waiting up to the given timeout for pending requests
    /// to complete.
    ///
    /// If the producer is unable to complete all requests before the timeout
    /// expires, this method will fail any unsent and unacknowledged records
    /// immediately.
    ///
    /// # Errors
    ///
    /// Returns `Err` if an error occurs during closing.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#close")]
    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send;
}
