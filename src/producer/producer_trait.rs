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
//!
//! Transactional methods are not included in this phase.

use std::time::Duration;

use crate::common::KafkaError;
use crate::common::KafkaFuture;
use crate::common::PartitionInfo;
use crate::producer::ProducerRecord;
use crate::producer::RecordMetadata;
use crate::producer::internals::Callback;

/// The interface for the [`KafkaProducer`](super::kafka_producer::KafkaProducer).
///
/// Translated from `org.apache.kafka.clients.producer.Producer`.
///
/// Transactional methods (`init_transactions`, `begin_transaction`,
/// `commit_transaction`, `abort_transaction`, `send_offsets_to_transaction`)
/// are not included in this phase.
#[allow(async_fn_in_trait)]
pub trait Producer<K, V> {
    /// Asynchronously send a record to a topic. Equivalent to
    /// `send_with_callback(record, None)`.
    ///
    /// See [`send_with_callback`](Producer::send_with_callback) for details.
    async fn send(&self, record: ProducerRecord<K, V>) -> Result<KafkaFuture<RecordMetadata>, KafkaError>;

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
    /// - The producer has already been closed ([`IllegalState`](KafkaError::IllegalState))
    /// - The key or value cannot be serialized ([`Serialization`](KafkaError::Serialization))
    /// - A Kafka-related error occurs
    async fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError>;

    /// Invoking this method makes all buffered records immediately available to send
    /// and awaits the completion of the requests associated with these records.
    ///
    /// # Errors
    ///
    /// Returns `Err` if an error occurs during flushing.
    async fn flush(&self) -> Result<(), KafkaError>;

    /// Get the partition metadata for the given topic.
    ///
    /// This can be used for custom partitioning.
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - The topic cannot be found within `max.block.ms` ([`Timeout`](KafkaError::Timeout))
    /// - The producer has been closed
    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError>;

    /// Close this producer. This method awaits until all previously sent requests
    /// complete.
    ///
    /// # Errors
    ///
    /// Returns `Err` if an error occurs during closing.
    async fn close(&self) -> Result<(), KafkaError>;

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
    async fn close_timeout(&self, timeout: Duration) -> Result<(), KafkaError>;
}
