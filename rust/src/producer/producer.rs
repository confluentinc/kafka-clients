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
use std::time::Duration;

use crate::common::Error;
use crate::common::KafkaFuture;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::metrics::MetricsView;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::OffsetAndMetadata;
use crate::producer::Callback;
use crate::producer::ProducerRecord;
use crate::producer::RecordMetadata;

/// The interface for the [`KafkaProducer`](super::KafkaProducer).
// Every asynchronous method returns `impl Future + Send` rather than being
// declared `async fn`, so that generic code can move the returned futures across
// tasks and box them as `dyn Future + Send`. That guarantee is what lets
// `DynProducer` be blanket-implemented for every `Producer`. Implementations may
// still be written with `async fn`.
#[doc(alias = "org.apache.kafka.clients.producer.Producer")]
pub trait Producer<K, V>: Send + Sync {
    /// Needs to be called before any other methods when the `transactional.id`
    /// is set in the configuration.
    ///
    /// See [`KafkaProducer::init_transactions`](super::KafkaProducer::init_transactions).
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if no `transactional.id` has been configured
    /// - `Error::is_unsupported_version_error` fatal error indicating the broker does not support
    ///   transactions (i.e. if its version is lower than 0.11.0.0)
    /// - `Error::is_authorization_error` error indicating that the configured transactional.id is
    ///   not authorized, or the idempotent producer id is unavailable. See the error for more
    ///   details. User may retry this function call after fixing the permission.
    /// - `Error::is_kafka_error` if the producer has encountered a previous fatal error or for any
    ///   other unexpected error
    /// - `Error::is_timeout_error` if the time taken for initialize the transaction has surpassed
    ///   `max.block.ms`.
    /// - `Error::is_interrupt_error` if the task is interrupted while blocked
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#initTransactions")]
    fn init_transactions(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Should be called before the start of each new transaction. Note that
    /// prior to the first invocation of this method, you must invoke
    /// [`init_transactions`](Self::init_transactions) exactly one time.
    ///
    /// See [`KafkaProducer::begin_transaction`](super::KafkaProducer::begin_transaction).
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if no `transactional.id` has been configured or if
    ///   `Self::init_transactions` has not yet been invoked
    /// - `Error::is_producer_fenced_error` if another producer with the same transactional.id is
    ///   active
    /// - `Error::is_invalid_producer_epoch_error` if the producer has attempted to produce with an
    ///   old epoch to the partition leader. See the error for more details
    /// - `Error::is_unsupported_version_error` fatal error indicating the broker does not support
    ///   transactions (i.e. if its version is lower than 0.11.0.0)
    /// - `Error::is_authorization_error` fatal error indicating that the configured
    ///   `transactional.id` is not authorized. See the error for more details
    /// - `Error::is_kafka_error` if the producer has encountered a previous fatal error or for any
    ///   other unexpected error
    ///
    /// # Translation notes
    ///
    /// Synchronous, as Java's `beginTransaction` never blocks.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#beginTransaction")]
    fn begin_transaction(&self) -> Result<(), Error>;

    /// Sends a list of specified offsets to the consumer group coordinator, and
    /// also marks those offsets as part of the current transaction. These
    /// offsets will be considered committed only if the transaction is
    /// committed successfully.
    ///
    /// See [`KafkaProducer::send_offsets_to_transaction`](super::KafkaProducer::send_offsets_to_transaction).
    ///
    /// # Arguments
    ///
    /// - `offsets` - the offsets to commit, the next message your application will consume for
    ///   each partition
    /// - `group_metadata` - the consumer group metadata, extracted from the consumer via
    ///   `KafkaConsumer::group_metadata`
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if no transactional.id has been configured or no
    ///   transaction has been started.
    /// - `Error::is_producer_fenced_error` fatal error indicating another producer with the same
    ///   transactional.id is active
    /// - `Error::is_unsupported_version_error` fatal error indicating the broker does not support
    ///   transactions (i.e. if its version is lower than 0.11.0.0) or the broker doesn't support
    ///   the latest version of transactional API with all consumer group metadata (i.e. if its
    ///   version is lower than 2.5.0).
    /// - `Error::is_unsupported_for_message_format_error` fatal error indicating the message format
    ///   used for the offsets topic on the broker does not support transactions
    /// - `Error::is_authorization_error` fatal error indicating that the configured
    ///   transactional.id is not authorized, or the consumer group id is not authorized.
    /// - `Error::is_consumer_commit_failed_error` if the commit failed and cannot be retried (e.g.
    ///   if the consumer has been kicked out of the group). Users should handle this by aborting
    ///   the transaction.
    /// - `Error::is_fenced_instance_id_error` if this producer instance gets fenced by broker due
    ///   to a mis-configured consumer instance id within group metadata.
    /// - `Error::is_invalid_producer_epoch_error` if the producer has attempted to produce with an
    ///   old epoch to the partition leader. See the error for more details
    /// - `Error::is_kafka_error` if the producer has encountered a previous fatal or abortable
    ///   error, or for any other unexpected error
    /// - `Error::is_timeout_error` if the combined time taken for resolving topic metadata and
    ///   sending the offsets has surpassed `max.block.ms`.
    /// - `Error::is_interrupt_error` if the task is interrupted while blocked
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#sendOffsetsToTransaction")]
    fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &dyn ConsumerGroupMetadata,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commits the ongoing transaction. This method will flush any unsent
    /// records before actually committing the transaction.
    ///
    /// See [`KafkaProducer::commit_transaction`](super::KafkaProducer::commit_transaction).
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if no transactional.id has been configured or no
    ///   transaction has been started
    /// - `Error::is_producer_fenced_error` fatal error indicating another producer with the same
    ///   transactional.id is active
    /// - `Error::is_unsupported_version_error` fatal error indicating the broker does not support
    ///   transactions (i.e. if its version is lower than 0.11.0.0)
    /// - `Error::is_authorization_error` fatal error indicating that the configured
    ///   transactional.id is not authorized. See the error for more details
    /// - `Error::is_invalid_producer_epoch_error` if the producer has attempted to produce with an
    ///   old epoch to the partition leader. See the error for more details
    /// - `Error::is_kafka_error` if the producer has encountered a previous fatal or abortable
    ///   error, or for any other unexpected error
    /// - `Error::is_timeout_error` if the time taken for committing the transaction has surpassed
    ///   `max.block.ms`.
    /// - `Error::is_interrupt_error` if the task is interrupted while blocked
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#commitTransaction")]
    fn commit_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Aborts the ongoing transaction. Any unflushed produce messages will be
    /// aborted when this call is made.
    ///
    /// See [`KafkaProducer::abort_transaction`](super::KafkaProducer::abort_transaction).
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if no transactional.id has been configured or no
    ///   transaction has been started
    /// - `Error::is_producer_fenced_error` fatal error indicating another producer with the same
    ///   transactional.id is active
    /// - `Error::is_invalid_producer_epoch_error` if the producer has attempted to produce with an
    ///   old epoch to the partition leader. See the error for more details
    /// - `Error::is_unsupported_version_error` fatal error indicating the broker does not support
    ///   transactions (i.e. if its version is lower than 0.11.0.0)
    /// - `Error::is_authorization_error` fatal error indicating that the configured
    ///   transactional.id is not authorized. See the error for more details
    /// - `Error::is_kafka_error` if the producer has encountered a previous fatal error or for any
    ///   other unexpected error
    /// - `Error::is_timeout_error` if the time taken for aborting the transaction has surpassed
    ///   `max.block.ms`.
    /// - `Error::is_interrupt_error` if the task is interrupted while blocked
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#abortTransaction")]
    fn abort_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Asynchronously send a record to a topic. Equivalent to
    /// `send_with_callback(record, None)`. See
    /// [`send_with_callback`](Producer::send_with_callback) for details.
    ///
    /// # Arguments
    ///
    /// - `record` - The record to send. If the topic or the partition specified in it cannot be
    ///   found in metadata within `max.block.ms`, the returned future will time out when retrieved.
    ///
    /// # Returns
    ///
    /// A future which will eventually contain the response information.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if a transactional.id has been configured and no
    ///   transaction has been started, or when send is invoked after producer has been closed.
    /// - `Error::is_interrupt_error` If the task is interrupted while blocked
    /// - `Error::is_serialization_error` If the key or value are not valid objects given the
    ///   configured serializers
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    /// - `Error::is_kafka_error` If a Kafka related error occurs that does not belong to the public
    ///   API errors.
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#send")]
    fn send(
        &self,
        record: ProducerRecord<K, V>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send;

    /// Asynchronously send a record to a topic and invoke the provided callback
    /// when the send has been acknowledged.
    ///
    /// The send is asynchronous and this method will return immediately (except
    /// for rare cases described below) once the record has been stored in the
    /// buffer of records waiting to be sent. This allows sending many records in
    /// parallel without blocking to wait for the response after each one. Can
    /// block for the following cases: 1) For the first record being sent to the
    /// cluster by this client for the given topic. In this case it will block for
    /// up to `max.block.ms` milliseconds while waiting for topic's metadata if
    /// Kafka cluster is unreachable; 2) Allocating a buffer if buffer pool
    /// doesn't have any free buffers.
    ///
    /// **Reducing first-send latency:** You can reduce the latency of the first
    /// send by preloading the metadata with
    /// [`partitions_for`](Self::partitions_for). However, be aware that metadata
    /// cache will be cleared to free up resources after `metadata.max.idle.ms`
    /// of inactivity, so subsequent sends after a long idle period will still
    /// experience delays.
    ///
    /// The result of the send is a [`RecordMetadata`] specifying the partition
    /// the record was sent to, the offset it was assigned and the timestamp of
    /// the record. If the producer is configured with acks = 0, the
    /// `RecordMetadata` will have offset = -1 because the producer does not wait
    /// for the acknowledgement from the broker. If CreateTime is used by the
    /// topic, the timestamp will be the user provided timestamp or the record
    /// send time if the user did not specify a timestamp for the record. If
    /// LogAppendTime is used for the topic, the timestamp will be the Kafka
    /// broker local time when the message is appended.
    ///
    /// Since the send call is asynchronous it returns a future for the
    /// `RecordMetadata` that will be assigned to this record. Awaiting this
    /// future waits until the associated request completes and then returns
    /// the metadata for the record or the error that occurred while sending the
    /// record. Fully non-blocking usage can make use of the `callback` parameter
    /// to provide a callback that will be invoked when the request is complete.
    ///
    /// Callbacks for records being sent to the same partition are guaranteed to
    /// execute in order.
    ///
    /// When used as part of a transaction, it is not necessary to define a
    /// callback or check the result of the future in order to detect errors
    /// from `send`. If any of the send calls failed with an irrecoverable error,
    /// the final [`commit_transaction`](Self::commit_transaction) call will fail
    /// and return the error from the last failed send. When this happens, your
    /// application should call [`abort_transaction`](Self::abort_transaction)
    /// to reset the state and continue to send data.
    ///
    /// Some transactional send errors cannot be resolved with a call to
    /// `abort_transaction`. In particular, if a transactional send finishes with
    /// a `ProducerFencedError`, a `OutOfOrderSequenceError`, a
    /// `UnsupportedVersionError`, or an `AuthorizationError`, then the only
    /// option left is to call [`close`](Self::close). Fatal errors cause the
    /// producer to enter a defunct state in which future API calls will continue
    /// to raise the same underlying error wrapped in a new `KafkaError`.
    ///
    /// It is a similar picture when idempotence is enabled, but no
    /// `transactional.id` has been configured. In this case,
    /// `UnsupportedVersionError` and `AuthorizationError` are considered fatal
    /// errors. However, `ProducerFencedError` does not need to be handled.
    /// Additionally, it is possible to continue sending after receiving an
    /// `OutOfOrderSequenceError`, but doing so can result in out of order
    /// delivery of pending messages. To ensure proper ordering, you should close
    /// the producer and create a new instance.
    ///
    /// If the message format of the destination topic is not upgraded to
    /// 0.11.0.0, idempotent and transactional produce requests will fail with an
    /// `UnsupportedForMessageFormatError` error. If this is encountered during a
    /// transaction, it is possible to abort and continue. But note that future
    /// sends to the same topic will continue receiving the same error until the
    /// topic is upgraded.
    ///
    /// Note that callbacks will generally execute in the I/O task of the
    /// producer and so should be reasonably fast or they will delay the sending
    /// of messages from other tasks. If you want to execute blocking or
    /// computationally expensive callbacks it is recommended to hand the work
    /// to another task from the callback body.
    ///
    /// # Arguments
    ///
    /// - `record` - The record to send. If the topic or the partition specified in it cannot be
    ///   found in metadata within `max.block.ms`, the returned future will time out when retrieved.
    /// - `callback` - A user-supplied callback to execute when the record has been acknowledged by
    ///   the server (`None` indicates no callback)
    ///
    /// # Returns
    ///
    /// A future which will eventually contain the response information.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if a transactional.id has been configured and no
    ///   transaction has been started, or when send is invoked after producer has been closed.
    /// - `Error::is_interrupt_error` If the task is interrupted while blocked
    /// - `Error::is_serialization_error` If the key or value are not valid objects given the
    ///   configured serializers
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    /// - `Error::is_kafka_error` If a Kafka related error occurs that does not belong to the public
    ///   API errors.
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#send")]
    fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Box<dyn Callback>>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send;

    /// Invoking this method makes all buffered records immediately available to
    /// send (even if `linger.ms` is greater than 0) and blocks on the completion
    /// of the requests associated with these records. The post-condition of
    /// `flush()` is that any previously sent record will have completed (e.g.
    /// its future is done and callbacks passed to `Self::send_with_callback`
    /// have been called). A request is considered completed when it is
    /// successfully acknowledged according to the `acks` configuration you have
    /// specified or else it results in an error.
    ///
    /// Other tasks can continue sending records while one task is blocked
    /// waiting for a flush call to complete, however no guarantee is made about
    /// the completion of records sent after the flush call begins.
    ///
    /// This method can be useful when consuming from some input system and
    /// producing into Kafka. The `flush()` call gives a convenient way to ensure
    /// all previously sent messages have actually completed. Note that records
    /// may be dropped if the produce request fails; to ensure that this does not
    /// occur set `retries=<large_number>` in the config.
    ///
    /// Applications don't need to call this method for transactional producers,
    /// since the [`commit_transaction`](Self::commit_transaction) will flush all
    /// buffered records before performing the commit. This ensures that all the
    /// `send` calls made since the previous
    /// [`begin_transaction`](Self::begin_transaction) are completed before the
    /// commit.
    ///
    /// **Important:** This method must not be called from within the callback
    /// provided to `Self::send_with_callback`. Invoking `flush()` in this
    /// context will result in a `KafkaError`, as it will cause a deadlock.
    ///
    /// # Errors
    ///
    /// - `Error::is_interrupt_error` If the task is interrupted while blocked
    /// - `Error::is_kafka_error` If the method is invoked inside a `Self::send` callback
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    /// [`Callback::on_completion`] is synchronous, so it cannot await this
    /// method.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#flush")]
    fn flush(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Get the partition metadata for the given topic. This can be used for
    /// custom partitioning.
    ///
    /// This will attempt to refresh metadata until it finds the topic in it, or
    /// the configured `max.block.ms` expires.
    ///
    /// # Arguments
    ///
    /// - `topic` - The topic to get partition metadata for
    ///
    /// # Returns
    ///
    /// The list of partitions.
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails.
    /// - `Error::is_authorization_error` if not authorized to the specified topic.
    /// - `Error::is_interrupt_error` if the task is interrupted while blocked
    /// - `Error::is_timeout_error` if the topic cannot be found in metadata within `max.block.ms`
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    /// - `Error::is_kafka_error` for all Kafka-related errors, including the case where this
    ///   method is called after producer close
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#partitionsFor")]
    fn partitions_for(&self, topic: &str) -> impl Future<Output = Result<Vec<PartitionInfo>, Error>> + Send;

    /// Get the full set of internal metrics maintained by the producer.
    ///
    /// The returned map is an unmodifiable live view of the metrics. Changes to
    /// the underlying metrics will be reflected in the returned map.
    ///
    /// # Returns
    ///
    /// An unmodifiable live view of the map of metrics currently maintained by
    /// the producer
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#metrics")]
    fn metrics(&self) -> MetricsView;

    /// Close this producer. This method blocks until all previously sent
    /// requests complete. This method is equivalent to
    /// `close_with_timeout(Duration::MAX)`.
    ///
    /// **If close() is called from a [`Callback`], a warning message will be
    /// logged and `close_with_timeout(Duration::ZERO)` will be called instead.
    /// We do this because the sender task would otherwise try to join itself and
    /// block forever.**
    ///
    /// # Errors
    ///
    /// - `Error::is_interrupt_error` If the task is interrupted while blocked.
    /// - `Error::is_kafka_error` If an unexpected error occurs while trying to close the client,
    ///   this error should be treated as fatal and indicate the client is no longer usable.
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    /// [`Callback::on_completion`] is synchronous, so it cannot await this
    /// method.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#close")]
    fn close(&self) -> impl Future<Output = Result<(), Error>> + Send;

    /// This method waits up to `timeout` for the producer to complete the
    /// sending of all incomplete requests.
    ///
    /// If the producer is unable to complete all requests before the timeout
    /// expires, this method will fail any unsent and unacknowledged records
    /// immediately. It will also abort the ongoing transaction if it's not
    /// already completing.
    ///
    /// If invoked from within a [`Callback`] this method will not block and will
    /// be equivalent to `close_with_timeout(Duration::ZERO)`. This is done since
    /// no further sending will happen while blocking the I/O task of the
    /// producer.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum time to wait for producer to complete any pending requests. The
    ///   value should be non-negative. Specifying a timeout of zero means do not wait for pending
    ///   send requests to complete.
    ///
    /// # Errors
    ///
    /// - `Error::is_interrupt_error` If the task is interrupted while blocked.
    /// - `Error::is_kafka_error` If an unexpected error occurs while trying to close the client,
    ///   this error should be treated as fatal and indicate the client is no longer usable.
    /// - `Error::is_local_illegal_argument_error` If the `timeout` is negative.
    ///
    /// # Translation notes
    ///
    /// A Rust task is never interrupted: dropping the future cancels the wait.
    /// A `Duration` is never negative. [`Callback::on_completion`] is
    /// synchronous, so it cannot await this method.
    #[doc(alias = "org.apache.kafka.clients.producer.Producer#close")]
    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send;
}
