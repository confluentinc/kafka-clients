// Copyright 2026 Confluent Inc.
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

//! The share consumer interface (KIP-932).
//!
//! Translated from `org.apache.kafka.clients.consumer.ShareConsumer`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::common::metrics::MetricsView;
use crate::common::{Error, TopicIdPartition};
use crate::consumer::{
    AcknowledgeOptions, AcknowledgeType, AcknowledgementCommitCallback, ConsumerRecord, ConsumerRecords,
    ShareConsumerHandle,
};

/// A client that consumes records from a Kafka cluster using a share group.
///
/// # See also
///
/// - [`KafkaShareConsumer`](crate::consumer::KafkaShareConsumer)
/// - [`MockShareConsumer`](crate::consumer::MockShareConsumer)
///
/// # Translation notes
///
/// Asynchronous where Java blocks; see
/// [`DynShareConsumer`](crate::consumer::DynShareConsumer).
//
// Methods that block in Java return `impl Future + Send`; hold
// `DynShareConsumer` to keep different implementations behind one type.
//
// Parameters follow the `Consumer` trait's convention (`consumer/mod.rs`): a
// `Vec` is retained by the implementation (`subscribe` moves the topics into
// the subscription), a reference is only read.
//
// The shape is the `Consumer` trait's (`share-consumer.md` §1): `Send +
// 'static`, not `Sync`, with `&mut self` on every method Java's
// `ShareConsumerImpl` guards with `acquireAndEnsureOpen()` or `acquire()`,
// and `&self` only on the unguarded `metrics` and `wakeup` and the Rust-only
// `handle` (`cargo xtask lint-custom check-consumer-receiver`). A method is
// asynchronous when Java blocks in it or runs the acknowledgement commit
// callback in it: `commit_async` does not block in Java, but it calls
// `handleCompletedAcknowledgements`, which awaits the async callback here.
//
// # Methods NOT translated
//
// - `clientInstanceId(Duration)`, `registerMetricForSubscription`,
//   `unregisterMetricFromSubscription`: KIP-714 broker-push telemetry, left
//   out exactly as the `Consumer` trait leaves them out (Milestone 18 decision
//   D2, 2026-11-20). When KIP-714 lands, both traits gain them together.
#[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer")]
pub trait ShareConsumer<K, V>: Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Get the current subscription. Will return the same topics used in the most recent call
    /// to [`Self::subscribe`], or an empty set if no such call has been made.
    ///
    /// The returned set is a snapshot of the current subscription at the time of the call. It
    /// will not be updated if the subscription changes afterward.
    ///
    /// # Returns
    ///
    /// An immutable snapshot of the set of topics currently subscribed to
    ///
    /// # Panics
    ///
    /// On [`KafkaShareConsumer`](crate::consumer::KafkaShareConsumer) and
    /// [`MockShareConsumer`](crate::consumer::MockShareConsumer), with `"This
    /// consumer has already been closed."` when the consumer is closed, where
    /// Java throws `IllegalStateException`: calling it after `close()` is a
    /// programming error.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#subscription")]
    fn subscription(&mut self) -> HashSet<String>;

    /// Subscribe to the given list of topics to get dynamically assigned partitions. **Topic
    /// subscriptions are not incremental. This list will replace the current assignment, if
    /// there is one.** If the given list of topics is empty, it is treated the same as
    /// [`Self::unsubscribe`].
    ///
    /// As part of group management, the coordinator will keep track of the list of consumers
    /// that belong to a particular group and will trigger a rebalance operation if any one of
    /// the following events are triggered:
    ///
    /// - A member joins or leaves the share group
    /// - An existing member of the share group is shut down or fails
    /// - The number of partitions changes for any of the subscribed topics
    /// - A subscribed topic is created or deleted
    ///
    /// # Arguments
    ///
    /// - `topics` - The list of topics to subscribe to
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if topics contains empty elements
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#subscribe")]
    fn subscribe(&mut self, topics: Vec<String>) -> impl Future<Output = Result<(), Error>> + Send;

    /// Unsubscribe from topics currently subscribed with [`Self::subscribe`].
    ///
    /// # Errors
    ///
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#unsubscribe")]
    fn unsubscribe(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Deliver records for the topics specified using [`Self::subscribe`]. It is an error to
    /// not have subscribed to any topics before polling for data.
    ///
    /// This method returns immediately if there are records available. Otherwise, it will
    /// await the passed timeout. If the timeout expires, an empty record set will be returned.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum time to block (must not be greater than `i64::MAX`
    ///   milliseconds)
    ///
    /// # Returns
    ///
    /// map of topic to records
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if caller lacks Read access to any of the subscribed
    ///   topics or to the share group. See the error for more details
    /// - `Error::is_local_illegal_argument_error` if the timeout value is negative
    /// - `Error::is_local_illegal_state_error` if the consumer is not subscribed to any topics,
    ///   or it is using explicit acknowledgement and has not acknowledged all records previously
    ///   delivered
    /// - `Error::is_invalid_topic_error` if the current subscription contains any invalid topic
    ///   (per `Topic::validate`)
    /// - `Error::is_wakeup_error` if [`Self::wakeup`] is called before or while this method is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   method is called
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    ///
    /// # Translation notes
    ///
    /// The returned future may be dropped before it completes, for example by
    /// `tokio::time::timeout`. Records it took from the cluster are then not
    /// lost: the next `poll()` returns them, and none is acknowledged before it
    /// is returned.
    ///
    /// WARNING: a `timeout` greater than `i64::MAX` milliseconds counts as
    /// `i64::MAX` milliseconds, where Java throws `ArithmeticException`, so
    /// `Duration::MAX` waits without limit.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#poll")]
    fn poll(&mut self, timeout: Duration) -> impl Future<Output = Result<ConsumerRecords<K, V>, Error>> + Send;

    /// Acknowledge successful delivery of a record returned on the last [`Self::poll`] call.
    /// The acknowledgement is committed on the next [`Self::commit_sync`],
    /// [`Self::commit_async`] or [`Self::poll`] call.
    ///
    /// This method can only be used if the consumer is using **explicit acknowledgement**.
    ///
    /// # Arguments
    ///
    /// - `record` - The record to acknowledge
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the record is not waiting to be acknowledged,
    ///   or the consumer is not using explicit acknowledgement
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#acknowledge(ConsumerRecord)")]
    fn acknowledge_with_record(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), Error>;

    /// Acknowledge delivery of a record returned on the last [`Self::poll`] call indicating
    /// whether it was processed successfully. The acknowledgement is committed on the next
    /// [`Self::commit_sync`], [`Self::commit_async`] or [`Self::poll`] call.
    ///
    /// This method can only be used if the consumer is using **explicit acknowledgement**.
    ///
    /// # Arguments
    ///
    /// - `record` - The record to acknowledge
    /// - `r#type` - The acknowledge type which indicates whether it was processed successfully
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the record is not waiting to be acknowledged,
    ///   or the consumer is not using explicit acknowledgement
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#acknowledge(ConsumerRecord,AcknowledgeType)")]
    fn acknowledge_with_record_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        r#type: AcknowledgeType,
    ) -> Result<(), Error>;

    /// Acknowledge delivery of a record returned on the last [`Self::poll`] call indicating
    /// whether it was processed successfully. The acknowledgement is committed on the next
    /// [`Self::commit_sync`], [`Self::commit_async`] or [`Self::poll`] call.
    ///
    /// This method can only be used if the consumer is using **explicit acknowledgement**.
    ///
    /// It provides an alternative to [`Self::acknowledge_with_record_type`] for situations where
    /// the [`ConsumerRecord`] is not available, such as when the record could not be
    /// deserialized.
    ///
    /// # Arguments
    ///
    /// - `options` - The topic, partition and offset of the record to acknowledge, and the
    ///   acknowledge type which indicates whether it was processed successfully
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the record is not waiting to be acknowledged,
    ///   or the consumer is not using explicit acknowledgement
    // Java's four parameters `(topic, partition, offset, type)` are the fields
    // of `AcknowledgeOptions` (CLAUDE.md §2: more than three parameters).
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#acknowledge(String,int,long,AcknowledgeType)")]
    fn acknowledge_with_options(&mut self, options: AcknowledgeOptions) -> Result<(), Error>;

    /// Commit the acknowledgements for the records returned. If the consumer is using explicit
    /// acknowledgement, the acknowledgements to commit have been indicated using
    /// [`Self::acknowledge_with_record`] or [`Self::acknowledge_with_record_type`]. If the
    /// consumer is using implicit acknowledgement, all the records returned by the latest call
    /// to [`Self::poll`] are acknowledged.
    ///
    /// This is a synchronous commit and will block until either the commit succeeds, an
    /// unrecoverable error is encountered (in which case it is returned to the caller), or the
    /// timeout specified by `default.api.timeout.ms` expires.
    ///
    /// # Returns
    ///
    /// A map of the results for each topic-partition for which delivery was acknowledged. If
    /// the acknowledgement failed for a topic-partition, an error is present.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if [`Self::wakeup`] is called before or while this method is
    ///   called
    /// - `Error::is_interrupt_error` if the thread is interrupted while blocked
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#commitSync()")]
    fn commit_sync(&mut self) -> impl Future<Output = Result<HashMap<TopicIdPartition, Option<Error>>, Error>> + Send;

    /// Commit the acknowledgements for the records returned. If the consumer is using explicit
    /// acknowledgement, the acknowledgements to commit have been indicated using
    /// [`Self::acknowledge_with_record`] or [`Self::acknowledge_with_record_type`]. If the
    /// consumer is using implicit acknowledgement, all the records returned by the latest call
    /// to [`Self::poll`] are acknowledged.
    ///
    /// This is a synchronous commit and will block until either the commit succeeds, an
    /// unrecoverable error is encountered (in which case it is returned to the caller), or the
    /// timeout expires.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum amount of time to await completion of the acknowledgement
    ///
    /// # Returns
    ///
    /// A map of the results for each topic-partition for which delivery was acknowledged. If
    /// the acknowledgement failed for a topic-partition, an error is present.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if the `timeout` is negative
    /// - `Error::is_wakeup_error` if [`Self::wakeup`] is called before or while this method is
    ///   called
    /// - `Error::is_interrupt_error` if the thread is interrupted while blocked
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#commitSync(Duration)")]
    fn commit_sync_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicIdPartition, Option<Error>>, Error>> + Send;

    /// Commit the acknowledgements for the records returned. If the consumer is using explicit
    /// acknowledgement, the acknowledgements to commit have been indicated using
    /// [`Self::acknowledge_with_record`] or [`Self::acknowledge_with_record_type`]. If the
    /// consumer is using implicit acknowledgement, all the records returned by the latest call
    /// to [`Self::poll`] are acknowledged.
    ///
    /// # Errors
    ///
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    ///
    /// # Translation notes
    ///
    /// Asynchronous: Java's `commitAsync` runs the acknowledgement commit
    /// callback for acknowledgements completed earlier, and the Rust callback
    /// is awaited.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#commitAsync")]
    fn commit_async(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Sets the acknowledgement commit callback which can be used to handle acknowledgement
    /// completion.
    ///
    /// # Arguments
    ///
    /// - `callback` - The acknowledgement commit callback, or `None` to remove it
    ///
    /// # Panics
    ///
    /// On [`KafkaShareConsumer`](crate::consumer::KafkaShareConsumer), with
    /// `"This consumer has already been closed."` when the consumer is closed,
    /// where Java throws `IllegalStateException`.
    /// [`MockShareConsumer`](crate::consumer::MockShareConsumer) follows Java's
    /// `MockShareConsumer`, which does not check.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#setAcknowledgementCommitCallback")]
    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>);

    /// Returns the acquisition lock timeout for the last set of records fetched from the
    /// cluster.
    ///
    /// # Returns
    ///
    /// The acquisition lock timeout in milliseconds, or `None` if the timeout is not known.
    ///
    /// # Panics
    ///
    /// On [`KafkaShareConsumer`](crate::consumer::KafkaShareConsumer), with
    /// `"This consumer has already been closed."` when the consumer is closed,
    /// where Java throws `IllegalStateException`.
    /// [`MockShareConsumer`](crate::consumer::MockShareConsumer) follows Java's
    /// `MockShareConsumer`, which does not check.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#acquisitionLockTimeoutMs")]
    fn acquisition_lock_timeout_ms(&mut self) -> Option<i32>;

    /// Get the metrics kept by the consumer
    ///
    /// The returned map is an unmodifiable live view of the metrics. Changes to the underlying
    /// metrics will be reflected in the returned map.
    ///
    /// # Returns
    ///
    /// An unmodifiable live view of the map of metrics currently maintained by the consumer
    ///
    /// # Panics
    ///
    /// On [`MockShareConsumer`](crate::consumer::MockShareConsumer), with
    /// `"This consumer has already been closed."` when the mock is closed, as
    /// Java's mock throws. `KafkaShareConsumer` does not check, as Java's.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#metrics")]
    fn metrics(&self) -> MetricsView;

    /// Close the consumer, waiting for up to the default timeout of 30 seconds for any needed
    /// cleanup. This will commit acknowledgements if possible within the default timeout. See
    /// [`Self::close_with_timeout`] for details. Note that [`Self::wakeup`] cannot be used to
    /// interrupt close.
    ///
    /// This close operation will attempt all shutdown steps even if one of them fails. It logs
    /// all encountered errors, continues to execute the next steps, and finally returns the
    /// first error found.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if [`Self::wakeup`] is called before or while this method is
    ///   called
    /// - `Error::is_interrupt_error` if the thread is interrupted before or while this method is
    ///   called
    /// - `Error::is_kafka_error` for any other error during close
    ///
    /// # Translation notes
    ///
    /// Wakeups are disabled once `close` starts. A `close` future dropped before
    /// it completes, for example by `tokio::time::timeout`, leaves the consumer
    /// open with wakeups still disabled: `wakeup`, also through a
    /// [`ShareConsumerHandle`], then has no effect. Call `close` again to finish
    /// closing.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#close()")]
    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Tries to close the consumer cleanly within the specified timeout. This method waits up to
    /// `timeout` for the consumer to complete acknowledgements and leave the group. If the
    /// consumer is unable to complete acknowledgements and gracefully leave the group before the
    /// timeout expires, the consumer is force closed. Note that [`Self::wakeup`] cannot be used
    /// to interrupt close.
    ///
    /// The actual maximum wait time is bounded by the
    /// `ConsumerConfig::REQUEST_TIMEOUT_MS_CONFIG` setting, which only applies to operations
    /// performed with the broker (coordinator-related requests). Even if a larger timeout is
    /// specified, the consumer will not wait longer than
    /// `ConsumerConfig::REQUEST_TIMEOUT_MS_CONFIG` for these requests to complete during the
    /// close operation. Note that the execution time of callbacks (such as
    /// [`AcknowledgementCommitCallback`]) do not consume time from the close timeout.
    ///
    /// This close operation will attempt all shutdown steps even if one of them fails. It logs
    /// all encountered errors, continues to execute the next steps, and finally returns the
    /// first error found.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum time to wait for consumer to close gracefully. The value must
    ///   be non-negative. Specifying a timeout of zero means do not wait for pending requests to
    ///   complete.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if the `timeout` is negative
    /// - `Error::is_wakeup_error` if [`Self::wakeup`] is called before or while this method is
    ///   called
    /// - `Error::is_interrupt_error` if the thread is interrupted before or while this method is
    ///   called
    /// - `Error::is_kafka_error` for any other error during close
    ///
    /// # Translation notes
    ///
    /// Wakeups are disabled once `close` starts. A `close` future dropped before
    /// it completes, for example by `tokio::time::timeout`, leaves the consumer
    /// open with wakeups still disabled: `wakeup`, also through a
    /// [`ShareConsumerHandle`], then has no effect. Call `close` again to finish
    /// closing.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#close(Duration)")]
    fn close_with_timeout(&mut self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send;

    /// Wake up the consumer. This method is thread-safe and is useful in particular to abort a
    /// long poll. The thread which is blocking in an operation will return `WakeupError`. If no
    /// thread is blocking in a method which can return `WakeupError`, the next call to such a
    /// method will raise it instead.
    ///
    /// # Translation notes
    ///
    /// The consumer is not `Sync`: from another task, call
    /// [`ShareConsumerHandle::wakeup`] on [`Self::handle`]'s handle.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumer#wakeup")]
    fn wakeup(&self);

    /// Returns a `Clone + Send + Sync` [`ShareConsumerHandle`] whose
    /// [`ShareConsumerHandle::wakeup`] wakes this consumer up from another task
    /// or thread.
    ///
    /// No Java method counterpart: Java's `wakeup()` is callable on the
    /// consumer from any thread, while `poll` holds this consumer as
    /// `&mut self` for as long as it waits.
    // Milestone 18 decision D6 (2026-11-20), as `Consumer::handle`.
    #[doc(alias = "rust-only")]
    fn handle(&self) -> ShareConsumerHandle;
}
