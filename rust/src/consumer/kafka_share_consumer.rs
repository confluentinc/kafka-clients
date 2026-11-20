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

//! The public share consumer type (KIP-932).
//!
//! Translated from `org.apache.kafka.clients.consumer.KafkaShareConsumer`.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::common::metrics::MetricsView;
use crate::common::serialization::Deserializer;
use crate::common::{Error, TopicIdPartition};
use crate::consumer::internals::ShareConsumerImpl;
use crate::consumer::{
    AcknowledgeOptions, AcknowledgeType, AcknowledgementCommitCallback, ConsumerRecord, ConsumerRecords, ShareConsumer,
    ShareConsumerConfig, ShareConsumerHandle,
};

/// A client that consumes records from a Kafka cluster using a share group.
///
/// # Cross-Version Compatibility
///
/// This client can communicate with brokers that are a version that supports share groups.
/// You will receive an `UnsupportedVersionError` when invoking an API that is not available on
/// the running broker version.
///
/// # Share Groups and Topic Subscriptions
///
/// Kafka uses the concept of *share groups* to allow a pool of share consumers to cooperate on
/// the work of consuming and processing records. All share consumer instances sharing the same
/// `group.id` will be part of the same share group.
///
/// Each consumer in a group can dynamically set the list of topics it wants to subscribe to
/// using the [`ShareConsumer::subscribe`] method. Kafka will deliver each message in the
/// subscribed topics to one consumer in the share group. Unlike consumer groups, share groups
/// balance the partitions between all members of the share group permitting multiple
/// consumers to consume from the same partitions. This gives more flexible sharing of records
/// than a consumer group, at the expense of record ordering.
///
/// Membership in a share group is maintained dynamically: if a consumer fails, the partitions
/// assigned to it will be reassigned to other consumers in the same group. Similarly, if a new
/// consumer joins the group, the partition assignment is re-evaluated and partitions can be
/// moved from existing consumers to the new one. This is known as *rebalancing* the group and
/// is discussed in more detail [below](#detecting-consumer-failures). Group rebalancing is also
/// used when new partitions are added to one of the subscribed topics. The group will
/// automatically detect the new partitions through periodic metadata refreshes and assign them
/// to the members of the group.
///
/// Conceptually, you can think of a share group as a single logical subscriber made up of
/// multiple consumers. In fact, in other messaging systems, a share group is roughly
/// equivalent to a *durable shared subscription*. You can have multiple share groups and
/// consumer groups independently consuming from the same topics.
///
/// # Detecting Consumer Failures
///
/// After subscribing to a set of topics, the consumer will automatically join the group when
/// [`ShareConsumer::poll`] is invoked. This method is designed to ensure consumer liveness. As
/// long as you continue to call poll, the consumer will stay in the group and continue to
/// receive records from the partitions it was assigned. Under the covers, the consumer sends
/// periodic heartbeats to the broker. If the consumer crashes or is unable to send heartbeats
/// for the duration of the share group's session time-out, then the consumer will be
/// considered dead and its partitions will be reassigned.
///
/// It is also possible that the consumer could encounter a "livelock" situation where it is
/// continuing to send heartbeats in the background, but no progress is being made. To prevent
/// the consumer from holding onto its partitions indefinitely in this case, we provide a
/// liveness detection mechanism using the `max.poll.interval.ms` setting. If you don't call
/// poll at least as frequently as this, the client will proactively leave the share group. So
/// to stay in the group, you must continue to call poll.
///
/// # Record Delivery and Acknowledgement
///
/// When a consumer in a share-group fetches records using [`ShareConsumer::poll`], it receives
/// available records from any of the topic-partitions that match its subscriptions. Records
/// are acquired for delivery to this consumer with a time-limited *acquisition lock*. While a
/// record is acquired, it is not available for another consumer. By default, the lock duration
/// is 30 seconds, but it can also be controlled using the group
/// `group.share.record.lock.duration.ms` configuration property. The idea is that the lock is
/// automatically released once the lock duration has elapsed, and then the record is available
/// to be given to another consumer. The consumer which holds the lock can deal with it in the
/// following ways:
///
/// - The consumer can acknowledge successful processing of the record
/// - The consumer can release the record, which makes the record available for another
///   delivery attempt
/// - The consumer can reject the record, which indicates that the record is unprocessable and
///   does not make the record available for another delivery attempt
/// - The consumer can renew the record, which indicates that the record is still being
///   processed by this consumer (see [below](#extending-processing-times-using-renewal-acknowledgement))
/// - The consumer can do nothing, in which case the lock is automatically released when the
///   lock duration has elapsed
///
/// The cluster limits the number of records acquired for consumers for each topic-partition in
/// a share group. Once the limit is reached, fetching records will temporarily yield no further
/// records until the number of acquired records reduces, as naturally happens when the locks
/// time out. This limit is controlled by the broker configuration property
/// `group.share.record.lock.partition.limit`. By limiting the duration of the acquisition lock
/// and automatically releasing the locks, the broker ensures delivery progresses even in the
/// presence of consumer failures.
///
/// The consumer can choose to use implicit or explicit acknowledgement of the records it
/// processes by using the consumer `share.acknowledgement.mode` configuration property.
///
/// If the application sets the property to "implicit" or does not set it at all, then the
/// consumer is using *implicit acknowledgement*. In this mode, the application acknowledges
/// delivery by:
///
/// - Calling [`ShareConsumer::poll`] without committing, which also implicitly acknowledges all
///   the delivered records and commits the acknowledgements to Kafka asynchronously. In this
///   case, no error is returned by a failure to commit the acknowledgements.
/// - Calling [`ShareConsumer::commit_sync`] or [`ShareConsumer::commit_async`] which implicitly
///   acknowledges all the delivered records as processed successfully and commits the
///   acknowledgements to Kafka.
/// - Calling [`ShareConsumer::close`] which releases any acquired records without
///   acknowledgement.
///
/// If the application sets the property to "explicit", then the consumer is using *explicit
/// acknowledgement*. The application must acknowledge all records returned from
/// [`ShareConsumer::poll`] using [`ShareConsumer::acknowledge_with_record_type`] before its next
/// call to [`ShareConsumer::poll`]. If the application calls [`ShareConsumer::poll`] without
/// having acknowledged all records, a `LocalIllegalStateError` is returned. The remaining
/// unacknowledged records can still be acknowledged. In this mode, the application
/// acknowledges delivery by:
///
/// - Calling [`ShareConsumer::poll`] after it has acknowledged all records, which commits the
///   acknowledgements to Kafka asynchronously. In this case, no error is returned by a failure
///   to commit the acknowledgements.
/// - Calling [`ShareConsumer::commit_sync`] or [`ShareConsumer::commit_async`] which commits any
///   pending acknowledgements to Kafka.
/// - Calling [`ShareConsumer::close`] which attempts to commit any pending acknowledgements and
///   releases any remaining acquired records.
///
/// The consumer guarantees that the records returned in the `ConsumerRecords` object for a
/// specific topic-partition are in order of increasing offset. For each topic-partition, Kafka
/// guarantees that acknowledgements for the records in a batch are performed atomically. This
/// makes error handling significantly more straightforward because there can be one error code
/// per partition.
///
/// # Share Acquire Mode and Batching
///
/// The share consumer is optimized to deliver records to the consuming application in batches
/// which align with the data when it was written to the topics, and it may also pre-fetch
/// records in readiness for the application. If the application needs precise control over
/// the number of records fetched or the processing time is significant, the application is able
/// to limit the number of records fetched.
///
/// The application chooses between the two modes using the consumer `share.acquire.mode`
/// configuration property.
///
/// - If the application sets the property to `"batch_optimized"` or does not set it at all, the
///   share consumer fetches records based on batch boundaries which may mean that the number of
///   records returned may exceed the `max.poll.records` configuration property. The share
///   consumer may also prefetch records and buffer them temporarily awaiting the application's
///   next call to [`ShareConsumer::poll`].
/// - If the application sets the property to `"record_limit"`, the share consumer fetches no
///   more than `max.poll.records` records at a time and does not prefetch. This is slower but
///   gives the application tighter control on how many records are fetched and when the
///   acquisition locks begin.
///
/// # Usage Examples
///
/// The share consumer APIs offer flexibility to cover a variety of consumption use cases. Here
/// are some examples to demonstrate how to use them.
///
/// ## Acknowledging a batch of records (implicit acknowledgement)
///
/// This example demonstrates implicit acknowledgement using [`ShareConsumer::poll`] to
/// acknowledge the records which were delivered in the previous poll. All the records
/// delivered are implicitly marked as successfully consumed and acknowledged synchronously with
/// Kafka as the consumer fetches more records.
///
/// ```no_run
/// use std::collections::HashMap;
/// use std::time::Duration;
///
/// use confluent_kafka::common::Error;
/// use confluent_kafka::common::serialization::StringDeserializer;
/// use confluent_kafka::consumer::{ConsumerRecord, KafkaShareConsumer, ShareConsumer};
///
/// # fn do_processing(_record: &ConsumerRecord<String, String>) {}
/// # async fn run() -> Result<(), Error> {
/// let props = HashMap::from([
///     ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
///     ("group.id".to_string(), "test".to_string()),
/// ]);
///
/// let mut consumer = KafkaShareConsumer::new(&props, StringDeserializer::new(), StringDeserializer::new())?;
/// consumer.subscribe(&["foo".to_string()]).await?;
/// loop {
///     let records = consumer.poll(Duration::from_millis(1000)).await?;
///     for record in &records {
///         println!("offset = {}, key = {:?}, value = {:?}", record.offset(), record.key(), record.value());
///         do_processing(record);
///     }
/// }
/// # }
/// ```
///
/// Alternatively, you can use [`ShareConsumer::commit_sync`] or [`ShareConsumer::commit_async`]
/// to commit the acknowledgements, but this is slightly less efficient because there is an
/// additional request sent to Kafka.
///
/// ```no_run
/// # use std::collections::HashMap;
/// # use std::time::Duration;
/// # use confluent_kafka::common::Error;
/// # use confluent_kafka::common::serialization::StringDeserializer;
/// # use confluent_kafka::consumer::{ConsumerRecord, KafkaShareConsumer, ShareConsumer};
/// # fn do_processing(_record: &ConsumerRecord<String, String>) {}
/// # async fn run() -> Result<(), Error> {
/// # let props = HashMap::from([
/// #     ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
/// #     ("group.id".to_string(), "test".to_string()),
/// # ]);
/// let mut consumer = KafkaShareConsumer::new(&props, StringDeserializer::new(), StringDeserializer::new())?;
/// consumer.subscribe(&["foo".to_string()]).await?;
/// loop {
///     let records = consumer.poll(Duration::from_millis(1000)).await?;
///     for record in &records {
///         println!("offset = {}, key = {:?}, value = {:?}", record.offset(), record.key(), record.value());
///         do_processing(record);
///     }
///     consumer.commit_sync().await?;
/// }
/// # }
/// ```
///
/// ## Per-record acknowledgement (explicit acknowledgement)
///
/// This example demonstrates using different acknowledge types depending on the outcome of
/// processing the records. Here the `share.acknowledgement.mode` property is set to "explicit"
/// so the consumer must explicitly acknowledge each record.
///
/// ```no_run
/// use std::collections::HashMap;
/// use std::time::Duration;
///
/// use confluent_kafka::common::Error;
/// use confluent_kafka::common::serialization::StringDeserializer;
/// use confluent_kafka::consumer::{AcknowledgeType, ConsumerRecord, KafkaShareConsumer, ShareConsumer};
///
/// # fn do_processing(_record: &ConsumerRecord<String, String>) -> Result<(), Error> { Ok(()) }
/// # async fn run() -> Result<(), Error> {
/// let props = HashMap::from([
///     ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
///     ("group.id".to_string(), "test".to_string()),
///     ("share.acknowledgement.mode".to_string(), "explicit".to_string()),
/// ]);
///
/// let mut consumer = KafkaShareConsumer::new(&props, StringDeserializer::new(), StringDeserializer::new())?;
/// consumer.subscribe(&["foo".to_string()]).await?;
/// loop {
///     let records = consumer.poll(Duration::from_millis(1000)).await?;
///     for record in &records {
///         match do_processing(record) {
///             Ok(()) => consumer.acknowledge_with_record_type(record, AcknowledgeType::Accept)?,
///             Err(_) => consumer.acknowledge_with_record_type(record, AcknowledgeType::Reject)?,
///         }
///     }
///     consumer.commit_sync().await?;
/// }
/// # }
/// ```
///
/// Each record processed is separately acknowledged using a call to
/// [`ShareConsumer::acknowledge_with_record_type`]. The [`AcknowledgeType`] argument indicates
/// whether the record was processed successfully or not. In this case, the bad records are
/// rejected meaning that they're not eligible for further delivery attempts. For a permanent
/// error such as a semantic error, this is appropriate. For a transient error which might not
/// affect a subsequent processing attempt, [`AcknowledgeType::Release`] is more appropriate
/// because the record remains eligible for further delivery attempts.
///
/// The calls to [`ShareConsumer::acknowledge_with_record_type`] are simply updating local
/// information in the consumer. It is only once [`ShareConsumer::commit_sync`] is called that
/// the acknowledgements are committed by sending the new state information to Kafka.
///
/// # Reading Transactional Records
///
/// The way that share groups handle transactional records is controlled by the
/// `group.share.isolation.level` configuration property. In a share group, the isolation level
/// applies to the entire share group, not just individual consumers.
///
/// In `read_uncommitted` isolation level, the share group consumes all non-transactional and
/// transactional records. The consumption is bounded by the high-water mark.
///
/// In `read_committed` isolation level, the share group only consumes non-transactional records
/// and committed transactional records. The set of records which are eligible to become
/// in-flight records are non-transactional records and committed transactional records only.
/// The consumption is bounded by the last stable offset, so an open transaction blocks the
/// progress of the share group with read_committed isolation level.
///
/// # Extending Processing Times using Renewal Acknowledgement
///
/// For situations in which the processing time is longer than the acquisition lock duration,
/// the application can use *renewal acknowledgement*. This allows the application to renew its
/// acquisition lock continuously while the processing of a record is in progress. The
/// application must be using explicit acknowledgement.
///
/// It must call [`ShareConsumer::poll`] regularly and also call
/// [`ShareConsumer::acknowledge_with_record_type`] specifying [`AcknowledgeType::Renew`] on each
/// iteration of its poll loop. When a record is renewed successfully, it is returned again by
/// the [`ShareConsumer::poll`] call. An acknowledgement commit callback can be used to observe
/// the completion of the acknowledgements.
///
/// This example illustrates how an application could use renewal acknowledgement.
///
/// ```no_run
/// use std::collections::{HashMap, HashSet};
/// use std::sync::{Arc, Mutex};
/// use std::time::Duration;
///
/// use async_trait::async_trait;
/// use confluent_kafka::common::serialization::StringDeserializer;
/// use confluent_kafka::common::{Error, TopicIdPartition};
/// use confluent_kafka::consumer::{
///     AcknowledgeType, AcknowledgementCommitCallback, ConsumerRecord, KafkaShareConsumer, ShareConsumer,
/// };
///
/// type Processing = Arc<Mutex<HashMap<i64, ConsumerRecord<String, String>>>>;
///
/// struct CancelOnFailure(Processing);
///
/// #[async_trait]
/// impl AcknowledgementCommitCallback for CancelOnFailure {
///     async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>) {
///         if error.is_some() {
///             let mut processing = self.0.lock().unwrap();
///             for offset in offsets.values().flatten() {
///                 if processing.remove(offset).is_some() {
///                     // Cancel the processing for this offset because the acknowledgement failed
///                 }
///             }
///         }
///     }
/// }
///
/// # fn processing_completed_successfully(_offset: i64) -> bool { true }
/// # fn processing_failed(_offset: i64) -> bool { false }
/// # async fn run() -> Result<(), Error> {
/// let props = HashMap::from([
///     ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
///     ("group.id".to_string(), "test".to_string()),
///     ("share.acknowledgement.mode".to_string(), "explicit".to_string()),
///     ("share.acquire.mode".to_string(), "record_limit".to_string()),
///     ("max.poll.records".to_string(), "1".to_string()),
/// ]);
///
/// let processing: Processing = Arc::default();
///
/// let mut consumer = KafkaShareConsumer::new(&props, StringDeserializer::new(), StringDeserializer::new())?;
/// consumer.set_acknowledgement_commit_callback(Some(Arc::new(CancelOnFailure(Arc::clone(&processing)))));
///
/// consumer.subscribe(&["foo".to_string()]).await?;
/// loop {
///     let records = consumer.poll(Duration::from_millis(1000)).await?;
///
///     // Get the acquisition lock timeout and prepare to wait half that time before renewing again
///     let time_to_wait_ms = consumer.acquisition_lock_timeout_ms().unwrap_or(10000) / 2;
///
///     for record in &records {
///         if processing.lock().unwrap().insert(record.offset(), record.clone()).is_none() {
///             // Start the processing on another task
///         }
///
///         // Wait for the processing to complete for up to time_to_wait_ms milliseconds
/// #       let _ = time_to_wait_ms;
///
///         if processing_completed_successfully(record.offset()) {
///             consumer.acknowledge_with_record_type(record, AcknowledgeType::Accept)?;
///             processing.lock().unwrap().remove(&record.offset());
///         } else if processing_failed(record.offset()) {
///             consumer.acknowledge_with_record_type(record, AcknowledgeType::Reject)?;
///             processing.lock().unwrap().remove(&record.offset());
///         } else {
///             consumer.acknowledge_with_record_type(record, AcknowledgeType::Renew)?;
///         }
///     }
/// }
/// # }
/// ```
///
/// Note that using renewal acknowledgements is intended only for situations where the
/// processing times of the records exceeds the acquisition lock duration. Consumers which use
/// renewal acknowledgements can impact the delivery progress of the share group. If the
/// leadership of the partition for a record being delivered changes or the application's
/// connection to the leader broker is disconnected, the current delivery attempt ends.
///
/// # Multithreaded Processing
///
/// The consumer is NOT thread-safe. It is the responsibility of the user to ensure that
/// multithreaded access is properly synchronized.
///
/// The only exception to this rule is [`ShareConsumer::wakeup`] which can safely be used from
/// an external thread to interrupt an active operation. In this case, a `WakeupError` will be
/// returned from the thread blocking on the operation. This can be used to shut down the
/// consumer from another thread. The following snippet shows the typical pattern:
///
/// ```no_run
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::time::Duration;
///
/// use confluent_kafka::common::Error;
/// use confluent_kafka::consumer::{KafkaShareConsumer, ShareConsumer, ShareConsumerHandle};
///
/// struct KafkaShareConsumerRunner {
///     closed: Arc<AtomicBool>,
///     handle: ShareConsumerHandle,
/// }
///
/// impl KafkaShareConsumerRunner {
///     fn start(mut consumer: KafkaShareConsumer<String, String>) -> (Self, tokio::task::JoinHandle<Result<(), Error>>) {
///         let closed = Arc::new(AtomicBool::new(false));
///         let runner = Self { closed: Arc::clone(&closed), handle: consumer.handle() };
///         let task = tokio::spawn(async move {
///             let result: Result<(), Error> = async {
///                 consumer.subscribe(&["topic".to_string()]).await?;
///                 while !closed.load(Ordering::SeqCst) {
///                     let _records = consumer.poll(Duration::from_millis(1000)).await?;
///                     // Handle new records
///                 }
///                 Ok(())
///             }
///             .await;
///             let result = match result {
///                 // Ignore the error if closing
///                 Err(e) if e.is_wakeup_error() && closed.load(Ordering::SeqCst) => Ok(()),
///                 other => other,
///             };
///             consumer.close().await?;
///             result
///         });
///         (runner, task)
///     }
///
///     // Shutdown hook which can be called from a separate thread
///     fn shutdown(&self) {
///         self.closed.store(true, Ordering::SeqCst);
///         self.handle.wakeup();
///     }
/// }
/// ```
///
/// Then in a separate thread, the consumer can be shutdown by setting the closed flag and
/// waking up the consumer.
///
/// ```
/// # use std::sync::atomic::{AtomicBool, Ordering};
/// # use confluent_kafka::consumer::{MockShareConsumer, ShareConsumer};
/// # let closed = AtomicBool::new(false);
/// # let handle = MockShareConsumer::<String, String>::new().handle();
/// closed.store(true, Ordering::SeqCst);
/// handle.wakeup();
/// ```
///
/// Note that while it is possible to use thread interrupts instead of [`ShareConsumer::wakeup`]
/// to abort a blocking operation (in which case, `InterruptError` will be raised), we
/// discourage their use since they may cause a clean shutdown of the consumer to be aborted.
/// Interrupts are mainly supported for those cases where using [`ShareConsumer::wakeup`] is
/// impossible, such as when a consumer thread is managed by code that is unaware of the Kafka
/// client.
///
/// We have intentionally avoided implementing a particular threading model for processing.
/// Various options for multithreaded processing are possible, of which the most
/// straightforward is to dedicate a thread to each consumer.
///
/// # Translation notes
///
/// The consumer is `Send` but not `Sync`, and every method Java guards takes `&mut self`, so
/// the compiler rejects the unsynchronized access for which Java throws
/// `ConcurrentModificationException`. `wakeup` from another task goes through the
/// `Send + Sync` [`ShareConsumerHandle`] that [`ShareConsumer::handle`] returns. Rust has no
/// thread interrupts: drop the future instead, for example with `tokio::time::timeout`.
///
/// A record acknowledged with [`AcknowledgeType::Renew`] is returned again as a new
/// `ConsumerRecord`, its key and value deserialized again, where Java returns the same object.
///
/// The consumer cannot be shared between threads:
///
/// ```compile_fail,E0277
/// use confluent_kafka::consumer::KafkaShareConsumer;
///
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<KafkaShareConsumer<String, String>>();
/// ```
// Java's class is a thin delegating wrapper: its constructor calls
// `ShareConsumerDelegateCreator.create(...)`, which only constructs a
// `ShareConsumerImpl` and wraps a failure that is not a `KafkaException` as
// "Failed to construct Kafka share consumer". There is one delegate, so it is
// held directly and the creator collapses into the constructor
// (`share-consumer.md` §1; `ShareConsumerImpl::new` keeps the message), as
// `KafkaConsumer` collapses `ConsumerDelegateCreator`.
#[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumer")]
pub struct KafkaShareConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    delegate: ShareConsumerImpl<K, V>,
    /// Zero-sized: keeps the consumer `Send` and makes it `!Sync`, the
    /// type-level form of Java's `acquire()` guard (see the type's docs).
    /// Cross-thread `wakeup` goes through [`ShareConsumerHandle`].
    not_sync: PhantomData<Cell<()>>,
}

impl<K, V> std::fmt::Debug for KafkaShareConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // The delegate's client id and group id, never the configuration.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaShareConsumer")
            .field("delegate", &self.delegate)
            .finish_non_exhaustive()
    }
}

impl<K, V> KafkaShareConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// A consumer is instantiated by providing a set of key-value pairs as configuration, and a
    /// key and a value [`Deserializer`].
    ///
    /// Valid configuration strings are documented at
    /// [`ConsumerConfig`](crate::consumer::ConsumerConfig).
    ///
    /// Note: after creating a `KafkaShareConsumer` you must always
    /// [`close`](ShareConsumer::close) it to avoid resource leaks.
    ///
    /// # Arguments
    ///
    /// - `configs` - The consumer configs
    /// - `key_deserializer` - The deserializer for key that implements `Deserializer`. The
    ///   configure() method won't be called in the consumer when the deserializer is passed in
    ///   directly.
    /// - `value_deserializer` - The deserializer for value that implements `Deserializer`. The
    ///   configure() method won't be called in the consumer when the deserializer is passed in
    ///   directly.
    ///
    /// # Errors
    ///
    /// - `Error::is_kafka_error` if the configuration is invalid, sets a key a share group does
    ///   not support (`"<keys> cannot be set when using a share group."`), or the consumer
    ///   cannot be built (`"Failed to construct Kafka share consumer"`, with the failure as its
    ///   source).
    ///
    /// # Translation notes
    ///
    /// The configuration values are strings, and the deserializers are not
    /// configuration keys.
    // Java's `KafkaShareConsumer(Map, Deserializer, Deserializer)`; the
    // `Properties` overload takes the same map in Rust, and the two overloads
    // without deserializers read them from class-valued keys, which the Rust
    // configuration does not have (CLAUDE.md §2). Java's
    // `ShareConsumerConfig.appendDeserializerToConfig` only records those
    // classes. A `ShareConsumerConfig` failure is not wrapped, as in Java,
    // where it is thrown before the creator runs.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.KafkaShareConsumer#KafkaShareConsumer(Map,Deserializer,Deserializer)"
    )]
    pub fn new(
        configs: &HashMap<String, String>,
        key_deserializer: impl Deserializer<K>,
        value_deserializer: impl Deserializer<V>,
    ) -> Result<Self, Error> {
        let config = ShareConsumerConfig::new(configs)?;
        Ok(Self {
            delegate: ShareConsumerImpl::new(
                config.into_consumer_config(),
                Box::new(key_deserializer),
                Box::new(value_deserializer),
            )?,
            not_sync: PhantomData,
        })
    }

    /// Java's visible-for-testing constructor over a `KafkaClient` (the
    /// `MockClient` of `KafkaShareConsumerTest`): the background task runs the
    /// real share request managers over `client`.
    #[cfg(test)]
    #[expect(clippy::too_many_arguments, reason = "the Java constructor's parameters")]
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumer#KafkaShareConsumer")]
    pub(crate) fn with_client<C: crate::KafkaClient + Send + 'static>(
        client_id: &str,
        group_id: &str,
        config: &ShareConsumerConfig,
        key_deserializer: impl Deserializer<K>,
        value_deserializer: impl Deserializer<V>,
        time: Arc<dyn crate::common::utils::Time>,
        client: C,
        subscriptions: Arc<std::sync::Mutex<crate::consumer::internals::SubscriptionState>>,
        metadata: Arc<crate::consumer::internals::ShareConsumerMetadata>,
    ) -> Result<Self, Error> {
        // Java's `LogContext` parameter: the Rust log context is built from
        // the client id and group id (`ShareConsumerImpl::create_log_context`).
        Ok(Self {
            delegate: ShareConsumerImpl::with_client(
                client_id,
                group_id,
                config,
                Box::new(key_deserializer),
                Box::new(value_deserializer),
                time,
                client,
                subscriptions,
                metadata,
            )?,
            not_sync: PhantomData,
        })
    }

    /// Java's test-only `clientId()`.
    #[cfg(test)]
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumer#clientId")]
    pub(crate) fn client_id(&self) -> &str {
        self.delegate.client_id()
    }

    /// Java's test-only `metricsRegistry()`.
    #[cfg(test)]
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumer#metricsRegistry")]
    pub(crate) fn metrics_registry(&self) -> &Arc<crate::common::metrics::Metrics> {
        self.delegate.metrics_registry()
    }

    /// Java's test-only `kafkaShareConsumerMetrics()`.
    #[cfg(test)]
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumer#kafkaShareConsumerMetrics")]
    pub(crate) fn kafka_share_consumer_metrics(
        &self,
    ) -> &crate::consumer::internals::metrics::KafkaShareConsumerMetrics {
        self.delegate.kafka_share_consumer_metrics()
    }
}

// Every method forwards to the delegate, as Java's `KafkaShareConsumer`
// forwards to `delegate`. The delegate's futures are returned as they are, so
// no call is boxed.
impl<K, V> ShareConsumer<K, V> for KafkaShareConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn subscription(&mut self) -> HashSet<String> {
        self.delegate.subscription()
    }

    fn subscribe(&mut self, topics: &[String]) -> impl Future<Output = Result<(), Error>> + Send {
        self.delegate.subscribe(topics)
    }

    fn unsubscribe(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        self.delegate.unsubscribe()
    }

    fn poll(&mut self, timeout: Duration) -> impl Future<Output = Result<ConsumerRecords<K, V>, Error>> + Send {
        self.delegate.poll(timeout)
    }

    fn acknowledge_with_record(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), Error> {
        self.delegate.acknowledge_with_record(record)
    }

    fn acknowledge_with_record_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        r#type: AcknowledgeType,
    ) -> Result<(), Error> {
        self.delegate.acknowledge_with_record_type(record, r#type)
    }

    fn acknowledge_with_options(&mut self, options: AcknowledgeOptions) -> Result<(), Error> {
        self.delegate.acknowledge_with_options(options)
    }

    fn commit_sync(&mut self) -> impl Future<Output = Result<HashMap<TopicIdPartition, Option<Error>>, Error>> + Send {
        self.delegate.commit_sync()
    }

    fn commit_sync_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicIdPartition, Option<Error>>, Error>> + Send {
        self.delegate.commit_sync_with_timeout(timeout)
    }

    fn commit_async(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        self.delegate.commit_async()
    }

    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {
        self.delegate.set_acknowledgement_commit_callback(callback)
    }

    fn acquisition_lock_timeout_ms(&mut self) -> Option<i32> {
        self.delegate.acquisition_lock_timeout_ms()
    }

    fn metrics(&self) -> MetricsView {
        self.delegate.metrics()
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        self.delegate.close()
    }

    fn close_with_timeout(&mut self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send {
        self.delegate.close_with_timeout(timeout)
    }

    fn wakeup(&self) {
        self.delegate.wakeup()
    }

    fn handle(&self) -> ShareConsumerHandle {
        self.delegate.handle()
    }
}

#[cfg(test)]
mod tests {
    //! Java's `KafkaShareConsumerTest`, which drives a `KafkaShareConsumer`
    //! over `MockClient` to check the share RPCs it sends, plus the R8 tests of
    //! `ShareConsumerHandle` and `acknowledge_with_options` (PLAN §4.1).

    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::{Mutex, MutexGuard, PoisonError};

    use futures_util::FutureExt;
    use tokio::sync::Notify;

    use super::*;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::protocol::Errors;
    use crate::common::record::TimestampType;
    use crate::common::record::internal::MemoryRecords;
    use crate::common::requests::{
        AbstractRequest, ConcreteResponse, FindCoordinatorResponse, MetadataResponse, RequestBuilder, RequestTestUtils,
        ShareAcknowledgeResponse, ShareFetchResponse, ShareGroupHeartbeatResponse,
    };
    use crate::common::serialization::StringDeserializer;
    use crate::common::utils::{MockTime, Time};
    use crate::common::{Node, TopicPartition, Uuid};
    use crate::consumer::AcknowledgeOptionsBuilder;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::{
        AutoOffsetResetStrategy, GroupCoordinatorNode, ShareConsumerMetadata, SubscriptionState,
    };
    use crate::share_acknowledge_response_data::{PartitionData as AckPartitionData, ShareAcknowledgeTopicResponse};
    use crate::share_fetch_response_data::{AcquiredRecords, PartitionData as FetchPartitionData};
    use crate::share_group_heartbeat_response_data::{Assignment, TopicPartitions};
    use crate::test_thread_safety::{is_send, is_sync};
    use crate::{
        ClientRequest, ClientResponse, KafkaClient, LeastLoadedNode, Metadata, MockClient, RequestCompletionHandler,
        RequestMatcher, ShareAcknowledgeResponseData, ShareGroupHeartbeatResponseData,
    };

    const GROUP_ID: &str = "test-group";
    const CLIENT_ID1: &str = "client-id-1";
    const TOPIC1: &str = "test1";
    const BATCH_SIZE: i32 = 10;
    const HEARTBEAT_INTERVAL_MS: i32 = 1000;

    /// How long a test waits for the background task, as Java's
    /// `TestUtils.waitForCondition` default.
    const WAIT: Duration = Duration::from_secs(15);

    /// The responses a matcher prepares while `MockClient::send` runs it.
    type Later = Arc<Mutex<Vec<(RequestMatcher, ConcreteResponse, Node)>>>;

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Java's `new MockClient(time, metadata)`, shared with the test.
    ///
    /// The consumer's background task owns its client, while Java's test
    /// keeps preparing responses on the same `MockClient`, from its own body
    /// and from inside a request matcher (`shareGroupHeartbeat` prepares the
    /// next heartbeat response while matching the current request). This
    /// wrapper shares one `MockClient` between the two: a matcher queues what
    /// it prepares in `later`, which `send` hands to the client after the
    /// client has run the matcher. It also plays Java's
    /// `MockClient.DefaultMockMetadataUpdater`: when the consumer requests a
    /// metadata update, `poll` applies the last metadata given to
    /// [`Self::update_metadata`] again, as `MockClient#poll` does with
    /// `updateWithCurrentMetadata`.
    ///
    /// Java's `MockClient#send` keeps a prepared response whose matcher does
    /// not match the request, and throws: the test then fails on
    /// `futureResponses().isEmpty()`. The Rust `MockClient` removes the
    /// response before it asserts the matcher, on the background task, whose
    /// failure the consumer completes the request with. So every matcher is
    /// wrapped to record a mismatch, and [`Self::future_responses_is_empty`]
    /// answers `false` after one, as Java's list would still hold the response.
    #[derive(Clone)]
    struct SharedMockClient {
        client: Arc<Mutex<MockClient>>,
        later: Later,
        mismatched: Arc<AtomicBool>,
        metadata: Arc<Metadata>,
        last_metadata: Arc<Mutex<Option<MetadataResponse>>>,
        time: Arc<MockTime>,
    }

    impl SharedMockClient {
        fn new(time: &Arc<MockTime>, metadata: &ShareConsumerMetadata) -> Self {
            Self {
                client: Arc::new(Mutex::new(MockClient::new(Arc::clone(time) as Arc<dyn Time>))),
                later: Arc::default(),
                mismatched: Arc::default(),
                metadata: metadata.metadata_arc(),
                last_metadata: Arc::default(),
                time: Arc::clone(time),
            }
        }

        fn client(&self) -> MutexGuard<'_, MockClient> {
            lock(&self.client)
        }

        /// Java's `client.updateMetadata(response)`.
        fn update_metadata(&self, response: MetadataResponse) {
            self.metadata
                .update_with_current_request_version(&response, false, self.time.milliseconds());
            self.client().set_nodes(self.metadata.fetch().nodes().to_vec());
            *lock(&self.last_metadata) = Some(response);
        }

        /// Java's `client.prepareResponseFrom(response, node)`.
        fn prepare_response_from(&self, response: ConcreteResponse, node: &Node) {
            self.client().prepare_response_from(response, node);
        }

        /// Java's `client.prepareResponseFrom(matcher, response, node)`.
        fn prepare_response_from_matcher(&self, matcher: RequestMatcher, response: ConcreteResponse, node: &Node) {
            let matcher = self.recording_mismatch(matcher);
            self.client().prepare_response_from_matcher(matcher, response, node);
        }

        /// Java's `client.futureResponses().isEmpty()`.
        fn future_responses_is_empty(&self) -> bool {
            !self.mismatched.load(Ordering::SeqCst)
                && self.client().num_awaiting_responses() == 0
                && lock(&self.later).is_empty()
        }

        fn recording_mismatch(&self, matcher: RequestMatcher) -> RequestMatcher {
            let mismatched = Arc::clone(&self.mismatched);
            Box::new(move |body| {
                let matches = matcher(body);
                if !matches {
                    mismatched.store(true, Ordering::SeqCst);
                }
                matches
            })
        }

        fn drain_later(&self) {
            let later = std::mem::take(&mut *lock(&self.later));
            for (matcher, response, node) in later {
                self.prepare_response_from_matcher(matcher, response, &node);
            }
        }
    }

    // Every method forwards to the shared `MockClient`. Its async methods
    // never pend, so each is driven to completion under the lock and its
    // result returned ready: no guard is held across an `.await`.
    impl KafkaClient for SharedMockClient {
        fn is_ready(&self, node: &Node, now: i64) -> bool {
            self.client().is_ready(node, now)
        }

        fn ready(&mut self, node: &Node, now: i64) -> impl Future<Output = bool> + Send {
            let ready = self.client().ready(node, now).now_or_never();
            std::future::ready(ready.expect("MockClient::ready does not pend"))
        }

        fn connection_delay(&self, node: &Node, now: i64) -> i64 {
            self.client().connection_delay(node, now)
        }

        fn poll_delay_ms(&self, node: &Node, now: i64) -> i64 {
            self.client().poll_delay_ms(node, now)
        }

        fn connection_failed(&self, node: &Node) -> bool {
            self.client().connection_failed(node)
        }

        fn authentication_error(&self, node: &Node) -> Option<Error> {
            self.client().authentication_error(node)
        }

        fn send(&mut self, request: ClientRequest, now: i64) {
            self.client().send(request, now);
            self.drain_later();
        }

        fn poll(&mut self, timeout: i64, now: i64) -> impl Future<Output = Vec<ClientResponse>> + Send {
            if self.metadata.update_requested()
                && let Some(response) = lock(&self.last_metadata).as_ref()
            {
                self.metadata.update_with_current_request_version(response, false, now);
            }
            let responses = self.client().poll(timeout, now).now_or_never();
            std::future::ready(responses.expect("MockClient::poll does not pend"))
        }

        fn disconnect(&mut self, node_id: &str) -> impl Future<Output = ()> + Send {
            self.client().disconnect(node_id).now_or_never();
            std::future::ready(())
        }

        fn close_connection(&mut self, node_id: &str) -> impl Future<Output = ()> + Send {
            self.client().close_connection(node_id).now_or_never();
            std::future::ready(())
        }

        fn least_loaded_node(&self, now: i64) -> LeastLoadedNode {
            self.client().least_loaded_node(now)
        }

        fn in_flight_request_count(&self) -> i32 {
            self.client().in_flight_request_count()
        }

        fn has_in_flight_requests(&self) -> bool {
            self.client().has_in_flight_requests()
        }

        fn in_flight_request_count_for_node(&self, node_id: &str) -> usize {
            self.client().in_flight_request_count_for_node(node_id)
        }

        fn has_in_flight_requests_for_node(&self, node_id: &str) -> bool {
            self.client().has_in_flight_requests_for_node(node_id)
        }

        fn has_ready_nodes(&self, now: i64) -> bool {
            self.client().has_ready_nodes(now)
        }

        fn wakeup(&self) {
            self.client().wakeup()
        }

        fn wakeup_handle(&self) -> Arc<Notify> {
            self.client().wakeup_handle()
        }

        fn wakeup_notify(&self) -> Arc<Notify> {
            self.client().wakeup_notify()
        }

        fn new_client_request(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
        ) -> ClientRequest {
            self.client()
                .new_client_request(node_id, request_builder, created_time_ms, expect_response)
        }

        fn new_client_request_with_timeout(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
            request_timeout_ms: i32,
            callback: Option<RequestCompletionHandler>,
        ) -> ClientRequest {
            self.client().new_client_request_with_timeout(
                node_id,
                request_builder,
                created_time_ms,
                expect_response,
                request_timeout_ms,
                callback,
            )
        }

        fn initiate_close(&self) {
            self.client().initiate_close()
        }

        fn active(&self) -> bool {
            self.client().active()
        }

        fn close(&mut self) -> impl Future<Output = ()> + Send {
            self.client().close().now_or_never();
            std::future::ready(())
        }
    }

    /// The fields of Java's `KafkaShareConsumerTest` the tests share.
    struct Fixture {
        time: Arc<MockTime>,
        subscription: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ShareConsumerMetadata>,
        client: SharedMockClient,
        topic_id1: Uuid,
        ti1p0: TopicIdPartition,
    }

    impl Fixture {
        /// `new ShareConsumerMetadata(0, 0, Long.MAX_VALUE, false, subscription, ...)`
        /// and `new MockClient(time, metadata)`.
        fn new() -> Self {
            let time = Arc::new(MockTime::new());
            let subscription = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
            let metadata = Arc::new(ShareConsumerMetadata::new(
                0,
                0,
                i64::MAX,
                false,
                Arc::clone(&subscription),
                ClusterResourceListeners::new(),
            ));
            let client = SharedMockClient::new(&time, &metadata);
            let topic_id1 = Uuid::random_uuid();
            let ti1p0 = TopicIdPartition::new(topic_id1, TopicPartition::new(TOPIC1, 0));
            Self { time, subscription, metadata, client, topic_id1, ti1p0 }
        }

        /// Java's `newShareConsumer(clientId, metadata, client)`.
        fn new_share_consumer(&self, client_id: &str) -> KafkaShareConsumer<String, String> {
            self.new_share_consumer_with(client_id, &[])
        }

        fn new_share_consumer_with(
            &self,
            client_id: &str,
            extra: &[(&str, &str)],
        ) -> KafkaShareConsumer<String, String> {
            let config = new_consumer_config(client_id, extra);
            KafkaShareConsumer::with_client(
                client_id,
                GROUP_ID,
                &config,
                StringDeserializer::new(),
                StringDeserializer::new(),
                Arc::clone(&self.time) as Arc<dyn Time>,
                self.client.clone(),
                Arc::clone(&self.subscription),
                Arc::clone(&self.metadata),
            )
            .expect("the consumer is built")
        }

        /// Java's `initMetadata(client, Map.of(topic1, 1))`. It is important
        /// that this response contains the topic ids of the subscribed topics,
        /// otherwise the membership manager asks for another update.
        fn init_metadata(&self) {
            let response = RequestTestUtils::metadata_update_with_ids(
                "kafka-cluster",
                1,
                &HashMap::new(),
                &HashMap::from([(TOPIC1.to_string(), 1)]),
                &|_| None,
                &HashMap::from([(TOPIC1.to_string(), self.topic_id1)]),
            );
            self.client.update_metadata(response);
        }

        /// `metadata.fetch().nodes().get(0)`.
        fn node(&self) -> Node {
            self.metadata.fetch().nodes()[0].clone()
        }

        /// Java's `findCoordinator(client, node)`.
        fn find_coordinator(&self, node: &Node) -> Node {
            self.client.prepare_response_from(
                ConcreteResponse::FindCoordinator(FindCoordinatorResponse::prepare_response(
                    Errors::None,
                    GROUP_ID,
                    node,
                )),
                node,
            );
            GroupCoordinatorNode::new(node.id(), node.host().to_string(), node.port()).expect("a valid node id")
        }

        /// Java's `shareGroupHeartbeatGenerator`: returns whether the member
        /// left, with the member id the first heartbeat carried.
        fn share_group_heartbeat_generator(&self, coordinator: &Node) -> HeartbeatGenerator {
            let generator = HeartbeatGenerator {
                later: Arc::clone(&self.client.later),
                coordinator: coordinator.clone(),
                tip: self.ti1p0.clone(),
                member_id: Arc::default(),
                heartbeats_received: Arc::default(),
                member_left: Arc::default(),
            };
            let (matcher, response) = generator.share_group_heartbeat(0);
            self.client.prepare_response_from_matcher(matcher, response, coordinator);
            generator
        }
    }

    /// The state of Java's `shareGroupHeartbeat` recursion.
    #[derive(Clone)]
    struct HeartbeatGenerator {
        later: Later,
        coordinator: Node,
        tip: TopicIdPartition,
        member_id: Arc<Mutex<Option<Uuid>>>,
        heartbeats_received: Arc<AtomicI32>,
        member_left: Arc<AtomicBool>,
    }

    impl HeartbeatGenerator {
        /// Java's `shareGroupHeartbeat`: the next heartbeat response, epoch
        /// `member_epoch`, and the matcher that prepares the one after it
        /// unless the member leaves.
        fn share_group_heartbeat(&self, member_epoch: i32) -> (RequestMatcher, ConcreteResponse) {
            let response = share_group_heartbeat_response(*lock(&self.member_id), member_epoch, &self.tip);
            let generator = self.clone();
            let matcher: RequestMatcher = Box::new(move |body| {
                let AbstractRequest::ShareGroupHeartbeat(request) = body else {
                    return false;
                };
                if request.data().member_epoch == 0 {
                    *lock(&generator.member_id) =
                        Some(Uuid::from_string(&request.data().member_id).expect("a member id"));
                }
                if request.data().member_epoch == -1 {
                    generator.member_left.store(true, Ordering::SeqCst);
                } else {
                    let (matcher, response) = generator.share_group_heartbeat(member_epoch + 1);
                    lock(&generator.later).push((matcher, response, generator.coordinator.clone()));
                }
                generator.heartbeats_received.fetch_add(1, Ordering::SeqCst);
                true
            });
            (matcher, ConcreteResponse::ShareGroupHeartbeat(response))
        }

        fn member_left(&self) -> bool {
            self.member_left.load(Ordering::SeqCst)
        }
    }

    /// Java's `newConsumerConfig(clientId)`. Java also sets the deserializer
    /// classes, which the Rust configuration has no keys for (CLAUDE.md §2).
    fn new_consumer_config(client_id: &str, extra: &[(&str, &str)]) -> ShareConsumerConfig {
        let mut configs = HashMap::from([
            (ConsumerConfig::CLIENT_ID_CONFIG.to_string(), client_id.to_string()),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), GROUP_ID.to_string()),
            (ConsumerConfig::MAX_POLL_RECORDS_CONFIG.to_string(), BATCH_SIZE.to_string()),
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "localhost:9092".to_string(),
            ),
        ]);
        for (key, value) in extra {
            configs.insert((*key).to_string(), (*value).to_string());
        }
        ShareConsumerConfig::new(&configs).expect("a valid share configuration")
    }

    /// Java's `shareGroupHeartbeatResponse(memberId, memberEpoch, tip)`.
    fn share_group_heartbeat_response(
        member_id: Option<Uuid>,
        member_epoch: i32,
        tip: &TopicIdPartition,
    ) -> ShareGroupHeartbeatResponse {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_member_id(member_id.map(|id| id.to_string()))
            .set_member_epoch(member_epoch)
            .set_heartbeat_interval_ms(HEARTBEAT_INTERVAL_MS);
        if member_epoch != -1 {
            let mut assigned_partitions = TopicPartitions::new();
            assigned_partitions
                .set_topic_id(tip.topic_id())
                .set_partitions(vec![tip.partition()]);
            let mut assignment = Assignment::new();
            assignment.set_topic_partitions(vec![assigned_partitions]);
            data.set_assignment(Some(assignment));
        }
        ShareGroupHeartbeatResponse::new(data)
    }

    /// Java's `shareFetchResponse(tip, count)`.
    fn share_fetch_response(tip: &TopicIdPartition, count: i32) -> ShareFetchResponse {
        let mut builder = MemoryRecords::builder_with_initial_capacity(
            1024,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
        );
        for i in 0..count {
            builder.append_kv(0, Some(format!("key-{i}").as_bytes()), Some(format!("value-{i}").as_bytes()));
        }
        let records = builder.build().into_buffer();
        let mut acquired = AcquiredRecords::new();
        acquired
            .set_first_offset(0)
            .set_last_offset(i64::from(count) - 1)
            .set_delivery_count(1);
        let mut part_data = FetchPartitionData::new();
        part_data
            .set_partition_index(tip.partition())
            .set_records(Some(records))
            .set_acquired_records(vec![acquired]);
        ShareFetchResponse::of(Errors::None, 0, [(tip.clone(), part_data)], &[], 0)
    }

    /// Java's `shareAcknowledgeResponse()`.
    fn share_acknowledge_response() -> ShareAcknowledgeResponse {
        ShareAcknowledgeResponse::new(ShareAcknowledgeResponseData::new())
    }

    /// Java's `shareAcknowledgeResponse(tip)`.
    fn share_acknowledge_response_for(tip: &TopicIdPartition) -> ShareAcknowledgeResponse {
        let mut part_data = AckPartitionData::new();
        part_data
            .set_partition_index(tip.partition())
            .set_error_code(Errors::None.code());
        let mut topic_response = ShareAcknowledgeTopicResponse::new();
        topic_response.set_topic_id(tip.topic_id()).set_partitions(vec![part_data]);
        let mut data = ShareAcknowledgeResponseData::new();
        data.set_responses(vec![topic_response]);
        ShareAcknowledgeResponse::new(data)
    }

    /// Request [A] of Java's tests: a SHARE_FETCH in a new share session,
    /// fetching from `topic_id1`, with no acknowledgements included.
    fn initial_share_fetch_matcher(topic_id1: Uuid) -> RequestMatcher {
        Box::new(move |body| {
            let AbstractRequest::ShareFetch(request) = body else {
                return false;
            };
            let data = request.data();
            let Some(topic) = data.topics.first() else {
                return false;
            };
            data.group_id.as_deref() == Some(GROUP_ID)
                && data.share_session_epoch == 0
                && data.batch_size == BATCH_SIZE
                && topic.topic_id() == topic_id1
                && topic.partitions().len() == 1
                && topic.partitions()[0].acknowledgement_batches.is_empty()
        })
    }

    /// A SHARE_ACKNOWLEDGE which closes the share session.
    fn closing_share_acknowledge_matcher() -> RequestMatcher {
        Box::new(|body| {
            let AbstractRequest::ShareAcknowledge(request) = body else {
                return false;
            };
            let data = request.data();
            data.group_id.as_deref() == Some(GROUP_ID) && data.share_session_epoch == -1 && data.topics.is_empty()
        })
    }

    /// `consumer.poll(timeout)`, failing the test after [`WAIT`] of real time:
    /// the `MockTime` clock does not move, so a `poll` whose fetch is never
    /// answered (a matcher that did not match) would otherwise never return.
    async fn poll_bounded(
        consumer: &mut KafkaShareConsumer<String, String>,
        timeout: Duration,
    ) -> ConsumerRecords<String, String> {
        tokio::time::timeout(WAIT, consumer.poll(timeout))
            .await
            .expect("the poll returns the fetched records")
            .expect("poll")
    }

    /// Polls `condition` until it holds, as Java's `TestUtils.waitForCondition`.
    async fn wait_for(condition: impl Fn() -> bool, what: &str) {
        let start = std::time::Instant::now();
        while !condition() {
            assert!(start.elapsed() < WAIT, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Java `KafkaShareConsumerTest#testVerifyHeartbeats`.
    ///
    /// Java sleeps one heartbeat interval of real time and then expects
    /// exactly two heartbeats; the clock is a `MockTime` that does not move, so
    /// no third heartbeat can be due. Here the test waits until the second
    /// heartbeat arrives (the background task runs concurrently), then for the
    /// same interval, and asserts there were exactly two.
    #[tokio::test(flavor = "multi_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumerTest#testVerifyHeartbeats")]
    async fn test_verify_heartbeats() {
        let f = Fixture::new();
        f.init_metadata();
        let node = f.node();
        let coordinator = f.find_coordinator(&node);

        // The member ID is created by the consumer and then sent in the initial request. The
        // responses need to contain the same member ID.
        let member_id: Arc<Mutex<Option<Uuid>>> = Arc::default();
        let heartbeats_received = Arc::new(AtomicI32::new(0));
        let first = {
            let member_id = Arc::clone(&member_id);
            let heartbeats_received = Arc::clone(&heartbeats_received);
            let later = Arc::clone(&f.client.later);
            let coordinator = coordinator.clone();
            let tip = f.ti1p0.clone();
            let matcher: RequestMatcher = Box::new(move |body| {
                let AbstractRequest::ShareGroupHeartbeat(request) = body else {
                    return false;
                };
                *lock(&member_id) = Some(Uuid::from_string(&request.data().member_id).expect("a member id"));
                let matches = request.data().member_epoch == 0;
                heartbeats_received.fetch_add(1, Ordering::SeqCst);

                let second_member_id = Arc::clone(&member_id);
                let second_heartbeats = Arc::clone(&heartbeats_received);
                let second: RequestMatcher = Box::new(move |body2| {
                    let AbstractRequest::ShareGroupHeartbeat(request2) = body2 else {
                        return false;
                    };
                    let expected = lock(&second_member_id).expect("set by the first heartbeat").to_string();
                    let matches2 = request2.data().member_id == expected && request2.data().member_epoch == 1;
                    second_heartbeats.fetch_add(1, Ordering::SeqCst);
                    matches2
                });
                let response = share_group_heartbeat_response(*lock(&member_id), 2, &tip);
                lock(&later).push((second, ConcreteResponse::ShareGroupHeartbeat(response), coordinator.clone()));
                matches
            });
            matcher
        };
        f.client.prepare_response_from_matcher(
            first,
            ConcreteResponse::ShareGroupHeartbeat(share_group_heartbeat_response(*lock(&member_id), 1, &f.ti1p0)),
            &coordinator,
        );

        let mut consumer = f.new_share_consumer(CLIENT_ID1);
        consumer.subscribe(&[TOPIC1.to_string()]).await.expect("subscribed");
        consumer.poll(Duration::ZERO).await.expect("poll");

        wait_for(|| heartbeats_received.load(Ordering::SeqCst) >= 2, "two heartbeats").await;
        tokio::time::sleep(Duration::from_millis(u64::try_from(HEARTBEAT_INTERVAL_MS).unwrap())).await;

        assert_eq!(2, heartbeats_received.load(Ordering::SeqCst));
        assert!(f.client.future_responses_is_empty());

        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// Java `KafkaShareConsumerTest#testVerifyFetchAndCommitSyncImplicit`.
    #[tokio::test(flavor = "multi_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumerTest#testVerifyFetchAndCommitSyncImplicit")]
    async fn test_verify_fetch_and_commit_sync_implicit() {
        let f = Fixture::new();
        f.init_metadata();
        let node = f.node();
        let coordinator = f.find_coordinator(&node);
        let heartbeats = f.share_group_heartbeat_generator(&coordinator);

        // [A] A SHARE_FETCH in a new share session, fetching from topic topicId1, with no
        // acknowledgements included. The response includes 2 records which are acquired.
        f.client.prepare_response_from_matcher(
            initial_share_fetch_matcher(f.topic_id1),
            ConcreteResponse::ShareFetch(share_fetch_response(&f.ti1p0, 2)),
            &node,
        );

        // [B] A SHARE_ACKNOWLEDGE for the two records acquired in [A].
        f.client.prepare_response_from_matcher(
            Box::new(|body| {
                let AbstractRequest::ShareAcknowledge(request) = body else {
                    return false;
                };
                let data = request.data();
                let Some(batch) = data
                    .topics
                    .first()
                    .and_then(|topic| topic.partitions.first())
                    .and_then(|partition| partition.acknowledgement_batches.first())
                else {
                    return false;
                };
                data.group_id.as_deref() == Some(GROUP_ID)
                    && data.share_session_epoch == 1
                    && batch.first_offset == 0
                    && batch.last_offset == 1
                    && batch.acknowledge_types == [1]
            }),
            ConcreteResponse::ShareAcknowledge(share_acknowledge_response_for(&f.ti1p0)),
            &node,
        );

        // [C] A SHARE_ACKNOWLEDGE which closes the share session.
        f.client.prepare_response_from_matcher(
            closing_share_acknowledge_matcher(),
            ConcreteResponse::ShareAcknowledge(share_acknowledge_response()),
            &node,
        );

        let mut consumer = f.new_share_consumer(CLIENT_ID1);
        consumer.subscribe(&[TOPIC1.to_string()]).await.expect("subscribed");

        // This will be a SHARE_GROUP_HEARTBEAT to establish the membership and then a SHARE_FETCH [A]
        poll_bounded(&mut consumer, Duration::from_millis(5000)).await;

        // This will be a SHARE_ACKNOWLEDGE [B]
        consumer.commit_sync().await.expect("commit");

        // This will be a SHARE_ACKNOWLEDGE [C] and a final SHARE_GROUP_HEARTBEAT to leave the group
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");

        assert!(heartbeats.member_left());
        assert!(f.client.future_responses_is_empty());
    }

    /// Java `KafkaShareConsumerTest#testVerifyFetchAndCloseImplicit`.
    #[tokio::test(flavor = "multi_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaShareConsumerTest#testVerifyFetchAndCloseImplicit")]
    async fn test_verify_fetch_and_close_implicit() {
        let f = Fixture::new();
        f.init_metadata();
        let node = f.node();
        let coordinator = f.find_coordinator(&node);
        let heartbeats = f.share_group_heartbeat_generator(&coordinator);

        // [A] A SHARE_FETCH in a new share session, fetching from topic topicId1, with no
        // acknowledgements included. The response includes 2 records which are acquired.
        f.client.prepare_response_from_matcher(
            initial_share_fetch_matcher(f.topic_id1),
            ConcreteResponse::ShareFetch(share_fetch_response(&f.ti1p0, 2)),
            &node,
        );

        // [B] A SHARE_ACKNOWLEDGE which closes the share session. Because this is implicit
        // acknowledgement, the acquired records are released by the broker when the share session
        // is closed.
        f.client.prepare_response_from_matcher(
            closing_share_acknowledge_matcher(),
            ConcreteResponse::ShareAcknowledge(share_acknowledge_response()),
            &node,
        );

        let mut consumer = f.new_share_consumer(CLIENT_ID1);
        consumer.subscribe(&[TOPIC1.to_string()]).await.expect("subscribed");

        // This will be a SHARE_GROUP_HEARTBEAT to establish the membership and then a SHARE_FETCH [A]
        poll_bounded(&mut consumer, Duration::from_millis(5000)).await;

        // This will be a SHARE_ACKNOWLEDGE [B] and a final SHARE_GROUP_HEARTBEAT to leave the group
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");

        assert!(heartbeats.member_left());
        assert!(f.client.future_responses_is_empty());
    }

    /// Java `KafkaShareConsumerTest#testShareConsumerBootstrapResolutionExceptionPropagatedToPoll`
    /// (KIP-909): with a positive `bootstrap.resolve.timeout.ms` and an
    /// unresolvable bootstrap host, `poll` fails with the bootstrap
    /// resolution error once the budget runs out, and so does every later call.
    /// Java asserts that the message mentions the DNS failure; the exact text
    /// is asserted here.
    #[tokio::test(flavor = "multi_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.KafkaShareConsumerTest#testShareConsumerBootstrapResolutionExceptionPropagatedToPoll"
    )]
    async fn test_share_consumer_bootstrap_resolution_error_propagated_to_poll() {
        // Use an invalid hostname that will fail DNS resolution (using RFC 6761 reserved .invalid TLD)
        let configs = HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "unresolvable.invalid:9092".to_string(),
            ),
            // Set a short bootstrap timeout so the test doesn't take too long
            (
                ConsumerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
                "3000".to_string(),
            ),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "test-share-group".to_string()),
        ]);
        let mut consumer =
            KafkaShareConsumer::<String, String>::new(&configs, StringDeserializer::new(), StringDeserializer::new())
                .expect("a positive timeout defers resolution, so construction succeeds");
        consumer.subscribe(&["test-topic".to_string()]).await.expect("subscribed");

        // Poll continuously until we get the BootstrapResolutionError. The error should be
        // returned after bootstrap.resolve.timeout.ms expires.
        let expected = "Failed to resolve bootstrap servers after 3000ms. \
                        Please check your bootstrap.servers configuration and DNS settings.";
        let start = std::time::Instant::now();
        let max_wait_time = Duration::from_millis(15000); // 15 seconds max to prevent test hanging
        let error = loop {
            assert!(
                start.elapsed() < max_wait_time,
                "Expected BootstrapResolutionError to be returned within {}ms",
                max_wait_time.as_millis()
            );
            if let Err(error) = consumer.poll(Duration::from_millis(100)).await {
                break error;
            }
        };
        assert!(error.is_bootstrap_resolution_error(), "{error:?}");
        assert_eq!(expected, error.message());

        // After the first failure, any further API call must also return it. This guards against
        // accidentally clearing the bootstrap error from the metadata layer.
        let error = consumer.poll(Duration::from_millis(100)).await.expect_err("still failing");
        assert!(error.is_bootstrap_resolution_error(), "{error:?}");
        assert_eq!(expected, error.message());

        consumer.close().await.expect("closed");
    }

    /// Java `KafkaShareConsumerTest#testShareConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero`:
    /// the default `bootstrap.resolve.timeout.ms=0` resolves DNS synchronously
    /// in the constructor; any failure surfaces as a configuration error
    /// wrapped in a `KafkaError` by the constructor, so no share consumer
    /// instance is created.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.KafkaShareConsumerTest#testShareConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero"
    )]
    fn test_share_consumer_constructor_fails_with_config_error_on_unresolvable_bootstrap_when_timeout_zero() {
        let configs = HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "unresolvable.invalid:9092".to_string(),
            ),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "test-share-group".to_string()),
        ]);
        let error =
            KafkaShareConsumer::<String, String>::new(&configs, StringDeserializer::new(), StringDeserializer::new())
                .expect_err("the bootstrap server cannot be resolved");
        assert!(matches!(error, Error::KafkaError(_)), "{error:?}");
        assert_eq!("Failed to construct Kafka share consumer", error.message());
        let cause = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<Error>())
            .expect("the construction failure is the source");
        assert!(matches!(cause, Error::Config(_)), "{cause:?}");
    }

    // ── Rust-only (translation-checklist R8, PLAN §4.1) ────────────────────

    /// A subscribed consumer over a `MockClient` that answers nothing, so a
    /// `poll` waits for its whole timeout.
    async fn new_subscribed_consumer(f: &Fixture) -> KafkaShareConsumer<String, String> {
        let mut consumer = f.new_share_consumer(CLIENT_ID1);
        consumer.subscribe(&[TOPIC1.to_string()]).await.expect("subscribed");
        consumer
    }

    /// Wakes `handle` from another task once `delay` has passed.
    fn wakeup_later(handle: ShareConsumerHandle, delay: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            handle.wakeup();
        })
    }

    /// The consumer is `Send + !Sync`, as Java's single-threaded consumer.
    #[test]
    fn kafka_share_consumer_is_send_and_not_sync() {
        assert!(is_send!(KafkaShareConsumer<String, String>));
        assert!(!is_sync!(KafkaShareConsumer<String, String>));
    }

    /// R8 H2: a `wakeup` from another task ends a `poll` that is waiting.
    #[tokio::test(flavor = "multi_thread")]
    async fn handle_wakeup_from_another_task_ends_a_waiting_poll() {
        let f = Fixture::new();
        let mut consumer = new_subscribed_consumer(&f).await;
        let waker = wakeup_later(consumer.handle(), Duration::from_millis(100));
        let start = std::time::Instant::now();
        let error = tokio::time::timeout(WAIT, consumer.poll(Duration::from_secs(60)))
            .await
            .expect("the wakeup ends the poll")
            .expect_err("woken up");
        assert!(error.is_wakeup_error(), "{error:?}");
        assert!(start.elapsed() < WAIT);
        waker.await.expect("join");
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// R8 H3: with no call waiting, a `wakeup` fails the next `poll` only.
    #[tokio::test(flavor = "multi_thread")]
    async fn handle_wakeup_before_poll_fails_only_the_next_poll() {
        let f = Fixture::new();
        let mut consumer = new_subscribed_consumer(&f).await;
        consumer.handle().wakeup();
        let error = consumer.poll(Duration::ZERO).await.expect_err("woken up");
        assert!(error.is_wakeup_error(), "{error:?}");
        assert_eq!(0, consumer.poll(Duration::ZERO).await.expect("not woken up again").count());
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// R8 H4: a `wakeup` from another task ends a `commit_sync` waiting for
    /// its SHARE_ACKNOWLEDGE response.
    #[tokio::test(flavor = "multi_thread")]
    async fn handle_wakeup_from_another_task_ends_a_waiting_commit_sync() {
        let f = Fixture::new();
        f.init_metadata();
        let node = f.node();
        let coordinator = f.find_coordinator(&node);
        let _heartbeats = f.share_group_heartbeat_generator(&coordinator);
        f.client.prepare_response_from_matcher(
            initial_share_fetch_matcher(f.topic_id1),
            ConcreteResponse::ShareFetch(share_fetch_response(&f.ti1p0, 2)),
            &node,
        );
        let mut consumer = new_subscribed_consumer(&f).await;
        let records = poll_bounded(&mut consumer, Duration::from_millis(5000)).await;
        assert_eq!(2, records.count());

        // No SHARE_ACKNOWLEDGE response is prepared, and the clock does not
        // move, so the commit waits until it is woken up.
        let waker = wakeup_later(consumer.handle(), Duration::from_millis(100));
        let error = tokio::time::timeout(WAIT, consumer.commit_sync())
            .await
            .expect("the wakeup ends the commit")
            .expect_err("woken up");
        assert!(error.is_wakeup_error(), "{error:?}");
        waker.await.expect("join");
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// R8 H5: a clone of the handle wakes the same consumer.
    #[tokio::test(flavor = "multi_thread")]
    async fn handle_clones_wake_the_same_consumer() {
        let f = Fixture::new();
        let mut consumer = new_subscribed_consumer(&f).await;
        let handle = consumer.handle();
        let clone = handle.clone();
        drop(handle);
        clone.wakeup();
        let error = consumer.poll(Duration::ZERO).await.expect_err("woken up");
        assert!(error.is_wakeup_error(), "{error:?}");
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// R8 H6: the handle outlives `close` and the consumer; `wakeup` then does
    /// nothing (Java's `wakeup` does not check `closed`).
    #[tokio::test(flavor = "multi_thread")]
    async fn handle_wakeup_after_close_does_not_panic() {
        let f = Fixture::new();
        let mut consumer = new_subscribed_consumer(&f).await;
        let handle = consumer.handle();
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
        handle.wakeup();
        consumer.wakeup();
        drop(consumer);
        handle.wakeup();
    }

    /// R8 O3: `acknowledge_with_options` keeps Java's misuse errors.
    #[tokio::test(flavor = "multi_thread")]
    async fn acknowledge_with_options_reports_java_misuse_errors() {
        let options = || {
            AcknowledgeOptionsBuilder::new()
                .set_topic(TOPIC1)
                .set_partition(0)
                .set_offset(5)
                .set_type(AcknowledgeType::Reject)
                .build()
                .expect("every parameter is set")
        };

        let f = Fixture::new();
        let mut implicit = f.new_share_consumer(CLIENT_ID1);
        let error = implicit.acknowledge_with_options(options()).expect_err("implicit mode");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!("Implicit acknowledgement of delivery is being used.", error.message());
        implicit.close_with_timeout(Duration::ZERO).await.expect("closed");

        let f = Fixture::new();
        let mut explicit =
            f.new_share_consumer_with(CLIENT_ID1, &[(ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit")]);
        let error = explicit.acknowledge_with_options(options()).expect_err("no batch failed");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!("The record cannot be acknowledged.", error.message());
        explicit.close_with_timeout(Duration::ZERO).await.expect("closed");
        let error = explicit.acknowledge_with_options(options()).expect_err("closed");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!("This consumer has already been closed.", error.message());
    }

    /// Java's test-only accessors reach the delegate.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_accessors_reach_the_delegate() {
        use crate::common::Metric;
        let f = Fixture::new();
        let mut consumer = f.new_share_consumer(CLIENT_ID1);
        assert_eq!(CLIENT_ID1, consumer.client_id());
        let time = f.time.milliseconds();
        consumer.kafka_share_consumer_metrics().record_poll_start(time);
        consumer.kafka_share_consumer_metrics().record_poll_end(time);
        let name = consumer
            .metrics_registry()
            .metric_name("poll-idle-ratio-avg", "consumer-share-metrics");
        let value = consumer
            .metrics()
            .get(&name)
            .and_then(|metric| metric.metric_value().as_double());
        assert_eq!(Some(0.0), value);
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }
}
