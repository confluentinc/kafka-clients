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

//! `ShareFetchCollector` — turns the [`ShareCompletedFetch`]es in a
//! [`ShareFetchBuffer`] into a [`ShareFetch`].
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareFetchCollector`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use log::{debug, trace, warn};

use crate::common::Error;
use crate::common::protocol::Errors;
use crate::consumer::internals::ConsumerShareFetchError;
use crate::consumer::internals::Deserializers;
use crate::consumer::internals::FetchUtils;
use crate::consumer::internals::ShareCompletedFetch;
use crate::consumer::internals::ShareConsumerMetadata;
use crate::consumer::internals::ShareFetch;
use crate::consumer::internals::ShareFetchBuffer;
use crate::consumer::internals::ShareFetchConfig;
use crate::consumer::internals::SubscriptionState;

/// What [`ShareFetchCollector::collect`] returns: the outer `Err` is any
/// failure Java's `collect` lets escape (a `KafkaException` when nothing was
/// collected, or another exception); the inner `Err` is Java's
/// `ShareFetchException`, which `ShareConsumerImpl#poll` catches separately.
pub(crate) type ShareFetchCollectResult<K, V> = Result<Result<ShareFetch<K, V>, ConsumerShareFetchError<K, V>>, Error>;

/// The test override of `ShareFetchCollector#initialize` (see the field).
#[cfg(test)]
type ForceInitializeError = Box<dyn Fn(&ShareCompletedFetch) -> Option<Error> + Send + Sync>;

/// Operates at the record-batch level, as that is what is stored in the
/// [`ShareFetchBuffer`]. Each record in the batch is converted to a
/// `ConsumerRecord` and added to the returned [`ShareFetch`].
///
/// Corresponds to `org.apache.kafka.clients.consumer.internals.ShareFetchCollector`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchCollector")]
pub(crate) struct ShareFetchCollector<K: 'static, V: 'static> {
    metadata: Arc<ShareConsumerMetadata>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    share_fetch_config: ShareFetchConfig,
    /// Shared with the consumer, as Java shares the object.
    deserializers: Arc<Deserializers<K, V>>,
    /// Test-only stand-in for the Java tests' anonymous subclasses overriding
    /// `initialize` (`ShareFetchCollectorTest#testErrorInInitialize`,
    /// `#testErrorInInitializeDoesNotLoseRecordsFromEarlierPartitions`):
    /// consulted before `initialize`, it fails it with the error it returns
    /// (translation-checklist R3 allows a faithful port of a test override).
    #[cfg(test)]
    force_initialize_error: Option<ForceInitializeError>,
}

impl<K, V> ShareFetchCollector<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// A collector. Java's `LogContext` is dropped (the `log` crate is used).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchCollector#ShareFetchCollector")]
    pub(crate) fn new(
        metadata: Arc<ShareConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        share_fetch_config: ShareFetchConfig,
        deserializers: Arc<Deserializers<K, V>>,
    ) -> Self {
        Self {
            metadata,
            subscriptions,
            share_fetch_config,
            deserializers,
            #[cfg(test)]
            force_initialize_error: None,
        }
    }

    /// Return the fetched records.
    ///
    /// Reads up to `max.poll.records` records from the buffer's fetches. When a
    /// fetch fails, the records already collected from other partitions are
    /// returned rather than lost, otherwise they could never be acknowledged
    /// and the completed fetches which delivered them would never be drained;
    /// the failing fetch stays at the head of the queue and is handled again
    /// on the next call.
    ///
    /// # Errors
    ///
    /// When nothing was collected: the inner `Err`, a
    /// [`ConsumerShareFetchError`], when a batch failed to decode (its cause is
    /// a `RecordDeserializationError` or a `CorruptRecordError`); the outer
    /// `Err` for anything else, such as a `TopicAuthorizationError` for a
    /// partition the consumer may not read.
    ///
    /// # Cancellation
    ///
    /// Dropping the future while a deserializer is pending puts the fetch being
    /// read back as the next in line; the records decoded so far in the dropped
    /// call are lost with it. They were acquired, so the broker releases them
    /// when their acquisition lock times out.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchCollector#collect")]
    pub(crate) async fn collect(&self, fetch_buffer: &ShareFetchBuffer) -> ShareFetchCollectResult<K, V> {
        let mut fetch = ShareFetch::empty();
        match self.collect_into(fetch_buffer, &mut fetch).await {
            Ok(None) => Ok(Ok(fetch)),
            // Records already collected from other partitions must be returned rather than lost, otherwise
            // they can never be acknowledged and the completed fetches which delivered them will never be drained.
            // The failing fetch is still at the head of the queue and will be handled again on the next poll.
            Ok(Some(cause)) => {
                if fetch.is_empty() {
                    Ok(Err(ConsumerShareFetchError::new(fetch, cause)))
                } else {
                    Ok(Ok(fetch))
                }
            },
            Err(e) if e.is_kafka_error() && !fetch.is_empty() => Ok(Ok(fetch)),
            Err(e) => Err(e),
        }
    }

    /// The body of Java's `try` block in `collect`: `Ok(Some(cause))` where
    /// Java throws `new ShareFetchException(fetch, cause)`.
    async fn collect_into(
        &self,
        fetch_buffer: &ShareFetchBuffer,
        fetch: &mut ShareFetch<K, V>,
    ) -> Result<Option<Error>, Error> {
        let mut records_remaining = self.share_fetch_config.max_poll_records;
        while records_remaining > 0 {
            let next_in_line_ready = fetch_buffer.next_in_line_fetch(|cf| cf.is_some_and(|cf| !cf.is_consumed()));
            if !next_in_line_ready {
                // Java peeks, initializes and sets the fetch next-in-line before
                // it polls it; it is polled first here, staying buffered.
                let Some(mut completed_fetch) = fetch_buffer.poll_checked_out() else {
                    break;
                };
                if !completed_fetch.is_initialized() {
                    match self.initialize(&mut completed_fetch) {
                        Ok(true) => fetch_buffer.set_next_in_line_fetch(Some(completed_fetch)),
                        Ok(false) => fetch_buffer.set_next_in_line_fetch(None),
                        Err(e) => {
                            // Java polls the failing fetch off the queue only when nothing was collected.
                            if fetch.is_empty() {
                                fetch_buffer.clear_checked_out();
                            } else {
                                fetch_buffer.push_front(completed_fetch);
                            }
                            return Err(e);
                        },
                    }
                } else {
                    fetch_buffer.set_next_in_line_fetch(Some(completed_fetch));
                }
            } else {
                let mut next_in_line = CheckedOut::new(fetch_buffer);
                let Some(completed_fetch) = next_in_line.fetch.as_mut() else {
                    break;
                };
                let tp = completed_fetch.partition.clone();
                let batch = completed_fetch
                    .fetch_records(&self.deserializers, records_remaining, self.share_fetch_config.check_crcs)
                    .await?;
                if batch.is_empty() {
                    completed_fetch.drain();
                }
                drop(next_in_line);

                records_remaining -= i32::try_from(batch.num_records()).unwrap_or(i32::MAX);
                let error = batch.error().map(|error| error.cause().clone());
                let has_cached_error = batch.has_cached_error();
                fetch.add(tp, batch);

                if let Some(cause) = error {
                    return Ok(Some(cause));
                } else if has_cached_error {
                    break;
                }
            }
        }
        Ok(None)
    }

    /// Initialize a ShareCompletedFetch object. Returns whether it is to be
    /// read: Java returns the fetch, or `null` for a partition error it
    /// handles here.
    ///
    /// # Errors
    ///
    /// `TopicAuthorizationError` for `TOPIC_AUTHORIZATION_FAILED`, and a
    /// `KafkaError` for `CORRUPT_MESSAGE` or an unexpected error code.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchCollector#initialize")]
    fn initialize(&self, completed_fetch: &mut ShareCompletedFetch) -> Result<bool, Error> {
        #[cfg(test)]
        if let Some(force) = &self.force_initialize_error
            && let Some(error) = force(completed_fetch)
        {
            return Err(error);
        }
        let error = Errors::for_code(completed_fetch.partition_data.error_code);
        if error == Errors::None {
            self.handle_initialize_success(completed_fetch);
            Ok(true)
        } else {
            self.handle_initialize_errors(completed_fetch, error)?;
            Ok(false)
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchCollector#handleInitializeSuccess")]
    fn handle_initialize_success(&self, completed_fetch: &mut ShareCompletedFetch) {
        trace!(
            "Preparing to read {} bytes of data for partition {}",
            completed_fetch.records_size(),
            completed_fetch.partition.topic_partition()
        );
        completed_fetch.set_initialized();
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchCollector#handleInitializeErrors")]
    fn handle_initialize_errors(&self, completed_fetch: &ShareCompletedFetch, error: Errors) -> Result<(), Error> {
        let tp = &completed_fetch.partition;
        match error {
            Errors::NotLeaderOrFollower
            | Errors::ReplicaNotAvailable
            | Errors::KafkaStorageError
            | Errors::FencedLeaderEpoch
            | Errors::OffsetNotAvailable => {
                debug!("Error in fetch for partition {}: {}", tp, error.error_name().unwrap_or("null"));
                self.request_metadata_update(tp);
            },
            Errors::UnknownTopicOrPartition => {
                warn!("Received unknown topic or partition error in fetch for partition {tp}.");
                self.request_metadata_update(tp);
            },
            Errors::UnknownTopicId => {
                warn!("Received unknown topic ID error in fetch for partition {tp}.");
                self.request_metadata_update(tp);
            },
            Errors::InconsistentTopicId => {
                warn!("Received inconsistent topic ID error in fetch for partition {tp}.");
                self.request_metadata_update(tp);
            },
            Errors::TopicAuthorizationFailed => {
                // Log the actual partition and not just the topic to help with ACL propagation issues in large clusters
                warn!("Not authorized to read from partition {}.", tp.topic_partition());
                return Err(Error::topic_authorization(HashSet::from([tp.topic().to_string()])));
            },
            Errors::UnknownLeaderEpoch => {
                debug!("Received unknown leader epoch error in fetch for partition {tp}.");
            },
            Errors::UnknownServerError => {
                warn!("Unknown server error while fetching topic-partition {}.", tp.topic_partition());
            },
            Errors::CorruptMessage => {
                return Err(Error::kafka_message(format!(
                    "Encountered corrupt message when fetching topic-partition {}",
                    tp.topic_partition()
                )));
            },
            _ => {
                return Err(Error::kafka_message(format!(
                    "Unexpected error code {} while fetching from topic-partition {}",
                    error.code(),
                    tp.topic_partition()
                )));
            },
        }
        Ok(())
    }

    /// `FetchUtils.requestMetadataUpdate(metadata, subscriptions, tp.topicPartition())`.
    fn request_metadata_update(&self, tp: &crate::common::TopicIdPartition) {
        FetchUtils::request_metadata_update(&self.metadata, &self.subscriptions, tp.topic_partition());
    }

    /// Installs the test override of `initialize` (see the field).
    #[cfg(test)]
    fn set_force_initialize_error(
        &mut self,
        force: impl Fn(&ShareCompletedFetch) -> Option<Error> + Send + Sync + 'static,
    ) {
        self.force_initialize_error = Some(Box::new(force));
    }
}

/// The next-in-line fetch, taken out of the buffer to be read across the
/// deserializers' `.await`s, and set back when this is dropped, whether the
/// read completed or its future was dropped (translation-checklist R4 item 8).
///
/// Rust-only (DoD #7): Java's collector reads the fetch in place.
struct CheckedOut<'a> {
    buffer: &'a ShareFetchBuffer,
    fetch: Option<ShareCompletedFetch>,
}

impl<'a> CheckedOut<'a> {
    fn new(buffer: &'a ShareFetchBuffer) -> Self {
        Self { buffer, fetch: buffer.take_next_in_line_fetch() }
    }
}

impl Drop for CheckedOut<'_> {
    fn drop(&mut self) {
        if let Some(fetch) = self.fetch.take() {
            self.buffer.set_next_in_line_fetch(Some(fetch));
        }
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `ShareFetchCollectorTest` (9 tests and 2 parameterised,
    //! run over their exact sources). Java's tests keep references to the
    //! completed fetches they add to the buffer; here the buffer owns them, so
    //! the tests read the next-in-line fetch through
    //! [`ShareFetchBuffer::next_in_line_fetch`].

    use std::collections::HashMap;
    use std::mem::discriminant;

    use futures_util::FutureExt;

    use super::*;
    use crate::common::TopicIdPartition;
    use crate::common::Uuid;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::metrics::Metrics;
    use crate::common::protocol::ApiKeys;
    use crate::common::utils::internals::BufferSupplier;
    use crate::consumer::internals::share_completed_fetch::tests::{acquired_records, partition_data};
    use crate::consumer::internals::{
        AutoOffsetResetStrategy, ShareFetchMetricsAggregator, ShareFetchMetricsManager, ShareFetchMetricsRegistry,
    };
    use crate::consumer::{AcknowledgeType, ConsumerConfig, ConsumerRecord};
    use crate::share_fetch_response_data::AcquiredRecords;

    const DEFAULT_RECORD_COUNT: i32 = 10;
    const DEFAULT_MAX_POLL_RECORDS: i32 = ConsumerConfig::DEFAULT_MAX_POLL_RECORDS;
    const DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS: Option<i32> = Some(30000);

    /// Java's `buildDependencies()`.
    struct Fixture {
        topic_a_partition0: TopicIdPartition,
        topic_a_partition1: TopicIdPartition,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        deserializers: Arc<Deserializers<String, String>>,
        fetch_collector: ShareFetchCollector<String, String>,
        fetch_buffer: ShareFetchBuffer,
        aggregator: Arc<ShareFetchMetricsAggregator>,
    }

    /// Java's `ShareCompletedFetchBuilder`.
    struct ShareCompletedFetchBuilder {
        record_count: i32,
        base_offset: i64,
        error: Option<Errors>,
        partition: TopicIdPartition,
    }

    impl Fixture {
        fn new() -> Self {
            let topic_a_partition0 = TopicIdPartition::with_partition_topic(Uuid::random_uuid(), 0, "topic-a");
            let topic_a_partition1 =
                TopicIdPartition::with_partition_topic(topic_a_partition0.topic_id(), 1, "topic-a");
            let props: HashMap<String, String> = [
                (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG, "localhost:9092".to_string()),
                (ConsumerConfig::MAX_POLL_RECORDS_CONFIG, DEFAULT_MAX_POLL_RECORDS.to_string()),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
            let config = ConsumerConfig::new(&props).expect("a valid config");
            let deserializers = Arc::new(Deserializers::new(
                Box::new(crate::common::serialization::StringDeserializer::new()),
                Box::new(crate::common::serialization::StringDeserializer::new()),
            ));
            let manager = Arc::new(ShareFetchMetricsManager::new(
                Arc::new(Metrics::new()),
                &ShareFetchMetricsRegistry::new(),
            ));
            let aggregator = Arc::new(ShareFetchMetricsAggregator::new(
                manager,
                HashSet::from([topic_a_partition0.topic_partition().clone()]),
            ));
            let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
            let share_fetch_config = ShareFetchConfig::with_consumer_config(&config).expect("a valid share config");
            let metadata = Arc::new(ShareConsumerMetadata::new(
                0,
                1000,
                10000,
                false,
                Arc::clone(&subscriptions),
                ClusterResourceListeners::new(),
            ));
            let fetch_collector = ShareFetchCollector::new(
                metadata,
                Arc::clone(&subscriptions),
                share_fetch_config,
                Arc::clone(&deserializers),
            );
            Self {
                topic_a_partition0,
                topic_a_partition1,
                subscriptions,
                deserializers,
                fetch_collector,
                fetch_buffer: ShareFetchBuffer::new(),
                aggregator,
            }
        }

        fn subscribe_and_assign(&self, tp: &TopicIdPartition) {
            let mut subscriptions = self.subscriptions.lock().unwrap();
            subscriptions
                .subscribe_with_topics(HashSet::from([tp.topic().to_string()]), None)
                .unwrap();
            subscriptions.assign_from_subscribed(&[tp.topic_partition().clone()]).unwrap();
        }

        fn builder(&self) -> ShareCompletedFetchBuilder {
            ShareCompletedFetchBuilder {
                record_count: DEFAULT_RECORD_COUNT,
                base_offset: 0,
                error: None,
                partition: self.topic_a_partition0.clone(),
            }
        }

        fn build(&self, builder: ShareCompletedFetchBuilder) -> ShareCompletedFetch {
            let mut records = crate::common::record::internal::MemoryRecords::builder_with_initial_capacity(
                1024,
                crate::common::compress::Compression::none().build(),
                crate::common::record::TimestampType::CreateTime,
                builder.base_offset,
            );
            for i in 0..builder.record_count {
                records.append_kv(0, Some(b"key"), Some(format!("value-{i}").as_bytes()));
            }
            // A response which acquired no records carries no AcquiredRecords entries at all.
            let acquired: Vec<AcquiredRecords> = if builder.record_count == 0 {
                Vec::new()
            } else {
                acquired_records(builder.base_offset, i64::from(builder.record_count))
            };
            let mut data = partition_data(Some(records.build().into_buffer()), acquired);
            data.set_partition_index(builder.partition.partition());
            if let Some(error) = builder.error {
                data.set_error_code(error.code());
            }
            ShareCompletedFetch::new(
                Arc::new(BufferSupplier::create()),
                0,
                builder.partition,
                data,
                DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
                Arc::clone(&self.aggregator),
                ApiKeys::SHARE_FETCH.latest_version(),
            )
        }

        fn collect(&self) -> ShareFetchCollectResult<String, String> {
            self.fetch_collector
                .collect(&self.fetch_buffer)
                .now_or_never()
                .expect("the test deserializers are always ready")
        }

        fn collect_ok(&self) -> ShareFetch<String, String> {
            self.collect().expect("no error").expect("no batch error")
        }

        fn next_in_line<R>(&self, f: impl FnOnce(&ShareCompletedFetch) -> R) -> R {
            self.fetch_buffer.next_in_line_fetch(|cf| f(cf.expect("a next-in-line fetch")))
        }

        /// Java's `drainAndAcknowledge()`.
        fn drain_and_acknowledge(&self) -> usize {
            let mut delivered = 0;
            for _ in 0..20 {
                let mut fetch = self.collect_ok();
                delivered += fetch.num_records();
                fetch.acknowledge_all(AcknowledgeType::Accept);
                fetch.take_acknowledged_records();
                assert!(fetch.is_empty());
                // The background thread's poll() prunes retained fetches which no longer have pending acknowledgements.
                self.fetch_buffer.buffered_nodes();
                let next_in_line_done = self
                    .fetch_buffer
                    .next_in_line_fetch(|cf| cf.is_none_or(ShareCompletedFetch::is_consumed));
                if self.fetch_buffer.is_empty() && next_in_line_done {
                    break;
                }
            }
            assert!(self.fetch_buffer.is_empty());
            delivered
        }
    }

    impl ShareCompletedFetchBuilder {
        fn record_count(mut self, record_count: i32) -> Self {
            self.record_count = record_count;
            self
        }

        fn base_offset(mut self, base_offset: i64) -> Self {
            self.base_offset = base_offset;
            self
        }

        fn error(mut self, error: Errors) -> Self {
            self.error = Some(error);
            self
        }

        fn partition(mut self, partition: TopicIdPartition) -> Self {
            self.partition = partition;
            self
        }
    }

    #[test]
    fn test_fetch_normal() {
        let record_count = DEFAULT_MAX_POLL_RECORDS;
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);

        let completed_fetch = f.build(f.builder().record_count(record_count));

        // Validate that the buffer is empty until after we add the fetch data.
        assert!(f.fetch_buffer.is_empty());
        // Validate that the completed fetch isn't initialized just because we add it to the buffer.
        assert!(!completed_fetch.is_initialized());
        f.fetch_buffer.add([completed_fetch]);
        assert!(!f.fetch_buffer.is_empty());
        assert!(!f.fetch_buffer.peek(|cf| cf.expect("queued").is_initialized()));

        // Fetch the data and validate that we get all the records we want back.
        let mut fetch = f.collect_ok();
        assert!(!fetch.is_empty());
        assert_eq!(record_count as usize, fetch.num_records());

        // When we collected the data from the buffer, this will cause the completed fetch to get initialized.
        assert!(f.next_in_line(ShareCompletedFetch::is_initialized));

        // However, even though we've collected the data, it isn't (completely) consumed yet.
        assert!(!f.next_in_line(ShareCompletedFetch::is_consumed));

        // The buffer is now considered "empty" because our queue is empty.
        assert!(f.fetch_buffer.is_empty());
        assert!(f.fetch_buffer.peek(|cf| cf.is_none()));
        assert!(f.fetch_buffer.poll().is_none());

        // However, while the queue is "empty", the next-in-line fetch is actually still in the buffer.
        assert!(f.fetch_buffer.next_in_line_fetch(|cf| cf.is_some()));

        // Now attempt to collect more records from the fetch buffer.
        let mut fetch = f.collect_ok();
        assert_eq!(0, fetch.num_records());
        assert!(fetch.is_empty());

        // However, once we read *past* the end of the records in the ShareCompletedFetch, then we will call
        // drain on it, and it will be considered all consumed.
        assert!(f.next_in_line(ShareCompletedFetch::is_consumed));
    }

    #[test]
    fn test_with_renew() {
        let record_count = DEFAULT_MAX_POLL_RECORDS;
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);

        let completed_fetch = f.build(f.builder().record_count(record_count));

        assert!(f.fetch_buffer.is_empty());
        assert!(!completed_fetch.is_initialized());
        f.fetch_buffer.add([completed_fetch]);
        assert!(!f.fetch_buffer.is_empty());

        // Fetch the data and validate that we get all the records we want back.
        let mut fetch = f.collect_ok();
        assert!(!fetch.is_empty());
        assert_eq!(record_count as usize, fetch.num_records());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, fetch.acquisition_lock_timeout_ms());

        // When we collected the data from the buffer, this will cause the completed fetch to get initialized.
        assert!(f.next_in_line(ShareCompletedFetch::is_initialized));
        // However, even though we've collected the data, it isn't (completely) consumed yet.
        assert!(!f.next_in_line(ShareCompletedFetch::is_consumed));
        // The buffer is now considered "empty" because our queue is empty.
        assert!(f.fetch_buffer.is_empty());
        assert!(f.fetch_buffer.peek(|cf| cf.is_none()));
        assert!(f.fetch_buffer.poll().is_none());
        // However, while the queue is "empty", the next-in-line fetch is actually still in the buffer.
        assert!(f.fetch_buffer.next_in_line_fetch(|cf| cf.is_some()));

        assert_eq!(500, fetch.num_records());
        let record = ConsumerRecord::new(
            f.topic_a_partition0.topic(),
            f.topic_a_partition0.partition(),
            0,
            Some(String::new()),
            Some(String::new()),
        );
        fetch.acknowledge_with_record(&record, AcknowledgeType::Renew).unwrap();
        assert_eq!(DEFAULT_MAX_POLL_RECORDS as usize, fetch.num_records());
        assert!(!fetch.has_renewals());

        let acknowledgements_map = fetch.take_acknowledged_records();
        assert!(fetch.has_renewals());
        assert_eq!(DEFAULT_MAX_POLL_RECORDS as usize - 1, fetch.num_records());

        let mut acks = acknowledgements_map[&f.topic_a_partition0].acknowledgements().clone();
        acks.complete(None);
        fetch
            .renew(&HashMap::from([(f.topic_a_partition0.clone(), acks)]), Some(20000))
            .unwrap();
        assert!(fetch.has_renewals());
        fetch
            .take_renewed_records(&f.deserializers)
            .now_or_never()
            .expect("ready")
            .unwrap();
        assert!(!fetch.has_renewals());
        assert_eq!(DEFAULT_MAX_POLL_RECORDS as usize, fetch.num_records());
        assert_eq!(Some(20000), fetch.acquisition_lock_timeout_ms());

        // Now attempt to collect more records from the fetch buffer.
        let mut fetch = f.collect_ok();
        assert_eq!(0, fetch.num_records());
        assert!(fetch.is_empty());

        // However, once we read *past* the end of the records in the ShareCompletedFetch, then we will call
        // drain on it, and it will be considered all consumed.
        assert!(f.next_in_line(ShareCompletedFetch::is_consumed));
    }

    #[test]
    fn test_second_fetch_for_same_partition_merges_without_leaking() {
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);

        // Two completed fetches arrive for the same partition before either is drained by the application.
        f.fetch_buffer.add([f.build(f.builder().base_offset(0).record_count(1))]);
        f.fetch_buffer.add([f.build(f.builder().base_offset(1).record_count(1))]);

        let mut fetch = f.collect_ok();

        // Both records were delivered to the application, merged into a single batch for the partition.
        assert_eq!(2, fetch.num_records());

        // The application acknowledges everything it was actually given.
        fetch.acknowledge_all(AcknowledgeType::Accept);
        fetch.take_acknowledged_records();

        // Nothing should remain buffered.
        assert!(f.fetch_buffer.buffered_partitions().is_empty());
        assert!(f.fetch_buffer.buffered_nodes().is_empty());
    }

    #[test]
    fn test_empty_fetch_followed_by_data_fetch_for_same_partition_does_not_leak() {
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);

        let empty_fetch = f.build(f.builder().record_count(0));
        let data_fetch = f.build(f.builder().base_offset(0).record_count(DEFAULT_RECORD_COUNT));
        f.fetch_buffer.add([empty_fetch, data_fetch]);

        assert_eq!(DEFAULT_RECORD_COUNT as usize, f.drain_and_acknowledge());

        // Every delivered record has been acknowledged, so the fetch that carried them must not report
        // pending acknowledgements, and the buffer must not retain it once it stops being next-in-line.
        assert!(!f.next_in_line(ShareCompletedFetch::has_pending_acknowledgements));
        f.fetch_buffer.add([f.build(f.builder().record_count(0))]);
        assert_eq!(0, f.drain_and_acknowledge());
        assert!(f.fetch_buffer.buffered_partitions().is_empty());
        assert!(f.fetch_buffer.buffered_nodes().is_empty());
    }

    #[test]
    fn test_repeated_fetches_for_same_partition_does_not_accumulate_retained_fetches() {
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);

        // Repeat empty-then-data pattern, acknowledging all each time. If a completed fetch were retained on each
        // iteration, the buffer would still consider the node buffered at the end.
        for i in 0..50 {
            f.fetch_buffer.add([
                f.build(f.builder().record_count(0)),
                f.build(
                    f.builder()
                        .base_offset(i * i64::from(DEFAULT_RECORD_COUNT))
                        .record_count(DEFAULT_RECORD_COUNT),
                ),
            ]);
            assert_eq!(DEFAULT_RECORD_COUNT as usize, f.drain_and_acknowledge());
        }

        f.fetch_buffer.add([f.build(f.builder().record_count(0))]);
        assert_eq!(0, f.drain_and_acknowledge());
        assert!(f.fetch_buffer.buffered_partitions().is_empty());
        assert!(f.fetch_buffer.buffered_nodes().is_empty());
    }

    /// Java's `testErrorInInitializeSource`.
    fn error_in_initialize_source() -> Vec<Error> {
        vec![
            Error::kafka_message(""),
            Error::topic_authorization(HashSet::from(["topic-a".to_string()])),
        ]
    }

    #[test]
    fn test_error_in_initialize_does_not_lose_records_from_earlier_partitions() {
        for expected_error in error_in_initialize_source() {
            let mut f = Fixture::new();
            {
                let mut subscriptions = f.subscriptions.lock().unwrap();
                subscriptions
                    .subscribe_with_topics(HashSet::from([f.topic_a_partition0.topic().to_string()]), None)
                    .unwrap();
                subscriptions
                    .assign_from_subscribed(&[
                        f.topic_a_partition0.topic_partition().clone(),
                        f.topic_a_partition1.topic_partition().clone(),
                    ])
                    .unwrap();
            }

            // A ShareFetchCollector that fails on ShareCompletedFetch initialization for partition 1 only.
            let failing = f.topic_a_partition1.clone();
            let error = expected_error.clone();
            f.fetch_collector
                .set_force_initialize_error(move |cf| (cf.partition == failing).then(|| error.clone()));

            // The failure is only discovered once partition 0's records have already been drained.
            let good_fetch = f.build(f.builder().record_count(2));
            let bad_fetch = f.build(f.builder().partition(f.topic_a_partition1.clone()));
            f.fetch_buffer.add([good_fetch, bad_fetch]);

            // The records already collected must be returned, not dropped on the floor.
            let mut fetch = f.collect_ok();
            assert_eq!(2, fetch.num_records());
            assert_eq!(
                2,
                fetch.records().get(f.topic_a_partition0.topic_partition()).map_or(0, Vec::len)
            );

            // The failing fetch stays at the head of the queue and is surfaced on the next collect.
            assert_eq!(
                Some(f.topic_a_partition1.clone()),
                f.fetch_buffer.peek(|cf| cf.map(|cf| cf.partition.clone()))
            );
            let error = f.collect().expect_err("the failing fetch is surfaced");
            assert_eq!(discriminant(&expected_error), discriminant(&error), "{error:?}");
            assert!(f.fetch_buffer.is_empty());

            // Acknowledging the records that were delivered releases the completed fetch which carried them.
            fetch.acknowledge_all(AcknowledgeType::Accept);
            fetch.take_acknowledged_records();
            assert!(f.fetch_buffer.buffered_partitions().is_empty());
            assert!(f.fetch_buffer.buffered_nodes().is_empty());
        }
    }

    #[test]
    fn test_error_in_initialize() {
        for expected_error in error_in_initialize_source() {
            let mut f = Fixture::new();
            f.subscribe_and_assign(&f.topic_a_partition0);

            // Create a ShareFetchCollector that fails on ShareCompletedFetch initialization.
            let error = expected_error.clone();
            f.fetch_collector.set_force_initialize_error(move |_| Some(error.clone()));

            // Add the ShareCompletedFetch to the ShareFetchBuffer queue - the number of records doesn't matter
            f.fetch_buffer.add([f.build(f.builder().record_count(10))]);

            // At first, the queue is populated
            assert!(!f.fetch_buffer.is_empty());

            // Now run our ill-fated collectFetch.
            let error = f.collect().expect_err("initialize fails");
            assert_eq!(discriminant(&expected_error), discriminant(&error), "{error:?}");
        }
    }

    #[test]
    fn test_fetch_with_topic_authorization_failed() {
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);
        f.fetch_buffer
            .add([f.build(f.builder().error(Errors::TopicAuthorizationFailed))]);
        let error = f.collect().expect_err("not authorized");
        let Error::TopicAuthorization(error) = error else {
            panic!("expected a TopicAuthorizationError, got {error:?}");
        };
        assert_eq!(&HashSet::from(["topic-a".to_string()]), error.unauthorized_topics());
    }

    #[test]
    fn test_fetch_with_unknown_leader_epoch() {
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);
        f.fetch_buffer.add([f.build(f.builder().error(Errors::UnknownLeaderEpoch))]);
        assert!(f.collect_ok().is_empty());
    }

    #[test]
    fn test_fetch_with_unknown_server_error() {
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);
        f.fetch_buffer.add([f.build(f.builder().error(Errors::UnknownServerError))]);
        assert!(f.collect_ok().is_empty());
    }

    #[test]
    fn test_fetch_with_corrupt_message() {
        let f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);
        f.fetch_buffer.add([f.build(f.builder().error(Errors::CorruptMessage))]);
        let error = f.collect().expect_err("corrupt");
        assert!(error.is_kafka_error(), "{error:?}");
        assert_eq!(
            "Encountered corrupt message when fetching topic-partition topic-a-0",
            error.message()
        );
    }

    /// Java's `testFetchWithOtherErrorsSource`: every error but the listed
    /// ones. Rust's `Errors` has no `values()`; every code is walked instead
    /// (`Errors::all_for_test`, the consumer precedent).
    #[test]
    fn test_fetch_with_other_errors() {
        let handled = [
            Errors::None,
            Errors::NotLeaderOrFollower,
            Errors::ReplicaNotAvailable,
            Errors::KafkaStorageError,
            Errors::FencedLeaderEpoch,
            Errors::OffsetNotAvailable,
            Errors::UnknownTopicOrPartition,
            Errors::UnknownTopicId,
            Errors::InconsistentTopicId,
            Errors::OffsetOutOfRange,
            Errors::TopicAuthorizationFailed,
            Errors::UnknownLeaderEpoch,
            Errors::UnknownServerError,
            Errors::CorruptMessage,
        ];
        let others: Vec<Errors> = Errors::all_for_test().into_iter().filter(|e| !handled.contains(e)).collect();
        assert!(others.len() > 100, "every other error is run: {}", others.len());
        for error in others {
            let f = Fixture::new();
            f.subscribe_and_assign(&f.topic_a_partition0);
            f.fetch_buffer.add([f.build(f.builder().error(error))]);
            let thrown = f.collect().expect_err("an unexpected error code fails the collect");
            assert!(thrown.is_kafka_error(), "{error:?}: {thrown:?}");
            assert_eq!(
                format!(
                    "Unexpected error code {} while fetching from topic-partition topic-a-0",
                    error.code()
                ),
                thrown.message()
            );
        }
    }

    /// Rust-only (G9): a batch that fails with nothing else collected comes
    /// back as a `ConsumerShareFetchError` carrying the fetch, whose failed
    /// record can be acknowledged, and the cause unchanged.
    #[test]
    fn test_batch_error_with_nothing_collected_is_a_share_fetch_error() {
        struct FailingDeserializer;
        impl crate::common::serialization::Deserializer<String> for FailingDeserializer {
            async fn deserialize(&self, _topic: &str, _data: &[u8]) -> Result<String, Error> {
                Err(Error::serialization("simulated failure"))
            }
        }

        let mut f = Fixture::new();
        f.subscribe_and_assign(&f.topic_a_partition0);
        let metadata = Arc::new(ShareConsumerMetadata::new(
            0,
            1000,
            10000,
            false,
            Arc::clone(&f.subscriptions),
            ClusterResourceListeners::new(),
        ));
        f.fetch_collector = ShareFetchCollector::new(
            metadata,
            Arc::clone(&f.subscriptions),
            f.fetch_collector.share_fetch_config.clone(),
            Arc::new(Deserializers::new(
                Box::new(crate::common::serialization::StringDeserializer::new()),
                Box::new(FailingDeserializer),
            )),
        );
        f.fetch_buffer.add([f.build(f.builder().record_count(1))]);
        let share_fetch_error = f.collect().expect("no other error").expect_err("the value does not decode");
        assert!(
            matches!(share_fetch_error.cause(), Error::RecordDeserialization(_)),
            "{share_fetch_error:?}"
        );
        assert_eq!(
            DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
            share_fetch_error.share_fetch().acquisition_lock_timeout_ms()
        );
        assert_eq!(
            "Error deserializing VALUE for partition topic-a-0 at offset 0. The record has been released.",
            share_fetch_error.cause().message()
        );
        let (mut fetch, cause) = share_fetch_error.into_parts();
        assert!(matches!(cause, Error::RecordDeserialization(_)));
        fetch
            .acknowledge_with_topic_partition_offset("topic-a", 0, 0, AcknowledgeType::Reject)
            .unwrap();
        let acks = fetch.take_acknowledged_records();
        assert_eq!(
            Some(AcknowledgeType::Reject),
            acks[&f.topic_a_partition0].acknowledgements().get(0)
        );
    }
}
