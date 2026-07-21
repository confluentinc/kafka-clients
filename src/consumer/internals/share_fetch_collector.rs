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

//! `ShareFetchCollector` — drains [`ShareFetchBuffer`] and produces a
//! [`ShareFetch`] of acquired records (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareFetchCollector`.
//!
//! `ShareFetchCollector` operates at the record-batch level, as that is what is
//! stored in the [`ShareFetchBuffer`]. Each record in a batch is converted to a
//! [`ConsumerRecord`](crate::consumer::ConsumerRecord) and added to the
//! returned [`ShareFetch`].
//!
//! # Deviations from Java
//!
//! - **Metadata type**: Java holds a `ShareConsumerMetadata`. That subclass is
//!   out of scope for this phase and not yet translated; the collector only
//!   uses it via `requestMetadataUpdate(metadata, subscriptions, tp)`, which
//!   takes the base metadata. This translation holds an
//!   [`Arc<ConsumerMetadata>`] instead — behaviourally identical for the one
//!   call site. When `ShareConsumerMetadata` is translated in a later phase,
//!   the field type can be swapped without changing the collect logic.
//! - **Error surface**: Java's `collect` throws either a bare `KafkaException`
//!   (from `initialize`) or a `ShareFetchException` (from the records branch).
//!   The Rust translation returns both as
//!   [`Err(ShareFetchException)`](ShareFetchException) — see the type docs.
//! - **Metrics**: `ShareFetchMetricsManager` / `ShareFetchMetricsAggregator`
//!   are omitted (deferred to KIP-714).

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use log::{debug, trace, warn};

use crate::common::protocol::Errors;
use crate::common::requests::share_fetch_response::records_size;
use crate::common::{KafkaError, TopicIdPartition};
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::deserializers::Deserializers;
use crate::consumer::internals::fetch_utils::request_metadata_update;
use crate::consumer::internals::share_completed_fetch::ShareCompletedFetch;
use crate::consumer::internals::share_fetch::ShareFetch;
use crate::consumer::internals::share_fetch_buffer::ShareFetchBuffer;
use crate::consumer::internals::share_fetch_config::ShareFetchConfig;
use crate::consumer::internals::share_fetch_exception::ShareFetchException;
use crate::consumer::internals::subscription_state::SubscriptionState;

/// `Err` payload pairing the rejected [`ShareCompletedFetch`] with the error,
/// so the collector can decide whether to restore the fetch on the queue.
type InitializeFail = Box<(ShareCompletedFetch, KafkaError)>;

/// Drains the [`ShareFetchBuffer`] and produces a [`ShareFetch`] of acquired
/// records.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareFetchCollector<K, V>`.
pub(crate) struct ShareFetchCollector<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    metadata: Arc<ConsumerMetadata>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    share_fetch_config: ShareFetchConfig,
    deserializers: Arc<Deserializers<K, V>>,
    /// Test-only injection point forcing [`Self::initialize`] to fail,
    /// translating Java's `ShareFetchCollectorTest.testErrorInInitialize`
    /// anonymous-subclass override (Rust structs have no inheritance). Gated
    /// behind `#[cfg(test)]` so it is compiled out of release builds — mirrors
    /// the `FetchCollector` precedent.
    #[cfg(test)]
    force_initialize_error: Option<Box<dyn Fn() -> KafkaError + Send + Sync>>,
}

impl<K, V> ShareFetchCollector<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Constructs a `ShareFetchCollector` from its dependencies.
    ///
    /// Translates Java's `ShareFetchCollector(LogContext, ShareConsumerMetadata,
    /// SubscriptionState, ShareFetchConfig, Deserializers)`. The `LogContext`
    /// is dropped (we use the `log` crate).
    pub(crate) fn new(
        metadata: Arc<ConsumerMetadata>,
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

    /// Test-only: install a closure forcing [`Self::initialize`] to fail on its
    /// next invocation (Java's `testErrorInInitialize`).
    #[cfg(test)]
    fn set_force_initialize_error(&mut self, f: impl Fn() -> KafkaError + Send + Sync + 'static) {
        self.force_initialize_error = Some(Box::new(f));
    }

    /// Returns the fetched records for the requested partitions.
    ///
    /// Translates Java's `ShareFetch<K, V> collect(ShareFetchBuffer)`.
    ///
    /// # Errors
    ///
    /// Returns [`ShareFetchException`] carrying the accumulated [`ShareFetch`]
    /// and the cause. Java throws a bare `KafkaException` from `initialize`
    /// (e.g. `TopicAuthorizationException`, or an `IllegalStateException` for an
    /// unexpected error code) and a `ShareFetchException` from the records
    /// branch; both are unified here (see the type docs). Matching Java's outer
    /// `catch (KafkaException e) { if (fetch.isEmpty()) throw e; }`, a
    /// swallowable error is dropped when records were already collected; an
    /// `IllegalState` error always propagates.
    // The `Err` variant intentionally carries the accumulated `ShareFetch`
    // (Java's `ShareFetchException.shareFetch()`), so it is large by design;
    // `collect` runs at most once per `poll()`, not on any hot path.
    #[allow(clippy::result_large_err)]
    pub(crate) fn collect(
        &self,
        fetch_buffer: &ShareFetchBuffer,
    ) -> Result<ShareFetch<K, V>, ShareFetchException<K, V>> {
        let mut fetch = ShareFetch::empty();
        let mut records_remaining = self.share_fetch_config.max_poll_records;

        // Track the first error so the loop can exit early; Java's `catch
        // (KafkaException e)` rethrows only if `fetch.isEmpty()`.
        let mut deferred_error: Option<KafkaError> = None;

        while records_remaining > 0 {
            let next_in_line = fetch_buffer.take_next_in_line_fetch();
            let next_is_consumed = next_in_line.as_ref().is_some_and(ShareCompletedFetch::is_consumed);

            if next_in_line.is_none() || next_is_consumed {
                // The next-in-line slot is empty or finished — drop it and pull
                // the next entry from the queue.
                drop(next_in_line);

                if fetch_buffer.is_empty() {
                    break;
                }
                let head_is_initialized = match fetch_buffer.peek_is_initialized() {
                    Some(b) => b,
                    None => break,
                };

                if !head_is_initialized {
                    let completed_fetch = fetch_buffer.poll().expect("non-empty checked above");
                    match self.initialize(completed_fetch) {
                        Ok(maybe_initialized) => {
                            fetch_buffer.set_next_in_line_fetch(maybe_initialized);
                        },
                        Err(boxed) => {
                            let (cf, e) = *boxed;
                            // Java: catch (Exception e) { if (fetch.isEmpty()) fetchBuffer.poll(); throw e; }
                            // We already polled `cf`; on the "empty" branch Java
                            // removes it (we drop it), otherwise it stays queued
                            // (we push it back).
                            if fetch.is_empty() {
                                drop(cf);
                            } else {
                                fetch_buffer.push_front(cf);
                            }
                            deferred_error = Some(e);
                            break;
                        },
                    }
                    // Java: fetchBuffer.poll() removes the head — already polled.
                } else {
                    let cf = fetch_buffer.poll().expect("non-empty checked above");
                    fetch_buffer.set_next_in_line_fetch(Some(cf));
                }
                continue;
            }

            // next_in_line is Some and not consumed.
            let mut cf = next_in_line.expect("verified Some above");
            let tp = cf.partition().clone();

            let batch = cf.fetch_records(&self.deserializers, records_remaining, self.share_fetch_config.check_crcs);

            if batch.is_empty() {
                cf.drain();
            }

            records_remaining -= batch.num_records() as i32;
            let batch_exception_cause = batch.get_exception().map(|e| e.cause().clone());
            let batch_has_cached_exception = batch.has_cached_exception();
            fetch.add(tp, batch);

            // Restore cf as next-in-line for the next iteration (Java keeps
            // using the same `nextInLineFetch` reference).
            fetch_buffer.set_next_in_line_fetch(Some(cf));

            if let Some(cause) = batch_exception_cause {
                // Java: throw new ShareFetchException(fetch, batch.getException().cause());
                deferred_error = Some(cause);
                break;
            } else if batch_has_cached_exception {
                break;
            }
        }

        if let Some(e) = deferred_error {
            // Java's outer `catch (KafkaException e)` swallows the error when
            // records are already in hand. `IllegalStateException` is NOT a
            // `KafkaException`, so it escapes unconditionally.
            let is_illegal_state = matches!(&e, KafkaError::IllegalState(_));
            if is_illegal_state || fetch.is_empty() {
                return Err(ShareFetchException::new(fetch, e));
            }
        }

        Ok(fetch)
    }

    /// Initializes a [`ShareCompletedFetch`]: runs per-partition error handling
    /// before its records are iterated. Returns `Ok(Some)` when the fetch is
    /// ready to be installed as next-in-line, `Ok(None)` when it should be
    /// skipped (a handled, recoverable error), or `Err((cf, e))` on a
    /// propagating error.
    ///
    /// Translates Java's `ShareCompletedFetch initialize(ShareCompletedFetch)`.
    fn initialize(&self, completed_fetch: ShareCompletedFetch) -> Result<Option<ShareCompletedFetch>, InitializeFail> {
        #[cfg(test)]
        if let Some(f) = self.force_initialize_error.as_ref() {
            let e = f();
            return Err(Box::new((completed_fetch, e)));
        }

        let error = Errors::for_code(completed_fetch.partition_data.error_code);
        if error == Errors::None {
            Ok(Some(self.handle_initialize_success(completed_fetch)))
        } else {
            match self.handle_initialize_errors(completed_fetch, error) {
                Ok(()) => Ok(None),
                Err(boxed) => Err(boxed),
            }
        }
    }

    /// Translates Java's `ShareCompletedFetch handleInitializeSuccess(ShareCompletedFetch)`.
    fn handle_initialize_success(&self, mut completed_fetch: ShareCompletedFetch) -> ShareCompletedFetch {
        trace!(
            "Preparing to read {} bytes of data for partition {}",
            records_size(&completed_fetch.partition_data),
            completed_fetch.partition().topic_partition()
        );
        completed_fetch.set_initialized();
        completed_fetch
    }

    /// Translates Java's `void handleInitializeErrors(ShareCompletedFetch, Errors)`.
    fn handle_initialize_errors(
        &self,
        completed_fetch: ShareCompletedFetch,
        error: Errors,
    ) -> Result<(), InitializeFail> {
        let tp: TopicIdPartition = completed_fetch.partition().clone();
        match error {
            Errors::NotLeaderOrFollower
            | Errors::ReplicaNotAvailable
            | Errors::KafkaStorageError
            | Errors::FencedLeaderEpoch
            | Errors::OffsetNotAvailable => {
                debug!("Error in fetch for partition {tp}: {:?}", error);
                request_metadata_update(&self.metadata, &self.subscriptions, tp.topic_partition());
                Ok(())
            },
            Errors::UnknownTopicOrPartition => {
                warn!("Received unknown topic or partition error in fetch for partition {tp}.");
                request_metadata_update(&self.metadata, &self.subscriptions, tp.topic_partition());
                Ok(())
            },
            Errors::UnknownTopicId => {
                warn!("Received unknown topic ID error in fetch for partition {tp}.");
                request_metadata_update(&self.metadata, &self.subscriptions, tp.topic_partition());
                Ok(())
            },
            Errors::InconsistentTopicId => {
                warn!("Received inconsistent topic ID error in fetch for partition {tp}.");
                request_metadata_update(&self.metadata, &self.subscriptions, tp.topic_partition());
                Ok(())
            },
            Errors::TopicAuthorizationFailed => {
                warn!("Not authorized to read from partition {}.", tp.topic_partition());
                let mut set: HashSet<String> = HashSet::new();
                set.insert(tp.topic().to_string());
                Err(Box::new((completed_fetch, KafkaError::topic_authorization(set))))
            },
            Errors::UnknownLeaderEpoch => {
                debug!("Received unknown leader epoch error in fetch for partition {tp}.");
                Ok(())
            },
            Errors::UnknownServerError => {
                warn!("Unknown server error while fetching topic-partition {}.", tp.topic_partition());
                Ok(())
            },
            Errors::CorruptMessage => Err(Box::new((
                completed_fetch,
                KafkaError::with_message(
                    Errors::CorruptMessage,
                    format!(
                        "Encountered corrupt message when fetching topic-partition {}",
                        tp.topic_partition()
                    ),
                ),
            ))),
            other => Err(Box::new((
                completed_fetch,
                KafkaError::illegal_state(format!(
                    "Unexpected error code {} while fetching from topic-partition {}",
                    other.code(),
                    tp.topic_partition()
                )),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::isolation_level::IsolationLevel;
    use crate::common::record::{MemoryRecords, SimpleRecord, TimestampType};
    use crate::common::serialization::Deserializer;
    use crate::common::{TopicPartition, Uuid};
    use crate::consumer::AcknowledgeType;
    use crate::consumer::ConsumerRecord;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::share_acquire_mode::ShareAcquireMode;
    use crate::share_fetch_response_data::{AcquiredRecords, PartitionData};

    const DEFAULT_RECORD_COUNT: i32 = 10;
    const DEFAULT_MAX_POLL_RECORDS: i32 = 500;
    const DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS: Option<i32> = Some(30_000);

    struct StringDeserializer;
    impl Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
            String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(e.to_string()))
        }
    }

    struct Harness {
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
        deserializers: Arc<Deserializers<String, String>>,
        share_fetch_config: ShareFetchConfig,
        fetch_buffer: ShareFetchBuffer,
        collector: ShareFetchCollector<String, String>,
        topic_a_partition0: TopicIdPartition,
    }

    fn build_dependencies(max_poll_records: i32) -> Harness {
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = Arc::new(ConsumerMetadata::new(
            0,
            1_000,
            10_000,
            false,
            false,
            subscriptions.clone(),
            ClusterResourceListeners::new(),
        ));
        let deserializers = Arc::new(Deserializers::new(Box::new(StringDeserializer), Box::new(StringDeserializer)));
        let share_fetch_config = ShareFetchConfig::new(
            1,
            50 * 1024 * 1024,
            500,
            1024 * 1024,
            max_poll_records,
            true,
            "",
            IsolationLevel::ReadUncommitted,
            ShareAcquireMode::BatchOptimized,
        );
        let collector = ShareFetchCollector::new(
            metadata.clone(),
            subscriptions.clone(),
            share_fetch_config.clone(),
            deserializers.clone(),
        );
        Harness {
            subscriptions,
            metadata,
            deserializers,
            share_fetch_config,
            fetch_buffer: ShareFetchBuffer::new(),
            collector,
            topic_a_partition0: TopicIdPartition::new(
                Uuid::random_uuid(),
                TopicPartition::new("topic-a".to_string(), 0),
            ),
        }
    }

    fn subscribe_and_assign(h: &Harness) {
        let mut guard = h.subscriptions.lock().expect("lock");
        let topics: HashSet<String> = [h.topic_a_partition0.topic().to_string()].into_iter().collect();
        guard.subscribe_to_share_group(topics).unwrap();
        guard
            .assign_from_subscribed(&[h.topic_a_partition0.topic_partition().clone()])
            .unwrap();
    }

    fn acquired_records(first_offset: i64, count: i64) -> Vec<AcquiredRecords> {
        let mut ar = AcquiredRecords::new();
        ar.first_offset = first_offset;
        ar.last_offset = first_offset + count - 1;
        ar.delivery_count = 1;
        vec![ar]
    }

    fn build_completed_fetch(h: &Harness, record_count: i32, error: Option<Errors>) -> ShareCompletedFetch {
        let simple: Vec<SimpleRecord> = (0..record_count)
            .map(|i| SimpleRecord::new(0, Some(b"key".to_vec()), Some(format!("value-{i}").into_bytes()), vec![]))
            .collect();
        let records =
            MemoryRecords::with_records_at_offset(2, 0, Compression::none(), TimestampType::CreateTime, &simple)
                .buffer()
                .to_vec();
        let mut pd = PartitionData::new();
        pd.partition_index = h.topic_a_partition0.partition();
        pd.records = Some(bytes::Bytes::from(records));
        pd.acquired_records = acquired_records(0, record_count as i64);
        if let Some(e) = error {
            pd.error_code = e.code();
        }
        ShareCompletedFetch::new(0, h.topic_a_partition0.clone(), pd, DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS)
    }

    /// Builds a [`ShareCompletedFetch`] on a distinct partition whose batch has
    /// a broken CRC (a byte in the records region is flipped), so that
    /// `ensure_valid` fails under `check_crcs = true`.
    fn build_corrupt_completed_fetch() -> ShareCompletedFetch {
        let tip = TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new("topic-a".to_string(), 1));
        let simple: Vec<SimpleRecord> = (0..5)
            .map(|i| SimpleRecord::new(0, Some(b"key".to_vec()), Some(format!("value-{i}").into_bytes()), vec![]))
            .collect();
        let mut records =
            MemoryRecords::with_records_at_offset(2, 0, Compression::none(), TimestampType::CreateTime, &simple)
                .buffer()
                .to_vec();
        // Flip the last byte (in the CRC-covered records region) so the stored
        // batch CRC no longer matches the recomputed one.
        let last = records.len() - 1;
        records[last] ^= 0xFF;

        let mut pd = PartitionData::new();
        pd.partition_index = 1;
        pd.records = Some(bytes::Bytes::from(records));
        pd.acquired_records = acquired_records(0, 5);
        ShareCompletedFetch::new(0, tip, pd, DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS)
    }

    /// Translated from `ShareFetchCollectorTest.testFetchNormal`.
    ///
    /// Java also asserts the `ShareCompletedFetch` lifecycle across the two
    /// collects (`isInitialized()` true, `isConsumed()` false, then true). Those
    /// assertions are dropped here because the `cf` is moved into the buffer's
    /// next-in-line slot (owned, not a shared reference we can inspect); the
    /// observable substitute is `has_next_in_line_fetch()`.
    #[test]
    fn test_fetch_normal() {
        let record_count = DEFAULT_MAX_POLL_RECORDS;
        let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
        subscribe_and_assign(&h);

        let completed_fetch = build_completed_fetch(&h, record_count, None);
        assert!(h.fetch_buffer.is_empty());
        h.fetch_buffer.add([completed_fetch]);
        assert!(!h.fetch_buffer.is_empty());
        // The head is not initialized just because it was added.
        assert_eq!(Some(false), h.fetch_buffer.peek_is_initialized());

        let mut fetch = h.collector.collect(&h.fetch_buffer).unwrap();
        assert!(!fetch.is_empty());
        assert_eq!(record_count as usize, fetch.num_records());

        // The queue is now empty, but the next-in-line fetch is still in the buffer.
        assert!(h.fetch_buffer.is_empty());
        assert!(h.fetch_buffer.has_next_in_line_fetch());

        // Collect again: empty result; the next-in-line gets drained.
        let mut fetch = h.collector.collect(&h.fetch_buffer).unwrap();
        assert_eq!(0, fetch.num_records());
        assert!(fetch.is_empty());
    }

    /// Translated from `ShareFetchCollectorTest.testWithRenew`.
    #[test]
    fn test_with_renew() {
        let record_count = DEFAULT_MAX_POLL_RECORDS;
        let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
        subscribe_and_assign(&h);

        let completed_fetch = build_completed_fetch(&h, record_count, None);
        h.fetch_buffer.add([completed_fetch]);

        let mut fetch = h.collector.collect(&h.fetch_buffer).unwrap();
        assert!(!fetch.is_empty());
        assert_eq!(record_count as usize, fetch.num_records());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, fetch.acquisition_lock_timeout_ms());

        // Acknowledge offset 0 with RENEW.
        let record: ConsumerRecord<String, String> =
            ConsumerRecord::new(h.topic_a_partition0.topic(), h.topic_a_partition0.partition(), 0, None, None);
        fetch.acknowledge(&record, AcknowledgeType::Renew).unwrap();
        assert_eq!(DEFAULT_MAX_POLL_RECORDS as usize, fetch.num_records());
        assert!(!fetch.has_renewals());

        let acks_map = fetch.take_acknowledged_records();
        assert!(fetch.has_renewals());
        assert_eq!((DEFAULT_MAX_POLL_RECORDS - 1) as usize, fetch.num_records());

        let mut acks = acks_map
            .get(&h.topic_a_partition0)
            .expect("acks present")
            .acknowledgements()
            .clone();
        acks.complete(None);
        let mut renew_map = indexmap::IndexMap::new();
        renew_map.insert(h.topic_a_partition0.clone(), acks);
        fetch.renew(&renew_map, Some(20_000)).unwrap();
        assert!(fetch.has_renewals());

        fetch.take_renewed_records();
        assert!(!fetch.has_renewals());
        assert_eq!(DEFAULT_MAX_POLL_RECORDS as usize, fetch.num_records());
        assert_eq!(Some(20_000), fetch.acquisition_lock_timeout_ms());

        // Collect again: empty.
        let mut fetch = h.collector.collect(&h.fetch_buffer).unwrap();
        assert_eq!(0, fetch.num_records());
        assert!(fetch.is_empty());
    }

    /// Translated from `ShareFetchCollectorTest.testErrorInInitialize`
    /// (`@ParameterizedTest` over `RuntimeException` and `KafkaException`).
    #[test]
    fn test_error_in_initialize() {
        // Two injected errors: a plain runtime-style error and a Kafka-style
        // error. Both propagate here because the fetch is empty.
        for inject in [
            || KafkaError::illegal_state("injected runtime error"),
            || KafkaError::new(crate::common::protocol::Errors::UnknownServerError),
        ] {
            let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
            subscribe_and_assign(&h);
            let mut collector = ShareFetchCollector::new(
                h.metadata.clone(),
                h.subscriptions.clone(),
                h.share_fetch_config.clone(),
                h.deserializers.clone(),
            );
            collector.set_force_initialize_error(inject);

            let completed_fetch = build_completed_fetch(&h, 10, None);
            h.fetch_buffer.add([completed_fetch]);
            assert!(!h.fetch_buffer.is_empty());

            let err = collector
                .collect(&h.fetch_buffer)
                .expect_err("initialize failure must propagate");
            // The cause is the injected error.
            let (_fetch, cause) = err.into_parts();
            assert!(!cause.message().is_empty());
        }
    }

    /// Translated from `ShareFetchCollectorTest.testFetchWithTopicAuthorizationFailed`.
    #[test]
    fn test_fetch_with_topic_authorization_failed() {
        let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
        subscribe_and_assign(&h);
        let completed_fetch = build_completed_fetch(&h, DEFAULT_RECORD_COUNT, Some(Errors::TopicAuthorizationFailed));
        h.fetch_buffer.add([completed_fetch]);

        let err = h
            .collector
            .collect(&h.fetch_buffer)
            .expect_err("topic auth failure must propagate");
        match err.cause() {
            KafkaError::TopicAuthorization(ta) => assert!(ta.unauthorized_topics.contains("topic-a")),
            other => panic!("expected TopicAuthorization, got {other:?}"),
        }
    }

    /// Translated from `ShareFetchCollectorTest.testFetchWithUnknownLeaderEpoch`.
    #[test]
    fn test_fetch_with_unknown_leader_epoch() {
        let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
        subscribe_and_assign(&h);
        let completed_fetch = build_completed_fetch(&h, DEFAULT_RECORD_COUNT, Some(Errors::UnknownLeaderEpoch));
        h.fetch_buffer.add([completed_fetch]);
        let mut fetch = h.collector.collect(&h.fetch_buffer).unwrap();
        assert!(fetch.is_empty());
    }

    /// Translated from `ShareFetchCollectorTest.testFetchWithUnknownServerError`.
    #[test]
    fn test_fetch_with_unknown_server_error() {
        let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
        subscribe_and_assign(&h);
        let completed_fetch = build_completed_fetch(&h, DEFAULT_RECORD_COUNT, Some(Errors::UnknownServerError));
        h.fetch_buffer.add([completed_fetch]);
        let mut fetch = h.collector.collect(&h.fetch_buffer).unwrap();
        assert!(fetch.is_empty());
    }

    /// Translated from `ShareFetchCollectorTest.testFetchWithCorruptMessage`.
    #[test]
    fn test_fetch_with_corrupt_message() {
        let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
        subscribe_and_assign(&h);
        let completed_fetch = build_completed_fetch(&h, DEFAULT_RECORD_COUNT, Some(Errors::CorruptMessage));
        h.fetch_buffer.add([completed_fetch]);
        let err = h
            .collector
            .collect(&h.fetch_buffer)
            .expect_err("corrupt message must propagate");
        assert!(
            err.cause().message().contains("corrupt message"),
            "got: {}",
            err.cause().message()
        );
    }

    /// Translated from `ShareFetchCollectorTest.testFetchWithOtherErrors`
    /// (`@ParameterizedTest`): every error not otherwise handled results in an
    /// `IllegalStateException`.
    #[test]
    fn test_fetch_with_other_errors() {
        let handled: HashSet<Errors> = [
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
        ]
        .into_iter()
        .collect();

        // Sweep every Kafka error code; skip the handled ones. Each remaining
        // (unhandled) error must hit the catch-all `IllegalState` arm — this is
        // the defensive check Java's parameterization provides (a newly added
        // error code must not be silently miscategorized). Rust's `Errors` has
        // no all-variants iterator, so we sweep by code via `Errors::for_code`
        // (out-of-range codes collapse to `UnknownServerError`, which is
        // handled and skipped), covering the same set Java iterates.
        let mut asserted_any = false;
        for code in 0..=130_i16 {
            let error = Errors::for_code(code);
            if handled.contains(&error) {
                continue;
            }
            let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
            subscribe_and_assign(&h);
            let completed_fetch = build_completed_fetch(&h, DEFAULT_RECORD_COUNT, Some(error));
            h.fetch_buffer.add([completed_fetch]);
            let err = h
                .collector
                .collect(&h.fetch_buffer)
                .expect_err("unexpected error must propagate");
            assert!(
                matches!(err.cause(), KafkaError::IllegalState(_)),
                "error {error:?} (code {code}): expected IllegalState, got: {:?}",
                err.cause()
            );
            assert!(
                err.cause().message().contains("Unexpected error code"),
                "error {error:?}: got: {}",
                err.cause().message()
            );
            asserted_any = true;
        }
        assert!(asserted_any, "sweep must cover at least one unhandled error code");
    }

    /// Regression for Critic Phase-3 Finding 1: a corrupt / CRC-failed batch
    /// encountered AFTER good records were already collected must be swallowed
    /// (Java's `catch (KafkaException e) { if (fetch.isEmpty()) throw e; }`,
    /// `ShareFetchCollector.java:121-125`), returning the good records and
    /// deferring the retriable corrupt error — NOT propagated as an error that
    /// drops the good records.
    ///
    /// Scenario: partition A (valid, 5 records) and partition B (a CRC-corrupt
    /// batch) with `check_crcs = true`. `collect` processes A first (records
    /// enter `fetch`), then B's `fetch_records` fails validation with an empty
    /// in-flight batch → `reject_record_batch` + `set_exception`. Because the
    /// error is now classified as `Errors::CorruptMessage` (not `IllegalState`),
    /// the collector swallows it (fetch is non-empty) and returns A's records.
    ///
    /// Before the fix, the corrupt error was mapped to `illegal_state`, which
    /// the collector's `is_illegal_state` escape treated as always-propagating,
    /// so `collect` returned `Err` and dropped A's already-collected records.
    #[test]
    fn test_corrupt_batch_after_good_records_is_swallowed() {
        let h = build_dependencies(DEFAULT_MAX_POLL_RECORDS);
        subscribe_and_assign(&h);

        // Partition A: valid, 5 records.
        let cf_a = build_completed_fetch(&h, 5, None);
        // Partition B: a CRC-corrupt batch on a different partition.
        let cf_b = build_corrupt_completed_fetch();

        h.fetch_buffer.add([cf_a, cf_b]);

        let mut fetch = h.collector.collect(&h.fetch_buffer).expect("corrupt error must be swallowed");
        assert!(!fetch.is_empty(), "partition A's records must be returned");
        assert_eq!(5, fetch.num_records());
    }
}
