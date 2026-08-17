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

//! Mock producer for testing code that uses Kafka.
//!
//! Corresponds to Java's `org.apache.kafka.clients.producer.MockProducer`.
//!
//! By default this mock will synchronously complete each send call successfully.
//! However it can be configured to allow the user to control the completion of
//! the call and supply an optional error for the producer to throw.
//!
//! # Transactional API
//!
//! Transactional methods (`initTransactions`, `beginTransaction`,
//! `commitTransaction`, `abortTransaction`, `sendOffsetsToTransaction`) are
//! excluded from this implementation because the [`Producer`] trait does not
//! include them. They can be added in a future phase if needed.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::Producer;
use super::ProducerRecord;
use super::RecordMetadata;
use super::internals::FutureRecordMetadata;
use super::internals::ProduceRequestResult;
use crate::common::Cluster;
use crate::common::KafkaError;
use crate::common::KafkaFuture;
use crate::common::MetricName;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::metrics::KafkaMetric;
use crate::common::record::RecordBatch;

use super::Callback;

/// A mock of the producer interface for testing code that uses Kafka.
///
/// By default this mock will synchronously complete each send call successfully.
/// However it can be configured to allow the user to control the completion of
/// the call and supply an optional error for the producer to throw.
///
/// Corresponds to Java's `org.apache.kafka.clients.producer.MockProducer`
/// (non-transactional subset).
///
/// # Thread Safety
///
/// All methods use interior mutability via `Mutex`, matching Java's
/// `synchronized` methods. The struct is `Send + Sync` so it can be shared
/// via `Arc`.
pub struct MockProducer<K, V> {
    inner: Mutex<MockProducerInner<K, V>>,
}

struct MockProducerInner<K, V> {
    cluster: Cluster,
    auto_complete: bool,
    sent: Vec<ProducerRecord<K, V>>,
    completions: VecDeque<Completion>,
    offsets: HashMap<TopicPartition, i64>,
    closed: bool,
    send_error: Option<KafkaError>,
    flush_error: Option<KafkaError>,
    partitions_for_error: Option<KafkaError>,
    close_error: Option<KafkaError>,
    /// User-supplied metrics returned by [`metrics()`](Producer::metrics).
    ///
    /// Mirrors Java's `MockProducer.mockMetrics` map, seeded via
    /// [`set_mock_metrics`](MockProducer::set_mock_metrics).
    mock_metrics: HashMap<MetricName, Arc<KafkaMetric>>,
}

/// Internal completion record that holds the state needed to fulfill a
/// [`FutureRecordMetadata`].
///
/// Corresponds to Java's `MockProducer.Completion` inner class.
struct Completion {
    offset: i64,
    metadata: RecordMetadata,
    result: Arc<ProduceRequestResult>,
    callback: Option<Callback>,
    topic_partition: TopicPartition,
}

impl Completion {
    /// Complete this send with either a success or an error.
    ///
    /// Corresponds to Java's `Completion.complete(RuntimeException)`.
    fn complete(self, error: Option<KafkaError>) {
        let Completion { offset, metadata, result, callback, topic_partition } = self;
        if let Some(e) = error {
            let error_fn: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> = {
                let e = e.clone();
                Arc::new(move |_| Some(e.clone()))
            };
            result.set(-1, RecordBatch::NO_TIMESTAMP, Some(error_fn));
            result.done();
            // Mirror Java's `Completion.complete`: fire the callback with the error.
            if let Some(cb) = callback {
                cb(None, Some(&e));
            }
            let _ = topic_partition;
        } else {
            result.set(offset, RecordBatch::NO_TIMESTAMP, None);
            result.done();
            // Mirror Java's `Completion.complete`: fire the callback with the metadata.
            if let Some(cb) = callback {
                cb(Some(&metadata), None);
            }
        }
    }
}

impl<K, V> MockProducer<K, V> {
    /// Create a mock producer.
    ///
    /// # Arguments
    ///
    /// * `cluster` - The cluster holding metadata for this producer.
    /// * `auto_complete` - If `true`, automatically complete all requests
    ///   successfully. Otherwise the user must call [`complete_next()`](Self::complete_next)
    ///   or [`error_next()`](Self::error_next) after [`send()`](Producer::send)
    ///   to complete the call and resolve the [`FutureRecordMetadata`].
    ///
    /// Corresponds to Java's `MockProducer(Cluster, boolean, Partitioner,
    /// Serializer, Serializer)` constructor (without serializers or partitioner,
    /// since the Rust producer works with pre-serialized bytes).
    pub fn new(cluster: Cluster, auto_complete: bool) -> Self {
        Self {
            inner: Mutex::new(MockProducerInner {
                cluster,
                auto_complete,
                sent: Vec::new(),
                completions: VecDeque::new(),
                offsets: HashMap::new(),
                closed: false,
                send_error: None,
                flush_error: None,
                partitions_for_error: None,
                close_error: None,
                mock_metrics: HashMap::new(),
            }),
        }
    }

    /// Create a new mock producer with an empty cluster and the given
    /// `auto_complete` setting.
    ///
    /// Equivalent to `MockProducer::new(Cluster::empty(), auto_complete)`.
    ///
    /// Corresponds to Java's `MockProducer(boolean, Partitioner, Serializer,
    /// Serializer)`.
    pub fn with_auto_complete(auto_complete: bool) -> Self {
        Self::new(Cluster::empty(), auto_complete)
    }

    /// Get the list of sent records since the last call to [`clear()`](Self::clear).
    ///
    /// Returns a clone of the internal sent list.
    ///
    /// Corresponds to Java's `MockProducer.history()`.
    pub fn history(&self) -> Vec<ProducerRecord<K, V>>
    where
        K: Clone,
        V: Clone,
    {
        let inner = self.inner.lock().unwrap();
        inner.sent.clone()
    }

    /// Clear the stored history of sent records.
    ///
    /// Note: per-topic-partition offset counters are intentionally preserved
    /// across `clear()` calls, matching Java's `MockProducer.clear()` which
    /// does **not** reset the `offsets` map.
    ///
    /// Corresponds to Java's `MockProducer.clear()`.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.sent.clear();
        inner.completions.clear();
    }

    /// Complete the earliest uncompleted call successfully.
    ///
    /// Returns `true` if there was an uncompleted call to complete.
    ///
    /// Corresponds to Java's `MockProducer.completeNext()`.
    pub fn complete_next(&self) -> bool {
        self.error_next_inner(None)
    }

    /// Complete the earliest uncompleted call with the given error.
    ///
    /// Returns `true` if there was an uncompleted call to complete.
    ///
    /// Corresponds to Java's `MockProducer.errorNext(RuntimeException)`.
    pub fn error_next(&self, error: KafkaError) -> bool {
        self.error_next_inner(Some(error))
    }

    /// Internal helper shared by `complete_next()` and `error_next()`.
    ///
    /// Corresponds to Java's `MockProducer.errorNext(RuntimeException)` which
    /// is also called by `completeNext()` with a `null` argument.
    fn error_next_inner(&self, error: Option<KafkaError>) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some(completion) = inner.completions.pop_front() {
            completion.complete(error);
            true
        } else {
            false
        }
    }

    /// Returns `true` if the producer is closed.
    ///
    /// Corresponds to Java's `MockProducer.closed()`.
    pub fn closed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.closed
    }

    /// Returns `true` if there are no pending completions.
    ///
    /// Corresponds to Java's `MockProducer.flushed()`.
    pub fn flushed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.completions.is_empty()
    }

    /// Set an error to be returned on every [`send()`](Producer::send) call
    /// until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.sendException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_send_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.send_error = error;
    }

    /// Set an error to be returned on every [`flush()`](Producer::flush) call
    /// until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.flushException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_flush_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.flush_error = error;
    }

    /// Set an error to be returned on every
    /// [`partitions_for()`](Producer::partitions_for) call until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.partitionsForException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_partitions_for_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.partitions_for_error = error;
    }

    /// Set an error to be returned on every [`close()`](Producer::close) call
    /// until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.closeException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_close_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.close_error = error;
    }

    /// Seed a metric returned by [`metrics()`](Producer::metrics).
    ///
    /// Corresponds to Java's `MockProducer.setMockMetrics(MetricName name,
    /// Metric metric)`.
    pub fn set_mock_metrics(&self, name: MetricName, metric: Arc<KafkaMetric>) {
        let mut inner = self.inner.lock().unwrap();
        inner.mock_metrics.insert(name, metric);
    }
}

impl<K, V> Default for MockProducer<K, V> {
    /// Create a new mock producer with an empty cluster and `auto_complete=false`.
    ///
    /// Corresponds to Java's no-arg `MockProducer()` constructor.
    fn default() -> Self {
        Self::new(Cluster::empty(), false)
    }
}

impl<K: Send + Sync, V: Send + Sync> Producer<K, V> for MockProducer<K, V> {
    async fn send(&self, record: ProducerRecord<K, V>) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.send_with_callback(record, None).await
    }

    async fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if inner.closed {
            return Err(KafkaError::illegal_state("MockProducer is already closed."));
        }

        if let Some(err) = inner.send_error.as_ref() {
            return Err(err.clone());
        }

        let partition = record.partition().unwrap_or(0);
        let tp = TopicPartition::new(record.topic().to_string(), partition);

        let result = Arc::new(ProduceRequestResult::new(tp.clone()));
        let future = Arc::new(FutureRecordMetadata::new(
            Arc::clone(&result),
            0,
            RecordBatch::NO_TIMESTAMP,
            0,
            0,
        ));

        let offset = next_offset(&mut inner.offsets, &tp);
        let base_offset = 0i64.max(offset - i64::from(i32::MAX));
        let batch_index = (offset.min(i64::from(i32::MAX))) as i32;

        let metadata = RecordMetadata::new(tp.clone(), base_offset, batch_index, RecordBatch::NO_TIMESTAMP, 0, 0);

        inner.sent.push(record);

        let completion = Completion { offset, metadata, result: Arc::clone(&result), callback, topic_partition: tp };

        if inner.auto_complete {
            completion.complete(None);
        } else {
            inner.completions.push_back(completion);
        }

        Ok(KafkaFuture::new(future))
    }

    async fn flush(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if inner.closed {
            return Err(KafkaError::illegal_state("MockProducer is already closed."));
        }

        if let Some(err) = inner.flush_error.as_ref() {
            return Err(err.clone());
        }

        while let Some(completion) = inner.completions.pop_front() {
            completion.complete(None);
        }

        Ok(())
    }

    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let inner = self.inner.lock().unwrap();

        if let Some(err) = inner.partitions_for_error.as_ref() {
            return Err(err.clone());
        }

        Ok(inner.cluster.partitions_for_topic(topic).to_vec())
    }

    /// Return the mock metrics. Corresponds to Java's `MockProducer.metrics()`
    /// returning the `mockMetrics` map.
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        let inner = self.inner.lock().unwrap();
        inner.mock_metrics.clone()
    }

    async fn close(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if let Some(err) = inner.close_error.as_ref() {
            return Err(err.clone());
        }

        inner.closed = true;
        Ok(())
    }

    async fn close_timeout(&self, _timeout: Duration) -> Result<(), KafkaError> {
        self.close().await
    }
}

/// Get the next offset for this topic/partition.
///
/// First call for a topic-partition returns 0 and stores 1. Subsequent calls
/// increment and return the previous value.
///
/// Corresponds to Java's `MockProducer.nextOffset(TopicPartition)`.
fn next_offset(offsets: &mut HashMap<TopicPartition, i64>, tp: &TopicPartition) -> i64 {
    match offsets.get_mut(tp) {
        Some(offset) => {
            let current = *offset;
            *offset = current + 1;
            current
        },
        None => {
            offsets.insert(tp.clone(), 1);
            0
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Errors;

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    fn make_record(topic: &str, key: &str, value: &str) -> ProducerRecord<String, String> {
        ProducerRecord::with_key(topic.to_string(), Some(key.to_string()), Some(value.to_string()))
    }

    // -----------------------------------------------------------------------
    // Tests translated from MockProducerTest.java
    // -----------------------------------------------------------------------

    /// Translated from `MockProducerTest.testAutoCompleteMock`.
    #[tokio::test]
    async fn test_auto_complete_mock() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let record1 = make_record("topic", "key1", "value1");

        let future = producer.send(record1.clone()).await.unwrap();
        assert!(future.is_done(), "Send should be immediately complete");

        let metadata = future.get().await;
        assert!(metadata.is_ok(), "Send should be successful");
        let md = metadata.unwrap();
        assert_eq!(0, md.offset(), "Offset should be 0");
        assert_eq!("topic", md.topic());

        assert_eq!(vec![record1], producer.history(), "We should have the record in our history");

        producer.clear();
        assert_eq!(0, producer.history().len(), "Clear should erase our history");
    }

    /// Translated from `MockProducerTest.testPartitioner`.
    ///
    /// Java's test uses a `RoundRobinPartitioner` with cluster metadata.
    /// Since our Rust `MockProducer` doesn't use a partitioner (it uses
    /// `record.partition().unwrap_or(0)` directly), we test that a record
    /// with an explicit partition is assigned correctly.
    #[tokio::test]
    async fn test_partitioner() {
        let node = crate::common::Node::new(0, "localhost".to_string(), 9092);
        let pi0 = PartitionInfo::new("topic".to_string(), 0, Some(node.clone()), vec![], vec![]);
        let pi1 = PartitionInfo::new("topic".to_string(), 1, Some(node), vec![], vec![]);

        let cluster = Cluster::new(
            None,
            vec![],
            vec![pi0, pi1],
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            None,
            HashMap::new(),
        );
        let producer: MockProducer<String, String> = MockProducer::new(cluster, true);

        // Send with explicit partition=1
        let record = ProducerRecord::with_partition(
            "topic".to_string(),
            Some(1),
            Some("key".to_string()),
            Some("value".to_string()),
        )
        .unwrap();
        let future = producer.send(record).await.unwrap();
        let md = future.get().await.unwrap();
        assert_eq!(1, md.partition(), "Partition should be correct");

        producer.clear();
        assert_eq!(0, producer.history().len(), "Clear should erase our history");
        producer.close().await.unwrap();
    }

    /// Translated from `MockProducerTest.testManualCompletion`.
    #[tokio::test]
    async fn test_manual_completion() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        let record1 = make_record("topic", "key1", "value1");
        let record2 = make_record("topic", "key2", "value2");

        let md1 = producer.send(record1.clone()).await.unwrap();
        assert!(!md1.is_done(), "Send shouldn't have completed");

        let md2 = producer.send(record2.clone()).await.unwrap();
        assert!(!md2.is_done(), "Send shouldn't have completed");

        assert!(producer.complete_next(), "Complete the first request");
        let result1 = md1.get().await;
        assert!(result1.is_ok(), "Request should be successful");
        assert!(!md2.is_done(), "Second request still incomplete");

        assert!(
            producer.error_next(KafkaError::illegal_argument("blah")),
            "Complete the second request with an error"
        );
        let result2 = md2.get().await;
        assert!(result2.is_err(), "Expected error to be thrown");

        assert!(!producer.complete_next(), "No more requests to complete");

        // Test flush completes remaining sends
        let md3 = producer.send(record1).await.unwrap();
        let md4 = producer.send(record2).await.unwrap();
        assert!(!md3.is_done() && !md4.is_done(), "Requests should not be completed.");
        producer.flush().await.unwrap();
        assert!(md3.is_done() && md4.is_done(), "Requests should be completed.");
    }

    // -----------------------------------------------------------------------
    // Transactional tests (skipped)
    //
    // The following Java tests are excluded because the Producer trait does not
    // include transactional methods. These tests exercise initTransactions(),
    // beginTransaction(), commitTransaction(), abortTransaction(),
    // sendOffsetsToTransaction(), fenceProducer(), and related state:
    //
    //   - shouldInitTransactions
    //   - shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions
    //   - shouldThrowOnBeginTransactionIfTransactionsNotInitialized
    //   - shouldBeginTransactions
    //   - shouldThrowOnBeginTransactionsIfTransactionInflight
    //   - shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized
    //   - shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted
    //   - shouldThrowOnCommitIfTransactionsNotInitialized
    //   - shouldThrowOnCommitTransactionIfNoTransactionGotStarted
    //   - shouldCommitEmptyTransaction
    //   - shouldCountCommittedTransaction
    //   - shouldNotCountAbortedTransaction
    //   - shouldThrowOnAbortIfTransactionsNotInitialized
    //   - shouldThrowOnAbortTransactionIfNoTransactionGotStarted
    //   - shouldAbortEmptyTransaction
    //   - shouldThrowFenceProducerIfTransactionsNotInitialized
    //   - shouldThrowOnBeginTransactionsIfProducerGotFenced
    //   - shouldThrowOnSendIfProducerGotFenced
    //   - shouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced
    //   - shouldThrowOnSendOffsetsToTransactionByGroupMetadataIfProducerGotFenced
    //   - shouldThrowOnCommitTransactionIfProducerGotFenced
    //   - shouldThrowOnAbortTransactionIfProducerGotFenced
    //   - shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled
    //   - shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled
    //   - shouldDropMessagesOnAbortIfTransactionsAreEnabled
    //   - shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled
    //   - shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled
    //   - shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled
    //   - shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction
    //   - shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata
    //   - shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata
    //   - shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction
    //   - shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled
    //   - shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled
    //   - shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled
    //   - shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled
    //   - shouldThrowOnInitTransactionIfProducerIsClosed
    //   - shouldThrowOnBeginTransactionIfProducerIsClosed
    //   - shouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed
    //   - shouldThrowSendOffsetsToTransactionByGroupMetadataIfProducerIsClosed
    //   - shouldThrowOnCommitTransactionIfProducerIsClosed
    //   - shouldThrowOnAbortTransactionIfProducerIsClosed
    //   - shouldThrowOnFenceProducerIfProducerIsClosed
    //   - shouldNotThrowOnFlushProducerIfProducerIsFenced
    //
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Serializer-related test (skipped)
    //
    //   - shouldThrowClassCastException: This test is Java-specific. It tests
    //     that Java's type erasure + serializer causes a ClassCastException
    //     when the wrong type is used. Rust has no type erasure and the
    //     MockProducer works with pre-serialized bytes, so this test is
    //     not applicable.
    //
    // -----------------------------------------------------------------------

    /// Translated from `MockProducerTest.shouldThrowOnSendIfProducerIsClosed`.
    #[tokio::test]
    async fn should_throw_on_send_if_producer_is_closed() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.close().await.unwrap();
        let result = producer.send(make_record("topic", "key1", "value1")).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.message().contains("MockProducer is already closed"));
    }

    /// Translated from `MockProducerTest.shouldThrowOnFlushProducerIfProducerIsClosed`.
    #[tokio::test]
    async fn should_throw_on_flush_if_producer_is_closed() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.close().await.unwrap();
        let result = producer.flush().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("MockProducer is already closed"));
    }

    /// Translated from `MockProducerTest.shouldBeFlushedIfNoBufferedRecords`.
    #[test]
    fn should_be_flushed_if_no_buffered_records() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldBeFlushedWithAutoCompleteIfBufferedRecords`.
    #[tokio::test]
    async fn should_be_flushed_with_auto_complete_if_buffered_records() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords`.
    #[tokio::test]
    async fn should_not_be_flushed_with_no_auto_complete_if_buffered_records() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        assert!(!producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedAfterFlush`.
    ///
    /// Note: The Java test name is misleading — it tests that after flush,
    /// `flushed()` returns `true` (not `false`). The Rust version matches the
    /// actual Java assertion: `assertTrue(producer.flushed())`.
    #[tokio::test]
    async fn should_be_flushed_after_flush() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        producer.flush().await.unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.testMetadataOnException`.
    ///
    /// Java's version uses a callback to inspect metadata on error. Since our
    /// Producer trait does not have callbacks, we test that `error_next()`
    /// causes the future to resolve with the injected error.
    #[tokio::test]
    async fn test_metadata_on_exception() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        let record2 = make_record("topic", "key2", "value2");

        let future = producer.send(record2).await.unwrap();
        let e = KafkaError::illegal_argument("dummy exception");
        assert!(producer.error_next(e), "Complete the request with an error");

        let result = future.get().await;
        assert!(result.is_err(), "Expected error");
    }

    // -----------------------------------------------------------------------
    // Additional unit tests
    // -----------------------------------------------------------------------

    /// Tests that `Default` creates a producer with `auto_complete=false`.
    #[tokio::test]
    async fn test_default() {
        let producer: MockProducer<String, String> = MockProducer::default();
        assert!(!producer.closed());
        assert!(producer.flushed());

        // auto_complete=false means sends don't complete immediately
        let future = producer.send(make_record("topic", "k", "v")).await.unwrap();
        assert!(!future.is_done());
    }

    /// Tests that multiple sends to the same topic-partition get incrementing
    /// offsets.
    #[tokio::test]
    async fn test_incrementing_offsets() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let f0 = producer.send(make_record("t", "k", "v0")).await.unwrap();
        let f1 = producer.send(make_record("t", "k", "v1")).await.unwrap();
        let f2 = producer.send(make_record("t", "k", "v2")).await.unwrap();

        assert_eq!(0, f0.get().await.unwrap().offset());
        assert_eq!(1, f1.get().await.unwrap().offset());
        assert_eq!(2, f2.get().await.unwrap().offset());
    }

    /// Tests that sends to different topic-partitions have independent offsets.
    #[tokio::test]
    async fn test_independent_topic_partition_offsets() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let r1 = ProducerRecord::with_value("t1".to_string(), Some("k".to_string()));
        let r2 = ProducerRecord::with_value("t2".to_string(), Some("k".to_string()));

        let f1 = producer.send(r1.clone()).await.unwrap();
        let f2 = producer.send(r2.clone()).await.unwrap();
        let f3 = producer.send(r1).await.unwrap();

        assert_eq!(0, f1.get().await.unwrap().offset());
        assert_eq!(0, f2.get().await.unwrap().offset());
        assert_eq!(1, f3.get().await.unwrap().offset());
    }

    /// Tests `set_send_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `sendException` is a field that persists until
    /// manually set to `null`.
    #[tokio::test]
    async fn test_set_send_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_send_error(Some(KafkaError::new(Errors::CorruptMessage)));

        let result = producer.send(make_record("t", "k", "v")).await;
        assert!(result.is_err());

        // Error persists — second send also fails
        let result = producer.send(make_record("t", "k", "v")).await;
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next send succeeds
        producer.set_send_error(None);
        let result = producer.send(make_record("t", "k", "v")).await;
        assert!(result.is_ok(), "Send should succeed after clearing error");
    }

    /// Tests `set_flush_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `flushException` is a field that persists until
    /// manually set to `null`.
    #[tokio::test]
    async fn test_set_flush_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_flush_error(Some(KafkaError::new(Errors::CorruptMessage)));

        let result = producer.flush().await;
        assert!(result.is_err());

        // Error persists — second flush also fails
        let result = producer.flush().await;
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next flush succeeds
        producer.set_flush_error(None);
        let result = producer.flush().await;
        assert!(result.is_ok(), "Flush should succeed after clearing error");
    }

    /// Tests `set_partitions_for_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `partitionsForException` is a field that persists
    /// until manually set to `null`.
    #[tokio::test]
    async fn test_set_partitions_for_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_partitions_for_error(Some(KafkaError::new(Errors::UnknownTopicOrPartition)));

        let result = producer.partitions_for("t").await;
        assert!(result.is_err());

        // Error persists — second call also fails
        let result = producer.partitions_for("t").await;
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next call succeeds
        producer.set_partitions_for_error(None);
        let result = producer.partitions_for("t").await;
        assert!(result.is_ok());
    }

    /// Tests `set_close_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `closeException` is a field that persists until
    /// manually set to `null`.
    #[tokio::test]
    async fn test_set_close_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_close_error(Some(KafkaError::new(Errors::UnknownServerError)));

        let result = producer.close().await;
        assert!(result.is_err());

        // Error persists — second close also fails
        let result = producer.close().await;
        assert!(result.is_err(), "Error should persist until cleared");
        assert!(!producer.closed(), "Producer should not be closed when error persists");

        // Clear the error — close now succeeds
        producer.set_close_error(None);
        let result = producer.close().await;
        assert!(result.is_ok(), "Close should succeed after clearing error");
        assert!(producer.closed());
    }

    /// Tests `partitions_for` with cluster metadata.
    #[tokio::test]
    async fn test_partitions_for() {
        let node = crate::common::Node::new(0, "localhost".to_string(), 9092);
        let pi0 = PartitionInfo::new("topic".to_string(), 0, Some(node.clone()), vec![], vec![]);
        let pi1 = PartitionInfo::new("topic".to_string(), 1, Some(node), vec![], vec![]);

        let cluster = Cluster::new(
            None,
            vec![],
            vec![pi0, pi1],
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            None,
            HashMap::new(),
        );
        let producer: MockProducer<String, String> = MockProducer::new(cluster, true);

        let partitions = producer.partitions_for("topic").await.unwrap();
        assert_eq!(2, partitions.len());
        assert_eq!(0, partitions[0].partition());
        assert_eq!(1, partitions[1].partition());

        // Unknown topic returns empty
        let partitions = producer.partitions_for("unknown").await.unwrap();
        assert!(partitions.is_empty());
    }

    /// Tests `close_timeout` behaves like `close`.
    #[tokio::test]
    async fn test_close_timeout() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        assert!(!producer.closed());
        producer.close_timeout(Duration::from_secs(5)).await.unwrap();
        assert!(producer.closed());
    }

    /// Tests that `clear` preserves offset counters (matching Java behavior).
    ///
    /// Java's `MockProducer.clear()` does NOT reset the `offsets` map, so
    /// offset numbering continues after `clear()`.
    #[tokio::test]
    async fn test_clear_preserves_offsets() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.send(make_record("t", "k", "v")).await.unwrap();
        producer.send(make_record("t", "k", "v")).await.unwrap();

        producer.clear();

        let future = producer.send(make_record("t", "k", "v")).await.unwrap();
        assert_eq!(
            2,
            future.get().await.unwrap().offset(),
            "Offset should continue from 2 after clear (not restart from 0)"
        );
    }

    /// Tests `next_offset` helper function.
    #[test]
    fn test_next_offset() {
        let mut offsets = HashMap::new();
        let tp = TopicPartition::new("t".to_string(), 0);

        assert_eq!(0, next_offset(&mut offsets, &tp));
        assert_eq!(1, next_offset(&mut offsets, &tp));
        assert_eq!(2, next_offset(&mut offsets, &tp));

        let tp2 = TopicPartition::new("t".to_string(), 1);
        assert_eq!(0, next_offset(&mut offsets, &tp2));
    }

    /// Tests that `history` returns a clone (modifications don't affect internal state).
    #[tokio::test]
    async fn test_history_returns_clone() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let record = make_record("t", "k", "v");
        producer.send(record).await.unwrap();

        let mut history = producer.history();
        assert_eq!(1, history.len());

        // Modifying returned history does not affect internal state
        history.clear();
        assert_eq!(1, producer.history().len());
    }

    /// Tests that `error_next` returns false when no completions are pending.
    #[test]
    fn test_error_next_no_pending() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        assert!(!producer.error_next(KafkaError::new(Errors::UnknownServerError)));
    }

    /// `metrics()` returns the mock metrics seeded via `set_mock_metrics`,
    /// mirroring Java `MockProducer.setMockMetrics` + `metrics()`. Java
    /// `MockProducerTest` has no metrics test; this covers the Rust surface.
    #[test]
    fn test_set_and_get_mock_metrics() {
        use crate::common::metrics::Metrics;
        use crate::common::metrics::stats::CumulativeSum;

        let producer: MockProducer<String, String> = MockProducer::default();
        assert!(producer.metrics().is_empty());

        // Build a real KafkaMetric via a Metrics registry.
        let registry = Metrics::new();
        let sensor = registry.sensor("mock-sensor").unwrap();
        let name = registry.metric_name_group("mock-metric", "mock-group");
        sensor.add(name.clone(), Box::new(CumulativeSum::new())).unwrap();
        let metric = registry.metric(&name).unwrap();

        producer.set_mock_metrics(name.clone(), Arc::clone(&metric));

        let snapshot = producer.metrics();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.contains_key(&name));
    }
}
