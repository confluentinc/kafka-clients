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
use std::sync::Mutex;
use std::time::Duration;

use super::future_record_metadata::{self, CompletionSender};
use super::record_metadata::RecordMetadata;
use super::{FutureRecordMetadata, Producer, ProducerRecord};
use crate::common::cluster::Cluster;
use crate::common::kafka_error::KafkaError;
use crate::common::partition_info::PartitionInfo;
use crate::common::topic_partition::TopicPartition;

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
///
/// # Examples
///
/// ```
/// use confluent_kafka_rust::clients::producer::{MockProducer, Producer, ProducerRecord};
///
/// // Auto-complete mode: sends complete immediately
/// let producer = MockProducer::with_auto_complete(true);
/// let record = ProducerRecord::new("topic").unwrap()
///     .with_key(b"key".to_vec())
///     .with_value(b"value".to_vec());
/// let mut future = producer.send(record).unwrap();
/// assert!(future.is_done());
///
/// // Manual mode: user controls completion
/// let producer = MockProducer::with_auto_complete(false);
/// let record = ProducerRecord::new("topic").unwrap()
///     .with_key(b"key".to_vec())
///     .with_value(b"value".to_vec());
/// let mut future = producer.send(record).unwrap();
/// assert!(!future.is_done());
/// assert!(producer.complete_next());
/// ```
pub struct MockProducer {
    inner: Mutex<MockProducerInner>,
}

struct MockProducerInner {
    cluster: Cluster,
    auto_complete: bool,
    sent: Vec<ProducerRecord>,
    completions: VecDeque<Completion>,
    offsets: HashMap<TopicPartition, i64>,
    closed: bool,
    send_error: Option<KafkaError>,
    flush_error: Option<KafkaError>,
    partitions_for_error: Option<KafkaError>,
    close_error: Option<KafkaError>,
}

/// Internal completion record that holds the state needed to fulfill a
/// [`FutureRecordMetadata`].
///
/// Corresponds to Java's `MockProducer.Completion` inner class.
struct Completion {
    #[allow(dead_code)]
    offset: i64,
    metadata: RecordMetadata,
    sender: Option<CompletionSender>,
    #[allow(dead_code)]
    topic_partition: TopicPartition,
}

impl Completion {
    /// Complete this send with either a success or an error.
    ///
    /// Corresponds to Java's `Completion.complete(RuntimeException)`.
    fn complete(&mut self, error: Option<KafkaError>) {
        if let Some(sender) = self.sender.take() {
            match error {
                Some(e) => {
                    let _ = sender.send(Err(e));
                },
                None => {
                    let _ = sender.send(Ok(self.metadata.clone()));
                },
            }
        }
    }
}

impl MockProducer {
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
    pub fn history(&self) -> Vec<ProducerRecord> {
        let inner = self.inner.lock().unwrap();
        inner.sent.clone()
    }

    /// Clear sent records, completions, and offsets.
    ///
    /// Corresponds to Java's `MockProducer.clear()`.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.sent.clear();
        inner.completions.clear();
        inner.offsets.clear();
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
        if let Some(mut completion) = inner.completions.pop_front() {
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
}

impl Default for MockProducer {
    /// Create a new mock producer with an empty cluster and `auto_complete=false`.
    ///
    /// Corresponds to Java's no-arg `MockProducer()` constructor.
    fn default() -> Self {
        Self::new(Cluster::empty(), false)
    }
}

impl Producer for MockProducer {
    /// Adds the record to the list of sent records.
    ///
    /// If `auto_complete` is `true`, the returned [`FutureRecordMetadata`] will
    /// be immediately resolved. Otherwise, the caller must call
    /// [`complete_next()`](MockProducer::complete_next) or
    /// [`error_next()`](MockProducer::error_next) to resolve it.
    ///
    /// Corresponds to Java's `MockProducer.send(ProducerRecord)`.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if:
    /// - The producer is closed.
    /// - A send error has been injected via [`set_send_error()`](MockProducer::set_send_error).
    fn send(&self, record: ProducerRecord) -> Result<FutureRecordMetadata, KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if inner.closed {
            return Err(KafkaError::illegal_state("MockProducer is already closed."));
        }

        if let Some(err) = inner.send_error.as_ref() {
            return Err(err.clone());
        }

        let partition = record.partition().unwrap_or(0);
        let tp = TopicPartition::new(record.topic().to_string(), partition);
        let offset = next_offset(&mut inner.offsets, &tp);

        // Match Java's offset splitting: baseOffset = max(0, offset - Integer.MAX_VALUE),
        // batchIndex = min(Integer.MAX_VALUE, offset).
        let base_offset = 0i64.max(offset - i64::from(i32::MAX));
        let batch_index = (offset.min(i64::from(i32::MAX))) as i32;

        let metadata = RecordMetadata::new(tp.clone(), base_offset, batch_index, -1 /* NO_TIMESTAMP */, 0, 0);

        inner.sent.push(record);

        if inner.auto_complete {
            // In auto-complete mode, create a pre-resolved future so that
            // `is_done()` returns `true` immediately — matching Java's
            // behavior where `Future.isDone()` is true after a synchronous
            // completion.
            Ok(FutureRecordMetadata::completed(Ok(metadata)))
        } else {
            let (sender, future) = future_record_metadata::create();
            let completion = Completion { offset, metadata, sender: Some(sender), topic_partition: tp };
            inner.completions.push_back(completion);
            Ok(future)
        }
    }

    /// Flush all pending completions.
    ///
    /// Corresponds to Java's `MockProducer.flush()`.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if:
    /// - The producer is closed.
    /// - A flush error has been injected via [`set_flush_error()`](MockProducer::set_flush_error).
    fn flush(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if inner.closed {
            return Err(KafkaError::illegal_state("MockProducer is already closed."));
        }

        if let Some(err) = inner.flush_error.as_ref() {
            return Err(err.clone());
        }

        while let Some(mut completion) = inner.completions.pop_front() {
            completion.complete(None);
        }

        Ok(())
    }

    /// Get the partition metadata for a topic.
    ///
    /// Corresponds to Java's `MockProducer.partitionsFor(String)`.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if a partitions_for error has been injected
    /// via [`set_partitions_for_error()`](MockProducer::set_partitions_for_error).
    fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let inner = self.inner.lock().unwrap();

        if let Some(err) = inner.partitions_for_error.as_ref() {
            return Err(err.clone());
        }

        Ok(inner.cluster.partitions_for_topic(topic).to_vec())
    }

    /// Close this producer.
    ///
    /// Corresponds to Java's `MockProducer.close()`.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if a close error has been injected
    /// via [`set_close_error()`](MockProducer::set_close_error).
    fn close(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if let Some(err) = inner.close_error.as_ref() {
            return Err(err.clone());
        }

        inner.closed = true;
        Ok(())
    }

    /// Close this producer with a timeout.
    ///
    /// The timeout is ignored since there is no real I/O to wait for.
    ///
    /// Corresponds to Java's `MockProducer.close(Duration)`.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if a close error has been injected
    /// via [`set_close_error()`](MockProducer::set_close_error).
    fn close_with_timeout(&self, _timeout: Duration) -> Result<(), KafkaError> {
        self.close()
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

    fn make_record(topic: &str, key: &str, value: &str) -> ProducerRecord {
        ProducerRecord::new(topic)
            .unwrap()
            .with_key(key.as_bytes().to_vec())
            .with_value(value.as_bytes().to_vec())
    }

    // -----------------------------------------------------------------------
    // Tests translated from MockProducerTest.java
    // -----------------------------------------------------------------------

    /// Translated from `MockProducerTest.testAutoCompleteMock`.
    #[tokio::test]
    async fn test_auto_complete_mock() {
        let producer = MockProducer::with_auto_complete(true);
        let record1 = make_record("topic", "key1", "value1");

        let mut future = producer.send(record1.clone()).unwrap();
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
        let producer = MockProducer::new(cluster, true);

        // Send with explicit partition=1
        let record = ProducerRecord::new("topic")
            .unwrap()
            .with_partition(1)
            .unwrap()
            .with_key(b"key".to_vec())
            .with_value(b"value".to_vec());
        let mut future = producer.send(record).unwrap();
        let md = future.get().await.unwrap();
        assert_eq!(1, md.partition(), "Partition should be correct");

        producer.clear();
        assert_eq!(0, producer.history().len(), "Clear should erase our history");
        producer.close().unwrap();
    }

    /// Translated from `MockProducerTest.testManualCompletion`.
    #[tokio::test]
    async fn test_manual_completion() {
        let producer = MockProducer::with_auto_complete(false);
        let record1 = make_record("topic", "key1", "value1");
        let record2 = make_record("topic", "key2", "value2");

        let mut md1 = producer.send(record1.clone()).unwrap();
        assert!(!md1.is_done(), "Send shouldn't have completed");

        let mut md2 = producer.send(record2.clone()).unwrap();
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
        let mut md3 = producer.send(record1).unwrap();
        let mut md4 = producer.send(record2).unwrap();
        assert!(!md3.is_done() && !md4.is_done(), "Requests should not be completed.");
        producer.flush().unwrap();
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
    #[test]
    fn should_throw_on_send_if_producer_is_closed() {
        let producer = MockProducer::with_auto_complete(true);
        producer.close().unwrap();
        let result = producer.send(make_record("topic", "key1", "value1"));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.message().contains("MockProducer is already closed"));
    }

    /// Translated from `MockProducerTest.shouldThrowOnFlushProducerIfProducerIsClosed`.
    #[test]
    fn should_throw_on_flush_if_producer_is_closed() {
        let producer = MockProducer::with_auto_complete(true);
        producer.close().unwrap();
        let result = producer.flush();
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("MockProducer is already closed"));
    }

    /// Translated from `MockProducerTest.shouldBeFlushedIfNoBufferedRecords`.
    #[test]
    fn should_be_flushed_if_no_buffered_records() {
        let producer = MockProducer::with_auto_complete(true);
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldBeFlushedWithAutoCompleteIfBufferedRecords`.
    #[test]
    fn should_be_flushed_with_auto_complete_if_buffered_records() {
        let producer = MockProducer::with_auto_complete(true);
        producer.send(make_record("topic", "key1", "value1")).unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords`.
    #[test]
    fn should_not_be_flushed_with_no_auto_complete_if_buffered_records() {
        let producer = MockProducer::with_auto_complete(false);
        producer.send(make_record("topic", "key1", "value1")).unwrap();
        assert!(!producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedAfterFlush`.
    ///
    /// Note: The Java test name is misleading — it tests that after flush,
    /// `flushed()` returns `true` (not `false`). The Rust version matches the
    /// actual Java assertion: `assertTrue(producer.flushed())`.
    #[test]
    fn should_be_flushed_after_flush() {
        let producer = MockProducer::with_auto_complete(false);
        producer.send(make_record("topic", "key1", "value1")).unwrap();
        producer.flush().unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.testMetadataOnException`.
    ///
    /// Java's version uses a callback to inspect metadata on error. Since our
    /// Producer trait does not have callbacks, we test that `error_next()`
    /// causes the future to resolve with the injected error.
    #[tokio::test]
    async fn test_metadata_on_exception() {
        let producer = MockProducer::with_auto_complete(false);
        let record2 = make_record("topic", "key2", "value2");

        let mut future = producer.send(record2).unwrap();
        let e = KafkaError::illegal_argument("dummy exception");
        assert!(producer.error_next(e), "Complete the request with an error");

        let result = future.get().await;
        assert!(result.is_err(), "Expected error");
    }

    // -----------------------------------------------------------------------
    // Additional unit tests
    // -----------------------------------------------------------------------

    /// Tests that `Default` creates a producer with `auto_complete=false`.
    #[test]
    fn test_default() {
        let producer = MockProducer::default();
        assert!(!producer.closed());
        assert!(producer.flushed());

        // auto_complete=false means sends don't complete immediately
        let mut future = producer.send(make_record("topic", "k", "v")).unwrap();
        assert!(!future.is_done());
    }

    /// Tests that multiple sends to the same topic-partition get incrementing
    /// offsets.
    #[tokio::test]
    async fn test_incrementing_offsets() {
        let producer = MockProducer::with_auto_complete(true);
        let mut f0 = producer.send(make_record("t", "k", "v0")).unwrap();
        let mut f1 = producer.send(make_record("t", "k", "v1")).unwrap();
        let mut f2 = producer.send(make_record("t", "k", "v2")).unwrap();

        assert_eq!(0, f0.get().await.unwrap().offset());
        assert_eq!(1, f1.get().await.unwrap().offset());
        assert_eq!(2, f2.get().await.unwrap().offset());
    }

    /// Tests that sends to different topic-partitions have independent offsets.
    #[tokio::test]
    async fn test_independent_topic_partition_offsets() {
        let producer = MockProducer::with_auto_complete(true);
        let r1 = ProducerRecord::new("t1").unwrap().with_key(b"k".to_vec());
        let r2 = ProducerRecord::new("t2").unwrap().with_key(b"k".to_vec());

        let mut f1 = producer.send(r1.clone()).unwrap();
        let mut f2 = producer.send(r2.clone()).unwrap();
        let mut f3 = producer.send(r1).unwrap();

        assert_eq!(0, f1.get().await.unwrap().offset());
        assert_eq!(0, f2.get().await.unwrap().offset());
        assert_eq!(1, f3.get().await.unwrap().offset());
    }

    /// Tests `set_send_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `sendException` is a field that persists until
    /// manually set to `null`.
    #[test]
    fn test_set_send_error() {
        let producer = MockProducer::with_auto_complete(true);
        producer.set_send_error(Some(KafkaError::new(Errors::CorruptMessage)));

        let result = producer.send(make_record("t", "k", "v"));
        assert!(result.is_err());

        // Error persists — second send also fails
        let result = producer.send(make_record("t", "k", "v"));
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next send succeeds
        producer.set_send_error(None);
        let result = producer.send(make_record("t", "k", "v"));
        assert!(result.is_ok(), "Send should succeed after clearing error");
    }

    /// Tests `set_flush_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `flushException` is a field that persists until
    /// manually set to `null`.
    #[test]
    fn test_set_flush_error() {
        let producer = MockProducer::with_auto_complete(true);
        producer.set_flush_error(Some(KafkaError::new(Errors::CorruptMessage)));

        let result = producer.flush();
        assert!(result.is_err());

        // Error persists — second flush also fails
        let result = producer.flush();
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next flush succeeds
        producer.set_flush_error(None);
        let result = producer.flush();
        assert!(result.is_ok(), "Flush should succeed after clearing error");
    }

    /// Tests `set_partitions_for_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `partitionsForException` is a field that persists
    /// until manually set to `null`.
    #[test]
    fn test_set_partitions_for_error() {
        let producer = MockProducer::with_auto_complete(true);
        producer.set_partitions_for_error(Some(KafkaError::new(Errors::UnknownTopicOrPartition)));

        let result = producer.partitions_for("t");
        assert!(result.is_err());

        // Error persists — second call also fails
        let result = producer.partitions_for("t");
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next call succeeds
        producer.set_partitions_for_error(None);
        let result = producer.partitions_for("t");
        assert!(result.is_ok(), "partitions_for should succeed after clearing error");
    }

    /// Tests `set_close_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `closeException` is a field that persists until
    /// manually set to `null`.
    #[test]
    fn test_set_close_error() {
        let producer = MockProducer::with_auto_complete(true);
        producer.set_close_error(Some(KafkaError::new(Errors::UnknownServerError)));

        let result = producer.close();
        assert!(result.is_err());

        // Error persists — second close also fails
        let result = producer.close();
        assert!(result.is_err(), "Error should persist until cleared");
        assert!(!producer.closed(), "Producer should not be closed when error persists");

        // Clear the error — close now succeeds
        producer.set_close_error(None);
        let result = producer.close();
        assert!(result.is_ok(), "Close should succeed after clearing error");
        assert!(producer.closed());
    }

    /// Tests `partitions_for` with cluster metadata.
    #[test]
    fn test_partitions_for() {
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
        let producer = MockProducer::new(cluster, true);

        let partitions = producer.partitions_for("topic").unwrap();
        assert_eq!(2, partitions.len());
        assert_eq!(0, partitions[0].partition());
        assert_eq!(1, partitions[1].partition());

        // Unknown topic returns empty
        let partitions = producer.partitions_for("unknown").unwrap();
        assert!(partitions.is_empty());
    }

    /// Tests `close_with_timeout` behaves like `close`.
    #[test]
    fn test_close_with_timeout() {
        let producer = MockProducer::with_auto_complete(true);
        assert!(!producer.closed());
        producer.close_with_timeout(Duration::from_secs(5)).unwrap();
        assert!(producer.closed());
    }

    /// Tests that `clear` resets offsets so they restart from 0.
    #[tokio::test]
    async fn test_clear_resets_offsets() {
        let producer = MockProducer::with_auto_complete(true);
        producer.send(make_record("t", "k", "v")).unwrap();
        producer.send(make_record("t", "k", "v")).unwrap();

        producer.clear();

        let mut future = producer.send(make_record("t", "k", "v")).unwrap();
        assert_eq!(
            0,
            future.get().await.unwrap().offset(),
            "Offset should restart from 0 after clear"
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
    #[test]
    fn test_history_returns_clone() {
        let producer = MockProducer::with_auto_complete(true);
        let record = make_record("t", "k", "v");
        producer.send(record.clone()).unwrap();

        let mut history = producer.history();
        assert_eq!(1, history.len());

        // Modifying returned history does not affect internal state
        history.clear();
        assert_eq!(1, producer.history().len());
    }

    /// Tests that `error_next` returns false when no completions are pending.
    #[test]
    fn test_error_next_no_pending() {
        let producer = MockProducer::with_auto_complete(false);
        assert!(!producer.error_next(KafkaError::new(Errors::UnknownServerError)));
    }
}
