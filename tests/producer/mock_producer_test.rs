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

//! External tests for [`MockProducer`].
//!
//! These tests mirror `org.apache.kafka.clients.producer.MockProducerTest`
//! (non-transactional, non-callback, non-partitioner, non-serializer subset).
//! They exercise the public API through the library crate boundary, verifying
//! that the types are properly exported and usable by downstream consumers.

use confluent_kafka::producer::MockProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn make_record(topic: &str, key: &str, value: &str) -> ProducerRecord<String, String> {
    ProducerRecord::with_key(topic.to_string(), Some(key.to_string()), Some(value.to_string()))
}

// ---------------------------------------------------------------------------
// Tests translated from MockProducerTest.java
// ---------------------------------------------------------------------------

/// Translated from `MockProducerTest.testAutoCompleteMock` (line 73).
///
/// Creates a MockProducer with `auto_complete=true`, sends a record, and
/// verifies:
/// - the future resolves immediately (`is_done()` returns `true`)
/// - offset and topic are correct
/// - `history()` contains the record
/// - `clear()` empties the history
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

    let history = producer.history();
    assert_eq!(1, history.len(), "We should have the record in our history");
    assert_eq!(record1, history[0]);

    // Matches Java: producer.clear(); assertEquals(0, producer.history().size())
    producer.clear();
    assert!(producer.history().is_empty(), "Clear should erase our history");
}

/// Translated from `MockProducerTest.testManualCompletion` (line 107).
///
/// Creates a MockProducer with `auto_complete=false`, sends two records, and
/// verifies:
/// - futures are not done until explicitly completed
/// - `complete_next()` resolves the first future with `Ok`
/// - `error_next(error)` resolves the second future with `Err`
/// - `complete_next()` returns `false` when there are no more pending requests
/// - `flush()` completes remaining pending sends
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

    let e = confluent_kafka::common::Error::local_illegal_argument("blah");
    assert!(producer.error_next(e), "Complete the second request with an error");
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

/// Translated from `MockProducerTest.shouldBeFlushedIfNoBufferedRecords` (line 696).
///
/// Creates a MockProducer with `auto_complete=true` and verifies that
/// `flushed()` returns `true` before any sends (no pending completions).
#[test]
fn test_should_be_flushed_if_no_buffered_records() {
    let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
    assert!(producer.flushed());
}

/// Translated from `MockProducerTest.shouldBeFlushedWithAutoCompleteIfBufferedRecords` (line 702).
///
/// Creates a MockProducer with `auto_complete=true`, sends a record, and
/// verifies that `flushed()` is still `true` because auto-complete resolves
/// sends immediately without leaving pending completions.
#[tokio::test]
async fn test_should_be_flushed_with_auto_complete_if_buffered_records() {
    let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
    producer.send(make_record("topic", "key1", "value1")).await.unwrap();
    assert!(producer.flushed());
}

/// Translated from `MockProducerTest.shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords` (line 709).
///
/// Creates a MockProducer with `auto_complete=false`, sends a record, and
/// verifies that `flushed()` returns `false` because the send is pending.
#[tokio::test]
async fn test_should_not_be_flushed_with_no_auto_complete_if_buffered_records() {
    let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
    producer.send(make_record("topic", "key1", "value1")).await.unwrap();
    assert!(!producer.flushed());
}

/// Translated from `MockProducerTest.shouldNotBeFlushedAfterFlush` (line 716).
///
/// Note: The Java test name is misleading -- it actually asserts that after
/// calling `flush()`, `flushed()` returns `true`. The Rust version matches
/// the actual Java assertion: `assertTrue(producer.flushed())`.
///
/// Creates a MockProducer with `auto_complete=false`, sends a record,
/// verifies not flushed, calls `flush()`, then verifies flushed.
#[tokio::test]
async fn test_should_be_flushed_after_flush() {
    let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
    producer.send(make_record("topic", "key1", "value1")).await.unwrap();
    assert!(!producer.flushed(), "Should not be flushed with pending send");
    producer.flush().await.unwrap();
    assert!(producer.flushed(), "Should be flushed after flush()");
}

/// Translated from `MockProducerTest.shouldThrowOnSendIfProducerIsClosed` (line 624).
///
/// Creates a MockProducer, closes it, then sends a record and verifies
/// the send returns `Err` with a message indicating the producer is closed.
#[tokio::test]
async fn test_should_throw_on_send_if_producer_is_closed() {
    let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
    producer.close().await.unwrap();
    let result = producer.send(make_record("topic", "key1", "value1")).await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        err.message().contains("MockProducer is already closed"),
        "Error message should indicate producer is closed, got: {}",
        err.message()
    );
}

/// Translated from `MockProducerTest.shouldThrowOnFlushProducerIfProducerIsClosed` (line 673).
///
/// Creates a MockProducer, closes it, then calls `flush()` and verifies
/// it returns `Err` with a message indicating the producer is closed.
#[tokio::test]
async fn test_should_throw_on_flush_if_producer_is_closed() {
    let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
    producer.close().await.unwrap();
    let result = producer.flush().await;
    assert!(result.is_err());
    assert!(
        result.unwrap_err().message().contains("MockProducer is already closed"),
        "Error message should indicate producer is closed"
    );
}

// ---------------------------------------------------------------------------
// Excluded tests (46 tests from MockProducerTest.java)
//
// The following Java tests are excluded because the Rust Producer trait does
// not include transactional, callback, partitioner, or serializer methods.
//
// ## Transactional lifecycle (10 tests)
//   - shouldInitTransactions
//   - shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions
//   - shouldBeginTransactions
//   - shouldThrowOnBeginTransactionIfTransactionsNotInitialized
//   - shouldThrowOnBeginTransactionsIfTransactionInflight
//   - shouldCommitEmptyTransaction
//   - shouldCountCommittedTransaction
//   - shouldNotCountAbortedTransaction
//   - shouldAbortEmptyTransaction
//   - shouldThrowOnAbortIfTransactionsNotInitialized
//
// ## Transactional send behavior (5 tests)
//   - shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled
//   - shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled
//   - shouldDropMessagesOnAbortIfTransactionsAreEnabled
//   - shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled
//   - shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled
//
// ## Consumer group offsets (10 tests)
//   - shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled
//   - shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction
//   - shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata
//   - shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata
//   - shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction
//   - shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled
//   - shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled
//   - shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled
//   - shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled
//   - shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized
//
// ## Transactional preconditions (4 tests)
//   - shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted
//   - shouldThrowOnCommitIfTransactionsNotInitialized
//   - shouldThrowOnCommitTransactionIfNoTransactionGotStarted
//   - shouldThrowOnAbortTransactionIfNoTransactionGotStarted
//
// ## Fencing (8 tests)
//   - shouldThrowFenceProducerIfTransactionsNotInitialized
//   - shouldThrowOnBeginTransactionsIfProducerGotFenced
//   - shouldThrowOnSendIfProducerGotFenced
//   - shouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced
//   - shouldThrowOnSendOffsetsToTransactionByGroupMetadataIfProducerGotFenced
//   - shouldThrowOnCommitTransactionIfProducerGotFenced
//   - shouldThrowOnAbortTransactionIfProducerGotFenced
//   - shouldNotThrowOnFlushProducerIfProducerIsFenced
//
// ## Closed + transactional (7 tests)
//   - shouldThrowOnInitTransactionIfProducerIsClosed
//   - shouldThrowOnBeginTransactionIfProducerIsClosed
//   - shouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed
//   - shouldThrowSendOffsetsToTransactionByGroupMetadataIfProducerIsClosed
//   - shouldThrowOnCommitTransactionIfProducerIsClosed
//   - shouldThrowOnAbortTransactionIfProducerIsClosed
//   - shouldThrowOnFenceProducerIfProducerIsClosed
//
// ## Callback (1 test)
//   - testMetadataOnException: Uses a send callback to inspect metadata on
//     error. The Rust Producer trait does not have a callback-based send
//     overload.
//
// ## Partitioner (1 test)
//   - testPartitioner: Uses a RoundRobinPartitioner. The Rust MockProducer
//     works with pre-serialized bytes and uses record.partition() directly
//     instead of a pluggable partitioner.
//
// ## Serializer (1 test)
//   - shouldThrowClassCastException: Java-specific test exercising type
//     erasure + serializer ClassCastException. Rust has no type erasure
//     and the MockProducer works with pre-serialized bytes.
//
// Total excluded: 46 tests (+ 1 testPartitioner already tested inline)
// ---------------------------------------------------------------------------
