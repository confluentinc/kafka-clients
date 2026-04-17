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

//! The future result of a record send.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.FutureRecordMetadata`.
//!
//! In Java, `FutureRecordMetadata` implements `Future<RecordMetadata>`. In Rust, we provide
//! async methods that return `Result<RecordMetadata, KafkaError>`.
//!
//! Each `FutureRecordMetadata` holds an `Arc<ProduceRequestResult>` (shared per batch) and
//! its own record-specific metadata (batch index, timestamp, sizes). It waits for the
//! `ProduceRequestResult` to complete, then constructs the `RecordMetadata`.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::kafka_future::KafkaFutureOps;
use crate::common::record::RecordBatch;
use crate::producer::RecordMetadata;
use crate::producer::internals::ProduceRequestResult;

/// The future result of a record send.
///
/// This is returned to the caller of `KafkaProducer.send()` and allows them to await
/// the result of their produce request.
///
/// When a batch is split into smaller batches, the [`chain`](Self::chain) method redirects
/// this future to wait on the new split batch's result instead.
pub struct FutureRecordMetadata {
    /// The produce request result shared across all records in the batch.
    result: Arc<ProduceRequestResult>,
    /// The index of this record within the batch.
    batch_index: i32,
    /// The create timestamp provided by the user or assigned by the producer.
    create_timestamp: i64,
    /// The size of the serialized key in bytes. -1 if null.
    serialized_key_size: i32,
    /// The size of the serialized value in bytes. -1 if null.
    serialized_value_size: i32,
    /// Chained future for when a batch is split. Protected by a mutex because
    /// `chain()` can be called from a different task than `get()`.
    ///
    /// Uses `Arc` so the chain pointer can be cloned out of the lock and awaited
    /// without holding the `MutexGuard` across `.await` points.
    next_record_metadata: Mutex<Option<Arc<FutureRecordMetadata>>>,
}

impl FutureRecordMetadata {
    /// Create a new `FutureRecordMetadata`.
    ///
    /// # Arguments
    ///
    /// * `result` - The produce request result shared by all records in the batch
    /// * `batch_index` - The index of this record within the batch
    /// * `create_timestamp` - The timestamp assigned to this record
    /// * `serialized_key_size` - Size of the serialized key (-1 if null)
    /// * `serialized_value_size` - Size of the serialized value (-1 if null)
    pub fn new(
        result: Arc<ProduceRequestResult>,
        batch_index: i32,
        create_timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
    ) -> Self {
        Self {
            result,
            batch_index,
            create_timestamp,
            serialized_key_size,
            serialized_value_size,
            next_record_metadata: Mutex::new(None),
        }
    }

    /// Create a `FutureRecordMetadata` that is already completed with an error.
    ///
    /// This is the Rust equivalent of Java's `KafkaProducer.FutureFailure`.
    /// When `do_send` catches an `ApiException`, it returns this so the caller
    /// gets back a future whose `get()` immediately returns the error.
    pub fn failed(topic_partition: TopicPartition, error: KafkaError) -> Self {
        let result = Arc::new(ProduceRequestResult::new(topic_partition));
        let error_clone = error.clone();
        let error_fn: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> =
            Arc::new(move |_| Some(error_clone.clone()));
        result.set(-1, RecordBatch::NO_TIMESTAMP, Some(error_fn));
        result.done();
        Self {
            result,
            batch_index: 0,
            create_timestamp: RecordBatch::NO_TIMESTAMP,
            serialized_key_size: -1,
            serialized_value_size: -1,
            next_record_metadata: Mutex::new(None),
        }
    }

    /// Await the completion of this record's produce request and return the metadata.
    ///
    /// This is the Rust equivalent of Java's `Future.get()`.
    ///
    /// Follows Java's await-then-read pattern: first awaits the current node's result,
    /// THEN checks for a chained future (set during batch splitting). This ordering
    /// is critical because `chain()` may be called between the user calling `get()`
    /// and the result completing. Reading `next_record_metadata` AFTER `await_completion()`
    /// guarantees the chain is visible if it was set before `done()`.
    ///
    /// # Errors
    ///
    /// Returns the error from the produce response if the record failed.
    pub fn get(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RecordMetadata, KafkaError>> + Send + '_>> {
        Box::pin(async move {
            // Step 1: Await THIS node's result (no lock held across await)
            self.result.await_completion().await;

            // Step 2: AFTER awaiting, check if there's a chained future.
            // This read-after-await ordering matches Java's volatile read of
            // `nextRecordMetadata` after `this.result.await()`.
            // Clone the Arc out of the lock so we don't hold the MutexGuard across await.
            let next = {
                let guard = self.next_record_metadata.lock().unwrap();
                guard.as_ref().map(Arc::clone)
            };

            if let Some(chained) = next {
                // Delegate to the chained future (recursive, matching Java's pattern)
                return chained.get().await;
            }

            self.value_or_error()
        })
    }

    /// Await the completion of this record's produce request with a timeout.
    ///
    /// This is the Rust equivalent of Java's `Future.get(timeout, unit)`.
    ///
    /// Follows the same await-then-read pattern as [`get`](Self::get): first awaits
    /// the current result with a timeout, THEN checks for a chained future and
    /// delegates to it with the remaining time.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum time to wait
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Timeout`] if the timeout elapses before the result is available.
    /// Returns the error from the produce response if the record failed.
    pub fn get_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RecordMetadata, KafkaError>> + Send + '_>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;

            // Step 1: Await THIS node's result with timeout
            let occurred = self.result.await_timeout(timeout).await;
            if !occurred {
                return Err(KafkaError::timeout(format!(
                    "Timeout after waiting for {} ms.",
                    timeout.as_millis()
                )));
            }

            // Step 2: AFTER awaiting, check for chained future (read-after-await)
            // Clone the Arc out of the lock so we don't hold the MutexGuard across await.
            let next = {
                let guard = self.next_record_metadata.lock().unwrap();
                guard.as_ref().map(Arc::clone)
            };

            if let Some(chained) = next {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                return chained.get_timeout(remaining).await;
            }

            self.value_or_error()
        })
    }

    /// Check for errors and return metadata or error.
    ///
    /// Returns the typed [`KafkaError`] directly from the produce result,
    /// preserving error code information (retriable, fatal, etc.) through the
    /// pipeline. This matches Java's `valueOrError()` which wraps the
    /// `RuntimeException` in an `ExecutionException`.
    fn value_or_error(&self) -> Result<RecordMetadata, KafkaError> {
        if let Some(error) = self.result.error(self.batch_index) {
            Err(error)
        } else {
            Ok(self.to_metadata())
        }
    }

    /// Construct `RecordMetadata` from this node's data.
    ///
    /// This is the synchronous equivalent of Java's `FutureRecordMetadata.value()`.
    /// It returns the metadata based on the current state of the result.
    /// Should only be called after the result has been set (via `set()`).
    pub fn value(&self) -> RecordMetadata {
        self.to_metadata()
    }

    /// Construct `RecordMetadata` from this node's data after its result has completed.
    fn to_metadata(&self) -> RecordMetadata {
        let timestamp = if self.result.has_log_append_time() {
            self.result.log_append_time()
        } else {
            self.create_timestamp
        };

        RecordMetadata::new(
            self.result.topic_partition().clone(),
            self.result.base_offset().unwrap_or(-1),
            self.batch_index,
            timestamp,
            self.serialized_key_size,
            self.serialized_value_size,
        )
    }

    /// Chain this future to wait on a different `FutureRecordMetadata`.
    ///
    /// This method is used when we have to split a large batch into smaller ones.
    /// A chained metadata will allow the future that has already been returned to the
    /// users to wait on the newly created split batches even after the old big batch
    /// has been deemed as done.
    pub fn chain(&self, future_record_metadata: FutureRecordMetadata) {
        self.chain_arc(Arc::new(future_record_metadata));
    }

    /// Chain this future to wait on an `Arc<FutureRecordMetadata>`.
    pub fn chain_arc(&self, future_record_metadata: Arc<FutureRecordMetadata>) {
        let mut next = self.next_record_metadata.lock().unwrap();
        if next.is_none() {
            *next = Some(future_record_metadata);
        } else {
            next.as_ref().unwrap().chain_arc(future_record_metadata);
        }
    }

    /// Whether this future is complete.
    ///
    /// This is the Rust equivalent of Java's `Future.isDone()`.
    /// If chained, checks whether the chained future's result is completed.
    /// Matches Java's pattern of reading `nextRecordMetadata` and delegating.
    pub fn is_done(&self) -> bool {
        let guard = self.next_record_metadata.lock().unwrap();
        if let Some(next) = guard.as_ref() {
            next.is_done()
        } else {
            self.result.completed()
        }
    }
}

impl KafkaFutureOps<RecordMetadata> for FutureRecordMetadata {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<RecordMetadata, KafkaError>> + Send + '_>> {
        FutureRecordMetadata::get(self)
    }

    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<RecordMetadata, KafkaError>> + Send + '_>> {
        FutureRecordMetadata::get_timeout(self, timeout)
    }

    fn is_done(&self) -> bool {
        FutureRecordMetadata::is_done(self)
    }
}

impl std::fmt::Debug for FutureRecordMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FutureRecordMetadata")
            .field("batch_index", &self.batch_index)
            .field("create_timestamp", &self.create_timestamp)
            .field("serialized_key_size", &self.serialized_key_size)
            .field("serialized_value_size", &self.serialized_value_size)
            .field("is_done", &self.is_done())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TopicPartition;
    use crate::common::record::RecordBatch;

    fn make_result(tp: TopicPartition) -> Arc<ProduceRequestResult> {
        Arc::new(ProduceRequestResult::new(tp))
    }

    /// Translated from `FutureRecordMetadataTest.testFutureGetWithSeconds` (adapted).
    ///
    /// The Java test uses mocks to verify `await(timeout, unit)` is called with the
    /// correct timeout values when chaining. In Rust, we verify the equivalent behavior
    /// by testing that a chained future correctly delegates to the chained result.
    #[tokio::test]
    async fn test_future_get_with_chained_result() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result1 = make_result(tp.clone());
        let result2 = make_result(tp);

        let future1 = FutureRecordMetadata::new(Arc::clone(&result1), 0, RecordBatch::NO_TIMESTAMP, 0, 0);
        let chained = FutureRecordMetadata::new(Arc::clone(&result2), 0, RecordBatch::NO_TIMESTAMP, 0, 0);

        future1.chain(chained);

        // Complete both results
        result1.set(100, RecordBatch::NO_TIMESTAMP, None);
        result1.done();
        result2.set(200, RecordBatch::NO_TIMESTAMP, None);
        result2.done();

        // The chained future should return the result from result2
        let metadata = future1.get().await.unwrap();
        assert_eq!(200, metadata.offset());
    }

    /// Translated from `FutureRecordMetadataTest.testFutureGetWithMilliSeconds` (adapted).
    ///
    /// Test that get with timeout works correctly through a chain.
    #[tokio::test]
    async fn test_future_get_with_timeout_chained() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result1 = make_result(tp.clone());
        let result2 = make_result(tp);

        let future1 = FutureRecordMetadata::new(Arc::clone(&result1), 0, RecordBatch::NO_TIMESTAMP, 0, 0);
        let chained = FutureRecordMetadata::new(Arc::clone(&result2), 0, RecordBatch::NO_TIMESTAMP, 0, 0);

        future1.chain(chained);

        // Complete both results
        result1.set(100, RecordBatch::NO_TIMESTAMP, None);
        result1.done();
        result2.set(200, RecordBatch::NO_TIMESTAMP, None);
        result2.done();

        let metadata = future1.get_timeout(std::time::Duration::from_secs(1)).await.unwrap();
        assert_eq!(200, metadata.offset());
    }

    #[tokio::test]
    async fn test_future_get_basic() {
        let tp = TopicPartition::new("test-topic".to_string(), 0);
        let result = make_result(tp);

        let future = FutureRecordMetadata::new(Arc::clone(&result), 3, 1234567890, 10, 20);

        assert!(!future.is_done());

        result.set(100, RecordBatch::NO_TIMESTAMP, None);
        result.done();

        assert!(future.is_done());

        let metadata = future.get().await.unwrap();
        assert_eq!(103, metadata.offset()); // base_offset(100) + batch_index(3)
        assert_eq!(1234567890, metadata.timestamp());
        assert_eq!(10, metadata.serialized_key_size());
        assert_eq!(20, metadata.serialized_value_size());
    }

    #[tokio::test]
    async fn test_future_get_with_log_append_time() {
        let tp = TopicPartition::new("test-topic".to_string(), 0);
        let result = make_result(tp);

        let future = FutureRecordMetadata::new(Arc::clone(&result), 0, 1000, 0, 0);

        result.set(0, 9999, None);
        result.done();

        let metadata = future.get().await.unwrap();
        // Should use log_append_time (9999) instead of create_timestamp (1000)
        assert_eq!(9999, metadata.timestamp());
    }

    #[tokio::test]
    async fn test_future_get_with_error() {
        use crate::common::protocol::Errors;

        let tp = TopicPartition::new("test-topic".to_string(), 0);
        let result = make_result(tp);

        let future = FutureRecordMetadata::new(Arc::clone(&result), 0, 1000, 0, 0);

        let error_fn: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> = Arc::new(|idx| {
            if idx == 0 {
                Some(KafkaError::new(Errors::RecordListTooLarge))
            } else {
                None
            }
        });
        result.set(-1, RecordBatch::NO_TIMESTAMP, Some(error_fn));
        result.done();

        let err = future.get().await.unwrap_err();
        assert_eq!(Errors::RecordListTooLarge, err.error());
    }

    #[tokio::test]
    async fn test_future_get_timeout_expires() {
        let tp = TopicPartition::new("test-topic".to_string(), 0);
        let result = make_result(tp);

        let future = FutureRecordMetadata::new(Arc::clone(&result), 0, 1000, 0, 0);

        let err = future.get_timeout(std::time::Duration::from_millis(10)).await.unwrap_err();
        assert!(matches!(err, KafkaError::Timeout(_)));
    }

    #[tokio::test]
    async fn test_is_done_with_chain() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result1 = make_result(tp.clone());
        let result2 = make_result(tp);

        let future1 = FutureRecordMetadata::new(Arc::clone(&result1), 0, RecordBatch::NO_TIMESTAMP, 0, 0);
        let chained = FutureRecordMetadata::new(Arc::clone(&result2), 0, RecordBatch::NO_TIMESTAMP, 0, 0);

        future1.chain(chained);

        // Even if result1 is done, isDone checks the chained future
        result1.set(0, RecordBatch::NO_TIMESTAMP, None);
        result1.done();
        assert!(!future1.is_done(), "Chained future is not done yet");

        result2.set(0, RecordBatch::NO_TIMESTAMP, None);
        result2.done();
        assert!(future1.is_done(), "Both futures are now done");
    }

    #[tokio::test]
    async fn test_double_chain() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result1 = make_result(tp.clone());
        let result2 = make_result(tp.clone());
        let result3 = make_result(tp);

        let future1 = FutureRecordMetadata::new(Arc::clone(&result1), 0, RecordBatch::NO_TIMESTAMP, 0, 0);
        let chained1 = FutureRecordMetadata::new(Arc::clone(&result2), 0, RecordBatch::NO_TIMESTAMP, 0, 0);
        let chained2 = FutureRecordMetadata::new(Arc::clone(&result3), 0, RecordBatch::NO_TIMESTAMP, 0, 0);

        future1.chain(chained1);
        future1.chain(chained2);

        result1.set(100, RecordBatch::NO_TIMESTAMP, None);
        result1.done();
        result2.set(200, RecordBatch::NO_TIMESTAMP, None);
        result2.done();
        result3.set(300, RecordBatch::NO_TIMESTAMP, None);
        result3.done();

        // Should follow the chain to the last result
        let metadata = future1.get().await.unwrap();
        assert_eq!(300, metadata.offset());
    }

    #[tokio::test]
    async fn test_async_get_waits_for_completion() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result = make_result(tp);

        let future = Arc::new(FutureRecordMetadata::new(Arc::clone(&result), 0, 1000, 0, 0));

        let future_clone = Arc::clone(&future);
        let handle = tokio::spawn(async move { future_clone.get().await });

        // Complete after a small delay
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        result.set(42, RecordBatch::NO_TIMESTAMP, None);
        result.done();

        let metadata = handle.await.unwrap().unwrap();
        assert_eq!(42, metadata.offset());
    }

    /// Test that chain() called AFTER get() starts waiting but BEFORE done()
    /// is correctly followed. This validates the await-then-read pattern:
    /// the chain must be visible after awaiting the current result.
    ///
    /// Simulates the batch splitting scenario:
    /// 1. User calls get() which starts awaiting result1
    /// 2. Producer receives MESSAGE_TOO_LARGE, calls chain() to redirect to result2
    /// 3. Producer calls done() on result1 (original batch)
    /// 4. get() should follow the chain and return result2's metadata
    #[tokio::test]
    async fn test_chain_set_during_await_is_followed() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result1 = make_result(tp.clone());
        let result2 = make_result(tp);

        let future = Arc::new(FutureRecordMetadata::new(
            Arc::clone(&result1),
            0,
            RecordBatch::NO_TIMESTAMP,
            0,
            0,
        ));

        // Start get() in a background task -- it will block on result1
        let future_clone = Arc::clone(&future);
        let result2_clone = Arc::clone(&result2);
        let handle = tokio::spawn(async move { future_clone.get().await });

        // Give the task time to start awaiting result1
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Step 2: chain() is called (batch split scenario) WHILE get() is awaiting
        let chained = FutureRecordMetadata::new(Arc::clone(&result2), 0, RecordBatch::NO_TIMESTAMP, 0, 0);
        future.chain(chained);

        // Step 3: Complete result1 (original batch marked as done)
        result1.set(100, RecordBatch::NO_TIMESTAMP, None);
        result1.done();

        // Step 4: Complete result2 (split batch)
        result2_clone.set(200, RecordBatch::NO_TIMESTAMP, None);
        result2_clone.done();

        // get() should follow the chain to result2 and return offset 200
        let metadata = handle.await.unwrap().unwrap();
        assert_eq!(
            200,
            metadata.offset(),
            "get() must follow chain set during await, not return stale result1 data"
        );
    }
}
