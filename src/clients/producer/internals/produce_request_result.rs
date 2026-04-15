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

//! A class that models the future completion of a produce request for a single partition.
//!
//! There is one of these per partition in a produce request and it is shared by all the
//! [`RecordMetadata`](crate::clients::producer::RecordMetadata) instances that are batched
//! together for the same partition in the request.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ProduceRequestResult`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::common::record::record_batch::RecordBatch;
use crate::common::topic_partition::TopicPartition;

/// The inner result data set when a produce request completes.
#[derive(Clone)]
pub struct ProduceResult {
    /// The base offset assigned to the record.
    pub base_offset: i64,
    /// The log append time or -1 if CreateTime is being used.
    pub log_append_time: i64,
    /// Optional error function that maps batch index to an error string.
    /// In Rust we store the error as an optional string per index.
    /// `None` means no error (successful response).
    pub error: Option<Arc<dyn Fn(i32) -> Option<String> + Send + Sync>>,
}

impl std::fmt::Debug for ProduceResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProduceResult")
            .field("base_offset", &self.base_offset)
            .field("log_append_time", &self.log_append_time)
            .field("error", &self.error.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

/// A class that models the future completion of a produce request for a single partition.
///
/// There is one of these per partition in a produce request and it is shared by all the
/// [`RecordMetadata`](crate::clients::producer::RecordMetadata) instances that are
/// batched together for the same partition in the request.
///
/// In Java, this uses a `CountDownLatch` for synchronization. In Rust, we use a
/// `tokio::sync::watch` channel: the sender sets the result and receivers (FutureRecordMetadata)
/// observe it.
pub struct ProduceRequestResult {
    /// The watch channel sender. Sends `None` initially, then `Some(ProduceResult)` when done.
    tx: watch::Sender<Option<ProduceResult>>,
    /// The watch channel receiver (cloned for each FutureRecordMetadata).
    rx: watch::Receiver<Option<ProduceResult>>,
    /// The topic and partition to which this record set was sent.
    topic_partition: TopicPartition,
    /// List of dependent ProduceRequestResults created when this batch is split.
    /// When a batch is too large to send, it's split into multiple smaller batches.
    /// The original batch's ProduceRequestResult tracks all the split batches here
    /// so that flush() can wait for all splits to complete via `await_all_dependents()`.
    dependent_results: Mutex<Vec<Arc<ProduceRequestResult>>>,
}

impl ProduceRequestResult {
    /// Create an instance of this class.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic and partition to which this record set was sent
    pub fn new(topic_partition: TopicPartition) -> Self {
        let (tx, rx) = watch::channel(None);
        Self { tx, rx, topic_partition, dependent_results: Mutex::new(Vec::new()) }
    }

    /// Set the result of the produce request.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - The base offset assigned to the record
    /// * `log_append_time` - The log append time or -1 if CreateTime is being used
    /// * `errors_by_index` - Function mapping the batch index to the error string,
    ///   or `None` if the response was successful
    pub fn set(
        &self,
        base_offset: i64,
        log_append_time: i64,
        errors_by_index: Option<Arc<dyn Fn(i32) -> Option<String> + Send + Sync>>,
    ) {
        // Store the result but don't notify yet (done() does that).
        // We use send_modify to avoid requiring the old value to implement PartialEq.
        self.tx.send_modify(|val| {
            *val = Some(ProduceResult { base_offset, log_append_time, error: errors_by_index });
        });
    }

    /// Mark this request as complete and unblock any tasks waiting on its completion.
    ///
    /// # Panics
    ///
    /// Panics if `set` was not called before `done`.
    pub fn done(&self) {
        // The value should already be set by `set()`. We just need to ensure
        // the receivers are notified, which `send_modify` already did.
        // But we verify the invariant.
        let current = self.tx.borrow().clone();
        assert!(current.is_some(), "The method `set` must be invoked before `done`.");
        // The watch channel already has the value set; receivers blocked on
        // `changed()` / `wait_for()` will see it.
    }

    /// Add a dependent ProduceRequestResult.
    ///
    /// This is used when a batch is split into multiple batches — in some cases
    /// like flush(), the original batch's result should not complete until all
    /// split batches have completed.
    pub fn add_dependent(&self, dependent_result: Arc<ProduceRequestResult>) {
        let mut deps = self.dependent_results.lock().unwrap();
        deps.push(dependent_result);
    }

    /// Await the completion of this request.
    ///
    /// This only waits for THIS request and not dependent results.
    /// When a batch is split into multiple batches, dependent results are created and
    /// tracked separately, but this method does not wait for them. Individual record
    /// futures automatically handle waiting for their respective split batch via
    /// [`FutureRecordMetadata::chain`](super::FutureRecordMetadata::chain),
    /// which redirects the future to point to the correct split batch's result.
    ///
    /// For flush() semantics that require waiting for all dependent results, use
    /// [`await_all_dependents`](Self::await_all_dependents).
    pub async fn await_completion(&self) {
        let mut rx = self.rx.clone();
        // Wait until the value is Some
        let _ = rx.wait_for(|v| v.is_some()).await;
    }

    /// Await the completion of this request with a timeout.
    ///
    /// Returns `true` if the request completed, `false` if the timeout elapsed.
    pub async fn await_timeout(&self, timeout: std::time::Duration) -> bool {
        let mut rx = self.rx.clone();
        tokio::time::timeout(timeout, rx.wait_for(|v| v.is_some())).await.is_ok()
    }

    /// Await the completion of this request and all the dependent requests.
    ///
    /// This method is used by flush() to ensure all split batches have completed
    /// before returning. This method waits for all dependent
    /// [`ProduceRequestResult`]s that were created when the batch was split.
    pub async fn await_all_dependents(self: &Arc<Self>) {
        let mut to_wait: VecDeque<Arc<ProduceRequestResult>> = VecDeque::new();
        to_wait.push_back(Arc::clone(self));

        while let Some(current) = to_wait.pop_front() {
            // First wait for THIS result to be released
            current.await_completion().await;

            // Add all dependent split batches to the queue.
            // We synchronize to get a consistent snapshot, then release the lock
            // before continuing. The actual waiting happens outside the lock.
            let deps: Vec<Arc<ProduceRequestResult>> = {
                let guard = current.dependent_results.lock().unwrap();
                guard.clone()
            };
            to_wait.extend(deps);
        }
    }

    /// The base offset for the request (the first offset in the record set).
    ///
    /// Returns `None` if the result has not been set yet.
    pub fn base_offset(&self) -> Option<i64> {
        self.rx.borrow().as_ref().map(|r| r.base_offset)
    }

    /// Return true if log append time is being used for this topic.
    pub fn has_log_append_time(&self) -> bool {
        self.rx
            .borrow()
            .as_ref()
            .is_some_and(|r| r.log_append_time != RecordBatch::NO_TIMESTAMP)
    }

    /// The log append time or -1 if CreateTime is being used.
    pub fn log_append_time(&self) -> i64 {
        self.rx
            .borrow()
            .as_ref()
            .map_or(RecordBatch::NO_TIMESTAMP, |r| r.log_append_time)
    }

    /// The error thrown (generally on the server) while processing this request.
    ///
    /// Returns `None` if there was no error for the given batch index.
    pub fn error(&self, batch_index: i32) -> Option<String> {
        let guard = self.rx.borrow();
        match guard.as_ref() {
            Some(result) => match &result.error {
                Some(errors_fn) => errors_fn(batch_index),
                None => None,
            },
            None => None,
        }
    }

    /// The topic and partition to which the record was appended.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// Has the request completed?
    ///
    /// This method only checks if THIS request has completed and not its dependent results.
    pub fn completed(&self) -> bool {
        self.rx.borrow().is_some()
    }

    /// Subscribe to this result by obtaining a watch receiver clone.
    ///
    /// Used internally by [`FutureRecordMetadata`](super::FutureRecordMetadata) to observe
    /// completion.
    pub fn subscribe(&self) -> watch::Receiver<Option<ProduceResult>> {
        self.rx.clone()
    }
}

impl std::fmt::Debug for ProduceRequestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProduceRequestResult")
            .field("topic_partition", &self.topic_partition)
            .field("completed", &self.completed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set_and_done() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result = ProduceRequestResult::new(tp);

        assert!(!result.completed());
        assert!(result.base_offset().is_none());

        result.set(42, RecordBatch::NO_TIMESTAMP, None);
        result.done();

        assert!(result.completed());
        assert_eq!(result.base_offset(), Some(42));
        assert!(!result.has_log_append_time());
        assert_eq!(result.log_append_time(), RecordBatch::NO_TIMESTAMP);
        assert!(result.error(0).is_none());
    }

    #[tokio::test]
    async fn test_with_log_append_time() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result = ProduceRequestResult::new(tp);

        result.set(0, 1234567890, None);
        result.done();

        assert!(result.has_log_append_time());
        assert_eq!(result.log_append_time(), 1234567890);
    }

    #[tokio::test]
    async fn test_with_errors() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result = ProduceRequestResult::new(tp);

        let errors_fn: Arc<dyn Fn(i32) -> Option<String> + Send + Sync> = Arc::new(|idx| {
            if idx == 0 {
                Some("Error for index 0".to_string())
            } else {
                None
            }
        });

        result.set(-1, RecordBatch::NO_TIMESTAMP, Some(errors_fn));
        result.done();

        assert_eq!(result.error(0), Some("Error for index 0".to_string()));
        assert!(result.error(1).is_none());
    }

    #[tokio::test]
    async fn test_await_completion() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result = Arc::new(ProduceRequestResult::new(tp));

        let result_clone = Arc::clone(&result);
        let handle = tokio::spawn(async move {
            result_clone.await_completion().await;
            assert!(result_clone.completed());
        });

        // Set and complete the result
        result.set(0, RecordBatch::NO_TIMESTAMP, None);
        result.done();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_await_timeout() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result = ProduceRequestResult::new(tp);

        // Should timeout since we never complete
        let completed = result.await_timeout(std::time::Duration::from_millis(10)).await;
        assert!(!completed);

        // Now complete it
        result.set(0, RecordBatch::NO_TIMESTAMP, None);
        result.done();

        let completed = result.await_timeout(std::time::Duration::from_millis(100)).await;
        assert!(completed);
    }

    #[tokio::test]
    async fn test_await_all_dependents() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let main_result = Arc::new(ProduceRequestResult::new(tp.clone()));

        let dep1 = Arc::new(ProduceRequestResult::new(tp.clone()));
        let dep2 = Arc::new(ProduceRequestResult::new(tp));

        main_result.add_dependent(Arc::clone(&dep1));
        main_result.add_dependent(Arc::clone(&dep2));

        let main_clone = Arc::clone(&main_result);
        let handle = tokio::spawn(async move {
            main_clone.await_all_dependents().await;
        });

        // Complete main
        main_result.set(0, RecordBatch::NO_TIMESTAMP, None);
        main_result.done();

        // Complete dependents
        dep1.set(1, RecordBatch::NO_TIMESTAMP, None);
        dep1.done();

        dep2.set(2, RecordBatch::NO_TIMESTAMP, None);
        dep2.done();

        handle.await.unwrap();
    }

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("my-topic".to_string(), 3);
        let result = ProduceRequestResult::new(tp.clone());
        assert_eq!(result.topic_partition(), &tp);
    }

    #[tokio::test]
    #[should_panic(expected = "The method `set` must be invoked before `done`.")]
    async fn test_done_without_set_panics() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let result = ProduceRequestResult::new(tp);
        result.done();
    }
}
