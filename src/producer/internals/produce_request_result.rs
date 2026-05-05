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

//! Translation of `org.apache.kafka.clients.producer.internals.ProduceRequestResult`.
//!
//! Models the future completion of a produce request for a single
//! partition. There is one of these per partition in a produce request,
//! and it is shared by all the [`RecordMetadata`](crate::producer::RecordMetadata)
//! instances batched for the same partition in the request.
//!
//! Java uses a [`CountDownLatch`] of count 1; Rust replaces it with a
//! [`tokio::sync::Notify`] backed by an [`std::sync::atomic::AtomicBool`]
//! for the "completed" flag. The `errors_by_index` callback is stored
//! behind a [`std::sync::Mutex`] so we can return references without
//! cloning, mirroring Java's `volatile Function<Integer, RuntimeException>`.
//!
//! Per CLAUDE.md rule 9.6 the mutex is **never** held across an `.await`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::error::Elapsed;

use crate::common::errors::KafkaError;
use crate::common::record::record_batch::NO_TIMESTAMP;
use crate::common::topic_partition::TopicPartition;

/// A function mapping batch index → optional [`KafkaError`]. `None` means
/// the indexed record succeeded; `Some(err)` means it failed with that
/// error. Mirrors Java's `Function<Integer, RuntimeException>`.
///
/// Wrapped in `Arc<dyn Fn ...>` so it can be cheaply cloned out of the
/// producer state and consulted by every record's `FutureRecordMetadata`.
pub(crate) type ErrorsByIndex = Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync>;

/// Set fields populated by `set(...)`. Held inside a `Mutex` so that
/// `set` and the readers (`base_offset`, `log_append_time`, `error`) see
/// a consistent snapshot.
struct ResultData {
    base_offset: Option<i64>,
    log_append_time: i64,
    errors_by_index: Option<ErrorsByIndex>,
}

/// See module-level docs.
pub(crate) struct ProduceRequestResult {
    completed: AtomicBool,
    notify: Notify,
    topic_partition: TopicPartition,
    data: Mutex<ResultData>,
    /// Dependent results created when this batch is split into multiple
    /// smaller batches. The original batch's `ProduceRequestResult`
    /// tracks all the splits here so that `flush()` can wait for all
    /// splits to complete via [`ProduceRequestResult::await_all_dependents`].
    dependent_results: Mutex<Vec<Arc<ProduceRequestResult>>>,
}

impl ProduceRequestResult {
    /// Create an instance of this class.
    ///
    /// `topic_partition` is the topic and partition to which this record
    /// set was sent.
    pub fn new(topic_partition: TopicPartition) -> Self {
        ProduceRequestResult {
            completed: AtomicBool::new(false),
            notify: Notify::new(),
            topic_partition,
            data: Mutex::new(ResultData { base_offset: None, log_append_time: NO_TIMESTAMP, errors_by_index: None }),
            dependent_results: Mutex::new(Vec::new()),
        }
    }

    /// Set the result of the produce request. Mirrors Java's `set(...)`.
    pub fn set(&self, base_offset: i64, log_append_time: i64, errors_by_index: Option<ErrorsByIndex>) {
        let mut data = self.data.lock().unwrap();
        data.base_offset = Some(base_offset);
        data.log_append_time = log_append_time;
        data.errors_by_index = errors_by_index;
    }

    /// Mark this request as complete and unblock any tasks awaiting it.
    /// Mirrors Java's `done()`. Panics on `IllegalStateException` if
    /// `set` has not been called first — this is a programmer error
    /// (not user input), per CLAUDE.md rule 10.1.
    pub fn done(&self) {
        {
            let data = self.data.lock().unwrap();
            assert!(
                data.base_offset.is_some(),
                "The method `set` must be invoked before this method."
            );
        }
        self.completed.store(true, Ordering::Release);
        // `notify_waiters` wakes all currently-waiting `notified()`
        // futures. Late-comers see `completed == true` on entry and
        // skip the await entirely.
        self.notify.notify_waiters();
    }

    /// Add a dependent [`ProduceRequestResult`]. Used when a batch is
    /// split into multiple batches — for `flush()` semantics, the
    /// original batch's result should not complete until all split
    /// batches have completed.
    pub fn add_dependent(&self, dependent_result: Arc<ProduceRequestResult>) {
        self.dependent_results.lock().unwrap().push(dependent_result);
    }

    /// Await the completion of this request.
    ///
    /// This only waits for THIS request and not its dependent results.
    /// Individual record futures handle waiting for their respective
    /// split batch via [`FutureRecordMetadata::chain`](super::future_record_metadata::FutureRecordMetadata::chain).
    pub async fn await_completion(&self) {
        if self.completed.load(Ordering::Acquire) {
            return;
        }
        // Register the waker BEFORE re-checking the flag to avoid a
        // missed-wake race. The future returned by `notified()` only
        // observes calls to `notify_waiters` made AFTER the future is
        // created.
        let notified = self.notify.notified();
        if self.completed.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    /// Await the completion of this request, up to the given duration.
    /// Returns `true` if the request completed, `false` if we timed
    /// out. Mirrors `await(long timeout, TimeUnit unit)`.
    pub async fn await_with_timeout(&self, timeout: Duration) -> bool {
        if self.completed.load(Ordering::Acquire) {
            return true;
        }
        let result: Result<(), Elapsed> = tokio::time::timeout(timeout, self.await_completion()).await;
        result.is_ok() && self.completed.load(Ordering::Acquire)
    }

    /// Await the completion of this request and all the dependent
    /// requests. Used by `flush()` to ensure all split batches have
    /// completed before returning.
    pub async fn await_all_dependents(self: Arc<Self>) {
        let mut to_wait: VecDeque<Arc<ProduceRequestResult>> = VecDeque::new();
        to_wait.push_back(self);

        while let Some(current) = to_wait.pop_front() {
            current.await_completion().await;
            // Snapshot dependents under the mutex, then release before
            // recursing — never hold the lock across `.await`.
            let snapshot: Vec<Arc<ProduceRequestResult>> = {
                let guard = current.dependent_results.lock().unwrap();
                guard.iter().cloned().collect()
            };
            to_wait.extend(snapshot);
        }
    }

    /// The base offset for the request (the first offset in the record
    /// set). Returns `None` if `set` has not been called.
    pub fn base_offset(&self) -> Option<i64> {
        self.data.lock().unwrap().base_offset
    }

    /// `true` iff log-append-time is being used for this topic.
    pub fn has_log_append_time(&self) -> bool {
        self.log_append_time() != NO_TIMESTAMP
    }

    /// The log-append-time, or `-1` if `CreateTime` is being used.
    pub fn log_append_time(&self) -> i64 {
        self.data.lock().unwrap().log_append_time
    }

    /// The error thrown (generally on the server) while processing this
    /// request. Returns `None` if there was no error.
    pub fn error(&self, batch_index: i32) -> Option<KafkaError> {
        let cb = {
            let guard = self.data.lock().unwrap();
            guard.errors_by_index.clone()
        };
        cb.and_then(|f| f(batch_index))
    }

    /// The topic and partition to which the record was appended.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// `true` iff `done()` has been called.
    pub fn completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    //! There is no dedicated `ProduceRequestResultTest.java` in the Apache
    //! Kafka 4.2 test suite; the class is exercised end-to-end through
    //! `RecordSendTest` (translated alongside `FutureRecordMetadata`)
    //! and through `RecordAccumulatorTest` / `SenderTest` (deferred to
    //! Phase 6d / 6e).

    use super::*;

    #[tokio::test]
    async fn await_returns_immediately_when_already_done() {
        let r = ProduceRequestResult::new(TopicPartition::new("t", 0));
        r.set(7, NO_TIMESTAMP, None);
        r.done();
        r.await_completion().await;
        assert_eq!(Some(7), r.base_offset());
        assert!(r.completed());
    }

    #[tokio::test]
    async fn await_with_timeout_times_out() {
        let r = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let occurred = r.await_with_timeout(Duration::from_millis(20)).await;
        assert!(!occurred);
        assert!(!r.completed());
    }

    #[tokio::test]
    async fn await_with_timeout_returns_true_after_done() {
        let r = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let r2 = Arc::clone(&r);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            r2.set(42, NO_TIMESTAMP, None);
            r2.done();
        });
        let occurred = r.await_with_timeout(Duration::from_millis(500)).await;
        assert!(occurred);
        assert_eq!(Some(42), r.base_offset());
    }

    #[tokio::test]
    async fn await_all_dependents_walks_chain() {
        let parent = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let child = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 1)));
        parent.add_dependent(Arc::clone(&child));

        let parent2 = Arc::clone(&parent);
        let child2 = Arc::clone(&child);
        tokio::spawn(async move {
            parent2.set(0, NO_TIMESTAMP, None);
            parent2.done();
            tokio::time::sleep(Duration::from_millis(20)).await;
            child2.set(0, NO_TIMESTAMP, None);
            child2.done();
        });

        Arc::clone(&parent).await_all_dependents().await;
        assert!(parent.completed());
        assert!(child.completed());
    }

    #[test]
    #[should_panic(expected = "The method `set` must be invoked before this method.")]
    fn done_panics_without_set() {
        let r = ProduceRequestResult::new(TopicPartition::new("t", 0));
        r.done();
    }

    #[test]
    fn error_returns_none_when_no_function() {
        let r = ProduceRequestResult::new(TopicPartition::new("t", 0));
        r.set(0, NO_TIMESTAMP, None);
        assert!(r.error(3).is_none());
    }

    #[test]
    fn error_dispatches_to_function() {
        let r = ProduceRequestResult::new(TopicPartition::new("t", 0));
        let f: ErrorsByIndex = Arc::new(|idx| {
            if idx == 2 {
                Some(KafkaError::CorruptRecord("boom".to_string()))
            } else {
                None
            }
        });
        r.set(-1, NO_TIMESTAMP, Some(f));
        assert!(r.error(0).is_none());
        assert!(matches!(r.error(2), Some(KafkaError::CorruptRecord(_))));
    }
}
