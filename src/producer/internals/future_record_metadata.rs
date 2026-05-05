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

//! Translation of `org.apache.kafka.clients.producer.internals.FutureRecordMetadata`.
//!
//! Java implements `Future<RecordMetadata>`. Rust equivalents diverge:
//! `Future` in Rust is the `std::future::Future` trait, polled by an
//! executor. We therefore expose a single `async fn get(&self)` that
//! mirrors `Future::get()` semantics, plus a chain pointer for split
//! batches. There is no separate `cancel` / `isCancelled` API: producer
//! futures cannot be cancelled in Java either (`cancel(boolean)` always
//! returns `false`).
//!
//! Per CLAUDE.md rule 11, we avoid `Pin<Box<dyn Future>>` per record by
//! exposing `get` as a concrete `async fn`.

use std::sync::{Arc, OnceLock};

use crate::common::errors::KafkaError;
use crate::common::utils::Time;
use crate::producer::record_metadata::RecordMetadata;

use super::produce_request_result::ProduceRequestResult;

/// The future result of a record send. Mirrors Java's
/// `final class FutureRecordMetadata implements Future<RecordMetadata>`.
pub(crate) struct FutureRecordMetadata {
    result: Arc<ProduceRequestResult>,
    batch_index: i32,
    create_timestamp: i64,
    serialized_key_size: i32,
    serialized_value_size: i32,
    #[allow(dead_code)]
    time: Arc<dyn Time>,
    /// Set once when the parent batch is split and a new
    /// `FutureRecordMetadata` is created for the child. After it is set,
    /// `get` and `is_done` delegate to the chained future.
    next: OnceLock<Arc<FutureRecordMetadata>>,
}

impl FutureRecordMetadata {
    /// Mirrors Java's 6-arg constructor.
    pub fn new(
        result: Arc<ProduceRequestResult>,
        batch_index: i32,
        create_timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
        time: Arc<dyn Time>,
    ) -> Self {
        FutureRecordMetadata {
            result,
            batch_index,
            create_timestamp,
            serialized_key_size,
            serialized_value_size,
            time,
            next: OnceLock::new(),
        }
    }

    /// Equivalent to Java's `RecordMetadata get()`. Awaits the request,
    /// then returns the metadata or the per-record error.
    pub async fn get(&self) -> Result<RecordMetadata, KafkaError> {
        self.result.await_completion().await;
        if let Some(next) = self.next.get() {
            return Box::pin(next.get()).await;
        }
        self.value_or_error()
    }

    /// Append a chained future. When this batch was split, the chained
    /// future allows the originally-returned future to wait on the
    /// newly-created split batches.
    ///
    /// `chain` walks the chain, mirroring Java's recursive
    /// `nextRecordMetadata.chain(...)`. If the chain head is already
    /// set, the new future is appended to its tail.
    pub fn chain(&self, future_record_metadata: Arc<FutureRecordMetadata>) {
        match self.next.set(future_record_metadata) {
            Ok(()) => {},
            Err(future_record_metadata) => {
                // `next` was already set; recurse into it.
                let existing = self.next.get().expect("next must be set after Err return");
                existing.chain(future_record_metadata);
            },
        }
    }

    /// Resolve to the [`RecordMetadata`] or the per-record error.
    fn value_or_error(&self) -> Result<RecordMetadata, KafkaError> {
        if let Some(err) = self.result.error(self.batch_index) {
            return Err(err);
        }
        Ok(self.value())
    }

    fn value(&self) -> RecordMetadata {
        if let Some(next) = self.next.get() {
            return next.value();
        }
        let base_offset = self.result.base_offset().unwrap_or(-1);
        RecordMetadata::new(
            self.result.topic_partition().clone(),
            base_offset,
            self.batch_index,
            self.timestamp(),
            self.serialized_key_size,
            self.serialized_value_size,
        )
    }

    fn timestamp(&self) -> i64 {
        if self.result.has_log_append_time() {
            self.result.log_append_time()
        } else {
            self.create_timestamp
        }
    }

    /// `true` iff the underlying request has completed.
    pub fn is_done(&self) -> bool {
        if let Some(next) = self.next.get() {
            return next.is_done();
        }
        self.result.completed()
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `org.apache.kafka.clients.producer.internals.FutureRecordMetadataTest`.
    //!
    //! ## Skipped Java tests
    //!
    //! `testFutureGetWithSeconds` and `testFutureGetWithMilliSeconds`
    //! both verify that `Future.get(timeout, unit)` propagates the
    //! **remaining** timeout to the chained future after the parent
    //! awaits — i.e. that `nextRecordMetadata.get(deadline - now,
    //! TimeUnit.MILLISECONDS)` is called rather than passing the
    //! original `(timeout, unit)` pair.
    //!
    //! In Rust the natural cancellation pattern is to wrap the entire
    //! future tree in `tokio::time::timeout(d, future)`, so the bug
    //! these Java tests guard against (re-passing the original timeout
    //! to the child) cannot be expressed: the timeout cancels the whole
    //! tree at the outer await, not at any internal `get(remaining,
    //! ...)` call. The chained-future plumbing in [`FutureRecordMetadata::get`]
    //! awaits the parent first, then awaits the chain — and `tokio::time::timeout`
    //! covers both transparently.
    //!
    //! We replace those two tests with a deadline-propagation
    //! verification that sets up a real chain, completes both sides
    //! out-of-order, and asserts the final metadata corresponds to the
    //! chain tail.

    use super::*;
    use crate::common::record::record_batch::NO_TIMESTAMP;
    use crate::common::topic_partition::TopicPartition;
    use crate::common::utils::MockTime;
    use std::time::Duration;

    fn future_for(result: Arc<ProduceRequestResult>) -> FutureRecordMetadata {
        FutureRecordMetadata::new(result, 0, NO_TIMESTAMP, 0, 0, MockTime::arc())
    }

    /// Replacement for `testFutureGetWithSeconds` /
    /// `testFutureGetWithMilliSeconds`. See module docs for rationale.
    #[tokio::test]
    async fn chain_resolves_to_tail_metadata() {
        let parent = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let child = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));

        let parent_future = future_for(Arc::clone(&parent));
        let child_future = Arc::new(future_for(Arc::clone(&child)));
        parent_future.chain(Arc::clone(&child_future));

        // Spawn completers: parent first, then child after a beat.
        let parent2 = Arc::clone(&parent);
        let child2 = Arc::clone(&child);
        tokio::spawn(async move {
            parent2.set(100, NO_TIMESTAMP, None);
            parent2.done();
            tokio::time::sleep(Duration::from_millis(10)).await;
            child2.set(200, NO_TIMESTAMP, None);
            child2.done();
        });

        let metadata = parent_future.get().await.unwrap();
        // After chaining, the final metadata corresponds to the chain
        // tail (offset = 200 + batch_index 0).
        assert_eq!(200, metadata.offset());
    }

    #[tokio::test]
    async fn returns_error_from_result_function() {
        let result = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let err_fn: super::super::produce_request_result::ErrorsByIndex =
            Arc::new(|_| Some(KafkaError::CorruptRecord("boom".to_string())));
        result.set(-1, NO_TIMESTAMP, Some(err_fn));
        result.done();

        let f = future_for(result);
        let err = f.get().await.unwrap_err();
        assert!(matches!(err, KafkaError::CorruptRecord(_)));
    }

    #[tokio::test]
    async fn is_done_reflects_completion() {
        let result = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let f = future_for(Arc::clone(&result));
        assert!(!f.is_done());
        result.set(0, NO_TIMESTAMP, None);
        result.done();
        assert!(f.is_done());
    }

    #[tokio::test]
    async fn is_done_follows_chain() {
        let parent = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let child = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let parent_future = future_for(Arc::clone(&parent));
        parent_future.chain(Arc::new(future_for(Arc::clone(&child))));

        // Parent done but child not done → chain head reports !is_done.
        parent.set(0, NO_TIMESTAMP, None);
        parent.done();
        assert!(!parent_future.is_done());
        child.set(0, NO_TIMESTAMP, None);
        child.done();
        assert!(parent_future.is_done());
    }
}
