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
//! Per CLAUDE.md rule 11, the inherent `get` is a concrete `async fn`
//! — no `Pin<Box<dyn Future>>` allocation per record on the in-crate
//! call path (`ProducerBatch`, `Sender::complete_batch` etc. call the
//! inherent method directly).
//!
//! Phase 7g adds a [`KafkaFutureOps`](crate::common::kafka_future::KafkaFutureOps)
//! trait impl below so that
//! [`KafkaProducer::send`](crate::producer::KafkaProducer) can wrap
//! this future in [`KafkaFuture<RecordMetadata>`](crate::common::KafkaFuture).
//! The trait method requires a boxed future (object-safety
//! requirement); that boxing is the per-`KafkaFuture::get()`-call
//! allocation, paid by callers who choose to wait — not by the
//! producer's `send()` allocation, which is one shared `Arc<dyn
//! KafkaFutureOps>`.

#![allow(dead_code)] // Phase 6b (ProducerBatch) wires `chain` and `is_done`.

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
    /// Unused in the Rust port: Java's `time` field powers the
    /// `get(timeout, unit)` overload (see module docs for skipped
    /// tests). Retained as a constructor parameter for Java parity so
    /// that 6b/6c builders can pass through their own `Time` source.
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

    /// Accessor for the create timestamp passed at construction. Used by
    /// [`ProducerBatch`](super::producer_batch::ProducerBatch) when
    /// building a per-record [`RecordMetadata`] for the `Callback`.
    pub fn create_timestamp(&self) -> i64 {
        self.create_timestamp
    }

    /// Accessor for the serialized key size passed at construction.
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// Accessor for the serialized value size passed at construction.
    pub fn serialized_value_size(&self) -> i32 {
        self.serialized_value_size
    }

    /// `true` iff the underlying request has completed.
    pub fn is_done(&self) -> bool {
        if let Some(next) = self.next.get() {
            return next.is_done();
        }
        self.result.completed()
    }
}

/// Wire [`FutureRecordMetadata`] into the
/// [`KafkaFutureOps`](crate::common::kafka_future::KafkaFutureOps)
/// trait so it can be wrapped in
/// [`KafkaFuture<RecordMetadata>`](crate::common::KafkaFuture) on the
/// `Producer::send` return path.
///
/// Mirrors the Java pattern where
/// `org.apache.kafka.clients.producer.internals.FutureRecordMetadata`
/// `implements Future<RecordMetadata>` and Java
/// `KafkaProducer#send` returns the same `Future` shape callers can
/// `.get()` on later. The Rust translation surfaces the same
/// contract via [`KafkaFuture<RecordMetadata>`].
///
/// The trait's `get` returns a boxed future (object-safety
/// requirement) but the boxed future is one Java-equivalent
/// `Arc<dyn KafkaFutureOps<RecordMetadata>>` per send — same per-send
/// allocation Java pays for `new FutureRecordMetadata(...)`.
impl crate::common::kafka_future::KafkaFutureOps<crate::producer::record_metadata::RecordMetadata>
    for FutureRecordMetadata
{
    fn get<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<crate::producer::record_metadata::RecordMetadata, KafkaError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(self.get())
    }

    fn is_done(&self) -> bool {
        self.is_done()
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
    //! We replace those two tests with two Rust-shape equivalents:
    //!
    //! - [`tests::chain_resolves_to_tail_metadata`] sets up a real
    //!   chain, completes parent then child out-of-order, and asserts
    //!   the final metadata corresponds to the chain tail.
    //! - [`tests::outer_timeout_cancels_chained_inner_await`] is the
    //!   deadline-propagation guard: completes the parent, leaves the
    //!   child pending, and asserts that `tokio::time::timeout(d,
    //!   parent_future.get())` fires (because `get` continues to await
    //!   the child after the parent resolves). This is the equivalent
    //!   of "the remaining timeout is propagated to the chained
    //!   future" in idiomatic Rust.

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

    /// Deadline-propagation guard equivalent to Java's
    /// `testFutureGetWithSeconds` / `testFutureGetWithMilliSeconds`.
    /// In Java the bug under test was: after the parent batch's `get`
    /// returns, the chained `get(timeout, unit)` must use the
    /// **remaining** budget, not the original. In Rust the natural
    /// idiom is `tokio::time::timeout(d, f.get())`, which transparently
    /// covers both awaits — this test asserts that wrapping the chain
    /// head's `get()` in `tokio::time::timeout(...)` does in fact
    /// cancel the chained inner await once the parent has completed
    /// and the child is still pending. Without proper chain
    /// propagation, the outer timeout would resolve immediately to the
    /// parent's metadata instead of waiting for the child.
    #[tokio::test]
    async fn outer_timeout_cancels_chained_inner_await() {
        let parent = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let child = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let parent_future = future_for(Arc::clone(&parent));
        parent_future.chain(Arc::new(future_for(Arc::clone(&child))));

        // Complete the parent only — child stays pending forever.
        parent.set(0, NO_TIMESTAMP, None);
        parent.done();

        let elapsed = tokio::time::timeout(Duration::from_millis(50), parent_future.get()).await;
        assert!(
            elapsed.is_err(),
            "outer tokio::time::timeout should fire because the chained child future never completes; got {elapsed:?}"
        );
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

    /// Phase 7g (2/N) — verify the `KafkaFutureOps` trait impl works
    /// through the public [`KafkaFuture`](crate::common::KafkaFuture)
    /// wrapper. This pins the round-trip Java's
    /// `Producer#send` callers will see: `producer.send(r).await?`
    /// returns a `KafkaFuture<RecordMetadata>`; `.get().await?`
    /// resolves to the same `RecordMetadata` the inner
    /// `FutureRecordMetadata::get().await` would produce.
    #[tokio::test]
    async fn kafka_future_wrapper_round_trips_via_trait_impl() {
        use crate::common::KafkaFuture;
        use crate::common::kafka_future::KafkaFutureOps;

        let result = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        let frm = Arc::new(future_for(Arc::clone(&result)));
        let frm_ops: Arc<dyn KafkaFutureOps<crate::producer::record_metadata::RecordMetadata>> = frm;
        let kf = KafkaFuture::new(frm_ops);

        // Before completion: is_done() should reflect the underlying
        // FutureRecordMetadata::is_done() (false → completed flag not
        // set yet).
        assert!(!kf.is_done());

        // Resolve out of band.
        let result_for_task = Arc::clone(&result);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            result_for_task.set(123, NO_TIMESTAMP, None);
            result_for_task.done();
        });

        let metadata = kf.get().await.expect("future should resolve");
        assert_eq!(metadata.offset(), 123);
        assert!(kf.is_done());
    }
}
