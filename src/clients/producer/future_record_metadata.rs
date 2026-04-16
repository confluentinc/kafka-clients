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

//! Future result of a record send operation.
//!
//! Corresponds to Java's `org.apache.kafka.clients.producer.internals.FutureRecordMetadata`
//! combined with `ProduceRequestResult`. Uses `tokio::sync::oneshot` for async
//! completion instead of Java's `CountDownLatch` + `Future` interface.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::oneshot;

use super::record_metadata::RecordMetadata;
use crate::common::kafka_error::KafkaError;

/// The sender half for completing a [`FutureRecordMetadata`].
///
/// Used by the producer internals (or `MockProducer`'s `Completion` struct)
/// to deliver the result of a produce operation.
pub type CompletionSender = oneshot::Sender<Result<RecordMetadata, KafkaError>>;

/// Creates a new `FutureRecordMetadata` and its corresponding [`CompletionSender`].
///
/// This is a convenience function that wraps `tokio::sync::oneshot::channel`.
pub fn create() -> (CompletionSender, FutureRecordMetadata) {
    let (tx, rx) = oneshot::channel();
    (tx, FutureRecordMetadata { receiver: Some(rx), result: None })
}

/// The future result of a record send.
///
/// Wraps a `tokio::sync::oneshot::Receiver` that will resolve to either a
/// [`RecordMetadata`] on success or a [`KafkaError`] on failure.
///
/// Implements [`Future`] so it can be `.await`ed directly.
///
/// Unlike a typical Rust future, this type supports calling [`get()`](Self::get)
/// multiple times (matching Java's `Future.get()` semantics). After the first
/// successful resolution, the result is cached and returned on subsequent calls.
/// This allows patterns like calling `get()` with a timeout first, then calling
/// `get()` again without a timeout on the same future.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.producer.internals.FutureRecordMetadata`.
///
/// # Examples
///
/// ```no_run
/// use confluent_kafka::clients::producer::future_record_metadata;
/// use confluent_kafka::clients::producer::RecordMetadata;
/// use confluent_kafka::common::TopicPartition;
///
/// # async fn example() {
/// let (sender, mut future) = future_record_metadata::create();
///
/// let tp = TopicPartition::new("topic".to_string(), 0);
/// let metadata = RecordMetadata::new(tp, 10, 0, 1000, 3, 5);
/// sender.send(Ok(metadata)).unwrap();
///
/// let result = future.get().await;
/// assert!(result.is_ok());
/// assert_eq!(result.unwrap().offset(), 10);
/// # }
/// ```
pub struct FutureRecordMetadata {
    /// The oneshot receiver, set to `None` after the result has been received
    /// and cached.
    receiver: Option<oneshot::Receiver<Result<RecordMetadata, KafkaError>>>,
    /// Cached result after first completion. Allows subsequent `get()` calls
    /// to return the same result without needing the receiver.
    result: Option<Result<RecordMetadata, KafkaError>>,
}

impl FutureRecordMetadata {
    /// Creates a `FutureRecordMetadata` from an existing oneshot receiver.
    pub fn new(receiver: oneshot::Receiver<Result<RecordMetadata, KafkaError>>) -> Self {
        Self { receiver: Some(receiver), result: None }
    }

    /// Creates a `FutureRecordMetadata` that is already completed with the
    /// given result.
    ///
    /// The returned future's [`is_done()`](Self::is_done) will return `true`
    /// immediately, and [`get()`](Self::get) will return the cached result
    /// without blocking.
    ///
    /// This is used by `MockProducer` in auto-complete mode to match Java's
    /// behavior where `Future.isDone()` returns `true` immediately after a
    /// synchronously completed send.
    pub fn completed(result: Result<RecordMetadata, KafkaError>) -> Self {
        Self { receiver: None, result: Some(result) }
    }

    /// Awaits the completion of the record send and returns the result.
    ///
    /// Returns `Ok(RecordMetadata)` on success, or `Err(KafkaError)` if the
    /// send failed or the sender was dropped.
    ///
    /// This method can be called multiple times. After the first successful
    /// completion, subsequent calls return the cached result immediately.
    /// This matches Java's `Future.get()` semantics where the same future
    /// can be called first with a timeout, and then again indefinitely.
    ///
    /// If cancelled (e.g., via `tokio::time::timeout`), the receiver is
    /// preserved so subsequent calls to `get()` can still succeed once the
    /// sender completes.
    pub async fn get(&mut self) -> Result<RecordMetadata, KafkaError> {
        // If we already have a cached result, return a clone of it.
        if let Some(ref result) = self.result {
            return result.clone();
        }

        // Poll the receiver in-place using poll_fn. This does NOT take()
        // the receiver, so if this future is cancelled (e.g., by timeout),
        // the receiver remains available for a subsequent call.
        std::future::poll_fn(|cx| self.poll_receiver(cx)).await
    }

    /// Internal poll helper that polls the receiver without consuming it
    /// until it's ready. Once ready, takes the receiver and caches the result.
    fn poll_receiver(&mut self, cx: &mut Context<'_>) -> Poll<Result<RecordMetadata, KafkaError>> {
        // Already have a result cached.
        if let Some(ref result) = self.result {
            return Poll::Ready(result.clone());
        }

        match self.receiver.as_mut() {
            Some(rx) => match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.receiver = None;
                    self.result = Some(result.clone());
                    Poll::Ready(result)
                },
                Poll::Ready(Err(_)) => {
                    self.receiver = None;
                    let err = Err(KafkaError::with_message(
                        crate::common::protocol::Errors::UnknownServerError,
                        "Producer was dropped before completing the send",
                    ));
                    self.result = Some(err.clone());
                    Poll::Ready(err)
                },
                Poll::Pending => Poll::Pending,
            },
            None => {
                // Receiver was already consumed but result wasn't cached.
                // This shouldn't happen in normal usage.
                Poll::Ready(Err(KafkaError::with_message(
                    crate::common::protocol::Errors::UnknownServerError,
                    "Producer was dropped before completing the send",
                )))
            },
        }
    }

    /// Checks if the result is available without blocking.
    ///
    /// Returns `true` if the sender has completed (either successfully or
    /// with an error) or been dropped, `false` if still pending.
    ///
    /// This method eagerly tries to receive from the underlying channel if
    /// the result has not been cached yet, so it can return `true` even
    /// without having been polled as a `Future`. This matches Java's
    /// `Future.isDone()` semantics.
    pub fn is_done(&mut self) -> bool {
        if self.result.is_some() {
            return true;
        }

        // Try to eagerly receive from the channel without blocking.
        if let Some(rx) = self.receiver.as_mut() {
            match rx.try_recv() {
                Ok(result) => {
                    self.result = Some(result);
                    self.receiver = None;
                    true
                },
                Err(oneshot::error::TryRecvError::Empty) => false,
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.receiver = None;
                    self.result = Some(Err(KafkaError::with_message(
                        crate::common::protocol::Errors::UnknownServerError,
                        "Producer was dropped before completing the send",
                    )));
                    true
                },
            }
        } else {
            // No receiver and no result — should not happen in normal usage
            false
        }
    }
}

impl Future for FutureRecordMetadata {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_receiver(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TopicPartition;

    // -----------------------------------------------------------------------
    // Translated from RecordSendTest.java
    // -----------------------------------------------------------------------

    /// Translated from `RecordSendTest.testTimeout`.
    ///
    /// Tests that waiting on a request that never completes times out,
    /// and that the *same* future resolves correctly once the underlying
    /// result is completed. This matches Java's test which calls
    /// `future.get(5, TimeUnit.MILLISECONDS)` (timeout), then completes
    /// the `ProduceRequestResult`, then calls `future.get()` again on the
    /// same future and verifies the offset.
    #[tokio::test]
    async fn test_timeout() {
        let base_offset = 45_i64;
        let rel_offset = 5;

        let (sender, mut future) = create();
        assert!(!future.is_done(), "Request is not completed");

        // Attempt to get with a short timeout -- should time out
        let timeout_result = tokio::time::timeout(std::time::Duration::from_millis(5), future.get()).await;
        assert!(timeout_result.is_err(), "Request should have timed out");

        // The future should still not be done after timeout
        assert!(!future.is_done(), "Request should still not be completed after timeout");

        // Complete the underlying request on the same future
        let tp = TopicPartition::new("test".to_string(), 0);
        let metadata = RecordMetadata::new(tp, base_offset, rel_offset, -1, 0, 0);
        sender.send(Ok(metadata)).unwrap();

        // Now the same future should resolve with the correct offset
        let result = future.get().await;
        assert!(result.is_ok());
        assert!(future.is_done());
        assert_eq!(result.unwrap().offset(), base_offset + i64::from(rel_offset));
    }

    /// Translated from `RecordSendTest.testError`.
    ///
    /// Tests that an asynchronous request will eventually return the right error.
    #[tokio::test]
    async fn test_error() {
        let (sender, mut future) = create();

        // Complete with error after a short delay (simulating async completion)
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = sender.send(Err(KafkaError::new(crate::common::protocol::Errors::CorruptMessage)));
        });

        let result = future.get().await;
        assert!(result.is_err());
    }

    /// Translated from `RecordSendTest.testBlocking`.
    ///
    /// Tests that an asynchronous request will eventually return the right offset.
    #[tokio::test]
    async fn test_blocking() {
        let base_offset = 45_i64;
        let rel_offset = 5;

        let (sender, mut future) = create();

        // Complete successfully after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let tp = TopicPartition::new("test".to_string(), 0);
            let metadata = RecordMetadata::new(tp, base_offset, rel_offset, -1, 0, 0);
            let _ = sender.send(Ok(metadata));
        });

        let result = future.get().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().offset(), base_offset + i64::from(rel_offset));
    }

    // -----------------------------------------------------------------------
    // FutureRecordMetadataTest.java tests are intentionally deferred.
    //
    // Java's `FutureRecordMetadataTest.java` contains two tests:
    // - `testFutureGetWithSeconds`
    // - `testFutureGetWithMilliSeconds`
    //
    // Both tests exercise the `chain()` method which supports batch splitting
    // (linking a chained future to a parent future). The `chain()` method is
    // intentionally out of scope for Phase 1 as per the milestone plan.
    // These tests will be translated when `chain()` is implemented in a
    // later phase.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Additional unit tests
    // -----------------------------------------------------------------------

    /// Tests that dropping the sender causes the future to return an error.
    #[tokio::test]
    async fn test_sender_dropped() {
        let (sender, mut future) = create();
        drop(sender);

        let result = future.get().await;
        assert!(result.is_err());
    }

    /// Tests the `get()` method directly.
    #[tokio::test]
    async fn test_get() {
        let (sender, mut future) = create();
        let tp = TopicPartition::new("topic".to_string(), 1);
        let metadata = RecordMetadata::new(tp, 100, 3, 5000, 10, 20);
        sender.send(Ok(metadata)).unwrap();

        let result = future.get().await;
        assert!(result.is_ok());
        let md = result.unwrap();
        assert_eq!(md.offset(), 103);
        assert_eq!(md.timestamp(), 5000);
        assert_eq!(md.topic(), "topic");
        assert_eq!(md.partition(), 1);
    }

    /// Tests creating via `FutureRecordMetadata::new`.
    #[tokio::test]
    async fn test_new_constructor() {
        let (tx, rx) = oneshot::channel();
        let mut future = FutureRecordMetadata::new(rx);

        let tp = TopicPartition::new("t".to_string(), 0);
        let md = RecordMetadata::new(tp, 0, 0, 0, 0, 0);
        tx.send(Ok(md)).unwrap();

        let result = future.get().await;
        assert!(result.is_ok());
    }

    /// Tests is_done after completion via Future trait.
    #[tokio::test]
    async fn test_is_done_after_completion() {
        let (sender, mut future) = create();
        assert!(!future.is_done());

        let tp = TopicPartition::new("t".to_string(), 0);
        let md = RecordMetadata::new(tp, 0, 0, 0, 0, 0);
        sender.send(Ok(md)).unwrap();

        // Poll the future to completion
        let result = (&mut future).await;
        assert!(result.is_ok());
        assert!(future.is_done());
    }

    /// Tests that `get()` can be called multiple times and returns the same result.
    #[tokio::test]
    async fn test_get_multiple_calls() {
        let (sender, mut future) = create();
        let tp = TopicPartition::new("topic".to_string(), 0);
        let metadata = RecordMetadata::new(tp, 42, 0, 1000, 5, 10);
        sender.send(Ok(metadata)).unwrap();

        // First call
        let result1 = future.get().await;
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap().offset(), 42);

        // Second call should return the same cached result
        let result2 = future.get().await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap().offset(), 42);
    }
}
