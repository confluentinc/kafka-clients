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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
    let done = Arc::new(AtomicBool::new(false));
    (tx, FutureRecordMetadata { receiver: rx, done })
}

/// The future result of a record send.
///
/// Wraps a `tokio::sync::oneshot::Receiver` that will resolve to either a
/// [`RecordMetadata`] on success or a [`KafkaError`] on failure.
///
/// Implements [`Future`] so it can be `.await`ed directly.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.producer.internals.FutureRecordMetadata`.
///
/// # Examples
///
/// ```no_run
/// use confluent_kafka_rust::clients::producer::future_record_metadata;
/// use confluent_kafka_rust::clients::producer::RecordMetadata;
/// use confluent_kafka_rust::common::TopicPartition;
///
/// # async fn example() {
/// let (sender, future) = future_record_metadata::create();
///
/// let tp = TopicPartition::new("topic".to_string(), 0);
/// let metadata = RecordMetadata::new(tp, 10, 0, 1000, 3, 5);
/// sender.send(Ok(metadata)).unwrap();
///
/// let result = future.await;
/// assert!(result.is_ok());
/// assert_eq!(result.unwrap().offset(), 10);
/// # }
/// ```
pub struct FutureRecordMetadata {
    receiver: oneshot::Receiver<Result<RecordMetadata, KafkaError>>,
    done: Arc<AtomicBool>,
}

impl FutureRecordMetadata {
    /// Creates a `FutureRecordMetadata` from an existing oneshot receiver.
    pub fn new(receiver: oneshot::Receiver<Result<RecordMetadata, KafkaError>>) -> Self {
        Self { receiver, done: Arc::new(AtomicBool::new(false)) }
    }

    /// Awaits the completion of the record send and returns the result.
    ///
    /// Returns `Ok(RecordMetadata)` on success, or `Err(KafkaError)` if the
    /// send failed or the sender was dropped.
    pub async fn get(self) -> Result<RecordMetadata, KafkaError> {
        match self.receiver.await {
            Ok(result) => {
                self.done.store(true, Ordering::Release);
                result
            },
            Err(_) => {
                self.done.store(true, Ordering::Release);
                Err(KafkaError::with_message(
                    crate::common::protocol::Errors::UnknownServerError,
                    "Producer was dropped before completing the send",
                ))
            },
        }
    }

    /// Checks if the result is available without blocking.
    ///
    /// Returns `true` if the sender has completed (either successfully or
    /// with an error) or been dropped, `false` if still pending.
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

impl Future for FutureRecordMetadata {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(result)) => {
                self.done.store(true, Ordering::Release);
                Poll::Ready(result)
            },
            Poll::Ready(Err(_)) => {
                self.done.store(true, Ordering::Release);
                Poll::Ready(Err(KafkaError::with_message(
                    crate::common::protocol::Errors::UnknownServerError,
                    "Producer was dropped before completing the send",
                )))
            },
            Poll::Pending => Poll::Pending,
        }
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
    /// Tests that waiting on a request that never completes times out.
    /// In Rust, we use `tokio::time::timeout` instead of Java's `Future.get(timeout)`.
    #[tokio::test]
    async fn test_timeout() {
        let base_offset = 45_i64;
        let rel_offset = 5;

        let (sender, future) = create();
        assert!(!future.is_done(), "Request is not completed");

        // Attempt to get with a short timeout — should time out
        let timeout_result = tokio::time::timeout(std::time::Duration::from_millis(5), future.get()).await;
        assert!(timeout_result.is_err(), "Request should have timed out");

        // Complete the request on a new future (the old one was consumed by get())
        let (sender2, future2) = create();
        let tp = TopicPartition::new("test".to_string(), 0);
        let metadata = RecordMetadata::new(tp, base_offset, rel_offset, -1, 0, 0);
        sender2.send(Ok(metadata)).unwrap();
        drop(sender); // drop unused sender

        let result = future2.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().offset(), base_offset + i64::from(rel_offset));
    }

    /// Translated from `RecordSendTest.testError`.
    ///
    /// Tests that an asynchronous request will eventually return the right error.
    #[tokio::test]
    async fn test_error() {
        let (sender, future) = create();

        // Complete with error after a short delay (simulating async completion)
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = sender.send(Err(KafkaError::new(crate::common::protocol::Errors::CorruptMessage)));
        });

        let result = future.await;
        assert!(result.is_err());
    }

    /// Translated from `RecordSendTest.testBlocking`.
    ///
    /// Tests that an asynchronous request will eventually return the right offset.
    #[tokio::test]
    async fn test_blocking() {
        let base_offset = 45_i64;
        let rel_offset = 5;

        let (sender, future) = create();

        // Complete successfully after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let tp = TopicPartition::new("test".to_string(), 0);
            let metadata = RecordMetadata::new(tp, base_offset, rel_offset, -1, 0, 0);
            let _ = sender.send(Ok(metadata));
        });

        let result = future.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().offset(), base_offset + i64::from(rel_offset));
    }

    // -----------------------------------------------------------------------
    // Additional unit tests
    // -----------------------------------------------------------------------

    /// Tests that dropping the sender causes the future to return an error.
    #[tokio::test]
    async fn test_sender_dropped() {
        let (sender, future) = create();
        drop(sender);

        let result = future.await;
        assert!(result.is_err());
    }

    /// Tests the `get()` method directly.
    #[tokio::test]
    async fn test_get() {
        let (sender, future) = create();
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
        let future = FutureRecordMetadata::new(rx);

        let tp = TopicPartition::new("t".to_string(), 0);
        let md = RecordMetadata::new(tp, 0, 0, 0, 0, 0);
        tx.send(Ok(md)).unwrap();

        let result = future.await;
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
}
