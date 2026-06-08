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

//! A flexible future for asynchronous Kafka operations.
//!
//! Translated from `org.apache.kafka.common.KafkaFuture`.
//!
//! In Java, `KafkaFuture<T>` is an abstract class implementing `Future<T>`.
//! In the Producer API, Java's `Producer.send()` returns `Future<RecordMetadata>`,
//! hiding the internal `FutureRecordMetadata` behind the `Future` interface.
//!
//! In Rust, `KafkaFuture<T>` serves as the concrete return type that hides
//! internal implementations behind a public, type-erased interface.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::common::KafkaError;

/// Internal trait representing the abstract methods of `KafkaFuture`.
///
/// This is the Rust equivalent of the abstract method set in Java's `KafkaFuture<T>`.
/// Different internal types (e.g., `FutureRecordMetadata` for the producer)
/// implement this trait and are wrapped in a `KafkaFuture<T>`.
pub(crate) trait KafkaFutureOps<T: Send>: Send + Sync {
    /// Await the result of this future.
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<T, KafkaError>> + Send + '_>>;

    /// Await the result of this future with a timeout.
    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<T, KafkaError>> + Send + '_>>;

    /// Whether this future is complete.
    fn is_done(&self) -> bool;
}

/// A flexible future which supports async result retrieval.
///
/// Translated from `org.apache.kafka.common.KafkaFuture`.
///
/// In Java, `KafkaFuture<T>` implements `Future<T>` and provides methods like
/// `get()`, `get(timeout, unit)`, and `isDone()`. In Rust, this struct wraps
/// an internal implementation and provides equivalent async methods.
///
/// This type is used as the return type for `Producer.send()`, hiding the
/// internal `FutureRecordMetadata` behind a public interface — matching how
/// Java's `Producer.send()` returns `Future<RecordMetadata>` rather than the
/// internal implementation type.
pub struct KafkaFuture<T: Send + 'static> {
    inner: Arc<dyn KafkaFutureOps<T>>,
}

impl<T: Send + 'static> KafkaFuture<T> {
    /// Create a new `KafkaFuture` wrapping an internal implementation.
    pub(crate) fn new(inner: Arc<dyn KafkaFutureOps<T>>) -> Self {
        Self { inner }
    }

    /// Create a `KafkaFuture` that is already resolved with the given result.
    ///
    /// Useful when the result is known at the time the future is constructed —
    /// for example when implementing the [`Producer`](crate::producer::Producer)
    /// trait by awaiting a remote call and wrapping the response in a future
    /// to satisfy the trait's return type. Analogous to [`std::future::ready`].
    ///
    /// Both [`get`](Self::get) and [`get_timeout`](Self::get_timeout) resolve
    /// immediately with a clone of the result; [`is_done`](Self::is_done)
    /// returns `true`.
    pub fn completed(result: Result<T, KafkaError>) -> Self
    where
        T: Clone + Sync,
    {
        Self { inner: Arc::new(CompletedFuture { result }) }
    }

    /// Await the result of this future.
    ///
    /// This is the Rust equivalent of Java's `Future.get()`.
    ///
    /// # Errors
    ///
    /// Returns the error from the underlying operation if it failed.
    pub async fn get(&self) -> Result<T, KafkaError> {
        self.inner.get().await
    }

    /// Await the result of this future with a timeout.
    ///
    /// This is the Rust equivalent of Java's `Future.get(timeout, unit)`.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum time to wait
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Timeout`] if the timeout elapses before the result
    /// is available. Returns the error from the underlying operation if it failed.
    pub async fn get_timeout(&self, timeout: Duration) -> Result<T, KafkaError> {
        self.inner.get_timeout(timeout).await
    }

    /// Whether this future is complete.
    ///
    /// This is the Rust equivalent of Java's `Future.isDone()`.
    pub fn is_done(&self) -> bool {
        self.inner.is_done()
    }
}

impl<T: Send + 'static> Clone for KafkaFuture<T> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<T: Send + 'static> std::fmt::Debug for KafkaFuture<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaFuture").field("is_done", &self.is_done()).finish()
    }
}

/// Internal `KafkaFutureOps` impl for an already-resolved future.
///
/// Used by [`KafkaFuture::completed`] to wrap a value that is already known
/// when the future is constructed. The result is cloned on each `get` call
/// so the future is reusable, matching Java's `Future` semantics.
struct CompletedFuture<T: Clone + Send + Sync + 'static> {
    result: Result<T, KafkaError>,
}

impl<T: Clone + Send + Sync + 'static> KafkaFutureOps<T> for CompletedFuture<T> {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<T, KafkaError>> + Send + '_>> {
        let result = self.result.clone();
        Box::pin(async move { result })
    }

    fn get_timeout(
        &self,
        _timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<T, KafkaError>> + Send + '_>> {
        let result = self.result.clone();
        Box::pin(async move { result })
    }

    fn is_done(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completed_resolves_with_ok_value() {
        let f: KafkaFuture<i32> = KafkaFuture::completed(Ok(42));
        assert!(f.is_done());
        assert_eq!(f.get().await.unwrap(), 42);
        // Reusable across multiple gets, matching Java Future semantics.
        assert_eq!(f.get().await.unwrap(), 42);
        assert_eq!(f.get_timeout(Duration::from_secs(1)).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn completed_resolves_with_err_value() {
        let f: KafkaFuture<i32> = KafkaFuture::completed(Err(KafkaError::IllegalArgument("test".to_string())));
        assert!(f.is_done());
        assert!(matches!(f.get().await, Err(KafkaError::IllegalArgument(_))));
        assert!(matches!(
            f.get_timeout(Duration::from_secs(1)).await,
            Err(KafkaError::IllegalArgument(_))
        ));
    }

    #[tokio::test]
    async fn completed_clone_shares_underlying_result() {
        let f1: KafkaFuture<String> = KafkaFuture::completed(Ok("hello".to_string()));
        let f2 = f1.clone();
        assert_eq!(f1.get().await.unwrap(), "hello");
        assert_eq!(f2.get().await.unwrap(), "hello");
    }
}
