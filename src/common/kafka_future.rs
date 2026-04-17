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
