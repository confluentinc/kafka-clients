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

//! Translation of `org.apache.kafka.common.KafkaFuture`.
//!
//! Java declares an abstract class
//! `public abstract class KafkaFuture<T> implements Future<T>`. Most of
//! its abstract surface (`thenApply`, `whenComplete`, `complete`,
//! `completeExceptionally`, `cancel`, `isCompletedExceptionally`,
//! `toCompletionStage`, `getNow`, the static factories
//! `completedFuture` and `allOf`) is out of Milestone-1 scope and would
//! pull the consumer / admin client surface in. This translation
//! ports only the methods that
//! [`org.apache.kafka.clients.producer.Producer#send`](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/Producer.html#send(org.apache.kafka.clients.producer.ProducerRecord))
//! actually surfaces to callers:
//!
//! | Java | Rust |
//! |------|------|
//! | `T get() throws InterruptedException, ExecutionException` | [`KafkaFuture::get`] |
//! | `T get(long, TimeUnit) throws ...` | [`KafkaFuture::get_timeout`] |
//! | `boolean isDone()` | [`KafkaFuture::is_done`] |
//!
//! Java uses inheritance (abstract methods + subclass override) to
//! plug in a concrete future like
//! [`FutureRecordMetadata`](crate::producer::internals::future_record_metadata::FutureRecordMetadata).
//! Rust does not have an inheritance shape, so the abstract-method set
//! becomes a `pub(crate) trait` [`KafkaFutureOps`] and the public
//! `KafkaFuture<T>` is a thin newtype around `Arc<dyn KafkaFutureOps<T>>`.
//!
//! # Allocation cost
//!
//! `KafkaFuture<T>` pays one heap allocation per future: the
//! `Arc<dyn KafkaFutureOps<T>>`. This mirrors Java's per-`Future`
//! allocation cost (every `KafkaFutureImpl` / `FutureRecordMetadata`
//! is one JVM heap allocation). CLAUDE.md rule 11's prohibition is on
//! type-erased `Pin<Box<dyn Future>>` *where an `impl Future` would
//! do* — the per-call producer-`send` allocation that Java's
//! `Future<RecordMetadata>` contract requires is unchanged.
//!
//! # Deferred / out-of-scope
//!
//! * `cancel(boolean)` / `isCancelled()` — Java's
//!   `FutureRecordMetadata.cancel()` always returns `false` and
//!   `isCancelled()` always returns `false`. The producer `send`
//!   contract does not support cancellation. Other Java callers
//!   (admin, consumer) will need cancellation; they re-introduce it
//!   in their own milestone.
//! * `thenApply` / `whenComplete` / `complete` / `completeExceptionally`
//!   — compositional API, not used by the producer surface.
//! * `getNow(valueIfAbsent)` — convenience method, not used by the
//!   producer surface.
//! * `toCompletionStage()` — bridge to Java's
//!   `java.util.concurrent.CompletionStage`, no Rust analogue
//!   required.
//! * Static factories `completedFuture(value)` and `allOf(...)` —
//!   admin/consumer surface; deferred.
//!
//! [`KafkaFuture::get`]: KafkaFuture::get
//! [`KafkaFuture::get_timeout`]: KafkaFuture::get_timeout
//! [`KafkaFuture::is_done`]: KafkaFuture::is_done

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::common::errors::KafkaError;

/// Internal trait that concrete future types (such as
/// [`FutureRecordMetadata`](crate::producer::internals::future_record_metadata::FutureRecordMetadata))
/// implement so they can be wrapped in [`KafkaFuture<T>`].
///
/// Mirrors the abstract-method set Java's
/// `org.apache.kafka.common.KafkaFuture<T>` declares. Crate-private
/// because Java's abstract methods are not a public extension point
/// for the producer use case — they exist for the
/// `KafkaFutureImpl` package-private subclass and (transitively) for
/// the `FutureRecordMetadata` that the producer returns.
///
/// # Bounds
///
/// `Send + Sync` mirrors Java's de-facto contract: `KafkaFuture`
/// instances are passed between threads (the IO thread completes
/// them; the user thread awaits them). Rust requires the bound be
/// explicit.
///
/// # `get` return shape
///
/// `get` returns a boxed future (`Pin<Box<dyn Future<Output = ...> +
/// Send + '_>>`) because the trait must be object-safe to support
/// `Arc<dyn KafkaFutureOps<T>>`. This is the one allocation the trait
/// shape demands; everything else stays on the stack.
pub(crate) trait KafkaFutureOps<T: Send>: Send + Sync {
    /// Implementation of [`KafkaFuture::get`]. Awaits the underlying
    /// future and returns the result.
    fn get<'a>(&'a self) -> Pin<Box<dyn std::future::Future<Output = Result<T, KafkaError>> + Send + 'a>>;

    /// Implementation of [`KafkaFuture::is_done`].
    fn is_done(&self) -> bool;
}

/// A flexible future which the producer returns from
/// [`Producer::send`](crate::producer::Producer::send).
///
/// Java's `KafkaFuture<T>` is an abstract class implementing
/// `java.util.concurrent.Future<T>`. The Rust translation surfaces
/// the three methods that
/// `org.apache.kafka.clients.producer.Producer#send` callers actually
/// use:
///
/// * [`KafkaFuture::get`] — await the result (Java's
///   `T get() throws InterruptedException, ExecutionException`).
/// * [`KafkaFuture::get_timeout`] — await with a deadline (Java's
///   `T get(long timeout, TimeUnit unit) throws ...,
///   TimeoutException`).
/// * [`KafkaFuture::is_done`] — non-blocking completion query
///   (Java's `boolean isDone()`).
///
/// See the module-level docs for the Milestone-1 method-surface
/// rationale and the deferred-method list.
///
/// # Send + Sync
///
/// `KafkaFuture<T>` is `Send + Sync` so it can be moved across
/// Tokio task boundaries (the IO thread completes the underlying
/// `FutureRecordMetadata`; the user task awaits it). Java's
/// equivalent is the JVM's heap-shared `Future` reference.
pub struct KafkaFuture<T: Send + 'static> {
    inner: Arc<dyn KafkaFutureOps<T>>,
}

impl<T: Send + 'static> KafkaFuture<T> {
    /// Wrap a concrete [`KafkaFutureOps`] implementor.
    ///
    /// Internal because the trait itself is `pub(crate)` — only
    /// in-crate code knows how to build a [`KafkaFutureOps`].
    ///
    /// Commit 1 ports the wrapper in isolation; commit 2 wires
    /// `FutureRecordMetadata` to implement `KafkaFutureOps`, and
    /// commit 3 makes the trait method on `Producer::send` return
    /// this type — at which point `new` becomes the
    /// `KafkaProducer::send` path's per-record allocation site.
    #[allow(dead_code)] // Wired by commit 3 of Phase 7g (Producer::send → KafkaFuture<RecordMetadata>).
    pub(crate) fn new(inner: Arc<dyn KafkaFutureOps<T>>) -> Self {
        KafkaFuture { inner }
    }

    /// Waits if necessary for this future to complete, and then returns
    /// its result. Mirrors Java's
    /// `T get() throws InterruptedException, ExecutionException`.
    ///
    /// On success returns `Ok(T)`. On failure (Java's
    /// `ExecutionException` cause) returns `Err(KafkaError)`. Java's
    /// `InterruptedException` has no Rust analogue: a Tokio task
    /// cancellation drops the future without surfacing an error.
    pub async fn get(&self) -> Result<T, KafkaError> {
        self.inner.get().await
    }

    /// Waits if necessary for at most the given time for this future
    /// to complete, and then returns its result, if available.
    /// Mirrors Java's
    /// `T get(long timeout, TimeUnit unit) throws InterruptedException,
    /// ExecutionException, TimeoutException`.
    ///
    /// Returns `Err(KafkaError::Timeout(...))` if the deadline fires
    /// before the future completes. Tokio's
    /// [`tokio::time::timeout`] is the natural Rust idiom: it cancels
    /// the inner future on deadline and yields an `Elapsed` error.
    /// We translate `Elapsed` into Java's `TimeoutException` shape via
    /// [`KafkaError::Timeout`].
    ///
    /// [`tokio::time::timeout`]: https://docs.rs/tokio/latest/tokio/time/fn.timeout.html
    pub async fn get_timeout(&self, timeout: Duration) -> Result<T, KafkaError> {
        match tokio::time::timeout(timeout, self.inner.get()).await {
            Ok(result) => result,
            Err(_) => Err(KafkaError::Timeout(format!(
                "Timeout after waiting for {} ms.",
                timeout.as_millis()
            ))),
        }
    }

    /// Returns `true` if completed in any fashion: normally or
    /// exceptionally. Mirrors Java's `boolean isDone()`.
    ///
    /// Non-blocking — does not poll the underlying future. Useful
    /// for `assert!(!future.is_done())` style invariants at the
    /// moment a `send()` returns (before the broker has acked).
    pub fn is_done(&self) -> bool {
        self.inner.is_done()
    }
}

impl<T: Send + 'static> std::fmt::Debug for KafkaFuture<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaFuture").field("is_done", &self.is_done()).finish()
    }
}

#[cfg(test)]
mod tests {
    //! Smoke tests for the [`KafkaFuture`] wrapper.
    //!
    //! These do not translate a specific Java test (Java's
    //! `KafkaFutureTest` exercises `thenApply` / `whenComplete` /
    //! `complete` / `completeExceptionally`, which are not part of
    //! the Milestone-1 surface). Instead we pin the three contract
    //! pieces this wrapper exposes:
    //!
    //! 1. `get()` round-trips a value from a hand-rolled
    //!    [`KafkaFutureOps`] implementor.
    //! 2. `get_timeout()` returns [`KafkaError::Timeout`] when the
    //!    deadline elapses.
    //! 3. `is_done()` reflects the state of the underlying ops
    //!    impl before vs. after completion.
    //! 4. `get()` propagates the error variant from the inner ops.
    //!
    //! Together these prove the wrapper does not add or lose any
    //! observable behaviour relative to the underlying
    //! [`KafkaFutureOps`].

    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    /// Hand-rolled `KafkaFutureOps<T>` for testing. Backed by a
    /// `Mutex<Option<Result<T, KafkaError>>>` plus a `Notify` that
    /// wakes a single `get()` await. Mirrors the role
    /// `ProduceRequestResult` plays for the real
    /// `FutureRecordMetadata`, in miniature.
    struct FakeOps<T: Send + Clone + 'static> {
        result: Mutex<Option<Result<T, KafkaError>>>,
        done: AtomicBool,
        notify: Notify,
    }

    impl<T: Send + Clone + 'static> FakeOps<T> {
        fn new() -> Arc<Self> {
            Arc::new(FakeOps { result: Mutex::new(None), done: AtomicBool::new(false), notify: Notify::new() })
        }

        fn complete(&self, value: T) {
            *self.result.lock().expect("poisoned") = Some(Ok(value));
            self.done.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }

        fn complete_err(&self, err: KafkaError) {
            *self.result.lock().expect("poisoned") = Some(Err(err));
            self.done.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }
    }

    impl<T: Send + Clone + 'static> KafkaFutureOps<T> for FakeOps<T> {
        fn get<'a>(&'a self) -> Pin<Box<dyn std::future::Future<Output = Result<T, KafkaError>> + Send + 'a>> {
            Box::pin(async move {
                if !self.done.load(Ordering::Acquire) {
                    // Subscribe BEFORE re-checking the flag — avoids the
                    // notify-before-await race documented on
                    // `Notify::notified`.
                    let notified = self.notify.notified();
                    if !self.done.load(Ordering::Acquire) {
                        notified.await;
                    }
                }
                let guard = self.result.lock().expect("poisoned");
                guard.as_ref().expect("done flag set without a result").clone()
            })
        }

        fn is_done(&self) -> bool {
            self.done.load(Ordering::Acquire)
        }
    }

    /// `get()` round-trips a `T` from a hand-rolled ops impl.
    #[tokio::test]
    async fn get_returns_completed_value() {
        let ops = FakeOps::<i32>::new();
        let fut = KafkaFuture::new(Arc::clone(&ops) as Arc<dyn KafkaFutureOps<i32>>);
        let ops_for_task = Arc::clone(&ops);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            ops_for_task.complete(42);
        });
        let v = fut.get().await.expect("future should resolve");
        assert_eq!(v, 42);
    }

    /// `get_timeout()` returns `KafkaError::Timeout` when the deadline
    /// elapses and the inner future has not completed.
    #[tokio::test]
    async fn get_timeout_returns_timeout_error_on_deadline() {
        let ops = FakeOps::<i32>::new();
        let fut = KafkaFuture::new(Arc::clone(&ops) as Arc<dyn KafkaFutureOps<i32>>);
        // Do not complete the ops — the deadline must fire.
        let err = fut.get_timeout(Duration::from_millis(20)).await.expect_err("expected timeout");
        match err {
            KafkaError::Timeout(msg) => assert!(
                msg.contains("Timeout after waiting"),
                "expected Java-shaped Timeout message, got: {msg}",
            ),
            other => panic!("expected Timeout variant, got {other:?}"),
        }
    }

    /// `is_done()` reflects the state of the underlying ops impl
    /// before and after completion. Non-blocking.
    #[tokio::test]
    async fn is_done_reflects_state_transitions() {
        let ops = FakeOps::<i32>::new();
        let fut = KafkaFuture::new(Arc::clone(&ops) as Arc<dyn KafkaFutureOps<i32>>);
        assert!(!fut.is_done(), "future should not be done before completion");
        ops.complete(7);
        assert!(fut.is_done(), "future should be done after completion");
    }

    /// `get()` propagates the error variant from the inner ops.
    #[tokio::test]
    async fn get_propagates_inner_error() {
        let ops = FakeOps::<i32>::new();
        let fut = KafkaFuture::new(Arc::clone(&ops) as Arc<dyn KafkaFutureOps<i32>>);
        ops.complete_err(KafkaError::CorruptRecord("boom".to_owned()));
        let err = fut.get().await.expect_err("expected inner error");
        assert!(matches!(err, KafkaError::CorruptRecord(_)), "got {err:?}");
    }
}
