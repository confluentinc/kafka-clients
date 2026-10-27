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
//!
//! [`KafkaFuture::then_apply`] takes a [`BaseFunction`]: a closure, or a value
//! from [`BaseFunction_new`]. Rust infers a closure's parameter type only from
//! an `Fn` bound, not from a trait the closure gets through a blanket impl,
//! so a closure whose body calls a method on its argument writes the
//! argument's type out (`|v: T| ..`). A function held as a trait object
//! (`Box<dyn BaseFunction<T, R>>`, `Arc<..>`) is not itself a
//! `BaseFunction`; it is passed as `move |v| f.apply(v)`:
//!
//! ```
//! use confluent_kafka::common::KafkaFuture;
//! use confluent_kafka::common::kafka_future::{self, BaseFunction};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let future = KafkaFuture::completed_future(Ok("four".to_string()));
//!
//! // The body calls `len()` on `v`, so its type is written out.
//! let len = future.then_apply(|v: String| Ok(v.len()));
//! assert_eq!(len.get().await.unwrap(), 4);
//!
//! // A stored function is forwarded to through a closure.
//! let f: Box<dyn BaseFunction<String, usize>> = Box::new(kafka_future::BaseFunction_new(|v: String| Ok(v.len())));
//! let len = future.then_apply(move |v| f.apply(v));
//! assert_eq!(len.get().await.unwrap(), 4);
//! # }
//! ```

use std::collections::HashMap;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::FutureExt;

use crate::common::Error;
use crate::common::internals::KafkaFutureImpl;

/// A function which takes objects of type `A` and returns objects of type `B`.
///
/// # Translation notes
///
/// Any closure `Fn(A) -> Result<B, Error> + Send + Sync` implements it.
#[doc(alias = "org.apache.kafka.common.KafkaFuture$BaseFunction")]
pub trait BaseFunction<A, B>: Send + Sync {
    /// Applies this function to `a`.
    ///
    /// # Arguments
    ///
    /// - `a` - the value to transform
    ///
    /// # Returns
    ///
    /// The transformed value.
    ///
    /// # Errors
    ///
    /// Any error the function returns; it fails the future `then_apply`
    /// returned, as a throwing Java `apply` does.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture$BaseFunction#apply")]
    fn apply(&self, a: A) -> Result<B, Error>;
}

impl<A, B, F> BaseFunction<A, B> for F
where
    F: Fn(A) -> Result<B, Error> + Send + Sync,
{
    fn apply(&self, a: A) -> Result<B, Error> {
        self(a)
    }
}

/// A [`BaseFunction`] that calls `function`, standing in for a Java lambda
/// implementing `KafkaFuture.BaseFunction`. A method Java adds later as an
/// overload of `apply` gets its own factory, `BaseFunction_with_<parameter>`.
///
/// `BaseFunction` is nested in `KafkaFuture`, so its factory lives in this
/// module with the interface's name as a prefix. `A` is a
/// parameter of this function, not of the caller's, so the closure's
/// parameter type must be written out:
///
/// ```
/// use confluent_kafka::common::kafka_future::{self, BaseFunction};
///
/// let len = kafka_future::BaseFunction_new(|s: String| Ok(s.len()));
/// assert_eq!(len.apply("four".to_string()).unwrap(), 4);
/// ```
///
/// # Arguments
///
/// - `function` - the body of [`apply`](BaseFunction::apply)
///
/// # Returns
///
/// A `BaseFunction` whose `apply` calls `function`.
// stands in for a Java lambda implementing `KafkaFuture.BaseFunction` (CLAUDE.md §3)
#[doc(alias = "rust-only")]
#[expect(non_snake_case)]
pub fn BaseFunction_new<A, B, F>(function: F) -> impl BaseFunction<A, B>
where
    F: Fn(A) -> Result<B, Error> + Send + Sync,
{
    struct FunctionBaseFunction<F>(F);

    impl<A, B, F> BaseFunction<A, B> for FunctionBaseFunction<F>
    where
        F: Fn(A) -> Result<B, Error> + Send + Sync,
    {
        fn apply(&self, a: A) -> Result<B, Error> {
            (self.0)(a)
        }
    }

    FunctionBaseFunction(function)
}

/// Internal trait representing the abstract methods of `KafkaFuture`.
///
/// This is the Rust equivalent of the abstract method set in Java's `KafkaFuture<T>`.
/// Different internal types (e.g., `FutureRecordMetadata` for the producer)
/// implement this trait and are wrapped in a `KafkaFuture<T>`.
pub(crate) trait KafkaFutureOps<T: Send>: Send + Sync {
    /// Await the result of this future.
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>>;

    /// Await the result of this future with a timeout.
    fn get_with_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>>;

    /// Whether this future is complete.
    fn is_done(&self) -> bool;

    /// Registers `callback` to run once with this future's result: on the
    /// code path that completes the future, after the result is stored, or at
    /// once on the calling task when the future is already complete. This is
    /// the hook behind the dependants of a future (`then_apply`, `all_of`),
    /// which Java's `CompletableFuture` completes the same way.
    fn on_complete(self: Arc<Self>, callback: CompletionCallback<T>);
}

/// A callback registered with [`KafkaFutureOps::on_complete`], invoked with a
/// reference to the future's result when it completes.
pub(crate) type CompletionCallback<T> = Box<dyn FnOnce(&Result<T, Error>) + Send>;

/// A flexible future which supports call chaining and other asynchronous
/// programming patterns.
///
/// # Translation notes
///
/// Java's `get()`, `get(timeout, unit)` and `isDone()` come from `Future<T>`;
/// here `get` and `get_with_timeout` are async. This type is also what
/// `Producer::send` returns, where Java returns `Future<RecordMetadata>`.
/// `toCompletionStage` is not translated.
#[doc(alias = "org.apache.kafka.common.KafkaFuture")]
pub struct KafkaFuture<T: Send + 'static> {
    inner: Arc<dyn KafkaFutureOps<T>>,
}

impl<T: Send + 'static> KafkaFuture<T> {
    /// Create a new `KafkaFuture` wrapping an internal implementation.
    pub(crate) fn new(inner: Arc<dyn KafkaFutureOps<T>>) -> Self {
        Self { inner }
    }

    /// Returns a new `KafkaFuture` that is already completed with the given value.
    ///
    /// # Arguments
    ///
    /// - `result` - the value, or the error the future fails with
    ///
    /// # Translation notes
    ///
    /// `result` may also be an error.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#completedFuture")]
    pub fn completed_future(result: Result<T, Error>) -> Self
    where
        T: Clone + Sync,
    {
        Self { inner: Arc::new(CompletedFuture { result }) }
    }

    /// Waits if necessary for this future to complete, and then returns its result.
    ///
    /// # Returns
    ///
    /// The future's value.
    ///
    /// # Errors
    ///
    /// The error the underlying operation failed with.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#get")]
    pub fn get(&self) -> impl Future<Output = Result<T, Error>> + Send {
        async move { self.inner.get().await }
    }

    /// Waits if necessary for at most the given time for this future to
    /// complete, and then returns its result, if available.
    ///
    /// # Arguments
    ///
    /// - `timeout` - the maximum time to wait
    ///
    /// # Returns
    ///
    /// The future's value.
    ///
    /// # Errors
    ///
    /// - [`Error::is_local_timeout_error`] if the wait timed out: Java's
    ///   `java.util.concurrent.TimeoutException`, not the retriable
    ///   `org.apache.kafka.common.errors.TimeoutException`.
    /// - The error the underlying operation failed with.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#get")]
    pub fn get_with_timeout(&self, timeout: Duration) -> impl Future<Output = Result<T, Error>> + Send {
        async move { self.inner.get_with_timeout(timeout).await }
    }

    /// Returns true if completed in any fashion: normally, exceptionally, or via
    /// cancellation.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#isDone")]
    pub fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Returns a new `KafkaFuture` that is completed when all the given futures
    /// have completed. If any future fails, the returned future returns its
    /// error. If multiple futures fail, which error gets returned is
    /// arbitrarily chosen.
    ///
    /// # Arguments
    ///
    /// - `futures` - the futures to wait for
    ///
    /// # Translation notes
    ///
    /// Unlike Java's variadic `allOf`, the futures share one type `T`. The
    /// error is the first failed future's in `futures`.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#allOf")]
    pub fn all_of(futures: Vec<KafkaFuture<T>>) -> KafkaFuture<()>
    where
        T: Clone + Sync,
    {
        let all = KafkaFutureImpl::new();
        if futures.is_empty() {
            all.complete(());
            return all.future();
        }
        // The number of futures still pending, and the first failure by
        // position, as `CompletableFuture.allOf` relays the leftmost one.
        let state = Arc::new(Mutex::new((futures.len(), None::<(usize, Error)>)));
        for (index, future) in futures.into_iter().enumerate() {
            let state = Arc::clone(&state);
            let all = all.clone();
            future.on_complete(move |result| {
                let outcome = {
                    let mut guard = state.lock().unwrap_or_else(PoisonError::into_inner);
                    let (remaining, first_error) = &mut *guard;
                    if let Err(error) = result
                        && first_error.as_ref().is_none_or(|(first, _)| index < *first)
                    {
                        *first_error = Some((index, error.clone()));
                    }
                    *remaining -= 1;
                    (*remaining == 0).then(|| first_error.take())
                };
                // Completed outside the lock: `all`'s own dependants run here.
                match outcome {
                    Some(None) => _ = all.complete(()),
                    Some(Some((_, error))) => _ = all.complete_with_error(error),
                    None => {},
                }
            });
        }
        all.future()
    }

    /// Returns a future that completes when all the given keyed futures
    /// complete, yielding a map from each key to its value. As soon as one
    /// future fails, the returned future fails with its error.
    ///
    /// This is the combinator behind the admin `*Result` aggregators
    /// (`DescribeTopicsResult::all_topic_names`, etc.). Java expresses the same
    /// thing inline as `KafkaFuture.allOf(...).thenApply(v -> collect each
    /// future.get())`, or, in `ListTransactionsResult.allByBrokerId`, with one
    /// `whenComplete` per future, which this is.
    pub(crate) fn join_map<K>(entries: Vec<(K, KafkaFuture<T>)>) -> KafkaFuture<HashMap<K, T>>
    where
        T: Clone + Sync,
        K: Hash + Eq + Clone + Send + Sync + 'static,
    {
        let joined = KafkaFutureImpl::new();
        if entries.is_empty() {
            joined.complete(HashMap::new());
            return joined.future();
        }
        let state = Arc::new(Mutex::new((entries.len(), HashMap::with_capacity(entries.len()))));
        for (key, future) in entries {
            let state = Arc::clone(&state);
            let joined = joined.clone();
            future.on_complete(move |result| match result {
                Err(error) => _ = joined.complete_with_error(error.clone()),
                Ok(value) => {
                    let map = {
                        let mut guard = state.lock().unwrap_or_else(PoisonError::into_inner);
                        let (remaining, map) = &mut *guard;
                        map.insert(key, value.clone());
                        *remaining -= 1;
                        (*remaining == 0).then(|| std::mem::take(map))
                    };
                    // Completed outside the lock: `joined`'s own dependants run here.
                    if let Some(map) = map {
                        joined.complete(map);
                    }
                },
            });
        }
        joined.future()
    }

    /// Returns a new `KafkaFuture` that, when this future completes normally,
    /// is executed with this future's result as the argument to the supplied
    /// function. The function may be invoked by the task that calls
    /// `then_apply` or it may be invoked by the task that completes the future.
    ///
    /// # Arguments
    ///
    /// - `function` - a closure or a [`BaseFunction_new`] value; the module
    ///   docs show how to write one
    ///
    /// # Translation notes
    ///
    /// An `Err` from `function`, or a panic in it, fails the returned future,
    /// as a Java throw does.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#thenApply")]
    pub fn then_apply<R, F>(&self, function: F) -> KafkaFuture<R>
    where
        T: Clone + Sync,
        R: Clone + Send + Sync + 'static,
        F: BaseFunction<T, R> + 'static,
    {
        let applied = KafkaFutureImpl::new();
        let completer = applied.clone();
        self.on_complete(move |result| match result {
            Ok(value) => _ = completer.complete_result(apply_function(&function, value.clone())),
            // Java's `CompletableFuture.thenApply` skips the function and
            // fails the dependant with the source's failure.
            Err(error) => _ = completer.complete_with_error(error.clone()),
        });
        applied.future()
    }

    /// Registers `callback` to run once with this future's result, on the
    /// code path completing it, or at once on the calling task when it is
    /// already complete (see [`KafkaFutureOps::on_complete`]).
    pub(crate) fn on_complete<F>(&self, callback: F)
    where
        F: FnOnce(&Result<T, Error>) + Send + 'static,
    {
        Arc::clone(&self.inner).on_complete(Box::new(callback));
    }

    /// The result of this future when it is already complete, read without
    /// waiting, as Java's `getNow` reads a future that `allOf` has completed;
    /// `None` while it is pending.
    pub(crate) fn result_if_done(&self) -> Option<Result<T, Error>> {
        if !self.is_done() {
            return None;
        }
        self.inner.get().now_or_never()
    }

    /// A view of this future whose value is `map` applied to this one's, each
    /// time it is read: by every `get`, and for every completion callback.
    ///
    /// Not `then_apply`, which runs its function once, eagerly: the C binding
    /// maps every future it hands out to its C value with this, a pure
    /// conversion that has no reason to run, nor to allocate the C value, for
    /// a future nobody reads (a producer record's, one per record). No Java
    /// counterpart (DoD #7): Java hands out its futures as they are.
    /// Crate-internal, and only the `ffi` feature names it.
    #[cfg(feature = "ffi")]
    pub(crate) fn map_lazily<R, F>(&self, map: F) -> KafkaFuture<R>
    where
        T: Clone,
        R: Send + 'static,
        F: Fn(T) -> R + Send + Sync + 'static,
    {
        KafkaFuture::new(Arc::new(LazyMapFuture { source: self.clone(), map: Arc::new(map) }))
    }
}

/// Internal `KafkaFutureOps` impl backing [`KafkaFuture::map_lazily`].
#[cfg(feature = "ffi")]
struct LazyMapFuture<T: Send + 'static, R> {
    source: KafkaFuture<T>,
    map: Arc<dyn Fn(T) -> R + Send + Sync>,
}

#[cfg(feature = "ffi")]
impl<T: Clone + Send + 'static, R: Send + 'static> KafkaFutureOps<R> for LazyMapFuture<T, R> {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<R, Error>> + Send + '_>> {
        Box::pin(async move { self.source.get().await.map(&*self.map) })
    }

    fn get_with_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<R, Error>> + Send + '_>> {
        Box::pin(async move { self.source.get_with_timeout(timeout).await.map(&*self.map) })
    }

    fn is_done(&self) -> bool {
        self.source.is_done()
    }

    fn on_complete(self: Arc<Self>, callback: CompletionCallback<R>) {
        let map = Arc::clone(&self.map);
        self.source.on_complete(move |result| match result {
            Ok(value) => callback(&Ok(map(value.clone()))),
            Err(error) => callback(&Err(error.clone())),
        });
    }
}

/// Applies a [`KafkaFuture::then_apply`] function, turning a panic into the
/// error the dependant fails with: the function runs on the code path that
/// completes the source (the admin client's background task, for instance),
/// which a panicking function must not unwind, as a Java function that throws
/// only fails its dependant.
fn apply_function<T, R>(function: &dyn BaseFunction<T, R>, value: T) -> Result<R, Error> {
    std::panic::catch_unwind(AssertUnwindSafe(|| function.apply(value))).unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload");
        Err(Error::local_illegal_state(format!(
            "KafkaFuture.then_apply function panicked: {message}"
        )))
    })
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
/// Used by [`KafkaFuture::completed_future`] to wrap a value that is already known
/// when the future is constructed. The result is cloned on each `get` call
/// so the future is reusable, matching Java's `Future` semantics.
struct CompletedFuture<T: Clone + Send + Sync + 'static> {
    result: Result<T, Error>,
}

impl<T: Clone + Send + Sync + 'static> KafkaFutureOps<T> for CompletedFuture<T> {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>> {
        let result = self.result.clone();
        Box::pin(async move { result })
    }

    fn get_with_timeout(
        &self,
        _timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>> {
        let result = self.result.clone();
        Box::pin(async move { result })
    }

    fn is_done(&self) -> bool {
        true
    }

    fn on_complete(self: Arc<Self>, callback: CompletionCallback<T>) {
        callback(&self.result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::Errors;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[tokio::test]
    async fn completed_resolves_with_ok_value() {
        let f: KafkaFuture<i32> = KafkaFuture::completed_future(Ok(42));
        assert!(f.is_done());
        assert_eq!(f.get().await.unwrap(), 42);
        // Reusable across multiple gets, matching Java Future semantics.
        assert_eq!(f.get().await.unwrap(), 42);
        assert_eq!(f.get_with_timeout(Duration::from_secs(1)).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn completed_resolves_with_err_value() {
        let f: KafkaFuture<i32> = KafkaFuture::completed_future(Err(Error::local_illegal_argument("test".to_string())));
        assert!(f.is_done());
        assert!(matches!(f.get().await, Err(Error::LocalIllegalArgument(_))));
        assert!(matches!(
            f.get_with_timeout(Duration::from_secs(1)).await,
            Err(Error::LocalIllegalArgument(_))
        ));
    }

    #[tokio::test]
    async fn completed_clone_shares_underlying_result() {
        let f1: KafkaFuture<String> = KafkaFuture::completed_future(Ok("hello".to_string()));
        let f2 = f1.clone();
        assert_eq!(f1.get().await.unwrap(), "hello");
        assert_eq!(f2.get().await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn impl_completes_value_after_the_fact() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let future = handle.future();
        assert!(!future.is_done());

        let completer = handle.clone();
        let task = tokio::spawn(async move { future.get().await });
        // Give the awaiting task a chance to register interest, then complete.
        tokio::task::yield_now().await;
        assert!(completer.complete(42));
        // Second completion is a no-op and returns false (first writer wins).
        assert!(!completer.complete(99));

        assert_eq!(task.await.unwrap().unwrap(), 42);
        assert!(handle.is_done());
    }

    #[tokio::test]
    async fn impl_completes_with_error() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let future = handle.future();
        assert!(handle.complete_with_error(Error::local_illegal_argument("boom".to_string())));
        match future.get().await {
            Err(Error::LocalIllegalArgument(msg)) => assert_eq!(msg.message(), "boom"),
            other => panic!("expected IllegalArgument, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn impl_get_timeout_elapses_when_never_completed() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let future = handle.future();
        let err = match future.get_with_timeout(Duration::from_millis(20)).await {
            Err(e) => e,
            other => panic!("expected a timeout, got {other:?}"),
        };
        // Java throws `java.util.concurrent.TimeoutException`
        // (`org.apache.kafka.common.KafkaFuture#get`), a checked exception beside the Kafka
        // hierarchy — so none of the predicates hold and there is no wire code.
        // `Error::Timeout` would be
        // `org.apache.kafka.common.errors.TimeoutException`: retriable, an
        // api error, and code 7.
        assert!(matches!(err, Error::LocalTimeout(_)), "got {err:?}");
        assert!(!err.is_retriable_error());
        assert!(!err.is_api_error());
        assert!(!err.is_kafka_error());
        assert_eq!(Errors::UnknownServerError, err.error());
    }

    #[tokio::test]
    async fn when_complete_runs_eagerly_on_completion() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let seen = Arc::new(Mutex::new(None));
        let seen_clone = Arc::clone(&seen);
        handle.when_complete(move |result| {
            *seen_clone.lock().unwrap() = result.as_ref().ok().copied();
        });
        // Nobody awaits get(); the callback must still fire on complete().
        handle.complete(7);
        assert_eq!(*seen.lock().unwrap(), Some(7));
    }

    #[tokio::test]
    async fn when_complete_runs_immediately_if_already_done() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        handle.complete(5);
        let seen = Arc::new(Mutex::new(None));
        let seen_clone = Arc::clone(&seen);
        handle.when_complete(move |result| {
            *seen_clone.lock().unwrap() = result.as_ref().ok().copied();
        });
        assert_eq!(*seen.lock().unwrap(), Some(5));
    }

    #[tokio::test]
    async fn all_of_succeeds_when_all_succeed() {
        let h1: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let all = KafkaFuture::all_of(vec![h1.future(), h2.future()]);
        h1.complete(1);
        h2.complete(2);
        assert_eq!(all.get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn all_of_fails_if_any_fails() {
        let h1: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let all = KafkaFuture::all_of(vec![h1.future(), h2.future()]);
        h1.complete(1);
        h2.complete_with_error(Error::local_illegal_argument("nope".to_string()));
        assert!(matches!(all.get().await, Err(Error::LocalIllegalArgument(_))));
    }

    #[tokio::test]
    async fn then_apply_transforms_value() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let mapped = handle.future().then_apply(|v| Ok(v * 2));
        handle.complete(21);
        assert_eq!(mapped.get().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn then_apply_takes_a_base_function_made_by_the_factory() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let mapped = handle.future().then_apply(BaseFunction_new(|v: i32| Ok(v.to_string())));
        handle.complete(21);
        assert_eq!(mapped.get().await.unwrap(), "21");
    }

    #[test]
    fn base_function_new_returns_the_function_result() {
        let function = BaseFunction_new(|v: i32| {
            if v < 0 {
                Err(Error::local_illegal_argument("negative".to_string()))
            } else {
                Ok(v * 2)
            }
        });
        assert_eq!(function.apply(4).unwrap(), 8);
        assert!(matches!(function.apply(-1), Err(Error::LocalIllegalArgument(_))));
    }

    #[tokio::test]
    async fn then_apply_propagates_source_error() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let mapped = handle.future().then_apply(|v| Ok(v * 2));
        handle.complete_with_error(Error::local_illegal_argument("src".to_string()));
        assert!(matches!(mapped.get().await, Err(Error::LocalIllegalArgument(_))));
    }

    #[tokio::test]
    async fn join_map_collects_values_when_all_succeed() {
        let h1: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let joined = KafkaFuture::join_map(vec![("a", h1.future()), ("b", h2.future())]);
        h1.complete(1);
        h2.complete(2);
        let map = joined.get().await.unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map["a"], 1);
        assert_eq!(map["b"], 2);
    }

    /// `join_map` short-circuits on the first error — the behavior
    /// `join_map_results` exists to complement. Pinned so the two stay distinct.
    ///
    /// The discriminator is the third entry, which is **never completed**: a
    /// short-circuiting implementation abandons it and returns the error at
    /// once, whereas any collect-all implementation (even one that returned the
    /// first error afterwards) would await it and hang past the timeout below.
    /// Asserting only that the error surfaces would pass for both.
    #[tokio::test]
    async fn join_map_short_circuits_on_first_error() {
        let bad: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let ok: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let never: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let joined = KafkaFuture::join_map(vec![("bad", bad.future()), ("ok", ok.future()), ("never", never.future())]);
        bad.complete_with_error(Error::local_illegal_argument("a failed"));
        ok.complete(2);
        // `never` is deliberately left pending.

        let outcome = tokio::time::timeout(Duration::from_secs(5), joined.get())
            .await
            .expect("join_map must abandon the keys after the failing one, not await them");
        match outcome {
            Err(Error::LocalIllegalArgument(e)) => assert_eq!(e.message(), "a failed"),
            other => panic!("expected IllegalArgument, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn then_apply_can_fail() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let mapped = handle
            .future()
            .then_apply(|_v| Err::<i32, _>(Error::local_illegal_state("bad".to_string())));
        handle.complete(1);
        assert!(matches!(mapped.get().await, Err(Error::LocalIllegalState(_))));
    }

    /// The value of a completed future, read without waiting, as Java's
    /// `getNow(valueIfAbsent)`.
    fn get_now<T: Clone + Send + 'static>(future: &KafkaFuture<T>, value_if_absent: T) -> Result<T, Error> {
        future.result_if_done().unwrap_or(Ok(value_if_absent))
    }

    /// Asserts that `future` failed with `message` and, when given, a source
    /// error carrying `source_message`, as Java's `awaitAndAssertFailure`.
    async fn await_and_assert_failure<T: std::fmt::Debug + Send + 'static>(
        future: &KafkaFuture<T>,
        message: &str,
        source_message: Option<&str>,
    ) {
        assert!(future.is_done());
        let error = future.get().await.expect_err("the future failed");
        assert_eq!(error.message(), message);
        assert_eq!(error.source().map(Error::message), source_message);
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testThenApplyOnSucceededFuture`
    #[test]
    fn test_then_apply_on_succeeded_future() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let doubled_future = future.future().then_apply(|integer| Ok(2 * integer));
        assert!(!doubled_future.is_done());
        let tripled_future = future.future().then_apply(|integer| Ok(3 * integer));
        assert!(!tripled_future.is_done());
        future.complete(21);
        assert_eq!(get_now(&future.future(), -1).unwrap(), 21);
        assert_eq!(get_now(&doubled_future, -1).unwrap(), 42);
        assert_eq!(get_now(&tripled_future, -1).unwrap(), 63);
        let quadrupled_future = future.future().then_apply(|integer| Ok(4 * integer));
        assert_eq!(get_now(&quadrupled_future, -1).unwrap(), 84);
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testThenApplyOnFailedFuture`
    #[tokio::test]
    async fn test_then_apply_on_failed_future() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let dependant_future = future.future().then_apply(|integer| Ok(2 * integer));
        future.complete_with_error(Error::kafka_message("We require more vespene gas"));
        await_and_assert_failure(&future.future(), "We require more vespene gas", None).await;
        await_and_assert_failure(&dependant_future, "We require more vespene gas", None).await;
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testThenApplyOnFailedFutureTricky`:
    /// Java's `CompletionException` wrapping a cause has no Rust counterpart;
    /// its shape here is an error with a source, which the dependant receives
    /// unchanged, source included.
    #[tokio::test]
    async fn test_then_apply_on_failed_future_tricky() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let dependant_future = future.future().then_apply(|integer| Ok(2 * integer));
        future.complete_with_error(Error::kafka_message_source(
            "java.lang.RuntimeException: We require more vespene gas",
            Error::kafka_message("We require more vespene gas"),
        ));
        let message = "java.lang.RuntimeException: We require more vespene gas";
        await_and_assert_failure(&future.future(), message, Some("We require more vespene gas")).await;
        await_and_assert_failure(&dependant_future, message, Some("We require more vespene gas")).await;
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testThenApplyOnFailedFutureTricky2`:
    /// the wrapped cause is one without a message, as Java's
    /// `CancellationException()`.
    #[tokio::test]
    async fn test_then_apply_on_failed_future_tricky2() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let dependant_future = future.future().then_apply(|integer| Ok(2 * integer));
        future.complete_with_error(Error::kafka_message_source(
            "java.util.concurrent.CancellationException",
            Error::kafka_message(""),
        ));
        let message = "java.util.concurrent.CancellationException";
        await_and_assert_failure(&future.future(), message, Some("")).await;
        await_and_assert_failure(&dependant_future, message, Some("")).await;
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testThenApplyOnSucceededFutureAndFunctionThrows`
    #[tokio::test]
    async fn test_then_apply_on_succeeded_future_and_function_throws() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let dependant_future = future
            .future()
            .then_apply(|_integer| Err::<i32, _>(Error::kafka_message("We require more vespene gas")));
        future.complete(21);
        assert_eq!(future.future().get().await.unwrap(), 21);
        await_and_assert_failure(&dependant_future, "We require more vespene gas", None).await;
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testThenApplyOnSucceededFutureAndFunctionThrowsCompletionException`
    #[tokio::test]
    async fn test_then_apply_on_succeeded_future_and_function_throws_completion_exception() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let dependant_future = future.future().then_apply(|_integer| {
            Err::<i32, _>(Error::kafka_message_source(
                "java.lang.RuntimeException: We require more vespene gas",
                Error::kafka_message("We require more vespene gas"),
            ))
        });
        future.complete(21);
        assert_eq!(future.future().get().await.unwrap(), 21);
        await_and_assert_failure(
            &dependant_future,
            "java.lang.RuntimeException: We require more vespene gas",
            Some("We require more vespene gas"),
        )
        .await;
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testThenApplyOnFailedFutureFunctionNotCalled`
    #[tokio::test]
    async fn test_then_apply_on_failed_future_function_not_called() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_function = Arc::clone(&ran);
        let dependant_future = future.future().then_apply(move |_integer| {
            // Because the top level future failed, this should never be called.
            ran_in_function.store(true, Ordering::SeqCst);
            Ok(0)
        });
        future.complete_with_error(Error::kafka_message("We require more minerals"));
        await_and_assert_failure(&future.future(), "We require more minerals", None).await;
        await_and_assert_failure(&dependant_future, "We require more minerals", None).await;
        assert!(!ran.load(Ordering::SeqCst));
    }

    /// The function runs once, when the source completes, with nobody waiting
    /// on the dependant; later reads answer the stored result without running
    /// it again.
    #[tokio::test]
    async fn then_apply_runs_the_function_once_at_completion() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let dependant_future = future.future().then_apply(move |integer| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(2 * integer)
        });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!dependant_future.is_done());
        future.complete(21);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "run by complete(), not by a get");
        assert!(dependant_future.is_done());
        for _ in 0..3 {
            assert_eq!(dependant_future.get().await.unwrap(), 42);
            assert_eq!(dependant_future.get_with_timeout(Duration::from_secs(1)).await.unwrap(), 42);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Over a future already complete, the function runs inside `then_apply`.
    #[test]
    fn then_apply_runs_the_function_inline_when_the_source_is_done() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let dependant_future = KafkaFuture::completed_future(Ok(21)).then_apply(move |integer: i32| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(2 * integer)
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(dependant_future.is_done());
        assert_eq!(get_now(&dependant_future, -1).unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// The function runs on the task completing the source, so a chain of
    /// dependants completes from one `complete`.
    #[test]
    fn then_apply_chain_completes_from_the_completing_call() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let chained = future
            .future()
            .then_apply(|integer| Ok(integer + 1))
            .then_apply(|integer| Ok(integer * 10));
        assert!(!chained.is_done());
        future.complete(1);
        assert_eq!(get_now(&chained, -1).unwrap(), 20);
    }

    /// A panic in the function fails the dependant instead of unwinding the
    /// code path that completes the source.
    #[test]
    fn then_apply_function_panic_fails_the_dependant() {
        let future: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let dependant_future = future.future().then_apply(|_integer| -> Result<i32, Error> { panic!("boom") });
        assert!(future.complete(21));
        let error = get_now(&dependant_future, -1).expect_err("the function panicked");
        assert!(error.is_local_illegal_state_error(), "got {error:?}");
        assert_eq!(error.message(), "KafkaFuture.then_apply function panicked: boom");
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testAllOfFuturesHandlesZeroFutures`
    #[tokio::test]
    async fn test_all_of_futures_handles_zero_futures() {
        let all_future = KafkaFuture::<i32>::all_of(Vec::new());
        assert!(all_future.is_done());
        all_future.get().await.unwrap();
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testAllOfFuturesWithFailure`
    /// shape: `all_of` completes only once every future has, and then with the
    /// failure of the first failed one in the list, as Java's
    /// `CompletableFuture.allOf`.
    #[test]
    fn all_of_waits_for_every_future_and_fails_with_the_first_failure() {
        let futures: Vec<KafkaFutureImpl<i32>> = (0..5).map(|_| KafkaFutureImpl::new()).collect();
        let all_future = KafkaFuture::all_of(futures.iter().map(KafkaFutureImpl::future).collect());
        futures[3].complete_with_error(Error::kafka_message("Fourth one failed"));
        futures[1].complete_with_error(Error::kafka_message("Second one failed"));
        futures[0].complete(0);
        futures[2].complete(2);
        assert!(!all_future.is_done(), "waits for the last future despite the failures");
        futures[4].complete(4);
        let error = get_now(&all_future, ()).expect_err("two futures failed");
        assert_eq!(error.message(), "Second one failed");
    }

    /// `org.apache.kafka.common.KafkaFutureTest#testAllOfFutures` shape: a
    /// dependant of `all_of` completes from the last source's completion,
    /// without anyone waiting.
    #[test]
    fn all_of_completes_its_dependants_from_the_last_completion() {
        let futures: Vec<KafkaFutureImpl<i32>> = (0..5).map(|_| KafkaFutureImpl::new()).collect();
        let sources: Vec<KafkaFuture<i32>> = futures.iter().map(KafkaFutureImpl::future).collect();
        let sum = KafkaFuture::all_of(sources.clone())
            .then_apply(move |()| sources.iter().map(|future| get_now(future, -1)).sum::<Result<i32, Error>>());
        for (index, future) in futures.iter().enumerate() {
            assert!(!sum.is_done());
            future.complete(index as i32);
        }
        assert_eq!(get_now(&sum, -1).unwrap(), 10);
    }
}
