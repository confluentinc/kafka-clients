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

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::common::Error;

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
}

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
    /// Unlike Java's variadic `allOf`, the futures share one type `T`.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#allOf")]
    pub fn all_of(futures: Vec<KafkaFuture<T>>) -> KafkaFuture<()>
    where
        T: Clone + Sync,
    {
        KafkaFuture::new(Arc::new(AllOfFuture { futures }))
    }

    /// Returns a future that completes when all the given keyed futures
    /// complete, yielding a map from each key to its resolved value. If any
    /// future completes exceptionally, the returned future yields that error.
    ///
    /// This is the combinator behind the admin `*Result` aggregators
    /// (`DescribeTopicsResult::all_topic_names`, etc.). Java expresses the same
    /// thing inline as `KafkaFuture.allOf(...).thenApply(v -> collect each
    /// future.get())`; the Rust port names it because the get-driven model
    /// cannot call `.get()` synchronously inside a `then_apply` closure.
    pub(crate) fn join_map<K>(entries: Vec<(K, KafkaFuture<T>)>) -> KafkaFuture<std::collections::HashMap<K, T>>
    where
        T: Clone + Sync,
        K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    {
        KafkaFuture::new(Arc::new(JoinMapFuture { entries }))
    }

    /// Returns a new `KafkaFuture` that, when this future completes normally,
    /// is executed with this future's result as the argument to the supplied
    /// function.
    ///
    /// # Arguments
    ///
    /// - `function` - a closure or a [`BaseFunction_new`] value; the module
    ///   docs show how to write one
    ///
    /// # Translation notes
    ///
    /// An `Err` from `function` fails the returned future, as a Java throw does.
    ///
    /// WARNING: `function` runs on the task awaiting the returned future, once
    /// per `get` call, not once on the task completing this future.
    #[doc(alias = "org.apache.kafka.common.KafkaFuture#thenApply")]
    pub fn then_apply<R, F>(&self, function: F) -> KafkaFuture<R>
    where
        T: Clone + Sync,
        R: Clone + Send + Sync + 'static,
        F: BaseFunction<T, R> + 'static,
    {
        KafkaFuture::new(Arc::new(ThenApplyFuture { source: self.clone(), function: Arc::new(function) }))
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
}

/// Internal `KafkaFutureOps` impl backing [`KafkaFuture::all_of`].
///
/// Lazily awaits every input future when `get()` is called; there is no eager
/// completion, which matches the way the admin `*Result::all()` methods are
/// consumed (the caller awaits the aggregate).
struct AllOfFuture<T: Clone + Send + Sync + 'static> {
    futures: Vec<KafkaFuture<T>>,
}

impl<T: Clone + Send + Sync + 'static> KafkaFutureOps<()> for AllOfFuture<T> {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move {
            for future in &self.futures {
                future.get().await?;
            }
            Ok(())
        })
    }

    fn get_with_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                // `java.util.concurrent.TimeoutException`, which is what
                // `Future.get(timeout, unit)` declares and what `KafkaFuture`
                // imports — not the retriable
                // `org.apache.kafka.common.errors.TimeoutException`.
                Err(_) => Err(Error::local_timeout(format!(
                    "Timed out waiting for KafkaFuture.all_of after {} ms",
                    timeout.as_millis()
                ))),
            }
        })
    }

    fn is_done(&self) -> bool {
        self.futures.iter().all(KafkaFuture::is_done)
    }
}

/// Internal `KafkaFutureOps` impl backing [`KafkaFuture::then_apply`].
struct ThenApplyFuture<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    source: KafkaFuture<T>,
    function: Arc<dyn BaseFunction<T, R>>,
}

impl<T, R> KafkaFutureOps<R> for ThenApplyFuture<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<R, Error>> + Send + '_>> {
        Box::pin(async move {
            let value = self.source.get().await?;
            self.function.apply(value)
        })
    }

    fn get_with_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<R, Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                // `java.util.concurrent.TimeoutException`, which is what
                // `Future.get(timeout, unit)` declares and what `KafkaFuture`
                // imports — not the retriable
                // `org.apache.kafka.common.errors.TimeoutException`.
                Err(_) => Err(Error::local_timeout(format!(
                    "Timed out waiting for KafkaFuture.then_apply after {} ms",
                    timeout.as_millis()
                ))),
            }
        })
    }

    fn is_done(&self) -> bool {
        self.source.is_done()
    }
}

/// Internal `KafkaFutureOps` impl backing [`KafkaFuture::join_map`].
struct JoinMapFuture<K, T>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    entries: Vec<(K, KafkaFuture<T>)>,
}

impl<K, T> KafkaFutureOps<std::collections::HashMap<K, T>> for JoinMapFuture<K, T>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<std::collections::HashMap<K, T>, Error>> + Send + '_>> {
        Box::pin(async move {
            let mut map = std::collections::HashMap::with_capacity(self.entries.len());
            for (key, future) in &self.entries {
                map.insert(key.clone(), future.get().await?);
            }
            Ok(map)
        })
    }

    fn get_with_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<std::collections::HashMap<K, T>, Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                // `java.util.concurrent.TimeoutException`, which is what
                // `Future.get(timeout, unit)` declares and what `KafkaFuture`
                // imports — not the retriable
                // `org.apache.kafka.common.errors.TimeoutException`.
                Err(_) => Err(Error::local_timeout(format!(
                    "Timed out waiting for KafkaFuture.join_map after {} ms",
                    timeout.as_millis()
                ))),
            }
        })
    }

    fn is_done(&self) -> bool {
        self.entries.iter().all(|(_, f)| f.is_done())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::Errors;
    use std::sync::Mutex;

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
}
