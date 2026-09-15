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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::common::Error;

/// Internal trait representing the abstract methods of `KafkaFuture`.
///
/// This is the Rust equivalent of the abstract method set in Java's `KafkaFuture<T>`.
/// Different internal types (e.g., `FutureRecordMetadata` for the producer)
/// implement this trait and are wrapped in a `KafkaFuture<T>`.
pub(crate) trait KafkaFutureOps<T: Send + 'static>: Send + Sync {
    /// Await the result of this future.
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>>;

    /// Await the result of this future with a timeout.
    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>>;

    /// Whether this future is complete.
    fn is_done(&self) -> bool;

    /// Registers `callback` to run when this future completes. Backs the public
    /// [`KafkaFuture::when_complete`].
    ///
    /// Default implementation: spawn a task that awaits [`get`](Self::get) and
    /// then invokes `callback` — correct for any implementor, but costs an extra
    /// task hop and requires a Tokio runtime context. [`Completable`] (the
    /// backing state of every [`KafkaFutureImpl`]-issued future — i.e. every
    /// per-key admin future) overrides this to fire eagerly and synchronously
    /// instead, with no task hop, matching Java's `KafkaFutureImpl`. The
    /// combinator futures below (`all_of`/`then_apply`/`join_map`/
    /// `join_map_results`) keep the default: they have no eager completion hook
    /// of their own, and nothing currently calls `when_complete` on one of them.
    fn register_completion(self: Arc<Self>, callback: CompletionCallback<T>)
    where
        Self: 'static,
    {
        tokio::spawn(async move {
            let result = self.get().await;
            callback(&result);
        });
    }
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
    pub fn completed(result: Result<T, Error>) -> Self
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
    pub async fn get(&self) -> Result<T, Error> {
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
    /// Returns [`Error::LocalTimeout`] if the timeout elapses before the
    /// result is available — Java's `java.util.concurrent.TimeoutException`,
    /// which `Future.get(timeout, unit)` declares, not the retriable
    /// `org.apache.kafka.common.errors.TimeoutException`. Returns the error from
    /// the underlying operation if it failed.
    pub async fn get_timeout(&self, timeout: Duration) -> Result<T, Error> {
        self.inner.get_timeout(timeout).await
    }

    /// Whether this future is complete.
    ///
    /// This is the Rust equivalent of Java's `Future.isDone()`.
    pub fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Returns a new `KafkaFuture` that is completed when all the given futures
    /// have completed. If any future completes exceptionally, the returned
    /// future returns that error. If multiple futures fail, which error gets
    /// returned is arbitrarily chosen (the first encountered while awaiting in
    /// order).
    ///
    /// Translated from `org.apache.kafka.common.KafkaFuture.allOf`. Unlike
    /// Java's variadic `allOf(KafkaFuture<?>...)`, this Rust version is
    /// homogeneous (`Vec<KafkaFuture<T>>`), which is all the admin `*Result`
    /// types require (they combine per-key futures of a single type).
    pub fn all_of(futures: Vec<KafkaFuture<T>>) -> KafkaFuture<()>
    where
        T: Clone + Sync,
    {
        KafkaFuture::new(Arc::new(AllOfFuture { futures }))
    }

    /// Returns a new `KafkaFuture` that, when this future completes normally,
    /// is completed with the result of applying `function` to this future's
    /// value. If this future completes exceptionally, the returned future
    /// completes with the same exception.
    ///
    /// Translated from `org.apache.kafka.common.KafkaFuture.thenApply`, for the
    /// common case where the transform cannot fail.
    pub fn then_apply<R, F>(&self, function: F) -> KafkaFuture<R>
    where
        T: Clone + Sync,
        R: Clone + Send + Sync + 'static,
        F: Fn(T) -> R + Send + Sync + 'static,
    {
        KafkaFuture::new(Arc::new(ThenApplyFuture {
            source: self.clone(),
            function: Arc::new(move |value| Ok(function(value))),
        }))
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
    pub fn join_map<K>(entries: Vec<(K, KafkaFuture<T>)>) -> KafkaFuture<std::collections::HashMap<K, T>>
    where
        T: Clone + Sync,
        K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    {
        KafkaFuture::new(Arc::new(JoinMapFuture { entries }))
    }

    /// Returns a future that completes when all the given keyed futures
    /// complete, yielding a map from each key to that future's **outcome**.
    ///
    /// This is the collect-all counterpart of [`join_map`](Self::join_map):
    /// a failed input future does **not** short-circuit, so every key's
    /// `Result` is preserved. Java has no direct equivalent because callers
    /// there hold the per-key `KafkaFuture`s themselves and inspect each one;
    /// the C FFI has no `KafkaFuture` type, so the admin bindings flatten a
    /// multi-key `*Result` into one handle carrying a value *and* an error per
    /// key (see `PLAN-bindings.md` §2 / D2 and `admin-client.md` §5, which
    /// requires per-key granularity to survive the boundary). `join_map`
    /// cannot be reused for that: its `future.get().await?` abandons the
    /// remaining keys on the first error.
    pub fn join_map_results<K>(
        entries: Vec<(K, KafkaFuture<T>)>,
    ) -> KafkaFuture<std::collections::HashMap<K, Result<T, Error>>>
    where
        T: Clone + Sync,
        K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    {
        KafkaFuture::new(Arc::new(JoinMapResultsFuture { entries }))
    }

    /// Like [`then_apply`](Self::then_apply) but the transform may fail. If
    /// `function` returns `Err`, the returned future completes with that error.
    ///
    /// This models the Java `thenApply` cases whose `BaseFunction` throws — for
    /// example `CreateTopicsResult.TopicMetadataAndConfig` accessors that call
    /// `ensureSuccess()` and rethrow a stored exception.
    pub fn then_apply_try<R, F>(&self, function: F) -> KafkaFuture<R>
    where
        T: Clone + Sync,
        R: Clone + Send + Sync + 'static,
        F: Fn(T) -> Result<R, Error> + Send + Sync + 'static,
    {
        KafkaFuture::new(Arc::new(ThenApplyFuture { source: self.clone(), function: Arc::new(function) }))
    }

    /// Registers `action` to run when this future completes, passing a
    /// reference to the result. If the future is already complete, `action`
    /// runs immediately rather than being queued.
    ///
    /// Translated from `org.apache.kafka.common.KafkaFuture.whenComplete`.
    /// Unlike Java (whose `whenComplete` returns a new chainable
    /// `KafkaFuture<T>`), this Rust version returns nothing: its callers (the
    /// admin client's per-key async FFI delivery, `admin-client.md` §4) only
    /// need to be notified, not to chain a follow-up future off the result. Add
    /// a chaining return type if a future admin tier needs it.
    ///
    /// `action` may run on whichever thread completes the underlying future —
    /// eagerly, with no extra latency, for the `KafkaFutureImpl`-backed futures
    /// every admin per-key result returns — or on a spawned Tokio task for the
    /// combinator futures (`all_of`/`then_apply`/`join_map`/
    /// `join_map_results`), which have no built-in completion hook and so are
    /// lazily awaited instead via a spawned task. The spawned-task path
    /// requires a Tokio runtime context; the eager path does not.
    pub fn when_complete<F>(&self, action: F)
    where
        F: FnOnce(&Result<T, Error>) + Send + 'static,
    {
        Arc::clone(&self.inner).register_completion(Box::new(action));
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
    result: Result<T, Error>,
}

impl<T: Clone + Send + Sync + 'static> KafkaFutureOps<T> for CompletedFuture<T> {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>> {
        let result = self.result.clone();
        Box::pin(async move { result })
    }

    fn get_timeout(
        &self,
        _timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>> {
        let result = self.result.clone();
        Box::pin(async move { result })
    }

    fn is_done(&self) -> bool {
        true
    }

    fn register_completion(self: Arc<Self>, callback: CompletionCallback<T>) {
        // The result is already known: fire immediately, no spawn needed.
        callback(&self.result);
    }
}

/// The shared, completable state behind a [`KafkaFutureImpl`].
///
/// This is the Rust equivalent of the `CompletableFuture` that backs Java's
/// `org.apache.kafka.common.internals.KafkaFutureImpl`. The value is set once
/// (first writer wins) and can be awaited by any number of consumers any
/// number of times, matching Java `Future.get()` semantics. A [`Notify`] wakes
/// awaiters; registered completion callbacks fire eagerly on completion.
struct Completable<T: Clone + Send + Sync + 'static> {
    inner: Mutex<CompletableInner<T>>,
    notify: Notify,
}

/// A completion callback registered on a [`Completable`], invoked with a
/// reference to the result when the future completes.
type CompletionCallback<T> = Box<dyn FnOnce(&Result<T, Error>) + Send>;

struct CompletableInner<T: Clone + Send + Sync + 'static> {
    result: Option<Result<T, Error>>,
    callbacks: Vec<CompletionCallback<T>>,
}

impl<T: Clone + Send + Sync + 'static> Completable<T> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(CompletableInner { result: None, callbacks: Vec::new() }),
            notify: Notify::new(),
        }
    }

    /// Set the result if not already set. Returns `true` if this call
    /// completed the future, `false` if it was already complete.
    fn set(&self, result: Result<T, Error>) -> bool {
        let callbacks = {
            let mut guard = self.inner.lock().unwrap();
            if guard.result.is_some() {
                return false;
            }
            guard.result = Some(result);
            std::mem::take(&mut guard.callbacks)
        };
        // Wake awaiters, then fire completion callbacks with a snapshot of the
        // result (cloned so we don't hold the lock across callback execution).
        self.notify.notify_waiters();
        if !callbacks.is_empty() {
            let snapshot = self.inner.lock().unwrap().result.clone().unwrap();
            for callback in callbacks {
                callback(&snapshot);
            }
        }
        true
    }

    /// Register a callback to run when this future completes. If the future is
    /// already complete, the callback runs immediately on the calling task.
    ///
    /// Reached two ways: via [`KafkaFutureImpl::when_complete`] (crate-internal,
    /// pre-erasure — its sole consumer, the `AdminApiDriver`
    /// `describeCluster().nodes()` chaining, arrives with a later admin tier),
    /// and via this type's [`KafkaFutureOps::register_completion`] override,
    /// which is how the public, post-erasure [`KafkaFuture::when_complete`]
    /// reaches it — the mechanism the admin client's per-key async FFI delivery
    /// (`admin-client.md` §4) actually uses today.
    fn on_complete(&self, callback: CompletionCallback<T>) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(result) = guard.result.clone() {
            drop(guard);
            callback(&result);
        } else {
            guard.callbacks.push(callback);
        }
    }
}

impl<T: Clone + Send + Sync + 'static> KafkaFutureOps<T> for Completable<T> {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>> {
        Box::pin(async move {
            loop {
                // Register interest before checking so a completion racing with
                // this check still wakes us (no lost wakeup).
                let notified = self.notify.notified();
                if let Some(result) = self.inner.lock().unwrap().result.clone() {
                    return result;
                }
                notified.await;
            }
        })
    }

    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                // `java.util.concurrent.TimeoutException`, which is what
                // `Future.get(timeout, unit)` declares and what `KafkaFuture`
                // imports (`KafkaFuture.java:27`) — not the retriable
                // `org.apache.kafka.common.errors.TimeoutException`.
                Err(_) => Err(Error::local_timeout(format!(
                    "Timed out waiting for KafkaFuture after {} ms",
                    timeout.as_millis()
                ))),
            }
        })
    }

    fn is_done(&self) -> bool {
        self.inner.lock().unwrap().result.is_some()
    }

    fn register_completion(self: Arc<Self>, callback: CompletionCallback<T>) {
        // Delegate to the inherent `on_complete` above (eager, zero-latency,
        // already tested) rather than the trait default's spawn-and-await.
        Completable::on_complete(&self, callback);
    }
}

/// A completable future handle.
///
/// Translated from `org.apache.kafka.common.internals.KafkaFutureImpl`. The
/// admin client creates one of these per result key, hands the caller the
/// public [`KafkaFuture`] view via [`future`](Self::future), and later
/// completes it from the background task when the response arrives.
///
/// Per CLAUDE.md (classes in an `internal`/`internals` package use only
/// `pub(crate)`), this handle is crate-internal; only the public
/// [`KafkaFuture`] view crosses the API boundary.
// `KafkaFutureImpl` is a foundational prerequisite for the admin client
// (Milestone 11): admin RPCs create these handles, return the public
// `KafkaFuture` view synchronously, and complete them later from the
// background task (see `src/admin`).
pub(crate) struct KafkaFutureImpl<T: Clone + Send + Sync + 'static> {
    state: Arc<Completable<T>>,
}

impl<T: Clone + Send + Sync + 'static> KafkaFutureImpl<T> {
    /// Create a new, uncompleted future handle.
    pub(crate) fn new() -> Self {
        Self { state: Arc::new(Completable::new()) }
    }

    /// If not already completed, sets the value returned by `get()` and related
    /// methods. Returns `true` if this call completed the future.
    ///
    /// Translated from `KafkaFutureImpl.complete`.
    pub(crate) fn complete(&self, value: T) -> bool {
        self.state.set(Ok(value))
    }

    /// If not already completed, causes `get()` and related methods to return
    /// the given error. Returns `true` if this call completed the future.
    ///
    /// Translated from `KafkaFutureImpl.completeExceptionally`. The Rust name
    /// differs from the Java one because CLAUDE.md §2 keeps the word
    /// "exception" out of Rust identifiers.
    pub(crate) fn complete_with_error(&self, error: Error) -> bool {
        self.state.set(Err(error))
    }

    /// Whether this future is complete.
    pub(crate) fn is_done(&self) -> bool {
        self.state.is_done()
    }

    /// Register an action to run when this future completes (with a reference
    /// to the result). If already complete, the action runs immediately.
    ///
    /// Translated from the eager side of `KafkaFuture.whenComplete` — used by
    /// the admin client to chain a follow-up `Call` when a prerequisite future
    /// (e.g. `describeCluster().nodes()`) resolves. That chaining arrives with a
    /// later admin tier (Phase-1 topic RPCs do not chain calls).
    #[allow(dead_code)]
    pub(crate) fn when_complete<F>(&self, action: F)
    where
        F: FnOnce(&Result<T, Error>) + Send + 'static,
    {
        self.state.on_complete(Box::new(action));
    }

    /// The public [`KafkaFuture`] view of this handle. Cloneable and awaitable
    /// independently of the handle; both share the same completion state.
    pub(crate) fn future(&self) -> KafkaFuture<T> {
        KafkaFuture::new(Arc::clone(&self.state) as Arc<dyn KafkaFutureOps<T>>)
    }
}

impl<T: Clone + Send + Sync + 'static> Clone for KafkaFutureImpl<T> {
    fn clone(&self) -> Self {
        Self { state: Arc::clone(&self.state) }
    }
}

impl<T: Clone + Send + Sync + 'static> Default for KafkaFutureImpl<T> {
    fn default() -> Self {
        Self::new()
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

    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                // `java.util.concurrent.TimeoutException`, which is what
                // `Future.get(timeout, unit)` declares and what `KafkaFuture`
                // imports (`KafkaFuture.java:27`) — not the retriable
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

/// Internal `KafkaFutureOps` impl backing [`KafkaFuture::then_apply`] /
/// [`KafkaFuture::then_apply_try`].
struct ThenApplyFuture<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    source: KafkaFuture<T>,
    #[allow(clippy::type_complexity)]
    function: Arc<dyn Fn(T) -> Result<R, Error> + Send + Sync>,
}

impl<T, R> KafkaFutureOps<R> for ThenApplyFuture<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<R, Error>> + Send + '_>> {
        Box::pin(async move {
            let value = self.source.get().await?;
            (self.function)(value)
        })
    }

    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<R, Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                // `java.util.concurrent.TimeoutException`, which is what
                // `Future.get(timeout, unit)` declares and what `KafkaFuture`
                // imports (`KafkaFuture.java:27`) — not the retriable
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

    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<std::collections::HashMap<K, T>, Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                // `java.util.concurrent.TimeoutException`, which is what
                // `Future.get(timeout, unit)` declares and what `KafkaFuture`
                // imports (`KafkaFuture.java:27`) — not the retriable
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

/// Internal `KafkaFutureOps` impl backing [`KafkaFuture::join_map_results`].
///
/// Mirrors [`JoinMapFuture`] but records each key's `Result` instead of
/// propagating the first error, so a partially failed batch keeps every
/// per-key outcome.
struct JoinMapResultsFuture<K, T>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    entries: Vec<(K, KafkaFuture<T>)>,
}

/// Shorthand for the per-key outcome map produced by [`JoinMapResultsFuture`].
type ResultMap<K, T> = std::collections::HashMap<K, Result<T, Error>>;

impl<K, T> KafkaFutureOps<ResultMap<K, T>> for JoinMapResultsFuture<K, T>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<ResultMap<K, T>, Error>> + Send + '_>> {
        Box::pin(async move {
            let mut map: ResultMap<K, T> = std::collections::HashMap::with_capacity(self.entries.len());
            for (key, future) in &self.entries {
                // No `?`: a failed key is recorded, not propagated.
                map.insert(key.clone(), future.get().await);
            }
            Ok(map)
        })
    }

    fn get_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<ResultMap<K, T>, Error>> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                Err(_) => Err(Error::timeout(format!(
                    "Timed out waiting for KafkaFuture.join_map_results after {} ms",
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
    use crate::common::protocol::Errors;

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
        let f: KafkaFuture<i32> = KafkaFuture::completed(Err(Error::local_illegal_argument("test".to_string())));
        assert!(f.is_done());
        assert!(matches!(f.get().await, Err(Error::LocalIllegalArgument(_))));
        assert!(matches!(
            f.get_timeout(Duration::from_secs(1)).await,
            Err(Error::LocalIllegalArgument(_))
        ));
    }

    #[tokio::test]
    async fn completed_clone_shares_underlying_result() {
        let f1: KafkaFuture<String> = KafkaFuture::completed(Ok("hello".to_string()));
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
        let err = match future.get_timeout(Duration::from_millis(20)).await {
            Err(e) => e,
            other => panic!("expected a timeout, got {other:?}"),
        };
        // Java throws `java.util.concurrent.TimeoutException`
        // (`KafkaFuture.java:27`), a checked exception beside the Kafka
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

    /// The public `KafkaFuture::when_complete` (not `KafkaFutureImpl::when_complete`
    /// above) is what the admin client's per-key async FFI delivery actually
    /// calls, since by the time a `*Result` hands out a per-key future it has
    /// already been erased to the public `KafkaFuture<T>` type
    /// (`admin-client.md` §4). This covers the not-yet-complete case: the
    /// future is backed by `Completable` (via `KafkaFutureImpl`), so the
    /// callback must fire exactly once and eagerly — no polling, no missed
    /// wakeup — the instant `complete` is called.
    #[tokio::test]
    async fn public_when_complete_fires_exactly_once_when_not_yet_complete() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let future = handle.future();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_clone = Arc::clone(&calls);
        future.when_complete(move |result| {
            calls_clone.lock().unwrap().push(result.as_ref().ok().copied());
        });
        assert!(calls.lock().unwrap().is_empty(), "must not fire before completion");
        handle.complete(11);
        assert_eq!(*calls.lock().unwrap(), vec![Some(11)]);
    }

    /// Same as above, but the future is already resolved when `when_complete`
    /// is registered — the callback must still fire exactly once, immediately.
    #[tokio::test]
    async fn public_when_complete_fires_exactly_once_when_already_complete() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::local_illegal_argument("boom"));
        let future = handle.future();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_clone = Arc::clone(&calls);
        future.when_complete(move |result| {
            calls_clone
                .lock()
                .unwrap()
                .push(matches!(result, Err(Error::LocalIllegalArgument(_))));
        });
        assert_eq!(*calls.lock().unwrap(), vec![true]);
    }

    /// Futures produced by combinators (no eager completion hook of their own)
    /// fall back to `KafkaFutureOps::register_completion`'s default: spawn a
    /// task that awaits `get()`. Exercises that path specifically, using
    /// `then_apply` as a representative combinator future.
    #[tokio::test]
    async fn public_when_complete_on_a_combinator_future_uses_spawn_fallback() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let mapped = handle.future().then_apply(|v| v * 2);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Mutex::new(Some(tx));
        mapped.when_complete(move |result| {
            let _ = tx.lock().unwrap().take().unwrap().send(result.as_ref().ok().copied());
        });
        handle.complete(21);
        let seen = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("spawn-based when_complete must still fire")
            .unwrap();
        assert_eq!(seen, Some(42));
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
        let mapped = handle.future().then_apply(|v| v * 2);
        handle.complete(21);
        assert_eq!(mapped.get().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn then_apply_propagates_source_error() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let mapped = handle.future().then_apply(|v| v * 2);
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
    async fn join_map_results_collects_values_when_all_succeed() {
        let h1: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let joined = KafkaFuture::join_map_results(vec![("a", h1.future()), ("b", h2.future())]);
        assert!(!joined.is_done());
        h1.complete(1);
        h2.complete(2);
        assert!(joined.is_done());
        let map = joined.get().await.unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(*map["a"].as_ref().unwrap(), 1);
        assert_eq!(*map["b"].as_ref().unwrap(), 2);
    }

    /// The whole point: a partially failed batch keeps both outcomes.
    #[tokio::test]
    async fn join_map_results_keeps_per_key_outcomes_on_mixed_batch() {
        let ok: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let bad: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let last: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        // Order matters: the failing key comes first, so a short-circuiting
        // implementation would drop the two that follow.
        let joined =
            KafkaFuture::join_map_results(vec![("bad", bad.future()), ("ok", ok.future()), ("last", last.future())]);
        bad.complete_with_error(Error::local_illegal_argument("boom"));
        ok.complete(7);
        last.complete(9);

        let map = joined.get().await.unwrap();
        assert_eq!(map.len(), 3);
        match map["bad"].as_ref() {
            Err(Error::LocalIllegalArgument(e)) => assert_eq!(e.message(), "boom"),
            other => panic!("expected IllegalArgument for `bad`, got {other:?}"),
        }
        assert_eq!(*map["ok"].as_ref().unwrap(), 7);
        assert_eq!(*map["last"].as_ref().unwrap(), 9);
    }

    #[tokio::test]
    async fn join_map_results_records_every_error_when_all_fail() {
        let h1: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let joined = KafkaFuture::join_map_results(vec![("a", h1.future()), ("b", h2.future())]);
        h1.complete_with_error(Error::local_illegal_argument("a"));
        h2.complete_with_error(Error::local_illegal_state("b"));
        let map = joined.get().await.unwrap();
        assert!(matches!(map["a"].as_ref(), Err(Error::LocalIllegalArgument(_))));
        assert!(matches!(map["b"].as_ref(), Err(Error::LocalIllegalState(_))));
    }

    #[tokio::test]
    async fn join_map_results_empty_is_done_and_yields_empty_map() {
        let joined: KafkaFuture<std::collections::HashMap<&str, Result<i32, Error>>> =
            KafkaFuture::join_map_results(Vec::new());
        assert!(joined.is_done());
        assert!(joined.get().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn join_map_results_get_timeout_elapses_when_a_key_never_completes() {
        let h1: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let joined = KafkaFuture::join_map_results(vec![("a", h1.future()), ("b", h2.future())]);
        h1.complete(1);
        match joined.get_timeout(Duration::from_millis(20)).await {
            Err(Error::Timeout(e)) => assert!(e.message().contains("join_map_results"), "unexpected message: {e}"),
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn then_apply_try_can_fail() {
        let handle: KafkaFutureImpl<i32> = KafkaFutureImpl::new();
        let mapped = handle
            .future()
            .then_apply_try(|_v| Err::<i32, _>(Error::local_illegal_state("bad".to_string())));
        handle.complete(1);
        assert!(matches!(mapped.get().await, Err(Error::LocalIllegalState(_))));
    }
}
