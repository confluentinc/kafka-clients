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

//! The completable `KafkaFuture` handle.
//!
//! Translated from `org.apache.kafka.common.internals.KafkaFutureImpl`.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::common::Error;
use crate::common::KafkaFuture;
use crate::common::KafkaFutureOps;

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
    /// Only reached via [`KafkaFutureImpl::when_complete`], whose sole consumer
    /// (the `AdminApiDriver` `describeCluster().nodes()` chaining) arrives with
    /// a later admin tier.
    #[allow(dead_code)]
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

    fn get_with_timeout(
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
