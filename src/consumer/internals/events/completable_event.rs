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

//! Completable event handle.
//!
//! Translated from Java's `CompletableEvent<T>` interface combined with
//! `CompletableApplicationEvent<T>` / `CompletableBackgroundEvent<T>` (each
//! Java subclass owns a `CompletableFuture<T>` and a deadline). In Rust the
//! sending half of a [`tokio::sync::oneshot`] channel + a deadline becomes
//! [`CompletableEventHandle`]. The receiving half lives on the app side and
//! is awaited via `ApplicationEventHandler::add_and_get`.
//!
//! The handle is **`Send`** so the background task that processes the event
//! can hand it off (e.g. to a request manager) and have the request manager
//! complete it later — mirroring how Java's `CompletableFuture` is shared
//! across threads.

use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::common::Error;

/// Sending end of a completable event paired with its deadline.
///
/// Cloned `Arc<HandleInner<T>>` allows the same logical completion slot to
/// be observed by:
///
/// 1. The event variant queued on the bg task (`complete(value)` /
///    `complete_with_error(err)`), and
/// 2. The [`crate::consumer::internals::events::CompletableEventReaper`]
///    (which fails it with `Error::timeout(...)` when the deadline
///    passes).
///
/// Multiple completion calls are idempotent — only the first call wins;
/// subsequent calls return `false`.
pub(crate) struct CompletableEventHandle<T: Send + 'static> {
    inner: Arc<HandleInner<T>>,
    deadline_ms: i64,
}

/// Shared completion slot. The [`Mutex`] is **synchronous** because the
/// critical section is sub-microsecond (one `Option::take`); per CLAUDE.md
/// §9 the guard MUST NOT be held across an `.await`.
struct HandleInner<T> {
    sender: Mutex<Option<oneshot::Sender<Result<T, Error>>>>,
}

impl<T: Send + 'static> CompletableEventHandle<T> {
    /// Creates a (handle, receiver) pair. The handle lives inside the
    /// `ApplicationEvent` (queued to the bg task); the receiver lives on
    /// the app side and is awaited.
    pub(crate) fn new(deadline_ms: i64) -> (Self, oneshot::Receiver<Result<T, Error>>) {
        let (tx, rx) = oneshot::channel();
        let inner = Arc::new(HandleInner { sender: Mutex::new(Some(tx)) });
        (Self { inner, deadline_ms }, rx)
    }

    /// Java: `future.complete(value)`. Returns `true` if THIS call performed
    /// the completion (i.e. the slot was not yet consumed). Idempotent —
    /// safe to call concurrently with `complete_with_error` and with the
    /// reaper's `fail_with_timeout`.
    pub(crate) fn complete(&self, value: T) -> bool {
        let sender_opt = {
            // Mutex critical section — never awaits.
            let mut guard = match self.inner.sender.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.take()
        };
        match sender_opt {
            Some(tx) => tx.send(Ok(value)).is_ok(),
            None => false,
        }
    }

    /// Java: `future.completeExceptionally(error)`. Same idempotency
    /// semantics as [`complete`]. The Rust name drops the Java spelling
    /// because CLAUDE.md §2 keeps "exception" out of Rust identifiers.
    pub(crate) fn complete_with_error(&self, error: Error) -> bool {
        let sender_opt = {
            let mut guard = match self.inner.sender.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.take()
        };
        match sender_opt {
            Some(tx) => tx.send(Err(error)).is_ok(),
            None => false,
        }
    }

    /// `true` if the inner sender has already been consumed (via any
    /// completion path or a reaper-induced timeout).
    pub(crate) fn is_done(&self) -> bool {
        let guard = match self.inner.sender.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.is_none()
    }

    /// Returns the deadline (absolute wall-clock millisecond timestamp).
    pub(crate) fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    /// Returns an opaque, type-erased view of this handle suitable for
    /// the reaper.
    ///
    /// Two `erased()` calls on the same `CompletableEventHandle` return
    /// **different** `Arc<dyn CompletableEventErasedHandle>` trait objects
    /// (so `Arc::ptr_eq` between them is `false`), but both delegate to
    /// the same underlying [`HandleInner`]. The reaper compares identity
    /// via [`CompletableEventErasedHandle::inner_id`] instead of
    /// `Arc::ptr_eq` for that reason — see [`crate::consumer::internals::events::CompletableEventReaper::contains`].
    pub(crate) fn erased(&self) -> Arc<dyn CompletableEventErasedHandle> {
        Arc::new(ErasedHandle::<T> { inner: Arc::clone(&self.inner), deadline_ms: self.deadline_ms })
    }
}

/// Object-safe trait for the reaper to iterate over heterogeneous
/// completable handles. The reaper does NOT need to know `T`; it only
/// needs `deadline_ms`, `is_done`, and a way to fail with a timeout.
pub(crate) trait CompletableEventErasedHandle: Send + Sync + 'static {
    fn deadline_ms(&self) -> i64;
    fn is_done(&self) -> bool;
    /// Java: `future.completeExceptionally(timeoutException)`. Returns
    /// `true` if this call performed the completion. Used by the reaper
    /// to expire deadline-exceeded events.
    fn fail_with_timeout(&self, error: Error) -> bool;
    /// Diagnostic name for the wrapped `T` — used in log/trace messages
    /// equivalent to Java's `event.getClass().getSimpleName()`.
    fn type_name(&self) -> &'static str;
    /// Stable identity of the underlying completion slot.
    ///
    /// Two erased handles created from the same
    /// [`CompletableEventHandle`] (via repeated calls to
    /// [`CompletableEventHandle::erased`]) MUST return the same
    /// `inner_id()` even though their `Arc<dyn ...>` differ. The reaper
    /// uses this to implement Java's `List.contains(event)`
    /// reference-equality without relying on the trait-object `Arc`.
    fn inner_id(&self) -> *const ();
}

/// Type-erased wrapper around [`HandleInner`]. Stored inside the reaper as
/// `Arc<dyn CompletableEventErasedHandle>` so the reaper does not need a
/// type parameter for every tracked event.
struct ErasedHandle<T: Send + 'static> {
    inner: Arc<HandleInner<T>>,
    deadline_ms: i64,
}

impl<T: Send + 'static> CompletableEventErasedHandle for ErasedHandle<T> {
    fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    fn is_done(&self) -> bool {
        let guard = match self.inner.sender.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.is_none()
    }

    fn fail_with_timeout(&self, error: Error) -> bool {
        let sender_opt = {
            let mut guard = match self.inner.sender.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.take()
        };
        match sender_opt {
            Some(tx) => tx.send(Err(error)).is_ok(),
            None => false,
        }
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    fn inner_id(&self) -> *const () {
        // The `HandleInner<T>` lives behind an `Arc`. `Arc::as_ptr`
        // returns the pointer to the inner struct — invariant for the
        // lifetime of every clone of that `Arc`. Casting through
        // `*const ()` erases `T` so the comparison works across
        // heterogeneous handles tracked by the reaper.
        Arc::as_ptr(&self.inner) as *const ()
    }
}

/// Return-type alias for [`make_completable_event`] — keeps the
/// `clippy::type_complexity` lint quiet without sacrificing the
/// explicit-triple ergonomics at the call site.
pub(crate) type CompletableEventTriple<T> = (
    CompletableEventHandle<T>,
    oneshot::Receiver<Result<T, Error>>,
    Arc<dyn CompletableEventErasedHandle>,
);

/// Helper that constructs a `(handle, receiver, erased_handle)` triple in
/// one call. Most callers use this rather than [`CompletableEventHandle::new`]
/// directly so they don't forget to register the erased handle with the
/// reaper.
pub(crate) fn make_completable_event<T: Send + 'static>(deadline_ms: i64) -> CompletableEventTriple<T> {
    let (handle, rx) = CompletableEventHandle::<T>::new(deadline_ms);
    let erased = handle.erased();
    (handle, rx, erased)
}

/// Translate Java's
/// `CompletableEvent.calculateDeadlineMs(currentTimeMs, timeoutMs)` —
/// saturating addition guarding against `i64::MAX` overflow.
pub(crate) fn calculate_deadline_ms(current_time_ms: i64, timeout_ms: i64) -> i64 {
    current_time_ms.saturating_add(timeout_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_succeeds_first_time_and_is_idempotent() {
        let (handle, rx) = CompletableEventHandle::<i32>::new(1_000);
        assert!(!handle.is_done());
        assert!(handle.complete(7));
        assert!(handle.is_done());
        // Subsequent call is a no-op.
        assert!(!handle.complete(8));

        let received = rx.blocking_recv().expect("sender lives until completion");
        assert_eq!(received.expect("ok"), 7);
    }

    #[test]
    fn complete_with_error_propagates_error() {
        let (handle, rx) = CompletableEventHandle::<()>::new(1_000);
        let err = Error::timeout("oops");
        assert!(handle.complete_with_error(err));
        assert!(handle.is_done());

        let received = rx.blocking_recv().expect("sender lives until completion");
        let got_err = received.expect_err("err variant");
        assert!(matches!(got_err, Error::Timeout(_)));
    }

    #[test]
    fn second_completion_is_noop_after_failed_with_timeout() {
        let (handle, _rx, erased) = make_completable_event::<()>(1_000);
        assert!(erased.fail_with_timeout(Error::timeout("deadline")));
        assert!(handle.is_done());
        // The handle's own complete() must observe is_done and return false.
        assert!(!handle.complete(()));
    }

    #[test]
    fn calculate_deadline_ms_saturates_on_overflow() {
        assert_eq!(calculate_deadline_ms(0, 100), 100);
        assert_eq!(calculate_deadline_ms(i64::MAX, 1), i64::MAX);
        assert_eq!(calculate_deadline_ms(i64::MAX - 5, 100), i64::MAX);
    }

    #[test]
    fn erased_type_name_includes_t() {
        let (_h, _rx, erased) = make_completable_event::<i64>(0);
        // Just ensure type_name is non-empty and contains "i64". Exact
        // value depends on rustc; we don't pin it.
        let name = erased.type_name();
        assert!(name.contains("i64"), "type_name should mention i64: {}", name);
    }

    #[test]
    fn dropping_receiver_makes_complete_return_false() {
        let (handle, rx) = CompletableEventHandle::<i32>::new(0);
        drop(rx);
        // The Sender::send call inside complete() fails because the
        // receiver was dropped; complete() should return false.
        assert!(!handle.complete(1));
        // is_done() reflects that the sender slot was consumed.
        assert!(handle.is_done());
    }
}
