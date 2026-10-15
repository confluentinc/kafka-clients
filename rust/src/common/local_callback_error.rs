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

//! The error a callback implemented in another language reports when it
//! raised one of that language's own errors.
//!
//! No Java counterpart (DoD #7). A Java listener's `Throwable` reaches
//! `ConsumerUtils.maybeWrapAsKafkaException` as itself; an error raised by a
//! Python (or any foreign) callback cannot cross the FFI boundary as a Rust
//! value. This class stands in for it: it carries the foreign error's text,
//! for the client's own logs, and an opaque pointer the binding uses to find
//! the original again when the error comes back out of the client.

use std::ffi::c_void;
use std::fmt;
use std::sync::Arc;

/// The function a binding gives to release its opaque pointer: called once,
/// with the pointer, when the last copy of the error holding it drops.
pub(crate) type ReleaseFn = unsafe extern "C" fn(opaque: *mut c_void);

/// An opaque pointer the error owns: [`release`](Self::release) runs once,
/// when the last copy of the error drops, on whatever task or thread drops it.
#[derive(Debug)]
struct OwnedOpaque {
    ptr: *mut c_void,
    release: ReleaseFn,
}

impl Drop for OwnedOpaque {
    fn drop(&mut self) {
        // SAFETY: `release` and `ptr` were handed over together by the binding that built the
        // error (`LocalCallbackError::with_release`, whose contract is the one
        // `kafka_common_Error_local_callback` documents): `release` may be called once, with
        // `ptr`, on any thread. This is that one call: `OwnedOpaque` is never cloned (copies of
        // the error share it through the `Arc`), so its `Drop` runs exactly once, when the last
        // copy goes.
        unsafe { (self.release)(self.ptr) }
    }
}

// SAFETY: Rust never reads through `ptr`; it only copies the address out and, once, hands it
// back to `release`. The binding that gives a release function promises it may run on any thread
// (the contract of `kafka_common_Error_local_callback`), so moving the owner — and with it the
// one call to `release` — to another thread is sound.
unsafe impl Send for OwnedOpaque {}
// SAFETY: a shared `&OwnedOpaque` only reads the address (`opaque()`), never through it, and the
// call to `release` needs the owner by value (its `Drop`), so sharing it across threads shares no
// memory behind the pointer.
unsafe impl Sync for OwnedOpaque {}

/// The binding's opaque pointer: borrowed (never dereferenced, freed or
/// retained by Rust, the binding keeping what it points at alive) or owned,
/// released once through the binding's function when the last copy of the
/// error drops.
#[derive(Clone, Debug)]
enum Opaque {
    Borrowed(*mut c_void),
    Owned(Arc<OwnedOpaque>),
}

// SAFETY: a borrowed pointer is an identifier only — Rust never reads through it, so copying it
// across tasks shares no memory. An owned one is `Arc<OwnedOpaque>`, `Send` because
// `OwnedOpaque` is `Send + Sync` (above).
unsafe impl Send for Opaque {}
// SAFETY: as for `Send`: `&Opaque` only reads the address, and `OwnedOpaque` is `Sync`.
unsafe impl Sync for Opaque {}

impl Opaque {
    fn ptr(&self) -> *mut c_void {
        match self {
            Opaque::Borrowed(ptr) => *ptr,
            Opaque::Owned(owned) => owned.ptr,
        }
    }
}

/// An error raised by a callback written in another language.
///
/// Like Java's `java.lang` runtime exceptions it sits beside `KafkaException`,
/// not below it, so [`Error::is_kafka_error`](crate::common::Error::is_kafka_error)
/// is `false` and the client wraps it exactly as Java wraps a listener's
/// foreign `Throwable`: the rebalance-listener path builds
/// `KafkaException("User rebalance callback throws an error")` with this error
/// as its [`source`](crate::common::Error::source), Java's `getCause()`.
///
/// The [`opaque`](Self::opaque) pointer lets the binding swap the original
/// error back in: on receiving an error whose cause chain holds a
/// `LocalCallbackError`, it reads the pointer back. Rust never dereferences
/// the pointer. Built with [`new`](Self::new) it does not own it either: the
/// binding keeps the object alive for as long as it may be read. Built by the
/// FFI with a release function, the error owns it: every clone shares it, and
/// the release function runs once, when the last copy drops, on whatever task
/// or thread drops it.
// a binding's foreign callback error, no Java class (DoD #7)
#[doc(alias = "rust-only")]
#[derive(Clone, Debug)]
pub struct LocalCallbackError {
    message: String,
    opaque: Opaque,
}

impl LocalCallbackError {
    /// Create the error with the foreign error's text and the binding's
    /// opaque pointer to it, which the error does not own.
    pub fn new(message: impl Into<String>, opaque: *mut c_void) -> Self {
        Self { message: message.into(), opaque: Opaque::Borrowed(opaque) }
    }

    /// Create the error owning `opaque`: `release(opaque)` runs exactly once,
    /// when the last copy of the error (clones and the errors whose cause it
    /// is included) drops, on whatever task or thread drops it.
    ///
    /// # Safety
    ///
    /// `release` must be sound to call once with `opaque`, from any thread, at
    /// any time until the process exits.
    pub(crate) unsafe fn with_release(message: impl Into<String>, opaque: *mut c_void, release: ReleaseFn) -> Self {
        Self {
            message: message.into(),
            opaque: Opaque::Owned(Arc::new(OwnedOpaque { ptr: opaque, release })),
        }
    }

    /// The foreign error's text, as the binding reported it.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The binding's opaque pointer to the foreign error, exactly as given.
    /// An owned pointer stays valid while any copy of the error lives.
    pub fn opaque(&self) -> *mut c_void {
        self.opaque.ptr()
    }
}

// No cause: the foreign error's own cause chain stays in its language.
impl crate::common::error::ErrorSource for LocalCallbackError {}

impl std::error::Error for LocalCallbackError {}

// No protocol code: raised only on the client side, never by a broker, so the
// trait default (`Errors::UnknownServerError`) is the right answer.
impl crate::common::error::ErrorCode for LocalCallbackError {}

impl crate::common::error::ErrorMessage for LocalCallbackError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for LocalCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalCallbackError: {}", self.message)
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::{self, ThreadId};

    use super::*;
    use crate::common::Error;

    /// What a test's release function points at: how many times it ran, and
    /// on which thread it last did.
    #[derive(Default)]
    struct ReleaseProbe {
        count: AtomicUsize,
        thread: Mutex<Option<ThreadId>>,
    }

    impl ReleaseProbe {
        fn opaque(&self) -> *mut c_void {
            (self as *const ReleaseProbe).cast_mut().cast::<c_void>()
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    /// Counts the call in the `ReleaseProbe` `opaque` points at.
    ///
    /// # Safety
    ///
    /// `opaque` must point at a live `ReleaseProbe`.
    unsafe extern "C" fn count_release(opaque: *mut c_void) {
        // SAFETY: Per this function's `# Safety`, `opaque` points at a live `ReleaseProbe`; it is
        // only read through a shared reference, whose fields are interior-mutable.
        let probe = unsafe { &*opaque.cast::<ReleaseProbe>() };
        *probe.thread.lock().unwrap() = Some(thread::current().id());
        probe.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Clones share the owned pointer, so its release runs once, when the
    /// last copy drops — the copy kept as the cause of a wrapping error
    /// included.
    #[test]
    fn owned_opaque_is_released_once_after_the_last_copy() {
        let probe = ReleaseProbe::default();
        // SAFETY: Test code: `count_release` only touches the probe, which outlives every copy
        // of the error (all are dropped before the asserts below and before `probe` goes).
        let err = unsafe { Error::local_callback_with_release("ValueError: bad", probe.opaque(), count_release) };
        let copy = err.clone();
        let wrapped = crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
            err,
            "User rebalance callback throws an error",
        );
        let wrapped_copy = wrapped.clone();
        match wrapped.source() {
            Some(Error::LocalCallback(e)) => assert_eq!(e.opaque(), probe.opaque()),
            other => panic!("unexpected cause {other:?}"),
        }
        drop(wrapped);
        assert_eq!(probe.count(), 0);
        drop(copy);
        assert_eq!(probe.count(), 0);
        drop(wrapped_copy);
        assert_eq!(probe.count(), 1);
    }

    /// The release runs on whatever thread drops the last copy.
    #[test]
    fn owned_opaque_is_released_on_the_dropping_thread() {
        let probe = ReleaseProbe::default();
        // SAFETY: Test code: `count_release` only touches the probe, which outlives the error:
        // the thread that drops it is joined before the asserts and before `probe` goes.
        let err = unsafe { LocalCallbackError::with_release("boom", probe.opaque(), count_release) };
        let dropper = thread::spawn(move || {
            drop(err);
            thread::current().id()
        });
        let dropper_id = dropper.join().unwrap();
        assert_ne!(dropper_id, thread::current().id());
        assert_eq!(probe.count(), 1);
        assert_eq!(*probe.thread.lock().unwrap(), Some(dropper_id));
    }

    /// The unowned error (`new`, `Error::local_callback`) releases nothing:
    /// dropping it leaves the pointee alone.
    #[test]
    fn borrowed_opaque_is_never_released() {
        let probe = ReleaseProbe::default();
        let err = Error::local_callback("boom", probe.opaque());
        let copy = err.clone();
        drop(err);
        drop(copy);
        drop(LocalCallbackError::new("boom", probe.opaque()));
        assert_eq!(probe.count(), 0);
        assert!(probe.thread.lock().unwrap().is_none());
    }

    #[test]
    fn carries_message_and_opaque_unchanged() {
        let mut target = 7u8;
        let opaque = (&mut target as *mut u8).cast::<c_void>();
        let err = LocalCallbackError::new("boom", opaque);
        assert_eq!(err.message(), "boom");
        assert_eq!(err.opaque(), opaque);
        assert_eq!(err.clone().opaque(), opaque);
        assert_eq!(err.to_string(), "LocalCallbackError: boom");
        assert!(LocalCallbackError::new("null", ptr::null_mut()).opaque().is_null());
    }

    /// Outside the Kafka hierarchy, so the listener path wraps it with Java's
    /// message and keeps it as the cause.
    #[test]
    fn listener_wrap_keeps_it_as_the_source() {
        let mut target = 0u8;
        let opaque = (&mut target as *mut u8).cast::<c_void>();
        let err = Error::local_callback("ValueError: bad", opaque);
        assert!(err.is_local_callback_error());
        assert!(!err.is_kafka_error());
        assert!(!err.is_api_error());
        assert!(!err.is_retriable_error());
        assert_eq!(err.message(), "ValueError: bad");

        let wrapped = crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
            err,
            "User rebalance callback throws an error",
        );
        assert!(wrapped.is_kafka_error());
        assert!(!wrapped.is_local_callback_error());
        assert_eq!(wrapped.message(), "User rebalance callback throws an error");
        let source = wrapped.source().expect("the foreign error is the cause");
        assert!(source.is_local_callback_error());
        match source {
            Error::LocalCallback(e) => assert_eq!(e.opaque(), opaque),
            other => panic!("unexpected cause {other:?}"),
        }
    }
}
