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

//! Shared C FFI machinery reused across the producer and consumer FFI layers.
//!
//! This module hosts the pieces that are not specific to either the producer
//! or the consumer:
//!
//! - The opaque [`kafka_common_KafkaError_t`] error handle and its accessor
//!   functions. `kafka_common_*` is shared verbatim between FFI surfaces — a
//!   second definition would make cbindgen emit a duplicate type.
//! - The async completion-queue / dispatcher-thread abstraction
//!   ([`CompletionJob`], [`spawn_dispatcher`], [`enqueue_or_run_inline`],
//!   [`dispatch_and_wait`]).
//! - The void-returning operation callback machinery ([`OperationCallbackFn`],
//!   [`OperationCompletion`], [`OperationCallbackTarget`]).
//! - The multi-shot callback ownership primitive ([`CallbackTarget`]).
//! - The default logger initialization helper ([`init_default_logger`]).
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![allow(non_snake_case, non_camel_case_types)]

use std::ffi::{CStr, CString, c_char};

use crate::common::KafkaError;
use crate::common::protocol::Errors;

/// Initialize the default stderr log backend if RUST_LOG is set.
/// Idempotent: succeeds once, silently no-ops on subsequent calls.
/// A custom log backend (e.g. Python logging bridge) can be set before
/// the first producer/consumer is created to override this default.
pub(crate) fn init_default_logger() {
    #[cfg(feature = "ffi")]
    {
        let _ = env_logger::try_init();
    }
}

// ---------------------------------------------------------------------------
// Error handle
// ---------------------------------------------------------------------------

/// Internal wrapper that pairs [`KafkaError`] with a [`CString`] for the
/// error message, so that [`kafka_common_KafkaError_message`] can return a valid
/// `*const c_char` that lives as long as the handle.
pub(crate) struct KafkaErrorInner {
    pub(crate) error: KafkaError,
    /// Cached CString for the error message, created once at construction time.
    pub(crate) message_cstring: CString,
}

/// Opaque error handle returned by functions that can fail.
///
/// Internally wraps a `Box<KafkaErrorInner>` containing the [`KafkaError`]
/// and a cached [`CString`] for the error message.
///
/// A null `kafka_common_KafkaError_t` pointer means success (no error).
#[repr(C)]
pub struct kafka_common_KafkaError_t {
    _private: [u8; 0],
}

/// Wraps a [`KafkaError`] into a heap-allocated opaque error pointer, including
/// a cached [`CString`] for the error message.
pub(crate) fn box_error(error: KafkaError) -> *mut kafka_common_KafkaError_t {
    let message_cstring = CString::new(error.message()).unwrap_or_else(|_| CString::new("").unwrap());
    let inner = KafkaErrorInner { error, message_cstring };
    Box::into_raw(Box::new(inner)) as *mut kafka_common_KafkaError_t
}

/// Creates a new error handle from a protocol error code and a message.
///
/// This is the inverse of the [`kafka_common_KafkaError_code`] /
/// [`kafka_common_KafkaError_message`] accessors: it lets a C (or Python)
/// callback that must *return* an error to the Rust core build the handle to
/// return. The rebalance-listener callbacks are the motivating case — they
/// return `Result<(), KafkaError>` in the core, i.e. a
/// `kafka_common_KafkaError_t*` in C, and a Python listener that raised an
/// exception has to convert it into one.
///
/// `code` is looked up as a Kafka protocol error code; unknown codes (including
/// any value outside the `i16` protocol range) map to
/// `Errors::UnknownServerError`, mirroring Java's `Errors.forCode`.
///
/// # Parameters
///
/// - `code`: Kafka protocol error code (see `kafka_common_KafkaError_code`).
/// - `message`: Null-terminated error message, or null for an empty message.
///
/// # Returns
///
/// A non-null error handle owned by the caller, who must free it with
/// [`kafka_common_KafkaError_destroy`] — unless it is handed to a Rust callback
/// that documents taking ownership of it.
///
/// # Safety
///
/// `message` must be null or a valid, null-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaError_new(
    code: i32,
    message: *const c_char,
) -> *mut kafka_common_KafkaError_t {
    let error = match i16::try_from(code) {
        Ok(code) => Errors::for_code(code),
        Err(_) => Errors::UnknownServerError,
    };
    let message = if message.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned()
    };
    box_error(KafkaError::with_message(error, message))
}

/// Casts a `*const kafka_common_KafkaError_t` to a reference to `KafkaErrorInner`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by [`box_error`].
pub(crate) unsafe fn error_ref(error: *const kafka_common_KafkaError_t) -> &'static KafkaErrorInner {
    unsafe { &*(error as *const KafkaErrorInner) }
}

/// Takes ownership of an error handle **returned by a C callback** and converts
/// it back into a [`KafkaError`], freeing the handle. A null pointer means
/// success and yields `None`.
///
/// This is the inbound counterpart of [`box_error`]: it is how a callback whose
/// Rust signature returns `Result<(), KafkaError>` (the rebalance-listener
/// methods) reports failure across the boundary. The handle is consumed exactly
/// as [`kafka_common_KafkaError_destroy`] would consume it, so the C callback
/// must not free it itself.
///
/// # Safety
///
/// `error` must be null or a handle created by [`box_error`] (i.e. by
/// [`kafka_common_KafkaError_new`] or returned from a fallible FFI function and
/// not yet destroyed). After this call the pointer is invalid.
pub(crate) unsafe fn take_error(error: *mut kafka_common_KafkaError_t) -> Option<KafkaError> {
    if error.is_null() {
        return None;
    }
    let inner = unsafe { Box::from_raw(error as *mut KafkaErrorInner) };
    Some(inner.error)
}

/// Returns the error code from a [`kafka_common_KafkaError_t`] handle.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// The numeric error code (i32), or `0` if the error handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaError_code(error: *const kafka_common_KafkaError_t) -> i32 {
    if error.is_null() {
        return 0;
    }
    i32::from(unsafe { error_ref(error) }.error.code())
}

/// Returns the error message as a null-terminated C string.
///
/// The returned pointer is valid until [`kafka_common_KafkaError_destroy`] is called on
/// the same handle.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// A `*const c_char` pointing to the error message, or null if the error
/// handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
/// The returned pointer must not be used after the error is destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaError_message(error: *const kafka_common_KafkaError_t) -> *const c_char {
    if error.is_null() {
        return std::ptr::null();
    }
    unsafe { error_ref(error) }.message_cstring.as_ptr()
}

/// Returns whether the error is retriable.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the error is retriable, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaError_is_retriable(error: *const kafka_common_KafkaError_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_retriable()
}

/// Returns whether the error is fatal (unrecoverable).
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the error is fatal, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaError_is_fatal(error: *const kafka_common_KafkaError_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fatal()
}

/// Destroys an error handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `error` must be null or a valid handle from a function that returned an error.
/// - After this call, the pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaError_destroy(error: *mut kafka_common_KafkaError_t) {
    if !error.is_null() {
        unsafe {
            drop(Box::from_raw(error as *mut KafkaErrorInner));
        }
    }
}

// ---------------------------------------------------------------------------
// Async (callback-based) delivery machinery
// ---------------------------------------------------------------------------
//
// The async API mirrors the librdkafka delivery-report model: each operation
// returns immediately and its result is delivered later through a C callback.
// All callbacks are invoked from a single per-handle **dispatcher thread**
// that drains a completion queue, so user callbacks run on one predictable
// thread and never on a tokio worker (a slow callback cannot stall I/O).

/// A unit of work executed by the dispatcher thread. Each async operation
/// captures its own C callback, `user_data`, and owned result handles into the
/// closure and bakes in the correct invocation, so the queue stays uniform
/// (one element type) while every operation delivers exactly the outputs its
/// sync counterpart produces.
pub(crate) type CompletionJob = Box<dyn FnOnce() + Send>;

/// Spawns a dispatcher thread that drains the completion queue, running each
/// queued [`CompletionJob`] in order. The thread exits once all senders are
/// dropped (after draining any queued jobs).
///
/// Returns the sender half of the completion queue and the thread join handle.
/// The caller stores the sender on its handle (cloned into each async op) and
/// keeps the join handle for teardown.
pub(crate) fn spawn_dispatcher(name: &str) -> (std::sync::mpsc::Sender<CompletionJob>, std::thread::JoinHandle<()>) {
    let (completion_tx, completion_rx) = std::sync::mpsc::channel::<CompletionJob>();
    let dispatcher = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            // Run each completion closure; exits once all senders are dropped
            // (after draining any queued jobs).
            while let Ok(job) = completion_rx.recv() {
                job();
            }
        })
        .expect("failed to spawn FFI callback dispatcher thread");
    (completion_tx, dispatcher)
}

/// Enqueues a [`CompletionJob`] on the dispatcher's completion queue. If the
/// dispatcher is gone (post-teardown), runs the job inline to honor the
/// callback obligation rather than leak the owned handles it captured.
pub(crate) fn enqueue_or_run_inline(tx: &std::sync::mpsc::Sender<CompletionJob>, job: CompletionJob) {
    if let Err(returned) = tx.send(job) {
        (returned.0)();
    }
}

/// Runs `f` on the dispatcher thread and awaits its return value.
///
/// This is how an **async** FFI adapter invokes a C callback whose *completion*
/// the Rust core must observe, rather than firing and forgetting it:
///
/// - the rebalance-listener callbacks return `Result<(), KafkaError>` (a
///   `kafka_common_KafkaError_t*` in C), so the adapter has to wait for the
///   return value;
/// - the commit callback returns nothing, but Java runs `onComplete` on the
///   thread inside `poll()` / `commitSync()`, so the adapter must not let that
///   call return before the C callback has (and must not let the C caller
///   release `user_data` while the job is still queued).
///
/// `f` therefore builds the owned C handles, calls the C function pointer, and
/// maps the result; all of that happens on the dispatcher thread, upholding the
/// invariant that user code never runs on a tokio worker. The awaiting task
/// yields its worker meanwhile, and because the dispatcher is a plain OS thread
/// the user callback may legally `block_on` a nested consumer operation.
///
/// **Head-of-line blocking caveat:** the job shares the single per-handle
/// dispatcher FIFO with every completion callback. A dispatcher callback that
/// block-waits on consumer progress (e.g. a delivery / commit callback that
/// waits for a rebalance to finish) deadlocks, because the job that would make
/// that progress is queued behind it. Dispatcher callbacks must not block on
/// consumer progress.
///
/// **Post-teardown:** once the dispatcher's receiver is gone,
/// [`enqueue_or_run_inline`] runs the job inline on the calling task instead —
/// so the returned value is still produced, but on a tokio worker, where a
/// nested `block_on` inside the user callback would panic.
pub(crate) async fn dispatch_and_wait<T, F>(tx: &std::sync::mpsc::Sender<CompletionJob>, f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<T>();
    enqueue_or_run_inline(
        tx,
        Box::new(move || {
            // The receiver is awaited below and never dropped early, so the
            // only way `send` fails is a cancelled awaiting task — in which
            // case nobody observes the value anyway.
            let _ = result_tx.send(f());
        }),
    );
    // Infallible: `enqueue_or_run_inline` either hands the job to the live
    // dispatcher (which runs every queued job before exiting) or runs it inline,
    // so `result_tx` is always used before it is dropped.
    result_rx.await.expect("dispatch_and_wait job dropped without sending a result")
}

/// Canonical operation callback signature (not exported). A null `error` means
/// success. The public per-method typedefs alias this shape.
pub(crate) type OperationCallbackFn = unsafe extern "C" fn(*mut kafka_common_KafkaError_t, *mut std::ffi::c_void);

/// Owned operation completion payload, fired by the dispatcher thread for
/// void-returning operations (`flush` / `close` / consumer void ops).
pub(crate) struct OperationCompletion {
    pub(crate) callback: OperationCallbackFn,
    pub(crate) user_data: *mut std::ffi::c_void,
    pub(crate) error: *mut kafka_common_KafkaError_t,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread;
// the C user is responsible for the thread-safety of `user_data`.
unsafe impl Send for OperationCompletion {}
impl OperationCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    pub(crate) unsafe fn fire(self) {
        unsafe { (self.callback)(self.error, self.user_data) };
    }
}

/// A C operation-callback target (function pointer + opaque `user_data`).
/// Wrapped so it can cross the tokio task / dispatcher thread boundary.
#[derive(Clone, Copy)]
pub(crate) struct OperationCallbackTarget {
    pub(crate) callback: OperationCallbackFn,
    pub(crate) user_data: *mut std::ffi::c_void,
}
// SAFETY: the C user owns the thread-safety of `user_data`; the function
// pointer is trivially shareable.
unsafe impl Send for OperationCallbackTarget {}

/// Ownership primitive for **multi-shot** callback registrations.
///
/// [`OperationCallbackTarget`] is `Copy` and fits one-shot operations, where the
/// C caller keeps `user_data` alive across a single completion. A long-lived
/// registration — a rebalance listener passed to `subscribe`, a commit callback
/// passed to `commit_async` — instead hands `user_data` to Rust for the whole
/// lifetime of the registration, so someone has to release it when the
/// registration goes away. `CallbackTarget` is that owner: it holds `user_data`
/// and an optional `destroy` hook that fires **exactly once**, when the adapter
/// holding this target is dropped (i.e. when the consumer drops the listener /
/// callback, on unsubscribe or close).
///
/// The hook is what lets a managed-runtime binding balance its reference count:
/// Python stores the listener `PyObject*` as `user_data`, `Py_INCREF`s it once
/// at registration time, and passes a `destroy` trampoline that takes the GIL
/// and `Py_DECREF`s it.
///
/// `destroy` may fire on **any thread** — whichever one drops the adapter (a
/// tokio worker running the consumer's background task, the dispatcher thread,
/// or the C thread calling `_destroy`). It must therefore be thread-agnostic
/// (Python: `PyGILState_Ensure`) and must not assume the calling thread already
/// holds any of the binding's locks.
pub(crate) struct CallbackTarget {
    /// Opaque pointer owned by this target for the lifetime of the registration.
    pub(crate) user_data: *mut std::ffi::c_void,
    /// Optional release hook, fired once from [`Drop`]. `None` means the C
    /// caller retains ownership of `user_data` (nothing to release).
    pub(crate) destroy: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
}

// SAFETY: `user_data` is an opaque pointer whose thread-safety is the C user's
// responsibility (identical to `OperationCompletion` / `OperationCallbackTarget`);
// the `destroy` function pointer is trivially shareable and, per the type's
// contract, callable from any thread. `CallbackTarget` itself never dereferences
// `user_data`.
unsafe impl Send for CallbackTarget {}
unsafe impl Sync for CallbackTarget {}

impl Drop for CallbackTarget {
    fn drop(&mut self) {
        if let Some(destroy) = self.destroy {
            // SAFETY: `destroy` was supplied by the C caller together with
            // `user_data` and documented as callable from any thread. `Drop`
            // runs exactly once per target, so the hook fires exactly once.
            unsafe { destroy(self.user_data) };
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    // -- CallbackTarget -----------------------------------------------------

    /// Stand-in for a binding's release trampoline (e.g. Python's `Py_DECREF`
    /// wrapper): increments the `AtomicUsize` that `user_data` points at.
    unsafe extern "C" fn counting_destroy(user_data: *mut std::ffi::c_void) {
        let counter = unsafe { &*(user_data as *const AtomicUsize) };
        counter.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn test_callback_target_destroy_fires_exactly_once() {
        // Boxed so the counter outlives the target and stays readable after the
        // hook fired (the hook does not free it — the test does).
        let counter = Box::into_raw(Box::new(AtomicUsize::new(0)));
        {
            let target =
                CallbackTarget { user_data: counter as *mut std::ffi::c_void, destroy: Some(counting_destroy) };
            assert_eq!(unsafe { &*counter }.load(Ordering::SeqCst), 0, "destroy fired before drop");
            drop(target);
        }
        assert_eq!(
            unsafe { &*counter }.load(Ordering::SeqCst),
            1,
            "destroy did not fire exactly once"
        );
        unsafe { drop(Box::from_raw(counter)) };
    }

    #[test]
    fn test_callback_target_without_destroy_drops_cleanly() {
        // `destroy: None` means the C caller retains ownership; dropping must
        // not touch `user_data` (here a dangling-but-never-read address).
        let target = CallbackTarget { user_data: 0x1234 as *mut std::ffi::c_void, destroy: None };
        drop(target);
    }

    // -- dispatch_and_wait --------------------------------------------------

    #[tokio::test]
    async fn test_dispatch_and_wait_runs_on_dispatcher_and_returns_value() {
        let (tx, dispatcher) = spawn_dispatcher("test-dispatch-and-wait");
        let caller_thread = std::thread::current().id();

        let (value, job_thread) = dispatch_and_wait(&tx, move || (42_u32, std::thread::current().id())).await;

        assert_eq!(value, 42, "closure return value not propagated");
        assert_ne!(
            job_thread, caller_thread,
            "job ran on the calling task's thread, not the dispatcher"
        );

        drop(tx);
        dispatcher.join().expect("dispatcher thread panicked");
    }

    #[tokio::test]
    async fn test_dispatch_and_wait_runs_inline_when_dispatcher_is_gone() {
        // Dropping the receiver closes the channel, which is what teardown looks
        // like to `enqueue_or_run_inline`.
        let (tx, rx) = std::sync::mpsc::channel::<CompletionJob>();
        drop(rx);
        let caller_thread = std::thread::current().id();

        let (value, job_thread) = dispatch_and_wait(&tx, move || (7_u32, std::thread::current().id())).await;

        assert_eq!(value, 7, "inline fallback did not propagate the return value");
        assert_eq!(
            job_thread, caller_thread,
            "inline fallback should run on the calling task's thread"
        );
    }

    // -- kafka_common_KafkaError_new ----------------------------------------

    #[test]
    fn test_kafka_error_new_round_trips_code_and_message() {
        let message = CString::new("boom").unwrap();
        let error = unsafe { kafka_common_KafkaError_new(7, message.as_ptr()) };
        assert!(!error.is_null());
        unsafe {
            // 7 == Errors::RequestTimedOut, which is retriable and not fatal.
            assert_eq!(kafka_common_KafkaError_code(error), 7);
            assert_eq!(CStr::from_ptr(kafka_common_KafkaError_message(error)).to_str().unwrap(), "boom");
            assert!(kafka_common_KafkaError_is_retriable(error));
            assert!(!kafka_common_KafkaError_is_fatal(error));
            kafka_common_KafkaError_destroy(error);
        }
    }

    #[test]
    fn test_kafka_error_new_null_message_is_empty() {
        let error = unsafe { kafka_common_KafkaError_new(29, std::ptr::null()) };
        assert!(!error.is_null());
        unsafe {
            assert_eq!(
                kafka_common_KafkaError_code(error),
                29,
                "TopicAuthorizationFailed code not preserved"
            );
            assert_eq!(CStr::from_ptr(kafka_common_KafkaError_message(error)).to_str().unwrap(), "");
            kafka_common_KafkaError_destroy(error);
        }
    }

    #[test]
    fn test_kafka_error_new_unknown_code_maps_to_unknown_server_error() {
        let message = CString::new("who knows").unwrap();
        for code in [i32::from(i16::MAX), i32::MAX, i32::MIN, 30_000] {
            let error = unsafe { kafka_common_KafkaError_new(code, message.as_ptr()) };
            assert!(!error.is_null());
            unsafe {
                assert_eq!(
                    kafka_common_KafkaError_code(error),
                    i32::from(Errors::UnknownServerError.code()),
                    "code {code} should map to UnknownServerError"
                );
                assert_eq!(
                    CStr::from_ptr(kafka_common_KafkaError_message(error)).to_str().unwrap(),
                    "who knows"
                );
                kafka_common_KafkaError_destroy(error);
            }
        }
    }
}
