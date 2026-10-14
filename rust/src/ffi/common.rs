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

//! Shared C FFI machinery reused across the producer, consumer and admin FFI
//! layers.
//!
//! This module hosts the pieces that are not specific to any one client:
//!
//! - The opaque [`kafka_common_Error_t`] error handle (CLAUDE.md §4): its
//!   constructors, one per `Error` static factory
//!   (`kafka_common_Error_kafka_message`, `kafka_common_Error_timeout`, ...),
//!   its accessors (`code`, `message`, `source`, `throttle_time_ms`), the
//!   [`kafka_common_Error_e`] enum a C caller switches on, and the
//!   per-class payload views (`kafka_common_Error_resource_not_found` ->
//!   `kafka_common_ResourceNotFoundError_t`). The hierarchy predicates are
//!   generated into `error_predicates.rs`. `kafka_common_*` is shared verbatim
//!   between FFI surfaces — a second definition would make cbindgen emit a
//!   duplicate type.
//! - The dispatcher-thread abstraction ([`CompletionJob`],
//!   [`spawn_dispatcher`], [`enqueue_or_run_inline`]) and the void-returning
//!   operation callback machinery ([`OperationCallbackFn`],
//!   [`OperationCompletion`], [`OperationCallbackTarget`]) the clients still
//!   run their `_async` variants on. They are superseded by the per-client
//!   callbacks vector in [`callback_queue`](crate::ffi::callback_queue) and go
//!   away as each client moves to its `_cb` variants.
//! - The default logger initialization helper ([`init_default_logger`]).
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![expect(non_camel_case_types)]

pub(crate) mod acl;
pub(crate) mod classic_group_state;
pub(crate) mod cluster;
pub(crate) mod cluster_resource;
pub(crate) mod config;
pub(crate) mod election_type;
pub(crate) mod errors;
pub(crate) mod group_state;
pub(crate) mod group_type;
pub(crate) mod header;
pub(crate) mod isolation_level;
pub(crate) mod metric;
pub(crate) mod metric_name;
pub(crate) mod metric_name_template;
pub(crate) mod metrics;
pub(crate) mod node;
pub(crate) mod partition_info;
pub(crate) mod quota;
pub(crate) mod record;
pub(crate) mod resource;
pub(crate) mod security;
pub(crate) mod serialization;
pub(crate) mod topic_collection;
pub(crate) mod topic_id_partition;
pub(crate) mod topic_partition;
pub(crate) mod topic_partition_info;
pub(crate) mod topic_partition_replica;
pub(crate) mod uuid;

use std::any::Any;
use std::collections::HashSet;
use std::ffi::{CString, c_char, c_void};
use std::panic::AssertUnwindSafe;
use std::sync::OnceLock;

use futures_util::FutureExt;

use crate::common::Error;
use crate::common::protocol::Errors;
use crate::ffi::callback_queue::{CallbackJob, SendPtr};
use crate::ffi::common::errors::local_callback_error::kafka_common_LocalCallbackError_release_fn_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_List_t, list_strings};
// The 167 enumerators are spelled out in full (see
// [`kafka_common_Error_e`]), so this glob keeps the match arms in
// `error_code_of` readable without repeating the type name on each one.
use kafka_common_Error_e::*;

/// Initializes the default stderr log backend, honouring `RUST_LOG`, the first
/// time a client is created from C. Idempotent: `try_init` succeeds once and
/// is a silent no-op afterwards, so a logger a binding installed beforehand
/// stays in place.
pub(crate) fn init_default_logger() {
    let _ = env_logger::try_init();
}

// ---------------------------------------------------------------------------
// Panic guard
// ---------------------------------------------------------------------------

/// Runs the body of an `extern "C"` entry point, turning a Rust panic into the
/// entry point's failure value instead of letting it unwind into the C caller.
///
/// This is the runtime half of [`ffi_guard`](crate::ffi::ffi_guard): the
/// attribute rewrites each entry point's body into
/// `ffi_guard_or("<name>", <on-panic closure>, move || <body>)`. A panic that
/// reached the `extern "C"` frame would abort the whole process — the release
/// profile keeps `panic = "unwind"` because std `Mutex` poisoning relies on it,
/// so the abort would come from the shim the compiler inserts at the boundary.
/// Here the panic is caught instead, turned into an [`Error`] by
/// [`panic_error`], logged at `error` level, and handed to `on_panic`, whose
/// value the entry point returns.
///
/// The default panic hook is left alone, so the usual "thread … panicked at …"
/// line still reaches stderr before the error is logged.
///
/// # Unwind safety
///
/// The body runs under [`AssertUnwindSafe`]. A panic can leave the state behind
/// a handle's lock half-updated; std `Mutex` poisoning reports that to every
/// later call on the handle, which then fails with the poison message instead
/// of reading the state. That is intended: such a handle should be destroyed
/// and recreated, as the generated C header says, and the poison is not
/// cleared anywhere.
///
/// `on_panic` must not panic itself: nothing is left to catch it.
///
/// No Java counterpart: Java has no C boundary.
pub(crate) fn ffi_guard_or<R>(fn_name: &'static str, on_panic: impl FnOnce(Error) -> R, body: impl FnOnce() -> R) -> R {
    match std::panic::catch_unwind(AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(payload) => on_panic(caught_panic_error(fn_name, payload)),
    }
}

/// The [`Error`] a panic caught by [`ffi_guard_or`] is reported as.
///
/// An [`Error::LocalIllegalState`] whose message is
/// `Rust panic caught at the FFI boundary in <fn_name>: <panic message>`. The
/// panic message is the payload when it is a `&str` or a `String`, which is
/// what `panic!`, `assert!`, `unwrap` and `expect` produce, and
/// `non-string panic payload` otherwise (`std::panic::panic_any`).
pub(crate) fn panic_error(fn_name: &str, payload: &(dyn Any + Send)) -> Error {
    let message = if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "non-string panic payload"
    };
    Error::local_illegal_state(format!("Rust panic caught at the FFI boundary in {fn_name}: {message}"))
}

/// Spawns the task a callback-style entry point hands its callback to,
/// aborting the process if the spawn panics.
///
/// `#[ffi_guard(on_panic = ...)]` fires the entry point's callback when the
/// body panics, which is right only while no task can fire it as well. Tokio
/// can panic inside `spawn` after the task is queued: the C caller's thread is
/// not one of the runtime's workers, so the task is queued and a worker is then
/// woken, and a failed wake is `expect("failed to wake I/O driver")`
/// (`runtime/io/driver.rs:260`, tokio 1.52.0). Unwinding from there would queue
/// the completion from `on_panic` while the queued task queues it too, and
/// would skip the client's `tasks` registration after the spawn, which
/// `destroy` relies on. So the process aborts, as every panic at the boundary
/// did before `#[ffi_guard]`: the OS refused to wake the runtime. See §4 item 6
/// of `design/current/appsec-7665-4521-ffi-panic-guard.md`.
///
/// Every task the FFI spawns holding a C callback or `opaque` goes through
/// here: the producer, consumer and admin `Client::spawn` and
/// `kafka_common_KafkaFuture_get_cb`.
///
/// No Java counterpart: Java has no C boundary.
pub(crate) fn spawn_callback_task<F>(runtime: &tokio::runtime::Handle, task: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_or_abort(|| runtime.spawn(task))
}

/// Runs `spawn` and aborts the process if it panics: the body of
/// [`spawn_callback_task`], taking the spawn as a closure so a test can make it
/// panic.
fn spawn_or_abort<J>(spawn: impl FnOnce() -> J) -> J {
    match std::panic::catch_unwind(AssertUnwindSafe(spawn)) {
        Ok(join) => join,
        // The payload is not dropped: nothing runs after the abort.
        Err(_) => {
            log::error!(
                "aborting: tokio panicked while spawning a task that owns a C callback, which may already be \
                 queued; unwinding would let the FFI guard fire that callback a second time"
            );
            std::process::abort()
        },
    }
}

// ---------------------------------------------------------------------------
// Panic completions of the `_cb` entry points
// ---------------------------------------------------------------------------

/// The completion of a `_cb` entry point that reports only an error.
pub(crate) type ErrorCompletion = unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The completion of a `_cb` entry point that reports an `int64_t` value or an
/// error.
pub(crate) type I64Completion = unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The completion of a `_cb` entry point that reports nothing but its `opaque`
/// (a Java `void` method that cannot fail, such as `Admin.close`).
pub(crate) type OpaqueCompletion = unsafe extern "C" fn(opaque: *mut c_void);

/// Delivers the completion of a `_cb` entry point whose body panicked: `cb`
/// receives the panic `error` and the caller's `opaque`.
///
/// `complete` is where the handle runs its completions: the client's
/// [`CallbackQueue::push`](crate::ffi::callback_queue::CallbackQueue::push),
/// so the completion fires from the client's `__execute_callbacks` like any
/// other, or, for a handle with no callbacks vector (a future with no client,
/// a standalone listener), a call on the calling thread. Each entry point's
/// `#[ffi_guard(on_panic = ...)]` calls this, or the helper of its callback's
/// shape, so the panic is reported exactly once: the guard fires only when
/// the body did not return, and every body hands its completion over (to a
/// task, the queue, the submission channel or the C method) as its last step,
/// after which nothing can panic.
///
/// # Safety
///
/// `cb` and `opaque` must be the pair the C caller passed to the entry point,
/// which has not fired `cb`.
pub(crate) unsafe fn deliver_panic(
    complete: impl FnOnce(CallbackJob),
    cb: ErrorCompletion,
    opaque: *mut c_void,
    error: Error,
) {
    let error = SendPtr(box_error(error) as *mut c_void);
    let opaque = SendPtr(opaque);
    complete(Box::new(move || {
        // SAFETY: Per this function's `# Safety`, `cb` and `opaque` are the pair the C caller
        // passed to the entry point, which has not fired `cb`, so calling `cb` with `opaque`
        // is the completion the caller is waiting for. `error` is the fresh `box_error` handle
        // above, owned by no one else, so handing it to `cb` transfers its ownership; the job
        // is a `FnOnce`, so `cb` fires exactly once.
        unsafe { cb(error.get() as *mut kafka_common_Error_t, opaque.get()) }
    }));
}

/// [`deliver_panic`] for a completion that also reports a pointer value: the
/// value is NULL.
///
/// # Safety
///
/// As [`deliver_panic`].
pub(crate) unsafe fn deliver_panic_value<P: 'static>(
    complete: impl FnOnce(CallbackJob),
    cb: unsafe extern "C" fn(value: *mut P, error: *mut kafka_common_Error_t, opaque: *mut c_void),
    opaque: *mut c_void,
    error: Error,
) {
    let error = SendPtr(box_error(error) as *mut c_void);
    let opaque = SendPtr(opaque);
    complete(Box::new(move || {
        // SAFETY: Per this function's `# Safety`, `cb` and `opaque` are the pair the C caller
        // passed to the entry point, which has not fired `cb`, so calling `cb` with `opaque`
        // is the completion the caller is waiting for. `error` is the fresh `box_error` handle
        // above, owned by no one else, so handing it to `cb` transfers its ownership; the job
        // is a `FnOnce`, so `cb` fires exactly once.
        unsafe { cb(std::ptr::null_mut(), error.get() as *mut kafka_common_Error_t, opaque.get()) }
    }));
}

/// [`deliver_panic`] for a completion that also reports an `int64_t` value:
/// the value is -1, as for every failure of such a completion.
///
/// # Safety
///
/// As [`deliver_panic`].
pub(crate) unsafe fn deliver_panic_i64(
    complete: impl FnOnce(CallbackJob),
    cb: I64Completion,
    opaque: *mut c_void,
    error: Error,
) {
    let error = SendPtr(box_error(error) as *mut c_void);
    let opaque = SendPtr(opaque);
    complete(Box::new(move || {
        // SAFETY: Per this function's `# Safety`, `cb` and `opaque` are the pair the C caller
        // passed to the entry point, which has not fired `cb`, so calling `cb` with `opaque`
        // is the completion the caller is waiting for. `error` is the fresh `box_error` handle
        // above, owned by no one else, so handing it to `cb` transfers its ownership; the job
        // is a `FnOnce`, so `cb` fires exactly once.
        unsafe { cb(-1, error.get() as *mut kafka_common_Error_t, opaque.get()) }
    }));
}

/// [`deliver_panic`] for a completion that reports only `opaque`: the callback
/// still fires, so the caller gets `opaque` back to release, and the panic is
/// known only from the error-level line [`ffi_guard_or`] already logged.
///
/// # Safety
///
/// As [`deliver_panic`].
pub(crate) unsafe fn deliver_panic_opaque(
    complete: impl FnOnce(CallbackJob),
    cb: OpaqueCompletion,
    opaque: *mut c_void,
    error: Error,
) {
    drop(error);
    let opaque = SendPtr(opaque);
    complete(Box::new(move || {
        // SAFETY: Per this function's `# Safety`, `cb` and `opaque` are the pair the C caller
        // passed to the entry point, which has not fired `cb`, so calling `cb` with `opaque`
        // is the completion the caller is waiting for; the job is a `FnOnce`, so `cb` fires
        // exactly once.
        unsafe { cb(opaque.get()) }
    }));
}

/// The completion sink of a handle with no callbacks vector: runs the
/// completion on the calling thread, as such a handle runs every completion
/// (see [`deliver_panic`]).
pub(crate) fn run_inline(job: CallbackJob) {
    job();
}

/// Runs a `_cb` entry point's task body, turning a panic inside it into the
/// task's `Err` result, which the task then delivers like any other failure.
///
/// The guard of the entry point does not cover the task: the body returned
/// once the task was spawned. Without this a panicking task would end without
/// its completion, so the C caller would never get `opaque` back; here the
/// completion carries the [`panic_error`] of `fn_name`, the entry point that
/// spawned the task, and the panic is logged as [`ffi_guard_or`] logs one.
/// The state the task was working on may be inconsistent afterwards, as for
/// any caught panic: the handle should be destroyed.
pub(crate) async fn catch_task_panic<T, F>(fn_name: &'static str, task: F) -> Result<T, Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    match AssertUnwindSafe(task).catch_unwind().await {
        Ok(result) => result,
        Err(payload) => Err(caught_panic_error(fn_name, payload)),
    }
}

/// The [`panic_error`] for a caught `payload`, logged at `error` level, with
/// the payload dropped under a second guard: a payload's destructor is
/// arbitrary code, and one that panicked would unwind out of the catching
/// frame after all, so whatever that second panic carries is leaked.
pub(crate) fn caught_panic_error(fn_name: &'static str, payload: Box<dyn Any + Send>) -> Error {
    let error = panic_error(fn_name, &*payload);
    if let Err(nested) = std::panic::catch_unwind(AssertUnwindSafe(move || drop(payload))) {
        std::mem::forget(nested);
    }
    log::error!("{}", error.message());
    error
}

/// Test-only fail point: makes the next delivery helper reached on this
/// thread panic before it hands its completion over, so a test can drive a
/// real `_cb` entry point down its guard's `on_panic` path.
#[cfg(test)]
pub(crate) mod fail_point {
    use std::cell::Cell;

    thread_local! {
        static ARMED: Cell<bool> = const { Cell::new(false) };
    }

    /// The message [`hit`] panics with.
    pub(crate) const MESSAGE: &str = "fail point before the completion hand-off";

    /// Arms the fail point for the next [`hit`] on this thread.
    pub(crate) fn arm() {
        ARMED.with(|armed| armed.set(true));
    }

    /// Panics, once, when [`arm`] was called on this thread.
    pub(crate) fn hit() {
        hit_if(take());
    }

    /// Disarms the fail point of this thread, returning whether it was armed,
    /// so a body can carry the arming into a task it spawns and fire it there
    /// with [`hit_if`].
    pub(crate) fn take() -> bool {
        ARMED.with(|armed| armed.replace(false))
    }

    /// Panics as [`hit`] does when `armed`, a value [`take`] returned.
    pub(crate) fn hit_if(armed: bool) {
        if armed {
            panic!("{MESSAGE}");
        }
    }
}

/// Test-only recorder for the completions a `_cb` entry point fires, one
/// callback per completion shape, so the tests of every module can check that
/// a caught panic is delivered exactly once, as the error of [`panic_error`].
#[cfg(test)]
pub(crate) mod panic_probe {
    use std::ffi::c_void;
    use std::sync::Mutex;

    use super::{kafka_common_Error_t, take_error};

    /// What each completion fired with, in firing order: the error message,
    /// or `"<no error>"` for a completion without one.
    #[derive(Default)]
    pub(crate) struct Probe {
        fired: Mutex<Vec<String>>,
    }

    impl Probe {
        pub(crate) fn as_opaque(&self) -> *mut c_void {
            self as *const Self as *mut c_void
        }

        pub(crate) fn fired(&self) -> Vec<String> {
            self.fired.lock().unwrap().clone()
        }

        /// Asserts exactly one completion fired, carrying the panic error of
        /// `fn_name` with the panic message `panic`.
        pub(crate) fn assert_one_panic(&self, fn_name: &str, panic: &str) {
            assert_eq!(
                self.fired(),
                [format!("Rust panic caught at the FFI boundary in {fn_name}: {panic}")]
            );
        }

        /// Records `error`, which must be the `LocalIllegalState` a panic is
        /// reported as.
        ///
        /// # Safety
        ///
        /// `error` must be an owned error handle not yet destroyed; it is consumed.
        unsafe fn record(&self, error: *mut kafka_common_Error_t) {
            // SAFETY: Test code: per this function's `# Safety`, `error` is an owned handle not
            // yet destroyed, which `take_error` consumes here, its final use.
            let error = unsafe { take_error(error) }.expect("a panic completion carries an error");
            assert!(error.is_local_illegal_state_error(), "{error:?}");
            self.fired.lock().unwrap().push(error.message().to_string());
        }
    }

    /// The [`Probe`] behind a completion's `opaque`.
    ///
    /// # Safety
    ///
    /// `opaque` must be [`Probe::as_opaque`] of a `Probe` that outlives `'a`.
    unsafe fn probe<'a>(opaque: *mut c_void) -> &'a Probe {
        // SAFETY: Test code: per this function's `# Safety`, `opaque` came from
        // `Probe::as_opaque` of a live `Probe` that outlives `'a`; the reference is shared, and
        // `Probe` mutates only through its `Mutex`.
        unsafe { &*(opaque as *const Probe) }
    }

    /// An `(error, opaque)` completion.
    pub(crate) unsafe extern "C" fn on_error(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        // SAFETY: Test code: every test registering this completion passes `Probe::as_opaque`
        // of a `Probe` it keeps alive until the completion has fired, and a completion owns
        // the error handle it receives.
        unsafe { probe(opaque).record(error) }
    }

    /// A `(value *, error, opaque)` completion: a failure has a NULL value.
    pub(crate) unsafe extern "C" fn on_value<P>(value: *mut P, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        assert!(value.is_null());
        // SAFETY: Test code: every test registering this completion passes `Probe::as_opaque`
        // of a `Probe` it keeps alive until the completion has fired, and a completion owns
        // the error handle it receives.
        unsafe { probe(opaque).record(error) }
    }

    /// An `(int64_t value, error, opaque)` completion: a failure has -1.
    pub(crate) unsafe extern "C" fn on_i64(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        assert_eq!(value, -1);
        // SAFETY: Test code: every test registering this completion passes `Probe::as_opaque`
        // of a `Probe` it keeps alive until the completion has fired, and a completion owns
        // the error handle it receives.
        unsafe { probe(opaque).record(error) }
    }

    /// An `(opaque)` completion, which carries no error.
    pub(crate) unsafe extern "C" fn on_opaque(opaque: *mut c_void) {
        // SAFETY: Test code: every test registering this completion passes `Probe::as_opaque`
        // of a `Probe` it keeps alive until the completion has fired.
        unsafe { probe(opaque) }.fired.lock().unwrap().push("<no error>".to_string());
    }
}

// ---------------------------------------------------------------------------
// Error handle
// ---------------------------------------------------------------------------

/// Internal wrapper that pairs [`Error`] with a [`CString`] for the
/// error message, so that [`kafka_common_Error_message`] can return a valid
/// `*const c_char` that lives as long as the handle.
pub(crate) struct ErrorInner {
    pub(crate) error: Error,
    /// Cached CString for the error message, created once at construction time.
    pub(crate) message_cstring: CString,
    /// The handle [`kafka_common_Error_source`] borrows out, built on first
    /// request from [`Error::source`] and owned by this handle so the chain
    /// of causes lives exactly as long as the outermost error.
    source: OnceLock<Option<Box<ErrorInner>>>,
    /// The payload view a `kafka_common_Error_<class>` accessor borrows out
    /// (an [`errors::Payload`] of the variant's class), built on first request
    /// and owned by this handle so it lives exactly as long as the error. An
    /// error has one class, so one slot suffices.
    payload: OnceLock<Box<dyn Any + Send + Sync>>,
}

impl ErrorInner {
    pub(crate) fn new(error: Error) -> Self {
        let message_cstring = CString::new(error.message()).unwrap_or_default();
        ErrorInner { error, message_cstring, source: OnceLock::new(), payload: OnceLock::new() }
    }

    /// The borrowed payload view of this error's class, built from `value`
    /// (the variant's payload, which the caller has already matched) on the
    /// first call and cached afterwards.
    pub(crate) fn payload_view<T: errors::PayloadClass>(&self, value: &T) -> *const errors::Payload<T> {
        let view = self.payload.get_or_init(|| Box::new(errors::Payload::new(value.clone())));
        view.downcast_ref::<errors::Payload<T>>()
            .map_or(std::ptr::null(), |payload| payload as *const _)
    }

    /// A borrowed handle on the error's cause, or null when it has none.
    fn source_ptr(&self) -> *const kafka_common_Error_t {
        match self
            .source
            .get_or_init(|| self.error.source().cloned().map(|e| Box::new(ErrorInner::new(e))))
        {
            Some(inner) => &**inner as *const ErrorInner as *const kafka_common_Error_t,
            None => std::ptr::null(),
        }
    }
}

/// Opaque error handle returned by functions that can fail.
///
/// Internally wraps a `Box<ErrorInner>` containing the [`Error`]
/// and a cached [`CString`] for the error message.
///
/// A null `kafka_common_Error_t` pointer means success (no error).
#[repr(C)]
// the handle over `common::Error` (CLAUDE.md §4)
#[doc(alias = "rust-only")]
pub struct kafka_common_Error_t {
    _private: [u8; 0],
}

/// Wraps a [`Error`] into a heap-allocated opaque error pointer, including
/// a cached [`CString`] for the error message.
pub(crate) fn box_error(error: Error) -> *mut kafka_common_Error_t {
    Box::into_raw(Box::new(ErrorInner::new(error))) as *mut kafka_common_Error_t
}

/// What a fallible entry point returns beside the error it stored in
/// `*out_err`: null for a pointer, a null `kafka_Bytes_t`, `0` for an
/// `int8_t`, `-1` for any wider integer. `#[ffi_guard]` returns the same value
/// for a caught panic, so a failure and a panic cannot be told apart by the
/// value, which the caller never reads: it tests `*out_err`.
///
/// No Java counterpart: Java has no C boundary.
pub(crate) trait FailValue {
    /// The failure value.
    fn fail() -> Self;
}

impl FailValue for () {
    fn fail() -> Self {}
}

impl<T> FailValue for *mut T {
    fn fail() -> Self {
        std::ptr::null_mut()
    }
}

impl<T> FailValue for *const T {
    fn fail() -> Self {
        std::ptr::null()
    }
}

impl FailValue for i8 {
    fn fail() -> Self {
        0
    }
}

impl FailValue for i16 {
    fn fail() -> Self {
        -1
    }
}

impl FailValue for i32 {
    fn fail() -> Self {
        -1
    }
}

impl FailValue for i64 {
    fn fail() -> Self {
        -1
    }
}

impl FailValue for crate::ffi::util::kafka_Bytes_t {
    fn fail() -> Self {
        Self::NULL
    }
}

/// Stores `error` in `*out_err`, as an owned handle the caller frees.
///
/// # Safety
///
/// `out_err` must be valid for one write. It should point at null, as every
/// entry point taking it requires, or the handle there leaks.
pub(crate) unsafe fn store_error(out_err: *mut *mut kafka_common_Error_t, error: Error) {
    // SAFETY: Per this function's `# Safety`, `out_err` is valid for one write. A raw pointer has
    // no drop glue, so the write reads nothing from the slot.
    unsafe { out_err.write(box_error(error)) }
}

/// Stores `error` in `*out_err` and returns the failure value: the tail of
/// every failing path of a fallible entry point.
///
/// # Safety
///
/// As [`store_error`].
pub(crate) unsafe fn fail_with<P: FailValue>(out_err: *mut *mut kafka_common_Error_t, error: Error) -> P {
    // SAFETY: Per this function's `# Safety`, `out_err` is as `store_error` requires.
    unsafe { store_error(out_err, error) };
    P::fail()
}

/// The value of `result` converted by `to_value`, or, for an error, the
/// failure value with the error stored in `*out_err`.
///
/// # Safety
///
/// As [`store_error`].
pub(crate) unsafe fn deliver_value<T, P: FailValue>(
    result: Result<T, Error>,
    out_err: *mut *mut kafka_common_Error_t,
    to_value: impl FnOnce(T) -> P,
) -> P {
    match result {
        Ok(value) => to_value(value),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Stores the error of `result`, if any, in `*out_err`.
///
/// # Safety
///
/// As [`store_error`].
pub(crate) unsafe fn deliver_unit(result: Result<(), Error>, out_err: *mut *mut kafka_common_Error_t) {
    if let Err(error) = result {
        // SAFETY: Per this function's `# Safety`, `out_err` is as `store_error` requires.
        unsafe { store_error(out_err, error) }
    }
}

/// Casts a `*const kafka_common_Error_t` to a reference to `ErrorInner`.
///
/// # Safety
///
/// The pointer must be non-null and point at a live `ErrorInner`: either an
/// owned handle from [`box_error`], or a borrowed handle a live owner hands out
/// ([`kafka_common_Error_source`], or a payload's or struct field's error
/// accessor). The handle, and for a borrowed one its owner, must stay alive
/// (not passed to [`kafka_common_Error_destroy`] nor consumed by
/// [`take_error`]) for as long as the returned reference, or anything borrowed
/// from it, is used: the `'static` lifetime is not real.
pub(crate) unsafe fn error_ref(error: *const kafka_common_Error_t) -> &'static ErrorInner {
    // SAFETY: Per this function's `# Safety`, `error` is non-null and points at a live
    // `ErrorInner`: an owned handle (the `Box::into_raw` of `box_error`) or one borrowed from
    // a live owner (an `ErrorInner` held inside another handle). Either way the cast targets
    // a live, correctly typed `ErrorInner`. The `&'static` is a convenience only: the same
    // contract makes the caller
    // keep the handle alive while it uses the reference, which is shared, so nothing writes
    // the `ErrorInner` under it (its lazy slots are `OnceLock`s).
    unsafe { &*(error as *const ErrorInner) }
}

// ---------------------------------------------------------------------------
// Constructors: the `Error` static factories (CLAUDE.md §4, "Static methods")
// ---------------------------------------------------------------------------
//
// These let a C (or Python) callback that must *return* an error to the Rust
// core build the handle to return. The rebalance-listener callbacks are the
// motivating case — they return `Result<(), Error>` in the core, i.e. a
// `kafka_common_Error_t *` in C, and a Python listener that raised an
// exception has to convert it into one. They mirror `Error`'s own
// constructors one for one, so C builds exactly the classes Rust can: there
// is no constructor from a bare numeric code, because a code does not
// identify a class a C caller may legitimately raise (Java's `Errors.forCode`
// is a wire concern, and the client-side classes have no wire code at all).
//
// Every constructor returns an owned handle the caller frees with
// [`kafka_common_Error_destroy`] — unless it hands it to a Rust callback that
// documents taking ownership of it. A `source` parameter is an owned handle
// the constructor consumes; the caller must not destroy it afterwards.

/// The error behind an owned `source` handle, consumed by the constructor.
///
/// A null `source` violates the constructor's precondition; the bare
/// `KafkaException` stands in so the call stays memory-safe.
///
/// # Safety
///
/// `source` must be null or an owned error handle not yet destroyed.
unsafe fn owned_source(source: *mut kafka_common_Error_t) -> Error {
    // SAFETY: Per this function's `# Safety`, `source` is null or an owned error handle not
    // yet destroyed, as `take_error` requires; the caller hands it over, so consuming it
    // here is its final use.
    unsafe { take_error(source) }.unwrap_or_else(Error::kafka)
}

/// A `kafka_List_t` of `const char *` topic names, read into a set.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings.
unsafe fn topic_set(topics: *const kafka_List_t) -> HashSet<String> {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list of
    // NUL-terminated strings, as `list_strings` requires; the names are copied out, so
    // nothing borrows the list after this call.
    unsafe { list_strings(topics) }.into_iter().collect()
}

/// `new KafkaException()`: the bare Kafka error with the default message.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Error_kafka() -> *mut kafka_common_Error_t {
    box_error(Error::kafka())
}

/// `new KafkaException(String message)`: the bare Kafka error with `message`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_kafka_message(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::kafka_message(unsafe { c_str_to_string(message) }))
}

/// `new KafkaException(Throwable cause)`: the bare Kafka error caused by
/// `source`, which is consumed.
///
/// # Safety
///
/// `source` must be an owned error handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_kafka_source(
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `source` is an owned error handle not yet
    // destroyed, as `owned_source` requires. The caller hands `source` over, so taking it here
    // is its single, final use.
    box_error(Error::kafka_source(unsafe { owned_source(source) }))
}

/// `new KafkaException(String message, Throwable cause)`: the bare Kafka
/// error with `message`, caused by `source`, which is consumed.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string; `source` must
/// be an owned error handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_kafka_message_source(
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires, and `source` is an owned error handle not yet
    // destroyed, as `owned_source` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call; the caller hands `source` over, so taking it here is
    // its single, final use.
    box_error(Error::kafka_message_source(unsafe { c_str_to_string(message) }, unsafe {
        owned_source(source)
    }))
}

/// `new TopicAuthorizationException(Set<String> unauthorizedTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_topic_authorization(
    topics: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list of
    // NUL-terminated strings, as `topic_set` requires. `topics` is only read here, into owned
    // values, so nothing borrows it after this call.
    box_error(Error::topic_authorization(unsafe { topic_set(topics) }))
}

/// `new TopicAuthorizationException(String message, Set<String> unauthorizedTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings; `message`
/// must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_topic_authorization_message(
    topics: *const kafka_List_t,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list of
    // NUL-terminated strings, as `topic_set` requires, and `message` is null or a valid
    // NUL-terminated C string, as `c_str_to_string` requires. `topics` and `message` are only
    // read here, into owned values, so nothing borrows them after this call.
    box_error(Error::topic_authorization_message(unsafe { topic_set(topics) }, unsafe {
        c_str_to_string(message)
    }))
}

/// `new InvalidTopicException(Set<String> invalidTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_topics(topics: *const kafka_List_t) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list of
    // NUL-terminated strings, as `topic_set` requires. `topics` is only read here, into owned
    // values, so nothing borrows it after this call.
    box_error(Error::invalid_topics(unsafe { topic_set(topics) }))
}

/// `new InvalidTopicException(String message, Set<String> invalidTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings; `message`
/// must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_topics_message(
    topics: *const kafka_List_t,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list of
    // NUL-terminated strings, as `topic_set` requires, and `message` is null or a valid
    // NUL-terminated C string, as `c_str_to_string` requires. `topics` and `message` are only
    // read here, into owned values, so nothing borrows them after this call.
    box_error(Error::invalid_topics_message(unsafe { topic_set(topics) }, unsafe {
        c_str_to_string(message)
    }))
}

/// `new GroupAuthorizationException(String groupId)`.
///
/// # Safety
///
/// `group_id` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_group_authorization(group_id: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `group_id` is only read here, into owned values,
    // so nothing borrows it after this call.
    box_error(Error::group_authorization(unsafe { c_str_to_string(group_id) }))
}

/// `new GroupAuthorizationException(String message, String groupId)`.
///
/// # Safety
///
/// `group_id` and `message` must each be null or a valid NUL-terminated C
/// string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_group_authorization_with_message(
    group_id: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::group_authorization_with_message(
        // SAFETY: Per this function's `# Safety`, `group_id` is null or a valid NUL-terminated
        // C string, as `c_str_to_string` requires. `group_id` is only read here, into owned
        // values, so nothing borrows it after this call.
        unsafe { c_str_to_string(group_id) },
        // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
        // string, as `c_str_to_string` requires. `message` is only read here, into owned
        // values, so nothing borrows it after this call.
        unsafe { c_str_to_string(message) },
    ))
}

/// `new InvalidGroupIdException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_group_id(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::invalid_group_id(unsafe { c_str_to_string(message) }))
}

/// `new ThrottlingQuotaExceededException(int throttleTimeMs, String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_throttling_quota_exceeded(
    throttle_time_ms: i32,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::throttling_quota_exceeded(throttle_time_ms, unsafe {
        c_str_to_string(message)
    }))
}

/// `new BufferExhaustedException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_buffer_exhausted(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::buffer_exhausted(unsafe { c_str_to_string(message) }))
}

/// `new IllegalArgumentException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_illegal_argument(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::local_illegal_argument(unsafe { c_str_to_string(message) }))
}

/// `new ConfigException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_config_message(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::config_message(unsafe { c_str_to_string(message) }))
}

/// `new ConfigException(String name, Object value)`: the value is its text.
///
/// # Safety
///
/// `name` and `value` must each be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_config_name_value(
    name: *const c_char,
    value: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `name` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires, and `value` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `name` and `value` are only read here, into owned
    // values, so nothing borrows them after this call.
    box_error(Error::config_name_value(unsafe { c_str_to_string(name) }, unsafe {
        c_str_to_string(value)
    }))
}

/// `new ConfigException(String name, Object value, String message)`: the
/// value is its text.
///
/// # Safety
///
/// `name`, `value` and `message` must each be null or a valid NUL-terminated
/// C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_config_name_value_message(
    name: *const c_char,
    value: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::config_name_value_message(
        // SAFETY: Per this function's `# Safety`, `name` is null or a valid NUL-terminated C
        // string, as `c_str_to_string` requires. `name` is only read here, into owned values,
        // so nothing borrows it after this call.
        unsafe { c_str_to_string(name) },
        // SAFETY: Per this function's `# Safety`, `value` is null or a valid NUL-terminated C
        // string, as `c_str_to_string` requires. `value` is only read here, into owned values,
        // so nothing borrows it after this call.
        unsafe { c_str_to_string(value) },
        // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
        // string, as `c_str_to_string` requires. `message` is only read here, into owned
        // values, so nothing borrows it after this call.
        unsafe { c_str_to_string(message) },
    ))
}

/// `new IllegalStateException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_illegal_state(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::local_illegal_state(unsafe { c_str_to_string(message) }))
}

/// `new TimeoutException(String message)` — the retriable
/// `org.apache.kafka.common.errors.TimeoutException`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_timeout(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::timeout(unsafe { c_str_to_string(message) }))
}

/// `new RecordTooLargeException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_too_large(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::record_too_large(unsafe { c_str_to_string(message) }))
}

/// `new CorrelationIdMismatchException(String message, int requestCorrelationId, int responseCorrelationId)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_correlation_id_mismatch(
    message: *const c_char,
    request_correlation_id: i32,
    response_correlation_id: i32,
) -> *mut kafka_common_Error_t {
    box_error(Error::correlation_id_mismatch(
        // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
        // string, as `c_str_to_string` requires. `message` is only read here, into owned
        // values, so nothing borrows it after this call.
        unsafe { c_str_to_string(message) },
        request_correlation_id,
        response_correlation_id,
    ))
}

/// `new InvalidReceiveException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_receive(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::invalid_receive(unsafe { c_str_to_string(message) }))
}

/// `new SchemaException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_schema(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::schema(unsafe { c_str_to_string(message) }))
}

/// `new SchemaException(String message, Throwable cause)`: `source` is
/// consumed.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string; `source` must
/// be an owned error handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_schema_source(
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires, and `source` is an owned error handle not yet
    // destroyed, as `owned_source` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call; the caller hands `source` over, so taking it here is
    // its single, final use.
    box_error(Error::schema_source(unsafe { c_str_to_string(message) }, unsafe {
        owned_source(source)
    }))
}

/// `new SerializationException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_serialization(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::serialization(unsafe { c_str_to_string(message) }))
}

/// `new UnsupportedVersionException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_unsupported_version(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::unsupported_version(unsafe { c_str_to_string(message) }))
}

/// `new WakeupException()` carrying `message`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_wakeup(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::wakeup(unsafe { c_str_to_string(message) }))
}

/// `new ConcurrentModificationException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_concurrent_modification(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::local_concurrent_modification(unsafe { c_str_to_string(message) }))
}

/// `new java.util.concurrent.TimeoutException(String message)` — the one
/// `Future.get(timeout, unit)` declares, not the retriable Kafka class.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_timeout(message: *const c_char) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::local_timeout(unsafe { c_str_to_string(message) }))
}

/// The error a callback written in another language reports, from an
/// interface method through `<Client>__set_callback_result`, when it raised one
/// of that language's errors: `message` is its text, kept for the client's
/// logs, and `opaque` the binding's pointer to it. No Java class. The client
/// treats it as Java treats a listener's foreign `Throwable` (it wraps it, so
/// it comes back as the cause of the operation's error), and
/// `kafka_common_Error_local_callback_error` reads the pointer back.
///
/// Rust never dereferences `opaque`. When `release` is `NULL` the error does
/// not own it: the caller keeps what it points at alive for as long as it may
/// read the pointer back. Otherwise the error owns `opaque`: Rust calls
/// `release(opaque)` exactly once, when the last copy of the error is dropped
/// — this handle and every error the client made from it, such as an error
/// whose cause it is, and the errors handed to other callbacks — and that can
/// happen on any thread, a Rust task included, and after this function and
/// the call that reported the error have returned.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string. `release`, when
/// not `NULL`, must be safe to call once with `opaque` from any thread, at any
/// time until the process exits.
// a binding's foreign callback error, no Java class (DoD #7)
#[doc(alias = "rust-only")]
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_callback(
    message: *const c_char,
    opaque: *mut std::ffi::c_void,
    release: kafka_common_LocalCallbackError_release_fn_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    let message = unsafe { c_str_to_string(message) };
    box_error(match release {
        // SAFETY: Per this function's `# Safety`, a non-null `release` is safe to call once with
        // `opaque` from any thread, at any time, as `local_callback_with_release` requires; the
        // error makes that one call when its last copy drops.
        Some(release) => unsafe { Error::local_callback_with_release(message, opaque, release) },
        None => Error::local_callback(message, opaque),
    })
}

/// `new TransactionAbortedException()`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Error_transaction_aborted() -> *mut kafka_common_Error_t {
    box_error(Error::transaction_aborted())
}

/// `new TransactionAbortedException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_transaction_aborted_message(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::transaction_aborted_message(unsafe { c_str_to_string(message) }))
}

/// `new RecordBatchTooLargeException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_batch_too_large(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated C
    // string, as `c_str_to_string` requires. `message` is only read here, into owned values, so
    // nothing borrows it after this call.
    box_error(Error::record_batch_too_large(unsafe { c_str_to_string(message) }))
}

/// Takes ownership of an error handle **returned by a C callback** and converts
/// it back into a [`Error`], freeing the handle. A null pointer means
/// success and yields `None`.
///
/// This is the inbound counterpart of [`box_error`]: it is how a callback whose
/// Rust signature returns `Result<(), Error>` (the rebalance-listener
/// methods) reports failure across the boundary. The handle is consumed exactly
/// as [`kafka_common_Error_destroy`] would consume it, so the C callback
/// must not free it itself.
///
/// # Safety
///
/// `error` must be null or a handle created by [`box_error`] (i.e. by one of
/// the `kafka_common_Error_<constructor>` functions above or returned from a
/// fallible FFI function and not yet destroyed). After this call the pointer
/// is invalid.
pub(crate) unsafe fn take_error(error: *mut kafka_common_Error_t) -> Option<Error> {
    if error.is_null() {
        return None;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // handle created by `box_error` (its `Box::into_raw` of an `ErrorInner`) and not yet
    // destroyed. The contract declares the pointer invalid after this call and forbids the
    // C callback from freeing it itself, so this `Box::from_raw` is the single, final use
    // of the allocation.
    let inner = unsafe { Box::from_raw(error as *mut ErrorInner) };
    Some(inner.error)
}

// ---------------------------------------------------------------------------
// Error enum
// ---------------------------------------------------------------------------

/// The class of an error, as a named constant a C caller can switch on.
///
/// # Why this exists (DoD #7)
///
/// This type has **no Java counterpart**, and needs none: Java discriminates an
/// error with `instanceof`, and a Rust caller discriminates it by matching the
/// [`Error`] variant. C can do neither — it cannot see enum variants — so its
/// whole budget for classifying an error is the numeric code, a free-text
/// message, and the hierarchy predicates. This is the same reason CLAUDE.md §4
/// gives for exporting the predicates at all, applied to the other half of the
/// problem: the predicates place an error in a *family*, and this enum names
/// the *class*.
///
/// It is needed because Java's `Errors` deliberately carries no code for a
/// class that only the client raises — *"Do not add exceptions that occur only
/// on the client or only on the server here"* (`protocol/Errors.java`) — so all
/// of those classes report `UNKNOWN_SERVER_ERROR` (-1), and to C they are
/// indistinguishable from each other and from a genuine unknown server error.
///
/// # The assignment rule, and the invariant it encodes
///
/// A class takes Java's wire code **iff it owns that code**, that is, iff
/// [`Errors::error`](crate::common::protocol::Errors::error) maps the code back
/// to it. Every other class takes a Rust-local negative.
///
/// The rule states one invariant: **every class a broker can actually report
/// owns its code and keeps it; every class only the client raises gets a
/// negative.** That is exactly the split `Errors.java`'s javadoc describes, so
/// the faithful half of this enum is `Errors` verbatim — values *and* names,
/// which come from
/// [`Errors::enum_name`](crate::common::protocol::Errors::enum_name) and
/// therefore equal Java's constants.
///
/// Four classes resolve to a positive code in Java by **inheritance** without
/// owning it, so under this rule they take negatives:
/// [`Error::ProducerBufferExhausted`] (`BufferExhaustedException extends
/// TimeoutException`, so Java answers 7, owned by [`Error::Timeout`]) and
/// [`Error::Authentication`] / [`Error::Authorization`] /
/// [`Error::SslAuthentication`] (all extend `InvalidConfigurationException`, so
/// Java answers 40, owned by [`Error::InvalidConfiguration`]).
///
/// # Injectivity and ABI
///
/// The 167 values are pairwise distinct — the code alone identifies the class,
/// which is what lets a C caller use it as its sole discriminator. The
/// negatives are ABI once published: a new class **appends at the most-negative
/// end**, because inserting one mid-list would silently renumber every class
/// after it. A table test pins all 29 literally so a renumbering cannot pass.
// Implementation notes for Rust readers, kept out of the generated header.
//
// **Why the negatives live here and not in `common::protocol::Errors`.**
// Putting them there would contradict the javadoc quoted above, break the three
// tests translated from Java's `ErrorsTest`, and let `Errors::for_code` resolve
// a garbled `-2` in an `int16` wire field to a client-side class — the
// generated deserializers do no range validation. Keeping the synthetic half in
// the FFI layer leaves `Errors` and every wire path untouched.
//
// **Why fully-spelled enumerator names.** CLAUDE.md §4 ("Enums") names a C
// enumerator `<prefix>_<VALUE>` — the enum's prefix without the `_e` suffix,
// then the key in constant case. cbindgen could only prefix with the export
// name, which would put the `_e` into every enumerator, so `[enum]
// prefix_with_name = false` in `cbindgen.toml` turns prefixing off for every
// C enum and each enumerator is written out in full in Rust: the Rust
// identifier is the C name, which is also what `check-ffi-translation` checks.
// As a bonus, one `grep kafka_common_Error_WAKEUP` finds this definition
// and every C use of it.
//
// **Why the keys are Java's `Errors` names, not the `Error` variants.** This
// is `Error`'s C enum, so §4 names it `kafka_common_Error_e`, with
// `kafka_common_Error__enum` returning it, as every other C enum is named. §4
// would also take the enumerator keys from the enum's own keys, the `Error`
// variants in constant case. That does not fit here, so the faithful half
// keeps Java's `Errors` constants (`CLUSTER_AUTHORIZATION_FAILED`, not
// `CLUSTER_AUTHORIZATION`; `STALE_CONTROLLER_EPOCH`, not `CONTROLLER_MOVED`):
// the variants do not map one to one onto the classes, because
// `Error::KafkaError` stores an `Errors` and reports the class that owns it,
// and the `Errors` names are the ones a C caller knows from Java and
// librdkafka. The client-only classes have no `Errors` constant and take their
// variant's name (`LOCAL_TIMEOUT`, `PRODUCER_BUFFER_EXHAUSTED`).
//
// `#[repr(C)]` rather than `#[repr(i32)]` so the C typedef names the enum
// itself rather than an integer alias beside it: given an explicit integer repr
// cbindgen must emit an integer typedef to pin the size, and says so at the
// site — "if we need to specify size, then we have no choice but to create a
// typedef, so `config.style` is not respected" (`ir/enumeration.rs`). The ABI
// is identical: a fieldless `#[repr(C)]` enum takes the target C ABI's default
// enum size, which is `int` on every target Rust supports, and a C enum
// spanning -30..=136 is `int` too. Naming the enum in the typedef is what lets
// a C or C++ caller switch on the real type and get the compiler's
// exhaustiveness warning.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_Error_e {
    // Java's `Errors`, at Java's own values. Names come from
    // `Errors::enum_name()`, so each equals Java's constant.
    kafka_common_Error_UNKNOWN_SERVER_ERROR = -1,
    kafka_common_Error_NONE = 0,
    kafka_common_Error_OFFSET_OUT_OF_RANGE = 1,
    kafka_common_Error_CORRUPT_MESSAGE = 2,
    kafka_common_Error_UNKNOWN_TOPIC_OR_PARTITION = 3,
    kafka_common_Error_INVALID_FETCH_SIZE = 4,
    kafka_common_Error_LEADER_NOT_AVAILABLE = 5,
    kafka_common_Error_NOT_LEADER_OR_FOLLOWER = 6,
    kafka_common_Error_REQUEST_TIMED_OUT = 7,
    kafka_common_Error_BROKER_NOT_AVAILABLE = 8,
    kafka_common_Error_REPLICA_NOT_AVAILABLE = 9,
    kafka_common_Error_MESSAGE_TOO_LARGE = 10,
    kafka_common_Error_STALE_CONTROLLER_EPOCH = 11,
    kafka_common_Error_OFFSET_METADATA_TOO_LARGE = 12,
    kafka_common_Error_NETWORK_ERROR = 13,
    kafka_common_Error_COORDINATOR_LOAD_IN_PROGRESS = 14,
    kafka_common_Error_COORDINATOR_NOT_AVAILABLE = 15,
    kafka_common_Error_NOT_COORDINATOR = 16,
    kafka_common_Error_INVALID_TOPIC_ERROR = 17,
    kafka_common_Error_RECORD_LIST_TOO_LARGE = 18,
    kafka_common_Error_NOT_ENOUGH_REPLICAS = 19,
    kafka_common_Error_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    kafka_common_Error_INVALID_REQUIRED_ACKS = 21,
    kafka_common_Error_ILLEGAL_GENERATION = 22,
    kafka_common_Error_INCONSISTENT_GROUP_PROTOCOL = 23,
    kafka_common_Error_INVALID_GROUP_ID = 24,
    kafka_common_Error_UNKNOWN_MEMBER_ID = 25,
    kafka_common_Error_INVALID_SESSION_TIMEOUT = 26,
    kafka_common_Error_REBALANCE_IN_PROGRESS = 27,
    kafka_common_Error_INVALID_COMMIT_OFFSET_SIZE = 28,
    kafka_common_Error_TOPIC_AUTHORIZATION_FAILED = 29,
    kafka_common_Error_GROUP_AUTHORIZATION_FAILED = 30,
    kafka_common_Error_CLUSTER_AUTHORIZATION_FAILED = 31,
    kafka_common_Error_INVALID_TIMESTAMP = 32,
    kafka_common_Error_UNSUPPORTED_SASL_MECHANISM = 33,
    kafka_common_Error_ILLEGAL_SASL_STATE = 34,
    kafka_common_Error_UNSUPPORTED_VERSION = 35,
    kafka_common_Error_TOPIC_ALREADY_EXISTS = 36,
    kafka_common_Error_INVALID_PARTITIONS = 37,
    kafka_common_Error_INVALID_REPLICATION_FACTOR = 38,
    kafka_common_Error_INVALID_REPLICA_ASSIGNMENT = 39,
    kafka_common_Error_INVALID_CONFIG = 40,
    kafka_common_Error_NOT_CONTROLLER = 41,
    kafka_common_Error_INVALID_REQUEST = 42,
    kafka_common_Error_UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,
    kafka_common_Error_POLICY_VIOLATION = 44,
    kafka_common_Error_OUT_OF_ORDER_SEQUENCE_NUMBER = 45,
    kafka_common_Error_DUPLICATE_SEQUENCE_NUMBER = 46,
    kafka_common_Error_INVALID_PRODUCER_EPOCH = 47,
    kafka_common_Error_INVALID_TXN_STATE = 48,
    kafka_common_Error_INVALID_PRODUCER_ID_MAPPING = 49,
    kafka_common_Error_INVALID_TRANSACTION_TIMEOUT = 50,
    kafka_common_Error_CONCURRENT_TRANSACTIONS = 51,
    kafka_common_Error_TRANSACTION_COORDINATOR_FENCED = 52,
    kafka_common_Error_TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53,
    kafka_common_Error_SECURITY_DISABLED = 54,
    kafka_common_Error_OPERATION_NOT_ATTEMPTED = 55,
    kafka_common_Error_KAFKA_STORAGE_ERROR = 56,
    kafka_common_Error_LOG_DIR_NOT_FOUND = 57,
    kafka_common_Error_SASL_AUTHENTICATION_FAILED = 58,
    kafka_common_Error_UNKNOWN_PRODUCER_ID = 59,
    kafka_common_Error_REASSIGNMENT_IN_PROGRESS = 60,
    kafka_common_Error_DELEGATION_TOKEN_AUTH_DISABLED = 61,
    kafka_common_Error_DELEGATION_TOKEN_NOT_FOUND = 62,
    kafka_common_Error_DELEGATION_TOKEN_OWNER_MISMATCH = 63,
    kafka_common_Error_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64,
    kafka_common_Error_DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65,
    kafka_common_Error_DELEGATION_TOKEN_EXPIRED = 66,
    kafka_common_Error_INVALID_PRINCIPAL_TYPE = 67,
    kafka_common_Error_NON_EMPTY_GROUP = 68,
    kafka_common_Error_GROUP_ID_NOT_FOUND = 69,
    kafka_common_Error_FETCH_SESSION_ID_NOT_FOUND = 70,
    kafka_common_Error_INVALID_FETCH_SESSION_EPOCH = 71,
    kafka_common_Error_LISTENER_NOT_FOUND = 72,
    kafka_common_Error_TOPIC_DELETION_DISABLED = 73,
    kafka_common_Error_FENCED_LEADER_EPOCH = 74,
    kafka_common_Error_UNKNOWN_LEADER_EPOCH = 75,
    kafka_common_Error_UNSUPPORTED_COMPRESSION_TYPE = 76,
    kafka_common_Error_STALE_BROKER_EPOCH = 77,
    kafka_common_Error_OFFSET_NOT_AVAILABLE = 78,
    kafka_common_Error_MEMBER_ID_REQUIRED = 79,
    kafka_common_Error_PREFERRED_LEADER_NOT_AVAILABLE = 80,
    kafka_common_Error_GROUP_MAX_SIZE_REACHED = 81,
    kafka_common_Error_FENCED_INSTANCE_ID = 82,
    kafka_common_Error_ELIGIBLE_LEADERS_NOT_AVAILABLE = 83,
    kafka_common_Error_ELECTION_NOT_NEEDED = 84,
    kafka_common_Error_NO_REASSIGNMENT_IN_PROGRESS = 85,
    kafka_common_Error_GROUP_SUBSCRIBED_TO_TOPIC = 86,
    kafka_common_Error_INVALID_RECORD = 87,
    kafka_common_Error_UNSTABLE_OFFSET_COMMIT = 88,
    kafka_common_Error_THROTTLING_QUOTA_EXCEEDED = 89,
    kafka_common_Error_PRODUCER_FENCED = 90,
    kafka_common_Error_RESOURCE_NOT_FOUND = 91,
    kafka_common_Error_DUPLICATE_RESOURCE = 92,
    kafka_common_Error_UNACCEPTABLE_CREDENTIAL = 93,
    kafka_common_Error_INCONSISTENT_VOTER_SET = 94,
    kafka_common_Error_INVALID_UPDATE_VERSION = 95,
    kafka_common_Error_FEATURE_UPDATE_FAILED = 96,
    kafka_common_Error_PRINCIPAL_DESERIALIZATION_FAILURE = 97,
    kafka_common_Error_SNAPSHOT_NOT_FOUND = 98,
    kafka_common_Error_POSITION_OUT_OF_RANGE = 99,
    kafka_common_Error_UNKNOWN_TOPIC_ID = 100,
    kafka_common_Error_DUPLICATE_BROKER_REGISTRATION = 101,
    kafka_common_Error_BROKER_ID_NOT_REGISTERED = 102,
    kafka_common_Error_INCONSISTENT_TOPIC_ID = 103,
    kafka_common_Error_INCONSISTENT_CLUSTER_ID = 104,
    kafka_common_Error_TRANSACTIONAL_ID_NOT_FOUND = 105,
    kafka_common_Error_FETCH_SESSION_TOPIC_ID_ERROR = 106,
    kafka_common_Error_INELIGIBLE_REPLICA = 107,
    kafka_common_Error_NEW_LEADER_ELECTED = 108,
    kafka_common_Error_OFFSET_MOVED_TO_TIERED_STORAGE = 109,
    kafka_common_Error_FENCED_MEMBER_EPOCH = 110,
    kafka_common_Error_UNRELEASED_INSTANCE_ID = 111,
    kafka_common_Error_UNSUPPORTED_ASSIGNOR = 112,
    kafka_common_Error_STALE_MEMBER_EPOCH = 113,
    kafka_common_Error_MISMATCHED_ENDPOINT_TYPE = 114,
    kafka_common_Error_UNSUPPORTED_ENDPOINT_TYPE = 115,
    kafka_common_Error_UNKNOWN_CONTROLLER_ID = 116,
    kafka_common_Error_UNKNOWN_SUBSCRIPTION_ID = 117,
    kafka_common_Error_TELEMETRY_TOO_LARGE = 118,
    kafka_common_Error_INVALID_REGISTRATION = 119,
    kafka_common_Error_TRANSACTION_ABORTABLE = 120,
    kafka_common_Error_INVALID_RECORD_STATE = 121,
    kafka_common_Error_SHARE_SESSION_NOT_FOUND = 122,
    kafka_common_Error_INVALID_SHARE_SESSION_EPOCH = 123,
    kafka_common_Error_FENCED_STATE_EPOCH = 124,
    kafka_common_Error_INVALID_VOTER_KEY = 125,
    kafka_common_Error_DUPLICATE_VOTER = 126,
    kafka_common_Error_VOTER_NOT_FOUND = 127,
    kafka_common_Error_INVALID_REGULAR_EXPRESSION = 128,
    kafka_common_Error_REBOOTSTRAP_REQUIRED = 129,
    kafka_common_Error_STREAMS_INVALID_TOPOLOGY = 130,
    kafka_common_Error_STREAMS_INVALID_TOPOLOGY_EPOCH = 131,
    kafka_common_Error_STREAMS_TOPOLOGY_FENCED = 132,
    kafka_common_Error_SHARE_SESSION_LIMIT_REACHED = 133,
    kafka_common_Error_GROUP_DELETION_FAILED = 134,
    kafka_common_Error_STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED = 135,
    kafka_common_Error_CONTROLLER_ID_NOT_REGISTERED = 136,

    // Classes Java gives no code of their own: Rust-local negatives,
    // grouped by origin. These are ABI — new classes append at the
    // most-negative end, never in the middle.
    // JDK-derived (`Local*`).
    kafka_common_Error_LOCAL_CONCURRENT_MODIFICATION = -2,
    kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT = -3,
    kafka_common_Error_LOCAL_ILLEGAL_STATE = -4,
    kafka_common_Error_LOCAL_TIMEOUT = -5,
    // `common` client-side classes.
    kafka_common_Error_API = -6,
    kafka_common_Error_AUTHENTICATION = -7,
    kafka_common_Error_AUTHORIZER_NOT_READY = -8,
    kafka_common_Error_AUTHORIZATION = -9,
    kafka_common_Error_CONFIG = -10,
    kafka_common_Error_DISCONNECT = -11,
    kafka_common_Error_INTERRUPT = -12,
    kafka_common_Error_INVALID_OFFSET = -13,
    kafka_common_Error_SCHEMA = -14,
    kafka_common_Error_SERIALIZATION = -15,
    kafka_common_Error_SSL_AUTHENTICATION = -16,
    kafka_common_Error_TRANSACTION_ABORTED = -17,
    kafka_common_Error_WAKEUP = -18,
    // Hand-written / consumer classes.
    kafka_common_Error_CONSUMER_COMMIT_FAILED = -19,
    kafka_common_Error_CONSUMER_LOG_TRUNCATION = -20,
    kafka_common_Error_CONSUMER_NO_OFFSET_FOR_PARTITION = -21,
    kafka_common_Error_CONSUMER_OFFSET_OUT_OF_RANGE = -22,
    kafka_common_Error_CONSUMER_RETRIABLE_COMMIT_FAILED = -23,
    kafka_common_Error_CORRELATION_ID_MISMATCH = -24,
    kafka_common_Error_INVALID_RECEIVE = -25,
    kafka_common_Error_QUOTA_VIOLATION = -26,
    kafka_common_Error_RECORD_DESERIALIZATION = -27,
    // Code owned by a superclass (`BufferExhaustedException extends TimeoutException`).
    kafka_common_Error_PRODUCER_BUFFER_EXHAUSTED = -28,
    // `common` client-side class added after the groups above were published
    // (KIP-909); appended here because the negatives are ABI.
    kafka_common_Error_BOOTSTRAP_RESOLUTION = -29,
    // A binding's foreign callback error (no Java class).
    kafka_common_Error_LOCAL_CALLBACK = -30,
}

/// The [`kafka_common_Error_e`] of the class that owns a protocol code.
///
/// This resolves the code through
/// [`Errors::error`] — the very
/// ownership relation [`kafka_common_Error_e`] is defined by — so the two
/// halves stay consistent by construction rather than by a second hand-written
/// table.
///
/// It exists for [`Error::KafkaError`], the one variant whose code is not a
/// constant: it stores an `Errors` rather than standing for a single class.
fn code_owned_by(error: Errors) -> kafka_common_Error_e {
    match error.error() {
        // `Errors::None` is the one code no class owns — Java declares it
        // `NONE(0, null, message -> null)`.
        None => kafka_common_Error_NONE,
        // No code is owned by the bare `KafkaException`, so this arm is
        // unreachable. Spelling it out rather than letting it fall into the
        // recursive arm below is what makes that recursion provably one level
        // deep; without it a future `Errors::error()` regression would hang the
        // ownership test instead of failing it. `UNKNOWN_SERVER_ERROR` is the
        // answer a bare `KafkaException` gives anyway (`Error::kafka` builds it
        // with `Errors::UnknownServerError`).
        Some(Error::KafkaError(_)) => kafka_common_Error_UNKNOWN_SERVER_ERROR,
        Some(owner) => error_code_of(&owner),
    }
}

/// The [`kafka_common_Error_e`] for an error, identifying its class.
///
/// This is **not** the same question as
/// [`Error::error`](crate::common::Error::error), and the two deliberately
/// differ. `Error::error` answers Java's *protocol* question — what
/// `Errors.forException` would say — so it walks the superclass chain and can
/// report a code the class does not own, or `UNKNOWN_SERVER_ERROR` for a class
/// Java gives no code at all. This function answers the *class* question for
/// callers that cannot see enum variants, so its value is unique per class.
///
/// They agree for every class that owns its code, and disagree for exactly the
/// four inheritance cases named on [`kafka_common_Error_e`]:
/// [`Error::ProducerBufferExhausted`] (Java: 7), [`Error::Authentication`],
/// [`Error::Authorization`] and [`Error::SslAuthentication`] (Java: 40).
///
/// The match is exhaustive with **no wildcard arm**, and must stay that way:
/// that is what makes adding an [`Error`] variant fail to compile until this
/// enum learns about it. A `_ =>` fallback is precisely how the two would
/// drift.
pub(crate) fn error_code_of(error: &Error) -> kafka_common_Error_e {
    match error {
        // The only non-constant arm: `Error::KafkaError` *stores* an `Errors`,
        // so it reports whatever code that value owns.
        Error::KafkaError(k) => code_owned_by(k.error()),
        Error::LocalIllegalArgument(_) => kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT,
        Error::LocalIllegalState(_) => kafka_common_Error_LOCAL_ILLEGAL_STATE,
        Error::LocalConcurrentModification(_) => kafka_common_Error_LOCAL_CONCURRENT_MODIFICATION,
        Error::LocalTimeout(_) => kafka_common_Error_LOCAL_TIMEOUT,
        Error::LocalCallback(_) => kafka_common_Error_LOCAL_CALLBACK,
        Error::Api(_) => kafka_common_Error_API,
        Error::Authentication(_) => kafka_common_Error_AUTHENTICATION,
        Error::Authorization(_) => kafka_common_Error_AUTHORIZATION,
        Error::AuthorizerNotReady(_) => kafka_common_Error_AUTHORIZER_NOT_READY,
        Error::BootstrapResolution(_) => kafka_common_Error_BOOTSTRAP_RESOLUTION,
        Error::BrokerIdNotRegistered(_) => kafka_common_Error_BROKER_ID_NOT_REGISTERED,
        Error::BrokerNotAvailable(_) => kafka_common_Error_BROKER_NOT_AVAILABLE,
        Error::ProducerBufferExhausted(_) => kafka_common_Error_PRODUCER_BUFFER_EXHAUSTED,
        Error::ClusterAuthorization(_) => kafka_common_Error_CLUSTER_AUTHORIZATION_FAILED,
        Error::ConcurrentTransactions(_) => kafka_common_Error_CONCURRENT_TRANSACTIONS,
        Error::ControllerIdNotRegistered(_) => kafka_common_Error_CONTROLLER_ID_NOT_REGISTERED,
        Error::ControllerMoved(_) => kafka_common_Error_STALE_CONTROLLER_EPOCH,
        Error::CoordinatorLoadInProgress(_) => kafka_common_Error_COORDINATOR_LOAD_IN_PROGRESS,
        Error::CoordinatorNotAvailable(_) => kafka_common_Error_COORDINATOR_NOT_AVAILABLE,
        Error::CorrelationIdMismatch(_) => kafka_common_Error_CORRELATION_ID_MISMATCH,
        Error::CorruptRecord(_) => kafka_common_Error_CORRUPT_MESSAGE,
        Error::DelegationTokenAuthorization(_) => kafka_common_Error_DELEGATION_TOKEN_AUTHORIZATION_FAILED,
        Error::DelegationTokenDisabled(_) => kafka_common_Error_DELEGATION_TOKEN_AUTH_DISABLED,
        Error::DelegationTokenExpired(_) => kafka_common_Error_DELEGATION_TOKEN_EXPIRED,
        Error::DelegationTokenNotFound(_) => kafka_common_Error_DELEGATION_TOKEN_NOT_FOUND,
        Error::DelegationTokenOwnerMismatch(_) => kafka_common_Error_DELEGATION_TOKEN_OWNER_MISMATCH,
        Error::Disconnect(_) => kafka_common_Error_DISCONNECT,
        Error::DuplicateBrokerRegistration(_) => kafka_common_Error_DUPLICATE_BROKER_REGISTRATION,
        Error::DuplicateResource(_) => kafka_common_Error_DUPLICATE_RESOURCE,
        Error::DuplicateSequence(_) => kafka_common_Error_DUPLICATE_SEQUENCE_NUMBER,
        Error::DuplicateVoter(_) => kafka_common_Error_DUPLICATE_VOTER,
        Error::ElectionNotNeeded(_) => kafka_common_Error_ELECTION_NOT_NEEDED,
        Error::EligibleLeadersNotAvailable(_) => kafka_common_Error_ELIGIBLE_LEADERS_NOT_AVAILABLE,
        Error::FeatureUpdateFailed(_) => kafka_common_Error_FEATURE_UPDATE_FAILED,
        Error::FencedInstanceId(_) => kafka_common_Error_FENCED_INSTANCE_ID,
        Error::FencedLeaderEpoch(_) => kafka_common_Error_FENCED_LEADER_EPOCH,
        Error::FencedMemberEpoch(_) => kafka_common_Error_FENCED_MEMBER_EPOCH,
        Error::FencedStateEpoch(_) => kafka_common_Error_FENCED_STATE_EPOCH,
        Error::FetchSessionIdNotFound(_) => kafka_common_Error_FETCH_SESSION_ID_NOT_FOUND,
        Error::FetchSessionTopicId(_) => kafka_common_Error_FETCH_SESSION_TOPIC_ID_ERROR,
        Error::GroupAuthorization(_) => kafka_common_Error_GROUP_AUTHORIZATION_FAILED,
        Error::GroupDeletionFailed(_) => kafka_common_Error_GROUP_DELETION_FAILED,
        Error::GroupIdNotFound(_) => kafka_common_Error_GROUP_ID_NOT_FOUND,
        Error::GroupMaxSizeReached(_) => kafka_common_Error_GROUP_MAX_SIZE_REACHED,
        Error::GroupNotEmpty(_) => kafka_common_Error_NON_EMPTY_GROUP,
        Error::GroupSubscribedToTopic(_) => kafka_common_Error_GROUP_SUBSCRIBED_TO_TOPIC,
        Error::IllegalGeneration(_) => kafka_common_Error_ILLEGAL_GENERATION,
        Error::IllegalSaslState(_) => kafka_common_Error_ILLEGAL_SASL_STATE,
        Error::InconsistentClusterId(_) => kafka_common_Error_INCONSISTENT_CLUSTER_ID,
        Error::InconsistentGroupProtocol(_) => kafka_common_Error_INCONSISTENT_GROUP_PROTOCOL,
        Error::InconsistentTopicId(_) => kafka_common_Error_INCONSISTENT_TOPIC_ID,
        Error::InconsistentVoterSet(_) => kafka_common_Error_INCONSISTENT_VOTER_SET,
        Error::IneligibleReplica(_) => kafka_common_Error_INELIGIBLE_REPLICA,
        Error::Interrupt(_) => kafka_common_Error_INTERRUPT,
        Error::InvalidCommitOffsetSize(_) => kafka_common_Error_INVALID_COMMIT_OFFSET_SIZE,
        Error::InvalidConfiguration(_) => kafka_common_Error_INVALID_CONFIG,
        Error::InvalidFetchSessionEpoch(_) => kafka_common_Error_INVALID_FETCH_SESSION_EPOCH,
        Error::InvalidFetchSize(_) => kafka_common_Error_INVALID_FETCH_SIZE,
        Error::InvalidGroupId(_) => kafka_common_Error_INVALID_GROUP_ID,
        Error::InvalidOffset(_) => kafka_common_Error_INVALID_OFFSET,
        Error::InvalidPartitions(_) => kafka_common_Error_INVALID_PARTITIONS,
        Error::InvalidPidMapping(_) => kafka_common_Error_INVALID_PRODUCER_ID_MAPPING,
        Error::InvalidPrincipalType(_) => kafka_common_Error_INVALID_PRINCIPAL_TYPE,
        Error::InvalidProducerEpoch(_) => kafka_common_Error_INVALID_PRODUCER_EPOCH,
        Error::InvalidRecord(_) => kafka_common_Error_INVALID_RECORD,
        Error::InvalidRecordState(_) => kafka_common_Error_INVALID_RECORD_STATE,
        Error::InvalidRegistration(_) => kafka_common_Error_INVALID_REGISTRATION,
        Error::InvalidRegularExpression(_) => kafka_common_Error_INVALID_REGULAR_EXPRESSION,
        Error::InvalidReplicaAssignment(_) => kafka_common_Error_INVALID_REPLICA_ASSIGNMENT,
        Error::InvalidReplicationFactor(_) => kafka_common_Error_INVALID_REPLICATION_FACTOR,
        Error::InvalidRequest(_) => kafka_common_Error_INVALID_REQUEST,
        Error::InvalidRequiredAcks(_) => kafka_common_Error_INVALID_REQUIRED_ACKS,
        Error::InvalidSessionTimeout(_) => kafka_common_Error_INVALID_SESSION_TIMEOUT,
        Error::InvalidShareSessionEpoch(_) => kafka_common_Error_INVALID_SHARE_SESSION_EPOCH,
        Error::InvalidTimestamp(_) => kafka_common_Error_INVALID_TIMESTAMP,
        Error::InvalidTopic(_) => kafka_common_Error_INVALID_TOPIC_ERROR,
        Error::InvalidTxnState(_) => kafka_common_Error_INVALID_TXN_STATE,
        Error::InvalidTxnTimeout(_) => kafka_common_Error_INVALID_TRANSACTION_TIMEOUT,
        Error::InvalidUpdateVersion(_) => kafka_common_Error_INVALID_UPDATE_VERSION,
        Error::InvalidVoterKey(_) => kafka_common_Error_INVALID_VOTER_KEY,
        Error::KafkaStorage(_) => kafka_common_Error_KAFKA_STORAGE_ERROR,
        Error::LeaderNotAvailable(_) => kafka_common_Error_LEADER_NOT_AVAILABLE,
        Error::ListenerNotFound(_) => kafka_common_Error_LISTENER_NOT_FOUND,
        Error::LogDirNotFound(_) => kafka_common_Error_LOG_DIR_NOT_FOUND,
        Error::MemberIdRequired(_) => kafka_common_Error_MEMBER_ID_REQUIRED,
        Error::MismatchedEndpointType(_) => kafka_common_Error_MISMATCHED_ENDPOINT_TYPE,
        Error::Network(_) => kafka_common_Error_NETWORK_ERROR,
        Error::NewLeaderElected(_) => kafka_common_Error_NEW_LEADER_ELECTED,
        Error::NoReassignmentInProgress(_) => kafka_common_Error_NO_REASSIGNMENT_IN_PROGRESS,
        Error::NotController(_) => kafka_common_Error_NOT_CONTROLLER,
        Error::NotCoordinator(_) => kafka_common_Error_NOT_COORDINATOR,
        Error::NotEnoughReplicas(_) => kafka_common_Error_NOT_ENOUGH_REPLICAS,
        Error::NotEnoughReplicasAfterAppend(_) => kafka_common_Error_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        Error::NotLeaderOrFollower(_) => kafka_common_Error_NOT_LEADER_OR_FOLLOWER,
        Error::OffsetMetadataTooLarge(_) => kafka_common_Error_OFFSET_METADATA_TOO_LARGE,
        Error::OffsetMovedToTieredStorage(_) => kafka_common_Error_OFFSET_MOVED_TO_TIERED_STORAGE,
        Error::OffsetNotAvailable(_) => kafka_common_Error_OFFSET_NOT_AVAILABLE,
        Error::OffsetOutOfRange(_) => kafka_common_Error_OFFSET_OUT_OF_RANGE,
        Error::OperationNotAttempted(_) => kafka_common_Error_OPERATION_NOT_ATTEMPTED,
        Error::OutOfOrderSequence(_) => kafka_common_Error_OUT_OF_ORDER_SEQUENCE_NUMBER,
        Error::PolicyViolation(_) => kafka_common_Error_POLICY_VIOLATION,
        Error::PositionOutOfRange(_) => kafka_common_Error_POSITION_OUT_OF_RANGE,
        Error::PreferredLeaderNotAvailable(_) => kafka_common_Error_PREFERRED_LEADER_NOT_AVAILABLE,
        Error::PrincipalDeserialization(_) => kafka_common_Error_PRINCIPAL_DESERIALIZATION_FAILURE,
        Error::ProducerFenced(_) => kafka_common_Error_PRODUCER_FENCED,
        Error::QuotaViolation(_) => kafka_common_Error_QUOTA_VIOLATION,
        Error::ReassignmentInProgress(_) => kafka_common_Error_REASSIGNMENT_IN_PROGRESS,
        Error::RebalanceInProgress(_) => kafka_common_Error_REBALANCE_IN_PROGRESS,
        Error::RebootstrapRequired(_) => kafka_common_Error_REBOOTSTRAP_REQUIRED,
        Error::RecordBatchTooLarge(_) => kafka_common_Error_RECORD_LIST_TOO_LARGE,
        Error::RecordDeserialization(_) => kafka_common_Error_RECORD_DESERIALIZATION,
        Error::RecordTooLarge(_) => kafka_common_Error_MESSAGE_TOO_LARGE,
        Error::InvalidReceive(_) => kafka_common_Error_INVALID_RECEIVE,
        Error::Config(_) => kafka_common_Error_CONFIG,
        Error::ConsumerRetriableCommitFailed(_) => kafka_common_Error_CONSUMER_RETRIABLE_COMMIT_FAILED,
        Error::ConsumerCommitFailed(_) => kafka_common_Error_CONSUMER_COMMIT_FAILED,
        Error::ConsumerNoOffsetForPartition(_) => kafka_common_Error_CONSUMER_NO_OFFSET_FOR_PARTITION,
        Error::ConsumerOffsetOutOfRange(_) => kafka_common_Error_CONSUMER_OFFSET_OUT_OF_RANGE,
        Error::ConsumerLogTruncation(_) => kafka_common_Error_CONSUMER_LOG_TRUNCATION,
        Error::ReplicaNotAvailable(_) => kafka_common_Error_REPLICA_NOT_AVAILABLE,
        Error::ResourceNotFound(_) => kafka_common_Error_RESOURCE_NOT_FOUND,
        Error::SaslAuthentication(_) => kafka_common_Error_SASL_AUTHENTICATION_FAILED,
        Error::Schema(_) => kafka_common_Error_SCHEMA,
        Error::SecurityDisabled(_) => kafka_common_Error_SECURITY_DISABLED,
        Error::Serialization(_) => kafka_common_Error_SERIALIZATION,
        Error::ShareSessionLimitReached(_) => kafka_common_Error_SHARE_SESSION_LIMIT_REACHED,
        Error::ShareSessionNotFound(_) => kafka_common_Error_SHARE_SESSION_NOT_FOUND,
        Error::SnapshotNotFound(_) => kafka_common_Error_SNAPSHOT_NOT_FOUND,
        Error::SslAuthentication(_) => kafka_common_Error_SSL_AUTHENTICATION,
        Error::StaleBrokerEpoch(_) => kafka_common_Error_STALE_BROKER_EPOCH,
        Error::StaleMemberEpoch(_) => kafka_common_Error_STALE_MEMBER_EPOCH,
        Error::StreamsInvalidTopology(_) => kafka_common_Error_STREAMS_INVALID_TOPOLOGY,
        Error::StreamsInvalidTopologyEpoch(_) => kafka_common_Error_STREAMS_INVALID_TOPOLOGY_EPOCH,
        Error::StreamsTopologyDescriptionUpdateFailed(_) => {
            kafka_common_Error_STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED
        },
        Error::StreamsTopologyFenced(_) => kafka_common_Error_STREAMS_TOPOLOGY_FENCED,
        Error::TelemetryTooLarge(_) => kafka_common_Error_TELEMETRY_TOO_LARGE,
        Error::ThrottlingQuotaExceeded(_) => kafka_common_Error_THROTTLING_QUOTA_EXCEEDED,
        Error::Timeout(_) => kafka_common_Error_REQUEST_TIMED_OUT,
        Error::TopicAuthorization(_) => kafka_common_Error_TOPIC_AUTHORIZATION_FAILED,
        Error::TopicDeletionDisabled(_) => kafka_common_Error_TOPIC_DELETION_DISABLED,
        Error::TopicExists(_) => kafka_common_Error_TOPIC_ALREADY_EXISTS,
        Error::TransactionAbortable(_) => kafka_common_Error_TRANSACTION_ABORTABLE,
        Error::TransactionAborted(_) => kafka_common_Error_TRANSACTION_ABORTED,
        Error::TransactionCoordinatorFenced(_) => kafka_common_Error_TRANSACTION_COORDINATOR_FENCED,
        Error::TransactionalIdAuthorization(_) => kafka_common_Error_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        Error::TransactionalIdNotFound(_) => kafka_common_Error_TRANSACTIONAL_ID_NOT_FOUND,
        Error::UnacceptableCredential(_) => kafka_common_Error_UNACCEPTABLE_CREDENTIAL,
        Error::UnknownControllerId(_) => kafka_common_Error_UNKNOWN_CONTROLLER_ID,
        Error::UnknownLeaderEpoch(_) => kafka_common_Error_UNKNOWN_LEADER_EPOCH,
        Error::UnknownMemberId(_) => kafka_common_Error_UNKNOWN_MEMBER_ID,
        Error::UnknownProducerId(_) => kafka_common_Error_UNKNOWN_PRODUCER_ID,
        Error::UnknownServer(_) => kafka_common_Error_UNKNOWN_SERVER_ERROR,
        Error::UnknownSubscriptionId(_) => kafka_common_Error_UNKNOWN_SUBSCRIPTION_ID,
        Error::UnknownTopicId(_) => kafka_common_Error_UNKNOWN_TOPIC_ID,
        Error::UnknownTopicOrPartition(_) => kafka_common_Error_UNKNOWN_TOPIC_OR_PARTITION,
        Error::UnreleasedInstanceId(_) => kafka_common_Error_UNRELEASED_INSTANCE_ID,
        Error::UnstableOffsetCommit(_) => kafka_common_Error_UNSTABLE_OFFSET_COMMIT,
        Error::UnsupportedAssignor(_) => kafka_common_Error_UNSUPPORTED_ASSIGNOR,
        Error::UnsupportedByAuthentication(_) => kafka_common_Error_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        Error::UnsupportedCompressionType(_) => kafka_common_Error_UNSUPPORTED_COMPRESSION_TYPE,
        Error::UnsupportedEndpointType(_) => kafka_common_Error_UNSUPPORTED_ENDPOINT_TYPE,
        Error::UnsupportedForMessageFormat(_) => kafka_common_Error_UNSUPPORTED_FOR_MESSAGE_FORMAT,
        Error::UnsupportedSaslMechanism(_) => kafka_common_Error_UNSUPPORTED_SASL_MECHANISM,
        Error::UnsupportedVersion(_) => kafka_common_Error_UNSUPPORTED_VERSION,
        Error::VoterNotFound(_) => kafka_common_Error_VOTER_NOT_FOUND,
        Error::Wakeup(_) => kafka_common_Error_WAKEUP,
    }
}

/// Returns the [`kafka_common_Error_e`] enumerator of a [`kafka_common_Error_t`]
/// handle, as `__enum` does for every C enum (CLAUDE.md §4 "Enums").
///
/// The value identifies the error's **class**, not merely its protocol code:
/// the values are pairwise distinct, so a `switch` on this value is enough to
/// tell any two errors apart. See [`kafka_common_Error_e`] for the
/// assignment rule and for the four classes whose value deliberately differs
/// from the code Java's `Errors.forException` would report.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// The error's [`kafka_common_Error_e`], or
/// `kafka_common_Error_NONE` (`0`) if the error handle is null. The
/// enumerators have explicit values that fit an `int`, so a C caller that was
/// comparing the previous `int32_t` return against integers keeps compiling
/// and keeps getting the same answers for every broker-reported code.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error__enum(error: *const kafka_common_Error_t) -> kafka_common_Error_e {
    if error.is_null() {
        return kafka_common_Error_NONE;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The `&'static ErrorInner` is used only to read `.error` for the duration of this
    // call, during which the C caller keeps the handle alive.
    error_code_of(&unsafe { error_ref(error) }.error)
}

/// Returns the error message as a null-terminated C string.
///
/// The returned pointer is valid as long as `error` is: until
/// [`kafka_common_Error_destroy`] is called on it or, for a borrowed handle
/// (from [`kafka_common_Error_source`] or a payload or struct-field accessor),
/// until its owner is destroyed by that owner's own `_destroy`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_message(error: *const kafka_common_Error_t) -> *const c_char {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The returned `message_cstring.as_ptr()` points into that same `ErrorInner`, so, as
    // this function's rustdoc states, it stays valid while the handle does: until
    // `kafka_common_Error_destroy` is called on it, or, for a borrowed one, until its owner
    // is destroyed by that owner's own `_destroy`.
    unsafe { error_ref(error) }.message_cstring.as_ptr()
}

/// Returns the error's cause (`Throwable.getCause()`), or null when it has
/// none.
///
/// The returned handle is *borrowed*: it is owned by `error` and valid until
/// [`kafka_common_Error_destroy`] is called on `error`. It must not be passed
/// to `kafka_common_Error_destroy` itself. Calling this on the returned handle
/// walks one more step down the chain of causes.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_source(error: *const kafka_common_Error_t) -> *const kafka_common_Error_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The returned cause is a borrowed handle owned by that `ErrorInner` (its `source`
    // slot), so, as this function's rustdoc states, it stays valid while `error` does.
    unsafe { error_ref(error) }.source_ptr()
}

/// Returns the throttle time the broker reported with this error
/// (`ThrottlingQuotaExceededException.throttleTimeMs()`), or `-1` when the
/// error carries none.
///
/// A caught Rust panic returns -1, as for no throttle time: Java's
/// `ThrottlingQuotaExceededException(int, String)` stores any `int`, and the C
/// constructors pass it through, so no value is outside its domain.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_throttle_time_ms(error: *const kafka_common_Error_t) -> i32 {
    if error.is_null() {
        return -1;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    unsafe { error_ref(error) }.error.throttle_time_ms().unwrap_or(-1)
}

// NOTE: fatality (`RequestUtils.isFatalException`) is deliberately NOT exported
// here. `org.apache.kafka.common.requests` carries the package disclaimer "This
// package is not a supported Kafka API; the implementation may change without
// warning between minor or patch releases", and CLAUDE.md §4 forbids C bindings
// for such packages. A C caller that needs the classification composes it from
// the exported predicates (`kafka_common_Error_is_authentication_error`,
// `kafka_common_Error_is_authorization_error`) and the error code.

/// Destroys an error handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `error` must be null or a valid handle from a function that returned an error.
///   It must be an owned handle: a borrowed one ([`kafka_common_Error_source`], a
///   payload's or struct field's error accessor) belongs to its owner and is
///   never passed here.
/// - After this call, the pointer is invalid and must not be used.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_destroy(error: *mut kafka_common_Error_t) {
    if !error.is_null() {
        // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`,
        // an owned handle from a function that returned an error, i.e. the `ErrorInner` that
        // `box_error` leaked with `Box::into_raw` (a borrowed handle, which points into its
        // owner, is never passed here). The contract declares the pointer invalid after this
        // call, and `take_error`'s contract forbids destroying a handle it already
        // consumed, so this `Box::from_raw` is the single, final use of the allocation.
        unsafe {
            drop(Box::from_raw(error as *mut ErrorInner));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::KafkaError;
    use crate::common::error::ErrorName;
    use crate::common::errors::{
        ApiError, AuthenticationError, AuthorizationError, AuthorizerNotReadyError, BootstrapResolutionError,
        DisconnectError, InterruptError, InvalidOffsetError, SslAuthenticationError,
    };
    use crate::common::metrics::QuotaViolationError;
    use crate::common::network::InvalidReceiveError;
    use crate::common::{MetricName, TopicPartition};
    use crate::consumer::{
        ConsumerCommitFailedError, ConsumerLogTruncationError, ConsumerNoOffsetForPartitionError,
        ConsumerOffsetOutOfRangeError, ConsumerRetriableCommitFailedError,
    };
    use crate::ffi::common::isolation_level::kafka_common_IsolationLevel_e;
    use crate::ffi::util::kafka_Bytes_t;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::ffi::CStr;

    /// Java's lowest and highest `Errors` codes — the full range `Errors::for_code`
    /// resolves to a named constant.
    const FIRST_CODE: i16 = -1;
    const LAST_CODE: i16 = Errors::MAX_CODE;

    /// The 29 classes that own no Java code, each paired with the enumerator it
    /// must map to and that enumerator's literal value.
    ///
    /// Both halves matter. The instance checks that `error_code_of`'s arm points
    /// at the right constant; the literal checks that the constant still has the
    /// value it was published with. See [`kafka_common_Error_e`] — the
    /// negatives are ABI, so a renumbering must fail here rather than silently
    /// reach a C caller.
    fn client_side_classes() -> Vec<(Error, kafka_common_Error_e, i32)> {
        vec![
            // -2 ..= -5: JDK-derived (`Local*`).
            (
                Error::local_concurrent_modification("m"),
                kafka_common_Error_LOCAL_CONCURRENT_MODIFICATION,
                -2,
            ),
            (
                Error::local_illegal_argument("m"),
                kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT,
                -3,
            ),
            (Error::local_illegal_state("m"), kafka_common_Error_LOCAL_ILLEGAL_STATE, -4),
            (Error::local_timeout("m"), kafka_common_Error_LOCAL_TIMEOUT, -5),
            // -6 ..= -18: `common` client-side classes.
            (Error::Api(ApiError::new("m")), kafka_common_Error_API, -6),
            (
                Error::Authentication(AuthenticationError::new("m")),
                kafka_common_Error_AUTHENTICATION,
                -7,
            ),
            (
                Error::AuthorizerNotReady(AuthorizerNotReadyError::new("m")),
                kafka_common_Error_AUTHORIZER_NOT_READY,
                -8,
            ),
            (
                Error::Authorization(AuthorizationError::new("m")),
                kafka_common_Error_AUTHORIZATION,
                -9,
            ),
            (Error::config_message("m"), kafka_common_Error_CONFIG, -10),
            (Error::Disconnect(DisconnectError::new("m")), kafka_common_Error_DISCONNECT, -11),
            (Error::Interrupt(InterruptError::new("m")), kafka_common_Error_INTERRUPT, -12),
            (
                Error::InvalidOffset(InvalidOffsetError::new("m")),
                kafka_common_Error_INVALID_OFFSET,
                -13,
            ),
            (Error::schema("m"), kafka_common_Error_SCHEMA, -14),
            (Error::serialization("m"), kafka_common_Error_SERIALIZATION, -15),
            (
                Error::SslAuthentication(SslAuthenticationError::new("m")),
                kafka_common_Error_SSL_AUTHENTICATION,
                -16,
            ),
            (Error::transaction_aborted(), kafka_common_Error_TRANSACTION_ABORTED, -17),
            (Error::wakeup("m"), kafka_common_Error_WAKEUP, -18),
            // -19 ..= -27: hand-written / consumer classes.
            (
                Error::ConsumerCommitFailed(ConsumerCommitFailedError::with_default_message()),
                kafka_common_Error_CONSUMER_COMMIT_FAILED,
                -19,
            ),
            (
                Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(HashMap::new(), HashMap::new()))),
                kafka_common_Error_CONSUMER_LOG_TRUNCATION,
                -20,
            ),
            (
                Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(TopicPartition::new(
                    "t", 0,
                ))),
                kafka_common_Error_CONSUMER_NO_OFFSET_FOR_PARTITION,
                -21,
            ),
            (
                Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(HashMap::new())),
                kafka_common_Error_CONSUMER_OFFSET_OUT_OF_RANGE,
                -22,
            ),
            (
                Error::ConsumerRetriableCommitFailed(ConsumerRetriableCommitFailedError::with_default_message()),
                kafka_common_Error_CONSUMER_RETRIABLE_COMMIT_FAILED,
                -23,
            ),
            (
                Error::correlation_id_mismatch("m", 1, 2),
                kafka_common_Error_CORRELATION_ID_MISMATCH,
                -24,
            ),
            (
                Error::InvalidReceive(InvalidReceiveError::new("m")),
                kafka_common_Error_INVALID_RECEIVE,
                -25,
            ),
            (
                Error::QuotaViolation(Box::new(QuotaViolationError::new(
                    MetricName::new("n", "g", "d", BTreeMap::new()),
                    1.0,
                    0.5,
                ))),
                kafka_common_Error_QUOTA_VIOLATION,
                -26,
            ),
            (
                Error::RecordDeserialization(Box::new(record_deserialization_error())),
                kafka_common_Error_RECORD_DESERIALIZATION,
                -27,
            ),
            // -28: code owned by a superclass.
            (Error::buffer_exhausted("m"), kafka_common_Error_PRODUCER_BUFFER_EXHAUSTED, -28),
            // -29: `common` client-side class appended after publication.
            (
                Error::BootstrapResolution(BootstrapResolutionError::new("m")),
                kafka_common_Error_BOOTSTRAP_RESOLUTION,
                -29,
            ),
            // -30: a binding's foreign callback error.
            (
                Error::local_callback("m", std::ptr::null_mut()),
                kafka_common_Error_LOCAL_CALLBACK,
                -30,
            ),
        ]
    }

    /// `catch_task_panic` passes a task's own result through and turns a
    /// panic, of either string payload, into the entry point's panic error.
    #[tokio::test]
    async fn catch_task_panic_turns_a_task_panic_into_its_error() {
        assert_eq!(catch_task_panic("f", async { Ok::<_, Error>(7) }).await.unwrap(), 7);
        let own = catch_task_panic("f", async { Err::<(), _>(Error::local_illegal_argument("own")) })
            .await
            .unwrap_err();
        assert!(own.is_local_illegal_argument_error());
        assert_eq!(own.message(), "own");

        let literal = catch_task_panic("kafka_x_cb", async {
            if std::hint::black_box(true) {
                panic!("literal");
            }
            Ok::<(), Error>(())
        })
        .await
        .unwrap_err();
        assert!(literal.is_local_illegal_state_error());
        assert_eq!(
            literal.message(),
            "Rust panic caught at the FFI boundary in kafka_x_cb: literal"
        );

        let formatted = catch_task_panic("kafka_y_cb", async {
            let n = 2;
            if std::hint::black_box(true) {
                panic!("formatted {n}");
            }
            Ok::<(), Error>(())
        })
        .await
        .unwrap_err();
        assert_eq!(
            formatted.message(),
            "Rust panic caught at the FFI boundary in kafka_y_cb: formatted 2"
        );
    }

    /// Every code whose `Errors::error()` names a class reports that class's own
    /// value, and the value equals `Errors::code()`.
    ///
    /// This machine-checks the faithful half of [`kafka_common_Error_e`]
    /// against `Errors` itself, so the two cannot drift: a code renamed, renumbered
    /// or re-pointed at a different class in `Errors` fails here.
    #[test]
    fn ffi_error_code_owned_codes_match_the_java_values() {
        let mut owned = 0;
        for code in FIRST_CODE..=LAST_CODE {
            let error = Errors::for_code(code);
            assert_eq!(error.code(), code, "Errors::for_code({code}) resolved to a different code");

            match error.error() {
                // `Errors::None` is Java's `NONE(0, null, message -> null)` — the
                // one code no class owns.
                None => {
                    assert_eq!(code, 0, "only NONE may map to no class, but {code} does");
                    assert_eq!(kafka_common_Error_NONE as i32, 0);
                },
                Some(owner) => {
                    owned += 1;
                    assert_eq!(
                        error_code_of(&owner) as i32,
                        i32::from(code),
                        "{} does not report its own code",
                        error.enum_name()
                    );
                },
            }
        }
        assert_eq!(owned, 137, "expected 137 classes to own a Java code");

        // `Error::KafkaError` is the one variant whose code is not a constant: it
        // stores an `Errors`, so it reports whatever that value owns.
        assert_eq!(
            error_code_of(&Error::KafkaError(KafkaError::new(Errors::CorruptMessage))),
            kafka_common_Error_CORRUPT_MESSAGE
        );
        assert_eq!(
            error_code_of(&Error::KafkaError(KafkaError::new(Errors::None))),
            kafka_common_Error_NONE
        );
        // A bare `KafkaException` reports -1, which is what Java's
        // `Errors.forException` answers for it.
        assert_eq!(
            error_code_of(&Error::kafka_message("m")),
            kafka_common_Error_UNKNOWN_SERVER_ERROR
        );
    }

    /// The 167 values are pairwise distinct, so the code alone identifies the
    /// class. A C caller has no other discriminator, and the gRPC test harness
    /// derives the error type from this value — a collision would silently
    /// misclassify one of the two classes involved.
    #[test]
    fn ffi_error_code_values_are_injective() {
        let mut seen: HashMap<i32, String> = HashMap::new();

        // `NONE` belongs to no class but occupies 0 in the space.
        seen.insert(kafka_common_Error_NONE as i32, "NONE".to_string());

        for code in FIRST_CODE..=LAST_CODE {
            let error = Errors::for_code(code);
            if let Some(owner) = error.error() {
                let value = error_code_of(&owner) as i32;
                if let Some(previous) = seen.insert(value, error.enum_name().to_string()) {
                    panic!("{value} is reported by both {previous} and {}", error.enum_name());
                }
            }
        }

        for (error, expected, _) in client_side_classes() {
            let value = error_code_of(&error) as i32;
            let name = format!("{expected:?}");
            if let Some(previous) = seen.insert(value, name.clone()) {
                panic!("{value} is reported by both {previous} and {name}");
            }
        }

        assert_eq!(seen.len(), 167, "expected 167 distinct error codes");
    }

    /// One instance of each of the 167 error classes: the owner of every Java code,
    /// the client-side classes, and the bare `KafkaException`, which reports
    /// `UNKNOWN_SERVER_ERROR` rather than a code of its own. The codes are pairwise
    /// distinct ([`ffi_error_code_values_are_injective`]), so no class appears twice.
    fn one_error_per_class() -> Vec<Error> {
        let coded = (FIRST_CODE..=LAST_CODE).filter_map(|code| Errors::for_code(code).error());
        let local = client_side_classes().into_iter().map(|(error, _, _)| error);
        coded.chain(local).chain([Error::kafka_message("m")]).collect()
    }

    /// `is_` + `payload` snake_cased with a trailing `Error` dropped + `_error`, the
    /// class-predicate name `cargo xtask lint-custom` (check-error-predicate)
    /// enforces.
    fn class_predicate(payload: &str) -> String {
        let base = payload.strip_suffix("Error").filter(|b| !b.is_empty()).unwrap_or(payload);
        let mut snake = String::new();
        for (i, c) in base.char_indices() {
            if c.is_ascii_uppercase() && i > 0 {
                snake.push('_');
            }
            snake.push(c.to_ascii_lowercase());
        }
        format!("is_{snake}_error")
    }

    /// The predicates for Java's intermediate (non-leaf) classes. Their exact
    /// coverage is asserted in both directions by the hierarchy tests in
    /// `common::error` (CLAUDE.md §12.4), so here they are only checked to answer
    /// `true` for their own class.
    const INTERMEDIATE_PREDICATES: [&str; 15] = [
        "is_kafka_error",
        "is_api_error",
        "is_retriable_error",
        "is_refresh_retriable_error",
        "is_invalid_metadata_error",
        "is_authentication_error",
        "is_authorization_error",
        "is_invalid_configuration_error",
        "is_application_recoverable_error",
        "is_invalid_offset_error",
        "is_out_of_order_sequence_error",
        "is_serialization_error",
        "is_timeout_error",
        "is_consumer_invalid_offset_error",
        "is_consumer_offset_out_of_range_error",
    ];

    /// The leaf classes that nonetheless have a Java subclass, as
    /// `(class predicate, subclass's own predicate)`. Every other leaf predicate is
    /// `true` for its own class only.
    const LEAF_SUBCLASSES: [(&str, &str); 1] = [
        // `CorrelationIdMismatchException extends IllegalStateException`
        // (`CorrelationIdMismatchException.java:23`).
        ("is_local_illegal_state_error", "is_correlation_id_mismatch_error"),
    ];

    /// Every error class has its own C predicate, `kafka_common_Error_<is_x_error>`,
    /// and each export answers like Java's `instanceof <class>`: `true` for the class
    /// itself and its subclasses, `false` for every other class (CLAUDE.md §3).
    ///
    /// Exercises the generated exports themselves, through the handle a C caller
    /// holds, against one instance of every class.
    #[test]
    fn ffi_every_class_has_its_own_predicate() {
        let predicates: HashMap<&str, _> = super::super::error_predicates::PREDICATES.iter().copied().collect();
        let errors: Vec<(String, *mut kafka_common_Error_t)> = one_error_per_class()
            .into_iter()
            .map(|error| (class_predicate(ErrorName::name(&error)), box_error(error)))
            .collect();
        assert_eq!(errors.len(), 167, "expected one error per class");

        let owners: HashSet<&str> = errors.iter().map(|(own, _)| own.as_str()).collect();
        assert_eq!(owners.len(), errors.len(), "two classes share a predicate name");

        for (own, handle) in &errors {
            let export = predicates
                .get(own.as_str())
                .unwrap_or_else(|| panic!("no C export for `{own}`"));
            // SAFETY: Test code: `handle` is a live `box_error` handle from `errors`, destroyed
            // only by the final loop below.
            assert_eq!(unsafe { export(*handle) }, 1, "`{own}` is false for its own class");
        }

        // Every export is either an intermediate predicate or some class's own.
        for name in predicates.keys() {
            assert!(
                owners.contains(name) || INTERMEDIATE_PREDICATES.contains(name),
                "`{name}` belongs to no error class"
            );
        }

        for (name, export) in &predicates {
            if INTERMEDIATE_PREDICATES.contains(name) {
                continue;
            }
            for (own, handle) in &errors {
                let expected = own == name || LEAF_SUBCLASSES.contains(&(*name, own.as_str()));
                assert_eq!(
                    // SAFETY: Test code: `handle` is a live `box_error` handle from `errors`,
                    // destroyed only by the final loop below.
                    unsafe { export(*handle) },
                    i8::from(expected),
                    "`{name}` on an instance of the class `{own}` names"
                );
            }
            assert_eq!(
                // SAFETY: Test code: every predicate export accepts a null handle.
                unsafe { export(std::ptr::null()) },
                0,
                "`{name}` must be false for a null handle"
            );
        }

        for (_, handle) in errors {
            // SAFETY: Test code: each `handle` is a `box_error` handle from `errors`, consumed
            // by this loop, so it is destroyed exactly once and not used afterwards.
            unsafe { kafka_common_Error_destroy(handle) };
        }
    }

    /// The 28 Rust-local negatives keep the values they were published with, and
    /// each client-side class maps to the enumerator named after it.
    ///
    /// These values are ABI (see [`kafka_common_Error_e`]): a new class
    /// appends at the most-negative end, so inserting one mid-list — which would
    /// renumber everything after it — must fail here.
    #[test]
    fn ffi_error_code_client_side_negatives_are_stable() {
        let classes = client_side_classes();
        assert_eq!(classes.len(), 29, "expected 29 classes with no Java code");

        for (error, expected, value) in classes {
            assert_eq!(expected as i32, value, "{expected:?} moved off its published value");
            assert_eq!(
                error_code_of(&error),
                expected,
                "{} maps to the wrong code",
                ErrorName::name(&error)
            );
        }
    }

    /// A `RecordDeserializationError` with the minimum context its Java
    /// ten-argument constructor requires.
    fn record_deserialization_error() -> crate::common::errors::RecordDeserializationError {
        use crate::common::errors::DeserializationErrorOrigin;
        use crate::common::record::TimestampType;
        crate::common::errors::RecordDeserializationError::new(
            DeserializationErrorOrigin::Value,
            TopicPartition::new("t", 0),
            0,
            0,
            TimestampType::CreateTime,
            None,
            None,
            None,
            "m",
        )
    }
    // -----------------------------------------------------------------------
    // #[ffi_guard] — decisions D2–D5 of
    // design/current/appsec-7665-4521-ffi-panic-guard.md
    // -----------------------------------------------------------------------
    //
    // Each `guarded_*` function is an `extern "C"` function under
    // `#[ffi_guard]` without `no_mangle`. Its body panics when asked to, and the
    // test checks the value the guard returned in place of the unwind that would
    // otherwise abort the process.

    /// Reads a boxed error's message and code, then frees it.
    ///
    /// # Safety
    ///
    /// `error` must be a live error handle returned by an FFI call (null fails the
    /// assertion); it is freed here and must not be used afterwards.
    unsafe fn take_error(error: *mut kafka_common_Error_t) -> (String, kafka_common_Error_e) {
        assert!(!error.is_null(), "expected an error handle");
        // SAFETY: Test helper: `error` is non-null (asserted above) and every caller passes
        // a handle freshly produced by `#[ffi_guard]`'s panic path
        // (`box_error(panic_error(..))`, returned by the guarded call) and
        // not yet destroyed, which satisfies `kafka_common_Error_message`'s `# Safety`. For
        // a non-null handle it returns the NUL-terminated `message_cstring` pointer, valid
        // until destroy, and the text is copied into an owned `String` before the handle is
        // freed below.
        let message = unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }
            .to_str()
            .unwrap()
            .to_owned();
        // SAFETY: Test helper: `error` is non-null (asserted above) and a fresh, not yet
        // destroyed `box_error` handle from `#[ffi_guard]`'s panic path, which satisfies
        // `kafka_common_Error__enum`'s `# Safety`; the call only copies out the code.
        let code = unsafe { kafka_common_Error__enum(error) };
        // SAFETY: Test helper: `error` is non-null (asserted above) and a fresh `box_error`
        // handle from `#[ffi_guard]`'s panic path that no other code frees (callers pass it
        // here exactly once), so this `kafka_common_Error_destroy` is the single, final
        // use, as its `# Safety` requires.
        unsafe { kafka_common_Error_destroy(error) };
        (message, code)
    }

    /// The message [`panic_error`] builds for a panic in `function`.
    fn panic_message(function: &str, text: &str) -> String {
        format!("Rust panic caught at the FFI boundary in {function}: {text}")
    }

    thread_local! {
        /// Set by `guarded_unit` just before it panics.
        static UNIT_BODY_RAN: RefCell<bool> = const { RefCell::new(false) };
        /// What `guarded_on_panic`'s closure received.
        static ON_PANIC_SAW: RefCell<Option<(String, kafka_common_Error_e)>> = const { RefCell::new(None) };
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_unit() {
        UNIT_BODY_RAN.with(|ran| *ran.borrow_mut() = true);
        panic!("unit boom");
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_bool(panic: bool) -> bool {
        if panic {
            panic!("bool boom");
        }
        true
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_i16(panic: bool) -> i16 {
        if panic {
            panic!("i16 boom");
        }
        16
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_i32(panic: bool) -> i32 {
        if panic {
            panic!("i32 boom");
        }
        32
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_i64(panic: bool) -> i64 {
        if panic {
            panic!("i64 boom");
        }
        64
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_topics_count(panic: bool) -> i32 {
        if panic {
            panic!("count boom");
        }
        3
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_offsets_count(panic: bool) -> i64 {
        if panic {
            panic!("count boom");
        }
        3
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_f64(panic: bool) -> f64 {
        if panic {
            panic!("f64 boom");
        }
        1.5
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_error_code(panic: bool) -> kafka_common_Error_e {
        if panic {
            panic!("code boom");
        }
        kafka_common_Error_NONE
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_error_return(panic: bool) -> *mut kafka_common_Error_t {
        if panic {
            panic!("error boom");
        }
        std::ptr::null_mut()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_borrowed_error(panic: bool) -> *const kafka_common_Error_t {
        if panic {
            panic!("borrowed boom");
        }
        std::ptr::null()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_mut_pointer(panic: bool) -> *mut u8 {
        if panic {
            panic!("pointer boom");
        }
        std::ptr::dangling_mut()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_const_pointer(panic: bool) -> *const c_char {
        if panic {
            panic!("string boom");
        }
        c"value".as_ptr()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_i8(panic: bool) -> i8 {
        if panic {
            panic!("i8 boom");
        }
        1
    }

    /// An `int8_t` carrying a Java `byte` code whose 0 is valid, in the shape of
    /// `kafka_common_IsolationLevel_id`.
    ///
    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard(fallback = -1)]
    unsafe extern "C" fn guarded_byte_code(panic: bool) -> i8 {
        if panic {
            panic!("byte code boom");
        }
        0
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_topics_size(panic: bool) -> i32 {
        if panic {
            panic!("size boom");
        }
        4
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_bytes(panic: bool) -> kafka_Bytes_t {
        if panic {
            panic!("bytes boom");
        }
        kafka_Bytes_t::from_slice(b"value")
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard(fallback = kafka_common_IsolationLevel_e::kafka_common_IsolationLevel_READ_UNCOMMITTED)]
    unsafe extern "C" fn guarded_enum(panic: bool) -> kafka_common_IsolationLevel_e {
        if panic {
            panic!("enum boom");
        }
        kafka_common_IsolationLevel_e::kafka_common_IsolationLevel_READ_COMMITTED
    }

    /// A fallible entry point in the CLAUDE.md §4 shape: it returns its value and
    /// stores an error in `*out_err`, here when `fail` asks for one.
    ///
    /// # Safety
    ///
    /// `out_err` must be a valid slot pointing at null.
    #[ffi_guard]
    unsafe extern "C" fn guarded_new(panic: bool, fail: bool, out_err: *mut *mut kafka_common_Error_t) -> *mut u8 {
        if panic {
            panic!("constructor boom");
        }
        if fail {
            // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
            return unsafe { fail_with(out_err, Error::local_illegal_argument("constructor failed")) };
        }
        std::ptr::dangling_mut()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard(fallback = 42)]
    unsafe extern "C" fn guarded_fallback() -> i32 {
        panic!("fallback boom");
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard(on_panic = |err| {
        ON_PANIC_SAW.with(|saw| *saw.borrow_mut() = Some((err.message().to_owned(), error_code_of(&err))));
        7
    })]
    unsafe extern "C" fn guarded_on_panic() -> i32 {
        panic!("custom path");
    }

    /// A callback-style entry point whose spawn panics the way tokio's can after
    /// queueing the task. `on_panic` prints instead of firing a callback, so the
    /// child process of `spawn_panic_aborts_before_the_guard` can show it never ran.
    ///
    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard(on_panic = |err| println!("on_panic fired: {}", err.message()))]
    unsafe extern "C" fn guarded_spawn_panics() {
        println!("spawning");
        spawn_or_abort::<()>(|| panic!("failed to wake I/O driver"));
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_formatted(value: i32) -> i32 {
        panic!("formatted payload {value}");
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_unwrap() -> i32 {
        // `black_box` keeps the `None` opaque, so this is a real runtime panic.
        std::hint::black_box(None::<i32>).unwrap()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_non_string_payload() -> i32 {
        std::panic::panic_any(42_u32)
    }

    /// A panic payload whose destructor panics as well.
    struct PanicsOnDrop;

    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("payload destructor");
        }
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_payload_panics_on_drop() -> i32 {
        std::panic::panic_any(PanicsOnDrop)
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_early_return(early: bool) -> i32 {
        if early {
            return 5;
        }
        6
    }

    #[test]
    fn ffi_guard_unit_return_is_logged_only() {
        // SAFETY: Test code: `guarded_unit` is a parameterless `extern "C"` test function
        // under `#[ffi_guard]`; the call is `unsafe` only because the function is declared
        // `unsafe extern "C"`, and it has no pointer arguments or preconditions to uphold.
        // The guard catches its panic, so nothing unwinds into this frame.
        unsafe { guarded_unit() };
        assert!(UNIT_BODY_RAN.with(|ran| *ran.borrow()), "the body ran up to its panic");
    }

    #[test]
    fn ffi_guard_scalar_fallbacks() {
        // SAFETY: Test code: every `guarded_*` function called here is an `extern "C"` test
        // function under `#[ffi_guard]` taking only a `bool`; the calls are `unsafe` solely
        // because the functions are declared `unsafe extern "C"`, and there are no pointer
        // arguments or preconditions to uphold. The guard turns each requested panic into
        // the documented fallback value.
        unsafe {
            assert!(!guarded_bool(true));
            assert_eq!(guarded_i16(true), -1);
            assert_eq!(guarded_i32(true), -1);
            assert_eq!(guarded_i64(true), -1);
            assert!(guarded_f64(true).is_nan());
        }
    }

    #[test]
    fn ffi_guard_count_fallback_is_zero() {
        // SAFETY: Test code: `guarded_topics_count` and `guarded_offsets_count` are `extern
        // "C"` test functions under `#[ffi_guard]` taking only a `bool`; the calls are
        // `unsafe` solely because the functions are declared `unsafe extern "C"`, with no
        // pointer arguments or preconditions to uphold. The guard turns the requested panic
        // into the documented `0` count fallback.
        unsafe {
            assert_eq!(guarded_topics_count(true), 0);
            assert_eq!(guarded_offsets_count(true), 0);
        }
    }

    #[test]
    fn ffi_guard_error_code_fallback_is_unknown_server_error() {
        // SAFETY: Test code: `guarded_error_code` is an `extern "C"` test function under
        // `#[ffi_guard]` taking only a `bool`; the call is `unsafe` solely because it is
        // declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard turns the requested panic into the documented error-code
        // fallback.
        let code = unsafe { guarded_error_code(true) };
        // The enumerator `error_code_of` produces for `Errors::UnknownServerError`.
        assert_eq!(code, code_owned_by(Errors::UnknownServerError));
        assert_eq!(code, kafka_common_Error_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn ffi_guard_error_return_reports_the_panic() {
        // SAFETY: Test code: `guarded_error_return(true)` takes only a `bool` and panics,
        // so the guard returns a fresh `box_error(panic_error(..))` handle (the `*mut
        // kafka_common_Error_t` fallback in `ffi-macros`), which satisfies `take_error`'s
        // expectation of a non-null, not yet destroyed handle; `take_error` asserts
        // non-null and frees it exactly once.
        let (message, code) = unsafe { take_error(guarded_error_return(true)) };
        assert_eq!(message, panic_message("guarded_error_return", "error boom"));
        assert_eq!(code, kafka_common_Error_LOCAL_ILLEGAL_STATE);
    }

    #[test]
    fn ffi_guard_borrowed_error_return_reports_the_panic() {
        // SAFETY: Test code: `guarded_borrowed_error(true)` takes only a `bool`; on its
        // panic the guard returns a fresh `box_error(..)` handle cast to `*const`, leaked
        // by design for a borrowing accessor (the `*const kafka_common_Error_t` fallback in
        // `ffi-macros`), so nothing else owns or frees it.
        let error = unsafe { guarded_borrowed_error(true) };
        // The handle is leaked by design (the caller of a borrowing accessor
        // never frees); the test knows it is fresh and frees it.
        // SAFETY: Test code: `error` is the fresh, never-freed handle the guard built on
        // the previous line, so casting away `const` and consuming it via `take_error`
        // (which asserts non-null) is the single, final use, as the comment above records.
        let (message, code) = unsafe { take_error(error as *mut kafka_common_Error_t) };
        assert_eq!(message, panic_message("guarded_borrowed_error", "borrowed boom"));
        assert_eq!(code, kafka_common_Error_LOCAL_ILLEGAL_STATE);
    }

    #[test]
    fn ffi_guard_pointer_fallbacks_are_null() {
        // SAFETY: Test code: `guarded_mut_pointer` and `guarded_const_pointer` are `extern
        // "C"` test functions under `#[ffi_guard]` taking only a `bool`; the calls are
        // `unsafe` solely because the functions are declared `unsafe extern "C"`, with no
        // pointer arguments or preconditions to uphold. The guard turns the requested panic
        // into the documented null fallback, and the results are only tested for null,
        // never dereferenced.
        unsafe {
            assert!(guarded_mut_pointer(true).is_null());
            assert!(guarded_const_pointer(true).is_null());
        }
    }

    #[test]
    fn ffi_guard_i8_size_and_bytes_fallbacks() {
        // SAFETY: Test code: `guarded_i8`, `guarded_topics_size` and `guarded_bytes` are
        // `extern "C"` test functions under `#[ffi_guard]` taking only a `bool`; the calls are
        // `unsafe` solely because the functions are declared `unsafe extern "C"`, with no
        // pointer arguments or preconditions to uphold. The returned `kafka_Bytes_t` is only
        // inspected, never dereferenced.
        unsafe {
            assert_eq!(guarded_i8(true), 0, "an int8_t boolean reads as false");
            assert_eq!(guarded_topics_size(true), 0, "a size reads as empty");
            let bytes = guarded_bytes(true);
            assert!(bytes.data.is_null(), "Java's null array");
            assert_eq!(bytes.len, 0);
        }
    }

    #[test]
    fn ffi_guard_byte_code_takes_its_declared_fallback() {
        // SAFETY: Test code: `guarded_byte_code` is an `extern "C"` test function under
        // `#[ffi_guard(fallback = -1)]` taking only a `bool`; the calls are `unsafe` solely
        // because it is declared `unsafe extern "C"`, with no pointer arguments or
        // preconditions to uphold.
        unsafe {
            assert_eq!(guarded_byte_code(false), 0, "0 is a valid code");
            assert_eq!(guarded_byte_code(true), -1, "a panic is no code");
        }
    }

    #[test]
    fn ffi_guard_enum_takes_the_named_fallback() {
        // SAFETY: Test code: `guarded_enum` is an `extern "C"` test function under
        // `#[ffi_guard]` taking only a `bool`; the call is `unsafe` solely because it is
        // declared `unsafe extern "C"`, with no pointer arguments or preconditions to uphold.
        let level = unsafe { guarded_enum(true) };
        assert_eq!(
            level,
            kafka_common_IsolationLevel_e::kafka_common_IsolationLevel_READ_UNCOMMITTED
        );
    }

    #[test]
    fn ffi_guard_stores_a_fallible_panic_in_out_err() {
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `err` is a live local pointing at null, the slot `guarded_new`'s
        // `# Safety` asks for.
        let value = unsafe { guarded_new(true, false, &mut err) };
        assert!(value.is_null(), "a panic returns the failure value");
        // SAFETY: Test code: `err` holds the fresh handle the guard stored on the panic path, not
        // yet freed by anyone, so `take_error` (which asserts non-null) consumes it exactly once.
        let (message, code) = unsafe { take_error(err) };
        assert_eq!(message, panic_message("guarded_new", "constructor boom"));
        assert_eq!(code, kafka_common_Error_LOCAL_ILLEGAL_STATE);

        // The body's own failure takes the same path: the same failure value, the error in
        // `*out_err`.
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: as above, with a fresh slot pointing at null.
        let value = unsafe { guarded_new(false, true, &mut err) };
        assert!(value.is_null(), "a failure returns the failure value");
        // SAFETY: Test code: `err` holds the fresh handle the body stored, consumed once.
        let (message, code) = unsafe { take_error(err) };
        assert_eq!(message, "constructor failed");
        assert_eq!(code, kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT);

        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: as above, with a fresh slot pointing at null.
        let value = unsafe { guarded_new(false, false, &mut err) };
        assert!(err.is_null(), "success leaves `*out_err` untouched");
        assert_eq!(value, std::ptr::dangling_mut());
    }

    #[test]
    fn ffi_guard_fallback_override_replaces_the_value() {
        // SAFETY: Test code: `guarded_fallback` takes no arguments and has no preconditions;
        // the call is `unsafe` solely because it is declared `unsafe extern "C"`.
        assert_eq!(unsafe { guarded_fallback() }, 42);
    }

    #[test]
    fn ffi_guard_on_panic_closure_takes_full_control() {
        // SAFETY: Test code: `guarded_on_panic` takes no arguments and has no preconditions;
        // the call is `unsafe` solely because it is declared `unsafe extern "C"`.
        assert_eq!(unsafe { guarded_on_panic() }, 7);
        let (message, code) = ON_PANIC_SAW.with(|saw| saw.borrow_mut().take()).expect("the closure ran");
        assert_eq!(message, panic_message("guarded_on_panic", "custom path"));
        assert_eq!(code, kafka_common_Error_LOCAL_ILLEGAL_STATE);
    }

    // The `_err` twins `#[ffi_guard]` emits beside each guarded function without an
    // error channel, which `confluent_kafka_bindings.h` declares for language bindings.

    #[test]
    fn ffi_guard_err_twin_stores_the_panic() {
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `err` is a live local pointing at null, the slot the twin's
        // `# Safety` asks for; `guarded_unit` itself has no preconditions.
        unsafe { guarded_unit_err(&mut err) };
        // SAFETY: Test code: `err` holds the fresh handle the twin stored on the panic path, not
        // yet freed by anyone, so `take_error` consumes it exactly once.
        let (message, code) = unsafe { take_error(err) };
        assert_eq!(message, panic_message("guarded_unit_err", "unit boom"));
        assert_eq!(code, kafka_common_Error_LOCAL_ILLEGAL_STATE);

        // A value-returning twin returns the entry point's own panic value.
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: as above, with a fresh slot pointing at null.
        assert!(!unsafe { guarded_bool_err(true, &mut err) }, "guarded_bool's derived false");
        // SAFETY: Test code: `err` holds the fresh panic handle, consumed exactly once.
        let (message, code) = unsafe { take_error(err) };
        assert_eq!(message, panic_message("guarded_bool_err", "bool boom"));
        assert_eq!(code, kafka_common_Error_LOCAL_ILLEGAL_STATE);

        // `fallback` is that value too; only `*out_err` tells it from a real 42.
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: as above, with a fresh slot pointing at null.
        assert_eq!(unsafe { guarded_fallback_err(&mut err) }, 42);
        // SAFETY: Test code: `err` holds the fresh panic handle, consumed exactly once.
        let (message, _) = unsafe { take_error(err) };
        assert_eq!(message, panic_message("guarded_fallback_err", "fallback boom"));

        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: as above, with a fresh slot pointing at null.
        unsafe { guarded_formatted_err(3, &mut err) };
        // SAFETY: Test code: `err` holds the fresh panic handle, consumed exactly once.
        let (message, _) = unsafe { take_error(err) };
        assert_eq!(message, panic_message("guarded_formatted_err", "formatted payload 3"));

        // A twin of a function returning an error as its value (an error factory)
        // returns null: the panic is in `*out_err`, never mistaken for the value.
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: as above, with a fresh slot pointing at null.
        assert!(unsafe { guarded_error_return_err(true, &mut err) }.is_null());
        // SAFETY: Test code: `err` holds the fresh panic handle, consumed exactly once.
        let (message, _) = unsafe { take_error(err) };
        assert_eq!(message, panic_message("guarded_error_return_err", "error boom"));
    }

    #[test]
    fn ffi_guard_err_twin_returns_the_value_on_success() {
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `err` is a live local pointing at null, the slot the twin's
        // `# Safety` asks for.
        assert!(unsafe { guarded_bool_err(false, &mut err) });
        assert!(err.is_null(), "success leaves `*out_err` untouched");
        // `return` in the body still returns the entry point's value.
        for (early, expected) in [(true, 5), (false, 6)] {
            // SAFETY: Test code: `err` is still null (asserted above or in the previous
            // iteration), the slot the twin's `# Safety` asks for.
            let value = unsafe { guarded_early_return_err(early, &mut err) };
            assert_eq!(value, expected, "for early = {early}");
            assert!(err.is_null());
        }
    }

    #[test]
    fn spawn_callback_task_runs_the_task() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        // From a thread that is not one of the runtime's workers, like a C caller's.
        let task = spawn_callback_task(runtime.handle(), async { 7 });
        assert_eq!(runtime.block_on(task).expect("the task ran"), 7);
    }

    /// A panic inside the spawn aborts the process before the guard sees it: the
    /// queued task would fire the callback, so `on_panic` firing it as well would
    /// be a second completion (§4 item 6 of
    /// `design/current/appsec-7665-4521-ffi-panic-guard.md`). The abort would end
    /// this test binary, so the test runs itself again in a child process, which
    /// takes the `CHILD` branch.
    #[test]
    fn spawn_panic_aborts_before_the_guard() {
        const CHILD: &str = "CONFLUENT_KAFKA_SPAWN_PANIC_CHILD";
        if std::env::var_os(CHILD).is_some() {
            // SAFETY: Test code: `guarded_spawn_panics` takes no arguments and has no
            // preconditions; the call is `unsafe` solely because it is declared `unsafe
            // extern "C"`.
            unsafe { guarded_spawn_panics() };
            println!("returned");
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("test binary path"))
            .args(["--exact", "ffi::common::tests::spawn_panic_aborts_before_the_guard"])
            .args(["--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()
            .expect("run the child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stdout.contains("spawning"),
            "the child never reached the spawn:\n{stdout}\n{stderr}"
        );
        assert!(stderr.contains("failed to wake I/O driver"), "no panic in the child:\n{stderr}");
        assert!(!stdout.contains("on_panic fired"), "the panic reached the guard:\n{stdout}");
        assert!(!stdout.contains("returned"), "the entry point returned:\n{stdout}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            // SIGABRT (6 on Linux and macOS), which `std::process::abort` raises.
            assert_eq!(
                output.status.signal(),
                Some(6),
                "child exited with {}:\n{stderr}",
                output.status
            );
        }
        #[cfg(not(unix))]
        assert!(!output.status.success(), "child exited with {}", output.status);
    }

    #[test]
    fn ffi_guard_string_and_str_payloads() {
        // `panic!` with arguments carries a `String`.
        let mut seen = None;
        let value = ffi_guard_or(
            "formatted",
            |err| {
                seen = Some(err.message().to_owned());
                -1
            },
            || -> i32 {
                let value = 9;
                panic!("formatted payload {value}")
            },
        );
        assert_eq!(value, -1);
        assert_eq!(seen.unwrap(), panic_message("formatted", "formatted payload 9"));
        // SAFETY: Test code: `guarded_formatted` is an `extern "C"` test function under
        // `#[ffi_guard]` taking only an `i32`; the call is `unsafe` solely because it is
        // declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard turns its panic into the documented `-1` fallback.
        assert_eq!(unsafe { guarded_formatted(3) }, -1);

        // A literal `panic!`, and `unwrap` on `None`, carry a `&'static str`.
        let payload: Box<dyn Any + Send> = Box::new("literal payload");
        assert_eq!(panic_error("f", &*payload).message(), panic_message("f", "literal payload"));
        let payload: Box<dyn Any + Send> = Box::new(String::from("owned payload"));
        assert_eq!(panic_error("f", &*payload).message(), panic_message("f", "owned payload"));
        let mut seen = None;
        ffi_guard_or(
            "unwrap",
            |err| seen = Some(err.message().to_owned()),
            || std::hint::black_box(None::<()>).unwrap(),
        );
        assert_eq!(
            seen.unwrap(),
            panic_message("unwrap", "called `Option::unwrap()` on a `None` value")
        );
        // SAFETY: Test code: `guarded_unwrap` is a parameterless `extern "C"` test function
        // under `#[ffi_guard]`; the call is `unsafe` solely because it is declared `unsafe
        // extern "C"`, with no pointer arguments or preconditions to uphold. The guard
        // turns its `unwrap` panic into the documented `-1` fallback.
        assert_eq!(unsafe { guarded_unwrap() }, -1);
    }

    #[test]
    fn ffi_guard_non_string_payload() {
        let payload: Box<dyn Any + Send> = Box::new(42_u32);
        let error = panic_error("f", &*payload);
        assert_eq!(error.message(), panic_message("f", "non-string panic payload"));
        assert_eq!(error_code_of(&error), kafka_common_Error_LOCAL_ILLEGAL_STATE);
        // SAFETY: Test code: `guarded_non_string_payload` is a parameterless `extern "C"`
        // test function under `#[ffi_guard]`; the call is `unsafe` solely because it is
        // declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard turns its `panic_any` payload into the documented `-1`
        // fallback.
        assert_eq!(unsafe { guarded_non_string_payload() }, -1);
    }

    #[test]
    fn ffi_guard_survives_a_payload_whose_destructor_panics() {
        // SAFETY: Test code: `guarded_payload_panics_on_drop` is a parameterless `extern
        // "C"` test function under `#[ffi_guard]`; the call is `unsafe` solely because it
        // is declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard catches both the panic and the payload destructor's nested
        // panic and returns the documented `-1` fallback.
        assert_eq!(unsafe { guarded_payload_panics_on_drop() }, -1);
    }

    #[test]
    fn ffi_guard_passes_through_when_nothing_panics() {
        // SAFETY: Test code: every `guarded_*` call here takes only a `bool`, no argument,
        // or `&mut err`, a live local pointing at null that outlives the call, the slot
        // `guarded_new` asks for; `guarded_const_pointer(false)` returns a pointer to the
        // `c"value"` literal, which is `'static`, so `CStr::from_ptr` reads a valid
        // NUL-terminated string; `guarded_bytes(false)` views the `'static` `b"value"`
        // literal, so the slice built from it is valid; and `guarded_mut_pointer(false)` and
        // the value written into `out_new` are dangling pointers that are only compared
        // against null, never dereferenced.
        unsafe {
            assert!(guarded_bool(false));
            assert_eq!(guarded_i16(false), 16);
            assert_eq!(guarded_i32(false), 32);
            assert_eq!(guarded_i64(false), 64);
            assert_eq!(guarded_topics_count(false), 3);
            assert_eq!(guarded_offsets_count(false), 3);
            assert_eq!(guarded_f64(false), 1.5);
            assert_eq!(guarded_error_code(false), kafka_common_Error_NONE);
            assert!(guarded_error_return(false).is_null());
            assert!(guarded_borrowed_error(false).is_null());
            assert!(!guarded_mut_pointer(false).is_null());
            assert_eq!(CStr::from_ptr(guarded_const_pointer(false)), c"value");
            // `return` inside the body still returns the function's value.
            assert_eq!(guarded_early_return(true), 5);
            assert_eq!(guarded_early_return(false), 6);

            assert_eq!(guarded_i8(false), 1);
            assert_eq!(guarded_topics_size(false), 4);
            let bytes = guarded_bytes(false);
            assert_eq!(std::slice::from_raw_parts(bytes.data, bytes.len as usize), b"value");
            assert_eq!(
                guarded_enum(false),
                kafka_common_IsolationLevel_e::kafka_common_IsolationLevel_READ_COMMITTED
            );

            let mut err = std::ptr::null_mut();
            assert!(!guarded_new(false, false, &mut err).is_null(), "success returns the value");
            assert!(err.is_null(), "and stores no error");
        }
    }
}
