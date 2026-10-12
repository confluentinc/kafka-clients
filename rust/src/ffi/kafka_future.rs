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

//! C FFI for [`KafkaFuture`]: the one generic `kafka_common_KafkaFuture_t`
//! whose `T` is a `void *` (CLAUDE.md §4, "Generic types").
//!
//! Every function returning a `KafkaFuture<T>` in Rust returns this handle in
//! C and documents the concrete type behind the `void *` its `get` delivers.
//! The Rust side maps its typed future to a `KafkaFuture<Arc<FfiValue>>` with
//! `then_apply` ([`FfiValue::owned`]), so the value is boxed exactly once, when
//! the future resolves, and freed when the last handle holding it goes away.
//!
//! # Value lifetime
//!
//! Java's `Future.get()` may be called any number of times and always answers
//! the same value, and a `KafkaFuture` is `Clone`. A `void *` cannot be cloned
//! per call, so `get` hands out a **borrowed** pointer to the value the handle
//! caches on first resolution: it stays valid until the handle is destroyed
//! (and, for a `_cb` delivery, at least until the callback returns). A caller
//! that needs the value for longer keeps the future handle alive. A `Void`
//! future (`all_of`, `Admin.*Result.all()`) delivers `NULL`. A value that C
//! supplied through `completed_future` is never freed by Rust.
//!
//! # Threads
//!
//! `get` blocks the calling thread. When the future came from a client it is
//! driven on that client's runtime; a future C created itself
//! (`completed_future`, `all_of` over such futures) is driven on a small
//! process-wide runtime, so `get` works outside any client. `get_cb` queues its
//! callback on the owning client's callbacks vector (drained by that client's
//! `__execute_callbacks`); a future with no client invokes the callback inline,
//! on the calling thread, before `get_cb` returns.
//!
//! A future may outlive its client. The client's `_destroy` fires the callback
//! of every `get_cb` still waiting, exactly once, before it returns: with the
//! value when the future has resolved, else with a
//! `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` error, "The client was
//! destroyed before the future completed". A `get_cb` after that invokes the
//! callback inline, the same way, as for a future with no client.

// FFI type names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures_util::FutureExt;

use crate::common::{Error, KafkaFuture};
use crate::ffi::callback_queue::{CallbackJob, CallbackQueue, SendPtr};
use crate::ffi::common::{
    box_error, catch_task_panic, caught_panic_error, deliver_panic_value, kafka_common_Error_t, spawn_callback_task,
    take_error,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{destroy_boxed, kafka_List_t, list_elements};

// ---------------------------------------------------------------------------
// The value behind the `void *`
// ---------------------------------------------------------------------------

/// The resolved value of an FFI future: a `void *` plus, when Rust produced
/// it, the destructor that frees it once the last handle sharing it is gone.
pub(crate) struct FfiValue {
    ptr: *mut c_void,
    destroy: Option<unsafe fn(*mut c_void)>,
}

// SAFETY: the struct never dereferences `ptr`; moving it to another thread only
// moves where `Drop` may run `destroy`. The pointer is one of three things:
// - a boxed Rust value (`owned`), which may be freed on any thread because `T` is
//   `Send` by the bound on `owned`;
// - a Rust-made C handle (`handle`), whose caller guarantees, per `handle`'s doc,
//   that it may be freed from any thread;
// - an opaque pointer C supplied and keeps alive (`borrowed`, `null`), never freed
//   here.
unsafe impl Send for FfiValue {}
// SAFETY: a shared `&FfiValue` only copies `ptr` out (`ptr()`) and never
// dereferences it; `destroy` is used only by `Drop`, through `&mut self`. What
// C reads through the pointer, possibly from several threads, is one of these:
// - a value boxed by `owned`, reached only from `map_future`, whose `T` is `Sync`;
// - a C handle made by `handle`, whose caller guarantees, per `handle`'s doc,
//   that its accessors may run from several threads at once;
// - C's own pointee.
unsafe impl Sync for FfiValue {}

impl FfiValue {
    /// Boxes a Rust value behind the `void *`, freed with the handle.
    pub(crate) fn owned<T: Send + 'static>(value: T) -> Arc<Self> {
        Arc::new(Self {
            ptr: Box::into_raw(Box::new(value)) as *mut c_void,
            destroy: Some(destroy_boxed::<T>),
        })
    }

    /// Wraps an owned C handle (a `<Type>_t *` from one of the `box_*`
    /// helpers), freed with the handle through its `_destroy`.
    ///
    /// The caller guarantees that `destroy` frees exactly `ptr`: it is the
    /// destructor paired with the helper that made `ptr` (a `box_*` helper and
    /// its `_destroy`, `into_c_string` and `destroy_string_element`). It also
    /// guarantees that the handle may be freed from any thread, and read
    /// through its accessors from several threads at once.
    pub(crate) fn handle(ptr: *mut c_void, destroy: unsafe fn(*mut c_void)) -> Arc<Self> {
        Arc::new(Self { ptr, destroy: Some(destroy) })
    }

    /// Wraps a pointer C owns, never freed by Rust.
    pub(crate) fn borrowed(ptr: *mut c_void) -> Arc<Self> {
        Arc::new(Self { ptr, destroy: None })
    }

    /// The `NULL` a `Void` future delivers.
    pub(crate) fn null() -> Arc<Self> {
        Self::borrowed(std::ptr::null_mut())
    }

    /// The pointer handed to C.
    pub(crate) fn ptr(&self) -> *mut c_void {
        self.ptr
    }
}

impl Drop for FfiValue {
    fn drop(&mut self) {
        if let Some(destroy) = self.destroy
            && !self.ptr.is_null()
        {
            // SAFETY: `ptr` is non-null (checked above) and is the pointer `destroy` was
            // paired with at construction. For `owned` that is a `Box::into_raw::<T>`
            // paired with `destroy_boxed::<T>`. For `handle` it is a handle paired with
            // the destructor that frees it, which `handle`'s caller guarantees. This is
            // the only drop of it, since the last `Arc` sharing this value has just gone.
            unsafe { destroy(self.ptr) };
        }
    }
}

/// A Rust future already mapped to its C value.
pub(crate) type FfiFuture = KafkaFuture<Arc<FfiValue>>;

/// Maps a typed future to its C value, boxing `T` when it resolves.
pub(crate) fn map_future<T>(future: &KafkaFuture<T>) -> FfiFuture
where
    T: Clone + Send + Sync + 'static,
{
    future.then_apply(|value| Ok(FfiValue::owned(value)))
}

/// Maps a typed future to the C handle `boxer` builds from its value when it
/// resolves, freed with `destroy` (the handle type's `_destroy`) together with
/// the future's value.
///
/// The caller gives each handle `boxer` makes the guarantees
/// [`FfiValue::handle`] requires: `destroy` frees exactly that handle, and the
/// handle may be freed and read from any thread.
pub(crate) fn map_future_handle<T, F>(future: &KafkaFuture<T>, boxer: F, destroy: unsafe fn(*mut c_void)) -> FfiFuture
where
    T: Clone + Send + Sync + 'static,
    F: Fn(T) -> *mut c_void + Send + Sync + 'static,
{
    future.then_apply(move |value| Ok(FfiValue::handle(boxer(value), destroy)))
}

/// Maps a `Void` future to the `NULL` it delivers.
pub(crate) fn map_void_future(future: &KafkaFuture<()>) -> FfiFuture {
    future.then_apply(|()| Ok(FfiValue::null()))
}

// ---------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------

/// What a [`kafka_common_KafkaFuture_t`] points at, shared between the handle
/// and any in-flight `_cb` delivery.
pub(crate) struct FutureInner {
    future: FfiFuture,
    /// The value `get` returned, cached so every `get` answers the same
    /// pointer and the value lives as long as the handle.
    result: OnceLock<Arc<FfiValue>>,
    /// The runtime of the client that produced the future, when any.
    runtime: Option<tokio::runtime::Handle>,
    /// The callbacks vector of that client, when any.
    queue: Option<Arc<CallbackQueue>>,
}

impl FutureInner {
    /// Resolves the future, blocking the calling thread.
    fn resolve(&self, timeout: Option<Duration>) -> Result<Arc<FfiValue>, Error> {
        if let Some(cached) = self.result.get() {
            return Ok(cached.clone());
        }
        let value = block_on(self.runtime.as_ref(), self.resolve_future(timeout))?;
        Ok(self.result.get_or_init(|| value).clone())
    }

    /// Resolves the future on the current task.
    async fn resolve_async(&self, timeout: Option<Duration>) -> Result<Arc<FfiValue>, Error> {
        if let Some(cached) = self.result.get() {
            return Ok(cached.clone());
        }
        let value = self.resolve_future(timeout).await?;
        Ok(self.result.get_or_init(|| value).clone())
    }

    /// Resolves the future without waiting, for a future whose client is
    /// destroyed: nothing is left to complete it, so it answers its value
    /// when it has one and [`CLIENT_DESTROYED`] otherwise.
    fn resolve_now(&self) -> Result<Arc<FfiValue>, Error> {
        if let Some(cached) = self.result.get() {
            return Ok(cached.clone());
        }
        match self.future.get().now_or_never() {
            Some(result) => {
                let value = result?;
                Ok(self.result.get_or_init(|| value).clone())
            },
            None => Err(Error::local_illegal_state(CLIENT_DESTROYED)),
        }
    }

    async fn resolve_future(&self, timeout: Option<Duration>) -> Result<Arc<FfiValue>, Error> {
        match timeout {
            None => self.future.get().await,
            Some(timeout) => self.future.get_with_timeout(timeout).await,
        }
    }
}

/// Drives `future` to completion on `handle`, or on the process-wide fallback
/// runtime when the future belongs to no client.
pub(crate) fn block_on<F: Future>(handle: Option<&tokio::runtime::Handle>, future: F) -> F::Output {
    match handle {
        Some(handle) => handle.block_on(future),
        None => fallback_runtime().block_on(future),
    }
}

/// The runtime driving futures C built outside any client.
///
/// Multi-threaded with one worker so that its timer driver runs while a C
/// thread blocks in `Runtime::block_on` (a current-thread runtime's drivers
/// are only driven by the thread inside `block_on`, which is fine too, but
/// `Handle::block_on` would not drive them).
fn fallback_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("kafka-ffi-future")
            .enable_all()
            .build()
            .expect("the FFI future runtime cannot be built")
    })
}

/// Opaque handle to a [`KafkaFuture`] whose value is a `void *` (CLAUDE.md
/// §4, "Generic types"); the function that returned it documents the type
/// behind the pointer. See the module docs for the value's lifetime.
#[repr(C)]
pub struct kafka_common_KafkaFuture_t {
    _private: [u8; 0],
}

/// Hands `future` to C as an owned handle, freed with
/// [`kafka_common_KafkaFuture_destroy`].
///
/// `runtime` and `queue` are those of the client that produced the future;
/// `None` for a future that belongs to no client.
pub(crate) fn box_future(
    future: FfiFuture,
    runtime: Option<tokio::runtime::Handle>,
    queue: Option<Arc<CallbackQueue>>,
) -> *mut kafka_common_KafkaFuture_t {
    Arc::into_raw(Arc::new(FutureInner { future, result: OnceLock::new(), runtime, queue })) as *mut _
}

/// The future behind a handle.
///
/// # Safety
///
/// `future` must be a valid handle from [`box_future`], not destroyed for
/// `'a`.
unsafe fn future_ref<'a>(future: *const kafka_common_KafkaFuture_t) -> &'a FutureInner {
    // SAFETY: per this function's `# Safety`, `future` came from `box_future`, whose
    // `Arc::into_raw` of a `FutureInner` is the allocation behind every future handle,
    // and the strong count that handle holds keeps it alive for `'a`. `FutureInner` is
    // only ever shared, so the `&` aliases nothing mutable.
    unsafe { &*(future as *const FutureInner) }
}

/// A new strong reference to the future behind a handle, for a delivery that
/// may outlive the handle.
///
/// # Safety
///
/// `future` must be a valid handle from [`box_future`].
unsafe fn future_arc(future: *const kafka_common_KafkaFuture_t) -> Arc<FutureInner> {
    let raw = future as *const FutureInner;
    // SAFETY: per this function's `# Safety`, `future` is a live handle from
    // `box_future`, so `raw` is the `Arc::into_raw` of a `FutureInner` whose strong count
    // is at least 1. Incrementing it first makes the `from_raw` a new reference that
    // leaves the handle's own reference in place.
    unsafe {
        Arc::increment_strong_count(raw);
        Arc::from_raw(raw)
    }
}

/// `typedef void (*kafka_common_KafkaFuture_get_cb_t)(void *value, kafka_common_Error_t *error, void *opaque);`
///
/// The completion of [`kafka_common_KafkaFuture_get_cb`]: `value` is the
/// future's value, borrowed as described in the module docs (`NULL` for a
/// `Void` future or on failure); `error` is `NULL` on success, otherwise an
/// owned error the callee frees with `kafka_common_Error_destroy`.
pub type kafka_common_KafkaFuture_get_cb_t =
    unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// `typedef void (*kafka_common_KafkaFuture_get_with_timeout_cb_t)(void *value, kafka_common_Error_t *error, void *opaque);`
///
/// The completion of [`kafka_common_KafkaFuture_get_with_timeout_cb`], with
/// the same contract as [`kafka_common_KafkaFuture_get_cb_t`].
pub type kafka_common_KafkaFuture_get_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The error a `get_cb` completion carries when the future's client was
/// destroyed before the future resolved. No Java counterpart: a Java
/// `KafkaFuture` outlives its client.
const CLIENT_DESTROYED: &str = "The client was destroyed before the future completed";

/// The completion a `get_cb` task owes its caller, delivered exactly once: by
/// [`complete`](Self::complete) when the task resolves the future, or, when the
/// task is dropped first, by `Drop`. A client's `_destroy` does not join these
/// tasks (they wait on futures it cannot wait for), so shutting its runtime
/// down drops the ones still waiting, as does spawning onto a runtime already
/// shut down; `Drop` then queues the completion with
/// [`FutureInner::resolve_now`]'s answer, and `_destroy`'s last drain runs it.
///
/// No Java counterpart (DoD #7): Java's `KafkaFuture` has no C callback whose
/// delivery must survive its task being dropped.
struct PendingGet {
    fn_name: &'static str,
    cb: kafka_common_KafkaFuture_get_cb_t,
    opaque: SendPtr,
    /// Moved into the queued job, so the value outlives a handle destroyed
    /// before the callback runs.
    inner: Arc<FutureInner>,
    queue: Arc<CallbackQueue>,
    delivered: bool,
}

impl PendingGet {
    /// Queues the completion with `result`.
    fn complete(mut self, result: Result<Arc<FfiValue>, Error>) {
        self.queue_completion(result);
    }

    fn queue_completion(&mut self, result: Result<Arc<FfiValue>, Error>) {
        self.delivered = true;
        let (cb, opaque, inner) = (self.cb, self.opaque, Arc::clone(&self.inner));
        self.queue.push(Box::new(move || {
            deliver(cb, result, opaque);
            drop(inner);
        }));
    }
}

impl Drop for PendingGet {
    fn drop(&mut self) {
        if !self.delivered {
            let inner = Arc::clone(&self.inner);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inner.resolve_now()))
                .unwrap_or_else(|payload| Err(caught_panic_error(self.fn_name, payload)));
            self.queue_completion(result);
        }
    }
}

/// Delivers a resolution to a `_cb_t` callback.
fn deliver(cb: kafka_common_KafkaFuture_get_cb_t, result: Result<Arc<FfiValue>, Error>, opaque: SendPtr) {
    match result {
        // SAFETY: `cb` is the function pointer the C caller passed to the `_cb` entry
        // point, called once with its own opaque pointer. `value` is kept alive by the
        // `Arc` this function holds, so the borrowed pointer is valid while `cb` runs.
        Ok(value) => unsafe { cb(value.ptr(), std::ptr::null_mut(), opaque.0) },
        // SAFETY: as above; the error is a fresh `box_error` handle that `cb` owns.
        Err(e) => unsafe { cb(std::ptr::null_mut(), box_error(e), opaque.0) },
    }
}

/// Queues the delivery on the owning client's callbacks vector, or runs it
/// inline when the future has no client. A panic while resolving is
/// delivered as the [`panic_error`](crate::ffi::common::panic_error) of
/// `fn_name`, the entry point.
///
/// # Safety
///
/// `future` must be a valid handle from [`box_future`], and `cb` and `opaque`
/// the pair the C caller passed to the `_cb` entry point.
unsafe fn get_cb(
    fn_name: &'static str,
    future: *const kafka_common_KafkaFuture_t,
    timeout: Option<Duration>,
    cb: kafka_common_KafkaFuture_get_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: per this function's `# Safety`, `future` is a valid handle from
    // `box_future`, which is what `future_arc` requires.
    let inner = unsafe { future_arc(future) };
    let opaque = SendPtr(opaque);
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    match (inner.runtime.clone(), inner.queue.clone()) {
        // The client was destroyed: its runtime is gone, so the future is
        // answered now, on this thread, as a client-less one would be.
        (Some(_), Some(queue)) if queue.is_closed() => deliver(cb, inner.resolve_now(), opaque),
        (Some(runtime), Some(queue)) => {
            let pending = PendingGet { fn_name, cb, opaque, inner, queue, delivered: false };
            spawn_callback_task(&runtime, async move {
                let result = catch_task_panic(fn_name, pending.inner.resolve_async(timeout)).await;
                pending.complete(result);
            });
        },
        _ => deliver(cb, inner.resolve(timeout), opaque),
    }
}

/// Where the guard of a `_cb` entry point delivers a caught panic: the
/// owning client's callbacks vector, or the calling thread for a future with
/// no client, as [`get_cb`] delivers every completion (see
/// [`deliver_panic_value`](crate::ffi::common::deliver_panic_value)).
///
/// # Safety
///
/// `future` must be a valid handle.
unsafe fn completion_sink(future: *const kafka_common_KafkaFuture_t) -> impl FnOnce(CallbackJob) {
    // SAFETY: per this function's `# Safety`, `future` is a valid handle, which is what
    // `future_arc` requires; the sink holds its own reference.
    let inner = unsafe { future_arc(future) };
    move |job| match (&inner.runtime, &inner.queue) {
        (Some(_), Some(queue)) => queue.push(job),
        _ => job(),
    }
}

fn timeout_ms(timeout: i64) -> Duration {
    Duration::from_millis(u64::try_from(timeout).unwrap_or(0))
}

/// `KafkaFuture.completedFuture(value)`: a future already resolved with
/// `result`, or failed with `result_error` when that is non-null.
///
/// `result` is a `void *` the caller owns and keeps alive while the future may
/// still deliver it; Rust never frees it. `result_error` is consumed: the
/// caller must not destroy it.
///
/// # Safety
///
/// `result_error` must be null or an owned error handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_completed_future(
    result: *const c_void,
    result_error: *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: per this function's `# Safety`, `result_error` is null or an owned error
    // handle not yet destroyed, which this call consumes as `take_error` requires.
    let result = match unsafe { take_error(result_error) } {
        Some(error) => Err(error),
        None => Ok(FfiValue::borrowed(result as *mut c_void)),
    };
    box_future(KafkaFuture::completed_future(result), None, None)
}

/// `KafkaFuture.get()`: blocks until the future resolves and stores its value
/// in `*out_get`, borrowed as described in the module docs.
///
/// Returns `NULL` on success, otherwise the error the operation failed with,
/// owned by the caller.
///
/// # Safety
///
/// `self_` must be a valid future handle and `out_get` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get(
    self_: *const kafka_common_KafkaFuture_t,
    out_get: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle; the
    // borrow lasts for this call only, while the caller keeps the handle alive.
    match unsafe { future_ref(self_) }.resolve(None) {
        Ok(value) => {
            // SAFETY: per this function's `# Safety`, `out_get` is a valid pointer. The
            // value it receives is the one the handle caches, alive until the handle is
            // destroyed (module docs).
            unsafe { *out_get = value.ptr() };
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// The non-blocking form of [`kafka_common_KafkaFuture_get`]: `cb` receives
/// the value or the error through the owning client's `__execute_callbacks`,
/// or inline before this returns when the future belongs to no client. A
/// call still waiting when the client is destroyed fires from its `_destroy`,
/// and one after it inline (see the module docs).
///
/// # Safety
///
/// `self_` must be a valid future handle; `opaque` is handed back to `cb`
/// untouched.
#[ffi_guard(on_panic = |err| {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle, as
    // `completion_sink` requires. The guard runs this only when the body did not return,
    // and the body hands its completion over as its last step, so `cb` has not fired for
    // this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get_cb(
    self_: *const kafka_common_KafkaFuture_t,
    cb: kafka_common_KafkaFuture_get_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle, and
    // `cb` and `opaque` are the pair the C caller passed.
    unsafe { get_cb("kafka_common_KafkaFuture_get_cb", self_, None, cb, opaque) };
}

/// `KafkaFuture.get(timeout, MILLISECONDS)`: like
/// [`kafka_common_KafkaFuture_get`], failing with the translation of
/// `java.util.concurrent.TimeoutException` (`kafka_common_Error_is_local_timeout_error`)
/// when `timeout` milliseconds elapse first. A negative timeout is zero.
///
/// # Safety
///
/// `self_` must be a valid future handle and `out_get_with_timeout` a valid
/// pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get_with_timeout(
    self_: *const kafka_common_KafkaFuture_t,
    timeout: i64,
    out_get_with_timeout: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle; the
    // borrow lasts for this call only, while the caller keeps the handle alive.
    match unsafe { future_ref(self_) }.resolve(Some(timeout_ms(timeout))) {
        Ok(value) => {
            // SAFETY: per this function's `# Safety`, `out_get_with_timeout` is a valid
            // pointer. The value it receives is the one the handle caches, alive until
            // the handle is destroyed (module docs).
            unsafe { *out_get_with_timeout = value.ptr() };
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// The non-blocking form of [`kafka_common_KafkaFuture_get_with_timeout`];
/// see [`kafka_common_KafkaFuture_get_cb`] for where `cb` runs.
///
/// # Safety
///
/// `self_` must be a valid future handle; `opaque` is handed back to `cb`
/// untouched.
#[ffi_guard(on_panic = |err| {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle, as
    // `completion_sink` requires. The guard runs this only when the body did not return,
    // and the body hands its completion over as its last step, so `cb` has not fired for
    // this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get_with_timeout_cb(
    self_: *const kafka_common_KafkaFuture_t,
    timeout: i64,
    cb: kafka_common_KafkaFuture_get_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle, and
    // `cb` and `opaque` are the pair the C caller passed.
    unsafe {
        get_cb(
            "kafka_common_KafkaFuture_get_with_timeout_cb",
            self_,
            Some(timeout_ms(timeout)),
            cb,
            opaque,
        )
    };
}

/// `KafkaFuture.isDone()`: 1 when the future has resolved, 0 otherwise.
///
/// # Safety
///
/// `self_` must be a valid future handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_is_done(self_: *const kafka_common_KafkaFuture_t) -> i8 {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle; the
    // borrow lasts for this call only.
    i8::from(unsafe { future_ref(self_) }.future.is_done())
}

/// `KafkaFuture.allOf(futures...)`: a `Void` future that resolves once every
/// future in `futures` has, and fails with the first failure among them.
///
/// `futures` is a `kafka_List_t` of `kafka_common_KafkaFuture_t *`, borrowed:
/// the caller still owns the list and each future, and may destroy them right
/// after this returns. The result delivers `NULL` from `get`.
///
/// # Safety
///
/// `futures` must be null or a valid list whose elements are valid future
/// handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_all_of(
    futures: *const kafka_List_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: per this function's `# Safety`, `futures` is null or a valid list, which the
    // caller keeps alive for this call; the borrow ends before return.
    let inners: Vec<&FutureInner> = unsafe { list_elements(futures) }
        .iter()
        // SAFETY: per this function's `# Safety`, every element is a valid future handle,
        // alive for this call. The borrows end before return: only clones of the futures,
        // runtime and queue outlive it.
        .map(|&f| unsafe { future_ref(f as *const _) })
        .collect();
    let runtime = inners.iter().find_map(|i| i.runtime.clone());
    let queue = inners.iter().find_map(|i| i.queue.clone());
    let all = KafkaFuture::all_of(inners.iter().map(|i| i.future.clone()).collect());
    box_future(map_void_future(&all), runtime, queue)
}

// ---------------------------------------------------------------------------
// `KafkaFuture.BaseFunction` and `thenApply`
// ---------------------------------------------------------------------------

/// `typedef kafka_common_Error_t *(*kafka_common_KafkaFuture_BaseFunction_apply_fn_t)(void *self, void *a, void **out_apply);`
///
/// `BaseFunction.apply(A a)`: `a` is the source future's value, borrowed for
/// the duration of the call (`NULL` for a `Void` future); the result goes in
/// `*out_apply`, a `void *` the implementation owns and keeps alive as long
/// as the derived future may deliver it (Rust never frees it). Returns `NULL`
/// on success, otherwise an owned error that fails the derived future, as a
/// throwing Java function does.
pub type kafka_common_KafkaFuture_BaseFunction_apply_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, a: *mut c_void, out_apply: *mut *mut c_void) -> *mut kafka_common_Error_t;

/// A C implementation of `KafkaFuture.BaseFunction` (CLAUDE.md §4 rule 3):
/// the caller's `void *self` plus its `apply`.
struct BaseFunctionInner {
    self_: SendPtr,
    apply: kafka_common_KafkaFuture_BaseFunction_apply_fn_t,
}

impl BaseFunctionInner {
    /// Invokes the C `apply` on `a`.
    fn apply(&self, a: *mut c_void) -> Result<*mut c_void, Error> {
        let mut out: *mut c_void = std::ptr::null_mut();
        // SAFETY: `apply` is the function pointer the C caller registered,
        // called with its own `self`.
        let error = unsafe { (self.apply)(self.self_.0, a, &raw mut out) };
        // SAFETY: `apply` returns null or an owned error handle, per the
        // `kafka_common_KafkaFuture_BaseFunction_apply_fn_t` contract, consumed here once.
        match unsafe { take_error(error) } {
            Some(error) => Err(error),
            None => Ok(out),
        }
    }
}

/// Opaque handle to a C implementation of `KafkaFuture.BaseFunction<A, B>`,
/// the function `then_apply` runs on a future's value. Java declares it as a
/// functional interface that the Rust API takes as a closure; C has no
/// closures, so it is an interface built with
/// [`kafka_common_KafkaFuture_BaseFunction_new`].
#[repr(C)]
pub struct kafka_common_KafkaFuture_BaseFunction_t {
    _private: [u8; 0],
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `function` must be a valid handle from
/// [`kafka_common_KafkaFuture_BaseFunction_new`], not destroyed for `'a`.
unsafe fn base_function_ref<'a>(function: *const kafka_common_KafkaFuture_BaseFunction_t) -> &'a BaseFunctionInner {
    // SAFETY: per this function's `# Safety`, `function` came from
    // `kafka_common_KafkaFuture_BaseFunction_new`, whose `Box::into_raw` of a
    // `BaseFunctionInner` is the allocation behind every function handle, alive for `'a`.
    // Nothing mutates a registration, so the `&` aliases nothing mutable.
    unsafe { &*(function as *const BaseFunctionInner) }
}

/// Registers a C `BaseFunction`: `self_` is the caller's opaque pointer
/// passed first to `apply`. The caller owns `self_` and keeps it alive until
/// every future derived through this function has been destroyed; the handle
/// itself may be destroyed as soon as `then_apply` returns, since the derived
/// future keeps its own copy of the registration.
///
/// Owned, freed with [`kafka_common_KafkaFuture_BaseFunction_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_KafkaFuture_BaseFunction_new(
    self_: *mut c_void,
    apply: kafka_common_KafkaFuture_BaseFunction_apply_fn_t,
) -> *mut kafka_common_KafkaFuture_BaseFunction_t {
    Box::into_raw(Box::new(BaseFunctionInner { self_: SendPtr(self_), apply })) as *mut _
}

/// `BaseFunction.apply(a)`: invokes the registered `apply` with its `self`,
/// storing the result in `*out_apply`. Returns `NULL` on success, otherwise
/// the error the function returned, owned by the caller.
///
/// # Safety
///
/// `self_` must be a valid function handle and `out_apply` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_apply(
    self_: *const kafka_common_KafkaFuture_BaseFunction_t,
    a: *mut c_void,
    out_apply: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    // SAFETY: per this function's `# Safety`, `self_` is a valid function handle; the
    // borrow lasts for this call only.
    match unsafe { base_function_ref(self_) }.apply(a) {
        Ok(value) => {
            // SAFETY: per this function's `# Safety`, `out_apply` is a valid pointer.
            unsafe { *out_apply = value };
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Frees a function handle; null is a no-op. Futures derived through it stay
/// valid: they hold their own copy of the registration.
///
/// # Safety
///
/// `self_` must be null or an owned function handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_destroy(
    self_: *mut kafka_common_KafkaFuture_BaseFunction_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`,
        // an owned function handle not yet destroyed, that is the `Box::into_raw` of
        // `kafka_common_KafkaFuture_BaseFunction_new`, reclaimed here once. Derived
        // futures hold their own copy, not a borrow of it.
        drop(unsafe { Box::from_raw(self_ as *mut BaseFunctionInner) });
    }
}

/// `KafkaFuture.thenApply(function)`: a new future that, when this one
/// completes normally, resolves to `function.apply(value)`, or fails with
/// the error `apply` returned; a failure of this future propagates unchanged.
///
/// The function runs once, on the thread that first resolves the derived
/// future: the caller of its `get`, or a runtime task for a `_cb`. (The Rust
/// `then_apply` re-evaluates its closure on every `get`, being a pure view
/// over the source; a C function may have side effects, so its first outcome
/// is memoized, as Java runs it once.) The value it produces is a `void *`
/// owned by the C side and never freed by Rust; the source value is borrowed
/// for the call only (module docs). `function` is borrowed: the derived future
/// keeps its own copy of the registration, so the handle may be destroyed right
/// after this returns, while the `self` it was built with must outlive the
/// derived future. The result is owned, freed with
/// [`kafka_common_KafkaFuture_destroy`], and belongs to the same client as
/// this future.
///
/// # Safety
///
/// `self_` must be a valid future handle and `function` a valid function
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_then_apply(
    self_: *const kafka_common_KafkaFuture_t,
    function: *const kafka_common_KafkaFuture_BaseFunction_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle; the
    // borrow lasts for this call only (the derived future clones what it keeps).
    let inner = unsafe { future_ref(self_) };
    // SAFETY: per this function's `# Safety`, `function` is a valid function handle; the
    // borrow lasts for this call only (the registration is copied out below).
    let registration = unsafe { base_function_ref(function) };
    let function = BaseFunctionInner { self_: registration.self_, apply: registration.apply };
    let memo: Mutex<Option<Result<Arc<FfiValue>, Error>>> = Mutex::new(None);
    let derived = inner.future.then_apply(move |source: Arc<FfiValue>| {
        let mut memo = memo.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        memo.get_or_insert_with(|| function.apply(source.ptr()).map(FfiValue::borrowed))
            .clone()
    });
    box_future(derived, inner.runtime.clone(), inner.queue.clone())
}

/// Frees the handle. The value `get` returned becomes invalid, unless a `_cb`
/// delivery is still pending, in which case it stays valid until that
/// callback returns.
///
/// # Safety
///
/// `self_` must be null or a valid future handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_destroy(self_: *mut kafka_common_KafkaFuture_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`,
        // a future handle not yet destroyed, so it still owns the strong reference
        // `box_future`'s `Arc::into_raw` handed out, which this releases once. A pending
        // `_cb` delivery holds its own reference from `future_arc`.
        drop(unsafe { Arc::from_raw(self_ as *const FutureInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
    use crate::ffi::common::fail_point;
    use crate::ffi::common::panic_probe::{Probe, on_value};
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::error_predicates::kafka_common_Error_is_local_timeout_error;
    use crate::ffi::util::{kafka_List_add, kafka_List_destroy, kafka_List_new};
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Counted(i64);
    impl Drop for Counted {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// `kafka_common_KafkaFuture_get` on `f`, with the error's message.
    ///
    /// # Safety
    ///
    /// `f` must be a live future handle.
    unsafe fn get(f: *const kafka_common_KafkaFuture_t) -> Result<*mut c_void, String> {
        let mut out: *mut c_void = std::ptr::null_mut();
        // SAFETY: Test helper: per this function's `# Safety`, `f` is a live future handle,
        // and `out` is a live local.
        let err = unsafe { kafka_common_KafkaFuture_get(f, &mut out) };
        if err.is_null() {
            Ok(out)
        } else {
            // SAFETY: Test helper: `err` is the fresh, non-null error handle `get` returned;
            // its message is copied into a `String` before the handle is destroyed, once.
            let msg = unsafe {
                let msg = CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap().to_string();
                kafka_common_Error_destroy(err);
                msg
            };
            Err(msg)
        }
    }

    #[test]
    fn a_rust_value_is_boxed_once_and_freed_with_the_handle() {
        let before = DROPS.load(Ordering::SeqCst);
        let typed = KafkaFuture::completed_future(Ok(42i64));
        let f = box_future(map_future(&typed), None, None);
        // `map_future` boxes an `i64`; use a counted type for the drop check.
        let counted = KafkaFuture::completed_future(Ok(Arc::new(Mutex::new(0))));
        let f2 = box_future(counted.then_apply(|_| Ok(FfiValue::owned(Counted(7)))), None, None);

        // SAFETY: Test code: `f` and `f2` are fresh `box_future` handles, each destroyed once
        // after its last use. `first` points at the `i64` and `p` at the `Counted` the
        // handles cache, both read before their handle is destroyed. Null is accepted by
        // `kafka_common_KafkaFuture_destroy`.
        unsafe {
            assert_eq!(kafka_common_KafkaFuture_is_done(f), 1);
            let first = get(f).unwrap();
            let second = get(f).unwrap();
            assert_eq!(first, second, "every get answers the same pointer");
            assert_eq!(*(first as *const i64), 42);

            let p = get(f2).unwrap();
            assert_eq!((*(p as *const Counted)).0, 7);
            assert_eq!(DROPS.load(Ordering::SeqCst), before, "the value lives as long as the handle");
            kafka_common_KafkaFuture_destroy(f2);
            assert_eq!(DROPS.load(Ordering::SeqCst), before + 1);
            kafka_common_KafkaFuture_destroy(f);
            kafka_common_KafkaFuture_destroy(std::ptr::null_mut());
        }
    }

    #[test]
    fn completed_future_from_c_delivers_the_borrowed_value_or_the_error() {
        let mut value = 5i32;
        // SAFETY: Test code: each `f` is the fresh handle `completed_future` returned,
        // destroyed once after its last use; the first borrows `value`, a live local that
        // outlives it, and the second consumes `err`, a fresh `box_error` handle the test
        // does not touch again.
        unsafe {
            let f = kafka_common_KafkaFuture_completed_future(
                &mut value as *mut i32 as *const c_void,
                std::ptr::null_mut(),
            );
            assert_eq!(get(f).unwrap(), &mut value as *mut i32 as *mut c_void);
            kafka_common_KafkaFuture_destroy(f);
            assert_eq!(value, 5, "Rust never frees a C-supplied value");

            let err = box_error(Error::timeout("boom"));
            let f = kafka_common_KafkaFuture_completed_future(std::ptr::null(), err);
            assert_eq!(get(f), Err("boom".to_string()));
            assert_eq!(get(f), Err("boom".to_string()), "a failed future keeps failing");
            kafka_common_KafkaFuture_destroy(f);
        }
    }

    #[test]
    fn all_of_resolves_to_null_and_reports_the_first_failure() {
        // SAFETY: Test code: `ok`, `failed`, each `all` and `empty` are fresh future handles,
        // each destroyed once after its last use; `failed` consumes a fresh `box_error`
        // handle. `list` is the fresh `kafka_List_new` handle, used by this thread only,
        // whose elements are those live future handles, and it is destroyed once. Null is
        // accepted by `all_of`.
        unsafe {
            let ok = kafka_common_KafkaFuture_completed_future(std::ptr::null(), std::ptr::null_mut());
            let failed = kafka_common_KafkaFuture_completed_future(
                std::ptr::null(),
                box_error(Error::local_illegal_state("bad")),
            );

            let list = kafka_List_new();
            kafka_List_add(list, ok as *mut c_void);
            let all = kafka_common_KafkaFuture_all_of(list);
            assert!(get(all).unwrap().is_null());
            kafka_common_KafkaFuture_destroy(all);

            kafka_List_add(list, failed as *mut c_void);
            let all = kafka_common_KafkaFuture_all_of(list);
            assert_eq!(get(all), Err("bad".to_string()));
            // The inputs are borrowed: destroying them after `all_of` is fine.
            kafka_List_destroy(list);
            kafka_common_KafkaFuture_destroy(ok);
            kafka_common_KafkaFuture_destroy(failed);
            kafka_common_KafkaFuture_destroy(all);

            let empty = kafka_common_KafkaFuture_all_of(std::ptr::null());
            assert!(get(empty).unwrap().is_null());
            kafka_common_KafkaFuture_destroy(empty);
        }
    }

    #[test]
    fn get_with_timeout_times_out_with_the_local_timeout_error() {
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(map_future(&pending.future()), None, None);
        let mut out: *mut c_void = std::ptr::null_mut();
        // SAFETY: Test code: `f` is the fresh `box_future` handle, destroyed once at the end;
        // `out` is a live local. Each `err` is the fresh, non-null error handle a timed-out
        // get returned, destroyed once. The value read at the end is the `i64` `f` caches.
        unsafe {
            assert_eq!(kafka_common_KafkaFuture_is_done(f), 0);
            let err = kafka_common_KafkaFuture_get_with_timeout(f, 10, &mut out);
            assert!(!err.is_null());
            assert_eq!(kafka_common_Error_is_local_timeout_error(err), 1);
            kafka_common_Error_destroy(err);
            // Negative timeouts are zero, not a panic.
            let err = kafka_common_KafkaFuture_get_with_timeout(f, -5, &mut out);
            assert!(!err.is_null());
            kafka_common_Error_destroy(err);
            // Once completed, the same handle answers the value.
            pending.complete(3);
            assert_eq!(kafka_common_KafkaFuture_is_done(f), 1);
            assert_eq!(*(get(f).unwrap() as *const i64), 3);
            kafka_common_KafkaFuture_destroy(f);
        }
    }

    #[test]
    fn get_blocks_until_another_thread_completes_the_future() {
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(map_future(&pending.future()), None, None);
        let completer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            pending.complete(11);
        });
        // SAFETY: Test code: `f` is the fresh `box_future` handle, read (the `i64` it caches)
        // and then destroyed once; the completer thread never touches the handle.
        unsafe {
            assert_eq!(*(get(f).unwrap() as *const i64), 11);
            completer.join().unwrap();
            kafka_common_KafkaFuture_destroy(f);
        }
    }

    struct Seen {
        value: *mut c_void,
        error: Option<String>,
    }
    // SAFETY: Test code: `value` is only compared, or read on the test thread after the
    // callback that recorded it has returned; `Seen` never dereferences it.
    unsafe impl Send for Seen {}

    /// Copies the `int64_t` behind `value` out while the delivery keeps it
    /// alive: a `_cb` value is valid until the callback returns (module docs).
    unsafe extern "C" fn record_i64(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        assert!(error.is_null());
        // SAFETY: Test code: every registration of this callback passes a live
        // `Mutex<Vec<i64>>` as `opaque`, which outlives the delivery.
        let seen = unsafe { &*(opaque as *const Mutex<Vec<i64>>) };
        // SAFETY: Test code: every registration is on a future of `i64`, and a `_cb` value
        // stays valid until the callback returns (module docs).
        seen.lock().unwrap().push(unsafe { *(value as *const i64) });
    }

    unsafe extern "C" fn record(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        // SAFETY: Test code: every registration of this callback passes a live
        // `Mutex<Vec<Seen>>` as `opaque`, which outlives the delivery.
        let seen = unsafe { &*(opaque as *const Mutex<Vec<Seen>>) };
        let error = if error.is_null() {
            None
        } else {
            // SAFETY: Test code: a non-null `error` is the owned handle the completion
            // hands over; its message is copied into a `String` before it is destroyed, once.
            let msg = unsafe {
                let msg = CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap().to_string();
                kafka_common_Error_destroy(error);
                msg
            };
            Some(msg)
        };
        seen.lock().unwrap().push(Seen { value, error });
    }

    #[test]
    fn get_cb_without_a_client_runs_inline() {
        let seen: Mutex<Vec<Seen>> = Mutex::new(Vec::new());
        let opaque = &seen as *const _ as *mut c_void;
        let mut value = 1i32;
        // SAFETY: Test code: `f` and `failed` are fresh `completed_future` handles, destroyed
        // once at the end; `f` borrows `value`, a live local that outlives it, and `failed`
        // consumes a fresh `box_error` handle. `opaque` is `seen`, the `Mutex<Vec<Seen>>`
        // `record` expects, alive until the inline deliveries are done.
        unsafe {
            let f = kafka_common_KafkaFuture_completed_future(
                &mut value as *mut i32 as *const c_void,
                std::ptr::null_mut(),
            );
            kafka_common_KafkaFuture_get_cb(f, record, opaque);
            kafka_common_KafkaFuture_get_with_timeout_cb(f, 100, record, opaque);
            let failed = kafka_common_KafkaFuture_completed_future(std::ptr::null(), box_error(Error::wakeup("w")));
            kafka_common_KafkaFuture_get_cb(failed, record, opaque);
            let seen = seen.into_inner().unwrap();
            assert_eq!(seen.len(), 3, "delivered before the calls returned");
            assert_eq!(seen[0].value, &mut value as *mut i32 as *mut c_void);
            assert!(seen[0].error.is_none());
            assert_eq!(seen[1].value, seen[0].value);
            assert!(seen[2].value.is_null());
            assert_eq!(seen[2].error.as_deref(), Some("w"));
            kafka_common_KafkaFuture_destroy(f);
            kafka_common_KafkaFuture_destroy(failed);
        }
    }

    #[test]
    fn get_cb_with_a_client_queues_on_its_callbacks_vector() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let queue = Arc::new(CallbackQueue::new());
        let typed = KafkaFuture::completed_future(Ok(9i64));
        let f = box_future(map_future(&typed), Some(runtime.handle().clone()), Some(queue.clone()));

        let seen: Mutex<Vec<i64>> = Mutex::new(Vec::new());
        let opaque = &seen as *const _ as *mut c_void;
        // SAFETY: Test code: `f` is the fresh `box_future` handle of an `i64` future, live at
        // the call and destroyed once just after; `opaque` is `seen`, the `Mutex<Vec<i64>>`
        // `record_i64` expects, alive until the queue below runs the delivery.
        unsafe {
            kafka_common_KafkaFuture_get_cb(f, record_i64, opaque);
            // Destroying the handle before the callback runs keeps the value alive
            // for the delivery — and only for the delivery, which is why
            // `record_i64` reads it inside the callback.
            kafka_common_KafkaFuture_destroy(f);
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while queue.is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(seen.lock().unwrap().is_empty(), "nothing runs until the client drains");
        assert_eq!(queue.execute(), 1);
        assert_eq!(seen.into_inner().unwrap(), vec![9]);
    }

    /// A panic before `get_cb` hands its completion over fires `cb` once with
    /// the panic error: inline for a future with no client, queued on the
    /// client's callbacks vector otherwise, so nothing fires until it drains.
    #[test]
    fn a_panicking_get_cb_delivers_one_panic_completion() {
        let mut value = 1i32;
        // SAFETY: Test code: `f` is the fresh `completed_future` handle, borrowing `value`, a
        // live local that outlives it, and destroyed once after the loop; each `probe` is the
        // live `Probe` `on_value` expects, alive until its inline delivery is checked.
        unsafe {
            let f = kafka_common_KafkaFuture_completed_future(
                &mut value as *mut i32 as *const c_void,
                std::ptr::null_mut(),
            );
            for (export, timeout) in [
                ("kafka_common_KafkaFuture_get_cb", None),
                ("kafka_common_KafkaFuture_get_with_timeout_cb", Some(100)),
            ] {
                let probe = Probe::default();
                fail_point::arm();
                match timeout {
                    None => kafka_common_KafkaFuture_get_cb(f, on_value, probe.as_opaque()),
                    Some(ms) => kafka_common_KafkaFuture_get_with_timeout_cb(f, ms, on_value, probe.as_opaque()),
                }
                probe.assert_one_panic(export, fail_point::MESSAGE);
            }
            kafka_common_KafkaFuture_destroy(f);
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let queue = Arc::new(CallbackQueue::new());
        let typed = KafkaFuture::completed_future(Ok(9i64));
        let f = box_future(map_future(&typed), Some(runtime.handle().clone()), Some(queue.clone()));
        let probe = Probe::default();
        fail_point::arm();
        // SAFETY: Test code: `f` is the fresh `box_future` handle, live until it is destroyed
        // once below; `probe` is the live `Probe` `on_value` expects, alive until the queued
        // delivery has run.
        unsafe { kafka_common_KafkaFuture_get_cb(f, on_value, probe.as_opaque()) };
        assert!(probe.fired().is_empty(), "queued, not fired");
        assert_eq!(queue.execute(), 1);
        probe.assert_one_panic("kafka_common_KafkaFuture_get_cb", fail_point::MESSAGE);
        // The panic came before the task was spawned: no second completion.
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(queue.execute(), 0);
        // SAFETY: Test code: `f` is still the live handle above, destroyed here, once.
        unsafe { kafka_common_KafkaFuture_destroy(f) };
    }

    /// A `get_cb` task the client's runtime drops while it waits (its
    /// `_destroy` shutting the runtime down) still queues its completion,
    /// once, with the destroyed-client error; one whose future had resolved
    /// gets the value; after the queue is closed a `get_cb` runs inline.
    #[test]
    fn a_dropped_get_cb_task_still_delivers_once() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let queue = Arc::new(CallbackQueue::new());
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(
            map_future(&pending.future()),
            Some(runtime.handle().clone()),
            Some(queue.clone()),
        );
        let seen: Mutex<Vec<Seen>> = Mutex::new(Vec::new());
        let opaque = &seen as *const _ as *mut c_void;
        // SAFETY: Test code: `f` is the fresh `box_future` handle of an `i64` future, live for
        // every call below and destroyed once at the end; `opaque` is `seen`, the
        // `Mutex<Vec<Seen>>` `record` expects, alive until every delivery has run. The value
        // read through `seen[2]` is the `i64` `f` caches, read before `f` is destroyed.
        unsafe {
            kafka_common_KafkaFuture_get_cb(f, record, opaque);
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(queue.execute(), 0, "still waiting");

            runtime.shutdown_timeout(Duration::from_secs(5));
            assert!(seen.lock().unwrap().is_empty(), "queued, not fired");
            queue.close();
            assert_eq!(queue.execute(), 1);
            {
                let seen = seen.lock().unwrap();
                assert_eq!(seen.len(), 1);
                assert!(seen[0].value.is_null());
                assert_eq!(seen[0].error.as_deref(), Some(CLIENT_DESTROYED));
            }

            // After the close: inline, with the value once the future resolved.
            kafka_common_KafkaFuture_get_cb(f, record, opaque);
            assert_eq!(seen.lock().unwrap()[1].error.as_deref(), Some(CLIENT_DESTROYED));
            assert!(pending.complete(5));
            kafka_common_KafkaFuture_get_with_timeout_cb(f, 100, record, opaque);
            {
                let seen = seen.lock().unwrap();
                assert_eq!(seen.len(), 3, "each call fired once, inline");
                assert!(seen[2].error.is_none());
                assert_eq!(*(seen[2].value as *const i64), 5);
            }
            assert_eq!(queue.execute(), 0);
            kafka_common_KafkaFuture_destroy(f);
        }
    }

    /// A `PendingGet` dropped undelivered queues one completion: the value of
    /// a resolved future, the destroyed-client error for a pending one.
    #[test]
    fn a_dropped_pending_get_queues_one_completion() {
        let queue = Arc::new(CallbackQueue::new());
        let seen: Mutex<Vec<Seen>> = Mutex::new(Vec::new());
        let opaque = SendPtr(&seen as *const _ as *mut c_void);
        let resolved = KafkaFuture::completed_future(Ok(3i64));
        let pending = KafkaFutureImpl::<i64>::new();
        // Held so the resolved value outlives the job that delivers it.
        let mut held = Vec::new();
        for future in [map_future(&resolved), map_future(&pending.future())] {
            let inner = Arc::new(FutureInner { future, result: OnceLock::new(), runtime: None, queue: None });
            held.push(Arc::clone(&inner));
            drop(PendingGet {
                fn_name: "kafka_common_KafkaFuture_get_cb",
                cb: record,
                opaque,
                inner,
                queue: queue.clone(),
                delivered: false,
            });
        }
        assert_eq!(queue.execute(), 2);
        let seen = seen.into_inner().unwrap();
        assert!(seen[0].error.is_none());
        // SAFETY: Test code: `seen[0].value` is the `i64` the first `FutureInner` caches,
        // kept alive by `held` until after this read.
        assert_eq!(unsafe { *(seen[0].value as *const i64) }, 3);
        assert_eq!(seen[1].error.as_deref(), Some(CLIENT_DESTROYED));
        assert_eq!(queue.execute(), 0);
        drop(held);
    }

    /// A C `BaseFunction`: counts its calls on `self` and answers a pointer to
    /// `self`'s own output slot, doubling the `int64_t` behind `a`.
    struct Doubler {
        calls: AtomicUsize,
        output: Mutex<i64>,
    }

    unsafe extern "C" fn double(
        self_: *mut c_void,
        a: *mut c_void,
        out_apply: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t {
        // SAFETY: Test code: every registration of this function passes a live `Doubler` as
        // `self`, which outlives every future derived through it.
        let doubler = unsafe { &*(self_ as *const Doubler) };
        doubler.calls.fetch_add(1, Ordering::SeqCst);
        let mut output = doubler.output.lock().unwrap();
        // SAFETY: Test code: every source future this function is applied to delivers an
        // `i64`, borrowed for the call (module docs), or the test passes a live `i64`.
        *output = unsafe { *(a as *const i64) } * 2;
        // SAFETY: Test code: `out_apply` is the valid out-pointer `BaseFunctionInner::apply`
        // or the test passes; the slot it receives lives in the `Doubler`.
        unsafe { *out_apply = &raw mut *output as *mut c_void };
        std::ptr::null_mut()
    }

    unsafe extern "C" fn fail(
        _self: *mut c_void,
        _a: *mut c_void,
        _out_apply: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t {
        box_error(Error::local_illegal_argument("apply failed"))
    }

    #[test]
    fn base_function_invoker_reaches_the_c_implementation() {
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        let failing = kafka_common_KafkaFuture_BaseFunction_new(std::ptr::null_mut(), fail);
        let mut a = 21i64;
        let mut out: *mut c_void = std::ptr::null_mut();
        // SAFETY: Test code: `function` (over `doubler`, a live local) and `failing` are fresh
        // function handles, each destroyed once at the end; `a` and `out` are live locals,
        // and `out` points into `doubler` when read. `error` is first null, then the fresh
        // handle `fail` returned, destroyed once after its message is read. Null is accepted
        // by `kafka_common_KafkaFuture_BaseFunction_destroy`.
        unsafe {
            let error = kafka_common_KafkaFuture_BaseFunction_apply(function, &raw mut a as *mut c_void, &raw mut out);
            assert!(error.is_null());
            assert_eq!(*(out as *const i64), 42);
            assert_eq!(doubler.calls.load(Ordering::SeqCst), 1);

            let error = kafka_common_KafkaFuture_BaseFunction_apply(failing, &raw mut a as *mut c_void, &raw mut out);
            assert!(!error.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "apply failed"
            );
            kafka_common_Error_destroy(error);
            kafka_common_KafkaFuture_BaseFunction_destroy(function);
            kafka_common_KafkaFuture_BaseFunction_destroy(failing);
            kafka_common_KafkaFuture_BaseFunction_destroy(std::ptr::null_mut());
        }
    }

    #[test]
    fn then_apply_runs_the_function_once_and_borrows_its_result() {
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        let pending = KafkaFutureImpl::<i64>::new();
        let source = box_future(map_future(&pending.future()), None, None);
        let again = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);

        // SAFETY: Test code: `source`, `derived` and `chained` are fresh future handles and
        // `function` and `again` fresh function handles over `doubler`, a live local that
        // outlives every derived future; each handle is destroyed once after its last use.
        // `first` and the chained value point into `doubler`, and the source value is the
        // `i64` `source` caches, read before it is destroyed.
        unsafe {
            let derived = kafka_common_KafkaFuture_then_apply(source, function);
            // The registration is copied: the handle can go right away.
            kafka_common_KafkaFuture_BaseFunction_destroy(function);
            assert_eq!(kafka_common_KafkaFuture_is_done(derived), 0);
            assert_eq!(
                doubler.calls.load(Ordering::SeqCst),
                0,
                "nothing runs before the source resolves"
            );

            pending.complete(21);
            assert_eq!(kafka_common_KafkaFuture_is_done(derived), 1);
            let first = get(derived).unwrap();
            assert_eq!(
                first,
                &raw const *doubler.output.lock().unwrap() as *mut c_void,
                "the C-owned pointer"
            );
            assert_eq!(*(first as *const i64), 42);
            assert_eq!(get(derived).unwrap(), first);
            assert_eq!(doubler.calls.load(Ordering::SeqCst), 1, "Java runs the function once");

            // Chaining through `then_apply` runs the next step on the first's
            // result; the source is left untouched.
            let chained = kafka_common_KafkaFuture_then_apply(derived, again);
            assert_eq!(*(get(chained).unwrap() as *const i64), 84);
            assert_eq!(doubler.calls.load(Ordering::SeqCst), 2);
            assert_eq!(*(get(source).unwrap() as *const i64), 21);

            kafka_common_KafkaFuture_BaseFunction_destroy(again);
            kafka_common_KafkaFuture_destroy(chained);
            kafka_common_KafkaFuture_destroy(derived);
            kafka_common_KafkaFuture_destroy(source);
        }
        assert_eq!(*doubler.output.lock().unwrap(), 84, "Rust never frees a C-produced value");
    }

    #[test]
    fn then_apply_fails_with_the_functions_error_or_the_sources() {
        let failing = kafka_common_KafkaFuture_BaseFunction_new(std::ptr::null_mut(), fail);
        let typed = KafkaFuture::completed_future(Ok(1i64));
        let source = box_future(map_future(&typed), None, None);
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        // SAFETY: Test code: `source`, `derived`, `failed_source` and `from_failed` are fresh
        // future handles, `failing` and `function` fresh function handles (`function` over
        // `doubler`, a live local that outlives its derived future); each is destroyed once
        // after its last use. `failed_source` consumes a fresh `box_error` handle.
        unsafe {
            let derived = kafka_common_KafkaFuture_then_apply(source, failing);
            assert_eq!(get(derived), Err("apply failed".to_string()));
            assert_eq!(get(derived), Err("apply failed".to_string()), "memoized, like the value");

            // A failed source propagates its error without calling the function.
            let failed_source =
                kafka_common_KafkaFuture_completed_future(std::ptr::null(), box_error(Error::timeout("late")));
            let from_failed = kafka_common_KafkaFuture_then_apply(failed_source, function);
            assert_eq!(get(from_failed), Err("late".to_string()));
            assert_eq!(doubler.calls.load(Ordering::SeqCst), 0);

            kafka_common_KafkaFuture_BaseFunction_destroy(failing);
            kafka_common_KafkaFuture_BaseFunction_destroy(function);
            kafka_common_KafkaFuture_destroy(derived);
            kafka_common_KafkaFuture_destroy(source);
            kafka_common_KafkaFuture_destroy(from_failed);
            kafka_common_KafkaFuture_destroy(failed_source);
        }
    }

    #[test]
    fn then_apply_inherits_the_source_client() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let queue = Arc::new(CallbackQueue::new());
        let typed = KafkaFuture::completed_future(Ok(4i64));
        let source = box_future(map_future(&typed), Some(runtime.handle().clone()), Some(queue.clone()));
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        // SAFETY: Test code: `source` is a fresh future handle and `function` a fresh function
        // handle over `doubler`, a live local that outlives the derived future; `derived` is
        // the handle `then_apply` returns. All three are destroyed once at the end.
        let derived = unsafe { kafka_common_KafkaFuture_then_apply(source, function) };

        let seen: Mutex<Vec<i64>> = Mutex::new(Vec::new());
        let opaque = &seen as *const _ as *mut c_void;
        // SAFETY: Test code: `derived` is the live handle above, delivering the `i64` that
        // `double` writes into `doubler`; `opaque` is `seen`, the `Mutex<Vec<i64>>`
        // `record_i64` expects, alive until the queue below runs the delivery.
        unsafe { kafka_common_KafkaFuture_get_cb(derived, record_i64, opaque) };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while queue.is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(seen.lock().unwrap().is_empty(), "queued on the source's client, not run inline");
        assert_eq!(queue.execute(), 1);
        assert_eq!(seen.into_inner().unwrap(), vec![8]);
        // SAFETY: Test code: each handle is still live and destroyed here, once.
        unsafe {
            kafka_common_KafkaFuture_BaseFunction_destroy(function);
            kafka_common_KafkaFuture_destroy(derived);
            kafka_common_KafkaFuture_destroy(source);
        }
    }
}
