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
//! `_execute_callbacks`); a future with no client invokes the callback inline,
//! on the calling thread, before `get_cb` returns.

// FFI type names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::common::{Error, KafkaFuture};
use crate::ffi::callback_queue::{CallbackQueue, SendPtr};
use crate::ffi::common::{box_error, kafka_common_Error_t, take_error};
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

// SAFETY: the pointer is either a boxed Rust value (owned here, `Send` by the
// bound on `owned`) or an opaque pointer C supplied and keeps alive; the struct
// never dereferences it.
unsafe impl Send for FfiValue {}
unsafe impl Sync for FfiValue {}

impl FfiValue {
    /// Boxes a Rust value behind the `void *`, freed with the handle.
    pub(crate) fn owned<T: Send + 'static>(value: T) -> Arc<Self> {
        Arc::new(Self {
            ptr: Box::into_raw(Box::new(value)) as *mut c_void,
            destroy: Some(destroy_boxed::<T>),
        })
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
            // SAFETY: `ptr` came from `Box::into_raw` of the type `destroy` was
            // instantiated for, and this is the only drop of it (the `Arc`
            // sharing this value has just reached zero).
            unsafe { destroy(self.ptr) };
        }
    }
}

/// A Rust future already mapped to its C value.
pub(crate) type FfiFuture = KafkaFuture<Arc<FfiValue>>;

/// Maps a typed future to its C value, boxing `T` when it resolves.
// wired by the first future over a Rust value, `KafkaProducer_send` (Phase 2)
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) fn map_future<T>(future: &KafkaFuture<T>) -> FfiFuture
where
    T: Clone + Send + Sync + 'static,
{
    future.then_apply(FfiValue::owned)
}

/// Maps a `Void` future to the `NULL` it delivers.
pub(crate) fn map_void_future(future: &KafkaFuture<()>) -> FfiFuture {
    future.then_apply(|()| FfiValue::null())
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
/// `future` must be a valid handle from [`box_future`].
unsafe fn future_ref<'a>(future: *const kafka_common_KafkaFuture_t) -> &'a FutureInner {
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

/// Delivers a resolution to a `_cb_t` callback.
fn deliver(cb: kafka_common_KafkaFuture_get_cb_t, result: Result<Arc<FfiValue>, Error>, opaque: SendPtr) {
    match result {
        // SAFETY: `cb` is the function pointer the C caller passed, called with
        // its own opaque pointer.
        Ok(value) => unsafe { cb(value.ptr(), std::ptr::null_mut(), opaque.0) },
        Err(e) => unsafe { cb(std::ptr::null_mut(), box_error(e), opaque.0) },
    }
}

/// Queues the delivery on the owning client's callbacks vector, or runs it
/// inline when the future has no client.
fn get_cb(
    future: *const kafka_common_KafkaFuture_t,
    timeout: Option<Duration>,
    cb: kafka_common_KafkaFuture_get_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: `future` is a valid handle (the caller's precondition).
    let inner = unsafe { future_arc(future) };
    let opaque = SendPtr(opaque);
    match (inner.runtime.clone(), inner.queue.clone()) {
        (Some(runtime), Some(queue)) => {
            runtime.spawn(async move {
                let result = inner.resolve_async(timeout).await;
                // `inner` moves into the job so the value outlives a handle
                // destroyed before the callback runs.
                queue.push(Box::new(move || {
                    deliver(cb, result, opaque);
                    drop(inner);
                }));
            });
        },
        _ => deliver(cb, inner.resolve(timeout), opaque),
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_completed_future(
    result: *const c_void,
    result_error: *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get(
    self_: *const kafka_common_KafkaFuture_t,
    out_get: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    match unsafe { future_ref(self_) }.resolve(None) {
        Ok(value) => {
            unsafe { *out_get = value.ptr() };
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// The non-blocking form of [`kafka_common_KafkaFuture_get`]: `cb` receives
/// the value or the error through the owning client's `_execute_callbacks`,
/// or inline before this returns when the future belongs to no client.
///
/// # Safety
///
/// `self_` must be a valid future handle; `opaque` is handed back to `cb`
/// untouched.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get_cb(
    self_: *const kafka_common_KafkaFuture_t,
    cb: kafka_common_KafkaFuture_get_cb_t,
    opaque: *mut c_void,
) {
    get_cb(self_, None, cb, opaque);
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get_with_timeout(
    self_: *const kafka_common_KafkaFuture_t,
    timeout: i64,
    out_get_with_timeout: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    match unsafe { future_ref(self_) }.resolve(Some(timeout_ms(timeout))) {
        Ok(value) => {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get_with_timeout_cb(
    self_: *const kafka_common_KafkaFuture_t,
    timeout: i64,
    cb: kafka_common_KafkaFuture_get_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    get_cb(self_, Some(timeout_ms(timeout)), cb, opaque);
}

/// `KafkaFuture.isDone()`: 1 when the future has resolved, 0 otherwise.
///
/// # Safety
///
/// `self_` must be a valid future handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_is_done(self_: *const kafka_common_KafkaFuture_t) -> i8 {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_all_of(
    futures: *const kafka_List_t,
) -> *mut kafka_common_KafkaFuture_t {
    let inners: Vec<&FutureInner> = unsafe { list_elements(futures) }
        .iter()
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
/// [`kafka_common_KafkaFuture_BaseFunction_new`].
unsafe fn base_function_ref<'a>(function: *const kafka_common_KafkaFuture_BaseFunction_t) -> &'a BaseFunctionInner {
    unsafe { &*(function as *const BaseFunctionInner) }
}

/// Registers a C `BaseFunction`: `self_` is the caller's opaque pointer
/// passed first to `apply`. The caller owns `self_` and keeps it alive until
/// every future derived through this function has been destroyed; the handle
/// itself may be destroyed as soon as `then_apply` returns, since the derived
/// future keeps its own copy of the registration.
///
/// Owned, freed with [`kafka_common_KafkaFuture_BaseFunction_destroy`].
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_apply(
    self_: *const kafka_common_KafkaFuture_BaseFunction_t,
    a: *mut c_void,
    out_apply: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    match unsafe { base_function_ref(self_) }.apply(a) {
        Ok(value) => {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_destroy(
    self_: *mut kafka_common_KafkaFuture_BaseFunction_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut BaseFunctionInner) });
    }
}

/// The derived future of `then_apply`: the source's value mapped through a C
/// `BaseFunction`, run once.
///
/// Java runs the function exactly once per derived future. The Rust
/// `then_apply` re-evaluates its closure on every `get` of the derived future
/// (it is a pure view over the source); a C function may have side effects,
/// so the first outcome is memoized here and every later evaluation answers
/// it. The source value is borrowed for the call only; the result is a C-owned
/// pointer Rust never frees.
///
/// # Safety
///
/// `function` must be a valid function handle.
unsafe fn then_apply(
    self_: *const kafka_common_KafkaFuture_t,
    function: *const kafka_common_KafkaFuture_BaseFunction_t,
) -> *mut kafka_common_KafkaFuture_t {
    let inner = unsafe { future_ref(self_) };
    let registration = unsafe { base_function_ref(function) };
    let function = BaseFunctionInner { self_: registration.self_, apply: registration.apply };
    let memo: Mutex<Option<Result<Arc<FfiValue>, Error>>> = Mutex::new(None);
    let derived = inner.future.then_apply_try(move |source: Arc<FfiValue>| {
        let mut memo = memo.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        memo.get_or_insert_with(|| function.apply(source.ptr()).map(FfiValue::borrowed))
            .clone()
    });
    box_future(derived, inner.runtime.clone(), inner.queue.clone())
}

/// `KafkaFuture.thenApply(function)`: a new future that, when this one
/// completes normally, resolves to `function.apply(value)`, or fails with
/// the error `apply` returned; a failure of this future propagates unchanged.
///
/// The function runs once, on the thread that first resolves the derived
/// future: the caller of its `get`, or a runtime task for a `_cb`. The value
/// it produces is a `void *` owned by the C side and never freed by Rust; the
/// source value is borrowed for the call only (module docs). `function` is
/// borrowed: the derived future keeps its own copy of the registration, so the
/// handle may be destroyed right after this returns, while the `self` it was
/// built with must outlive the derived future. The result is owned, freed with
/// [`kafka_common_KafkaFuture_destroy`], and belongs to the same client as
/// this future.
///
/// # Safety
///
/// `self_` must be a valid future handle and `function` a valid function
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_then_apply(
    self_: *const kafka_common_KafkaFuture_t,
    function: *const kafka_common_KafkaFuture_BaseFunction_t,
) -> *mut kafka_common_KafkaFuture_t {
    unsafe { then_apply(self_, function) }
}

/// The fallible `then_apply` the Rust API adds beside Java's (`then_apply_try`,
/// `rust-only`): in C the two coincide, because every `apply` already carries
/// the error slot. It exists so that the C surface names every public Rust
/// method; see [`kafka_common_KafkaFuture_then_apply`] for the contract.
///
/// # Safety
///
/// `self_` must be a valid future handle and `function` a valid function
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_then_apply_try(
    self_: *const kafka_common_KafkaFuture_t,
    function: *const kafka_common_KafkaFuture_BaseFunction_t,
) -> *mut kafka_common_KafkaFuture_t {
    unsafe { then_apply(self_, function) }
}

/// Frees the handle. The value `get` returned becomes invalid, unless a `_cb`
/// delivery is still pending, in which case it stays valid until that
/// callback returns.
///
/// # Safety
///
/// `self_` must be null or a valid future handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_destroy(self_: *mut kafka_common_KafkaFuture_t) {
    if !self_.is_null() {
        drop(unsafe { Arc::from_raw(self_ as *const FutureInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
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

    unsafe fn get(f: *const kafka_common_KafkaFuture_t) -> Result<*mut c_void, String> {
        let mut out: *mut c_void = std::ptr::null_mut();
        let err = unsafe { kafka_common_KafkaFuture_get(f, &mut out) };
        if err.is_null() {
            Ok(out)
        } else {
            let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(err)) }
                .to_str()
                .unwrap()
                .to_string();
            unsafe { kafka_common_Error_destroy(err) };
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
        let f2 = box_future(counted.then_apply(|_| FfiValue::owned(Counted(7))), None, None);

        assert_eq!(unsafe { kafka_common_KafkaFuture_is_done(f) }, 1);
        let first = unsafe { get(f) }.unwrap();
        let second = unsafe { get(f) }.unwrap();
        assert_eq!(first, second, "every get answers the same pointer");
        assert_eq!(unsafe { *(first as *const i64) }, 42);

        let p = unsafe { get(f2) }.unwrap();
        assert_eq!(unsafe { (*(p as *const Counted)).0 }, 7);
        assert_eq!(DROPS.load(Ordering::SeqCst), before, "the value lives as long as the handle");
        unsafe { kafka_common_KafkaFuture_destroy(f2) };
        assert_eq!(DROPS.load(Ordering::SeqCst), before + 1);
        unsafe { kafka_common_KafkaFuture_destroy(f) };
        unsafe { kafka_common_KafkaFuture_destroy(std::ptr::null_mut()) };
    }

    #[test]
    fn completed_future_from_c_delivers_the_borrowed_value_or_the_error() {
        let mut value = 5i32;
        let f = unsafe {
            kafka_common_KafkaFuture_completed_future(&mut value as *mut i32 as *const c_void, std::ptr::null_mut())
        };
        assert_eq!(unsafe { get(f) }.unwrap(), &mut value as *mut i32 as *mut c_void);
        unsafe { kafka_common_KafkaFuture_destroy(f) };
        assert_eq!(value, 5, "Rust never frees a C-supplied value");

        let err = box_error(Error::timeout("boom"));
        let f = unsafe { kafka_common_KafkaFuture_completed_future(std::ptr::null(), err) };
        assert_eq!(unsafe { get(f) }, Err("boom".to_string()));
        assert_eq!(unsafe { get(f) }, Err("boom".to_string()), "a failed future keeps failing");
        unsafe { kafka_common_KafkaFuture_destroy(f) };
    }

    #[test]
    fn all_of_resolves_to_null_and_reports_the_first_failure() {
        let ok = unsafe { kafka_common_KafkaFuture_completed_future(std::ptr::null(), std::ptr::null_mut()) };
        let failed = unsafe {
            kafka_common_KafkaFuture_completed_future(std::ptr::null(), box_error(Error::local_illegal_state("bad")))
        };

        let list = kafka_List_new();
        unsafe { kafka_List_add(list, ok as *mut c_void) };
        let all = unsafe { kafka_common_KafkaFuture_all_of(list) };
        assert!(unsafe { get(all) }.unwrap().is_null());
        unsafe { kafka_common_KafkaFuture_destroy(all) };

        unsafe { kafka_List_add(list, failed as *mut c_void) };
        let all = unsafe { kafka_common_KafkaFuture_all_of(list) };
        assert_eq!(unsafe { get(all) }, Err("bad".to_string()));
        // The inputs are borrowed: destroying them after `all_of` is fine.
        unsafe {
            kafka_List_destroy(list);
            kafka_common_KafkaFuture_destroy(ok);
            kafka_common_KafkaFuture_destroy(failed);
            kafka_common_KafkaFuture_destroy(all);
        }

        let empty = unsafe { kafka_common_KafkaFuture_all_of(std::ptr::null()) };
        assert!(unsafe { get(empty) }.unwrap().is_null());
        unsafe { kafka_common_KafkaFuture_destroy(empty) };
    }

    #[test]
    fn get_with_timeout_times_out_with_the_local_timeout_error() {
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(map_future(&pending.future()), None, None);
        assert_eq!(unsafe { kafka_common_KafkaFuture_is_done(f) }, 0);
        let mut out: *mut c_void = std::ptr::null_mut();
        let err = unsafe { kafka_common_KafkaFuture_get_with_timeout(f, 10, &mut out) };
        assert!(!err.is_null());
        assert_eq!(unsafe { kafka_common_Error_is_local_timeout_error(err) }, 1);
        unsafe { kafka_common_Error_destroy(err) };
        // Negative timeouts are zero, not a panic.
        let err = unsafe { kafka_common_KafkaFuture_get_with_timeout(f, -5, &mut out) };
        assert!(!err.is_null());
        unsafe { kafka_common_Error_destroy(err) };
        // Once completed, the same handle answers the value.
        pending.complete(3);
        assert_eq!(unsafe { kafka_common_KafkaFuture_is_done(f) }, 1);
        assert_eq!(unsafe { *(get(f).unwrap() as *const i64) }, 3);
        unsafe { kafka_common_KafkaFuture_destroy(f) };
    }

    #[test]
    fn get_blocks_until_another_thread_completes_the_future() {
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(map_future(&pending.future()), None, None);
        let completer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            pending.complete(11);
        });
        assert_eq!(unsafe { *(get(f).unwrap() as *const i64) }, 11);
        completer.join().unwrap();
        unsafe { kafka_common_KafkaFuture_destroy(f) };
    }

    struct Seen {
        value: *mut c_void,
        error: Option<String>,
    }
    unsafe impl Send for Seen {}

    /// Copies the `int64_t` behind `value` out while the delivery keeps it
    /// alive: a `_cb` value is valid until the callback returns (module docs).
    unsafe extern "C" fn record_i64(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        assert!(error.is_null());
        let seen = unsafe { &*(opaque as *const Mutex<Vec<i64>>) };
        seen.lock().unwrap().push(unsafe { *(value as *const i64) });
    }

    unsafe extern "C" fn record(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        let seen = unsafe { &*(opaque as *const Mutex<Vec<Seen>>) };
        let error = if error.is_null() {
            None
        } else {
            let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }
                .to_str()
                .unwrap()
                .to_string();
            unsafe { kafka_common_Error_destroy(error) };
            Some(msg)
        };
        seen.lock().unwrap().push(Seen { value, error });
    }

    #[test]
    fn get_cb_without_a_client_runs_inline() {
        let seen: Mutex<Vec<Seen>> = Mutex::new(Vec::new());
        let opaque = &seen as *const _ as *mut c_void;
        let mut value = 1i32;
        let f = unsafe {
            kafka_common_KafkaFuture_completed_future(&mut value as *mut i32 as *const c_void, std::ptr::null_mut())
        };
        unsafe { kafka_common_KafkaFuture_get_cb(f, record, opaque) };
        unsafe { kafka_common_KafkaFuture_get_with_timeout_cb(f, 100, record, opaque) };
        let failed =
            unsafe { kafka_common_KafkaFuture_completed_future(std::ptr::null(), box_error(Error::wakeup("w"))) };
        unsafe { kafka_common_KafkaFuture_get_cb(failed, record, opaque) };
        let seen = seen.into_inner().unwrap();
        assert_eq!(seen.len(), 3, "delivered before the calls returned");
        assert_eq!(seen[0].value, &mut value as *mut i32 as *mut c_void);
        assert!(seen[0].error.is_none());
        assert_eq!(seen[1].value, seen[0].value);
        assert!(seen[2].value.is_null());
        assert_eq!(seen[2].error.as_deref(), Some("w"));
        unsafe {
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
        unsafe { kafka_common_KafkaFuture_get_cb(f, record_i64, opaque) };
        // Destroying the handle before the callback runs keeps the value alive
        // for the delivery — and only for the delivery, which is why
        // `record_i64` reads it inside the callback.
        unsafe { kafka_common_KafkaFuture_destroy(f) };

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while queue.is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(seen.lock().unwrap().is_empty(), "nothing runs until the client drains");
        assert_eq!(queue.execute(), 1);
        assert_eq!(seen.into_inner().unwrap(), vec![9]);
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
        let doubler = unsafe { &*(self_ as *const Doubler) };
        doubler.calls.fetch_add(1, Ordering::SeqCst);
        let mut output = doubler.output.lock().unwrap();
        *output = unsafe { *(a as *const i64) } * 2;
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
        let mut a = 21i64;
        let mut out: *mut c_void = std::ptr::null_mut();
        let error =
            unsafe { kafka_common_KafkaFuture_BaseFunction_apply(function, &raw mut a as *mut c_void, &raw mut out) };
        assert!(error.is_null());
        assert_eq!(unsafe { *(out as *const i64) }, 42);
        assert_eq!(doubler.calls.load(Ordering::SeqCst), 1);

        let failing = kafka_common_KafkaFuture_BaseFunction_new(std::ptr::null_mut(), fail);
        let error =
            unsafe { kafka_common_KafkaFuture_BaseFunction_apply(failing, &raw mut a as *mut c_void, &raw mut out) };
        assert!(!error.is_null());
        assert_eq!(
            unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }.to_str().unwrap(),
            "apply failed"
        );
        unsafe {
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

        let derived = unsafe { kafka_common_KafkaFuture_then_apply(source, function) };
        // The registration is copied: the handle can go right away.
        unsafe { kafka_common_KafkaFuture_BaseFunction_destroy(function) };
        assert_eq!(unsafe { kafka_common_KafkaFuture_is_done(derived) }, 0);
        assert_eq!(
            doubler.calls.load(Ordering::SeqCst),
            0,
            "nothing runs before the source resolves"
        );

        pending.complete(21);
        assert_eq!(unsafe { kafka_common_KafkaFuture_is_done(derived) }, 1);
        let first = unsafe { get(derived) }.unwrap();
        assert_eq!(
            first,
            &raw const *doubler.output.lock().unwrap() as *mut c_void,
            "the C-owned pointer"
        );
        assert_eq!(unsafe { *(first as *const i64) }, 42);
        assert_eq!(unsafe { get(derived) }.unwrap(), first);
        assert_eq!(doubler.calls.load(Ordering::SeqCst), 1, "Java runs the function once");

        // Chaining through `then_apply_try` runs the next step on the first's
        // result; the source is left untouched.
        let again = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        let chained = unsafe { kafka_common_KafkaFuture_then_apply_try(derived, again) };
        assert_eq!(unsafe { *(get(chained).unwrap() as *const i64) }, 84);
        assert_eq!(doubler.calls.load(Ordering::SeqCst), 2);
        assert_eq!(unsafe { *(get(source).unwrap() as *const i64) }, 21);

        unsafe {
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
        let derived = unsafe { kafka_common_KafkaFuture_then_apply(source, failing) };
        assert_eq!(unsafe { get(derived) }, Err("apply failed".to_string()));
        assert_eq!(
            unsafe { get(derived) },
            Err("apply failed".to_string()),
            "memoized, like the value"
        );

        // A failed source propagates its error without calling the function.
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        let failed_source =
            unsafe { kafka_common_KafkaFuture_completed_future(std::ptr::null(), box_error(Error::timeout("late"))) };
        let from_failed = unsafe { kafka_common_KafkaFuture_then_apply(failed_source, function) };
        assert_eq!(unsafe { get(from_failed) }, Err("late".to_string()));
        assert_eq!(doubler.calls.load(Ordering::SeqCst), 0);

        unsafe {
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
        let derived = unsafe { kafka_common_KafkaFuture_then_apply(source, function) };

        let seen: Mutex<Vec<i64>> = Mutex::new(Vec::new());
        let opaque = &seen as *const _ as *mut c_void;
        unsafe { kafka_common_KafkaFuture_get_cb(derived, record_i64, opaque) };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while queue.is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(seen.lock().unwrap().is_empty(), "queued on the source's client, not run inline");
        assert_eq!(queue.execute(), 1);
        assert_eq!(seen.into_inner().unwrap(), vec![8]);
        unsafe {
            kafka_common_KafkaFuture_BaseFunction_destroy(function);
            kafka_common_KafkaFuture_destroy(derived);
            kafka_common_KafkaFuture_destroy(source);
        }
    }
}
