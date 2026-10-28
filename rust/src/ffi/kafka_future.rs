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
//! The Rust side maps its typed future to a `KafkaFuture<Arc<FfiValue>>`
//! ([`FfiValue::owned`]) lazily, when its value is read, so a future nobody
//! reads boxes nothing; the handle caches the value it read, freed when the
//! last handle holding it goes away.
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
//! driven on that client's runtime. A future that belongs to no client can only
//! be built from `completed_future`, directly or through `then_apply` and
//! `all_of` over such futures, whose functions run inline, so it is already
//! complete: `get` answers it on
//! the calling thread without waiting and without any runtime, and a timeout
//! never elapses. `get_cb` queues its callback on the owning client's callbacks
//! vector (drained by that client's `__execute_callbacks`); a future with no
//! client invokes the callback inline, on the calling thread, before `get_cb`
//! returns. A `then_apply` function over a client's future runs from that
//! client's `__execute_callbacks`, queued when the source completes, unless
//! the source is already complete when `then_apply` is called, in which case it
//! runs inline. A blocking `get` on a future `then_apply` returned runs the
//! client's pending callbacks, not only its own `apply`, on the calling thread
//! while it waits.
//!
//! A future may outlive its client. The client's `_destroy` fires the callback
//! of every `get_cb` still waiting, exactly once, before it returns: with the
//! value when the future has resolved, else with a
//! `kafka_common_Error_LOCAL_ILLEGAL_STATE` error, "The client was
//! destroyed before the future completed". A `get_cb` after that invokes the
//! callback inline, the same way, as for a future with no client. A blocking
//! `get` still waiting when the client is destroyed returns the same way once
//! the destroy has stopped the client's background work, and a `get` after
//! that returns at once. While the destroy stops the client's runtime, a
//! `get_with_timeout` waits without its timeout, since the runtime's timers
//! stop with it.

// FFI type names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use futures_util::FutureExt;

use crate::common::internals::KafkaFutureImpl;
use crate::common::{Error, KafkaFuture};
use crate::ffi::callback_queue::{CallbackJob, CallbackQueue, SendPtr};
use crate::ffi::common::fail_with;
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
// - a value boxed by `owned`, `Sync` by the bound on `owned`;
// - a C handle made by `handle`, whose caller guarantees, per `handle`'s doc,
//   that its accessors may run from several threads at once;
// - C's own pointee.
unsafe impl Sync for FfiValue {}

impl FfiValue {
    /// Boxes a Rust value behind the `void *`, freed with the handle.
    pub(crate) fn owned<T: Send + Sync + 'static>(value: T) -> Arc<Self> {
        Arc::new(Self {
            ptr: Box::into_raw(Box::new(value)) as *mut c_void,
            destroy: Some(destroy_boxed::<T>),
        })
    }

    /// Wraps an owned C handle (a `<Type>_t *` from one of the `box_*`
    /// helpers), freed with the handle through its `_destroy`.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or an owned handle that `destroy` frees exactly:
    /// `destroy` is the destructor paired with the helper that made `ptr` (a
    /// `box_*` helper and its `_destroy`, `into_c_string` and
    /// `destroy_string_element`), and nothing else frees `ptr`. The handle must
    /// also be one that may be freed from any thread, and read through its
    /// accessors from several threads at once: `Drop` frees it and the `Send`
    /// and `Sync` impls share it on that promise.
    pub(crate) unsafe fn handle(ptr: *mut c_void, destroy: unsafe fn(*mut c_void)) -> Arc<Self> {
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
    future.map_lazily(FfiValue::owned)
}

/// Maps a typed future to the C handle `boxer` builds from its value when it
/// resolves, freed with `destroy` (the handle type's `_destroy`) together with
/// the future's value.
///
/// # Safety
///
/// Each pointer `boxer` returns must meet [`FfiValue::handle`]'s `# Safety`
/// with `destroy`: null or an owned handle `destroy` frees exactly, which may
/// be freed from any thread and read from several threads at once.
pub(crate) unsafe fn map_future_handle<T, F>(
    future: &KafkaFuture<T>,
    boxer: F,
    destroy: unsafe fn(*mut c_void),
) -> FfiFuture
where
    T: Clone + Send + Sync + 'static,
    F: Fn(T) -> *mut c_void + Send + Sync + 'static,
{
    future.map_lazily(move |value| {
        // SAFETY: Per this function's `# Safety`, the pointer `boxer` returns and `destroy` meet
        // `FfiValue::handle`'s `# Safety`.
        unsafe { FfiValue::handle(boxer(value), destroy) }
    })
}

/// Maps a `Void` future to the `NULL` it delivers.
pub(crate) fn map_void_future(future: &KafkaFuture<()>) -> FfiFuture {
    future.map_lazily(|()| FfiValue::null())
}

// ---------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------

/// What a `kafka_common_KafkaFuture_t` points at, shared between the handle
/// and any in-flight `_cb` delivery.
pub(crate) struct FutureInner {
    future: FfiFuture,
    /// The value `get` returned, cached so every `get` answers the same
    /// pointer and the value lives as long as the handle.
    result: OnceLock<Arc<FfiValue>>,
    /// The runtime of the client that produced the future, when any. A
    /// future without one is already complete (module docs), so nothing drives
    /// it.
    runtime: Option<tokio::runtime::Handle>,
    /// The callbacks vector of that client, when any.
    queue: Option<Arc<CallbackQueue>>,
    /// Whether the future waits for a callback in `queue` to report, so a
    /// blocking `get` runs the queued callbacks on the calling thread while
    /// it waits (see [`box_report_future`]).
    drains_queue: bool,
    /// The handles this future was built from (`then_apply`'s source,
    /// `all_of`'s futures), kept as long as it is: a future `then_apply`
    /// returned cancels its function once nothing can observe it
    /// ([`ApplyGate`]), and a future built on it observes it.
    sources: Vec<Arc<FutureInner>>,
    /// For a future `then_apply` returned, the gate its function runs through.
    apply_gate: Option<Arc<ApplyGate>>,
}

impl Drop for FutureInner {
    fn drop(&mut self) {
        if let Some(gate) = &self.apply_gate {
            gate.cancel();
        }
    }
}

/// Runs a `then_apply` function at most once, unless the future it completes
/// is dropped first: the last reference to that future going (its handle, a
/// pending `get_cb`, a future built on it) cancels a function not started
/// yet, so the C caller may free the function's `self_` after it. A function
/// running on another thread is waited for; one running on the dropping
/// thread (a function destroying its own future) is not.
struct ApplyGate {
    state: Mutex<ApplyState>,
    finished: Condvar,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ApplyState {
    Pending,
    Running(ThreadId),
    Done,
}

/// Marks the function finished when dropped, a panic included.
struct ApplyRun<'a>(&'a ApplyGate);

impl Drop for ApplyRun<'_> {
    fn drop(&mut self) {
        *self.0.state.lock().unwrap_or_else(PoisonError::into_inner) = ApplyState::Done;
        self.0.finished.notify_all();
    }
}

impl ApplyGate {
    fn new() -> Self {
        Self { state: Mutex::new(ApplyState::Pending), finished: Condvar::new() }
    }

    /// Starts the function, or `None` when it was cancelled.
    fn enter(&self) -> Option<ApplyRun<'_>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match *state {
            ApplyState::Pending => {
                *state = ApplyState::Running(std::thread::current().id());
                Some(ApplyRun(self))
            },
            ApplyState::Running(_) | ApplyState::Done => None,
        }
    }

    fn cancel(&self) {
        let current = std::thread::current().id();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            match *state {
                ApplyState::Running(thread) if thread != current => {
                    state = self.finished.wait(state).unwrap_or_else(PoisonError::into_inner);
                },
                _ => break,
            }
        }
        *state = ApplyState::Done;
    }
}

impl FutureInner {
    fn new(
        future: FfiFuture,
        runtime: Option<tokio::runtime::Handle>,
        queue: Option<Arc<CallbackQueue>>,
        drains_queue: bool,
    ) -> Self {
        Self {
            future,
            result: OnceLock::new(),
            runtime,
            queue,
            drains_queue,
            sources: Vec::new(),
            apply_gate: None,
        }
    }

    /// Resolves the future, blocking the calling thread.
    fn resolve(&self, timeout: Option<Duration>) -> Result<Arc<FfiValue>, Error> {
        if let Some(cached) = self.result.get() {
            return Ok(cached.clone());
        }
        match &self.runtime {
            Some(runtime) => {
                let value = match &self.queue {
                    // The client is stopped: only the destroy's last callbacks can
                    // still complete a future of it, and a wait here may be one of
                    // them, so it answers what the future holds now.
                    Some(queue) if queue.is_client_stopped() => return self.resolve_now(),
                    Some(queue) if self.drains_queue => self.resolve_draining(runtime, queue, timeout)?,
                    Some(queue) => block_on(runtime, self.resolve_until_stopped(queue, timeout))?,
                    None => block_on(runtime, self.resolve_future(timeout))?,
                };
                Ok(self.result.get_or_init(|| value).clone())
            },
            // Already complete (module docs): answered on this thread, with no
            // runtime, so the timeout, which needs a timer, is not armed.
            None => self.resolve_ready(NOT_COMPLETE_WITHOUT_CLIENT),
        }
    }

    /// Resolves the future, blocking the calling thread, and runs the
    /// callbacks queued on `queue` meanwhile, on this thread and outside the
    /// runtime, so they may call blocking entry points themselves: the
    /// future waits for one of them to report, which the thread pumping the
    /// queue may be this one. From inside one of those callbacks the nested
    /// drain runs nothing, so a `get` there waits for whatever reports from
    /// elsewhere, and on its own record forever, as a Java `get` inside a
    /// producer callback does.
    fn resolve_draining(
        &self,
        runtime: &tokio::runtime::Handle,
        queue: &CallbackQueue,
        timeout: Option<Duration>,
    ) -> Result<Arc<FfiValue>, Error> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            queue.execute();
            let stopping = queue.is_client_stopping();
            let step = block_on(runtime, async {
                let remaining = match stopping {
                    true => None,
                    false => deadline.map(|deadline| deadline.saturating_duration_since(Instant::now())),
                };
                // Every branch is cancellation-safe: the stop signals are latches, `get` only waits
                // for the shared completion, and a push that races `pushed` leaves its permit
                // behind. Once the client is stopping its runtime's timers stop too, so the wait
                // drops its timeout: the stopping branch comes before the timed wait, so a wait the
                // shutdown wakes goes round untimed instead of polling its stopped timer.
                tokio::select! {
                    biased;
                    () = queue.client_stopped() => Some(self.resolve_now()),
                    () = queue.client_stopping(), if !stopping => None,
                    result = self.resolve_future(remaining) => Some(result),
                    () = queue.pushed() => None,
                }
            });
            if let Some(result) = step {
                return result;
            }
        }
    }

    /// Resolves the future, or, once the client stops
    /// ([`CallbackQueue::mark_client_stopped`]), answers what it holds then
    /// ([`Self::resolve_now`]): a client whose `_destroy` had to cancel its
    /// background task where it stood (an admin client inside a TCP connect
    /// that outlasted the destroy's join) leaves its futures pending forever.
    /// While the client is stopping
    /// ([`CallbackQueue::mark_client_stopping`]) its runtime's timers stop,
    /// so the wait goes on without its timeout.
    async fn resolve_until_stopped(
        &self,
        queue: &CallbackQueue,
        timeout: Option<Duration>,
    ) -> Result<Arc<FfiValue>, Error> {
        // Every branch is cancellation-safe: the stop signals are latches, and `get` only waits
        // for the shared completion. The stop signals come first, so a wait the runtime's
        // shutdown wakes is not polled into its stopped timer.
        let timed = async {
            tokio::select! {
                biased;
                () = queue.client_stopped() => Some(self.resolve_now()),
                () = queue.client_stopping() => None,
                result = self.resolve_future(timeout) => Some(result),
            }
        };
        if let Some(result) = timed.await {
            return result;
        }
        tokio::select! {
            biased;
            () = queue.client_stopped() => self.resolve_now(),
            result = self.resolve_future(None) => result,
        }
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
        self.resolve_ready(CLIENT_DESTROYED)
    }

    /// Answers the future's value, or its failure, when it already has one,
    /// on the calling thread and without a runtime; a future still pending
    /// fails with a `LocalIllegalState` error carrying `pending`, because
    /// nothing is left that could complete it.
    ///
    /// `get` is polled once, never `get_with_timeout`: the timeout of a
    /// derived future (`then_apply`, `all_of`) arms a Tokio timer, which
    /// panics outside a runtime, and a future that is already complete
    /// cannot time out.
    fn resolve_ready(&self, pending: &'static str) -> Result<Arc<FfiValue>, Error> {
        if let Some(cached) = self.result.get() {
            return Ok(cached.clone());
        }
        match self.future.get().now_or_never() {
            Some(result) => {
                let value = result?;
                Ok(self.result.get_or_init(|| value).clone())
            },
            None => Err(Error::local_illegal_state(pending)),
        }
    }

    async fn resolve_future(&self, timeout: Option<Duration>) -> Result<Arc<FfiValue>, Error> {
        match timeout {
            None => self.future.get().await,
            Some(timeout) => self.future.get_with_timeout(timeout).await,
        }
    }
}

/// Drives `future` to completion on `handle`, the runtime of the client the
/// operation belongs to, blocking the calling thread.
pub(crate) fn block_on<F: Future>(handle: &tokio::runtime::Handle, future: F) -> F::Output {
    handle.block_on(future)
}

/// Opaque handle to a [`KafkaFuture`] whose value is a `void *`; the function
/// that returned it documents the type behind the pointer. `get` hands out a
/// pointer borrowed from the value the handle caches: it stays valid until the
/// handle is destroyed and, for a `_cb` delivery, at least until the callback
/// returns. A `Void` future delivers `NULL`.
#[repr(C)]
pub struct kafka_common_KafkaFuture_t {
    _private: [u8; 0],
}

/// Hands `future` to C as an owned handle, freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// `runtime` and `queue` are those of the client that produced the future;
/// `None` for a future that belongs to no client.
pub(crate) fn box_future(
    future: FfiFuture,
    runtime: Option<tokio::runtime::Handle>,
    queue: Option<Arc<CallbackQueue>>,
) -> *mut kafka_common_KafkaFuture_t {
    box_future_inner(future, runtime, queue, false)
}

/// [`box_future`] for a future of a client that completes only once a
/// callback queued on `queue` reported (a producer record sent with a
/// delivery callback): a blocking `get` on it runs the queued callbacks on
/// the calling thread while it waits, so a caller that pumps the queue
/// itself, on one thread, does not wait for its own pump. The futures
/// derived from it (`then_apply`, `all_of`) do the same.
pub(crate) fn box_report_future(
    future: FfiFuture,
    runtime: tokio::runtime::Handle,
    queue: Arc<CallbackQueue>,
) -> *mut kafka_common_KafkaFuture_t {
    box_future_inner(future, Some(runtime), Some(queue), true)
}

fn box_future_inner(
    future: FfiFuture,
    runtime: Option<tokio::runtime::Handle>,
    queue: Option<Arc<CallbackQueue>>,
    drains_queue: bool,
) -> *mut kafka_common_KafkaFuture_t {
    box_inner(FutureInner::new(future, runtime, queue, drains_queue))
}

fn box_inner(inner: FutureInner) -> *mut kafka_common_KafkaFuture_t {
    Arc::into_raw(Arc::new(inner)) as *mut _
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
/// The completion of `kafka_common_KafkaFuture_get_cb`: `value` is the
/// future's value (`NULL` for a `Void` future or on failure), borrowed from the
/// future: valid at least until the callback returns, and until the future
/// handle is destroyed; `error` is `NULL` on success, otherwise an owned error
/// the callee frees with `kafka_common_Error_destroy`.
pub type kafka_common_KafkaFuture_get_cb_t =
    unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// `typedef void (*kafka_common_KafkaFuture_get_with_timeout_cb_t)(void *value, kafka_common_Error_t *error, void *opaque);`
///
/// The completion of `kafka_common_KafkaFuture_get_with_timeout_cb`, with
/// the same contract as `kafka_common_KafkaFuture_get_cb_t`.
pub type kafka_common_KafkaFuture_get_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The error a `get_cb` completion carries when the future's client was
/// destroyed before the future resolved. No Java counterpart: a Java
/// `KafkaFuture` outlives its client.
const CLIENT_DESTROYED: &str = "The client was destroyed before the future completed";

/// The error `get` answers for a future that belongs to no client and is not
/// complete. Only crate code can build one: every future C builds without a
/// client is complete. No Java counterpart: a Java `KafkaFuture`
/// is completed by whoever holds its `KafkaFutureImpl`.
const NOT_COMPLETE_WITHOUT_CLIENT: &str = "A KafkaFuture that belongs to no client must already be complete";

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
        // No client: the future is already complete (module docs), so it is
        // answered on this thread.
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

/// Returns a new KafkaFuture that is already completed with the given value.
///
/// # Parameters
///
/// - `result` - the value, a `void *`
/// - `result_error` - `NULL`, or the error the future fails with instead
///
/// # Returns
///
/// The new future.
///
/// # Safety
///
/// The future belongs to no client: `get` answers it on the calling thread.
/// `result` stays the caller's, kept alive while the future may deliver it;
/// Rust never frees it. `result_error` must be `NULL` or an owned error handle
/// not yet destroyed, which this call takes over. The returned future is freed
/// with `kafka_common_KafkaFuture_destroy`.
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

/// Waits if necessary for this future to complete, and then returns its result.
///
/// # Returns
///
/// The future's value, or `NULL` on failure.
///
/// # Errors
///
/// - The error the underlying operation failed with.
/// - `kafka_common_Error_is_local_illegal_state_error` for a caught Rust panic, or if the
///   future's client was destroyed before the future completed.
///
/// # Safety
///
/// `self_` must be a valid future handle and `out_err` a valid slot pointing at
/// `NULL`. The value is borrowed from the future: it stays valid until the
/// future handle is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get(
    self_: *const kafka_common_KafkaFuture_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle; the
    // borrow lasts for this call only, while the caller keeps the handle alive.
    match unsafe { future_ref(self_) }.resolve(None) {
        Ok(value) => value.ptr(),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(e) => unsafe { fail_with(out_err, e) },
    }
}

/// Waits if necessary for this future to complete, and then returns its result.
///
/// # Parameters
///
/// - `cb` - the completion, receiving the value and the error, `NULL` on
///   success; run by the owning client's `__execute_callbacks`, or before this
///   returns for a future with no client; one still waiting when the client is
///   destroyed fires from its `_destroy`, one after it inline
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - The error the underlying operation failed with.
/// - `kafka_common_Error_is_local_illegal_state_error` for a caught Rust panic, or if the
///   future's client was destroyed before the future completed.
///
/// # Safety
///
/// `self_` must be a valid future handle; `opaque` is handed back to `cb`
/// untouched. The value `cb` receives is borrowed from the future: it stays
/// valid at least until `cb` returns, and until the future handle is destroyed.
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

/// Waits if necessary for at most the given time for this future to complete,
/// and then returns its result, if available.
///
/// # Parameters
///
/// - `timeout` - the maximum time to wait, in milliseconds; negative is zero
///
/// # Returns
///
/// The future's value, or `NULL` on failure.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_timeout_error` if the wait timed out.
/// - The error the underlying operation failed with.
/// - `kafka_common_Error_is_local_illegal_state_error` for a caught Rust panic, or if the
///   future's client was destroyed before the future completed.
///
/// # Safety
///
/// `self_` must be a valid future handle and `out_err` a valid slot pointing at
/// `NULL`. The value is borrowed from the future: it stays valid until the
/// future handle is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_get_with_timeout(
    self_: *const kafka_common_KafkaFuture_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle; the
    // borrow lasts for this call only, while the caller keeps the handle alive.
    match unsafe { future_ref(self_) }.resolve(Some(timeout_ms(timeout))) {
        Ok(value) => value.ptr(),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(e) => unsafe { fail_with(out_err, e) },
    }
}

/// Waits if necessary for at most the given time for this future to complete,
/// and then returns its result, if available.
///
/// # Parameters
///
/// - `timeout` - the maximum time to wait, in milliseconds; negative is zero
/// - `cb` - the completion, receiving the value and the error, `NULL` on
///   success; run as for `kafka_common_KafkaFuture_get_cb`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_timeout_error` if the wait timed out.
/// - The error the underlying operation failed with.
/// - `kafka_common_Error_is_local_illegal_state_error` for a caught Rust panic, or if the
///   future's client was destroyed before the future completed.
///
/// # Safety
///
/// `self_` must be a valid future handle; `opaque` is handed back to `cb`
/// untouched. The value `cb` receives is borrowed from the future: it stays
/// valid at least until `cb` returns, and until the future handle is destroyed.
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

/// Returns 1 if completed in any fashion: normally, exceptionally, or via
/// cancellation, else 0.
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

/// Returns a new KafkaFuture that is completed when all the given futures have
/// completed. If any future fails, the returned future returns its error. If
/// multiple futures fail, which error gets returned is arbitrarily chosen.
///
/// # Parameters
///
/// - `futures` - a `kafka_List_t` of `kafka_common_KafkaFuture_t *`
///
/// # Returns
///
/// The new future, whose `get` delivers `NULL`.
///
/// # Safety
///
/// `futures` must be `NULL` or a valid list whose elements are valid future
/// handles. The list and the futures stay the caller's, and may be destroyed
/// right after this returns. The returned future is owned, freed with
/// `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_all_of(
    futures: *const kafka_List_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: per this function's `# Safety`, `futures` is null or a valid list, which the
    // caller keeps alive for this call; the borrow ends before return.
    let inners: Vec<Arc<FutureInner>> = unsafe { list_elements(futures) }
        .iter()
        // SAFETY: per this function's `# Safety`, every element is a valid future handle,
        // alive for this call; each `future_arc` takes a reference of its own, which the
        // returned future keeps (`FutureInner::sources`).
        .map(|&f| unsafe { future_arc(f as *const _) })
        .collect();
    // A future waiting for a report drains its own client's queue, so the result takes that
    // client when one of the futures is such a future.
    let owner = inners.iter().find(|i| i.drains_queue);
    let runtime = owner.map_or_else(|| inners.iter().find_map(|i| i.runtime.clone()), |i| i.runtime.clone());
    let queue = owner.map_or_else(|| inners.iter().find_map(|i| i.queue.clone()), |i| i.queue.clone());
    let drains_queue = owner.is_some();
    let all = KafkaFuture::all_of(inners.iter().map(|i| i.future.clone()).collect());
    let mut inner = FutureInner::new(map_void_future(&all), runtime, queue, drains_queue);
    inner.sources = inners;
    box_inner(inner)
}

// ---------------------------------------------------------------------------
// `KafkaFuture.BaseFunction` and `thenApply`
// ---------------------------------------------------------------------------

/// `typedef void *(*kafka_common_KafkaFuture_BaseFunction_apply_fn_t)(void *self, const void *a, kafka_common_Error_t **out_err);`
///
/// `BaseFunction.apply(A a)`: `a` is the source future's value, borrowed for
/// the duration of the call (`NULL` for a `Void` future). Returns the result,
/// a `void *` the implementation owns and keeps alive as long as the derived
/// future may deliver it (Rust never frees it). On failure the implementation
/// stores an owned error in `*out_err`, which points at `NULL` on entry, and
/// that error fails the derived future, as a throwing Java function does; the
/// returned value is then ignored.
pub type kafka_common_KafkaFuture_BaseFunction_apply_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, a: *const c_void, out_err: *mut *mut kafka_common_Error_t) -> *mut c_void;

/// A C implementation of `KafkaFuture.BaseFunction` (CLAUDE.md §4 rule 3):
/// the caller's `void *self` plus its `apply`.
struct BaseFunctionInner {
    self_: SendPtr,
    apply: kafka_common_KafkaFuture_BaseFunction_apply_fn_t,
}

impl BaseFunctionInner {
    /// Invokes the C `apply` on `a`.
    fn apply(&self, a: *const c_void) -> Result<*mut c_void, Error> {
        let mut error: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: `apply` is the function pointer the C caller registered,
        // called with its own `self` and a slot pointing at null.
        let out = unsafe { (self.apply)(self.self_.0, a, &raw mut error) };
        // SAFETY: `apply` leaves the slot null or stores an owned error handle, per the
        // `kafka_common_KafkaFuture_BaseFunction_apply_fn_t` contract, consumed here once.
        match unsafe { take_error(error) } {
            Some(error) => Err(error),
            None => Ok(out),
        }
    }
}

/// Opaque handle to a C implementation of `KafkaFuture.BaseFunction<A, B>`,
/// the function `then_apply` runs on a future's value. Java declares it as a
/// functional interface, the Rust `common::kafka_future::BaseFunction` trait,
/// which C cannot implement, so it is an interface built with
/// `kafka_common_KafkaFuture_BaseFunction_new`.
#[repr(C)]
pub struct kafka_common_KafkaFuture_BaseFunction_t {
    _private: [u8; 0],
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `function` must be a valid handle from
/// `kafka_common_KafkaFuture_BaseFunction_new`, not destroyed for `'a`.
unsafe fn base_function_ref<'a>(function: *const kafka_common_KafkaFuture_BaseFunction_t) -> &'a BaseFunctionInner {
    // SAFETY: per this function's `# Safety`, `function` came from
    // `kafka_common_KafkaFuture_BaseFunction_new`, whose `Box::into_raw` of a
    // `BaseFunctionInner` is the allocation behind every function handle, alive for `'a`.
    // Nothing mutates a registration, so the `&` aliases nothing mutable.
    unsafe { &*(function as *const BaseFunctionInner) }
}

/// Returns a C `BaseFunction`: `apply` is called with `self_` first.
///
/// # Parameters
///
/// - `self_` - the caller's opaque pointer
/// - `apply` - the implementation of `apply`
///
/// # Safety
///
/// The caller owns `self_` and keeps it alive until `apply` has run, or until
/// the future `kafka_common_KafkaFuture_then_apply` returned has been
/// destroyed, with every future built on it (`then_apply`, `all_of`) and every
/// pending `get_cb` on them: that last release cancels an `apply` not started,
/// and waits for one running on another thread. The returned handle is owned
/// until `kafka_common_KafkaFuture_then_apply` takes it over; freed with
/// `kafka_common_KafkaFuture_BaseFunction_destroy` only when not taken over.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_KafkaFuture_BaseFunction_new(
    self_: *mut c_void,
    apply: kafka_common_KafkaFuture_BaseFunction_apply_fn_t,
) -> *mut kafka_common_KafkaFuture_BaseFunction_t {
    Box::into_raw(Box::new(BaseFunctionInner { self_: SendPtr(self_), apply })) as *mut _
}

/// Invokes the registered `apply` with its `self`.
///
/// # Parameters
///
/// - `a` - the argument, passed through
///
/// # Returns
///
/// The function's result, or `NULL` on failure.
///
/// # Errors
///
/// - The error the function reported.
/// - `kafka_common_Error_is_local_illegal_state_error` for a caught Rust panic.
///
/// # Safety
///
/// `self_` must be a valid function handle and `out_err` a valid slot pointing
/// at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_apply(
    self_: *const kafka_common_KafkaFuture_BaseFunction_t,
    a: *const c_void,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: per this function's `# Safety`, `self_` is a valid function handle; the
    // borrow lasts for this call only.
    match unsafe { base_function_ref(self_) }.apply(a) {
        Ok(value) => value,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(e) => unsafe { fail_with(out_err, e) },
    }
}

/// Frees a function handle `then_apply` did not consume; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned function handle neither destroyed nor
/// consumed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_destroy(
    self_: *mut kafka_common_KafkaFuture_BaseFunction_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`,
        // an owned function handle neither destroyed nor consumed, that is the `Box::into_raw`
        // of `kafka_common_KafkaFuture_BaseFunction_new`, reclaimed here once.
        drop(unsafe { Box::from_raw(self_ as *mut BaseFunctionInner) });
    }
}

/// Returns a new KafkaFuture that, when this future completes normally, is
/// executed with this futures's result as the argument to the supplied
/// function. The function may be invoked by the thread that calls
/// `kafka_common_KafkaFuture_then_apply` or it may be invoked by the thread
/// that completes the future.
///
/// # Parameters
///
/// - `function` - the function, from `kafka_common_KafkaFuture_BaseFunction_new`
///
/// # Returns
///
/// The new future, of the same client as this one.
///
/// # Translation notes
///
/// `apply` runs inline if this future is complete, else from its client's
/// `__execute_callbacks`. Its error fails the returned future.
///
/// # Safety
///
/// `self_` must be a valid future handle and `function` a valid function
/// handle, which this call takes over. The value `apply` returns stays the
/// C side's; the source value is borrowed for the call only. The returned
/// future is owned, freed with `kafka_common_KafkaFuture_destroy`; once it is
/// destroyed, with every future built on it and every pending `get_cb` on
/// them, an `apply` not started is not called
/// (`kafka_common_KafkaFuture_BaseFunction_new`).
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_then_apply(
    self_: *const kafka_common_KafkaFuture_t,
    function: *mut kafka_common_KafkaFuture_BaseFunction_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: per this function's `# Safety`, `self_` is a valid future handle; this takes a
    // reference of its own, which the derived future keeps (`FutureInner::sources`).
    let inner = unsafe { future_arc(self_) };
    let gate = Arc::new(ApplyGate::new());
    let apply_gate = Arc::clone(&gate);
    // SAFETY: per this function's `# Safety`, `function` is a valid function handle, i.e. the
    // `Box::into_raw` of `kafka_common_KafkaFuture_BaseFunction_new`, not used again, so
    // `Box::from_raw` reclaims it exactly once and the registration moves into the closure.
    let function = *unsafe { Box::from_raw(function as *mut BaseFunctionInner) };
    let applied: KafkaFutureImpl<Arc<FfiValue>> = KafkaFutureImpl::new();
    let completer = applied.clone();
    // Set once `on_complete` has returned: a completion seen before that, on
    // the caller's thread, ran inside this call, which runs `apply` itself, as
    // Java runs the function in the thread calling `thenApply` when the
    // future is already complete. A completion racing this call on another
    // thread is queued.
    let registered = Arc::new(AtomicBool::new(false));
    let caller = std::thread::current().id();
    let queue = inner.queue.clone();
    let registered_in_callback = Arc::clone(&registered);
    inner.future.on_complete(move |result| {
        let source = match result {
            Ok(source) => Arc::clone(source),
            // The function is not called: the dependant fails as the source did.
            Err(error) => {
                completer.complete_with_error(error.clone());
                return;
            },
        };
        // A function whose future can no longer be observed is not called: its
        // `self_` may be freed (`ApplyGate`).
        let apply = move || match apply_gate.enter() {
            Some(_running) => {
                completer.complete_result(function.apply(source.ptr()).map(FfiValue::borrowed));
            },
            None => {
                completer.complete_with_error(Error::local_illegal_state(
                    "The future was destroyed before its function ran",
                ));
            },
        };
        match queue {
            // Completed later, by the client: C code never runs on a Rust
            // background task, so `apply` waits for `__execute_callbacks`
            // (or runs on the pushing thread once the queue is closed).
            Some(queue) if registered_in_callback.load(Ordering::Acquire) || std::thread::current().id() != caller => {
                queue.push(Box::new(apply))
            },
            _ => apply(),
        }
    });
    registered.store(true, Ordering::Release);
    let drains_queue = inner.drains_queue || inner.queue.is_some();
    let mut derived = FutureInner::new(applied.future(), inner.runtime.clone(), inner.queue.clone(), drains_queue);
    derived.sources = vec![inner];
    derived.apply_gate = Some(gate);
    box_inner(derived)
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
    use std::sync::Mutex;
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
        let mut err = std::ptr::null_mut();
        // SAFETY: Test helper: per this function's `# Safety`, `f` is a live future handle,
        // and `err` is a valid slot pointing at null.
        let out = unsafe { kafka_common_KafkaFuture_get(f, &mut err) };
        if err.is_null() {
            Ok(out)
        } else {
            assert!(out.is_null(), "a failed get returns NULL");
            // SAFETY: Test helper: `err` is the fresh, non-null error handle `get` stored;
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

    /// The runtime a client drives its futures on: multi-threaded, so its timer
    /// runs while a C thread blocks in `Handle::block_on`.
    fn client_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn get_with_timeout_times_out_with_the_local_timeout_error() {
        // Only a client's future can be pending, so this one has a client's
        // runtime (and no callbacks vector, which `get` does not use).
        let runtime = client_runtime();
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(map_future(&pending.future()), Some(runtime.handle().clone()), None);
        // SAFETY: Test code: `f` is the fresh `box_future` handle, destroyed once at the end;
        // each `err` is a valid slot pointing at null, then the fresh, non-null error handle a
        // timed-out get stored, destroyed once. The value read at the end is the `i64` `f`
        // caches.
        unsafe {
            assert_eq!(kafka_common_KafkaFuture_is_done(f), 0);
            let mut err = std::ptr::null_mut();
            let out = kafka_common_KafkaFuture_get_with_timeout(f, 10, &mut err);
            assert!(!err.is_null());
            assert!(out.is_null());
            assert_eq!(kafka_common_Error_is_local_timeout_error(err), 1);
            kafka_common_Error_destroy(err);
            // Negative timeouts are zero, not a panic.
            let mut err = std::ptr::null_mut();
            let out = kafka_common_KafkaFuture_get_with_timeout(f, -5, &mut err);
            assert!(!err.is_null());
            assert!(out.is_null());
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
        let runtime = client_runtime();
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(map_future(&pending.future()), Some(runtime.handle().clone()), None);
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

    /// A report-backed future's blocking `get` runs the queued callbacks on
    /// the calling thread, one of which completes it, as a C caller pumping
    /// its producer on one thread relies on; a timed `get` still times out
    /// while it drains, and a future derived with `then_apply` drains too.
    #[test]
    fn a_report_future_runs_the_queued_callbacks_while_get_waits() {
        let runtime = client_runtime();
        let queue = Arc::new(CallbackQueue::new());
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_report_future(map_future(&pending.future()), runtime.handle().clone(), Arc::clone(&queue));
        let plain = box_future(
            map_future(&pending.future()),
            Some(runtime.handle().clone()),
            Some(Arc::clone(&queue)),
        );
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        // SAFETY: Test code: `f` and `plain` are the fresh handles above and `function` a fresh
        // function handle over `doubler`, a live local that outlives the derived future, consumed
        // by `then_apply`; each other handle is destroyed once at the end. `err` is a valid slot pointing at null; the error a
        // timed-out get stores there is destroyed once.
        let derived = unsafe {
            let mut err = std::ptr::null_mut();
            kafka_common_KafkaFuture_get_with_timeout(f, 20, &mut err);
            assert_eq!(kafka_common_Error_is_local_timeout_error(err), 1, "nothing queued completes it");
            kafka_common_Error_destroy(err);
            kafka_common_KafkaFuture_then_apply(f, function)
        };
        // SAFETY: Test code: `derived` and `plain` are live handles.
        unsafe {
            assert!(future_ref(derived).drains_queue, "then_apply inherits the drain");
            assert!(!future_ref(plain).drains_queue, "box_future does not drain");
        }
        let caller = std::thread::current().id();
        let ran_on = Arc::new(Mutex::new(None));
        let pusher = {
            let ran_on = Arc::clone(&ran_on);
            let pending = pending.clone();
            let queue = Arc::clone(&queue);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                queue.push(Box::new(move || {
                    *ran_on.lock().unwrap() = Some(std::thread::current().id());
                    pending.complete(21);
                }));
            })
        };
        // SAFETY: Test code: `derived` is still live; the value read is the `i64` `double` wrote.
        assert_eq!(unsafe { *(get(derived).unwrap() as *const i64) }, 42);
        pusher.join().unwrap();
        assert_eq!(*ran_on.lock().unwrap(), Some(caller), "the callback ran on the waiting thread");
        // SAFETY: Test code: `f` is still live; the value read is the `i64` it caches. Each
        // handle is then destroyed here, once.
        unsafe {
            assert_eq!(*(get(f).unwrap() as *const i64), 21);
            kafka_common_KafkaFuture_destroy(derived);
            kafka_common_KafkaFuture_destroy(plain);
            kafka_common_KafkaFuture_destroy(f);
        }
    }

    #[test]
    fn a_future_without_a_client_is_answered_with_no_runtime() {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "the test thread is outside any runtime"
        );
        let value = 5i64;
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        // SAFETY: Test code: `completed`, `failed`, `derived` and `all` are fresh future
        // handles and `function` a fresh function handle over `doubler`, a live local that
        // outlives `derived`, consumed by `then_apply`; each other handle is destroyed once after
        // its last use. `completed`
        // borrows `value`, a live local, and `failed` consumes a fresh `box_error` handle;
        // each `err` is the fresh error handle a failed get returned, destroyed once. `out`
        // is a live local, and every value read through it points at `value` or into
        // `doubler`.
        unsafe {
            let completed =
                kafka_common_KafkaFuture_completed_future(&value as *const i64 as *const c_void, std::ptr::null_mut());
            let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
            let derived = kafka_common_KafkaFuture_then_apply(completed, function);
            let list = kafka_List_new();
            kafka_List_add(list, completed as *mut c_void);
            kafka_List_add(list, derived as *mut c_void);
            let all = kafka_common_KafkaFuture_all_of(list);
            kafka_List_destroy(list);

            for timeout in [None, Some(0), Some(1000)] {
                let started = std::time::Instant::now();
                for (f, expected) in [(completed, 5), (derived, 10)] {
                    let mut err = std::ptr::null_mut();
                    let out = match timeout {
                        None => kafka_common_KafkaFuture_get(f, &mut err),
                        Some(ms) => kafka_common_KafkaFuture_get_with_timeout(f, ms, &mut err),
                    };
                    assert!(err.is_null(), "{timeout:?}");
                    assert_eq!(*(out as *const i64), expected, "{timeout:?}");
                }
                let mut err = std::ptr::null_mut();
                let out = match timeout {
                    None => kafka_common_KafkaFuture_get(all, &mut err),
                    Some(ms) => kafka_common_KafkaFuture_get_with_timeout(all, ms, &mut err),
                };
                assert!(err.is_null() && out.is_null(), "{timeout:?}: a Void future delivers NULL");
                assert!(started.elapsed() < Duration::from_millis(500), "{timeout:?}: nothing waits");
            }
            assert_eq!(doubler.calls.load(Ordering::SeqCst), 1, "Java runs the function once");

            let failed = kafka_common_KafkaFuture_completed_future(std::ptr::null(), box_error(Error::timeout("late")));
            let mut err = std::ptr::null_mut();
            let out = kafka_common_KafkaFuture_get_with_timeout(failed, 1000, &mut err);
            assert!(out.is_null());
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(), "late");
            kafka_common_Error_destroy(err);
            assert_eq!(get(failed), Err("late".to_string()));

            for f in [completed, derived, all, failed] {
                kafka_common_KafkaFuture_destroy(f);
            }
        }
        // Before d4475efa0 these gets built a process-wide fallback runtime, whose worker was
        // named `kafka-ffi-future` (cut to 15 bytes in `comm`); none may exist now.
        #[cfg(target_os = "linux")]
        {
            let names = thread_names();
            assert!(!names.iter().any(|name| name.starts_with("kafka-ffi-futur")), "{names:?}");
        }
    }

    /// The names of this process's threads, from `/proc/self/task/*/comm`
    /// (where Linux cuts a name to 15 bytes).
    #[cfg(target_os = "linux")]
    fn thread_names() -> Vec<String> {
        std::fs::read_dir("/proc/self/task")
            .unwrap()
            .filter_map(|task| std::fs::read_to_string(task.ok()?.path().join("comm")).ok())
            .map(|name| name.trim_end().to_string())
            .collect()
    }

    #[test]
    fn a_pending_future_without_a_client_fails_instead_of_waiting() {
        // Only crate code can build one; it fails rather than block a thread
        // nothing would wake.
        let pending = KafkaFutureImpl::<i64>::new();
        let f = box_future(map_future(&pending.future()), None, None);
        let seen: Mutex<Vec<String>> = Mutex::new(Vec::new());
        // SAFETY: Test code: `f` is the fresh `box_future` handle, destroyed once at the end;
        // `err` is a valid slot pointing at null, then the fresh error handle the get stored,
        // destroyed once. `opaque` is `seen`, the `Mutex<Vec<String>>` `record_error`
        // expects, which outlives the inline delivery.
        unsafe {
            let mut err = std::ptr::null_mut();
            let out = kafka_common_KafkaFuture_get_with_timeout(f, 1000, &mut err);
            assert!(!err.is_null());
            assert!(out.is_null());
            assert_eq!(
                kafka_common_Error_is_local_timeout_error(err),
                0,
                "not a timeout: it never waited"
            );
            kafka_common_Error_destroy(err);
            assert_eq!(get(f), Err(NOT_COMPLETE_WITHOUT_CLIENT.to_string()));
            kafka_common_KafkaFuture_get_cb(f, record_error, &seen as *const _ as *mut c_void);
            assert_eq!(*seen.lock().unwrap(), [NOT_COMPLETE_WITHOUT_CLIENT], "delivered inline");
            kafka_common_KafkaFuture_destroy(f);
        }
    }

    /// Records the message of the error a `_cb` delivery carries, which it
    /// owns and destroys.
    unsafe extern "C" fn record_error(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        assert!(value.is_null() && !error.is_null());
        // SAFETY: Test code: every registration of this callback passes a live
        // `Mutex<Vec<String>>` as `opaque`, which outlives the delivery; `error` is the
        // non-null owned handle the delivery hands over, read and then destroyed once.
        unsafe {
            let seen = &*(opaque as *const Mutex<Vec<String>>);
            seen.lock()
                .unwrap()
                .push(CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap().to_string());
            kafka_common_Error_destroy(error);
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
            let inner = Arc::new(FutureInner::new(future, None, None, false));
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
        a: *const c_void,
        _out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        // SAFETY: Test code: every registration of this function passes a live `Doubler` as
        // `self`, which outlives every future derived through it.
        let doubler = unsafe { &*(self_ as *const Doubler) };
        doubler.calls.fetch_add(1, Ordering::SeqCst);
        let mut output = doubler.output.lock().unwrap();
        // SAFETY: Test code: every source future this function is applied to delivers an
        // `i64`, borrowed for the call (module docs), or the test passes a live `i64`.
        *output = unsafe { *(a as *const i64) } * 2;
        &raw mut *output as *mut c_void
    }

    unsafe extern "C" fn fail(
        _self: *mut c_void,
        _a: *const c_void,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        // SAFETY: Test code: the function-pointer contract hands every implementation a valid
        // slot pointing at null.
        unsafe { *out_err = box_error(Error::local_illegal_argument("apply failed")) };
        std::ptr::null_mut()
    }

    #[test]
    fn base_function_invoker_reaches_the_c_implementation() {
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
        let failing = kafka_common_KafkaFuture_BaseFunction_new(std::ptr::null_mut(), fail);
        let a = 21i64;
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: `function` (over `doubler`, a live local) and `failing` are fresh
        // function handles, each destroyed once at the end; `a` is a live local, and `out`
        // points into `doubler` when read. `error` is a valid slot pointing at null, then the
        // fresh handle `fail` stored, destroyed once after its message is read. Null is
        // accepted by `kafka_common_KafkaFuture_BaseFunction_destroy`.
        unsafe {
            let out =
                kafka_common_KafkaFuture_BaseFunction_apply(function, &raw const a as *const c_void, &raw mut error);
            assert!(error.is_null());
            assert_eq!(*(out as *const i64), 42);
            assert_eq!(doubler.calls.load(Ordering::SeqCst), 1);

            let out =
                kafka_common_KafkaFuture_BaseFunction_apply(failing, &raw const a as *const c_void, &raw mut error);
            assert!(!error.is_null());
            assert!(out.is_null());
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
        // outlives every derived future, each consumed by its `then_apply`; each future handle is
        // destroyed once after its last use.
        // `first` and the chained value point into `doubler`, and the source value is the
        // `i64` `source` caches, read before it is destroyed.
        unsafe {
            // The function handle is consumed.
            let derived = kafka_common_KafkaFuture_then_apply(source, function);
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
        // `doubler`, a live local that outlives its derived future), each consumed by its
        // `then_apply`; every future handle is destroyed once after its last use. `failed_source` consumes a fresh `box_error` handle.
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
        // the handle `then_apply` returns, which consumes `function`. Both futures are destroyed
        // once at the end.
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
            kafka_common_KafkaFuture_destroy(derived);
            kafka_common_KafkaFuture_destroy(source);
        }
    }

    /// Over a client's pending future, `then_apply`'s function is queued on the
    /// client when the source completes, on whichever thread completes it, and
    /// runs once, from `__execute_callbacks`, which completes the dependant;
    /// reading the dependant does not run it again.
    #[test]
    fn then_apply_queues_the_function_on_completion_and_runs_it_once() {
        let runtime = client_runtime();
        let queue = Arc::new(CallbackQueue::new());
        for complete_on_another_thread in [true, false] {
            let pending = KafkaFutureImpl::<i64>::new();
            let source = box_future(
                map_future(&pending.future()),
                Some(runtime.handle().clone()),
                Some(Arc::clone(&queue)),
            );
            let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
            let function = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
            // SAFETY: Test code: `source` is a fresh future handle and `function` a fresh function
            // handle over `doubler`, a live local that outlives `derived`, consumed by `then_apply`.
            // Both futures are destroyed once at the end of the iteration; the values read are the
            // `i64`s `source` caches and `double` writes into `doubler`.
            unsafe {
                let derived = kafka_common_KafkaFuture_then_apply(source, function);
                if complete_on_another_thread {
                    let pending = pending.clone();
                    std::thread::spawn(move || pending.complete(21)).join().unwrap();
                } else {
                    pending.complete(21);
                }
                assert_eq!(kafka_common_KafkaFuture_is_done(source), 1);
                assert_eq!(kafka_common_KafkaFuture_is_done(derived), 0, "the function is queued");
                assert_eq!(doubler.calls.load(Ordering::SeqCst), 0, "not run on the completing thread");
                assert_eq!(queue.execute(), 1);
                assert_eq!(doubler.calls.load(Ordering::SeqCst), 1);
                assert_eq!(kafka_common_KafkaFuture_is_done(derived), 1);
                for _ in 0..3 {
                    assert_eq!(*(get(derived).unwrap() as *const i64), 42);
                }
                assert_eq!(doubler.calls.load(Ordering::SeqCst), 1, "Java runs the function once");
                assert_eq!(queue.execute(), 0);

                // Over a source already complete, it runs inline, queuing nothing.
                let again = kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double);
                let inline = kafka_common_KafkaFuture_then_apply(source, again);
                assert_eq!(doubler.calls.load(Ordering::SeqCst), 2);
                assert_eq!(kafka_common_KafkaFuture_is_done(inline), 1);
                assert!(queue.is_empty());
                assert_eq!(*(get(inline).unwrap() as *const i64), 42);

                kafka_common_KafkaFuture_destroy(inline);
                kafka_common_KafkaFuture_destroy(derived);
                kafka_common_KafkaFuture_destroy(source);
            }
        }
    }

    #[test]
    fn then_apply_does_not_run_the_function_of_a_destroyed_future() {
        let runtime = client_runtime();
        let queue = Arc::new(CallbackQueue::new());
        let pending = KafkaFutureImpl::<i64>::new();
        let source = box_future(
            map_future(&pending.future()),
            Some(runtime.handle().clone()),
            Some(Arc::clone(&queue)),
        );
        let doubler = Doubler { calls: AtomicUsize::new(0), output: Mutex::new(0) };
        // SAFETY: Test code: `source` is a fresh future handle and each function a fresh
        // function handle over `doubler`, a live local, consumed by `then_apply`. Each future
        // is destroyed once; `all` borrows `kept` through the list, destroyed after `all_of`.
        unsafe {
            let dropped = kafka_common_KafkaFuture_then_apply(
                source,
                kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double),
            );
            let kept = kafka_common_KafkaFuture_then_apply(
                source,
                kafka_common_KafkaFuture_BaseFunction_new(&doubler as *const _ as *mut c_void, double),
            );
            let list = kafka_List_new();
            kafka_List_add(list, kept as *mut c_void);
            let all = kafka_common_KafkaFuture_all_of(list);
            kafka_List_destroy(list);
            // `dropped` has no other user, `kept` is still needed by `all`.
            kafka_common_KafkaFuture_destroy(dropped);
            kafka_common_KafkaFuture_destroy(kept);

            pending.complete(21);
            assert_eq!(queue.execute(), 2, "both jobs are dequeued");
            assert_eq!(
                doubler.calls.load(Ordering::SeqCst),
                1,
                "only the function `all` depends on runs"
            );
            assert!(get(all).unwrap().is_null());

            kafka_common_KafkaFuture_destroy(all);
            kafka_common_KafkaFuture_destroy(source);
        }
    }

    /// A function that blocks until the test releases it.
    struct Blocking {
        entered: Mutex<std::sync::mpsc::Sender<()>>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        finished: std::sync::atomic::AtomicBool,
    }

    unsafe extern "C" fn block(
        self_: *mut c_void,
        _a: *const c_void,
        _out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        // SAFETY: Test code: the only registration passes a live `Blocking`, which outlives
        // the destruction of the future derived through it.
        let blocking = unsafe { &*(self_ as *const Blocking) };
        blocking.entered.lock().unwrap().send(()).unwrap();
        blocking.release.lock().unwrap().recv().unwrap();
        blocking.finished.store(true, Ordering::SeqCst);
        std::ptr::null_mut()
    }

    #[test]
    fn destroying_a_future_waits_for_its_running_function() {
        let runtime = client_runtime();
        let queue = Arc::new(CallbackQueue::new());
        let pending = KafkaFutureImpl::<i64>::new();
        let source = box_future(
            map_future(&pending.future()),
            Some(runtime.handle().clone()),
            Some(Arc::clone(&queue)),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocking = Arc::new(Blocking {
            entered: Mutex::new(entered_tx),
            release: Mutex::new(release_rx),
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        // SAFETY: Test code: `source` is a fresh future handle and the function a fresh
        // function handle over `blocking`, kept alive by the `Arc` past every use below.
        // `derived` is destroyed once, by the second thread; `source` once at the end.
        unsafe {
            let derived = kafka_common_KafkaFuture_then_apply(
                source,
                kafka_common_KafkaFuture_BaseFunction_new(Arc::as_ptr(&blocking) as *mut c_void, block),
            ) as usize;
            pending.complete(21);
            let runner = {
                let queue = Arc::clone(&queue);
                std::thread::spawn(move || queue.execute())
            };
            entered_rx.recv().unwrap();
            let destroyer = {
                let blocking = Arc::clone(&blocking);
                std::thread::spawn(move || {
                    kafka_common_KafkaFuture_destroy(derived as *mut kafka_common_KafkaFuture_t);
                    blocking.finished.load(Ordering::SeqCst)
                })
            };
            std::thread::sleep(Duration::from_millis(50));
            release_tx.send(()).unwrap();
            assert!(
                destroyer.join().unwrap(),
                "destroy returns only after the function has returned"
            );
            assert_eq!(runner.join().unwrap(), 1);
            kafka_common_KafkaFuture_destroy(source);
        }
    }
}
