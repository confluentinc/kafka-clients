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

//! C FFI layer for the Kafka share consumer API (KIP-932).
//!
//! Exposes the share consumer (`ShareConsumer<Bytes, Bytes>` trait,
//! [`KafkaShareConsumer`], [`MockShareConsumer`]) via C-callable `extern "C"`
//! functions, so non-Rust callers can drive the whole
//! subscribe → poll → acknowledge → commit → close flow, with an optional
//! registered acknowledgement-commit callback.
//!
//! # Concurrency model — single-owner access guard
//!
//! The `ShareConsumer` trait is `Send + 'static` but **not** `Sync`, and every
//! blocking method takes `&mut self`. The handle owns the consumer directly
//! behind an [`UnsafeCell`] and a non-reentrant single-owner guard
//! (`owner: AtomicU64`):
//!
//! - Every FFI call (sync or async) must [`acquire`] before touching the
//!   consumer. Concurrent access from a second thread — or, for the async
//!   surface, a second operation while one is already in flight — fails fast
//!   with an "not safe for multi-threaded access" error.
//! - [`kafka_consumer_ShareConsumer_wakeup`] is the one method that **bypasses**
//!   the guard: its whole purpose is to interrupt a `poll` another thread holds.
//!
//! # Async delivery
//!
//! `_async` entry points return immediately and deliver their result later
//! through a C callback fired on a single per-handle dispatcher thread (the
//! shared machinery in [`super::common`]). The guard is held from submission
//! until the awaited operation completes and is released **inside the completion
//! job, just before the callback fires**, so a genuinely concurrent operation is
//! rejected for the whole in-flight window.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with
// PascalCase type names, which intentionally differs from Rust's snake_case.
#![allow(non_snake_case, non_camel_case_types)]

use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::common::serialization::BytesDeserializer;
use crate::common::{KafkaError, TopicIdPartition, Uuid};
use crate::consumer::acknowledgement_commit_callback::AcknowledgementCommitCallback;
use crate::consumer::{
    AcknowledgeType, ConsumerRecord, MockShareConsumer, ShareConsumer, ShareConsumerConfig, WakeupHandle,
    new_share_consumer_with_wakeup,
};

use super::common::{
    self, CompletionJob, KafkaErrorInner, OperationCallbackFn, OperationCallbackTarget, OperationCompletion,
    SendUserData, borrow_error_ptr, box_error, enqueue_or_run_inline, init_default_logger, kafka_common_KafkaError_t,
};
use super::records::{
    box_records, box_string_list, kafka_consumer_ConsumerRecord_t, kafka_consumer_ConsumerRecords_t,
    kafka_consumer_StringList_t, record_ref,
};

// The share consumer is monomorphized over refcounted `bytes::Bytes` keys and
// values: each record's key/value is a zero-copy slice of the owning fetch
// buffer, and `Bytes: Clone` satisfies the share consumer's `K/V: Clone` bound
// for the KIP-932 RENEW retention path. The C side borrows ptr+len from the
// `Bytes`; the batch keeps the buffer alive.
type Bytes = bytes::Bytes;

// ---------------------------------------------------------------------------
// Access guard
// ---------------------------------------------------------------------------

/// Free sentinel for [`ShareConsumerHandle::owner`]: no thread/future currently
/// holds the consumer.
const NO_OWNER: u64 = u64::MAX;

/// Returns a stable, process-unique `u64` identifying the current OS thread.
///
/// The value need only be stable for the lifetime of the thread and distinct
/// from other live threads' values. A thread-local counter handed out from a
/// process-global atomic satisfies both without depending on a platform
/// thread-id API. `NO_OWNER` (`u64::MAX`) is the reserved free sentinel, so the
/// counter starts at 0 and would only collide after `u64::MAX` thread creations.
fn current_thread_id() -> u64 {
    use std::cell::Cell;
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    thread_local! {
        static THREAD_ID: Cell<u64> = Cell::new(NEXT_ID.fetch_add(1, Ordering::Relaxed));
    }
    THREAD_ID.with(Cell::get)
}

/// Acquires the single-owner guard for `h`, or returns an error if another
/// thread/future already holds it (the non-reentrant guard rejects re-entry
/// too). The `ShareConsumer` trait is not `Sync`, so this is the runtime
/// equivalent of the `&mut self` exclusivity the borrow checker enforces on the
/// Rust API.
fn acquire(h: &ShareConsumerHandle) -> Result<(), KafkaError> {
    let tid = current_thread_id();
    match h.owner.compare_exchange(NO_OWNER, tid, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Ok(()),
        // There is no dedicated concurrent-modification error code in this
        // client; `IllegalState` is the closest runtime-exception analog and the
        // message spells out the violation.
        Err(_) => Err(KafkaError::illegal_state(
            "KafkaShareConsumer is not safe for multi-threaded access.",
        )),
    }
}

/// Releases the single-owner guard on its shared owner cell.
///
/// The cell is held behind an [`Arc`] so an async completion job can release the
/// guard through its own clone — even after the handle it came from has been
/// destroyed — without touching freed memory.
fn release_owner(owner: &AtomicU64) {
    owner.store(NO_OWNER, Ordering::Release);
}

/// Releases the single-owner guard for `h`.
fn release(h: &ShareConsumerHandle) {
    release_owner(&h.owner);
}

/// RAII guard that releases the access guard on scope exit (return or panic).
/// Used by the sync FFI path; the async path releases inside the completion job
/// instead.
struct ReleaseGuard<'a>(&'a ShareConsumerHandle);
impl Drop for ReleaseGuard<'_> {
    fn drop(&mut self) {
        release(self.0);
    }
}

/// The error handed to an async op's C callback when the awaited operation
/// **panicked** instead of returning a value or `Err`. There is no Kafka error
/// code for "the client panicked mid-operation"; surfacing it as an
/// illegal-state failure — the closest runtime-exception analog, as with the
/// access-guard rejection — lets the caller observe and free an ordinary error
/// handle rather than wait forever for a callback that would otherwise never come.
fn op_panicked_error() -> KafkaError {
    KafkaError::illegal_state("KafkaShareConsumer operation failed unexpectedly.")
}

/// The action a [`PanicCompletionGuard`] runs on a panic unwind: it consumes the
/// payload to enqueue an error completion that releases the guard and fires the
/// callback.
type PanicAction<P> = Box<dyn FnOnce(P) + Send>;

/// A one-shot RAII "bomb" that makes the async dispatch helpers panic-safe.
///
/// An op awaited on a worker task can *panic*, not just return `Err`. Without
/// this guard the unwind skips everything after the `.await`, so the single-owner
/// guard is never released (the consumer stays locked forever) and no completion
/// is ever enqueued (the C callback never fires and the caller hangs). While
/// armed, dropping this guard — which only happens on a panic unwind, since the
/// normal path [`disarm`](Self::disarm)s it first — runs `on_panic(payload)`,
/// which enqueues an error completion that releases the guard (through the shared
/// owner cell, exactly as an ordinary completion does) and fires the callback.
///
/// `payload` holds the move-only pieces a completion needs (the result builder
/// and `user_data` for a value op). Exactly one path consumes them: the bomb owns
/// them while armed, and `disarm` hands them back to the ordinary completion job
/// on the normal path — so the guard is released once and the callback fires
/// once, never twice and never both success and error.
struct PanicCompletionGuard<P> {
    armed: Option<(P, PanicAction<P>)>,
}

impl<P> PanicCompletionGuard<P> {
    fn new(payload: P, on_panic: impl FnOnce(P) + Send + 'static) -> Self {
        Self { armed: Some((payload, Box::new(on_panic))) }
    }

    /// Defuses the bomb: the op returned normally, so ownership of `payload`
    /// passes back to the caller for the ordinary completion job.
    fn disarm(mut self) -> P {
        let (payload, _on_panic) = self.armed.take().expect("panic guard is armed until disarmed");
        payload
    }
}

impl<P> Drop for PanicCompletionGuard<P> {
    fn drop(&mut self) {
        if let Some((payload, on_panic)) = self.armed.take() {
            on_panic(payload);
        }
    }
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// The two share-consumer implementations exposed through the FFI.
enum ShareConsumerKind {
    /// Production KIP-932 share consumer.
    Kafka(Box<dyn ShareConsumer<Bytes, Bytes>>),
    /// Broker-less test consumer with FFI driver methods.
    Mock(Box<MockShareConsumer<Bytes, Bytes>>),
}

/// Per-consumer handle state. Owns the consumer directly behind an
/// [`UnsafeCell`] and guards access with the single-owner owner cell; see the
/// module documentation for the concurrency model.
struct ShareConsumerHandle {
    /// The consumer. Exclusive access is enforced by `owner`, not the type
    /// system — `UnsafeCell` is needed to hand out `&mut` from a shared
    /// `&ShareConsumerHandle`.
    consumer: UnsafeCell<ShareConsumerKind>,
    /// `NO_OWNER`, or the thread id (from [`current_thread_id`]) holding it.
    /// Shared via [`Arc`] so an in-flight async op's completion job can release
    /// the guard through its own clone after the handle is gone.
    owner: Arc<AtomicU64>,
    /// Drives app-side async methods via `block_on` (sync path). `Option` so
    /// `destroy` can drop it (stopping all spawned tasks) while the handle box
    /// is still alive for those tasks to unwind against.
    runtime: Option<tokio::runtime::Runtime>,
    /// Handle for spawning async-variant awaiters (async path).
    runtime_handle: tokio::runtime::Handle,
    /// Sender for the completion-dispatch queue (shared machinery).
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
    /// Dispatcher thread join handle; detached (dropped, not joined) on destroy.
    #[allow(dead_code)]
    dispatcher: std::thread::JoinHandle<()>,
    /// Wakeup handle captured at construction; fired by
    /// [`kafka_consumer_ShareConsumer_wakeup`] without acquiring the guard.
    wakeup_handle: WakeupHandle,
    /// Whether this handle wraps a [`MockShareConsumer`].
    #[allow(dead_code)]
    is_mock: bool,
}

// SAFETY: `acquire()` guarantees at most one thread/future accesses
// `*consumer.get()` at any instant, and `ShareConsumerKind: Send`, so exclusive
// cross-thread access is sound. `UnsafeCell` is needed to hand out `&mut` from a
// shared `&ShareConsumerHandle`.
unsafe impl Send for ShareConsumerHandle {}
unsafe impl Sync for ShareConsumerHandle {}

impl ShareConsumerHandle {
    /// The `block_on` runtime for the sync FFI path. Present for the whole life
    /// of the handle; only `destroy` clears it (after which no FFI call runs).
    fn runtime(&self) -> &tokio::runtime::Runtime {
        self.runtime.as_ref().expect("share consumer runtime present before destroy")
    }
}

/// Builds a [`ShareConsumerHandle`] around a [`ShareConsumerKind`], spawning the
/// callback dispatcher thread, and returns the leaked C handle.
fn build_share_consumer_handle(
    kind: ShareConsumerKind,
    wakeup_handle: WakeupHandle,
    is_mock: bool,
) -> *mut kafka_consumer_ShareConsumer_t {
    // A multi-thread runtime so the async-variant awaiter tasks make progress
    // outside `block_on`. The production consumer's own background pipeline runs
    // on its dedicated IO thread, independent of this runtime.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for KafkaShareConsumer");
    let runtime_handle = runtime.handle().clone();
    let (completion_tx, dispatcher) = common::spawn_dispatcher("kafka-share-consumer-callback-dispatcher");

    let handle = Box::new(ShareConsumerHandle {
        consumer: UnsafeCell::new(kind),
        owner: Arc::new(AtomicU64::new(NO_OWNER)),
        runtime: Some(runtime),
        runtime_handle,
        completion_tx,
        dispatcher,
        wakeup_handle,
        is_mock,
    });
    Box::into_raw(handle) as *mut kafka_consumer_ShareConsumer_t
}

/// Casts a `*const kafka_consumer_ShareConsumer_t` to a `&'static
/// ShareConsumerHandle`.
///
/// # Safety
///
/// `consumer` must be non-null and created by a share-consumer constructor.
unsafe fn handle_ref(consumer: *const kafka_consumer_ShareConsumer_t) -> &'static ShareConsumerHandle {
    unsafe { &*(consumer as *const ShareConsumerHandle) }
}

/// Returns a `&mut` to the share-consumer trait object behind the handle's
/// `UnsafeCell`. The access guard guarantees exclusivity.
///
/// # Safety
///
/// The caller must hold the access guard (`acquire` succeeded and the guard is
/// still held).
// The `&mut` from `&` is the whole point of the `UnsafeCell` + access-guard
// design: the guard enforces the exclusivity the borrow checker cannot.
#[allow(clippy::mut_from_ref)]
unsafe fn consumer_mut(h: &ShareConsumerHandle) -> &mut dyn ShareConsumer<Bytes, Bytes> {
    match unsafe { &mut *h.consumer.get() } {
        ShareConsumerKind::Kafka(c) => c.as_mut(),
        ShareConsumerKind::Mock(c) => c.as_mut(),
    }
}

/// Returns the [`MockShareConsumer`] behind the guard, or an error if this
/// handle wraps a production consumer.
///
/// # Safety
///
/// The caller must hold the access guard.
// The `&mut` from `&` is the whole point of the `UnsafeCell` + access-guard
// design: the guard enforces the exclusivity the borrow checker cannot.
#[allow(clippy::mut_from_ref)]
unsafe fn mock_mut(h: &ShareConsumerHandle) -> Result<&mut MockShareConsumer<Bytes, Bytes>, KafkaError> {
    match unsafe { &mut *h.consumer.get() } {
        ShareConsumerKind::Mock(c) => Ok(c.as_mut()),
        ShareConsumerKind::Kafka(_) => {
            Err(KafkaError::illegal_state("operation is only supported on a MockShareConsumer"))
        },
    }
}

// ---------------------------------------------------------------------------
// Opaque types
// ---------------------------------------------------------------------------

/// Opaque share-consumer handle.
#[repr(C)]
pub struct kafka_consumer_ShareConsumer_t {
    _private: [u8; 0],
}

/// Opaque share-consumer-configuration properties handle (a
/// `HashMap<String, String>`).
#[repr(C)]
pub struct kafka_consumer_ShareConsumerProperties_t {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------------
// ShareConsumerProperties
// ---------------------------------------------------------------------------

/// Casts a `*const kafka_consumer_ShareConsumerProperties_t` to a reference.
///
/// # Safety
///
/// `props` must be a valid handle from a `ShareConsumerProperties` constructor.
unsafe fn properties_ref(props: *const kafka_consumer_ShareConsumerProperties_t) -> &'static HashMap<String, String> {
    unsafe { &*(props as *const HashMap<String, String>) }
}

/// Casts a `*mut kafka_consumer_ShareConsumerProperties_t` to a mutable
/// reference.
///
/// # Safety
///
/// `props` must be a valid handle from a `ShareConsumerProperties` constructor.
unsafe fn properties_mut(props: *mut kafka_consumer_ShareConsumerProperties_t) -> &'static mut HashMap<String, String> {
    unsafe { &mut *(props as *mut HashMap<String, String>) }
}

/// Creates an empty share-consumer properties handle.
///
/// # Returns
///
/// A non-null opaque properties handle. The caller must free it with
/// [`kafka_consumer_ShareConsumerProperties_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_ShareConsumerProperties_new() -> *mut kafka_consumer_ShareConsumerProperties_t {
    let map: HashMap<String, String> = HashMap::new();
    Box::into_raw(Box::new(map)) as *mut kafka_consumer_ShareConsumerProperties_t
}

/// Creates share-consumer properties from a NULL-terminated flat array of C
/// strings.
///
/// The array contains alternating key-value pairs terminated by a NULL pointer:
/// `["key1", "val1", "key2", "val2", ..., NULL]`.
///
/// # Returns
///
/// A non-null handle on success, or NULL if `configs` is NULL or an odd number
/// of non-NULL entries is found. The caller must free a non-null handle with
/// [`kafka_consumer_ShareConsumerProperties_destroy`].
///
/// # Safety
///
/// `configs` must be NULL or point to a NULL-terminated array of valid,
/// null-terminated C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumerProperties_from_configs(
    configs: *const *const c_char,
) -> *mut kafka_consumer_ShareConsumerProperties_t {
    if configs.is_null() {
        return std::ptr::null_mut();
    }
    let mut map: HashMap<String, String> = HashMap::new();
    let mut i = 0usize;
    loop {
        let key_ptr = unsafe { *configs.add(i) };
        if key_ptr.is_null() {
            break;
        }
        let val_ptr = unsafe { *configs.add(i + 1) };
        if val_ptr.is_null() {
            return std::ptr::null_mut();
        }
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_string_lossy().to_string();
        let val = unsafe { CStr::from_ptr(val_ptr) }.to_string_lossy().to_string();
        map.insert(key, val);
        i += 2;
    }
    Box::into_raw(Box::new(map)) as *mut kafka_consumer_ShareConsumerProperties_t
}

/// Adds or overwrites a configuration key-value pair. No-op if any parameter is
/// null.
///
/// # Safety
///
/// `props` must be a valid handle; `key` and `value` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumerProperties_put(
    props: *mut kafka_consumer_ShareConsumerProperties_t,
    key: *const c_char,
    value: *const c_char,
) {
    if props.is_null() || key.is_null() || value.is_null() {
        return;
    }
    let map = unsafe { properties_mut(props) };
    let k = unsafe { CStr::from_ptr(key) }.to_string_lossy().to_string();
    let v = unsafe { CStr::from_ptr(value) }.to_string_lossy().to_string();
    map.insert(k, v);
}

/// Destroys a properties handle. Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// `props` must be null or a valid handle from a `ShareConsumerProperties`
/// constructor. After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumerProperties_destroy(
    props: *mut kafka_consumer_ShareConsumerProperties_t,
) {
    if !props.is_null() {
        unsafe {
            drop(Box::from_raw(props as *mut HashMap<String, String>));
        }
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

/// Creates a new Kafka share consumer connected to a real cluster.
///
/// The consumer uses the byte-array deserializers for both key and value
/// ([`bytes::Bytes`]). `share.acknowledgement.mode` is taken from `props`
/// (default `implicit`, applied by the config parser); a missing or blank
/// `group.id` is rejected with a clear error.
///
/// # Parameters
///
/// - `props`: Non-null properties handle. The caller retains ownership.
/// - `out_error`: Pointer where an error handle will be written on failure, or
///   null if the caller does not need error details.
///
/// # Returns
///
/// A non-null share-consumer handle on success, or null on failure. If
/// `out_error` is non-null, `*out_error` is set to null on success or to a
/// valid error handle on failure (free it with
/// [`kafka_common_KafkaError_destroy`](super::common::kafka_common_KafkaError_destroy)).
///
/// # Safety
///
/// `props` must be a valid, non-null properties handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaShareConsumer_new(
    props: *const kafka_consumer_ShareConsumerProperties_t,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> *mut kafka_consumer_ShareConsumer_t {
    init_default_logger();
    if props.is_null() {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(KafkaError::illegal_argument("properties handle must not be null")) };
        }
        return std::ptr::null_mut();
    }
    let map = unsafe { properties_ref(props) };
    let config = match ShareConsumerConfig::from_properties(map) {
        Ok(c) => c,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    // Route through the wakeup-returning factory so the `WakeupHandle` is
    // captured before the consumer is type-erased to `Box<dyn ShareConsumer>` —
    // `wakeup()` must fire without borrowing the guarded consumer. The error is
    // returned unwrapped here (e.g. a blank `group.id` surfaces its own
    // message), unlike the public `new_share_consumer` wrapper.
    let (consumer, wakeup_handle) = match new_share_consumer_with_wakeup::<Bytes, Bytes>(
        config,
        Box::new(BytesDeserializer),
        Box::new(BytesDeserializer),
    ) {
        Ok(pair) => pair,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    if !out_error.is_null() {
        unsafe { *out_error = std::ptr::null_mut() };
    }
    build_share_consumer_handle(ShareConsumerKind::Kafka(consumer), wakeup_handle, false)
}

/// Creates a new mock share consumer (broker-less, for tests).
///
/// # Returns
///
/// A non-null share-consumer handle. The caller must free it with
/// [`kafka_consumer_ShareConsumer_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_MockShareConsumer_new() -> *mut kafka_consumer_ShareConsumer_t {
    init_default_logger();
    let consumer: MockShareConsumer<Bytes, Bytes> = MockShareConsumer::new();
    let wakeup_handle = consumer.wakeup_handle();
    build_share_consumer_handle(ShareConsumerKind::Mock(Box::new(consumer)), wakeup_handle, true)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Destroys a share-consumer handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op). Does NOT acquire the guard;
/// destroying concurrently with an in-flight op is a C lifetime precondition the
/// caller must uphold.
///
/// # Safety
///
/// `consumer` must be null or a valid handle from a share-consumer constructor.
/// After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_destroy(consumer: *mut kafka_consumer_ShareConsumer_t) {
    if consumer.is_null() {
        return;
    }
    let mut handle = unsafe { Box::from_raw(consumer as *mut ShareConsumerHandle) };

    // 1. Drop the runtime FIRST, while the handle box is still alive. This is a
    //    blocking shutdown: it waits for the worker threads to stop, so no
    //    spawned async op is still borrowing the consumer (via `consumer_mut`) or
    //    about to enqueue a completion job. `Option::take` drops it in place
    //    without freeing the box the running tasks unwind against.
    drop(handle.runtime.take());

    // 2. Drop the rest of the handle. The consumer's own `Drop` signals and
    //    joins its bg pipeline; dropping `completion_tx` lets the detached
    //    dispatcher drain any already-enqueued completion jobs and then exit.
    //    Those jobs release the guard through their own `Arc<AtomicU64>` clone
    //    and own their result handles, so none of them touches this freed box.
    drop(handle);
}

/// Wakes up a share consumer blocked in `poll` (or another long operation).
///
/// **Bypasses the access guard** — this is its whole purpose: it interrupts a
/// `poll` held by another thread. Callable from any thread.
///
/// # Safety
///
/// `consumer` must be a valid handle from a share-consumer constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_wakeup(consumer: *const kafka_consumer_ShareConsumer_t) {
    if consumer.is_null() {
        return;
    }
    let handle = unsafe { handle_ref(consumer) };
    handle.wakeup_handle.wakeup();
}

// ---------------------------------------------------------------------------
// Async op dispatch (void-returning methods)
// ---------------------------------------------------------------------------

/// Completion callback for void-returning async share-consumer ops. A null
/// `error` means success.
pub type kafka_consumer_ShareConsumer_op_callback_t = unsafe extern "C" fn(*mut kafka_common_KafkaError_t, *mut c_void);

/// Runs a void-returning consumer op synchronously under the access guard.
/// Returns null on success, or a non-null error handle on failure (including a
/// concurrent-access rejection if the guard cannot be acquired).
///
/// # Safety
///
/// `consumer` must be a valid handle.
unsafe fn sync_void_op<F>(consumer: *const kafka_consumer_ShareConsumer_t, op: F) -> *mut kafka_common_KafkaError_t
where
    // A higher-ranked bound ties the returned future's lifetime to the borrow of
    // the consumer, so the future may borrow `&mut self` for its duration.
    F: for<'a> FnOnce(
        &'a mut dyn ShareConsumer<Bytes, Bytes>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), KafkaError>> + 'a>>,
{
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let fut = op(unsafe { consumer_mut(h) });
    match h.runtime().block_on(fut) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
}

/// Async dispatch for a void-returning consumer op (one-operation-in-flight).
/// The access guard is held from submission until the completion job fires, so
/// any concurrent op is rejected until completion. If the guard cannot be
/// acquired, the callback fires inline with the error.
///
/// `op` must capture only `Send` data (already-marshaled owned values), never
/// raw C pointers, so the spawned future stays `Send`.
///
/// # Safety
///
/// `consumer` must be a valid handle. The closure runs on the runtime; it
/// receives the guarded `&mut dyn ShareConsumer`.
unsafe fn async_void_op<F, Fut>(
    consumer: *const kafka_consumer_ShareConsumer_t,
    callback: OperationCallbackFn,
    user_data: *mut c_void,
    op: F,
) where
    F: FnOnce(&'static mut dyn ShareConsumer<Bytes, Bytes>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), KafkaError>> + Send,
{
    let h = unsafe { handle_ref(consumer) };
    let target = OperationCallbackTarget { callback, user_data };
    if let Err(e) = acquire(h) {
        unsafe { (target.callback)(box_error(e), target.user_data) };
        return;
    }
    let tx = h.completion_tx.clone();
    // The completion job releases the guard through a clone of the shared owner
    // cell, so it stays valid even if `destroy` frees the handle before the job
    // runs on the dispatcher.
    let owner = Arc::clone(&h.owner);
    // Capture the `&'static ShareConsumerHandle` (Send+Sync via the unsafe
    // impls), NOT a bare `*mut` (raw pointers are !Send and would make the
    // future !Send). The handle is leaked, so the borrow is effectively
    // `'static`. It is used only while the op runs; `destroy` blocks on the
    // runtime shutdown before freeing it, so `consumer_mut` never races the free.
    let hs: &'static ShareConsumerHandle = unsafe { handle_ref(consumer) };
    h.runtime_handle.spawn(async move {
        let target = target;
        // Arm the panic-safety bomb before awaiting: if `op` panics and unwinds,
        // the guard's drop enqueues an error completion so the consumer is
        // released and the callback still fires. Disarmed on the normal path just
        // below. `target` is `Copy`, so both the bomb and the normal completion
        // hold their own copy.
        let panic_guard = PanicCompletionGuard::new((), {
            let owner = Arc::clone(&owner);
            let tx = tx.clone();
            move |()| {
                // Capture the whole `target` (Send), not its `*mut c_void` field
                // disjointly (which would make the closure !Send).
                let target = target;
                let completion = OperationCompletion {
                    callback: target.callback,
                    user_data: target.user_data,
                    error: box_error(op_panicked_error()),
                };
                let job: CompletionJob = Box::new(move || {
                    release_owner(&owner);
                    unsafe { completion.fire() };
                });
                enqueue_or_run_inline(&tx, job);
            }
        });
        // SAFETY: the guard is held for the whole submit->callback window.
        let consumer = unsafe { consumer_mut(hs) };
        let result = op(consumer).await;
        panic_guard.disarm();
        let error = match result {
            Ok(()) => std::ptr::null_mut(),
            Err(e) => box_error(e),
        };
        let completion = OperationCompletion { callback: target.callback, user_data: target.user_data, error };
        let job: CompletionJob = Box::new(move || {
            // Release BEFORE firing the callback: the awaited op is complete, so
            // the consumer is no longer borrowed. This avoids a
            // release-vs-next-op race when the callback resumes embedder work on
            // another thread.
            release_owner(&owner);
            unsafe { completion.fire() };
        });
        enqueue_or_run_inline(&tx, job);
    });
}

/// Async dispatch for a value-returning consumer op (one-operation-in-flight).
/// Runs the awaited `op` on the runtime, then hands its `Result<T>` to `complete`
/// on the dispatcher thread, which builds the typed result handle and fires the
/// C callback. The access guard is held from submission until `complete` runs, so
/// any concurrent op is rejected until completion; on an acquire failure the
/// callback fires inline with the error (guard not taken).
///
/// `op` must capture only `Send` data so the spawned future stays `Send`;
/// `complete` runs on the dispatcher, receiving the op result and the C
/// `user_data`.
///
/// # Safety
///
/// `consumer` must be a valid handle. The closure runs on the runtime; it
/// receives the guarded `&mut dyn ShareConsumer`.
unsafe fn async_value_op<T, Fut, F, C>(
    consumer: *const kafka_consumer_ShareConsumer_t,
    user_data: *mut c_void,
    op: F,
    complete: C,
) where
    T: Send + 'static,
    Fut: std::future::Future<Output = Result<T, KafkaError>> + Send,
    F: FnOnce(&'static mut dyn ShareConsumer<Bytes, Bytes>) -> Fut + Send + 'static,
    C: FnOnce(Result<T, KafkaError>, *mut c_void) + Send + 'static,
{
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        // Rejected: fire the callback inline with the error; guard not taken.
        complete(Err(e), user_data);
        return;
    }
    let tx = h.completion_tx.clone();
    // Released by the completion job through this clone, so it stays valid even
    // if `destroy` frees the handle before the job runs on the dispatcher.
    let owner = Arc::clone(&h.owner);
    // Capture the `&'static ShareConsumerHandle` (Send+Sync via the unsafe
    // impls), NOT a bare `*mut`. The handle is leaked, so the borrow is
    // effectively `'static`; it is used only while the op runs, and `destroy`
    // blocks on the runtime shutdown before freeing it.
    let hs: &'static ShareConsumerHandle = unsafe { handle_ref(consumer) };
    let ud = SendUserData(user_data);
    h.runtime_handle.spawn(async move {
        // Arm the panic-safety bomb before awaiting. `complete` and `ud` are
        // move-only and consumed by exactly one path: the bomb owns them while
        // armed and, on a panic unwind, fires the callback with an internal error;
        // on the normal path `disarm` hands them back to the ordinary completion
        // job. Either way the guard is released and the callback fires once.
        let panic_guard = PanicCompletionGuard::new((complete, ud), {
            let owner = Arc::clone(&owner);
            let tx = tx.clone();
            move |(complete, ud): (C, SendUserData)| {
                let job: CompletionJob = Box::new(move || {
                    release_owner(&owner);
                    complete(Err(op_panicked_error()), ud.into_ptr());
                });
                enqueue_or_run_inline(&tx, job);
            }
        });
        let result = op(unsafe { consumer_mut(hs) }).await;
        let (complete, ud) = panic_guard.disarm();
        let job: CompletionJob = Box::new(move || {
            // Release BEFORE firing the callback: the awaited op is complete, so
            // the consumer is no longer borrowed. `complete` builds the result
            // handle from the already-owned `T` (no consumer access), then fires.
            release_owner(&owner);
            complete(result, ud.into_ptr());
        });
        enqueue_or_run_inline(&tx, job);
    });
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// Reads `count` topic names from a C array into a `Vec<String>`.
///
/// # Safety
///
/// `topics` must point to `count` valid C strings. A non-positive `count` yields
/// an empty vec.
unsafe fn read_topics(topics: *const *const c_char, count: i32) -> Vec<String> {
    let n = count.max(0) as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        out.push(unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string());
    }
    out
}

/// Subscribes to a list of topics (sync). `topics` is an array of `count` C
/// strings. Returns null on success, non-null error on failure.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_subscribe(
    consumer: *const kafka_consumer_ShareConsumer_t,
    topics: *const *const c_char,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let topic_vec = unsafe { read_topics(topics, count) };
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.subscribe(topic_vec))) }
}

/// Subscribes to a list of topics (async).
/// See [`kafka_consumer_ShareConsumer_subscribe`].
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_subscribe_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    topics: *const *const c_char,
    count: i32,
    callback: kafka_consumer_ShareConsumer_op_callback_t,
    user_data: *mut c_void,
) {
    let topic_vec = unsafe { read_topics(topics, count) };
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.subscribe(topic_vec)) };
}

/// Unsubscribes from all topics (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_unsubscribe(
    consumer: *const kafka_consumer_ShareConsumer_t,
) -> *mut kafka_common_KafkaError_t {
    unsafe { sync_void_op(consumer, |c| Box::pin(c.unsubscribe())) }
}

/// Unsubscribes from all topics (async).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_unsubscribe_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    callback: kafka_consumer_ShareConsumer_op_callback_t,
    user_data: *mut c_void,
) {
    unsafe { async_void_op(consumer, callback, user_data, |c| c.unsubscribe()) };
}

/// Returns the current topic subscription as a
/// [`kafka_consumer_StringList_t`](super::records::kafka_consumer_StringList_t)
/// (free it with `kafka_consumer_StringList_destroy`). On failure returns null
/// and, if `out_error` is non-null, writes the error there (including a
/// concurrent-access rejection or the closed-consumer state).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_subscription(
    consumer: *const kafka_consumer_ShareConsumer_t,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> *mut kafka_consumer_StringList_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(e) };
        }
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    match unsafe { consumer_mut(h) }.subscription() {
        Ok(set) => {
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_string_list(set)
        },
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

// ---------------------------------------------------------------------------
// poll
// ---------------------------------------------------------------------------

/// Polls for records (synchronous). Drives the consumer's `poll(timeout)` under
/// the access guard via `block_on`.
///
/// # Parameters
///
/// - `consumer`: Non-null consumer handle.
/// - `timeout_ms`: Poll timeout in milliseconds.
/// - `out_error`: Pointer where an error handle is written on failure, or null.
///
/// # Returns
///
/// A non-null
/// [`kafka_consumer_ConsumerRecords_t`](super::records::kafka_consumer_ConsumerRecords_t)
/// handle on success (free it with `kafka_consumer_ConsumerRecords_destroy`), or
/// null on failure with `*out_error` set.
///
/// # Safety
///
/// `consumer` must be a valid handle from a share-consumer constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_poll(
    consumer: *const kafka_consumer_ShareConsumer_t,
    timeout_ms: i64,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> *mut kafka_consumer_ConsumerRecords_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(e) };
        }
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    let result = h.runtime().block_on(unsafe { consumer_mut(h).poll(timeout) });
    match result {
        Ok(records) => {
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_records(records)
        },
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

/// Completion callback for [`kafka_consumer_ShareConsumer_poll_async`].
///
/// On success `records` is non-null and `error` is null; on failure `records` is
/// null and `error` is non-null. The callback takes ownership of whichever handle
/// is non-null and must free it.
pub type kafka_consumer_ShareConsumer_poll_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_ConsumerRecords_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// A poll-callback target (function pointer + opaque `user_data`), wrapped so it
/// can cross the tokio task / dispatcher thread boundary.
#[derive(Clone, Copy)]
struct PollCallbackTarget {
    callback: kafka_consumer_ShareConsumer_poll_callback_t,
    user_data: *mut c_void,
}
// SAFETY: the C user owns the thread-safety of `user_data`; the function pointer
// is trivially shareable.
unsafe impl Send for PollCallbackTarget {}

/// Owned poll completion payload, fired by the dispatcher thread. Carries the raw
/// result handles (one of `records`/`error` is non-null) and a clone of the
/// shared owner cell so the access guard is released **after** the awaited op
/// completes but **before** the callback fires — and safely even if `destroy`
/// already freed the handle the op ran on.
struct PollCompletion {
    target: PollCallbackTarget,
    records: *mut kafka_consumer_ConsumerRecords_t,
    error: *mut kafka_common_KafkaError_t,
    owner: Arc<AtomicU64>,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread; the
// C user owns the thread-safety of `user_data`. `Arc<AtomicU64>` is Send.
unsafe impl Send for PollCompletion {}
impl PollCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    unsafe fn fire(self) {
        // Release BEFORE firing the callback. By the time this completion job
        // runs, the awaited op has fully completed (`poll().await` returned), so
        // the consumer is no longer borrowed. Releasing before the callback
        // avoids a release-vs-next-op race for embedders that resume work from
        // the callback. The callback only reads the already-built result handles;
        // it does not touch the consumer.
        release_owner(&self.owner);
        unsafe { (self.target.callback)(self.records, self.error, self.target.user_data) };
    }
}

/// Polls for records asynchronously (one-operation-in-flight). The access guard
/// is held from submission until the callback fires, so any concurrent op (sync
/// or async) is rejected until completion.
///
/// If the guard cannot be acquired, the callback fires inline with the error.
///
/// # Safety
///
/// `consumer` must be a valid handle from a share-consumer constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_poll_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    timeout_ms: i64,
    callback: kafka_consumer_ShareConsumer_poll_callback_t,
    user_data: *mut c_void,
) {
    let h = unsafe { handle_ref(consumer) };
    let target = PollCallbackTarget { callback, user_data };
    if let Err(e) = acquire(h) {
        // Rejected: deliver the error through the callback inline. The guard was
        // not taken, so nothing to release.
        unsafe { (target.callback)(std::ptr::null_mut(), box_error(e), target.user_data) };
        return;
    }
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    let tx = h.completion_tx.clone();
    // Released by the completion job through this clone, so it stays valid even
    // if `destroy` frees the handle before the job runs.
    let owner = Arc::clone(&h.owner);
    // Capture the `&'static ShareConsumerHandle` (Send+Sync via the unsafe
    // impls), NOT a bare `*mut` (raw pointers are !Send and would make the
    // future !Send). The handle is leaked, so the borrow is effectively
    // `'static`. It is used only while the op runs; `destroy` blocks on the
    // runtime shutdown before freeing it, so `consumer_mut` never races the free.
    let hs: &'static ShareConsumerHandle = unsafe { handle_ref(consumer) };
    h.runtime_handle.spawn(async move {
        let target = target;
        // Arm the panic-safety bomb before awaiting: a panicking `poll` unwind
        // enqueues an error completion (null records) so the guard is released and
        // the callback still fires, instead of locking the consumer and hanging
        // the caller. Disarmed on the normal path just below. `target` is `Copy`,
        // so both the bomb and the normal completion hold their own copy.
        let panic_guard = PanicCompletionGuard::new((), {
            let owner = Arc::clone(&owner);
            let tx = tx.clone();
            move |()| {
                let completion = PollCompletion {
                    target,
                    records: std::ptr::null_mut(),
                    error: box_error(op_panicked_error()),
                    owner,
                };
                let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
                enqueue_or_run_inline(&tx, job);
            }
        });
        let result = unsafe { consumer_mut(hs).poll(timeout).await };
        panic_guard.disarm();
        // No `.await` after building the raw handles below.
        let (records, error) = match result {
            Ok(r) => (box_records(r), std::ptr::null_mut()),
            Err(e) => (std::ptr::null_mut(), box_error(e)),
        };
        let completion = PollCompletion { target, records, error, owner };
        let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
        enqueue_or_run_inline(&tx, job);
    });
}

// ---------------------------------------------------------------------------
// Acknowledge (sync, guarded — records intent only, no network)
// ---------------------------------------------------------------------------

/// How a delivered record was handled, for the `acknowledge*` entry points
/// (KIP-932).
// The variant names are the C ABI enum values, so they stay upper-case; their
// discriminants arrive from C callers, so Rust's dead-code analysis cannot see
// them being constructed.
#[allow(dead_code, clippy::upper_case_acronyms)]
#[derive(Clone, Copy)]
#[repr(i32)]
pub enum kafka_consumer_AcknowledgeType_t {
    /// The record was consumed successfully.
    ACCEPT = 1,
    /// Release the record for another delivery attempt.
    RELEASE = 2,
    /// Reject the record; do not release it for another attempt.
    REJECT = 3,
    /// The record is still being processed; renew its acquisition lock.
    RENEW = 4,
}

impl kafka_consumer_AcknowledgeType_t {
    /// Maps the C enum onto the internal [`AcknowledgeType`].
    fn to_ack_type(self) -> AcknowledgeType {
        match self {
            Self::ACCEPT => AcknowledgeType::Accept,
            Self::RELEASE => AcknowledgeType::Release,
            Self::REJECT => AcknowledgeType::Reject,
            Self::RENEW => AcknowledgeType::Renew,
        }
    }
}

/// Runs a sync, guarded, non-awaiting consumer op (the `acknowledge*` family
/// records intent in the current fetch without any network round-trip). Returns
/// null on success, or a non-null error handle on failure (including a
/// concurrent-access rejection if the guard cannot be acquired).
///
/// # Safety
///
/// `consumer` must be a valid handle.
unsafe fn guarded_ack_op<F>(consumer: *const kafka_consumer_ShareConsumer_t, op: F) -> *mut kafka_common_KafkaError_t
where
    F: FnOnce(&mut dyn ShareConsumer<Bytes, Bytes>) -> Result<(), KafkaError>,
{
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    match op(unsafe { consumer_mut(h) }) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
}

/// Acknowledges successful delivery of `record` with `ACCEPT`.
///
/// `record` must be a pointer obtained from this consumer's most recent poll
/// batch, and that batch must still be alive (it owns the record). A record that
/// is no longer in flight surfaces an `IllegalState` error, exactly as the
/// underlying consumer reports.
///
/// Returns null on success, or a non-null error handle on failure.
///
/// # Safety
///
/// `consumer` must be a valid handle; `record` a valid record pointer from this
/// consumer's last poll.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_acknowledge(
    consumer: *const kafka_consumer_ShareConsumer_t,
    record: *const kafka_consumer_ConsumerRecord_t,
) -> *mut kafka_common_KafkaError_t {
    let rec = unsafe { record_ref(record) };
    unsafe { guarded_ack_op(consumer, |c| c.acknowledge(rec)) }
}

/// Acknowledges delivery of `record` with the given [`kafka_consumer_AcknowledgeType_t`].
///
/// See [`kafka_consumer_ShareConsumer_acknowledge`] for the record-lifetime
/// contract.
///
/// # Safety
///
/// `consumer` must be a valid handle; `record` a valid record pointer from this
/// consumer's last poll.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_acknowledge_with_type(
    consumer: *const kafka_consumer_ShareConsumer_t,
    record: *const kafka_consumer_ConsumerRecord_t,
    ack_type: kafka_consumer_AcknowledgeType_t,
) -> *mut kafka_common_KafkaError_t {
    let rec = unsafe { record_ref(record) };
    let ack = ack_type.to_ack_type();
    unsafe { guarded_ack_op(consumer, |c| c.acknowledge_with_type(rec, ack)) }
}

/// Acknowledges delivery of the record identified by `(topic, partition,
/// offset)` with the given [`kafka_consumer_AcknowledgeType_t`]. Unlike the
/// record-pointer variants, this does not borrow a polled batch.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_acknowledge_by_offset(
    consumer: *const kafka_consumer_ShareConsumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    ack_type: kafka_consumer_AcknowledgeType_t,
) -> *mut kafka_common_KafkaError_t {
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let ack = ack_type.to_ack_type();
    unsafe { guarded_ack_op(consumer, move |c| c.acknowledge_by_offset(&topic_str, partition, offset, ack)) }
}

// ---------------------------------------------------------------------------
// TopicIdPartition — a borrowed key inside the commit / ack-callback containers
// ---------------------------------------------------------------------------

/// Opaque handle to a topic-id-partition: a topic name, its 16-byte topic id,
/// and a partition number. **Borrowed** from an owning result container (a
/// [`kafka_consumer_ShareCommitResult_t`] or
/// [`kafka_consumer_ShareAcknowledgeOffsets_t`]); it has no standalone
/// destructor and is invalidated when its container is destroyed.
#[repr(C)]
pub struct kafka_common_TopicIdPartition_t {
    _private: [u8; 0],
}

/// Owns the derived, C-ready fields of a [`TopicIdPartition`]: the cached
/// NUL-terminated topic name, the 16 raw big-endian topic-id bytes, and the
/// partition. Living inside a container, it hands out stable borrowed pointers.
struct TopicIdPartitionInner {
    topic_c: CString,
    topic_id_bytes: [u8; 16],
    partition: i32,
}

impl TopicIdPartitionInner {
    fn new(tip: &TopicIdPartition) -> Self {
        Self {
            topic_c: CString::new(tip.topic().as_bytes()).unwrap_or_default(),
            topic_id_bytes: tip.topic_id().to_bytes(),
            partition: tip.partition(),
        }
    }
}

/// Returns the topic name as a NUL-terminated C string (owned by the container,
/// valid until it is destroyed), or null if `tip` is null.
///
/// # Safety
///
/// `tip` must be a borrowed handle obtained from a result container.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_topic(
    tip: *const kafka_common_TopicIdPartition_t,
) -> *const c_char {
    if tip.is_null() {
        return std::ptr::null();
    }
    unsafe { &*(tip as *const TopicIdPartitionInner) }.topic_c.as_ptr()
}

/// Returns a pointer to the 16 raw big-endian topic-id bytes (owned by the
/// container, valid until it is destroyed), or null if `tip` is null. The length
/// is always 16.
///
/// # Safety
///
/// `tip` must be a borrowed handle obtained from a result container.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_topic_id(
    tip: *const kafka_common_TopicIdPartition_t,
) -> *const u8 {
    if tip.is_null() {
        return std::ptr::null();
    }
    unsafe { &*(tip as *const TopicIdPartitionInner) }.topic_id_bytes.as_ptr()
}

/// Returns the partition number, or `-1` if `tip` is null.
///
/// # Safety
///
/// `tip` must be a borrowed handle obtained from a result container.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_partition(tip: *const kafka_common_TopicIdPartition_t) -> i32 {
    if tip.is_null() {
        return -1;
    }
    unsafe { &*(tip as *const TopicIdPartitionInner) }.partition
}

// ---------------------------------------------------------------------------
// ShareCommitResult — the commit_sync return value
// ---------------------------------------------------------------------------

/// Opaque handle to a `commit_sync` result: a per-partition outcome map
/// (`Map<TopicIdPartition, Optional<KafkaException>>`). A null per-partition
/// error means that partition's acknowledgements committed successfully.
#[repr(C)]
pub struct kafka_consumer_ShareCommitResult_t {
    _private: [u8; 0],
}

/// One committed partition and its outcome. The error payload is owned here (not
/// boxed separately) so [`kafka_consumer_ShareCommitResult_get_error`] can hand
/// out a borrowed `kafka_common_KafkaError_t` that lives as long as the result.
struct ShareCommitEntry {
    partition: TopicIdPartitionInner,
    error: Option<KafkaErrorInner>,
}

/// Owns the committed entries in a deterministic order for stable indexed access.
struct ShareCommitResultInner {
    entries: Vec<ShareCommitEntry>,
}

/// Boxes the `commit_sync` outcome map into an opaque result handle. Entries are
/// sorted by `(topic, partition)` so indexed access is deterministic (the source
/// `HashMap` has no stable order).
fn box_share_commit_result(
    map: HashMap<TopicIdPartition, Option<KafkaError>>,
) -> *mut kafka_consumer_ShareCommitResult_t {
    let mut entries: Vec<ShareCommitEntry> = map
        .into_iter()
        .map(|(tip, error)| ShareCommitEntry {
            partition: TopicIdPartitionInner::new(&tip),
            error: error.map(KafkaErrorInner::new),
        })
        .collect();
    entries.sort_by(|a, b| {
        a.partition
            .topic_c
            .as_bytes()
            .cmp(b.partition.topic_c.as_bytes())
            .then(a.partition.partition.cmp(&b.partition.partition))
    });
    Box::into_raw(Box::new(ShareCommitResultInner { entries })) as *mut kafka_consumer_ShareCommitResult_t
}

/// Returns the number of committed partitions, or 0 if `result` is null.
///
/// # Safety
///
/// `result` must be a valid share-commit-result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareCommitResult_count(
    result: *const kafka_consumer_ShareCommitResult_t,
) -> i32 {
    if result.is_null() {
        return 0;
    }
    unsafe { &*(result as *const ShareCommitResultInner) }.entries.len() as i32
}

/// Returns the partition at `index` (borrowed; valid until the result is
/// destroyed), or null if out of range.
///
/// # Safety
///
/// `result` must be a valid share-commit-result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareCommitResult_get_partition(
    result: *const kafka_consumer_ShareCommitResult_t,
    index: i32,
) -> *const kafka_common_TopicIdPartition_t {
    if result.is_null() || index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(result as *const ShareCommitResultInner) }
        .entries
        .get(index as usize)
    {
        Some(entry) => &entry.partition as *const TopicIdPartitionInner as *const kafka_common_TopicIdPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the error for the partition at `index` (borrowed; valid until the
/// result is destroyed), or **null if that partition committed successfully** (or
/// if `index` is out of range). The returned pointer is read-only — do NOT pass
/// it to `kafka_common_KafkaError_destroy` (it is owned by the result).
///
/// # Safety
///
/// `result` must be a valid share-commit-result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareCommitResult_get_error(
    result: *const kafka_consumer_ShareCommitResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if result.is_null() || index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(result as *const ShareCommitResultInner) }
        .entries
        .get(index as usize)
    {
        Some(entry) => match &entry.error {
            Some(inner) => borrow_error_ptr(inner),
            None => std::ptr::null(),
        },
        None => std::ptr::null(),
    }
}

/// Destroys a share-commit-result handle, freeing the owned entries (and any
/// borrowed partition / error pointers obtained from it). Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid share-commit-result handle. After this call
/// the pointer (and anything borrowed from it) is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareCommitResult_destroy(result: *mut kafka_consumer_ShareCommitResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ShareCommitResultInner)) };
    }
}

// ---------------------------------------------------------------------------
// Commit (async)
// ---------------------------------------------------------------------------

/// Completion callback for the async commit-sync ops. On success `result` is
/// non-null (a [`kafka_consumer_ShareCommitResult_t`], free it with
/// `kafka_consumer_ShareCommitResult_destroy`) and `error` is null; on failure
/// `result` is null and `error` is non-null. The callback owns whichever handle
/// is non-null and must free it.
pub type kafka_consumer_ShareConsumer_commit_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_ShareCommitResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Commits the acknowledgements for the last poll (sync), waiting up to
/// `default.api.timeout.ms`. On success returns a non-null
/// [`kafka_consumer_ShareCommitResult_t`] (free it with
/// `kafka_consumer_ShareCommitResult_destroy`) and sets `*out_error` to null; on
/// failure returns null with `*out_error` set.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_commit_sync(
    consumer: *const kafka_consumer_ShareConsumer_t,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> *mut kafka_consumer_ShareCommitResult_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(e) };
        }
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    match h.runtime().block_on(unsafe { consumer_mut(h).commit_sync() }) {
        Ok(map) => {
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_share_commit_result(map)
        },
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

/// Commits the acknowledgements for the last poll asynchronously
/// (one-operation-in-flight), waiting up to `default.api.timeout.ms`. See
/// [`kafka_consumer_ShareConsumer_commit_sync`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_commit_sync_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    callback: kafka_consumer_ShareConsumer_commit_callback_t,
    user_data: *mut c_void,
) {
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.commit_sync().await },
            move |result, ud| {
                let (res, err) = match result {
                    Ok(map) => (box_share_commit_result(map), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(res, err, ud);
            },
        )
    };
}

/// Commits the acknowledgements for the last poll (sync), waiting up to
/// `timeout_ms`. See [`kafka_consumer_ShareConsumer_commit_sync`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_commit_sync_timeout(
    consumer: *const kafka_consumer_ShareConsumer_t,
    timeout_ms: i64,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> *mut kafka_consumer_ShareCommitResult_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(e) };
        }
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    match h.runtime().block_on(unsafe { consumer_mut(h).commit_sync_timeout(timeout) }) {
        Ok(map) => {
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_share_commit_result(map)
        },
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

/// Commits the acknowledgements for the last poll asynchronously
/// (one-operation-in-flight), waiting up to `timeout_ms`. See
/// [`kafka_consumer_ShareConsumer_commit_sync`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_commit_sync_timeout_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    timeout_ms: i64,
    callback: kafka_consumer_ShareConsumer_commit_callback_t,
    user_data: *mut c_void,
) {
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.commit_sync_timeout(timeout).await },
            move |result, ud| {
                let (res, err) = match result {
                    Ok(map) => (box_share_commit_result(map), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(res, err, ud);
            },
        )
    };
}

/// Commits the acknowledgements for the last poll without waiting for the
/// network (sync). Java's `commitAsync` does not block on the broker; it drains
/// and fires the registered ack-commit callback when the acknowledgement
/// completes. Returns null on success, non-null error on failure.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_commit_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
) -> *mut kafka_common_KafkaError_t {
    unsafe { sync_void_op(consumer, |c| Box::pin(c.commit_async())) }
}

/// Commits the acknowledgements for the last poll without waiting for the
/// network (async dispatch of the non-blocking op). See
/// [`kafka_consumer_ShareConsumer_commit_async`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_commit_async_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    callback: kafka_consumer_ShareConsumer_op_callback_t,
    user_data: *mut c_void,
) {
    unsafe { async_void_op(consumer, callback, user_data, |c| c.commit_async()) };
}

// ---------------------------------------------------------------------------
// Close (async)
// ---------------------------------------------------------------------------

/// Closes the consumer (sync), waiting up to the default close timeout. Returns
/// null on success, non-null error on failure.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_close(
    consumer: *const kafka_consumer_ShareConsumer_t,
) -> *mut kafka_common_KafkaError_t {
    unsafe { sync_void_op(consumer, |c| Box::pin(c.close())) }
}

/// Closes the consumer (sync), waiting up to `timeout_ms`. See
/// [`kafka_consumer_ShareConsumer_close`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_close_timeout(
    consumer: *const kafka_consumer_ShareConsumer_t,
    timeout_ms: i64,
) -> *mut kafka_common_KafkaError_t {
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.close_timeout(timeout))) }
}

/// Closes the consumer asynchronously (one-operation-in-flight), waiting up to
/// the default close timeout. See [`kafka_consumer_ShareConsumer_close`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_close_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    callback: kafka_consumer_ShareConsumer_op_callback_t,
    user_data: *mut c_void,
) {
    unsafe { async_void_op(consumer, callback, user_data, |c| c.close()) };
}

/// Closes the consumer asynchronously (one-operation-in-flight), waiting up to
/// `timeout_ms`. See [`kafka_consumer_ShareConsumer_close`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_close_timeout_async(
    consumer: *const kafka_consumer_ShareConsumer_t,
    timeout_ms: i64,
    callback: kafka_consumer_ShareConsumer_op_callback_t,
    user_data: *mut c_void,
) {
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.close_timeout(timeout)) };
}

// ---------------------------------------------------------------------------
// acquisition_lock_timeout_ms (sync state read)
// ---------------------------------------------------------------------------

/// Returns whether an acquisition-lock timeout is known for the last fetched
/// records, writing it to `*out_ms` when present.
///
/// Returns `true` and sets `*out_ms` if the timeout is present; returns `false`
/// (leaving `*out_ms` untouched) if it is absent or on error. On error `*out_error`
/// is set to a non-null handle; on success or plain absence it is set to null.
///
/// # Safety
///
/// `consumer` must be a valid handle; `out_ms` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_acquisition_lock_timeout_ms(
    consumer: *const kafka_consumer_ShareConsumer_t,
    out_ms: *mut i32,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> bool {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(e) };
        }
        return false;
    }
    let _g = ReleaseGuard(h);
    match unsafe { consumer_mut(h) }.acquisition_lock_timeout_ms() {
        Ok(Some(ms)) => {
            if !out_ms.is_null() {
                unsafe { *out_ms = ms };
            }
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            true
        },
        Ok(None) => {
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            false
        },
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            false
        },
    }
}

// ---------------------------------------------------------------------------
// ShareAcknowledgeOffsets — the registered ack-commit callback payload
// ---------------------------------------------------------------------------

/// Opaque handle to the completed offsets delivered to a registered
/// acknowledgement-commit callback (`Map<TopicIdPartition, Set<Long>>`).
///
/// **Owned by the C callback**: the callback receives it, reads it, and frees it
/// with [`kafka_consumer_ShareAcknowledgeOffsets_destroy`] (unlike the borrowed
/// result-container sub-handles).
#[repr(C)]
pub struct kafka_consumer_ShareAcknowledgeOffsets_t {
    _private: [u8; 0],
}

/// Owns the completed partitions in a deterministic order, each paired with its
/// sorted list of completed offsets, for stable indexed access.
struct ShareAcknowledgeOffsetsInner {
    partitions: Vec<TopicIdPartitionInner>,
    /// `offsets[i]` are the completed offsets for `partitions[i]`, sorted ascending.
    offsets: Vec<Vec<i64>>,
}

/// Marshals the **borrowed** completed-offsets map into an owned handle. Called
/// from the ack-commit callback before its borrow ends; partitions are sorted by
/// `(topic, partition)` and each partition's offsets ascending, so the C side
/// sees a deterministic layout.
fn box_share_acknowledge_offsets(
    map: &HashMap<TopicIdPartition, HashSet<i64>>,
) -> *mut kafka_consumer_ShareAcknowledgeOffsets_t {
    let mut entries: Vec<(TopicIdPartitionInner, Vec<i64>)> = map
        .iter()
        .map(|(tip, offs)| {
            let mut sorted: Vec<i64> = offs.iter().copied().collect();
            sorted.sort_unstable();
            (TopicIdPartitionInner::new(tip), sorted)
        })
        .collect();
    entries.sort_by(|a, b| {
        a.0.topic_c
            .as_bytes()
            .cmp(b.0.topic_c.as_bytes())
            .then(a.0.partition.cmp(&b.0.partition))
    });
    let (partitions, offsets): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
    Box::into_raw(Box::new(ShareAcknowledgeOffsetsInner { partitions, offsets }))
        as *mut kafka_consumer_ShareAcknowledgeOffsets_t
}

/// Returns the number of completed partitions, or 0 if `offsets` is null.
///
/// # Safety
///
/// `offsets` must be a valid share-acknowledge-offsets handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareAcknowledgeOffsets_partition_count(
    offsets: *const kafka_consumer_ShareAcknowledgeOffsets_t,
) -> i32 {
    if offsets.is_null() {
        return 0;
    }
    unsafe { &*(offsets as *const ShareAcknowledgeOffsetsInner) }.partitions.len() as i32
}

/// Returns the partition at `index` (borrowed; valid until the handle is
/// destroyed), or null if out of range.
///
/// # Safety
///
/// `offsets` must be a valid share-acknowledge-offsets handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareAcknowledgeOffsets_get_partition(
    offsets: *const kafka_consumer_ShareAcknowledgeOffsets_t,
    index: i32,
) -> *const kafka_common_TopicIdPartition_t {
    if offsets.is_null() || index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(offsets as *const ShareAcknowledgeOffsetsInner) }
        .partitions
        .get(index as usize)
    {
        Some(tip) => tip as *const TopicIdPartitionInner as *const kafka_common_TopicIdPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the number of completed offsets for the partition at `index`, or 0 if
/// out of range.
///
/// # Safety
///
/// `offsets` must be a valid share-acknowledge-offsets handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareAcknowledgeOffsets_offset_count(
    offsets: *const kafka_consumer_ShareAcknowledgeOffsets_t,
    index: i32,
) -> i32 {
    if offsets.is_null() || index < 0 {
        return 0;
    }
    match unsafe { &*(offsets as *const ShareAcknowledgeOffsetsInner) }
        .offsets
        .get(index as usize)
    {
        Some(offs) => offs.len() as i32,
        None => 0,
    }
}

/// Returns the `offset_index`-th completed offset for the partition at
/// `partition_index` (offsets are sorted ascending), or `-1` if either index is
/// out of range.
///
/// # Safety
///
/// `offsets` must be a valid share-acknowledge-offsets handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareAcknowledgeOffsets_get_offset(
    offsets: *const kafka_consumer_ShareAcknowledgeOffsets_t,
    partition_index: i32,
    offset_index: i32,
) -> i64 {
    if offsets.is_null() || partition_index < 0 || offset_index < 0 {
        return -1;
    }
    let inner = unsafe { &*(offsets as *const ShareAcknowledgeOffsetsInner) };
    match inner
        .offsets
        .get(partition_index as usize)
        .and_then(|offs| offs.get(offset_index as usize))
    {
        Some(&offset) => offset,
        None => -1,
    }
}

/// Destroys a share-acknowledge-offsets handle. The registered callback owns the
/// handle it receives and must call this exactly once. Safe with null (no-op).
///
/// # Safety
///
/// `offsets` must be null or a valid share-acknowledge-offsets handle. After this
/// call the pointer (and anything borrowed from it) is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareAcknowledgeOffsets_destroy(
    offsets: *mut kafka_consumer_ShareAcknowledgeOffsets_t,
) {
    if !offsets.is_null() {
        unsafe { drop(Box::from_raw(offsets as *mut ShareAcknowledgeOffsetsInner)) };
    }
}

// ---------------------------------------------------------------------------
// Registered acknowledgement-commit callback
// ---------------------------------------------------------------------------

/// The non-null C ack-commit callback signature, used for internal storage once
/// a callback has been registered. The exported ABI typedef
/// [`kafka_consumer_ShareConsumer_AcknowledgementCommitCallback_t`] is the
/// nullable (`Option`) form.
type AckCommitCallbackFn = unsafe extern "C" fn(
    *const kafka_consumer_ShareAcknowledgeOffsets_t,
    *const kafka_common_KafkaError_t,
    *mut c_void,
);

/// Callback invoked when a share-group acknowledgement commit completes.
///
/// `offsets` is always non-null (the completed offsets); `error` is null on
/// success or non-null on failure. The callback **takes ownership** of both
/// non-null handles and must free them
/// ([`kafka_consumer_ShareAcknowledgeOffsets_destroy`] and, if non-null,
/// `kafka_common_KafkaError_destroy`).
///
/// Nullable at the ABI boundary: passing a null pointer to
/// [`kafka_consumer_ShareConsumer_set_acknowledgement_commit_callback`] clears
/// the registered callback.
pub type kafka_consumer_ShareConsumer_AcknowledgementCommitCallback_t = Option<
    unsafe extern "C" fn(
        *const kafka_consumer_ShareAcknowledgeOffsets_t,
        *const kafka_common_KafkaError_t,
        *mut c_void,
    ),
>;

/// The Rust `AcknowledgementCommitCallback` that bridges to a registered C
/// callback. Stored as `Arc<dyn AcknowledgementCommitCallback>` on the consumer,
/// so it must be `Send + Sync`: the C fn pointer and the completion sender are
/// both `Send + Sync`, and [`SendUserData`] is too (only its pointer value is
/// read through the shared reference).
struct FfiAckCommitCallback {
    callback: AckCommitCallbackFn,
    user_data: SendUserData,
    /// A clone of the handle's dispatcher queue, so the C callback fires on the
    /// single dispatcher thread like every other FFI callback.
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
}

/// Owned ack-commit completion payload fired on the dispatcher thread. Transfers
/// ownership of the marshaled offsets handle and the optional boxed error to the
/// C callback.
struct AckCommitCompletion {
    callback: AckCommitCallbackFn,
    user_data: *mut c_void,
    offsets: *mut kafka_consumer_ShareAcknowledgeOffsets_t,
    error: *mut kafka_common_KafkaError_t,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread; the
// C user owns the thread-safety of `user_data`.
unsafe impl Send for AckCommitCompletion {}
impl AckCommitCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread. Ownership of
    /// `offsets` and `error` passes to the C callback.
    unsafe fn fire(self) {
        unsafe { (self.callback)(self.offsets, self.error as *const _, self.user_data) };
    }
}

#[async_trait::async_trait]
impl AcknowledgementCommitCallback for FfiAckCommitCallback {
    async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>) {
        // `offsets` / `error` are BORROWED and the borrow ends when this returns.
        // Marshal them into owned C handles now (one allocation per commit, off
        // the per-record hot path), then hand them to a completion job. There is
        // no `.await` between the borrow and the marshal, so the borrowed data is
        // fully captured before it can go away.
        let offsets_handle = box_share_acknowledge_offsets(offsets);
        let error_handle = match error {
            Some(e) => box_error(e.clone()),
            None => std::ptr::null_mut(),
        };
        // Copy the pointer value into a fresh completion payload so the closure
        // stays `Send` (a bare `*mut c_void` capture would not be).
        let completion = AckCommitCompletion {
            callback: self.callback,
            user_data: self.user_data.0,
            offsets: offsets_handle,
            error: error_handle,
        };
        let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
        // Route onto the shared dispatcher thread — never a tokio worker, never a
        // per-call spawn. If the dispatcher is gone, run inline to honor the
        // callback and free the owned handles.
        enqueue_or_run_inline(&self.completion_tx, job);
    }
}

/// Registers a C acknowledgement-commit callback, or clears it when `callback` is
/// null. When registered, the callback fires on the shared dispatcher thread as
/// each acknowledgement commit completes, receiving an owned
/// [`kafka_consumer_ShareAcknowledgeOffsets_t`] and (on failure) an owned
/// `kafka_common_KafkaError_t` that it must free.
///
/// Returns null on success, or a non-null error on a concurrent-access rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle. `callback`, when non-null, must remain a
/// valid function pointer, and `user_data`'s thread-safety is the C caller's
/// responsibility.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ShareConsumer_set_acknowledgement_commit_callback(
    consumer: *const kafka_consumer_ShareConsumer_t,
    callback: kafka_consumer_ShareConsumer_AcknowledgementCommitCallback_t,
    user_data: *mut c_void,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let registered: Option<Arc<dyn AcknowledgementCommitCallback>> = callback.map(|cb| {
        Arc::new(FfiAckCommitCallback {
            callback: cb,
            user_data: SendUserData(user_data),
            completion_tx: h.completion_tx.clone(),
        }) as Arc<dyn AcknowledgementCommitCallback>
    });
    unsafe { consumer_mut(h) }.set_acknowledgement_commit_callback(registered);
    std::ptr::null_mut()
}

// ---------------------------------------------------------------------------
// MockShareConsumer driver methods (broker-less test support)
//
// These match on `ShareConsumerKind::Mock` and return `illegal_state` for the
// `Kafka` arm (they have no production analog). They run under the access guard
// like any sync call.
// ---------------------------------------------------------------------------

/// Adds a record to a mock share consumer's pending queue (mock only).
///
/// The record's topic must already be subscribed. `key`/`value` are (ptr, len)
/// pairs; pass `len < 0` for an absent key/value. On failure (including
/// `illegal_state` if `consumer` wraps a production consumer, or if the topic is
/// not subscribed) writes the error to `*out_error` when non-null.
///
/// # Safety
///
/// `topic` must be a valid C string; `key`/`value` valid for `key_len`/
/// `value_len` bytes (or null if the length is negative); `consumer` a valid
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockShareConsumer_add_record(
    consumer: *const kafka_consumer_ShareConsumer_t,
    topic: *const c_char,
    partition: i32,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
    offset: i64,
    out_error: *mut *mut kafka_common_KafkaError_t,
) {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(e) };
        }
        return;
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let key_bytes: Option<Bytes> = if key_len < 0 || key.is_null() {
        None
    } else {
        Some(Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(key, key_len as usize)
        }))
    };
    let value_bytes: Option<Bytes> = if value_len < 0 || value.is_null() {
        None
    } else {
        Some(Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(value, value_len as usize)
        }))
    };
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return;
        },
    };
    let record = ConsumerRecord::new(topic_str, partition, offset, key_bytes, value_bytes);
    let result = mock.add_record(record);
    if !out_error.is_null() {
        unsafe {
            *out_error = match result {
                Ok(()) => std::ptr::null_mut(),
                Err(e) => box_error(e),
            }
        };
    }
}

/// Sets the client instance ID returned by a mock share consumer (mock only).
///
/// `id` points to the 16 raw UUID bytes. No-op if `consumer` wraps a production
/// consumer or the guard cannot be acquired.
///
/// # Safety
///
/// `id` must point to 16 valid bytes; `consumer` a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockShareConsumer_set_client_instance_id(
    consumer: *const kafka_consumer_ShareConsumer_t,
    id: *const u8,
) {
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return;
    }
    let _g = ReleaseGuard(h);
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(unsafe { std::slice::from_raw_parts(id, 16) });
    if let Ok(mock) = unsafe { mock_mut(h) } {
        mock.set_client_instance_id(Uuid::from_bytes(bytes));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::ffi::{CStr, CString, c_void};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::*;
    use crate::common::protocol::Errors;
    use crate::common::{TopicIdPartition, Uuid};
    use crate::consumer::ConsumerRecords;
    use crate::consumer::acknowledgement_commit_callback::AcknowledgementCommitCallback;
    use crate::ffi::common::{kafka_common_KafkaError_destroy, kafka_common_KafkaError_message};
    use crate::ffi::records::{
        kafka_consumer_ConsumerRecord_delivery_count, kafka_consumer_ConsumerRecord_key,
        kafka_consumer_ConsumerRecord_offset, kafka_consumer_ConsumerRecord_value,
        kafka_consumer_ConsumerRecords_count, kafka_consumer_ConsumerRecords_destroy,
        kafka_consumer_ConsumerRecords_get, kafka_consumer_StringList_count, kafka_consumer_StringList_destroy,
        kafka_consumer_StringList_get,
    };

    /// Subscribes a mock consumer to `topic` over the ABI.
    fn subscribe(consumer: *const kafka_consumer_ShareConsumer_t, topic: &str) {
        let topic_c = CString::new(topic).unwrap();
        let topics = [topic_c.as_ptr()];
        let err = unsafe { kafka_consumer_ShareConsumer_subscribe(consumer, topics.as_ptr(), 1) };
        assert!(err.is_null(), "subscribe should succeed");
    }

    /// Reads a record's borrowed byte slice via a (ptr, len) accessor, or `None`
    /// if the accessor reports absence (len < 0).
    unsafe fn read_bytes(ptr: *const u8, len: i32) -> Option<Vec<u8>> {
        if len < 0 || ptr.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(ptr, len as usize) }.to_vec())
        }
    }

    /// Reads a boxed error handle's message as an owned `String`, then frees it.
    unsafe fn take_error_message(err: *mut kafka_common_KafkaError_t) -> String {
        assert!(!err.is_null(), "expected a non-null error handle");
        let msg = unsafe { CStr::from_ptr(kafka_common_KafkaError_message(err)) }
            .to_string_lossy()
            .into_owned();
        unsafe { kafka_common_KafkaError_destroy(err) };
        msg
    }

    /// A mock share consumer drives the full lifecycle over the ABI: subscribe,
    /// observe the subscription, fire a wakeup, and tear down cleanly.
    #[test]
    fn test_mock_lifecycle_subscribe_subscription_wakeup_destroy() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        assert!(!consumer.is_null());

        let topic = CString::new("share-topic").unwrap();
        let topics = [topic.as_ptr()];
        let err = unsafe { kafka_consumer_ShareConsumer_subscribe(consumer, topics.as_ptr(), 1) };
        assert!(err.is_null(), "subscribe should succeed");

        let mut sub_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let list = unsafe { kafka_consumer_ShareConsumer_subscription(consumer, &mut sub_error) };
        assert!(sub_error.is_null(), "subscription() should not error");
        assert!(!list.is_null());
        unsafe {
            assert_eq!(kafka_consumer_StringList_count(list), 1);
            let name = CStr::from_ptr(kafka_consumer_StringList_get(list, 0)).to_string_lossy();
            assert_eq!(name, "share-topic");
            kafka_consumer_StringList_destroy(list);
        }

        // Bypasses the guard; must be safe with no in-flight poll.
        unsafe { kafka_consumer_ShareConsumer_wakeup(consumer) };

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// While the single-owner guard is held, a second operation on the same
    /// handle is rejected with the multi-threaded-access error (message content
    /// asserted, not just presence).
    #[test]
    fn test_guard_rejects_concurrent_op() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        let h = unsafe { handle_ref(consumer) };

        // Take the guard directly, standing in for an in-flight operation.
        acquire(h).expect("first acquire succeeds");

        let topic = CString::new("share-topic").unwrap();
        let topics = [topic.as_ptr()];
        let err = unsafe { kafka_consumer_ShareConsumer_subscribe(consumer, topics.as_ptr(), 1) };
        let msg = unsafe { take_error_message(err) };
        assert!(
            msg.contains("not safe for multi-threaded access"),
            "unexpected guard-rejection message: {msg}"
        );

        release(h);
        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    unsafe extern "C" fn send_op_result(error: *mut kafka_common_KafkaError_t, user_data: *mut c_void) {
        let ok = error.is_null();
        // The completion callback owns the error handle; free it.
        unsafe { kafka_common_KafkaError_destroy(error) };
        let tx = unsafe { &*(user_data as *const std::sync::mpsc::Sender<bool>) };
        tx.send(ok).ok();
    }

    /// The async subscribe path fires its callback with a null error on success
    /// and releases the guard, so a follow-up sync op then succeeds.
    #[test]
    fn test_subscribe_async_fires_callback_and_releases_guard() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        let (tx, rx) = std::sync::mpsc::channel::<bool>();

        let topic = CString::new("share-topic").unwrap();
        let topics = [topic.as_ptr()];
        unsafe {
            kafka_consumer_ShareConsumer_subscribe_async(
                consumer,
                topics.as_ptr(),
                1,
                send_op_result,
                &tx as *const _ as *mut c_void,
            )
        };
        let ok = rx.recv_timeout(Duration::from_secs(5)).expect("callback must fire");
        assert!(ok, "subscribe_async should report success");

        // Guard was released inside the completion job, so this sync read works.
        let mut sub_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let list = unsafe { kafka_consumer_ShareConsumer_subscription(consumer, &mut sub_error) };
        assert!(sub_error.is_null());
        unsafe {
            assert_eq!(kafka_consumer_StringList_count(list), 1);
            kafka_consumer_StringList_destroy(list);
        }

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Frees whichever handle a poll callback receives; used where the test does
    /// not inspect the delivered batch.
    unsafe extern "C" fn poll_free_all(
        records: *mut kafka_consumer_ConsumerRecords_t,
        error: *mut kafka_common_KafkaError_t,
        _user_data: *mut c_void,
    ) {
        if !records.is_null() {
            unsafe { kafka_consumer_ConsumerRecords_destroy(records) };
        }
        if !error.is_null() {
            unsafe { kafka_common_KafkaError_destroy(error) };
        }
    }

    /// A test-only [`ShareConsumer`] whose `poll` parks its worker thread on a
    /// two-party [`Barrier`](std::sync::Barrier). It lets a test hold an async
    /// operation genuinely in flight — the worker is busy *inside* `poll`, not
    /// idle at an `await` the runtime could cancel — while it tears the handle
    /// down, so the exact window the blocking runtime-drop guard protects is
    /// exercised deterministically instead of relying on scheduling variance.
    struct BarrierConsumer {
        /// Fires once `poll` has entered and borrowed the consumer.
        started_tx: std::sync::mpsc::Sender<()>,
        /// The test releases the parked `poll` by arriving at this barrier.
        barrier: Arc<std::sync::Barrier>,
    }

    #[async_trait::async_trait]
    impl ShareConsumer<Bytes, Bytes> for BarrierConsumer {
        fn subscription(&self) -> Result<HashSet<String>, KafkaError> {
            Ok(HashSet::new())
        }

        async fn subscribe(&mut self, _topics: Vec<String>) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn poll(&mut self, _timeout: Duration) -> Result<ConsumerRecords<Bytes, Bytes>, KafkaError> {
            // Snapshot the barrier before parking so the wait runs against the
            // shared allocation (kept alive by the test), independent of `self`.
            let barrier = Arc::clone(&self.barrier);
            self.started_tx.send(()).ok();
            // Block the worker synchronously — the runtime cannot cancel a worker
            // stuck in blocking code, so dropping the runtime must join it and
            // therefore wait here until the test releases the barrier.
            barrier.wait();
            Ok(ConsumerRecords::empty())
        }

        fn acknowledge(&mut self, _record: &ConsumerRecord<Bytes, Bytes>) -> Result<(), KafkaError> {
            Ok(())
        }

        fn acknowledge_with_type(
            &mut self,
            _record: &ConsumerRecord<Bytes, Bytes>,
            _ack_type: AcknowledgeType,
        ) -> Result<(), KafkaError> {
            Ok(())
        }

        fn acknowledge_by_offset(
            &mut self,
            _topic: &str,
            _partition: i32,
            _offset: i64,
            _ack_type: AcknowledgeType,
        ) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
            Ok(HashMap::new())
        }

        async fn commit_sync_timeout(
            &mut self,
            _timeout: Duration,
        ) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
            Ok(HashMap::new())
        }

        async fn commit_async(&mut self) -> Result<(), KafkaError> {
            Ok(())
        }

        fn set_acknowledgement_commit_callback(&mut self, _callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {}

        async fn client_instance_id(&mut self, _timeout: Duration) -> Result<Uuid, KafkaError> {
            Err(KafkaError::illegal_state("clientInstanceId not set"))
        }

        fn acquisition_lock_timeout_ms(&self) -> Result<Option<i32>, KafkaError> {
            Ok(None)
        }

        async fn close(&mut self) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn close_timeout(&mut self, _timeout: Duration) -> Result<(), KafkaError> {
            Ok(())
        }

        fn wakeup(&self) {}
    }

    /// Regression guard for the teardown-safe destroy path: `destroy` must not
    /// free the handle box while an async operation is still in flight. A
    /// [`BarrierConsumer`] holds a `poll_async` genuinely in flight (its worker is
    /// parked mid-`poll`), then `destroy` is called from another thread. A correct
    /// `destroy` drops the tokio runtime, which joins the parked worker and so
    /// blocks until the operation completes; the test asserts it does NOT return
    /// early. If the blocking runtime-drop is reverted to a non-blocking
    /// `shutdown_background`, `destroy` returns while the worker is still parked —
    /// this assertion fires, and (were the barrier then released) the worker would
    /// dereference the freed handle box, which is the original SIGBUS.
    #[test]
    fn test_destroy_blocks_until_in_flight_async_completes() {
        use std::sync::mpsc;
        use std::thread;

        let (started_tx, started_rx) = mpsc::channel::<()>();
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let kind = ShareConsumerKind::Kafka(Box::new(BarrierConsumer { started_tx, barrier: Arc::clone(&barrier) }));
        let wakeup = WakeupHandle::for_mock(Arc::new(AtomicBool::new(false)));
        let consumer = build_share_consumer_handle(kind, wakeup, false);

        // Fire an async poll; the barrier-consumer parks its worker inside poll().
        unsafe { kafka_consumer_ShareConsumer_poll_async(consumer, 0, poll_free_all, std::ptr::null_mut()) };
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("poll_async must enter poll and park the worker");

        // Tear the handle down from another thread while the op is in flight.
        let consumer_addr = consumer as usize;
        let (destroy_done_tx, destroy_done_rx) = mpsc::channel::<()>();
        let destroyer = thread::spawn(move || {
            let ptr = consumer_addr as *mut kafka_consumer_ShareConsumer_t;
            unsafe { kafka_consumer_ShareConsumer_destroy(ptr) };
            destroy_done_tx.send(()).ok();
        });

        // The op is still parked, so a correct destroy (blocking runtime-drop)
        // cannot have returned yet. A non-blocking shutdown would return here.
        let returned_early = destroy_done_rx.recv_timeout(Duration::from_millis(500)).is_ok();
        assert!(
            !returned_early,
            "destroy returned while an async op was still in flight — the teardown \
             guard's blocking runtime-drop has regressed to a non-blocking shutdown"
        );

        // Release the parked poll; destroy must now drain and return, and the
        // completion job runs safely on the detached dispatcher after the box is
        // freed (it holds only its own Arc + owned result handles).
        barrier.wait();
        destroy_done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("destroy must return once the in-flight op completes");
        destroyer.join().expect("destroy thread joins cleanly");
    }

    /// A test-only [`ShareConsumer`] whose awaited ops **panic** rather than
    /// return. It stands in for an abnormal internal failure (a bug / `unwrap`
    /// inside an awaited op) and proves the async dispatch helpers stay
    /// panic-safe: on a panic unwind the spawned task's drop guard must still
    /// release the single-owner guard AND fire the C callback with an error,
    /// instead of leaving the consumer permanently locked and the caller hanging
    /// with no callback. Non-awaited methods stay benign so the guard-freedom
    /// probe and `destroy` run cleanly after the panic.
    struct PanicConsumer;

    #[async_trait::async_trait]
    impl ShareConsumer<Bytes, Bytes> for PanicConsumer {
        fn subscription(&self) -> Result<HashSet<String>, KafkaError> {
            Ok(HashSet::new())
        }

        async fn subscribe(&mut self, _topics: Vec<String>) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn poll(&mut self, _timeout: Duration) -> Result<ConsumerRecords<Bytes, Bytes>, KafkaError> {
            panic!("poll panicked (test)");
        }

        fn acknowledge(&mut self, _record: &ConsumerRecord<Bytes, Bytes>) -> Result<(), KafkaError> {
            Ok(())
        }

        fn acknowledge_with_type(
            &mut self,
            _record: &ConsumerRecord<Bytes, Bytes>,
            _ack_type: AcknowledgeType,
        ) -> Result<(), KafkaError> {
            Ok(())
        }

        fn acknowledge_by_offset(
            &mut self,
            _topic: &str,
            _partition: i32,
            _offset: i64,
            _ack_type: AcknowledgeType,
        ) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
            panic!("commit_sync panicked (test)");
        }

        async fn commit_sync_timeout(
            &mut self,
            _timeout: Duration,
        ) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
            panic!("commit_sync_timeout panicked (test)");
        }

        async fn commit_async(&mut self) -> Result<(), KafkaError> {
            panic!("commit_async panicked (test)");
        }

        fn set_acknowledgement_commit_callback(&mut self, _callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {}

        async fn client_instance_id(&mut self, _timeout: Duration) -> Result<Uuid, KafkaError> {
            Err(KafkaError::illegal_state("clientInstanceId not set"))
        }

        fn acquisition_lock_timeout_ms(&self) -> Result<Option<i32>, KafkaError> {
            Ok(None)
        }

        async fn close(&mut self) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn close_timeout(&mut self, _timeout: Duration) -> Result<(), KafkaError> {
            Ok(())
        }

        fn wakeup(&self) {}
    }

    /// Reports a poll callback's outcome as `Some(error_message)` when it received
    /// a non-null error, or `None` when it received records. Frees whichever handle
    /// is non-null, then sends the outcome over the channel in `user_data`.
    unsafe extern "C" fn capture_poll_outcome(
        records: *mut kafka_consumer_ConsumerRecords_t,
        error: *mut kafka_common_KafkaError_t,
        user_data: *mut c_void,
    ) {
        let outcome = if error.is_null() {
            if !records.is_null() {
                unsafe { kafka_consumer_ConsumerRecords_destroy(records) };
            }
            None
        } else {
            let msg = unsafe { CStr::from_ptr(kafka_common_KafkaError_message(error)) }
                .to_string_lossy()
                .into_owned();
            unsafe { kafka_common_KafkaError_destroy(error) };
            Some(msg)
        };
        let tx = unsafe { &*(user_data as *const std::sync::mpsc::Sender<Option<String>>) };
        tx.send(outcome).ok();
    }

    /// [`capture_poll_outcome`] for the commit-sync callback shape.
    unsafe extern "C" fn capture_commit_outcome(
        result: *mut kafka_consumer_ShareCommitResult_t,
        error: *mut kafka_common_KafkaError_t,
        user_data: *mut c_void,
    ) {
        let outcome = if error.is_null() {
            if !result.is_null() {
                unsafe { kafka_consumer_ShareCommitResult_destroy(result) };
            }
            None
        } else {
            let msg = unsafe { CStr::from_ptr(kafka_common_KafkaError_message(error)) }
                .to_string_lossy()
                .into_owned();
            unsafe { kafka_common_KafkaError_destroy(error) };
            Some(msg)
        };
        let tx = unsafe { &*(user_data as *const std::sync::mpsc::Sender<Option<String>>) };
        tx.send(outcome).ok();
    }

    /// Panic-safety teeth for [`kafka_consumer_ShareConsumer_poll_async`]: when the
    /// awaited `poll` panics and unwinds, the spawned task's drop guard must still
    /// release the single-owner guard AND fire the callback with a non-null error.
    /// Pre-fix (no drop guard) the completion is never enqueued, so the callback
    /// never fires — the `recv_timeout` below trips and the test fails — and the
    /// guard is never released, so the `acquire` below would also fail.
    #[test]
    fn test_poll_async_panic_releases_guard_and_fires_error() {
        let kind = ShareConsumerKind::Kafka(Box::new(PanicConsumer));
        let wakeup = WakeupHandle::for_mock(Arc::new(AtomicBool::new(false)));
        let consumer = build_share_consumer_handle(kind, wakeup, false);

        let (tx, rx) = std::sync::mpsc::channel::<Option<String>>();
        unsafe {
            kafka_consumer_ShareConsumer_poll_async(consumer, 0, capture_poll_outcome, &tx as *const _ as *mut c_void)
        };

        let outcome = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("poll callback must fire even though poll panicked");
        let msg = outcome.expect("a panicking poll must deliver a non-null error to the callback");
        assert!(
            msg.contains("failed unexpectedly"),
            "unexpected panic-completion error message: {msg}"
        );

        // The drop guard released the single-owner guard, so a fresh acquire
        // succeeds — the consumer is not locked out.
        let h = unsafe { handle_ref(consumer) };
        acquire(h).expect("the single-owner guard must be free after the panicking op");
        release(h);

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Panic-safety teeth for the value-returning async path
    /// ([`kafka_consumer_ShareConsumer_commit_sync_async`] over [`async_value_op`]):
    /// a panicking `commit_sync` still releases the guard and fires the callback
    /// with a non-null error.
    #[test]
    fn test_commit_sync_async_panic_releases_guard_and_fires_error() {
        let kind = ShareConsumerKind::Kafka(Box::new(PanicConsumer));
        let wakeup = WakeupHandle::for_mock(Arc::new(AtomicBool::new(false)));
        let consumer = build_share_consumer_handle(kind, wakeup, false);

        let (tx, rx) = std::sync::mpsc::channel::<Option<String>>();
        unsafe {
            kafka_consumer_ShareConsumer_commit_sync_async(
                consumer,
                capture_commit_outcome,
                &tx as *const _ as *mut c_void,
            )
        };

        let outcome = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("commit callback must fire even though commit_sync panicked");
        let msg = outcome.expect("a panicking commit_sync must deliver a non-null error to the callback");
        assert!(
            msg.contains("failed unexpectedly"),
            "unexpected panic-completion error message: {msg}"
        );

        let h = unsafe { handle_ref(consumer) };
        acquire(h).expect("the single-owner guard must be free after the panicking op");
        release(h);

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Panic-safety teeth for the void async path ([`async_void_op`], here via
    /// [`kafka_consumer_ShareConsumer_commit_async_async`]): a panicking void op
    /// still releases the guard and fires the callback with a non-null error
    /// (`send_op_result` reports `false` when the delivered error is non-null).
    #[test]
    fn test_void_async_op_panic_releases_guard_and_fires_error() {
        let kind = ShareConsumerKind::Kafka(Box::new(PanicConsumer));
        let wakeup = WakeupHandle::for_mock(Arc::new(AtomicBool::new(false)));
        let consumer = build_share_consumer_handle(kind, wakeup, false);

        let (tx, rx) = std::sync::mpsc::channel::<bool>();
        unsafe {
            kafka_consumer_ShareConsumer_commit_async_async(consumer, send_op_result, &tx as *const _ as *mut c_void)
        };
        let ok = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("void-op callback must fire even though the op panicked");
        assert!(!ok, "a panicking void op must deliver a non-null error to the callback");

        let h = unsafe { handle_ref(consumer) };
        acquire(h).expect("the single-owner guard must be free after the panicking op");
        release(h);

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Adds a record to a subscribed mock, then polls it back over the ABI and
    /// reads the offset, key, value, and delivery count. The byte slices are
    /// borrowed from the boxed batch (zero-copy) and stay valid until it is
    /// destroyed.
    #[test]
    fn test_poll_reads_added_record_fields() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        subscribe(consumer, "share-topic");

        let topic = CString::new("share-topic").unwrap();
        let key = b"k1";
        let value = b"v1";
        let mut add_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        unsafe {
            kafka_consumer_MockShareConsumer_add_record(
                consumer,
                topic.as_ptr(),
                0,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
                7,
                &mut add_error,
            )
        };
        assert!(add_error.is_null(), "add_record should succeed on a subscribed topic");

        let mut poll_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let records = unsafe { kafka_consumer_ShareConsumer_poll(consumer, 0, &mut poll_error) };
        assert!(poll_error.is_null());
        assert!(!records.is_null());

        unsafe {
            assert_eq!(kafka_consumer_ConsumerRecords_count(records), 1);
            let rec = kafka_consumer_ConsumerRecords_get(records, 0);
            assert!(!rec.is_null());
            assert_eq!(kafka_consumer_ConsumerRecord_offset(rec), 7);

            let mut key_len = 0i32;
            let key_ptr = kafka_consumer_ConsumerRecord_key(rec, &mut key_len);
            assert_eq!(read_bytes(key_ptr, key_len), Some(b"k1".to_vec()));

            let mut value_len = 0i32;
            let value_ptr = kafka_consumer_ConsumerRecord_value(rec, &mut value_len);
            assert_eq!(read_bytes(value_ptr, value_len), Some(b"v1".to_vec()));

            // A mock-added record carries no broker-assigned delivery count.
            let mut delivery = 0i32;
            assert!(!kafka_consumer_ConsumerRecord_delivery_count(rec, &mut delivery));

            kafka_consumer_ConsumerRecords_destroy(records);
        }

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    unsafe extern "C" fn send_poll_count(
        records: *mut kafka_consumer_ConsumerRecords_t,
        error: *mut kafka_common_KafkaError_t,
        user_data: *mut c_void,
    ) {
        let count = if records.is_null() {
            unsafe { kafka_common_KafkaError_destroy(error) };
            -1
        } else {
            let n = unsafe { kafka_consumer_ConsumerRecords_count(records) };
            unsafe { kafka_consumer_ConsumerRecords_destroy(records) };
            n
        };
        let tx = unsafe { &*(user_data as *const std::sync::mpsc::Sender<i32>) };
        tx.send(count).ok();
    }

    /// The async poll path delivers the boxed batch to its callback and releases
    /// the guard inside the completion job.
    #[test]
    fn test_poll_async_delivers_batch_to_callback() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        subscribe(consumer, "share-topic");

        let topic = CString::new("share-topic").unwrap();
        let value = b"v1";
        let mut add_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        unsafe {
            kafka_consumer_MockShareConsumer_add_record(
                consumer,
                topic.as_ptr(),
                0,
                std::ptr::null(),
                -1,
                value.as_ptr(),
                value.len() as i32,
                0,
                &mut add_error,
            )
        };
        assert!(add_error.is_null());

        let (tx, rx) = std::sync::mpsc::channel::<i32>();
        unsafe {
            kafka_consumer_ShareConsumer_poll_async(consumer, 0, send_poll_count, &tx as *const _ as *mut c_void)
        };
        let count = rx.recv_timeout(Duration::from_secs(5)).expect("poll callback must fire");
        assert_eq!(count, 1, "poll_async should deliver the one added record");

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Polls a record from a mock, then acknowledges it through all three entry
    /// points. The mock accepts every acknowledgement (it does not track
    /// in-flight offsets), so each returns a null error; the non-in-flight
    /// `IllegalState` path exercises the production consumer and is covered by
    /// the broker-driven integration layer.
    #[test]
    fn test_acknowledge_polled_record() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        subscribe(consumer, "share-topic");

        let topic = CString::new("share-topic").unwrap();
        let value = b"v1";
        let mut add_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        unsafe {
            kafka_consumer_MockShareConsumer_add_record(
                consumer,
                topic.as_ptr(),
                0,
                std::ptr::null(),
                -1,
                value.as_ptr(),
                value.len() as i32,
                3,
                &mut add_error,
            )
        };
        assert!(add_error.is_null());

        let mut poll_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let records = unsafe { kafka_consumer_ShareConsumer_poll(consumer, 0, &mut poll_error) };
        assert!(!records.is_null());

        unsafe {
            let rec = kafka_consumer_ConsumerRecords_get(records, 0);

            let err = kafka_consumer_ShareConsumer_acknowledge(consumer, rec);
            assert!(err.is_null(), "acknowledge (accept) should succeed on the mock");

            let err = kafka_consumer_ShareConsumer_acknowledge_with_type(
                consumer,
                rec,
                kafka_consumer_AcknowledgeType_t::RELEASE,
            );
            assert!(err.is_null(), "acknowledge_with_type should succeed on the mock");

            let err = kafka_consumer_ShareConsumer_acknowledge_by_offset(
                consumer,
                topic.as_ptr(),
                0,
                3,
                kafka_consumer_AcknowledgeType_t::REJECT,
            );
            assert!(err.is_null(), "acknowledge_by_offset should succeed on the mock");

            kafka_consumer_ConsumerRecords_destroy(records);
        }

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Acknowledging while the single-owner guard is held is rejected with the
    /// multi-threaded-access error.
    #[test]
    fn test_acknowledge_by_offset_rejected_under_guard() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        let h = unsafe { handle_ref(consumer) };
        acquire(h).expect("first acquire succeeds");

        let topic = CString::new("share-topic").unwrap();
        let err = unsafe {
            kafka_consumer_ShareConsumer_acknowledge_by_offset(
                consumer,
                topic.as_ptr(),
                0,
                0,
                kafka_consumer_AcknowledgeType_t::ACCEPT,
            )
        };
        let msg = unsafe { take_error_message(err) };
        assert!(msg.contains("not safe for multi-threaded access"), "unexpected message: {msg}");

        release(h);
        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Constructing a production share consumer with a blank `group.id` fails at
    /// the ABI boundary: a null handle is returned and `out_error` carries the
    /// precise cause (the unwrapped `group.id` rejection, not the generic
    /// construction-wrapper message).
    #[test]
    fn test_kafka_share_consumer_new_blank_group_id_sets_out_error() {
        let props = kafka_consumer_ShareConsumerProperties_new();
        let bootstrap_key = CString::new("bootstrap.servers").unwrap();
        let bootstrap_val = CString::new("localhost:59999").unwrap();
        let group_key = CString::new("group.id").unwrap();
        let group_blank = CString::new("   ").unwrap();
        unsafe {
            kafka_consumer_ShareConsumerProperties_put(props, bootstrap_key.as_ptr(), bootstrap_val.as_ptr());
            kafka_consumer_ShareConsumerProperties_put(props, group_key.as_ptr(), group_blank.as_ptr());
        }

        let mut out_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let consumer = unsafe { kafka_consumer_KafkaShareConsumer_new(props, &mut out_error) };
        assert!(consumer.is_null(), "a blank group.id must not build a consumer");

        let msg = unsafe { take_error_message(out_error) };
        assert!(
            msg.contains("You must provide a valid group.id"),
            "unexpected group.id rejection message: {msg}"
        );

        unsafe { kafka_consumer_ShareConsumerProperties_destroy(props) };
    }

    /// The mock's `commit_sync` returns an empty per-partition outcome map; over
    /// the ABI that surfaces as a non-null result handle with count 0 and no
    /// error.
    #[test]
    fn test_commit_sync_returns_empty_result_on_mock() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        subscribe(consumer, "share-topic");

        let mut out_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let result = unsafe { kafka_consumer_ShareConsumer_commit_sync(consumer, &mut out_error) };
        assert!(out_error.is_null(), "commit_sync should not error on the mock");
        assert!(!result.is_null(), "commit_sync returns a (possibly empty) result handle");
        unsafe {
            assert_eq!(kafka_consumer_ShareCommitResult_count(result), 0);
            kafka_consumer_ShareCommitResult_destroy(result);
        }
        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// Directly exercises the `ShareCommitResult` container with a synthetic
    /// outcome map: one partition that committed OK (null error) and one that
    /// failed. The mock always commits an empty map, so this is the only test
    /// that verifies the partition + per-partition error marshaling — the topic
    /// id, name, partition, and error message survive the boundary, and a null
    /// error means "committed OK".
    #[test]
    fn test_share_commit_result_container_marshals_partitions_and_errors() {
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let ok_tip = TopicIdPartition::from_parts(topic_id, 0, "topic-ok");
        let err_tip = TopicIdPartition::from_parts(topic_id, 1, "topic-err");

        let mut map: HashMap<TopicIdPartition, Option<KafkaError>> = HashMap::new();
        map.insert(ok_tip, None);
        map.insert(
            err_tip,
            Some(KafkaError::with_message(
                Errors::InvalidRecordState,
                "record no longer acquirable",
            )),
        );

        let result = box_share_commit_result(map);
        assert!(!result.is_null());
        unsafe {
            assert_eq!(kafka_consumer_ShareCommitResult_count(result), 2);

            // Entries are sorted by (topic, partition): "topic-err" before "topic-ok".
            let p0 = kafka_consumer_ShareCommitResult_get_partition(result, 0);
            assert!(!p0.is_null());
            let topic0 = CStr::from_ptr(kafka_common_TopicIdPartition_topic(p0)).to_string_lossy();
            assert_eq!(topic0, "topic-err");
            assert_eq!(kafka_common_TopicIdPartition_partition(p0), 1);
            let id_bytes = std::slice::from_raw_parts(kafka_common_TopicIdPartition_topic_id(p0), 16);
            assert_eq!(id_bytes, topic_id.to_bytes());

            // "topic-err" carries the failure, message content preserved.
            let e0 = kafka_consumer_ShareCommitResult_get_error(result, 0);
            assert!(!e0.is_null(), "the failed partition must expose its error");
            let msg = CStr::from_ptr(kafka_common_KafkaError_message(e0)).to_string_lossy();
            assert_eq!(msg, "record no longer acquirable");

            // "topic-ok" committed successfully → null error.
            let p1 = kafka_consumer_ShareCommitResult_get_partition(result, 1);
            let topic1 = CStr::from_ptr(kafka_common_TopicIdPartition_topic(p1)).to_string_lossy();
            assert_eq!(topic1, "topic-ok");
            assert_eq!(kafka_common_TopicIdPartition_partition(p1), 0);
            assert!(
                kafka_consumer_ShareCommitResult_get_error(result, 1).is_null(),
                "a committed-OK partition must report a null error"
            );

            // Out-of-range indices are null-safe on both accessors.
            assert!(kafka_consumer_ShareCommitResult_get_partition(result, 2).is_null());
            assert!(kafka_consumer_ShareCommitResult_get_error(result, 2).is_null());

            kafka_consumer_ShareCommitResult_destroy(result);
        }
    }

    /// `commit_async` on the mock completes without error.
    #[test]
    fn test_commit_async_succeeds_on_mock() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        subscribe(consumer, "share-topic");
        let err = unsafe { kafka_consumer_ShareConsumer_commit_async(consumer) };
        assert!(err.is_null(), "commit_async should succeed on the mock");
        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// `close` and `close_timeout` on the mock complete without error.
    #[test]
    fn test_close_succeeds_on_mock() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        let err = unsafe { kafka_consumer_ShareConsumer_close(consumer) };
        assert!(err.is_null(), "close should succeed on the mock");
        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };

        let consumer = kafka_consumer_MockShareConsumer_new();
        let err = unsafe { kafka_consumer_ShareConsumer_close_timeout(consumer, 5000) };
        assert!(err.is_null(), "close_timeout should succeed on the mock");
        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// The mock reports no acquisition-lock timeout: presence is false, no error
    /// is set, and the out-parameter is left untouched.
    #[test]
    fn test_acquisition_lock_timeout_ms_absent_on_mock() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        let mut out_ms = 12345i32; // sentinel; must survive an "absent" result
        let mut out_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let present =
            unsafe { kafka_consumer_ShareConsumer_acquisition_lock_timeout_ms(consumer, &mut out_ms, &mut out_error) };
        assert!(!present, "the mock reports no acquisition-lock timeout");
        assert!(out_error.is_null(), "absence is not an error");
        assert_eq!(out_ms, 12345, "out_ms must be left untouched when the timeout is absent");
        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    unsafe extern "C" fn send_commit_count(
        result: *mut kafka_consumer_ShareCommitResult_t,
        error: *mut kafka_common_KafkaError_t,
        user_data: *mut c_void,
    ) {
        let count = if result.is_null() {
            unsafe { kafka_common_KafkaError_destroy(error) };
            -1
        } else {
            let n = unsafe { kafka_consumer_ShareCommitResult_count(result) };
            unsafe { kafka_consumer_ShareCommitResult_destroy(result) };
            n
        };
        let tx = unsafe { &*(user_data as *const std::sync::mpsc::Sender<i32>) };
        tx.send(count).ok();
    }

    /// The async commit-sync path delivers a non-null (empty) result to its
    /// callback and releases the guard inside the completion job, so a follow-up
    /// sync commit then succeeds.
    #[test]
    fn test_commit_sync_async_delivers_result_and_releases_guard() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        subscribe(consumer, "share-topic");

        let (tx, rx) = std::sync::mpsc::channel::<i32>();
        unsafe {
            kafka_consumer_ShareConsumer_commit_sync_async(consumer, send_commit_count, &tx as *const _ as *mut c_void)
        };
        let count = rx.recv_timeout(Duration::from_secs(5)).expect("commit callback must fire");
        assert_eq!(count, 0, "the mock commits an empty outcome map");

        // Guard released in the completion job → this sync commit now succeeds.
        let mut out_error: *mut kafka_common_KafkaError_t = std::ptr::null_mut();
        let result = unsafe { kafka_consumer_ShareConsumer_commit_sync(consumer, &mut out_error) };
        assert!(out_error.is_null());
        assert!(!result.is_null());
        unsafe { kafka_consumer_ShareCommitResult_destroy(result) };

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }

    /// What a stub C ack callback observed, marshaled back to owned Rust values.
    #[derive(Debug)]
    struct DeliveredAck {
        /// `(topic, 16-byte topic id, partition, sorted offsets)` per partition.
        partitions: Vec<(String, [u8; 16], i32, Vec<i64>)>,
        error_msg: Option<String>,
    }

    /// A Rust-defined stub for the C ack-commit callback: reads every field back
    /// out of the delivered `ShareAcknowledgeOffsets_t` (and the optional error),
    /// frees the owned handles exactly once, then sends the observed data to the
    /// test over a channel in `user_data`.
    unsafe extern "C" fn send_ack_delivery(
        offsets: *const kafka_consumer_ShareAcknowledgeOffsets_t,
        error: *const kafka_common_KafkaError_t,
        user_data: *mut c_void,
    ) {
        let mut partitions = Vec::new();
        let pc = unsafe { kafka_consumer_ShareAcknowledgeOffsets_partition_count(offsets) };
        for i in 0..pc {
            let tip = unsafe { kafka_consumer_ShareAcknowledgeOffsets_get_partition(offsets, i) };
            let topic = unsafe { CStr::from_ptr(kafka_common_TopicIdPartition_topic(tip)) }
                .to_string_lossy()
                .into_owned();
            let mut id = [0u8; 16];
            id.copy_from_slice(unsafe { std::slice::from_raw_parts(kafka_common_TopicIdPartition_topic_id(tip), 16) });
            let part = unsafe { kafka_common_TopicIdPartition_partition(tip) };
            let oc = unsafe { kafka_consumer_ShareAcknowledgeOffsets_offset_count(offsets, i) };
            let offs: Vec<i64> = (0..oc)
                .map(|j| unsafe { kafka_consumer_ShareAcknowledgeOffsets_get_offset(offsets, i, j) })
                .collect();
            partitions.push((topic, id, part, offs));
        }
        let error_msg = if error.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(kafka_common_KafkaError_message(error)) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        // The callback owns the delivered handles; free them exactly once.
        unsafe { kafka_consumer_ShareAcknowledgeOffsets_destroy(offsets as *mut _) };
        if !error.is_null() {
            unsafe { kafka_common_KafkaError_destroy(error as *mut _) };
        }
        let tx = unsafe { &*(user_data as *const std::sync::mpsc::Sender<DeliveredAck>) };
        tx.send(DeliveredAck { partitions, error_msg }).ok();
    }

    /// Drives [`FfiAckCommitCallback::on_complete`] directly with a synthetic
    /// completed-offsets map and a failure error, then asserts the delivered
    /// `ShareAcknowledgeOffsets_t` has the right partitions (sorted), the offsets
    /// sorted ascending, the topic id bytes intact, and the error message mapped.
    /// This is the teeth for the marshaling: `MockShareConsumer`'s setter is a
    /// no-op and never fires the callback, so end-to-end firing through the ABI
    /// is covered by the share-consumer §31 drain tests, not here.
    #[test]
    fn test_ffi_ack_commit_callback_marshals_offsets_and_error() {
        let (tx, dispatcher) = common::spawn_dispatcher("test-ack-dispatcher");
        let (result_tx, result_rx) = std::sync::mpsc::channel::<DeliveredAck>();

        let topic_id = Uuid::new(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);
        let mut map: HashMap<TopicIdPartition, HashSet<i64>> = HashMap::new();
        map.insert(TopicIdPartition::from_parts(topic_id, 0, "t-a"), HashSet::from([10, 5, 7]));
        map.insert(TopicIdPartition::from_parts(topic_id, 3, "t-b"), HashSet::from([1]));

        let cb = FfiAckCommitCallback {
            callback: send_ack_delivery,
            user_data: SendUserData(&result_tx as *const _ as *mut c_void),
            completion_tx: tx,
        };

        let err = KafkaError::with_message(Errors::InvalidRecordState, "ack failed");
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(cb.on_complete(&map, Some(&err)));

        let delivered = result_rx.recv_timeout(Duration::from_secs(5)).expect("ack callback must fire");
        assert_eq!(delivered.partitions.len(), 2);

        // Sorted by (topic, partition): t-a(0) before t-b(3).
        let (topic0, id0, part0, offs0) = &delivered.partitions[0];
        assert_eq!(topic0, "t-a");
        assert_eq!(*id0, topic_id.to_bytes());
        assert_eq!(*part0, 0);
        assert_eq!(offs0, &vec![5, 7, 10], "offsets are marshaled sorted ascending");

        let (topic1, _id1, part1, offs1) = &delivered.partitions[1];
        assert_eq!(topic1, "t-b");
        assert_eq!(*part1, 3);
        assert_eq!(offs1, &vec![1]);

        assert_eq!(delivered.error_msg.as_deref(), Some("ack failed"));

        // Drop the last sender so the dispatcher drains and exits; join to prove
        // the completion job ran and freed its handles without leak/double-free.
        drop(cb);
        dispatcher.join().expect("dispatcher joins cleanly");
    }

    /// A successful acknowledgement commit delivers a non-null offsets handle and
    /// a **null** error to the callback.
    #[test]
    fn test_ffi_ack_commit_callback_null_error_on_success() {
        let (tx, dispatcher) = common::spawn_dispatcher("test-ack-dispatcher-ok");
        let (result_tx, result_rx) = std::sync::mpsc::channel::<DeliveredAck>();

        let topic_id = Uuid::new(1, 2);
        let mut map: HashMap<TopicIdPartition, HashSet<i64>> = HashMap::new();
        map.insert(TopicIdPartition::from_parts(topic_id, 2, "t-ok"), HashSet::from([42]));

        let cb = FfiAckCommitCallback {
            callback: send_ack_delivery,
            user_data: SendUserData(&result_tx as *const _ as *mut c_void),
            completion_tx: tx,
        };

        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(cb.on_complete(&map, None));

        let delivered = result_rx.recv_timeout(Duration::from_secs(5)).expect("ack callback must fire");
        assert_eq!(delivered.partitions.len(), 1);
        assert_eq!(delivered.partitions[0].0, "t-ok");
        assert_eq!(delivered.partitions[0].3, vec![42]);
        assert!(delivered.error_msg.is_none(), "a successful commit delivers a null error");

        drop(cb);
        dispatcher.join().expect("dispatcher joins cleanly");
    }

    /// A stub C ack callback that just frees the delivered handles.
    unsafe extern "C" fn drop_ack(
        offsets: *const kafka_consumer_ShareAcknowledgeOffsets_t,
        error: *const kafka_common_KafkaError_t,
        _user_data: *mut c_void,
    ) {
        unsafe { kafka_consumer_ShareAcknowledgeOffsets_destroy(offsets as *mut _) };
        if !error.is_null() {
            unsafe { kafka_common_KafkaError_destroy(error as *mut _) };
        }
    }

    /// Registering then clearing the acknowledgement-commit callback over the ABI
    /// succeeds and does not crash (the mock's setter is a no-op; clearing passes
    /// `None`, which drops the stored C pointers on a production consumer).
    #[test]
    fn test_set_acknowledgement_commit_callback_register_then_clear() {
        let consumer = kafka_consumer_MockShareConsumer_new();

        let err = unsafe {
            kafka_consumer_ShareConsumer_set_acknowledgement_commit_callback(
                consumer,
                Some(drop_ack),
                std::ptr::null_mut(),
            )
        };
        assert!(err.is_null(), "registering a callback should succeed on the mock");

        let err = unsafe {
            kafka_consumer_ShareConsumer_set_acknowledgement_commit_callback(consumer, None, std::ptr::null_mut())
        };
        assert!(err.is_null(), "clearing the callback should succeed on the mock");

        unsafe { kafka_consumer_ShareConsumer_destroy(consumer) };
    }
}
