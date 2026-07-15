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
//! functions, so non-Rust callers can drive the subscribe → poll → acknowledge
//! flow. Commit / close / the acknowledgement-commit callback are a later phase.
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
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::common::serialization::BytesDeserializer;
use crate::common::{KafkaError, Uuid};
use crate::consumer::{
    ConsumerRecord, MockShareConsumer, ShareConsumer, ShareConsumerConfig, WakeupHandle, new_share_consumer_with_wakeup,
};

use super::common::{
    self, CompletionJob, OperationCallbackFn, OperationCallbackTarget, OperationCompletion, box_error,
    enqueue_or_run_inline, init_default_logger, kafka_common_KafkaError_t,
};
use super::records::{box_records, box_string_list, kafka_consumer_ConsumerRecords_t, kafka_consumer_StringList_t};

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

/// Releases the single-owner guard for `h`.
fn release(h: &ShareConsumerHandle) {
    h.owner.store(NO_OWNER, Ordering::Release);
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
/// [`UnsafeCell`] and guards access with the single-owner [`AtomicU64`]; see the
/// module documentation for the concurrency model.
struct ShareConsumerHandle {
    /// The consumer. Exclusive access is enforced by `owner`, not the type
    /// system — `UnsafeCell` is needed to hand out `&mut` from a shared
    /// `&ShareConsumerHandle`.
    consumer: UnsafeCell<ShareConsumerKind>,
    /// `NO_OWNER`, or the thread id (from [`current_thread_id`]) holding it.
    owner: AtomicU64,
    /// Drives app-side async methods via `block_on` (sync path).
    runtime: tokio::runtime::Runtime,
    /// Handle for spawning async-variant awaiters (async path).
    runtime_handle: tokio::runtime::Handle,
    /// Sender for the completion-dispatch queue (shared machinery).
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
    /// Dispatcher thread join handle; detached on destroy.
    dispatcher: Mutex<Option<std::thread::JoinHandle<()>>>,
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
        owner: AtomicU64::new(NO_OWNER),
        runtime,
        runtime_handle,
        completion_tx,
        dispatcher: Mutex::new(Some(dispatcher)),
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
    let handle = unsafe { Box::from_raw(consumer as *mut ShareConsumerHandle) };
    let ShareConsumerHandle { consumer, runtime, completion_tx, dispatcher, .. } = *handle;

    // 1. Shut down the runtime first. This cancels any in-flight async-variant
    //    future that borrows `*consumer.get()`, so the consumer is no longer
    //    aliased when we drop it next.
    runtime.shutdown_background();
    // 2. Drop the consumer; its own `Drop` signals and joins the bg pipeline.
    drop(consumer);
    // 3. Close the completion channel and detach the dispatcher (do NOT join —
    //    outstanding completion jobs may still hold a cloned `completion_tx`,
    //    and the dispatcher exits once all clones are released).
    drop(completion_tx);
    drop(dispatcher.into_inner().unwrap_or(None));
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
    match h.runtime.block_on(fut) {
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
    // Capture the `&'static ShareConsumerHandle` (Send+Sync via the unsafe
    // impls), NOT a bare `*mut` (raw pointers are !Send and would make the
    // future !Send). The handle is leaked, so the borrow is effectively
    // `'static`.
    let hs: &'static ShareConsumerHandle = unsafe { handle_ref(consumer) };
    h.runtime_handle.spawn(async move {
        let target = target;
        // SAFETY: the guard is held for the whole submit->callback window.
        let consumer = unsafe { consumer_mut(hs) };
        let result = op(consumer).await;
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
            release(hs);
            unsafe { completion.fire() };
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
    let result = h.runtime.block_on(unsafe { consumer_mut(h).poll(timeout) });
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
/// result handles (one of `records`/`error` is non-null) and the `&'static
/// ShareConsumerHandle` so the access guard is released **after** the awaited op
/// completes but **before** the callback fires.
struct PollCompletion {
    target: PollCallbackTarget,
    records: *mut kafka_consumer_ConsumerRecords_t,
    error: *mut kafka_common_KafkaError_t,
    handle: &'static ShareConsumerHandle,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread; the
// C user owns the thread-safety of `user_data`. The `&ShareConsumerHandle` is
// Send via the type's `unsafe impl Send`.
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
        release(self.handle);
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
    // Capture the `&'static ShareConsumerHandle` (Send+Sync via the unsafe
    // impls), NOT a bare `*mut` (raw pointers are !Send and would make the
    // future !Send). The handle is leaked, so the borrow is effectively
    // `'static`.
    let hs: &'static ShareConsumerHandle = unsafe { handle_ref(consumer) };
    h.runtime_handle.spawn(async move {
        let target = target;
        let result = unsafe { consumer_mut(hs).poll(timeout).await };
        // No `.await` after building the raw handles below.
        let (records, error) = match result {
            Ok(r) => (box_records(r), std::ptr::null_mut()),
            Err(e) => (std::ptr::null_mut(), box_error(e)),
        };
        let completion = PollCompletion { target, records, error, handle: hs };
        let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
        enqueue_or_run_inline(&tx, job);
    });
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
    use std::ffi::{CStr, CString, c_void};
    use std::time::Duration;

    use super::*;
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

    unsafe extern "C" fn free_error_only(error: *mut kafka_common_KafkaError_t, _user_data: *mut c_void) {
        unsafe { kafka_common_KafkaError_destroy(error) };
    }

    /// Destroying a handle while an async op may still be in flight is safe: the
    /// runtime is shut down, the consumer dropped, and the dispatcher detached
    /// without a deadlock. `user_data` is null so nothing dangles if the
    /// callback runs on the dispatcher after teardown returns.
    #[test]
    fn test_destroy_after_in_flight_async_is_safe() {
        let consumer = kafka_consumer_MockShareConsumer_new();
        let topic = CString::new("share-topic").unwrap();
        let topics = [topic.as_ptr()];
        unsafe {
            kafka_consumer_ShareConsumer_subscribe_async(
                consumer,
                topics.as_ptr(),
                1,
                free_error_only,
                std::ptr::null_mut(),
            );
            kafka_consumer_ShareConsumer_destroy(consumer);
        }
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
}
