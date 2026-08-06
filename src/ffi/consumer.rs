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

//! C FFI layer for the Kafka consumer API.
//!
//! This module exposes the consumer (`Consumer<Vec<u8>, Vec<u8>>` trait,
//! [`AsyncKafkaConsumer`], [`MockConsumer`]) via C-callable `extern "C"`
//! functions.
//!
//! # Concurrency model — single-owner access guard
//!
//! Unlike the producer (`KafkaProducer: Sync`), the `Consumer` trait is
//! `Send + 'static` but **not** `Sync`, and every blocking method takes
//! `&mut self`. The handle owns the consumer directly behind an
//! [`UnsafeCell`] and a non-reentrant single-owner guard (`owner: AtomicU64`),
//! mirroring Java's `KafkaConsumer.acquire()/release()`:
//!
//! - Every FFI call (sync or async) must [`acquire`] before touching the
//!   consumer. Concurrent access from a second thread — or, for the async
//!   surface, a second operation while one is already in flight — fails fast
//!   with [`KafkaError::concurrent_modification`], exactly as Java throws
//!   `ConcurrentModificationException`.
//! - [`kafka_consumer_Consumer_wakeup`] is the one method that **bypasses**
//!   the guard, matching Java.
//!
//! See `design/current/consumer-ffi-plan.md` (Phase B/C) for the full design.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with
// PascalCase type names, which intentionally differs from Rust's snake_case
// convention.
#![allow(non_snake_case, non_camel_case_types)]

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::common::header::{Header, RecordHeader};
use crate::common::serialization::BytesDeserializer;
use crate::common::{KafkaError, Node, PartitionInfo, TopicPartition};
use crate::consumer::async_kafka_consumer::AsyncKafkaConsumer;
use crate::consumer::{
    AutoOffsetResetStrategy, CloseOptions, Consumer, ConsumerGroupMetadata, ConsumerHandle, ConsumerRecord,
    ConsumerRecords, GroupProtocol, MockConsumer, OffsetAndMetadata, OffsetAndTimestamp,
};

use super::common::{
    self, CompletionJob, OperationCallbackFn, OperationCallbackTarget, OperationCompletion, box_error,
    enqueue_or_run_inline, init_default_logger, kafka_common_KafkaError_t,
};

// The byte-array consumer is monomorphized over refcounted `bytes::Bytes` keys
// and values, so each record's key/value is a zero-copy slice of the owning
// fetch buffer (consumer-threading.md §27). The C side borrows ptr+len from the
// `Bytes` (which derefs to `&[u8]`); the batch keeps the buffer alive.
type Bytes = bytes::Bytes;

// ---------------------------------------------------------------------------
// Access guard
// ---------------------------------------------------------------------------

/// Free sentinel for [`FfiConsumerHandle::owner`]: no thread/future currently
/// holds the consumer.
const NO_OWNER: u64 = u64::MAX;

/// Returns a stable, process-unique `u64` identifying the current OS thread.
///
/// The value need only be stable for the lifetime of the thread and distinct
/// from other live threads' values — we do not depend on a platform thread-id
/// API. A thread-local counter handed out from a process-global atomic
/// satisfies both properties (and avoids hashing `ThreadId`, whose internal
/// representation is unspecified). `NO_OWNER` (`u64::MAX`) is reserved as the
/// free sentinel, so the counter starts at 0 and would only collide after
/// `u64::MAX` thread creations.
fn current_thread_id() -> u64 {
    use std::cell::Cell;
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    thread_local! {
        static THREAD_ID: Cell<u64> = Cell::new(NEXT_ID.fetch_add(1, Ordering::Relaxed));
    }
    THREAD_ID.with(Cell::get)
}

/// Acquires the single-owner guard for `h`. Returns
/// [`KafkaError::concurrent_modification`] if another thread/future already
/// holds it (the non-reentrant guard rejects re-entry too — see module docs).
fn acquire(h: &FfiConsumerHandle) -> Result<(), KafkaError> {
    let tid = current_thread_id();
    match h.owner.compare_exchange(NO_OWNER, tid, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Ok(()),
        Err(_) => Err(KafkaError::concurrent_modification(
            "KafkaConsumer is not safe for multi-threaded access.",
        )),
    }
}

/// Releases the single-owner guard for `h`.
fn release(h: &FfiConsumerHandle) {
    h.owner.store(NO_OWNER, Ordering::Release);
}

/// RAII guard that releases the access guard on scope exit (return or panic),
/// mirroring Java's `finally { release(); }`. Used by the sync FFI path; the
/// async path releases inside the completion job instead.
struct ReleaseGuard<'a>(&'a FfiConsumerHandle);
impl Drop for ReleaseGuard<'_> {
    fn drop(&mut self) {
        release(self.0);
    }
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// The two consumer implementations exposed through the FFI.
enum ConsumerKind {
    /// Production KIP-848 consumer.
    Async(Box<dyn Consumer<Bytes, Bytes>>),
    /// Broker-less test consumer with FFI driver methods.
    Mock(Box<MockConsumer<Bytes, Bytes>>),
}

/// Per-consumer handle state. Owns the consumer directly behind an
/// [`UnsafeCell`] and guards access with the single-owner [`AtomicU64`]; see
/// the module documentation for the concurrency model.
struct FfiConsumerHandle {
    /// The consumer. Exclusive access is enforced by `owner`, not the type
    /// system — `UnsafeCell` is needed to hand out `&mut` from a shared
    /// `&FfiConsumerHandle`.
    consumer: UnsafeCell<ConsumerKind>,
    /// `NO_OWNER`, or the thread id (from [`current_thread_id`]) holding it.
    owner: AtomicU64,
    /// Drives app-side async methods via `block_on` (sync path).
    runtime: tokio::runtime::Runtime,
    /// Handle for spawning async-variant awaiters (async path).
    runtime_handle: tokio::runtime::Handle,
    /// Sender for the completion-dispatch queue (reused from `common.rs`).
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
    /// Dispatcher thread join handle; detached on destroy.
    dispatcher: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Core [`ConsumerHandle`] captured at construction. Its `wakeup()` is
    /// fired by [`kafka_consumer_Consumer_wakeup`] without acquiring the guard,
    /// which is the whole point of the handle: every method takes `&self`, so
    /// it bypasses the single-owner guard by design.
    consumer_handle: ConsumerHandle,
    /// Whether this handle wraps a [`MockConsumer`].
    #[allow(dead_code)]
    is_mock: bool,
}

// SAFETY: `acquire()` guarantees at most one thread/future accesses
// `*consumer.get()` at any instant, and `ConsumerKind: Send`, so exclusive
// cross-thread access is sound. `UnsafeCell` is needed to hand out `&mut` from
// a shared `&FfiConsumerHandle`.
unsafe impl Send for FfiConsumerHandle {}
unsafe impl Sync for FfiConsumerHandle {}

/// Builds a [`FfiConsumerHandle`] around a [`ConsumerKind`], spawning the
/// callback dispatcher thread, and returns the leaked C handle.
fn build_consumer_handle(
    kind: ConsumerKind,
    consumer_handle: ConsumerHandle,
    is_mock: bool,
) -> *mut kafka_consumer_Consumer_t {
    // A multi-thread runtime so the async-variant awaiter tasks and the
    // consumer's own background task make progress outside `block_on`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for KafkaConsumer");
    let runtime_handle = runtime.handle().clone();
    let (completion_tx, dispatcher) = common::spawn_dispatcher("kafka-consumer-callback-dispatcher");

    let handle = Box::new(FfiConsumerHandle {
        consumer: UnsafeCell::new(kind),
        owner: AtomicU64::new(NO_OWNER),
        runtime,
        runtime_handle,
        completion_tx,
        dispatcher: Mutex::new(Some(dispatcher)),
        consumer_handle,
        is_mock,
    });
    Box::into_raw(handle) as *mut kafka_consumer_Consumer_t
}

/// Casts a `*const kafka_consumer_Consumer_t` to a `&'static FfiConsumerHandle`.
///
/// # Safety
///
/// `consumer` must be non-null and created by a consumer constructor.
unsafe fn handle_ref(consumer: *const kafka_consumer_Consumer_t) -> &'static FfiConsumerHandle {
    unsafe { &*(consumer as *const FfiConsumerHandle) }
}

// ---------------------------------------------------------------------------
// Opaque types
// ---------------------------------------------------------------------------

/// Opaque consumer handle.
#[repr(C)]
pub struct kafka_consumer_Consumer_t {
    _private: [u8; 0],
}

/// Opaque consumer-configuration properties handle (a `HashMap<String,String>`).
#[repr(C)]
pub struct kafka_consumer_ConsumerProperties_t {
    _private: [u8; 0],
}

/// Opaque handle to a polled batch of records (owns the buffers).
#[repr(C)]
pub struct kafka_consumer_ConsumerRecords_t {
    _private: [u8; 0],
}

/// Opaque handle to a single consumer record (borrows from the owning batch).
#[repr(C)]
pub struct kafka_consumer_ConsumerRecord_t {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------------
// ConsumerProperties
// ---------------------------------------------------------------------------

/// Casts a `*const kafka_consumer_ConsumerProperties_t` to a reference.
///
/// # Safety
///
/// `props` must be a valid handle from a `ConsumerProperties` constructor.
unsafe fn properties_ref(props: *const kafka_consumer_ConsumerProperties_t) -> &'static HashMap<String, String> {
    unsafe { &*(props as *const HashMap<String, String>) }
}

/// Casts a `*mut kafka_consumer_ConsumerProperties_t` to a mutable reference.
///
/// # Safety
///
/// `props` must be a valid handle from a `ConsumerProperties` constructor.
unsafe fn properties_mut(props: *mut kafka_consumer_ConsumerProperties_t) -> &'static mut HashMap<String, String> {
    unsafe { &mut *(props as *mut HashMap<String, String>) }
}

/// Creates an empty consumer properties handle.
///
/// # Returns
///
/// A non-null opaque properties handle. The caller must free it with
/// [`kafka_consumer_ConsumerProperties_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_ConsumerProperties_new() -> *mut kafka_consumer_ConsumerProperties_t {
    let map: HashMap<String, String> = HashMap::new();
    Box::into_raw(Box::new(map)) as *mut kafka_consumer_ConsumerProperties_t
}

/// Creates consumer properties from a NULL-terminated flat array of C strings.
///
/// The array contains alternating key-value pairs terminated by a NULL pointer:
/// `["key1", "val1", "key2", "val2", ..., NULL]`.
///
/// # Returns
///
/// A non-null handle on success, or NULL if `configs` is NULL or an odd number
/// of non-NULL entries is found. The caller must free a non-null handle with
/// [`kafka_consumer_ConsumerProperties_destroy`].
///
/// # Safety
///
/// `configs` must be NULL or point to a NULL-terminated array of valid,
/// null-terminated C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerProperties_from_configs(
    configs: *const *const c_char,
) -> *mut kafka_consumer_ConsumerProperties_t {
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
    Box::into_raw(Box::new(map)) as *mut kafka_consumer_ConsumerProperties_t
}

/// Adds or overwrites a configuration key-value pair. No-op if any parameter
/// is null.
///
/// # Safety
///
/// `props` must be a valid handle; `key` and `value` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerProperties_put(
    props: *mut kafka_consumer_ConsumerProperties_t,
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
/// `props` must be null or a valid handle from a `ConsumerProperties`
/// constructor. After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerProperties_destroy(props: *mut kafka_consumer_ConsumerProperties_t) {
    if !props.is_null() {
        unsafe {
            drop(Box::from_raw(props as *mut HashMap<String, String>));
        }
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

/// Creates a new Kafka consumer connected to a real cluster.
///
/// Mirrors Java's `new KafkaConsumer(Properties)`. The consumer uses the
/// byte-array deserializers for both key and value (`Vec<u8>`).
///
/// # Parameters
///
/// - `props`: Non-null properties handle. The caller retains ownership.
/// - `out_error`: Pointer where an error handle will be written on failure,
///   or null if the caller does not need error details.
///
/// # Returns
///
/// A non-null consumer handle on success, or null on failure. If `out_error`
/// is non-null, `*out_error` is set to null on success or to a valid error
/// handle on failure (free it with [`kafka_common_KafkaError_destroy`]).
///
/// # Safety
///
/// `props` must be a valid, non-null properties handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaConsumer_new(
    props: *const kafka_consumer_ConsumerProperties_t,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> *mut kafka_consumer_Consumer_t {
    init_default_logger();
    if props.is_null() {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(KafkaError::illegal_argument("properties handle must not be null")) };
        }
        return std::ptr::null_mut();
    }
    let map = unsafe { properties_ref(props) };
    let config = match crate::consumer::ConsumerConfig::from_properties(map) {
        Ok(c) => c,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    // Replicate the `GroupProtocol::of` gate from `new_consumer`
    // (`src/consumer/mod.rs`): classic protocol is unsupported in this client.
    // We construct the concrete `AsyncKafkaConsumer` directly (rather than
    // routing through `new_consumer`, which erases the concrete type) so we can
    // capture its `handle()` before boxing it as a trait object.
    match GroupProtocol::of(config.group_protocol()) {
        Ok(GroupProtocol::Consumer) => {},
        Ok(GroupProtocol::Classic) => {
            if !out_error.is_null() {
                unsafe {
                    *out_error = box_error(KafkaError::unsupported_version(
                        "Classic group protocol is not yet supported in this client; \
                         set group.protocol=consumer (KIP-848).",
                    ))
                };
            }
            return std::ptr::null_mut();
        },
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    }

    let consumer =
        match AsyncKafkaConsumer::<Bytes, Bytes>::new(config, Box::new(BytesDeserializer), Box::new(BytesDeserializer))
        {
            Ok(c) => c,
            Err(e) => {
                if !out_error.is_null() {
                    unsafe { *out_error = box_error(e) };
                }
                return std::ptr::null_mut();
            },
        };

    let consumer_handle = consumer.handle();
    if !out_error.is_null() {
        unsafe { *out_error = std::ptr::null_mut() };
    }
    build_consumer_handle(ConsumerKind::Async(Box::new(consumer)), consumer_handle, false)
}

/// Creates a new mock consumer (broker-less, for tests).
///
/// # Parameters
///
/// - `auto_offset_reset`: Null-terminated reset strategy name
///   (`"earliest"`, `"latest"`, `"none"`, or `"by_duration:<ISO-8601>"`);
///   defaults to `"latest"` if null or unparseable.
///
/// # Returns
///
/// A non-null consumer handle. The caller must free it with
/// [`kafka_consumer_Consumer_destroy`].
///
/// # Safety
///
/// `auto_offset_reset` must be null or a valid, null-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_new(
    auto_offset_reset: *const c_char,
) -> *mut kafka_consumer_Consumer_t {
    init_default_logger();
    let strategy = if auto_offset_reset.is_null() {
        AutoOffsetResetStrategy::LATEST
    } else {
        let s = unsafe { CStr::from_ptr(auto_offset_reset) }.to_string_lossy().to_string();
        AutoOffsetResetStrategy::from_string(&s).unwrap_or(AutoOffsetResetStrategy::LATEST)
    };
    let consumer: MockConsumer<Bytes, Bytes> = MockConsumer::new(strategy);
    // `MockConsumer` exposes a `ConsumerHandle` through the `Consumer` trait
    // (its `wakeup()` is backed by a shared flag observed by the next `poll`),
    // so we capture it here just like the async arm — no no-op handle is needed.
    let consumer_handle = consumer.handle();
    build_consumer_handle(ConsumerKind::Mock(Box::new(consumer)), consumer_handle, true)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Destroys a consumer handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op). Does NOT acquire the guard
/// (mirrors Java); destroying concurrently with an in-flight op is a C
/// lifetime precondition the caller must uphold (CLAUDE.md FFI §3).
///
/// # Safety
///
/// `consumer` must be null or a valid handle from a consumer constructor.
/// After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_destroy(consumer: *mut kafka_consumer_Consumer_t) {
    if consumer.is_null() {
        return;
    }
    let handle = unsafe { Box::from_raw(consumer as *mut FfiConsumerHandle) };
    let FfiConsumerHandle { consumer, runtime, completion_tx, dispatcher, .. } = *handle;

    // 1. Shut down the runtime first. This cancels any in-flight async-variant
    //    future that borrows `*consumer.get()`, so the consumer is no longer
    //    aliased when we drop it next.
    runtime.shutdown_background();
    // 2. Drop the consumer; its own `Drop` joins the internal bg task.
    drop(consumer);
    // 3. Close the completion channel and detach the dispatcher (do NOT join —
    //    outstanding completion jobs may still hold a cloned `completion_tx`,
    //    and the dispatcher exits once all clones are released).
    drop(completion_tx);
    drop(dispatcher.into_inner().unwrap_or(None));
}

/// Wakes up a consumer blocked in `poll` (or another long operation).
///
/// **Bypasses the access guard** — this is its whole purpose, matching Java's
/// `wakeup()`, which interrupts a `poll` held by another thread. Callable from
/// any thread.
///
/// # Safety
///
/// `consumer` must be a valid handle from a consumer constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_wakeup(consumer: *const kafka_consumer_Consumer_t) {
    if consumer.is_null() {
        return;
    }
    let handle = unsafe { handle_ref(consumer) };
    handle.consumer_handle.wakeup();
}

// ---------------------------------------------------------------------------
// poll
// ---------------------------------------------------------------------------

/// Polls for records (synchronous). Drives the consumer's `poll(timeout)`
/// under the access guard via `block_on`.
///
/// # Parameters
///
/// - `consumer`: Non-null consumer handle.
/// - `timeout_ms`: Poll timeout in milliseconds.
/// - `out_error`: Pointer where an error handle is written on failure, or null.
///
/// # Returns
///
/// A non-null [`kafka_consumer_ConsumerRecords_t`] handle on success (free it
/// with [`kafka_consumer_ConsumerRecords_destroy`]), or null on failure with
/// `*out_error` set.
///
/// # Safety
///
/// `consumer` must be a valid handle from a consumer constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll(
    consumer: *const kafka_consumer_Consumer_t,
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

/// Completion callback for [`kafka_consumer_Consumer_poll_async`].
///
/// On success `records` is non-null and `error` is null; on failure `records`
/// is null and `error` is non-null. The callback takes ownership of whichever
/// handle is non-null and must free it.
pub type kafka_consumer_Consumer_poll_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_ConsumerRecords_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Polls for records asynchronously (one-operation-in-flight). The access
/// guard is held from submission until the callback fires, so any concurrent
/// op (sync or async) is rejected with a `ConcurrentModification` error until
/// completion.
///
/// If the guard cannot be acquired, the callback fires inline with the error.
///
/// # Safety
///
/// `consumer` must be a valid handle from a consumer constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll_async(
    consumer: *const kafka_consumer_Consumer_t,
    timeout_ms: i64,
    callback: kafka_consumer_Consumer_poll_callback_t,
    user_data: *mut c_void,
) {
    let h = unsafe { handle_ref(consumer) };
    let target = PollCallbackTarget { callback, user_data };
    if let Err(e) = acquire(h) {
        // Rejected: deliver the error through the callback inline. The guard
        // was not taken, so nothing to release.
        unsafe { (target.callback)(std::ptr::null_mut(), box_error(e), target.user_data) };
        return;
    }
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    let tx = h.completion_tx.clone();
    // Capture the `&'static FfiConsumerHandle` (Send+Sync via the unsafe impls),
    // NOT a bare `*mut` (raw pointers are !Send and would make the future
    // !Send). The handle is leaked, so the borrow is effectively `'static`.
    let hs: &'static FfiConsumerHandle = unsafe { handle_ref(consumer) };
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

/// Owned poll completion payload, fired by the dispatcher thread. Carries the
/// raw result handles (one of `records`/`error` is non-null) and the
/// `&'static FfiConsumerHandle` so the access guard is released **after** the
/// callback fires (keeping `owner` held for the whole submit->callback window).
struct PollCompletion {
    target: PollCallbackTarget,
    records: *mut kafka_consumer_ConsumerRecords_t,
    error: *mut kafka_common_KafkaError_t,
    handle: &'static FfiConsumerHandle,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread;
// the C user is responsible for the thread-safety of `user_data`. The
// `&FfiConsumerHandle` is Send via the type's `unsafe impl Send`.
unsafe impl Send for PollCompletion {}
impl PollCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    unsafe fn fire(self) {
        // Release BEFORE firing the callback. By the time this completion job
        // runs, the awaited op has fully completed (`poll().await` returned), so
        // the consumer is no longer borrowed and the guard's job is done. The
        // guard is held for the whole submit -> op-complete window (rejecting
        // genuinely concurrent ops); releasing before the callback avoids a
        // release-vs-next-op race for embedders that resume work from the
        // callback (e.g. an async runtime scheduling the awaiting task onto
        // another thread). The callback only reads the already-built result
        // handles; it does not touch the consumer.
        release(self.handle);
        unsafe { (self.target.callback)(self.records, self.error, self.target.user_data) };
    }
}

/// A poll-callback target (function pointer + opaque `user_data`), wrapped so
/// it can cross the tokio task / dispatcher thread boundary.
#[derive(Clone, Copy)]
struct PollCallbackTarget {
    callback: kafka_consumer_Consumer_poll_callback_t,
    user_data: *mut c_void,
}
// SAFETY: the C user owns the thread-safety of `user_data`; the function
// pointer is trivially shareable.
unsafe impl Send for PollCallbackTarget {}

/// Returns a `&mut` to the consumer trait object behind the handle's
/// `UnsafeCell`. The access guard guarantees exclusivity.
///
/// # Safety
///
/// The caller must hold the access guard (`acquire` succeeded and the guard is
/// still held).
// The `&mut` from `&` is the whole point of the `UnsafeCell` + access-guard
// design: the guard enforces the exclusivity the borrow checker cannot.
#[allow(clippy::mut_from_ref)]
unsafe fn consumer_mut(h: &FfiConsumerHandle) -> &mut dyn Consumer<Bytes, Bytes> {
    match unsafe { &mut *h.consumer.get() } {
        ConsumerKind::Async(c) => c.as_mut(),
        ConsumerKind::Mock(c) => c.as_mut(),
    }
}

// ---------------------------------------------------------------------------
// ConsumerRecords + ConsumerRecord marshaling (zero-copy, see §27)
// ---------------------------------------------------------------------------

/// Owns the polled batch plus a flat index into its records (insertion order).
/// Record getters borrow from `records`; the flat pointers are valid until the
/// handle is destroyed.
struct ConsumerRecordsInner {
    /// Owns the record buffers.
    records: ConsumerRecords<Bytes, Bytes>,
    /// Index → record pointer (insertion order). Pointers borrow into
    /// `records` and stay valid because `records` is boxed and never moved.
    flat: Vec<*const ConsumerRecord<Bytes, Bytes>>,
}

/// Boxes a polled batch into an opaque records handle, building the flat index
/// in insertion order. Zero-copy: the records (and their key/value buffers)
/// are moved into the box, not cloned.
fn box_records(records: ConsumerRecords<Bytes, Bytes>) -> *mut kafka_consumer_ConsumerRecords_t {
    // Box `records` first so its address is stable, then collect borrowed
    // pointers into it.
    let mut inner = Box::new(ConsumerRecordsInner { records, flat: Vec::new() });
    let flat: Vec<*const ConsumerRecord<Bytes, Bytes>> = (&inner.records)
        .into_iter()
        .map(|r| r as *const ConsumerRecord<Bytes, Bytes>)
        .collect();
    inner.flat = flat;
    Box::into_raw(inner) as *mut kafka_consumer_ConsumerRecords_t
}

/// Casts a records handle to its inner type.
///
/// # Safety
///
/// `records` must be a valid handle from [`box_records`].
unsafe fn records_ref(records: *const kafka_consumer_ConsumerRecords_t) -> &'static ConsumerRecordsInner {
    unsafe { &*(records as *const ConsumerRecordsInner) }
}

/// Returns the number of records in the batch.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_count(records: *const kafka_consumer_ConsumerRecords_t) -> i32 {
    if records.is_null() {
        return 0;
    }
    unsafe { records_ref(records) }.flat.len() as i32
}

/// Returns whether the batch is empty.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_is_empty(
    records: *const kafka_consumer_ConsumerRecords_t,
) -> bool {
    if records.is_null() {
        return true;
    }
    unsafe { records_ref(records) }.flat.is_empty()
}

/// Returns the record at index `index` (borrowed; valid until the records
/// handle is destroyed), or null if out of range.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_get(
    records: *const kafka_consumer_ConsumerRecords_t,
    index: i32,
) -> *const kafka_consumer_ConsumerRecord_t {
    if records.is_null() || index < 0 {
        return std::ptr::null();
    }
    let inner = unsafe { records_ref(records) };
    match inner.flat.get(index as usize) {
        Some(&ptr) => ptr as *const kafka_consumer_ConsumerRecord_t,
        None => std::ptr::null(),
    }
}

/// Destroys a records handle, freeing the owned batch. Safe with null (no-op).
///
/// # Safety
///
/// `records` must be null or a valid records handle. After this call the
/// pointer (and any record pointers obtained from it) are invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_destroy(records: *mut kafka_consumer_ConsumerRecords_t) {
    if !records.is_null() {
        unsafe {
            drop(Box::from_raw(records as *mut ConsumerRecordsInner));
        }
    }
}

/// Casts a record handle to its inner type.
///
/// # Safety
///
/// `record` must be a valid record pointer obtained from
/// [`kafka_consumer_ConsumerRecords_get`].
unsafe fn record_ref(record: *const kafka_consumer_ConsumerRecord_t) -> &'static ConsumerRecord<Bytes, Bytes> {
    unsafe { &*(record as *const ConsumerRecord<Bytes, Bytes>) }
}

/// Returns the partition of a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_partition(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.partition()
}

/// Returns the offset of a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_offset(record: *const kafka_consumer_ConsumerRecord_t) -> i64 {
    unsafe { record_ref(record) }.offset()
}

/// Returns the timestamp of a record (milliseconds since epoch, or
/// `NO_TIMESTAMP` = -1).
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i64 {
    unsafe { record_ref(record) }.timestamp()
}

/// Returns the topic name of a record as a (ptr, len) pair (NOT
/// NUL-terminated). `out_len` receives the byte length. The pointer borrows
/// into the batch and is valid until the records handle is destroyed.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_topic(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const c_char {
    let rec = unsafe { record_ref(record) };
    let topic = rec.topic();
    if !out_len.is_null() {
        unsafe { *out_len = topic.len() as i32 };
    }
    topic.as_ptr() as *const c_char
}

/// Returns the key bytes of a record as a (ptr, len) pair, or (null, -1) if the
/// key is absent. The pointer borrows into the batch (zero-copy) and is valid
/// until the records handle is destroyed.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_key(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const u8 {
    let rec = unsafe { record_ref(record) };
    match rec.key() {
        Some(k) => {
            if !out_len.is_null() {
                unsafe { *out_len = k.len() as i32 };
            }
            k.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the value bytes of a record as a (ptr, len) pair, or (null, -1) if
/// the value is absent. The pointer borrows into the batch (zero-copy) and is
/// valid until the records handle is destroyed.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_value(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const u8 {
    let rec = unsafe { record_ref(record) };
    match rec.value() {
        Some(v) => {
            if !out_len.is_null() {
                unsafe { *out_len = v.len() as i32 };
            }
            v.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the timestamp type of a record as its numeric id
/// (`-1` = NoTimestampType, `0` = CreateTime, `1` = LogAppendTime).
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp_type(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.timestamp_type().id()
}

/// Returns the serialized key size in bytes, or `-1` if the key is null.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_key_size(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.serialized_key_size()
}

/// Returns the serialized value size in bytes, or `-1` if the value is null.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_value_size(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.serialized_value_size()
}

/// Returns the leader epoch of a record. Writes the epoch to `*out_epoch` and
/// returns `true` if present; returns `false` (leaving `*out_epoch` untouched)
/// if absent.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_epoch` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_leader_epoch(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_epoch: *mut i32,
) -> bool {
    match unsafe { record_ref(record) }.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                unsafe { *out_epoch = epoch };
            }
            true
        },
        None => false,
    }
}

/// Returns the delivery count of a record (KIP-932 share consumer). Writes the
/// count to `*out_count` and returns `true` if present; returns `false` if
/// absent.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_count` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_delivery_count(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_count: *mut i32,
) -> bool {
    match unsafe { record_ref(record) }.delivery_count() {
        Some(count) => {
            if !out_count.is_null() {
                unsafe { *out_count = count as i32 };
            }
            true
        },
        None => false,
    }
}

/// Returns the number of headers attached to a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_count(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.headers().into_iter().count() as i32
}

/// Returns the borrowed header at `index` (insertion order), or null if `record`
/// has no header at that index. The returned slice borrows into the record and
/// is valid until the records handle is destroyed.
fn header_at(rec: &ConsumerRecord<Bytes, Bytes>, index: i32) -> Option<&RecordHeader> {
    if index < 0 {
        return None;
    }
    rec.headers().into_iter().nth(index as usize)
}

/// Returns the key of the header at `index` as a (ptr, len) pair (NOT
/// NUL-terminated), or (null, -1) if out of range. `out_len` receives the byte
/// length. The pointer borrows into the batch.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_key(
    record: *const kafka_consumer_ConsumerRecord_t,
    index: i32,
    out_len: *mut i32,
) -> *const c_char {
    let rec = unsafe { record_ref(record) };
    match header_at(rec, index) {
        Some(header) => {
            let key = header.key();
            if !out_len.is_null() {
                unsafe { *out_len = key.len() as i32 };
            }
            key.as_ptr() as *const c_char
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the value of the header at `index` as a (ptr, len) pair, or
/// (null, -1) if out of range or the header value is null. `out_len` receives
/// the byte length. The pointer borrows into the batch (zero-copy).
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_value(
    record: *const kafka_consumer_ConsumerRecord_t,
    index: i32,
    out_len: *mut i32,
) -> *const u8 {
    let rec = unsafe { record_ref(record) };
    match header_at(rec, index).and_then(|h| h.value()) {
        Some(value) => {
            if !out_len.is_null() {
                unsafe { *out_len = value.len() as i32 };
            }
            value.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

// ---------------------------------------------------------------------------
// MockConsumer driver methods (broker-less test support)
//
// These match on `ConsumerKind::Mock` and return `illegal_state` for the
// `Async` arm (they have no production analog). They run under the access
// guard like any sync call.
// ---------------------------------------------------------------------------

/// Returns the [`MockConsumer`] behind the guard, or an error if this handle
/// wraps an async consumer.
///
/// # Safety
///
/// The caller must hold the access guard.
// The `&mut` from `&` is the whole point of the `UnsafeCell` + access-guard
// design: the guard enforces the exclusivity the borrow checker cannot.
#[allow(clippy::mut_from_ref)]
unsafe fn mock_mut(h: &FfiConsumerHandle) -> Result<&mut MockConsumer<Bytes, Bytes>, KafkaError> {
    match unsafe { &mut *h.consumer.get() } {
        ConsumerKind::Mock(c) => Ok(c.as_mut()),
        ConsumerKind::Async(_) => Err(KafkaError::illegal_state("operation is only supported on a MockConsumer")),
    }
}

/// Assigns the consumer to a set of `(topic, partition)` pairs (sync).
///
/// `topics` and `partitions` are parallel arrays of length `count`. Works on
/// both the async and mock consumers.
///
/// Returns null on success, or a non-null error handle on failure.
///
/// # Safety
///
/// `topics` must point to `count` valid C strings; `partitions` to `count`
/// `i32` values. `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let n = count.max(0) as usize;
    let mut tps = Vec::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        let partition = unsafe { *partitions.add(i) };
        tps.push(crate::common::TopicPartition::new(topic, partition));
    }
    match h.runtime.block_on(unsafe { consumer_mut(h).assign(tps) }) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
}

/// Adds a record to a mock consumer's pending queue (mock only).
///
/// The record's partition must already be assigned. `key`/`value` are
/// (ptr, len) pairs; pass `len < 0` for an absent key/value.
///
/// Returns null on success, or a non-null error handle on failure (including
/// `illegal_state` if `consumer` wraps an async consumer).
///
/// # Safety
///
/// `topic` must be a valid C string; `key`/`value` valid for `key_len`/
/// `value_len` bytes (or null if the length is negative).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_add_record(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let key_vec: Option<Bytes> = if key_len < 0 || key.is_null() {
        None
    } else {
        Some(Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(key, key_len as usize)
        }))
    };
    let value_vec: Option<Bytes> = if value_len < 0 || value.is_null() {
        None
    } else {
        Some(Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(value, value_len as usize)
        }))
    };
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    let record = ConsumerRecord::new(topic_str, partition, offset, key_vec, value_vec);
    match mock.add_record(record) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
}

/// Updates the end offsets of a mock consumer for a single `(topic, partition)`
/// (mock only). Used for `seekToEnd` / `LATEST` resets and `end_offsets`.
///
/// Returns null on success, or a non-null error handle (incl. `illegal_state`
/// for an async consumer).
///
/// # Safety
///
/// `topic` must be a valid C string; `consumer` a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_end_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    let mut map = HashMap::new();
    map.insert(crate::common::TopicPartition::new(topic_str, partition), offset);
    mock.update_end_offsets(map);
    std::ptr::null_mut()
}

/// Updates the beginning offsets of a mock consumer for a single
/// `(topic, partition)` (mock only). Used for `seekToBeginning` / `EARLIEST`
/// resets and `beginning_offsets`.
///
/// Returns null on success, or a non-null error handle (incl. `illegal_state`
/// for an async consumer).
///
/// # Safety
///
/// `topic` must be a valid C string; `consumer` a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_beginning_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    let mut map = HashMap::new();
    map.insert(crate::common::TopicPartition::new(topic_str, partition), offset);
    mock.update_beginning_offsets(map);
    std::ptr::null_mut()
}

/// Registers partition metadata for a topic on a mock consumer (mock only),
/// used to drive `partitions_for` / `list_topics`. Each of the `count`
/// partitions is given a single leader node `(leader_id, leader_host,
/// leader_port)` that also serves as its sole replica and in-sync replica.
///
/// Returns null on success, or a non-null error handle (incl. `illegal_state`
/// for an async consumer).
///
/// # Safety
///
/// `topic` / `leader_host` must be valid C strings; `consumer` a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_partitions(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition_count: i32,
    leader_id: i32,
    leader_host: *const c_char,
    leader_port: i32,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let host = unsafe { CStr::from_ptr(leader_host) }.to_string_lossy().to_string();
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    let infos: Vec<PartitionInfo> = (0..partition_count.max(0))
        .map(|p| {
            let leader = Node::new(leader_id, host.clone(), leader_port);
            PartitionInfo::new(topic_str.clone(), p, Some(leader.clone()), vec![leader.clone()], vec![leader])
        })
        .collect();
    match mock.update_partitions(&topic_str, infos) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
}

/// Injects an `illegal_state` error to be returned by the next `poll` on a mock
/// consumer (mock only), with the given message. Mirrors Java's
/// `setPollException`.
///
/// Returns null on success, or a non-null error handle (incl. `illegal_state`
/// for an async consumer).
///
/// # Safety
///
/// `message` must be a valid C string; `consumer` a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_poll_error(
    consumer: *const kafka_consumer_Consumer_t,
    message: *const c_char,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let msg = unsafe { CStr::from_ptr(message) }.to_string_lossy().to_string();
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    mock.set_poll_exception(KafkaError::illegal_state(msg));
    std::ptr::null_mut()
}

// ---------------------------------------------------------------------------
// Phase E — input marshaling helpers (C arrays -> Rust)
// ---------------------------------------------------------------------------

/// Reads `count` `(topic, partition)` pairs from parallel C arrays into a
/// `Vec<TopicPartition>`.
///
/// # Safety
///
/// `topics` must point to `count` valid C strings; `partitions` to `count`
/// `i32` values. A non-positive `count` yields an empty vec.
unsafe fn read_topic_partitions(
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> Vec<TopicPartition> {
    let n = count.max(0) as usize;
    let mut tps = Vec::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        let partition = unsafe { *partitions.add(i) };
        tps.push(TopicPartition::new(topic, partition));
    }
    tps
}

/// Reads `count` topic names from a C array into a `Vec<String>`.
///
/// # Safety
///
/// `topics` must point to `count` valid C strings.
unsafe fn read_topics(topics: *const *const c_char, count: i32) -> Vec<String> {
    let n = count.max(0) as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        out.push(unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string());
    }
    out
}

// ---------------------------------------------------------------------------
// Phase E — single-value opaque handles
//
// Each handle owns a Rust value (boxed) and exposes getters. Non-hot-path
// string getters return cached, NUL-terminated `CString`s owned by the handle.
// ---------------------------------------------------------------------------

/// Opaque handle to a [`TopicPartition`].
#[repr(C)]
pub struct kafka_consumer_TopicPartition_t {
    _private: [u8; 0],
}

/// Cached topic-partition handle: owns the value and a NUL-terminated topic
/// `CString` for the string getter.
struct TopicPartitionInner {
    tp: TopicPartition,
    topic_c: std::ffi::CString,
}

/// Returns the topic of a topic-partition handle as a NUL-terminated C string
/// (owned by the handle; valid until it is destroyed).
///
/// # Safety
///
/// `tp` must be a valid topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartition_topic(
    tp: *const kafka_consumer_TopicPartition_t,
) -> *const c_char {
    let inner = unsafe { &*(tp as *const TopicPartitionInner) };
    inner.topic_c.as_ptr()
}

/// Returns the partition of a topic-partition handle.
///
/// # Safety
///
/// `tp` must be a valid topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartition_partition(tp: *const kafka_consumer_TopicPartition_t) -> i32 {
    let inner = unsafe { &*(tp as *const TopicPartitionInner) };
    inner.tp.partition()
}

/// Destroys a topic-partition handle. Safe with null (no-op).
///
/// # Safety
///
/// `tp` must be null or a valid topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartition_destroy(tp: *mut kafka_consumer_TopicPartition_t) {
    if !tp.is_null() {
        unsafe { drop(Box::from_raw(tp as *mut TopicPartitionInner)) };
    }
}

/// Opaque handle to an [`OffsetAndMetadata`].
#[repr(C)]
pub struct kafka_consumer_OffsetAndMetadata_t {
    _private: [u8; 0],
}

/// Cached offset-and-metadata handle: owns the value and a NUL-terminated
/// metadata `CString`.
struct OffsetAndMetadataInner {
    oam: OffsetAndMetadata,
    metadata_c: std::ffi::CString,
}

/// Returns the committed offset.
///
/// # Safety
///
/// `oam` must be a valid offset-and-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_offset(
    oam: *const kafka_consumer_OffsetAndMetadata_t,
) -> i64 {
    let inner = unsafe { &*(oam as *const OffsetAndMetadataInner) };
    inner.oam.offset()
}

/// Returns the commit metadata as a NUL-terminated C string (empty string if
/// none; owned by the handle).
///
/// # Safety
///
/// `oam` must be a valid offset-and-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_metadata(
    oam: *const kafka_consumer_OffsetAndMetadata_t,
) -> *const c_char {
    let inner = unsafe { &*(oam as *const OffsetAndMetadataInner) };
    inner.metadata_c.as_ptr()
}

/// Returns the leader epoch via `*out_epoch`, or `false` if absent.
///
/// # Safety
///
/// `oam` must be a valid handle; `out_epoch` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_leader_epoch(
    oam: *const kafka_consumer_OffsetAndMetadata_t,
    out_epoch: *mut i32,
) -> bool {
    let inner = unsafe { &*(oam as *const OffsetAndMetadataInner) };
    match inner.oam.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                unsafe { *out_epoch = epoch };
            }
            true
        },
        None => false,
    }
}

/// Destroys an offset-and-metadata handle. Safe with null (no-op).
///
/// # Safety
///
/// `oam` must be null or a valid offset-and-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_destroy(oam: *mut kafka_consumer_OffsetAndMetadata_t) {
    if !oam.is_null() {
        unsafe { drop(Box::from_raw(oam as *mut OffsetAndMetadataInner)) };
    }
}

/// Opaque handle to an [`OffsetAndTimestamp`].
#[repr(C)]
pub struct kafka_consumer_OffsetAndTimestamp_t {
    _private: [u8; 0],
}

/// Returns the offset.
///
/// # Safety
///
/// `oat` must be a valid offset-and-timestamp handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_offset(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    unsafe { &*(oat as *const OffsetAndTimestamp) }.offset()
}

/// Returns the timestamp (milliseconds since epoch).
///
/// # Safety
///
/// `oat` must be a valid offset-and-timestamp handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_timestamp(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    unsafe { &*(oat as *const OffsetAndTimestamp) }.timestamp()
}

/// Returns the leader epoch via `*out_epoch`, or `false` if absent.
///
/// # Safety
///
/// `oat` must be a valid handle; `out_epoch` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_leader_epoch(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
    out_epoch: *mut i32,
) -> bool {
    match unsafe { &*(oat as *const OffsetAndTimestamp) }.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                unsafe { *out_epoch = epoch };
            }
            true
        },
        None => false,
    }
}

/// Destroys an offset-and-timestamp handle. Safe with null (no-op).
///
/// # Safety
///
/// `oat` must be null or a valid offset-and-timestamp handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_destroy(oat: *mut kafka_consumer_OffsetAndTimestamp_t) {
    if !oat.is_null() {
        unsafe { drop(Box::from_raw(oat as *mut OffsetAndTimestamp)) };
    }
}

/// Opaque handle to a [`ConsumerGroupMetadata`].
#[repr(C)]
pub struct kafka_consumer_ConsumerGroupMetadata_t {
    _private: [u8; 0],
}

/// Cached group-metadata handle: owns the value plus NUL-terminated string
/// getters.
struct ConsumerGroupMetadataInner {
    meta: ConsumerGroupMetadata,
    group_id_c: std::ffi::CString,
    member_id_c: std::ffi::CString,
    group_instance_id_c: Option<std::ffi::CString>,
}

fn box_group_metadata(meta: ConsumerGroupMetadata) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
    let group_id_c = std::ffi::CString::new(meta.group_id().as_bytes()).unwrap_or_default();
    let member_id_c = std::ffi::CString::new(meta.member_id().as_bytes()).unwrap_or_default();
    let group_instance_id_c = meta
        .group_instance_id()
        .map(|s| std::ffi::CString::new(s.as_bytes()).unwrap_or_default());
    Box::into_raw(Box::new(ConsumerGroupMetadataInner {
        meta,
        group_id_c,
        member_id_c,
        group_instance_id_c,
    })) as *mut kafka_consumer_ConsumerGroupMetadata_t
}

/// Returns the group id as a NUL-terminated C string (owned by the handle).
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.group_id_c.as_ptr()
}

/// Returns the generation id.
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_generation_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> i32 {
    unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.meta.generation_id()
}

/// Returns the member id as a NUL-terminated C string (owned by the handle).
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_member_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.member_id_c.as_ptr()
}

/// Returns the group instance id as a NUL-terminated C string, or null if
/// absent (owned by the handle).
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_instance_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    match &unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.group_instance_id_c {
        Some(c) => c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Destroys a group-metadata handle. Safe with null (no-op).
///
/// # Safety
///
/// `meta` must be null or a valid group-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_destroy(
    meta: *mut kafka_consumer_ConsumerGroupMetadata_t,
) {
    if !meta.is_null() {
        unsafe { drop(Box::from_raw(meta as *mut ConsumerGroupMetadataInner)) };
    }
}

/// Opaque handle to a [`Node`] (broker).
#[repr(C)]
pub struct kafka_common_Node_t {
    _private: [u8; 0],
}

/// Returns the node id.
///
/// # Safety
///
/// `node` must be a valid node handle obtained from a `PartitionInfo` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_id(node: *const kafka_common_Node_t) -> i32 {
    unsafe { &*(node as *const Node) }.id()
}

/// Returns the node host as a (ptr, len) pair (NOT NUL-terminated; borrows into
/// the owning `PartitionInfo`). `out_len` receives the byte length.
///
/// # Safety
///
/// `node` must be a valid node handle; `out_len` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_host(node: *const kafka_common_Node_t, out_len: *mut i32) -> *const c_char {
    let host = unsafe { &*(node as *const Node) }.host();
    if !out_len.is_null() {
        unsafe { *out_len = host.len() as i32 };
    }
    host.as_ptr() as *const c_char
}

/// Returns the node port.
///
/// # Safety
///
/// `node` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_port(node: *const kafka_common_Node_t) -> i32 {
    unsafe { &*(node as *const Node) }.port()
}

/// Returns the node rack as a (ptr, len) pair, or (null, -1) if no rack.
///
/// # Safety
///
/// `node` must be a valid node handle; `out_len` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_rack(node: *const kafka_common_Node_t, out_len: *mut i32) -> *const c_char {
    match unsafe { &*(node as *const Node) }.rack() {
        Some(rack) => {
            if !out_len.is_null() {
                unsafe { *out_len = rack.len() as i32 };
            }
            rack.as_ptr() as *const c_char
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Opaque handle to a [`PartitionInfo`].
#[repr(C)]
pub struct kafka_consumer_PartitionInfo_t {
    _private: [u8; 0],
}

/// Cached partition-info handle: owns the value plus a NUL-terminated topic
/// `CString`. `Node` getters borrow directly into the owned `PartitionInfo`.
struct PartitionInfoInner {
    info: PartitionInfo,
    topic_c: std::ffi::CString,
}

/// Returns the topic as a NUL-terminated C string (owned by the handle).
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_topic(
    info: *const kafka_consumer_PartitionInfo_t,
) -> *const c_char {
    unsafe { &*(info as *const PartitionInfoInner) }.topic_c.as_ptr()
}

/// Returns the partition number.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_partition(info: *const kafka_consumer_PartitionInfo_t) -> i32 {
    unsafe { &*(info as *const PartitionInfoInner) }.info.partition()
}

/// Returns the leader node (borrowed; valid until the handle is destroyed), or
/// null if the partition has no leader.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_leader(
    info: *const kafka_consumer_PartitionInfo_t,
) -> *const kafka_common_Node_t {
    match unsafe { &*(info as *const PartitionInfoInner) }.info.leader() {
        Some(node) => node as *const Node as *const kafka_common_Node_t,
        None => std::ptr::null(),
    }
}

/// Returns the number of replica nodes.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_replica_count(
    info: *const kafka_consumer_PartitionInfo_t,
) -> i32 {
    unsafe { &*(info as *const PartitionInfoInner) }.info.replicas().len() as i32
}

/// Returns the replica node at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_replica(
    info: *const kafka_consumer_PartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    node_at(unsafe { &*(info as *const PartitionInfoInner) }.info.replicas(), index)
}

/// Returns the number of in-sync replica nodes.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_in_sync_replica_count(
    info: *const kafka_consumer_PartitionInfo_t,
) -> i32 {
    unsafe { &*(info as *const PartitionInfoInner) }.info.in_sync_replicas().len() as i32
}

/// Returns the in-sync replica node at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_in_sync_replica(
    info: *const kafka_consumer_PartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    node_at(unsafe { &*(info as *const PartitionInfoInner) }.info.in_sync_replicas(), index)
}

/// Returns the number of offline replica nodes.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_offline_replica_count(
    info: *const kafka_consumer_PartitionInfo_t,
) -> i32 {
    unsafe { &*(info as *const PartitionInfoInner) }.info.offline_replicas().len() as i32
}

/// Returns the offline replica node at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_offline_replica(
    info: *const kafka_consumer_PartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    node_at(unsafe { &*(info as *const PartitionInfoInner) }.info.offline_replicas(), index)
}

/// Returns the node at `index` in `nodes` (borrowed), or null if out of range.
fn node_at(nodes: &[Node], index: i32) -> *const kafka_common_Node_t {
    if index < 0 {
        return std::ptr::null();
    }
    match nodes.get(index as usize) {
        Some(node) => node as *const Node as *const kafka_common_Node_t,
        None => std::ptr::null(),
    }
}

/// Destroys a partition-info handle. Safe with null (no-op). Invalidates any
/// `Node` handles obtained from it.
///
/// # Safety
///
/// `info` must be null or a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfo_destroy(info: *mut kafka_consumer_PartitionInfo_t) {
    if !info.is_null() {
        unsafe { drop(Box::from_raw(info as *mut PartitionInfoInner)) };
    }
}

// ---------------------------------------------------------------------------
// Phase E — map / list result handles
//
// Each handle owns its entries in stable (boxed) allocations, plus a parallel
// `Vec` of the key topic-partition handles for stable indexing. Getters return
// borrowed sub-handles valid until the map/list is destroyed.
// ---------------------------------------------------------------------------

/// Opaque handle to a `Map<TopicPartition, OffsetAndMetadata>` result
/// (`committed`).
#[repr(C)]
pub struct kafka_consumer_OffsetMap_t {
    _private: [u8; 0],
}

/// Owns parallel key (`TopicPartition`) and value (`OffsetAndMetadata`) handle
/// vecs for stable indexed access.
struct OffsetMapInner {
    keys: Vec<TopicPartitionInner>,
    values: Vec<OffsetAndMetadataInner>,
}

fn box_offset_map(map: HashMap<TopicPartition, OffsetAndMetadata>) -> *mut kafka_consumer_OffsetMap_t {
    let mut keys = Vec::with_capacity(map.len());
    let mut values = Vec::with_capacity(map.len());
    for (tp, oam) in map {
        let topic_c = std::ffi::CString::new(tp.topic().as_bytes()).unwrap_or_default();
        keys.push(TopicPartitionInner { tp, topic_c });
        let metadata_c = std::ffi::CString::new(oam.metadata().as_bytes()).unwrap_or_default();
        values.push(OffsetAndMetadataInner { oam, metadata_c });
    }
    Box::into_raw(Box::new(OffsetMapInner { keys, values })) as *mut kafka_consumer_OffsetMap_t
}

/// Returns the number of entries.
///
/// # Safety
///
/// `map` must be a valid offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_count(map: *const kafka_consumer_OffsetMap_t) -> i32 {
    unsafe { &*(map as *const OffsetMapInner) }.keys.len() as i32
}

/// Returns the key (topic-partition) at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_get_key(
    map: *const kafka_consumer_OffsetMap_t,
    index: i32,
) -> *const kafka_consumer_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(map as *const OffsetMapInner) }.keys.get(index as usize) {
        Some(k) => k as *const TopicPartitionInner as *const kafka_consumer_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the value (offset-and-metadata) at `index` (borrowed), or null if out
/// of range.
///
/// # Safety
///
/// `map` must be a valid offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_get_value(
    map: *const kafka_consumer_OffsetMap_t,
    index: i32,
) -> *const kafka_consumer_OffsetAndMetadata_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(map as *const OffsetMapInner) }.values.get(index as usize) {
        Some(v) => v as *const OffsetAndMetadataInner as *const kafka_consumer_OffsetAndMetadata_t,
        None => std::ptr::null(),
    }
}

/// Destroys an offset-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_destroy(map: *mut kafka_consumer_OffsetMap_t) {
    if !map.is_null() {
        unsafe { drop(Box::from_raw(map as *mut OffsetMapInner)) };
    }
}

/// Opaque handle to a `Map<TopicPartition, OffsetAndTimestamp>` result
/// (`offsets_for_times`).
#[repr(C)]
pub struct kafka_consumer_OffsetAndTimestampMap_t {
    _private: [u8; 0],
}

struct OffsetAndTimestampMapInner {
    keys: Vec<TopicPartitionInner>,
    values: Vec<OffsetAndTimestamp>,
}

fn box_offset_and_timestamp_map(
    map: HashMap<TopicPartition, OffsetAndTimestamp>,
) -> *mut kafka_consumer_OffsetAndTimestampMap_t {
    let mut keys = Vec::with_capacity(map.len());
    let mut values = Vec::with_capacity(map.len());
    for (tp, oat) in map {
        let topic_c = std::ffi::CString::new(tp.topic().as_bytes()).unwrap_or_default();
        keys.push(TopicPartitionInner { tp, topic_c });
        values.push(oat);
    }
    Box::into_raw(Box::new(OffsetAndTimestampMapInner { keys, values })) as *mut kafka_consumer_OffsetAndTimestampMap_t
}

/// Returns the number of entries.
///
/// # Safety
///
/// `map` must be a valid offset-and-timestamp-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_count(
    map: *const kafka_consumer_OffsetAndTimestampMap_t,
) -> i32 {
    unsafe { &*(map as *const OffsetAndTimestampMapInner) }.keys.len() as i32
}

/// Returns the key (topic-partition) at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid offset-and-timestamp-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_get_key(
    map: *const kafka_consumer_OffsetAndTimestampMap_t,
    index: i32,
) -> *const kafka_consumer_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(map as *const OffsetAndTimestampMapInner) }.keys.get(index as usize) {
        Some(k) => k as *const TopicPartitionInner as *const kafka_consumer_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the value (offset-and-timestamp) at `index` (borrowed), or null if
/// out of range.
///
/// # Safety
///
/// `map` must be a valid offset-and-timestamp-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_get_value(
    map: *const kafka_consumer_OffsetAndTimestampMap_t,
    index: i32,
) -> *const kafka_consumer_OffsetAndTimestamp_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(map as *const OffsetAndTimestampMapInner) }
        .values
        .get(index as usize)
    {
        Some(v) => v as *const OffsetAndTimestamp as *const kafka_consumer_OffsetAndTimestamp_t,
        None => std::ptr::null(),
    }
}

/// Destroys an offset-and-timestamp-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid offset-and-timestamp-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_destroy(
    map: *mut kafka_consumer_OffsetAndTimestampMap_t,
) {
    if !map.is_null() {
        unsafe { drop(Box::from_raw(map as *mut OffsetAndTimestampMapInner)) };
    }
}

/// Opaque handle to a `Map<TopicPartition, Long>` result
/// (`beginning_offsets` / `end_offsets`).
#[repr(C)]
pub struct kafka_consumer_LongOffsetMap_t {
    _private: [u8; 0],
}

struct LongOffsetMapInner {
    keys: Vec<TopicPartitionInner>,
    values: Vec<i64>,
}

fn box_long_offset_map(map: HashMap<TopicPartition, i64>) -> *mut kafka_consumer_LongOffsetMap_t {
    let mut keys = Vec::with_capacity(map.len());
    let mut values = Vec::with_capacity(map.len());
    for (tp, offset) in map {
        let topic_c = std::ffi::CString::new(tp.topic().as_bytes()).unwrap_or_default();
        keys.push(TopicPartitionInner { tp, topic_c });
        values.push(offset);
    }
    Box::into_raw(Box::new(LongOffsetMapInner { keys, values })) as *mut kafka_consumer_LongOffsetMap_t
}

/// Returns the number of entries.
///
/// # Safety
///
/// `map` must be a valid long-offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_count(map: *const kafka_consumer_LongOffsetMap_t) -> i32 {
    unsafe { &*(map as *const LongOffsetMapInner) }.keys.len() as i32
}

/// Returns the key (topic-partition) at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid long-offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_get_key(
    map: *const kafka_consumer_LongOffsetMap_t,
    index: i32,
) -> *const kafka_consumer_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(map as *const LongOffsetMapInner) }.keys.get(index as usize) {
        Some(k) => k as *const TopicPartitionInner as *const kafka_consumer_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the offset value at `index`, or `-1` if out of range.
///
/// # Safety
///
/// `map` must be a valid long-offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_get_value(
    map: *const kafka_consumer_LongOffsetMap_t,
    index: i32,
) -> i64 {
    if index < 0 {
        return -1;
    }
    match unsafe { &*(map as *const LongOffsetMapInner) }.values.get(index as usize) {
        Some(&offset) => offset,
        None => -1,
    }
}

/// Destroys a long-offset-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid long-offset-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_destroy(map: *mut kafka_consumer_LongOffsetMap_t) {
    if !map.is_null() {
        unsafe { drop(Box::from_raw(map as *mut LongOffsetMapInner)) };
    }
}

/// Opaque handle to a `List<PartitionInfo>` result (`partitions_for`).
#[repr(C)]
pub struct kafka_consumer_PartitionInfoList_t {
    _private: [u8; 0],
}

struct PartitionInfoListInner {
    items: Vec<PartitionInfoInner>,
}

pub(crate) fn box_partition_info_list(infos: Vec<PartitionInfo>) -> *mut kafka_consumer_PartitionInfoList_t {
    let items = infos
        .into_iter()
        .map(|info| {
            let topic_c = std::ffi::CString::new(info.topic().as_bytes()).unwrap_or_default();
            PartitionInfoInner { info, topic_c }
        })
        .collect();
    Box::into_raw(Box::new(PartitionInfoListInner { items })) as *mut kafka_consumer_PartitionInfoList_t
}

/// Returns the number of partition-info entries.
///
/// # Safety
///
/// `list` must be a valid partition-info-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfoList_count(
    list: *const kafka_consumer_PartitionInfoList_t,
) -> i32 {
    unsafe { &*(list as *const PartitionInfoListInner) }.items.len() as i32
}

/// Returns the partition-info at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `list` must be a valid partition-info-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfoList_get(
    list: *const kafka_consumer_PartitionInfoList_t,
    index: i32,
) -> *const kafka_consumer_PartitionInfo_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(list as *const PartitionInfoListInner) }.items.get(index as usize) {
        Some(i) => i as *const PartitionInfoInner as *const kafka_consumer_PartitionInfo_t,
        None => std::ptr::null(),
    }
}

/// Destroys a partition-info-list handle. Safe with null (no-op).
///
/// # Safety
///
/// `list` must be null or a valid partition-info-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_PartitionInfoList_destroy(list: *mut kafka_consumer_PartitionInfoList_t) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list as *mut PartitionInfoListInner)) };
    }
}

/// Opaque handle to a `Map<String, List<PartitionInfo>>` result
/// (`list_topics`).
#[repr(C)]
pub struct kafka_consumer_TopicPartitionInfoMap_t {
    _private: [u8; 0],
}

struct TopicPartitionInfoMapInner {
    topics: Vec<std::ffi::CString>,
    lists: Vec<PartitionInfoListInner>,
}

fn box_topic_partition_info_map(
    map: HashMap<String, Vec<PartitionInfo>>,
) -> *mut kafka_consumer_TopicPartitionInfoMap_t {
    let mut topics = Vec::with_capacity(map.len());
    let mut lists = Vec::with_capacity(map.len());
    for (topic, infos) in map {
        topics.push(std::ffi::CString::new(topic.as_bytes()).unwrap_or_default());
        let items = infos
            .into_iter()
            .map(|info| {
                let topic_c = std::ffi::CString::new(info.topic().as_bytes()).unwrap_or_default();
                PartitionInfoInner { info, topic_c }
            })
            .collect();
        lists.push(PartitionInfoListInner { items });
    }
    Box::into_raw(Box::new(TopicPartitionInfoMapInner { topics, lists })) as *mut kafka_consumer_TopicPartitionInfoMap_t
}

/// Returns the number of topics.
///
/// # Safety
///
/// `map` must be a valid topic-partition-info-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartitionInfoMap_count(
    map: *const kafka_consumer_TopicPartitionInfoMap_t,
) -> i32 {
    unsafe { &*(map as *const TopicPartitionInfoMapInner) }.topics.len() as i32
}

/// Returns the topic name at `index` as a NUL-terminated C string (owned by the
/// handle), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid topic-partition-info-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartitionInfoMap_get_topic(
    map: *const kafka_consumer_TopicPartitionInfoMap_t,
    index: i32,
) -> *const c_char {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(map as *const TopicPartitionInfoMapInner) }
        .topics
        .get(index as usize)
    {
        Some(t) => t.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the partition-info list for the topic at `index` (borrowed), or null
/// if out of range.
///
/// # Safety
///
/// `map` must be a valid topic-partition-info-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartitionInfoMap_get_partitions(
    map: *const kafka_consumer_TopicPartitionInfoMap_t,
    index: i32,
) -> *const kafka_consumer_PartitionInfoList_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(map as *const TopicPartitionInfoMapInner) }
        .lists
        .get(index as usize)
    {
        Some(l) => l as *const PartitionInfoListInner as *const kafka_consumer_PartitionInfoList_t,
        None => std::ptr::null(),
    }
}

/// Destroys a topic-partition-info-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid topic-partition-info-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartitionInfoMap_destroy(
    map: *mut kafka_consumer_TopicPartitionInfoMap_t,
) {
    if !map.is_null() {
        unsafe { drop(Box::from_raw(map as *mut TopicPartitionInfoMapInner)) };
    }
}

/// Opaque handle to a `Set<TopicPartition>` result (`assignment` / `paused`).
#[repr(C)]
pub struct kafka_consumer_TopicPartitionList_t {
    _private: [u8; 0],
}

struct TopicPartitionListInner {
    items: Vec<TopicPartitionInner>,
}

fn box_topic_partition_list(tps: impl IntoIterator<Item = TopicPartition>) -> *mut kafka_consumer_TopicPartitionList_t {
    let items = tps
        .into_iter()
        .map(|tp| {
            let topic_c = std::ffi::CString::new(tp.topic().as_bytes()).unwrap_or_default();
            TopicPartitionInner { tp, topic_c }
        })
        .collect();
    Box::into_raw(Box::new(TopicPartitionListInner { items })) as *mut kafka_consumer_TopicPartitionList_t
}

/// Returns the number of topic-partitions.
///
/// # Safety
///
/// `list` must be a valid topic-partition-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartitionList_count(
    list: *const kafka_consumer_TopicPartitionList_t,
) -> i32 {
    unsafe { &*(list as *const TopicPartitionListInner) }.items.len() as i32
}

/// Returns the topic-partition at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `list` must be a valid topic-partition-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartitionList_get(
    list: *const kafka_consumer_TopicPartitionList_t,
    index: i32,
) -> *const kafka_consumer_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(list as *const TopicPartitionListInner) }.items.get(index as usize) {
        Some(i) => i as *const TopicPartitionInner as *const kafka_consumer_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Destroys a topic-partition-list handle. Safe with null (no-op).
///
/// # Safety
///
/// `list` must be null or a valid topic-partition-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_TopicPartitionList_destroy(list: *mut kafka_consumer_TopicPartitionList_t) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list as *mut TopicPartitionListInner)) };
    }
}

/// Opaque handle to a `Set<String>` result (`subscription`).
#[repr(C)]
pub struct kafka_consumer_StringList_t {
    _private: [u8; 0],
}

struct StringListInner {
    items: Vec<std::ffi::CString>,
}

fn box_string_list(strings: impl IntoIterator<Item = String>) -> *mut kafka_consumer_StringList_t {
    let items = strings
        .into_iter()
        .map(|s| std::ffi::CString::new(s.as_bytes()).unwrap_or_default())
        .collect();
    Box::into_raw(Box::new(StringListInner { items })) as *mut kafka_consumer_StringList_t
}

/// Returns the number of strings.
///
/// # Safety
///
/// `list` must be a valid string-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_count(list: *const kafka_consumer_StringList_t) -> i32 {
    unsafe { &*(list as *const StringListInner) }.items.len() as i32
}

/// Returns the string at `index` as a NUL-terminated C string (owned by the
/// handle), or null if out of range.
///
/// # Safety
///
/// `list` must be a valid string-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_get(
    list: *const kafka_consumer_StringList_t,
    index: i32,
) -> *const c_char {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(list as *const StringListInner) }.items.get(index as usize) {
        Some(s) => s.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Destroys a string-list handle. Safe with null (no-op).
///
/// # Safety
///
/// `list` must be null or a valid string-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_destroy(list: *mut kafka_consumer_StringList_t) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list as *mut StringListInner)) };
    }
}

/// Frees a NUL-terminated C string returned by a consumer getter that allocates
/// an owned string (e.g. [`kafka_consumer_Consumer_client_id`]). Safe with null.
///
/// # Safety
///
/// `s` must be null or a string returned by such a getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_string_destroy(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(std::ffi::CString::from_raw(s)) };
    }
}

// ---------------------------------------------------------------------------
// Phase D — method groups
//
// Each blocking-in-Java method has a sync variant (`block_on` under the guard)
// and an async variant (one-operation-in-flight: the guard is held from
// submission until the completion callback fires).
// ---------------------------------------------------------------------------

/// Runs a void-returning consumer op synchronously under the access guard.
/// Returns null on success, or a non-null error handle on failure (including a
/// `ConcurrentModification` error if the guard cannot be acquired).
///
/// `op` receives `&mut dyn Consumer` and returns the future to drive.
///
/// # Safety
///
/// `consumer` must be a valid handle.
unsafe fn sync_void_op<F>(consumer: *const kafka_consumer_Consumer_t, op: F) -> *mut kafka_common_KafkaError_t
where
    // A higher-ranked bound ties the returned future's lifetime to the borrow
    // of the consumer, so the future may borrow `&mut self` for its duration
    // (which `FnOnce(&mut _) -> impl Future` cannot express with one type
    // parameter).
    F: for<'a> FnOnce(
        &'a mut dyn Consumer<Bytes, Bytes>,
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

/// Owned arguments captured at async-submit time, moved into the spawned task.
/// `op` must capture only `Send` data (already-marshaled owned values), never
/// raw C pointers.
///
/// # Safety
///
/// `consumer` must be a valid handle. The closure runs on the runtime; it
/// receives the guarded `&mut dyn Consumer`.
unsafe fn async_void_op<F, Fut>(
    consumer: *const kafka_consumer_Consumer_t,
    callback: OperationCallbackFn,
    user_data: *mut c_void,
    op: F,
) where
    F: FnOnce(&'static mut dyn Consumer<Bytes, Bytes>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), KafkaError>> + Send,
{
    let h = unsafe { handle_ref(consumer) };
    let target = OperationCallbackTarget { callback, user_data };
    if let Err(e) = acquire(h) {
        unsafe { (target.callback)(box_error(e), target.user_data) };
        return;
    }
    let tx = h.completion_tx.clone();
    let hs: &'static FfiConsumerHandle = unsafe { handle_ref(consumer) };
    h.runtime_handle.spawn(async move {
        let target = target;
        // SAFETY: the guard is held for the whole submit->callback window.
        let consumer = unsafe { consumer_mut(hs) };
        let result = op(consumer).await;
        let error = match result {
            Ok(()) => std::ptr::null_mut(),
            Err(e) => box_error(e),
        };
        let op = OperationCompletion { callback: target.callback, user_data: target.user_data, error };
        let release_handle = hs;
        let job: CompletionJob = Box::new(move || {
            // Release BEFORE firing the callback: the awaited op is complete, so
            // the consumer is no longer borrowed. This avoids a release-vs-next-op
            // race when the callback resumes embedder work on another thread.
            release(release_handle);
            unsafe { op.fire() };
        });
        enqueue_or_run_inline(&tx, job);
    });
}

/// A raw `user_data` pointer wrapped so it can cross into the spawned task and
/// completion job. The C user owns its thread-safety (CLAUDE.md FFI §3).
struct SendUserData(*mut c_void);
// SAFETY: the C user is responsible for the thread-safety of `user_data`.
unsafe impl Send for SendUserData {}
impl SendUserData {
    /// Consume the wrapper, returning the raw pointer. Taking `self` by value
    /// forces a completion closure that calls this to capture the whole
    /// `SendUserData` (which is `Send`) rather than disjointly capturing the
    /// inner `*mut c_void` field (which is not) — see Rust 2021 closure capture.
    fn into_ptr(self) -> *mut c_void {
        self.0
    }
}

/// Async dispatch for a **data-returning** consumer op (one-operation-in-flight),
/// mirroring [`async_void_op`] but for methods that return a value. The access
/// guard is held from submission until the completion job fires, so any
/// concurrent op (sync or async) is rejected with `ConcurrentModification` until
/// completion. If the guard cannot be acquired, `complete` fires inline with the
/// error.
///
/// `op` runs the awaited consumer method on the runtime; `complete` runs on the
/// dispatcher thread, builds the C result handle from the `Ok` value (or an
/// error handle from the `Err`) and fires the typed C callback with `user_data`.
/// Building the handle inside `complete` (not inside the spawned task) keeps the
/// future free of non-`Send` raw pointers — the symmetric reason
/// [`kafka_consumer_Consumer_poll_async`] forbids `.await` after building handles.
///
/// # Safety
///
/// `consumer` must be a valid handle from a consumer constructor.
unsafe fn async_value_op<T, Fut, F, C>(
    consumer: *const kafka_consumer_Consumer_t,
    user_data: *mut c_void,
    op: F,
    complete: C,
) where
    T: Send + 'static,
    Fut: std::future::Future<Output = Result<T, KafkaError>> + Send,
    F: FnOnce(&'static mut dyn Consumer<Bytes, Bytes>) -> Fut + Send + 'static,
    C: FnOnce(Result<T, KafkaError>, *mut c_void) + Send + 'static,
{
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        // Rejected: fire the callback inline with the error; guard not taken.
        complete(Err(e), user_data);
        return;
    }
    let tx = h.completion_tx.clone();
    let hs: &'static FfiConsumerHandle = unsafe { handle_ref(consumer) };
    let ud = SendUserData(user_data);
    h.runtime_handle.spawn(async move {
        let ud = ud;
        let result = op(unsafe { consumer_mut(hs) }).await;
        let job: CompletionJob = Box::new(move || {
            // Release BEFORE firing the callback: the awaited op is complete, so
            // the consumer is no longer borrowed. This avoids a release-vs-next-op
            // race when the callback resumes embedder work on another thread.
            release(hs);
            complete(result, ud.into_ptr());
        });
        enqueue_or_run_inline(&tx, job);
    });
}

// ── subscribe ──

/// Subscribes to a list of topics (sync). `topics` is an array of `count` C
/// strings. Returns null on success, non-null error on failure.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let topic_vec = unsafe { read_topics(topics, count) };
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.subscribe(topic_vec))) }
}

/// Completion callback for void-returning async consumer ops.
pub type kafka_consumer_Consumer_op_callback_t = unsafe extern "C" fn(*mut kafka_common_KafkaError_t, *mut c_void);

/// Subscribes to a list of topics (async). See [`kafka_consumer_Consumer_subscribe`].
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let topic_vec = unsafe { read_topics(topics, count) };
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.subscribe(topic_vec)) };
}

// ── unsubscribe ──

/// Unsubscribes from all topics / partitions (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_KafkaError_t {
    unsafe { sync_void_op(consumer, |c| Box::pin(c.unsubscribe())) }
}

/// Unsubscribes from all topics / partitions (async).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    unsafe { async_void_op(consumer, callback, user_data, |c| c.unsubscribe()) };
}

// ── assign (async; sync already defined above) ──

/// Assigns the consumer to a set of `(topic, partition)` pairs (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` `count` valid C strings,
/// `partitions` `count` `i32` values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.assign(tps)) };
}

// ── seek ──

/// Seeks a single partition to `offset` (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
) -> *mut kafka_common_KafkaError_t {
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.seek(tp, offset))) }
}

/// Seeks a single partition to `offset` (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_async(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.seek(tp, offset)) };
}

/// Seeks a single partition to `offset` with commit metadata / leader epoch
/// (sync). Pass `metadata == NULL` for no metadata and `leader_epoch < 0` for
/// no leader epoch.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string; `metadata` null
/// or a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_metadata(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    metadata: *const c_char,
) -> *mut kafka_common_KafkaError_t {
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    let metadata_str = if metadata.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(metadata) }.to_string_lossy().to_string()
    };
    let epoch = if leader_epoch < 0 { None } else { Some(leader_epoch) };
    let oam = match OffsetAndMetadata::with_leader_epoch(offset, epoch, metadata_str) {
        Ok(o) => o,
        Err(e) => return box_error(e),
    };
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.seek_with_metadata(tp, oam))) }
}

/// Seeks the given partitions to their beginning offsets (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.seek_to_beginning(&tps).await })) }
}

/// Seeks the given partitions to their beginning offsets (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe {
        async_void_op(consumer, callback, user_data, move |c| async move {
            c.seek_to_beginning(&tps).await
        })
    };
}

/// Seeks the given partitions to their end offsets (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.seek_to_end(&tps).await })) }
}

/// Seeks the given partitions to their end offsets (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { async_void_op(consumer, callback, user_data, move |c| async move { c.seek_to_end(&tps).await }) };
}

// ── pause / resume ──

/// Pauses fetching for the given partitions (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.pause(&tps).await })) }
}

/// Pauses fetching for the given partitions (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { async_void_op(consumer, callback, user_data, move |c| async move { c.pause(&tps).await }) };
}

/// Resumes fetching for the given partitions (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.resume(&tps).await })) }
}

/// Resumes fetching for the given partitions (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe { async_void_op(consumer, callback, user_data, move |c| async move { c.resume(&tps).await }) };
}

// ── commit ──

/// Commits the consumed offsets synchronously (sync; no offsets argument means
/// commit the current positions).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_KafkaError_t {
    unsafe { sync_void_op(consumer, |c| Box::pin(c.commit_sync())) }
}

/// Commits the current positions asynchronously (async dispatch of
/// [`kafka_consumer_Consumer_commit_sync`]; the callback fires when the commit
/// has completed). One-operation-in-flight; reuses [`kafka_consumer_Consumer_op_callback_t`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    unsafe { async_void_op(consumer, callback, user_data, |c| c.commit_sync()) };
}

/// Reads `count` `(topic, partition, offset, leader_epoch, metadata)` tuples
/// from parallel C arrays into a `HashMap<TopicPartition, OffsetAndMetadata>`.
/// `metadata` may be null (whole array) or contain null entries (per element);
/// `leader_epoch` entries `< 0` mean no epoch.
///
/// # Safety
///
/// All non-null arrays must have `count` valid entries.
unsafe fn read_offset_map(
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
    let n = count.max(0) as usize;
    let mut map = HashMap::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        let partition = unsafe { *partitions.add(i) };
        let offset = unsafe { *offsets.add(i) };
        let epoch = if leader_epochs.is_null() {
            None
        } else {
            let e = unsafe { *leader_epochs.add(i) };
            if e < 0 { None } else { Some(e) }
        };
        let meta = if metadata.is_null() {
            String::new()
        } else {
            let m = unsafe { *metadata.add(i) };
            if m.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(m) }.to_string_lossy().to_string()
            }
        };
        let oam = OffsetAndMetadata::with_leader_epoch(offset, epoch, meta)?;
        map.insert(TopicPartition::new(topic, partition), oam);
    }
    Ok(map)
}

/// Commits specific offsets synchronously (sync). Parallel arrays of
/// `(topic, partition, offset, leader_epoch, metadata)`; `metadata` may be null
/// and `leader_epoch < 0` means no epoch.
///
/// # Safety
///
/// `consumer` a valid handle; arrays `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    let map = match unsafe { read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.commit_sync_offsets(map))) }
}

/// Commits specific offsets asynchronously (async dispatch of
/// [`kafka_consumer_Consumer_commit_sync_offsets`]; the callback fires when the
/// commit has completed). One-operation-in-flight; reuses
/// [`kafka_consumer_Consumer_op_callback_t`].
///
/// # Safety
///
/// `consumer` a valid handle; arrays `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_offsets_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    let map = match unsafe { read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count) } {
        Ok(m) => m,
        Err(e) => {
            // Marshaling failed: fire inline with the error (no guard taken).
            unsafe { callback(box_error(e), user_data) };
            return;
        },
    };
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.commit_sync_offsets(map)) };
}

/// Commits the consumed offsets asynchronously (sync call, returns once the
/// async commit is initiated; the listener-taking variant is not exposed —
/// decision #4).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_KafkaError_t {
    unsafe { sync_void_op(consumer, |c| Box::pin(c.commit_async())) }
}

// ── enforce_rebalance ──

/// Triggers a rebalance (sync). `reason` may be null. Under KIP-848 this
/// returns an unsupported-version error, matching Java.
///
/// # Safety
///
/// `consumer` a valid handle; `reason` null or a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance(
    consumer: *const kafka_consumer_Consumer_t,
    reason: *const c_char,
) -> *mut kafka_common_KafkaError_t {
    let reason_str = if reason.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(reason) }.to_string_lossy().to_string())
    };
    unsafe {
        sync_void_op(consumer, move |c| {
            Box::pin(async move { c.enforce_rebalance(reason_str.as_deref()).await })
        })
    }
}

// ── close ──

/// Closes the consumer with the default timeout (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_KafkaError_t {
    unsafe { sync_void_op(consumer, |c| Box::pin(c.close())) }
}

/// Closes the consumer with a timeout in milliseconds (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_with_timeout(
    consumer: *const kafka_consumer_Consumer_t,
    timeout_ms: i64,
) -> *mut kafka_common_KafkaError_t {
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    let options = CloseOptions::timeout(timeout);
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.close_with_options(options))) }
}

/// Closes the consumer asynchronously (default timeout).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    unsafe { async_void_op(consumer, callback, user_data, |c| c.close()) };
}

// ---------------------------------------------------------------------------
// Phase D — scalar / map / list returns
// ---------------------------------------------------------------------------

/// Returns the current position of `(topic, partition)` (sync). On success
/// writes the offset to `*out_position` and returns null; on failure returns a
/// non-null error handle.
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string; `out_position` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    out_position: *mut i64,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    match h.runtime.block_on(unsafe { consumer_mut(h).position(&tp) }) {
        Ok(pos) => {
            if !out_position.is_null() {
                unsafe { *out_position = pos };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Completion callback for [`kafka_consumer_Consumer_position_async`]. On
/// success `error` is null and `position` is the offset; on failure `error` is
/// non-null and `position` is 0. (Position is never absent on success, so —
/// unlike `current_lag` — no presence flag is needed.) The callback owns
/// `error` if non-null.
pub type kafka_consumer_Consumer_position_callback_t =
    unsafe extern "C" fn(i64, *mut kafka_common_KafkaError_t, *mut c_void);

/// Returns the current position of `(topic, partition)` asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_position`].
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_async(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    callback: kafka_consumer_Consumer_position_callback_t,
    user_data: *mut c_void,
) {
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.position(&tp).await },
            move |result, ud| {
                let (pos, err) = match result {
                    Ok(p) => (p, std::ptr::null_mut()),
                    Err(e) => (0i64, box_error(e)),
                };
                callback(pos, err, ud);
            },
        )
    };
}

/// Returns the last committed offsets for the given partitions (sync). On
/// success writes an [`kafka_consumer_OffsetMap_t`] to `*out_map` (free it with
/// [`kafka_consumer_OffsetMap_destroy`]) and returns null; on failure returns a
/// non-null error and leaves `*out_map` untouched.
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` entries; `out_map`
/// valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_OffsetMap_t,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    match h.runtime.block_on(unsafe { consumer_mut(h).committed(&tps) }) {
        Ok(map) => {
            if !out_map.is_null() {
                unsafe { *out_map = box_offset_map(map) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Completion callback for [`kafka_consumer_Consumer_committed_async`]. On
/// success `map` is non-null (an [`kafka_consumer_OffsetMap_t`], free with
/// [`kafka_consumer_OffsetMap_destroy`]) and `error` is null; on failure `map`
/// is null and `error` is non-null. The callback owns whichever is non-null.
pub type kafka_consumer_Consumer_committed_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_OffsetMap_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Returns the last committed offsets for the given partitions asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_committed`].
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_committed_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.committed(&tps).await },
            move |result, ud| {
                let (map, err) = match result {
                    Ok(m) => (box_offset_map(m), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(map, err, ud);
            },
        )
    };
}

/// Looks up offsets by timestamp for the given partitions (sync). Parallel
/// arrays of `(topic, partition, timestamp)`. On success writes an
/// [`kafka_consumer_OffsetAndTimestampMap_t`] to `*out_map`. Unresolved
/// partitions are omitted from the map (see [`Consumer::offsets_for_times`]).
///
/// # Safety
///
/// `consumer` a valid handle; arrays `count` valid entries; `out_map` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    timestamps: *const i64,
    count: i32,
    out_map: *mut *mut kafka_consumer_OffsetAndTimestampMap_t,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let n = count.max(0) as usize;
    let mut req = HashMap::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        let partition = unsafe { *partitions.add(i) };
        let timestamp = unsafe { *timestamps.add(i) };
        req.insert(TopicPartition::new(topic, partition), timestamp);
    }
    match h.runtime.block_on(unsafe { consumer_mut(h).offsets_for_times(req) }) {
        Ok(map) => {
            if !out_map.is_null() {
                unsafe { *out_map = box_offset_and_timestamp_map(map) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Reads `count` `(topic, partition, timestamp)` tuples from parallel C arrays
/// into the `HashMap<TopicPartition, i64>` request for `offsets_for_times`.
///
/// # Safety
///
/// All arrays must have `count` valid entries.
unsafe fn read_timestamps_to_search(
    topics: *const *const c_char,
    partitions: *const i32,
    timestamps: *const i64,
    count: i32,
) -> HashMap<TopicPartition, i64> {
    let n = count.max(0) as usize;
    let mut req = HashMap::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        let partition = unsafe { *partitions.add(i) };
        let timestamp = unsafe { *timestamps.add(i) };
        req.insert(TopicPartition::new(topic, partition), timestamp);
    }
    req
}

/// Completion callback for [`kafka_consumer_Consumer_offsets_for_times_async`].
/// On success `map` is a non-null [`kafka_consumer_OffsetAndTimestampMap_t`]
/// (free with [`kafka_consumer_OffsetAndTimestampMap_destroy`]) and `error` is
/// null; on failure `map` is null and `error` is non-null. The callback owns
/// whichever is non-null.
pub type kafka_consumer_Consumer_offsets_for_times_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_OffsetAndTimestampMap_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Looks up offsets by timestamp asynchronously (one-operation-in-flight).
/// See [`kafka_consumer_Consumer_offsets_for_times`].
///
/// # Safety
///
/// `consumer` a valid handle; arrays `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    timestamps: *const i64,
    count: i32,
    callback: kafka_consumer_Consumer_offsets_for_times_callback_t,
    user_data: *mut c_void,
) {
    let req = unsafe { read_timestamps_to_search(topics, partitions, timestamps, count) };
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.offsets_for_times(req).await },
            move |result, ud| {
                let (map, err) = match result {
                    Ok(m) => (box_offset_and_timestamp_map(m), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(map, err, ud);
            },
        )
    };
}

/// Returns the beginning offsets for the given partitions (sync). On success
/// writes a [`kafka_consumer_LongOffsetMap_t`] to `*out_map`.
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` entries; `out_map`
/// valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_LongOffsetMap_t,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    match h.runtime.block_on(unsafe { consumer_mut(h).beginning_offsets(&tps) }) {
        Ok(map) => {
            if !out_map.is_null() {
                unsafe { *out_map = box_long_offset_map(map) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Completion callback shared by [`kafka_consumer_Consumer_beginning_offsets_async`]
/// and [`kafka_consumer_Consumer_end_offsets_async`] (both return a
/// [`kafka_consumer_LongOffsetMap_t`]). On success `map` is non-null (free with
/// [`kafka_consumer_LongOffsetMap_destroy`]) and `error` is null; on failure
/// `map` is null and `error` is non-null. The callback owns whichever is non-null.
pub type kafka_consumer_Consumer_long_offsets_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_LongOffsetMap_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Returns the beginning offsets for the given partitions asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_beginning_offsets`].
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_long_offsets_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.beginning_offsets(&tps).await },
            move |result, ud| {
                let (map, err) = match result {
                    Ok(m) => (box_long_offset_map(m), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(map, err, ud);
            },
        )
    };
}

/// Returns the end offsets for the given partitions (sync). On success writes a
/// [`kafka_consumer_LongOffsetMap_t`] to `*out_map`.
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` entries; `out_map`
/// valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_LongOffsetMap_t,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    match h.runtime.block_on(unsafe { consumer_mut(h).end_offsets(&tps) }) {
        Ok(map) => {
            if !out_map.is_null() {
                unsafe { *out_map = box_long_offset_map(map) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Returns the end offsets for the given partitions asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_end_offsets`].
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_long_offsets_callback_t,
    user_data: *mut c_void,
) {
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.end_offsets(&tps).await },
            move |result, ud| {
                let (map, err) = match result {
                    Ok(m) => (box_long_offset_map(m), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(map, err, ud);
            },
        )
    };
}

/// Returns the partition metadata for a topic (sync). On success writes a
/// [`kafka_consumer_PartitionInfoList_t`] to `*out_list`.
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string; `out_list` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    out_list: *mut *mut kafka_consumer_PartitionInfoList_t,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    match h.runtime.block_on(unsafe { consumer_mut(h).partitions_for(&topic_str) }) {
        Ok(infos) => {
            if !out_list.is_null() {
                unsafe { *out_list = box_partition_info_list(infos) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Completion callback for [`kafka_consumer_Consumer_partitions_for_async`]. On
/// success `list` is a non-null [`kafka_consumer_PartitionInfoList_t`] (free with
/// [`kafka_consumer_PartitionInfoList_destroy`]) and `error` is null; on failure
/// `list` is null and `error` is non-null. The callback owns whichever is non-null.
pub type kafka_consumer_Consumer_partitions_for_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_PartitionInfoList_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Returns the partition metadata for a topic asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_partitions_for`].
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_async(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    callback: kafka_consumer_Consumer_partitions_for_callback_t,
    user_data: *mut c_void,
) {
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    unsafe {
        async_value_op(
            consumer,
            user_data,
            move |c| async move { c.partitions_for(&topic_str).await },
            move |result, ud| {
                let (list, err) = match result {
                    Ok(infos) => (box_partition_info_list(infos), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(list, err, ud);
            },
        )
    };
}

/// Returns metadata for all topics the consumer is authorized to view (sync).
/// On success writes a [`kafka_consumer_TopicPartitionInfoMap_t`] to `*out_map`.
///
/// # Safety
///
/// `consumer` a valid handle; `out_map` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics(
    consumer: *const kafka_consumer_Consumer_t,
    out_map: *mut *mut kafka_consumer_TopicPartitionInfoMap_t,
) -> *mut kafka_common_KafkaError_t {
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    match h.runtime.block_on(unsafe { consumer_mut(h).list_topics() }) {
        Ok(map) => {
            if !out_map.is_null() {
                unsafe { *out_map = box_topic_partition_info_map(map) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Completion callback for [`kafka_consumer_Consumer_list_topics_async`]. On
/// success `map` is a non-null [`kafka_consumer_TopicPartitionInfoMap_t`] (free
/// with [`kafka_consumer_TopicPartitionInfoMap_destroy`]) and `error` is null;
/// on failure `map` is null and `error` is non-null. The callback owns whichever
/// is non-null.
pub type kafka_consumer_Consumer_list_topics_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_TopicPartitionInfoMap_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Returns metadata for all topics the consumer is authorized to view
/// asynchronously (one-operation-in-flight). See [`kafka_consumer_Consumer_list_topics`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_list_topics_callback_t,
    user_data: *mut c_void,
) {
    unsafe {
        async_value_op(
            consumer,
            user_data,
            |c| async move { c.list_topics().await },
            move |result, ud| {
                let (map, err) = match result {
                    Ok(m) => (box_topic_partition_info_map(m), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(map, err, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// Phase D — sync state reads (under the access guard)
// ---------------------------------------------------------------------------

/// Returns the current assignment as a [`kafka_consumer_TopicPartitionList_t`]
/// (free with [`kafka_consumer_TopicPartitionList_destroy`]), or null on a
/// concurrent-access rejection (the guard could not be acquired).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assignment(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_TopicPartitionList_t {
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let set = unsafe { consumer_mut(h) }.assignment();
    box_topic_partition_list(set)
}

/// Returns the current topic subscription as a
/// [`kafka_consumer_StringList_t`], or null on a concurrent-access rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscription(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_StringList_t {
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let set = unsafe { consumer_mut(h) }.subscription();
    box_string_list(set)
}

/// Returns the currently paused partitions as a
/// [`kafka_consumer_TopicPartitionList_t`], or null on a concurrent-access
/// rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_paused(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_TopicPartitionList_t {
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let set = unsafe { consumer_mut(h) }.paused();
    box_topic_partition_list(set)
}

/// Returns the consumer group metadata as a
/// [`kafka_consumer_ConsumerGroupMetadata_t`], or null on a concurrent-access
/// rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_group_metadata(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let meta = unsafe { consumer_mut(h) }.group_metadata();
    box_group_metadata(meta)
}

/// Returns the client id as an owned NUL-terminated C string (free it with
/// [`kafka_consumer_string_destroy`]), or null on a concurrent-access rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_client_id(consumer: *const kafka_consumer_Consumer_t) -> *mut c_char {
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let id = unsafe { consumer_mut(h) }.client_id().to_string();
    match std::ffi::CString::new(id) {
        Ok(c) => c.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Returns the current lag of `(topic, partition)` (sync, never blocks). Writes
/// the lag to `*out_lag` and returns `true` if known; returns `false` if the
/// lag is unknown or the guard could not be acquired.
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string; `out_lag` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_current_lag(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    out_lag: *mut i64,
) -> bool {
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return false;
    }
    let _g = ReleaseGuard(h);
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    match unsafe { consumer_mut(h) }.current_lag(&tp) {
        Some(lag) => {
            if !out_lag.is_null() {
                unsafe { *out_lag = lag };
            }
            true
        },
        None => false,
    }
}
