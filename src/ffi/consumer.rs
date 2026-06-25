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

use crate::common::KafkaError;
use crate::common::serialization::ByteArrayDeserializer;
use crate::consumer::async_kafka_consumer::AsyncKafkaConsumer;
use crate::consumer::{
    AutoOffsetResetStrategy, Consumer, ConsumerRecord, ConsumerRecords, GroupProtocol, MockConsumer, WakeupHandle,
};

use super::common::{
    self, CompletionJob, box_error, enqueue_or_run_inline, init_default_logger, kafka_common_KafkaError_t,
};

// The byte-array consumer is monomorphized over `Vec<u8>` keys and values.
type Bytes = Vec<u8>;

// ---------------------------------------------------------------------------
// Access guard
// ---------------------------------------------------------------------------

/// Free sentinel for [`ConsumerHandle::owner`]: no thread/future currently
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
fn acquire(h: &ConsumerHandle) -> Result<(), KafkaError> {
    let tid = current_thread_id();
    match h.owner.compare_exchange(NO_OWNER, tid, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Ok(()),
        Err(_) => Err(KafkaError::concurrent_modification(
            "KafkaConsumer is not safe for multi-threaded access.",
        )),
    }
}

/// Releases the single-owner guard for `h`.
fn release(h: &ConsumerHandle) {
    h.owner.store(NO_OWNER, Ordering::Release);
}

/// RAII guard that releases the access guard on scope exit (return or panic),
/// mirroring Java's `finally { release(); }`. Used by the sync FFI path; the
/// async path releases inside the completion job instead.
struct ReleaseGuard<'a>(&'a ConsumerHandle);
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
struct ConsumerHandle {
    /// The consumer. Exclusive access is enforced by `owner`, not the type
    /// system — `UnsafeCell` is needed to hand out `&mut` from a shared
    /// `&ConsumerHandle`.
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
    /// Wakeup handle captured at construction; fired by
    /// [`kafka_consumer_Consumer_wakeup`] without acquiring the guard.
    wakeup_handle: WakeupHandle,
    /// Whether this handle wraps a [`MockConsumer`].
    #[allow(dead_code)]
    is_mock: bool,
}

// SAFETY: `acquire()` guarantees at most one thread/future accesses
// `*consumer.get()` at any instant, and `ConsumerKind: Send`, so exclusive
// cross-thread access is sound. `UnsafeCell` is needed to hand out `&mut` from
// a shared `&ConsumerHandle`.
unsafe impl Send for ConsumerHandle {}
unsafe impl Sync for ConsumerHandle {}

/// Builds a [`ConsumerHandle`] around a [`ConsumerKind`], spawning the
/// callback dispatcher thread, and returns the leaked C handle.
fn build_consumer_handle(
    kind: ConsumerKind,
    wakeup_handle: WakeupHandle,
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

    let handle = Box::new(ConsumerHandle {
        consumer: UnsafeCell::new(kind),
        owner: AtomicU64::new(NO_OWNER),
        runtime,
        runtime_handle,
        completion_tx,
        dispatcher: Mutex::new(Some(dispatcher)),
        wakeup_handle,
        is_mock,
    });
    Box::into_raw(handle) as *mut kafka_consumer_Consumer_t
}

/// Casts a `*const kafka_consumer_Consumer_t` to a `&'static ConsumerHandle`.
///
/// # Safety
///
/// `consumer` must be non-null and created by a consumer constructor.
unsafe fn handle_ref(consumer: *const kafka_consumer_Consumer_t) -> &'static ConsumerHandle {
    unsafe { &*(consumer as *const ConsumerHandle) }
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
    // capture its `wakeup_handle()` before boxing it as a trait object.
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

    let consumer = match AsyncKafkaConsumer::<Bytes, Bytes>::new(
        config,
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    ) {
        Ok(c) => c,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    let wakeup_handle = consumer.wakeup_handle();
    if !out_error.is_null() {
        unsafe { *out_error = std::ptr::null_mut() };
    }
    build_consumer_handle(ConsumerKind::Async(Box::new(consumer)), wakeup_handle, false)
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
    // `MockConsumer` exposes a `WakeupHandle` through the `Consumer` trait
    // (backed by a shared `AtomicBool` flag observed by the next `poll`), so we
    // capture it here just like the async arm — no no-op handle is needed.
    let wakeup_handle = consumer.wakeup_handle();
    build_consumer_handle(ConsumerKind::Mock(Box::new(consumer)), wakeup_handle, true)
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
    let handle = unsafe { Box::from_raw(consumer as *mut ConsumerHandle) };
    let ConsumerHandle { consumer, runtime, completion_tx, dispatcher, .. } = *handle;

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
    handle.wakeup_handle.wakeup();
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
    // Capture the `&'static ConsumerHandle` (Send+Sync via the unsafe impls),
    // NOT a bare `*mut` (raw pointers are !Send and would make the future
    // !Send). The handle is leaked, so the borrow is effectively `'static`.
    let hs: &'static ConsumerHandle = unsafe { handle_ref(consumer) };
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
/// `&'static ConsumerHandle` so the access guard is released **after** the
/// callback fires (keeping `owner` held for the whole submit->callback window).
struct PollCompletion {
    target: PollCallbackTarget,
    records: *mut kafka_consumer_ConsumerRecords_t,
    error: *mut kafka_common_KafkaError_t,
    handle: &'static ConsumerHandle,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread;
// the C user is responsible for the thread-safety of `user_data`. The
// `&ConsumerHandle` is Send via the type's `unsafe impl Send`.
unsafe impl Send for PollCompletion {}
impl PollCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    unsafe fn fire(self) {
        unsafe { (self.target.callback)(self.records, self.error, self.target.user_data) };
        // Release only after the callback fires, so `owner` stays held and
        // rejects concurrent ops for the whole submit->callback window.
        release(self.handle);
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
unsafe fn consumer_mut(h: &ConsumerHandle) -> &mut dyn Consumer<Bytes, Bytes> {
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
unsafe fn mock_mut(h: &ConsumerHandle) -> Result<&mut MockConsumer<Bytes, Bytes>, KafkaError> {
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
        Some(unsafe { std::slice::from_raw_parts(key, key_len as usize) }.to_vec())
    };
    let value_vec: Option<Bytes> = if value_len < 0 || value.is_null() {
        None
    } else {
        Some(unsafe { std::slice::from_raw_parts(value, value_len as usize) }.to_vec())
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
