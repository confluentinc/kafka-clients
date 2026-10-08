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
//!   with [`Error::local_concurrent_modification`], exactly as Java throws
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
#![expect(non_camel_case_types)]

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::common::header::{Header, RecordHeader};
use crate::common::metrics::KafkaMetric;
use crate::common::serialization::BytesDeserializer;
use crate::common::{Error, Node, PartitionInfo, TopicPartition};
use crate::consumer::AsyncKafkaConsumer;
// `crate::consumer::ConsumerHandle` is aliased because this module already has a
// private `ConsumerHandle` (the state behind `kafka_consumer_Consumer_t`), which
// is an unrelated concept.
use crate::consumer::internals::AutoOffsetResetStrategy;
use crate::consumer::{
    Consumer, ConsumerGroupMetadata, ConsumerHandle, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords,
    GroupProtocol, MockConsumer, OffsetAndMetadata, OffsetAndTimestamp, OffsetCommitCallback,
};

use super::common::{
    self, CallbackTarget, CompletionJob, OperationCallbackFn, OperationCallbackTarget, OperationCompletion, box_error,
    dispatch_and_wait, enqueue_or_run_inline, init_default_logger, kafka_common_Error_t, spawn_callback_task,
};
use super::ffi_guard;

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
/// [`Error::local_concurrent_modification`] if another thread/future already
/// holds it (the non-reentrant guard rejects re-entry too — see module docs).
fn acquire(h: &FfiConsumerHandle) -> Result<(), Error> {
    let tid = current_thread_id();
    match h.owner.compare_exchange(NO_OWNER, tid, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Ok(()),
        Err(_) => Err(Error::local_concurrent_modification(
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
    #[expect(dead_code)]
    is_mock: bool,
}

// SAFETY: `acquire()` guarantees at most one thread/future accesses
// `*consumer.get()` at any instant, and `ConsumerKind: Send`, so exclusive
// cross-thread access is sound. `UnsafeCell` is needed to hand out `&mut` from
// a shared `&FfiConsumerHandle`.
unsafe impl Send for FfiConsumerHandle {}
// SAFETY: `Sync` is what lets several C threads hold a `&FfiConsumerHandle` at
// once. The only non-`Sync` field is the `UnsafeCell`, and every access to
// `*consumer.get()` goes through `consumer_mut` / `mock_mut`, whose contract
// requires the single-owner guard taken by `acquire()`, so two live references
// never reach the cell at the same time; the remaining fields (`AtomicU64`, the
// runtime and its handle, the `Mutex`, the channel sender, `ConsumerHandle`) are
// `Sync` on their own.
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
    // SAFETY: Per this function's `# Safety`, `consumer` is non-null and was produced by
    // `Box::into_raw` in `build_consumer_handle` (the only constructor of
    // `FfiConsumerHandle`). The allocation is leaked and freed only by
    // `kafka_consumer_Consumer_destroy`, so the `&'static` is valid for as long as the C
    // caller keeps the handle alive, which every caller's `# Safety` requires for the
    // duration of its use. Sharing the reference across threads is sound via the `unsafe
    // impl Sync`; exclusive access to the inner consumer is enforced separately by
    // `acquire`.
    unsafe { &*(consumer as *const FfiConsumerHandle) }
}

/// Clones the core [`ConsumerHandle`] captured at construction together with a
/// [`tokio::runtime::Handle`] for the consumer's runtime — the two pieces
/// [`super::consumer_handle::kafka_consumer_Consumer_handle`] needs to build a
/// `kafka_consumer_ConsumerHandle_t`.
///
/// Deliberately does **not** acquire the access guard: every core
/// `ConsumerHandle` method takes `&self` and the value is captured once at
/// construction (it never changes), so there is nothing to serialize. Handing
/// out the handle while an operation is in flight is exactly the point — see
/// the `consumer_handle` module docs.
///
/// # Safety
///
/// `consumer` must be non-null and created by a consumer constructor.
pub(crate) unsafe fn clone_core_handle(
    consumer: *const kafka_consumer_Consumer_t,
) -> (ConsumerHandle, tokio::runtime::Handle) {
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's own `# Safety` requires of `consumer`. The reference is used only to
    // clone `consumer_handle` and `runtime_handle`, both `&self` reads of fields fixed at
    // construction, and it does not outlive this call, during which the C caller keeps the
    // handle alive; deliberately no guard is taken, as the rustdoc explains.
    let h = unsafe { handle_ref(consumer) };
    (h.consumer_handle.clone(), h.runtime_handle.clone())
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
    // SAFETY: Per this function's `# Safety`, `props` is a valid handle from a
    // `ConsumerProperties` constructor, i.e. a `HashMap<String, String>` leaked via
    // `Box::into_raw` in `kafka_consumer_ConsumerProperties_new` or `_from_configs`; it
    // stays allocated until `kafka_consumer_ConsumerProperties_destroy`, and callers use
    // the returned reference only for the duration of their own synchronous call.
    unsafe { &*(props as *const HashMap<String, String>) }
}

/// Casts a `*mut kafka_consumer_ConsumerProperties_t` to a mutable reference.
///
/// # Safety
///
/// `props` must be a valid handle from a `ConsumerProperties` constructor.
unsafe fn properties_mut(props: *mut kafka_consumer_ConsumerProperties_t) -> &'static mut HashMap<String, String> {
    // SAFETY: Same provenance as `properties_ref`: per this function's `# Safety`, `props`
    // is a live `HashMap<String, String>` from `Box::into_raw` in a `ConsumerProperties`
    // constructor, alive until `kafka_consumer_ConsumerProperties_destroy`. The `&mut` is
    // used only within the caller's synchronous call
    // (`kafka_consumer_ConsumerProperties_put`); its exclusivity relies on the C caller not
    // operating on the same properties handle from another thread at the same time, which
    // no guard enforces and the contract states only as "a valid handle".
    unsafe { &mut *(props as *mut HashMap<String, String>) }
}

/// Creates an empty consumer properties handle.
///
/// # Returns
///
/// A non-null opaque properties handle. The caller must free it with
/// [`kafka_consumer_ConsumerProperties_destroy`].
#[ffi_guard]
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
#[ffi_guard]
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
        // SAFETY: `configs` is non-null (checked above) and, per this function's `#
        // Safety`, points to a NULL-terminated array of C-string pointers, so element `i`
        // is readable: `i` advances by two and the loop exits at the first NULL key, so
        // every index read so far lies at or before the terminator.
        let key_ptr = unsafe { *configs.add(i) };
        if key_ptr.is_null() {
            break;
        }
        // SAFETY: The key at index `i` was non-null (a NULL key breaks out of the loop
        // before this read), so per the `# Safety` contract the NULL terminator has not
        // been reached yet and index `i + 1` is within the array: at worst it is the
        // terminator itself, which is read and then rejected (returning NULL) rather than
        // dereferenced.
        let val_ptr = unsafe { *configs.add(i + 1) };
        if val_ptr.is_null() {
            return std::ptr::null_mut();
        }
        // SAFETY: `key_ptr` is non-null (a NULL key breaks out of the loop before this
        // point) and, per the `# Safety` contract, every non-NULL entry of `configs` is a
        // valid NUL-terminated C string that stays alive for the call; the bytes are copied
        // into an owned `String` immediately.
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_string_lossy().to_string();
        // SAFETY: `val_ptr` is non-null (a NULL value returns NULL from the function before
        // this point) and, per the `# Safety` contract, a valid NUL-terminated C string
        // alive for the duration of the call; it is copied into an owned `String`
        // immediately.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerProperties_put(
    props: *mut kafka_consumer_ConsumerProperties_t,
    key: *const c_char,
    value: *const c_char,
) {
    if props.is_null() || key.is_null() || value.is_null() {
        return;
    }
    // SAFETY: `props` is non-null (checked above together with `key` and `value`) and, per
    // this function's `# Safety`, a valid properties handle, which is what `properties_mut`
    // requires. The `&mut HashMap` is used only within this synchronous call; its
    // exclusivity relies on the C caller not using the same properties handle concurrently,
    // which no guard enforces (the contract says only "a valid handle").
    let map = unsafe { properties_mut(props) };
    // SAFETY: `key` is non-null (checked above) and, per this function's `# Safety`, a
    // valid NUL-terminated C string for the duration of the call; it is copied into an
    // owned `String` immediately.
    let k = unsafe { CStr::from_ptr(key) }.to_string_lossy().to_string();
    // SAFETY: `value` is non-null (checked above) and, per this function's `# Safety`, a
    // valid NUL-terminated C string for the duration of the call; it is copied into an
    // owned `String` immediately.
    let v = unsafe { CStr::from_ptr(value) }.to_string_lossy().to_string();
    map.insert(k, v);
}

/// Destroys a properties handle. Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// `props` must be null or a valid handle from a `ConsumerProperties`
/// constructor. After this call the pointer is invalid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerProperties_destroy(props: *mut kafka_consumer_ConsumerProperties_t) {
    if !props.is_null() {
        // SAFETY: `props` is non-null (checked above) and, per this function's `# Safety`,
        // a handle produced by `Box::into_raw` of a `Box<HashMap<String, String>>` in
        // `kafka_consumer_ConsumerProperties_new` or `_from_configs`; the contract declares
        // the pointer invalid after this call, so this is the single, final use.
        // `kafka_consumer_KafkaConsumer_new` only borrows the properties (the caller
        // retains ownership), so no other path frees them.
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
/// handle on failure (free it with [`kafka_common_Error_destroy`]).
///
/// # Safety
///
/// `props` must be a valid, non-null properties handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaConsumer_new(
    props: *const kafka_consumer_ConsumerProperties_t,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_Consumer_t {
    init_default_logger();
    if props.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above); per the parameter docs it is
            // either null or a slot for one error handle, and exactly one freshly boxed
            // handle from `box_error` is written, whose ownership passes to the caller.
            unsafe { *out_error = box_error(Error::local_illegal_argument("properties handle must not be null")) };
        }
        return std::ptr::null_mut();
    }
    // SAFETY: `props` is non-null (checked above) and, per this function's `# Safety`, a
    // valid properties handle, which is what `properties_ref` requires; the borrowed map is
    // read only while building `ConsumerConfig` within this call, during which the caller
    // retains ownership of the handle.
    let map = unsafe { properties_ref(props) };
    let config = match crate::consumer::ConsumerConfig::new(map) {
        Ok(c) => c,
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above); per the parameter docs
                // it points to a slot for one error handle, and exactly one freshly boxed
                // `box_error(e)` handle is written for the caller to free with
                // `kafka_common_Error_destroy`.
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    // Replicate the `GroupProtocol::of` gate from `KafkaConsumer::new`
    // (`src/consumer/mod.rs`): classic protocol is unsupported in this client.
    // We construct the concrete `AsyncKafkaConsumer` directly (rather than
    // routing through `KafkaConsumer::new`, which erases the concrete type) so we can
    // capture its `handle()` before boxing it as a trait object.
    match GroupProtocol::of(config.group_protocol()) {
        Ok(GroupProtocol::Consumer) => {},
        Ok(GroupProtocol::Classic) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above); per the parameter docs
                // it points to a slot for one error handle, and exactly one freshly boxed
                // handle is written for the caller to free with
                // `kafka_common_Error_destroy`.
                unsafe {
                    *out_error = box_error(Error::unsupported_version(
                        "Classic group protocol is not yet supported in this client; \
                         set group.protocol=consumer (KIP-848).",
                    ))
                };
            }
            return std::ptr::null_mut();
        },
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above); per the parameter docs
                // it points to a slot for one error handle, and exactly one freshly boxed
                // `box_error(e)` handle is written for the caller to free with
                // `kafka_common_Error_destroy`.
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
                    // SAFETY: `out_error` is non-null (checked above); per the parameter
                    // docs it points to a slot for one error handle, and exactly one
                    // freshly boxed `box_error(e)` handle is written for the caller to free
                    // with `kafka_common_Error_destroy`.
                    unsafe { *out_error = box_error(e) };
                }
                return std::ptr::null_mut();
            },
        };

    let consumer_handle = consumer.handle();
    if !out_error.is_null() {
        // SAFETY: `out_error` is non-null (checked above); per the documented contract
        // `*out_error` is set to null on success, so exactly one null pointer is written
        // through the caller-provided, writable slot.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_new(
    auto_offset_reset: *const c_char,
) -> *mut kafka_consumer_Consumer_t {
    init_default_logger();
    let strategy = if auto_offset_reset.is_null() {
        AutoOffsetResetStrategy::LATEST
    } else {
        // SAFETY: `auto_offset_reset` is non-null (checked above) and, per this function's
        // `# Safety`, a valid NUL-terminated C string for the duration of the call; it is
        // copied into an owned `String` immediately.
        let s = unsafe { CStr::from_ptr(auto_offset_reset) }.to_string_lossy().to_string();
        AutoOffsetResetStrategy::from_string(&s).unwrap_or(AutoOffsetResetStrategy::LATEST)
    };
    let consumer: MockConsumer<Bytes, Bytes> = MockConsumer::with_auto_offset_reset_strategy(strategy);
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
/// lifetime precondition the caller must uphold (CLAUDE.md FFI §4).
///
/// # Safety
///
/// `consumer` must be null or a valid handle from a consumer constructor.
/// After this call the pointer is invalid. No `_async` operation on the handle
/// may still be in flight: its awaiter task and completion job use the handle
/// until the callback has fired, and this function does not wait for them, so
/// it must only be called once every pending callback has been delivered.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_destroy(consumer: *mut kafka_consumer_Consumer_t) {
    if consumer.is_null() {
        return;
    }
    // SAFETY: `consumer` is non-null (checked above) and, per this function's `# Safety`, a
    // handle created by `Box::into_raw` in `build_consumer_handle` (the only producer of
    // `kafka_consumer_Consumer_t`) that becomes invalid after this call, so reclaiming the
    // `Box` is the single, final use. The `&'static FfiConsumerHandle` references captured
    // by async-variant tasks (`poll_async`, `async_void_op`, `async_value_op`) are kept
    // from outliving this free only by the documented precondition that no operation is in
    // flight when `destroy` runs (the guard is deliberately not acquired, mirroring Java):
    // a task touches its `hs` for the last time in the completion job's `release`, before
    // the C callback fires. `runtime.shutdown_background()` does not wait for running
    // tasks, so that ordering is a contractual guarantee from the C caller, not one this
    // function enforces.
    let handle = unsafe { Box::from_raw(consumer as *mut FfiConsumerHandle) };
    let FfiConsumerHandle { consumer, runtime, completion_tx, dispatcher, .. } = *handle;

    // 1. Shut down the runtime first. `shutdown_background` drops every
    //    async-variant awaiter that is not being polled at this instant and does
    //    not wait for one that is, which is why the `# Safety` contract requires
    //    that no `_async` operation is still in flight: an awaiter borrows
    //    `*consumer.get()`, which is dropped next.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_wakeup(consumer: *const kafka_consumer_Consumer_t) {
    if consumer.is_null() {
        return;
    }
    // SAFETY: `consumer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a consumer constructor, which is what `handle_ref` requires. The
    // reference is used only to call `consumer_handle.wakeup()` (`&self`) within this call;
    // bypassing the access guard is the documented purpose of the core `ConsumerHandle`,
    // whose methods all take `&self`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll(
    consumer: *const kafka_consumer_Consumer_t,
    timeout_ms: i64,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_ConsumerRecords_t {
    // SAFETY: `handle_ref` requires a non-null handle created by a consumer constructor;
    // this function's `# Safety` requires exactly that of `consumer` (there is no null
    // check, so a null handle is a contract violation by the C caller). The reference is
    // used only for the duration of this synchronous call, during which the caller keeps
    // the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above); per the parameter docs it is
            // null or a slot for one error handle, and exactly one freshly boxed
            // `box_error(e)` handle, owned by the caller, is written.
            unsafe { *out_error = box_error(e) };
        }
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    // SAFETY: `consumer_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns, so the
    // `&mut dyn Consumer` is exclusive for the whole `block_on(poll)` window per the
    // `UnsafeCell` + `acquire` invariant documented on `FfiConsumerHandle`'s `unsafe impl
    // Send/Sync`. `block_on` drives the future to completion before `_g` drops, so no
    // borrow outlives the guard.
    let result = h.runtime.block_on(unsafe { consumer_mut(h).poll(timeout) });
    match result {
        Ok(records) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above); per the documented
                // contract `*out_error` is set to null on success, so exactly one null
                // pointer is written through the caller's writable slot.
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_records(records)
        },
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above); per the parameter docs
                // it is null or a slot for one error handle, and exactly one freshly boxed
                // `box_error(e)` handle, owned by the caller, is written.
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
    unsafe extern "C" fn(*mut kafka_consumer_ConsumerRecords_t, *mut kafka_common_Error_t, *mut c_void);

/// Polls for records asynchronously (one-operation-in-flight). The access
/// guard is held from submission until the callback fires, so any concurrent
/// op (sync or async) is rejected with a `LocalConcurrentModification` error until
/// completion.
///
/// If the guard cannot be acquired, the callback fires inline with the error.
///
/// # Safety
///
/// `consumer` must be a valid handle from a consumer constructor.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(std::ptr::null_mut(), box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll_async(
    consumer: *const kafka_consumer_Consumer_t,
    timeout_ms: i64,
    callback: kafka_consumer_Consumer_poll_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `handle_ref` requires a non-null handle created by a consumer constructor,
    // which this function's `# Safety` requires of `consumer` (no null check here). `h` is
    // used only on the submitting thread within this call, for `acquire`, cloning
    // `completion_tx` and spawning on `runtime_handle`.
    let h = unsafe { handle_ref(consumer) };
    let target = PollCallbackTarget { callback, user_data };
    if let Err(e) = acquire(h) {
        // Rejected: deliver the error through the callback inline. The guard
        // was not taken, so nothing to release.
        // SAFETY: `callback` was supplied by the C caller along with `user_data`; this is
        // the documented inline rejection path ("If the guard cannot be acquired, the
        // callback fires inline with the error"), fired exactly once on the calling thread
        // with a null records pointer and a fresh `box_error(e)` handle whose ownership
        // passes to the callback per the `kafka_consumer_Consumer_poll_callback_t` docs.
        // The guard was not taken, so no release is owed, and the function returns without
        // spawning, so the completion path cannot fire a second time.
        unsafe { (target.callback)(std::ptr::null_mut(), box_error(e), target.user_data) };
        return;
    }
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    let tx = h.completion_tx.clone();
    // Capture the `&'static FfiConsumerHandle` (Send+Sync via the unsafe impls),
    // NOT a bare `*mut` (raw pointers are !Send and would make the future
    // !Send). The handle is leaked, so the borrow is effectively `'static`.
    // SAFETY: `handle_ref`'s precondition holds exactly as for `h`: per this function's `#
    // Safety`, `consumer` is a non-null handle from a consumer constructor, leaked by
    // `build_consumer_handle` and freed only by `kafka_consumer_Consumer_destroy`, so the
    // `&'static` is sound while the handle lives. `hs` escapes into the spawned task and
    // the `PollCompletion` job, and is last touched by `release(self.handle)` inside
    // `PollCompletion::fire`, which runs before the C callback. Its keep-alive is not
    // mechanical (`destroy` registers no task to join and `shutdown_background` does not
    // wait): it is the documented `destroy` precondition that the caller must not destroy
    // the consumer while an operation is in flight, i.e. before its completion callback has
    // fired.
    let hs: &'static FfiConsumerHandle = unsafe { handle_ref(consumer) };
    spawn_callback_task(&h.runtime_handle, async move {
        let target = target;
        // SAFETY: `consumer_mut` requires the guard: `acquire(h)` on the calling thread
        // succeeded (the `Err` path fired the callback inline and returned without
        // spawning), and with no `ReleaseGuard` on this path the guard is held for the
        // whole submit->callback window; it is released only by `release(self.handle)` in
        // `PollCompletion::fire` on the dispatcher thread, after `poll().await` has
        // returned and the `&mut dyn Consumer` borrow has ended. `hs` is the leaked handle
        // whose lifetime is covered by the `destroy` in-flight precondition.
        let result = unsafe { consumer_mut(hs).poll(timeout).await };
        // No `.await` after building the raw handles below.
        let (records, error) = match result {
            Ok(r) => (box_records(r), std::ptr::null_mut()),
            Err(e) => (std::ptr::null_mut(), box_error(e)),
        };
        let completion = PollCompletion { target, records, error, handle: hs };
        // SAFETY: `PollCompletion::fire` requires exactly one call on the dispatcher
        // thread: the `FnOnce` job consumes `completion` and is boxed once, and
        // `enqueue_or_run_inline` either hands it to the single per-handle dispatcher
        // thread or, only if that thread has already exited (the task's `tx` clone keeps
        // the channel itself open), runs it inline exactly once on this tokio worker, the
        // post-teardown fallback documented in `common.rs`. The raw pointers are owned
        // handles moved to the dispatcher thread; the C user is responsible for the
        // thread-safety of `user_data`.
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
    error: *mut kafka_common_Error_t,
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
        // SAFETY: `self.target.callback` was supplied by the C caller along with
        // `user_data`; this is the single completion invocation for the poll (the inline
        // rejection path returns without spawning). `self.records` and `self.error` were
        // freshly built by `box_records` / `box_error` on the task, exactly one of them is
        // non-null, and ownership transfers to the callee per the
        // `kafka_consumer_Consumer_poll_callback_t` docs. It fires on the dispatcher thread
        // (or inline on a tokio worker in `enqueue_or_run_inline`'s post-teardown fallback)
        // after `release(self.handle)`, and only reads the already-built result handles,
        // never the consumer. The C user is responsible for the thread-safety of
        // `user_data`, which stays valid until this callback fires per the async-op
        // contract.
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
#[expect(clippy::mut_from_ref)]
unsafe fn consumer_mut(h: &FfiConsumerHandle) -> &mut dyn Consumer<Bytes, Bytes> {
    // SAFETY: Interior-mutability hand-out: per this function's `# Safety` the caller holds
    // the access guard, and `acquire()` guarantees at most one thread/future accesses
    // `*consumer.get()` at any instant (the invariant documented on the `unsafe impl
    // Send/Sync` for `FfiConsumerHandle`), so producing a `&mut ConsumerKind` from the
    // shared `&FfiConsumerHandle` creates no aliasing `&mut`; `UnsafeCell::get` yields a
    // valid, aligned pointer to the live field.
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
    // SAFETY: Per this function's `# Safety`, `records` is a handle from `box_records`,
    // i.e. a `ConsumerRecordsInner` leaked via `Box::into_raw`; it stays allocated until
    // `kafka_consumer_ConsumerRecords_destroy`, and every caller uses the reference only
    // within its own synchronous call, during which the C caller keeps the batch alive.
    unsafe { &*(records as *const ConsumerRecordsInner) }
}

/// Returns the number of records in the batch.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_count(records: *const kafka_consumer_ConsumerRecords_t) -> i32 {
    if records.is_null() {
        return 0;
    }
    // SAFETY: `records` is non-null (checked above) and, per this function's `# Safety`, a
    // valid records handle, i.e. one produced by `box_records` as `records_ref` requires;
    // the reference is used only to read `flat.len()` within this call.
    unsafe { records_ref(records) }.flat.len() as i32
}

/// Returns whether the batch is empty.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_is_empty(
    records: *const kafka_consumer_ConsumerRecords_t,
) -> bool {
    if records.is_null() {
        return true;
    }
    // SAFETY: `records` is non-null (checked above) and, per this function's `# Safety`, a
    // valid records handle produced by `box_records`, as `records_ref` requires; the
    // reference is used only to read `flat.is_empty()` within this call.
    unsafe { records_ref(records) }.flat.is_empty()
}

/// Returns the record at index `index` (borrowed; valid until the records
/// handle is destroyed), or null if out of range.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_get(
    records: *const kafka_consumer_ConsumerRecords_t,
    index: i32,
) -> *const kafka_consumer_ConsumerRecord_t {
    if records.is_null() || index < 0 {
        return std::ptr::null();
    }
    // SAFETY: `records` is non-null (checked above) and, per this function's `# Safety`, a
    // valid records handle produced by `box_records`, as `records_ref` requires. `index` is
    // non-negative (checked above) and bounds-checked by `get`; the returned record pointer
    // borrows into `inner.records`, which is boxed and never moved, so it stays valid until
    // the batch is destroyed, exactly as documented.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_destroy(records: *mut kafka_consumer_ConsumerRecords_t) {
    if !records.is_null() {
        // SAFETY: `records` is non-null (checked above) and, per this function's `#
        // Safety`, a handle produced by `Box::into_raw` in `box_records` that becomes
        // invalid, together with every record pointer borrowed from it, after this call;
        // those borrowed record pointers are the only other references into the allocation,
        // so reclaiming the `Box` here is the single, final use.
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
    // SAFETY: Per this function's `# Safety`, `record` was obtained from
    // `kafka_consumer_ConsumerRecords_get`, i.e. it is an entry of
    // `ConsumerRecordsInner::flat` pointing into the boxed, never-moved `records` batch; it
    // stays valid until `kafka_consumer_ConsumerRecords_destroy`, which the C caller must
    // not call while still using the record, and every getter uses the reference only
    // within its own call.
    unsafe { &*(record as *const ConsumerRecord<Bytes, Bytes>) }
}

/// Returns the partition of a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_partition(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`; this function's `# Safety` requires `record` to
    // be such a valid record pointer (no null check, so null is a contract violation). The
    // borrow lasts only for this call, during which the owning batch is kept alive by the C
    // caller.
    unsafe { record_ref(record) }.partition()
}

/// Returns the offset of a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_offset(record: *const kafka_consumer_ConsumerRecord_t) -> i64 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`; this function's `# Safety` requires `record` to
    // be such a valid record pointer (no null check). The borrow is used only to read
    // `offset()` within this call, during which the owning batch is kept alive by the C
    // caller.
    unsafe { record_ref(record) }.offset()
}

/// Returns the timestamp of a record (milliseconds since epoch, or
/// `NO_TIMESTAMP` = -1).
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i64 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`; this function's `# Safety` requires `record` to
    // be such a valid record pointer (no null check). The borrow is used only to read
    // `timestamp()` within this call, during which the owning batch is kept alive by the C
    // caller.
    unsafe { record_ref(record) }.timestamp()
}

/// Returns the topic name of a record as a (ptr, len) pair (NOT
/// NUL-terminated). `out_len` receives the byte length. The pointer borrows
/// into the batch and is valid until the records handle is destroyed.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` must be a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_topic(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const c_char {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`, which this function's `# Safety` requires of
    // `record` (no null check). The returned topic pointer borrows into the batch, which
    // stays alive until the C caller destroys the records handle, as the rustdoc documents.
    let rec = unsafe { record_ref(record) };
    let topic = rec.topic();
    if !out_len.is_null() {
        // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
        // Safety`, a valid pointer; exactly one `i32` (the topic byte length) is written.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_key(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const u8 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`, which this function's `# Safety` requires of
    // `record` (no null check). The returned key pointer borrows into the batch (zero-copy)
    // and stays valid until the records handle is destroyed, as documented.
    let rec = unsafe { record_ref(record) };
    match rec.key() {
        Some(k) => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (the key byte length) is
                // written.
                unsafe { *out_len = k.len() as i32 };
            }
            k.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (`-1`, the documented
                // absent-key marker) is written.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_value(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const u8 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`, which this function's `# Safety` requires of
    // `record` (no null check). The returned value pointer borrows into the batch
    // (zero-copy) and stays valid until the records handle is destroyed, as documented.
    let rec = unsafe { record_ref(record) };
    match rec.value() {
        Some(v) => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (the value byte length) is
                // written.
                unsafe { *out_len = v.len() as i32 };
            }
            v.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (`-1`, the documented
                // absent-value marker) is written.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp_type(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`; this function's `# Safety` requires `record` to
    // be such a valid record pointer (no null check). The borrow is used only to read
    // `timestamp_type()` within this call, during which the owning batch is kept alive by
    // the C caller.
    unsafe { record_ref(record) }.timestamp_type().id()
}

/// Returns the serialized key size in bytes, or `-1` if the key is null.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_key_size(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`; this function's `# Safety` requires `record` to
    // be such a valid record pointer (no null check). The borrow is used only to read
    // `serialized_key_size()` within this call, during which the owning batch is kept alive
    // by the C caller.
    unsafe { record_ref(record) }.serialized_key_size()
}

/// Returns the serialized value size in bytes, or `-1` if the value is null.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_value_size(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`; this function's `# Safety` requires `record` to
    // be such a valid record pointer (no null check). The borrow is used only to read
    // `serialized_value_size()` within this call, during which the owning batch is kept
    // alive by the C caller.
    unsafe { record_ref(record) }.serialized_value_size()
}

/// Returns the leader epoch of a record. Writes the epoch to `*out_epoch` and
/// returns `true` if present; returns `false` (leaving `*out_epoch` untouched)
/// if absent.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_epoch` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_leader_epoch(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_epoch: *mut i32,
) -> bool {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`, which this function's `# Safety` requires of
    // `record` (no null check); the borrow is used only to read `leader_epoch()` within
    // this call, during which the owning batch is kept alive by the C caller.
    match unsafe { record_ref(record) }.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                // SAFETY: `out_epoch` is non-null (checked above) and, per this function's
                // `# Safety`, a valid pointer; exactly one `i32` is written, and only on
                // the `Some` path, leaving `*out_epoch` untouched otherwise as documented.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_delivery_count(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_count: *mut i32,
) -> bool {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`, which this function's `# Safety` requires of
    // `record` (no null check); the borrow is used only to read `delivery_count()` within
    // this call, during which the owning batch is kept alive by the C caller.
    match unsafe { record_ref(record) }.delivery_count() {
        Some(count) => {
            if !out_count.is_null() {
                // SAFETY: `out_count` is non-null (checked above) and, per this function's
                // `# Safety`, a valid pointer; exactly one `i32` is written, and only when
                // a delivery count is present.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_count(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`; this function's `# Safety` requires `record` to
    // be such a valid record pointer (no null check). The borrow is used only to count
    // `headers()` within this call, during which the owning batch is kept alive by the C
    // caller.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_key(
    record: *const kafka_consumer_ConsumerRecord_t,
    index: i32,
    out_len: *mut i32,
) -> *const c_char {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`, which this function's `# Safety` requires of
    // `record` (no null check). The returned header-key pointer borrows into the record and
    // therefore into the batch, valid until the records handle is destroyed, as documented;
    // `index` is range-checked by `header_at`.
    let rec = unsafe { record_ref(record) };
    match header_at(rec, index) {
        Some(header) => {
            let key = header.key();
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (the header key byte length)
                // is written.
                unsafe { *out_len = key.len() as i32 };
            }
            key.as_ptr() as *const c_char
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (`-1`, the documented
                // out-of-range marker) is written.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_value(
    record: *const kafka_consumer_ConsumerRecord_t,
    index: i32,
    out_len: *mut i32,
) -> *const u8 {
    // SAFETY: `record_ref` requires a pointer obtained from
    // `kafka_consumer_ConsumerRecords_get`, which this function's `# Safety` requires of
    // `record` (no null check). The returned header-value pointer borrows into the record
    // and therefore into the batch (zero-copy), valid until the records handle is
    // destroyed, as documented; `index` is range-checked by `header_at`.
    let rec = unsafe { record_ref(record) };
    match header_at(rec, index).and_then(|h| h.value()) {
        Some(value) => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (the header value byte
                // length) is written.
                unsafe { *out_len = value.len() as i32 };
            }
            value.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (`-1`, the documented
                // out-of-range / null-value marker) is written.
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
#[expect(clippy::mut_from_ref)]
unsafe fn mock_mut(h: &FfiConsumerHandle) -> Result<&mut MockConsumer<Bytes, Bytes>, Error> {
    // SAFETY: Interior-mutability hand-out with the same invariant as `consumer_mut`: per
    // this function's `# Safety` the caller holds the access guard, and `acquire()`
    // guarantees at most one thread/future accesses `*consumer.get()` at any instant
    // (documented on the `unsafe impl Send/Sync` for `FfiConsumerHandle`), so the `&mut
    // ConsumerKind` produced from `UnsafeCell::get` is unaliased; the `Async` arm hands out
    // nothing and returns an error instead.
    match unsafe { &mut *h.consumer.get() } {
        ConsumerKind::Mock(c) => Ok(c.as_mut()),
        ConsumerKind::Async(_) => Err(Error::local_illegal_state("operation is only supported on a MockConsumer")),
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's `# Safety` requires of `consumer` ("`consumer` must be a valid
    // handle"; no null check here). The reference is used only within this synchronous
    // call, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let n = count.max(0) as usize;
    let mut tps = Vec::with_capacity(n);
    for i in 0..n {
        // SAFETY: Per this function's `# Safety`, `topics` points to `count` valid C-string
        // pointers; the loop bound is `n = count.max(0)`, so a negative `count` reads
        // nothing and every index `i < n` is within the promised entries.
        let topic_ptr = unsafe { *topics.add(i) };
        // SAFETY: `topic_ptr` is the `i`-th entry of `topics`, which per this function's `#
        // Safety` is a valid NUL-terminated C string for the duration of the call (a NULL
        // entry is not tolerated by the contract and is not checked here); the bytes are
        // copied into an owned `String` immediately.
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        // SAFETY: Per this function's `# Safety`, `partitions` points to `count` `i32`
        // values and `i < count.max(0)`, so the read is within bounds; a negative `count`
        // yields no reads.
        let partition = unsafe { *partitions.add(i) };
        tps.push(crate::common::TopicPartition::new(topic, partition));
    }
    // SAFETY: `consumer_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early with the error boxed) and `_g: ReleaseGuard` holds it until this
    // function returns, so the `&mut dyn Consumer` is exclusive for the `block_on(assign)`
    // window and the borrow ends before `_g` drops, per the `UnsafeCell` + `acquire`
    // invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
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
/// `consumer` must be a valid handle; `topic` must be a valid C string;
/// `key`/`value` valid for `key_len`/`value_len` bytes (or null if the length is
/// negative).
#[ffi_guard]
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
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null handle created by a consumer constructor.
    // This function's `# Safety` does not state that requirement for `consumer` (it covers
    // only `topic`, `key` and `value`), so the block relies on the caller passing a live
    // mock-consumer handle, as every sibling `kafka_consumer_MockConsumer_*` driver
    // documents for its own `consumer` parameter; the reference is used only within this
    // synchronous call.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call (no null check; null is a contract violation); it is
    // copied into an owned `String` immediately.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let key_vec: Option<Bytes> = if key_len < 0 || key.is_null() {
        None
    } else {
        // SAFETY: On this branch `key_len >= 0` and `key` is non-null (both checked above),
        // and per this function's `# Safety` `key` is valid for `key_len` bytes; the slice
        // is copied into an owned `Bytes` before the call returns, so it never outlives the
        // caller's buffer.
        Some(Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(key, key_len as usize)
        }))
    };
    let value_vec: Option<Bytes> = if value_len < 0 || value.is_null() {
        None
    } else {
        // SAFETY: On this branch `value_len >= 0` and `value` is non-null (both checked
        // above), and per this function's `# Safety` `value` is valid for `value_len`
        // bytes; the slice is copied into an owned `Bytes` before the call returns, so it
        // never outlives the caller's buffer.
        Some(Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(value, value_len as usize)
        }))
    };
    // SAFETY: `mock_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns, so the
    // `&mut MockConsumer` is exclusive for the remainder of this synchronous call, per the
    // `acquire` invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_end_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's `# Safety` requires of `consumer` ("`consumer` a valid handle"; no
    // null check here). The reference is used only within this synchronous call, during
    // which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call (no null check); it is copied into an owned `String`
    // immediately.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    // SAFETY: `mock_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns, so the
    // `&mut MockConsumer` is exclusive for the rest of this synchronous call, per the
    // `acquire` invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_beginning_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's `# Safety` requires of `consumer` ("`consumer` a valid handle"; no
    // null check here). The reference is used only within this synchronous call, during
    // which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call (no null check); it is copied into an owned `String`
    // immediately.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    // SAFETY: `mock_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns, so the
    // `&mut MockConsumer` is exclusive for the rest of this synchronous call, per the
    // `acquire` invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_partitions(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition_count: i32,
    leader_id: i32,
    leader_host: *const c_char,
    leader_port: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's `# Safety` requires of `consumer` ("`consumer` a valid handle"; no
    // null check here). The reference is used only within this synchronous call, during
    // which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call (no null check); it is copied into an owned `String`
    // immediately.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    // SAFETY: Per this function's `# Safety`, `leader_host` is a valid NUL-terminated C
    // string for the duration of the call (no null check); it is copied into an owned
    // `String` immediately.
    let host = unsafe { CStr::from_ptr(leader_host) }.to_string_lossy().to_string();
    // SAFETY: `mock_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns, so the
    // `&mut MockConsumer` is exclusive for the rest of this synchronous call, per the
    // `acquire` invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_poll_error(
    consumer: *const kafka_consumer_Consumer_t,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's `# Safety` requires of `consumer` ("`consumer` a valid handle"; no
    // null check here). The reference is used only within this synchronous call, during
    // which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated C string
    // for the duration of the call (no null check); it is copied into an owned `String`
    // immediately.
    let msg = unsafe { CStr::from_ptr(message) }.to_string_lossy().to_string();
    // SAFETY: `mock_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns, so the
    // `&mut MockConsumer` is exclusive for the rest of this synchronous call, per the
    // `acquire` invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    mock.set_poll_error(Error::local_illegal_state(msg));
    std::ptr::null_mut()
}

/// Simulates a rebalance on a mock consumer (mock only), mirroring Java's
/// `MockConsumer.rebalance(Collection<TopicPartition>)`.
///
/// `topics` and `partitions` are parallel arrays of length `count` describing the
/// **new full assignment**. The revoked and added partitions are computed against
/// the current assignment, the registered
/// [`kafka_consumer_ConsumerRebalanceListener_t`] callbacks are invoked
/// (`on_partitions_revoked` with the removed partitions if any were removed, then
/// `on_partitions_assigned` with the *added* partitions — which fires even when
/// nothing was added), and the assignment is replaced. `on_partitions_lost` is
/// never fired by the mock, matching Java.
///
/// The consumer must have a topic subscription (Java throws
/// `IllegalArgumentException` when a manual assignment is in use). Buffered
/// records are cleared, as in Java.
///
/// This call does not return until the listener callbacks have returned, and an
/// error returned by a callback is propagated as this function's return value.
///
/// Returns null on success, or a non-null error handle on failure (including
/// `illegal_state` when `consumer` wraps an async consumer).
///
/// # Safety
///
/// `topics` must point to `count` valid C strings and `partitions` to `count`
/// `i32` values; `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_rebalance(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's `# Safety` requires of `consumer` ("`consumer` must be a valid
    // handle"; no null check here). The reference is used only within this synchronous
    // call, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, and tolerates a non-positive `count` (empty
    // result); this function's `# Safety` promises exactly those two arrays for the
    // duration of the call. NULL entries are not tolerated by either contract and are not
    // checked.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `mock_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns, covering
    // the `block_on(mock.rebalance(..))` window as well. The listener callbacks that
    // `rebalance` invokes run inline on this thread while the guard is held, and any
    // re-entrant call on the same handle is rejected by the non-reentrant guard with
    // `LocalConcurrentModification` rather than aliasing this `&mut`, per the `acquire`
    // invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
    let mock = match unsafe { mock_mut(h) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    // The core `rebalance` is `async fn` (the listener methods are async),
    // unlike the other mock driver methods, so it needs the runtime.
    match h.runtime.block_on(mock.rebalance(&tps)) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
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
pub(crate) unsafe fn read_topic_partitions(
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> Vec<TopicPartition> {
    let n = count.max(0) as usize;
    let mut tps = Vec::with_capacity(n);
    for i in 0..n {
        // SAFETY: Per this function's `# Safety`, `topics` points to `count` valid C-string
        // pointers; the loop bound is `n = count.max(0)`, so each index `i < n` is within
        // the promised entries and a non-positive `count` reads nothing, as the contract
        // documents.
        let topic_ptr = unsafe { *topics.add(i) };
        // SAFETY: `topic_ptr` is the `i`-th entry of `topics`, which per this function's `#
        // Safety` is a valid NUL-terminated C string for the duration of the call (a NULL
        // entry is a contract violation and is deliberately not checked here); the bytes
        // are copied into an owned `String` immediately.
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        // SAFETY: Per this function's `# Safety`, `partitions` points to `count` `i32`
        // values and `i < count.max(0)`, so the read is within bounds; a non-positive
        // `count` yields no reads.
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
        // SAFETY: Per this function's `# Safety`, `topics` points to `count` valid C-string
        // pointers; the loop bound is `n = count.max(0)`, so each index `i < n` is within
        // the promised entries and a non-positive `count` reads nothing.
        let topic_ptr = unsafe { *topics.add(i) };
        // SAFETY: `topic_ptr` is the `i`-th entry of `topics`, which per this function's `#
        // Safety` is a valid NUL-terminated C string for the duration of the call (NULL
        // entries are not tolerated by the contract and are not checked); it is copied into
        // an owned `String` immediately.
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
pub struct kafka_common_TopicPartition_t {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_topic(tp: *const kafka_common_TopicPartition_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `tp` is a valid topic-partition handle:
    // either an owned `TopicPartitionInner` leaked by `Box::into_raw` in
    // `box_topic_partition`, or a borrowed entry of a map/list handle's
    // `Vec<TopicPartitionInner>` as returned by `kafka_consumer_OffsetMap_get_key`,
    // `kafka_consumer_OffsetAndTimestampMap_get_key`,
    // `kafka_consumer_LongOffsetMap_get_key` or `kafka_common_TopicPartitionList_get`,
    // valid until its owner is destroyed. No null check; the borrow lasts only for this
    // call, and the returned `topic_c` pointer is owned by the handle and valid as long as
    // it is, as documented.
    let inner = unsafe { &*(tp as *const TopicPartitionInner) };
    inner.topic_c.as_ptr()
}

/// Returns the partition of a topic-partition handle.
///
/// # Safety
///
/// `tp` must be a valid topic-partition handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_partition(tp: *const kafka_common_TopicPartition_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `tp` is a valid topic-partition handle (an
    // owned `TopicPartitionInner` from `box_topic_partition`, or a borrowed `Vec` entry
    // handed out by a map/list `get_key` / `get` getter, valid until its owner is
    // destroyed); no null check, and the borrow is used only to read `partition()` within
    // this call.
    let inner = unsafe { &*(tp as *const TopicPartitionInner) };
    inner.tp.partition()
}

/// Destroys a topic-partition handle. Safe with null (no-op).
///
/// # Safety
///
/// `tp` must be null or a valid topic-partition handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_destroy(tp: *mut kafka_common_TopicPartition_t) {
    if !tp.is_null() {
        // SAFETY: `tp` is non-null (checked above) and, per this function's `# Safety`, a
        // valid topic-partition handle that must not be used after this call. Reclaiming
        // the `Box` is sound for the owned handles produced by `Box::into_raw` in
        // `box_topic_partition` (reached via
        // `kafka_common_RecordDeserializationError_partition`, whose docs direct the caller
        // to this destroy), which makes this the single, final use. The borrowed `*const`
        // keys handed out by the map/list getters are `Vec` entries freed with their owner
        // and must never be passed here; the contract's "valid topic-partition handle"
        // wording leaves that distinction implicit.
        unsafe { drop(Box::from_raw(tp as *mut TopicPartitionInner)) };
    }
}

/// Boxes a single [`TopicPartition`] into an owned handle, mirroring the
/// per-entry construction used by the map/list handles below. Reused by
/// `kafka_common_RecordDeserializationError_partition`
/// (`src/ffi/common.rs`) since `RecordDeserializationError` carries a single
/// non-optional `TopicPartition` rather than a collection.
pub(crate) fn box_topic_partition(tp: TopicPartition) -> *mut kafka_common_TopicPartition_t {
    let topic_c = std::ffi::CString::new(tp.topic().as_bytes()).unwrap_or_default();
    Box::into_raw(Box::new(TopicPartitionInner { tp, topic_c })) as *mut kafka_common_TopicPartition_t
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_offset(
    oam: *const kafka_consumer_OffsetAndMetadata_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `oam` is a valid offset-and-metadata handle;
    // the only producer of such a pointer is `kafka_consumer_OffsetMap_get_value`, which
    // returns a borrowed `*const OffsetAndMetadataInner` into the owning
    // `OffsetMapInner::values` built in `box_offset_map`, valid until that map is
    // destroyed. No null check; the borrow is used only to read `offset()` within this
    // call.
    let inner = unsafe { &*(oam as *const OffsetAndMetadataInner) };
    inner.oam.offset()
}

/// Returns the commit metadata as a NUL-terminated C string (empty string if
/// none; owned by the handle).
///
/// # Safety
///
/// `oam` must be a valid offset-and-metadata handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_metadata(
    oam: *const kafka_consumer_OffsetAndMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `oam` is a valid offset-and-metadata handle,
    // i.e. a borrowed `*const OffsetAndMetadataInner` from
    // `kafka_consumer_OffsetMap_get_value` into the owning map's `values` vec (built in
    // `box_offset_map`), valid until that map is destroyed. No null check; the returned
    // `metadata_c` pointer is owned by that entry and stays valid as long as the owning map
    // does, as documented.
    let inner = unsafe { &*(oam as *const OffsetAndMetadataInner) };
    inner.metadata_c.as_ptr()
}

/// Returns the leader epoch via `*out_epoch`, or `false` if absent.
///
/// # Safety
///
/// `oam` must be a valid handle; `out_epoch` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_leader_epoch(
    oam: *const kafka_consumer_OffsetAndMetadata_t,
    out_epoch: *mut i32,
) -> bool {
    // SAFETY: Per this function's `# Safety`, `oam` is a valid offset-and-metadata handle,
    // i.e. a borrowed `*const OffsetAndMetadataInner` from
    // `kafka_consumer_OffsetMap_get_value` into the owning map's `values` vec, valid until
    // that map is destroyed. No null check; the borrow is used only to read
    // `leader_epoch()` within this call.
    let inner = unsafe { &*(oam as *const OffsetAndMetadataInner) };
    match inner.oam.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                // SAFETY: `out_epoch` is non-null (checked above) and, per this function's
                // `# Safety`, a valid pointer; exactly one `i32` is written, and only on
                // the `Some` path.
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
/// `oam` must be null or an owned offset-and-metadata handle. The pointers
/// [`kafka_consumer_OffsetMap_get_value`] returns are borrowed from the map and
/// must not be passed here.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_destroy(oam: *mut kafka_consumer_OffsetAndMetadata_t) {
    if !oam.is_null() {
        // SAFETY: `oam` is non-null (checked above) and, per this function's `# Safety`, a
        // valid handle that becomes invalid after this call, which would make this the
        // single, final use. However, no FFI function produces an owned (`Box::into_raw`)
        // `kafka_consumer_OffsetAndMetadata_t`: `OffsetAndMetadataInner` is constructed
        // only inside `box_offset_map`, and the only pointer of this type a C caller can
        // obtain is the borrowed `*const` entry from `kafka_consumer_OffsetMap_get_value`,
        // which is freed with its map and must not be reclaimed by `Box::from_raw`.
        // Soundness therefore relies on no caller ever holding a pointer that may
        // legitimately be passed here.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_offset(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `oat` is a valid offset-and-timestamp handle;
    // the only producer of such a pointer is
    // `kafka_consumer_OffsetAndTimestampMap_get_value`, which returns a borrowed `*const
    // OffsetAndTimestamp` into the owning map's `values` vec built in
    // `box_offset_and_timestamp_map`, valid until that map is destroyed. No null check; the
    // borrow is used only to read `offset()` within this call.
    unsafe { &*(oat as *const OffsetAndTimestamp) }.offset()
}

/// Returns the timestamp (milliseconds since epoch).
///
/// # Safety
///
/// `oat` must be a valid offset-and-timestamp handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_timestamp(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `oat` is a valid offset-and-timestamp handle,
    // i.e. a borrowed `*const OffsetAndTimestamp` from
    // `kafka_consumer_OffsetAndTimestampMap_get_value` into the owning map's `values` vec,
    // valid until that map is destroyed. No null check; the borrow is used only to read
    // `timestamp()` within this call.
    unsafe { &*(oat as *const OffsetAndTimestamp) }.timestamp()
}

/// Returns the leader epoch via `*out_epoch`, or `false` if absent.
///
/// # Safety
///
/// `oat` must be a valid handle; `out_epoch` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_leader_epoch(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
    out_epoch: *mut i32,
) -> bool {
    // SAFETY: Per this function's `# Safety`, `oat` is a valid offset-and-timestamp handle,
    // i.e. a borrowed `*const OffsetAndTimestamp` from
    // `kafka_consumer_OffsetAndTimestampMap_get_value` into the owning map's `values` vec,
    // valid until that map is destroyed. No null check; the borrow is used only to read
    // `leader_epoch()` within this call.
    match unsafe { &*(oat as *const OffsetAndTimestamp) }.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                // SAFETY: `out_epoch` is non-null (checked above) and, per this function's
                // `# Safety`, a valid pointer; exactly one `i32` is written, and only on
                // the `Some` path.
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
/// `oat` must be null or an owned offset-and-timestamp handle. The pointers
/// [`kafka_consumer_OffsetAndTimestampMap_get_value`] returns are borrowed from
/// the map and must not be passed here.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_destroy(oat: *mut kafka_consumer_OffsetAndTimestamp_t) {
    if !oat.is_null() {
        // SAFETY: `oat` is non-null (checked above) and, per this function's `# Safety`, a
        // valid handle that becomes invalid after this call, which would make this the
        // single, final use. However, no FFI function produces an owned (`Box::into_raw`)
        // `kafka_consumer_OffsetAndTimestamp_t`: the only pointer of this type a C caller
        // can obtain is the borrowed `*const OffsetAndTimestamp` entry from
        // `kafka_consumer_OffsetAndTimestampMap_get_value`, which lives in the owning map's
        // `values` vec and must not be reclaimed by `Box::from_raw`. Soundness therefore
        // relies on no caller ever holding a pointer that may legitimately be passed here.
        unsafe { drop(Box::from_raw(oat as *mut OffsetAndTimestamp)) };
    }
}

/// Opaque handle to a [`ConsumerGroupMetadata`].
///
/// Obtained only from [`kafka_consumer_Consumer_group_metadata`]: Java has
/// deprecated the public `ConsumerGroupMetadata` constructors since 4.2, and
/// the class becomes an interface in Kafka 5.0, so the C API does not expose
/// a constructor either.
#[repr(C)]
pub struct kafka_consumer_ConsumerGroupMetadata_t {
    _private: [u8; 0],
}

/// Cached group-metadata handle: shares the consumer's metadata and owns
/// NUL-terminated copies of its strings for the getters.
struct ConsumerGroupMetadataInner {
    meta: Arc<dyn ConsumerGroupMetadata>,
    group_id_c: std::ffi::CString,
    member_id_c: std::ffi::CString,
    group_instance_id_c: Option<std::ffi::CString>,
}

fn box_group_metadata(meta: Arc<dyn ConsumerGroupMetadata>) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `meta` is a valid group-metadata handle, i.e.
    // a `ConsumerGroupMetadataInner` leaked via `Box::into_raw` in `box_group_metadata`
    // (reached only through `kafka_consumer_Consumer_group_metadata`) and alive until
    // `kafka_consumer_ConsumerGroupMetadata_destroy`; no null check. The returned
    // `group_id_c` pointer is owned by the handle and valid as long as it is, as
    // documented.
    unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.group_id_c.as_ptr()
}

/// Returns the generation id.
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_generation_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `meta` is a valid group-metadata handle from
    // `box_group_metadata` (`Box::into_raw`), alive until
    // `kafka_consumer_ConsumerGroupMetadata_destroy`; no null check, and the borrow is used
    // only to read `generation_id()` within this call.
    unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.meta.generation_id()
}

/// Returns the member id as a NUL-terminated C string (owned by the handle).
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_member_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `meta` is a valid group-metadata handle from
    // `box_group_metadata` (`Box::into_raw`), alive until
    // `kafka_consumer_ConsumerGroupMetadata_destroy`; no null check. The returned
    // `member_id_c` pointer is owned by the handle and valid as long as it is, as
    // documented.
    unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.member_id_c.as_ptr()
}

/// Returns the group instance id as a NUL-terminated C string, or null if
/// absent (owned by the handle).
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_instance_id(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `meta` is a valid group-metadata handle from
    // `box_group_metadata` (`Box::into_raw`), alive until
    // `kafka_consumer_ConsumerGroupMetadata_destroy`; no null check. The returned
    // `group_instance_id_c` pointer (or null when absent) is owned by the handle and valid
    // as long as it is, as documented.
    match &unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.group_instance_id_c {
        Some(c) => c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Borrows the [`ConsumerGroupMetadata`] inside a group-metadata handle.
///
/// Shared with the producer FFI so
/// `kafka_producer_Producer_send_offsets_to_transaction` can take the very
/// handle a consumer produced, mirroring Java's
/// `producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())`.
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
pub(crate) unsafe fn group_metadata_ref(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> &'static Arc<dyn ConsumerGroupMetadata> {
    // SAFETY: Per this function's `# Safety`, `meta` is a valid group-metadata handle from
    // `box_group_metadata` (`Box::into_raw`), alive until
    // `kafka_consumer_ConsumerGroupMetadata_destroy`. The `&'static Arc` is handed to the
    // producer FFI, which either dereferences it within its own synchronous call or
    // `Arc::clone`s it for its async path, so nothing holds the reference itself beyond the
    // call during which the C caller keeps the metadata handle alive.
    &unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.meta
}

/// Destroys a group-metadata handle. Safe with null (no-op).
///
/// # Safety
///
/// `meta` must be null or a valid group-metadata handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_destroy(
    meta: *mut kafka_consumer_ConsumerGroupMetadata_t,
) {
    if !meta.is_null() {
        // SAFETY: `meta` is non-null (checked above) and, per this function's `# Safety`, a
        // handle produced by `Box::into_raw` in `box_group_metadata` (the only producer of
        // `kafka_consumer_ConsumerGroupMetadata_t`, via
        // `kafka_consumer_Consumer_group_metadata`) that becomes invalid after this call,
        // so reclaiming the `Box` is the single, final use; the inner `Arc<dyn
        // ConsumerGroupMetadata>` is dropped with it.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_id(node: *const kafka_common_Node_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `node` is a valid node handle obtained from a
    // `PartitionInfo` getter, i.e. a borrowed `*const Node` pointing into the
    // `PartitionInfo` owned by a `PartitionInfoInner` (its leader, replicas, in-sync or
    // offline replicas), valid until the owning partition-info list or map is destroyed,
    // which the C caller must not do while using the node. No null check; the borrow is
    // used only to read `id()` within this call.
    unsafe { &*(node as *const Node) }.id()
}

/// Returns the node host as a (ptr, len) pair (NOT NUL-terminated; borrows into
/// the owning `PartitionInfo`). `out_len` receives the byte length.
///
/// # Safety
///
/// `node` must be a valid node handle; `out_len` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_host(node: *const kafka_common_Node_t, out_len: *mut i32) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `node` is a valid node handle, i.e. a
    // borrowed `*const Node` from a `PartitionInfo` getter pointing into a
    // `PartitionInfoInner`-owned `PartitionInfo`, valid until the owning list or map is
    // destroyed. No null check; the returned host pointer borrows into that same
    // `PartitionInfo`, as documented.
    let host = unsafe { &*(node as *const Node) }.host();
    if !out_len.is_null() {
        // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
        // Safety`, a valid pointer; exactly one `i32` (the host byte length) is written.
        unsafe { *out_len = host.len() as i32 };
    }
    host.as_ptr() as *const c_char
}

/// Returns the node port.
///
/// # Safety
///
/// `node` must be a valid node handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_port(node: *const kafka_common_Node_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `node` is a valid node handle, i.e. a
    // borrowed `*const Node` from a `PartitionInfo` getter pointing into a
    // `PartitionInfoInner`-owned `PartitionInfo`, valid until the owning list or map is
    // destroyed. No null check; the borrow is used only to read `port()` within this call.
    unsafe { &*(node as *const Node) }.port()
}

/// Returns the node rack as a (ptr, len) pair, or (null, -1) if no rack.
///
/// # Safety
///
/// `node` must be a valid node handle; `out_len` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_rack(node: *const kafka_common_Node_t, out_len: *mut i32) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `node` is a valid node handle, i.e. a
    // borrowed `*const Node` from a `PartitionInfo` getter pointing into a
    // `PartitionInfoInner`-owned `PartitionInfo`, valid until the owning list or map is
    // destroyed. No null check; the returned rack pointer borrows into that same
    // `PartitionInfo`.
    match unsafe { &*(node as *const Node) }.rack() {
        Some(rack) => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (the rack byte length) is
                // written.
                unsafe { *out_len = rack.len() as i32 };
            }
            rack.as_ptr() as *const c_char
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer; exactly one `i32` (`-1`, the documented no-rack
                // marker) is written.
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns whether the node is fenced — Java's `Node.isFenced()`.
///
/// Only `describeCluster` with `includeFencedBrokers(true)` (KIP-1073) can
/// return a fenced broker, so every node obtained any other way answers `false`.
///
/// # Safety
///
/// `node` must be a valid node handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_is_fenced(node: *const kafka_common_Node_t) -> bool {
    // SAFETY: Per this function's `# Safety`, `node` is a valid node handle, i.e. a pointer
    // to a live `Node` this library handed out (owned or borrowed from a live result); the
    // reference is used only to read one flag during this call.
    unsafe { &*(node as *const Node) }.is_fenced()
}

/// Opaque handle to a [`PartitionInfo`].
#[repr(C)]
pub struct kafka_common_PartitionInfo_t {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_topic(info: *const kafka_common_PartitionInfo_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle.
    // `PartitionInfoInner` is constructed only inside `box_partition_info_list` and
    // `box_topic_partition_info_map`, and the only way C obtains such a pointer is the
    // borrowed `*const` entry from `kafka_common_PartitionInfoList_get`, valid until the
    // owning list (or the map owning that list) is destroyed. No null check; the returned
    // `topic_c` pointer is owned by that entry and valid as long as its owner is, as
    // documented.
    unsafe { &*(info as *const PartitionInfoInner) }.topic_c.as_ptr()
}

/// Returns the partition number.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_partition(info: *const kafka_common_PartitionInfo_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec (built in `box_partition_info_list` /
    // `box_topic_partition_info_map`), valid until its owner is destroyed. No null check;
    // the borrow is used only to read `partition()` within this call.
    unsafe { &*(info as *const PartitionInfoInner) }.info.partition()
}

/// Returns the leader node (borrowed; valid until the handle is destroyed), or
/// null if the partition has no leader.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_leader(
    info: *const kafka_common_PartitionInfo_t,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec, valid until its owner is destroyed. No null
    // check; the returned leader `Node` pointer borrows into `info`'s owned `PartitionInfo`
    // and is valid until the owner is destroyed, as documented.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_replica_count(info: *const kafka_common_PartitionInfo_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec, valid until its owner is destroyed. No null
    // check; the borrow is used only to read `replicas().len()` within this call.
    unsafe { &*(info as *const PartitionInfoInner) }.info.replicas().len() as i32
}

/// Returns the replica node at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_replica(
    info: *const kafka_common_PartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec, valid until its owner is destroyed. No null
    // check; `index` is range-checked by `node_at`, and the returned `Node` pointer borrows
    // into `info`'s owned `PartitionInfo`, valid until the owner is destroyed, as
    // documented.
    node_at(unsafe { &*(info as *const PartitionInfoInner) }.info.replicas(), index)
}

/// Returns the number of in-sync replica nodes.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_in_sync_replica_count(
    info: *const kafka_common_PartitionInfo_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec, valid until its owner is destroyed. No null
    // check; the borrow is used only to read `in_sync_replicas().len()` within this call.
    unsafe { &*(info as *const PartitionInfoInner) }.info.in_sync_replicas().len() as i32
}

/// Returns the in-sync replica node at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_in_sync_replica(
    info: *const kafka_common_PartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec, valid until its owner is destroyed. No null
    // check; `index` is range-checked by `node_at`, and the returned `Node` pointer borrows
    // into `info`'s owned `PartitionInfo`, valid until the owner is destroyed, as
    // documented.
    node_at(unsafe { &*(info as *const PartitionInfoInner) }.info.in_sync_replicas(), index)
}

/// Returns the number of offline replica nodes.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_offline_replica_count(
    info: *const kafka_common_PartitionInfo_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec, valid until its owner is destroyed. No null
    // check; the borrow is used only to read `offline_replicas().len()` within this call.
    unsafe { &*(info as *const PartitionInfoInner) }.info.offline_replicas().len() as i32
}

/// Returns the offline replica node at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_offline_replica(
    info: *const kafka_common_PartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle, i.e.
    // a borrowed `*const PartitionInfoInner` from `kafka_common_PartitionInfoList_get` into
    // a `PartitionInfoListInner::items` vec, valid until its owner is destroyed. No null
    // check; `index` is range-checked by `node_at`, and the returned `Node` pointer borrows
    // into `info`'s owned `PartitionInfo`, valid until the owner is destroyed, as
    // documented.
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
/// `info` must be null or an owned partition-info handle. The pointers
/// [`kafka_common_PartitionInfoList_get`] returns are borrowed from the list and
/// must not be passed here.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_destroy(info: *mut kafka_common_PartitionInfo_t) {
    if !info.is_null() {
        // SAFETY: `info` is non-null (checked above) and, per this function's `# Safety`, a
        // valid handle that becomes invalid after this call, which would make this the
        // single, final use. However, no FFI function produces an owned (`Box::into_raw`)
        // `kafka_common_PartitionInfo_t`: `PartitionInfoInner` values live only inside
        // `PartitionInfoListInner::items` (built in `box_partition_info_list` /
        // `box_topic_partition_info_map`), and the only pointer of this type a C caller can
        // obtain is the borrowed `*const` from `kafka_common_PartitionInfoList_get`, which
        // is freed with its list and must not be reclaimed by `Box::from_raw`. Soundness
        // therefore relies on no caller ever holding a pointer that may legitimately be
        // passed here.
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

pub(crate) fn box_offset_map(map: HashMap<TopicPartition, OffsetAndMetadata>) -> *mut kafka_consumer_OffsetMap_t {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_count(map: *const kafka_consumer_OffsetMap_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid offset-map handle, i.e. an
    // `OffsetMapInner` leaked via `Box::into_raw` in `box_offset_map` and alive until
    // `kafka_consumer_OffsetMap_destroy`; no null check, and the borrow is used only to
    // read `keys.len()` within this call.
    unsafe { &*(map as *const OffsetMapInner) }.keys.len() as i32
}

/// Returns the key (topic-partition) at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid offset-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_get_key(
    map: *const kafka_consumer_OffsetMap_t,
    index: i32,
) -> *const kafka_common_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid offset-map handle, i.e. an
    // `OffsetMapInner` leaked via `Box::into_raw` in `box_offset_map`, alive until
    // `kafka_consumer_OffsetMap_destroy`; no null check. `index` is non-negative (checked
    // above) and bounds-checked by `get`; the returned key pointer borrows into `keys` and
    // is valid until the map is destroyed, as documented.
    match unsafe { &*(map as *const OffsetMapInner) }.keys.get(index as usize) {
        Some(k) => k as *const TopicPartitionInner as *const kafka_common_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the value (offset-and-metadata) at `index` (borrowed), or null if out
/// of range.
///
/// # Safety
///
/// `map` must be a valid offset-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_get_value(
    map: *const kafka_consumer_OffsetMap_t,
    index: i32,
) -> *const kafka_consumer_OffsetAndMetadata_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid offset-map handle, i.e. an
    // `OffsetMapInner` leaked via `Box::into_raw` in `box_offset_map`, alive until
    // `kafka_consumer_OffsetMap_destroy`; no null check. `index` is non-negative (checked
    // above) and bounds-checked by `get`; the returned value pointer borrows into `values`
    // and is valid until the map is destroyed, as documented.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetMap_destroy(map: *mut kafka_consumer_OffsetMap_t) {
    if !map.is_null() {
        // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a
        // handle produced by `Box::into_raw` in `box_offset_map` that becomes invalid after
        // this call, so reclaiming the `Box` is the single, final use; the borrowed
        // key/value sub-handles handed out by the getters die with it, which is their
        // documented borrowed-until-destroyed contract.
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

pub(crate) fn box_offset_and_timestamp_map(
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_count(
    map: *const kafka_consumer_OffsetAndTimestampMap_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid offset-and-timestamp-map
    // handle, i.e. an `OffsetAndTimestampMapInner` leaked via `Box::into_raw` in
    // `box_offset_and_timestamp_map` and alive until
    // `kafka_consumer_OffsetAndTimestampMap_destroy`; no null check, and the borrow is used
    // only to read `keys.len()` within this call.
    unsafe { &*(map as *const OffsetAndTimestampMapInner) }.keys.len() as i32
}

/// Returns the key (topic-partition) at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid offset-and-timestamp-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_get_key(
    map: *const kafka_consumer_OffsetAndTimestampMap_t,
    index: i32,
) -> *const kafka_common_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid offset-and-timestamp-map
    // handle, i.e. an `OffsetAndTimestampMapInner` leaked via `Box::into_raw` in
    // `box_offset_and_timestamp_map`, alive until
    // `kafka_consumer_OffsetAndTimestampMap_destroy`; no null check. `index` is
    // non-negative (checked above) and bounds-checked by `get`; the returned key pointer
    // borrows into `keys` and is valid until the map is destroyed, as documented.
    match unsafe { &*(map as *const OffsetAndTimestampMapInner) }.keys.get(index as usize) {
        Some(k) => k as *const TopicPartitionInner as *const kafka_common_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the value (offset-and-timestamp) at `index` (borrowed), or null if
/// out of range.
///
/// # Safety
///
/// `map` must be a valid offset-and-timestamp-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_get_value(
    map: *const kafka_consumer_OffsetAndTimestampMap_t,
    index: i32,
) -> *const kafka_consumer_OffsetAndTimestamp_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid offset-and-timestamp-map
    // handle, i.e. an `OffsetAndTimestampMapInner` leaked via `Box::into_raw` in
    // `box_offset_and_timestamp_map`, alive until
    // `kafka_consumer_OffsetAndTimestampMap_destroy`; no null check. `index` is
    // non-negative (checked above) and bounds-checked by `get`; the returned value pointer
    // borrows into `values` and is valid until the map is destroyed, as documented.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestampMap_destroy(
    map: *mut kafka_consumer_OffsetAndTimestampMap_t,
) {
    if !map.is_null() {
        // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a
        // handle produced by `Box::into_raw` in `box_offset_and_timestamp_map` that becomes
        // invalid after this call, so reclaiming the `Box` is the single, final use; the
        // borrowed key/value sub-handles handed out by the getters die with it, per their
        // documented borrowed-until-destroyed contract.
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

pub(crate) fn box_long_offset_map(map: HashMap<TopicPartition, i64>) -> *mut kafka_consumer_LongOffsetMap_t {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_count(map: *const kafka_consumer_LongOffsetMap_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid long-offset-map handle, i.e.
    // a `LongOffsetMapInner` leaked via `Box::into_raw` in `box_long_offset_map` and alive
    // until `kafka_consumer_LongOffsetMap_destroy`; no null check, and the borrow is used
    // only to read `keys.len()` within this call.
    unsafe { &*(map as *const LongOffsetMapInner) }.keys.len() as i32
}

/// Returns the key (topic-partition) at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid long-offset-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_get_key(
    map: *const kafka_consumer_LongOffsetMap_t,
    index: i32,
) -> *const kafka_common_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid long-offset-map handle, i.e.
    // a `LongOffsetMapInner` leaked via `Box::into_raw` in `box_long_offset_map`, alive
    // until `kafka_consumer_LongOffsetMap_destroy`; no null check. `index` is non-negative
    // (checked above) and bounds-checked by `get`; the returned key pointer borrows into
    // `keys` and is valid until the map is destroyed, as documented.
    match unsafe { &*(map as *const LongOffsetMapInner) }.keys.get(index as usize) {
        Some(k) => k as *const TopicPartitionInner as *const kafka_common_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Returns the offset value at `index`, or `-1` if out of range.
///
/// # Safety
///
/// `map` must be a valid long-offset-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_get_value(
    map: *const kafka_consumer_LongOffsetMap_t,
    index: i32,
) -> i64 {
    if index < 0 {
        return -1;
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid long-offset-map handle, i.e.
    // a `LongOffsetMapInner` leaked via `Box::into_raw` in `box_long_offset_map`, alive
    // until `kafka_consumer_LongOffsetMap_destroy`; no null check. `index` is non-negative
    // (checked above) and bounds-checked by `get`; the `i64` is copied out, so nothing
    // borrowed escapes the call.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_LongOffsetMap_destroy(map: *mut kafka_consumer_LongOffsetMap_t) {
    if !map.is_null() {
        // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a
        // handle produced by `Box::into_raw` in `box_long_offset_map` that becomes invalid
        // after this call, so reclaiming the `Box` is the single, final use; the borrowed
        // key sub-handles handed out by `get_key` die with it, per their documented
        // borrowed-until-destroyed contract.
        unsafe { drop(Box::from_raw(map as *mut LongOffsetMapInner)) };
    }
}

/// Opaque handle to a `List<PartitionInfo>` result (`partitions_for`).
#[repr(C)]
pub struct kafka_common_PartitionInfoList_t {
    _private: [u8; 0],
}

struct PartitionInfoListInner {
    items: Vec<PartitionInfoInner>,
}

pub(crate) fn box_partition_info_list(infos: Vec<PartitionInfo>) -> *mut kafka_common_PartitionInfoList_t {
    let items = infos
        .into_iter()
        .map(|info| {
            let topic_c = std::ffi::CString::new(info.topic().as_bytes()).unwrap_or_default();
            PartitionInfoInner { info, topic_c }
        })
        .collect();
    Box::into_raw(Box::new(PartitionInfoListInner { items })) as *mut kafka_common_PartitionInfoList_t
}

/// Returns the number of partition-info entries.
///
/// # Safety
///
/// `list` must be a valid partition-info-list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfoList_count(list: *const kafka_common_PartitionInfoList_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `list` is a valid partition-info-list handle:
    // either a `PartitionInfoListInner` leaked via `Box::into_raw` in
    // `box_partition_info_list`, alive until `kafka_common_PartitionInfoList_destroy`, or
    // the borrowed entry of a `TopicPartitionInfoMapInner::lists` vec returned by
    // `kafka_common_TopicPartitionInfoMap_get_partitions`, alive until that map is
    // destroyed. No null check; the borrow is used only to read `items.len()` within this
    // call.
    unsafe { &*(list as *const PartitionInfoListInner) }.items.len() as i32
}

/// Returns the partition-info at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `list` must be a valid partition-info-list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfoList_get(
    list: *const kafka_common_PartitionInfoList_t,
    index: i32,
) -> *const kafka_common_PartitionInfo_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `list` is a valid partition-info-list handle
    // (an owned `PartitionInfoListInner` from `box_partition_info_list`, or the borrowed
    // `lists` entry handed out by `kafka_common_TopicPartitionInfoMap_get_partitions`),
    // alive until its owner is destroyed. No null check; `index` is non-negative (checked
    // above) and bounds-checked by `get`, and the returned partition-info pointer borrows
    // into `items`, valid until the owner is destroyed, as documented.
    match unsafe { &*(list as *const PartitionInfoListInner) }.items.get(index as usize) {
        Some(i) => i as *const PartitionInfoInner as *const kafka_common_PartitionInfo_t,
        None => std::ptr::null(),
    }
}

/// Destroys a partition-info-list handle. Safe with null (no-op).
///
/// # Safety
///
/// `list` must be null or a valid partition-info-list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfoList_destroy(list: *mut kafka_common_PartitionInfoList_t) {
    if !list.is_null() {
        // SAFETY: `list` is non-null (checked above) and, per this function's `# Safety`, a
        // valid partition-info-list handle that must not be used after this call.
        // Reclaiming the `Box` is sound for the owned lists produced by `Box::into_raw` in
        // `box_partition_info_list` (`partitions_for`), which makes this the single, final
        // use. The borrowed `*const PartitionInfoListInner` handed out by
        // `kafka_common_TopicPartitionInfoMap_get_partitions` is a `Vec` entry owned by the
        // map and must never be passed here; the contract's "valid partition-info-list
        // handle" wording leaves that distinction implicit.
        unsafe { drop(Box::from_raw(list as *mut PartitionInfoListInner)) };
    }
}

// ---------------------------------------------------------------------------
// MetricMap — the `Consumer::metrics()` snapshot
// ---------------------------------------------------------------------------

// The metric value-kind discriminants (0=Double, 1=String, 2=Long, 3=Int) are
// the shared `crate::ffi::common::METRIC_VALUE_*` constants — cbindgen does not
// emit them into the header, so there is nothing consumer-specific to export.

/// Opaque handle to a `Map<MetricName, Metric>` snapshot
/// (`Consumer::metrics()`).
///
/// Java's `metrics()` returns live `Metric` objects whose `metricValue()`
/// re-measures on each read. This handle is a **point-in-time snapshot**: each
/// entry's value was measured once, when `metrics()` was called. That matches
/// the documented contract of [`crate::consumer::Consumer::metrics`] ("a
/// point-in-time snapshot ... not a live view"), and it is the only thing that
/// can cross an FFI boundary without an upcall per read.
#[repr(C)]
pub struct kafka_consumer_MetricMap_t {
    _private: [u8; 0],
}

// The snapshot representation (`MetricEntry` / `MetricMapInner`), the
// snapshot-building logic, and the index-walking accessor helpers live in
// `ffi::common` and are shared verbatim with the producer FFI surface. Only the
// namespaced opaque type + `extern "C"` wrappers are consumer-specific here.
use crate::ffi::common::MetricMapInner;

fn box_metric_map(metrics: HashMap<crate::common::MetricName, Arc<KafkaMetric>>) -> *mut kafka_consumer_MetricMap_t {
    Box::into_raw(crate::ffi::common::build_metric_map_inner(metrics)) as *mut kafka_consumer_MetricMap_t
}

/// Returns the number of metric entries.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_count(map: *const kafka_consumer_MetricMap_t) -> i32 {
    // SAFETY: `crate::ffi::common::metric_map_count` requires a valid metric-map backing
    // pointer; per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map` and alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; the helper borrows it only for
    // this call.
    unsafe { crate::ffi::common::metric_map_count(map as *const MetricMapInner) }
}

/// Returns the metric name at `index` (borrowed; valid until the map is
/// destroyed), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_name(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: `crate::ffi::common::metric_map_get_name` requires a valid metric-map backing
    // pointer; per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the returned string borrows into the snapshot, valid until the map is
    // destroyed, as documented.
    unsafe { crate::ffi::common::metric_map_get_name(map as *const MetricMapInner, index) }
}

/// Returns the metric group at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_group(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: `crate::ffi::common::metric_map_get_group` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the returned string borrows into the snapshot, valid until the map is
    // destroyed, as documented.
    unsafe { crate::ffi::common::metric_map_get_group(map as *const MetricMapInner, index) }
}

/// Returns the metric description at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_description(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: `crate::ffi::common::metric_map_get_description` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the returned string borrows into the snapshot, valid until the map is
    // destroyed, as documented.
    unsafe { crate::ffi::common::metric_map_get_description(map as *const MetricMapInner, index) }
}

/// Returns the number of tags on the metric at `index`, or `-1` if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_tag_count(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> i32 {
    // SAFETY: `crate::ffi::common::metric_map_get_tag_count` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper (returning `-1` when out of range).
    unsafe { crate::ffi::common::metric_map_get_tag_count(map as *const MetricMapInner, index) }
}

/// Returns the `tag_index`-th tag key of the metric at `index` (borrowed), or
/// null if either index is out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_tag_key(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    // SAFETY: `crate::ffi::common::metric_map_get_tag_key` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` and `tag_index` are
    // range-checked by the helper, and the returned string borrows into the snapshot, valid
    // until the map is destroyed, as documented.
    unsafe { crate::ffi::common::metric_map_get_tag_key(map as *const MetricMapInner, index, tag_index) }
}

/// Returns the `tag_index`-th tag value of the metric at `index` (borrowed), or
/// null if either index is out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_tag_value(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    // SAFETY: `crate::ffi::common::metric_map_get_tag_value` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` and `tag_index` are
    // range-checked by the helper, and the returned string borrows into the snapshot, valid
    // until the map is destroyed, as documented.
    unsafe { crate::ffi::common::metric_map_get_tag_value(map as *const MetricMapInner, index, tag_index) }
}

/// Returns which `get_value_*` accessor is valid for the metric at `index`.
/// Defaults to `DOUBLE` when `index` is out of range (the `get_value_double`
/// accessor then returns `0.0`).
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_value_kind(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> i32 {
    // SAFETY: `crate::ffi::common::metric_map_get_value_kind` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the value is copied out.
    unsafe { crate::ffi::common::metric_map_get_value_kind(map as *const MetricMapInner, index) }
}

/// Returns the `Double` reading of the metric at `index`, or `0.0` if out of
/// range or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_value_double(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> f64 {
    // SAFETY: `crate::ffi::common::metric_map_get_value_double` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the value is copied out.
    unsafe { crate::ffi::common::metric_map_get_value_double(map as *const MetricMapInner, index) }
}

/// Returns the `String` reading of the metric at `index` (borrowed), or null if
/// out of range. Empty for a non-`String` kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_value_string(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: `crate::ffi::common::metric_map_get_value_string` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the returned string borrows into the snapshot, valid until the map is
    // destroyed, as documented.
    unsafe { crate::ffi::common::metric_map_get_value_string(map as *const MetricMapInner, index) }
}

/// Returns the `Long` reading of the metric at `index`, or `0` if out of range
/// or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_value_long(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> i64 {
    // SAFETY: `crate::ffi::common::metric_map_get_value_long` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the value is copied out.
    unsafe { crate::ffi::common::metric_map_get_value_long(map as *const MetricMapInner, index) }
}

/// Returns the `Int` reading of the metric at `index`, or `0` if out of range
/// or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_get_value_int(
    map: *const kafka_consumer_MetricMap_t,
    index: i32,
) -> i32 {
    // SAFETY: `crate::ffi::common::metric_map_get_value_int` requires a valid metric-map
    // backing pointer; per this function's `# Safety`, `map` is a valid metric-map handle,
    // i.e. the `MetricMapInner` leaked via `Box::into_raw` in `box_metric_map`, alive until
    // `kafka_consumer_MetricMap_destroy`. No null check; `index` is range-checked by the
    // helper, and the value is copied out.
    unsafe { crate::ffi::common::metric_map_get_value_int(map as *const MetricMapInner, index) }
}

/// Destroys a metric-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MetricMap_destroy(map: *mut kafka_consumer_MetricMap_t) {
    // SAFETY: `crate::ffi::common::metric_map_destroy` is documented safe with null (a
    // no-op) and otherwise requires a valid metric-map backing pointer; per this function's
    // `# Safety`, `map` is null or a handle produced by `Box::into_raw` in `box_metric_map`
    // that becomes invalid after this call, so the helper's `Box::from_raw` is the single,
    // final use, and the strings it handed out borrowed die with it as documented.
    unsafe { crate::ffi::common::metric_map_destroy(map as *mut MetricMapInner) };
}

/// Opaque handle to a `Map<String, List<PartitionInfo>>` result
/// (`list_topics`).
#[repr(C)]
pub struct kafka_common_TopicPartitionInfoMap_t {
    _private: [u8; 0],
}

struct TopicPartitionInfoMapInner {
    topics: Vec<std::ffi::CString>,
    lists: Vec<PartitionInfoListInner>,
}

fn box_topic_partition_info_map(map: HashMap<String, Vec<PartitionInfo>>) -> *mut kafka_common_TopicPartitionInfoMap_t {
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
    Box::into_raw(Box::new(TopicPartitionInfoMapInner { topics, lists })) as *mut kafka_common_TopicPartitionInfoMap_t
}

/// Returns the number of topics.
///
/// # Safety
///
/// `map` must be a valid topic-partition-info-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfoMap_count(
    map: *const kafka_common_TopicPartitionInfoMap_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid topic-partition-info-map
    // handle, i.e. a `TopicPartitionInfoMapInner` leaked via `Box::into_raw` in
    // `box_topic_partition_info_map` and alive until
    // `kafka_common_TopicPartitionInfoMap_destroy`; no null check, and the borrow is used
    // only to read `topics.len()` within this call.
    unsafe { &*(map as *const TopicPartitionInfoMapInner) }.topics.len() as i32
}

/// Returns the topic name at `index` as a NUL-terminated C string (owned by the
/// handle), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid topic-partition-info-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfoMap_get_topic(
    map: *const kafka_common_TopicPartitionInfoMap_t,
    index: i32,
) -> *const c_char {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid topic-partition-info-map
    // handle, i.e. a `TopicPartitionInfoMapInner` leaked via `Box::into_raw` in
    // `box_topic_partition_info_map`, alive until
    // `kafka_common_TopicPartitionInfoMap_destroy`; no null check. `index` is non-negative
    // (checked above) and bounds-checked by `get`; the returned C string is owned by the
    // map and valid until it is destroyed, as documented.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfoMap_get_partitions(
    map: *const kafka_common_TopicPartitionInfoMap_t,
    index: i32,
) -> *const kafka_common_PartitionInfoList_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `map` is a valid topic-partition-info-map
    // handle, i.e. a `TopicPartitionInfoMapInner` leaked via `Box::into_raw` in
    // `box_topic_partition_info_map`, alive until
    // `kafka_common_TopicPartitionInfoMap_destroy`; no null check. `index` is non-negative
    // (checked above) and bounds-checked by `get`; the returned list pointer borrows into
    // `lists` and is valid until the map is destroyed, as documented, and is meant to be
    // read through the `PartitionInfoList` getters, not destroyed.
    match unsafe { &*(map as *const TopicPartitionInfoMapInner) }
        .lists
        .get(index as usize)
    {
        Some(l) => l as *const PartitionInfoListInner as *const kafka_common_PartitionInfoList_t,
        None => std::ptr::null(),
    }
}

/// Destroys a topic-partition-info-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid topic-partition-info-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfoMap_destroy(map: *mut kafka_common_TopicPartitionInfoMap_t) {
    if !map.is_null() {
        // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a
        // handle produced by `Box::into_raw` in `box_topic_partition_info_map` that becomes
        // invalid after this call, so reclaiming the `Box` is the single, final use; the
        // borrowed topic strings and list sub-handles handed out by the getters die with
        // it, per their documented borrowed-until-destroyed contract.
        unsafe { drop(Box::from_raw(map as *mut TopicPartitionInfoMapInner)) };
    }
}

/// Opaque handle to a `Set<TopicPartition>` result (`assignment` / `paused`).
#[repr(C)]
pub struct kafka_common_TopicPartitionList_t {
    _private: [u8; 0],
}

struct TopicPartitionListInner {
    items: Vec<TopicPartitionInner>,
}

pub(crate) fn box_topic_partition_list(
    tps: impl IntoIterator<Item = TopicPartition>,
) -> *mut kafka_common_TopicPartitionList_t {
    let items = tps
        .into_iter()
        .map(|tp| {
            let topic_c = std::ffi::CString::new(tp.topic().as_bytes()).unwrap_or_default();
            TopicPartitionInner { tp, topic_c }
        })
        .collect();
    Box::into_raw(Box::new(TopicPartitionListInner { items })) as *mut kafka_common_TopicPartitionList_t
}

/// Returns the number of topic-partitions.
///
/// # Safety
///
/// `list` must be a valid topic-partition-list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionList_count(list: *const kafka_common_TopicPartitionList_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `list` is a valid topic-partition-list
    // handle, i.e. a `TopicPartitionListInner` leaked via `Box::into_raw` in
    // `box_topic_partition_list` and alive until `kafka_common_TopicPartitionList_destroy`;
    // no null check, and the borrow is used only to read `items.len()` within this call.
    unsafe { &*(list as *const TopicPartitionListInner) }.items.len() as i32
}

/// Returns the topic-partition at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `list` must be a valid topic-partition-list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionList_get(
    list: *const kafka_common_TopicPartitionList_t,
    index: i32,
) -> *const kafka_common_TopicPartition_t {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `list` is a valid topic-partition-list
    // handle, i.e. a `TopicPartitionListInner` leaked via `Box::into_raw` in
    // `box_topic_partition_list`, alive until `kafka_common_TopicPartitionList_destroy`; no
    // null check. `index` is non-negative (checked above) and bounds-checked by `get`; the
    // returned topic-partition pointer borrows into `items` and is valid until the list is
    // destroyed, as documented.
    match unsafe { &*(list as *const TopicPartitionListInner) }.items.get(index as usize) {
        Some(i) => i as *const TopicPartitionInner as *const kafka_common_TopicPartition_t,
        None => std::ptr::null(),
    }
}

/// Destroys a topic-partition-list handle. Safe with null (no-op).
///
/// # Safety
///
/// `list` must be null or a valid topic-partition-list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionList_destroy(list: *mut kafka_common_TopicPartitionList_t) {
    if !list.is_null() {
        // SAFETY: `list` is non-null (checked above) and, per this function's `# Safety`, a
        // handle produced by `Box::into_raw` in `box_topic_partition_list` that becomes
        // invalid after this call, so reclaiming the `Box` is the single, final use; the
        // borrowed topic-partition sub-handles handed out by `get` die with it, per their
        // documented borrowed-until-destroyed contract.
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

pub(crate) fn box_string_list(strings: impl IntoIterator<Item = String>) -> *mut kafka_consumer_StringList_t {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_count(list: *const kafka_consumer_StringList_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `list` is a valid string-list handle, i.e. a
    // `StringListInner` leaked via `Box::into_raw` in `box_string_list` and alive until
    // `kafka_consumer_StringList_destroy`; no null check, and the borrow is used only to
    // read `items.len()` within this call.
    unsafe { &*(list as *const StringListInner) }.items.len() as i32
}

/// Returns the string at `index` as a NUL-terminated C string (owned by the
/// handle), or null if out of range.
///
/// # Safety
///
/// `list` must be a valid string-list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_get(
    list: *const kafka_consumer_StringList_t,
    index: i32,
) -> *const c_char {
    if index < 0 {
        return std::ptr::null();
    }
    // SAFETY: Per this function's `# Safety`, `list` is a valid string-list handle, i.e. a
    // `StringListInner` leaked via `Box::into_raw` in `box_string_list`, alive until
    // `kafka_consumer_StringList_destroy`; no null check. `index` is non-negative (checked
    // above) and bounds-checked by `get`; the returned C string is owned by the list and
    // valid until it is destroyed, as documented.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_destroy(list: *mut kafka_consumer_StringList_t) {
    if !list.is_null() {
        // SAFETY: `list` is non-null (checked above) and, per this function's `# Safety`, a
        // handle produced by `Box::into_raw` in `box_string_list` that becomes invalid
        // after this call, so reclaiming the `Box` is the single, final use; the C strings
        // handed out by `get` die with it, per their documented owned-by-the-handle
        // contract.
        unsafe { drop(Box::from_raw(list as *mut StringListInner)) };
    }
}

/// Frees a NUL-terminated C string returned by a consumer getter that allocates
/// an owned string (e.g. [`kafka_consumer_Consumer_client_id`]). Safe with null.
///
/// # Safety
///
/// `s` must be null or a string returned by such a getter.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_string_destroy(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: `s` is non-null (checked above) and, per this function's `# Safety`, a
        // string returned by an owned-string consumer getter, i.e. produced by
        // `CString::into_raw` (as in `kafka_consumer_Consumer_client_id`); the documented
        // contract makes this the single, final use, and `CString::from_raw` reclaims
        // exactly the allocation `into_raw` leaked.
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
/// `LocalConcurrentModification` error if the guard cannot be acquired).
///
/// `op` receives `&mut dyn Consumer` and returns the future to drive.
///
/// # Safety
///
/// `consumer` must be a valid handle.
unsafe fn sync_void_op<F>(consumer: *const kafka_consumer_Consumer_t, op: F) -> *mut kafka_common_Error_t
where
    // A higher-ranked bound ties the returned future's lifetime to the borrow
    // of the consumer, so the future may borrow `&mut self` for its duration
    // (which `FnOnce(&mut _) -> impl Future` cannot express with one type
    // parameter).
    F: for<'a> FnOnce(
        &'a mut dyn Consumer<Bytes, Bytes>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + 'a>>,
{
    // SAFETY: `handle_ref` requires a non-null handle created by a consumer constructor,
    // which this function's `# Safety` requires of `consumer` (no null check); `h` is used
    // only within this synchronous call, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard: `acquire(h)` succeeded (an `Err`
    // returned early) and `_g: ReleaseGuard` holds it until this function returns. The
    // higher-ranked bound on `F` ties the returned future to the `&mut dyn Consumer`
    // borrow, and `block_on(fut)` drives it to completion before `_g` drops, so the
    // exclusive access ends inside the guarded window, per the `UnsafeCell` + `acquire`
    // invariant documented on `FfiConsumerHandle`'s `unsafe impl Send/Sync`.
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
/// If the access guard is already held, the callback fires inline on the calling
/// thread with the rejection error; otherwise it fires from the dispatcher thread
/// once the op completes.
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
    Fut: std::future::Future<Output = Result<(), Error>> + Send,
{
    // SAFETY: `handle_ref` requires a non-null handle from a consumer constructor, which
    // this function's `# Safety` requires of `consumer` (no null check); `h` is used only
    // on the submitting thread within this call, for `acquire`, cloning `completion_tx` and
    // spawning on `runtime_handle`.
    let h = unsafe { handle_ref(consumer) };
    let target = OperationCallbackTarget { callback, user_data };
    if let Err(e) = acquire(h) {
        // SAFETY: `callback` was supplied by the C caller along with `user_data` (carried
        // in `OperationCallbackTarget`); on this rejection path the guard was not taken and
        // nothing is spawned, so this is the single invocation, fired inline on the calling
        // thread with a fresh `box_error(e)` handle the callback owns, matching the
        // `OperationCallbackFn` contract (a non-null `error` means failure). Unlike the
        // success path it does not go through the dispatcher thread; the C user is
        // responsible for the thread-safety of `user_data`.
        unsafe { (target.callback)(box_error(e), target.user_data) };
        return;
    }
    let tx = h.completion_tx.clone();
    // SAFETY: `handle_ref`'s precondition holds exactly as for `h`: per this function's `#
    // Safety`, `consumer` is a valid handle from a consumer constructor, leaked by
    // `build_consumer_handle` and freed only by `kafka_consumer_Consumer_destroy`, so the
    // `&'static` is sound while the handle lives. `hs` escapes into the spawned task and
    // the completion job (as `release_handle`), where it is last touched by
    // `release(release_handle)` before `op.fire()`. Its keep-alive is not mechanical
    // (`destroy` registers no task to join and `shutdown_background` does not wait): it is
    // the documented `destroy` precondition that the caller must not destroy the consumer
    // while an operation is in flight, i.e. before its completion callback has fired.
    let hs: &'static FfiConsumerHandle = unsafe { handle_ref(consumer) };
    spawn_callback_task(&h.runtime_handle, async move {
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
            // SAFETY: `OperationCompletion::fire` requires exactly one call on the
            // dispatcher thread: the `FnOnce` job consumes `op`, is boxed once, and
            // `enqueue_or_run_inline` hands it to the single per-handle dispatcher thread
            // or, only if that thread has already exited (the task's `tx` clone keeps the
            // channel itself open), runs it inline exactly once on this tokio worker, the
            // post-teardown fallback documented in `common.rs`. `callback` and `user_data`
            // were supplied together by the C caller; `error` is a fresh `box_error` handle
            // (or null for success) whose ownership transfers to the callee; the raw
            // pointers are owned handles moved to the dispatcher thread, and the C user is
            // responsible for the thread-safety of `user_data`.
            unsafe { op.fire() };
        });
        enqueue_or_run_inline(&tx, job);
    });
}

/// A raw `user_data` pointer wrapped so it can cross into the spawned task and
/// completion job. The C user owns its thread-safety (CLAUDE.md FFI §4).
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
/// concurrent op (sync or async) is rejected with `LocalConcurrentModification` until
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
    Fut: std::future::Future<Output = Result<T, Error>> + Send,
    F: FnOnce(&'static mut dyn Consumer<Bytes, Bytes>) -> Fut + Send + 'static,
    C: FnOnce(Result<T, Error>, *mut c_void) + Send + 'static,
{
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this helper's `# Safety`, `consumer` is a valid handle from a consumer
    // constructor, i.e. the `Box::into_raw` of `build_consumer_handle`. `h` is used only on
    // the calling thread, to try `acquire(h)`, clone `completion_tx` and reach
    // `runtime_handle`, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        // Rejected: fire the callback inline with the error; guard not taken.
        complete(Err(e), user_data);
        return;
    }
    let tx = h.completion_tx.clone();
    // SAFETY: Same precondition as `h` above: per this helper's `# Safety`, `consumer` is a
    // valid handle from a consumer constructor, so `handle_ref` yields a reference to the
    // leaked `FfiConsumerHandle`. `hs` escapes into the spawned task and the completion
    // job, where it is used only for `consumer_mut(hs)` and `release(hs)`; both uses finish
    // before `complete` fires the C callback, and `acquire(h)` succeeded above, so the
    // guard is held for the whole submit->callback window. The allocation outliving that
    // window is not enforced by a join: it relies on `kafka_consumer_Consumer_destroy`'s
    // documented precondition that destroying a consumer with an operation in flight is a C
    // lifetime violation (CLAUDE.md FFI §4), so a caller that destroys only after the
    // callback has fired never races this reference.
    let hs: &'static FfiConsumerHandle = unsafe { handle_ref(consumer) };
    let ud = SendUserData(user_data);
    spawn_callback_task(&h.runtime_handle, async move {
        let ud = ud;
        // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)`
        // succeeded above (the rejection path returned early) and the guard is released
        // only by `release(hs)` in the completion job, after this future has completed, so
        // the guard is held for the whole submit->callback window and the `&mut` handed out
        // is exclusive per the single-owner `acquire()` design documented at the `SAFETY`
        // comment on `FfiConsumerHandle`'s `unsafe impl Send`. `hs` is the `&'static
        // FfiConsumerHandle` from `handle_ref(consumer)`, kept alive by the destroy
        // precondition noted there.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topics` requires `topics` to point to `count` valid C strings, exactly
    // what this function's `# Safety` promises; it reads `count.max(0)` entries, so a
    // non-positive `count` reads nothing, and it tolerates no NULL array or entry (the
    // contract requires valid strings whenever `count > 0`). The strings are copied into
    // owned `String`s before anything else runs.
    let topic_vec = unsafe { read_topics(topics, count) };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `topic_vec`, no
    // raw C pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.subscribe_with_topics(topic_vec))) }
}

/// Completion callback for void-returning async consumer ops. Receives null on
/// success or an error handle the callback owns (free it with
/// [`kafka_common_Error_destroy`]), followed by the `user_data` passed to the
/// entry point.
pub type kafka_consumer_Consumer_op_callback_t = unsafe extern "C" fn(*mut kafka_common_Error_t, *mut c_void);

/// Subscribes to a list of topics (async). See [`kafka_consumer_Consumer_subscribe`].
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` `count` valid C strings.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topics` requires `topics` to point to `count` valid C strings, exactly
    // what this function's `# Safety` promises; it reads `count.max(0)` entries, so a
    // non-positive `count` reads nothing, and it tolerates no NULL array or entry (the
    // contract requires valid strings whenever `count > 0`). The strings are copied into
    // owned `String`s before anything else runs.
    let topic_vec = unsafe { read_topics(topics, count) };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `topic_vec` and holds no raw C pointers. `callback` was supplied by the C caller
    // along with `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md
    // FFI §4 forbids checking required parameters); the helper fires it inline on this
    // thread with a fresh `box_error` handle if the guard is rejected, and otherwise
    // exactly once from the dispatcher thread after `release`, with null or a fresh error
    // handle the callback owns. Under the one-shot `OperationCallbackTarget` convention the
    // C caller keeps `user_data` valid across that single completion, and must not destroy
    // the consumer while the op is in flight (`kafka_consumer_Consumer_destroy`'s
    // documented precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.subscribe_with_topics(topic_vec)) };
}

// ── subscribe with a ConsumerRebalanceListener ──

/// `on_partitions_revoked` callback of a
/// [`kafka_consumer_ConsumerRebalanceListener_t`] — the C equivalent of Java's
/// `ConsumerRebalanceListener.onPartitionsRevoked(Collection<TopicPartition>)`.
///
/// `partitions` is the (always non-null) list of partitions being taken away.
/// **The callee owns it** and must free it with
/// [`kafka_common_TopicPartitionList_destroy`], matching the "callbacks own the
/// handles delivered to them" convention of the other consumer callbacks.
///
/// # Return value
///
/// Return `NULL` for success. Returning a non-null error handle (built with
/// [`kafka_common_Error_new`]) is the C equivalent of the Java listener
/// throwing: **ownership of that handle transfers to the client**, which converts
/// it back into the `Result::Err` the core sees, so the error propagates out of
/// the operation that triggered the rebalance. Do not destroy a handle you
/// return.
///
/// # Invocation context
///
/// Invoked on the consumer's callback dispatcher thread — never on a tokio
/// worker, and never concurrently with another callback of the same consumer.
/// The rebalance does not proceed until the callback returns
/// (`consumer-threading.md` §31). The callback must **not** call the plain
/// `kafka_consumer_Consumer_*` API of the consumer being rebalanced (the access
/// guard is held by the app thread driving the rebalance, so those calls fail
/// with a `ConcurrentModification` error); use
/// `kafka_consumer_ConsumerHandle_t` (obtained with
/// `kafka_consumer_Consumer_handle`) instead, which is exactly the sanctioned
/// reentrancy path Java gets for free by running the callback on the polling
/// thread.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t =
    unsafe extern "C" fn(*mut kafka_common_TopicPartitionList_t, *mut c_void) -> *mut kafka_common_Error_t;

/// `on_partitions_assigned` callback of a
/// [`kafka_consumer_ConsumerRebalanceListener_t`] — Java's
/// `ConsumerRebalanceListener.onPartitionsAssigned(Collection<TopicPartition>)`.
///
/// `partitions` carries the **newly added** partitions (not the full
/// assignment), matching Java; the list may be empty while the callback still
/// fires. Ownership and return-value rules are identical to
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t`].
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_callback_t =
    unsafe extern "C" fn(*mut kafka_common_TopicPartitionList_t, *mut c_void) -> *mut kafka_common_Error_t;

/// `on_partitions_lost` callback of a
/// [`kafka_consumer_ConsumerRebalanceListener_t`] — Java's
/// `ConsumerRebalanceListener.onPartitionsLost(Collection<TopicPartition>)`.
///
/// Optional: passing `NULL` to
/// [`kafka_consumer_ConsumerRebalanceListener_new`] reproduces Java's default
/// implementation, which delegates to `onPartitionsRevoked`. Ownership and
/// return-value rules are identical to
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t`].
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_callback_t =
    unsafe extern "C" fn(*mut kafka_common_TopicPartitionList_t, *mut c_void) -> *mut kafka_common_Error_t;

/// Release hook for the `user_data` handed to
/// [`kafka_consumer_ConsumerRebalanceListener_new`].
///
/// Fired **exactly once**, when the client drops its last reference to the
/// listener. May run on **any** thread, so it must be thread-agnostic.
///
/// # When it fires
///
/// Ownership of the listener transfers unconditionally to the subscribe call
/// (see [`kafka_consumer_Consumer_subscribe_with_listener`]), but the *moment*
/// of release depends on whether the core got as far as registering it. The
/// complete list of triggers:
///
/// 1. [`kafka_consumer_ConsumerRebalanceListener_destroy`] on a listener that
///    was never passed to a subscribe call.
/// 2. The subscribe call was rejected **before the core registered** the
///    listener — the access guard rejected it (`ConcurrentModification`), the
///    consumer is already closed, `group.id` is not configured, or a topic in
///    the list is empty/blank. Released before the call returns its error.
/// 3. A subscribe with an **empty topic list** on a real consumer. Java treats
///    that as `unsubscribe()` (`AsyncKafkaConsumer.java:2161-2163`) and never
///    registers the listener, so it is released even though the call returns
///    **success**. (A `MockConsumer` has no such short-circuit — Java's
///    `MockConsumer.subscribe(Collection, Optional)` registers unconditionally —
///    so there the listener stays registered.)
/// 4. A later `subscribe` / `subscribe_with_listener` / pattern subscribe
///    **replaces** the registration, including a listener-less `subscribe`
///    (Java's `registerRebalanceListener(Optional.empty())`).
/// 5. The consumer is destroyed.
///
/// # When it does NOT fire
///
/// - A subscribe that failed **after** the core registered the listener — most
///   notably a subscription-type conflict (`assign()` then
///   `subscribe_with_listener()`, "Subscription to topics, partitions and pattern
///   are mutually exclusive"). Java's `SubscriptionState.subscribe(Set,
///   Optional)` calls `registerRebalanceListener(listener)` *before*
///   `setSubscriptionType(...)`, which is what throws
///   (`SubscriptionState.java:192-196`), so the registration outlives the
///   failure. The hook then fires only on trigger 4 or 5 above. **Do not treat
///   a failed subscribe as proof that `user_data` has been released.**
/// - `unsubscribe` / `close` — they clear the subscription but keep the
///   registered listener, matching Java's `SubscriptionState.unsubscribe()`.
///
/// [`kafka_consumer_ConsumerRebalanceListener_new`] spells this signature out
/// inline as `Option<unsafe extern "C" fn(*mut c_void)>` (a nullable function
/// pointer) rather than naming this alias: cbindgen only recognizes `Option<...>`
/// as a nullable function pointer when the bare `fn` type is written literally,
/// and emits an un-compilable `Option<...>` for an aliased one. The C signature
/// is identical either way — this typedef is the name C callers should use.
pub type kafka_consumer_ConsumerRebalanceListener_user_data_destroy_t = unsafe extern "C" fn(*mut c_void);

/// Opaque handle to a rebalance listener built by
/// [`kafka_consumer_ConsumerRebalanceListener_new`], for
/// [`kafka_consumer_Consumer_subscribe_with_listener`] — the C stand-in for
/// Java's `ConsumerRebalanceListener` implementation object.
///
/// A listener handle is **consumed** by the subscribe call it is passed to (even
/// if that call fails); [`kafka_consumer_ConsumerRebalanceListener_destroy`]
/// exists only for a listener that is never subscribed.
#[repr(C)]
pub struct kafka_consumer_ConsumerRebalanceListener_t {
    _private: [u8; 0],
}

/// Shared shape of the three listener callbacks. All three public typedefs above
/// alias exactly this signature, so one invocation helper serves them all.
type RebalanceCallbackFn =
    unsafe extern "C" fn(*mut kafka_common_TopicPartitionList_t, *mut c_void) -> *mut kafka_common_Error_t;

/// Contents of a [`kafka_consumer_ConsumerRebalanceListener_t`] before it is
/// handed to a consumer: the C callbacks plus the owned `user_data`.
///
/// Holding the [`CallbackTarget`] here (rather than only on the subscribed
/// adapter) is what makes the `user_data_destroy` hook fire for a listener that
/// is destroyed without ever being subscribed.
struct RebalanceListenerInner {
    on_revoked: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t,
    on_assigned: kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_callback_t,
    /// `None` reproduces Java's default `onPartitionsLost` (delegate to revoked).
    on_lost: Option<kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_callback_t>,
    /// Owns `user_data` (and fires the destroy hook on drop).
    target: CallbackTarget,
}

/// Adapts a C listener triple to the core [`ConsumerRebalanceListener`] trait.
///
/// Created when the listener is handed to a consumer; dropped when the consumer
/// releases the registration, which fires the `user_data_destroy` hook.
struct FfiRebalanceListener {
    on_revoked: RebalanceCallbackFn,
    on_assigned: RebalanceCallbackFn,
    on_lost: Option<RebalanceCallbackFn>,
    target: CallbackTarget,
    /// Sender for the consumer's completion-dispatch queue.
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
}

impl FfiRebalanceListener {
    /// Invokes one of the C callbacks on the dispatcher thread and maps its
    /// return value back to `Result<(), Error>`.
    ///
    /// The partitions are cloned into an owned, `Send` `Vec` *before* the job;
    /// the C list handle itself is built inside the job, on the dispatcher
    /// thread, so no raw pointer is ever held across an `.await` (same reason
    /// [`async_value_op`] builds its handles in `complete`).
    ///
    /// Running the callback on the dispatcher thread — a plain OS thread — is
    /// what lets user code inside the listener legally block on a nested
    /// `kafka_consumer_ConsumerHandle_*` operation: the awaiting consumer task
    /// yields its tokio worker meanwhile, and the consumer's background task
    /// keeps spinning (`consumer-threading.md` §31), so the nested op completes.
    async fn invoke(&self, callback: RebalanceCallbackFn, partitions: &[TopicPartition]) -> Result<(), Error> {
        let partitions = partitions.to_vec();
        // Capture the whole `Send` wrapper rather than the raw pointer field
        // (Rust 2021 disjoint closure capture would otherwise grab the latter).
        let user_data = SendUserData(self.target.user_data);
        let error = dispatch_and_wait(&self.completion_tx, move || {
            let user_data = user_data;
            let list = box_topic_partition_list(partitions);
            // SAFETY: `callback` was supplied by the C caller along with
            // `user_data`; `list` is freshly allocated and handed over, and any
            // returned error handle is owned by us from here on.
            let error = unsafe { callback(list, user_data.into_ptr()) };
            // SAFETY: `take_error` requires null or a handle created by `box_error`, and
            // consumes it. `error` is the listener callback's return value, which the
            // `kafka_consumer_ConsumerRebalanceListener_on_partitions_*_callback_t`
            // contract requires to be `NULL` or an error handle built with
            // `kafka_common_Error_new` (a `box_error` allocation) whose ownership transfers
            // to the client ("Do not destroy a handle you return"); it is consumed exactly
            // once here and never touched again.
            unsafe { common::take_error(error) }
        })
        .await;
        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl ConsumerRebalanceListener for FfiRebalanceListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.invoke(self.on_revoked, partitions).await
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.invoke(self.on_assigned, partitions).await
    }

    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        match self.on_lost {
            Some(on_lost) => self.invoke(on_lost, partitions).await,
            // No `on_partitions_lost` supplied: reproduce the Java interface's
            // default, which calls `onPartitionsRevoked(partitions)`.
            None => self.on_partitions_revoked(partitions).await,
        }
    }
}

/// Builds the listener contents from the C callback set, taking ownership of
/// `user_data`.
///
/// Split out from [`kafka_consumer_ConsumerRebalanceListener_new`] so the
/// nullable-function-pointer typedefs (which the exported signature has to spell
/// out inline for cbindgen's sake) are still named by the Rust code that consumes
/// them.
fn new_rebalance_listener_inner(
    on_revoked: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t,
    on_assigned: kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_callback_t,
    on_lost: Option<kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_callback_t>,
    user_data: *mut c_void,
    user_data_destroy: Option<kafka_consumer_ConsumerRebalanceListener_user_data_destroy_t>,
) -> Box<RebalanceListenerInner> {
    Box::new(RebalanceListenerInner {
        on_revoked,
        on_assigned,
        on_lost,
        target: CallbackTarget { user_data, destroy: user_data_destroy },
    })
}

/// Creates a rebalance listener for
/// [`kafka_consumer_Consumer_subscribe_with_listener`] — the C stand-in for
/// implementing Java's `ConsumerRebalanceListener` interface.
///
/// # Parameters
///
/// - `on_partitions_revoked`: required; see
///   [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t`].
/// - `on_partitions_assigned`: required; see
///   [`kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_callback_t`].
/// - `on_partitions_lost`: optional. Pass `NULL` for Java's default behavior
///   (delegate to `on_partitions_revoked`); see
///   [`kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_callback_t`].
/// - `user_data`: opaque pointer passed to every callback. Ownership is
///   transferred to the listener.
/// - `user_data_destroy`: optional release hook for `user_data`, fired exactly
///   once when the registration is released; pass `NULL` to keep ownership on
///   the C side. See
///   [`kafka_consumer_ConsumerRebalanceListener_user_data_destroy_t`].
///
/// # Returns
///
/// A non-null listener handle. Pass it to
/// [`kafka_consumer_Consumer_subscribe_with_listener`] (or its `_async`
/// variant), which **consumes** it — including when the subscribe fails. Only a
/// listener that is never subscribed should be released with
/// [`kafka_consumer_ConsumerRebalanceListener_destroy`].
///
/// # Safety
///
/// The callback pointers must remain valid for the lifetime of the listener, and
/// `user_data` until `user_data_destroy` fires (or, with no hook, until the
/// listener is released).
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_new(
    on_partitions_revoked: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t,
    on_partitions_assigned: kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_callback_t,
    // Spelled out inline (rather than as the `..._on_partitions_lost_callback_t`
    // alias) so cbindgen emits a nullable C function pointer — see that
    // typedef's docs.
    on_partitions_lost: Option<
        unsafe extern "C" fn(*mut kafka_common_TopicPartitionList_t, *mut c_void) -> *mut kafka_common_Error_t,
    >,
    user_data: *mut c_void,
    user_data_destroy: Option<unsafe extern "C" fn(*mut c_void)>,
) -> *mut kafka_consumer_ConsumerRebalanceListener_t {
    let inner = new_rebalance_listener_inner(
        on_partitions_revoked,
        on_partitions_assigned,
        on_partitions_lost,
        user_data,
        user_data_destroy,
    );
    Box::into_raw(inner) as *mut kafka_consumer_ConsumerRebalanceListener_t
}

/// Destroys a listener handle that was **never passed to a subscribe call**,
/// firing its `user_data_destroy` hook. Safe with null (no-op).
///
/// A listener handed to [`kafka_consumer_Consumer_subscribe_with_listener`] (or
/// its `_async` variant) has already been consumed — destroying it afterwards is
/// a double free.
///
/// # Safety
///
/// `listener` must be null or a listener handle from
/// [`kafka_consumer_ConsumerRebalanceListener_new`] that was not passed to a
/// subscribe call. After this call the pointer is invalid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_destroy(
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
) {
    if !listener.is_null() {
        // SAFETY: `listener` is non-null (checked above) and, per this function's `#
        // Safety`, a handle from `kafka_consumer_ConsumerRebalanceListener_new`, the
        // `Box::into_raw` of a `RebalanceListenerInner`, that was never passed to a
        // subscribe call, so it has not already been consumed by `take_rebalance_listener`.
        // This is therefore the single, final use; the contract declares the pointer
        // invalid afterwards, and dropping the inner `CallbackTarget` fires
        // `user_data_destroy` exactly once.
        unsafe { drop(Box::from_raw(listener as *mut RebalanceListenerInner)) };
    }
}

/// Consumes a C listener handle, turning it into the core trait object.
///
/// # Safety
///
/// `listener` must be a non-null handle from
/// [`kafka_consumer_ConsumerRebalanceListener_new`] that has not been consumed
/// or destroyed. After this call the pointer is invalid.
unsafe fn take_rebalance_listener(
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
) -> Arc<dyn ConsumerRebalanceListener> {
    let RebalanceListenerInner { on_revoked, on_assigned, on_lost, target } =
        // SAFETY: Per this function's `# Safety`, `listener` is a non-null handle from
        // `kafka_consumer_ConsumerRebalanceListener_new` (the `Box::into_raw` of a
        // `RebalanceListenerInner`) that has not been consumed or destroyed, so reclaiming
        // the box here is the single, final use of the pointer; the subscribe entry points
        // consume the handle unconditionally and document that destroying it afterwards is
        // a double free, and the contract declares the pointer invalid after this call. The
        // fields move into the `Arc<FfiRebalanceListener>`, including the `CallbackTarget`
        // that keeps owning `user_data`.
        *unsafe { Box::from_raw(listener as *mut RebalanceListenerInner) };
    Arc::new(FfiRebalanceListener { on_revoked, on_assigned, on_lost, target, completion_tx })
}

/// Subscribes to a list of topics with a rebalance listener (sync) — Java's
/// `subscribe(Collection<String>, ConsumerRebalanceListener)`.
///
/// `topics` is an array of `count` C strings. Returns null on success, a non-null
/// error handle on failure.
///
/// # Listener lifetime
///
/// *Ownership* of `listener` transfers **unconditionally**, including when this
/// call fails: do not reuse or destroy the handle after this call. The
/// `user_data_destroy` hook, however, fires when the client drops its last
/// reference to the listener, which is **not** necessarily before this call
/// returns:
///
/// - Rejected **before** the core registered it (guard rejection, closed
///   consumer, missing `group.id`, blank topic name): released before this call
///   returns its error.
/// - Empty topic list on a real consumer: Java treats it as `unsubscribe()` and
///   never registers the listener, so it is released — and this call returns
///   **success**. (A `MockConsumer` registers it instead, matching Java.)
/// - Rejected **after** the core registered it (e.g. a subscription-type
///   conflict after `assign()`): the registration survives the error, exactly as
///   in Java, so the listener is **NOT** released here — it is released later, on
///   replacement or consumer destroy. Do not treat an error return as proof that
///   `user_data` is free.
/// - Accepted: released when a subsequent `subscribe` / `subscribe_with_listener`
///   / pattern subscribe replaces the registration (a listener-less `subscribe`
///   counts — it clears it, as Java's `registerRebalanceListener(Optional.empty())`
///   does), or when the consumer is destroyed. `unsubscribe` and `close` keep it,
///   matching Java's `SubscriptionState.unsubscribe()`.
///
/// See [`kafka_consumer_ConsumerRebalanceListener_user_data_destroy_t`] for the
/// authoritative trigger list.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` must point to `count` valid C
/// strings; `listener` must be a non-null, not-yet-consumed handle from
/// [`kafka_consumer_ConsumerRebalanceListener_new`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_listener(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    count: i32,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only to clone `completion_tx`
    // within this synchronous call, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    // Take ownership of the listener BEFORE anything that can fail, so every
    // early return (incl. `sync_void_op` dropping the closure on guard
    // rejection) releases it and fires the destroy hook.
    // SAFETY: `take_rebalance_listener` requires a non-null, not-yet-consumed handle from
    // `kafka_consumer_ConsumerRebalanceListener_new`, exactly what this function's `#
    // Safety` promises for `listener`; it is consumed here once, before anything can fail,
    // so every return path (including `sync_void_op` dropping the closure on guard
    // rejection) releases the resulting `Arc` as the documented unconditional ownership
    // transfer requires, and the C caller must not reuse or destroy the pointer afterwards.
    let listener = unsafe { take_rebalance_listener(listener, h.completion_tx.clone()) };
    // SAFETY: `read_topics` requires `topics` to point to `count` valid C strings, exactly
    // what this function's `# Safety` promises; it reads `count.max(0)` entries, so a
    // non-positive `count` reads nothing, and it tolerates no NULL array or entry (the
    // contract requires valid strings whenever `count > 0`). The strings are copied into
    // owned `String`s before anything else runs.
    let topic_vec = unsafe { read_topics(topics, count) };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only owned values (`topic_vec`
    // and the `Arc<dyn ConsumerRebalanceListener>` already taken from the C handle), no raw
    // C pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle, dropping the closure and thereby the
    // listener) and holds it via `ReleaseGuard` while `block_on` drives the op on this
    // calling thread.
    unsafe {
        sync_void_op(consumer, move |c| {
            Box::pin(c.subscribe_with_topics_listener(topic_vec, listener))
        })
    }
}

/// Subscribes to a list of topics with a rebalance listener (async). See
/// [`kafka_consumer_Consumer_subscribe_with_listener`] for the listener contract.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` must point to `count` valid C
/// strings; `listener` must be a non-null, not-yet-consumed handle from
/// [`kafka_consumer_ConsumerRebalanceListener_new`]; `callback` must be a valid
/// function pointer.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_listener_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    count: i32,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only to clone `completion_tx`
    // within this synchronous call, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    // SAFETY: `take_rebalance_listener` requires a non-null, not-yet-consumed handle from
    // `kafka_consumer_ConsumerRebalanceListener_new`, exactly what this function's `#
    // Safety` promises for `listener`; it is consumed here once, before anything can fail,
    // so every path (including `async_void_op` dropping the closure on guard rejection)
    // releases the resulting `Arc` as the documented unconditional ownership transfer
    // requires, and the C caller must not reuse or destroy the pointer afterwards.
    let listener = unsafe { take_rebalance_listener(listener, h.completion_tx.clone()) };
    // SAFETY: `read_topics` requires `topics` to point to `count` valid C strings, exactly
    // what this function's `# Safety` promises; it reads `count.max(0)` entries, so a
    // non-positive `count` reads nothing, and it tolerates no NULL array or entry (the
    // contract requires valid strings whenever `count > 0`). The strings are copied into
    // owned `String`s before anything else runs.
    let topic_vec = unsafe { read_topics(topics, count) };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `topic_vec` and the `Arc<dyn ConsumerRebalanceListener>`, no raw C pointers.
    // `callback` was supplied by the C caller along with `user_data` and this function's `#
    // Safety` requires it to be a valid function pointer; the helper fires it inline on
    // this thread with a fresh `box_error` handle if the guard is rejected, and otherwise
    // exactly once from the dispatcher thread after `release`, with null or a fresh error
    // handle the callback owns. Under the one-shot `OperationCallbackTarget` convention the
    // C caller keeps `user_data` valid across that single completion, and must not destroy
    // the consumer while the op is in flight (`kafka_consumer_Consumer_destroy`'s
    // documented precondition).
    unsafe {
        async_void_op(consumer, callback, user_data, move |c| {
            c.subscribe_with_topics_listener(topic_vec, listener)
        })
    };
}

// ── unsubscribe ──

/// Unsubscribes from all topics / partitions (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures nothing, and the helper acquires
    // the access guard (or returns a `LocalConcurrentModification` error handle) and holds
    // it via `ReleaseGuard` while `block_on` drives `unsubscribe` on this calling thread.
    unsafe { sync_void_op(consumer, |c| Box::pin(c.unsubscribe())) }
}

/// Unsubscribes from all topics / partitions (async).
///
/// # Safety
///
/// `consumer` must be a valid handle.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send` data: the closure captures nothing.
    // `callback` was supplied by the C caller along with `user_data` (a pair this
    // function's `# Safety` leaves implicit; CLAUDE.md FFI §4 forbids checking required
    // parameters); the helper fires it inline on this thread with a fresh `box_error`
    // handle if the guard is rejected, and otherwise exactly once from the dispatcher
    // thread after `release`, with null or a fresh error handle the callback owns. Under
    // the one-shot `OperationCallbackTarget` convention the C caller keeps `user_data`
    // valid across that single completion, and must not destroy the consumer while the op
    // is in flight (`kafka_consumer_Consumer_destroy`'s documented precondition).
    unsafe { async_void_op(consumer, callback, user_data, |c| c.unsubscribe()) };
}

// ── assign (async; sync already defined above) ──

/// Assigns the consumer to a set of `(topic, partition)` pairs (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics` `count` valid C strings,
/// `partitions` `count` `i32` values.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, exactly what this function's `# Safety`
    // promises; it reads `count.max(0)` entries, so a non-positive `count` yields an empty
    // vec, and it tolerates no NULL array or entry (the contract requires valid entries
    // whenever `count > 0`). Everything is copied into owned `TopicPartition`s before
    // anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `tps` and holds no raw C pointers. `callback` was supplied by the C caller along with
    // `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4
    // forbids checking required parameters); the helper fires it inline on this thread with
    // a fresh `box_error` handle if the guard is rejected, and otherwise exactly once from
    // the dispatcher thread after `release`, with null or a fresh error handle the callback
    // owns. Under the one-shot `OperationCallbackTarget` convention the C caller keeps
    // `user_data` valid across that single completion, and must not destroy the consumer
    // while the op is in flight (`kafka_consumer_Consumer_destroy`'s documented
    // precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.assign(tps)) };
}

// ── seek ──

/// Seeks a single partition to `offset` (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `tp` and the
    // `offset` value, no raw C pointers, and the helper acquires the access guard (or
    // returns a `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard`
    // while `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.seek_with_offset(tp, offset))) }
}

/// Seeks a single partition to `offset` (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_async(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `tp` and the `offset` value, no raw C pointers. `callback` was supplied by the C
    // caller along with `user_data` (a pair this function's `# Safety` leaves implicit;
    // CLAUDE.md FFI §4 forbids checking required parameters); the helper fires it inline on
    // this thread with a fresh `box_error` handle if the guard is rejected, and otherwise
    // exactly once from the dispatcher thread after `release`, with null or a fresh error
    // handle the callback owns. Under the one-shot `OperationCallbackTarget` convention the
    // C caller keeps `user_data` valid across that single completion, and must not destroy
    // the consumer while the op is in flight (`kafka_consumer_Consumer_destroy`'s
    // documented precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.seek_with_offset(tp, offset)) };
}

/// Seeks a single partition to `offset` with commit metadata / leader epoch
/// (sync). Pass `metadata == NULL` for no metadata and `leader_epoch < 0` for
/// no leader epoch.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string; `metadata` null
/// or a valid C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_metadata(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    metadata: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    let metadata_str = if metadata.is_null() {
        String::new()
    } else {
        // SAFETY: `metadata` is non-null (checked above) and, per this function's `#
        // Safety`, null or a valid NUL-terminated C string; the bytes are copied into an
        // owned `String` immediately so the borrow does not outlive this statement.
        unsafe { CStr::from_ptr(metadata) }.to_string_lossy().to_string()
    };
    let epoch = if leader_epoch < 0 { None } else { Some(leader_epoch) };
    let oam = match OffsetAndMetadata::with_leader_epoch_metadata(offset, epoch, metadata_str) {
        Ok(o) => o,
        Err(e) => return box_error(e),
    };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `tp` and `oam`,
    // no raw C pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.seek_with_offset_and_metadata(tp, oam))) }
}

/// Seeks a single partition to `offset` with commit metadata / leader epoch
/// (async). Pass `metadata == NULL` for no metadata and `leader_epoch < 0` for
/// no leader epoch.
///
/// # Safety
///
/// `consumer` must be a valid handle; `topic` a valid C string; `metadata` null
/// or a valid C string.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_metadata_async(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    metadata: *const c_char,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    let metadata_str = if metadata.is_null() {
        String::new()
    } else {
        // SAFETY: `metadata` is non-null (checked above) and, per this function's `#
        // Safety`, null or a valid NUL-terminated C string; the bytes are copied into an
        // owned `String` immediately so the borrow does not outlive this statement.
        unsafe { CStr::from_ptr(metadata) }.to_string_lossy().to_string()
    };
    let epoch = if leader_epoch < 0 { None } else { Some(leader_epoch) };
    let oam = match OffsetAndMetadata::with_leader_epoch_metadata(offset, epoch, metadata_str) {
        Ok(o) => o,
        Err(e) => {
            // Marshaling failed: fire inline with the error (no guard taken).
            // SAFETY: `callback` was supplied by the C caller along with `user_data` (a
            // pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4 forbids
            // checking required parameters). This is the documented marshaling-failure
            // path: the pair fires synchronously on the calling thread before any guard is
            // taken, so `user_data` is still valid under the one-shot convention, and
            // `box_error(e)` is a fresh handle the callback owns. The function returns
            // right after, `async_void_op` is never reached and the `ffi_guard` fires only
            // on a panic, so this is the single invocation.
            unsafe { callback(box_error(e), user_data) };
            return;
        },
    };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `tp` and `oam`, no raw C pointers. `callback` was supplied by the C caller along with
    // `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4
    // forbids checking required parameters) and has not fired yet on this path (the
    // marshaling-failure branch returned); the helper fires it inline on this thread with a
    // fresh `box_error` handle if the guard is rejected, and otherwise exactly once from
    // the dispatcher thread after `release`, with null or a fresh error handle the callback
    // owns. Under the one-shot `OperationCallbackTarget` convention the C caller keeps
    // `user_data` valid across that single completion, and must not destroy the consumer
    // while the op is in flight (`kafka_consumer_Consumer_destroy`'s documented
    // precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.seek_with_offset_and_metadata(tp, oam)) };
}

/// Seeks the given partitions to their beginning offsets (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `tps`, no raw C
    // pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.seek_to_beginning(&tps).await })) }
}

/// Seeks the given partitions to their beginning offsets (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `tps` and holds no raw C pointers. `callback` was supplied by the C caller along with
    // `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4
    // forbids checking required parameters); the helper fires it inline on this thread with
    // a fresh `box_error` handle if the guard is rejected, and otherwise exactly once from
    // the dispatcher thread after `release`, with null or a fresh error handle the callback
    // owns. Under the one-shot `OperationCallbackTarget` convention the C caller keeps
    // `user_data` valid across that single completion, and must not destroy the consumer
    // while the op is in flight (`kafka_consumer_Consumer_destroy`'s documented
    // precondition).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `tps`, no raw C
    // pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.seek_to_end(&tps).await })) }
}

/// Seeks the given partitions to their end offsets (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `tps` and holds no raw C pointers. `callback` was supplied by the C caller along with
    // `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4
    // forbids checking required parameters); the helper fires it inline on this thread with
    // a fresh `box_error` handle if the guard is rejected, and otherwise exactly once from
    // the dispatcher thread after `release`, with null or a fresh error handle the callback
    // owns. Under the one-shot `OperationCallbackTarget` convention the C caller keeps
    // `user_data` valid across that single completion, and must not destroy the consumer
    // while the op is in flight (`kafka_consumer_Consumer_destroy`'s documented
    // precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| async move { c.seek_to_end(&tps).await }) };
}

// ── pause / resume ──

/// Pauses fetching for the given partitions (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `tps`, no raw C
    // pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.pause(&tps).await })) }
}

/// Pauses fetching for the given partitions (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `tps` and holds no raw C pointers. `callback` was supplied by the C caller along with
    // `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4
    // forbids checking required parameters); the helper fires it inline on this thread with
    // a fresh `box_error` handle if the guard is rejected, and otherwise exactly once from
    // the dispatcher thread after `release`, with null or a fresh error handle the callback
    // owns. Under the one-shot `OperationCallbackTarget` convention the C caller keeps
    // `user_data` valid across that single completion, and must not destroy the consumer
    // while the op is in flight (`kafka_consumer_Consumer_destroy`'s documented
    // precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| async move { c.pause(&tps).await }) };
}

/// Resumes fetching for the given partitions (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `tps`, no raw C
    // pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(async move { c.resume(&tps).await })) }
}

/// Resumes fetching for the given partitions (async).
///
/// # Safety
///
/// `consumer` must be a valid handle; `topics`/`partitions` `count` entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `tps` and holds no raw C pointers. `callback` was supplied by the C caller along with
    // `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4
    // forbids checking required parameters); the helper fires it inline on this thread with
    // a fresh `box_error` handle if the guard is rejected, and otherwise exactly once from
    // the dispatcher thread after `release`, with null or a fresh error handle the callback
    // owns. Under the one-shot `OperationCallbackTarget` convention the C caller keeps
    // `user_data` valid across that single completion, and must not destroy the consumer
    // while the op is in flight (`kafka_consumer_Consumer_destroy`'s documented
    // precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| async move { c.resume(&tps).await }) };
}

// ── commit ──

/// Commits the consumed offsets synchronously (sync; no offsets argument means
/// commit the current positions).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures nothing, and the helper acquires
    // the access guard (or returns a `LocalConcurrentModification` error handle) and holds
    // it via `ReleaseGuard` while `block_on` drives `commit_sync` on this calling thread.
    unsafe { sync_void_op(consumer, |c| Box::pin(c.commit_sync())) }
}

/// Commits the current positions asynchronously (async dispatch of
/// [`kafka_consumer_Consumer_commit_sync`]; the callback fires when the commit
/// has completed). One-operation-in-flight; reuses [`kafka_consumer_Consumer_op_callback_t`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send` data: the closure captures nothing.
    // `callback` was supplied by the C caller along with `user_data` (a pair this
    // function's `# Safety` leaves implicit; CLAUDE.md FFI §4 forbids checking required
    // parameters); the helper fires it inline on this thread with a fresh `box_error`
    // handle if the guard is rejected, and otherwise exactly once from the dispatcher
    // thread after `release`, with null or a fresh error handle the callback owns. Under
    // the one-shot `OperationCallbackTarget` convention the C caller keeps `user_data`
    // valid across that single completion, and must not destroy the consumer while the op
    // is in flight (`kafka_consumer_Consumer_destroy`'s documented precondition).
    unsafe { async_void_op(consumer, callback, user_data, |c| c.commit_sync()) };
}

/// Reads `count` `(topic, partition, offset, leader_epoch, metadata)` tuples
/// from parallel C arrays into a `HashMap<TopicPartition, OffsetAndMetadata>`.
/// `metadata` may be null (whole array) or contain null entries (per element);
/// `leader_epoch` entries `< 0` mean no epoch.
///
/// Shared with the producer FFI, so
/// `kafka_producer_Producer_send_offsets_to_transaction` marshals its offsets
/// exactly like `kafka_consumer_Consumer_commit_sync_offsets`.
///
/// # Safety
///
/// `topics` must point to `count` valid C strings and `partitions` / `offsets` to
/// `count` values; `leader_epochs` must be null or have `count` entries;
/// `metadata` must be null or have `count` entries, each null or a valid C string.
pub(crate) unsafe fn read_offset_map(
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
    let n = count.max(0) as usize;
    let mut map = HashMap::with_capacity(n);
    for i in 0..n {
        // SAFETY: `i < n` where `n = count.max(0)`, so a non-positive `count` reads
        // nothing, and every caller's `# Safety`
        // (`kafka_consumer_Consumer_commit_sync_offsets` and siblings: "arrays `count`
        // valid entries", with only `metadata` documented as nullable) gives `topics`
        // `count` readable `*const c_char` entries, so this one-pointer read is in bounds.
        // `topics` itself is not null-checked; this helper's own `# Safety` ("all non-null
        // arrays") does not require it non-null, only the entry points' contracts do.
        let topic_ptr = unsafe { *topics.add(i) };
        // SAFETY: `topic_ptr` is the `topics[i]` entry just read, which the callers' `#
        // Safety` contracts require to be a valid NUL-terminated C string (this helper
        // reads `(topic, partition, offset, leader_epoch, metadata)` tuples and documents
        // only `metadata` as nullable, whole-array or per entry); there is no per-entry
        // null check for topics, and the bytes are copied into an owned `String`
        // immediately.
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        // SAFETY: `i < n = count.max(0)` and, per the `# Safety` of this helper and of its
        // callers ("arrays `count` valid entries"), `partitions` has `count` valid `i32`
        // entries, so one `i32` is read in bounds. `partitions` is not null-checked; the
        // entry points' contracts require it non-null whenever `count > 0`.
        let partition = unsafe { *partitions.add(i) };
        // SAFETY: `i < n = count.max(0)` and, per the `# Safety` of this helper and of its
        // callers ("arrays `count` valid entries"), `offsets` has `count` valid `i64`
        // entries, so one `i64` is read in bounds. `offsets` is not null-checked; the entry
        // points' contracts require it non-null whenever `count > 0`.
        let offset = unsafe { *offsets.add(i) };
        let epoch = if leader_epochs.is_null() {
            None
        } else {
            // SAFETY: `leader_epochs` is non-null (checked above; the null case yields
            // `None`) and, per this helper's `# Safety`, a non-null array has `count` valid
            // entries; `i < n = count.max(0)`, so one `i32` is read in bounds.
            let e = unsafe { *leader_epochs.add(i) };
            if e < 0 { None } else { Some(e) }
        };
        let meta = if metadata.is_null() {
            String::new()
        } else {
            // SAFETY: `metadata` is non-null (checked above; the null case yields an empty
            // string) and, per this helper's `# Safety` and the callers' docs ("`metadata`
            // may be null (whole array or individual entries)"), a non-null array has
            // `count` valid `*const c_char` entries; `i < n = count.max(0)`, so one pointer
            // is read in bounds.
            let m = unsafe { *metadata.add(i) };
            if m.is_null() {
                String::new()
            } else {
                // SAFETY: `m` is the `metadata[i]` entry just read and is non-null (checked
                // above; a NULL entry yields an empty string); per the callers' contracts a
                // non-null entry is a valid NUL-terminated C string, and the bytes are
                // copied into an owned `String` immediately.
                unsafe { CStr::from_ptr(m) }.to_string_lossy().to_string()
            }
        };
        let oam = OffsetAndMetadata::with_leader_epoch_metadata(offset, epoch, meta)?;
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_offset_map` requires every non-null array to have `count` valid
    // entries; this function's `# Safety` promises "arrays `count` valid entries", and its
    // docs allow only `metadata` to be null (whole array or per entry) and `leader_epoch <
    // 0` for no epoch, which is exactly what the helper tolerates. It reads `count.max(0)`
    // entries, so a non-positive `count` reads nothing; everything is copied into an owned
    // map, and a semantic failure (e.g. a negative offset) surfaces as `Err` and is
    // returned as an error handle.
    let map = match unsafe { read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `map`, no raw C
    // pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.commit_sync_with_offsets(map))) }
}

/// Commits specific offsets asynchronously (async dispatch of
/// [`kafka_consumer_Consumer_commit_sync_offsets`]; the callback fires when the
/// commit has completed). One-operation-in-flight; reuses
/// [`kafka_consumer_Consumer_op_callback_t`].
///
/// # Safety
///
/// `consumer` a valid handle; arrays `count` valid entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
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
    // SAFETY: `read_offset_map` requires every non-null array to have `count` valid
    // entries; this function's `# Safety` promises "arrays `count` valid entries", and its
    // docs allow only `metadata` to be null (whole array or per entry) and `leader_epoch <
    // 0` for no epoch, which is exactly what the helper tolerates. It reads `count.max(0)`
    // entries, so a non-positive `count` reads nothing; everything is copied into an owned
    // map, and a semantic failure surfaces as `Err`, handled on the marshaling-failure path
    // below.
    let map = match unsafe { read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count) } {
        Ok(m) => m,
        Err(e) => {
            // Marshaling failed: fire inline with the error (no guard taken).
            // SAFETY: `callback` was supplied by the C caller along with `user_data` (a
            // pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4 forbids
            // checking required parameters). This is the documented marshaling-failure
            // path: the pair fires synchronously on the calling thread before any guard is
            // taken, so `user_data` is still valid under the one-shot convention, and
            // `box_error(e)` is a fresh handle the callback owns. The function returns
            // right after, `async_void_op` is never reached and the `ffi_guard` fires only
            // on a panic, so this is the single invocation.
            unsafe { callback(box_error(e), user_data) };
            return;
        },
    };
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send`, already-marshaled data: the closure owns
    // `map` and holds no raw C pointers. `callback` was supplied by the C caller along with
    // `user_data` (a pair this function's `# Safety` leaves implicit; CLAUDE.md FFI §4
    // forbids checking required parameters) and has not fired yet on this path (the
    // marshaling-failure branch returned); the helper fires it inline on this thread with a
    // fresh `box_error` handle if the guard is rejected, and otherwise exactly once from
    // the dispatcher thread after `release`, with null or a fresh error handle the callback
    // owns. Under the one-shot `OperationCallbackTarget` convention the C caller keeps
    // `user_data` valid across that single completion, and must not destroy the consumer
    // while the op is in flight (`kafka_consumer_Consumer_destroy`'s documented
    // precondition).
    unsafe { async_void_op(consumer, callback, user_data, move |c| c.commit_sync_with_offsets(map)) };
}

/// Commits the consumed offsets asynchronously (sync call, returns once the
/// async commit is initiated). The callback-taking variants are
/// [`kafka_consumer_Consumer_commit_async_with_callback`] and
/// [`kafka_consumer_Consumer_commit_async_offsets_with_callback`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures nothing, and the helper acquires
    // the access guard (or returns a `LocalConcurrentModification` error handle) and holds
    // it via `ReleaseGuard` while `block_on` drives `commit_async` on this calling thread.
    unsafe { sync_void_op(consumer, |c| Box::pin(c.commit_async())) }
}

// ── commit_async with an OffsetCommitCallback ──

/// Completion callback for [`kafka_consumer_Consumer_commit_async_with_callback`]
/// and [`kafka_consumer_Consumer_commit_async_offsets_with_callback`] — the C
/// equivalent of Java's `OffsetCommitCallback.onComplete(offsets, exception)`.
///
/// `offsets` is the (always non-null) map of offsets the commit applies to, and
/// `error` is null on success / non-null on failure — mirroring Java's
/// "exception == null means success". **The callee owns both handles** and must
/// free them with [`kafka_consumer_OffsetMap_destroy`] /
/// [`kafka_common_Error_destroy`] (matching the "callbacks own the handles
/// delivered to them" convention of the async consumer callbacks).
///
/// Invoked on the consumer's callback dispatcher thread, exactly once per
/// registration, when the commit completes. See
/// [`kafka_consumer_Consumer_commit_async_with_callback`] for the full contract.
///
/// Named after the **Java** method it carries the callback for
/// (`commitAsync(OffsetCommitCallback)`) rather than after either C entry point,
/// since one typedef serves both (C has no overloading, so the Java overloads
/// become `..._commit_async_with_callback` and
/// `..._commit_async_offsets_with_callback`); the CLAUDE.md §4 spelling would be
/// the doubly-suffixed `..._commit_async_with_callback_callback_t`. It also
/// deliberately does not reuse the identically-shaped
/// [`kafka_consumer_Consumer_committed_callback_t`], whose map is a *query
/// result* rather than the offsets a commit applied to.
pub type kafka_consumer_Consumer_commit_async_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_OffsetMap_t, *mut kafka_common_Error_t, *mut c_void);

/// Release hook for the `user_data` handed to
/// [`kafka_consumer_Consumer_commit_async_with_callback`] /
/// [`kafka_consumer_Consumer_commit_async_offsets_with_callback`].
///
/// Fired exactly once, after the registration is dropped by the consumer (i.e.
/// after the commit completed and the callback returned, or immediately if the
/// call failed before the callback could be registered). May run on **any**
/// thread — the app thread making the call, a consumer worker, or the dispatcher
/// thread — so it must be thread-agnostic.
///
/// The two `_with_callback` entry points spell this signature out inline as
/// `Option<unsafe extern "C" fn(*mut c_void)>` (a nullable function pointer)
/// rather than naming this alias: cbindgen only recognizes `Option<...>` as a
/// nullable function pointer when the bare `fn` type is written literally, and
/// emits an un-compilable `Option<...>` for an aliased one. The C signature is
/// identical either way — this typedef is the name C callers should use.
pub type kafka_consumer_Consumer_commit_async_user_data_destroy_t = unsafe extern "C" fn(*mut c_void);

/// Adapts a C commit-callback triple to the core [`OffsetCommitCallback`] trait.
///
/// Holds the `user_data` inside a [`CallbackTarget`], so the optional
/// `user_data_destroy` hook fires exactly once when the core drops the
/// registration (the trait object is stored as `Arc<dyn OffsetCommitCallback>`).
struct FfiOffsetCommitCallback {
    callback: kafka_consumer_Consumer_commit_async_callback_t,
    /// Owns `user_data` (and fires the destroy hook on drop).
    target: CallbackTarget,
    /// Sender for the consumer's completion-dispatch queue.
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
}

#[async_trait]
impl OffsetCommitCallback for FfiOffsetCommitCallback {
    async fn on_complete(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
        // Clone the borrowed inputs into owned, `Send` values *before* the job:
        // the C handles themselves are built inside the job, on the dispatcher
        // thread, so no raw pointer is ever held across an `.await` (the same
        // reason `async_value_op` builds its handles in `complete`).
        let offsets = offsets.clone();
        let error = error.cloned();
        let callback = self.callback;
        // Capture the whole `Send` wrapper rather than the raw pointer field
        // (Rust 2021 disjoint closure capture would otherwise grab the latter).
        let user_data = SendUserData(self.target.user_data);
        // Wait for the C callback to return before returning from `on_complete`:
        // Java runs `onComplete` on the thread calling `poll()`/`commitSync()`,
        // so the FFI call must not return first — and firing-and-forgetting
        // would let the C caller release `user_data` while the job is queued.
        dispatch_and_wait(&self.completion_tx, move || {
            let user_data = user_data;
            let map = box_offset_map(offsets);
            let error = error.map(box_error).unwrap_or(std::ptr::null_mut());
            // SAFETY: `callback` was supplied by the C caller along with
            // `user_data`; both handles are freshly allocated and handed over.
            unsafe { callback(map, error, user_data.into_ptr()) };
        })
        .await;
    }
}

/// Builds the core-side commit callback from the C triple.
///
/// Constructing the adapter **transfers ownership of `user_data`** to it, so
/// simply dropping the returned `Arc` (instead of handing it to the consumer)
/// fires `user_data_destroy` — which is what makes the "the destroy hook runs
/// even when the call fails before registering the callback" contract hold
/// without any explicit cleanup path.
fn make_commit_callback(
    callback: kafka_consumer_Consumer_commit_async_callback_t,
    user_data: *mut c_void,
    // Same type the entry points spell out inline; named here so the C-facing
    // alias is tied to the Rust code that consumes it.
    user_data_destroy: Option<kafka_consumer_Consumer_commit_async_user_data_destroy_t>,
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
) -> Arc<dyn OffsetCommitCallback> {
    Arc::new(FfiOffsetCommitCallback {
        callback,
        target: CallbackTarget { user_data, destroy: user_data_destroy },
        completion_tx,
    })
}

/// Commits the consumed offsets asynchronously, notifying `callback` when the
/// commit completes — Java's `commitAsync(OffsetCommitCallback)`.
///
/// Like [`kafka_consumer_Consumer_commit_async`] this call is synchronous and
/// returns as soon as the commit has been initiated (null on success, a non-null
/// error handle on failure).
/// A caught panic returns a `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` error
/// handle, and the guard itself never invokes `callback`. Against a real broker
/// the callback is handed off when the consumer spawns the task that waits for
/// the commit's result. A panic before that drops the registration during the
/// unwind: `callback` never fires, and `user_data_destroy` fires exactly once,
/// before this function returns. A panic raised after the task is queued, even
/// inside the spawn, does not cancel it: `callback` can still fire later with
/// the commit's outcome, although this call returned an error, and
/// `user_data_destroy` still fires exactly once, after `callback` if it fires.
/// On a return that reports a panic, do not release `user_data` yourself:
/// `user_data_destroy` releases it, or, with no destroy hook, `callback` does
/// if it fires.
///
/// # Callback contract
///
/// - Fires **exactly once** per successful call, on the consumer's callback
///   dispatcher thread, when the commit completes.
/// - `offsets` and a non-null `error` are owned by the callee (see
///   [`kafka_consumer_Consumer_commit_async_callback_t`]).
/// - The callback must **not** call the plain `kafka_consumer_Consumer_*` API of
///   this consumer: the access guard is held for the duration of the commit, so
///   those calls fail with a `ConcurrentModification` error. Use
///   [`kafka_consumer_ConsumerHandle_t`] (obtained with
///   [`kafka_consumer_Consumer_handle`]) instead — it bypasses the guard on
///   purpose and is the sanctioned reentrancy path, matching Java, where
///   `acquire()` is reentrant for the polling thread and `onComplete` therefore
///   may call `seek(...)` / `commitSync()` on the consumer.
/// - Independently of that: a callback that block-waits on *consumer progress*
///   (e.g. spins until another thread's `poll` returns) deadlocks the dispatcher
///   queue, because the callback runs on the single dispatcher thread that must
///   also deliver every other callback of this consumer.
/// - On a `MockConsumer` the core invokes the callback inline during the commit
///   (with `error` always null), so the callback has already run by the time this
///   function returns. Against a real broker the commit completes later, on a
///   consumer task, and the callback fires then.
///
/// # `user_data` ownership
///
/// `user_data` is handed to the consumer for the lifetime of the registration.
/// Pass a non-null `user_data_destroy` to be notified when it is released; the
/// hook fires exactly once, on an unspecified thread, and fires **even when this
/// function returns an error** (the transfer is unconditional). Pass null to
/// retain ownership on the C side.
///
/// # Safety
///
/// `consumer` must be a valid handle; `callback` must be a valid function
/// pointer and `user_data` must stay valid until `user_data_destroy` fires (or,
/// with no destroy hook, until the callback has run).
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_callback(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_commit_async_callback_t,
    user_data: *mut c_void,
    user_data_destroy: Option<unsafe extern "C" fn(*mut c_void)>,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only to clone `completion_tx`
    // within this synchronous call, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    let cb = make_commit_callback(callback, user_data, user_data_destroy, h.completion_tx.clone());
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only `cb`, an `Arc<dyn
    // OffsetCommitCallback>` whose `user_data` is owned by a `CallbackTarget`
    // (`Send`/`Sync` by its own documented contract) rather than a bare raw pointer, and
    // the helper acquires the access guard (or returns a `LocalConcurrentModification`
    // error handle, dropping `cb` and thereby firing `user_data_destroy`) and holds it via
    // `ReleaseGuard` while `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.commit_async_with_callback(cb))) }
}

/// Commits specific offsets asynchronously, notifying `callback` when the commit
/// completes — Java's
/// `commitAsync(Map<TopicPartition, OffsetAndMetadata>, OffsetCommitCallback)`.
///
/// Offsets are passed as the same parallel arrays as
/// [`kafka_consumer_Consumer_commit_sync_offsets`]: `metadata` may be null (whole
/// array or individual entries) and a `leader_epoch` entry `< 0` means no epoch.
///
/// The callback and `user_data` contracts are identical to
/// [`kafka_consumer_Consumer_commit_async_with_callback`]. In particular, if the
/// offsets fail to marshal (e.g. a negative offset) this returns the error
/// **without registering the callback** — the callback never fires, but
/// `user_data_destroy` still does.
/// A caught panic returns a `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` error
/// handle, and the guard itself never invokes `callback`. Against a real broker
/// the callback is handed off when the consumer spawns the task that waits for
/// the commit's result. A panic before that drops the registration during the
/// unwind: `callback` never fires, and `user_data_destroy` fires exactly once,
/// before this function returns. A panic raised after the task is queued, even
/// inside the spawn, does not cancel it: `callback` can still fire later with
/// the commit's outcome, although this call returned an error, and
/// `user_data_destroy` still fires exactly once, after `callback` if it fires.
/// On a return that reports a panic, do not release `user_data` yourself:
/// `user_data_destroy` releases it, or, with no destroy hook, `callback` does
/// if it fires.
///
/// # Safety
///
/// `consumer` must be a valid handle; the arrays must have `count` valid
/// entries; see [`kafka_consumer_Consumer_commit_async_with_callback`] for the
/// `callback` / `user_data` requirements.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_offsets_with_callback(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
    callback: kafka_consumer_Consumer_commit_async_callback_t,
    user_data: *mut c_void,
    user_data_destroy: Option<unsafe extern "C" fn(*mut c_void)>,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only to clone `completion_tx`
    // within this synchronous call, during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    // Build the adapter (which takes ownership of `user_data`) BEFORE anything
    // that can fail, so every early return drops it and fires the destroy hook.
    let cb = make_commit_callback(callback, user_data, user_data_destroy, h.completion_tx.clone());
    // SAFETY: `read_offset_map` requires every non-null array to have `count` valid
    // entries; this function's `# Safety` promises the arrays have `count` valid entries,
    // and its docs allow only `metadata` to be null (whole array or per entry) and
    // `leader_epoch < 0` for no epoch, which is exactly what the helper tolerates. It reads
    // `count.max(0)` entries, so a non-positive `count` reads nothing; everything is copied
    // into an owned map, and an `Err` is returned as an error handle, dropping the
    // already-built `cb` so `user_data_destroy` still fires as documented.
    let map = match unsafe { read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `map` and `cb`
    // (whose `user_data` is owned by a `CallbackTarget`, not a bare raw pointer), and the
    // helper acquires the access guard (or returns a `LocalConcurrentModification` error
    // handle, dropping `cb` and thereby firing `user_data_destroy`) and holds it via
    // `ReleaseGuard` while `block_on` drives the op on this calling thread.
    unsafe { sync_void_op(consumer, move |c| Box::pin(c.commit_async_with_offsets_callback(map, cb))) }
}

// ── enforce_rebalance ──

/// Triggers a rebalance (sync). `reason` may be null. Under KIP-848 this
/// returns an unsupported-version error, matching Java.
///
/// # Safety
///
/// `consumer` a valid handle; `reason` null or a valid C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance(
    consumer: *const kafka_consumer_Consumer_t,
    reason: *const c_char,
) -> *mut kafka_common_Error_t {
    let reason_str = if reason.is_null() {
        None
    } else {
        // SAFETY: `reason` is non-null (checked above) and, per this function's `# Safety`,
        // null or a valid NUL-terminated C string; the bytes are copied into an owned
        // `String` immediately so the borrow does not outlive this statement.
        Some(unsafe { CStr::from_ptr(reason) }.to_string_lossy().to_string())
    };
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures only the owned `reason_str`, no
    // raw C pointers, and the helper acquires the access guard (or returns a
    // `LocalConcurrentModification` error handle) and holds it via `ReleaseGuard` while
    // `block_on` drives the op on this calling thread.
    unsafe {
        sync_void_op(consumer, move |c| {
            Box::pin(async move {
                // The C entry point collapses Java's two overloads behind a
                // nullable `reason`; branch onto the matching Rust method.
                match reason_str {
                    Some(r) => c.enforce_rebalance_with_reason(&r).await,
                    None => c.enforce_rebalance().await,
                }
            })
        })
    }
}

// ── close ──

/// Closes the consumer with the default timeout (sync).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `sync_void_op` requires `consumer` to be a valid handle, which this
    // function's `# Safety` promises; the closure captures nothing, and the helper acquires
    // the access guard (or returns a `LocalConcurrentModification` error handle) and holds
    // it via `ReleaseGuard` while `block_on` drives `close` on this calling thread.
    unsafe { sync_void_op(consumer, |c| Box::pin(c.close())) }
}

/// Closes the consumer asynchronously (default timeout).
///
/// # Safety
///
/// `consumer` must be a valid handle.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_op_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `async_void_op` requires `consumer` to be a valid handle (this function's `#
    // Safety`) and an `op` capturing only `Send` data: the closure captures nothing.
    // `callback` was supplied by the C caller along with `user_data` (a pair this
    // function's `# Safety` leaves implicit; CLAUDE.md FFI §4 forbids checking required
    // parameters); the helper fires it inline on this thread with a fresh `box_error`
    // handle if the guard is rejected, and otherwise exactly once from the dispatcher
    // thread after `release`, with null or a fresh error handle the callback owns. Under
    // the one-shot `OperationCallbackTarget` convention the C caller keeps `user_data`
    // valid across that single completion, and must not destroy the consumer while the op
    // is in flight (`kafka_consumer_Consumer_destroy`'s documented precondition).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    out_position: *mut i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard`, `block_on` and `consumer_mut`),
    // during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned an error handle) and `_g: ReleaseGuard` holds the
    // guard until this function returns, after `block_on` has driven the returned future to
    // completion, so the `&mut` is exclusive at this instant per the single-owner
    // `acquire()` design documented at the `SAFETY` comment on `FfiConsumerHandle`'s
    // `unsafe impl Send`.
    match h.runtime.block_on(unsafe { consumer_mut(h).position(&tp) }) {
        Ok(pos) => {
            if !out_position.is_null() {
                // SAFETY: `out_position` is non-null (checked above) and, per this
                // function's `# Safety`, valid, i.e. writable for one `i64`; exactly one
                // element is written.
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
    unsafe extern "C" fn(i64, *mut kafka_common_Error_t, *mut c_void);

/// Returns the current position of `(topic, partition)` asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_position`].
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(0, box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_async(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    callback: kafka_consumer_Consumer_position_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    // SAFETY: `async_value_op` requires `consumer` to be a valid handle from a consumer
    // constructor, which this function's `# Safety` promises; `op` captures only the owned
    // `tp`. The block also encloses the `complete` closure's call of `callback`, which the
    // C caller supplied along with `user_data` (a pair this function's `# Safety` leaves
    // implicit; CLAUDE.md FFI §4 forbids checking required parameters): it runs on the
    // dispatcher thread after `release` (or inline on this thread if the guard is
    // rejected), exactly once; `ud` is the caller's own `user_data` handed back, `pos` is a
    // plain value, and `err` is null or a fresh `box_error` handle the callback owns per
    // `kafka_consumer_Consumer_position_callback_t`. The C caller keeps `user_data` valid
    // across that single completion (one-shot `OperationCallbackTarget` convention) and
    // must not destroy the consumer while the op is in flight
    // (`kafka_consumer_Consumer_destroy`'s documented precondition).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_OffsetMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard`, `block_on` and `consumer_mut`),
    // during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before the op runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned an error handle) and `_g: ReleaseGuard` holds the
    // guard until this function returns, after `block_on` has driven the returned future to
    // completion, so the `&mut` is exclusive at this instant per the single-owner
    // `acquire()` design documented at the `SAFETY` comment on `FfiConsumerHandle`'s
    // `unsafe impl Send`.
    match h.runtime.block_on(unsafe { consumer_mut(h).committed(&tps) }) {
        Ok(map) => {
            if !out_map.is_null() {
                // SAFETY: `out_map` is non-null (checked above) and, per this function's `#
                // Safety`, valid, i.e. writable for one pointer; exactly one element is
                // written, a freshly allocated `kafka_consumer_OffsetMap_t` whose ownership
                // passes to the caller (free with `kafka_consumer_OffsetMap_destroy`).
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
    unsafe extern "C" fn(*mut kafka_consumer_OffsetMap_t, *mut kafka_common_Error_t, *mut c_void);

/// Returns the last committed offsets for the given partitions asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_committed`].
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` valid entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(std::ptr::null_mut(), box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_committed_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` valid entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_value_op` requires `consumer` to be a valid handle from a consumer
    // constructor, which this function's `# Safety` promises; `op` captures only the owned
    // `tps`. The block also encloses the `complete` closure's call of `callback`, which the
    // C caller supplied along with `user_data` (a pair this function's `# Safety` leaves
    // implicit; CLAUDE.md FFI §4 forbids checking required parameters): it runs on the
    // dispatcher thread after `release` (or inline on this thread if the guard is
    // rejected), exactly once; `ud` is the caller's own `user_data` handed back, `map` is a
    // fresh `box_offset_map` handle or null and `err` is null or a fresh `box_error`
    // handle, the callback owning whichever is non-null per
    // `kafka_consumer_Consumer_committed_callback_t`. The C caller keeps `user_data` valid
    // across that single completion (one-shot `OperationCallbackTarget` convention) and
    // must not destroy the consumer while the op is in flight
    // (`kafka_consumer_Consumer_destroy`'s documented precondition).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    timestamps: *const i64,
    count: i32,
    out_map: *mut *mut kafka_consumer_OffsetAndTimestampMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard`, `block_on` and `consumer_mut`),
    // during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    let n = count.max(0) as usize;
    let mut req = HashMap::with_capacity(n);
    for i in 0..n {
        // SAFETY: `i < n = count.max(0)`, so a non-positive `count` reads nothing, and per
        // this function's `# Safety` the arrays have `count` valid entries, so this
        // one-pointer read from `topics` is in bounds; `topics` is not null-checked, which
        // the contract does not require (it must be non-null whenever `count > 0`).
        let topic_ptr = unsafe { *topics.add(i) };
        // SAFETY: `topic_ptr` is the `topics[i]` entry just read, a valid NUL-terminated C
        // string per this function's `# Safety` ("arrays `count` valid entries"); there is
        // no per-entry null tolerance, and the bytes are copied into an owned `String`
        // immediately.
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        // SAFETY: `i < n = count.max(0)` and per this function's `# Safety` `partitions`
        // has `count` valid `i32` entries, so one `i32` is read in bounds; `partitions` is
        // not null-checked, which the contract does not require.
        let partition = unsafe { *partitions.add(i) };
        // SAFETY: `i < n = count.max(0)` and per this function's `# Safety` `timestamps`
        // has `count` valid `i64` entries, so one `i64` is read in bounds; `timestamps` is
        // not null-checked, which the contract does not require.
        let timestamp = unsafe { *timestamps.add(i) };
        req.insert(TopicPartition::new(topic, partition), timestamp);
    }
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned an error handle) and `_g: ReleaseGuard` holds the
    // guard until this function returns, after `block_on` has driven the returned future to
    // completion, so the `&mut` is exclusive at this instant per the single-owner
    // `acquire()` design documented at the `SAFETY` comment on `FfiConsumerHandle`'s
    // `unsafe impl Send`.
    match h.runtime.block_on(unsafe { consumer_mut(h).offsets_for_times(req) }) {
        Ok(map) => {
            if !out_map.is_null() {
                // SAFETY: `out_map` is non-null (checked above) and, per this function's `#
                // Safety`, valid, i.e. writable for one pointer; exactly one element is
                // written, a freshly allocated `kafka_consumer_OffsetAndTimestampMap_t`
                // whose ownership passes to the caller (free with
                // `kafka_consumer_OffsetAndTimestampMap_destroy`).
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
pub(crate) unsafe fn read_timestamps_to_search(
    topics: *const *const c_char,
    partitions: *const i32,
    timestamps: *const i64,
    count: i32,
) -> HashMap<TopicPartition, i64> {
    let n = count.max(0) as usize;
    let mut req = HashMap::with_capacity(n);
    for i in 0..n {
        // SAFETY: `i < n = count.max(0)`, so a non-positive `count` reads nothing, and per
        // this helper's `# Safety` all arrays have `count` valid entries, so this
        // one-pointer read from `topics` is in bounds; `topics` is not null-checked, which
        // the contract does not require (it must be non-null whenever `count > 0`).
        let topic_ptr = unsafe { *topics.add(i) };
        // SAFETY: `topic_ptr` is the `topics[i]` entry just read, a valid NUL-terminated C
        // string per this helper's `# Safety` ("all arrays must have `count` valid
        // entries"); there is no per-entry null tolerance, and the bytes are copied into an
        // owned `String` immediately.
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        // SAFETY: `i < n = count.max(0)` and per this helper's `# Safety` `partitions` has
        // `count` valid `i32` entries, so one `i32` is read in bounds; `partitions` is not
        // null-checked, which the contract does not require.
        let partition = unsafe { *partitions.add(i) };
        // SAFETY: `i < n = count.max(0)` and per this helper's `# Safety` `timestamps` has
        // `count` valid `i64` entries, so one `i64` is read in bounds; `timestamps` is not
        // null-checked, which the contract does not require.
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
    unsafe extern "C" fn(*mut kafka_consumer_OffsetAndTimestampMap_t, *mut kafka_common_Error_t, *mut c_void);

/// Looks up offsets by timestamp asynchronously (one-operation-in-flight).
/// See [`kafka_consumer_Consumer_offsets_for_times`].
///
/// # Safety
///
/// `consumer` a valid handle; arrays `count` valid entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(std::ptr::null_mut(), box_error(err), user_data) }
})]
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
    // SAFETY: `read_timestamps_to_search` requires all three arrays to have `count` valid
    // entries, which this function's `# Safety` promises ("arrays `count` valid entries");
    // it reads `count.max(0)` entries, so a non-positive `count` reads nothing, and it
    // tolerates no NULL array or entry (the contract requires valid entries whenever `count
    // > 0`). Everything is copied into an owned `HashMap` before anything else runs.
    let req = unsafe { read_timestamps_to_search(topics, partitions, timestamps, count) };
    // SAFETY: `async_value_op` requires `consumer` to be a valid handle from a consumer
    // constructor, which this function's `# Safety` promises; `op` captures only the owned
    // `req`. The block also encloses the `complete` closure's call of `callback`, which the
    // C caller supplied along with `user_data` (a pair this function's `# Safety` leaves
    // implicit; CLAUDE.md FFI §4 forbids checking required parameters): it runs on the
    // dispatcher thread after `release` (or inline on this thread if the guard is
    // rejected), exactly once; `ud` is the caller's own `user_data` handed back, `map` is a
    // fresh `box_offset_and_timestamp_map` handle or null and `err` is null or a fresh
    // `box_error` handle, the callback owning whichever is non-null per
    // `kafka_consumer_Consumer_offsets_for_times_callback_t`. The C caller keeps
    // `user_data` valid across that single completion (one-shot `OperationCallbackTarget`
    // convention) and must not destroy the consumer while the op is in flight
    // (`kafka_consumer_Consumer_destroy`'s documented precondition).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_LongOffsetMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard`, `block_on` and `consumer_mut`),
    // during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before the op runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned an error handle) and `_g: ReleaseGuard` holds the
    // guard until this function returns, after `block_on` has driven the returned future to
    // completion, so the `&mut` is exclusive at this instant per the single-owner
    // `acquire()` design documented at the `SAFETY` comment on `FfiConsumerHandle`'s
    // `unsafe impl Send`.
    match h.runtime.block_on(unsafe { consumer_mut(h).beginning_offsets(&tps) }) {
        Ok(map) => {
            if !out_map.is_null() {
                // SAFETY: `out_map` is non-null (checked above) and, per this function's `#
                // Safety`, valid, i.e. writable for one pointer; exactly one element is
                // written, a freshly allocated `kafka_consumer_LongOffsetMap_t` whose
                // ownership passes to the caller (free with
                // `kafka_consumer_LongOffsetMap_destroy`).
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
    unsafe extern "C" fn(*mut kafka_consumer_LongOffsetMap_t, *mut kafka_common_Error_t, *mut c_void);

/// Returns the beginning offsets for the given partitions asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_beginning_offsets`].
///
/// # Safety
///
/// `consumer` a valid handle; `topics`/`partitions` `count` valid entries.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(std::ptr::null_mut(), box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_long_offsets_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` valid entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_value_op` requires `consumer` to be a valid handle from a consumer
    // constructor, which this function's `# Safety` promises; `op` captures only the owned
    // `tps`. The block also encloses the `complete` closure's call of `callback`, which the
    // C caller supplied along with `user_data` (a pair this function's `# Safety` leaves
    // implicit; CLAUDE.md FFI §4 forbids checking required parameters): it runs on the
    // dispatcher thread after `release` (or inline on this thread if the guard is
    // rejected), exactly once; `ud` is the caller's own `user_data` handed back, `map` is a
    // fresh `box_long_offset_map` handle or null and `err` is null or a fresh `box_error`
    // handle, the callback owning whichever is non-null per
    // `kafka_consumer_Consumer_long_offsets_callback_t`. The C caller keeps `user_data`
    // valid across that single completion (one-shot `OperationCallbackTarget` convention)
    // and must not destroy the consumer while the op is in flight
    // (`kafka_consumer_Consumer_destroy`'s documented precondition).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_LongOffsetMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard`, `block_on` and `consumer_mut`),
    // during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before the op runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned an error handle) and `_g: ReleaseGuard` holds the
    // guard until this function returns, after `block_on` has driven the returned future to
    // completion, so the `&mut` is exclusive at this instant per the single-owner
    // `acquire()` design documented at the `SAFETY` comment on `FfiConsumerHandle`'s
    // `unsafe impl Send`.
    match h.runtime.block_on(unsafe { consumer_mut(h).end_offsets(&tps) }) {
        Ok(map) => {
            if !out_map.is_null() {
                // SAFETY: `out_map` is non-null (checked above) and, per this function's `#
                // Safety`, valid, i.e. writable for one pointer; exactly one element is
                // written, a freshly allocated `kafka_consumer_LongOffsetMap_t` whose
                // ownership passes to the caller (free with
                // `kafka_consumer_LongOffsetMap_destroy`).
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
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(std::ptr::null_mut(), box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_async(
    consumer: *const kafka_consumer_Consumer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    callback: kafka_consumer_Consumer_long_offsets_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values, which this function's `# Safety` promises
    // (`topics`/`partitions` `count` valid entries); it reads `count.max(0)` entries, so a
    // non-positive `count` yields an empty vec, and it tolerates no NULL array or entry
    // (the contract requires valid entries whenever `count > 0`). Everything is copied into
    // owned `TopicPartition`s before anything else runs.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    // SAFETY: `async_value_op` requires `consumer` to be a valid handle from a consumer
    // constructor, which this function's `# Safety` promises; `op` captures only the owned
    // `tps`. The block also encloses the `complete` closure's call of `callback`, which the
    // C caller supplied along with `user_data` (a pair this function's `# Safety` leaves
    // implicit; CLAUDE.md FFI §4 forbids checking required parameters): it runs on the
    // dispatcher thread after `release` (or inline on this thread if the guard is
    // rejected), exactly once; `ud` is the caller's own `user_data` handed back, `map` is a
    // fresh `box_long_offset_map` handle or null and `err` is null or a fresh `box_error`
    // handle, the callback owning whichever is non-null per
    // `kafka_consumer_Consumer_long_offsets_callback_t`. The C caller keeps `user_data`
    // valid across that single completion (one-shot `OperationCallbackTarget` convention)
    // and must not destroy the consumer while the op is in flight
    // (`kafka_consumer_Consumer_destroy`'s documented precondition).
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
/// [`kafka_common_PartitionInfoList_t`] to `*out_list`.
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string; `out_list` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    out_list: *mut *mut kafka_common_PartitionInfoList_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard`, `block_on` and `consumer_mut`),
    // during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned an error handle) and `_g: ReleaseGuard` holds the
    // guard until this function returns, after `block_on` has driven the returned future to
    // completion, so the `&mut` is exclusive at this instant per the single-owner
    // `acquire()` design documented at the `SAFETY` comment on `FfiConsumerHandle`'s
    // `unsafe impl Send`.
    match h.runtime.block_on(unsafe { consumer_mut(h).partitions_for(&topic_str) }) {
        Ok(infos) => {
            if !out_list.is_null() {
                // SAFETY: `out_list` is non-null (checked above) and, per this function's
                // `# Safety`, valid, i.e. writable for one pointer; exactly one element is
                // written, a freshly allocated `kafka_common_PartitionInfoList_t` whose
                // ownership passes to the caller (free with
                // `kafka_common_PartitionInfoList_destroy`).
                unsafe { *out_list = box_partition_info_list(infos) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Completion callback for [`kafka_consumer_Consumer_partitions_for_async`]. On
/// success `list` is a non-null [`kafka_common_PartitionInfoList_t`] (free with
/// [`kafka_common_PartitionInfoList_destroy`]) and `error` is null; on failure
/// `list` is null and `error` is non-null. The callback owns whichever is non-null.
pub type kafka_consumer_Consumer_partitions_for_callback_t =
    unsafe extern "C" fn(*mut kafka_common_PartitionInfoList_t, *mut kafka_common_Error_t, *mut c_void);

/// Returns the partition metadata for a topic asynchronously
/// (one-operation-in-flight). See [`kafka_consumer_Consumer_partitions_for`].
///
/// # Safety
///
/// `consumer` a valid handle; `topic` a valid C string.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(std::ptr::null_mut(), box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_async(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    callback: kafka_consumer_Consumer_partitions_for_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    // SAFETY: `async_value_op` requires `consumer` to be a valid handle from a consumer
    // constructor, which this function's `# Safety` promises; `op` captures only the owned
    // `topic_str`. The block also encloses the `complete` closure's call of `callback`,
    // which the C caller supplied along with `user_data` (a pair this function's `# Safety`
    // leaves implicit; CLAUDE.md FFI §4 forbids checking required parameters): it runs on
    // the dispatcher thread after `release` (or inline on this thread if the guard is
    // rejected), exactly once; `ud` is the caller's own `user_data` handed back, `list` is
    // a fresh `box_partition_info_list` handle or null and `err` is null or a fresh
    // `box_error` handle, the callback owning whichever is non-null per
    // `kafka_consumer_Consumer_partitions_for_callback_t`. The C caller keeps `user_data`
    // valid across that single completion (one-shot `OperationCallbackTarget` convention)
    // and must not destroy the consumer while the op is in flight
    // (`kafka_consumer_Consumer_destroy`'s documented precondition).
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
/// On success writes a [`kafka_common_TopicPartitionInfoMap_t`] to `*out_map`.
///
/// # Safety
///
/// `consumer` a valid handle; `out_map` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics(
    consumer: *const kafka_consumer_Consumer_t,
    out_map: *mut *mut kafka_common_TopicPartitionInfoMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard`, `block_on` and `consumer_mut`),
    // during which the C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if let Err(e) = acquire(h) {
        return box_error(e);
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned an error handle) and `_g: ReleaseGuard` holds the
    // guard until this function returns, after `block_on` has driven the returned future to
    // completion, so the `&mut` is exclusive at this instant per the single-owner
    // `acquire()` design documented at the `SAFETY` comment on `FfiConsumerHandle`'s
    // `unsafe impl Send`.
    match h.runtime.block_on(unsafe { consumer_mut(h).list_topics() }) {
        Ok(map) => {
            if !out_map.is_null() {
                // SAFETY: `out_map` is non-null (checked above) and, per this function's `#
                // Safety`, valid, i.e. writable for one pointer; exactly one element is
                // written, a freshly allocated `kafka_common_TopicPartitionInfoMap_t` whose
                // ownership passes to the caller (free with
                // `kafka_common_TopicPartitionInfoMap_destroy`).
                unsafe { *out_map = box_topic_partition_info_map(map) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Completion callback for [`kafka_consumer_Consumer_list_topics_async`]. On
/// success `map` is a non-null [`kafka_common_TopicPartitionInfoMap_t`] (free
/// with [`kafka_common_TopicPartitionInfoMap_destroy`]) and `error` is null;
/// on failure `map` is null and `error` is non-null. The callback owns whichever
/// is non-null.
pub type kafka_consumer_Consumer_list_topics_callback_t =
    unsafe extern "C" fn(*mut kafka_common_TopicPartitionInfoMap_t, *mut kafka_common_Error_t, *mut c_void);

/// Returns metadata for all topics the consumer is authorized to view
/// asynchronously (one-operation-in-flight). See [`kafka_consumer_Consumer_list_topics`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
/// `callback` must be a valid function pointer and `user_data` must stay valid
/// until the callback has run.
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires the C caller's own `callback`/`user_data`
    // pair on the calling thread, exactly as the function's callback contract documents for
    // a synchronous failure; `box_error(err)` is a fresh handle the callback owns. The
    // panic aborted the body before its own callback path ran, so this is the single
    // invocation: a panic in the spawn that hands the callback to a task aborts the process
    // instead (`spawn_callback_task`).
    unsafe { callback(std::ptr::null_mut(), box_error(err), user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_async(
    consumer: *const kafka_consumer_Consumer_t,
    callback: kafka_consumer_Consumer_list_topics_callback_t,
    user_data: *mut c_void,
) {
    // SAFETY: `async_value_op` requires `consumer` to be a valid handle from a consumer
    // constructor, which this function's `# Safety` promises; `op` captures nothing. The
    // block also encloses the `complete` closure's call of `callback`, which the C caller
    // supplied along with `user_data` (a pair this function's `# Safety` leaves implicit;
    // CLAUDE.md FFI §4 forbids checking required parameters): it runs on the dispatcher
    // thread after `release` (or inline on this thread if the guard is rejected), exactly
    // once; `ud` is the caller's own `user_data` handed back, `map` is a fresh
    // `box_topic_partition_info_map` handle or null and `err` is null or a fresh
    // `box_error` handle, the callback owning whichever is non-null per
    // `kafka_consumer_Consumer_list_topics_callback_t`. The C caller keeps `user_data`
    // valid across that single completion (one-shot `OperationCallbackTarget` convention)
    // and must not destroy the consumer while the op is in flight
    // (`kafka_consumer_Consumer_destroy`'s documented precondition).
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

/// Returns the current assignment as a [`kafka_common_TopicPartitionList_t`]
/// (free with [`kafka_common_TopicPartitionList_destroy`]), or null on a
/// concurrent-access rejection (the guard could not be acquired).
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assignment(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_TopicPartitionList_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard` and `consumer_mut`), during which the
    // C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned null) and `_g: ReleaseGuard` holds the guard until
    // this function returns; the `&mut` is used only for the synchronous `assignment()`
    // call in this statement, so the access is exclusive per the single-owner `acquire()`
    // design documented at the `SAFETY` comment on `FfiConsumerHandle`'s `unsafe impl
    // Send`.
    let set = unsafe { consumer_mut(h) }.assignment();
    box_topic_partition_list(set)
}

/// Returns a snapshot of the consumer's metrics as a
/// [`kafka_consumer_MetricMap_t`], or null on a concurrent-access rejection.
///
/// Translates Java's `Map<MetricName, ? extends Metric> metrics()`. Each
/// entry's value is measured once, at call time — see
/// [`kafka_consumer_MetricMap_t`] on snapshot vs. live semantics. Free the
/// result with [`kafka_consumer_MetricMap_destroy`].
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_metrics(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_MetricMap_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard` and `consumer_mut`), during which the
    // C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned null) and `_g: ReleaseGuard` holds the guard until
    // this function returns; the `&mut` is used only for the synchronous `metrics()` call
    // in this statement, so the access is exclusive per the single-owner `acquire()` design
    // documented at the `SAFETY` comment on `FfiConsumerHandle`'s `unsafe impl Send`.
    let metrics = unsafe { consumer_mut(h) }.metrics();
    box_metric_map(metrics)
}

/// Returns the current topic subscription as a
/// [`kafka_consumer_StringList_t`], or null on a concurrent-access rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscription(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_StringList_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard` and `consumer_mut`), during which the
    // C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned null) and `_g: ReleaseGuard` holds the guard until
    // this function returns; the `&mut` is used only for the synchronous `subscription()`
    // call in this statement, so the access is exclusive per the single-owner `acquire()`
    // design documented at the `SAFETY` comment on `FfiConsumerHandle`'s `unsafe impl
    // Send`.
    let set = unsafe { consumer_mut(h) }.subscription();
    box_string_list(set)
}

/// Returns the currently paused partitions as a
/// [`kafka_common_TopicPartitionList_t`], or null on a concurrent-access
/// rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_paused(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_common_TopicPartitionList_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard` and `consumer_mut`), during which the
    // C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned null) and `_g: ReleaseGuard` holds the guard until
    // this function returns; the `&mut` is used only for the synchronous `paused()` call in
    // this statement, so the access is exclusive per the single-owner `acquire()` design
    // documented at the `SAFETY` comment on `FfiConsumerHandle`'s `unsafe impl Send`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_group_metadata(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard` and `consumer_mut`), during which the
    // C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned null) and `_g: ReleaseGuard` holds the guard until
    // this function returns; the `&mut` is used only for the synchronous `group_metadata()`
    // call in this statement, so the access is exclusive per the single-owner `acquire()`
    // design documented at the `SAFETY` comment on `FfiConsumerHandle`'s `unsafe impl
    // Send`.
    let meta = unsafe { consumer_mut(h) }.group_metadata();
    box_group_metadata(meta)
}

/// Returns the client id as an owned NUL-terminated C string (free it with
/// [`kafka_consumer_string_destroy`]), or null on a concurrent-access rejection.
///
/// # Safety
///
/// `consumer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_client_id(consumer: *const kafka_consumer_Consumer_t) -> *mut c_char {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard` and `consumer_mut`), during which the
    // C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return std::ptr::null_mut();
    }
    let _g = ReleaseGuard(h);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned null) and `_g: ReleaseGuard` holds the guard until
    // this function returns; the `&mut` is used only for the synchronous `client_id()` call
    // in this statement, whose result is copied into an owned `String`, so the access is
    // exclusive per the single-owner `acquire()` design documented at the `SAFETY` comment
    // on `FfiConsumerHandle`'s `unsafe impl Send`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_current_lag(
    consumer: *const kafka_consumer_Consumer_t,
    topic: *const c_char,
    partition: i32,
    out_lag: *mut i64,
) -> bool {
    // SAFETY: `handle_ref` requires a non-null pointer created by a consumer constructor;
    // per this function's `# Safety`, `consumer` is a valid handle, i.e. the
    // `Box::into_raw` of `build_consumer_handle`. `h` is used only for the duration of this
    // synchronous call (`acquire`, the `ReleaseGuard` and `consumer_mut`), during which the
    // C caller keeps the handle alive.
    let h = unsafe { handle_ref(consumer) };
    if acquire(h).is_err() {
        return false;
    }
    let _g = ReleaseGuard(h);
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated C string
    // for the duration of the call; it is not null-checked, which the contract does not
    // require, and the bytes are copied into an owned `String` immediately so the borrow
    // does not outlive this statement.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);
    // SAFETY: `consumer_mut` requires the access guard to be held: `acquire(h)` succeeded
    // above (the `Err` path returned `false`) and `_g: ReleaseGuard` holds the guard until
    // this function returns; the `&mut` is used only for the synchronous `current_lag(&tp)`
    // call in this statement, so the access is exclusive per the single-owner `acquire()`
    // design documented at the `SAFETY` comment on `FfiConsumerHandle`'s `unsafe impl
    // Send`.
    match unsafe { consumer_mut(h) }.current_lag(&tp) {
        Some(lag) => {
            if !out_lag.is_null() {
                // SAFETY: `out_lag` is non-null (checked above) and, per this function's `#
                // Safety`, valid, i.e. writable for one `i64`; exactly one element is
                // written.
                unsafe { *out_lag = lag };
            }
            true
        },
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI32;

    use super::*;
    use crate::common::MetricName;
    use crate::common::MetricValue;
    use crate::common::metrics::{ClosureGauge, ClosureMeasurable, MetricConfig, MetricValueProvider};
    use crate::common::utils::SystemTime;
    use crate::consumer::ConsumerGroupMetadataImpl;
    use crate::ffi::common::kafka_common_ErrorCode_t::kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE;
    use crate::ffi::common::{kafka_common_Error_code, kafka_common_Error_destroy, kafka_common_Error_message};
    use std::collections::BTreeMap;

    fn metric(name: &str, tags: &[(&str, &str)], provider: MetricValueProvider) -> (MetricName, Arc<KafkaMetric>) {
        let tag_map: BTreeMap<String, String> = tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let mn = MetricName::new(name, "grp", "desc", tag_map);
        let km = KafkaMetric::new(mn.clone(), provider, Arc::new(MetricConfig::new()), Arc::new(SystemTime));
        (mn, Arc::new(km))
    }

    /// `kafka_common_Node_is_fenced` reads Java's `Node.isFenced()`: a broker
    /// that `describeCluster(includeFencedBrokers)` reports fenced answers
    /// `true`, every other node `false`.
    #[test]
    fn node_is_fenced_reads_the_fenced_flag() {
        let fenced = Node::with_rack_is_fenced(1, "h".to_string(), 9092, Some("r1".to_string()), true);
        let active = Node::new(2, "h".to_string(), 9092);
        // SAFETY: Test code: Every accessor is called on a live handle (built by this test or
        // received by the running callback and not yet destroyed); borrowed results are read before
        // their owner is freed, and out-of-range indices exercise the documented null / -1 / 0
        // paths.
        unsafe {
            assert!(kafka_common_Node_is_fenced(
                &fenced as *const Node as *const kafka_common_Node_t
            ));
            assert!(!kafka_common_Node_is_fenced(
                &active as *const Node as *const kafka_common_Node_t
            ));
        }
    }

    /// A group-metadata handle's four accessors read back exactly the
    /// consumer's values, and `destroy` frees it. The handle is built with the
    /// same `box_group_metadata` that `kafka_consumer_Consumer_group_metadata`
    /// uses, because the C API has no constructor.
    #[test]
    fn group_metadata_handle_round_trips_all_fields() {
        let meta = box_group_metadata(Arc::new(
            ConsumerGroupMetadataImpl::with_generation_id_member_id_group_instance_id(
                "my-group",
                42,
                "member-7",
                Some("static-3".to_string()),
            ),
        ));
        assert!(!meta.is_null());
        // SAFETY: Test: `meta` is the non-null (asserted) handle `box_group_metadata`
        // returned above, satisfying `kafka_consumer_ConsumerGroupMetadata_group_id`'s `#
        // Safety` ("a valid group-metadata handle"); the returned pointer is the
        // handle-owned NUL-terminated `group_id_c`, valid until
        // `kafka_consumer_ConsumerGroupMetadata_destroy(meta)` at the end of the test, and
        // it is only read before that.
        let read_group = unsafe { CStr::from_ptr(kafka_consumer_ConsumerGroupMetadata_group_id(meta)) };
        assert_eq!(read_group.to_str().unwrap(), "my-group");
        // SAFETY: Test: `meta` is the live non-null handle built by `box_group_metadata`
        // above, as `kafka_consumer_ConsumerGroupMetadata_generation_id`'s `# Safety`
        // requires; it is destroyed only at the end of the test.
        assert_eq!(unsafe { kafka_consumer_ConsumerGroupMetadata_generation_id(meta) }, 42);
        // SAFETY: Test: `meta` is the live non-null handle built by `box_group_metadata`
        // above, satisfying the accessor's `# Safety`; the returned pointer is the
        // handle-owned NUL-terminated `member_id_c`, valid until the destroy call at the
        // end of the test, and it is only read before that.
        let read_member = unsafe { CStr::from_ptr(kafka_consumer_ConsumerGroupMetadata_member_id(meta)) };
        assert_eq!(read_member.to_str().unwrap(), "member-7");
        // SAFETY: Test: `meta` is the live non-null handle built by `box_group_metadata`
        // above, as `kafka_consumer_ConsumerGroupMetadata_group_instance_id`'s `# Safety`
        // requires; the accessor returns the handle-owned instance-id string or null, and
        // the handle is destroyed only at the end of the test.
        let read_instance = unsafe { kafka_consumer_ConsumerGroupMetadata_group_instance_id(meta) };
        assert!(!read_instance.is_null());
        // SAFETY: Test: `read_instance` is non-null (asserted above) and points at the
        // handle-owned NUL-terminated `group_instance_id_c` of `meta`, valid until
        // `kafka_consumer_ConsumerGroupMetadata_destroy(meta)` on the next line; it is only
        // read here.
        assert_eq!(unsafe { CStr::from_ptr(read_instance) }.to_str().unwrap(), "static-3");
        // SAFETY: Test: `meta` is the handle `box_group_metadata` created above and this is
        // its single destroy, after the last accessor read, satisfying `# Safety` ("null or
        // a valid group-metadata handle"); the pointer is not used afterwards.
        unsafe { kafka_consumer_ConsumerGroupMetadata_destroy(meta) };
    }

    /// An absent `group_instance_id` (a dynamic member has no static-membership
    /// id) surfaces as a null accessor return; an unknown member id is the
    /// empty string, never null.
    #[test]
    fn group_metadata_handle_absent_instance_id_is_null() {
        let meta = box_group_metadata(Arc::new(ConsumerGroupMetadataImpl::new("g")));
        assert!(!meta.is_null());
        // SAFETY: Test: `meta` is the non-null (asserted) handle `box_group_metadata`
        // returned above, satisfying `kafka_consumer_ConsumerGroupMetadata_member_id`'s `#
        // Safety`; the returned pointer is the handle-owned NUL-terminated `member_id_c`
        // (the empty string here), valid until the destroy call at the end of the test, and
        // it is only read before that.
        let read_member = unsafe { CStr::from_ptr(kafka_consumer_ConsumerGroupMetadata_member_id(meta)) };
        assert_eq!(read_member.to_str().unwrap(), "");
        // SAFETY: Test: `meta` is the live non-null handle built by `box_group_metadata`
        // above, as `kafka_consumer_ConsumerGroupMetadata_generation_id`'s `# Safety`
        // requires; it is destroyed only at the end of the test.
        assert_eq!(unsafe { kafka_consumer_ConsumerGroupMetadata_generation_id(meta) }, -1);
        // SAFETY: Test: `meta` is the live non-null handle built by `box_group_metadata`
        // above, as `kafka_consumer_ConsumerGroupMetadata_group_instance_id`'s `# Safety`
        // requires; the metadata has no instance id, so the documented null return is what
        // is asserted, and the handle is destroyed only on the next line.
        assert!(unsafe { kafka_consumer_ConsumerGroupMetadata_group_instance_id(meta) }.is_null());
        // SAFETY: Test: `meta` is the handle `box_group_metadata` created above and this is
        // its single destroy, after the last accessor read, satisfying `# Safety` ("null or
        // a valid group-metadata handle"); the pointer is not used afterwards.
        unsafe { kafka_consumer_ConsumerGroupMetadata_destroy(meta) };
    }

    /// The handle shares the consumer's metadata rather than copying it:
    /// `group_metadata_ref` hands back the very `Arc` it was built from.
    #[test]
    fn group_metadata_handle_shares_the_consumer_arc() {
        let shared: Arc<dyn ConsumerGroupMetadata> = Arc::new(ConsumerGroupMetadataImpl::new("g"));
        let meta = box_group_metadata(Arc::clone(&shared));
        // SAFETY: Test: `group_metadata_ref` requires a valid group-metadata handle; `meta`
        // was built by `box_group_metadata(Arc::clone(&shared))` just above and is
        // destroyed only on the next line, after `Arc::ptr_eq` has finished with the
        // returned reference, so the `&'static Arc` is not held past this expression.
        assert!(Arc::ptr_eq(unsafe { group_metadata_ref(meta) }, &shared));
        // SAFETY: Test: `meta` is the handle `box_group_metadata` created above and this is
        // its single destroy, after `group_metadata_ref`'s borrow ended, satisfying `#
        // Safety` ("null or a valid group-metadata handle"); dropping it releases the
        // handle's `Arc` clone, which the following strong-count assertion checks.
        unsafe { kafka_consumer_ConsumerGroupMetadata_destroy(meta) };
        assert_eq!(Arc::strong_count(&shared), 1);
    }

    /// Every `MetricValue` variant round-trips through `box_metric_map` to the
    /// matching kind + `get_value_*` accessor, and tags are flattened in order.
    #[test]
    fn metric_map_carries_all_value_kinds_and_tags() {
        let mut metrics: HashMap<MetricName, Arc<KafkaMetric>> = HashMap::new();
        let (measurable_name, m) = metric(
            "measurable",
            &[("client-id", "c1"), ("topic", "t")],
            MetricValueProvider::Measurable(Box::new(ClosureMeasurable::new(|_, _| 42.5))),
        );
        metrics.insert(measurable_name.clone(), m);
        for (name, value) in [
            ("as-string", MetricValue::String("hello".to_string())),
            ("as-long", MetricValue::Long(-9_000_000_000)),
            ("as-int", MetricValue::Int(-7)),
        ] {
            let v = value.clone();
            let (n, m) = metric(
                name,
                &[],
                MetricValueProvider::Gauge(Box::new(ClosureGauge::new(move |_, _| v.clone()))),
            );
            metrics.insert(n, m);
        }

        let map = box_metric_map(metrics);
        // SAFETY: Test: `map` is the handle `box_metric_map(metrics)` returned above,
        // satisfying `kafka_consumer_MetricMap_count`'s `# Safety` ("a valid metric-map
        // handle"); it is destroyed once, at the end of the test.
        assert_eq!(unsafe { kafka_consumer_MetricMap_count(map) }, 4);

        // Entry order follows HashMap iteration, so find each by name.
        let mut seen = 0;
        for i in 0..4 {
            // SAFETY: Test: `map` is the live metric-map handle built above, satisfying the
            // accessor's `# Safety`; `i` ranges over `0..4`, the entry count asserted
            // above, so `kafka_consumer_MetricMap_get_name` returns a non-null handle-owned
            // NUL-terminated string (null only out of range), valid until
            // `kafka_consumer_MetricMap_destroy(map)` at the end of the test and copied
            // into an owned `String` here.
            let name = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_name(map, i)) }
                .to_str()
                .unwrap()
                .to_string();
            // SAFETY: Test: `map` is the live metric-map handle built above, satisfying
            // `kafka_consumer_MetricMap_get_value_kind`'s `# Safety`; `i` is within the
            // asserted entry count, and the handle is destroyed only at the end of the
            // test.
            let kind = unsafe { kafka_consumer_MetricMap_get_value_kind(map, i) };
            // Group/description are the same for every fixture entry.
            // SAFETY: Test: `map` is the live metric-map handle built above, satisfying the
            // accessor's `# Safety`; `i` is within the asserted entry count, so
            // `kafka_consumer_MetricMap_get_group` returns a non-null handle-owned string,
            // valid until the destroy at the end of the test and only read here.
            let group = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_group(map, i)) };
            assert_eq!(group.to_str().unwrap(), "grp");
            // SAFETY: Test: `map` is the live metric-map handle built above, satisfying the
            // accessor's `# Safety`; `i` is within the asserted entry count, so
            // `kafka_consumer_MetricMap_get_description` returns a non-null handle-owned
            // string, valid until the destroy at the end of the test and only read here.
            let desc = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_description(map, i)) };
            assert_eq!(desc.to_str().unwrap(), "desc");
            match name.as_str() {
                "measurable" => {
                    assert_eq!(kind, crate::ffi::common::METRIC_VALUE_DOUBLE);
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_value_double`'s `# Safety`;
                    // `i` indexes the "measurable" entry matched by name, and the handle is
                    // destroyed only at the end of the test.
                    assert_eq!(unsafe { kafka_consumer_MetricMap_get_value_double(map, i) }, 42.5);
                    // Tags are sorted (BTreeMap): client-id then topic.
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_tag_count`'s `# Safety`; `i`
                    // is within the asserted entry count, and the handle is destroyed only
                    // at the end of the test.
                    assert_eq!(unsafe { kafka_consumer_MetricMap_get_tag_count(map, i) }, 2);
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_tag_key`'s `# Safety`; the
                    // "measurable" entry at `i` has two tags (asserted just above), so tag
                    // index `0` is in range and the accessor returns a non-null
                    // handle-owned string, valid until the destroy at the end of the test
                    // and only read here.
                    let k0 = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_tag_key(map, i, 0)) };
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_tag_value`'s `# Safety`; the
                    // "measurable" entry at `i` has two tags (asserted just above), so tag
                    // index `0` is in range and the accessor returns a non-null
                    // handle-owned string, valid until the destroy at the end of the test
                    // and only read here.
                    let v0 = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_tag_value(map, i, 0)) };
                    assert_eq!((k0.to_str().unwrap(), v0.to_str().unwrap()), ("client-id", "c1"));
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_tag_key`'s `# Safety`; the
                    // "measurable" entry at `i` has two tags (asserted above), so tag index
                    // `1` is in range and the accessor returns a non-null handle-owned
                    // string, valid until the destroy at the end of the test and only read
                    // here.
                    let k1 = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_tag_key(map, i, 1)) };
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_tag_value`'s `# Safety`; the
                    // "measurable" entry at `i` has two tags (asserted above), so tag index
                    // `1` is in range and the accessor returns a non-null handle-owned
                    // string, valid until the destroy at the end of the test and only read
                    // here.
                    let v1 = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_tag_value(map, i, 1)) };
                    assert_eq!((k1.to_str().unwrap(), v1.to_str().unwrap()), ("topic", "t"));
                },
                "as-string" => {
                    assert_eq!(kind, crate::ffi::common::METRIC_VALUE_STRING);
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_value_string`'s `# Safety`;
                    // the entry at `i` is the string-valued "as-string" gauge matched by
                    // name, so the accessor returns a non-null handle-owned string, valid
                    // until the destroy at the end of the test and only read here.
                    let s = unsafe { CStr::from_ptr(kafka_consumer_MetricMap_get_value_string(map, i)) };
                    assert_eq!(s.to_str().unwrap(), "hello");
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_tag_count`'s `# Safety`; `i`
                    // is within the asserted entry count, and the handle is destroyed only
                    // at the end of the test.
                    assert_eq!(unsafe { kafka_consumer_MetricMap_get_tag_count(map, i) }, 0);
                },
                "as-long" => {
                    assert_eq!(kind, crate::ffi::common::METRIC_VALUE_LONG);
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_value_long`'s `# Safety`;
                    // `i` indexes the "as-long" entry matched by name, and the handle is
                    // destroyed only at the end of the test.
                    assert_eq!(unsafe { kafka_consumer_MetricMap_get_value_long(map, i) }, -9_000_000_000);
                },
                "as-int" => {
                    assert_eq!(kind, crate::ffi::common::METRIC_VALUE_INT);
                    // SAFETY: Test: `map` is the live metric-map handle built above,
                    // satisfying `kafka_consumer_MetricMap_get_value_int`'s `# Safety`; `i`
                    // indexes the "as-int" entry matched by name, and the handle is
                    // destroyed only at the end of the test.
                    assert_eq!(unsafe { kafka_consumer_MetricMap_get_value_int(map, i) }, -7);
                },
                other => panic!("unexpected metric name {other}"),
            }
            seen += 1;
        }
        assert_eq!(seen, 4);

        // SAFETY: Test: `map` is the handle `box_metric_map(metrics)` created above and
        // this is its single destroy, after the last accessor read, satisfying `# Safety`
        // ("null or a valid metric-map handle"); the pointer is not used afterwards.
        unsafe { kafka_consumer_MetricMap_destroy(map) };
    }

    /// Out-of-range indices are reported rather than panicking, matching the
    /// other `*_get_*` accessors in this module.
    #[test]
    fn metric_map_out_of_range_accessors_are_safe() {
        let map = box_metric_map(HashMap::new());
        // SAFETY: Test: `map` is the (empty) handle `box_metric_map(HashMap::new())`
        // returned above, satisfying `kafka_consumer_MetricMap_count`'s `# Safety` ("a
        // valid metric-map handle"); it is destroyed once, below.
        assert_eq!(unsafe { kafka_consumer_MetricMap_count(map) }, 0);
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; index `0` is deliberately out of range for an empty
        // map to exercise the documented null return (`metric_entry` yields `None`).
        assert!(unsafe { kafka_consumer_MetricMap_get_name(map, 0) }.is_null());
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; index `-1` is deliberately negative to exercise the
        // documented null return (`metric_entry` rejects negative indices).
        assert!(unsafe { kafka_consumer_MetricMap_get_name(map, -1) }.is_null());
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; index `5` is deliberately out of range to exercise the
        // documented null return (`metric_entry` yields `None`).
        assert!(unsafe { kafka_consumer_MetricMap_get_value_string(map, 5) }.is_null());
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; index `0` is deliberately out of range to exercise the
        // documented `-1` return for a missing entry.
        assert_eq!(unsafe { kafka_consumer_MetricMap_get_tag_count(map, 0) }, -1);
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; metric index `0` is deliberately out of range to
        // exercise the documented null return when either index is invalid.
        assert!(unsafe { kafka_consumer_MetricMap_get_tag_key(map, 0, 0) }.is_null());
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; index `0` is deliberately out of range to exercise the
        // documented `0.0` return for a missing entry.
        assert_eq!(unsafe { kafka_consumer_MetricMap_get_value_double(map, 0) }, 0.0);
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; index `0` is deliberately out of range to exercise the
        // documented `0` return for a missing entry.
        assert_eq!(unsafe { kafka_consumer_MetricMap_get_value_long(map, 0) }, 0);
        // SAFETY: Test: `map` is the live empty metric-map handle built above, satisfying
        // the accessor's `# Safety`; index `0` is deliberately out of range to exercise the
        // documented `0` return for a missing entry.
        assert_eq!(unsafe { kafka_consumer_MetricMap_get_value_int(map, 0) }, 0);
        // Default kind for an out-of-range index.
        assert_eq!(
            // SAFETY: Test: `map` is the live empty metric-map handle built above,
            // satisfying `kafka_consumer_MetricMap_get_value_kind`'s `# Safety`; index `0`
            // is deliberately out of range to exercise the documented default-kind
            // (`METRIC_VALUE_DOUBLE`) return for a missing entry.
            unsafe { kafka_consumer_MetricMap_get_value_kind(map, 0) },
            crate::ffi::common::METRIC_VALUE_DOUBLE
        );
        // SAFETY: Test: `map` is the handle `box_metric_map(HashMap::new())` created above
        // and this is its single destroy, after the last accessor call, satisfying `#
        // Safety` ("null or a valid metric-map handle"); the pointer is not used
        // afterwards.
        unsafe { kafka_consumer_MetricMap_destroy(map) };
        // Destroy is null-safe.
        // SAFETY: Test: a deliberate NULL is passed to exercise the documented null no-op
        // path of `kafka_consumer_MetricMap_destroy` ("Safe with null"), which its `#
        // Safety` permits.
        unsafe { kafka_consumer_MetricMap_destroy(std::ptr::null_mut()) };
    }

    // ── FfiRebalanceListener ──
    //
    // The dispatcher-thread callback behavior of the listener is covered by the
    // Unity suite `c/tests/test_consumer_callbacks.c` (matching the
    // existing split: the C surface is tested from C). What cannot be reached
    // from there is `on_partitions_lost` with a NULL `lost` callback, because
    // `MockConsumer::rebalance` never fires `on_partitions_lost` (neither does
    // Java's) — so the Java-default delegation to `on_partitions_revoked` is
    // asserted here, by invoking the adapter directly.

    /// Counters shared with the stub callbacks below, addressed through
    /// `user_data` so no global state is involved.
    struct StubCounters {
        revoked_calls: AtomicI32,
        assigned_calls: AtomicI32,
        lost_calls: AtomicI32,
        /// Partition count of the list delivered to the last stub invocation.
        last_partition_count: AtomicI32,
    }

    impl StubCounters {
        fn new() -> Self {
            StubCounters {
                revoked_calls: AtomicI32::new(0),
                assigned_calls: AtomicI32::new(0),
                lost_calls: AtomicI32::new(0),
                last_partition_count: AtomicI32::new(-1),
            }
        }
    }

    /// Records the invocation, destroys the delivered list (the callee owns it),
    /// and reports success.
    ///
    /// # Safety
    ///
    /// Called by the library through the listener callback contract: `partitions` must
    /// be a fresh list handle the callee owns (it is destroyed here) and `user_data` the
    /// `&StubCounters` that `stub_listener` registered, alive until the dispatcher is
    /// joined.
    unsafe extern "C" fn stub_revoked(
        partitions: *mut kafka_common_TopicPartitionList_t,
        user_data: *mut c_void,
    ) -> *mut kafka_common_Error_t {
        // SAFETY: Test stub: `user_data` is the `&StubCounters` that `stub_listener`
        // registered as the `CallbackTarget`'s `user_data` (with no destroy hook); each
        // test owns `counters` on its stack frame and joins the dispatcher before it goes
        // out of scope, so the pointer is live, and the shared reborrow is used only for
        // atomic updates during this call.
        let counters = unsafe { &*(user_data as *const StubCounters) };
        counters.revoked_calls.fetch_add(1, Ordering::SeqCst);
        counters
            .last_partition_count
            // SAFETY: Test stub: `partitions` is the fresh, non-null list
            // `FfiRebalanceListener::invoke` builds with `box_topic_partition_list` and
            // hands to the callee, satisfying `kafka_common_TopicPartitionList_count`'s `#
            // Safety` ("a valid topic-partition-list handle"); it is read before the
            // destroy call below.
            .store(unsafe { kafka_common_TopicPartitionList_count(partitions) }, Ordering::SeqCst);
        // SAFETY: Test stub: the callee owns the delivered `partitions` list per the
        // `kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t`
        // contract, and this is its single destroy, after the last read;
        // `kafka_common_TopicPartitionList_destroy`'s `# Safety` accepts a valid list
        // handle.
        unsafe { kafka_common_TopicPartitionList_destroy(partitions) };
        std::ptr::null_mut()
    }

    /// # Safety
    ///
    /// Same requirements as `stub_revoked`: an owned `partitions` list and a live
    /// `&StubCounters` in `user_data`.
    unsafe extern "C" fn stub_assigned(
        partitions: *mut kafka_common_TopicPartitionList_t,
        user_data: *mut c_void,
    ) -> *mut kafka_common_Error_t {
        // SAFETY: Test stub: `user_data` is the `&StubCounters` that `stub_listener`
        // registered as the `CallbackTarget`'s `user_data` (with no destroy hook); each
        // test owns `counters` on its stack frame and joins the dispatcher before it goes
        // out of scope, so the pointer is live, and the shared reborrow is used only for an
        // atomic increment during this call.
        let counters = unsafe { &*(user_data as *const StubCounters) };
        counters.assigned_calls.fetch_add(1, Ordering::SeqCst);
        // SAFETY: Test stub: the callee owns the delivered `partitions` list (a fresh
        // `box_topic_partition_list` handle from `FfiRebalanceListener::invoke`) per the
        // listener callback contract, and this is its single destroy;
        // `kafka_common_TopicPartitionList_destroy`'s `# Safety` accepts a valid list
        // handle.
        unsafe { kafka_common_TopicPartitionList_destroy(partitions) };
        std::ptr::null_mut()
    }

    /// # Safety
    ///
    /// Same requirements as `stub_revoked`: an owned `partitions` list and a live
    /// `&StubCounters` in `user_data`.
    unsafe extern "C" fn stub_lost(
        partitions: *mut kafka_common_TopicPartitionList_t,
        user_data: *mut c_void,
    ) -> *mut kafka_common_Error_t {
        // SAFETY: Test stub: `user_data` is the `&StubCounters` that `stub_listener`
        // registered as the `CallbackTarget`'s `user_data` (with no destroy hook); each
        // test owns `counters` on its stack frame and joins the dispatcher before it goes
        // out of scope, so the pointer is live, and the shared reborrow is used only for an
        // atomic increment during this call.
        let counters = unsafe { &*(user_data as *const StubCounters) };
        counters.lost_calls.fetch_add(1, Ordering::SeqCst);
        // SAFETY: Test stub: the callee owns the delivered `partitions` list (a fresh
        // `box_topic_partition_list` handle from `FfiRebalanceListener::invoke`) per the
        // listener callback contract, and this is its single destroy;
        // `kafka_common_TopicPartitionList_destroy`'s `# Safety` accepts a valid list
        // handle.
        unsafe { kafka_common_TopicPartitionList_destroy(partitions) };
        std::ptr::null_mut()
    }

    /// Returns an error handle, mirroring a C listener that "throws".
    ///
    /// # Safety
    ///
    /// Same requirements as `stub_revoked`: an owned `partitions` list and a live
    /// `&StubCounters` in `user_data`.
    unsafe extern "C" fn stub_revoked_failing(
        partitions: *mut kafka_common_TopicPartitionList_t,
        user_data: *mut c_void,
    ) -> *mut kafka_common_Error_t {
        // SAFETY: Test stub: `user_data` is the `&StubCounters` that `stub_listener`
        // registered as the `CallbackTarget`'s `user_data` (with no destroy hook); each
        // test owns `counters` on its stack frame and joins the dispatcher before it goes
        // out of scope, so the pointer is live, and the shared reborrow is used only for an
        // atomic increment during this call.
        let counters = unsafe { &*(user_data as *const StubCounters) };
        counters.revoked_calls.fetch_add(1, Ordering::SeqCst);
        // SAFETY: Test stub: the callee owns the delivered `partitions` list (a fresh
        // `box_topic_partition_list` handle from `FfiRebalanceListener::invoke`) per the
        // listener callback contract, and this is its single destroy; the error handle
        // returned next is a fresh `box_error` allocation whose ownership passes to the
        // client, as the contract requires.
        unsafe { kafka_common_TopicPartitionList_destroy(partitions) };
        box_error(Error::local_illegal_state("listener refused the revocation"))
    }

    /// Builds an adapter over the stub callbacks, plus a live dispatcher for it.
    fn stub_listener(
        counters: &StubCounters,
        on_revoked: RebalanceCallbackFn,
        on_lost: Option<RebalanceCallbackFn>,
    ) -> (FfiRebalanceListener, std::thread::JoinHandle<()>) {
        let (completion_tx, dispatcher) = common::spawn_dispatcher("ffi-listener-test-dispatcher");
        let listener = FfiRebalanceListener {
            on_revoked,
            on_assigned: stub_assigned,
            on_lost,
            // The test owns `counters`, so no release hook.
            target: CallbackTarget { user_data: counters as *const StubCounters as *mut c_void, destroy: None },
            completion_tx,
        };
        (listener, dispatcher)
    }

    #[tokio::test]
    async fn on_partitions_lost_delegates_to_revoked_when_no_lost_callback() {
        let counters = StubCounters::new();
        let (listener, dispatcher) = stub_listener(&counters, stub_revoked, None);
        let partitions = vec![
            TopicPartition::new("t".to_string(), 0),
            TopicPartition::new("t".to_string(), 1),
        ];

        listener
            .on_partitions_lost(&partitions)
            .await
            .expect("delegated call must succeed");

        // Java's `ConsumerRebalanceListener.onPartitionsLost` default calls
        // `onPartitionsRevoked(partitions)` — with the same partitions.
        assert_eq!(1, counters.revoked_calls.load(Ordering::SeqCst));
        assert_eq!(0, counters.lost_calls.load(Ordering::SeqCst));
        assert_eq!(0, counters.assigned_calls.load(Ordering::SeqCst));
        assert_eq!(2, counters.last_partition_count.load(Ordering::SeqCst));

        drop(listener);
        dispatcher.join().expect("dispatcher thread must exit cleanly");
    }

    #[tokio::test]
    async fn on_partitions_lost_uses_the_lost_callback_when_supplied() {
        let counters = StubCounters::new();
        let (listener, dispatcher) = stub_listener(&counters, stub_revoked, Some(stub_lost));

        listener
            .on_partitions_lost(&[TopicPartition::new("t".to_string(), 0)])
            .await
            .expect("lost callback must succeed");

        assert_eq!(1, counters.lost_calls.load(Ordering::SeqCst));
        assert_eq!(0, counters.revoked_calls.load(Ordering::SeqCst));

        drop(listener);
        dispatcher.join().expect("dispatcher thread must exit cleanly");
    }

    #[tokio::test]
    async fn an_error_handle_returned_by_a_callback_becomes_an_err() {
        let counters = StubCounters::new();
        let (listener, dispatcher) = stub_listener(&counters, stub_revoked_failing, None);

        // Both the direct revoke path and the delegating lost path must surface
        // the error the C callback returned.
        let revoked_error = listener
            .on_partitions_revoked(&[TopicPartition::new("t".to_string(), 0)])
            .await
            .expect_err("the callback's error handle must become an Err");
        assert!(
            revoked_error.message().contains("listener refused the revocation"),
            "unexpected message: {}",
            revoked_error.message()
        );

        let lost_error = listener
            .on_partitions_lost(&[TopicPartition::new("t".to_string(), 0)])
            .await
            .expect_err("the delegated call must propagate the error too");
        assert!(
            lost_error.message().contains("listener refused the revocation"),
            "unexpected message: {}",
            lost_error.message()
        );
        assert_eq!(2, counters.revoked_calls.load(Ordering::SeqCst));

        drop(listener);
        dispatcher.join().expect("dispatcher thread must exit cleanly");
    }

    /// `_destroy` on a never-subscribed listener fires the `user_data_destroy`
    /// hook exactly once (the C-side counterpart is covered in the Unity suite,
    /// but the never-subscribed path has no consumer involved at all).
    #[test]
    fn destroying_a_never_subscribed_listener_fires_the_destroy_hook() {
        static DESTROY_CALLS: AtomicI32 = AtomicI32::new(0);

        /// # Safety
        ///
        /// `_user_data` is ignored, so the caller has nothing to uphold; the signature is
        /// `unsafe extern "C"` only to match the `user_data_destroy` hook type.
        unsafe extern "C" fn counting_destroy(_user_data: *mut c_void) {
            DESTROY_CALLS.fetch_add(1, Ordering::SeqCst);
        }

        // SAFETY: Test: the arguments satisfy
        // `kafka_consumer_ConsumerRebalanceListener_new`'s `# Safety`: `stub_revoked` and
        // `stub_assigned` are `extern "C"` functions of the listener signature, valid for
        // the whole test; `on_partitions_lost` is deliberately `None` (the documented
        // nullable hook); `user_data` is a deliberate NULL that nothing dereferences
        // because the listener is never subscribed or invoked; and `counting_destroy` only
        // touches a static counter, so it needs no live `user_data`.
        let listener = unsafe {
            kafka_consumer_ConsumerRebalanceListener_new(
                stub_revoked,
                stub_assigned,
                None,
                std::ptr::null_mut(),
                Some(counting_destroy),
            )
        };
        assert!(!listener.is_null());
        assert_eq!(0, DESTROY_CALLS.load(Ordering::SeqCst));

        // SAFETY: Test: `listener` is the non-null (asserted) handle
        // `kafka_consumer_ConsumerRebalanceListener_new` returned above and was never
        // passed to a subscribe call, so this is its single, final use as that function's
        // `# Safety` requires; dropping it fires `counting_destroy` once, which the next
        // assertion checks, and the pointer is not used afterwards.
        unsafe { kafka_consumer_ConsumerRebalanceListener_destroy(listener) };
        assert_eq!(1, DESTROY_CALLS.load(Ordering::SeqCst));

        // Null is a no-op.
        // SAFETY: Test: a deliberate NULL is passed to exercise the documented null no-op
        // path of `kafka_consumer_ConsumerRebalanceListener_destroy` ("Safe with null"),
        // which its `# Safety` permits.
        unsafe { kafka_consumer_ConsumerRebalanceListener_destroy(std::ptr::null_mut()) };
        assert_eq!(1, DESTROY_CALLS.load(Ordering::SeqCst));
    }

    /// Counts a commit callback's invocations and its `user_data_destroy` firings;
    /// the commit panic tests pass it as `user_data`.
    #[derive(Default)]
    struct CommitCallbackCounts {
        callbacks: AtomicI32,
        destroys: AtomicI32,
    }

    /// A commit callback that counts itself in the [`CommitCallbackCounts`] passed
    /// as `user_data`, freeing whichever handles it is given, as a C caller must.
    ///
    /// # Safety
    ///
    /// Called by the library through `kafka_consumer_Consumer_commit_async_callback_t`:
    /// `offsets` and `error` must be null or owned handles (both are destroyed here) and
    /// `user_data` the `&CommitCallbackCounts` the test passed, alive until the test's
    /// final assertion.
    unsafe extern "C" fn count_commit_callback(
        offsets: *mut kafka_consumer_OffsetMap_t,
        error: *mut kafka_common_Error_t,
        user_data: *mut c_void,
    ) {
        // SAFETY: Test callback: `user_data` is the `&CommitCallbackCounts` pointer
        // `assert_commit_panic_is_a_synchronous_failure` passes to the commit entry point;
        // `counts` lives on that function's frame past the 200 ms grace sleep and the final
        // assertions, so the pointer is live if this callback ever fires (the test asserts
        // it does not), and the shared reborrow is used only for an atomic increment.
        let counts = unsafe { &*(user_data as *const CommitCallbackCounts) };
        counts.callbacks.fetch_add(1, Ordering::SeqCst);
        if !offsets.is_null() {
            // SAFETY: Test callback: `offsets` is non-null (checked above) and, per
            // `kafka_consumer_Consumer_commit_async_callback_t`, a freshly allocated map
            // the callee owns; this is its single destroy, satisfying
            // `kafka_consumer_OffsetMap_destroy`'s `# Safety`.
            unsafe { kafka_consumer_OffsetMap_destroy(offsets) };
        }
        if !error.is_null() {
            // SAFETY: Test callback: `error` is non-null (checked above) and, per
            // `kafka_consumer_Consumer_commit_async_callback_t`, a freshly allocated error
            // handle the callee owns; this is its single destroy, satisfying
            // `kafka_common_Error_destroy`'s `# Safety`.
            unsafe { kafka_common_Error_destroy(error) };
        }
    }

    /// A `user_data_destroy` hook that counts itself in the
    /// [`CommitCallbackCounts`] passed as `user_data`.
    ///
    /// # Safety
    ///
    /// `user_data` must be the `&CommitCallbackCounts` the test registered through
    /// `make_commit_callback`, alive until the test's final assertion.
    unsafe extern "C" fn count_commit_destroy(user_data: *mut c_void) {
        // SAFETY: Test hook: `user_data` is the `&CommitCallbackCounts` the test registered
        // via `make_commit_callback`; `CallbackTarget::drop` fires this hook exactly once,
        // during the unwind inside the `commit(...)` call (the entry points document that a
        // panic before hand-off fires `user_data_destroy` before returning) or at the
        // latest when `kafka_consumer_Consumer_destroy` drops the consumer, both before
        // `counts` goes out of scope at the end of
        // `assert_commit_panic_is_a_synchronous_failure`; the shared reborrow is used only
        // for an atomic increment.
        let counts = unsafe { &*(user_data as *const CommitCallbackCounts) };
        counts.destroys.fetch_add(1, Ordering::SeqCst);
    }

    /// Asserts that `commit`, a call to one of the two commit-with-callback entry
    /// points, reports a caught panic as a synchronous failure. These keep the
    /// plain guard (§6 note 8), so the panic must come back as a
    /// `LOCAL_ILLEGAL_STATE` error handle naming `function`, with no callback and
    /// exactly one `user_data_destroy` (COMMENTS.79.md issue 1).
    ///
    /// The panic comes from making the call inside another tokio runtime, which
    /// the synchronous consumer API does not support: `sync_void_op`'s `block_on`
    /// panics ("Cannot start a runtime from within a runtime") before it polls the
    /// commit, and the unwind drops the callback adapter with the unpolled future.
    /// It is the one synchronous panic these entry points can be driven into
    /// without a test hook in production code.
    fn assert_commit_panic_is_a_synchronous_failure(
        function: &str,
        commit: impl FnOnce(*const kafka_consumer_Consumer_t, *mut c_void) -> *mut kafka_common_Error_t,
    ) {
        // SAFETY: Test: `auto_offset_reset` is a deliberate NULL, which
        // `kafka_consumer_MockConsumer_new`'s `# Safety` permits ("null or a valid,
        // null-terminated C string") and documents as defaulting to `latest`; the returned
        // handle is destroyed once, with `kafka_consumer_Consumer_destroy` below.
        let consumer = unsafe { kafka_consumer_MockConsumer_new(std::ptr::null()) };
        let counts = CommitCallbackCounts::default();
        let user_data = &counts as *const CommitCallbackCounts as *mut c_void;
        let outer = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let error = outer.block_on(async { commit(consumer, user_data) });
        drop(outer);

        assert!(!error.is_null(), "a caught panic must return an error handle");
        assert_eq!(
            // SAFETY: Test: `error` is the non-null (asserted) handle the commit entry
            // point returned, satisfying `kafka_common_Error_code`'s `# Safety` ("a valid
            // handle from a function that returned an error, or null"); it is destroyed
            // only after the message is copied below.
            unsafe { kafka_common_Error_code(error) },
            kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE
        );
        // SAFETY: Test: `error` is the live non-null error handle returned by the commit
        // call; `kafka_common_Error_message` returns its handle-owned NUL-terminated
        // message, valid until `kafka_common_Error_destroy(error)`, and the bytes are
        // copied into an owned `String` before that call on the next line.
        let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: Test: `error` is the handle returned by the commit call and this is its
        // single destroy, after its last read, satisfying `kafka_common_Error_destroy`'s `#
        // Safety`; the pointer is not used afterwards.
        unsafe { kafka_common_Error_destroy(error) };
        assert!(
            msg.starts_with(&format!("Rust panic caught at the FFI boundary in {function}:")),
            "unexpected error message: {msg}"
        );
        assert!(
            msg.contains("Cannot start a runtime from within a runtime"),
            "unexpected error message: {msg}"
        );

        // Outside any runtime: dropping the consumer's own runtime inside one
        // would panic.
        // SAFETY: Test: `consumer` is the handle `kafka_consumer_MockConsumer_new` returned
        // above (`# Safety`: null or a valid handle from a consumer constructor) and this
        // is its single destroy, called outside any tokio runtime (the outer runtime was
        // dropped) and after the only operation on it has returned, so no op is in flight,
        // as `kafka_consumer_Consumer_destroy` requires; the pointer is not used
        // afterwards.
        unsafe { kafka_consumer_Consumer_destroy(consumer) };
        // Give a stray completion ample time to reach the detached dispatcher.
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(
            counts.callbacks.load(Ordering::SeqCst),
            0,
            "a panic returned as an error handle must not also invoke the callback"
        );
        assert_eq!(
            counts.destroys.load(Ordering::SeqCst),
            1,
            "user_data_destroy must fire exactly once"
        );
    }

    #[test]
    fn test_commit_async_with_callback_panic_is_a_synchronous_failure() {
        assert_commit_panic_is_a_synchronous_failure(
            "kafka_consumer_Consumer_commit_async_with_callback",
            // SAFETY: Test: the arguments satisfy
            // `kafka_consumer_Consumer_commit_async_with_callback`'s `# Safety`: `consumer`
            // is the mock handle the helper created with `kafka_consumer_MockConsumer_new`,
            // `count_commit_callback` is a valid `extern "C"` function of the
            // commit-callback signature, and `user_data` points at the helper's `counts`,
            // which stays valid until `count_commit_destroy` fires (before the call
            // returns, per the entry point's documented panic path).
            |consumer, user_data| unsafe {
                kafka_consumer_Consumer_commit_async_with_callback(
                    consumer,
                    count_commit_callback,
                    user_data,
                    Some(count_commit_destroy),
                )
            },
        );
    }

    /// The offsets marshal successfully, so the panic comes after the only
    /// synchronous failure the documentation names.
    #[test]
    fn test_commit_async_offsets_with_callback_panic_is_a_synchronous_failure() {
        let topic = std::ffi::CString::new("topic").unwrap();
        let topics = [topic.as_ptr()];
        let partitions = [0_i32];
        let offsets = [5_i64];
        let leader_epochs = [-1_i32];
        assert_commit_panic_is_a_synchronous_failure(
            "kafka_consumer_Consumer_commit_async_offsets_with_callback",
            // SAFETY: Test: the arguments satisfy
            // `kafka_consumer_Consumer_commit_async_offsets_with_callback`'s `# Safety`:
            // `consumer` is the mock handle the helper created with
            // `kafka_consumer_MockConsumer_new`; `topics`, `partitions`, `offsets` and
            // `leader_epochs` are stack arrays with one entry each, matching `count = 1`,
            // and `topic` is an owned `CString` that outlives the call; `metadata` is a
            // deliberate NULL, which the function documents as allowed for the whole array,
            // and `leader_epochs[0] = -1` means no epoch; `count_commit_callback` is a
            // valid `extern "C"` function and `user_data` points at the helper's `counts`,
            // valid until `count_commit_destroy` fires before the call returns.
            |consumer, user_data| unsafe {
                kafka_consumer_Consumer_commit_async_offsets_with_callback(
                    consumer,
                    topics.as_ptr(),
                    partitions.as_ptr(),
                    offsets.as_ptr(),
                    leader_epochs.as_ptr(),
                    std::ptr::null(),
                    1,
                    count_commit_callback,
                    user_data,
                    Some(count_commit_destroy),
                )
            },
        );
    }
}
