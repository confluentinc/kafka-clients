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
//!   ([`CompletionJob`], [`spawn_dispatcher`], [`enqueue_or_run_inline`]).
//! - The void-returning operation callback machinery ([`OperationCallbackFn`],
//!   [`OperationCompletion`], [`OperationCallbackTarget`]).
//! - The default logger initialization helper ([`init_default_logger`]).
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![allow(non_snake_case, non_camel_case_types)]

use std::ffi::{CString, c_char};

use crate::common::KafkaError;

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

/// Casts a `*const kafka_common_KafkaError_t` to a reference to `KafkaErrorInner`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by [`box_error`].
pub(crate) unsafe fn error_ref(error: *const kafka_common_KafkaError_t) -> &'static KafkaErrorInner {
    unsafe { &*(error as *const KafkaErrorInner) }
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
// that drains a completion queue, so a slow callback cannot stall I/O.
//
// That is the normal path, not a guarantee: a caller that cannot reach the
// dispatcher runs the job on its own thread instead, so the callback obligation
// is never dropped (see `enqueue_or_run_inline`, and the operations that fire
// their callback inline when they cannot submit the request at all). Callbacks
// are therefore not guaranteed to be serialised on one thread, and can run on a
// tokio worker.

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

// ---------------------------------------------------------------------------
// MetricMap — shared snapshot machinery for `metrics()` FFI surfaces
// ---------------------------------------------------------------------------
//
// Both the consumer (`kafka_consumer_MetricMap_*`) and producer
// (`kafka_producer_MetricMap_*`) FFI surfaces expose the result of Java's
// `Map<MetricName, ? extends Metric> metrics()` as an opaque, index-walkable
// snapshot handle. The *exported* opaque type and accessor functions stay
// namespaced (ABI is pinned per surface), but the internal representation and
// the snapshot-building / index-walking logic are identical, so they live here
// once and each surface's exported functions are thin delegators.

/// Metric value kinds, mirroring [`crate::common::MetricValue`]'s variants.
///
/// Returned as a plain `int32_t` by the `..._MetricMap_get_value_kind`
/// accessor on each FFI surface to tell the caller which `get_value_*` accessor
/// is valid:
///
/// - `0` — `Double`: a measurable (or `Double`-valued gauge). Use
///   `get_value_double`.
/// - `1` — `String`: a string-valued gauge. Use `get_value_string`.
/// - `2` — `Long`: a long-valued gauge. Use `get_value_long`.
/// - `3` — `Int`: an integer-valued gauge. Use `get_value_int`.
///
/// These are plain integers rather than a C enum because `cbindgen.toml`
/// restricts `item_types` to functions/structs/typedefs — the generated header
/// contains no enums at all, and adding one type would mean exporting every
/// other enum reachable in the crate. (For the same reason cbindgen does not
/// emit these constants into the header; they are the Rust-side source of truth
/// shared by the consumer and producer surfaces and by their tests.)
pub(crate) const METRIC_VALUE_DOUBLE: i32 = 0;
/// See [`METRIC_VALUE_DOUBLE`].
pub(crate) const METRIC_VALUE_STRING: i32 = 1;
/// See [`METRIC_VALUE_DOUBLE`].
pub(crate) const METRIC_VALUE_LONG: i32 = 2;
/// See [`METRIC_VALUE_DOUBLE`].
pub(crate) const METRIC_VALUE_INT: i32 = 3;

/// One flattened metric entry. `MetricName`'s four fields plus the measured
/// value; tags are parallel key/value vectors so the C side can walk them by
/// index without another opaque type.
pub(crate) struct MetricEntry {
    pub(crate) name_c: CString,
    pub(crate) group_c: CString,
    pub(crate) description_c: CString,
    pub(crate) tag_keys: Vec<CString>,
    pub(crate) tag_values: Vec<CString>,
    pub(crate) kind: i32,
    pub(crate) double_value: f64,
    pub(crate) string_value: CString,
    pub(crate) long_value: i64,
    pub(crate) int_value: i32,
}

/// The heap-owned backing of an opaque `kafka_*_MetricMap_t` handle. Each
/// namespaced opaque type is a `#[repr(C)]` zero-sized placeholder that is cast
/// to `*const MetricMapInner` inside the accessors.
pub(crate) struct MetricMapInner {
    pub(crate) entries: Vec<MetricEntry>,
}

/// Builds the snapshot backing from a `metrics()` map. Each value is measured
/// exactly once here — the resulting handle is a point-in-time snapshot, the
/// only thing that can cross an FFI boundary without an upcall per read.
pub(crate) fn build_metric_map_inner(
    metrics: std::collections::HashMap<crate::common::MetricName, std::sync::Arc<crate::common::metrics::KafkaMetric>>,
) -> Box<MetricMapInner> {
    use crate::common::{Metric, MetricValue};
    let mut entries = Vec::with_capacity(metrics.len());
    for (name, metric) in metrics {
        // `metric_value()` is the one measurement taken for this snapshot.
        let value = metric.metric_value();
        let mut tag_keys = Vec::with_capacity(name.tags().len());
        let mut tag_values = Vec::with_capacity(name.tags().len());
        for (k, v) in name.tags() {
            tag_keys.push(CString::new(k.as_bytes()).unwrap_or_default());
            tag_values.push(CString::new(v.as_bytes()).unwrap_or_default());
        }
        let (kind, double_value, string_value, long_value, int_value) = match value {
            MetricValue::Double(d) => (METRIC_VALUE_DOUBLE, d, CString::default(), 0, 0),
            MetricValue::String(s) => (METRIC_VALUE_STRING, 0.0, CString::new(s.as_bytes()).unwrap_or_default(), 0, 0),
            MetricValue::Long(l) => (METRIC_VALUE_LONG, 0.0, CString::default(), l, 0),
            MetricValue::Int(i) => (METRIC_VALUE_INT, 0.0, CString::default(), 0, i),
        };
        entries.push(MetricEntry {
            name_c: CString::new(name.name().as_bytes()).unwrap_or_default(),
            group_c: CString::new(name.group().as_bytes()).unwrap_or_default(),
            description_c: CString::new(name.description().as_bytes()).unwrap_or_default(),
            tag_keys,
            tag_values,
            kind,
            double_value,
            string_value,
            long_value,
            int_value,
        });
    }
    Box::new(MetricMapInner { entries })
}

/// Resolves the entry at `index`, or `None` if out of range.
///
/// Returns a caller-scoped borrow rather than `&'static` — the entry is only
/// valid as long as the backing [`MetricMapInner`] allocation is, and it must
/// not be held past the matching `*_MetricMap_destroy` call.
///
/// # Safety
/// `inner` must be a valid pointer obtained from [`build_metric_map_inner`].
pub(crate) unsafe fn metric_entry<'a>(inner: *const MetricMapInner, index: i32) -> Option<&'a MetricEntry> {
    if index < 0 {
        return None;
    }
    unsafe { &*inner }.entries.get(index as usize)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_count(inner: *const MetricMapInner) -> i32 {
    unsafe { &*inner }.entries.len() as i32
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_name(inner: *const MetricMapInner, index: i32) -> *const c_char {
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.name_c.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_group(inner: *const MetricMapInner, index: i32) -> *const c_char {
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.group_c.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_description(inner: *const MetricMapInner, index: i32) -> *const c_char {
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.description_c.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_tag_count(inner: *const MetricMapInner, index: i32) -> i32 {
    unsafe { metric_entry(inner, index) }.map_or(-1, |e| e.tag_keys.len() as i32)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_tag_key(inner: *const MetricMapInner, index: i32, tag_index: i32) -> *const c_char {
    if tag_index < 0 {
        return std::ptr::null();
    }
    unsafe { metric_entry(inner, index) }
        .and_then(|e| e.tag_keys.get(tag_index as usize))
        .map_or(std::ptr::null(), |k| k.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_tag_value(
    inner: *const MetricMapInner,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    if tag_index < 0 {
        return std::ptr::null();
    }
    unsafe { metric_entry(inner, index) }
        .and_then(|e| e.tag_values.get(tag_index as usize))
        .map_or(std::ptr::null(), |v| v.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_kind(inner: *const MetricMapInner, index: i32) -> i32 {
    unsafe { metric_entry(inner, index) }.map_or(METRIC_VALUE_DOUBLE, |e| e.kind)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_double(inner: *const MetricMapInner, index: i32) -> f64 {
    unsafe { metric_entry(inner, index) }.map_or(0.0, |e| e.double_value)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_string(inner: *const MetricMapInner, index: i32) -> *const c_char {
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.string_value.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_long(inner: *const MetricMapInner, index: i32) -> i64 {
    unsafe { metric_entry(inner, index) }.map_or(0, |e| e.long_value)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_int(inner: *const MetricMapInner, index: i32) -> i32 {
    unsafe { metric_entry(inner, index) }.map_or(0, |e| e.int_value)
}

/// Reclaims a metric-map backing pointer. Safe with null (no-op).
///
/// # Safety
/// `inner` must be null or a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_destroy(inner: *mut MetricMapInner) {
    if !inner.is_null() {
        unsafe { drop(Box::from_raw(inner)) };
    }
}
