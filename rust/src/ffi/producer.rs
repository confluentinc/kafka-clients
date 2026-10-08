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

//! C FFI functions for the Kafka producer.
//!
//! This module provides `extern "C"` functions that expose the [`Producer`]
//! trait through opaque pointer handles, suitable for use from C, C++, or
//! any language with C FFI support.
//!
//! # Design
//!
//! - **Opaque handles**: [`kafka_producer_Producer_t`], [`kafka_common_KafkaFuture_RecordMetadata_t`], and
//!   [`kafka_producer_RecordMetadata_t`] are opaque types. Callers receive and pass raw
//!   pointers to these types; the internal layout is hidden.
//!
//! - **Fixed-width types**: All struct fields and function parameters use
//!   `i32`, `i64`, `bool`, and pointers — never `usize` or `size_t`. This
//!   ensures identical struct layouts on 32-bit and 64-bit platforms.
//!
//! - **Error handles**: Functions that can fail return `*mut kafka_common_Error_t`.
//!   A null return means success; a non-null return is an error handle that
//!   the caller inspects via [`kafka_common_Error_code`] / [`kafka_common_Error_message`]
//!   and frees with [`kafka_common_Error_destroy`].
//!
//! - **Null safety**: All functions check for null pointers and return
//!   appropriate error codes (or do nothing for void functions).
//!
//! - **Naming convention**: Functions follow the `kafka_<TypeName>_<method>`
//!   pattern where `TypeName` is PascalCase, matching the Rust/Java type name.
//!
//! # Concurrency model — unguarded sends, mutually-exclusive transaction control
//!
//! `KafkaProducer` is `Sync` and Java's `send()` is explicitly documented as safe
//! to call concurrently from many threads, so — unlike the consumer FFI, which
//! wraps every call in a single-owner access guard — the send surface here has
//! **no access guard at all**: no send is ever *rejected* for concurrency, from
//! any thread, at any time, including while a transaction is open.
//!
//! "Not rejected" is not the same as "not serialized", and the two blocking send
//! functions are in fact serialized: [`kafka_producer_Producer_send`] holds the
//! `kind` mutex across its enqueue and
//! [`kafka_producer_Producer_send_batch`] holds it across the whole batch, so N
//! threads calling either take turns, and one metadata fetch blocks all of them
//! for up to `max.block.ms`. Only [`kafka_producer_Producer_send_async`] and
//! [`kafka_producer_Producer_send_batch_async`] are genuinely concurrent — they
//! touch no shared mutex, just an unbounded channel drained by the submission
//! task. Callers that need real send parallelism should prefer the async pair.
//! (Removing that serialization is a change to the blocking send path, out of
//! scope here.)
//!
//! Java's *transaction-control* methods are the exception: `initTransactions`,
//! `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction` and
//! `abortTransaction` are **not** safe to call concurrently with each other. The
//! constraint is on *overlap*, not on thread identity — the calls may come from
//! different threads in sequence (say, off a pool), they just must not run at the
//! same time. The five FFI counterparts therefore share a narrow
//! mutual-exclusion flag (`ProducerHandle::txn_control_busy`) that rejects an
//! overlapping transaction-control call with
//! [`Error::local_concurrent_modification`], the same fail-fast error the consumer
//! guard uses; unlike the consumer's single-*owner* guard it records no owning
//! thread, precisely because thread identity is not part of the contract.
//!
//! The flag is scoped to those five functions only: it never covers `send`, and
//! it is held only for the duration of one control call — never across an open
//! transaction — so the `send` calls between `begin` and `commit` are unaffected.
//! The flag is released before returning (RAII via [`with_txn_control`], so a
//! panic inside a control call still releases it).
//!
//! **Each control function also has a non-blocking `_async` variant**
//! ([`kafka_producer_Producer_init_transactions_async`],
//! [`kafka_producer_Producer_begin_transaction_async`],
//! [`kafka_producer_Producer_send_offsets_to_transaction_async`],
//! [`kafka_producer_Producer_commit_transaction_async`] and
//! [`kafka_producer_Producer_abort_transaction_async`]), so the Python client — and
//! any caller that must not block a thread — can build both a synchronous and an
//! asynchronous transaction API on top of them. Each returns immediately and reports
//! its outcome through an operation callback fired on the producer's dispatcher
//! thread (null error on success). They share the one `txn_control_busy` flag with
//! the synchronous functions — CAS'd on the calling thread, then held across the
//! spawned task by an async-lifetime guard until the operation finishes — so overlap
//! is rejected with [`Error::local_concurrent_modification`] in every combination
//! (sync vs. sync, async vs. async, and sync vs. async), and, like the synchronous
//! ones, each drains the submission queue before it runs (see below). The sync and
//! async forms route through [`with_txn_control`] and [`with_txn_control_async`]
//! respectively, which is what keeps the flag impossible to skip.
//!
//! **Async sends are supported inside a transaction.** Each of the five
//! transaction-control functions drains the submission queue before it runs —
//! exactly as `flush`/`close` do (see [`drain_submitted_sends_via`] and
//! [`with_txn_control`]) — so every [`kafka_producer_Producer_send_async`] /
//! [`kafka_producer_Producer_send_batch_async`] that *returned* to the caller before
//! the control call began is handed to the producer via `producer.send()` first. The
//! contract is the same one `flush`/`close` give: all sends that had *returned* (not
//! merely been called) are included in the operation. `commit_transaction` then
//! commits those records and `abort_transaction` discards them, through the producer's
//! own Java-faithful accumulator handling. Sends racing concurrently on another thread
//! are not ordered against the control call — the same boundary `flush`/`close` and
//! [`drain_submitted_sends_await`] already describe. The deleted per-operation
//! `Submit`/`Discard` ordering machinery (a discard window, an `ends_discard` barrier)
//! is **not** reintroduced: the plain drain plus the producer's commit/abort logic is
//! sufficient. See `.claude/rules/producer-transactions.md` §13.
//!
//! See `design/history/Milestone-11/producer-transactions-ffi-plan.md` for the
//! full design and the rejected alternatives.
//!
//! [`Producer`]: crate::producer::Producer
//! [`Errors`]: crate::common::protocol::Errors

// FFI function names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![expect(non_camel_case_types)]

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char};
use std::sync::{Arc, Mutex};

use crate::common::Error;
use crate::common::KafkaFuture;
use crate::common::MetricName;
use crate::common::TopicPartition;
use crate::common::metrics::KafkaMetric;
use crate::common::protocol::Errors;
use crate::common::serialization::ByteArraySerializer;
#[cfg(test)]
use crate::ffi::common::kafka_common_ErrorCode_t;
#[cfg(test)]
use crate::ffi::common::kafka_common_ErrorCode_t::{
    kafka_common_ErrorCode_CORRUPT_MESSAGE, kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE, kafka_common_ErrorCode_NONE,
};
use crate::ffi::common::{
    self, CompletionJob, MetricMapInner, OperationCallbackFn, OperationCallbackTarget, OperationCompletion, box_error,
    enqueue_or_run_inline, init_default_logger, kafka_common_Error_t, spawn_callback_task,
};
#[cfg(test)]
use crate::ffi::common::{
    kafka_common_Error_code, kafka_common_Error_destroy, kafka_common_Error_is_retriable_error,
    kafka_common_Error_message,
};
// PartitionInfoList handle + builder are shared with the consumer FFI so
// kafka_producer_Producer_partitions_for can return the same opaque type; the
// group-metadata handle and the offsets-map reader are shared so
// kafka_producer_Producer_send_offsets_to_transaction takes exactly what the
// consumer FFI produces and marshals offsets exactly like
// kafka_consumer_Consumer_commit_sync_offsets.
use crate::ffi::consumer::{
    box_partition_info_list, group_metadata_ref, kafka_common_PartitionInfoList_t,
    kafka_consumer_ConsumerGroupMetadata_t, read_offset_map,
};
use crate::ffi::ffi_guard;
use crate::producer::Callback;
use crate::producer::KafkaProducer;
use crate::producer::MockProducer;
use crate::producer::Producer;
use crate::producer::ProducerConfig;
use crate::producer::RecordMetadata;
use crate::producer::{ProducerRecord, ProducerRecordOptionsBuilder};

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Internal enum wrapping all supported producer implementations.
///
/// Using an enum instead of `dyn Any` makes the FFI layer type-safe and avoids
/// downcasting. New producer kinds (e.g., `Real(KafkaProducer)`) can be added
/// as variants.
enum ProducerKind {
    /// A mock producer for testing.
    ///
    /// The `Runtime` is stored alongside the producer so that async trait
    /// methods (`send`, `flush`, `close`) can be driven via `runtime.block_on()`.
    ///
    /// Boxed to reduce enum size variance (MockProducer is much larger than KafkaProducer).
    Mock(Box<MockProducer<Vec<u8>, Vec<u8>>>, tokio::runtime::Runtime),
    /// A real Kafka producer connected to a cluster.
    ///
    /// The `Runtime` is stored alongside the producer so that:
    /// 1. The sender background task (spawned by `with_config`) has a runtime to run on.
    /// 2. Async trait methods (`send`, `flush`, `close`) are driven via `runtime.block_on()`.
    ///
    /// Drop order is left-to-right: the producer is dropped first (its `Drop`
    /// impl calls `force_close()`), then the runtime is dropped (blocking until
    /// the sender task exits).
    Kafka(Box<KafkaProducer<Vec<u8>, Vec<u8>>>, tokio::runtime::Runtime),
}

impl ProducerKind {
    /// Returns a reference to the tokio runtime associated with this producer.
    fn runtime(&self) -> &tokio::runtime::Runtime {
        match self {
            ProducerKind::Mock(_, rt) | ProducerKind::Kafka(_, rt) => rt,
        }
    }
}

/// Internal wrapper that pairs a [`KafkaFuture<RecordMetadata>`] with the
/// [`tokio::runtime::Handle`] of the producer that created it.
///
/// This allows [`kafka_common_KafkaFuture_RecordMetadata_get`] and
/// [`kafka_common_KafkaFuture_RecordMetadata_get_all`] to call `handle.block_on()`
/// instead of creating throwaway runtimes.
struct FfiFuture {
    future: KafkaFuture<RecordMetadata>,
    runtime_handle: tokio::runtime::Handle,
    /// Sender for the producer's completion-dispatch queue, so
    /// [`kafka_common_KafkaFuture_RecordMetadata_get_async`] can deliver its
    /// callback on the same dispatcher thread as every other completion.
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
}

/// Internal wrapper that pairs [`RecordMetadata`] with a [`CString`] for the
/// topic name, so that [`kafka_producer_RecordMetadata_topic`] can return a valid
/// `*const c_char` that lives as long as the handle.
struct RecordMetadataInner {
    metadata: RecordMetadata,
    /// Cached CString for the topic, created once at construction time.
    topic_cstring: CString,
}

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------

/// Opaque producer handle exposed to C callers.
///
/// Internally wraps a `Box<Mutex<ProducerKind>>`. The `Mutex` provides
/// thread-safe access matching Java's `synchronized` methods.
#[repr(C)]
pub struct kafka_producer_Producer_t {
    _private: [u8; 0],
}

/// Opaque future handle for a pending send result.
///
/// Internally wraps a `Box<KafkaFuture<RecordMetadata>>`.
#[repr(C)]
pub struct kafka_common_KafkaFuture_RecordMetadata_t {
    _private: [u8; 0],
}

/// Opaque record metadata handle returned after a successful send.
///
/// Internally wraps a `Box<RecordMetadataInner>`.
#[repr(C)]
pub struct kafka_producer_RecordMetadata_t {
    _private: [u8; 0],
}

/// Opaque properties handle for producer configuration.
///
/// Internally wraps a `Box<HashMap<String, String>>`. Properties are
/// populated via [`kafka_producer_ProducerProperties_put`] or created
/// in bulk with [`kafka_producer_ProducerProperties_from_configs`],
/// then passed to [`kafka_producer_KafkaProducer_new`].
#[repr(C)]
pub struct kafka_producer_ProducerProperties_t {
    _private: [u8; 0],
}

/// A single record in a batch send call.
///
/// All fields use fixed-width types for cross-platform FFI portability.
///
/// # Field Conventions
///
/// - `partition`: Use `-1` for "no partition specified" (let the producer choose).
/// - `timestamp`: Use `-1` for "no timestamp" (let the producer stamp the record).
///   When `>= 0`, interpreted as milliseconds since epoch.
/// - `key_len`: Use `-1` to indicate no key. When `>= 0`, `key` must point to
///   a valid buffer of that length.
/// - `value_len`: Use `-1` to indicate no value. When `>= 0`, `value` must
///   point to a valid buffer of that length.
#[repr(C)]
pub struct kafka_producer_ProducerRecord_t {
    /// Null-terminated UTF-8 topic name.
    pub(crate) topic: *const c_char,
    /// Partition number, or -1 for unset.
    pub(crate) partition: i32,
    /// Timestamp in milliseconds since epoch, or -1 for unset.
    pub(crate) timestamp: i64,
    /// Pointer to key bytes, or null if no key.
    pub(crate) key: *const u8,
    /// Key length in bytes, or -1 for no key.
    pub(crate) key_len: i32,
    /// Pointer to value bytes, or null if no value.
    pub(crate) value: *const u8,
    /// Value length in bytes, or -1 for no value.
    pub(crate) value_len: i32,
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Casts a `*mut kafka_producer_Producer_t` to a reference to the internal `Mutex<ProducerKind>`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`kafka_producer_MockProducer_new`] (or a future constructor).
unsafe fn producer_ref(producer: *mut kafka_producer_Producer_t) -> &'static Mutex<ProducerKind> {
    // SAFETY: `producer_handle` requires a non-null pointer created by
    // `build_producer_handle`, which is exactly this function's own `# Safety` contract
    // (`producer` non-null and created by a producer constructor, all of which allocate
    // through `build_producer_handle`); the resulting `&'static ProducerHandle` is only
    // narrowed to its `kind` field, and the caller inherits the same lifetime obligation
    // (use within one C call, or a task registered via `reserve_pending_task`).
    &unsafe { producer_handle(producer) }.kind
}

/// Casts a `*mut kafka_producer_Producer_t` to a reference to the internal
/// [`ProducerHandle`] (the producer plus its async delivery machinery).
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`build_producer_handle`] (via a producer constructor).
unsafe fn producer_handle(producer: *mut kafka_producer_Producer_t) -> &'static ProducerHandle {
    // SAFETY: Per this function's `# Safety`, `producer` is non-null and was created by
    // `build_producer_handle`, which leaks a `Box<ProducerHandle>` via `Box::into_raw` and
    // hands the pointer to C, so the opaque `kafka_producer_Producer_t` pointer is a live,
    // aligned `*const ProducerHandle` until `kafka_producer_Producer_destroy` reclaims it.
    // Callers are responsible for the `&'static` lifetime: within one synchronous C call
    // the C caller keeps the handle alive, and the escaping uses (`submission_loop`,
    // `flush_or_close_async`, `partitions_for_async`, `with_txn_control_async`) register
    // their task via `reserve_pending_task` so `destroy` joins it before freeing the
    // handle.
    unsafe { &*(producer as *const ProducerHandle) }
}

/// Casts a `*mut kafka_common_KafkaFuture_RecordMetadata_t` to a reference to
/// [`FfiFuture`].
///
/// # Safety
///
/// The pointer must be non-null and must have been created by a send function.
unsafe fn future_ref(future: *mut kafka_common_KafkaFuture_RecordMetadata_t) -> &'static FfiFuture {
    // SAFETY: Per this function's `# Safety`, `future` is non-null and was created by a
    // send function, i.e. by `box_future`, which leaks a `Box<FfiFuture>` via
    // `Box::into_raw`; the pointer is therefore a live, aligned `*const FfiFuture` until
    // `kafka_common_KafkaFuture_RecordMetadata_destroy`/`_destroy_all` reclaims it, and
    // every caller uses the returned reference only within its own synchronous C call,
    // during which the C caller keeps the handle alive.
    unsafe { &*(future as *const FfiFuture) }
}

/// Casts a `*const kafka_producer_RecordMetadata_t` to a reference to `RecordMetadataInner`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`kafka_common_KafkaFuture_RecordMetadata_get`].
unsafe fn metadata_ref(metadata: *const kafka_producer_RecordMetadata_t) -> &'static RecordMetadataInner {
    // SAFETY: Per this function's `# Safety`, `metadata` is non-null and was created by
    // `kafka_common_KafkaFuture_RecordMetadata_get`, whose `box_metadata` leaks a
    // `Box<RecordMetadataInner>` via `Box::into_raw` (the other metadata-producing paths
    // use the same `box_metadata`); the pointer is therefore a live, aligned `*const
    // RecordMetadataInner` until `kafka_producer_RecordMetadata_destroy`/`_copy` reclaims
    // it, and callers use the returned reference only within their own synchronous C call,
    // during which the C caller keeps the handle alive.
    unsafe { &*(metadata as *const RecordMetadataInner) }
}

/// Send a record through the producer.
///
/// For [`ProducerKind::Kafka`] this builds a `ProducerRecord<&[u8], &[u8]>`
/// from the borrowed slices and sends via the zero-copy path.
///
/// For [`ProducerKind::Mock`] this falls back to the allocating path
/// since `MockProducer` expects owned records.
fn producer_send(
    kind: &ProducerKind,
    record: ProducerRecord<&[u8], &[u8]>,
) -> Result<KafkaFuture<RecordMetadata>, Error> {
    let rt = kind.runtime();
    match kind {
        ProducerKind::Mock(mock, _) => {
            let (topic, partition, timestamp, _headers, key, value) = record.into_parts();
            let owned_record = ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value.map(|v| v.to_vec()))
                .set_partition(partition)
                .set_timestamp(timestamp)
                .set_key(key.map(|k| k.to_vec()))
                .build()
                .and_then(|options| {
                    ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
                })?;
            rt.block_on(mock.send(owned_record))
        },
        ProducerKind::Kafka(producer, _) => rt.block_on(producer.send(record, None)),
    }
}

/// Send a record through the producer, also firing `callback` on completion.
///
/// Identical to [`producer_send`] except that the native [`Callback`] is
/// attached to the record, mirroring Java's `send(record, Callback)`: the
/// returned future *and* the callback both report the same outcome.
fn producer_send_with_callback(
    kind: &ProducerKind,
    record: ProducerRecord<&[u8], &[u8]>,
    callback: Callback,
) -> Result<KafkaFuture<RecordMetadata>, Error> {
    let rt = kind.runtime();
    match kind {
        ProducerKind::Mock(mock, _) => {
            let (topic, partition, timestamp, _headers, key, value) = record.into_parts();
            let owned_record = ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value.map(|v| v.to_vec()))
                .set_partition(partition)
                .set_timestamp(timestamp)
                .set_key(key.map(|k| k.to_vec()))
                .build()
                .and_then(|options| {
                    ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
                })?;
            rt.block_on(mock.send_with_callback(owned_record, Some(callback)))
        },
        ProducerKind::Kafka(producer, _) => rt.block_on(producer.send(record, Some(callback))),
    }
}

/// Casts a `*const kafka_producer_ProducerProperties_t` to a reference to
/// `HashMap<String, String>`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`kafka_producer_ProducerProperties_new`] or
/// [`kafka_producer_ProducerProperties_from_configs`].
unsafe fn properties_ref(props: *const kafka_producer_ProducerProperties_t) -> &'static HashMap<String, String> {
    // SAFETY: Per this function's `# Safety`, `props` is non-null and was created by
    // `kafka_producer_ProducerProperties_new` or `_from_configs`, both of which leak a
    // `Box<HashMap<String, String>>` via `Box::into_raw`; the pointer is therefore a live,
    // aligned `*const HashMap<String, String>` until
    // `kafka_producer_ProducerProperties_destroy` reclaims it, and the only caller
    // (`kafka_producer_KafkaProducer_new`) reads through the reference within its own call,
    // during which the C caller, who retains ownership, keeps the handle alive.
    unsafe { &*(props as *const HashMap<String, String>) }
}

/// Casts a `*mut kafka_producer_ProducerProperties_t` to a mutable reference
/// to `HashMap<String, String>`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`kafka_producer_ProducerProperties_new`] or
/// [`kafka_producer_ProducerProperties_from_configs`].
unsafe fn properties_mut(props: *mut kafka_producer_ProducerProperties_t) -> &'static mut HashMap<String, String> {
    // SAFETY: Per this function's `# Safety`, `props` is non-null and was created by
    // `kafka_producer_ProducerProperties_new` or `_from_configs`, i.e. it is a leaked
    // `Box<HashMap<String, String>>`, so the cast targets a live, aligned map. The
    // `&'static mut` is used by `kafka_producer_ProducerProperties_put` only for the
    // duration of that call; its exclusivity relies on the C caller not using the same
    // properties handle concurrently, which neither this `# Safety` nor the handle's
    // documentation spells out (see flags).
    unsafe { &mut *(props as *mut HashMap<String, String>) }
}

/// Wraps a `KafkaFuture<RecordMetadata>` and the producer's runtime handle
/// into a heap-allocated opaque pointer.
fn box_future(
    future: KafkaFuture<RecordMetadata>,
    runtime_handle: tokio::runtime::Handle,
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
) -> *mut kafka_common_KafkaFuture_RecordMetadata_t {
    let ffi_future = FfiFuture { future, runtime_handle, completion_tx };
    Box::into_raw(Box::new(ffi_future)) as *mut kafka_common_KafkaFuture_RecordMetadata_t
}

/// Wraps a [`RecordMetadata`] into a heap-allocated opaque pointer, including
/// a cached [`CString`] for the topic name.
fn box_metadata(metadata: RecordMetadata) -> *mut kafka_producer_RecordMetadata_t {
    // Construct the CString eagerly. If the topic contains an interior NUL
    // (which should never happen for valid Kafka topic names), replace it
    // with a fallback.
    let topic_cstring = CString::new(metadata.topic()).unwrap_or_else(|_| CString::new("").unwrap());
    let inner = RecordMetadataInner { metadata, topic_cstring };
    Box::into_raw(Box::new(inner)) as *mut kafka_producer_RecordMetadata_t
}

// ---------------------------------------------------------------------------
// Async (callback-based) delivery machinery
// ---------------------------------------------------------------------------
//
// The async API mirrors the librdkafka delivery-report model: each operation
// returns immediately and its result is delivered later through a C callback.
// All callbacks are invoked from a single per-producer **dispatcher thread**
// that drains a completion queue, so user callbacks run on one predictable
// thread and never on a tokio worker (a slow callback cannot stall producer
// I/O). The non-blocking send path funnels through one shared **submission
// task** (not a per-message `tokio::spawn`, per CLAUDE.md §13).

// Internal canonical callback signatures (not exported). There are only three
// distinct shapes; the public per-method typedefs below alias these. The
// `OperationCallbackFn` shape (a null `error` means success) lives in
// `crate::ffi::common` since it is reused by the consumer's void-returning ops.
//
// - Record:    `metadata` non-null on success / `error` non-null on failure.
// - Batch:     parallel `metadata[i]`/`errors[i]` arrays valid only for the call.
type RecordCallbackFn =
    unsafe extern "C" fn(*mut kafka_producer_RecordMetadata_t, *mut kafka_common_Error_t, *mut std::ffi::c_void);
type BatchCallbackFn = unsafe extern "C" fn(
    *mut *mut kafka_producer_RecordMetadata_t,
    *mut *mut kafka_common_Error_t,
    i32,
    *mut std::ffi::c_void,
);

// Public per-method callback typedefs. Per CLAUDE.md §4, an async callback type
// is named after its C method plus a `_callback` suffix, so each async function
// has its own typedef even when the underlying signature is shared. In every
// case the caller owns any non-null handle delivered to the callback and frees
// it with the matching `*_destroy`.

/// Completion callback for [`kafka_producer_Producer_send_async`], also used by
/// [`kafka_producer_Producer_send_with_callback`] (the delivery-report shape is
/// identical, so the latter reuses this typedef rather than adding a
/// `..._send_with_callback_callback_t` alias of the same signature).
///
/// `metadata` is non-null on success, `error` is non-null on failure. Note that
/// a real (non-mock) producer rejecting the record with a retriable/API-level
/// error delivers **both**: a placeholder `metadata` (offset and partition `-1`)
/// alongside the `error`, mirroring Java's
/// `callback.onCompletion(nullMetadata, e)` in `KafkaProducer.doSend`'s
/// `catch (ApiException e)` arm. That covers rejections *before* the record
/// accumulator (unresolvable metadata, `max.request.size` exceeded, invalid
/// topic) as well as rejections *inside* it (buffer exhaustion /
/// `max.block.ms` expiry). Test `error` first. The callee owns, and must
/// destroy, every non-null handle.
///
/// It does **not** cover a producer closed mid-send: Java raises a bare
/// `KafkaException` there (`RecordAccumulator.java:427-428`,
/// `BufferPool.java:119`/`:157`), which `doSend` rethrows from its
/// `catch (KafkaException e)` arm without invoking the callback
/// (`KafkaProducer.java:1073-1077`). The failure is reported by the return
/// code of the `send` call itself, and this callback never fires.
pub type kafka_producer_Producer_send_callback_t =
    unsafe extern "C" fn(*mut kafka_producer_RecordMetadata_t, *mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Per-record completion callback for [`kafka_producer_Producer_send_batch_async`].
pub type kafka_producer_Producer_send_batch_callback_t =
    unsafe extern "C" fn(*mut kafka_producer_RecordMetadata_t, *mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for [`kafka_common_KafkaFuture_RecordMetadata_get_async`].
pub type kafka_common_KafkaFuture_RecordMetadata_get_callback_t =
    unsafe extern "C" fn(*mut kafka_producer_RecordMetadata_t, *mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Aggregate completion callback for [`kafka_common_KafkaFuture_RecordMetadata_get_all_async`].
pub type kafka_common_KafkaFuture_RecordMetadata_get_all_callback_t = unsafe extern "C" fn(
    *mut *mut kafka_producer_RecordMetadata_t,
    *mut *mut kafka_common_Error_t,
    i32,
    *mut std::ffi::c_void,
);
/// Completion callback for [`kafka_producer_Producer_flush_async`].
pub type kafka_producer_Producer_flush_callback_t =
    unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for [`kafka_producer_Producer_close_async`].
pub type kafka_producer_Producer_close_callback_t =
    unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for [`kafka_producer_Producer_init_transactions_async`], the
/// async counterpart of [`kafka_producer_Producer_init_transactions`]. A null
/// `error` means success; a non-null [`kafka_common_Error_t`] is owned by the
/// callee, which frees it with [`kafka_common_Error_destroy`]. Named after the
/// Java `initTransactions` method per CLAUDE.md §4; all five transaction-control
/// completion typedefs alias the same [`OperationCallbackFn`] shape that
/// flush/close use.
pub type kafka_producer_Producer_init_transactions_callback_t =
    unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for [`kafka_producer_Producer_begin_transaction_async`]
/// (same shape as [`kafka_producer_Producer_init_transactions_callback_t`]).
pub type kafka_producer_Producer_begin_transaction_callback_t =
    unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for
/// [`kafka_producer_Producer_send_offsets_to_transaction_async`] (same shape as
/// [`kafka_producer_Producer_init_transactions_callback_t`]).
pub type kafka_producer_Producer_send_offsets_to_transaction_callback_t =
    unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for [`kafka_producer_Producer_commit_transaction_async`]
/// (same shape as [`kafka_producer_Producer_init_transactions_callback_t`]).
pub type kafka_producer_Producer_commit_transaction_callback_t =
    unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for [`kafka_producer_Producer_abort_transaction_async`]
/// (same shape as [`kafka_producer_Producer_init_transactions_callback_t`]).
pub type kafka_producer_Producer_abort_transaction_callback_t =
    unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Completion callback for [`kafka_producer_Producer_partitions_for_async`]. On
/// success `list` is a non-null [`kafka_common_PartitionInfoList_t`] (free with
/// [`kafka_common_PartitionInfoList_destroy`]) and `error` is null; on failure
/// `list` is null and `error` is non-null. The caller owns whichever is non-null.
/// (Named after the consumer sibling `..._partitions_for_callback_t` rather than
/// the `..._partitions_for_async_callback_t` that CLAUDE.md §4 would suggest, for
/// consistency with `kafka_consumer_Consumer_partitions_for_callback_t`.)
pub type kafka_producer_Producer_partitions_for_callback_t =
    unsafe extern "C" fn(*mut kafka_common_PartitionInfoList_t, *mut kafka_common_Error_t, *mut std::ffi::c_void);

/// Owned per-record completion payload, fired by the dispatcher thread.
struct RecordCompletion {
    callback: RecordCallbackFn,
    user_data: *mut std::ffi::c_void,
    metadata: *mut kafka_producer_RecordMetadata_t,
    error: *mut kafka_common_Error_t,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread;
// the C user is responsible for the thread-safety of `user_data`.
unsafe impl Send for RecordCompletion {}
impl RecordCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    unsafe fn fire(self) {
        // SAFETY: `self.callback` and `self.user_data` are the pair the C caller supplied
        // to the send function (captured in a `RecordCallbackTarget`), and
        // `self.metadata`/`self.error` are fresh owned handles from
        // `box_metadata`/`box_error` (or null) whose ownership transfers to the callee,
        // which frees them with the matching `*_destroy`. Consuming `self` makes this the
        // single invocation for this completion, performed on the dispatcher thread per
        // this method's `# Safety` (or inline on the completing thread once the dispatcher
        // has exited, the documented `enqueue_or_run_inline` fallback); `user_data` stays
        // valid until the callback fires per the caller's contract, and the C user is
        // responsible for its thread-safety.
        unsafe { (self.callback)(self.metadata, self.error, self.user_data) };
    }
}

/// Owned aggregate completion payload (`get_all_async`).
struct RecordBatchCompletion {
    callback: BatchCallbackFn,
    user_data: *mut std::ffi::c_void,
    metadata: Vec<*mut kafka_producer_RecordMetadata_t>,
    errors: Vec<*mut kafka_common_Error_t>,
}
// SAFETY: see `RecordCompletion`.
unsafe impl Send for RecordBatchCompletion {}
impl RecordBatchCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    unsafe fn fire(mut self) {
        let count = self.metadata.len() as i32;
        // SAFETY: `self.callback`/`self.user_data` are the pair the C caller supplied to
        // `kafka_common_KafkaFuture_RecordMetadata_get_all_async`; `self.metadata` and
        // `self.errors` are `Vec`s pushed in lockstep (one entry each per future), so both
        // arrays hold exactly `count = self.metadata.len()` entries and stay allocated for
        // the duration of the call, after which only the `Vec` storage is freed. Every
        // non-null handle inside was freshly built by `box_metadata`/`box_error` and
        // ownership transfers to the callee. Consuming `self` makes this the single
        // invocation, on the dispatcher thread per this method's `# Safety` (or inline
        // post-teardown per `enqueue_or_run_inline`); the C user is responsible for the
        // thread-safety of `user_data`.
        unsafe { (self.callback)(self.metadata.as_mut_ptr(), self.errors.as_mut_ptr(), count, self.user_data) };
        // The array storage (`Vec`s) is freed here when `self` drops; the
        // individual handle pointers were handed to the caller, which owns them.
    }
}

/// A C callback target (function pointer + opaque `user_data`) captured by a
/// Rust [`Callback`]. Wrapped so it can cross the tokio task / dispatcher
/// thread boundary.
#[derive(Clone, Copy)]
struct RecordCallbackTarget {
    callback: RecordCallbackFn,
    user_data: *mut std::ffi::c_void,
}
// SAFETY: the C user owns the thread-safety of `user_data`; the function
// pointer is trivially shareable.
unsafe impl Send for RecordCallbackTarget {}
// SAFETY: a `&RecordCallbackTarget` exposes only the two `Copy` fields, and the
// type never dereferences `user_data`, so sharing it across threads adds no
// access beyond what the C contract on `user_data` already allows.
unsafe impl Sync for RecordCallbackTarget {}

/// Aggregate-callback target for `get_all_async`. See [`RecordCallbackTarget`].
#[derive(Clone, Copy)]
struct RecordBatchCallbackTarget {
    callback: BatchCallbackFn,
    user_data: *mut std::ffi::c_void,
}
// SAFETY: see `RecordCallbackTarget`.
unsafe impl Send for RecordBatchCallbackTarget {}

/// Builds a native producer [`Callback`] that, when fired on completion,
/// converts the borrowed metadata/error into owned C handles and enqueues a
/// [`CompletionJob`] for the dispatcher thread.
///
/// `fired` is a per-record at-most-once guard **shared** by every callback built
/// for the same record (the one handed to `producer.send`, and any the submission
/// task fires itself on error). Only the first to win the compare-exchange
/// delivers; the rest are no-ops. This is what makes the delivery report fire
/// **exactly once**: `producer.send` returning `Err` is ambiguous — on its
/// early-guard paths (`ensure_not_closed`, `throw_if_in_prepared_state`,
/// `wait_on_metadata` error) the callback was dropped unfired and the task must
/// fire it, but on its post-append path (`do_send_bytes` `maybe_add_partition`
/// failing after `accumulator.append` already moved the callback into a batch,
/// `kafka_producer.rs:1129`) the batch still fires it later. The submission task
/// cannot tell these apart from the `Err` alone, so it fires unconditionally on
/// `Err` and lets the guard collapse the post-append case's second fire — which
/// would otherwise double-free `user_data`.
fn make_record_callback(
    target: RecordCallbackTarget,
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
    fired: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Callback {
    Box::new(move |metadata: Option<&RecordMetadata>, error: Option<&Error>| {
        // At-most-once: if another callback for this record already delivered, do
        // nothing — including freeing no handles, so `user_data` is freed once.
        if fired
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        // Capture the whole `Send + Sync` wrapper (not its raw-pointer field,
        // which disjoint closure capture would otherwise grab directly).
        let target = target;
        let metadata_ptr = metadata.map(|m| box_metadata(m.clone())).unwrap_or(std::ptr::null_mut());
        let error_ptr = error.map(|e| box_error(e.clone())).unwrap_or(std::ptr::null_mut());
        let completion = RecordCompletion {
            callback: target.callback,
            user_data: target.user_data,
            metadata: metadata_ptr,
            error: error_ptr,
        };
        // SAFETY: `RecordCompletion::fire` requires exactly one call on the dispatcher
        // thread: `completion` is moved into this `FnOnce` job, which
        // `enqueue_or_run_inline` runs exactly once, on the dispatcher thread while it
        // lives or inline once it has exited (the documented post-teardown fallback). The
        // `fired` compare-exchange above guarantees that only one of the callbacks built
        // for this record ever reaches this point, so the C caller's `callback`/`user_data`
        // pair fires once and the fresh `metadata_ptr`/`error_ptr` handles are handed over
        // exactly once.
        let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
        // If the dispatcher is gone (post-teardown), run inline to honor the
        // callback obligation rather than leak the owned handles.
        enqueue_or_run_inline(&completion_tx, job);
    })
}

/// A non-blocking send submitted to the per-producer submission task.
///
/// Holds a fully-built `ProducerRecord` (validated synchronously in
/// `send_async` / `send_batch_async`) whose `key`/`value` borrow into the C
/// caller's memory, lifetime-extended to `'static` under the documented
/// contract that the caller keeps the buffers valid until the completion
/// callback fires. (`ProducerRecord<&'static [u8], &'static [u8]>` is `Send`.)
///
/// Carries the `Copy` [`RecordCallbackTarget`] rather than a pre-built
/// [`Callback`], so the submission task can honour the callback even when the
/// producer's `send` drops it unfired: `send` moves the `Callback` in by value and
/// its early-error paths (`ensure_not_closed`, `throw_if_in_prepared_state`, a
/// non-`ApiException` metadata error) return `Err` without invoking it. Keeping
/// the target lets the task fire once from a fresh callback on that `Err`
/// (CLAUDE.md §11.5), while the delivery path owns the single fire on success.
struct SendRequest {
    record: ProducerRecord<&'static [u8], &'static [u8]>,
    target: RecordCallbackTarget,
}

/// An item on the submission channel.
///
/// The channel carries an ordering barrier as well as sends, so `flush`/`close` —
/// and every transaction-control call ([`with_txn_control`]) — can wait for records
/// the application queued with `send_async` to be handed to the producer before they
/// proceed: `send_async` only *queues* a record — the real `producer.send()` happens
/// later, on the submission task — so without the barrier a `flush`/`close`/commit/
/// abort could proceed with a queued record still unsent. See
/// [`drain_submitted_sends_await`]. (The same barrier serves both the
/// non-transactional `flush`/`close` drain and the transaction-control drain that
/// makes async sends supported inside a transaction — see the module-level
/// "Concurrency model" docs.)
enum SubmitRequest {
    /// A non-blocking send to hand to the producer.
    Send(SendRequest),
    /// A marker placed behind a set of queued sends.
    ///
    /// Signals `ack` if one was supplied. FIFO delivery is what makes it a
    /// barrier: the task fully finishes each send before taking the next item, so
    /// dequeuing this marker means everything ahead of it is done.
    Barrier {
        ack: Option<tokio::sync::oneshot::Sender<()>>,
    },
}

/// A lifetime-extended reference to the inner producer, obtained from the
/// leaked producer handle. Sound while the handle is alive: every task that
/// calls [`producer_static_ref`] registers its `JoinHandle` via
/// [`reserve_pending_task`], and `destroy` joins all of them before dropping
/// the producer.
enum ProducerStaticRef {
    Kafka(&'static KafkaProducer<Vec<u8>, Vec<u8>>),
    Mock(&'static MockProducer<Vec<u8>, Vec<u8>>),
}

/// Obtains a [`ProducerStaticRef`] from a leaked producer handle pointer,
/// holding the `kind` mutex only briefly (never across an `.await`).
///
/// # Safety
/// `ptr` must be a live `*const ProducerHandle` (leaked, not yet destroyed).
unsafe fn producer_static_ref(ptr: usize) -> ProducerStaticRef {
    // SAFETY: Per this function's `# Safety`, `ptr` is a live `*const ProducerHandle`
    // leaked by `build_producer_handle` (via `Box::into_raw`) and not yet destroyed, so the
    // dereference is of a valid, aligned allocation. The `&ProducerHandle` is used only
    // within this function, to lock `kind`; the callers bound the lifetime of what they
    // derive from it — `submission_loop`, `flush_or_close_async`, `partitions_for_async`
    // and `with_txn_control_async` run in tasks registered via `reserve_pending_task` that
    // `destroy` joins before freeing the handle, and `with_txn_control` uses it
    // synchronously inside a C call during which the C caller keeps the handle alive.
    let handle = unsafe { &*(ptr as *const ProducerHandle) };
    let guard = handle.kind.lock().unwrap();
    match &*guard {
        ProducerKind::Kafka(k, _) => {
            // SAFETY: `k` is the `Box<KafkaProducer>` stored in `ProducerKind::Kafka`, so
            // `k.as_ref()` points at a heap allocation that is never moved or replaced
            // while the handle lives (`kind` is only ever consumed by `destroy`, via
            // `into_inner`); extending the borrow to `'static` is sound under this
            // function's `# Safety` (a live, not yet destroyed handle), and every use is
            // bounded by the caller's keep-alive — a task registered via
            // `reserve_pending_task` that `destroy` joins before dropping `kind`, or
            // `with_txn_control`'s synchronous `block_on` inside a C call — as documented
            // on `ProducerStaticRef`. The `kind` guard protects only this lookup and is
            // dropped on return, so no lock is held across the callers' `.await`s.
            ProducerStaticRef::Kafka(unsafe { &*(k.as_ref() as *const KafkaProducer<Vec<u8>, Vec<u8>>) })
        },
        ProducerKind::Mock(m, _) => {
            // SAFETY: `m` is the `Box<MockProducer>` stored in `ProducerKind::Mock`, so
            // `m.as_ref()` points at a heap allocation that is never moved or replaced
            // while the handle lives (`kind` is only ever consumed by `destroy`, via
            // `into_inner`); extending the borrow to `'static` is sound under this
            // function's `# Safety` (a live, not yet destroyed handle), and every use is
            // bounded by the caller's keep-alive — a task registered via
            // `reserve_pending_task` that `destroy` joins before dropping `kind`, or
            // `with_txn_control`'s synchronous `block_on` inside a C call — as documented
            // on `ProducerStaticRef`. The `kind` guard protects only this lookup and is
            // dropped on return.
            ProducerStaticRef::Mock(unsafe { &*(m.as_ref() as *const MockProducer<Vec<u8>, Vec<u8>>) })
        },
    }
}

/// Decrements [`ProducerHandle::queued_sends`] once the submission task is done
/// with a request, whether it was produced, discarded, or panicked on. Keeping it
/// in a `Drop` means the counter cannot drift, and a drifted counter would make
/// every later barrier wait for a request that no longer exists.
struct QueueDepthGuard<'a>(&'a ProducerHandle);
impl Drop for QueueDepthGuard<'_> {
    fn drop(&mut self) {
        self.0.queued_sends.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// The shared submission task: drives non-blocking sends to enqueue off the
/// caller's thread. One task per producer (not a per-message spawn, §11).
async fn submission_loop(ptr: usize, mut rx: tokio::sync::mpsc::UnboundedReceiver<SubmitRequest>) {
    while let Some(request) = rx.recv().await {
        let SendRequest { record, target } = match request {
            SubmitRequest::Send(send) => send,
            SubmitRequest::Barrier { ack } => {
                // FIFO delivery means reaching this marker proves every send ahead
                // of it has been fully handed over; acking it is what lets
                // `flush`/`close` wait for the non-transactional async queue to
                // drain (see `drain_submitted_sends_await`).
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
                continue;
            },
        };
        // SAFETY: the handle outlives the submission task — `destroy` joins this
        // task (registered via `reserve_pending_task`) before dropping the
        // producer it borrows from. Reached on the send path only, exactly where
        // `producer_static_ref(ptr)` below already dereferences the same handle.
        let handle = unsafe { &*(ptr as *const ProducerHandle) };
        // Decremented only once the send below has fully completed, so the counter
        // means "queued or in flight": a barrier has to wait for an in-flight
        // handover too, not merely for the queue to empty.
        let _depth = QueueDepthGuard(handle);
        // One at-most-once guard per record, shared by every callback built below:
        // the one handed to `send` (which may fire via a batch later) and the
        // task's own error re-fire. Whichever fires first wins; the rest no-op, so
        // the C delivery report — and the free of `user_data` — happens exactly
        // once even on `send`'s ambiguous post-append `Err` path (see
        // `make_record_callback`).
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completion_tx = handle.completion_tx.clone();
        let fire_error = |error: Error| {
            make_record_callback(target, completion_tx.clone(), std::sync::Arc::clone(&fired))(None, Some(&error));
        };
        // The callback handed to `send` shares `fired` with `fire_error`, so at
        // most one of the two delivers.
        let callback = make_record_callback(target, handle.completion_tx.clone(), std::sync::Arc::clone(&fired));
        // Brief lock to extend a reference to the inner producer; guard dropped
        // before the `.await` below (CLAUDE.md §11.6).
        // SAFETY: `ptr` is the same live leaked `*const ProducerHandle` as `handle` above,
        // so `producer_static_ref`'s `# Safety` (a live, not yet destroyed handle) holds
        // for the same reason: this task is registered via `reserve_pending_task` and
        // `destroy` joins it before dropping the producer. `producer_static_ref` takes and
        // releases the `kind` lock internally, so no guard is held across the
        // `kp.send`/`mp.send_with_callback` `.await`s below, and the extended
        // inner-producer reference is used only within this iteration.
        match unsafe { producer_static_ref(ptr) } {
            ProducerStaticRef::Kafka(kp) => {
                // Hot path: the borrowed record is sent directly — no copy, no
                // field reconstruction. On `Err`, fire the guarded callback: it is
                // a no-op if a batch already owns and will fire the original.
                if let Err(e) = kp.send(record, Some(callback)).await {
                    fire_error(e);
                }
            },
            ProducerStaticRef::Mock(mp) => {
                // MockProducer takes an owned record; copy the borrowed bytes
                // (test helper, not a hot path). Re-validation cannot fail since
                // the record was already built in `send_async`.
                let (topic, partition, timestamp, headers, key, value) = record.into_parts();
                match ProducerRecordOptionsBuilder::new()
                    .set_topic(topic)
                    .set_value(value.map(|v| v.to_vec()))
                    .set_partition(partition)
                    .set_timestamp(timestamp)
                    .set_key(key.map(|k| k.to_vec()))
                    .set_headers(Some(headers))
                    .build()
                    .and_then(|options| {
                        ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
                    }) {
                    Ok(record) => {
                        if let Err(e) = mp.send_with_callback(record, Some(callback)).await {
                            fire_error(e);
                        }
                    },
                    // `callback` is unused here (record build failed before send);
                    // firing it delivers once (it shares `fired`).
                    Err(e) => callback(None, Some(&e)),
                }
            },
        }
    }
}

/// Per-producer handle state: the producer behind its `Mutex`, plus the async
/// delivery machinery (completion queue + dispatcher thread + submission task).
struct ProducerHandle {
    kind: Mutex<ProducerKind>,
    /// Sender for the completion-dispatch queue (closures run by the dispatcher).
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
    /// Sender for the non-blocking send submission channel.
    submit_tx: tokio::sync::mpsc::UnboundedSender<SubmitRequest>,
    /// Dispatcher thread join handle; taken and joined on destroy.
    dispatcher: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Join handles for every task spawned on this producer's runtime that
    /// reaches into the producer via [`producer_static_ref`]: the long-lived
    /// submission task, plus one short-lived task per `flush_async` /
    /// `close_async` / `partitions_for_async` / transaction-control `_async`
    /// call. `destroy` joins all of these *before* dropping the producer, since
    /// each holds a raw `&'static` reference into it that must not outlive its
    /// memory.
    pending_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Transaction-control mutual-exclusion flag. `true` while one of the five
    /// transaction-control functions is executing. See the module-level
    /// "Concurrency model" docs for the model; it deliberately does **not** cover
    /// `send`.
    ///
    /// Taken and released by [`with_txn_control`], which is the only way to reach
    /// it: routing every control function through that closure is what makes the
    /// flag impossible to skip by accident, so a sixth control function added later
    /// inherits the mutual exclusion by construction.
    txn_control_busy: std::sync::atomic::AtomicBool,
    /// Non-blocking sends that have been queued but not yet fully handed to the
    /// producer (in-flight included).
    ///
    /// Exists so the overwhelmingly common case — no async send outstanding —
    /// costs `flush`/`close` one atomic load instead of a channel round-trip
    /// through the submission task. See [`drain_submitted_sends_await`].
    queued_sends: std::sync::atomic::AtomicUsize,
}

/// Locks `pending_tasks` and reserves room for one more task, so the caller can
/// register a task for `destroy` to join before the producer is freed. Prunes
/// already-finished handles first so `pending_tasks` does not grow unbounded over
/// a producer's lifetime under repeated `flush_async` / `close_async` /
/// `partitions_for_async` / transaction-control `_async` calls.
///
/// This is the first half of a two-phase registration: the caller keeps the
/// returned guard across its `spawn` and then `push`es the new `JoinHandle`, which
/// cannot fail because the room is already reserved. Everything that can panic —
/// the lock (poisoned by an earlier caught panic) and the allocation — therefore
/// happens *before* the task exists, and a panic in the spawn itself aborts the
/// process ([`spawn_callback_task`]). That is what lets `#[ffi_guard]` fire a
/// callback-style entry point's callback on a panic without a task already
/// spawned delivering a second completion (D4 of
/// `design/current/appsec-7665-4521-ffi-panic-guard.md`). The spawned tasks never
/// take this lock, so holding it across the `spawn` cannot deadlock.
fn reserve_pending_task(handle: &ProducerHandle) -> std::sync::MutexGuard<'_, Vec<tokio::task::JoinHandle<()>>> {
    let mut tasks = handle.pending_tasks.lock().unwrap();
    tasks.retain(|t| !t.is_finished());
    tasks.reserve(1);
    tasks
}

/// Builds a [`ProducerHandle`] around a [`ProducerKind`], spawning the
/// dispatcher thread and the submission task, and returns the leaked C handle.
fn build_producer_handle(kind: ProducerKind) -> *mut kafka_producer_Producer_t {
    let (completion_tx, dispatcher) = common::spawn_dispatcher("kafka-producer-callback-dispatcher");
    let (submit_tx, submit_rx) = tokio::sync::mpsc::unbounded_channel::<SubmitRequest>();

    let rt_handle = kind.runtime().handle().clone();

    let handle = Box::new(ProducerHandle {
        kind: Mutex::new(kind),
        completion_tx,
        submit_tx,
        dispatcher: Mutex::new(Some(dispatcher)),
        pending_tasks: Mutex::new(Vec::new()),
        txn_control_busy: std::sync::atomic::AtomicBool::new(false),
        queued_sends: std::sync::atomic::AtomicUsize::new(0),
    });
    let ptr = Box::into_raw(handle);

    // Spawn the submission task on the producer's runtime, capturing the leaked
    // handle pointer (as `usize` to cross the task boundary). Stash the join
    // handle back on the handle itself so `destroy` can wait for the task to
    // actually finish before freeing the producer it borrows from.
    // SAFETY: `ptr` was just produced by `Box::into_raw(handle)`, so it is non-null,
    // aligned and points at a live `ProducerHandle` that nothing else references yet (the
    // submission task is spawned only afterwards and the pointer reaches C only on return);
    // the shared reference is used solely for this `reserve_pending_task` call, and the
    // `pending` guard derived from it is dropped before the function returns.
    let mut pending = reserve_pending_task(unsafe { &*ptr });
    let task = rt_handle.spawn(submission_loop(ptr as usize, submit_rx));
    pending.push(task);
    drop(pending);

    ptr as *mut kafka_producer_Producer_t
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Creates a new mock producer.
///
/// # Parameters
///
/// - `auto_complete`: If `true`, sends complete immediately. If `false`, the
///   caller must use [`kafka_producer_MockProducer_complete_next`] or
///   [`kafka_producer_MockProducer_error_next`] to resolve sends.
///
/// # Returns
///
/// A non-null opaque producer handle, or null on failure (should not happen
/// for mock producers).
///
/// # Safety
///
/// The returned handle must eventually be freed with [`kafka_producer_Producer_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_MockProducer_new(auto_complete: bool) -> *mut kafka_producer_Producer_t {
    // A multi-thread runtime so the async submission task and completion
    // callbacks are driven in the background (a current-thread runtime only
    // makes progress inside `block_on`, which the non-blocking async API does
    // not call, so the submission task would never be polled).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for MockProducer");
    let kind = ProducerKind::Mock(Box::new(MockProducer::with_auto_complete(auto_complete)), runtime);
    build_producer_handle(kind)
}

// ---------------------------------------------------------------------------
// ProducerProperties
// ---------------------------------------------------------------------------

/// Creates an empty producer properties handle.
///
/// # Returns
///
/// A non-null opaque properties handle. The caller must free it with
/// [`kafka_producer_ProducerProperties_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_ProducerProperties_new() -> *mut kafka_producer_ProducerProperties_t {
    let map: HashMap<String, String> = HashMap::new();
    Box::into_raw(Box::new(map)) as *mut kafka_producer_ProducerProperties_t
}

/// Creates producer properties from a NULL-terminated flat array of C strings.
///
/// The array contains alternating key-value pairs terminated by a NULL pointer:
/// `["key1", "val1", "key2", "val2", ..., NULL]`.
///
/// # Parameters
///
/// - `configs`: Pointer to a NULL-terminated array of null-terminated C strings.
///   Entries are read in pairs (key, value) until a NULL pointer is encountered.
///
/// # Returns
///
/// A non-null opaque properties handle on success, or NULL if:
/// - `configs` is NULL
/// - An odd number of non-NULL entries is found (missing value for a key)
///
/// The caller must free a non-null handle with
/// [`kafka_producer_ProducerProperties_destroy`].
///
/// # Safety
///
/// - `configs` must be NULL or point to a NULL-terminated array of valid,
///   null-terminated C strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerProperties_from_configs(
    configs: *const *const c_char,
) -> *mut kafka_producer_ProducerProperties_t {
    if configs.is_null() {
        return std::ptr::null_mut();
    }

    let mut map: HashMap<String, String> = HashMap::new();
    let mut i = 0usize;
    loop {
        // SAFETY: `configs` is non-null (checked above) and, per this function's `#
        // Safety`, points to a NULL-terminated array of C-string pointers; the loop only
        // reads entry `i` after every earlier entry was read and found non-null, so the
        // terminator guarantees `configs.add(i)` is still inside the array and readable.
        let key_ptr = unsafe { *configs.add(i) };
        if key_ptr.is_null() {
            break;
        }
        // SAFETY: `configs.add(i + 1)` is read only after `*configs.add(i)` was found
        // non-null; because the array is NULL-terminated per this function's `# Safety`, a
        // non-null key cannot be the last element, so the value slot exists and is readable
        // (a NULL there is the terminator itself and is handled as the odd-count error).
        let val_ptr = unsafe { *configs.add(i + 1) };
        if val_ptr.is_null() {
            // Odd number of entries — missing value for the last key.
            return std::ptr::null_mut();
        }
        // SAFETY: `key_ptr` is non-null (checked above) and, per this function's `#
        // Safety`, every entry before the terminator is a valid null-terminated C string;
        // the `CStr` is copied into an owned `String` immediately, so nothing borrowed
        // outlives the call.
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_string_lossy().to_string();
        // SAFETY: `val_ptr` is non-null (checked above) and, per this function's `#
        // Safety`, every entry before the terminator is a valid null-terminated C string;
        // the `CStr` is copied into an owned `String` immediately, so nothing borrowed
        // outlives the call.
        let val = unsafe { CStr::from_ptr(val_ptr) }.to_string_lossy().to_string();
        map.insert(key, val);
        i += 2;
    }

    Box::into_raw(Box::new(map)) as *mut kafka_producer_ProducerProperties_t
}

/// Adds or overwrites a configuration key-value pair.
///
/// # Parameters
///
/// - `props`: Non-null properties handle.
/// - `key`: Non-null, null-terminated configuration key (e.g., `"bootstrap.servers"`).
/// - `value`: Non-null, null-terminated configuration value.
///
/// No-op if any parameter is null.
///
/// # Safety
///
/// - `props` must be a valid handle from [`kafka_producer_ProducerProperties_new`]
///   or [`kafka_producer_ProducerProperties_from_configs`].
/// - `key` and `value` must be valid, null-terminated C strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerProperties_put(
    props: *mut kafka_producer_ProducerProperties_t,
    key: *const c_char,
    value: *const c_char,
) {
    if props.is_null() || key.is_null() || value.is_null() {
        return;
    }
    // SAFETY: `props` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from `kafka_producer_ProducerProperties_new` or `_from_configs`, which
    // is what `properties_mut` requires; the `&'static mut HashMap` is used only for the
    // `insert` in this call, during which the C caller keeps the handle alive. The
    // exclusivity of the mutable borrow relies on the C caller not touching the same
    // properties handle concurrently (another `put`, or `kafka_producer_KafkaProducer_new`
    // reading it), which this `# Safety` does not state (see flags).
    let map = unsafe { properties_mut(props) };
    // SAFETY: `key` is non-null (checked above) and, per this function's `# Safety`, a
    // valid null-terminated C string; it is copied into an owned `String` at once, so
    // nothing borrowed outlives the call.
    let k = unsafe { CStr::from_ptr(key) }.to_string_lossy().to_string();
    // SAFETY: `value` is non-null (checked above) and, per this function's `# Safety`, a
    // valid null-terminated C string; it is copied into an owned `String` at once, so
    // nothing borrowed outlives the call.
    let v = unsafe { CStr::from_ptr(value) }.to_string_lossy().to_string();
    map.insert(k, v);
}

/// Destroys a properties handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `props` must be null or a valid handle from
///   [`kafka_producer_ProducerProperties_new`] or
///   [`kafka_producer_ProducerProperties_from_configs`].
/// - After this call, the pointer is invalid and must not be used.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerProperties_destroy(props: *mut kafka_producer_ProducerProperties_t) {
    if !props.is_null() {
        // SAFETY: `props` is non-null (checked above) and, per this function's `# Safety`,
        // a valid handle from `kafka_producer_ProducerProperties_new` or `_from_configs`,
        // both of which created it with `Box::into_raw(Box::new(HashMap<String, String>))`;
        // the same `# Safety` declares the pointer invalid after this call, so this
        // `Box::from_raw` is the single, final use of the allocation.
        unsafe {
            drop(Box::from_raw(props as *mut HashMap<String, String>));
        }
    }
}

// ---------------------------------------------------------------------------
// KafkaProducer
// ---------------------------------------------------------------------------

/// Creates a new Kafka producer connected to a real cluster.
///
/// Configuration is passed via a [`kafka_producer_ProducerProperties_t`]
/// handle, mirroring Java's `new KafkaProducer(Properties)`. Keys use the
/// standard Kafka config names (e.g., `"bootstrap.servers"`, `"batch.size"`).
///
/// # Parameters
///
/// - `props`: Non-null properties handle created via
///   [`kafka_producer_ProducerProperties_new`] or
///   [`kafka_producer_ProducerProperties_from_configs`]. The caller retains
///   ownership and must free it separately.
/// - `out_error`: Pointer where an error handle will be written on failure,
///   or null if the caller does not need error details.
///
/// # Returns
///
/// A non-null producer handle on success, or null on failure.
/// If `out_error` is non-null, `*out_error` is set to null on success or
/// to a valid [`kafka_common_Error_t`] handle on failure (caller must
/// free it with [`kafka_common_Error_destroy`]).
///
/// # Safety
///
/// - `props` must be a valid, non-null properties handle.
/// - The returned handle must eventually be freed with
///   [`kafka_producer_Producer_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_new(
    props: *const kafka_producer_ProducerProperties_t,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_producer_Producer_t {
    init_default_logger();

    if props.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    // SAFETY: `props` is non-null (checked above) and, per this function's `# Safety`, a
    // valid properties handle from `kafka_producer_ProducerProperties_new` or
    // `_from_configs`, which is what `properties_ref` requires; the `&'static HashMap` is
    // read only by `ProducerConfig::new` within this call, during which the C caller, who
    // retains ownership of the properties, keeps the handle alive.
    let map = unsafe { properties_ref(props) };
    let config = match ProducerConfig::new(map) {
        Ok(c) => c,
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    // Create a multi-thread tokio runtime for the producer's background sender task.
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(Error::local_illegal_state("failed to create tokio runtime")) };
            }
            return std::ptr::null_mut();
        },
    };

    // Enter the runtime so that KafkaProducer::new can call tokio::task::spawn.
    let _guard = runtime.enter();
    let producer = match KafkaProducer::<Vec<u8>, Vec<u8>>::new(
        config,
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    ) {
        Ok(p) => p,
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    let kind = ProducerKind::Kafka(Box::new(producer), runtime);
    if !out_error.is_null() {
        // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
        // Parameters`, a pointer where an error handle will be written on failure or null
        // if the caller does not need error details; exactly one element is written (null,
        // meaning success).
        unsafe { *out_error = std::ptr::null_mut() };
    }
    build_producer_handle(kind)
}

/// Destroys a producer handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op in that case).
///
/// # Safety
///
/// - `producer` must be null or a valid handle from a `kafka_producer_new_*`
///   function.
/// - After this call, the pointer is invalid and must not be used.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_destroy(producer: *mut kafka_producer_Producer_t) {
    if producer.is_null() {
        return;
    }
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a producer constructor, i.e. the pointer `build_producer_handle`
    // leaked with `Box::into_raw(Box<ProducerHandle>)`; the same `# Safety` declares the
    // pointer invalid after this call, so this `Box::from_raw` is the single, final use.
    // The destructuring below drops `submit_tx` and joins every task registered in
    // `pending_tasks` before dropping `kind`, so no `&'static` reference derived from this
    // allocation outlives it.
    let handle = unsafe { Box::from_raw(producer as *mut ProducerHandle) };
    let ProducerHandle {
        kind,
        completion_tx,
        submit_tx,
        dispatcher,
        pending_tasks,
        // No teardown obligation: the flag is a plain atomic and the counter only
        // meant "queued sends outstanding", which drop(submit_tx) below drains.
        txn_control_busy: _,
        queued_sends: _,
    } = *handle;

    // 1. Stop accepting new sends; the submission task's `recv()` returns `None`
    //    and the task ends.
    drop(submit_tx);
    // 2. Wait for every task that reaches into the producer to actually finish.
    //    Each one may still be mid-`.await` on the producer (see
    //    `producer_static_ref`'s callers: `submission_loop`,
    //    `flush_or_close_async`, `partitions_for_async`), holding a raw
    //    `&'static` reference into it. Dropping `kind` (next step) frees that
    //    memory, so we must join here first or risk a use-after-free race.
    //
    //    Both locks are read poison-tolerantly. A lock is poisoned only by a panic
    //    that `#[ffi_guard]` caught, after which the C header tells the caller to
    //    destroy the handle, so this is the one call that must still work on a
    //    poisoned handle. Reading poison as "no tasks", or panicking on `kind`,
    //    would skip the join and free the producer under a task still using it.
    let tasks = pending_tasks.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
    let kind = kind.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
    if !tasks.is_empty() {
        kind.runtime().block_on(async {
            for task in tasks {
                let _ = task.await;
            }
        });
    }
    // 3. Drop the producer and its runtime. The producer's `Drop` force-closes;
    //    dropping the runtime waits for any other remaining tasks. Any
    //    in-flight record callbacks fire here and enqueue jobs onto the
    //    still-open completion channel (the clones live inside those callbacks).
    drop(kind);
    // 4. Close the completion channel; the dispatcher drains remaining jobs
    //    (firing their callbacks) and then exits. Join it.
    //
    // NOTE: every live future handle (`FfiFuture`) and in-flight callback holds
    // a clone of `completion_tx`. The dispatcher exits only once all clones are
    // gone, so joining here would hang if the caller destroys the producer
    // while futures/callbacks are still outstanding (a contract violation, but
    // we must not deadlock). We therefore detach the dispatcher: dropping our
    // sender lets it exit as soon as the remaining clones are released.
    drop(completion_tx);
    drop(dispatcher.into_inner().unwrap_or(None));
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

/// Sends a single record through the producer.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `topic`: Non-null, null-terminated UTF-8 topic name.
/// - `partition`: Partition number, or `-1` for no partition hint.
/// - `timestamp`: Timestamp in milliseconds since epoch, or `-1` to let
///   the producer stamp the record.
/// - `key`: Pointer to key bytes, or null if `key_len` is `-1`.
/// - `key_len`: Key length in bytes, or `-1` for no key.
/// - `value`: Pointer to value bytes, or null if `value_len` is `-1`.
/// - `value_len`: Value length in bytes, or `-1` for no value.
/// - `out_error`: Pointer where an error handle will be written on failure,
///   or null if the caller does not need error details.
///
/// # Returns
///
/// A non-null future handle on success, or null on failure.
/// If `out_error` is non-null, `*out_error` is set to null on success or
/// to a valid [`kafka_common_Error_t`] handle on failure.
///
/// # Safety
///
/// - `producer` must be a valid handle.
/// - `topic` must be a valid C string.
/// - `key` must be valid for `key_len` bytes if `key_len >= 0`.
/// - `value` must be valid for `value_len` bytes if `value_len >= 0`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send(
    producer: *mut kafka_producer_Producer_t,
    topic: *const c_char,
    partition: i32,
    timestamp: i64,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_RecordMetadata_t {
    if producer.is_null() || topic.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    // SAFETY: `topic` is non-null (checked above) and, per this function's `# Safety`, a
    // valid null-terminated C string; it is copied into an owned `String` immediately, so
    // nothing borrowed outlives the call.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().into_owned();

    let key_slice: Option<&[u8]> = if key_len >= 0 {
        if key.is_null() {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        // SAFETY: This branch is reached only when `key_len >= 0` (so `key_len as usize` is
        // the non-negative length) and `key` is non-null (checked above); per this
        // function's `# Safety`, `key` is then valid for `key_len` bytes. The slice is used
        // only within this call: `producer_send` writes the bytes into the accumulator's
        // batch buffer (or copies them for the mock) before returning, as the zero-copy
        // contract documented on `kafka_producer_Producer_send_with_callback` states for
        // both synchronous sends.
        Some(unsafe { std::slice::from_raw_parts(key, key_len as usize) })
    } else {
        None
    };

    let value_slice: Option<&[u8]> = if value_len >= 0 {
        if value.is_null() {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        // SAFETY: This branch is reached only when `value_len >= 0` (so `value_len as
        // usize` is the non-negative length) and `value` is non-null (checked above); per
        // this function's `# Safety`, `value` is then valid for `value_len` bytes. The
        // slice is used only within this call: `producer_send` writes the bytes into the
        // accumulator's batch buffer (or copies them for the mock) before returning, as the
        // zero-copy contract documented on `kafka_producer_Producer_send_with_callback`
        // states for both synchronous sends.
        Some(unsafe { std::slice::from_raw_parts(value, value_len as usize) })
    } else {
        None
    };

    let partition_opt = if partition >= 0 { Some(partition) } else { None };
    let timestamp_opt = if timestamp >= 0 { Some(timestamp) } else { None };

    let record = match ProducerRecordOptionsBuilder::new()
        .set_topic(topic_str)
        .set_value(value_slice)
        .set_partition(partition_opt)
        .set_timestamp(timestamp_opt)
        .set_key(key_slice)
        .build()
        .and_then(|options| {
            ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
        }) {
        Ok(r) => r,
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, i.e. one created by `build_producer_handle`, which is what
    // `producer_handle` requires; the `&'static ProducerHandle` is used only for the
    // duration of this call (cloning `completion_tx`, locking `kind`), during which the C
    // caller keeps the handle alive.
    let handle = unsafe { producer_handle(producer) };
    let completion_tx = handle.completion_tx.clone();
    let guard = handle.kind.lock().unwrap();
    let runtime_handle = guard.runtime().handle().clone();
    match producer_send(&guard, record) {
        Ok(future) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written (null, meaning success).
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_future(future, runtime_handle, completion_tx)
        },
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

/// Sends a single record through the producer, returning a future **and**
/// invoking `callback` on completion.
///
/// This is the C equivalent of Java's `Producer.send(record, Callback)`: both
/// the returned future and the callback report the same outcome. Use
/// [`kafka_producer_Producer_send`] when only the future is needed, or
/// [`kafka_producer_Producer_send_async`] when only the callback is needed (that
/// one also avoids blocking the caller).
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `topic`: Non-null, null-terminated UTF-8 topic name.
/// - `partition`: Partition number, or `-1` for no partition hint.
/// - `timestamp`: Timestamp in milliseconds since epoch, or `-1` to let
///   the producer stamp the record.
/// - `key`: Pointer to key bytes, or null if `key_len` is `-1`.
/// - `key_len`: Key length in bytes, or `-1` for no key.
/// - `value`: Pointer to value bytes, or null if `value_len` is `-1`.
/// - `value_len`: Value length in bytes, or `-1` for no value.
/// - `callback`: Delivery callback, invoked exactly once on the producer's
///   dedicated dispatcher thread with a non-null
///   [`kafka_producer_RecordMetadata_t`] on success or a non-null
///   [`kafka_common_Error_t`] on failure — see
///   [`kafka_producer_Producer_send_callback_t`] for the one case that delivers
///   both. The callee owns whichever handles are non-null and must free them
///   with the matching `*_destroy`.
/// - `user_data`: Opaque pointer passed back to `callback`.
/// - `out_error`: Pointer where an error handle will be written on failure,
///   or null if the caller does not need error details.
///
/// # Returns
///
/// A non-null future handle on success, or null on failure. The caller owns the
/// future and must free it with [`kafka_common_KafkaFuture_RecordMetadata_destroy`].
/// If `out_error` is non-null, `*out_error` is set to null on success or to a
/// valid [`kafka_common_Error_t`] handle on failure.
///
/// `out_error` reports synchronous validation errors (null topic / bad
/// key/value length) and synchronous send failures (closed producer), in which
/// case `callback` is **not** invoked.
/// A caught panic also returns null, with a
/// `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` error in `*out_error`, and the
/// guard itself never invokes `callback`. The record is handed off when the
/// producer appends it, with `callback`, to a batch; a panic raised after that
/// leaves the record in its batch, so `callback` can still fire for it later.
/// On a return that reports a panic, do not release `user_data` yourself: only
/// `callback` may release it, if it fires. A caller that releases `user_data`
/// on a null return must therefore pass a non-null `out_error`: with a null
/// one, the panic is only logged and cannot be told from the failures above.
///
/// # Zero-copy / lifetime contract
///
/// The `key` and `value` buffers are **not** copied by this layer, but as with
/// [`kafka_producer_Producer_send`] they are consumed before the call returns
/// (written straight into the record accumulator's batch buffer), so they only
/// need to stay valid for the duration of the call — unlike
/// [`kafka_producer_Producer_send_async`], which borrows them until the callback
/// fires.
///
/// # Safety
///
/// - `producer` must be a valid handle.
/// - `topic` must be a valid C string.
/// - `key` must be valid for `key_len` bytes if `key_len >= 0`.
/// - `value` must be valid for `value_len` bytes if `value_len >= 0`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_with_callback(
    producer: *mut kafka_producer_Producer_t,
    topic: *const c_char,
    partition: i32,
    timestamp: i64,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
    callback: kafka_producer_Producer_send_callback_t,
    user_data: *mut std::ffi::c_void,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_RecordMetadata_t {
    if producer.is_null() || topic.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    // SAFETY: `topic` is non-null (checked above) and, per this function's `# Safety`, a
    // valid null-terminated C string; it is copied into an owned `String` immediately, so
    // nothing borrowed outlives the call.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().into_owned();

    let key_slice: Option<&[u8]> = if key_len >= 0 {
        if key.is_null() {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        // SAFETY: This branch is reached only when `key_len >= 0` (so `key_len as usize` is
        // the non-negative length) and `key` is non-null (checked above); per this
        // function's `# Safety`, `key` is then valid for `key_len` bytes. The slice is used
        // only within this call: per this function's zero-copy contract, the bytes are
        // written straight into the record accumulator's batch buffer (or copied for the
        // mock) before the call returns, so the buffer need not outlive it.
        Some(unsafe { std::slice::from_raw_parts(key, key_len as usize) })
    } else {
        None
    };

    let value_slice: Option<&[u8]> = if value_len >= 0 {
        if value.is_null() {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        // SAFETY: This branch is reached only when `value_len >= 0` (so `value_len as
        // usize` is the non-negative length) and `value` is non-null (checked above); per
        // this function's `# Safety`, `value` is then valid for `value_len` bytes. The
        // slice is used only within this call: per this function's zero-copy contract, the
        // bytes are written straight into the record accumulator's batch buffer (or copied
        // for the mock) before the call returns, so the buffer need not outlive it.
        Some(unsafe { std::slice::from_raw_parts(value, value_len as usize) })
    } else {
        None
    };

    let partition_opt = if partition >= 0 { Some(partition) } else { None };
    let timestamp_opt = if timestamp >= 0 { Some(timestamp) } else { None };

    let record = match ProducerRecordOptionsBuilder::new()
        .set_topic(topic_str)
        .set_value(value_slice)
        .set_partition(partition_opt)
        .set_timestamp(timestamp_opt)
        .set_key(key_slice)
        .build()
        .and_then(|options| {
            ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
        }) {
        Ok(r) => r,
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, i.e. one created by `build_producer_handle`, which is what
    // `producer_handle` requires; the `&'static ProducerHandle` is used only for the
    // duration of this call (cloning `completion_tx`, locking `kind`), during which the C
    // caller keeps the handle alive. The `callback`/`user_data` pair captured into `cb` is
    // the C caller's and is fired at most once through the dispatcher by
    // `make_record_callback`.
    let handle = unsafe { producer_handle(producer) };
    let completion_tx = handle.completion_tx.clone();
    // The native callback that converts the delivery result into owned C handles
    // and hands them to the dispatcher thread. Dropped unfired if the send fails
    // synchronously (no handles were allocated yet). This synchronous path has a
    // single fire site (the delivery path on `Ok`), so a fresh at-most-once guard
    // is all `make_record_callback` needs here — no second firing site to share it
    // with, unlike the async submission path.
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cb = make_record_callback(RecordCallbackTarget { callback, user_data }, completion_tx.clone(), fired);
    let guard = handle.kind.lock().unwrap();
    let runtime_handle = guard.runtime().handle().clone();
    match producer_send_with_callback(&guard, record, cb) {
        Ok(future) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written (null, meaning success).
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_future(future, runtime_handle, completion_tx)
        },
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

/// Inner implementation of [`kafka_producer_Producer_send_batch`].
///
/// Separated from the `extern "C"` wrapper, whose `#[ffi_guard]` turns a panic
/// here into a `-1` return, so tests can also observe the panic message itself.
///
/// # Panics
///
/// Panics if `producer`, `records`, `out_futures`, or `out_errors` is null,
/// or if `count` is negative: a negative count must fail rather than be clamped.
///
/// # Safety
///
/// Same requirements as [`kafka_producer_Producer_send_batch`].
unsafe fn send_batch_inner(
    producer: *mut kafka_producer_Producer_t,
    records: *const kafka_producer_ProducerRecord_t,
    count: i32,
    out_futures: *mut *mut kafka_common_KafkaFuture_RecordMetadata_t,
    out_errors: *mut *mut kafka_common_Error_t,
) -> i32 {
    assert!(!producer.is_null(), "producer must not be null");
    assert!(!records.is_null(), "records must not be null");
    assert!(!out_futures.is_null(), "out_futures must not be null");
    assert!(!out_errors.is_null(), "out_errors must not be null");
    // A negative count must fail rather than be clamped; the entry point's
    // `#[ffi_guard]` reports this panic as the call's failure.
    assert!(count >= 0, "count must not be negative");

    let count = count as usize;

    // SAFETY: `producer` is non-null (asserted above); `producer_handle` further requires a
    // handle created by `build_producer_handle`, which `kafka_producer_Producer_send_batch`
    // states only in its `# Parameters` ("Non-null producer handle") and not in the `#
    // Safety` section this function inherits (see flags), so this relies on the C caller
    // passing a producer handle. The `&'static ProducerHandle` is used only within this
    // synchronous call (cloning `completion_tx`, locking `kind`), during which the C caller
    // keeps the handle alive.
    let handle = unsafe { producer_handle(producer) };
    let completion_tx = handle.completion_tx.clone();
    let guard = handle.kind.lock().unwrap();
    let runtime_handle = guard.runtime().handle().clone();
    let mut success_count: i32 = 0;

    for i in 0..count {
        // SAFETY: `records` is non-null and `count >= 0` (both asserted above), and `i <
        // count`; per `kafka_producer_Producer_send_batch`'s `# Safety`, which this
        // function inherits, `records` points to at least `count` valid
        // `kafka_producer_ProducerRecord_t` structs, so `records.add(i)` is in bounds and
        // readable. `rec` is used only within this iteration.
        let rec = unsafe { &*records.add(i) };

        if rec.topic.is_null() {
            // SAFETY: `out_futures` and `out_errors` are non-null (asserted above) and, per
            // `kafka_producer_Producer_send_batch`'s `# Safety`, each point to at least
            // `count` writable pointer slots; `i < count`, and slot `i` of each array is
            // written exactly once on this path before `continue` (a null future and a
            // fresh `box_error` handle the caller owns).
            unsafe {
                *out_futures.add(i) = std::ptr::null_mut();
                *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
            }
            continue;
        }

        // SAFETY: `rec.topic` is non-null (checked above) and, per
        // `kafka_producer_Producer_send_batch`'s `# Safety`, each record's `topic` is a
        // valid C string; it is copied into an owned `String` immediately, so nothing
        // borrowed outlives the call.
        let topic_str = unsafe { CStr::from_ptr(rec.topic) }.to_string_lossy().into_owned();

        let key: Option<&[u8]> = if rec.key_len >= 0 {
            if rec.key.is_null() {
                // SAFETY: `out_futures` and `out_errors` are non-null (asserted above) and,
                // per `kafka_producer_Producer_send_batch`'s `# Safety`, each point to at
                // least `count` writable pointer slots; `i < count`, and slot `i` of each
                // array is written exactly once on this path before `continue` (a null
                // future and a fresh `box_error` handle the caller owns).
                unsafe {
                    *out_futures.add(i) = std::ptr::null_mut();
                    *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
                }
                continue;
            }
            // SAFETY: This branch is reached only when `rec.key_len >= 0` (so the `usize`
            // cast is the non-negative length) and `rec.key` is non-null (checked above);
            // the `kafka_producer_ProducerRecord_t` field conventions, which the `#
            // Safety`'s "valid structs" incorporate, require `key` to point to a valid
            // buffer of `key_len` bytes whenever `key_len >= 0`. The slice is used only
            // within this call, consumed by `producer_send` before it returns.
            Some(unsafe { std::slice::from_raw_parts(rec.key, rec.key_len as usize) })
        } else {
            None
        };

        let value: Option<&[u8]> = if rec.value_len >= 0 {
            if rec.value.is_null() {
                // SAFETY: `out_futures` and `out_errors` are non-null (asserted above) and,
                // per `kafka_producer_Producer_send_batch`'s `# Safety`, each point to at
                // least `count` writable pointer slots; `i < count`, and slot `i` of each
                // array is written exactly once on this path before `continue` (a null
                // future and a fresh `box_error` handle the caller owns).
                unsafe {
                    *out_futures.add(i) = std::ptr::null_mut();
                    *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
                }
                continue;
            }
            // SAFETY: This branch is reached only when `rec.value_len >= 0` (so the `usize`
            // cast is the non-negative length) and `rec.value` is non-null (checked above);
            // the `kafka_producer_ProducerRecord_t` field conventions, which the `#
            // Safety`'s "valid structs" incorporate, require `value` to point to a valid
            // buffer of `value_len` bytes whenever `value_len >= 0`. The slice is used only
            // within this call, consumed by `producer_send` before it returns.
            Some(unsafe { std::slice::from_raw_parts(rec.value, rec.value_len as usize) })
        } else {
            None
        };

        let partition = if rec.partition >= 0 { Some(rec.partition) } else { None };
        let timestamp = if rec.timestamp >= 0 { Some(rec.timestamp) } else { None };

        let record = match ProducerRecordOptionsBuilder::new()
            .set_topic(topic_str)
            .set_value(value)
            .set_partition(partition)
            .set_timestamp(timestamp)
            .set_key(key)
            .build()
            .and_then(|options| {
                ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
            }) {
            Ok(r) => r,
            Err(e) => {
                // SAFETY: `out_futures` and `out_errors` are non-null (asserted above) and,
                // per `kafka_producer_Producer_send_batch`'s `# Safety`, each point to at
                // least `count` writable pointer slots; `i < count`, and slot `i` of each
                // array is written exactly once on this path before `continue` (a null
                // future and a fresh `box_error` handle carrying the record-construction
                // error, which the caller owns).
                unsafe {
                    *out_futures.add(i) = std::ptr::null_mut();
                    *out_errors.add(i) = box_error(e);
                }
                continue;
            },
        };

        match producer_send(&guard, record) {
            // SAFETY: `out_futures` and `out_errors` are non-null (asserted above) and, per
            // `kafka_producer_Producer_send_batch`'s `# Safety`, each point to at least
            // `count` writable pointer slots; `i < count`, and this success arm writes slot
            // `i` of each exactly once: a fresh `box_future` handle the caller must
            // destroy, and a null error.
            Ok(future) => unsafe {
                *out_futures.add(i) = box_future(future, runtime_handle.clone(), completion_tx.clone());
                *out_errors.add(i) = std::ptr::null_mut();
                success_count += 1;
            },
            // SAFETY: `out_futures` and `out_errors` are non-null (asserted above) and, per
            // `kafka_producer_Producer_send_batch`'s `# Safety`, each point to at least
            // `count` writable pointer slots; `i < count`, and this failure arm writes slot
            // `i` of each exactly once: a null future and a fresh `box_error` handle the
            // caller owns.
            Err(e) => unsafe {
                *out_futures.add(i) = std::ptr::null_mut();
                *out_errors.add(i) = box_error(e);
            },
        }
    }

    success_count
}

/// Sends a batch of records through the producer.
///
/// Iterates over `records[0..count]`, attempting to send every record.
/// For each record, `out_futures[i]` receives the future handle on success
/// (non-null) or null on failure, and `out_errors[i]` receives null on
/// success or a non-null error handle on failure. The caller must free
/// every non-null future with [`kafka_common_KafkaFuture_RecordMetadata_destroy`]
/// and every non-null error with [`kafka_common_Error_destroy`].
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `records`: Non-null pointer to an array of [`kafka_producer_ProducerRecord_t`].
/// - `count`: Number of records in the array (must be `>= 0`).
/// - `out_futures`: Non-null pointer to an array of `*mut kafka_common_KafkaFuture_RecordMetadata_t`
///   with at least `count` entries. Caller must allocate this array.
/// - `out_errors`: Non-null pointer to an array of `*mut kafka_common_Error_t`
///   with at least `count` entries. Caller must allocate this array.
///
/// # Returns
///
/// The number of records successfully sent (`0..=count`).
///
/// # Panics
///
/// Panics if `producer`, `records`, `out_futures`, or `out_errors` is null,
/// or if `count` is negative. These are programming errors (violated
/// preconditions).
///
/// # Safety
///
/// - `producer` must be a valid handle from a producer constructor.
/// - `records` must point to at least `count` valid [`kafka_producer_ProducerRecord_t`] structs.
/// - `out_futures` must point to at least `count` writable pointer slots.
/// - `out_errors` must point to at least `count` writable pointer slots.
/// - Each `kafka_producer_ProducerRecord_t.topic` must be a valid C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_batch(
    producer: *mut kafka_producer_Producer_t,
    records: *const kafka_producer_ProducerRecord_t,
    count: i32,
    out_futures: *mut *mut kafka_common_KafkaFuture_RecordMetadata_t,
    out_errors: *mut *mut kafka_common_Error_t,
) -> i32 {
    // SAFETY: `send_batch_inner` has the same `# Safety` requirements as this function,
    // which the C caller upholds; the arguments are forwarded unchanged.
    unsafe { send_batch_inner(producer, records, count, out_futures, out_errors) }
}

/// Asynchronously sends a single record, invoking `callback` on completion.
///
/// Same input parameters as [`kafka_producer_Producer_send`], plus a completion
/// `callback` + `user_data`. Unlike the sync send, this returns **without**
/// blocking the caller (the enqueue / metadata wait runs on the producer's
/// submission task) and delivers the result through `callback` instead of a
/// future.
///
/// `callback` is invoked on the producer's dedicated dispatcher thread. On
/// success, `metadata` is non-null and `error` is null. On failure, `error` is
/// always non-null and `metadata` **may also be non-null**, carrying `-1` in
/// every unknown field — which of the two happens depends on where the failure
/// arose, and mirrors Java, whose three callback sites differ:
///
/// - a broker-side or delivery failure passes null metadata
///   (`ProducerBatch.java:315`);
/// - a synchronous `ApiException` inside `send` passes a `RecordMetadata(tp, -1,
///   -1, NO_TIMESTAMP, -1, -1)` (`KafkaProducer.java:1060-1061`), as does the
///   `MockProducer` completion path (`MockProducer.java:578`).
///
/// So do **not** treat the two as mutually exclusive: the caller owns *every*
/// non-null handle and must free each with the matching `*_destroy`, testing them
/// independently rather than in an `if`/`else`.
///
/// `out_error` reports only synchronous validation errors (null topic / bad
/// key/value length), in which case `callback` is **not** invoked.
/// A caught panic is reported the same way: `*out_error` receives a
/// `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` error (with a null `out_error`
/// it is only logged), and it is never reported through `callback`. The only
/// panic reachable in this function happens after the record was queued, so that
/// record is still sent and `callback` still fires for it, exactly once.
///
/// # Zero-copy / lifetime contract
///
/// The `key` and `value` buffers are **not** copied; they are borrowed by the
/// submission task. The caller **must keep them valid until `callback` fires**.
///
/// # Safety
///
/// - `producer` must be a valid handle.
/// - `topic` must be a valid C string.
/// - `key`/`value` must be valid for `key_len`/`value_len` bytes when `>= 0`,
///   and remain valid until `callback` is invoked.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_async(
    producer: *mut kafka_producer_Producer_t,
    topic: *const c_char,
    partition: i32,
    timestamp: i64,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
    callback: kafka_producer_Producer_send_callback_t,
    user_data: *mut std::ffi::c_void,
    out_error: *mut *mut kafka_common_Error_t,
) {
    if producer.is_null() || topic.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per the `out_error`
            // contract this function inherits from `kafka_producer_Producer_send`'s `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return;
    }

    // SAFETY: `topic` is non-null (checked above) and, per this function's `# Safety`, a
    // valid C string; it is copied into an owned `String` immediately, so nothing borrowed
    // outlives the call (the record carries the owned copy to the submission task).
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().into_owned();

    // Borrow key/value into caller memory, lifetime-extended to 'static under
    // the documented contract (caller keeps buffers valid until the callback).
    let key_slice: Option<&'static [u8]> = if key_len >= 0 {
        if key.is_null() {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per the `out_error`
                // contract this function inherits from `kafka_producer_Producer_send`'s `#
                // Parameters`, a pointer where an error handle will be written on failure
                // or null if the caller does not need error details; exactly one element is
                // written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return;
        }
        // SAFETY: This branch is reached only when `key_len >= 0` (so `key_len as usize` is
        // the non-negative length) and `key` is non-null (checked above); per this
        // function's `# Safety`, `key` is then valid for `key_len` bytes and remains valid
        // until `callback` is invoked. The `'static` lifetime is a cast of exactly that
        // contract: the slice is moved into the `SendRequest` and read once, when the
        // submission task hands the record to `producer.send`, which copies the bytes into
        // the batch before the delivery callback can fire; on the early-return paths below
        // the slice is dropped unread.
        Some(unsafe { std::slice::from_raw_parts(key, key_len as usize) })
    } else {
        None
    };

    let value_slice: Option<&'static [u8]> = if value_len >= 0 {
        if value.is_null() {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per the `out_error`
                // contract this function inherits from `kafka_producer_Producer_send`'s `#
                // Parameters`, a pointer where an error handle will be written on failure
                // or null if the caller does not need error details; exactly one element is
                // written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return;
        }
        // SAFETY: This branch is reached only when `value_len >= 0` (so `value_len as
        // usize` is the non-negative length) and `value` is non-null (checked above); per
        // this function's `# Safety`, `value` is then valid for `value_len` bytes and
        // remains valid until `callback` is invoked. The `'static` lifetime is a cast of
        // exactly that contract: the slice is moved into the `SendRequest` and read once,
        // when the submission task hands the record to `producer.send`, which copies the
        // bytes into the batch before the delivery callback can fire; on the early-return
        // paths below the slice is dropped unread.
        Some(unsafe { std::slice::from_raw_parts(value, value_len as usize) })
    } else {
        None
    };

    let partition_opt = if partition >= 0 { Some(partition) } else { None };
    let timestamp_opt = if timestamp >= 0 { Some(timestamp) } else { None };

    // Build (and validate) the record once, here, so construction errors are
    // reported synchronously via `out_error` rather than deferred to the task.
    let record = match ProducerRecordOptionsBuilder::new()
        .set_topic(topic_str)
        .set_value(value_slice)
        .set_partition(partition_opt)
        .set_timestamp(timestamp_opt)
        .set_key(key_slice)
        .build()
        .and_then(|options| {
            ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
        }) {
        Ok(r) => r,
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per the `out_error`
                // contract this function inherits from `kafka_producer_Producer_send`'s `#
                // Parameters`, a pointer where an error handle will be written on failure
                // or null if the caller does not need error details; exactly one element is
                // written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            return;
        },
    };

    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, i.e. one created by `build_producer_handle`, which is what
    // `producer_handle` requires; the `&'static ProducerHandle` is used only within this
    // call (`queued_sends`, `submit_tx`), during which the C caller keeps the handle alive
    // — the request sent on `submit_tx` carries no reference into the handle.
    let handle = unsafe { producer_handle(producer) };
    // Carry the target, not a pre-built callback: the submission task builds the
    // callback and can re-fire on `send`'s error paths (see `SendRequest`).
    let request = SubmitRequest::Send(SendRequest { record, target: RecordCallbackTarget { callback, user_data } });

    // Count the send before it is visible on the channel, so a concurrent
    // flush/close drain can never observe a depth lower than reality.
    handle.queued_sends.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    if handle.submit_tx.send(request).is_err() {
        handle.queued_sends.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        // Submission task gone (producer torn down): report synchronously. The
        // unfired callback is dropped (no handles were allocated yet).
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per the `out_error`
            // contract this function inherits from `kafka_producer_Producer_send`'s `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(Error::local_illegal_state("producer is closed")) };
        }
        return;
    }

    if !out_error.is_null() {
        // SAFETY: `out_error` is non-null (checked above) and, per the `out_error` contract
        // this function inherits from `kafka_producer_Producer_send`'s `# Parameters`, a
        // pointer where an error handle will be written on failure or null if the caller
        // does not need error details; exactly one element is written (null, meaning the
        // record was queued).
        unsafe { *out_error = std::ptr::null_mut() };
    }
}

/// Asynchronously sends a batch of records, invoking `callback` once per record
/// on completion (librdkafka-style per-record delivery; correlate via the
/// `RecordMetadata` topic/partition/offset and `user_data`).
///
/// `out_errors[i]` receives a non-null handle for records that fail synchronous
/// validation (those do not produce a callback); null otherwise. Returns the
/// number of records accepted for delivery.
/// A caught panic returns `-1`; it is not stored in `out_errors`, and the guard
/// itself never invokes `callback`. Each record is handed off when it is queued
/// for the producer's submission task. A panic raised after that, even inside
/// the queueing step, leaves every record queued by then in flight, so
/// `callback` can still fire for each of them. That includes the record whose
/// queueing panicked, although its `out_errors` slot is left unwritten. `-1`
/// does not say which records were queued, so treat every record as possibly
/// queued: keep its `key`/`value` valid until its callback fires, and do not
/// release `user_data`, which all the records share, yourself.
///
/// The same zero-copy / lifetime contract as
/// [`kafka_producer_Producer_send_async`] applies to every record's
/// `key`/`value`, and so does its callback-handle rule — the callback is built by
/// the same bridge, so `metadata` and `error` are **not** mutually exclusive and
/// every non-null handle must be freed.
///
/// # Panics
///
/// Panics if `producer`, `records`, or `out_errors` is null, or `count < 0`.
///
/// # Safety
///
/// - `producer` must be a valid handle from a producer constructor.
/// - `records` must point to at least `count` valid records whose `key`/`value`
///   remain valid until their callbacks fire.
/// - `out_errors` must point to at least `count` writable pointer slots.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_batch_async(
    producer: *mut kafka_producer_Producer_t,
    records: *const kafka_producer_ProducerRecord_t,
    count: i32,
    callback: kafka_producer_Producer_send_batch_callback_t,
    user_data: *mut std::ffi::c_void,
    out_errors: *mut *mut kafka_common_Error_t,
) -> i32 {
    assert!(!producer.is_null(), "producer must not be null");
    assert!(!records.is_null(), "records must not be null");
    assert!(!out_errors.is_null(), "out_errors must not be null");
    // A negative count must fail rather than be clamped; the entry point's
    // `#[ffi_guard]` reports this panic as the call's failure.
    assert!(count >= 0, "count must not be negative");

    // SAFETY: `producer` is non-null (asserted above); `producer_handle` further requires a
    // handle created by `build_producer_handle`, which this function's `# Safety` does not
    // state — it lists only `records` and `out_errors`, and there is no `# Parameters`
    // section (see flags) — so this relies on the C caller passing a producer handle as
    // every other producer entry point requires. The `&'static ProducerHandle` is used only
    // within this synchronous call (`queued_sends`, `submit_tx`), during which the C caller
    // keeps the handle alive.
    let handle = unsafe { producer_handle(producer) };
    let mut accepted: i32 = 0;

    for i in 0..count as usize {
        // SAFETY: `records` is non-null and `count >= 0` (both asserted above), and `i <
        // count`; per this function's `# Safety`, `records` points to at least `count`
        // valid records, so `records.add(i)` is in bounds and readable. `rec` is used only
        // within this iteration.
        let rec = unsafe { &*records.add(i) };

        if rec.topic.is_null() {
            // SAFETY: `out_errors` is non-null (asserted above) and, per this function's `#
            // Safety`, points to at least `count` writable pointer slots; `i < count`, and
            // slot `i` is written exactly once on this path before `continue`, with a fresh
            // `box_error` handle the caller owns.
            unsafe { *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest)) };
            continue;
        }
        // SAFETY: `rec.topic` is non-null (checked above) and is a null-terminated topic
        // name per the `kafka_producer_ProducerRecord_t` field documentation that this
        // function's `# Safety` ("valid records") incorporates; it is copied into an owned
        // `String` immediately, so nothing borrowed outlives the call.
        let topic = unsafe { CStr::from_ptr(rec.topic) }.to_string_lossy().into_owned();

        let key: Option<&'static [u8]> = if rec.key_len >= 0 {
            if rec.key.is_null() {
                // SAFETY: `out_errors` is non-null (asserted above) and, per this
                // function's `# Safety`, points to at least `count` writable pointer slots;
                // `i < count`, and slot `i` is written exactly once on this path before
                // `continue`, with a fresh `box_error` handle the caller owns.
                unsafe { *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest)) };
                continue;
            }
            // SAFETY: This branch is reached only when `rec.key_len >= 0` (so the `usize`
            // cast is the non-negative length) and `rec.key` is non-null (checked above);
            // per this function's `# Safety`, each record's `key`/`value` remain valid
            // until its callback fires, which is exactly what bounds the `'static` cast:
            // the slice is moved into the `SendRequest` and read once by the submission
            // task when it hands the record to `producer.send`, before the delivery
            // callback can fire; on the early-return paths it is dropped unread.
            Some(unsafe { std::slice::from_raw_parts(rec.key, rec.key_len as usize) })
        } else {
            None
        };

        let value: Option<&'static [u8]> = if rec.value_len >= 0 {
            if rec.value.is_null() {
                // SAFETY: `out_errors` is non-null (asserted above) and, per this
                // function's `# Safety`, points to at least `count` writable pointer slots;
                // `i < count`, and slot `i` is written exactly once on this path before
                // `continue`, with a fresh `box_error` handle the caller owns.
                unsafe { *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest)) };
                continue;
            }
            // SAFETY: This branch is reached only when `rec.value_len >= 0` (so the `usize`
            // cast is the non-negative length) and `rec.value` is non-null (checked above);
            // per this function's `# Safety`, each record's `key`/`value` remain valid
            // until its callback fires, which is exactly what bounds the `'static` cast:
            // the slice is moved into the `SendRequest` and read once by the submission
            // task when it hands the record to `producer.send`, before the delivery
            // callback can fire; on the early-return paths it is dropped unread.
            Some(unsafe { std::slice::from_raw_parts(rec.value, rec.value_len as usize) })
        } else {
            None
        };

        let partition = if rec.partition >= 0 { Some(rec.partition) } else { None };
        let timestamp = if rec.timestamp >= 0 { Some(rec.timestamp) } else { None };

        let record = match ProducerRecordOptionsBuilder::new()
            .set_topic(topic)
            .set_value(value)
            .set_partition(partition)
            .set_timestamp(timestamp)
            .set_key(key)
            .build()
            .and_then(|options| {
                ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message()))
            }) {
            Ok(r) => r,
            Err(e) => {
                // SAFETY: `out_errors` is non-null (asserted above) and, per this
                // function's `# Safety`, points to at least `count` writable pointer slots;
                // `i < count`, and slot `i` is written exactly once on this path before
                // `continue`, with a fresh `box_error` handle (the record-construction
                // error) the caller owns.
                unsafe { *out_errors.add(i) = box_error(e) };
                continue;
            },
        };
        // Carry the target, not a pre-built callback: the submission task builds
        // the callback and can re-fire on `send`'s error paths (see `SendRequest`).
        let request = SubmitRequest::Send(SendRequest { record, target: RecordCallbackTarget { callback, user_data } });

        handle.queued_sends.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if handle.submit_tx.send(request).is_err() {
            handle.queued_sends.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            // SAFETY: `out_errors` is non-null (asserted above) and, per this function's `#
            // Safety`, points to at least `count` writable pointer slots; `i < count`, and
            // slot `i` is written exactly once on this path before `continue`, with a fresh
            // `box_error` handle the caller owns (the request was dropped unread, so no
            // callback will fire for it).
            unsafe { *out_errors.add(i) = box_error(Error::local_illegal_state("producer is closed")) };
            continue;
        }
        // SAFETY: `out_errors` is non-null (asserted above) and, per this function's `#
        // Safety`, points to at least `count` writable pointer slots; `i < count`, and this
        // is the single write to slot `i` on the accepted path (null, meaning no
        // synchronous error).
        unsafe { *out_errors.add(i) = std::ptr::null_mut() };
        accepted += 1;
    }

    accepted
}

// ---------------------------------------------------------------------------
// Future
// ---------------------------------------------------------------------------

/// Checks if a future has resolved.
///
/// This eagerly polls the underlying channel, so it can return `true` even
/// without having been awaited. This matches Java's `Future.isDone()`.
///
/// # Parameters
///
/// - `future`: Non-null future handle.
///
/// # Returns
///
/// `true` if the future has resolved (success or error), `false` if still
/// pending. Returns `false` if `future` is null.
///
/// # Safety
///
/// `future` must be a valid handle from a send function, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_RecordMetadata_is_done(
    future: *mut kafka_common_KafkaFuture_RecordMetadata_t,
) -> bool {
    if future.is_null() {
        return false;
    }
    // SAFETY: `future` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a send function, i.e. one created by `box_future`, which is what
    // `future_ref` requires; the `&'static FfiFuture` is used only for the `is_done` poll
    // within this call, during which the C caller keeps the handle alive.
    let f = unsafe { future_ref(future) };
    f.future.is_done()
}

/// Blocks until the future resolves and returns the record metadata.
///
/// # Parameters
///
/// - `future`: Non-null future handle.
/// - `out_error`: Pointer where an error handle will be written on failure,
///   or null if the caller does not need error details.
///
/// # Returns
///
/// A non-null [`kafka_producer_RecordMetadata_t`] handle on success (caller
/// must free with [`kafka_producer_RecordMetadata_destroy`]), or null on
/// failure. If `out_error` is non-null, `*out_error` is set to null on
/// success or to a valid [`kafka_common_Error_t`] handle on failure.
///
/// # Safety
///
/// - `future` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_RecordMetadata_get(
    future: *mut kafka_common_KafkaFuture_RecordMetadata_t,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_producer_RecordMetadata_t {
    if future.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    // SAFETY: `future` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, i.e. one created by `box_future` in a send function, which is what
    // `future_ref` requires; the `&'static FfiFuture` is used only for the `block_on`
    // within this call, during which the C caller keeps the handle alive (the future is not
    // consumed).
    let f = unsafe { future_ref(future) };

    match f.runtime_handle.block_on(f.future.get()) {
        Ok(metadata) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written (null, meaning success).
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_metadata(metadata)
        },
        Err(e) => {
            if !out_error.is_null() {
                // SAFETY: `out_error` is non-null (checked above) and, per this function's
                // `# Parameters`, a pointer where an error handle will be written on
                // failure or null if the caller does not need error details; exactly one
                // element is written, a fresh `box_error` handle the caller owns.
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

/// Blocks until all futures in the array resolve, writing results into the
/// parallel output arrays.
///
/// For each index `i` in `0..count`:
/// - On success: `out_metadata[i]` is set to a valid
///   [`kafka_producer_RecordMetadata_t`] handle and `out_errors[i]` is set to
///   null.
/// - On failure: `out_errors[i]` is set to a valid [`kafka_common_Error_t`]
///   handle and `out_metadata[i]` is set to null.
/// - If `futures[i]` is null it is treated as an error
///   ([`Errors::InvalidRequest`]).
///
/// The caller must free every non-null metadata handle with
/// [`kafka_producer_RecordMetadata_destroy`] and every non-null error handle
/// with [`kafka_common_Error_destroy`].  The future handles in `futures` are
/// **not** consumed — the caller still owns them and must destroy them
/// separately.
///
/// # Parameters
///
/// - `futures`: Non-null pointer to an array of `count` future handles.
/// - `count`: Number of elements (must be ≥ 0).
/// - `out_metadata`: Non-null pointer to a caller-allocated array of `count`
///   `*mut kafka_producer_RecordMetadata_t`.
/// - `out_errors`: Non-null pointer to a caller-allocated array of `count`
///   `*mut kafka_common_Error_t`.
///
/// # Safety
///
/// - `futures`, `out_metadata`, and `out_errors` must be non-null and point to
///   arrays of at least `count` elements.
/// - Each non-null entry in `futures` must be a valid handle from a send
///   function.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_RecordMetadata_get_all(
    futures: *mut *mut kafka_common_KafkaFuture_RecordMetadata_t,
    count: i32,
    out_metadata: *mut *mut kafka_producer_RecordMetadata_t,
    out_errors: *mut *mut kafka_common_Error_t,
) {
    assert!(!futures.is_null(), "futures must not be null");
    assert!(!out_metadata.is_null(), "out_metadata must not be null");
    assert!(!out_errors.is_null(), "out_errors must not be null");
    // A negative count must fail rather than be clamped; the entry point's
    // `#[ffi_guard]` reports this panic as the call's failure.
    assert!(count >= 0, "count must not be negative");

    let count = count as usize;

    for i in 0..count {
        // SAFETY: `futures` is non-null and `count >= 0` (both asserted above), and `i <
        // count`; per this function's `# Safety`, `futures` points to an array of at least
        // `count` elements, so `futures.add(i)` is readable (a null entry is tolerated and
        // handled below).
        let future_ptr = unsafe { *futures.add(i) };
        if future_ptr.is_null() {
            // SAFETY: `out_metadata` and `out_errors` are non-null (asserted above) and,
            // per this function's `# Safety`, point to arrays of at least `count` elements;
            // `i < count`, and slot `i` of each is written exactly once on this path before
            // `continue` (null metadata and a fresh `box_error` handle the caller owns).
            unsafe {
                *out_metadata.add(i) = std::ptr::null_mut();
                *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
            }
            continue;
        }

        // SAFETY: `future_ptr` is non-null (checked above) and, per this function's `#
        // Safety`, every non-null entry of `futures` is a valid handle from a send
        // function, which is what `future_ref` requires; the reference is used only for the
        // `block_on` within this call, during which the C caller keeps the handles alive
        // (they are not consumed).
        let f = unsafe { future_ref(future_ptr) };
        match f.runtime_handle.block_on(f.future.get()) {
            // SAFETY: `out_metadata` and `out_errors` are non-null (asserted above) and,
            // per this function's `# Safety`, point to arrays of at least `count` elements;
            // `i < count`, and this success arm writes slot `i` of each exactly once: a
            // fresh `box_metadata` handle the caller owns, and a null error.
            Ok(metadata) => unsafe {
                *out_metadata.add(i) = box_metadata(metadata);
                *out_errors.add(i) = std::ptr::null_mut();
            },
            // SAFETY: `out_metadata` and `out_errors` are non-null (asserted above) and,
            // per this function's `# Safety`, point to arrays of at least `count` elements;
            // `i < count`, and this failure arm writes slot `i` of each exactly once: null
            // metadata and a fresh `box_error` handle the caller owns.
            Err(e) => unsafe {
                *out_metadata.add(i) = std::ptr::null_mut();
                *out_errors.add(i) = box_error(e);
            },
        }
    }
}

/// Asynchronously awaits a future, invoking `callback` on completion instead
/// of blocking (the async counterpart of
/// [`kafka_common_KafkaFuture_RecordMetadata_get`]).
///
/// `callback` fires on the producer's dispatcher thread with a non-null
/// metadata handle on success or a non-null error handle on failure; the caller
/// owns whichever is non-null. The future handle is **not** consumed — the
/// caller still owns it and must destroy it (after the callback has fired).
///
/// # Safety
///
/// - `future` must be a valid handle from a send function, or null (null is
///   reported as an error through `callback`).
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
pub unsafe extern "C" fn kafka_common_KafkaFuture_RecordMetadata_get_async(
    future: *mut kafka_common_KafkaFuture_RecordMetadata_t,
    callback: kafka_common_KafkaFuture_RecordMetadata_get_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    if future.is_null() {
        // Programming error: deliver an error through the callback inline.
        let error = box_error(Error::new(Errors::InvalidRequest));
        // SAFETY: `callback` was supplied by the C caller along with `user_data`, and this
        // function's `# Safety` documents a null `future` as reported through `callback`;
        // `error` is a fresh `box_error` handle the callee owns and the metadata argument
        // is null. The call runs inline on the calling thread and the function returns
        // immediately afterwards without spawning anything, so this is the single
        // invocation for this call.
        unsafe { callback(std::ptr::null_mut(), error, user_data) };
        return;
    }

    // SAFETY: `future` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a send function, i.e. one created by `box_future`, which is what
    // `future_ref` requires. The `&'static FfiFuture` is used only synchronously here — to
    // clone `future` and `completion_tx` and to spawn on `runtime_handle` — and the spawned
    // task captures only those owned clones plus the `Copy` target, so nothing borrowed
    // from the handle escapes this call, during which the C caller keeps the handle alive.
    let f = unsafe { future_ref(future) };
    let fut = f.future.clone();
    let tx = f.completion_tx.clone();
    let target = RecordCallbackTarget { callback, user_data };

    spawn_callback_task(&f.runtime_handle, async move {
        let target = target;
        // No `.await` follows the handle construction below, so the raw
        // pointers never cross a suspension point.
        let (metadata, error) = match fut.get().await {
            Ok(m) => (box_metadata(m), std::ptr::null_mut()),
            Err(e) => (std::ptr::null_mut(), box_error(e)),
        };
        let completion = RecordCompletion { callback: target.callback, user_data: target.user_data, metadata, error };
        // SAFETY: `RecordCompletion::fire` requires exactly one call on the dispatcher
        // thread: `completion` is moved into this `FnOnce` job, which
        // `enqueue_or_run_inline` runs exactly once (on the dispatcher thread, or inline
        // once it has exited). `metadata`/`error` are fresh `box_metadata`/`box_error`
        // handles built after the only `.await`, the `callback`/`user_data` pair is the one
        // the C caller supplied (`user_data` valid until the callback fires per the
        // caller's contract), and this spawned task is the body's single fire site for this
        // call.
        let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
        enqueue_or_run_inline(&tx, job);
    });
}

/// The `#[ffi_guard]` on-panic path of
/// [`kafka_common_KafkaFuture_RecordMetadata_get_all_async`]: fires `callback` once, in
/// the shape it promises — `count` null metadata entries and `count` error handles,
/// each a copy of `error` — so the panic reaches the caller exactly like any other
/// per-future failure (D4 of `design/current/appsec-7665-4521-ffi-panic-guard.md`).
///
/// A negative `count` (the precondition whose panic may have brought us here)
/// yields empty arrays: the callback still fires, and the panic itself is reported
/// only by the guard's log line.
///
/// # Safety
///
/// Same requirements as the callback contract of
/// [`kafka_common_KafkaFuture_RecordMetadata_get_all_async`].
unsafe fn fire_get_all_callback_with_error(
    callback: kafka_common_KafkaFuture_RecordMetadata_get_all_callback_t,
    count: i32,
    error: Error,
    user_data: *mut std::ffi::c_void,
) {
    let len = count.max(0) as usize;
    let mut metadata: Vec<*mut kafka_producer_RecordMetadata_t> = vec![std::ptr::null_mut(); len];
    let mut errors: Vec<*mut kafka_common_Error_t> = (0..len).map(|_| box_error(error.clone())).collect();
    // SAFETY: `callback`/`user_data` are the pair the C caller supplied to
    // `kafka_common_KafkaFuture_RecordMetadata_get_all_async`, whose callback contract this
    // function inherits per its `# Safety`; `metadata` and `errors` are local `Vec`s of
    // exactly `len = count.max(0)` entries each (nulls, and fresh `box_error` copies the
    // callee owns), alive for the duration of the call, after which only their storage is
    // freed. It fires inline, once, on the calling thread — the guard calls it only after
    // the body panicked before reaching its own fire site.
    unsafe { callback(metadata.as_mut_ptr(), errors.as_mut_ptr(), len as i32, user_data) };
}

/// Asynchronously awaits all futures, invoking `callback` once with parallel
/// result arrays (the async counterpart of
/// [`kafka_common_KafkaFuture_RecordMetadata_get_all`]).
///
/// `callback` fires on the dispatcher thread with `metadata[0..count]` /
/// `errors[0..count]`: per index, exactly one is non-null (a null `futures[i]`
/// yields an `InvalidRequest` error). The arrays are valid only for the
/// duration of the call; the individual handles within are owned by the caller
/// and must be freed with the matching `*_destroy`. The future handles are
/// **not** consumed.
///
/// # Panics
///
/// Panics if `futures` is null or `count < 0`.
///
/// # Safety
///
/// - `futures` must point to at least `count` future handles (null entries
///   allowed).
#[ffi_guard(on_panic = |err| {
    // SAFETY: On a caught panic the guard fires `fire_get_all_callback_with_error`, which
    // invokes the C caller's own `callback`/`user_data` pair on the calling thread in the
    // aggregate shape the function's callback contract documents (`count` null metadata
    // entries and `count` fresh error handles the callback owns), under that helper's own
    // `# Safety` (the same requirements as this function's callback contract). The panic
    // aborted the body before its own callback path ran, so this is the single invocation:
    // a panic in the spawn that hands the callback to a task aborts the process instead
    // (`spawn_callback_task`).
    unsafe { fire_get_all_callback_with_error(callback, count, err, user_data) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_RecordMetadata_get_all_async(
    futures: *mut *mut kafka_common_KafkaFuture_RecordMetadata_t,
    count: i32,
    callback: kafka_common_KafkaFuture_RecordMetadata_get_all_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    assert!(!futures.is_null(), "futures must not be null");
    // A negative count must fail rather than be clamped; the entry point's
    // `#[ffi_guard]` reports this panic as the call's failure.
    assert!(count >= 0, "count must not be negative");
    let count = count as usize;

    // Clone the futures and grab a runtime handle + completion sender from the
    // first non-null future.
    let mut futs: Vec<Option<KafkaFuture<RecordMetadata>>> = Vec::with_capacity(count);
    let mut runtime: Option<tokio::runtime::Handle> = None;
    let mut completion: Option<std::sync::mpsc::Sender<CompletionJob>> = None;
    for i in 0..count {
        // SAFETY: `futures` is non-null and `count >= 0` (both asserted above), and `i <
        // count`; per this function's `# Safety`, `futures` points to at least `count`
        // future handles with null entries allowed, so `futures.add(i)` is readable and a
        // null `fp` is handled below.
        let fp = unsafe { *futures.add(i) };
        if fp.is_null() {
            futs.push(None);
        } else {
            // SAFETY: `fp` is non-null (checked above) and, per this function's `# Safety`,
            // an entry of `futures` is a future handle, i.e. one created by `box_future` as
            // `future_ref` requires; the reference is used only synchronously here to clone
            // `future`, `runtime_handle` and `completion_tx` — the spawned task captures
            // only those owned clones — so nothing borrowed from the handle escapes this
            // call, during which the C caller keeps the handles alive (they are not
            // consumed).
            let f = unsafe { future_ref(fp) };
            if runtime.is_none() {
                runtime = Some(f.runtime_handle.clone());
                completion = Some(f.completion_tx.clone());
            }
            futs.push(Some(f.future.clone()));
        }
    }

    let (runtime, completion) = match (runtime, completion) {
        (Some(rt), Some(tx)) => (rt, tx),
        _ => {
            // No non-null future (no runtime to drive): deliver inline, each an
            // InvalidRequest error.
            let mut metadata: Vec<*mut kafka_producer_RecordMetadata_t> = vec![std::ptr::null_mut(); count];
            let mut errors: Vec<*mut kafka_common_Error_t> =
                (0..count).map(|_| box_error(Error::new(Errors::InvalidRequest))).collect();
            // SAFETY: `callback` was supplied by the C caller along with `user_data`;
            // `metadata` and `errors` are local `Vec`s of exactly `count` entries each
            // (nulls, and fresh `box_error` handles the callee owns), alive for the
            // duration of the call. This path is taken only when no entry was non-null, so
            // no task is spawned and the function returns right after: a single inline
            // invocation on the calling thread, which the contract allows for a synchronous
            // failure.
            unsafe { callback(metadata.as_mut_ptr(), errors.as_mut_ptr(), count as i32, user_data) };
            return;
        },
    };

    let target = RecordBatchCallbackTarget { callback, user_data };
    spawn_callback_task(&runtime, async move {
        let target = target;
        // Await all futures first, collecting owned (Send) results so no raw
        // pointers are held across a suspension point.
        let mut results: Vec<Option<Result<RecordMetadata, Error>>> = Vec::with_capacity(count);
        for f in futs {
            match f {
                None => results.push(None),
                Some(fut) => results.push(Some(fut.get().await)),
            }
        }
        // No more `.await`s: build the raw-pointer arrays.
        let mut metadata = Vec::with_capacity(count);
        let mut errors = Vec::with_capacity(count);
        for r in results {
            match r {
                None => {
                    metadata.push(std::ptr::null_mut());
                    errors.push(box_error(Error::new(Errors::InvalidRequest)));
                },
                Some(Ok(m)) => {
                    metadata.push(box_metadata(m));
                    errors.push(std::ptr::null_mut());
                },
                Some(Err(e)) => {
                    metadata.push(std::ptr::null_mut());
                    errors.push(box_error(e));
                },
            }
        }
        let batch = RecordBatchCompletion { callback: target.callback, user_data: target.user_data, metadata, errors };
        // SAFETY: `RecordBatchCompletion::fire` requires exactly one call on the dispatcher
        // thread: `batch` is moved into this `FnOnce` job, which `enqueue_or_run_inline`
        // runs exactly once (on the dispatcher thread, or inline once it has exited). Its
        // parallel arrays were built after the last `.await` from fresh
        // `box_metadata`/`box_error` handles (one entry each per future), the
        // `callback`/`user_data` pair is the one the C caller supplied, and this spawned
        // task is the body's single fire site for this call (the inline no-future path
        // above returns before spawning).
        let job: CompletionJob = Box::new(move || unsafe { batch.fire() });
        enqueue_or_run_inline(&completion, job);
    });
}

/// Destroys a future handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `future` must be null or a valid handle from a send function.
/// - After this call, the pointer is invalid and must not be used.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_RecordMetadata_destroy(
    future: *mut kafka_common_KafkaFuture_RecordMetadata_t,
) {
    if !future.is_null() {
        // SAFETY: `future` is non-null (checked above) and, per this function's `# Safety`,
        // a valid handle from a send function, i.e. the pointer `box_future` leaked with
        // `Box::into_raw(Box<FfiFuture>)`; the same `# Safety` declares the pointer invalid
        // after this call, so this `Box::from_raw` is the single, final use of the
        // allocation.
        unsafe {
            drop(Box::from_raw(future as *mut FfiFuture));
        }
    }
}

/// Destroys an array of future handles, freeing all associated resources.
///
/// Null entries in the array are skipped (no-op for each).
///
/// # Parameters
///
/// - `futures`: Non-null pointer to an array of `count` future handles.
/// - `count`: Number of elements (must be ≥ 0).
///
/// # Safety
///
/// - `futures` must be non-null and point to an array of at least `count`
///   elements.
/// - Each non-null entry must be a valid handle from a send function.
/// - After this call, all pointers in the array are invalid and must not be
///   used.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaFuture_RecordMetadata_destroy_all(
    futures: *mut *mut kafka_common_KafkaFuture_RecordMetadata_t,
    count: i32,
) {
    assert!(!futures.is_null(), "futures must not be null");
    // A negative count must fail rather than be clamped; the entry point's
    // `#[ffi_guard]` reports this panic as the call's failure.
    assert!(count >= 0, "count must not be negative");

    for i in 0..count as usize {
        // SAFETY: `futures` is non-null and `count >= 0` (both asserted above), and `i <
        // count`; per this function's `# Safety`, `futures` points to an array of at least
        // `count` elements, so `futures.add(i)` is readable (null entries are skipped
        // below).
        let future = unsafe { *futures.add(i) };
        if !future.is_null() {
            // SAFETY: `future` is non-null (checked above) and, per this function's `#
            // Safety`, every non-null entry is a valid handle from a send function, i.e. a
            // `Box<FfiFuture>` leaked by `box_future` via `Box::into_raw`; the same `#
            // Safety` declares all pointers in the array invalid after this call, so this
            // `Box::from_raw` is the single, final use of each entry.
            unsafe {
                drop(Box::from_raw(future as *mut FfiFuture));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RecordMetadata
// ---------------------------------------------------------------------------

/// Returns the offset of the record.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
///
/// # Returns
///
/// The offset, or `-1` if the metadata handle is null.
///
/// # Safety
///
/// `metadata` must be a valid handle from [`kafka_common_KafkaFuture_RecordMetadata_get`], or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_offset(metadata: *const kafka_producer_RecordMetadata_t) -> i64 {
    if metadata.is_null() {
        return -1;
    }
    // SAFETY: `metadata` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from `kafka_common_KafkaFuture_RecordMetadata_get`, i.e. a
    // `Box<RecordMetadataInner>` leaked by `box_metadata`, which is what `metadata_ref`
    // requires; the reference is used only to read `offset` within this call, during which
    // the C caller keeps the handle alive.
    unsafe { metadata_ref(metadata) }.metadata.offset()
}

/// Returns the topic name as a null-terminated C string.
///
/// The returned pointer is valid until [`kafka_producer_RecordMetadata_destroy`] is
/// called on the same handle.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
///
/// # Returns
///
/// A `*const c_char` pointing to the topic name, or null if the metadata
/// handle is null.
///
/// # Safety
///
/// `metadata` must be a valid handle from [`kafka_common_KafkaFuture_RecordMetadata_get`], or null.
/// The returned pointer must not be used after the metadata is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_topic(
    metadata: *const kafka_producer_RecordMetadata_t,
) -> *const c_char {
    if metadata.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `metadata` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from `kafka_common_KafkaFuture_RecordMetadata_get`, i.e. a
    // `Box<RecordMetadataInner>` leaked by `box_metadata`, which is what `metadata_ref`
    // requires. The returned pointer targets `topic_cstring`, which lives inside that boxed
    // allocation until `kafka_producer_RecordMetadata_destroy` — exactly the validity this
    // function's docs and `# Safety` promise the caller.
    unsafe { metadata_ref(metadata) }.topic_cstring.as_ptr()
}

/// Returns the partition number of the record.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
///
/// # Returns
///
/// The partition number, or `-1` if the metadata handle is null.
///
/// # Safety
///
/// `metadata` must be a valid handle from [`kafka_common_KafkaFuture_RecordMetadata_get`], or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_partition(
    metadata: *const kafka_producer_RecordMetadata_t,
) -> i32 {
    if metadata.is_null() {
        return -1;
    }
    // SAFETY: `metadata` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from `kafka_common_KafkaFuture_RecordMetadata_get`, i.e. a
    // `Box<RecordMetadataInner>` leaked by `box_metadata`, which is what `metadata_ref`
    // requires; the reference is used only to read `partition` within this call, during
    // which the C caller keeps the handle alive.
    unsafe { metadata_ref(metadata) }.metadata.partition()
}

/// Returns the timestamp of the record.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
///
/// # Returns
///
/// The timestamp in milliseconds, or `-1` if the metadata handle is null
/// or no timestamp was set.
///
/// # Safety
///
/// `metadata` must be a valid handle from [`kafka_common_KafkaFuture_RecordMetadata_get`], or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_timestamp(
    metadata: *const kafka_producer_RecordMetadata_t,
) -> i64 {
    if metadata.is_null() {
        return -1;
    }
    // SAFETY: `metadata` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from `kafka_common_KafkaFuture_RecordMetadata_get`, i.e. a
    // `Box<RecordMetadataInner>` leaked by `box_metadata`, which is what `metadata_ref`
    // requires; the reference is used only to read `timestamp` within this call, during
    // which the C caller keeps the handle alive.
    unsafe { metadata_ref(metadata) }.metadata.timestamp()
}

/// Copies all metadata fields to the caller via a callback, then destroys the
/// handle.
///
/// This is a convenience function that extracts offset, partition, topic, and
/// timestamp in a single call and frees the handle, avoiding multiple
/// round-trips through the FFI boundary.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
/// - `callback`: Function pointer invoked with the extracted fields.
/// - `user_data`: Opaque pointer forwarded to `callback`.
///
/// The callback signature is:
/// ```c
/// void callback(int64_t offset, int32_t partition,
///               const char *topic, int64_t timestamp,
///               void *user_data);
/// ```
///
/// # Safety
///
/// - `metadata` must be a valid, non-null handle from
///   [`kafka_common_KafkaFuture_RecordMetadata_get`].
/// - `callback` must be a valid function pointer.
/// - The `topic` pointer passed to the callback is only valid for the duration
///   of the callback invocation.
/// - After this call the metadata handle is destroyed and must not be used.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_copy(
    metadata: *mut kafka_producer_RecordMetadata_t,
    callback: unsafe extern "C" fn(i64, i32, *const c_char, i64, *mut std::ffi::c_void),
    user_data: *mut std::ffi::c_void,
) {
    if metadata.is_null() {
        return;
    }

    // SAFETY: `metadata` is non-null (checked above) and, per this function's `# Safety`, a
    // valid non-null handle from `kafka_common_KafkaFuture_RecordMetadata_get`, i.e. a
    // `Box<RecordMetadataInner>` leaked by `box_metadata`, which is what `metadata_ref`
    // requires; `inner` is read only until the `Box::from_raw` below reclaims the
    // allocation and is not touched afterwards.
    let inner = unsafe { metadata_ref(metadata) };
    let offset = inner.metadata.offset();
    let partition = inner.metadata.partition();
    let topic = inner.topic_cstring.as_ptr();
    let timestamp = inner.metadata.timestamp();

    // SAFETY: `callback` was supplied by the C caller along with `user_data` and, per this
    // function's `# Safety`, is a valid function pointer; it is invoked inline on the
    // calling thread exactly once (straight-line code with no other fire site). `topic`
    // points into `inner.topic_cstring`, which stays allocated until the `Box::from_raw`
    // that runs after this call returns, matching the `# Safety` statement that `topic` is
    // valid only for the duration of the callback; the remaining arguments are plain
    // copies.
    unsafe {
        callback(offset, partition, topic, timestamp, user_data);
    }

    // Destroy the handle after the callback returns.
    // SAFETY: `metadata` is non-null (checked above) and is the pointer `box_metadata`
    // leaked with `Box::into_raw(Box<RecordMetadataInner>)`; this function's `# Safety`
    // declares the handle destroyed and unusable after the call, so this `Box::from_raw` is
    // the single, final use. It runs only after `callback` has returned, and `inner`, which
    // aliases the same allocation, is not used past this point.
    unsafe {
        drop(Box::from_raw(metadata as *mut RecordMetadataInner));
    }
}

/// Destroys a record metadata handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `metadata` must be null or a valid handle from [`kafka_common_KafkaFuture_RecordMetadata_get`].
/// - After this call, the pointer is invalid and must not be used.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_destroy(metadata: *mut kafka_producer_RecordMetadata_t) {
    if !metadata.is_null() {
        // SAFETY: `metadata` is non-null (checked above) and, per this function's `#
        // Safety`, a valid handle from `kafka_common_KafkaFuture_RecordMetadata_get`, i.e.
        // the pointer `box_metadata` leaked with `Box::into_raw(Box<RecordMetadataInner>)`;
        // the same `# Safety` declares the pointer invalid after this call, so this
        // `Box::from_raw` is the single, final use of the allocation.
        unsafe {
            drop(Box::from_raw(metadata as *mut RecordMetadataInner));
        }
    }
}

// ---------------------------------------------------------------------------
// Producer operations
// ---------------------------------------------------------------------------

/// Flushes all pending records.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `out_error`: Pointer where an error handle will be written on failure,
///   or null if the caller does not need error details.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_flush(
    producer: *mut kafka_producer_Producer_t,
    out_error: *mut *mut kafka_common_Error_t,
) {
    if producer.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return;
    }

    // Hand over any records still queued by `send_async` before flushing, so
    // `flush` observes them — Java's `flush()` blocks until every prior send
    // completes, and a queued record has not reached the accumulator `flush`
    // drains. Without this the FFI flush could return "done" with records unsent.
    // The drain must run without holding the `kind` lock: the submission task
    // takes that lock to hand over the sends ahead of the barrier, so blocking on
    // the barrier while holding it would deadlock. Grab a runtime handle under a
    // brief lock, drop it, then drain, then take the lock for the flush itself.
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, i.e. one created by `build_producer_handle`, which is what
    // `producer_handle` requires; the `&'static ProducerHandle` is used only within this
    // call (`queued_sends`, `submit_tx` for the drain), during which the C caller keeps the
    // handle alive.
    let handle = unsafe { producer_handle(producer) };
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a producer constructor, which is what `producer_ref` requires; the
    // `&'static Mutex<ProducerKind>` is locked only within this call (briefly for the
    // runtime handle, then again for the flush itself), during which the C caller keeps the
    // handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let rt_handle = producer_mtx.lock().unwrap().runtime().handle().clone();
    if let Err(e) = drain_submitted_sends_via(&handle.queued_sends, &handle.submit_tx, &rt_handle) {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(e) };
        }
        return;
    }

    let guard = producer_mtx.lock().unwrap();
    let rt = guard.runtime();
    let result = match &*guard {
        ProducerKind::Mock(mock, _) => rt.block_on(mock.flush()),
        ProducerKind::Kafka(kafka, _) => rt.block_on(kafka.flush()),
    };
    if !out_error.is_null() {
        // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
        // Parameters`, a pointer where an error handle will be written on failure or null
        // if the caller does not need error details; exactly one element is written: null
        // on success, or a fresh `box_error` handle the caller owns.
        unsafe {
            *out_error = match result {
                Ok(()) => std::ptr::null_mut(),
                Err(e) => box_error(e),
            };
        }
    }
}

// ---------------------------------------------------------------------------
// MetricMap — the `Producer::metrics()` snapshot
// ---------------------------------------------------------------------------
//
// The snapshot representation (`MetricEntry` / `MetricMapInner`), the
// snapshot-building logic, and the index-walking accessor helpers live in
// `ffi::common` and are shared verbatim with the consumer FFI surface
// (`kafka_consumer_MetricMap_*`). Only the namespaced opaque type + `extern "C"`
// wrappers are producer-specific here. The value-kind discriminants
// (0=Double, 1=String, 2=Long, 3=Int) are the shared
// `crate::ffi::common::METRIC_VALUE_*` constants — cbindgen does not emit them
// into the header, so there is nothing producer-specific to export.

/// Opaque handle to a `Map<MetricName, Metric>` snapshot
/// (`Producer::metrics()`).
///
/// Java's `metrics()` returns live `Metric` objects whose `metricValue()`
/// re-measures on each read. This handle is a **point-in-time snapshot**: each
/// entry's value was measured once, when `metrics()` was called. That matches
/// the documented contract of [`crate::producer::Producer::metrics`], and it is
/// the only thing that can cross an FFI boundary without an upcall per read.
///
/// This is a distinct producer-namespaced type (not shared with
/// `kafka_consumer_MetricMap_t`): the two opaque types are pinned per FFI
/// surface by cbindgen and the C tests, so they cannot be merged without an ABI
/// break. Only the internal machinery is shared (see `ffi::common`).
#[repr(C)]
pub struct kafka_producer_MetricMap_t {
    _private: [u8; 0],
}

/// Reads a snapshot of the producer's metrics, mirroring Java's
/// `Map<MetricName, ? extends Metric> metrics()`. Each entry's value is measured
/// once, at call time. Returns a [`kafka_producer_MetricMap_t`]; free it with
/// [`kafka_producer_MetricMap_destroy`]. `metrics()` does not block in Java, so
/// this takes no runtime.
///
/// # Safety
///
/// `producer` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_metrics(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_producer_MetricMap_t {
    if producer.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a producer constructor, which is what `producer_ref` requires; the
    // `&'static Mutex<ProducerKind>` is locked only within this call to take the metrics
    // snapshot, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    let metrics: HashMap<MetricName, Arc<KafkaMetric>> = match &*guard {
        ProducerKind::Mock(mock, _) => mock.metrics(),
        ProducerKind::Kafka(kafka, _) => kafka.metrics(),
    };
    Box::into_raw(common::build_metric_map_inner(metrics)) as *mut kafka_producer_MetricMap_t
}

/// Returns the number of metric entries.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_count(map: *const kafka_producer_MetricMap_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer `common::metric_map_count`
    // requires (the helper dereferences `inner` unconditionally, so null is excluded by the
    // contract rather than by a check); the read is confined to this call, during which the
    // C caller keeps the handle alive.
    unsafe { common::metric_map_count(map as *const MetricMapInner) }
}

/// Returns the metric name at `index` (borrowed; valid until the map is
/// destroyed), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_name(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer `common::metric_map_get_name`
    // requires (the helper dereferences `inner` unconditionally, so null is excluded by the
    // contract rather than by a check, while `index` is bounds-checked by the helper). The
    // returned pointer borrows a `CString` inside the map and is valid until
    // `kafka_producer_MetricMap_destroy`, as documented.
    unsafe { common::metric_map_get_name(map as *const MetricMapInner, index) }
}

/// Returns the metric group at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_group(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer `common::metric_map_get_group`
    // requires (the helper dereferences `inner` unconditionally, so null is excluded by the
    // contract rather than by a check, while `index` is bounds-checked by the helper). The
    // returned pointer borrows a `CString` inside the map and is valid until
    // `kafka_producer_MetricMap_destroy`, as documented.
    unsafe { common::metric_map_get_group(map as *const MetricMapInner, index) }
}

/// Returns the metric description at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_description(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_description` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` is bounds-checked by the helper). The returned pointer borrows a `CString`
    // inside the map and is valid until `kafka_producer_MetricMap_destroy`, as documented.
    unsafe { common::metric_map_get_description(map as *const MetricMapInner, index) }
}

/// Returns the number of tags on the metric at `index`, or `-1` if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_tag_count(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_tag_count` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` is bounds-checked by the helper, which returns `-1` out of range); the read
    // is confined to this call.
    unsafe { common::metric_map_get_tag_count(map as *const MetricMapInner, index) }
}

/// Returns the `tag_index`-th tag key of the metric at `index` (borrowed), or
/// null if either index is out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_tag_key(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer `common::metric_map_get_tag_key`
    // requires (the helper dereferences `inner` unconditionally, so null is excluded by the
    // contract rather than by a check, while `index` and `tag_index` are bounds-checked by
    // the helper). The returned pointer borrows a `CString` inside the map and is valid
    // until `kafka_producer_MetricMap_destroy`, as documented.
    unsafe { common::metric_map_get_tag_key(map as *const MetricMapInner, index, tag_index) }
}

/// Returns the `tag_index`-th tag value of the metric at `index` (borrowed), or
/// null if either index is out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_tag_value(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_tag_value` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` and `tag_index` are bounds-checked by the helper). The returned pointer
    // borrows a `CString` inside the map and is valid until
    // `kafka_producer_MetricMap_destroy`, as documented.
    unsafe { common::metric_map_get_tag_value(map as *const MetricMapInner, index, tag_index) }
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
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_kind(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_value_kind` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` is bounds-checked by the helper); the read is confined to this call.
    unsafe { common::metric_map_get_value_kind(map as *const MetricMapInner, index) }
}

/// Returns the `Double` reading of the metric at `index`, or `0.0` if out of
/// range or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_double(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_value_double` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` is bounds-checked by the helper); the read is confined to this call.
    unsafe { common::metric_map_get_value_double(map as *const MetricMapInner, index) }
}

/// Returns the `String` reading of the metric at `index` (borrowed), or null if
/// out of range. Empty for a non-`String` kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_string(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_value_string` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` is bounds-checked by the helper). The returned pointer borrows a `CString`
    // inside the map and is valid until `kafka_producer_MetricMap_destroy`, as documented.
    unsafe { common::metric_map_get_value_string(map as *const MetricMapInner, index) }
}

/// Returns the `Long` reading of the metric at `index`, or `0` if out of range
/// or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_long(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_value_long` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` is bounds-checked by the helper); the read is confined to this call.
    unsafe { common::metric_map_get_value_long(map as *const MetricMapInner, index) }
}

/// Returns the `Int` reading of the metric at `index`, or `0` if out of range
/// or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_int(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `map` is a valid metric-map handle, i.e. the
    // `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` leaked with
    // `Box::into_raw`, which is the valid backing pointer
    // `common::metric_map_get_value_int` requires (the helper dereferences `inner`
    // unconditionally, so null is excluded by the contract rather than by a check, while
    // `index` is bounds-checked by the helper); the read is confined to this call.
    unsafe { common::metric_map_get_value_int(map as *const MetricMapInner, index) }
}

/// Destroys a metric-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid metric-map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_destroy(map: *mut kafka_producer_MetricMap_t) {
    // SAFETY: `common::metric_map_destroy` is a no-op on null and otherwise requires a
    // valid metric-map backing pointer; per this function's `# Safety`, `map` is null or a
    // valid metric-map handle, i.e. the `Box<MetricMapInner>` that
    // `kafka_producer_Producer_metrics` leaked with `Box::into_raw`. Destroying is the
    // documented final use of the handle, so the helper's `Box::from_raw` reclaims the
    // allocation exactly once.
    unsafe { common::metric_map_destroy(map as *mut MetricMapInner) };
}

/// Returns the partition metadata for a topic. On success writes a
/// [`kafka_common_PartitionInfoList_t`] to `*out_list` (free it with
/// [`kafka_common_PartitionInfoList_destroy`]) and returns null; on failure
/// returns a non-null error and leaves `*out_list` untouched. The
/// `PartitionInfoList` handle/accessors are shared with the consumer FFI.
///
/// # Safety
///
/// `producer` must be a valid handle; `topic` a valid C string; `out_list` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for(
    producer: *mut kafka_producer_Producer_t,
    topic: *const c_char,
    out_list: *mut *mut kafka_common_PartitionInfoList_t,
) -> *mut kafka_common_Error_t {
    if producer.is_null() || topic.is_null() {
        return box_error(Error::new(Errors::InvalidRequest));
    }
    // SAFETY: `topic` is non-null (checked above) and, per this function's `# Safety`, a
    // valid C string; it is copied into an owned `String` immediately, so nothing borrowed
    // outlives the call.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, which is what `producer_ref` requires; the `&'static
    // Mutex<ProducerKind>` is locked only within this call to run `partitions_for` under
    // `block_on`, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    let rt = guard.runtime();
    let result = match &*guard {
        ProducerKind::Mock(mock, _) => rt.block_on(mock.partitions_for(&topic_str)),
        ProducerKind::Kafka(kafka, _) => rt.block_on(kafka.partitions_for(&topic_str)),
    };
    match result {
        Ok(infos) => {
            if !out_list.is_null() {
                // SAFETY: `out_list` is non-null (checked above) and, per this function's
                // `# Safety`, valid, i.e. writable; exactly one element is written, a fresh
                // `box_partition_info_list` handle whose ownership passes to the caller
                // (freed with `kafka_common_PartitionInfoList_destroy`, as documented).
                unsafe { *out_list = box_partition_info_list(infos) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Closes the producer.
///
/// After closing, further send calls will fail.
///
/// # Parameters
///
/// - `producer`: Producer handle, or null (no-op).
/// - `out_error`: Pointer where an error handle will be written on failure,
///   or null if the caller does not need error details.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close(
    producer: *mut kafka_producer_Producer_t,
    out_error: *mut *mut kafka_common_Error_t,
) {
    if producer.is_null() {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written (null: closing a null producer is the documented no-op success).
            unsafe { *out_error = std::ptr::null_mut() };
        }
        return;
    }

    // Hand over records still queued by `send_async` before closing. Java's
    // `close()` flushes by default (only `close(Duration.ZERO)` discards, and this
    // FFI exposes only the flushing form), so queued records must be produced, not
    // dropped. Without this, close would race the queued `send`s: the producer
    // shuts down, each queued `send` then fails `ensure_not_closed`, and the
    // record is lost. As in `flush`, drain without holding the `kind` lock.
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, i.e. one created by `build_producer_handle`, which is what
    // `producer_handle` requires; the `&'static ProducerHandle` is used only within this
    // call (`queued_sends`, `submit_tx` for the drain), during which the C caller keeps the
    // handle alive.
    let handle = unsafe { producer_handle(producer) };
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a producer constructor, which is what `producer_ref` requires; the
    // `&'static Mutex<ProducerKind>` is locked only within this call (briefly for the
    // runtime handle, then again for the close itself), during which the C caller keeps the
    // handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let rt_handle = producer_mtx.lock().unwrap().runtime().handle().clone();
    if let Err(e) = drain_submitted_sends_via(&handle.queued_sends, &handle.submit_tx, &rt_handle) {
        if !out_error.is_null() {
            // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
            // Parameters`, a pointer where an error handle will be written on failure or
            // null if the caller does not need error details; exactly one element is
            // written, a fresh `box_error` handle the caller owns.
            unsafe { *out_error = box_error(e) };
        }
        return;
    }

    let guard = producer_mtx.lock().unwrap();
    let rt = guard.runtime();
    let result = match &*guard {
        ProducerKind::Mock(mock, _) => rt.block_on(mock.close()),
        ProducerKind::Kafka(kafka, _) => rt.block_on(kafka.close()),
    };
    if !out_error.is_null() {
        // SAFETY: `out_error` is non-null (checked above) and, per this function's `#
        // Parameters`, a pointer where an error handle will be written on failure or null
        // if the caller does not need error details; exactly one element is written: null
        // on success, or a fresh `box_error` handle the caller owns.
        unsafe {
            *out_error = match result {
                Ok(()) => std::ptr::null_mut(),
                Err(e) => box_error(e),
            };
        }
    }
}

/// Shared implementation of [`kafka_producer_Producer_flush_async`] and
/// [`kafka_producer_Producer_close_async`].
fn flush_or_close_async(
    producer: *mut kafka_producer_Producer_t,
    callback: OperationCallbackFn,
    user_data: *mut std::ffi::c_void,
    is_close: bool,
) {
    if producer.is_null() {
        // Match the sync APIs: flush(null) is an error, close(null) is success.
        let error = if is_close {
            std::ptr::null_mut()
        } else {
            box_error(Error::new(Errors::InvalidRequest))
        };
        // SAFETY: `callback` was supplied by the C caller along with `user_data` through
        // `kafka_producer_Producer_flush_async`/`_close_async`, whose `# Safety` documents
        // a null `producer` as reported via `callback` (flush) or as a no-op success
        // (close); `error` is a fresh `box_error` handle the callee owns, or null for
        // close. The call runs inline on the calling thread and the function returns right
        // after without spawning anything, so this is the single invocation for this call.
        unsafe { callback(error, user_data) };
        return;
    }

    // SAFETY: `producer` is non-null (checked above) and, per the `# Safety` of both
    // callers (`kafka_producer_Producer_flush_async`/`_close_async`), a valid handle, i.e.
    // one created by `build_producer_handle` as `producer_handle` requires. This `handle`
    // reference is used only on the calling thread for the duration of this call (cloning
    // `completion_tx`, locking `kind`, `reserve_pending_task`), while the spawned task
    // receives the address as a `usize` and derives its own reference under its own
    // justification.
    let handle = unsafe { producer_handle(producer) };
    let completion = handle.completion_tx.clone();
    let runtime = handle.kind.lock().unwrap().runtime().handle().clone();
    let ptr = producer as usize;
    let target = OperationCallbackTarget { callback, user_data };

    // Two-phase registration (see `reserve_pending_task`): nothing that can panic
    // runs after the spawn, so a caught panic never races this task's completion.
    let mut pending = reserve_pending_task(handle);
    let task = spawn_callback_task(&runtime, async move {
        let target = target;
        // SAFETY: the handle outlives this task — it is registered via
        // `reserve_pending_task` and `destroy` joins it before dropping the
        // producer it borrows from.
        let h = unsafe { &*(ptr as *const ProducerHandle) };
        // Order this flush/close after records still queued by `send_async`, the
        // async counterpart of the sync path's drain. Both flush and close must
        // hand queued records over (Java `flush` blocks until sends complete;
        // `close` flushes) rather than race them, so a drain failure aborts the
        // operation with the error rather than reporting false success. Awaiting
        // the barrier holds no lock, so the submission task processing the sends
        // ahead of it is free to take the `kind` lock (no deadlock).
        let result = match drain_submitted_sends_await(&h.queued_sends, &h.submit_tx).await {
            Err(e) => Err(e),
            // Brief lock to extend a reference to the inner producer; the guard is
            // dropped before the `.await` (CLAUDE.md §11.6).
            // SAFETY: `ptr` is the same live leaked `*const ProducerHandle` as `h`, so
            // `producer_static_ref`'s `# Safety` (a live, not yet destroyed handle) holds
            // for the same reason: this task is registered via `reserve_pending_task` and
            // `destroy` joins it before dropping the producer. `producer_static_ref` takes
            // and releases the `kind` lock internally, so no guard is held across the
            // `flush`/`close` `.await`, and the extended inner-producer reference is used
            // only within this task.
            Ok(()) => match unsafe { producer_static_ref(ptr) } {
                ProducerStaticRef::Kafka(k) => {
                    if is_close {
                        k.close().await
                    } else {
                        k.flush().await
                    }
                },
                ProducerStaticRef::Mock(m) => {
                    if is_close {
                        m.close().await
                    } else {
                        m.flush().await
                    }
                },
            },
        };
        let error = match result {
            Ok(()) => std::ptr::null_mut(),
            Err(e) => box_error(e),
        };
        let op = OperationCompletion { callback: target.callback, user_data: target.user_data, error };
        // SAFETY: `OperationCompletion::fire` requires exactly one call on the dispatcher
        // thread: `op` is moved into this `FnOnce` job, which `enqueue_or_run_inline` runs
        // exactly once (on the dispatcher thread, or inline once it has exited). `error` is
        // a fresh `box_error` handle or null, built after the last `.await`; the
        // `callback`/`user_data` pair is the one the C caller supplied, and this task is
        // the single fire site for a call that reached the spawn (the inline null-producer
        // fire above returns before any task exists).
        let job: CompletionJob = Box::new(move || unsafe { op.fire() });
        enqueue_or_run_inline(&completion, job);
    });
    pending.push(task);
}

/// Asynchronously flushes all pending records, invoking `callback` on
/// completion (the async counterpart of [`kafka_producer_Producer_flush`]).
///
/// Returns immediately; `callback` fires on the producer's dispatcher thread
/// with a null error on success or a non-null [`kafka_common_Error_t`] the
/// caller must free on failure.
///
/// # Safety
///
/// `producer` must be a valid handle, or null (null reported via `callback`).
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
pub unsafe extern "C" fn kafka_producer_Producer_flush_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_flush_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    flush_or_close_async(producer, callback, user_data, false);
}

/// Asynchronously closes the producer, invoking `callback` on completion (the
/// async counterpart of [`kafka_producer_Producer_close`]).
///
/// Returns immediately; `callback` fires on the producer's dispatcher thread
/// with a null error on success or a non-null [`kafka_common_Error_t`] the
/// caller must free on failure.
///
/// # Safety
///
/// `producer` must be a valid handle, or null (null is a no-op success).
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
pub unsafe extern "C" fn kafka_producer_Producer_close_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_close_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    flush_or_close_async(producer, callback, user_data, true);
}

/// Owned `partitions_for` completion payload, fired by the dispatcher thread.
struct PartitionInfoListCompletion {
    callback: kafka_producer_Producer_partitions_for_callback_t,
    user_data: *mut std::ffi::c_void,
    list: *mut kafka_common_PartitionInfoList_t,
    error: *mut kafka_common_Error_t,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread; the
// C user is responsible for the thread-safety of `user_data`.
unsafe impl Send for PartitionInfoListCompletion {}
impl PartitionInfoListCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    unsafe fn fire(self) {
        // SAFETY: `self.callback`/`self.user_data` are the pair the C caller supplied to
        // `kafka_producer_Producer_partitions_for_async`, and `self.list`/`self.error` are
        // fresh owned handles from `box_partition_info_list`/`box_error` (exactly one of
        // them non-null) whose ownership transfers to the callee. Consuming `self` makes
        // this the single invocation for this completion, on the dispatcher thread per this
        // method's `# Safety` (or inline on the completing thread once the dispatcher has
        // exited, per `enqueue_or_run_inline`); `user_data` stays valid until the callback
        // fires per the caller's contract, and the C user is responsible for its
        // thread-safety.
        unsafe { (self.callback)(self.list, self.error, self.user_data) };
    }
}

/// Value-callback target (function pointer + opaque `user_data`) for
/// `partitions_for_async`, wrapped so it can cross the tokio task / dispatcher
/// thread boundary. See [`RecordCallbackTarget`].
#[derive(Clone, Copy)]
struct PartitionInfoListCallbackTarget {
    callback: kafka_producer_Producer_partitions_for_callback_t,
    user_data: *mut std::ffi::c_void,
}
// SAFETY: the C user owns the thread-safety of `user_data`; the function pointer
// is trivially shareable.
unsafe impl Send for PartitionInfoListCallbackTarget {}

/// Returns the partition metadata for a topic asynchronously, invoking
/// `callback` on completion (the async counterpart of
/// [`kafka_producer_Producer_partitions_for`]).
///
/// Returns immediately; `callback` fires on the producer's dispatcher thread
/// with a non-null [`kafka_common_PartitionInfoList_t`] and null error on
/// success, or a null list and non-null [`kafka_common_Error_t`] on
/// failure. The caller owns whichever handle is non-null.
///
/// # Safety
///
/// `producer` must be a valid handle, or null (null reported via `callback`);
/// `topic` a valid C string.
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
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for_async(
    producer: *mut kafka_producer_Producer_t,
    topic: *const c_char,
    callback: kafka_producer_Producer_partitions_for_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    if producer.is_null() {
        // SAFETY: `callback` was supplied by the C caller along with `user_data`, and this
        // function's `# Safety` documents a null `producer` as reported via `callback`; the
        // list argument is null and the error is a fresh `box_error` handle the callee
        // owns. The call runs inline on the calling thread and the function returns right
        // after without spawning anything, so this is the single invocation for this call.
        unsafe { callback(std::ptr::null_mut(), box_error(Error::new(Errors::InvalidRequest)), user_data) };
        return;
    }
    // SAFETY: `topic` is not null-checked here (unlike the synchronous
    // `kafka_producer_Producer_partitions_for`); its validity rests entirely on this
    // function's `# Safety`, which requires `topic` to be a valid C string. It is copied
    // into an owned `String` immediately, so nothing borrowed outlives the call.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();

    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle, i.e. one created by `build_producer_handle` as `producer_handle`
    // requires. This `handle` reference is used only on the calling thread for the duration
    // of this call (cloning `completion_tx`, locking `kind`, `reserve_pending_task`), while
    // the spawned task receives the address as a `usize` and derives its own reference
    // under its own justification.
    let handle = unsafe { producer_handle(producer) };
    let completion = handle.completion_tx.clone();
    let runtime = handle.kind.lock().unwrap().runtime().handle().clone();
    let ptr = producer as usize;
    let target = PartitionInfoListCallbackTarget { callback, user_data };

    // Two-phase registration (see `reserve_pending_task`): nothing that can panic
    // runs after the spawn, so a caught panic never races this task's completion.
    let mut pending = reserve_pending_task(handle);
    let task = spawn_callback_task(&runtime, async move {
        let target = target;
        // Brief lock to extend a reference to the inner producer; the guard is
        // dropped before the `.await` (CLAUDE.md §11.6).
        // SAFETY: `ptr` is the address of the live `ProducerHandle` validated above, so
        // `producer_static_ref`'s `# Safety` (a live, not yet destroyed handle) holds: this
        // task is registered via `reserve_pending_task` (the `pending` guard is held across
        // the `spawn` and the `JoinHandle` is pushed right after) and `destroy` joins it
        // before dropping the producer. `producer_static_ref` releases the `kind` lock
        // before returning, so none is held across the `partitions_for` `.await`, and the
        // extended inner-producer reference is used only within this task.
        let result = match unsafe { producer_static_ref(ptr) } {
            ProducerStaticRef::Kafka(k) => k.partitions_for(&topic_str).await,
            ProducerStaticRef::Mock(m) => m.partitions_for(&topic_str).await,
        };
        let (list, error) = match result {
            Ok(infos) => (box_partition_info_list(infos), std::ptr::null_mut()),
            Err(e) => (std::ptr::null_mut(), box_error(e)),
        };
        let completion_payload =
            PartitionInfoListCompletion { callback: target.callback, user_data: target.user_data, list, error };
        // SAFETY: `PartitionInfoListCompletion::fire` requires exactly one call on the
        // dispatcher thread: `completion_payload` is moved into this `FnOnce` job, which
        // `enqueue_or_run_inline` runs exactly once (on the dispatcher thread, or inline
        // once it has exited). `list`/`error` are fresh handles built after the only
        // `.await`, the `callback`/`user_data` pair is the one the C caller supplied, and
        // this task is the single fire site for a call that reached the spawn (the inline
        // null-producer fire above returns before any task exists).
        let job: CompletionJob = Box::new(move || unsafe { completion_payload.fire() });
        enqueue_or_run_inline(&completion, job);
    });
    pending.push(task);
}

// ---------------------------------------------------------------------------
// Transactions
//
// The C counterparts of Java's five transaction-control methods
// (`KafkaProducer.java:648, 674, 733, 779, 813`). Design record:
// `design/history/Milestone-11/producer-transactions-ffi-plan.md`.
//
// Two properties are load-bearing and are what the plan document argues for:
//
//  1. **Blocking only.** Java's transaction API has no future-returning form, so
//     unlike the Admin FFI (whose Java surface *is* `KafkaFuture`-based and
//     therefore gets `_async` twins) there is no async contract to translate. A
//     caller must know each step's outcome before taking the next one, so an
//     `_async` variant would only add a way to violate the one-at-a-time rule.
//
//  2. **Guarded against each other, never against `send`.** All five acquire
//     `ProducerHandle::txn_control_busy` on entry and release it on return (RAII,
//     so a panic still releases). The flag is *not* held from `begin_transaction`
//     to `commit_transaction`: doing so would block the `send` calls that have to
//     happen in between, which is exactly the problem `PLAN.md` §7.2/§9.6 raised
//     against extending the Milestone-9 one-operation-in-flight guard. Holding it
//     per call is enough, because that is precisely Java's contract: the control
//     methods must not *overlap*; an open transaction is not itself an operation.
//
// Unlike the consumer's single-*owner* guard, this flag records no thread id and
// is deliberately not thread-affine: Java permits the lifecycle to be driven by
// different threads in sequence (e.g. off a pool), only never concurrently.
//
// Both synchronous (`kafka_producer_Producer_send` / `..._send_batch`) and async
// (`..._send_async` / `..._send_batch_async`) sends are supported inside a
// transaction. The synchronous pair registers the record before returning; the async
// pair only queues it for later, so `with_txn_control` drains the submission queue
// before running each control op (as `flush`/`close` do), handing every already-
// returned async send to the producer first. `commit_transaction` then commits those
// records and `abort_transaction` discards them, through the producer's own
// accumulator handling. See `.claude/rules/producer-transactions.md` §13.
// ---------------------------------------------------------------------------

/// RAII release of the transaction-control flag, so the flag is cleared on
/// return *or* panic — mirroring the consumer FFI's `ReleaseGuard` and Java's
/// `finally { release(); }`.
struct TxnControlGuard<'a>(&'a ProducerHandle);
impl Drop for TxnControlGuard<'_> {
    fn drop(&mut self) {
        self.0.txn_control_busy.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Async-lifetime analog of [`TxnControlGuard`]: releases the transaction-control
/// flag when dropped — on the calling thread if the call fails or panics before its
/// task is spawned, otherwise when the spawned task exits normally or unwinds on a
/// panic. It holds the handle by raw pointer (as a `usize`) rather than a borrow so
/// it can be moved into the spawned task; that reach into the handle is sound for
/// the same reason [`producer_static_ref`] and [`flush_or_close_async`] are —
/// `destroy` joins every task registered via [`reserve_pending_task`] before it
/// drops the producer, so the handle outlives the guard.
struct TxnControlAsyncGuard {
    handle_ptr: usize,
}
impl Drop for TxnControlAsyncGuard {
    fn drop(&mut self) {
        // SAFETY: the handle outlives this guard. On the calling thread the C caller
        // keeps it alive for the duration of the call; inside the spawned task it is
        // alive because that task is registered via `reserve_pending_task` and joined
        // by `destroy` before the producer is freed.
        let handle = unsafe { &*(self.handle_ptr as *const ProducerHandle) };
        handle.txn_control_busy.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Drives the async drain to completion with `block_on`, for the synchronous
/// `flush`/`close` paths. Orders those calls after non-blocking sends the
/// application queued with `send_async` / `send_batch_async`, so a queued record is
/// handed to the producer before `flush`/`close` drain the accumulator.
///
/// `send_async` only *queues* a record; the real `producer.send()` runs later on
/// the submission task, and `flush()` drains the producer's accumulator — which a
/// still-queued record has not reached yet. Without this ordering a `flush`/`close`
/// could return with a queued record unsent (Java's `flush` blocks until every
/// prior send completes; `close` flushes by default).
///
/// The same drain also runs before every transaction-control op ([`with_txn_control`]),
/// which is what makes async sends supported inside a transaction: a record queued by
/// `send_async` before `commit`/`abort` is handed to the producer here, then committed
/// or discarded by the producer's own accumulator handling.
///
/// # Cost
///
/// The `queued_sends == 0` fast path is the normal case — blocking sends never
/// queue, and async sends are usually long since drained — and costs one atomic
/// load, no channel round-trip.
///
/// # Errors
///
/// Reports [`Error::local_illegal_state`] if the submission task is gone. That is
/// not benign: a tokio receiver dropped with items still queued drops those items
/// *and their callbacks*, so the records were never produced and nothing will ever
/// report on them. Returning `Ok` here would tell the caller a flush/close
/// succeeded while records inside it silently vanished.
///
/// Sends queued *concurrently* by another thread are deliberately not covered: the
/// guarantee is "everything already submitted when this call began", which is the
/// strongest claim that can be ordered.
///
/// Split out from [`drain_submitted_sends_await`] for testability: both failure
/// paths require a dead submission task, which cannot be arranged through the C API
/// without destroying the handle, and they are the paths where returning `Ok` would
/// report a false flush/close.
fn drain_submitted_sends_via(
    queued_sends: &std::sync::atomic::AtomicUsize,
    submit_tx: &tokio::sync::mpsc::UnboundedSender<SubmitRequest>,
    runtime: &tokio::runtime::Handle,
) -> Result<(), Error> {
    runtime.block_on(drain_submitted_sends_await(queued_sends, submit_tx))
}

/// The `.await` form of the drain, shared by the sync path (via `block_on` above)
/// and the async `flush`/`close` path, which awaits it directly rather than
/// nesting a `block_on` inside a spawned task.
///
/// Returns immediately when nothing is queued (the normal case — one atomic
/// load). Otherwise pushes a barrier and awaits it, so every already-queued send
/// has been handed to the producer when it resolves.
async fn drain_submitted_sends_await(
    queued_sends: &std::sync::atomic::AtomicUsize,
    submit_tx: &tokio::sync::mpsc::UnboundedSender<SubmitRequest>,
) -> Result<(), Error> {
    if queued_sends.load(std::sync::atomic::Ordering::Acquire) == 0 {
        return Ok(());
    }
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    if submit_tx.send(SubmitRequest::Barrier { ack: Some(ack_tx) }).is_err() {
        return Err(Error::local_illegal_state(
            "the producer's send-submission task has stopped; records queued by \
             send_async were dropped without being produced and their callbacks \
             will never fire",
        ));
    }
    ack_rx.await.map_err(|_| {
        Error::local_illegal_state(
            "the producer's send-submission task stopped while ordering queued \
             sends; records queued by send_async may not have been produced",
        )
    })
}

/// Runs one transaction-control operation under the transaction-control flag.
///
/// Handles everything the five share: the null-handle check, the mutual-exclusion
/// CAS, releasing the flag on **every** exit path including a panic, and converting
/// the `Result` into the FFI's null-means-success error pointer. `op` receives the
/// inner producer and the runtime handle to drive it on.
///
/// The `kind` mutex is taken only briefly — to extend a reference to the inner
/// producer and clone the runtime handle — and dropped before `op` runs, so no
/// lock is held across the transaction RPC (CLAUDE.md §11.6) and a `send` between
/// `begin` and `commit` is never blocked by a control call for longer than that
/// brief lock.
///
/// Before running `op` it drains the submission queue ([`drain_submitted_sends_via`]),
/// exactly as `flush`/`close` do: every record still queued by `send_async` /
/// `send_batch_async` that *returned* to the caller before this control call began is
/// handed to the producer via `producer.send()` first. That is what makes async sends
/// supported inside a transaction — `commit_transaction` then includes those records
/// and `abort_transaction` discards them, through the producer's own accumulator
/// handling. Sends racing concurrently on another thread are not covered (the same
/// boundary [`drain_submitted_sends_await`] documents), and the normal case with
/// nothing queued costs one atomic load.
///
/// Taking a closure rather than returning the guard to the caller is deliberate: a
/// returned guard is only held for as long as each caller keeps a binding alive,
/// so a routine "unused variable" cleanup from `_guard` to `_` would silently
/// disable the mutual exclusion for that function — no compile error, no failing
/// test, and `#[must_use]` does not fire on `_`. Routing all five through here
/// makes the flag unskippable, and a sixth control function added later inherits
/// it by construction.
///
/// # Errors
///
/// - [`Errors::InvalidRequest`] if `producer` is null.
/// - [`Error::local_concurrent_modification`] if another transaction-control call
///   is already running.
/// - Whatever `op` returns.
///
/// # Safety
///
/// `producer` must be null or a valid handle from a producer constructor.
unsafe fn with_txn_control<F>(producer: *mut kafka_producer_Producer_t, op: F) -> *mut kafka_common_Error_t
where
    F: FnOnce(ProducerStaticRef, &tokio::runtime::Handle) -> Result<(), Error>,
{
    if producer.is_null() {
        return box_error(Error::with_message(Errors::InvalidRequest, "producer handle must not be null"));
    }
    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a producer constructor, i.e. one created by `build_producer_handle`
    // as `producer_handle` requires; the `&'static ProducerHandle` is used only for the
    // duration of this synchronous call (the CAS, `TxnControlGuard`, locking `kind`, the
    // drain), during which the C caller keeps the handle alive.
    let handle = unsafe { producer_handle(producer) };
    if handle
        .txn_control_busy
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .is_err()
    {
        return box_error(Error::local_concurrent_modification(
            "Transactional methods of KafkaProducer are not safe for concurrent access.",
        ));
    }
    // Held for the rest of the function, so every path below — an early return,
    // `op`'s error, or a panic inside `op` — releases the flag.
    let _guard = TxnControlGuard(handle);

    // Brief lock to clone the runtime handle and extend a reference to the inner
    // producer; the guard is dropped before `op` runs its `block_on`, so no lock
    // is held across the transaction RPC. The synchronous `block_on` completes
    // within this C call, and the C caller keeps the handle alive for its
    // duration, so the `&'static` reference does not outlive the producer and no
    // task registration is needed.
    let runtime = handle.kind.lock().unwrap().runtime().handle().clone();

    // Hand over records still queued by `send_async` / `send_batch_async` before
    // running the control op, exactly as `flush`/`close` do (see
    // [`drain_submitted_sends_via`] and [`flush_or_close_async`]). This is what makes
    // async sends supported inside a transaction: every `send_async` that *returned*
    // to the caller before this control call began is registered with the producer
    // via `producer.send()` before `op` runs, so `commit_transaction` includes those
    // records and `abort_transaction` discards them — the producer's own Java-faithful
    // accumulator handling then does the right thing. Sends racing concurrently on
    // another thread are not covered, the same boundary the drain already documents.
    // The normal case — no async send outstanding — is a single atomic load. A drain
    // failure means the submission task is gone and its queued records vanished, so
    // the control op is aborted with that error rather than reporting a commit/abort
    // that silently dropped records; `_guard` still releases `txn_control_busy` on
    // this early return.
    if let Err(e) = drain_submitted_sends_via(&handle.queued_sends, &handle.submit_tx, &runtime) {
        return box_error(e);
    }

    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // live handle from a producer constructor, so `producer as usize` satisfies
    // `producer_static_ref`'s `# Safety` (a leaked, not yet destroyed `*const
    // ProducerHandle`). The extended inner-producer reference is consumed only by `op`,
    // whose `block_on` completes within this C call while the C caller keeps the handle
    // alive, so it does not outlive the producer and no task registration is needed;
    // `producer_static_ref` releases the `kind` lock before `op` runs, so none is held
    // across the transaction RPC.
    let inner = unsafe { producer_static_ref(producer as usize) };

    match op(inner, &runtime) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
}

/// Initializes the transactional state, blocking until the producer id and epoch
/// have been obtained (or `max.block.ms` expires).
///
/// This is Java's `initTransactions()`, and must be called exactly once, before
/// any other transactional method, when `transactional.id` is configured. It
/// also completes or aborts any transaction left open by a previous producer
/// instance with the same `transactional.id`.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
///
/// # Returns
///
/// Null on success, or a non-null error handle the caller frees with
/// `kafka_common_Error_destroy`. A timeout error is safe to retry.
///
/// Like all five transaction-control functions, this fails with a
/// `ConcurrentModification` error (message: "Transactional methods of
/// KafkaProducer are not safe for concurrent access.") if another
/// transaction-control call is running — they must not overlap. That is a
/// caller-sequencing bug, not a transaction failure: the transaction is untouched
/// and must not be aborted in response.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `with_txn_control` requires `producer` to be null or a valid handle from a
    // producer constructor, which is exactly this function's `# Safety` (`producer` must be
    // a valid handle, or null) and is upheld by the C caller; `producer` is forwarded
    // unchanged, and the closure only drives the inner producer through `runtime.block_on`
    // within this call.
    unsafe {
        with_txn_control(producer, |inner, runtime| match inner {
            ProducerStaticRef::Kafka(k) => runtime.block_on(k.init_transactions()),
            ProducerStaticRef::Mock(m) => runtime.block_on(m.init_transactions()),
        })
    }
}

/// Begins a new transaction.
///
/// This is Java's `beginTransaction()`. Java's body is a pure state transition
/// that never waits, and the Rust counterpart is synchronous too, so no
/// `block_on` is needed for the transition itself.
///
/// `kafka_producer_Producer_init_transactions` must have completed successfully
/// exactly once before the first call.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
///
/// # Returns
///
/// Null on success, or a non-null error handle the caller frees with
/// `kafka_common_Error_destroy`, including the `ConcurrentModification`
/// rejection described on `kafka_producer_Producer_init_transactions`.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_begin_transaction(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `with_txn_control` requires `producer` to be null or a valid handle from a
    // producer constructor, which is exactly this function's `# Safety` (`producer` must be
    // a valid handle, or null) and is upheld by the C caller; `producer` is forwarded
    // unchanged, and the closure only calls the synchronous `begin_transaction` on the
    // inner producer within this call.
    unsafe {
        with_txn_control(producer, |inner, _runtime| match inner {
            ProducerStaticRef::Kafka(k) => k.begin_transaction(),
            ProducerStaticRef::Mock(m) => m.begin_transaction(),
        })
    }
}

/// Sends consumer-group offsets to the group coordinator as part of the ongoing
/// transaction, blocking until the coordinator has acknowledged them.
///
/// This is Java's `sendOffsetsToTransaction(Map, ConsumerGroupMetadata)`, the
/// producer half of the consume-transform-produce pattern. The offsets are only
/// considered committed if the transaction itself commits.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `topics`, `partitions`, `offsets`, `leader_epochs`, `metadata`: parallel
///   arrays of `count` entries describing the offsets to stage, marshaled
///   exactly like `kafka_consumer_Consumer_commit_sync_offsets`: `metadata` may
///   be null (or hold null entries) and a `leader_epoch < 0` means no epoch. As
///   in Java, each offset is the offset of the **next** record to consume.
/// - `count`: number of entries in the parallel arrays, which must be `>= 0`.
///   Zero stages nothing. **Whether it also succeeds depends on the backend**, and
///   both behaviours are faithful to their Java counterpart, which disagree with
///   each other: `KafkaProducer` short-circuits an empty map *before* consulting
///   transaction state (Java `KafkaProducer:738`), so a zero count returns success
///   even with no transaction open — it reports success without having staged
///   anything, so do not read it as confirmation that a transaction exists.
///   `MockProducer` runs its state checks *first* (Java `MockProducer:186-193`,
///   empty check at `:194-196`), so a zero count outside an initialised, in-flight
///   transaction returns an error. Inside an open transaction — the only state the
///   two agree on — both succeed.
///
///   Java takes a `Map`, so a repeated `(topic, partition)` cannot arise there;
///   here it can, and the last entry for a partition wins.
/// - `group_metadata`: non-null `kafka_consumer_ConsumerGroupMetadata_t`, the
///   handle returned by `kafka_consumer_Consumer_group_metadata`. It is
///   borrowed, not consumed — the caller still owns and destroys it. Prefer it
///   over a group-id-only metadata: it carries the generation and member id that
///   give stronger fencing.
///
/// # Returns
///
/// Null on success, or a non-null error handle the caller frees with
/// `kafka_common_Error_destroy`. If
/// `kafka_common_Error_is_transaction_abortable_error` is true for that error the
/// transaction must be aborted with `kafka_producer_Producer_abort_transaction`.
/// The `ConcurrentModification` rejection described on
/// `kafka_producer_Producer_init_transactions` also applies, and is **not** a
/// reason to abort.
///
/// # Panics
///
/// Panics if `count` is negative — a violated precondition, matching
/// `kafka_producer_Producer_send_batch`. Clamping it instead would stage no
/// offsets and still report success, which in a consume-transform-produce loop
/// silently breaks exactly-once.
///
/// # Safety
///
/// - `producer` must be a valid handle, or null.
/// - When `count > 0`, `topics`, `partitions` and `offsets` must each point to
///   `count` valid entries and are **not** null-checked — passing null is a
///   violated precondition, not a reported error (CLAUDE.md FFI §4), exactly as
///   for `kafka_consumer_Consumer_commit_sync_offsets`. `leader_epochs` and
///   `metadata` are the only two that may be null, and then `count` entries are
///   still required of whichever is non-null. `count == 0` reads none of them, so
///   all five may be null in that case.
/// - `group_metadata` must be a valid group-metadata handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_offsets_to_transaction(
    producer: *mut kafka_producer_Producer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `send_offsets_to_transaction_inner` has the same `# Safety` requirements as
    // this function, which the C caller upholds; the arguments are forwarded unchanged.
    unsafe {
        send_offsets_to_transaction_inner(
            producer,
            topics,
            partitions,
            offsets,
            leader_epochs,
            metadata,
            count,
            group_metadata,
        )
    }
}

/// Inner implementation of [`kafka_producer_Producer_send_offsets_to_transaction`].
///
/// Separated from the `extern "C"` wrapper, whose `#[ffi_guard]` turns a panic
/// here into a returned error handle.
///
/// # Panics
///
/// Panics if `count` is negative: it must fail rather than be clamped.
///
/// # Safety
///
/// Same requirements as [`kafka_producer_Producer_send_offsets_to_transaction`].
#[expect(clippy::too_many_arguments)]
unsafe fn send_offsets_to_transaction_inner(
    producer: *mut kafka_producer_Producer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *mut kafka_common_Error_t {
    // A negative count must fail rather than be clamped; the entry point's
    // `#[ffi_guard]` reports this panic as the call's failure.
    assert!(count >= 0, "count must not be negative");
    // Pure argument preconditions are checked before the guard is taken, so a
    // malformed call costs nothing and cannot be reported as a concurrency
    // rejection.
    if group_metadata.is_null() {
        return box_error(Error::local_illegal_argument(
            "group_metadata must not be null; pass the handle from kafka_consumer_Consumer_group_metadata",
        ));
    }
    // SAFETY: `with_txn_control` requires `producer` to be null or a valid handle, which
    // `kafka_producer_Producer_send_offsets_to_transaction`'s `# Safety` (inherited by this
    // function) promises and the C caller upholds. Inside the closure, `read_offset_map`
    // requires every non-null array to hold `count` valid entries: `count >= 0` (asserted
    // above) and, per that `# Safety`, when `count > 0` `topics`, `partitions` and
    // `offsets` point to `count` valid entries and are deliberately not null-checked, while
    // `leader_epochs` and `metadata` may be null (and `metadata` may hold null entries),
    // which the helper tolerates; `count == 0` reads none of them. `group_metadata_ref`
    // requires a valid group-metadata handle: `group_metadata` is non-null (checked above)
    // and valid per the same `# Safety`, and `group` is borrowed only for the `block_on`
    // inside this call, during which the C caller, who still owns the handle, keeps it
    // alive.
    unsafe {
        with_txn_control(producer, |inner, runtime| {
            // Marshaling stays inside the guard because it feeds `op`; its failure
            // path is one of the early returns the guard must survive.
            let offsets_map = read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count)?;
            // The handle outlives this blocking call, so the metadata is borrowed
            // straight out of it.
            let group = &**group_metadata_ref(group_metadata);
            match inner {
                ProducerStaticRef::Kafka(k) => runtime.block_on(k.send_offsets_to_transaction(offsets_map, group)),
                ProducerStaticRef::Mock(m) => runtime.block_on(m.send_offsets_to_transaction(offsets_map, group)),
            }
        })
    }
}

/// Commits the ongoing transaction, blocking until it has completed.
///
/// This is Java's `commitTransaction()`. It flushes any unsent records first, so
/// every `send` in the transaction must have succeeded for the commit to succeed.
///
/// # Async sends are included
///
/// Records sent with the synchronous `kafka_producer_Producer_send` /
/// `kafka_producer_Producer_send_batch` — which register the record before returning
/// — are part of the transaction, and so are records queued with the async
/// `kafka_producer_Producer_send_async` / `kafka_producer_Producer_send_batch_async`:
/// this call first drains the submission queue (as `flush`/`close` do), so every async
/// send that had *returned* to the caller before the commit began is handed to the
/// producer and committed with the transaction. Sends racing concurrently on another
/// thread are not included. See `.claude/rules/producer-transactions.md` §13.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
///
/// # Returns
///
/// Null on success, or a non-null error handle the caller frees with
/// `kafka_common_Error_destroy`. If
/// `kafka_common_Error_is_transaction_abortable_error` is true for that error, call
/// `kafka_producer_Producer_abort_transaction`. A timeout error, however, does
/// **not** say whether the commit reached the broker: it is safe to retry the
/// commit, but not to abort instead — the only other option is to close the
/// producer. The `ConcurrentModification` rejection described on
/// `kafka_producer_Producer_init_transactions` also applies, and is **not** a
/// reason to abort.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `with_txn_control` requires `producer` to be null or a valid handle from a
    // producer constructor, which is exactly this function's `# Safety` (`producer` must be
    // a valid handle, or null) and is upheld by the C caller; `producer` is forwarded
    // unchanged, and the closure only drives the inner producer through `runtime.block_on`
    // within this call.
    unsafe {
        with_txn_control(producer, |inner, runtime| match inner {
            ProducerStaticRef::Kafka(k) => runtime.block_on(k.commit_transaction()),
            ProducerStaticRef::Mock(m) => runtime.block_on(m.commit_transaction()),
        })
    }
}

/// Aborts the ongoing transaction, blocking until it has completed.
///
/// This is Java's `abortTransaction()`. Any unflushed records — those sent with the
/// synchronous `kafka_producer_Producer_send` / `kafka_producer_Producer_send_batch`
/// and not yet delivered, **and** those queued with the async
/// `kafka_producer_Producer_send_async` / `kafka_producer_Producer_send_batch_async`
/// that had already returned — are discarded, the same treatment Java gives
/// accumulator records on abort. Abort is the recovery operation and stays available
/// even when `kafka_producer_Producer_commit_transaction` cannot make progress.
///
/// # Async sends are included
///
/// This call first drains the submission queue (as `flush`/`close` do), so every async
/// send that had *returned* to the caller before the abort began is handed to the
/// producer and then discarded as part of the aborted transaction, rather than leaking
/// out after it. Sends racing concurrently on another thread are not ordered against
/// this call. See `.claude/rules/producer-transactions.md` §13.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
///
/// # Returns
///
/// Null on success, or a non-null error handle the caller frees with
/// `kafka_common_Error_destroy`. As for
/// `kafka_producer_Producer_commit_transaction`, a timeout error is safe to retry
/// but does not permit switching to a different operation, and the
/// `ConcurrentModification` rejection described on
/// `kafka_producer_Producer_init_transactions` is **not** a reason to retry
/// with a different operation.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `with_txn_control` requires `producer` to be null or a valid handle from a
    // producer constructor, which is exactly this function's `# Safety` (`producer` must be
    // a valid handle, or null) and is upheld by the C caller; `producer` is forwarded
    // unchanged, and the closure only drives the inner producer through `runtime.block_on`
    // within this call.
    unsafe {
        with_txn_control(producer, |inner, runtime| match inner {
            ProducerStaticRef::Kafka(k) => runtime.block_on(k.abort_transaction()),
            ProducerStaticRef::Mock(m) => runtime.block_on(m.abort_transaction()),
        })
    }
}

// ---------------------------------------------------------------------------
// Async (callback-based) transaction control
//
// The non-blocking twins of the five functions above. Java's transaction API has
// no future-returning form, but the Python client needs to drive the lifecycle
// from an event loop (and to build a synchronous API on top without blocking a
// worker thread), which is what emasab's PR #168 review asked for. Each returns
// immediately and reports through an [`OperationCallbackFn`], mirroring
// `flush_async` / `close_async`.
//
// All five route through `with_txn_control_async`, the async analog of
// `with_txn_control`: it combines that function's null-check + mutual-exclusion
// CAS + drain-before-op + null-means-success mapping with `flush_or_close_async`'s
// spawn + task-registration + dispatcher-thread completion. The one
// `txn_control_busy` flag is shared with the synchronous path, so a sync call and
// an in-flight async call reject each other with `concurrent_modification`. See the
// module-level "Concurrency model" docs and `.claude/rules/producer-transactions.md`
// §13.
// ---------------------------------------------------------------------------

/// Async analog of [`with_txn_control`]: runs one transaction-control operation off
/// the calling thread and delivers the result through an [`OperationCallbackFn`].
///
/// Shared by the five `_async` transaction-control entry points, combining the two
/// patterns their synchronous siblings and `flush`/`close` already use:
///
///  - from [`with_txn_control`]: the null-handle check, the mutual-exclusion CAS,
///    releasing the flag on **every** exit (success, op error, drain error, panic,
///    and the `prepare` failure below), the drain-before-op, and the
///    null-means-success error mapping;
///  - from [`flush_or_close_async`]: spawning the work on the producer's runtime,
///    registering the task so `destroy` joins it, awaiting the drain directly
///    (never `block_on` inside a task), and delivering completion through the
///    dispatcher thread via [`enqueue_or_run_inline`].
///
/// The flag is CAS'd on the **calling thread** so an overlapping control call —
/// synchronous or asynchronous — is rejected immediately with
/// [`Error::local_concurrent_modification`] and its callback fires synchronously
/// (no task is spawned), exactly as the sync path returns the rejection inline.
/// When the CAS succeeds the flag is held across the spawned task by
/// [`TxnControlAsyncGuard`], which releases it on drop.
///
/// `prepare` runs on the calling thread **after** the CAS and returns the operation
/// to spawn. It exists for `send_offsets_to_transaction_async`, which must marshal
/// its caller-owned C arrays (valid only for the duration of this synchronous call)
/// while the transaction-control flag is held — mirroring the sync path's order
/// (CAS → marshal → op). A `prepare` failure releases the flag and reports the
/// error through the callback without spawning. The other four pass a trivial
/// `|| Ok(op)`.
///
/// The returned `run` closure receives the inner producer and yields the future to
/// await after the drain.
///
/// # Safety
///
/// `producer` must be null or a valid handle from a producer constructor.
unsafe fn with_txn_control_async<Prepare, Run, Fut>(
    producer: *mut kafka_producer_Producer_t,
    callback: OperationCallbackFn,
    user_data: *mut std::ffi::c_void,
    prepare: Prepare,
) where
    Prepare: FnOnce() -> Result<Run, Error>,
    Run: FnOnce(ProducerStaticRef) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), Error>> + Send,
{
    // Null handling matches the sync txn functions (a null producer is an error),
    // reported synchronously through the callback as `flush_or_close_async` does for
    // its null case.
    if producer.is_null() {
        let error = box_error(Error::with_message(Errors::InvalidRequest, "producer handle must not be null"));
        // SAFETY: `callback` was supplied by the C caller along with `user_data` through
        // one of the five `_async` transaction-control entry points, whose docs report a
        // null `producer` through `callback`; `error` is a fresh `box_error` handle the
        // callee owns. The call runs inline on the calling thread and the function returns
        // right after — before the flag is taken or any task is spawned — so this is the
        // single invocation for this call.
        unsafe { callback(error, user_data) };
        return;
    }

    // SAFETY: `producer` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a producer constructor, i.e. one created by `build_producer_handle`
    // as `producer_handle` requires. This `handle` reference is used only on the calling
    // thread for the duration of this call (the CAS, cloning `completion_tx`, locking
    // `kind`, `reserve_pending_task`), while the spawned task and the
    // `TxnControlAsyncGuard` receive the address as a `usize` and derive their own
    // references under their own justifications.
    let handle = unsafe { producer_handle(producer) };

    // Reject an overlapping control call immediately, on the calling thread, with
    // the same message and fail-fast timing as the sync path — do NOT spawn.
    if handle
        .txn_control_busy
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .is_err()
    {
        let error = box_error(Error::local_concurrent_modification(
            "Transactional methods of KafkaProducer are not safe for concurrent access.",
        ));
        // SAFETY: `callback` was supplied by the C caller along with `user_data`; `error`
        // is a fresh `box_error` handle the callee owns, carrying the
        // concurrent-modification rejection the entry points document. The CAS failed, so
        // the flag was not taken and no task is spawned; the call runs inline on the
        // calling thread and the function returns right after, so this is the single
        // invocation for this call.
        unsafe { callback(error, user_data) };
        return;
    }

    // Flag is now held; every path from here must release it, so the guard that
    // releases it on drop is taken at once. Created here rather than inside the task,
    // it also covers a panic on this thread before the spawn — in `prepare`, or on a
    // poisoned `kind` lock — which unwinds through it and frees the flag before
    // `#[ffi_guard]` reports the panic through the callback. Otherwise the flag would
    // stay set and every later control call would be rejected as concurrent.
    let guard = TxnControlAsyncGuard { handle_ptr: producer as usize };

    // `prepare` runs on the calling thread (it marshals caller-owned C data that is
    // only valid for this synchronous call) after the CAS, mirroring the sync path's
    // order. A failure releases the flag and reports through the callback without
    // spawning.
    let run = match prepare() {
        Ok(run) => run,
        Err(e) => {
            drop(guard);
            let error = box_error(e);
            // SAFETY: `callback` was supplied by the C caller along with `user_data`;
            // `error` is a fresh `box_error` handle the callee owns, carrying the `prepare`
            // failure. `guard` was dropped just before, releasing `txn_control_busy`, and
            // no task has been spawned, so the call runs inline on the calling thread and
            // is the single invocation for this call.
            unsafe { callback(error, user_data) };
            return;
        },
    };

    let completion = handle.completion_tx.clone();
    let runtime = handle.kind.lock().unwrap().runtime().handle().clone();
    let ptr = producer as usize;
    let target = OperationCallbackTarget { callback, user_data };

    // Two-phase registration (see `reserve_pending_task`): nothing that can panic
    // runs after the spawn, so a caught panic never races this task's completion.
    let mut pending = reserve_pending_task(handle);
    let task = spawn_callback_task(&runtime, async move {
        let target = target;
        // Releases `txn_control_busy` on every exit of this task: the normal path
        // drops it explicitly before delivering completion (so the flag is free by
        // the time the caller observes the result, as it is on the sync path); a
        // panic in the drain or op unwinds through this binding and drops it too.
        let guard = guard;
        // SAFETY: the handle outlives this task — it is registered via
        // `reserve_pending_task` below and `destroy` joins it before dropping the
        // producer it borrows from.
        let h = unsafe { &*(ptr as *const ProducerHandle) };
        // Hand over records still queued by `send_async` before the op, exactly as
        // the sync path and `flush`/`close` do (see `drain_submitted_sends_await`).
        // Await it directly — we are already async — never `block_on` in a task.
        let result = match drain_submitted_sends_await(&h.queued_sends, &h.submit_tx).await {
            Err(e) => Err(e),
            // `producer_static_ref` takes the `kind` lock only to extend the
            // reference and drops it before returning, so no lock is held across the
            // op's `.await` (CLAUDE.md §11.6).
            // SAFETY: `ptr` is the same live leaked `*const ProducerHandle` as `h`, so
            // `producer_static_ref`'s `# Safety` (a live, not yet destroyed handle) holds
            // for the same reason: this task is registered via `reserve_pending_task` and
            // `destroy` joins it before dropping the producer. `producer_static_ref`
            // releases the `kind` lock before returning, so none is held while `run`'s
            // future is awaited, and the extended inner-producer reference is used only
            // within this task.
            Ok(()) => run(unsafe { producer_static_ref(ptr) }).await,
        };
        // Release the flag before delivering completion, so a caller that reacts to
        // the callback by issuing the next control op is never spuriously rejected.
        drop(guard);
        let error = match result {
            Ok(()) => std::ptr::null_mut(),
            Err(e) => box_error(e),
        };
        let op_completion = OperationCompletion { callback: target.callback, user_data: target.user_data, error };
        // SAFETY: `OperationCompletion::fire` requires exactly one call on the dispatcher
        // thread: `op_completion` is moved into this `FnOnce` job, which
        // `enqueue_or_run_inline` runs exactly once (on the dispatcher thread, or inline
        // once it has exited). `error` is a fresh `box_error` handle or null, built after
        // the last `.await` and after `guard` released the flag; the `callback`/`user_data`
        // pair is the one the C caller supplied, and this task is the single fire site for
        // a call that reached the spawn (every inline fire above returns before any task
        // exists).
        let job: CompletionJob = Box::new(move || unsafe { op_completion.fire() });
        enqueue_or_run_inline(&completion, job);
    });
    pending.push(task);
}

/// Asynchronously initializes the transactional state (the async counterpart of
/// [`kafka_producer_Producer_init_transactions`]).
///
/// Returns immediately; `callback` fires on the producer's dispatcher thread with a
/// null error on success or a non-null [`kafka_common_Error_t`] the caller
/// frees with `kafka_common_Error_destroy`. A timeout error is safe to retry.
///
/// Like all transaction-control functions this shares the one mutual-exclusion flag
/// with its siblings — synchronous and asynchronous alike — so an overlapping
/// control call is rejected with a `ConcurrentModification` error (message:
/// "Transactional methods of KafkaProducer are not safe for concurrent access."),
/// reported through `callback`. That is a caller-sequencing bug, not a transaction
/// failure: the transaction is untouched and must not be aborted in response.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (null is reported through `callback`).
/// - `callback`: Fired once on completion.
/// - `user_data`: Opaque pointer passed back to `callback`.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
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
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_init_transactions_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    // SAFETY: `with_txn_control_async` requires `producer` to be null or a valid handle
    // from a producer constructor, which is exactly this function's `# Safety` (`producer`
    // must be a valid handle, or null) and is upheld by the C caller; `producer`,
    // `callback` and `user_data` are forwarded unchanged, the trivial `prepare` cannot
    // fail, and the helper fires `callback` exactly once (inline on rejection, or through
    // the dispatcher on completion).
    unsafe {
        with_txn_control_async(producer, callback, user_data, || {
            Ok(|inner| async move {
                match inner {
                    ProducerStaticRef::Kafka(k) => k.init_transactions().await,
                    ProducerStaticRef::Mock(m) => m.init_transactions().await,
                }
            })
        });
    }
}

/// Asynchronously begins a new transaction (the async counterpart of
/// [`kafka_producer_Producer_begin_transaction`]).
///
/// Java's `beginTransaction()` is a pure state transition that never waits; this
/// still runs on the producer's runtime so it goes through the same
/// mutual-exclusion flag and submission-queue drain as the other control functions,
/// and delivers its result through `callback`.
///
/// Returns immediately; `callback` fires on the producer's dispatcher thread with a
/// null error on success or a non-null [`kafka_common_Error_t`] the caller
/// frees, including the `ConcurrentModification` rejection described on
/// [`kafka_producer_Producer_init_transactions_async`].
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (null is reported through `callback`).
/// - `callback`: Fired once on completion.
/// - `user_data`: Opaque pointer passed back to `callback`.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
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
pub unsafe extern "C" fn kafka_producer_Producer_begin_transaction_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_begin_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    // SAFETY: `with_txn_control_async` requires `producer` to be null or a valid handle
    // from a producer constructor, which is exactly this function's `# Safety` (`producer`
    // must be a valid handle, or null) and is upheld by the C caller; `producer`,
    // `callback` and `user_data` are forwarded unchanged, the trivial `prepare` cannot
    // fail, and the helper fires `callback` exactly once (inline on rejection, or through
    // the dispatcher on completion).
    unsafe {
        with_txn_control_async(producer, callback, user_data, || {
            Ok(|inner| async move {
                match inner {
                    ProducerStaticRef::Kafka(k) => k.begin_transaction(),
                    ProducerStaticRef::Mock(m) => m.begin_transaction(),
                }
            })
        });
    }
}

/// Asynchronously sends consumer-group offsets to the group coordinator as part of
/// the ongoing transaction (the async counterpart of
/// [`kafka_producer_Producer_send_offsets_to_transaction`]).
///
/// The offsets are only considered committed if the transaction itself commits.
/// Returns immediately; `callback` fires on the producer's dispatcher thread with a
/// null error on success or a non-null [`kafka_common_Error_t`] the caller
/// frees. If `kafka_common_Error_is_transaction_abortable_error` is true for that error the
/// transaction must be aborted. The `ConcurrentModification` rejection described on
/// [`kafka_producer_Producer_init_transactions_async`] also applies, and is **not** a
/// reason to abort.
///
/// The parallel arrays and `group_metadata` are marshaled **on the calling thread**,
/// before this function returns, so the caller may free them as soon as it does —
/// exactly as it may for the synchronous
/// [`kafka_producer_Producer_send_offsets_to_transaction`].
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (null is reported through `callback`).
/// - `topics`, `partitions`, `offsets`, `leader_epochs`, `metadata`: parallel arrays
///   of `count` entries, marshaled exactly as for the synchronous function —
///   `metadata` may be null (or hold null entries) and a `leader_epoch < 0` means no
///   epoch. Each offset is the offset of the **next** record to consume. A repeated
///   `(topic, partition)` keeps the last entry.
/// - `count`: number of entries, which must be `>= 0`. Zero stages nothing; whether
///   it also succeeds is backend-specific, exactly as documented on the synchronous
///   counterpart.
/// - `group_metadata`: non-null `kafka_consumer_ConsumerGroupMetadata_t`, borrowed
///   (the caller still owns and destroys it).
/// - `callback`: Fired once on completion.
/// - `user_data`: Opaque pointer passed back to `callback`.
///
/// # Panics
///
/// Panics if `count` is negative — a violated precondition, matching the synchronous
/// function. Clamping instead would stage no offsets yet report success, silently
/// breaking exactly-once in a consume-transform-produce loop.
///
/// # Safety
///
/// - `producer` must be a valid handle, or null.
/// - When `count > 0`, `topics`, `partitions` and `offsets` must each point to
///   `count` valid entries and are **not** null-checked (a violated precondition,
///   per CLAUDE.md FFI §4), exactly as for the synchronous function. `leader_epochs`
///   and `metadata` may be null; `count == 0` reads none of the arrays.
/// - `group_metadata` must be a valid group-metadata handle, or null (null is
///   reported through `callback`).
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
pub unsafe extern "C" fn kafka_producer_Producer_send_offsets_to_transaction_async(
    producer: *mut kafka_producer_Producer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    callback: kafka_producer_Producer_send_offsets_to_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    // SAFETY: `send_offsets_to_transaction_async_inner` has the same `# Safety`
    // requirements as this function, which the C caller upholds; the arguments are
    // forwarded unchanged.
    unsafe {
        send_offsets_to_transaction_async_inner(
            producer,
            topics,
            partitions,
            offsets,
            leader_epochs,
            metadata,
            count,
            group_metadata,
            callback,
            user_data,
        );
    }
}

/// Inner implementation of
/// [`kafka_producer_Producer_send_offsets_to_transaction_async`].
///
/// Separated from the `extern "C"` wrapper, whose `#[ffi_guard]` reports a panic
/// here through `callback`.
///
/// # Panics
///
/// Panics if `count` is negative: it must fail rather than be clamped.
///
/// # Safety
///
/// Same requirements as [`kafka_producer_Producer_send_offsets_to_transaction_async`].
#[expect(clippy::too_many_arguments)]
unsafe fn send_offsets_to_transaction_async_inner(
    producer: *mut kafka_producer_Producer_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    callback: kafka_producer_Producer_send_offsets_to_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    // Pure argument preconditions are checked on the calling thread before the guard
    // is taken, matching the sync path: a malformed call costs nothing and is never
    // reported as a concurrency rejection.
    // A negative count must fail rather than be clamped; the entry point's
    // `#[ffi_guard]` reports this panic as the call's failure.
    assert!(count >= 0, "count must not be negative");
    if group_metadata.is_null() {
        let error = box_error(Error::local_illegal_argument(
            "group_metadata must not be null; pass the handle from kafka_consumer_Consumer_group_metadata",
        ));
        // SAFETY: `callback` was supplied by the C caller together with `user_data` and,
        // per this function's callback contract, fires exactly once; on this
        // null-`group_metadata` path it fires inline on the calling thread before
        // `with_txn_control_async` is entered and the function returns immediately
        // afterwards, so no task is spawned and no second firing can follow. `error` is a
        // fresh `box_error` handle whose ownership passes to the callee.
        unsafe { callback(error, user_data) };
        return;
    }
    // SAFETY: `with_txn_control_async` requires `producer` to be null or a valid handle
    // from a producer constructor, which this function's `# Safety` promises. The `prepare`
    // closure runs synchronously on the calling thread (after the CAS, before any spawn),
    // so `read_offset_map` reads `topics`, `partitions` and `offsets` while the caller's
    // arrays are still valid: per `# Safety` they hold `count` valid entries whenever
    // `count > 0`, `count` was asserted non-negative above, `count == 0` reads nothing, and
    // `leader_epochs`/`metadata` are null-checked by the helper. `group_metadata_ref`
    // requires a valid group-metadata handle: `group_metadata` is non-null (checked above)
    // and valid per `# Safety`, and only an `Arc` clone of its metadata is moved into the
    // spawned op, so the caller may destroy the handle as soon as this returns. The op
    // reaches the producer solely through `producer_static_ref`, inside a task registered
    // via `reserve_pending_task` that `destroy` joins before dropping the handle, and
    // `callback`/`user_data` are delivered once through the dispatcher.
    unsafe {
        with_txn_control_async(producer, callback, user_data, move || {
            // Marshal the caller-owned C arrays and clone the borrowed group metadata
            // on the CALLING thread, after the CAS (so it is inside the guarded
            // window) but before the spawn — the arrays are only valid for the
            // duration of this synchronous call. This mirrors the sync
            // `send_offsets_to_transaction_inner` order (CAS → marshal → op). A
            // marshaling failure is the early return the flag must survive, handled
            // by `with_txn_control_async`.
            let offsets_map = read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count)?;
            // The caller may destroy the handle as soon as this returns, so the
            // spawned op holds its own reference to the metadata: an `Arc` clone
            // of the one inside the handle. The owned map + metadata are then
            // moved into the spawned op.
            let group = Arc::clone(group_metadata_ref(group_metadata));
            Ok(move |inner| async move {
                match inner {
                    ProducerStaticRef::Kafka(k) => k.send_offsets_to_transaction(offsets_map, &*group).await,
                    ProducerStaticRef::Mock(m) => m.send_offsets_to_transaction(offsets_map, &*group).await,
                }
            })
        });
    }
}

/// Asynchronously commits the ongoing transaction (the async counterpart of
/// [`kafka_producer_Producer_commit_transaction`]).
///
/// It flushes any unsent records first, so every `send` in the transaction must have
/// succeeded for the commit to succeed. Records queued with the async
/// `kafka_producer_Producer_send_async` / `kafka_producer_Producer_send_batch_async`
/// that had *returned* before this call are drained into the transaction first (as
/// `flush`/`close` do); sends racing concurrently on another thread are not
/// included. See `.claude/rules/producer-transactions.md` §13.
///
/// Returns immediately; `callback` fires on the producer's dispatcher thread with a
/// null error on success or a non-null [`kafka_common_Error_t`] the caller
/// frees. If `kafka_common_Error_is_transaction_abortable_error` is true for that error,
/// abort the transaction. A timeout error, however, does **not** say whether the
/// commit reached the broker: it is safe to retry the commit, but not to abort
/// instead — the only other option is to close the producer. The
/// `ConcurrentModification` rejection described on
/// [`kafka_producer_Producer_init_transactions_async`] also applies, and is **not** a
/// reason to abort.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (null is reported through `callback`).
/// - `callback`: Fired once on completion.
/// - `user_data`: Opaque pointer passed back to `callback`.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
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
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_commit_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    // SAFETY: `with_txn_control_async` requires `producer` to be null or a valid handle
    // from a producer constructor, which this function's `# Safety` promises. The trivial
    // `prepare` touches no caller memory; the op reaches the producer only through
    // `producer_static_ref`, inside a task registered via `reserve_pending_task` that
    // `destroy` joins before dropping the handle, and `callback`/`user_data` were supplied
    // together by the C caller and are delivered once through the dispatcher.
    unsafe {
        with_txn_control_async(producer, callback, user_data, || {
            Ok(|inner| async move {
                match inner {
                    ProducerStaticRef::Kafka(k) => k.commit_transaction().await,
                    ProducerStaticRef::Mock(m) => m.commit_transaction().await,
                }
            })
        });
    }
}

/// Asynchronously aborts the ongoing transaction (the async counterpart of
/// [`kafka_producer_Producer_abort_transaction`]).
///
/// Any unflushed records — synchronous and already-returned async alike — are
/// discarded, the same treatment Java gives accumulator records on abort. This call
/// drains the submission queue first (as `flush`/`close` do), so every async send
/// that had *returned* before the abort is handed to the producer and then discarded
/// as part of the aborted transaction rather than leaking out after it; sends racing
/// concurrently on another thread are not ordered against it. Abort is the recovery
/// operation and stays available even when
/// [`kafka_producer_Producer_commit_transaction_async`] cannot make progress. See
/// `.claude/rules/producer-transactions.md` §13.
///
/// Returns immediately; `callback` fires on the producer's dispatcher thread with a
/// null error on success or a non-null [`kafka_common_Error_t`] the caller
/// frees. As for commit, a timeout error is safe to retry but does not permit
/// switching to a different operation, and the `ConcurrentModification` rejection
/// described on [`kafka_producer_Producer_init_transactions_async`] is **not** a
/// reason to retry with a different operation.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (null is reported through `callback`).
/// - `callback`: Fired once on completion.
/// - `user_data`: Opaque pointer passed back to `callback`.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
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
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_abort_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    // SAFETY: `with_txn_control_async` requires `producer` to be null or a valid handle
    // from a producer constructor, which this function's `# Safety` promises. The trivial
    // `prepare` touches no caller memory; the op reaches the producer only through
    // `producer_static_ref`, inside a task registered via `reserve_pending_task` that
    // `destroy` joins before dropping the handle, and `callback`/`user_data` were supplied
    // together by the C caller and are delivered once through the dispatcher.
    unsafe {
        with_txn_control_async(producer, callback, user_data, || {
            Ok(|inner| async move {
                match inner {
                    ProducerStaticRef::Kafka(k) => k.abort_transaction().await,
                    ProducerStaticRef::Mock(m) => m.abort_transaction().await,
                }
            })
        });
    }
}

// ---------------------------------------------------------------------------
// Mock-specific operations
// ---------------------------------------------------------------------------

/// Completes the next pending send successfully.
///
/// Only valid for mock producers. Returns `false` if there are no pending
/// completions or if the producer is null or not a mock.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_complete_next(producer: *mut kafka_producer_Producer_t) -> bool {
    if producer.is_null() {
        return false;
    }

    // SAFETY: `producer_ref` requires a non-null handle created by a producer constructor:
    // `producer` is non-null (checked above) and, per this function's `# Safety`, a valid
    // handle. The returned `&'static Mutex` is used only for the duration of this
    // synchronous call, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock, _) => mock.complete_next(),
        ProducerKind::Kafka(..) => false,
    }
}

/// Completes the next pending send with an error.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (must be a mock producer).
/// - `error_code`: Kafka error code (e.g., `2` for `CorruptMessage`).
/// - `error_message`: Optional null-terminated error message, or null to use
///   the default message for the error code.
///
/// # Returns
///
/// `true` if there was a pending completion, `false` otherwise.
///
/// # Safety
///
/// - `producer` must be a valid handle.
/// - `error_message` must be a valid C string or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_error_next(
    producer: *mut kafka_producer_Producer_t,
    error_code: i32,
    error_message: *const c_char,
) -> bool {
    if producer.is_null() {
        return false;
    }

    // SAFETY: `mock_error` requires `error_message` to be a valid C string or null, which
    // is exactly what this function's `# Safety` promises for `error_message`; the text is
    // copied into an owned `Error` before this call returns.
    let error = unsafe { mock_error(Errors::for_code(error_code as i16), error_message) };

    // SAFETY: `producer_ref` requires a non-null handle created by a producer constructor:
    // `producer` is non-null (checked above) and, per this function's `# Safety`, a valid
    // handle. The returned `&'static Mutex` is used only for the duration of this
    // synchronous call, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock, _) => mock.error_next(error),
        ProducerKind::Kafka(..) => false,
    }
}

/// Builds the [`Error`] a mock driver hook installs: `error_message` when
/// non-null, otherwise the default message for `error`.
///
/// # Safety
///
/// `error_message` must be a valid C string, or null.
unsafe fn mock_error(error: Errors, error_message: *const c_char) -> Error {
    if error_message.is_null() {
        Error::new(error)
    } else {
        // SAFETY: `error_message` is non-null here (the `else` branch of the null check)
        // and, per `mock_error`'s `# Safety`, a valid NUL-terminated C string; it is only
        // read, and `to_string_lossy` copies it out before the function returns.
        let msg = unsafe { CStr::from_ptr(error_message) }.to_string_lossy();
        Error::with_message(error, msg.as_ref())
    }
}

/// Returns the number of records in the sent history.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (must be a mock producer).
///
/// # Returns
///
/// The number of sent records, or `0` if the producer is null or not a mock.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_history_count(producer: *const kafka_producer_Producer_t) -> i32 {
    if producer.is_null() {
        return 0;
    }

    // SAFETY: This is `producer_handle`'s cast spelled out because the parameter is
    // `*const`: `producer` is non-null (checked above) and, per this function's `# Safety`,
    // a valid handle, i.e. a `ProducerHandle` leaked by `build_producer_handle`. The
    // reference is used only to lock `kind` for the duration of this synchronous call,
    // during which the C caller keeps the handle alive.
    let producer_mtx = &unsafe { &*(producer as *const ProducerHandle) }.kind;
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock, _) => {
            let count = mock.history().len();
            // Clamp to i32::MAX to avoid overflow (extremely unlikely in practice).
            count.min(i32::MAX as usize) as i32
        },
        ProducerKind::Kafka(..) => 0,
    }
}

/// Installs (or clears) the error every subsequent
/// [`kafka_producer_Producer_commit_transaction`] returns on a mock producer.
///
/// Mirrors Java's public `MockProducer.commitTransactionException` field, and is
/// the mock driver hook for the abortable-commit path: install
/// `TransactionAbortable` (code 120) and the commit fails with an error for which
/// `kafka_common_Error_is_transaction_abortable_error` is true, leaving the
/// transaction open so the test can then abort it. Only
/// `commitTransactionException` is exposed — the sibling init / begin /
/// send-offsets / abort hooks have no C caller.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (must be a mock producer).
/// - `clear`: When `true`, removes any installed error and `error_code` /
///   `error_message` are ignored. Clearing is signalled out of band rather than
///   by a reserved `error_code`, because every `i16` is a legitimate code —
///   `-1` is `UnknownServerError`.
/// - `error_code`: Kafka error code to install when `clear` is `false`. Must be
///   non-zero (`0` is `Errors::None`, which would install an error handle that
///   reports success) and must fit in an `i16`; anything else is rejected rather
///   than truncated. A code that is in range but unassigned resolves to
///   `UnknownServerError`, as in [`kafka_producer_MockProducer_error_next`].
///   Note the deliberate divergence from that neighbour: `error_next` predates
///   this function and casts out-of-range codes with `as i16`, silently
///   truncating them. Its behaviour is left as it is (changing a shipped API is
///   not in scope here); new surface rejects instead.
/// - `error_message`: Optional null-terminated message, or null to use the
///   default message for the error code.
///
/// # Returns
///
/// `true` if the hook was applied. `false` if the producer is null, is not a
/// mock, or (with `clear` false) `error_code` is zero or outside `i16` range.
///
/// # Safety
///
/// - `producer` must be a valid handle, or null.
/// - `error_message` must be a valid C string or null.
/// - This is a **setup-only** call: it must not overlap a transaction-control
///   call on the same producer. It writes the mock's installed-error field
///   without taking `txn_control_busy`, so racing it against
///   [`kafka_producer_Producer_commit_transaction`] leaves it undefined whether
///   that commit observes the new value. (The mock's own lock keeps this a
///   logical race, not undefined behaviour.) Call it before the control calls it
///   is meant to affect.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_commit_transaction_error(
    producer: *mut kafka_producer_Producer_t,
    clear: bool,
    error_code: i32,
    error_message: *const c_char,
) -> bool {
    if producer.is_null() {
        return false;
    }

    // SAFETY: `producer_ref` requires a non-null handle created by a producer constructor:
    // `producer` is non-null (checked above) and, per this function's `# Safety`, a valid
    // handle. The returned `&'static Mutex` is used only for the duration of this
    // synchronous call, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        // Establish that this is a mock before validating the code or building
        // the error, so a misdirected call on a real producer does no work.
        ProducerKind::Mock(mock, _) => {
            let error = if clear {
                None
            } else {
                // Reject rather than truncate or resolve to a surprising code.
                let Ok(code) = i16::try_from(error_code) else {
                    return false;
                };
                if code == 0 {
                    return false;
                }
                // SAFETY: `mock_error` requires `error_message` to be a valid C string or
                // null, which this function's `# Safety` promises; the branch is reached
                // only with `clear == false` and an in-range, non-zero code, and the text
                // is copied into an owned `Error` before the mock stores it.
                Some(unsafe { mock_error(Errors::for_code(code), error_message) })
            };
            mock.set_commit_transaction_error(error);
            true
        },
        ProducerKind::Kafka(..) => false,
    }
}

/// Whether the mock has staged consumer-group offsets in the current
/// transaction (mock only).
///
/// Mirrors Java's `MockProducer.sentOffsets()`. `false` if the producer is null
/// or not a mock.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_sent_offsets(producer: *mut kafka_producer_Producer_t) -> bool {
    if producer.is_null() {
        return false;
    }
    // SAFETY: `producer_ref` requires a non-null handle created by a producer constructor:
    // `producer` is non-null (checked above) and, per this function's `# Safety`, a valid
    // handle. The returned `&'static Mutex` is used only for the duration of this
    // synchronous call, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock, _) => mock.sent_offsets(),
        ProducerKind::Kafka(..) => false,
    }
}

/// Looks up an offset a committed transaction staged for `(group_id, topic,
/// partition)` on a mock producer, so a test can verify the round-trip through
/// `kafka_producer_Producer_send_offsets_to_transaction`.
///
/// Searches Java's `MockProducer.consumerGroupOffsetsHistory()`, latest
/// transaction first, and reports the newest entry for that key.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (must be a mock producer).
/// - `group_id`, `topic`: the key to look up. Either may be null, which matches
///   nothing and returns `false` — a caller probing "is anything recorded for X"
///   should not have to synthesise a dummy string.
/// - `partition`: the partition to look up.
/// - `out_offset`: receives the committed offset. May be null.
/// - `out_leader_epoch`: receives the leader epoch, or `-1` if the entry has
///   none. May be null.
/// - `out_metadata`: receives the commit metadata as a NUL-terminated string. May
///   be null, and is left untouched when `metadata_cap <= 0`. If the metadata does
///   not fit it is truncated at a UTF-8 character boundary, never mid-sequence, so
///   the result is always a valid string — but truncation is not reported (the
///   `bool` return means "found", not "fit"), so size the buffer generously.
/// - `metadata_cap`: size of the `out_metadata` buffer in bytes.
///
/// # Returns
///
/// `true` if an entry was found, `false` if not, or if the producer is null or
/// not a mock.
///
/// # Safety
///
/// - `producer` must be a valid handle, or null.
/// - `group_id` and `topic` must each be a valid C string, or null.
/// - `out_offset` / `out_leader_epoch` must be null or writable.
/// - `out_metadata` must be null or writable for `metadata_cap` bytes.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_committed_offset(
    producer: *mut kafka_producer_Producer_t,
    group_id: *const c_char,
    topic: *const c_char,
    partition: i32,
    out_offset: *mut i64,
    out_leader_epoch: *mut i32,
    out_metadata: *mut c_char,
    metadata_cap: i32,
) -> bool {
    if producer.is_null() || group_id.is_null() || topic.is_null() {
        return false;
    }
    // SAFETY: `group_id` is non-null (checked above together with `producer` and `topic`)
    // and, per this function's `# Safety`, a valid C string; it is copied into an owned
    // `String` before any other work.
    let group = unsafe { CStr::from_ptr(group_id) }.to_string_lossy().to_string();
    // SAFETY: `topic` is non-null (checked above together with `producer` and `group_id`)
    // and, per this function's `# Safety`, a valid C string; it is copied into an owned
    // `String` before any other work.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);

    // SAFETY: `producer_ref` requires a non-null handle created by a producer constructor:
    // `producer` is non-null (checked above) and, per this function's `# Safety`, a valid
    // handle. The returned `&'static Mutex` is used only for the duration of this
    // synchronous call, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    // `committed_offset` scans under the mock's own lock and clones just the one
    // entry, rather than deep-cloning the whole offsets history per lookup.
    let found = match &*guard {
        ProducerKind::Mock(mock, _) => mock.committed_offset(&group, &tp),
        ProducerKind::Kafka(..) => return false,
    };
    let Some(found) = found else {
        return false;
    };
    if !out_offset.is_null() {
        // SAFETY: `out_offset` is non-null (checked above) and, per this function's `#
        // Safety`, writable; exactly one `i64` is written.
        unsafe { *out_offset = found.offset() };
    }
    if !out_leader_epoch.is_null() {
        // SAFETY: `out_leader_epoch` is non-null (checked above) and, per this function's
        // `# Safety`, writable; exactly one `i32` is written.
        unsafe { *out_leader_epoch = found.leader_epoch().unwrap_or(-1) };
    }
    if !out_metadata.is_null() && metadata_cap > 0 {
        let metadata = found.metadata();
        let room = (metadata_cap as usize) - 1;
        // Cut on a character boundary so a truncated multi-byte sequence never
        // reaches C. The metadata arrives through `to_string_lossy`, so non-ASCII
        // is expected rather than exotic.
        let n = if metadata.len() <= room {
            metadata.len()
        } else {
            metadata
                .char_indices()
                .map(|(i, _)| i)
                .take_while(|&i| i <= room)
                .last()
                .unwrap_or(0)
        };
        // SAFETY: `out_metadata` is non-null and `metadata_cap > 0` (both checked above),
        // and per this function's `# Safety` the buffer is writable for `metadata_cap`
        // bytes. By construction `n <= room == metadata_cap - 1`: either the whole string
        // fits, or `n` is the last char boundary `<= room` (the `char_indices` chain always
        // yields index 0 for a non-empty string), so the `n` copied bytes plus the
        // terminating NUL at index `n` stay inside the buffer. The source is the `&str`
        // owned by `found`, a Rust-side clone of the mock's entry, so it cannot overlap the
        // C buffer, which makes `copy_nonoverlapping` applicable.
        unsafe {
            std::ptr::copy_nonoverlapping(metadata.as_ptr(), out_metadata as *mut u8, n);
            *out_metadata.add(n) = 0;
        }
    }
    true
}

/// Clears the sent history and pending completions.
///
/// # Safety
///
/// `producer` must be a valid handle, or null (no-op).
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_clear(producer: *mut kafka_producer_Producer_t) {
    if producer.is_null() {
        return;
    }

    // SAFETY: `producer_ref` requires a non-null handle created by a producer constructor:
    // `producer` is non-null (checked above) and, per this function's `# Safety`, a valid
    // handle. The returned `&'static Mutex` is used only for the duration of this
    // synchronous call, during which the C caller keeps the handle alive.
    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock, _) => mock.clear(),
        ProducerKind::Kafka(..) => {},
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: asserts that a `*mut kafka_common_Error_t` is null (success) and returns nothing.
    /// Panics with the error message if non-null.
    ///
    /// # Safety
    ///
    /// `err` must be null or a live error handle returned by an FFI call; a non-null
    /// handle is freed here and must not be used afterwards.
    unsafe fn assert_success(err: *mut kafka_common_Error_t) {
        if !err.is_null() {
            // SAFETY: `err` is non-null (checked above) and, by this helper's contract, an
            // owned error handle an FFI call returned to the test;
            // `kafka_common_Error_message` returns a pointer valid until the handle is
            // destroyed, and `to_string_lossy` copies the text before
            // `kafka_common_Error_destroy` consumes `err`.
            let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(err)) }.to_string_lossy();
            // SAFETY: `err` is non-null (checked above) and an owned error handle an FFI
            // call returned to the test; this is its single destroy, after the message was
            // copied out, and the helper then panics so nothing uses `err` afterwards.
            unsafe { kafka_common_Error_destroy(err) };
            panic!("Expected success but got error: {msg}");
        }
    }

    /// Helper: asserts that a `*mut kafka_common_Error_t` is non-null (failure), destroys it,
    /// and returns the error code.
    ///
    /// # Safety
    ///
    /// `err` must be a live error handle returned by an FFI call (null fails the
    /// assertion); it is freed here and must not be used afterwards.
    unsafe fn assert_error(err: *mut kafka_common_Error_t) -> kafka_common_ErrorCode_t {
        assert!(!err.is_null(), "Expected an error but got success");
        // SAFETY: `err` is non-null (asserted above) and, by this helper's contract, an
        // owned error handle an FFI call returned to the test; `kafka_common_Error_code`
        // only reads it.
        let code = unsafe { kafka_common_Error_code(err) };
        // SAFETY: `err` is the owned error handle just read; this is its single destroy,
        // and only the copied `code` is returned.
        unsafe { kafka_common_Error_destroy(err) };
        code
    }

    // -- Lifecycle tests ----------------------------------------------------

    #[test]
    fn test_create_and_destroy_mock_producer() {
        let producer = kafka_producer_MockProducer_new(true);
        assert!(!producer.is_null());
        // SAFETY: `producer` is the handle `kafka_producer_MockProducer_new` returned
        // (asserted non-null); this is its single `kafka_producer_Producer_destroy` and
        // nothing uses it afterwards.
        unsafe {
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_destroy_null_is_noop() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_producer_Producer_destroy` is specified as a no-op for a null handle.
        unsafe {
            kafka_producer_Producer_destroy(std::ptr::null_mut());
        }
    }

    // -- Send tests ---------------------------------------------------------

    #[test]
    fn test_send_auto_complete() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("test-topic").unwrap();
        let key = b"key";
        let value = b"value";

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `topic` is an owned `CString` and `key`/`value` are static byte literals passed
        // with their exact lengths, all outliving the call, so
        // `kafka_producer_Producer_send`'s `# Safety` holds, and `&mut err` is a writable
        // local. `assert_success` consumes `err` if non-null, `is_done` gets the non-null
        // future `send` returned, and the future and the producer are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
                &mut err,
            );
            assert_success(err);
            assert!(!future.is_null());
            assert!(kafka_common_KafkaFuture_RecordMetadata_is_done(future));

            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_manual_complete() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("test-topic").unwrap();

        // SAFETY: `producer` is the live manual-completion handle
        // `kafka_producer_MockProducer_new(false)` returned and `topic` an owned `CString`
        // alive for the call; null `key`/`value` with length `-1` are the documented
        // no-key/no-value form, and `&mut err` is a writable local. `is_done` and
        // `complete_next` get the live future/producer, and the future and the producer are
        // each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_success(err);
            assert!(!future.is_null());
            assert!(!kafka_common_KafkaFuture_RecordMetadata_is_done(future));

            // Complete it
            assert!(kafka_producer_MockProducer_complete_next(producer));
            assert!(kafka_common_KafkaFuture_RecordMetadata_is_done(future));

            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_null_producer() {
        let topic = CString::new("topic").unwrap();

        // SAFETY: Null `producer` is passed deliberately to exercise the documented failure
        // path: `kafka_producer_Producer_send` null-checks `producer` and reports
        // `InvalidRequest` through `out_error` instead of dereferencing it. `topic` is an
        // owned `CString` alive for the call, `&mut err` a writable local, `assert_error`
        // destroys the returned error once, and no future handle is created.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                std::ptr::null_mut(),
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_error(err);
            assert!(future.is_null(), "future should be null when producer is null");
        }
    }

    #[test]
    fn test_send_null_topic() {
        let producer = kafka_producer_MockProducer_new(true);

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // null `topic` is passed deliberately to exercise the documented failure path
        // (`kafka_producer_Producer_send` null-checks `topic` and reports `InvalidRequest`
        // through `out_error`), `&mut err` is a writable local, `assert_error` destroys
        // that error once, no future is created, and the producer is destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                std::ptr::null(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_error(err);
            assert!(future.is_null(), "future should be null when topic is null");
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_with_partition() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `topic` an owned `CString` alive for the call; `&mut err` are writable
        // locals. `kafka_common_KafkaFuture_RecordMetadata_get` requires a valid future or
        // null and gets the future `send` returned (success asserted), and
        // `RecordMetadata_partition` gets the non-null metadata it returned. Metadata,
        // future and producer are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                3,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_success(err);

            // Get metadata and verify partition
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let metadata = kafka_common_KafkaFuture_RecordMetadata_get(future, &mut err);
            assert_success(err);
            assert!(!metadata.is_null());
            assert_eq!(kafka_producer_RecordMetadata_partition(metadata), 3);

            kafka_producer_RecordMetadata_destroy(metadata);
            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    // -- Batch send tests ---------------------------------------------------

    #[test]
    fn test_send_batch() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic1 = CString::new("topic1").unwrap();
        let topic2 = CString::new("topic2").unwrap();
        let key = b"key";
        let value = b"value";

        let records = [
            kafka_producer_ProducerRecord_t {
                topic: topic1.as_ptr(),
                partition: -1,
                timestamp: -1,
                key: key.as_ptr(),
                key_len: key.len() as i32,
                value: value.as_ptr(),
                value_len: value.len() as i32,
            },
            kafka_producer_ProducerRecord_t {
                topic: topic2.as_ptr(),
                partition: 1,
                timestamp: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            },
        ];

        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 2] =
            [std::ptr::null_mut(), std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 2] = [std::ptr::null_mut(), std::ptr::null_mut()];

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `records` is a local array of 2 whose `topic1`/`topic2` `CString`s and static
        // `key`/`value` literals outlive the call, and `futures`/`errors` are 2-slot
        // locals, matching `count == 2` as `kafka_producer_Producer_send_batch`'s `#
        // Safety` requires. `is_done` and `history_count` get live handles, each returned
        // future is destroyed exactly once in the loop (the errors are asserted null), and
        // the producer is destroyed once.
        unsafe {
            let sent = kafka_producer_Producer_send_batch(
                producer,
                records.as_ptr(),
                2,
                futures.as_mut_ptr(),
                errors.as_mut_ptr(),
            );
            assert_eq!(sent, 2);

            for i in 0..2 {
                assert!(errors[i].is_null(), "errors[{i}] should be null on success");
                assert!(!futures[i].is_null());
                assert!(kafka_common_KafkaFuture_RecordMetadata_is_done(futures[i]));
            }

            // Check history count
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 2);

            for f in &futures {
                kafka_common_KafkaFuture_RecordMetadata_destroy(*f);
            }
            kafka_producer_Producer_destroy(producer);
        }
    }

    /// The delivery report for one record must fire **exactly once**, even when
    /// `producer.send` both moves the callback into a batch (which fires it later)
    /// and returns `Err` (so the submission task fires it too). That is the
    /// double-free the shared at-most-once guard prevents: without it the C
    /// callback — and the free of `user_data` — would run twice.
    ///
    /// This tests the FFI mechanism directly rather than through a real
    /// transactional `KafkaProducer`, because the post-append `Err` path needs
    /// cached partition metadata (so `send` reaches `maybe_add_partition` past
    /// `wait_on_metadata`), which a broker-less test cannot arrange. The two
    /// callbacks built here — sharing one `fired` — are exactly the batch's copy
    /// and the submission task's error re-fire.
    #[test]
    fn test_record_callback_fires_exactly_once_across_shared_guard() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

        /// # Safety
        ///
        /// Called by the library through `kafka_producer_Producer_send_callback_t`:
        /// `metadata` and `error` must be null or owned handles (both are destroyed here);
        /// `_user_data` is ignored.
        unsafe extern "C" fn counting_cb(
            metadata: *mut kafka_producer_RecordMetadata_t,
            error: *mut kafka_common_Error_t,
            _user_data: *mut std::ffi::c_void,
        ) {
            INVOCATIONS.fetch_add(1, Ordering::SeqCst);
            // Free whichever owned handle we were given, as a real C caller must.
            if !metadata.is_null() {
                // SAFETY: `metadata` is non-null (checked above) and, per
                // `kafka_producer_Producer_send_callback_t`'s contract, a freshly built
                // handle the callee owns, handed over by the dispatcher through
                // `make_record_callback`; this is its single destroy.
                unsafe { kafka_producer_RecordMetadata_destroy(metadata) };
            }
            if !error.is_null() {
                // SAFETY: `error` is non-null (checked above) and, per
                // `kafka_producer_Producer_send_callback_t`'s contract, a freshly built
                // error handle the callee owns, handed over by the dispatcher through
                // `make_record_callback`; this is its single destroy.
                unsafe { kafka_common_Error_destroy(error) };
            }
        }

        INVOCATIONS.store(0, Ordering::SeqCst);
        let (tx, dispatcher) = common::spawn_dispatcher("test-exactly-once");
        let target = RecordCallbackTarget { callback: counting_cb, user_data: std::ptr::null_mut() };
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Two callbacks for the same record, sharing the guard.
        let batch_copy = make_record_callback(target, tx.clone(), std::sync::Arc::clone(&fired));
        let task_refire = make_record_callback(target, tx.clone(), std::sync::Arc::clone(&fired));

        // Fire order does not matter — model the task re-firing on `Err` first,
        // then the batch firing later on abort/close.
        task_refire(None, Some(&Error::local_illegal_state("no open transaction")));
        batch_copy(None, Some(&Error::transaction_aborted()));

        // Drain the dispatcher and count.
        drop(tx);
        dispatcher.join().expect("dispatcher thread joins");
        assert_eq!(
            INVOCATIONS.load(Ordering::SeqCst),
            1,
            "the record's C delivery callback must fire exactly once (no double-free)"
        );
    }

    /// A negative `count` must fail rather than be clamped. Clamping would give an
    /// empty offsets map, which `KafkaProducer` reports as success without staging
    /// anything (it short-circuits before consulting transaction state), so the
    /// transaction would commit with no offsets staged and no error surfaced —
    /// silently breaking exactly-once. Mirrors `send_batch`'s own count assert. The
    /// assert panics and `#[ffi_guard]` turns that into the returned error, so the
    /// real `extern "C"` entry point is called and the test process keeps running.
    #[test]
    fn test_send_offsets_to_transaction_negative_count_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        // `group_metadata` is null on purpose: the count assert must fire before
        // anything else is looked at, so no valid handle is needed to reach it.
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned.
        // `count == -1` is passed deliberately so `send_offsets_to_transaction_inner`'s
        // count assert panics before any array or `group_metadata` is read, which is why
        // the null arrays are permitted (the `# Safety` only requires them for `count > 0`)
        // and the null `group_metadata` is allowed by `# Safety`; `#[ffi_guard]` turns the
        // panic into the returned error handle the test then owns.
        let error = unsafe {
            kafka_producer_Producer_send_offsets_to_transaction(
                producer,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                -1,
                std::ptr::null(),
            )
        };
        assert!(!error.is_null(), "a negative count must return an error, not success");
        assert_eq!(
            // SAFETY: `error` is non-null (asserted above) and the error handle
            // `kafka_producer_Producer_send_offsets_to_transaction` returned to the test;
            // `kafka_common_Error_code` only reads it.
            unsafe { kafka_common_Error_code(error) },
            kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE
        );
        // SAFETY: `take_error_message` requires a non-null owned error handle (it asserts
        // that itself): `error` is the handle returned above, read once by
        // `kafka_common_Error_code`, and this is its single destroy.
        let msg = unsafe { take_error_message(error) };
        assert!(
            msg.starts_with(
                "Rust panic caught at the FFI boundary in kafka_producer_Producer_send_offsets_to_transaction:"
            ),
            "unexpected error message: {msg}"
        );
        assert!(msg.contains("count must not be negative"), "unexpected error message: {msg}");
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // the failed call borrowed nothing and spawned nothing (its count assert fired
        // before `with_txn_control`), so this single `kafka_producer_Producer_destroy` is
        // the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// The async `send_offsets_to_transaction` mirror of the count assert: a negative
    /// count must fail rather than be clamped, matching
    /// `test_send_offsets_to_transaction_negative_count_returns_error`. The real
    /// `extern "C"` entry point is called; its `#[ffi_guard]` reports the assert's
    /// panic through the callback, exactly once and before returning. The assert
    /// fires before anything else is looked at, so the null arrays and
    /// `group_metadata` are never reached.
    #[test]
    fn test_send_offsets_to_transaction_async_negative_count_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        let captured = CapturedOpResult::new();
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned.
        // `count == -1` is passed deliberately so the count assert panics before any array
        // or `group_metadata` is read (the `# Safety` requires the arrays only for `count >
        // 0` and allows a null `group_metadata`). The entry point's
        // `#[ffi_guard(on_panic)]` fires `capture_op_result` inline on this thread exactly
        // once with a fresh error handle the callback destroys; `user_data` points at
        // `captured`, a local alive for the whole call, and nothing is spawned.
        unsafe {
            kafka_producer_Producer_send_offsets_to_transaction_async(
                producer,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                -1,
                std::ptr::null(),
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        assert_eq!(
            captured.fired(),
            1,
            "the callback must fire exactly once, before the call returns"
        );
        let msg = captured.message().expect("the callback must deliver an error");
        assert!(
            msg.starts_with(
                "Rust panic caught at the FFI boundary in kafka_producer_Producer_send_offsets_to_transaction_async:"
            ),
            "unexpected error message: {msg}"
        );
        assert!(msg.contains("count must not be negative"), "unexpected error message: {msg}");
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // the failed call spawned no task (its count assert fired before
        // `with_txn_control_async`), so this single `kafka_producer_Producer_destroy` is
        // the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// Builds a throwaway runtime for the drain-ordering tests.
    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
    }

    /// The empty-queue fast path must not touch the channel at all. Proven by
    /// giving it a channel whose receiver is already gone: if the barrier were
    /// pushed, the send would fail and this would return `Err`.
    #[test]
    fn test_drain_submitted_sends_skips_barrier_when_queue_is_empty() {
        let runtime = test_runtime();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<SubmitRequest>();
        drop(rx);
        let queued = std::sync::atomic::AtomicUsize::new(0);
        assert!(drain_submitted_sends_via(&queued, &tx, runtime.handle()).is_ok());
    }

    /// A dead submission task must be reported, never silently treated as
    /// success: a tokio receiver dropped with items queued drops those items *and*
    /// their callbacks, so the records were never produced and nothing will ever
    /// report on them. Returning `Ok` would tell the caller flush/close succeeded
    /// with those records still unsent.
    #[test]
    fn test_drain_submitted_sends_errors_when_submission_task_is_gone() {
        let runtime = test_runtime();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<SubmitRequest>();
        drop(rx);
        let queued = std::sync::atomic::AtomicUsize::new(1);
        let err = drain_submitted_sends_via(&queued, &tx, runtime.handle())
            .expect_err("a dead submission task must not be reported as success");
        assert!(err.message().contains("send-submission task has stopped"), "{}", err.message());
    }

    /// Same requirement for the other half: the barrier was accepted but the task
    /// went away without acknowledging it.
    #[test]
    fn test_drain_submitted_sends_errors_when_ack_is_dropped() {
        let runtime = test_runtime();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<SubmitRequest>();
        // Take the barrier off the channel and drop it without signalling, which is
        // what a task shutting down mid-flight does.
        runtime.spawn(async move {
            let _ = rx.recv().await;
        });
        let queued = std::sync::atomic::AtomicUsize::new(1);
        let err = drain_submitted_sends_via(&queued, &tx, runtime.handle())
            .expect_err("a dropped acknowledgement must not be reported as success");
        assert!(
            err.message().contains("stopped while ordering queued sends"),
            "{}",
            err.message()
        );
    }

    /// Builds a leaked producer handle whose submission task is already dead — its
    /// receiver dropped — and whose `queued_sends` counter is non-zero, so any drain
    /// of the submission queue fails with the "send-submission task has stopped"
    /// error. Used to prove `with_txn_control` drains *before* running the control op:
    /// without the drain the control op would run and the mock would return a
    /// different error (or none), so the drain's message is the separator.
    ///
    /// The caller reclaims the handle with `reclaim_producer_handle`.
    fn dead_submission_handle() -> *mut kafka_producer_Producer_t {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let kind = ProducerKind::Mock(Box::new(MockProducer::with_auto_complete(true)), runtime);
        // A disconnected completion channel: nothing fires on the drain-failure path,
        // so the sender is never used, but the field must be present.
        let (completion_tx, _completion_rx) = std::sync::mpsc::channel::<CompletionJob>();
        let (submit_tx, submit_rx) = tokio::sync::mpsc::unbounded_channel::<SubmitRequest>();
        // Drop the receiver so the drain's barrier push fails, mirroring a submission
        // task that has stopped with records still queued.
        drop(submit_rx);
        let handle = Box::new(ProducerHandle {
            kind: Mutex::new(kind),
            completion_tx,
            submit_tx,
            dispatcher: Mutex::new(None),
            pending_tasks: Mutex::new(Vec::new()),
            txn_control_busy: std::sync::atomic::AtomicBool::new(false),
            queued_sends: std::sync::atomic::AtomicUsize::new(1),
        });
        Box::into_raw(handle) as *mut kafka_producer_Producer_t
    }

    /// Reclaims a handle from `dead_submission_handle` and drops it. Sound because the
    /// drain fails before `with_txn_control` extends any `'static` reference or spawns
    /// any task, so nothing still borrows the handle when it is freed.
    ///
    /// # Safety
    ///
    /// `producer` must be a handle returned by `dead_submission_handle` that has not
    /// been reclaimed or destroyed before, with no `with_txn_control` call or spawned
    /// task still borrowing it; it is freed here and must not be used afterwards.
    unsafe fn reclaim_producer_handle(producer: *mut kafka_producer_Producer_t) {
        // SAFETY: `producer` must be a `ProducerHandle` leaked by `Box::into_raw` (the one
        // `dead_submission_handle` builds, or the identically built one in
        // `test_send_async_panic_after_queueing_is_reported_in_out_error`), so
        // reconstituting the `Box` is the matching single free. Nothing still borrows it at
        // the call sites: the sync control ops made on it returned the drain error from
        // `with_txn_control` before `producer_static_ref` extended any `'static` reference
        // and without spawning, and `send_async` spawns nothing; the `&'static
        // ProducerHandle` those calls derived through `producer_handle` lived only for
        // their duration.
        drop(unsafe { Box::from_raw(producer as *mut ProducerHandle) });
    }

    /// Reads the message of a returned error pointer, asserting it is non-null, then
    /// frees it.
    ///
    /// # Safety
    ///
    /// `err` must be a live error handle returned by an FFI call (null fails the
    /// assertion); it is freed here and must not be used afterwards.
    unsafe fn take_error_message(err: *mut kafka_common_Error_t) -> String {
        assert!(!err.is_null(), "expected a non-null error");
        // SAFETY: `err` is non-null (asserted above) and an owned error handle an FFI call
        // returned to the test; `kafka_common_Error_message` returns a pointer valid until
        // the handle is destroyed, and `into_owned` copies the text before
        // `kafka_common_Error_destroy` consumes `err`.
        let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(err)) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: `err` is the owned error handle whose message was just copied; this is
        // its single destroy and nothing uses it afterwards.
        unsafe { kafka_common_Error_destroy(err) };
        msg
    }

    /// `commit_transaction` must drain the submission queue before it runs (emasab's
    /// PR #168 contract: sends that had *returned* before the control call are included
    /// in it — `.claude/rules/producer-transactions.md` §13). Proven here by a dead
    /// submission task with a record still queued: the commit fails with the drain's
    /// error rather than committing a transaction whose queued record vanished. If the
    /// drain were removed, `commit_transaction` would reach the mock and return a
    /// different error, so the drain's message is the separator.
    #[test]
    fn test_with_txn_control_drains_before_commit() {
        let producer = dead_submission_handle();
        // SAFETY: `kafka_producer_Producer_commit_transaction` requires a valid handle or
        // null: `producer` is the `ProducerHandle` `dead_submission_handle` leaked via
        // `Box::into_raw`, laid out exactly as `build_producer_handle` builds one. Its
        // drain fails (`queued_sends == 1`, submission receiver dropped), so
        // `with_txn_control` returns a fresh error handle before `producer_static_ref` runs
        // and without spawning; `take_error_message` asserts it non-null and destroys it
        // once.
        let msg = unsafe { take_error_message(kafka_producer_Producer_commit_transaction(producer)) };
        assert!(
            msg.contains("send-submission task has stopped"),
            "commit_transaction must surface the drain error, got: {msg}"
        );
        // SAFETY: `reclaim_producer_handle` requires the leaked handle from
        // `dead_submission_handle` with no outstanding borrows: the only call made on
        // `producer` was `commit_transaction`, whose `with_txn_control` returned the drain
        // error before `producer_static_ref` and spawned nothing, and whose `&'static
        // ProducerHandle` and `TxnControlGuard` ended with that call. This is the single
        // free.
        unsafe { reclaim_producer_handle(producer) };
    }

    /// The same proof for a second control op, showing the drain lives in the shared
    /// `with_txn_control` path rather than in one function: `begin_transaction` also
    /// surfaces the drain error instead of reaching the mock's state transition.
    #[test]
    fn test_with_txn_control_drains_before_begin() {
        let producer = dead_submission_handle();
        // SAFETY: `kafka_producer_Producer_begin_transaction` requires a valid handle or
        // null: `producer` is the `ProducerHandle` `dead_submission_handle` leaked via
        // `Box::into_raw`, laid out exactly as `build_producer_handle` builds one. Its
        // drain fails (`queued_sends == 1`, submission receiver dropped), so
        // `with_txn_control` returns a fresh error handle before `producer_static_ref` runs
        // and without spawning; `take_error_message` asserts it non-null and destroys it
        // once.
        let msg = unsafe { take_error_message(kafka_producer_Producer_begin_transaction(producer)) };
        assert!(
            msg.contains("send-submission task has stopped"),
            "begin_transaction must surface the drain error, got: {msg}"
        );
        // SAFETY: `reclaim_producer_handle` requires the leaked handle from
        // `dead_submission_handle` with no outstanding borrows: the only call made on
        // `producer` was `begin_transaction`, whose `with_txn_control` returned the drain
        // error before `producer_static_ref` and spawned nothing, and whose `&'static
        // ProducerHandle` and `TxnControlGuard` ended with that call. This is the single
        // free.
        unsafe { reclaim_producer_handle(producer) };
    }

    /// Captures the error an [`OperationCallbackFn`] delivers, for the async
    /// transaction-control tests. `fired` counts invocations; `message` is the error
    /// text (`None` on a null-error success).
    struct CapturedOpResult {
        fired: std::sync::atomic::AtomicUsize,
        message: Mutex<Option<String>>,
    }
    impl CapturedOpResult {
        fn new() -> Self {
            Self { fired: std::sync::atomic::AtomicUsize::new(0), message: Mutex::new(None) }
        }
        fn fired(&self) -> usize {
            self.fired.load(std::sync::atomic::Ordering::Acquire)
        }
        fn message(&self) -> Option<String> {
            self.message.lock().unwrap().clone()
        }
    }

    /// An [`OperationCallbackFn`] that records the delivered error message (if any)
    /// into the [`CapturedOpResult`] passed as `user_data`, freeing the error handle.
    ///
    /// # Safety
    ///
    /// `error` must be null or an owned error handle (destroyed here) and `user_data`
    /// the `&CapturedOpResult` the test passed together with this callback, alive until
    /// the callback has fired.
    unsafe extern "C" fn capture_op_result(error: *mut kafka_common_Error_t, user_data: *mut std::ffi::c_void) {
        // SAFETY: `user_data` is the `&captured as *const CapturedOpResult` pointer every
        // caller passes together with this callback, a test local that outlives the firing:
        // the tests either observe the inline firing before the entry point returns, or
        // spin on `fired` and let `kafka_producer_Producer_destroy` join the spawned task
        // before `captured` goes out of scope. Only shared access is taken.
        let captured = unsafe { &*(user_data as *const CapturedOpResult) };
        if !error.is_null() {
            // SAFETY: `error` is non-null (checked above) and, per the operation-callback
            // contract, a freshly built handle the callee owns;
            // `kafka_common_Error_message` is valid until the handle is destroyed, and
            // `into_owned` copies the text before `kafka_common_Error_destroy` consumes it.
            let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }
                .to_string_lossy()
                .into_owned();
            *captured.message.lock().unwrap() = Some(msg);
            // SAFETY: `error` is the owned callback error handle whose message was just
            // copied; this is its single destroy.
            unsafe { kafka_common_Error_destroy(error) };
        }
        captured.fired.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    /// Spins up to ~5s for an async completion callback to fire, mirroring the C
    /// `wait_for` helper. A spawned transaction-control op reaches into the producer
    /// handle by raw pointer, so the task must be done touching it before `destroy`
    /// frees the handle; waiting for the callback (fired after the last such access)
    /// is the synchronization that makes the subsequent `destroy` sound.
    fn wait_for_fired(captured: &CapturedOpResult) {
        for _ in 0..5000 {
            if captured.fired() >= 1 {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("timed out waiting for the async callback to fire");
    }

    /// The async commit variant drains the submission queue before running the op,
    /// exactly like the sync `commit_transaction` (PR #168 §13): a record queued by
    /// `send_async` that had returned before the commit is included in it. Proven the
    /// same way as `test_with_txn_control_drains_before_commit` — a dead submission
    /// task with a record still queued makes the drain fail, so the async commit's
    /// callback carries that drain error rather than a mock state error, which only
    /// happens if the drain ran through the real production path *before* the op. This
    /// exercises the async path end to end (spawn → drain → completion callback), not
    /// a fixture (DoD #12). `destroy` joins the spawned task, whose completion job
    /// fires the callback inline (the dead handle's completion receiver is gone), so
    /// the message is captured by the time `destroy` returns.
    #[test]
    fn test_commit_transaction_async_drains_before_op() {
        let producer = dead_submission_handle();
        let captured = CapturedOpResult::new();
        // SAFETY: `kafka_producer_Producer_commit_transaction_async` requires a valid
        // handle or null: `producer` is the `ProducerHandle` `dead_submission_handle`
        // leaked via `Box::into_raw`. `capture_op_result`/`user_data` are passed as a pair;
        // `user_data` points at `captured`, which outlives the callback because the test
        // spins on `fired` and `kafka_producer_Producer_destroy` joins the task registered
        // via `reserve_pending_task` before `captured` goes out of scope. The dead handle's
        // completion receiver is gone, so `enqueue_or_run_inline` fires the callback inline
        // in the task, exactly once, with a fresh error handle the callback destroys.
        unsafe {
            kafka_producer_Producer_commit_transaction_async(
                producer,
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        // Wait for the spawned task to finish touching the handle before destroy.
        wait_for_fired(&captured);
        // SAFETY: `producer` is the hand-built handle `dead_submission_handle` leaked via
        // `Box::into_raw`, laid out exactly as `build_producer_handle` builds one;
        // `kafka_producer_Producer_destroy` reconstitutes the `Box` once, joins the task
        // `commit_transaction_async` registered via `reserve_pending_task` (which has
        // already fired its callback) before dropping the producer, and tolerates the
        // `None` dispatcher. Nothing uses the pointer afterwards.
        unsafe { kafka_producer_Producer_destroy(producer) };
        assert_eq!(captured.fired(), 1, "the async commit must fire its callback exactly once");
        let msg = captured
            .message()
            .expect("the async commit callback must deliver the drain error");
        assert!(
            msg.contains("send-submission task has stopped"),
            "commit_transaction_async must surface the drain error, got: {msg}"
        );
    }

    /// The same proof for `begin_transaction_async`, showing the drain lives in the
    /// shared `with_txn_control_async` path rather than in one function — mirrors
    /// `test_with_txn_control_drains_before_begin`.
    #[test]
    fn test_begin_transaction_async_drains_before_op() {
        let producer = dead_submission_handle();
        let captured = CapturedOpResult::new();
        // SAFETY: `kafka_producer_Producer_begin_transaction_async` requires a valid handle
        // or null: `producer` is the `ProducerHandle` `dead_submission_handle` leaked via
        // `Box::into_raw`. `capture_op_result`/`user_data` are passed as a pair;
        // `user_data` points at `captured`, which outlives the callback because the test
        // spins on `fired` and `kafka_producer_Producer_destroy` joins the task registered
        // via `reserve_pending_task` before `captured` goes out of scope. The dead handle's
        // completion receiver is gone, so `enqueue_or_run_inline` fires the callback inline
        // in the task, exactly once, with a fresh error handle the callback destroys.
        unsafe {
            kafka_producer_Producer_begin_transaction_async(
                producer,
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        // Wait for the spawned task to finish touching the handle before destroy.
        wait_for_fired(&captured);
        // SAFETY: `producer` is the hand-built handle `dead_submission_handle` leaked via
        // `Box::into_raw`, laid out exactly as `build_producer_handle` builds one;
        // `kafka_producer_Producer_destroy` reconstitutes the `Box` once, joins the task
        // `begin_transaction_async` registered via `reserve_pending_task` (which has
        // already fired its callback) before dropping the producer, and tolerates the
        // `None` dispatcher. Nothing uses the pointer afterwards.
        unsafe { kafka_producer_Producer_destroy(producer) };
        assert_eq!(captured.fired(), 1, "the async begin must fire its callback exactly once");
        let msg = captured
            .message()
            .expect("the async begin callback must deliver the drain error");
        assert!(
            msg.contains("send-submission task has stopped"),
            "begin_transaction_async must surface the drain error, got: {msg}"
        );
    }

    /// A control call — async or sync — issued while the transaction-control flag is
    /// already held (i.e. another control op is in flight) is rejected with
    /// `concurrent_modification`, on the calling thread, without spawning. Both entry
    /// points are checked against the *same* held flag, which is how a sync call and
    /// an in-flight async call reject each other. Deterministic: the async rejection
    /// fires the callback synchronously (the CAS fails before the spawn), so no wait
    /// and no timing is involved.
    #[test]
    fn test_txn_control_async_rejects_when_control_busy() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer_handle` requires a non-null handle created by
        // `build_producer_handle`: `producer` is the handle
        // `kafka_producer_MockProducer_new` just returned. The `&'static ProducerHandle` is
        // used only to toggle `txn_control_busy` and inspect `pending_tasks`, all before
        // `kafka_producer_Producer_destroy(producer)` frees the handle at the end of the
        // test.
        let handle = unsafe { producer_handle(producer) };
        // Simulate another control op already in flight.
        handle.txn_control_busy.store(true, std::sync::atomic::Ordering::Release);
        let tasks_before = handle.pending_tasks.lock().unwrap().len();

        // Async entry point: the CAS fails, the callback fires synchronously on this
        // thread with the guard message, and nothing is spawned.
        let captured = CapturedOpResult::new();
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `capture_op_result`/`user_data` a pair pointing at the local `captured`.
        // Because `txn_control_busy` was set above, `with_txn_control_async`'s CAS fails
        // and the callback fires inline on this thread, exactly once, with a fresh error
        // handle the callback destroys; nothing is spawned (asserted via `pending_tasks`),
        // so nothing outlives the call.
        unsafe {
            kafka_producer_Producer_commit_transaction_async(
                producer,
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        assert_eq!(
            captured.fired(),
            1,
            "a rejected async call must fire its callback synchronously (inline)"
        );
        let msg = captured.message().expect("the rejected async call must deliver an error");
        assert!(
            msg.contains("not safe for concurrent access"),
            "async overlap must report concurrent_modification, got: {msg}"
        );
        assert_eq!(
            handle.pending_tasks.lock().unwrap().len(),
            tasks_before,
            "a rejected async call must not spawn a task"
        );

        // The sync entry point shares the same flag, so it is rejected too.
        // SAFETY: `kafka_producer_Producer_commit_transaction` requires a valid handle or
        // null: `producer` is the live mock handle. The still-held flag makes
        // `with_txn_control` return the concurrent-modification error before touching the
        // producer; `take_error_message` asserts the fresh handle non-null and destroys it
        // once.
        let sync_msg = unsafe { take_error_message(kafka_producer_Producer_commit_transaction(producer)) };
        assert!(
            sync_msg.contains("not safe for concurrent access"),
            "the sync path shares the flag and must also reject, got: {sync_msg}"
        );

        // Release the simulated in-flight op and tear down.
        handle.txn_control_busy.store(false, std::sync::atomic::Ordering::Release);
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // the flag was released above, the rejected calls spawned nothing, and `handle` is
        // not used after this point, so this single `kafka_producer_Producer_destroy` is
        // the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// Poisons `lock` the way a panic caught by `#[ffi_guard]` does: by panicking
    /// while holding it.
    fn poison<T>(lock: &Mutex<T>) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = lock.lock().unwrap();
            panic!("poisoning the lock for a test");
        }));
        assert!(result.is_err(), "the poisoning closure must panic");
        assert!(lock.is_poisoned(), "the lock must be poisoned");
    }

    /// A `partitions_for_async` callback recording into a [`CapturedOpResult`]: the
    /// error through [`capture_op_result`], and a delivered list — which no test using
    /// it expects — as a message that fails their assertions.
    ///
    /// # Safety
    ///
    /// Same requirements as `capture_op_result`, plus `list` must be null or an owned
    /// partition-info list handle (destroyed here).
    unsafe extern "C" fn capture_partitions_result(
        list: *mut kafka_common_PartitionInfoList_t,
        error: *mut kafka_common_Error_t,
        user_data: *mut std::ffi::c_void,
    ) {
        // SAFETY: `capture_op_result` has the same requirements as this callback: `error`
        // is the operation error handle (null, or owned by the callee) and `user_data` the
        // `&captured` pointer the test passed together with this callback, a local alive
        // for the inline firing.
        unsafe { capture_op_result(error, user_data) };
        if !list.is_null() {
            // SAFETY: `list` is non-null (checked above) and, per
            // `kafka_producer_Producer_partitions_for_callback_t`'s contract, a freshly
            // built list handle the callee owns; `kafka_common_PartitionInfoList_destroy`
            // is its single destroy.
            unsafe { crate::ffi::consumer::kafka_common_PartitionInfoList_destroy(list) };
            // SAFETY: `user_data` is the `&captured as *const CapturedOpResult` pointer the
            // test passed together with this callback, a local alive for the inline firing;
            // only shared access is taken.
            let captured = unsafe { &*(user_data as *const CapturedOpResult) };
            *captured.message.lock().unwrap() = Some("unexpected non-null partition list".to_owned());
        }
    }

    /// Captures a `get_all_async` completion: how often it fired, the `count` it was
    /// given, how many metadata entries were non-null, and the message of every error
    /// entry. Every delivered handle is freed.
    struct CapturedGetAll {
        fired: std::sync::atomic::AtomicUsize,
        count: std::sync::atomic::AtomicI32,
        non_null_metadata: std::sync::atomic::AtomicUsize,
        messages: Mutex<Vec<String>>,
    }
    impl CapturedGetAll {
        fn new() -> Self {
            Self {
                fired: std::sync::atomic::AtomicUsize::new(0),
                count: std::sync::atomic::AtomicI32::new(-1),
                non_null_metadata: std::sync::atomic::AtomicUsize::new(0),
                messages: Mutex::new(Vec::new()),
            }
        }
    }

    /// A [`kafka_common_KafkaFuture_RecordMetadata_get_all_callback_t`] that records into
    /// the [`CapturedGetAll`] passed as `user_data`.
    ///
    /// # Safety
    ///
    /// Called by the library through
    /// `kafka_common_KafkaFuture_RecordMetadata_get_all_callback_t`: `metadata` and
    /// `errors` must hold `count` entries, each null or an owned handle (destroyed here),
    /// and `user_data` must be the `&CapturedGetAll` the test passed, alive until the
    /// callback has fired.
    unsafe extern "C" fn capture_get_all(
        metadata: *mut *mut kafka_producer_RecordMetadata_t,
        errors: *mut *mut kafka_common_Error_t,
        count: i32,
        user_data: *mut std::ffi::c_void,
    ) {
        // SAFETY: `user_data` is the `&captured as *const CapturedGetAll` pointer each
        // caller passes together with this callback, a test local that outlives the firing
        // because both tests observe the inline firing before the entry point returns; only
        // shared access is taken.
        let captured = unsafe { &*(user_data as *const CapturedGetAll) };
        for i in 0..count.max(0) as usize {
            // SAFETY: Per `kafka_common_KafkaFuture_RecordMetadata_get_all_callback_t`'s
            // contract `metadata` holds `count` entries valid for the duration of the
            // callback; `i < count.max(0)` keeps the read in range and guards a negative
            // `count`.
            let entry = unsafe { *metadata.add(i) };
            if !entry.is_null() {
                captured.non_null_metadata.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                // SAFETY: `entry` is non-null (checked above) and, per the `get_all`
                // callback contract, a freshly built metadata handle the callee owns; this
                // is its single destroy.
                unsafe { kafka_producer_RecordMetadata_destroy(entry) };
            }
            // SAFETY: Per `kafka_common_KafkaFuture_RecordMetadata_get_all_callback_t`'s
            // contract `errors` holds `count` entries valid for the duration of the
            // callback; `i < count.max(0)` keeps the read in range and guards a negative
            // `count`.
            let error = unsafe { *errors.add(i) };
            if !error.is_null() {
                // SAFETY: `error` is non-null (checked above) and, per the `get_all`
                // callback contract, a freshly built error handle the callee owns;
                // `kafka_common_Error_message` is valid until it is destroyed, and
                // `into_owned` copies the text before `kafka_common_Error_destroy` consumes
                // it.
                let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }
                    .to_string_lossy()
                    .into_owned();
                captured.messages.lock().unwrap().push(msg);
                // SAFETY: `error` is the owned callback error handle whose message was just
                // copied; this is its single destroy.
                unsafe { kafka_common_Error_destroy(error) };
            }
        }
        captured.count.store(count, std::sync::atomic::Ordering::Release);
        captured.fired.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    /// `get_all_async` reports a panic through its callback, exactly once and in the
    /// callback's own shape: a null `futures` array trips a precondition assert, and
    /// the caller still receives `count` error entries and no metadata, each naming
    /// the entry point and the violated precondition (D4).
    #[test]
    fn test_get_all_async_null_futures_reports_panic_through_callback() {
        let captured = CapturedGetAll::new();
        // SAFETY: Null `futures` is passed deliberately to trip the documented precondition
        // assert of `kafka_common_KafkaFuture_RecordMetadata_get_all_async`; its
        // `#[ffi_guard(on_panic)]` then runs `fire_get_all_callback_with_error`, which
        // builds `count` null metadata slots and `count` fresh error handles and fires
        // `capture_get_all` inline on this thread, exactly once. `user_data` points at
        // `captured`, a local alive for the whole call, and the callback destroys every
        // handle it receives.
        unsafe {
            kafka_common_KafkaFuture_RecordMetadata_get_all_async(
                std::ptr::null_mut(),
                2,
                capture_get_all,
                &captured as *const CapturedGetAll as *mut std::ffi::c_void,
            );
        }
        // The panic path fires inline, before the call returns.
        assert_eq!(captured.fired.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(captured.count.load(std::sync::atomic::Ordering::Acquire), 2);
        assert_eq!(captured.non_null_metadata.load(std::sync::atomic::Ordering::Acquire), 0);
        let messages = captured.messages.lock().unwrap().clone();
        assert_eq!(messages.len(), 2, "one error per requested entry");
        for msg in messages {
            assert!(
                msg.starts_with(
                    "Rust panic caught at the FFI boundary in kafka_common_KafkaFuture_RecordMetadata_get_all_async:"
                ),
                "unexpected error message: {msg}"
            );
            assert!(msg.contains("futures must not be null"), "unexpected error message: {msg}");
        }
    }

    /// A negative `count` must fail rather than be clamped, and `get_all_async` still
    /// keeps its fire-once promise: the callback fires with empty arrays, since there
    /// is no valid length to report the error at.
    #[test]
    fn test_get_all_async_negative_count_fires_callback_once() {
        let captured = CapturedGetAll::new();
        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 1] = [std::ptr::null_mut()];
        // SAFETY: `futures` is a one-element local array and `count == -1` is passed
        // deliberately to trip the documented count assert before any entry is read; the
        // entry point's `#[ffi_guard(on_panic)]` makes `fire_get_all_callback_with_error`
        // fire `capture_get_all` inline on this thread, exactly once, with empty arrays
        // (negative `count` yields length 0). `user_data` points at `captured`, a local
        // alive for the whole call.
        unsafe {
            kafka_common_KafkaFuture_RecordMetadata_get_all_async(
                futures.as_mut_ptr(),
                -1,
                capture_get_all,
                &captured as *const CapturedGetAll as *mut std::ffi::c_void,
            );
        }
        assert_eq!(captured.fired.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(captured.count.load(std::sync::atomic::Ordering::Acquire), 0);
        assert!(captured.messages.lock().unwrap().is_empty());
    }

    /// After a caught panic has poisoned the handle's `kind` lock, a later
    /// `partitions_for_async` fails through `#[ffi_guard]` (D6): the callback fires
    /// exactly once, before the call returns, with a null list and the panic error
    /// (D4). `destroy` must still tear the poisoned handle down.
    #[test]
    fn test_partitions_for_async_on_poisoned_handle_reports_panic_through_callback() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer_handle` requires a non-null handle created by
        // `build_producer_handle`: `producer` is the handle
        // `kafka_producer_MockProducer_new` just returned. The `&'static ProducerHandle` is
        // used only to poison the lock within this expression, long before
        // `kafka_producer_Producer_destroy` frees the handle at the end of the test.
        poison(&unsafe { producer_handle(producer) }.kind);
        let topic = CString::new("topic").unwrap();
        let captured = CapturedOpResult::new();
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `topic` an owned `CString` alive for the call, as
        // `kafka_producer_Producer_partitions_for_async`'s `# Safety` requires;
        // `capture_partitions_result`/`user_data` are a pair pointing at the local
        // `captured`. The poisoned `kind` lock panics before `reserve_pending_task` and the
        // spawn, so the entry point's guard fires the callback inline on this thread,
        // exactly once, with a null list and a fresh error handle the callback destroys;
        // nothing outlives the call.
        unsafe {
            kafka_producer_Producer_partitions_for_async(
                producer,
                topic.as_ptr(),
                capture_partitions_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        assert_eq!(captured.fired(), 1, "the panic must be reported once, before the call returns");
        let msg = captured.message().expect("the callback must deliver the panic error");
        assert!(
            msg.starts_with("Rust panic caught at the FFI boundary in kafka_producer_Producer_partitions_for_async:"),
            "unexpected error message: {msg}"
        );
        assert!(msg.contains("PoisonError"), "unexpected error message: {msg}");
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // the failed call spawned no task, and `kafka_producer_Producer_destroy` reads the
        // poisoned `kind` lock poison-tolerantly, so this single destroy is the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// Regression for the two-phase task registration: with `pending_tasks` poisoned,
    /// `flush_async` must fail *before* it spawns its task. Registering after the
    /// spawn let `#[ffi_guard]` report the panic through the callback while the
    /// already-spawned task delivered a second completion.
    #[test]
    fn test_flush_async_with_poisoned_task_list_fires_callback_once() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer_handle` requires a non-null handle created by
        // `build_producer_handle`: `producer` is the handle
        // `kafka_producer_MockProducer_new` just returned. The `&'static ProducerHandle` is
        // used only to poison the lock within this expression, long before
        // `kafka_producer_Producer_destroy` frees the handle at the end of the test.
        poison(&unsafe { producer_handle(producer) }.pending_tasks);
        let captured = CapturedOpResult::new();
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `capture_op_result`/`user_data` a pair pointing at the local `captured`. The
        // poisoned `pending_tasks` lock makes `reserve_pending_task` panic before the
        // spawn, so the entry point's guard fires the callback inline on this thread
        // exactly once with a fresh error handle the callback destroys (the 200 ms wait
        // below checks for a second completion); nothing outlives the call.
        unsafe {
            kafka_producer_Producer_flush_async(
                producer,
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        assert_eq!(captured.fired(), 1, "the panic must be reported before the call returns");
        // Give a stray task ample time to deliver a second completion.
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(captured.fired(), 1, "the callback must fire exactly once");
        let msg = captured.message().expect("the callback must deliver the panic error");
        assert!(
            msg.starts_with("Rust panic caught at the FFI boundary in kafka_producer_Producer_flush_async:"),
            "unexpected error message: {msg}"
        );
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // no task was spawned, and `kafka_producer_Producer_destroy` reads the poisoned
        // `pending_tasks` lock poison-tolerantly, so this single destroy is the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// Regression for creating `TxnControlAsyncGuard` right after the CAS: a panic
    /// between the CAS and the spawn — here the poisoned `kind` lock — must still
    /// release the transaction-control flag, or every later control call on the
    /// handle would be rejected as concurrent.
    #[test]
    fn test_commit_transaction_async_panic_before_spawn_releases_txn_flag() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer_handle` requires a non-null handle created by
        // `build_producer_handle`: `producer` is the handle
        // `kafka_producer_MockProducer_new` just returned. The `&'static ProducerHandle` is
        // used to poison `kind` and to read `txn_control_busy`, all before
        // `kafka_producer_Producer_destroy` frees the handle at the end of the test.
        let handle = unsafe { producer_handle(producer) };
        poison(&handle.kind);
        let captured = CapturedOpResult::new();
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `capture_op_result`/`user_data` a pair pointing at the local `captured`. The
        // CAS succeeds and `TxnControlAsyncGuard` is created, then the poisoned `kind` lock
        // panics before the spawn; the unwind drops the guard (releasing the flag) and the
        // entry point's guard fires the callback inline on this thread exactly once with a
        // fresh error handle the callback destroys, so nothing outlives the call.
        unsafe {
            kafka_producer_Producer_commit_transaction_async(
                producer,
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        assert_eq!(captured.fired(), 1, "the panic must be reported once, before the call returns");
        let msg = captured.message().expect("the callback must deliver the panic error");
        assert!(
            msg.starts_with(
                "Rust panic caught at the FFI boundary in kafka_producer_Producer_commit_transaction_async:"
            ),
            "unexpected error message: {msg}"
        );
        assert!(
            !handle.txn_control_busy.load(std::sync::atomic::Ordering::Acquire),
            "the transaction-control flag must be released on the panic path"
        );
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // no task was spawned, `handle` is not used after the flag assertion, and
        // `kafka_producer_Producer_destroy` tolerates the poisoned `kind` lock, so this
        // single destroy is the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// `destroy` is the documented way out after a caught panic, so it must still
    /// join the handle's tasks when both of its locks are poisoned: reading a
    /// poisoned `pending_tasks` as empty, or panicking on `kind`, would free the
    /// producer under a task that may still be using it.
    #[test]
    fn test_destroy_joins_pending_tasks_on_a_poisoned_handle() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer_handle` requires a non-null handle created by
        // `build_producer_handle`: `producer` is the handle
        // `kafka_producer_MockProducer_new` just returned. The `&'static ProducerHandle` is
        // used to register a task via `reserve_pending_task` and to poison both locks, all
        // before `kafka_producer_Producer_destroy` frees the handle.
        let handle = unsafe { producer_handle(producer) };
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let runtime = handle.kind.lock().unwrap().runtime().handle().clone();
        {
            let mut pending = reserve_pending_task(handle);
            let finished = Arc::clone(&finished);
            pending.push(runtime.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                finished.store(true, std::sync::atomic::Ordering::Release);
            }));
        }
        poison(&handle.kind);
        poison(&handle.pending_tasks);
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned,
        // with one extra task registered via `reserve_pending_task`;
        // `kafka_producer_Producer_destroy` reads both poisoned locks poison-tolerantly and
        // joins that task before dropping the producer (the property under test), and this
        // single destroy is the final use of the pointer.
        unsafe { kafka_producer_Producer_destroy(producer) };
        assert!(
            finished.load(std::sync::atomic::Ordering::Acquire),
            "destroy must join every registered task before it frees the producer"
        );
    }

    /// A send callback that counts its invocations in the `AtomicUsize` passed as
    /// `user_data`, freeing whichever handles it is given, as a C caller must.
    ///
    /// # Safety
    ///
    /// Called by the library through `kafka_producer_Producer_send_callback_t`:
    /// `metadata` and `error` must be null or owned handles (both are destroyed here) and
    /// `user_data` the `&AtomicUsize` the test passed, alive past the producer's teardown.
    unsafe extern "C" fn count_send_callback(
        metadata: *mut kafka_producer_RecordMetadata_t,
        error: *mut kafka_common_Error_t,
        user_data: *mut std::ffi::c_void,
    ) {
        // SAFETY: `user_data` is the `&invocations as *const AtomicUsize` pointer each
        // caller passes together with this callback, a stack `AtomicUsize` the tests keep
        // alive past the producer's teardown and their final assertion; only shared, atomic
        // access is taken.
        let invocations = unsafe { &*(user_data as *const std::sync::atomic::AtomicUsize) };
        invocations.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if !metadata.is_null() {
            // SAFETY: `metadata` is non-null (checked above) and, per
            // `kafka_producer_Producer_send_callback_t`'s contract, a freshly built handle
            // the callee owns, handed over by the dispatcher through
            // `make_record_callback`; this is its single destroy.
            unsafe { kafka_producer_RecordMetadata_destroy(metadata) };
        }
        if !error.is_null() {
            // SAFETY: `error` is non-null (checked above) and, per
            // `kafka_producer_Producer_send_callback_t`'s contract, a freshly built error
            // handle the callee owns, handed over by the dispatcher through
            // `make_record_callback`; this is its single destroy.
            unsafe { kafka_common_Error_destroy(error) };
        }
    }

    /// `send_with_callback` keeps the plain guard (§6 note 8), because its docs say
    /// a synchronous failure does not invoke the callback. A caught panic — here
    /// from the `kind` lock an earlier panic poisoned — is such a failure: null,
    /// the panic error in `out_error`, and no callback, not even after teardown
    /// (COMMENTS.79.md issue 1).
    #[test]
    fn test_send_with_callback_panic_is_a_synchronous_failure() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer_handle` requires a non-null handle created by
        // `build_producer_handle`: `producer` is the handle
        // `kafka_producer_MockProducer_new` just returned. The `&'static ProducerHandle` is
        // used only to poison the lock within this expression, long before
        // `kafka_producer_Producer_destroy` frees the handle at the end of the test.
        poison(&unsafe { producer_handle(producer) }.kind);
        let topic = CString::new("topic").unwrap();
        let value = b"value";
        let invocations = std::sync::atomic::AtomicUsize::new(0);
        let mut out_error: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned,
        // `topic` an owned `CString` and `value` a static byte literal passed with its
        // exact length, satisfying `kafka_producer_Producer_send_with_callback`'s `#
        // Safety`; `&mut out_error` is a writable local and
        // `count_send_callback`/`user_data` a pair pointing at the local `invocations`. The
        // poisoned `kind` lock panics before `producer_send_with_callback`, so the record
        // callback built just before is dropped unfired and the guard reports the panic
        // through `out_error` only, as the function's docs specify.
        let future = unsafe {
            kafka_producer_Producer_send_with_callback(
                producer,
                topic.as_ptr(),
                -1,
                -1,
                std::ptr::null(),
                -1,
                value.as_ptr(),
                value.len() as i32,
                count_send_callback,
                &invocations as *const std::sync::atomic::AtomicUsize as *mut std::ffi::c_void,
                &mut out_error,
            )
        };
        assert!(future.is_null(), "a caught panic must return null");
        assert!(!out_error.is_null(), "a caught panic must be stored in out_error");
        assert_eq!(
            // SAFETY: `out_error` is non-null (asserted above) and the error handle the
            // guard stored for this call; `kafka_common_Error_code` only reads it.
            unsafe { kafka_common_Error_code(out_error) },
            kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE
        );
        // SAFETY: `take_error_message` requires a non-null owned error handle: `out_error`
        // is the handle the guard stored, read once above by `kafka_common_Error_code`, and
        // this is its single destroy.
        let msg = unsafe { take_error_message(out_error) };
        assert!(
            msg.starts_with("Rust panic caught at the FFI boundary in kafka_producer_Producer_send_with_callback:"),
            "unexpected error message: {msg}"
        );
        assert!(msg.contains("PoisonError"), "unexpected error message: {msg}");
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // the failed send registered no record and spawned nothing, and
        // `kafka_producer_Producer_destroy` tolerates the poisoned `kind` lock, so this
        // single destroy is the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
        // Give a stray completion ample time to reach the detached dispatcher.
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(
            invocations.load(std::sync::atomic::Ordering::Acquire),
            0,
            "a panic reported through out_error must not also invoke the callback"
        );
    }

    /// `send_batch_async` keeps the plain guard (§6 note 8), because its docs say a
    /// record that fails synchronously produces no callback. A caught panic — here
    /// from the negative-`count` precondition — is such a failure: `-1`, nothing
    /// stored in `out_errors`, and no callback, not even after teardown
    /// (COMMENTS.79.md issue 1).
    #[test]
    fn test_send_batch_async_panic_is_a_synchronous_failure() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();
        let records = [kafka_producer_ProducerRecord_t {
            topic: topic.as_ptr(),
            partition: -1,
            timestamp: -1,
            key: std::ptr::null(),
            key_len: -1,
            value: std::ptr::null(),
            value_len: -1,
        }];
        // Neither a success (null) nor an error handle, so any store shows.
        let sentinel = std::ptr::dangling_mut::<kafka_common_Error_t>();
        let mut errors: [*mut kafka_common_Error_t; 1] = [sentinel];
        let invocations = std::sync::atomic::AtomicUsize::new(0);
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned,
        // `records` a one-element local array whose `topic` `CString` outlives the call,
        // `errors` a one-slot local, and `count_send_callback`/`user_data` a pair pointing
        // at the local `invocations`. `count == -1` is passed deliberately to trip the
        // documented count assert, which fires before `producer_handle` or any array read,
        // so `#[ffi_guard]` returns -1 and nothing is queued or spawned.
        let accepted = unsafe {
            kafka_producer_Producer_send_batch_async(
                producer,
                records.as_ptr(),
                -1,
                count_send_callback,
                &invocations as *const std::sync::atomic::AtomicUsize as *mut std::ffi::c_void,
                errors.as_mut_ptr(),
            )
        };
        assert_eq!(accepted, -1, "a caught panic must return -1");
        assert_eq!(errors[0], sentinel, "a caught panic must not be stored in out_errors");
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // the failed call queued nothing and spawned nothing, so this single
        // `kafka_producer_Producer_destroy` is the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };
        // Give a stray completion ample time to reach the detached dispatcher.
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(
            invocations.load(std::sync::atomic::Ordering::Acquire),
            0,
            "a panic reported through the return value must not also invoke the callback"
        );
    }

    /// `send_async` keeps the plain guard (§6 note 8), and its one unwinding panic
    /// comes after the hand-off: tokio's `UnboundedSender::send` queues the request
    /// and then wakes the receiver, and a waker that panics unwinds out of that wake
    /// (tokio 1.52.0, `sync/mpsc/chan.rs:528-534`, `sync/task/atomic_waker.rs:303-308`).
    /// The test registers such a waker on a submission channel it owns and checks that
    /// the panic lands in `out_error` while the request stays queued, counted and
    /// carrying this call's callback target (COMMENTS.79.md issue 2).
    ///
    /// The callback counter staying at 0 means only that the guard did not invoke the
    /// callback. The test holds the receiver, so nothing delivers the queued record
    /// here. In production the submission task does deliver it and fires the callback
    /// exactly once, under an at-most-once flag of its own, so an `on_panic` that fired
    /// the callback would fire it a second time and free `user_data` twice. The
    /// contract for this path is one callback, from the delivery, not none.
    #[test]
    fn test_send_async_panic_after_queueing_is_reported_in_out_error() {
        struct PanickingWake;
        impl std::task::Wake for PanickingWake {
            fn wake(self: std::sync::Arc<Self>) {
                panic!("the submission receiver's waker panics");
            }
        }
        // Built like `dead_submission_handle`, except that the test keeps both
        // receivers: the submission one to register the waker and see what was
        // queued, the completion one to run whatever reaches the dispatcher queue.
        // No submission task or dispatcher thread exists.
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let kind = ProducerKind::Mock(Box::new(MockProducer::with_auto_complete(true)), runtime);
        let (completion_tx, completion_rx) = std::sync::mpsc::channel::<CompletionJob>();
        let (submit_tx, mut submit_rx) = tokio::sync::mpsc::unbounded_channel::<SubmitRequest>();
        let handle = Box::new(ProducerHandle {
            kind: Mutex::new(kind),
            completion_tx,
            submit_tx,
            dispatcher: Mutex::new(None),
            pending_tasks: Mutex::new(Vec::new()),
            txn_control_busy: std::sync::atomic::AtomicBool::new(false),
            queued_sends: std::sync::atomic::AtomicUsize::new(0),
        });
        let producer = Box::into_raw(handle) as *mut kafka_producer_Producer_t;
        let waker = std::task::Waker::from(std::sync::Arc::new(PanickingWake));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            submit_rx.poll_recv(&mut cx).is_pending(),
            "polling the empty channel registers the panicking waker"
        );

        let topic = CString::new("topic").unwrap();
        let value = b"value";
        let invocations = std::sync::atomic::AtomicUsize::new(0);
        let user_data = &invocations as *const std::sync::atomic::AtomicUsize as *mut std::ffi::c_void;
        let mut out_error: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: `kafka_producer_Producer_send_async` requires a valid handle, a valid
        // `topic` C string and a `value` valid for `value_len` bytes until the callback
        // fires: `producer` is the `ProducerHandle` this test leaked via `Box::into_raw`
        // (laid out as `build_producer_handle` builds one), `topic` an owned `CString`,
        // `value` a static byte literal, `&mut out_error` a writable local, and
        // `count_send_callback`/`user_data` a pair pointing at the local `invocations`. The
        // only reach into the handle is the `&'static` from `producer_handle`, used within
        // the call; the queued request is drained and dropped by the test itself before the
        // handle is reclaimed.
        unsafe {
            kafka_producer_Producer_send_async(
                producer,
                topic.as_ptr(),
                -1,
                -1,
                std::ptr::null(),
                -1,
                value.as_ptr(),
                value.len() as i32,
                count_send_callback,
                user_data,
                &mut out_error,
            )
        };
        assert!(!out_error.is_null(), "a caught panic must be stored in out_error");
        assert_eq!(
            // SAFETY: `out_error` is non-null (asserted above) and the error handle the
            // guard stored for this call; `kafka_common_Error_code` only reads it.
            unsafe { kafka_common_Error_code(out_error) },
            kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE
        );
        // SAFETY: `take_error_message` requires a non-null owned error handle: `out_error`
        // is the handle the guard stored, read once above by `kafka_common_Error_code`, and
        // this is its single destroy.
        let msg = unsafe { take_error_message(out_error) };
        assert!(
            msg.starts_with("Rust panic caught at the FFI boundary in kafka_producer_Producer_send_async:"),
            "unexpected error message: {msg}"
        );
        assert!(
            msg.contains("the submission receiver's waker panics"),
            "unexpected error message: {msg}"
        );

        // The panic came after the hand-off: the request is counted and on the channel,
        // carrying this call's callback target, exactly as after a successful call.
        // SAFETY: `producer_handle` requires a non-null handle created by
        // `build_producer_handle`: `producer` is the identically laid-out `ProducerHandle`
        // this test leaked via `Box::into_raw` and has not yet reclaimed; the reference is
        // used only for this one atomic load.
        let queued_sends = unsafe { producer_handle(producer) }
            .queued_sends
            .load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(queued_sends, 1, "the queued request must still be counted");
        let Ok(SubmitRequest::Send(request)) = submit_rx.try_recv() else {
            panic!("the request must reach the channel before the waker panics");
        };
        assert_eq!(
            request.target.user_data, user_data,
            "the queued request must carry this call's callback target"
        );
        drop(request);
        assert!(submit_rx.try_recv().is_err(), "send_async must queue exactly one request");
        drop(submit_rx);
        // Sound: `send_async` spawns nothing and keeps no reference to the handle, and
        // the one request that borrowed this call's buffers has been dropped.
        // SAFETY: `reclaim_producer_handle` requires a `Box::into_raw`-leaked
        // `ProducerHandle` with nothing still borrowing it: `send_async` spawned no task
        // and kept no reference (its `&'static ProducerHandle` ended with the call), the
        // one queued `SendRequest` and `submit_rx` were dropped above, and `completion_rx`
        // is the test's own receiver, drained only afterwards. This is the single free.
        unsafe { reclaim_producer_handle(producer) };
        // Run whatever reached the completion queue, as the dispatcher would, so that a
        // callback fired through it is counted too.
        while let Ok(job) = completion_rx.try_recv() {
            job();
        }
        assert_eq!(
            invocations.load(std::sync::atomic::Ordering::Acquire),
            0,
            "the guard must not invoke the callback: the queued record still owns its one firing"
        );
    }

    /// Helper: asserts that `kafka_producer_Producer_send_batch` with the given
    /// arguments fails rather than aborting the process or clamping: the real
    /// `extern "C"` entry point returns `-1`, the failure value `#[ffi_guard]`
    /// produces for a caught panic. `send_batch` has no `out_error`, so the panic
    /// message is checked by running `send_batch_inner` through `ffi_guard_or`, the
    /// runtime helper the attribute expands to, with an `on_panic` that keeps the
    /// error.
    ///
    /// # Safety
    ///
    /// Every argument is forwarded unchanged to `kafka_producer_Producer_send_batch`
    /// and `send_batch_inner`, so the arguments must either violate one of the
    /// preconditions that entry point checks before dereferencing anything (a null
    /// pointer or a negative `count`, the cases these tests exercise) or satisfy its
    /// `# Safety` contract in full.
    unsafe fn assert_send_batch_fails(
        producer: *mut kafka_producer_Producer_t,
        records: *const kafka_producer_ProducerRecord_t,
        count: i32,
        out_futures: *mut *mut kafka_common_KafkaFuture_RecordMetadata_t,
        out_errors: *mut *mut kafka_common_Error_t,
        expected_msg: &str,
    ) {
        // SAFETY: The real entry point is called with the test's arguments: the non-null
        // ones satisfy `kafka_producer_Producer_send_batch`'s `# Safety` (records whose
        // `topic` `CString`s outlive the call, out arrays with at least `count` slots), and
        // exactly one null pointer or a negative `count` is passed deliberately to trip the
        // documented precondition assert, which fires before any dereference;
        // `#[ffi_guard]` turns that panic into the -1 return.
        let returned = unsafe { kafka_producer_Producer_send_batch(producer, records, count, out_futures, out_errors) };
        assert_eq!(returned, -1, "a violated precondition must make send_batch return -1");

        let mut caught: Option<Error> = None;
        let returned = common::ffi_guard_or(
            "kafka_producer_Producer_send_batch",
            |error| {
                caught = Some(error);
                -1
            },
            // SAFETY: `send_batch_inner` has the same `# Safety` requirements as
            // `kafka_producer_Producer_send_batch`; the arguments are forwarded unchanged,
            // the deliberately violated precondition panics on the leading asserts before
            // any dereference, and `ffi_guard_or` catches the panic exactly as the entry
            // point's `#[ffi_guard]` would.
            || unsafe { send_batch_inner(producer, records, count, out_futures, out_errors) },
        );
        assert_eq!(returned, -1);
        let error = caught.expect("the violated precondition must panic");
        assert!(
            error.message().contains(expected_msg),
            "Expected an error containing \"{expected_msg}\" but got: \"{}\"",
            error.message()
        );
    }

    #[test]
    fn test_send_batch_null_producer_returns_error() {
        let topic = CString::new("topic").unwrap();
        let records = [kafka_producer_ProducerRecord_t {
            topic: topic.as_ptr(),
            partition: -1,
            timestamp: -1,
            key: std::ptr::null(),
            key_len: -1,
            value: std::ptr::null(),
            value_len: -1,
        }];
        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 1] = [std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 1] = [std::ptr::null_mut()];

        // SAFETY: `assert_send_batch_fails` has the same requirements as
        // `kafka_producer_Producer_send_batch` and forwards the arguments unchanged; null
        // `producer` is passed deliberately so the leading `producer must not be null`
        // assert fires before any dereference, while `records` (with `topic` alive),
        // `futures` and `errors` are one-element locals valid for `count == 1`. No handle
        // is created.
        unsafe {
            assert_send_batch_fails(
                std::ptr::null_mut(),
                records.as_ptr(),
                1,
                futures.as_mut_ptr(),
                errors.as_mut_ptr(),
                "producer must not be null",
            );
        }
    }

    #[test]
    fn test_send_batch_null_records_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 1] = [std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 1] = [std::ptr::null_mut()];

        // SAFETY: `assert_send_batch_fails` has the same requirements as
        // `kafka_producer_Producer_send_batch` and forwards the arguments unchanged;
        // `producer` is the live handle `kafka_producer_MockProducer_new` returned, null
        // `records` is passed deliberately so the `records must not be null` assert fires
        // before any dereference, and `futures`/`errors` are one-element locals valid for
        // `count == 1`. The producer is destroyed once.
        unsafe {
            assert_send_batch_fails(
                producer,
                std::ptr::null(),
                1,
                futures.as_mut_ptr(),
                errors.as_mut_ptr(),
                "records must not be null",
            );
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_batch_null_out_futures_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();
        let records = [kafka_producer_ProducerRecord_t {
            topic: topic.as_ptr(),
            partition: -1,
            timestamp: -1,
            key: std::ptr::null(),
            key_len: -1,
            value: std::ptr::null(),
            value_len: -1,
        }];
        let mut errors: [*mut kafka_common_Error_t; 1] = [std::ptr::null_mut()];

        // SAFETY: `assert_send_batch_fails` has the same requirements as
        // `kafka_producer_Producer_send_batch` and forwards the arguments unchanged;
        // `producer` is the live handle `kafka_producer_MockProducer_new` returned,
        // `records` a one-element local whose `topic` outlives the call, `errors` a
        // one-slot local, and null `out_futures` is passed deliberately so the `out_futures
        // must not be null` assert fires before any dereference. The producer is destroyed
        // once.
        unsafe {
            assert_send_batch_fails(
                producer,
                records.as_ptr(),
                1,
                std::ptr::null_mut(),
                errors.as_mut_ptr(),
                "out_futures must not be null",
            );
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_batch_null_out_errors_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();
        let records = [kafka_producer_ProducerRecord_t {
            topic: topic.as_ptr(),
            partition: -1,
            timestamp: -1,
            key: std::ptr::null(),
            key_len: -1,
            value: std::ptr::null(),
            value_len: -1,
        }];
        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 1] = [std::ptr::null_mut()];

        // SAFETY: `assert_send_batch_fails` has the same requirements as
        // `kafka_producer_Producer_send_batch` and forwards the arguments unchanged;
        // `producer` is the live handle `kafka_producer_MockProducer_new` returned,
        // `records` a one-element local whose `topic` outlives the call, `futures` a
        // one-slot local, and null `out_errors` is passed deliberately so the `out_errors
        // must not be null` assert fires before any dereference. The producer is destroyed
        // once.
        unsafe {
            assert_send_batch_fails(
                producer,
                records.as_ptr(),
                1,
                futures.as_mut_ptr(),
                std::ptr::null_mut(),
                "out_errors must not be null",
            );
            kafka_producer_Producer_destroy(producer);
        }
    }

    /// A negative `count` must fail rather than be clamped: clamping would send
    /// nothing and report success.
    #[test]
    fn test_send_batch_negative_count_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();
        let records = [kafka_producer_ProducerRecord_t {
            topic: topic.as_ptr(),
            partition: -1,
            timestamp: -1,
            key: std::ptr::null(),
            key_len: -1,
            value: std::ptr::null(),
            value_len: -1,
        }];
        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 1] = [std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 1] = [std::ptr::null_mut()];

        // SAFETY: `assert_send_batch_fails` has the same requirements as
        // `kafka_producer_Producer_send_batch` and forwards the arguments unchanged;
        // `producer` is the live handle `kafka_producer_MockProducer_new` returned,
        // `records`, `futures` and `errors` are valid one-element locals, and `count == -1`
        // is passed deliberately so the `count must not be negative` assert fires before
        // any array is read. The producer is destroyed once.
        unsafe {
            assert_send_batch_fails(
                producer,
                records.as_ptr(),
                -1,
                futures.as_mut_ptr(),
                errors.as_mut_ptr(),
                "count must not be negative",
            );
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_batch_zero_count() {
        let producer = kafka_producer_MockProducer_new(true);
        let mut futures: *mut kafka_common_KafkaFuture_RecordMetadata_t = std::ptr::null_mut();
        let mut errors: *mut kafka_common_Error_t = std::ptr::null_mut();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned.
        // With `count == 0` no record or output slot is read or written, so `&dummy_record`
        // (a non-null local whose null `topic` is never dereferenced) and the single-slot
        // `&mut futures`/`&mut errors` satisfy `kafka_producer_Producer_send_batch`'s
        // requirement of at least `count` entries while passing its non-null asserts.
        // `history_count` gets the same live handle and the producer is destroyed once.
        unsafe {
            // Zero count with non-null pointers is valid -- no records sent.
            // Use a dummy non-null pointer for records since count is 0.
            let dummy_record = kafka_producer_ProducerRecord_t {
                topic: std::ptr::null(),
                partition: -1,
                timestamp: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            };
            let sent = kafka_producer_Producer_send_batch(producer, &dummy_record, 0, &mut futures, &mut errors);
            assert_eq!(sent, 0);
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 0);

            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_batch_partial_failure_continues_all() {
        // First record succeeds, second has a null topic (error), third succeeds.
        let producer = kafka_producer_MockProducer_new(true);
        let topic1 = CString::new("topic1").unwrap();
        let topic3 = CString::new("topic3").unwrap();

        let records = [
            kafka_producer_ProducerRecord_t {
                topic: topic1.as_ptr(),
                partition: -1,
                timestamp: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            },
            kafka_producer_ProducerRecord_t {
                topic: std::ptr::null(), // Error at index 1
                partition: -1,
                timestamp: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            },
            kafka_producer_ProducerRecord_t {
                topic: topic3.as_ptr(),
                partition: -1,
                timestamp: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            },
        ];

        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 3] =
            [std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 3] =
            [std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut()];

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `records` holds 3 locals whose `topic1`/`topic3` `CString`s outlive the call,
        // with `records[1].topic` null deliberately to exercise the documented per-record
        // `InvalidRequest` path, and `futures`/`errors` are 3-slot locals matching `count
        // == 3`. The two returned futures and the one returned error are each destroyed
        // exactly once, the remaining slots are asserted null, and the producer is
        // destroyed once.
        unsafe {
            let sent = kafka_producer_Producer_send_batch(
                producer,
                records.as_ptr(),
                3,
                futures.as_mut_ptr(),
                errors.as_mut_ptr(),
            );
            assert_eq!(sent, 2, "Two records should succeed");

            // Index 0: success
            assert!(!futures[0].is_null(), "First future should be valid");
            assert!(errors[0].is_null(), "First error should be null");

            // Index 1: error (null topic)
            assert!(futures[1].is_null(), "Second future should be null");
            assert!(!errors[1].is_null(), "Second error should be non-null");

            // Index 2: success (batch continues past the error)
            assert!(!futures[2].is_null(), "Third future should be valid");
            assert!(errors[2].is_null(), "Third error should be null");

            // Clean up
            kafka_common_KafkaFuture_RecordMetadata_destroy(futures[0]);
            kafka_common_Error_destroy(errors[1]);
            kafka_common_KafkaFuture_RecordMetadata_destroy(futures[2]);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_batch_all_fail() {
        // All records have null topics -- all should fail, none succeed.
        let producer = kafka_producer_MockProducer_new(true);

        let records = [
            kafka_producer_ProducerRecord_t {
                topic: std::ptr::null(),
                partition: -1,
                timestamp: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            },
            kafka_producer_ProducerRecord_t {
                topic: std::ptr::null(),
                partition: -1,
                timestamp: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            },
        ];

        let mut futures: [*mut kafka_common_KafkaFuture_RecordMetadata_t; 2] =
            [std::ptr::null_mut(), std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 2] = [std::ptr::null_mut(), std::ptr::null_mut()];

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // both `records` have a null `topic` deliberately so each takes the documented
        // per-record `InvalidRequest` path, and `futures`/`errors` are 2-slot locals
        // matching `count == 2`. Each returned error is destroyed exactly once, the futures
        // are asserted null, and the producer is destroyed once.
        unsafe {
            let sent = kafka_producer_Producer_send_batch(
                producer,
                records.as_ptr(),
                2,
                futures.as_mut_ptr(),
                errors.as_mut_ptr(),
            );
            assert_eq!(sent, 0);

            for i in 0..2 {
                assert!(futures[i].is_null(), "futures[{i}] should be null");
                assert!(!errors[i].is_null(), "errors[{i}] should be non-null");
                kafka_common_Error_destroy(errors[i]);
            }

            kafka_producer_Producer_destroy(producer);
        }
    }

    // -- Future tests -------------------------------------------------------

    #[test]
    fn test_future_get_success() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("my-topic").unwrap();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned,
        // `topic` an owned `CString` alive for the call, and `&mut err` writable locals.
        // `kafka_common_KafkaFuture_RecordMetadata_get` requires a valid future or null and
        // gets the future `send` returned; the `RecordMetadata_*` accessors get the
        // non-null metadata it returned (asserted), and `topic_ptr` is read before that
        // handle is destroyed. Metadata, future and producer are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                0,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_success(err);

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let metadata = kafka_common_KafkaFuture_RecordMetadata_get(future, &mut err);
            assert_success(err);
            assert!(!metadata.is_null());

            assert_eq!(kafka_producer_RecordMetadata_offset(metadata), 0);
            assert_eq!(kafka_producer_RecordMetadata_partition(metadata), 0);

            // Check topic
            let topic_ptr = kafka_producer_RecordMetadata_topic(metadata);
            assert!(!topic_ptr.is_null());
            let topic_str = CStr::from_ptr(topic_ptr).to_str().unwrap();
            assert_eq!(topic_str, "my-topic");

            kafka_producer_RecordMetadata_destroy(metadata);
            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_future_get_error() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live manual-completion handle
        // `kafka_producer_MockProducer_new(false)` returned, and `topic`/`err_msg` are
        // owned `CString`s alive for the calls, satisfying
        // `kafka_producer_MockProducer_error_next`'s `# Safety` (valid handle, valid or
        // null message). `get` receives the future `send` returned and reports the
        // installed error through `&mut err`, a writable local whose non-null handle is
        // read via `kafka_common_Error_code`/`_message` before its single destroy; the
        // future and the producer are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_success(err);

            let err_msg = CString::new("test error").unwrap();
            kafka_producer_MockProducer_error_next(
                producer,
                i32::from(Errors::CorruptMessage.code()),
                err_msg.as_ptr(),
            );

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let metadata = kafka_common_KafkaFuture_RecordMetadata_get(future, &mut err);
            assert!(!err.is_null(), "Expected an error from future get");
            assert_eq!(kafka_common_Error_code(err), kafka_common_ErrorCode_CORRUPT_MESSAGE);

            // Verify the error message is accessible
            let msg_ptr = kafka_common_Error_message(err);
            assert!(!msg_ptr.is_null());

            assert!(metadata.is_null());

            kafka_common_Error_destroy(err);
            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_future_is_done_null() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_common_KafkaFuture_RecordMetadata_is_done` is specified to return `false`
        // for a null future.
        unsafe {
            assert!(!kafka_common_KafkaFuture_RecordMetadata_is_done(std::ptr::null_mut()));
        }
    }

    #[test]
    fn test_future_get_null_params() {
        // SAFETY: Null `future` is passed deliberately to exercise the documented failure
        // path: `kafka_common_KafkaFuture_RecordMetadata_get` accepts null and reports
        // `InvalidRequest` through `out_error`; `&mut err` is a writable local and
        // `assert_error` destroys the returned error once. No metadata handle is created.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let metadata = kafka_common_KafkaFuture_RecordMetadata_get(std::ptr::null_mut(), &mut err);
            assert_error(err);
            assert!(metadata.is_null());
        }
    }

    #[test]
    fn test_future_destroy_null() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_common_KafkaFuture_RecordMetadata_destroy` is specified as a no-op for a
        // null future.
        unsafe {
            kafka_common_KafkaFuture_RecordMetadata_destroy(std::ptr::null_mut());
        }
    }

    // -- RecordMetadata tests -----------------------------------------------

    #[test]
    fn test_metadata_null_returns_defaults() {
        // SAFETY: Null is passed deliberately to exercise the documented null paths: the
        // `kafka_producer_RecordMetadata_*` accessors accept null and return `-1`/`-1`/null
        // without dereferencing it.
        unsafe {
            assert_eq!(kafka_producer_RecordMetadata_offset(std::ptr::null()), -1);
            assert_eq!(kafka_producer_RecordMetadata_partition(std::ptr::null()), -1);
            assert!(kafka_producer_RecordMetadata_topic(std::ptr::null()).is_null());
        }
    }

    #[test]
    fn test_metadata_destroy_null() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_producer_RecordMetadata_destroy` is specified as a no-op for a null
        // handle.
        unsafe {
            kafka_producer_RecordMetadata_destroy(std::ptr::null_mut());
        }
    }

    // -- Flush and close tests ----------------------------------------------

    #[test]
    fn test_flush() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live manual-completion handle
        // `kafka_producer_MockProducer_new(false)` returned and `topic` an owned `CString`
        // alive for the call; `&mut err` are writable locals.
        // `kafka_producer_Producer_flush` requires a valid handle or null and gets the live
        // one, `is_done` gets the future `send` returned, and the future and the producer
        // are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_success(err);

            assert!(!kafka_common_KafkaFuture_RecordMetadata_is_done(future));

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_flush(producer, &mut err);
            assert_success(err);

            assert!(kafka_common_KafkaFuture_RecordMetadata_is_done(future));

            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_flush_null() {
        // SAFETY: Null `producer` is passed deliberately to exercise the documented failure
        // path: `kafka_producer_Producer_flush` accepts null and reports `InvalidRequest`
        // through `out_error`; `&mut err` is a writable local and `assert_error` destroys
        // the returned error once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_flush(std::ptr::null_mut(), &mut err);
            assert_error(err);
        }
    }

    #[test]
    fn test_close_and_send_fails() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `topic` an owned `CString` alive for the call; `&mut err` are writable
        // locals. `close` does not free the handle, so the subsequent `send` on the closed
        // producer is valid and takes the documented error path (`assert_error` destroys
        // that error once, the future is asserted null), and the producer is destroyed
        // once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(producer, &mut err);
            assert_success(err);

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_error(err);
            assert!(future.is_null());

            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_close_null() {
        // SAFETY: Null `producer` is passed deliberately to exercise the documented null
        // path: `kafka_producer_Producer_close` treats null as a no-op success and writes
        // null into `*out_error`; `&mut err` is a writable local that `assert_success` only
        // reads.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(std::ptr::null_mut(), &mut err);
            assert_success(err);
        }
    }

    // -- Mock-specific tests ------------------------------------------------

    #[test]
    fn test_mock_complete_next_no_pending() {
        let producer = kafka_producer_MockProducer_new(false);
        // SAFETY: `producer` is the live manual-completion handle
        // `kafka_producer_MockProducer_new(false)` returned, satisfying
        // `kafka_producer_MockProducer_complete_next`'s `# Safety` (valid handle or null);
        // it is destroyed once afterwards.
        unsafe {
            assert!(!kafka_producer_MockProducer_complete_next(producer));
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_complete_next_null() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_producer_MockProducer_complete_next` accepts null and returns `false`.
        unsafe {
            assert!(!kafka_producer_MockProducer_complete_next(std::ptr::null_mut()));
        }
    }

    #[test]
    fn test_mock_error_next_no_pending() {
        let producer = kafka_producer_MockProducer_new(false);
        let msg = CString::new("err").unwrap();
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new(false)`
        // returned and `msg` an owned `CString` alive for the call, satisfying
        // `kafka_producer_MockProducer_error_next`'s `# Safety` (valid handle, valid or
        // null message); the producer is destroyed once afterwards.
        unsafe {
            assert!(!kafka_producer_MockProducer_error_next(producer, 2, msg.as_ptr()));
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_error_next_null_message() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live manual-completion handle
        // `kafka_producer_MockProducer_new(false)` returned and `topic` an owned `CString`
        // alive for the call; `&mut err` are writable locals. `error_next` gets the live
        // handle with a null `error_message`, which its `# Safety` allows (default
        // message); `get` receives the future `send` returned and `assert_error` destroys
        // the reported error once; the future and the producer are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_success(err);

            // Error with null message -- should use default message.
            assert!(kafka_producer_MockProducer_error_next(
                producer,
                i32::from(Errors::CorruptMessage.code()),
                std::ptr::null(),
            ));

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let metadata = kafka_common_KafkaFuture_RecordMetadata_get(future, &mut err);
            assert_error(err);
            assert!(metadata.is_null());

            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_error_next_null_producer() {
        let msg = CString::new("err").unwrap();
        // SAFETY: Null `producer` is passed deliberately:
        // `kafka_producer_MockProducer_error_next` null-checks `producer` in its body and
        // returns `false`, so the block relies on that check rather than on its `# Safety`
        // section (which only states that `producer` must be a valid handle). `msg` is an
        // owned `CString` alive for the call.
        unsafe {
            assert!(!kafka_producer_MockProducer_error_next(std::ptr::null_mut(), 2, msg.as_ptr()));
        }
    }

    #[test]
    fn test_mock_history_count() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `topic` an owned `CString` alive for both sends; `&mut err` are writable
        // locals. `kafka_producer_MockProducer_history_count` requires a valid handle or
        // null and gets the live one each time; the two returned futures `f1`/`f2` are each
        // destroyed once, then the producer is destroyed once.
        unsafe {
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 0);

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let f1 = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 1);

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let f2 = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 2);

            kafka_common_KafkaFuture_RecordMetadata_destroy(f1);
            kafka_common_KafkaFuture_RecordMetadata_destroy(f2);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_history_count_null() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_producer_MockProducer_history_count` accepts null and returns `0`.
        unsafe {
            assert_eq!(kafka_producer_MockProducer_history_count(std::ptr::null()), 0);
        }
    }

    #[test]
    fn test_mock_clear() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `topic` an owned `CString` alive for the call; `&mut err` is a writable
        // local. `history_count` and `kafka_producer_MockProducer_clear` require a valid
        // handle or null and get the live one; the returned future `f` and the producer are
        // each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let f = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 1);

            kafka_producer_MockProducer_clear(producer);
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 0);

            kafka_common_KafkaFuture_RecordMetadata_destroy(f);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_clear_null() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_producer_MockProducer_clear` accepts null as a no-op.
        unsafe {
            kafka_producer_MockProducer_clear(std::ptr::null_mut());
        }
    }

    // -- Error handle tests -------------------------------------------------

    #[test]
    fn test_error_code_and_message() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `close` is given a null `out_error`, which its docs allow, and does not free the
        // handle. `topic` is an owned `CString` alive for the send and `&mut err` a
        // writable local; the send on the closed producer takes the documented error path,
        // so `err` is a non-null owned handle (asserted) that
        // `kafka_common_Error_code`/`_message` read, with `msg_ptr` consumed before its
        // single `kafka_common_Error_destroy`. The future is asserted null and the producer
        // is destroyed once.
        unsafe {
            // Close and then try to send -- should produce an error handle
            kafka_producer_Producer_close(producer, std::ptr::null_mut());

            let topic = CString::new("topic").unwrap();
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert!(!err.is_null());
            assert!(future.is_null());

            // Verify we can get a code and message from the error handle. Sending
            // on a closed producer is `MockProducer.verify_not_closed`'s
            // `IllegalStateException`, which the code space now names outright
            // instead of collapsing it onto -1.
            let code = kafka_common_Error_code(err);
            assert_eq!(code, kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE);

            let msg_ptr = kafka_common_Error_message(err);
            assert!(!msg_ptr.is_null());
            let msg = CStr::from_ptr(msg_ptr).to_str().unwrap();
            assert!(!msg.is_empty(), "Error message should not be empty");

            kafka_common_Error_destroy(err);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_error_is_retriable_error() {
        // Create an error by sending to a closed producer
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `close` is given a null `out_error`, which its docs allow, and does not free the
        // handle. `topic` is an owned `CString` alive for the send and `&mut err` a
        // writable local; the send on the closed producer takes the documented error path,
        // so `err` is a non-null owned handle (asserted) that
        // `kafka_common_Error_is_retriable_error` only reads before its single destroy, and
        // the returned future is null. The producer is destroyed once.
        unsafe {
            kafka_producer_Producer_close(producer, std::ptr::null_mut());

            let topic = CString::new("topic").unwrap();
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let _future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert!(!err.is_null());

            // Just verify the function is callable and returns a boolean.
            let _retriable = kafka_common_Error_is_retriable_error(err);

            kafka_common_Error_destroy(err);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_error_null_safety() {
        // SAFETY: Null is passed deliberately to every call to exercise the documented null
        // paths: `kafka_common_Error_code`, `_message`, `_is_retriable_error` and
        // `_destroy` each accept null and return `NONE`/null/`false`/no-op without
        // dereferencing it.
        unsafe {
            assert_eq!(kafka_common_Error_code(std::ptr::null()), kafka_common_ErrorCode_NONE);
            assert!(kafka_common_Error_message(std::ptr::null()).is_null());
            assert!(!kafka_common_Error_is_retriable_error(std::ptr::null()));
            kafka_common_Error_destroy(std::ptr::null_mut()); // no-op
        }
    }

    // -- Integration-style round-trip tests ---------------------------------

    #[test]
    fn test_full_send_get_destroy_cycle() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("round-trip").unwrap();
        let key = b"my-key";
        let value = b"my-value";

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `topic` is an owned `CString` and `key`/`value` static byte literals passed with
        // their exact lengths, all alive for the send, and `&mut err` are writable locals.
        // `get` receives the future `send` returned, the `RecordMetadata_*` accessors and
        // `topic_ptr` use the returned metadata before it is destroyed, `history_count`
        // gets the live handle, and metadata, future and producer are each destroyed once.
        unsafe {
            // Send
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                2,
                -1, // timestamp
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
                &mut err,
            );
            assert_success(err);

            // Get metadata
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let metadata = kafka_common_KafkaFuture_RecordMetadata_get(future, &mut err);
            assert_success(err);

            // Verify metadata
            assert_eq!(kafka_producer_RecordMetadata_offset(metadata), 0);
            assert_eq!(kafka_producer_RecordMetadata_partition(metadata), 2);
            let topic_ptr = kafka_producer_RecordMetadata_topic(metadata);
            let topic_str = CStr::from_ptr(topic_ptr).to_str().unwrap();
            assert_eq!(topic_str, "round-trip");

            // History
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 1);

            // Clean up
            kafka_producer_RecordMetadata_destroy(metadata);
            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_multiple_sends_incrementing_offsets() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `topic` an owned `CString` alive for every iteration; `&mut err` are writable
        // locals. In each iteration `get` receives the future `send` returned,
        // `RecordMetadata_offset` reads the returned metadata, and that metadata and future
        // are destroyed exactly once; the producer is destroyed once after the loop.
        unsafe {
            for expected_offset in 0..3_i64 {
                let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
                let future = kafka_producer_Producer_send(
                    producer,
                    topic.as_ptr(),
                    0,
                    -1, // timestamp
                    std::ptr::null(),
                    -1,
                    std::ptr::null(),
                    -1,
                    &mut err,
                );
                assert_success(err);

                let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
                let metadata = kafka_common_KafkaFuture_RecordMetadata_get(future, &mut err);
                assert_success(err);
                assert_eq!(kafka_producer_RecordMetadata_offset(metadata), expected_offset);

                kafka_producer_RecordMetadata_destroy(metadata);
                kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            }

            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_after_close_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `topic` an owned `CString` alive for the call; `&mut err` are writable
        // locals. `close` does not free the handle, so the subsequent `send` is valid and
        // takes the documented error path: `assert_error` destroys that error once, the
        // future is asserted null, and the producer is destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(producer, &mut err);
            assert_success(err);

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_error(err);
            assert!(future.is_null(), "Future should be null on error");

            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_flush_after_close_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned
        // and `&mut err` are writable locals. `close` does not free the handle, so the
        // subsequent `flush` is valid and takes the documented error path; `assert_error`
        // destroys that error once and the producer is destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(producer, &mut err);
            assert_success(err);

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_flush(producer, &mut err);
            assert_error(err);

            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_only_key() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();
        let key = b"only-key";

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `topic` is an owned `CString` and `key` a static byte literal passed with its
        // exact length, both alive for the call, while null `value` with length `-1` is the
        // documented no-value form; `&mut err` is a writable local. `is_done` gets the
        // returned future, and the future and the producer are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                key.as_ptr(),
                key.len() as i32,
                std::ptr::null(),
                -1,
                &mut err,
            );
            assert_success(err);
            assert!(kafka_common_KafkaFuture_RecordMetadata_is_done(future));

            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    // -- ProducerProperties tests ---------------------------------------------

    #[test]
    fn test_properties_new_and_put() {
        // SAFETY: `props` is the non-null handle `kafka_producer_ProducerProperties_new`
        // returned (asserted); `put` requires that handle plus valid C strings, and
        // `key`/`val` are owned `CString`s alive for the call.
        // `kafka_producer_KafkaProducer_new` requires a valid non-null `props` and a
        // writable or null `out_error`, both satisfied, and does not retain `props`, so
        // `props` is destroyed once right after; the producer is closed and destroyed once
        // each, with `assert_success` consuming any error.
        unsafe {
            let props = kafka_producer_ProducerProperties_new();
            assert!(!props.is_null());

            let key = CString::new("bootstrap.servers").unwrap();
            let val = CString::new("localhost:9092").unwrap();
            kafka_producer_ProducerProperties_put(props, key.as_ptr(), val.as_ptr());

            // Create a producer from the properties to verify they work.
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let producer = kafka_producer_KafkaProducer_new(props, &mut err);
            assert_success(err);
            assert!(!producer.is_null());

            kafka_producer_ProducerProperties_destroy(props);
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(producer, &mut err);
            assert_success(err);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_properties_from_configs() {
        let k1 = CString::new("bootstrap.servers").unwrap();
        let v1 = CString::new("localhost:9092").unwrap();
        let k2 = CString::new("client.id").unwrap();
        let v2 = CString::new("test-client").unwrap();

        // SAFETY: `kafka_producer_ProducerProperties_from_configs` requires null or a
        // NULL-terminated array of valid C strings: `configs` is a local array of pointers
        // to the owned `CString`s `k1`..`v2`, alive for the call, ending in null. The
        // returned `props` is asserted non-null before `kafka_producer_KafkaProducer_new`
        // (which does not retain it), then destroyed once; the producer is closed and
        // destroyed once each, with `assert_success` consuming any error.
        unsafe {
            let configs = [k1.as_ptr(), v1.as_ptr(), k2.as_ptr(), v2.as_ptr(), std::ptr::null()];
            let props = kafka_producer_ProducerProperties_from_configs(configs.as_ptr());
            assert!(!props.is_null());

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let producer = kafka_producer_KafkaProducer_new(props, &mut err);
            assert_success(err);
            assert!(!producer.is_null());

            kafka_producer_ProducerProperties_destroy(props);
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(producer, &mut err);
            assert_success(err);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_properties_from_configs_null() {
        // SAFETY: Null `configs` is passed deliberately to exercise the documented null
        // path: `kafka_producer_ProducerProperties_from_configs` returns null without
        // reading anything, so no handle is created.
        unsafe {
            let props = kafka_producer_ProducerProperties_from_configs(std::ptr::null());
            assert!(props.is_null());
        }
    }

    #[test]
    fn test_properties_from_configs_odd_count() {
        let k1 = CString::new("bootstrap.servers").unwrap();
        let v1 = CString::new("localhost:9092").unwrap();
        let k2 = CString::new("client.id").unwrap();
        // Missing value for k2 — odd number of entries before NULL.
        // SAFETY: `configs` is a local NULL-terminated array of pointers to the owned
        // `CString`s `k1`/`v1`/`k2`, alive for the call, as `from_configs`'s `# Safety`
        // requires; the odd entry count is deliberate to exercise the documented null
        // return, so no handle is created.
        unsafe {
            let configs = [k1.as_ptr(), v1.as_ptr(), k2.as_ptr(), std::ptr::null()];
            let props = kafka_producer_ProducerProperties_from_configs(configs.as_ptr());
            assert!(props.is_null());
        }
    }

    #[test]
    fn test_properties_from_configs_empty() {
        // SAFETY: `configs` is a local array holding only the NULL terminator, a valid
        // NULL-terminated array per `from_configs`'s `# Safety`; the returned `props` is
        // asserted non-null and destroyed once.
        unsafe {
            let configs = [std::ptr::null()];
            let props = kafka_producer_ProducerProperties_from_configs(configs.as_ptr());
            assert!(!props.is_null());
            kafka_producer_ProducerProperties_destroy(props);
        }
    }

    #[test]
    fn test_properties_put_null_is_noop() {
        let key = CString::new("key").unwrap();
        let val = CString::new("val").unwrap();
        // SAFETY: Each `kafka_producer_ProducerProperties_put` call passes one null
        // argument deliberately to exercise the documented no-op path (its docs state it is
        // a no-op if any parameter is null); `key`/`val` are owned `CString`s alive for the
        // calls, and `props` is the handle `kafka_producer_ProducerProperties_new`
        // returned, destroyed once at the end.
        unsafe {
            // All null combinations are no-ops.
            kafka_producer_ProducerProperties_put(std::ptr::null_mut(), key.as_ptr(), val.as_ptr());
            let props = kafka_producer_ProducerProperties_new();
            kafka_producer_ProducerProperties_put(props, std::ptr::null(), val.as_ptr());
            kafka_producer_ProducerProperties_put(props, key.as_ptr(), std::ptr::null());
            kafka_producer_ProducerProperties_destroy(props);
        }
    }

    #[test]
    fn test_properties_destroy_null() {
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_producer_ProducerProperties_destroy` is specified as a no-op for a null
        // handle.
        unsafe {
            kafka_producer_ProducerProperties_destroy(std::ptr::null_mut());
        }
    }

    // -- KafkaProducer lifecycle tests ----------------------------------------

    /// Helper: creates a KafkaProducer via FFI with the given bootstrap servers.
    fn create_kafka_producer(bootstrap: &str) -> (*mut kafka_producer_Producer_t, *mut kafka_common_Error_t) {
        let key = CString::new("bootstrap.servers").unwrap();
        let val = CString::new(bootstrap).unwrap();
        let configs = [key.as_ptr(), val.as_ptr(), std::ptr::null()];
        // SAFETY: `kafka_producer_ProducerProperties_from_configs` requires null or a
        // NULL-terminated array of valid C strings: `configs` is a local array of pointers
        // to the owned `CString`s `key`/`val`, alive for the call, ending in null.
        let props = unsafe { kafka_producer_ProducerProperties_from_configs(configs.as_ptr()) };
        let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: `kafka_producer_KafkaProducer_new` requires a valid non-null `props` and
        // a writable or null `out_error`: `props` is the handle `from_configs` returned for
        // a well-formed single pair (non-null for an even, well-formed list), and `&mut
        // err` is a writable local. Ownership of the returned producer and error handles
        // passes to the caller of this helper.
        let producer = unsafe { kafka_producer_KafkaProducer_new(props, &mut err) };
        // SAFETY: `props` is the handle `from_configs` returned above;
        // `kafka_producer_KafkaProducer_new` does not retain it (the caller keeps ownership
        // per its docs), so this is its single destroy, and `ProducerProperties_destroy`
        // tolerates null anyway.
        unsafe { kafka_producer_ProducerProperties_destroy(props) };
        (producer, err)
    }

    #[test]
    fn test_create_and_destroy_kafka_producer() {
        // SAFETY: `create_kafka_producer` hands back the owned `producer` and `err` handles
        // `kafka_producer_KafkaProducer_new` returned: `assert_success` consumes `err` if
        // non-null, `producer` is asserted non-null, `&mut err` is a writable local, and
        // the producer is closed and destroyed once each.
        unsafe {
            let (producer, err) = create_kafka_producer("localhost:9092");
            assert_success(err);
            assert!(!producer.is_null());

            // Close before destroy
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(producer, &mut err);
            assert_success(err);

            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_create_kafka_producer_null_props() {
        // SAFETY: Null `props` is passed deliberately: `kafka_producer_KafkaProducer_new`
        // null-checks `props` in its body and reports `InvalidRequest` through `out_error`,
        // so the block relies on that check rather than on its `# Safety` section (which
        // requires a non-null handle). `&mut err` is a writable local, `assert_error`
        // destroys the returned error once, and no producer is created.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let producer = kafka_producer_KafkaProducer_new(std::ptr::null(), &mut err);
            assert_error(err);
            assert!(producer.is_null());
        }
    }

    #[test]
    fn test_create_kafka_producer_null_out_error() {
        // SAFETY: `props` is the handle `kafka_producer_ProducerProperties_new` returned
        // and `key`/`val` are owned `CString`s alive for `put`.
        // `kafka_producer_KafkaProducer_new` and `kafka_producer_Producer_close` are given
        // a null `out_error`, which their docs allow (error details not wanted); the
        // producer is asserted non-null, `props` is destroyed once (not retained by the
        // producer), and the producer is closed and destroyed once each.
        unsafe {
            let key = CString::new("bootstrap.servers").unwrap();
            let val = CString::new("localhost:9092").unwrap();
            let props = kafka_producer_ProducerProperties_new();
            kafka_producer_ProducerProperties_put(props, key.as_ptr(), val.as_ptr());
            // Passing null for out_error means "don't care about error details".
            let producer = kafka_producer_KafkaProducer_new(props, std::ptr::null_mut());
            assert!(!producer.is_null());
            kafka_producer_ProducerProperties_destroy(props);
            kafka_producer_Producer_close(producer, std::ptr::null_mut());
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_create_kafka_producer_invalid_config_value() {
        let key = CString::new("batch.size").unwrap();
        let val = CString::new("not-a-number").unwrap();
        // SAFETY: `configs` is a local NULL-terminated array of pointers to the owned
        // `CString`s `key`/`val`, alive for the call; `props` is the resulting handle
        // (non-null for one well-formed pair). `kafka_producer_KafkaProducer_new` fails on
        // the invalid value and reports through `&mut err`, a writable local, which
        // `assert_error` destroys once; `producer` is null and `props` is destroyed once.
        unsafe {
            let configs = [key.as_ptr(), val.as_ptr(), std::ptr::null()];
            let props = kafka_producer_ProducerProperties_from_configs(configs.as_ptr());
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let producer = kafka_producer_KafkaProducer_new(props, &mut err);
            assert_error(err);
            assert!(producer.is_null());
            kafka_producer_ProducerProperties_destroy(props);
        }
    }

    #[test]
    fn test_kafka_producer_mock_ops_return_defaults() {
        // SAFETY: `create_kafka_producer` hands back the owned `producer` and `err` handles
        // from `kafka_producer_KafkaProducer_new`; `assert_success` consumes `err` and
        // `producer` is asserted non-null. The `kafka_producer_MockProducer_*` calls get
        // that valid handle (and a null `error_message`, which `error_next`'s `# Safety`
        // allows) and fall through for a `ProducerKind::Kafka`; `&mut err` is a writable
        // local and the producer is closed and destroyed once each.
        unsafe {
            let (producer, err) = create_kafka_producer("localhost:9092");
            assert_success(err);
            assert!(!producer.is_null());

            // Mock-specific operations should return no-op values for Kafka producer.
            assert!(!kafka_producer_MockProducer_complete_next(producer));
            assert!(!kafka_producer_MockProducer_error_next(producer, 2, std::ptr::null()));
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 0);
            kafka_producer_MockProducer_clear(producer); // no-op

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_close(producer, &mut err);
            assert_success(err);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_only_value() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();
        let value = b"only-value";

        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // `topic` is an owned `CString` and `value` a static byte literal passed with its
        // exact length, both alive for the call, while null `key` with length `-1` is the
        // documented no-key form; `&mut err` is a writable local. `is_done` gets the
        // returned future, and the future and the producer are each destroyed once.
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(
                producer,
                topic.as_ptr(),
                -1,
                -1, // timestamp
                std::ptr::null(),
                -1,
                value.as_ptr(),
                value.len() as i32,
                &mut err,
            );
            assert_success(err);
            assert!(kafka_common_KafkaFuture_RecordMetadata_is_done(future));

            kafka_common_KafkaFuture_RecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    // -- Metrics tests ------------------------------------------------------

    fn metric_fixture(
        name: &str,
        tags: &[(&str, &str)],
        provider: crate::common::metrics::MetricValueProvider,
    ) -> (MetricName, Arc<KafkaMetric>) {
        use crate::common::metrics::MetricConfig;
        use crate::common::utils::SystemTime;
        use std::collections::BTreeMap;
        let tag_map: BTreeMap<String, String> = tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let mn = MetricName::new(name, "grp", "desc", tag_map);
        let km = KafkaMetric::new(mn.clone(), provider, Arc::new(MetricConfig::new()), Arc::new(SystemTime));
        (mn, Arc::new(km))
    }

    /// Every `MetricValue` variant round-trips through the producer metric-map
    /// to the matching kind + `get_value_*` accessor, and tags are flattened in
    /// order. Mirrors the consumer FFI's metric-map round-trip test.
    #[test]
    fn metric_map_carries_all_value_kinds_and_tags() {
        use crate::common::MetricValue;
        use crate::common::metrics::{ClosureGauge, ClosureMeasurable, MetricValueProvider};

        let mut metrics: HashMap<MetricName, Arc<KafkaMetric>> = HashMap::new();
        let (measurable_name, m) = metric_fixture(
            "measurable",
            &[("client-id", "c1"), ("topic", "t")],
            MetricValueProvider::Measurable(Box::new(ClosureMeasurable::new(|_, _| 42.5))),
        );
        metrics.insert(measurable_name, m);
        for (name, value) in [
            ("as-string", MetricValue::String("hello".to_string())),
            ("as-long", MetricValue::Long(-9_000_000_000)),
            ("as-int", MetricValue::Int(-7)),
        ] {
            let v = value.clone();
            let (n, m) = metric_fixture(
                name,
                &[],
                MetricValueProvider::Gauge(Box::new(ClosureGauge::new(move |_, _| v.clone()))),
            );
            metrics.insert(n, m);
        }

        let map = Box::into_raw(common::build_metric_map_inner(metrics)) as *mut kafka_producer_MetricMap_t;
        // SAFETY: `kafka_producer_MetricMap_count` requires a valid metric-map handle:
        // `map` was leaked by `Box::into_raw(common::build_metric_map_inner(metrics))` and
        // cast to the opaque type exactly as `kafka_producer_Producer_metrics` produces
        // one, and stays alive until `kafka_producer_MetricMap_destroy(map)` at the end of
        // the test.
        assert_eq!(unsafe { kafka_producer_MetricMap_count(map) }, 4);

        let mut seen = 0;
        for i in 0..4 {
            // SAFETY: `map` is the live metric-map handle leaked above;
            // `kafka_producer_MetricMap_get_name` returns a borrowed pointer valid until
            // the map is destroyed, or null when out of range, and `i` in `0..4` with
            // `count` asserted 4 keeps it in range, so the pointer is non-null; the string
            // is copied out (`to_string`) before `destroy`.
            let name = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_name(map, i)) }
                .to_str()
                .unwrap()
                .to_string();
            // SAFETY: `kafka_producer_MetricMap_get_value_kind` requires a valid metric-map
            // handle: `map` is the live handle leaked above, and `i` is in range (the
            // accessor defaults for an out-of-range index anyway).
            let kind = unsafe { kafka_producer_MetricMap_get_value_kind(map, i) };
            // SAFETY: `map` is the live metric-map handle leaked above;
            // `kafka_producer_MetricMap_get_group` returns a borrowed pointer valid until
            // the map is destroyed, non-null because `i` in `0..4` is in range of the 4
            // entries, and `group` is used only within this loop iteration, before
            // `destroy`.
            let group = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_group(map, i)) };
            assert_eq!(group.to_str().unwrap(), "grp");
            // SAFETY: `map` is the live metric-map handle leaked above;
            // `kafka_producer_MetricMap_get_description` returns a borrowed pointer valid
            // until the map is destroyed, non-null because `i` in `0..4` is in range of the
            // 4 entries, and `desc` is used only within this loop iteration, before
            // `destroy`.
            let desc = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_description(map, i)) };
            assert_eq!(desc.to_str().unwrap(), "desc");
            match name.as_str() {
                "measurable" => {
                    assert_eq!(kind, common::METRIC_VALUE_DOUBLE);
                    // SAFETY: `kafka_producer_MetricMap_get_value_double` requires a valid
                    // metric-map handle: `map` is the live handle leaked above and `i` is
                    // in range.
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_value_double(map, i) }, 42.5);
                    // Tags are sorted (BTreeMap): client-id then topic.
                    // SAFETY: `kafka_producer_MetricMap_get_tag_count` requires a valid
                    // metric-map handle: `map` is the live handle leaked above and `i` is
                    // in range.
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_tag_count(map, i) }, 2);
                    // SAFETY: `map` is the live metric-map handle leaked above; the entry
                    // at `i` is the one built with 2 tags (`get_tag_count` asserted 2), so
                    // `kafka_producer_MetricMap_get_tag_key(map, i, 0)` returns a non-null
                    // borrowed pointer valid until the map is destroyed, and `k0` is used
                    // only within this iteration.
                    let k0 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_key(map, i, 0)) };
                    // SAFETY: `map` is the live metric-map handle leaked above; the entry
                    // at `i` has 2 tags (asserted), so
                    // `kafka_producer_MetricMap_get_tag_value(map, i, 0)` returns a
                    // non-null borrowed pointer valid until the map is destroyed, and `v0`
                    // is used only within this iteration.
                    let v0 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_value(map, i, 0)) };
                    assert_eq!((k0.to_str().unwrap(), v0.to_str().unwrap()), ("client-id", "c1"));
                    // SAFETY: `map` is the live metric-map handle leaked above; the entry
                    // at `i` has 2 tags (asserted), so
                    // `kafka_producer_MetricMap_get_tag_key(map, i, 1)` returns a non-null
                    // borrowed pointer valid until the map is destroyed, and `k1` is used
                    // only within this iteration.
                    let k1 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_key(map, i, 1)) };
                    // SAFETY: `map` is the live metric-map handle leaked above; the entry
                    // at `i` has 2 tags (asserted), so
                    // `kafka_producer_MetricMap_get_tag_value(map, i, 1)` returns a
                    // non-null borrowed pointer valid until the map is destroyed, and `v1`
                    // is used only within this iteration.
                    let v1 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_value(map, i, 1)) };
                    assert_eq!((k1.to_str().unwrap(), v1.to_str().unwrap()), ("topic", "t"));
                },
                "as-string" => {
                    assert_eq!(kind, common::METRIC_VALUE_STRING);
                    // SAFETY: `map` is the live metric-map handle leaked above; the entry
                    // at `i` is the `String`-kind metric (`kind` asserted), so
                    // `kafka_producer_MetricMap_get_value_string(map, i)` returns a
                    // non-null borrowed pointer valid until the map is destroyed, and `s`
                    // is used only within this iteration.
                    let s = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_value_string(map, i)) };
                    assert_eq!(s.to_str().unwrap(), "hello");
                    // SAFETY: `kafka_producer_MetricMap_get_tag_count` requires a valid
                    // metric-map handle: `map` is the live handle leaked above and `i` is
                    // in range.
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_tag_count(map, i) }, 0);
                },
                "as-long" => {
                    assert_eq!(kind, common::METRIC_VALUE_LONG);
                    // SAFETY: `kafka_producer_MetricMap_get_value_long` requires a valid
                    // metric-map handle: `map` is the live handle leaked above and `i` is
                    // in range.
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_value_long(map, i) }, -9_000_000_000);
                },
                "as-int" => {
                    assert_eq!(kind, common::METRIC_VALUE_INT);
                    // SAFETY: `kafka_producer_MetricMap_get_value_int` requires a valid
                    // metric-map handle: `map` is the live handle leaked above and `i` is
                    // in range.
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_value_int(map, i) }, -7);
                },
                other => panic!("unexpected metric name {other}"),
            }
            seen += 1;
        }
        assert_eq!(seen, 4);

        // SAFETY: `map` was leaked by `Box::into_raw(common::build_metric_map_inner(..))`
        // above, so `kafka_producer_MetricMap_destroy` is its matching single free; every
        // borrowed pointer the accessors handed out was consumed inside the loop, so
        // nothing uses the map afterwards.
        unsafe { kafka_producer_MetricMap_destroy(map) };
    }

    /// Out-of-range indices are reported rather than panicking, matching the
    /// other `*_get_*` accessors.
    #[test]
    fn metric_map_out_of_range_accessors_are_safe() {
        let map = Box::into_raw(common::build_metric_map_inner(HashMap::new())) as *mut kafka_producer_MetricMap_t;
        // SAFETY: `kafka_producer_MetricMap_count` requires a valid metric-map handle:
        // `map` was leaked by
        // `Box::into_raw(common::build_metric_map_inner(HashMap::new()))`, exactly as
        // `kafka_producer_Producer_metrics` produces one, and stays alive until the
        // `destroy` below.
        assert_eq!(unsafe { kafka_producer_MetricMap_count(map) }, 0);
        // SAFETY: `map` is the live empty metric-map handle leaked above; index `0` is out
        // of range deliberately, and `kafka_producer_MetricMap_get_name` is documented to
        // return null rather than read past the entries.
        assert!(unsafe { kafka_producer_MetricMap_get_name(map, 0) }.is_null());
        // SAFETY: `map` is the live empty metric-map handle leaked above; index `-1` is out
        // of range deliberately, and `kafka_producer_MetricMap_get_name` is documented to
        // return null rather than read past the entries.
        assert!(unsafe { kafka_producer_MetricMap_get_name(map, -1) }.is_null());
        // SAFETY: `map` is the live empty metric-map handle leaked above; index `5` is out
        // of range deliberately, and `kafka_producer_MetricMap_get_value_string` is
        // documented to return null rather than read past the entries.
        assert!(unsafe { kafka_producer_MetricMap_get_value_string(map, 5) }.is_null());
        // SAFETY: `map` is the live empty metric-map handle leaked above; index `0` is out
        // of range deliberately, and `kafka_producer_MetricMap_get_tag_count` is documented
        // to return `-1` rather than read past the entries.
        assert_eq!(unsafe { kafka_producer_MetricMap_get_tag_count(map, 0) }, -1);
        // SAFETY: `map` is the live empty metric-map handle leaked above; both indices are
        // out of range deliberately, and `kafka_producer_MetricMap_get_tag_key` is
        // documented to return null rather than read past the entries.
        assert!(unsafe { kafka_producer_MetricMap_get_tag_key(map, 0, 0) }.is_null());
        // SAFETY: `map` is the live empty metric-map handle leaked above; index `0` is out
        // of range deliberately, and `kafka_producer_MetricMap_get_value_double` is
        // documented to return `0.0` rather than read past the entries.
        assert_eq!(unsafe { kafka_producer_MetricMap_get_value_double(map, 0) }, 0.0);
        // SAFETY: `map` is the live empty metric-map handle leaked above; index `0` is out
        // of range deliberately, and `kafka_producer_MetricMap_get_value_long` is
        // documented to return `0` rather than read past the entries.
        assert_eq!(unsafe { kafka_producer_MetricMap_get_value_long(map, 0) }, 0);
        // SAFETY: `map` is the live empty metric-map handle leaked above; index `0` is out
        // of range deliberately, and `kafka_producer_MetricMap_get_value_int` is documented
        // to return `0` rather than read past the entries.
        assert_eq!(unsafe { kafka_producer_MetricMap_get_value_int(map, 0) }, 0);
        assert_eq!(
            // SAFETY: `map` is the live empty metric-map handle leaked above; index `0` is
            // out of range deliberately, and `kafka_producer_MetricMap_get_value_kind` is
            // documented to default to `DOUBLE` rather than read past the entries.
            unsafe { kafka_producer_MetricMap_get_value_kind(map, 0) },
            common::METRIC_VALUE_DOUBLE
        );
        // SAFETY: `map` was leaked by `Box::into_raw(common::build_metric_map_inner(..))`
        // above, so `kafka_producer_MetricMap_destroy` is its matching single free, and
        // nothing uses it afterwards.
        unsafe { kafka_producer_MetricMap_destroy(map) };
        // Destroy is null-safe.
        // SAFETY: Null is passed deliberately to exercise the documented null path:
        // `kafka_producer_MetricMap_destroy` is specified as a no-op for a null handle.
        unsafe { kafka_producer_MetricMap_destroy(std::ptr::null_mut()) };
    }

    /// `kafka_producer_Producer_metrics` on a `MockProducer` returns a valid,
    /// empty snapshot handle by default (no metrics seeded via the mock's
    /// `set_mock_metrics`, which the FFI does not expose). Null producer yields
    /// a null handle.
    #[test]
    fn test_mock_producer_metrics_snapshot() {
        let producer = kafka_producer_MockProducer_new(true);
        // SAFETY: `kafka_producer_Producer_metrics` requires a valid handle: `producer` is
        // the live handle `kafka_producer_MockProducer_new` returned. The returned snapshot
        // is a fresh handle the test owns and destroys once.
        let map = unsafe { kafka_producer_Producer_metrics(producer) };
        assert!(!map.is_null());
        // SAFETY: `kafka_producer_MetricMap_count` requires a valid metric-map handle:
        // `map` is the non-null snapshot `kafka_producer_Producer_metrics` just returned
        // (asserted), alive until its `destroy`.
        assert_eq!(unsafe { kafka_producer_MetricMap_count(map) }, 0);
        // SAFETY: `map` is the snapshot handle `kafka_producer_Producer_metrics` returned;
        // this is its single `kafka_producer_MetricMap_destroy` and nothing uses it
        // afterwards.
        unsafe { kafka_producer_MetricMap_destroy(map) };
        // SAFETY: `producer` is the live handle `kafka_producer_MockProducer_new` returned;
        // the snapshot taken from it was already destroyed, so this single
        // `kafka_producer_Producer_destroy` is the final use.
        unsafe { kafka_producer_Producer_destroy(producer) };

        // Null producer -> null handle (no panic).
        // SAFETY: Null `producer` is passed deliberately: `kafka_producer_Producer_metrics`
        // null-checks `producer` in its body and returns a null handle, so the block relies
        // on that check rather than on its `# Safety` section (which only states that
        // `producer` must be a valid handle). No handle is created.
        let null_map = unsafe { kafka_producer_Producer_metrics(std::ptr::null_mut()) };
        assert!(null_map.is_null());
    }
}
