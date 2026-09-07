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
//! - **Opaque handles**: [`kafka_producer_Producer_t`], [`kafka_producer_FutureRecordMetadata_t`], and
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
#![allow(non_snake_case, non_camel_case_types)]

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
    enqueue_or_run_inline, init_default_logger, kafka_common_Error_t,
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
    box_partition_info_list, group_metadata_ref, kafka_consumer_ConsumerGroupMetadata_t,
    kafka_consumer_PartitionInfoList_t, read_offset_map,
};
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
    /// 1. The sender background task (spawned by `from_config`) has a runtime to run on.
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
/// This allows [`kafka_producer_FutureRecordMetadata_get`] and
/// [`kafka_producer_FutureRecordMetadata_get_all`] to call `handle.block_on()`
/// instead of creating throwaway runtimes.
struct FfiFuture {
    future: KafkaFuture<RecordMetadata>,
    runtime_handle: tokio::runtime::Handle,
    /// Sender for the producer's completion-dispatch queue, so
    /// [`kafka_producer_FutureRecordMetadata_get_async`] can deliver its
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
pub struct kafka_producer_FutureRecordMetadata_t {
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
    pub topic: *const c_char,
    /// Partition number, or -1 for unset.
    pub partition: i32,
    /// Timestamp in milliseconds since epoch, or -1 for unset.
    pub timestamp: i64,
    /// Pointer to key bytes, or null if no key.
    pub key: *const u8,
    /// Key length in bytes, or -1 for no key.
    pub key_len: i32,
    /// Pointer to value bytes, or null if no value.
    pub value: *const u8,
    /// Value length in bytes, or -1 for no value.
    pub value_len: i32,
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
    unsafe { &*(producer as *const ProducerHandle) }
}

/// Casts a `*mut kafka_producer_FutureRecordMetadata_t` to a reference to
/// [`FfiFuture`].
///
/// # Safety
///
/// The pointer must be non-null and must have been created by a send function.
unsafe fn future_ref(future: *mut kafka_producer_FutureRecordMetadata_t) -> &'static FfiFuture {
    unsafe { &*(future as *const FfiFuture) }
}

/// Casts a `*const kafka_producer_RecordMetadata_t` to a reference to `RecordMetadataInner`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`kafka_producer_FutureRecordMetadata_get`].
unsafe fn metadata_ref(metadata: *const kafka_producer_RecordMetadata_t) -> &'static RecordMetadataInner {
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
            let owned_record = ProducerRecord::new_options(
                ProducerRecordOptionsBuilder::new()
                    .set_topic(topic)
                    .set_value(value.map(|v| v.to_vec()))
                    .set_partition(partition)
                    .set_timestamp(timestamp)
                    .set_key(key.map(|k| k.to_vec()))
                    .build(),
            )
            .map_err(|e| Error::local_illegal_argument(e.message()))?;
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
            let owned_record = ProducerRecord::new_options(
                ProducerRecordOptionsBuilder::new()
                    .set_topic(topic)
                    .set_value(value.map(|v| v.to_vec()))
                    .set_partition(partition)
                    .set_timestamp(timestamp)
                    .set_key(key.map(|k| k.to_vec()))
                    .build(),
            )
            .map_err(|e| Error::local_illegal_argument(e.message()))?;
            rt.block_on(mock.send_callback(owned_record, Some(callback)))
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
    unsafe { &mut *(props as *mut HashMap<String, String>) }
}

/// Wraps a `KafkaFuture<RecordMetadata>` and the producer's runtime handle
/// into a heap-allocated opaque pointer.
fn box_future(
    future: KafkaFuture<RecordMetadata>,
    runtime_handle: tokio::runtime::Handle,
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
) -> *mut kafka_producer_FutureRecordMetadata_t {
    let ffi_future = FfiFuture { future, runtime_handle, completion_tx };
    Box::into_raw(Box::new(ffi_future)) as *mut kafka_producer_FutureRecordMetadata_t
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
// task** (not a per-message `tokio::spawn`, per CLAUDE.md §11).

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

// Public per-method callback typedefs. Per CLAUDE.md §3, an async callback type
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
/// Completion callback for [`kafka_producer_FutureRecordMetadata_get_async`].
pub type kafka_producer_FutureRecordMetadata_get_callback_t =
    unsafe extern "C" fn(*mut kafka_producer_RecordMetadata_t, *mut kafka_common_Error_t, *mut std::ffi::c_void);
/// Aggregate completion callback for [`kafka_producer_FutureRecordMetadata_get_all_async`].
pub type kafka_producer_FutureRecordMetadata_get_all_callback_t = unsafe extern "C" fn(
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
/// Java `initTransactions` method per CLAUDE.md §3; all five transaction-control
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
/// success `list` is a non-null [`kafka_consumer_PartitionInfoList_t`] (free with
/// [`kafka_consumer_PartitionInfoList_destroy`]) and `error` is null; on failure
/// `list` is null and `error` is non-null. The caller owns whichever is non-null.
/// (Named after the consumer sibling `..._partitions_for_callback_t` rather than
/// the `..._partitions_for_async_callback_t` that CLAUDE.md §3 would suggest, for
/// consistency with `kafka_consumer_Consumer_partitions_for_callback_t`.)
pub type kafka_producer_Producer_partitions_for_callback_t =
    unsafe extern "C" fn(*mut kafka_consumer_PartitionInfoList_t, *mut kafka_common_Error_t, *mut std::ffi::c_void);

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
/// (CLAUDE.md §9.5), while the delivery path owns the single fire on success.
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
/// [`register_pending_task`], and `destroy` joins all of them before dropping
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
    let handle = unsafe { &*(ptr as *const ProducerHandle) };
    let guard = handle.kind.lock().unwrap();
    match &*guard {
        ProducerKind::Kafka(k, _) => {
            ProducerStaticRef::Kafka(unsafe { &*(k.as_ref() as *const KafkaProducer<Vec<u8>, Vec<u8>>) })
        },
        ProducerKind::Mock(m, _) => {
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
        // task (registered via `register_pending_task`) before dropping the
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
        // before the `.await` below (CLAUDE.md §9.6).
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
                match ProducerRecord::new_options(
                    ProducerRecordOptionsBuilder::new()
                        .set_topic(topic)
                        .set_value(value.map(|v| v.to_vec()))
                        .set_partition(partition)
                        .set_timestamp(timestamp)
                        .set_key(key.map(|k| k.to_vec()))
                        .set_headers(Some(headers))
                        .build(),
                ) {
                    Ok(record) => {
                        if let Err(e) = mp.send_callback(record, Some(callback)).await {
                            fire_error(e);
                        }
                    },
                    // `callback` is unused here (record build failed before send);
                    // firing it delivers once (it shares `fired`).
                    Err(e) => callback(None, Some(&Error::local_illegal_argument(e.message()))),
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
    /// `close_async` / `partitions_for_async` call. `destroy` joins all of
    /// these *before* dropping the producer, since each holds a raw
    /// `&'static` reference into it that must not outlive its memory.
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

/// Registers `task` so `destroy` will join it before the producer is freed,
/// pruning already-finished handles first so `pending_tasks` does not grow
/// unbounded over a producer's lifetime under repeated `flush_async` /
/// `close_async` / `partitions_for_async` calls.
fn register_pending_task(handle: &ProducerHandle, task: tokio::task::JoinHandle<()>) {
    let mut tasks = handle.pending_tasks.lock().unwrap();
    tasks.retain(|t| !t.is_finished());
    tasks.push(task);
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
    let task = rt_handle.spawn(submission_loop(ptr as usize, submit_rx));
    register_pending_task(unsafe { &*ptr }, task);

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
    let kind = ProducerKind::Mock(Box::new(MockProducer::new_auto_complete(auto_complete)), runtime);
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
        let key_ptr = unsafe { *configs.add(i) };
        if key_ptr.is_null() {
            break;
        }
        let val_ptr = unsafe { *configs.add(i + 1) };
        if val_ptr.is_null() {
            // Odd number of entries — missing value for the last key.
            return std::ptr::null_mut();
        }
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_string_lossy().to_string();
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerProperties_put(
    props: *mut kafka_producer_ProducerProperties_t,
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerProperties_destroy(props: *mut kafka_producer_ProducerProperties_t) {
    if !props.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_new(
    props: *const kafka_producer_ProducerProperties_t,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_producer_Producer_t {
    init_default_logger();

    if props.is_null() {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    let map = unsafe { properties_ref(props) };
    let config = match ProducerConfig::from_properties(map) {
        Ok(c) => c,
        Err(e) => {
            if !out_error.is_null() {
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
                unsafe { *out_error = box_error(Error::local_illegal_state("failed to create tokio runtime")) };
            }
            return std::ptr::null_mut();
        },
    };

    // Enter the runtime so that KafkaProducer::from_config can call tokio::task::spawn.
    let _guard = runtime.enter();
    let producer = match KafkaProducer::<Vec<u8>, Vec<u8>>::from_config(
        config,
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    ) {
        Ok(p) => p,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    let kind = ProducerKind::Kafka(Box::new(producer), runtime);
    if !out_error.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_destroy(producer: *mut kafka_producer_Producer_t) {
    if producer.is_null() {
        return;
    }
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
    let tasks = pending_tasks.into_inner().unwrap_or_default();
    if !tasks.is_empty() {
        kind.lock().unwrap().runtime().block_on(async {
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
) -> *mut kafka_producer_FutureRecordMetadata_t {
    if producer.is_null() || topic.is_null() {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().into_owned();

    let key_slice: Option<&[u8]> = if key_len >= 0 {
        if key.is_null() {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        Some(unsafe { std::slice::from_raw_parts(key, key_len as usize) })
    } else {
        None
    };

    let value_slice: Option<&[u8]> = if value_len >= 0 {
        if value.is_null() {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        Some(unsafe { std::slice::from_raw_parts(value, value_len as usize) })
    } else {
        None
    };

    let partition_opt = if partition >= 0 { Some(partition) } else { None };
    let timestamp_opt = if timestamp >= 0 { Some(timestamp) } else { None };

    let record = match ProducerRecord::new_options(
        ProducerRecordOptionsBuilder::new()
            .set_topic(topic_str)
            .set_value(value_slice)
            .set_partition(partition_opt)
            .set_timestamp(timestamp_opt)
            .set_key(key_slice)
            .build(),
    ) {
        Ok(r) => r,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::local_illegal_argument(e.message())) };
            }
            return std::ptr::null_mut();
        },
    };

    let handle = unsafe { producer_handle(producer) };
    let completion_tx = handle.completion_tx.clone();
    let guard = handle.kind.lock().unwrap();
    let runtime_handle = guard.runtime().handle().clone();
    match producer_send(&guard, record) {
        Ok(future) => {
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_future(future, runtime_handle, completion_tx)
        },
        Err(e) => {
            if !out_error.is_null() {
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
/// future and must free it with [`kafka_producer_FutureRecordMetadata_destroy`].
/// If `out_error` is non-null, `*out_error` is set to null on success or to a
/// valid [`kafka_common_Error_t`] handle on failure.
///
/// `out_error` reports synchronous validation errors (null topic / bad
/// key/value length) and synchronous send failures (closed producer), in which
/// case `callback` is **not** invoked.
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
) -> *mut kafka_producer_FutureRecordMetadata_t {
    if producer.is_null() || topic.is_null() {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().into_owned();

    let key_slice: Option<&[u8]> = if key_len >= 0 {
        if key.is_null() {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        Some(unsafe { std::slice::from_raw_parts(key, key_len as usize) })
    } else {
        None
    };

    let value_slice: Option<&[u8]> = if value_len >= 0 {
        if value.is_null() {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return std::ptr::null_mut();
        }
        Some(unsafe { std::slice::from_raw_parts(value, value_len as usize) })
    } else {
        None
    };

    let partition_opt = if partition >= 0 { Some(partition) } else { None };
    let timestamp_opt = if timestamp >= 0 { Some(timestamp) } else { None };

    let record = match ProducerRecord::new_options(
        ProducerRecordOptionsBuilder::new()
            .set_topic(topic_str)
            .set_value(value_slice)
            .set_partition(partition_opt)
            .set_timestamp(timestamp_opt)
            .set_key(key_slice)
            .build(),
    ) {
        Ok(r) => r,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::local_illegal_argument(e.message())) };
            }
            return std::ptr::null_mut();
        },
    };

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
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_future(future, runtime_handle, completion_tx)
        },
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            std::ptr::null_mut()
        },
    }
}

/// Inner implementation of [`kafka_producer_Producer_send_batch`].
///
/// Separated from the `extern "C"` wrapper so that tests can call it
/// directly and catch panics without hitting the FFI boundary abort.
///
/// # Panics
///
/// Panics if `producer`, `records`, `out_futures`, or `out_errors` is null,
/// or if `count` is negative.
///
/// # Safety
///
/// Same requirements as [`kafka_producer_Producer_send_batch`].
unsafe fn send_batch_inner(
    producer: *mut kafka_producer_Producer_t,
    records: *const kafka_producer_ProducerRecord_t,
    count: i32,
    out_futures: *mut *mut kafka_producer_FutureRecordMetadata_t,
    out_errors: *mut *mut kafka_common_Error_t,
) -> i32 {
    assert!(!producer.is_null(), "producer must not be null");
    assert!(!records.is_null(), "records must not be null");
    assert!(!out_futures.is_null(), "out_futures must not be null");
    assert!(!out_errors.is_null(), "out_errors must not be null");
    assert!(count >= 0, "count must not be negative");

    let count = count as usize;

    let handle = unsafe { producer_handle(producer) };
    let completion_tx = handle.completion_tx.clone();
    let guard = handle.kind.lock().unwrap();
    let runtime_handle = guard.runtime().handle().clone();
    let mut success_count: i32 = 0;

    for i in 0..count {
        let rec = unsafe { &*records.add(i) };

        if rec.topic.is_null() {
            unsafe {
                *out_futures.add(i) = std::ptr::null_mut();
                *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
            }
            continue;
        }

        let topic_str = unsafe { CStr::from_ptr(rec.topic) }.to_string_lossy().into_owned();

        let key: Option<&[u8]> = if rec.key_len >= 0 {
            if rec.key.is_null() {
                unsafe {
                    *out_futures.add(i) = std::ptr::null_mut();
                    *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
                }
                continue;
            }
            Some(unsafe { std::slice::from_raw_parts(rec.key, rec.key_len as usize) })
        } else {
            None
        };

        let value: Option<&[u8]> = if rec.value_len >= 0 {
            if rec.value.is_null() {
                unsafe {
                    *out_futures.add(i) = std::ptr::null_mut();
                    *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
                }
                continue;
            }
            Some(unsafe { std::slice::from_raw_parts(rec.value, rec.value_len as usize) })
        } else {
            None
        };

        let partition = if rec.partition >= 0 { Some(rec.partition) } else { None };
        let timestamp = if rec.timestamp >= 0 { Some(rec.timestamp) } else { None };

        let record = match ProducerRecord::new_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic_str)
                .set_value(value)
                .set_partition(partition)
                .set_timestamp(timestamp)
                .set_key(key)
                .build(),
        ) {
            Ok(r) => r,
            Err(e) => {
                unsafe {
                    *out_futures.add(i) = std::ptr::null_mut();
                    *out_errors.add(i) = box_error(Error::local_illegal_argument(e.message()));
                }
                continue;
            },
        };

        match producer_send(&guard, record) {
            Ok(future) => unsafe {
                *out_futures.add(i) = box_future(future, runtime_handle.clone(), completion_tx.clone());
                *out_errors.add(i) = std::ptr::null_mut();
                success_count += 1;
            },
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
/// every non-null future with [`kafka_producer_FutureRecordMetadata_destroy`]
/// and every non-null error with [`kafka_common_Error_destroy`].
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `records`: Non-null pointer to an array of [`kafka_producer_ProducerRecord_t`].
/// - `count`: Number of records in the array (must be `>= 0`).
/// - `out_futures`: Non-null pointer to an array of `*mut kafka_producer_FutureRecordMetadata_t`
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
/// - `records` must point to at least `count` valid [`kafka_producer_ProducerRecord_t`] structs.
/// - `out_futures` must point to at least `count` writable pointer slots.
/// - `out_errors` must point to at least `count` writable pointer slots.
/// - Each `kafka_producer_ProducerRecord_t.topic` must be a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_batch(
    producer: *mut kafka_producer_Producer_t,
    records: *const kafka_producer_ProducerRecord_t,
    count: i32,
    out_futures: *mut *mut kafka_producer_FutureRecordMetadata_t,
    out_errors: *mut *mut kafka_common_Error_t,
) -> i32 {
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
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return;
    }

    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().into_owned();

    // Borrow key/value into caller memory, lifetime-extended to 'static under
    // the documented contract (caller keeps buffers valid until the callback).
    let key_slice: Option<&'static [u8]> = if key_len >= 0 {
        if key.is_null() {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return;
        }
        Some(unsafe { std::slice::from_raw_parts(key, key_len as usize) })
    } else {
        None
    };

    let value_slice: Option<&'static [u8]> = if value_len >= 0 {
        if value.is_null() {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
            }
            return;
        }
        Some(unsafe { std::slice::from_raw_parts(value, value_len as usize) })
    } else {
        None
    };

    let partition_opt = if partition >= 0 { Some(partition) } else { None };
    let timestamp_opt = if timestamp >= 0 { Some(timestamp) } else { None };

    // Build (and validate) the record once, here, so construction errors are
    // reported synchronously via `out_error` rather than deferred to the task.
    let record = match ProducerRecord::new_options(
        ProducerRecordOptionsBuilder::new()
            .set_topic(topic_str)
            .set_value(value_slice)
            .set_partition(partition_opt)
            .set_timestamp(timestamp_opt)
            .set_key(key_slice)
            .build(),
    ) {
        Ok(r) => r,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(Error::local_illegal_argument(e.message())) };
            }
            return;
        },
    };

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
            unsafe { *out_error = box_error(Error::local_illegal_state("producer is closed")) };
        }
        return;
    }

    if !out_error.is_null() {
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
/// - `records` must point to at least `count` valid records whose `key`/`value`
///   remain valid until their callbacks fire.
/// - `out_errors` must point to at least `count` writable pointer slots.
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
    assert!(count >= 0, "count must not be negative");

    let handle = unsafe { producer_handle(producer) };
    let mut accepted: i32 = 0;

    for i in 0..count as usize {
        let rec = unsafe { &*records.add(i) };

        if rec.topic.is_null() {
            unsafe { *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest)) };
            continue;
        }
        let topic = unsafe { CStr::from_ptr(rec.topic) }.to_string_lossy().into_owned();

        let key: Option<&'static [u8]> = if rec.key_len >= 0 {
            if rec.key.is_null() {
                unsafe { *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest)) };
                continue;
            }
            Some(unsafe { std::slice::from_raw_parts(rec.key, rec.key_len as usize) })
        } else {
            None
        };

        let value: Option<&'static [u8]> = if rec.value_len >= 0 {
            if rec.value.is_null() {
                unsafe { *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest)) };
                continue;
            }
            Some(unsafe { std::slice::from_raw_parts(rec.value, rec.value_len as usize) })
        } else {
            None
        };

        let partition = if rec.partition >= 0 { Some(rec.partition) } else { None };
        let timestamp = if rec.timestamp >= 0 { Some(rec.timestamp) } else { None };

        let record = match ProducerRecord::new_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value)
                .set_partition(partition)
                .set_timestamp(timestamp)
                .set_key(key)
                .build(),
        ) {
            Ok(r) => r,
            Err(e) => {
                unsafe { *out_errors.add(i) = box_error(Error::local_illegal_argument(e.message())) };
                continue;
            },
        };
        // Carry the target, not a pre-built callback: the submission task builds
        // the callback and can re-fire on `send`'s error paths (see `SendRequest`).
        let request = SubmitRequest::Send(SendRequest { record, target: RecordCallbackTarget { callback, user_data } });

        handle.queued_sends.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if handle.submit_tx.send(request).is_err() {
            handle.queued_sends.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            unsafe { *out_errors.add(i) = box_error(Error::local_illegal_state("producer is closed")) };
            continue;
        }
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_FutureRecordMetadata_is_done(
    future: *mut kafka_producer_FutureRecordMetadata_t,
) -> bool {
    if future.is_null() {
        return false;
    }
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_FutureRecordMetadata_get(
    future: *mut kafka_producer_FutureRecordMetadata_t,
    out_error: *mut *mut kafka_common_Error_t,
) -> *mut kafka_producer_RecordMetadata_t {
    if future.is_null() {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(Error::new(Errors::InvalidRequest)) };
        }
        return std::ptr::null_mut();
    }

    let f = unsafe { future_ref(future) };

    match f.runtime_handle.block_on(f.future.get()) {
        Ok(metadata) => {
            if !out_error.is_null() {
                unsafe { *out_error = std::ptr::null_mut() };
            }
            box_metadata(metadata)
        },
        Err(e) => {
            if !out_error.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_FutureRecordMetadata_get_all(
    futures: *mut *mut kafka_producer_FutureRecordMetadata_t,
    count: i32,
    out_metadata: *mut *mut kafka_producer_RecordMetadata_t,
    out_errors: *mut *mut kafka_common_Error_t,
) {
    assert!(!futures.is_null(), "futures must not be null");
    assert!(!out_metadata.is_null(), "out_metadata must not be null");
    assert!(!out_errors.is_null(), "out_errors must not be null");
    assert!(count >= 0, "count must not be negative");

    let count = count as usize;

    for i in 0..count {
        let future_ptr = unsafe { *futures.add(i) };
        if future_ptr.is_null() {
            unsafe {
                *out_metadata.add(i) = std::ptr::null_mut();
                *out_errors.add(i) = box_error(Error::new(Errors::InvalidRequest));
            }
            continue;
        }

        let f = unsafe { future_ref(future_ptr) };
        match f.runtime_handle.block_on(f.future.get()) {
            Ok(metadata) => unsafe {
                *out_metadata.add(i) = box_metadata(metadata);
                *out_errors.add(i) = std::ptr::null_mut();
            },
            Err(e) => unsafe {
                *out_metadata.add(i) = std::ptr::null_mut();
                *out_errors.add(i) = box_error(e);
            },
        }
    }
}

/// Asynchronously awaits a future, invoking `callback` on completion instead
/// of blocking (the async counterpart of
/// [`kafka_producer_FutureRecordMetadata_get`]).
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_FutureRecordMetadata_get_async(
    future: *mut kafka_producer_FutureRecordMetadata_t,
    callback: kafka_producer_FutureRecordMetadata_get_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    if future.is_null() {
        // Programming error: deliver an error through the callback inline.
        let error = box_error(Error::new(Errors::InvalidRequest));
        unsafe { callback(std::ptr::null_mut(), error, user_data) };
        return;
    }

    let f = unsafe { future_ref(future) };
    let fut = f.future.clone();
    let tx = f.completion_tx.clone();
    let target = RecordCallbackTarget { callback, user_data };

    f.runtime_handle.spawn(async move {
        let target = target;
        // No `.await` follows the handle construction below, so the raw
        // pointers never cross a suspension point.
        let (metadata, error) = match fut.get().await {
            Ok(m) => (box_metadata(m), std::ptr::null_mut()),
            Err(e) => (std::ptr::null_mut(), box_error(e)),
        };
        let completion = RecordCompletion { callback: target.callback, user_data: target.user_data, metadata, error };
        let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
        enqueue_or_run_inline(&tx, job);
    });
}

/// Asynchronously awaits all futures, invoking `callback` once with parallel
/// result arrays (the async counterpart of
/// [`kafka_producer_FutureRecordMetadata_get_all`]).
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_FutureRecordMetadata_get_all_async(
    futures: *mut *mut kafka_producer_FutureRecordMetadata_t,
    count: i32,
    callback: kafka_producer_FutureRecordMetadata_get_all_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    assert!(!futures.is_null(), "futures must not be null");
    assert!(count >= 0, "count must not be negative");
    let count = count as usize;

    // Clone the futures and grab a runtime handle + completion sender from the
    // first non-null future.
    let mut futs: Vec<Option<KafkaFuture<RecordMetadata>>> = Vec::with_capacity(count);
    let mut runtime: Option<tokio::runtime::Handle> = None;
    let mut completion: Option<std::sync::mpsc::Sender<CompletionJob>> = None;
    for i in 0..count {
        let fp = unsafe { *futures.add(i) };
        if fp.is_null() {
            futs.push(None);
        } else {
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
            unsafe { callback(metadata.as_mut_ptr(), errors.as_mut_ptr(), count as i32, user_data) };
            return;
        },
    };

    let target = RecordBatchCallbackTarget { callback, user_data };
    runtime.spawn(async move {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_FutureRecordMetadata_destroy(
    future: *mut kafka_producer_FutureRecordMetadata_t,
) {
    if !future.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_FutureRecordMetadata_destroy_all(
    futures: *mut *mut kafka_producer_FutureRecordMetadata_t,
    count: i32,
) {
    assert!(!futures.is_null(), "futures must not be null");
    assert!(count >= 0, "count must not be negative");

    for i in 0..count as usize {
        let future = unsafe { *futures.add(i) };
        if !future.is_null() {
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
/// `metadata` must be a valid handle from [`kafka_producer_FutureRecordMetadata_get`], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_offset(metadata: *const kafka_producer_RecordMetadata_t) -> i64 {
    if metadata.is_null() {
        return -1;
    }
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
/// `metadata` must be a valid handle from [`kafka_producer_FutureRecordMetadata_get`], or null.
/// The returned pointer must not be used after the metadata is destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_topic(
    metadata: *const kafka_producer_RecordMetadata_t,
) -> *const c_char {
    if metadata.is_null() {
        return std::ptr::null();
    }
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
/// `metadata` must be a valid handle from [`kafka_producer_FutureRecordMetadata_get`], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_partition(
    metadata: *const kafka_producer_RecordMetadata_t,
) -> i32 {
    if metadata.is_null() {
        return -1;
    }
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
/// `metadata` must be a valid handle from [`kafka_producer_FutureRecordMetadata_get`], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_timestamp(
    metadata: *const kafka_producer_RecordMetadata_t,
) -> i64 {
    if metadata.is_null() {
        return -1;
    }
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
///   [`kafka_producer_FutureRecordMetadata_get`].
/// - `callback` must be a valid function pointer.
/// - The `topic` pointer passed to the callback is only valid for the duration
///   of the callback invocation.
/// - After this call the metadata handle is destroyed and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_copy(
    metadata: *mut kafka_producer_RecordMetadata_t,
    callback: unsafe extern "C" fn(i64, i32, *const c_char, i64, *mut std::ffi::c_void),
    user_data: *mut std::ffi::c_void,
) {
    if metadata.is_null() {
        return;
    }

    let inner = unsafe { metadata_ref(metadata) };
    let offset = inner.metadata.offset();
    let partition = inner.metadata.partition();
    let topic = inner.topic_cstring.as_ptr();
    let timestamp = inner.metadata.timestamp();

    unsafe {
        callback(offset, partition, topic, timestamp, user_data);
    }

    // Destroy the handle after the callback returns.
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
/// - `metadata` must be null or a valid handle from [`kafka_producer_FutureRecordMetadata_get`].
/// - After this call, the pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_destroy(metadata: *mut kafka_producer_RecordMetadata_t) {
    if !metadata.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_flush(
    producer: *mut kafka_producer_Producer_t,
    out_error: *mut *mut kafka_common_Error_t,
) {
    if producer.is_null() {
        if !out_error.is_null() {
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
    let handle = unsafe { producer_handle(producer) };
    let producer_mtx = unsafe { producer_ref(producer) };
    let rt_handle = producer_mtx.lock().unwrap().runtime().handle().clone();
    if let Err(e) = drain_submitted_sends_via(&handle.queued_sends, &handle.submit_tx, &rt_handle) {
        if !out_error.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_metrics(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_producer_MetricMap_t {
    if producer.is_null() {
        return std::ptr::null_mut();
    }
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_count(map: *const kafka_producer_MetricMap_t) -> i32 {
    unsafe { common::metric_map_count(map as *const MetricMapInner) }
}

/// Returns the metric name at `index` (borrowed; valid until the map is
/// destroyed), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_name(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    unsafe { common::metric_map_get_name(map as *const MetricMapInner, index) }
}

/// Returns the metric group at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_group(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    unsafe { common::metric_map_get_group(map as *const MetricMapInner, index) }
}

/// Returns the metric description at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_description(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    unsafe { common::metric_map_get_description(map as *const MetricMapInner, index) }
}

/// Returns the number of tags on the metric at `index`, or `-1` if out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_tag_count(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i32 {
    unsafe { common::metric_map_get_tag_count(map as *const MetricMapInner, index) }
}

/// Returns the `tag_index`-th tag key of the metric at `index` (borrowed), or
/// null if either index is out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_tag_key(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    unsafe { common::metric_map_get_tag_key(map as *const MetricMapInner, index, tag_index) }
}

/// Returns the `tag_index`-th tag value of the metric at `index` (borrowed), or
/// null if either index is out of range.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_tag_value(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    unsafe { common::metric_map_get_tag_value(map as *const MetricMapInner, index, tag_index) }
}

/// Returns which `get_value_*` accessor is valid for the metric at `index`.
/// Defaults to `DOUBLE` when `index` is out of range (the `get_value_double`
/// accessor then returns `0.0`).
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_kind(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i32 {
    unsafe { common::metric_map_get_value_kind(map as *const MetricMapInner, index) }
}

/// Returns the `Double` reading of the metric at `index`, or `0.0` if out of
/// range or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_double(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> f64 {
    unsafe { common::metric_map_get_value_double(map as *const MetricMapInner, index) }
}

/// Returns the `String` reading of the metric at `index` (borrowed), or null if
/// out of range. Empty for a non-`String` kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_string(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> *const c_char {
    unsafe { common::metric_map_get_value_string(map as *const MetricMapInner, index) }
}

/// Returns the `Long` reading of the metric at `index`, or `0` if out of range
/// or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_long(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i64 {
    unsafe { common::metric_map_get_value_long(map as *const MetricMapInner, index) }
}

/// Returns the `Int` reading of the metric at `index`, or `0` if out of range
/// or a different kind.
///
/// # Safety
///
/// `map` must be a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_get_value_int(
    map: *const kafka_producer_MetricMap_t,
    index: i32,
) -> i32 {
    unsafe { common::metric_map_get_value_int(map as *const MetricMapInner, index) }
}

/// Destroys a metric-map handle. Safe with null (no-op).
///
/// # Safety
///
/// `map` must be null or a valid metric-map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MetricMap_destroy(map: *mut kafka_producer_MetricMap_t) {
    unsafe { common::metric_map_destroy(map as *mut MetricMapInner) };
}

/// Returns the partition metadata for a topic. On success writes a
/// [`kafka_consumer_PartitionInfoList_t`] to `*out_list` (free it with
/// [`kafka_consumer_PartitionInfoList_destroy`]) and returns null; on failure
/// returns a non-null error and leaves `*out_list` untouched. The
/// `PartitionInfoList` handle/accessors are shared with the consumer FFI.
///
/// # Safety
///
/// `producer` must be a valid handle; `topic` a valid C string; `out_list` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for(
    producer: *mut kafka_producer_Producer_t,
    topic: *const c_char,
    out_list: *mut *mut kafka_consumer_PartitionInfoList_t,
) -> *mut kafka_common_Error_t {
    if producer.is_null() || topic.is_null() {
        return box_error(Error::new(Errors::InvalidRequest));
    }
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close(
    producer: *mut kafka_producer_Producer_t,
    out_error: *mut *mut kafka_common_Error_t,
) {
    if producer.is_null() {
        if !out_error.is_null() {
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
    let handle = unsafe { producer_handle(producer) };
    let producer_mtx = unsafe { producer_ref(producer) };
    let rt_handle = producer_mtx.lock().unwrap().runtime().handle().clone();
    if let Err(e) = drain_submitted_sends_via(&handle.queued_sends, &handle.submit_tx, &rt_handle) {
        if !out_error.is_null() {
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
        unsafe { callback(error, user_data) };
        return;
    }

    let handle = unsafe { producer_handle(producer) };
    let completion = handle.completion_tx.clone();
    let runtime = handle.kind.lock().unwrap().runtime().handle().clone();
    let ptr = producer as usize;
    let target = OperationCallbackTarget { callback, user_data };

    let task = runtime.spawn(async move {
        let target = target;
        // SAFETY: the handle outlives this task — it is registered via
        // `register_pending_task` and `destroy` joins it before dropping the
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
            // dropped before the `.await` (CLAUDE.md §9.6).
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
        let job: CompletionJob = Box::new(move || unsafe { op.fire() });
        enqueue_or_run_inline(&completion, job);
    });
    register_pending_task(handle, task);
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
    list: *mut kafka_consumer_PartitionInfoList_t,
    error: *mut kafka_common_Error_t,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread; the
// C user is responsible for the thread-safety of `user_data`.
unsafe impl Send for PartitionInfoListCompletion {}
impl PartitionInfoListCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    unsafe fn fire(self) {
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
/// with a non-null [`kafka_consumer_PartitionInfoList_t`] and null error on
/// success, or a null list and non-null [`kafka_common_Error_t`] on
/// failure. The caller owns whichever handle is non-null.
///
/// # Safety
///
/// `producer` must be a valid handle, or null (null reported via `callback`);
/// `topic` a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for_async(
    producer: *mut kafka_producer_Producer_t,
    topic: *const c_char,
    callback: kafka_producer_Producer_partitions_for_callback_t,
    user_data: *mut std::ffi::c_void,
) {
    if producer.is_null() {
        unsafe { callback(std::ptr::null_mut(), box_error(Error::new(Errors::InvalidRequest)), user_data) };
        return;
    }
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();

    let handle = unsafe { producer_handle(producer) };
    let completion = handle.completion_tx.clone();
    let runtime = handle.kind.lock().unwrap().runtime().handle().clone();
    let ptr = producer as usize;
    let target = PartitionInfoListCallbackTarget { callback, user_data };

    let task = runtime.spawn(async move {
        let target = target;
        // Brief lock to extend a reference to the inner producer; the guard is
        // dropped before the `.await` (CLAUDE.md §9.6).
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
        let job: CompletionJob = Box::new(move || unsafe { completion_payload.fire() });
        enqueue_or_run_inline(&completion, job);
    });
    register_pending_task(handle, task);
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
/// flag when dropped, whether the spawned task exits normally or unwinds on a
/// panic. It holds the handle by raw pointer (as a `usize`) rather than a borrow so
/// it can be moved into the spawned task; that reach into the handle is sound for
/// the same reason [`producer_static_ref`] and [`flush_or_close_async`] are —
/// `destroy` joins every task registered via [`register_pending_task`] before it
/// drops the producer, so the handle outlives the guard.
struct TxnControlAsyncGuard {
    handle_ptr: usize,
}
impl Drop for TxnControlAsyncGuard {
    fn drop(&mut self) {
        // SAFETY: the handle outlives the task that owns this guard (registered via
        // `register_pending_task`, joined by `destroy` before the producer is freed).
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
/// lock is held across the transaction RPC (CLAUDE.md §9.6) and a `send` between
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_begin_transaction(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
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
///   violated precondition, not a reported error (CLAUDE.md FFI §3), exactly as
///   for `kafka_consumer_Consumer_commit_sync_offsets`. `leader_epochs` and
///   `metadata` are the only two that may be null, and then `count` entries are
///   still required of whichever is non-null. `count == 0` reads none of them, so
///   all five may be null in that case.
/// - `group_metadata` must be a valid group-metadata handle, or null.
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
/// Separated from the `extern "C"` wrapper for the same reason as
/// [`send_batch_inner`]: a panic that would unwind out of an `extern "C"` function
/// aborts the process, so the `count` precondition can only be tested by calling
/// this directly.
///
/// # Panics
///
/// Panics if `count` is negative.
///
/// # Safety
///
/// Same requirements as [`kafka_producer_Producer_send_offsets_to_transaction`].
#[allow(clippy::too_many_arguments)]
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
    assert!(count >= 0, "count must not be negative");
    // Pure argument preconditions are checked before the guard is taken, so a
    // malformed call costs nothing and cannot be reported as a concurrency
    // rejection.
    if group_metadata.is_null() {
        return box_error(Error::local_illegal_argument(
            "group_metadata must not be null; pass the handle from kafka_consumer_Consumer_group_metadata",
        ));
    }
    unsafe {
        with_txn_control(producer, |inner, runtime| {
            // Marshaling stays inside the guard because it feeds `op`; its failure
            // path is one of the early returns the guard must survive.
            let offsets_map = read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count)?;
            // The Rust API takes the metadata by value (the transaction manager
            // moves it into the `AddOffsetsToTxn` handler), so clone out of the
            // borrowed handle.
            let group = group_metadata_ref(group_metadata).clone();
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction(
    producer: *mut kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
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
        unsafe { callback(error, user_data) };
        return;
    }

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
        unsafe { callback(error, user_data) };
        return;
    }

    // Flag is now held; every path from here must release it. `prepare` runs on the
    // calling thread (it marshals caller-owned C data that is only valid for this
    // synchronous call) after the CAS, mirroring the sync path's order. A failure
    // releases the flag and reports through the callback without spawning.
    let run = match prepare() {
        Ok(run) => run,
        Err(e) => {
            handle.txn_control_busy.store(false, std::sync::atomic::Ordering::Release);
            let error = box_error(e);
            unsafe { callback(error, user_data) };
            return;
        },
    };

    let completion = handle.completion_tx.clone();
    let runtime = handle.kind.lock().unwrap().runtime().handle().clone();
    let ptr = producer as usize;
    let target = OperationCallbackTarget { callback, user_data };

    let task = runtime.spawn(async move {
        let target = target;
        // Releases `txn_control_busy` on every exit of this task: the normal path
        // drops it explicitly before delivering completion (so the flag is free by
        // the time the caller observes the result, as it is on the sync path); a
        // panic in the drain or op unwinds through this binding and drops it too.
        let guard = TxnControlAsyncGuard { handle_ptr: ptr };
        // SAFETY: the handle outlives this task — it is registered via
        // `register_pending_task` below and `destroy` joins it before dropping the
        // producer it borrows from.
        let h = unsafe { &*(ptr as *const ProducerHandle) };
        // Hand over records still queued by `send_async` before the op, exactly as
        // the sync path and `flush`/`close` do (see `drain_submitted_sends_await`).
        // Await it directly — we are already async — never `block_on` in a task.
        let result = match drain_submitted_sends_await(&h.queued_sends, &h.submit_tx).await {
            Err(e) => Err(e),
            // `producer_static_ref` takes the `kind` lock only to extend the
            // reference and drops it before returning, so no lock is held across the
            // op's `.await` (CLAUDE.md §9.6).
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
        let job: CompletionJob = Box::new(move || unsafe { op_completion.fire() });
        enqueue_or_run_inline(&completion, job);
    });
    register_pending_task(handle, task);
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_init_transactions_callback_t,
    user_data: *mut std::ffi::c_void,
) {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_begin_transaction_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_begin_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
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
///   per CLAUDE.md FFI §3), exactly as for the synchronous function. `leader_epochs`
///   and `metadata` may be null; `count == 0` reads none of the arrays.
/// - `group_metadata` must be a valid group-metadata handle, or null (null is
///   reported through `callback`).
#[allow(clippy::too_many_arguments)]
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
/// Separated from the `extern "C"` wrapper for the same reason as
/// [`send_offsets_to_transaction_inner`]: a panic that would unwind out of an
/// `extern "C"` function aborts the process, so the `count` precondition can only be
/// tested by calling this directly.
///
/// # Panics
///
/// Panics if `count` is negative.
///
/// # Safety
///
/// Same requirements as [`kafka_producer_Producer_send_offsets_to_transaction_async`].
#[allow(clippy::too_many_arguments)]
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
    assert!(count >= 0, "count must not be negative");
    if group_metadata.is_null() {
        let error = box_error(Error::local_illegal_argument(
            "group_metadata must not be null; pass the handle from kafka_consumer_Consumer_group_metadata",
        ));
        unsafe { callback(error, user_data) };
        return;
    }
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
            // The Rust API takes the metadata by value (the transaction manager moves
            // it into the `AddOffsetsToTxn` handler), so clone out of the borrowed
            // handle. The owned map + metadata are then moved into the spawned op.
            let group = group_metadata_ref(group_metadata).clone();
            Ok(move |inner| async move {
                match inner {
                    ProducerStaticRef::Kafka(k) => k.send_offsets_to_transaction(offsets_map, group).await,
                    ProducerStaticRef::Mock(m) => m.send_offsets_to_transaction(offsets_map, group).await,
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_commit_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction_async(
    producer: *mut kafka_producer_Producer_t,
    callback: kafka_producer_Producer_abort_transaction_callback_t,
    user_data: *mut std::ffi::c_void,
) {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_complete_next(producer: *mut kafka_producer_Producer_t) -> bool {
    if producer.is_null() {
        return false;
    }

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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_error_next(
    producer: *mut kafka_producer_Producer_t,
    error_code: i32,
    error_message: *const c_char,
) -> bool {
    if producer.is_null() {
        return false;
    }

    let error = unsafe { mock_error(Errors::for_code(error_code as i16), error_message) };

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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_history_count(producer: *const kafka_producer_Producer_t) -> i32 {
    if producer.is_null() {
        return 0;
    }

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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_sent_offsets(producer: *mut kafka_producer_Producer_t) -> bool {
    if producer.is_null() {
        return false;
    }
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
    let group = unsafe { CStr::from_ptr(group_id) }.to_string_lossy().to_string();
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    let tp = TopicPartition::new(topic_str, partition);

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
        unsafe { *out_offset = found.offset() };
    }
    if !out_leader_epoch.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_clear(producer: *mut kafka_producer_Producer_t) {
    if producer.is_null() {
        return;
    }

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
    unsafe fn assert_success(err: *mut kafka_common_Error_t) {
        if !err.is_null() {
            let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(err)) }.to_string_lossy();
            unsafe { kafka_common_Error_destroy(err) };
            panic!("Expected success but got error: {msg}");
        }
    }

    /// Helper: asserts that a `*mut kafka_common_Error_t` is non-null (failure), destroys it,
    /// and returns the error code.
    unsafe fn assert_error(err: *mut kafka_common_Error_t) -> kafka_common_ErrorCode_t {
        assert!(!err.is_null(), "Expected an error but got success");
        let code = unsafe { kafka_common_Error_code(err) };
        unsafe { kafka_common_Error_destroy(err) };
        code
    }

    // -- Lifecycle tests ----------------------------------------------------

    #[test]
    fn test_create_and_destroy_mock_producer() {
        let producer = kafka_producer_MockProducer_new(true);
        assert!(!producer.is_null());
        unsafe {
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_destroy_null_is_noop() {
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
            assert!(kafka_producer_FutureRecordMetadata_is_done(future));

            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_manual_complete() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("test-topic").unwrap();

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
            assert!(!kafka_producer_FutureRecordMetadata_is_done(future));

            // Complete it
            assert!(kafka_producer_MockProducer_complete_next(producer));
            assert!(kafka_producer_FutureRecordMetadata_is_done(future));

            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_null_producer() {
        let topic = CString::new("topic").unwrap();

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
            let metadata = kafka_producer_FutureRecordMetadata_get(future, &mut err);
            assert_success(err);
            assert!(!metadata.is_null());
            assert_eq!(kafka_producer_RecordMetadata_partition(metadata), 3);

            kafka_producer_RecordMetadata_destroy(metadata);
            kafka_producer_FutureRecordMetadata_destroy(future);
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

        let mut futures: [*mut kafka_producer_FutureRecordMetadata_t; 2] = [std::ptr::null_mut(), std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 2] = [std::ptr::null_mut(), std::ptr::null_mut()];

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
                assert!(kafka_producer_FutureRecordMetadata_is_done(futures[i]));
            }

            // Check history count
            assert_eq!(kafka_producer_MockProducer_history_count(producer as *const _), 2);

            for f in &futures {
                kafka_producer_FutureRecordMetadata_destroy(*f);
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

        unsafe extern "C" fn counting_cb(
            metadata: *mut kafka_producer_RecordMetadata_t,
            error: *mut kafka_common_Error_t,
            _user_data: *mut std::ffi::c_void,
        ) {
            INVOCATIONS.fetch_add(1, Ordering::SeqCst);
            // Free whichever owned handle we were given, as a real C caller must.
            if !metadata.is_null() {
                unsafe { kafka_producer_RecordMetadata_destroy(metadata) };
            }
            if !error.is_null() {
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

    /// A negative `count` must panic rather than be clamped. Clamping would give an
    /// empty offsets map, which `KafkaProducer` reports as success without staging
    /// anything (it short-circuits before consulting transaction state), so the
    /// transaction would commit with no offsets staged and no error surfaced —
    /// silently breaking exactly-once. Mirrors `send_batch`'s own count assert.
    #[test]
    fn test_send_offsets_to_transaction_negative_count_panics() {
        let producer = kafka_producer_MockProducer_new(true);
        // `group_metadata` is null on purpose: the count assert must fire before
        // anything else is looked at, so no valid handle is needed to reach it.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            send_offsets_to_transaction_inner(
                producer,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                -1,
                std::ptr::null(),
            )
        }));
        match result {
            Ok(_) => panic!("Expected a panic for a negative count but the call succeeded"),
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                assert!(msg.contains("count must not be negative"), "unexpected panic message: {msg}");
            },
        }
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// The async `send_offsets_to_transaction` mirror of the count assert: a negative
    /// count is a violated precondition and panics, matching
    /// `test_send_offsets_to_transaction_negative_count_panics`. Tested through the
    /// inner (non-`extern "C"`) function, since a panic out of the `extern "C"`
    /// wrapper would abort the process. The assert fires before anything else is
    /// looked at, so the null callback `user_data` and group_metadata are never
    /// reached.
    #[test]
    fn test_send_offsets_to_transaction_async_negative_count_panics() {
        let producer = kafka_producer_MockProducer_new(true);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            send_offsets_to_transaction_async_inner(
                producer,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                -1,
                std::ptr::null(),
                capture_op_result,
                std::ptr::null_mut(),
            )
        }));
        match result {
            Ok(_) => panic!("Expected a panic for a negative count but the call succeeded"),
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                assert!(msg.contains("count must not be negative"), "unexpected panic message: {msg}");
            },
        }
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
        let kind = ProducerKind::Mock(Box::new(MockProducer::new_auto_complete(true)), runtime);
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
    unsafe fn reclaim_producer_handle(producer: *mut kafka_producer_Producer_t) {
        drop(unsafe { Box::from_raw(producer as *mut ProducerHandle) });
    }

    /// Reads the message of a returned error pointer, asserting it is non-null, then
    /// frees it.
    unsafe fn take_error_message(err: *mut kafka_common_Error_t) -> String {
        assert!(!err.is_null(), "expected a non-null error");
        let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(err)) }
            .to_string_lossy()
            .into_owned();
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
        let msg = unsafe { take_error_message(kafka_producer_Producer_commit_transaction(producer)) };
        assert!(
            msg.contains("send-submission task has stopped"),
            "commit_transaction must surface the drain error, got: {msg}"
        );
        unsafe { reclaim_producer_handle(producer) };
    }

    /// The same proof for a second control op, showing the drain lives in the shared
    /// `with_txn_control` path rather than in one function: `begin_transaction` also
    /// surfaces the drain error instead of reaching the mock's state transition.
    #[test]
    fn test_with_txn_control_drains_before_begin() {
        let producer = dead_submission_handle();
        let msg = unsafe { take_error_message(kafka_producer_Producer_begin_transaction(producer)) };
        assert!(
            msg.contains("send-submission task has stopped"),
            "begin_transaction must surface the drain error, got: {msg}"
        );
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
    unsafe extern "C" fn capture_op_result(error: *mut kafka_common_Error_t, user_data: *mut std::ffi::c_void) {
        let captured = unsafe { &*(user_data as *const CapturedOpResult) };
        if !error.is_null() {
            let msg = unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }
                .to_string_lossy()
                .into_owned();
            *captured.message.lock().unwrap() = Some(msg);
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
        unsafe {
            kafka_producer_Producer_commit_transaction_async(
                producer,
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        // Wait for the spawned task to finish touching the handle before destroy.
        wait_for_fired(&captured);
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
        unsafe {
            kafka_producer_Producer_begin_transaction_async(
                producer,
                capture_op_result,
                &captured as *const CapturedOpResult as *mut std::ffi::c_void,
            );
        }
        // Wait for the spawned task to finish touching the handle before destroy.
        wait_for_fired(&captured);
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
        let handle = unsafe { producer_handle(producer) };
        // Simulate another control op already in flight.
        handle.txn_control_busy.store(true, std::sync::atomic::Ordering::Release);
        let tasks_before = handle.pending_tasks.lock().unwrap().len();

        // Async entry point: the CAS fails, the callback fires synchronously on this
        // thread with the guard message, and nothing is spawned.
        let captured = CapturedOpResult::new();
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
        let sync_msg = unsafe { take_error_message(kafka_producer_Producer_commit_transaction(producer)) };
        assert!(
            sync_msg.contains("not safe for concurrent access"),
            "the sync path shares the flag and must also reject, got: {sync_msg}"
        );

        // Release the simulated in-flight op and tear down.
        handle.txn_control_busy.store(false, std::sync::atomic::Ordering::Release);
        unsafe { kafka_producer_Producer_destroy(producer) };
    }

    /// Helper: asserts that calling `send_batch_inner` with the given arguments
    /// panics with a message containing `expected_msg`.
    unsafe fn assert_send_batch_panics(
        producer: *mut kafka_producer_Producer_t,
        records: *const kafka_producer_ProducerRecord_t,
        count: i32,
        out_futures: *mut *mut kafka_producer_FutureRecordMetadata_t,
        out_errors: *mut *mut kafka_common_Error_t,
        expected_msg: &str,
    ) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            send_batch_inner(producer, records, count, out_futures, out_errors);
        }));
        match result {
            Ok(_) => panic!("Expected panic containing \"{expected_msg}\" but call succeeded"),
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                assert!(
                    msg.contains(expected_msg),
                    "Expected panic containing \"{expected_msg}\" but got: \"{msg}\""
                );
            },
        }
    }

    #[test]
    fn test_send_batch_null_producer_panics() {
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
        let mut futures: [*mut kafka_producer_FutureRecordMetadata_t; 1] = [std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 1] = [std::ptr::null_mut()];

        unsafe {
            assert_send_batch_panics(
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
    fn test_send_batch_null_records_panics() {
        let producer = kafka_producer_MockProducer_new(true);
        let mut futures: [*mut kafka_producer_FutureRecordMetadata_t; 1] = [std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 1] = [std::ptr::null_mut()];

        unsafe {
            assert_send_batch_panics(
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
    fn test_send_batch_null_out_futures_panics() {
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

        unsafe {
            assert_send_batch_panics(
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
    fn test_send_batch_null_out_errors_panics() {
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
        let mut futures: [*mut kafka_producer_FutureRecordMetadata_t; 1] = [std::ptr::null_mut()];

        unsafe {
            assert_send_batch_panics(
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

    #[test]
    fn test_send_batch_negative_count_panics() {
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
        let mut futures: [*mut kafka_producer_FutureRecordMetadata_t; 1] = [std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 1] = [std::ptr::null_mut()];

        unsafe {
            assert_send_batch_panics(
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
        let mut futures: *mut kafka_producer_FutureRecordMetadata_t = std::ptr::null_mut();
        let mut errors: *mut kafka_common_Error_t = std::ptr::null_mut();

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

        let mut futures: [*mut kafka_producer_FutureRecordMetadata_t; 3] =
            [std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 3] =
            [std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut()];

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
            kafka_producer_FutureRecordMetadata_destroy(futures[0]);
            kafka_common_Error_destroy(errors[1]);
            kafka_producer_FutureRecordMetadata_destroy(futures[2]);
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

        let mut futures: [*mut kafka_producer_FutureRecordMetadata_t; 2] = [std::ptr::null_mut(), std::ptr::null_mut()];
        let mut errors: [*mut kafka_common_Error_t; 2] = [std::ptr::null_mut(), std::ptr::null_mut()];

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
            let metadata = kafka_producer_FutureRecordMetadata_get(future, &mut err);
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
            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_future_get_error() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("topic").unwrap();

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
            let metadata = kafka_producer_FutureRecordMetadata_get(future, &mut err);
            assert!(!err.is_null(), "Expected an error from future get");
            assert_eq!(kafka_common_Error_code(err), kafka_common_ErrorCode_CORRUPT_MESSAGE);

            // Verify the error message is accessible
            let msg_ptr = kafka_common_Error_message(err);
            assert!(!msg_ptr.is_null());

            assert!(metadata.is_null());

            kafka_common_Error_destroy(err);
            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_future_is_done_null() {
        unsafe {
            assert!(!kafka_producer_FutureRecordMetadata_is_done(std::ptr::null_mut()));
        }
    }

    #[test]
    fn test_future_get_null_params() {
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let metadata = kafka_producer_FutureRecordMetadata_get(std::ptr::null_mut(), &mut err);
            assert_error(err);
            assert!(metadata.is_null());
        }
    }

    #[test]
    fn test_future_destroy_null() {
        unsafe {
            kafka_producer_FutureRecordMetadata_destroy(std::ptr::null_mut());
        }
    }

    // -- RecordMetadata tests -----------------------------------------------

    #[test]
    fn test_metadata_null_returns_defaults() {
        unsafe {
            assert_eq!(kafka_producer_RecordMetadata_offset(std::ptr::null()), -1);
            assert_eq!(kafka_producer_RecordMetadata_partition(std::ptr::null()), -1);
            assert!(kafka_producer_RecordMetadata_topic(std::ptr::null()).is_null());
        }
    }

    #[test]
    fn test_metadata_destroy_null() {
        unsafe {
            kafka_producer_RecordMetadata_destroy(std::ptr::null_mut());
        }
    }

    // -- Flush and close tests ----------------------------------------------

    #[test]
    fn test_flush() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("topic").unwrap();

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

            assert!(!kafka_producer_FutureRecordMetadata_is_done(future));

            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            kafka_producer_Producer_flush(producer, &mut err);
            assert_success(err);

            assert!(kafka_producer_FutureRecordMetadata_is_done(future));

            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_flush_null() {
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
        unsafe {
            assert!(!kafka_producer_MockProducer_complete_next(producer));
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_complete_next_null() {
        unsafe {
            assert!(!kafka_producer_MockProducer_complete_next(std::ptr::null_mut()));
        }
    }

    #[test]
    fn test_mock_error_next_no_pending() {
        let producer = kafka_producer_MockProducer_new(false);
        let msg = CString::new("err").unwrap();
        unsafe {
            assert!(!kafka_producer_MockProducer_error_next(producer, 2, msg.as_ptr()));
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_error_next_null_message() {
        let producer = kafka_producer_MockProducer_new(false);
        let topic = CString::new("topic").unwrap();

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
            let metadata = kafka_producer_FutureRecordMetadata_get(future, &mut err);
            assert_error(err);
            assert!(metadata.is_null());

            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_error_next_null_producer() {
        let msg = CString::new("err").unwrap();
        unsafe {
            assert!(!kafka_producer_MockProducer_error_next(std::ptr::null_mut(), 2, msg.as_ptr()));
        }
    }

    #[test]
    fn test_mock_history_count() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

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

            kafka_producer_FutureRecordMetadata_destroy(f1);
            kafka_producer_FutureRecordMetadata_destroy(f2);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_history_count_null() {
        unsafe {
            assert_eq!(kafka_producer_MockProducer_history_count(std::ptr::null()), 0);
        }
    }

    #[test]
    fn test_mock_clear() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

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

            kafka_producer_FutureRecordMetadata_destroy(f);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_clear_null() {
        unsafe {
            kafka_producer_MockProducer_clear(std::ptr::null_mut());
        }
    }

    // -- Error handle tests -------------------------------------------------

    #[test]
    fn test_error_code_and_message() {
        let producer = kafka_producer_MockProducer_new(true);
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
            let metadata = kafka_producer_FutureRecordMetadata_get(future, &mut err);
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
            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_multiple_sends_incrementing_offsets() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

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
                let metadata = kafka_producer_FutureRecordMetadata_get(future, &mut err);
                assert_success(err);
                assert_eq!(kafka_producer_RecordMetadata_offset(metadata), expected_offset);

                kafka_producer_RecordMetadata_destroy(metadata);
                kafka_producer_FutureRecordMetadata_destroy(future);
            }

            kafka_producer_Producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_after_close_returns_error() {
        let producer = kafka_producer_MockProducer_new(true);
        let topic = CString::new("topic").unwrap();

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
            assert!(kafka_producer_FutureRecordMetadata_is_done(future));

            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    // -- ProducerProperties tests ---------------------------------------------

    #[test]
    fn test_properties_new_and_put() {
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
        unsafe {
            let configs = [k1.as_ptr(), v1.as_ptr(), k2.as_ptr(), std::ptr::null()];
            let props = kafka_producer_ProducerProperties_from_configs(configs.as_ptr());
            assert!(props.is_null());
        }
    }

    #[test]
    fn test_properties_from_configs_empty() {
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
        unsafe {
            kafka_producer_ProducerProperties_destroy(std::ptr::null_mut());
        }
    }

    // -- KafkaProducer lifecycle tests ----------------------------------------

    /// Helper: creates a KafkaProducer via FFI with the given bootstrap servers.
    unsafe fn create_kafka_producer(bootstrap: &str) -> (*mut kafka_producer_Producer_t, *mut kafka_common_Error_t) {
        let key = CString::new("bootstrap.servers").unwrap();
        let val = CString::new(bootstrap).unwrap();
        let configs = [key.as_ptr(), val.as_ptr(), std::ptr::null()];
        let props = unsafe { kafka_producer_ProducerProperties_from_configs(configs.as_ptr()) };
        let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
        let producer = unsafe { kafka_producer_KafkaProducer_new(props, &mut err) };
        unsafe { kafka_producer_ProducerProperties_destroy(props) };
        (producer, err)
    }

    #[test]
    fn test_create_and_destroy_kafka_producer() {
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
        unsafe {
            let mut err: *mut kafka_common_Error_t = std::ptr::null_mut();
            let producer = kafka_producer_KafkaProducer_new(std::ptr::null(), &mut err);
            assert_error(err);
            assert!(producer.is_null());
        }
    }

    #[test]
    fn test_create_kafka_producer_null_out_error() {
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
            assert!(kafka_producer_FutureRecordMetadata_is_done(future));

            kafka_producer_FutureRecordMetadata_destroy(future);
            kafka_producer_Producer_destroy(producer);
        }
    }

    // -- Metrics tests ------------------------------------------------------

    fn metric_fixture(
        name: &str,
        tags: &[(&str, &str)],
        provider: crate::common::metrics::MetricValueProvider,
    ) -> (MetricName, Arc<KafkaMetric>) {
        use crate::common::metrics::{MetricConfig, SystemTime};
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
        assert_eq!(unsafe { kafka_producer_MetricMap_count(map) }, 4);

        let mut seen = 0;
        for i in 0..4 {
            let name = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_name(map, i)) }
                .to_str()
                .unwrap()
                .to_string();
            let kind = unsafe { kafka_producer_MetricMap_get_value_kind(map, i) };
            let group = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_group(map, i)) };
            assert_eq!(group.to_str().unwrap(), "grp");
            let desc = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_description(map, i)) };
            assert_eq!(desc.to_str().unwrap(), "desc");
            match name.as_str() {
                "measurable" => {
                    assert_eq!(kind, common::METRIC_VALUE_DOUBLE);
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_value_double(map, i) }, 42.5);
                    // Tags are sorted (BTreeMap): client-id then topic.
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_tag_count(map, i) }, 2);
                    let k0 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_key(map, i, 0)) };
                    let v0 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_value(map, i, 0)) };
                    assert_eq!((k0.to_str().unwrap(), v0.to_str().unwrap()), ("client-id", "c1"));
                    let k1 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_key(map, i, 1)) };
                    let v1 = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_tag_value(map, i, 1)) };
                    assert_eq!((k1.to_str().unwrap(), v1.to_str().unwrap()), ("topic", "t"));
                },
                "as-string" => {
                    assert_eq!(kind, common::METRIC_VALUE_STRING);
                    let s = unsafe { CStr::from_ptr(kafka_producer_MetricMap_get_value_string(map, i)) };
                    assert_eq!(s.to_str().unwrap(), "hello");
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_tag_count(map, i) }, 0);
                },
                "as-long" => {
                    assert_eq!(kind, common::METRIC_VALUE_LONG);
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_value_long(map, i) }, -9_000_000_000);
                },
                "as-int" => {
                    assert_eq!(kind, common::METRIC_VALUE_INT);
                    assert_eq!(unsafe { kafka_producer_MetricMap_get_value_int(map, i) }, -7);
                },
                other => panic!("unexpected metric name {other}"),
            }
            seen += 1;
        }
        assert_eq!(seen, 4);

        unsafe { kafka_producer_MetricMap_destroy(map) };
    }

    /// Out-of-range indices are reported rather than panicking, matching the
    /// other `*_get_*` accessors.
    #[test]
    fn metric_map_out_of_range_accessors_are_safe() {
        let map = Box::into_raw(common::build_metric_map_inner(HashMap::new())) as *mut kafka_producer_MetricMap_t;
        assert_eq!(unsafe { kafka_producer_MetricMap_count(map) }, 0);
        assert!(unsafe { kafka_producer_MetricMap_get_name(map, 0) }.is_null());
        assert!(unsafe { kafka_producer_MetricMap_get_name(map, -1) }.is_null());
        assert!(unsafe { kafka_producer_MetricMap_get_value_string(map, 5) }.is_null());
        assert_eq!(unsafe { kafka_producer_MetricMap_get_tag_count(map, 0) }, -1);
        assert!(unsafe { kafka_producer_MetricMap_get_tag_key(map, 0, 0) }.is_null());
        assert_eq!(unsafe { kafka_producer_MetricMap_get_value_double(map, 0) }, 0.0);
        assert_eq!(unsafe { kafka_producer_MetricMap_get_value_long(map, 0) }, 0);
        assert_eq!(unsafe { kafka_producer_MetricMap_get_value_int(map, 0) }, 0);
        assert_eq!(
            unsafe { kafka_producer_MetricMap_get_value_kind(map, 0) },
            common::METRIC_VALUE_DOUBLE
        );
        unsafe { kafka_producer_MetricMap_destroy(map) };
        // Destroy is null-safe.
        unsafe { kafka_producer_MetricMap_destroy(std::ptr::null_mut()) };
    }

    /// `kafka_producer_Producer_metrics` on a `MockProducer` returns a valid,
    /// empty snapshot handle by default (no metrics seeded via the mock's
    /// `set_mock_metrics`, which the FFI does not expose). Null producer yields
    /// a null handle.
    #[test]
    fn test_mock_producer_metrics_snapshot() {
        let producer = kafka_producer_MockProducer_new(true);
        let map = unsafe { kafka_producer_Producer_metrics(producer) };
        assert!(!map.is_null());
        assert_eq!(unsafe { kafka_producer_MetricMap_count(map) }, 0);
        unsafe { kafka_producer_MetricMap_destroy(map) };
        unsafe { kafka_producer_Producer_destroy(producer) };

        // Null producer -> null handle (no panic).
        let null_map = unsafe { kafka_producer_Producer_metrics(std::ptr::null_mut()) };
        assert!(null_map.is_null());
    }
}
