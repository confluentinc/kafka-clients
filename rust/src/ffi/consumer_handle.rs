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

//! C FFI layer for the consumer **reentrancy handle**
//! ([`crate::consumer::ConsumerHandle`]).
//!
//! `kafka_consumer_ConsumerHandle_t` is the C view of the core
//! `ConsumerHandle`: the sanctioned way to call back into a consumer from
//! *inside* a user callback (a rebalance listener, an offset-commit callback),
//! which is what Java code does when it captures the `consumer` variable and
//! calls `consumer.commitSync()` from `onPartitionsRevoked` (see
//! `consumer-threading.md` §31). Obtain one with
//! [`kafka_consumer_Consumer_handle`] and release it with
//! [`kafka_consumer_ConsumerHandle_destroy`].
//!
//! # No access guard — by design
//!
//! Nothing in this module acquires the single-owner access guard that
//! [`super::consumer`] uses (`acquire`/`release`). That is deliberate and is
//! the whole reason the type exists: every method of the core `ConsumerHandle`
//! takes `&self` and the type is `Clone + Send + Sync`, so there is no
//! exclusive borrow to serialize. A handle operation therefore succeeds while
//! another consumer operation is in flight, whereas the equivalent
//! `kafka_consumer_Consumer_*` call would be rejected with
//! `ConcurrentModificationError` (`consumer-threading.md` §31, "In-callback
//! reentrancy"; known risk 2 of the callback-bridging design).
//!
//! # Threading contract
//!
//! Every operation that is `async` in the core is exposed here as a
//! **synchronous** C function that drives the future to completion on the
//! *calling* thread (`tokio::runtime::Handle::block_on`). Consequently:
//!
//! - **Safe** to call from the consumer's callback dispatcher thread (where
//!   all C callbacks are invoked) and from any plain OS thread the embedder
//!   owns. The consumer's background task keeps spinning on its own runtime
//!   worker threads, so the awaited operation makes progress.
//! - **Must NOT** be called from a thread that is already inside a tokio
//!   runtime (a worker thread, or any code reached from `block_on`):
//!   `Handle::block_on` panics there. Rather than let a panic cross the FFI
//!   boundary (CLAUDE.md §12.1), every entry point detects that situation and
//!   fails with an `IllegalStateError` instead. The only way an embedder can
//!   hit this is by calling a handle op from a callback that ran *inline on a
//!   runtime worker* — which happens only in the documented teardown fallback
//!   (`enqueue_or_run_inline` after the dispatcher is gone; known risk 3),
//!   i.e. after the consumer was destroyed without being closed first.
//!
//! # Lifetime contract
//!
//! A handle borrows nothing from C, but it keeps the consumer's shared state
//! alive only in the memory-safety sense: the owning consumer's background
//! task and runtime are shut down by
//! [`super::consumer::kafka_consumer_Consumer_destroy`], after which handle
//! operations can no longer complete. Therefore:
//!
//! - A handle is usable only while its consumer is alive.
//! - Destroy every handle **before** destroying the consumer.
//! - Handles are independent: destroying one does not affect another, and
//!   destroying a handle does not affect the consumer.
//!
//! # Method surface
//!
//! `wakeup`, the three sync getters (`assignment` / `subscription` /
//! `paused`), and the seventeen async operations of the core handle. Lifecycle
//! operations (`poll`, `subscribe`, `unsubscribe`, `close`) are intentionally
//! absent — Java never invokes those reentrantly from a callback — and so are
//! callback-taking commit variants, matching the core handle (use
//! [`super::consumer::kafka_consumer_Consumer_commit_async_with_callback`] on
//! the owning consumer for those).
//!
//! On a handle obtained from a `MockConsumer`, `wakeup` works and the sync
//! getters return empty lists, but every async operation fails with
//! `UnsupportedVersionError` — the mock has no event pipeline, so its test
//! surface drives the `MockConsumer` entry points directly. This is core
//! behavior, not an FFI limitation.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

use std::ffi::{CStr, c_char};
use std::future::Future;
use std::time::Duration;

use crate::common::{Error, TopicPartition};
use crate::consumer::{ConsumerHandle, OffsetAndMetadata};

use super::common::{box_error, kafka_common_Error_t};
use super::consumer::{
    box_long_offset_map, box_offset_and_timestamp_map, box_offset_map, box_string_list, box_topic_partition_list,
    clone_core_handle, kafka_common_TopicPartitionList_t, kafka_consumer_Consumer_t, kafka_consumer_LongOffsetMap_t,
    kafka_consumer_OffsetAndTimestampMap_t, kafka_consumer_OffsetMap_t, kafka_consumer_StringList_t, read_offset_map,
    read_timestamps_to_search, read_topic_partitions,
};
use super::ffi_guard;

// ---------------------------------------------------------------------------
// Opaque type + wrapper
// ---------------------------------------------------------------------------

/// Opaque consumer reentrancy-handle (see the module documentation).
#[repr(C)]
pub struct kafka_consumer_ConsumerHandle_t {
    _private: [u8; 0],
}

/// Backing state of a [`kafka_consumer_ConsumerHandle_t`]: a clone of the core
/// handle plus a handle to the owning consumer's runtime, which drives the
/// core's `async fn`s on the calling thread.
struct ConsumerHandleWrapper {
    inner: ConsumerHandle,
    runtime: tokio::runtime::Handle,
}

/// Casts a `*const kafka_consumer_ConsumerHandle_t` to a
/// `&'static ConsumerHandleWrapper`.
///
/// # Safety
///
/// `handle` must be non-null and created by [`kafka_consumer_Consumer_handle`].
unsafe fn wrapper_ref(handle: *const kafka_consumer_ConsumerHandle_t) -> &'static ConsumerHandleWrapper {
    // SAFETY: Per this helper's `# Safety`, `handle` is non-null and was created by
    // `kafka_consumer_Consumer_handle`, the only producer of such pointers, which leaks a
    // `Box<ConsumerHandleWrapper>` via `Box::into_raw`; the allocation therefore has the
    // layout of `ConsumerHandleWrapper` and stays live until
    // `kafka_consumer_ConsumerHandle_destroy` reclaims it with `Box::from_raw`, after which
    // that function's contract declares the pointer invalid. Only a shared reference is
    // produced; every method of the wrapped core `ConsumerHandle` takes `&self` and the
    // type is `Clone + Send + Sync` (module docs, "No access guard"), so concurrent calls
    // through one handle from several threads are sound. The `'static` lifetime is a
    // convenience: each caller (`kafka_consumer_ConsumerHandle_wakeup`, the three sync
    // getters, and the `block_on_void` / `block_on_value` / `block_on_position` drivers,
    // which hand `&w.inner` to a future they run to completion with `Handle::block_on`
    // before returning) uses the reference only for the duration of its own synchronous FFI
    // call, during which the C caller keeps the handle alive (module docs, "Lifetime
    // contract").
    unsafe { &*(handle as *const ConsumerHandleWrapper) }
}

/// Rejects a call made from inside a tokio runtime, where
/// [`tokio::runtime::Handle::block_on`] would panic. See the module
/// documentation ("Threading contract").
fn ensure_blocking_allowed() -> Result<(), Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::local_illegal_state(
            "kafka_consumer_ConsumerHandle operations block the calling thread and cannot be \
             called from within an async runtime; call them from the consumer's callback \
             dispatcher thread or from a plain thread.",
        ));
    }
    Ok(())
}

/// Drives a void-returning core-handle future to completion on the calling
/// thread. Returns null on success, or an owned error handle on failure.
fn block_on_void<F>(handle: *const kafka_consumer_ConsumerHandle_t, op: F) -> *mut kafka_common_Error_t
where
    F: FnOnce(&'static ConsumerHandle) -> BoxFuture,
{
    if let Err(e) = ensure_blocking_allowed() {
        return box_error(e);
    }
    // SAFETY: `wrapper_ref` requires `handle` non-null and created by
    // `kafka_consumer_Consumer_handle`. `block_on_void` performs no check itself: it is
    // private to this module and its only callers are the `extern "C"` entry points
    // (`assign`, `seek`, `seek_with_metadata`, `seek_to_beginning`, `seek_to_end`, `pause`,
    // `resume`, `commit_sync`, `commit_sync_offsets`, `commit_async`,
    // `commit_async_offsets`), each of which forwards its own `handle` parameter unchanged
    // and requires exactly that of it in its `# Safety` (a valid handle from
    // `kafka_consumer_Consumer_handle`). The `&'static ConsumerHandleWrapper` and the
    // `&w.inner` borrowed from it live only as long as this call: `op` builds a future that
    // `w.runtime.block_on` drives to completion on the calling thread before returning, the
    // core `async fn`s borrow `&self` only for the returned future's lifetime, and nothing
    // is stored or spawned, while the C caller keeps the handle alive for the call per the
    // module's lifetime contract. `ensure_blocking_allowed` has already established that
    // this thread is not inside a tokio runtime, so `Handle::block_on` is permitted here
    // (module docs, "Threading contract").
    let w = unsafe { wrapper_ref(handle) };
    match w.runtime.block_on(op(&w.inner)) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => box_error(e),
    }
}

/// Boxed future type used by [`block_on_void`], so one helper can serve every
/// void-returning core-handle method (each returns a distinct anonymous
/// future). One `Box` per FFI call is amortized over a whole consumer
/// operation — not a hot path (CLAUDE.md §13).
type BoxFuture = std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + 'static>>;

/// Drives a value-returning core-handle future to completion on the calling
/// thread, handing the `Ok` value to `complete` (which builds the C handle) and
/// writing it through `out`. Returns null on success, or an owned error handle
/// on failure — in which case `*out` is left untouched.
///
/// A null `out` discards the result without allocating a C handle for it,
/// mirroring the `kafka_consumer_Consumer_*` sync variants.
fn block_on_value<T, F, C, H>(
    handle: *const kafka_consumer_ConsumerHandle_t,
    out: *mut *mut H,
    op: F,
    complete: C,
) -> *mut kafka_common_Error_t
where
    F: FnOnce(&'static ConsumerHandle) -> std::pin::Pin<Box<dyn Future<Output = Result<T, Error>> + 'static>>,
    C: FnOnce(T) -> *mut H,
{
    if let Err(e) = ensure_blocking_allowed() {
        return box_error(e);
    }
    // SAFETY: `wrapper_ref` requires `handle` non-null and created by
    // `kafka_consumer_Consumer_handle`. `block_on_value` performs no check itself: it is
    // private to this module and its only callers are the `extern "C"` entry points
    // `committed`, `beginning_offsets`, `end_offsets` and `offsets_for_times`, each of
    // which forwards its own `handle` parameter unchanged and requires a valid handle of it
    // in its `# Safety`. The `&'static ConsumerHandleWrapper` and the `&w.inner` borrowed
    // from it live only as long as this call: `op` builds a future that
    // `w.runtime.block_on` drives to completion on the calling thread before returning, the
    // core `async fn`s borrow `&self` only for the returned future's lifetime, and nothing
    // is stored or spawned, while the C caller keeps the handle alive for the call per the
    // module's lifetime contract. `ensure_blocking_allowed` has already established that
    // this thread is not inside a tokio runtime, so `Handle::block_on` is permitted here
    // (module docs, "Threading contract").
    let w = unsafe { wrapper_ref(handle) };
    match w.runtime.block_on(op(&w.inner)) {
        Ok(value) => {
            if !out.is_null() {
                // SAFETY: `out` is non-null (checked above) and, per the calling entry
                // point's `# Safety` (`out_map` valid), points to a writable `*mut H` slot
                // owned by the C caller for the duration of the call. Exactly one
                // pointer-sized element is written, and only on the success path, so a
                // failure leaves `*out` untouched as the entry points document; the value
                // written is the freshly boxed handle built by `complete`, whose ownership
                // transfers to the C caller.
                unsafe { *out = complete(value) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

// ---------------------------------------------------------------------------
// Constructor / lifecycle
// ---------------------------------------------------------------------------

/// Returns a new reentrancy handle for `consumer` — the C equivalent of the
/// core `Consumer::handle()`.
///
/// The handle is independent of the consumer's access guard, so it can be used
/// from inside a user callback while a consumer operation is in flight (see the
/// [module documentation](self)). It does **not** acquire the guard, so this
/// call never fails with `ConcurrentModificationError`.
///
/// # Returns
///
/// A non-null handle. The caller owns it and must free it with
/// [`kafka_consumer_ConsumerHandle_destroy`] **before**
/// [`super::consumer::kafka_consumer_Consumer_destroy`].
///
/// # Safety
///
/// `consumer` must be a valid handle from a consumer constructor.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_handle(
    consumer: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_ConsumerHandle_t {
    // SAFETY: `clone_core_handle` requires `consumer` non-null and created by a consumer
    // constructor, which is exactly what this function's `# Safety` requires of `consumer`
    // ("a valid handle from a consumer constructor"). The borrow it takes through
    // `handle_ref` lasts only for the two field clones performed during this call; the
    // returned `ConsumerHandle` and `tokio::runtime::Handle` are owned values, so the
    // wrapper built from them borrows nothing from the consumer's allocation. The module's
    // lifetime contract (destroy handles before the consumer) concerns the runtime still
    // being able to complete operations, not memory validity.
    let (inner, runtime) = unsafe { clone_core_handle(consumer) };
    let wrapper = Box::new(ConsumerHandleWrapper { inner, runtime });
    Box::into_raw(wrapper) as *mut kafka_consumer_ConsumerHandle_t
}

/// Destroys a reentrancy handle. Safe to call with a null pointer (no-op).
///
/// Destroying a handle never affects the owning consumer or any other handle.
///
/// # Safety
///
/// `handle` must be null or a valid handle from
/// [`kafka_consumer_Consumer_handle`]. After this call the pointer is invalid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_destroy(handle: *mut kafka_consumer_ConsumerHandle_t) {
    if handle.is_null() {
        return;
    }
    // SAFETY: `handle` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from `kafka_consumer_Consumer_handle`, which created it with
    // `Box::into_raw(Box::new(ConsumerHandleWrapper { .. }))`, so the cast restores the
    // original type and allocation. The contract declares the pointer invalid after this
    // call, making this its single, final use. Dropping the wrapper releases only its own
    // clones of the core `ConsumerHandle` and of the runtime `Handle`, so neither the
    // owning consumer nor any other reentrancy handle is affected (module docs, "Lifetime
    // contract").
    drop(unsafe { Box::from_raw(handle as *mut ConsumerHandleWrapper) });
}

/// Wakes up the owning consumer, exactly like
/// [`super::consumer::kafka_consumer_Consumer_wakeup`]. Callable from any
/// thread (it neither blocks nor takes the guard). Safe to call with a null
/// pointer (no-op).
///
/// # Safety
///
/// `handle` must be null or a valid handle from
/// [`kafka_consumer_Consumer_handle`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_wakeup(handle: *const kafka_consumer_ConsumerHandle_t) {
    if handle.is_null() {
        return;
    }
    // SAFETY: `handle` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from `kafka_consumer_Consumer_handle`, which is what `wrapper_ref`
    // requires. The reference is used only for the synchronous, non-blocking
    // `ConsumerHandle::wakeup` call on the calling thread, during which the C caller keeps
    // the handle alive; `wakeup` takes `&self` and is callable from any thread, so no guard
    // or runtime context is needed.
    unsafe { wrapper_ref(handle) }.inner.wakeup();
}

// ---------------------------------------------------------------------------
// Sync getters
//
// Unlike their `kafka_consumer_Consumer_*` siblings these never return null on
// a concurrent-access rejection — there is no guard to reject them.
// ---------------------------------------------------------------------------

/// Returns the owning consumer's current assignment as a non-null
/// [`kafka_common_TopicPartitionList_t`] (free it with
/// [`super::consumer::kafka_common_TopicPartitionList_destroy`]).
///
/// Always empty on a `MockConsumer`-derived handle (core behavior).
///
/// # Safety
///
/// `handle` must be a valid handle from [`kafka_consumer_Consumer_handle`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_assignment(
    handle: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_common_TopicPartitionList_t {
    // SAFETY: `wrapper_ref` requires `handle` non-null and created by
    // `kafka_consumer_Consumer_handle`, which this function's `# Safety` requires of
    // `handle` ("a valid handle from `kafka_consumer_Consumer_handle`"). The reference is
    // used only for the synchronous `ConsumerHandle::assignment()` call, which copies the
    // set out of the shared `SubscriptionState` under its own mutex;
    // `box_topic_partition_list` then moves that copy into a new, caller-owned list, so
    // nothing borrows the handle after this call returns, during which the C caller keeps
    // it alive.
    box_topic_partition_list(unsafe { wrapper_ref(handle) }.inner.assignment())
}

/// Returns the owning consumer's current topic subscription as a non-null
/// [`kafka_consumer_StringList_t`] (free it with
/// [`super::consumer::kafka_consumer_StringList_destroy`]).
///
/// Always empty on a `MockConsumer`-derived handle (core behavior).
///
/// # Safety
///
/// `handle` must be a valid handle from [`kafka_consumer_Consumer_handle`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_subscription(
    handle: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_consumer_StringList_t {
    // SAFETY: `wrapper_ref` requires `handle` non-null and created by
    // `kafka_consumer_Consumer_handle`, which this function's `# Safety` requires of
    // `handle` ("a valid handle from `kafka_consumer_Consumer_handle`"). The reference is
    // used only for the synchronous `ConsumerHandle::subscription()` call, which copies the
    // set out of the shared `SubscriptionState` under its own mutex; `box_string_list` then
    // moves that copy into a new, caller-owned list, so nothing borrows the handle after
    // this call returns, during which the C caller keeps it alive.
    box_string_list(unsafe { wrapper_ref(handle) }.inner.subscription())
}

/// Returns the owning consumer's currently paused partitions as a non-null
/// [`kafka_common_TopicPartitionList_t`] (free it with
/// [`super::consumer::kafka_common_TopicPartitionList_destroy`]).
///
/// Always empty on a `MockConsumer`-derived handle (core behavior).
///
/// # Safety
///
/// `handle` must be a valid handle from [`kafka_consumer_Consumer_handle`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_paused(
    handle: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_common_TopicPartitionList_t {
    // SAFETY: `wrapper_ref` requires `handle` non-null and created by
    // `kafka_consumer_Consumer_handle`, which this function's `# Safety` requires of
    // `handle` ("a valid handle from `kafka_consumer_Consumer_handle`"). The reference is
    // used only for the synchronous `ConsumerHandle::paused()` call, which copies the set
    // out of the shared `SubscriptionState` under its own mutex; `box_topic_partition_list`
    // then moves that copy into a new, caller-owned list, so nothing borrows the handle
    // after this call returns, during which the C caller keeps it alive.
    box_topic_partition_list(unsafe { wrapper_ref(handle) }.inner.paused())
}

// ---------------------------------------------------------------------------
// assign / seek / pause / resume
// ---------------------------------------------------------------------------

/// Assigns the owning consumer to a set of `(topic, partition)` pairs.
/// Returns null on success, non-null error on failure.
///
/// An **empty** assignment is rejected: on the owning consumer `assign([])`
/// leaves the group, which the reentrancy handle deliberately does not expose
/// (`consumer-threading.md` §31). The core error is returned unchanged.
///
/// # Safety
///
/// `handle` must be a valid handle; `topics` `count` valid C strings and
/// `partitions` `count` `i32` values.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_assign(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises exactly that (`topics`
    // `count` valid C strings and `partitions` `count` `i32` values) for the duration of
    // the call. The helper copies each entry into an owned `TopicPartition`, so nothing
    // borrows the C arrays after it returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_void(handle, move |h| Box::pin(async move { h.assign(tps).await }))
}

/// Seeks a single partition to `offset`. Returns null on success, non-null
/// error on failure.
///
/// # Safety
///
/// `handle` must be a valid handle; `topic` a valid C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: `topic_partition` requires `topic` to be a valid, NUL-terminated C string,
    // which this function's `# Safety` requires of `topic` for the duration of the call;
    // the helper copies the bytes into an owned `String`, so the C string is not borrowed
    // after it returns.
    let tp = unsafe { topic_partition(topic, partition) };
    block_on_void(handle, move |h| Box::pin(async move { h.seek_with_offset(tp, offset).await }))
}

/// Seeks a single partition to `offset` with commit metadata / leader epoch.
/// Pass `metadata == NULL` for no metadata and `leader_epoch < 0` for no
/// leader epoch. Returns null on success, non-null error on failure.
///
/// # Safety
///
/// `handle` must be a valid handle; `topic` a valid C string; `metadata` null
/// or a valid C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_metadata(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    metadata: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: `topic_partition` requires `topic` to be a valid, NUL-terminated C string,
    // which this function's `# Safety` requires of `topic` for the duration of the call;
    // the helper copies the bytes into an owned `String`, so the C string is not borrowed
    // after it returns.
    let tp = unsafe { topic_partition(topic, partition) };
    let metadata_str = if metadata.is_null() {
        String::new()
    } else {
        // SAFETY: `metadata` is non-null (checked above) and, per this function's `#
        // Safety`, null or a valid C string for the duration of the call; the bytes are
        // copied into an owned `String` at once, so the C string is not borrowed
        // afterwards.
        unsafe { CStr::from_ptr(metadata) }.to_string_lossy().to_string()
    };
    let epoch = if leader_epoch < 0 { None } else { Some(leader_epoch) };
    let oam = match OffsetAndMetadata::with_leader_epoch_metadata(offset, epoch, metadata_str) {
        Ok(o) => o,
        Err(e) => return box_error(e),
    };
    block_on_void(handle, move |h| {
        Box::pin(async move { h.seek_with_offset_and_metadata(tp, oam).await })
    })
}

/// Seeks the given partitions to their beginning offsets. Returns null on
/// success, non-null error on failure.
///
/// # Safety
///
/// `handle` must be a valid handle; `topics` `count` valid C strings and
/// `partitions` `count` `i32` values.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_beginning(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises `count` entries in each
    // array for the duration of the call, which for the `const char *` array `topics` means
    // `count` valid, non-NULL C strings (the wording of
    // `kafka_consumer_ConsumerHandle_assign` states this explicitly). The helper copies
    // each entry into an owned `TopicPartition`, so nothing borrows the C arrays after it
    // returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_void(handle, move |h| Box::pin(async move { h.seek_to_beginning(&tps).await }))
}

/// Seeks the given partitions to their end offsets. Returns null on success,
/// non-null error on failure.
///
/// # Safety
///
/// `handle` must be a valid handle; `topics` `count` valid C strings and
/// `partitions` `count` `i32` values.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_end(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises `count` entries in each
    // array for the duration of the call, which for the `const char *` array `topics` means
    // `count` valid, non-NULL C strings (the wording of
    // `kafka_consumer_ConsumerHandle_assign` states this explicitly). The helper copies
    // each entry into an owned `TopicPartition`, so nothing borrows the C arrays after it
    // returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_void(handle, move |h| Box::pin(async move { h.seek_to_end(&tps).await }))
}

/// Pauses fetching for the given partitions. Returns null on success,
/// non-null error on failure.
///
/// # Safety
///
/// `handle` must be a valid handle; `topics` `count` valid C strings and
/// `partitions` `count` `i32` values.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_pause(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises `count` entries in each
    // array for the duration of the call, which for the `const char *` array `topics` means
    // `count` valid, non-NULL C strings (the wording of
    // `kafka_consumer_ConsumerHandle_assign` states this explicitly). The helper copies
    // each entry into an owned `TopicPartition`, so nothing borrows the C arrays after it
    // returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_void(handle, move |h| Box::pin(async move { h.pause(&tps).await }))
}

/// Resumes fetching for the given partitions. Returns null on success,
/// non-null error on failure.
///
/// # Safety
///
/// `handle` must be a valid handle; `topics` `count` valid C strings and
/// `partitions` `count` `i32` values.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_resume(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises `count` entries in each
    // array for the duration of the call, which for the `const char *` array `topics` means
    // `count` valid, non-NULL C strings (the wording of
    // `kafka_consumer_ConsumerHandle_assign` states this explicitly). The helper copies
    // each entry into an owned `TopicPartition`, so nothing borrows the C arrays after it
    // returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_void(handle, move |h| Box::pin(async move { h.resume(&tps).await }))
}

// ---------------------------------------------------------------------------
// position / committed / offset queries
// ---------------------------------------------------------------------------

/// Returns the current position of `(topic, partition)` using the consumer's
/// `default.api.timeout.ms`. On success writes the offset to `*out_position`
/// and returns null; on failure returns a non-null error handle and leaves
/// `*out_position` untouched.
///
/// # Safety
///
/// `handle` a valid handle; `topic` a valid C string; `out_position` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topic: *const c_char,
    partition: i32,
    out_position: *mut i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: `topic_partition` requires `topic` to be a valid, NUL-terminated C string,
    // which this function's `# Safety` requires of `topic` for the duration of the call;
    // the helper copies the bytes into an owned `String`, so the C string is not borrowed
    // after it returns.
    let tp = unsafe { topic_partition(topic, partition) };
    block_on_position(handle, out_position, move |h| Box::pin(async move { h.position(&tp).await }))
}

/// Returns the current position of `(topic, partition)`, bounded by
/// `timeout_ms`. See [`kafka_consumer_ConsumerHandle_position`].
///
/// # Safety
///
/// `handle` a valid handle; `topic` a valid C string; `out_position` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position_timeout(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topic: *const c_char,
    partition: i32,
    timeout_ms: i64,
    out_position: *mut i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: `topic_partition` requires `topic` to be a valid, NUL-terminated C string,
    // which this function's `# Safety` requires of `topic` for the duration of the call;
    // the helper copies the bytes into an owned `String`, so the C string is not borrowed
    // after it returns.
    let tp = unsafe { topic_partition(topic, partition) };
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    block_on_position(handle, out_position, move |h| {
        Box::pin(async move { h.position_with_timeout(&tp, timeout).await })
    })
}

/// Returns the last committed offsets for the given partitions. On success
/// writes a [`kafka_consumer_OffsetMap_t`] to `*out_map` (free it with
/// [`super::consumer::kafka_consumer_OffsetMap_destroy`]) and returns null; on
/// failure returns a non-null error and leaves `*out_map` untouched.
///
/// # Safety
///
/// `handle` a valid handle; `topics` `count` valid C strings and `partitions`
/// `count` `i32` values; `out_map` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_committed(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_OffsetMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises `count` entries in each
    // array for the duration of the call, which for the `const char *` array `topics` means
    // `count` valid, non-NULL C strings (the wording of
    // `kafka_consumer_ConsumerHandle_assign` states this explicitly). The helper copies
    // each entry into an owned `TopicPartition`, so nothing borrows the C arrays after it
    // returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_value(
        handle,
        out_map,
        move |h| Box::pin(async move { h.committed(&tps).await }),
        box_offset_map,
    )
}

/// Returns the beginning offsets for the given partitions. On success writes a
/// [`kafka_consumer_LongOffsetMap_t`] to `*out_map` (free it with
/// [`super::consumer::kafka_consumer_LongOffsetMap_destroy`]).
///
/// # Safety
///
/// `handle` a valid handle; `topics` `count` valid C strings and `partitions`
/// `count` `i32` values; `out_map` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_beginning_offsets(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_LongOffsetMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises `count` entries in each
    // array for the duration of the call, which for the `const char *` array `topics` means
    // `count` valid, non-NULL C strings (the wording of
    // `kafka_consumer_ConsumerHandle_assign` states this explicitly). The helper copies
    // each entry into an owned `TopicPartition`, so nothing borrows the C arrays after it
    // returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_value(
        handle,
        out_map,
        move |h| Box::pin(async move { h.beginning_offsets(&tps).await }),
        box_long_offset_map,
    )
}

/// Returns the end offsets for the given partitions. On success writes a
/// [`kafka_consumer_LongOffsetMap_t`] to `*out_map` (free it with
/// [`super::consumer::kafka_consumer_LongOffsetMap_destroy`]).
///
/// # Safety
///
/// `handle` a valid handle; `topics` `count` valid C strings and `partitions`
/// `count` `i32` values; `out_map` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_end_offsets(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    out_map: *mut *mut kafka_consumer_LongOffsetMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_topic_partitions` requires `topics` to point to `count` valid C strings
    // and `partitions` to `count` `i32` values; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL entry. This function's `# Safety` promises `count` entries in each
    // array for the duration of the call, which for the `const char *` array `topics` means
    // `count` valid, non-NULL C strings (the wording of
    // `kafka_consumer_ConsumerHandle_assign` states this explicitly). The helper copies
    // each entry into an owned `TopicPartition`, so nothing borrows the C arrays after it
    // returns.
    let tps = unsafe { read_topic_partitions(topics, partitions, count) };
    block_on_value(
        handle,
        out_map,
        move |h| Box::pin(async move { h.end_offsets(&tps).await }),
        box_long_offset_map,
    )
}

/// Looks up offsets by timestamp for the given partitions. Parallel arrays of
/// `(topic, partition, timestamp)`. On success writes a
/// [`kafka_consumer_OffsetAndTimestampMap_t`] to `*out_map` (free it with
/// [`super::consumer::kafka_consumer_OffsetAndTimestampMap_destroy`]);
/// unresolved partitions are omitted from the map.
///
/// # Safety
///
/// `handle` a valid handle; arrays `count` valid entries; `out_map` valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_offsets_for_times(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    timestamps: *const i64,
    count: i32,
    out_map: *mut *mut kafka_consumer_OffsetAndTimestampMap_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_timestamps_to_search` requires `topics`, `partitions` and `timestamps`
    // to have `count` valid entries each; it clamps a negative `count` to zero and
    // dereferences every `topics[i]` with `CStr::from_ptr`, so it tolerates neither a NULL
    // array nor a NULL topic entry. This function's `# Safety` promises `count` valid
    // entries in the arrays for the duration of the call. The helper copies each entry into
    // an owned map, so nothing borrows the C arrays after it returns.
    let req = unsafe { read_timestamps_to_search(topics, partitions, timestamps, count) };
    block_on_value(
        handle,
        out_map,
        move |h| Box::pin(async move { h.offsets_for_times(req).await }),
        box_offset_and_timestamp_map,
    )
}

// ---------------------------------------------------------------------------
// commit
// ---------------------------------------------------------------------------

/// Commits the offsets the owning consumer has consumed, synchronously
/// (Java `commitSync()` with no arguments). Returns null on success, non-null
/// error on failure.
///
/// This is the operation a rebalance listener calls to flush offsets before
/// its partitions are taken away (`consumer-threading.md` §31).
///
/// # Safety
///
/// `handle` must be a valid handle from [`kafka_consumer_Consumer_handle`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync(
    handle: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_common_Error_t {
    block_on_void(handle, |h| Box::pin(async move { h.commit_sync().await }))
}

/// Commits specific offsets synchronously. Parallel arrays of
/// `(topic, partition, offset, leader_epoch, metadata)`; `metadata` may be null
/// (whole array or per entry) and `leader_epoch < 0` means no epoch. Returns
/// null on success, non-null error on failure.
///
/// # Safety
///
/// `handle` a valid handle; arrays `count` valid entries.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync_offsets(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_offset_map` requires every array it reads to have `count` valid
    // entries: it clamps a negative `count` to zero, dereferences `topics[i]` (as a C
    // string), `partitions[i]` and `offsets[i]` unconditionally, and tolerates NULL only
    // for the whole `leader_epochs` array, the whole `metadata` array and individual
    // `metadata[i]` entries. This function's `# Safety` promises `count` valid entries in
    // the arrays for the duration of the call, and its description documents the NULL
    // `metadata` forms and `leader_epoch < 0` as the permitted sentinels. Every entry is
    // copied into the owned map, or rejected with a validation error before any operation
    // is driven, so nothing borrows the C arrays afterwards.
    let map = match unsafe { read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    block_on_void(handle, move |h| Box::pin(async move { h.commit_sync_with_offsets(map).await }))
}

/// Commits the offsets the owning consumer has consumed asynchronously
/// (fire-and-forget; returns once the commit has been initiated). There is no
/// callback-taking variant on the handle, matching the core — register an
/// `OffsetCommitCallback` on the owning consumer with
/// [`super::consumer::kafka_consumer_Consumer_commit_async_with_callback`]
/// instead.
///
/// # Safety
///
/// `handle` must be a valid handle from [`kafka_consumer_Consumer_handle`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async(
    handle: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_common_Error_t {
    block_on_void(handle, |h| Box::pin(async move { h.commit_async().await }))
}

/// Commits specific offsets asynchronously (fire-and-forget). Same array shape
/// as [`kafka_consumer_ConsumerHandle_commit_sync_offsets`].
///
/// # Safety
///
/// `handle` a valid handle; arrays `count` valid entries.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_offsets(
    handle: *const kafka_consumer_ConsumerHandle_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    leader_epochs: *const i32,
    metadata: *const *const c_char,
    count: i32,
) -> *mut kafka_common_Error_t {
    // SAFETY: `read_offset_map` requires every array it reads to have `count` valid
    // entries: it clamps a negative `count` to zero, dereferences `topics[i]` (as a C
    // string), `partitions[i]` and `offsets[i]` unconditionally, and tolerates NULL only
    // for the whole `leader_epochs` array, the whole `metadata` array and individual
    // `metadata[i]` entries. This function's `# Safety` promises `count` valid entries in
    // the arrays for the duration of the call, and its description documents the NULL
    // `metadata` forms and `leader_epoch < 0` as the permitted sentinels. Every entry is
    // copied into the owned map, or rejected with a validation error before any operation
    // is driven, so nothing borrows the C arrays afterwards.
    let map = match unsafe { read_offset_map(topics, partitions, offsets, leader_epochs, metadata, count) } {
        Ok(m) => m,
        Err(e) => return box_error(e),
    };
    block_on_void(handle, move |h| Box::pin(async move { h.commit_async_offsets(map).await }))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Builds a [`TopicPartition`] from a C string + partition index.
///
/// # Safety
///
/// `topic` must be a valid, null-terminated C string.
unsafe fn topic_partition(topic: *const c_char, partition: i32) -> TopicPartition {
    // SAFETY: Per this helper's `# Safety`, `topic` is a valid, NUL-terminated C string for
    // the duration of the call; each caller (`kafka_consumer_ConsumerHandle_seek`,
    // `seek_with_metadata`, `position`, `position_timeout`) forwards a `topic` parameter
    // that its own `# Safety` requires to be a valid C string. `CStr::from_ptr` borrows it
    // only until `to_string_lossy().to_string()` has copied the bytes into the owned
    // `String`.
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy().to_string();
    TopicPartition::new(topic_str, partition)
}

/// Shared body of [`kafka_consumer_ConsumerHandle_position`] and
/// [`kafka_consumer_ConsumerHandle_position_timeout`]: drives the future and
/// writes the offset through `out_position` on success.
fn block_on_position<F>(
    handle: *const kafka_consumer_ConsumerHandle_t,
    out_position: *mut i64,
    op: F,
) -> *mut kafka_common_Error_t
where
    F: FnOnce(&'static ConsumerHandle) -> std::pin::Pin<Box<dyn Future<Output = Result<i64, Error>> + 'static>>,
{
    if let Err(e) = ensure_blocking_allowed() {
        return box_error(e);
    }
    // SAFETY: `wrapper_ref` requires `handle` non-null and created by
    // `kafka_consumer_Consumer_handle`. `block_on_position` performs no check itself: it is
    // private to this module and its only callers are the `extern "C"` entry points
    // `position` and `position_timeout`, each of which forwards its own `handle` parameter
    // unchanged and requires a valid handle of it in its `# Safety`. The `&'static
    // ConsumerHandleWrapper` and the `&w.inner` borrowed from it live only as long as this
    // call: `op` builds a future that `w.runtime.block_on` drives to completion on the
    // calling thread before returning, the core `async fn`s borrow `&self` only for the
    // returned future's lifetime, and nothing is stored or spawned, while the C caller
    // keeps the handle alive for the call per the module's lifetime contract.
    // `ensure_blocking_allowed` has already established that this thread is not inside a
    // tokio runtime, so `Handle::block_on` is permitted here (module docs, "Threading
    // contract").
    let w = unsafe { wrapper_ref(handle) };
    match w.runtime.block_on(op(&w.inner)) {
        Ok(pos) => {
            if !out_position.is_null() {
                // SAFETY: `out_position` is non-null (checked above) and, per the calling
                // entry point's `# Safety` (`out_position` valid), points to a writable
                // `i64` owned by the C caller for the duration of the call. Exactly one
                // `i64` is written, and only on the success path, so a failure leaves
                // `*out_position` untouched as documented.
                unsafe { *out_position = pos };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::consumer::{
        kafka_common_TopicPartitionList_count, kafka_common_TopicPartitionList_destroy,
        kafka_consumer_Consumer_destroy, kafka_consumer_Consumer_poll, kafka_consumer_ConsumerRecords_destroy,
        kafka_consumer_MockConsumer_new, kafka_consumer_StringList_count, kafka_consumer_StringList_destroy,
    };

    /// Reads an error handle's message into an owned `String`.
    ///
    /// # Safety
    ///
    /// `err` must be a valid, non-null error handle.
    unsafe fn error_message(err: *const kafka_common_Error_t) -> String {
        // SAFETY: `kafka_common_Error_message` requires `error` to be null or a valid error
        // handle; per this helper's `# Safety`, `err` is a valid, non-null error handle
        // (every caller asserts `!err.is_null()` on the handle an FFI call just returned
        // before calling this), so the returned pointer is non-null and points at the
        // message `CString` owned by that error.
        let message = unsafe { kafka_common_Error_message(err) };
        // SAFETY: `message` is the non-null pointer `kafka_common_Error_message` returned
        // above for the valid `err`; it points at the message `CString` owned by the error
        // handle and stays valid until the error is destroyed, which every caller does only
        // after this helper has returned. The bytes are copied into an owned `String` here,
        // so the borrow ends within the call.
        unsafe { CStr::from_ptr(message) }.to_string_lossy().to_string()
    }

    /// Builds a mock consumer and a handle for it.
    fn mock_with_handle() -> (*mut kafka_consumer_Consumer_t, *mut kafka_consumer_ConsumerHandle_t) {
        let strategy = CString::new("earliest").unwrap();
        // SAFETY: `kafka_consumer_MockConsumer_new` requires `auto_offset_reset` to be null
        // or a valid C string; `strategy` is an owned `CString` local that outlives the
        // call, and the function copies the string before returning. The returned non-null
        // consumer handle (asserted below) is owned by the calling test, which destroys it
        // exactly once with `kafka_consumer_Consumer_destroy` after destroying every
        // reentrancy handle derived from it.
        let consumer = unsafe { kafka_consumer_MockConsumer_new(strategy.as_ptr()) };
        assert!(!consumer.is_null());
        // SAFETY: `kafka_consumer_Consumer_handle` requires a valid handle from a consumer
        // constructor; `consumer` is the non-null handle `kafka_consumer_MockConsumer_new`
        // returned just above. The returned reentrancy handle is owned by the calling test,
        // which destroys it exactly once with `kafka_consumer_ConsumerHandle_destroy`
        // before destroying the consumer, as the lifetime contract requires.
        let handle = unsafe { kafka_consumer_Consumer_handle(consumer) };
        assert!(!handle.is_null());
        (consumer, handle)
    }

    #[test]
    fn handle_new_and_destroy() {
        let (consumer, handle) = mock_with_handle();
        // SAFETY: `handle` is the reentrancy handle `mock_with_handle` obtained from
        // `kafka_consumer_Consumer_handle`; this is its single, final use (it is not
        // touched afterwards), and it precedes `kafka_consumer_Consumer_destroy` as the
        // lifetime contract requires.
        unsafe { kafka_consumer_ConsumerHandle_destroy(handle) };
        // A second handle after the first was destroyed still works.
        // SAFETY: `consumer` is the live mock consumer from `mock_with_handle`, a valid
        // handle from a consumer constructor that is destroyed only at the end of this
        // test; destroying the first reentrancy handle did not affect it (module docs,
        // "Handles are independent").
        let handle2 = unsafe { kafka_consumer_Consumer_handle(consumer) };
        assert!(!handle2.is_null());
        // SAFETY: `handle2` is the non-null reentrancy handle
        // `kafka_consumer_Consumer_handle` returned just above; this is its single, final
        // use and it precedes `kafka_consumer_Consumer_destroy`.
        unsafe { kafka_consumer_ConsumerHandle_destroy(handle2) };
        // SAFETY: `consumer` is the handle `kafka_consumer_MockConsumer_new` returned in
        // `mock_with_handle`; both reentrancy handles derived from it have already been
        // destroyed, and this is its single, final use.
        unsafe { kafka_consumer_Consumer_destroy(consumer) };
    }

    #[test]
    fn destroy_null_is_noop() {
        // SAFETY: A NULL pointer is passed deliberately to exercise the documented no-op
        // path (`# Safety`: `handle` must be null or a valid handle); the function returns
        // before touching it.
        unsafe { kafka_consumer_ConsumerHandle_destroy(std::ptr::null_mut()) };
        // SAFETY: A NULL pointer is passed deliberately to exercise the documented no-op
        // path (`# Safety`: `handle` must be null or a valid handle); the function returns
        // before touching it.
        unsafe { kafka_consumer_ConsumerHandle_wakeup(std::ptr::null()) };
    }

    #[test]
    fn sync_getters_return_empty_lists_on_a_mock_handle() {
        let (consumer, handle) = mock_with_handle();

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle` (a valid
        // handle from `kafka_consumer_Consumer_handle`), destroyed only at the end of this
        // test; the sync getter neither blocks nor needs a runtime context.
        let assignment = unsafe { kafka_consumer_ConsumerHandle_assignment(handle) };
        assert!(!assignment.is_null());
        // SAFETY: `kafka_common_TopicPartitionList_count` requires a valid list handle;
        // `assignment` is the non-null list `kafka_consumer_ConsumerHandle_assignment`
        // returned just above (asserted), owned by this test and alive until it is
        // destroyed on the next line.
        assert_eq!(0, unsafe { kafka_common_TopicPartitionList_count(assignment) });
        // SAFETY: `assignment` is the list returned by
        // `kafka_consumer_ConsumerHandle_assignment` above and owned by this test; this is
        // its single, final use (`# Safety`: null or a valid list handle).
        unsafe { kafka_common_TopicPartitionList_destroy(assignment) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle` (a valid
        // handle from `kafka_consumer_Consumer_handle`), destroyed only at the end of this
        // test; the sync getter neither blocks nor needs a runtime context.
        let subscription = unsafe { kafka_consumer_ConsumerHandle_subscription(handle) };
        assert!(!subscription.is_null());
        // SAFETY: `kafka_consumer_StringList_count` requires a valid string-list handle;
        // `subscription` is the non-null list `kafka_consumer_ConsumerHandle_subscription`
        // returned just above (asserted), owned by this test and alive until it is
        // destroyed on the next line.
        assert_eq!(0, unsafe { kafka_consumer_StringList_count(subscription) });
        // SAFETY: `subscription` is the list returned by
        // `kafka_consumer_ConsumerHandle_subscription` above and owned by this test; this
        // is its single, final use (`# Safety`: null or a valid string-list handle).
        unsafe { kafka_consumer_StringList_destroy(subscription) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle` (a valid
        // handle from `kafka_consumer_Consumer_handle`), destroyed only at the end of this
        // test; the sync getter neither blocks nor needs a runtime context.
        let paused = unsafe { kafka_consumer_ConsumerHandle_paused(handle) };
        assert!(!paused.is_null());
        // SAFETY: `kafka_common_TopicPartitionList_count` requires a valid list handle;
        // `paused` is the non-null list `kafka_consumer_ConsumerHandle_paused` returned
        // just above (asserted), owned by this test and alive until it is destroyed on the
        // next line.
        assert_eq!(0, unsafe { kafka_common_TopicPartitionList_count(paused) });
        // SAFETY: `paused` is the list returned by `kafka_consumer_ConsumerHandle_paused`
        // above and owned by this test; this is its single, final use (`# Safety`: null or
        // a valid list handle).
        unsafe { kafka_common_TopicPartitionList_destroy(paused) };

        // SAFETY: `handle` is the reentrancy handle from `mock_with_handle`; this is its
        // single, final use and it precedes `kafka_consumer_Consumer_destroy`, as the
        // lifetime contract requires.
        unsafe { kafka_consumer_ConsumerHandle_destroy(handle) };
        // SAFETY: `consumer` is the mock consumer from `mock_with_handle`; its reentrancy
        // handle was destroyed on the previous line, and this is its single, final use.
        unsafe { kafka_consumer_Consumer_destroy(consumer) };
    }

    /// Every async op on a mock-derived handle fails with the core's
    /// `unsupported_version` error rather than hanging or panicking.
    #[test]
    fn async_ops_are_unsupported_on_a_mock_handle() {
        let (consumer, handle) = mock_with_handle();

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle` (a valid
        // handle from `kafka_consumer_Consumer_handle`), destroyed only at the end of this
        // test. The test body runs on a plain test thread with no tokio runtime entered, so
        // the blocking operation is permitted by the threading contract; on the mock it
        // fails before any future is driven and returns an owned error handle.
        let err = unsafe { kafka_consumer_ConsumerHandle_commit_sync(handle) };
        assert!(!err.is_null());
        // SAFETY: `error_message` requires a valid, non-null error handle; `err` is the
        // error handle `kafka_consumer_ConsumerHandle_commit_sync` returned above (asserted
        // non-null), owned by this test and alive until `kafka_common_Error_destroy` below.
        let message = unsafe { error_message(err) };
        assert!(
            message.contains("not supported on a MockConsumer handle"),
            "unexpected message: {message}"
        );
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_commit_sync` above and owned by this test; its
        // message has already been copied out, and this is its single, final use (`#
        // Safety`: null or a valid error handle).
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`, destroyed
        // only at the end of this test; the test thread is not inside a tokio runtime, so
        // the blocking operation is permitted by the threading contract.
        let err = unsafe { kafka_consumer_ConsumerHandle_commit_async(handle) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_commit_async` above (asserted non-null) and owned
        // by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let topic = CString::new("t").unwrap();
        let topics = [topic.as_ptr()];
        let partitions = [0i32];

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. Per
        // `kafka_consumer_ConsumerHandle_assign`'s `# Safety`, `topics` must hold `count`
        // valid C strings and `partitions` `count` `i32` values: `topics` is a one-element
        // array holding a pointer into the owned `CString` `topic`, `partitions` is a
        // one-element `[i32]`, and `count` is 1, matching both lengths; all three locals
        // outlive the call. The test thread is not inside a tokio runtime, so the blocking
        // operation is permitted.
        let err = unsafe { kafka_consumer_ConsumerHandle_assign(handle, topics.as_ptr(), partitions.as_ptr(), 1) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_assign` above (asserted non-null) and owned by
        // this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`; `topic`
        // is an owned `CString` local that outlives the call, satisfying
        // `kafka_consumer_ConsumerHandle_seek`'s requirement of a valid C string. The test
        // thread is not inside a tokio runtime, so the blocking operation is permitted.
        let err = unsafe { kafka_consumer_ConsumerHandle_seek(handle, topic.as_ptr(), 0, 5) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_seek` above (asserted non-null) and owned by this
        // test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`; `topic`
        // is an owned `CString` local that outlives the call. `metadata` is deliberately
        // NULL and `leader_epoch` is -1 to exercise the documented "no metadata" and "no
        // leader epoch" forms (`# Safety`: `metadata` null or a valid C string). The test
        // thread is not inside a tokio runtime, so the blocking operation is permitted.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_seek_with_metadata(handle, topic.as_ptr(), 0, 5, -1, std::ptr::null())
        };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_seek_with_metadata` above (asserted non-null) and
        // owned by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let err =
            // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`.
            // `topics` is a one-element array holding a pointer into the owned `CString`
            // `topic`, `partitions` is a one-element `[i32]`, and `count` is 1, matching
            // both lengths, as `kafka_consumer_ConsumerHandle_seek_to_beginning`'s `#
            // Safety` requires; all locals outlive the call. The test thread is not inside
            // a tokio runtime, so the blocking operation is permitted.
            unsafe { kafka_consumer_ConsumerHandle_seek_to_beginning(handle, topics.as_ptr(), partitions.as_ptr(), 1) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_seek_to_beginning` above (asserted non-null) and
        // owned by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // is a one-element array holding a pointer into the owned `CString` `topic`,
        // `partitions` is a one-element `[i32]`, and `count` is 1, matching both lengths,
        // as `kafka_consumer_ConsumerHandle_seek_to_end`'s `# Safety` requires; all locals
        // outlive the call. The test thread is not inside a tokio runtime, so the blocking
        // operation is permitted.
        let err = unsafe { kafka_consumer_ConsumerHandle_seek_to_end(handle, topics.as_ptr(), partitions.as_ptr(), 1) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_seek_to_end` above (asserted non-null) and owned
        // by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // is a one-element array holding a pointer into the owned `CString` `topic`,
        // `partitions` is a one-element `[i32]`, and `count` is 1, matching both lengths,
        // as `kafka_consumer_ConsumerHandle_pause`'s `# Safety` requires; all locals
        // outlive the call. The test thread is not inside a tokio runtime, so the blocking
        // operation is permitted.
        let err = unsafe { kafka_consumer_ConsumerHandle_pause(handle, topics.as_ptr(), partitions.as_ptr(), 1) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_pause` above (asserted non-null) and owned by this
        // test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // is a one-element array holding a pointer into the owned `CString` `topic`,
        // `partitions` is a one-element `[i32]`, and `count` is 1, matching both lengths,
        // as `kafka_consumer_ConsumerHandle_resume`'s `# Safety` requires; all locals
        // outlive the call. The test thread is not inside a tokio runtime, so the blocking
        // operation is permitted.
        let err = unsafe { kafka_consumer_ConsumerHandle_resume(handle, topics.as_ptr(), partitions.as_ptr(), 1) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_resume` above (asserted non-null) and owned by
        // this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let mut position = -7i64;
        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`; `topic`
        // is an owned `CString` local, and `out_position` is `&mut position`, a writable
        // stack `i64` that outlives the call, as `kafka_consumer_ConsumerHandle_position`'s
        // `# Safety` requires (`topic` a valid C string, `out_position` valid). The test
        // thread is not inside a tokio runtime, so the blocking operation is permitted; the
        // following assertion checks that the slot is untouched on failure.
        let err = unsafe { kafka_consumer_ConsumerHandle_position(handle, topic.as_ptr(), 0, &mut position) };
        assert!(!err.is_null());
        assert_eq!(-7, position, "out_position must be untouched on failure");
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_position` above (asserted non-null) and owned by
        // this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let err =
            // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`;
            // `topic` is an owned `CString` local, and `out_position` is `&mut position`, a
            // writable stack `i64` that outlives the call, as
            // `kafka_consumer_ConsumerHandle_position_timeout`'s `# Safety` requires. The
            // test thread is not inside a tokio runtime, so the blocking operation is
            // permitted.
            unsafe { kafka_consumer_ConsumerHandle_position_timeout(handle, topic.as_ptr(), 0, 100, &mut position) };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_position_timeout` above (asserted non-null) and
        // owned by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let mut offset_map: *mut kafka_consumer_OffsetMap_t = std::ptr::null_mut();
        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // is a one-element array holding a pointer into the owned `CString` `topic`,
        // `partitions` is a one-element `[i32]`, `count` is 1, and `out_map` is `&mut
        // offset_map`, a writable local `*mut kafka_consumer_OffsetMap_t` slot, as
        // `kafka_consumer_ConsumerHandle_committed`'s `# Safety` requires; all locals
        // outlive the call. The test thread is not inside a tokio runtime, so the blocking
        // operation is permitted; the following assertion checks that the slot is untouched
        // on failure.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_committed(handle, topics.as_ptr(), partitions.as_ptr(), 1, &mut offset_map)
        };
        assert!(!err.is_null());
        assert!(offset_map.is_null(), "out_map must be untouched on failure");
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_committed` above (asserted non-null) and owned by
        // this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let mut long_map: *mut kafka_consumer_LongOffsetMap_t = std::ptr::null_mut();
        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // is a one-element array holding a pointer into the owned `CString` `topic`,
        // `partitions` is a one-element `[i32]`, `count` is 1, and `out_map` is `&mut
        // long_map`, a writable local `*mut kafka_consumer_LongOffsetMap_t` slot, as
        // `kafka_consumer_ConsumerHandle_beginning_offsets`'s `# Safety` requires; all
        // locals outlive the call. The test thread is not inside a tokio runtime, so the
        // blocking operation is permitted.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_beginning_offsets(
                handle,
                topics.as_ptr(),
                partitions.as_ptr(),
                1,
                &mut long_map,
            )
        };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_beginning_offsets` above (asserted non-null) and
        // owned by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // is a one-element array holding a pointer into the owned `CString` `topic`,
        // `partitions` is a one-element `[i32]`, `count` is 1, and `out_map` is `&mut
        // long_map`, a writable local `*mut kafka_consumer_LongOffsetMap_t` slot, as
        // `kafka_consumer_ConsumerHandle_end_offsets`'s `# Safety` requires; all locals
        // outlive the call. The test thread is not inside a tokio runtime, so the blocking
        // operation is permitted.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_end_offsets(handle, topics.as_ptr(), partitions.as_ptr(), 1, &mut long_map)
        };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_end_offsets` above (asserted non-null) and owned
        // by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let timestamps = [0i64];
        let mut ts_map: *mut kafka_consumer_OffsetAndTimestampMap_t = std::ptr::null_mut();
        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // (one pointer into the owned `CString` `topic`), `partitions` (one `i32`) and
        // `timestamps` (one `i64`) each have the `count` of 1 entries that
        // `kafka_consumer_ConsumerHandle_offsets_for_times`'s `# Safety` requires, and
        // `out_map` is `&mut ts_map`, a writable local `*mut
        // kafka_consumer_OffsetAndTimestampMap_t` slot; all locals outlive the call. The
        // test thread is not inside a tokio runtime, so the blocking operation is
        // permitted.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_offsets_for_times(
                handle,
                topics.as_ptr(),
                partitions.as_ptr(),
                timestamps.as_ptr(),
                1,
                &mut ts_map,
            )
        };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_offsets_for_times` above (asserted non-null) and
        // owned by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        let commit_offsets = [1i64];
        let epochs = [-1i32];
        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // (one pointer into the owned `CString` `topic`), `partitions`, `commit_offsets`
        // and `epochs` are one-element locals matching the `count` of 1 that
        // `kafka_consumer_ConsumerHandle_commit_sync_offsets`'s `# Safety` requires;
        // `metadata` is deliberately NULL to exercise the documented "whole array null"
        // form, which `read_offset_map` checks for before reading, and the epoch of -1 is
        // the documented "no epoch" sentinel. All locals outlive the call, and the test
        // thread is not inside a tokio runtime, so the blocking operation is permitted.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_commit_sync_offsets(
                handle,
                topics.as_ptr(),
                partitions.as_ptr(),
                commit_offsets.as_ptr(),
                epochs.as_ptr(),
                std::ptr::null(),
                1,
            )
        };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_commit_sync_offsets` above (asserted non-null) and
        // owned by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // (one pointer into the owned `CString` `topic`), `partitions`, `commit_offsets`
        // and `epochs` are one-element locals matching the `count` of 1 that
        // `kafka_consumer_ConsumerHandle_commit_async_offsets`'s `# Safety` requires;
        // `metadata` is deliberately NULL to exercise the documented "whole array null"
        // form, which `read_offset_map` checks for before reading, and the epoch of -1 is
        // the documented "no epoch" sentinel. All locals outlive the call, and the test
        // thread is not inside a tokio runtime, so the blocking operation is permitted.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_commit_async_offsets(
                handle,
                topics.as_ptr(),
                partitions.as_ptr(),
                commit_offsets.as_ptr(),
                epochs.as_ptr(),
                std::ptr::null(),
                1,
            )
        };
        assert!(!err.is_null());
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_commit_async_offsets` above (asserted non-null)
        // and owned by this test; this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the reentrancy handle from `mock_with_handle`; this is its
        // single, final use and it precedes `kafka_consumer_Consumer_destroy`, as the
        // lifetime contract requires.
        unsafe { kafka_consumer_ConsumerHandle_destroy(handle) };
        // SAFETY: `consumer` is the mock consumer from `mock_with_handle`; its reentrancy
        // handle was destroyed on the previous line, and this is its single, final use.
        unsafe { kafka_consumer_Consumer_destroy(consumer) };
    }

    /// Marshaling failures are reported before the op is driven, so an invalid
    /// offset yields the core's validation error (not `unsupported_version`).
    #[test]
    fn commit_offsets_marshaling_error_is_returned() {
        let (consumer, handle) = mock_with_handle();
        let topic = CString::new("t").unwrap();
        let topics = [topic.as_ptr()];
        let partitions = [0i32];
        let offsets = [-1i64]; // invalid: negative offset
        let epochs = [-1i32];

        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`. `topics`
        // (one pointer into the owned `CString` `topic`), `partitions`, `offsets` and
        // `epochs` are one-element locals matching the `count` of 1 that
        // `kafka_consumer_ConsumerHandle_commit_sync_offsets`'s `# Safety` requires, and
        // `metadata` is deliberately NULL (a documented form). The negative offset is a
        // deliberately invalid value, so `read_offset_map` returns the core validation
        // error before any operation is driven; all locals outlive the call, and the test
        // thread is not inside a tokio runtime.
        let err = unsafe {
            kafka_consumer_ConsumerHandle_commit_sync_offsets(
                handle,
                topics.as_ptr(),
                partitions.as_ptr(),
                offsets.as_ptr(),
                epochs.as_ptr(),
                std::ptr::null(),
                1,
            )
        };
        assert!(!err.is_null());
        // SAFETY: `error_message` requires a valid, non-null error handle; `err` is the
        // error handle `kafka_consumer_ConsumerHandle_commit_sync_offsets` returned above
        // (asserted non-null), owned by this test and alive until
        // `kafka_common_Error_destroy` below.
        let message = unsafe { error_message(err) };
        assert!(message.contains("negative offset"), "unexpected message: {message}");
        // SAFETY: `err` is the error handle returned by
        // `kafka_consumer_ConsumerHandle_commit_sync_offsets` above and owned by this test;
        // its message has already been copied out, and this is its single, final use.
        unsafe { kafka_common_Error_destroy(err) };

        // SAFETY: `handle` is the reentrancy handle from `mock_with_handle`; this is its
        // single, final use and it precedes `kafka_consumer_Consumer_destroy`, as the
        // lifetime contract requires.
        unsafe { kafka_consumer_ConsumerHandle_destroy(handle) };
        // SAFETY: `consumer` is the mock consumer from `mock_with_handle`; its reentrancy
        // handle was destroyed on the previous line, and this is its single, final use.
        unsafe { kafka_consumer_Consumer_destroy(consumer) };
    }

    /// A handle op from inside a tokio runtime fails with a clear error rather
    /// than panicking inside `Handle::block_on` (module docs, "Threading
    /// contract").
    #[test]
    fn blocking_op_from_within_a_runtime_is_rejected() {
        let (consumer, handle) = mock_with_handle();
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle`,
            // destroyed only after `rt.block_on` returns. The call is made deliberately
            // from inside a tokio runtime context so that `ensure_blocking_allowed` rejects
            // it with the `IllegalStateError` the threading contract documents; on that
            // path `block_on_void` returns before `wrapper_ref` dereferences `handle`, and
            // the returned error handle is owned by this test.
            let err = unsafe { kafka_consumer_ConsumerHandle_commit_sync(handle) };
            assert!(!err.is_null());
            // SAFETY: `error_message` requires a valid, non-null error handle; `err` is the
            // error handle `kafka_consumer_ConsumerHandle_commit_sync` returned above
            // (asserted non-null), owned by this test and alive until
            // `kafka_common_Error_destroy` below.
            let message = unsafe { error_message(err) };
            assert!(
                message.contains("cannot be called from within an async runtime"),
                "unexpected message: {message}"
            );
            // SAFETY: `err` is the error handle returned by
            // `kafka_consumer_ConsumerHandle_commit_sync` above and owned by this test; its
            // message has already been copied out, and this is its single, final use.
            unsafe { kafka_common_Error_destroy(err) };

            // The sync getters do not block, so they still work in that context.
            // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle` (a
            // valid handle from `kafka_consumer_Consumer_handle`); the sync getter never
            // blocks, so it is permitted inside the runtime context (the threading contract
            // restricts only the blocking operations).
            let assignment = unsafe { kafka_consumer_ConsumerHandle_assignment(handle) };
            assert!(!assignment.is_null());
            // SAFETY: `assignment` is the non-null list returned by
            // `kafka_consumer_ConsumerHandle_assignment` above (asserted) and owned by this
            // test; this is its single, final use (`# Safety`: null or a valid list
            // handle).
            unsafe { kafka_common_TopicPartitionList_destroy(assignment) };
        });

        // SAFETY: `handle` is the reentrancy handle from `mock_with_handle`; `rt.block_on`
        // has returned, this is the handle's single, final use, and it precedes
        // `kafka_consumer_Consumer_destroy`, as the lifetime contract requires.
        unsafe { kafka_consumer_ConsumerHandle_destroy(handle) };
        // SAFETY: `consumer` is the mock consumer from `mock_with_handle`; its reentrancy
        // handle was destroyed on the previous line, and this is its single, final use.
        unsafe { kafka_consumer_Consumer_destroy(consumer) };
    }

    /// `wakeup()` through the handle sets the same flag the owning
    /// `MockConsumer`'s next `poll` observes.
    #[test]
    fn wakeup_wakes_the_next_poll() {
        let (consumer, handle) = mock_with_handle();
        // SAFETY: `handle` is the live reentrancy handle from `mock_with_handle` (`#
        // Safety`: null or a valid handle from `kafka_consumer_Consumer_handle`); `wakeup`
        // is synchronous and only sets the mock's shared wakeup flag.
        unsafe { kafka_consumer_ConsumerHandle_wakeup(handle) };

        let mut poll_err: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: `kafka_consumer_Consumer_poll` requires `consumer` to be a valid handle
        // from a consumer constructor; `consumer` is the live mock consumer from
        // `mock_with_handle`, destroyed only at the end of this test. `out_error` is `&mut
        // poll_err`, a writable local slot that outlives the call (the function null-checks
        // it and writes exactly one pointer). The test thread is not inside a tokio runtime
        // and nothing else holds the consumer's access guard; the error handle written for
        // the wakeup failure is owned by this test.
        let records = unsafe { kafka_consumer_Consumer_poll(consumer, 10, &mut poll_err) };
        assert!(records.is_null());
        assert!(!poll_err.is_null());
        // SAFETY: `poll_err` is the error handle `kafka_consumer_Consumer_poll` wrote above
        // (asserted non-null) and owned by this test; this is its single, final use (`#
        // Safety`: null or a valid error handle).
        unsafe { kafka_common_Error_destroy(poll_err) };

        // The flag was cleared, so the next poll succeeds.
        let mut poll_err2: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: `kafka_consumer_Consumer_poll` requires `consumer` to be a valid handle
        // from a consumer constructor; `consumer` is the live mock consumer from
        // `mock_with_handle`. `out_error` is `&mut poll_err2`, a writable local slot that
        // outlives the call. The previous poll cleared the wakeup flag and released the
        // access guard, so this poll returns a records handle owned by this test.
        let records = unsafe { kafka_consumer_Consumer_poll(consumer, 10, &mut poll_err2) };
        assert!(poll_err2.is_null());
        assert!(!records.is_null());
        // SAFETY: `records` is the non-null records handle `kafka_consumer_Consumer_poll`
        // returned above (asserted) and owned by this test; this is its single, final use
        // (`# Safety`: null or a valid records handle).
        unsafe { kafka_consumer_ConsumerRecords_destroy(records) };

        // SAFETY: `handle` is the reentrancy handle from `mock_with_handle`; this is its
        // single, final use and it precedes `kafka_consumer_Consumer_destroy`, as the
        // lifetime contract requires.
        unsafe { kafka_consumer_ConsumerHandle_destroy(handle) };
        // SAFETY: `consumer` is the mock consumer from `mock_with_handle`; its reentrancy
        // handle was destroyed on the previous line, and this is its single, final use.
        unsafe { kafka_consumer_Consumer_destroy(consumer) };
    }
}
