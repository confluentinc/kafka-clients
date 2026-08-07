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

//! C FFI layer for the Kafka admin API.
//!
//! This module exposes the admin client (the [`Admin`] trait, `KafkaAdminClient`
//! and [`MockAdminClient`]) via C-callable `extern "C"` functions. It mirrors the
//! consumer FFI (`src/ffi/consumer.rs`) and reuses the shared dispatcher-thread
//! machinery in `src/ffi/common.rs` — see `PLAN-bindings.md` §1/§7 (decision D4).
//!
//! # Concurrency model — no access guard
//!
//! Unlike the consumer (whose `Consumer` trait is `!Sync` with `&mut self`
//! methods, forcing a single-owner guard that mirrors Java's
//! `KafkaConsumer.acquire()`), [`Admin`] is `Send + Sync` and every method takes
//! `&self`. Java's `KafkaAdminClient` is likewise thread-safe. So this module
//! has **no** `UnsafeCell`, no `owner: AtomicU64` guard, no `acquire`/`release`,
//! and no `wakeup()` abort path: concurrent calls from many C threads are
//! allowed, exactly as in Java.
//!
//! # API shape
//!
//! Per `admin-client.md` §1, Java's per-RPC `Admin` methods do **not** block —
//! they enqueue work on the background task and return a `*Result` holding one
//! `KafkaFuture<T>` per key. C has no `KafkaFuture`, so (decisions D1/D2 in
//! `PLAN-bindings.md` §7) every RPC gets:
//!
//! - a **bare (synchronous)** entry point that submits the RPC and blocks until
//!   every per-key future has resolved, writing one flattened result handle; and
//! - an **`_async`** entry point that submits the RPC, returns immediately, and
//!   delivers the same flattened result handle through a C callback (see
//!   *Callback thread* below).
//!
//! In both cases the RPC method itself is invoked on the **calling** thread, so
//! the request is enqueued as promptly as in Java; only the awaiting of the
//! per-key futures moves to the tokio runtime.
//!
//! # Callback thread
//!
//! Every `_async` entry point fires its callback **exactly once**, but not
//! always on the same thread:
//!
//! - Normally, on the handle's **dispatcher thread**, after the RPC's futures
//!   have resolved.
//! - **Synchronously, on the calling thread, before the entry point returns**,
//!   when the operation fails before it can be submitted: `admin` is NULL, or
//!   argument marshaling fails (an unparseable base64 topic id passed to
//!   `kafka_admin_AdminClient_delete_topics_by_ids_async` /
//!   `_describe_topics_by_ids_async`, or an unknown `AlterConfigOp.OpType` code
//!   passed to `kafka_admin_AdminClient_incremental_alter_configs_async`). This
//!   is plain bad input, not only a programming error, so a caller must not
//!   assume the entry point has returned by the time the callback runs.
//! - On a **tokio worker thread**, if the dispatcher's completion queue can no
//!   longer be reached when the result arrives. Handle destruction does not
//!   cause this: each async operation clones the sender before spawning and
//!   holds it for the whole life of its task, so the dispatcher cannot exit
//!   while an operation is outstanding. What remains is a dispatcher thread that
//!   terminated abnormally, i.e. a panic inside an earlier callback.
//!
//! Firing inline keeps the callback obligation total — no path drops it — but it
//! means a caller must not hold a lock across `..._async(...)` and re-acquire it
//! in the callback, and must publish anything the callback needs (including
//! `user_data`) *before* the submit rather than after it.
//!
//! cbindgen does **not** copy this module documentation into
//! `target/include/confluent_kafka.h` — only per-item rustdoc. So every `_async`
//! entry point below restates the rule in full rather than pointing here; a
//! cross-reference to this section would dangle for a C reader.
//!
//! A flattened `kafka_admin_*Result_t` exposes `_count` / `_get_key(i)` /
//! `_get_value(i)` / `_get_error(i)` / `_destroy`, so per-key data *and* per-key
//! errors survive the boundary; only independent per-key *timing* is lost (which
//! C cannot express without a `KafkaFuture` type).
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with
// PascalCase type names, which intentionally differs from Rust's snake_case
// convention.
#![allow(non_snake_case, non_camel_case_types)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::Mutex;
use std::time::Duration;

use crate::admin::{
    Admin, AdminClientConfig, AlterClientQuotasOptions, AlterConfigOp, AlterConfigsOptions,
    AlterConsumerGroupOffsetsOptions, AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions,
    ClassicGroupDescription, Config, ConfigEntry, ConfigSource, ConfigType, ConsumerGroupDescription,
    CreateAclsOptions, CreatePartitionsOptions, CreateTopicsOptions, DeleteAclsOptions,
    DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions, DeleteRecordsOptions, DeleteTopicsOptions,
    DeletedRecords, DescribeAclsOptions, DescribeClassicGroupsOptions, DescribeClientQuotasOptions,
    DescribeClusterOptions, DescribeConfigsOptions, DescribeConsumerGroupsOptions, DescribeLogDirsOptions,
    DescribeReplicaLogDirsOptions, DescribeTopicsOptions, ElectLeadersOptions, FilterResults, GroupListing,
    GroupOffsets, ListConfigResourcesOptions, ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec,
    ListGroupsOptions, ListOffsetsOptions, ListOffsetsResultInfo, ListPartitionReassignmentsOptions, ListTopicsOptions,
    LogDirDescription, MemberAssignment, MemberDescription, MemberToRemove, MockAdminClient, NewPartitionReassignment,
    NewPartitions, NewTopic, OffsetSpec, OpType, PartitionReassignment, RecordsToDelete,
    RemoveMembersFromConsumerGroupOptions, ReplicaLogDirInfo, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
// `listClientMetricsResources` (superseded by `listConfigResources` filtered to
// CLIENT_METRICS) and `listConsumerGroups` (superseded by `listGroups`) are both
// deprecated in
// Java 4.1 but still part of the `Admin` surface, so the FFI exposes them for
// parity. A `#![deny(warnings)]` crate needs the `use` item itself allowed, not
// only the functions.
#[allow(deprecated)]
use crate::admin::{
    ClientMetricsResourceListing, ConsumerGroupListing, ListClientMetricsResourcesOptions, ListConsumerGroupsOptions,
};
use crate::common::acl::{
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
};
use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::quota::{
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent, Op as ClientQuotaOp,
};
use crate::common::requests::describe_client_quotas_request::{
    MATCH_TYPE_DEFAULT, MATCH_TYPE_EXACT, MATCH_TYPE_SPECIFIED,
};
use crate::common::requests::list_offsets_request::{
    EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP, EARLIEST_TIMESTAMP, LATEST_TIERED_TIMESTAMP,
    LATEST_TIMESTAMP, MAX_TIMESTAMP,
};
use crate::common::resource::{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType};
use crate::common::{
    ElectionType, GroupState, GroupType, IsolationLevel, KafkaError, KafkaFuture, Node, TopicCollection,
    TopicPartition, TopicPartitionInfo, TopicPartitionReplica, Uuid,
};
use crate::consumer::OffsetAndMetadata;

use super::common::{
    self, CompletionJob, KafkaErrorInner, OperationCallbackFn, OperationCallbackTarget, OperationCompletion, box_error,
    enqueue_or_run_inline, init_default_logger, kafka_common_KafkaError_t,
};
use super::consumer::kafka_common_Node_t;

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// The two admin implementations exposed through the FFI.
enum AdminKind {
    /// Production, network-backed admin client (`KafkaAdminClient` behind the
    /// `Admin` trait object returned by `new_admin_client`).
    Kafka(Box<dyn Admin>),
    /// In-memory admin client with immediately-resolved futures, for tests.
    Mock(Box<MockAdminClient>),
}

/// Per-admin-client handle state.
///
/// Owns the admin client directly (no `UnsafeCell`: [`Admin`] is `Send + Sync`
/// and its methods take `&self` — see the module documentation), the tokio
/// runtime driving the background I/O task, and the callback dispatcher thread.
struct AdminHandle {
    kind: AdminKind,
    /// Drives the sync entry points via `block_on` and hosts the admin client's
    /// own background task. Multi-thread for the same reason the producer and
    /// consumer handles are: a current-thread runtime only makes progress inside
    /// `block_on`, so the admin background task would stall between calls (and
    /// the `_async` awaiter tasks would never run at all).
    runtime: tokio::runtime::Runtime,
    /// Handle for spawning the `_async` awaiter tasks.
    runtime_handle: tokio::runtime::Handle,
    /// Sender for the completion-dispatch queue (reused from `common.rs`).
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,
    /// Dispatcher thread join handle; detached on destroy.
    dispatcher: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Whether this handle wraps a [`MockAdminClient`]. Read by the
    /// `kafka_admin_MockAdminClient_*` driver functions.
    is_mock: bool,
}

impl AdminHandle {
    /// The admin client behind either kind, as a trait object.
    fn admin(&self) -> &dyn Admin {
        match &self.kind {
            AdminKind::Kafka(admin) => admin.as_ref(),
            AdminKind::Mock(mock) => mock.as_ref(),
        }
    }
}

/// Builds an [`AdminHandle`] around an [`AdminKind`], spawning the callback
/// dispatcher thread, and returns the leaked C handle.
///
/// `runtime` is passed in (rather than built here) because the production
/// constructor must build it *first*: `KafkaAdminClient::from_config` calls
/// `tokio::spawn` for its background task, so it has to run inside the runtime
/// context.
fn build_admin_handle(
    kind: AdminKind,
    runtime: tokio::runtime::Runtime,
    is_mock: bool,
) -> *mut kafka_admin_AdminClient_t {
    let runtime_handle = runtime.handle().clone();
    let (completion_tx, dispatcher) = common::spawn_dispatcher("kafka-admin-callback-dispatcher");

    let handle = Box::new(AdminHandle {
        kind,
        runtime,
        runtime_handle,
        completion_tx,
        dispatcher: Mutex::new(Some(dispatcher)),
        is_mock,
    });
    Box::into_raw(handle) as *mut kafka_admin_AdminClient_t
}

/// Casts a `*const kafka_admin_AdminClient_t` to a `&'static AdminHandle`.
///
/// # Safety
///
/// `admin` must be non-null and created by an admin-client constructor.
unsafe fn handle_ref(admin: *const kafka_admin_AdminClient_t) -> &'static AdminHandle {
    unsafe { &*(admin as *const AdminHandle) }
}

// ---------------------------------------------------------------------------
// Opaque types
// ---------------------------------------------------------------------------

/// Opaque admin-client handle.
///
/// One type covers both the production client and [`MockAdminClient`], mirroring
/// the consumer FFI's single `kafka_consumer_Consumer_t` (Java's `Admin`
/// interface is the common supertype of `KafkaAdminClient` and
/// `MockAdminClient`). Instance methods are named
/// `kafka_admin_AdminClient_<method>`; the mock's own driver methods are named
/// `kafka_admin_MockAdminClient_<method>` and take the same handle.
#[repr(C)]
pub struct kafka_admin_AdminClient_t {
    _private: [u8; 0],
}

/// Opaque admin-configuration properties handle (a `HashMap<String, String>`).
#[repr(C)]
pub struct kafka_admin_AdminClientProperties_t {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------------
// AdminClientProperties
// ---------------------------------------------------------------------------

/// Casts a `*const kafka_admin_AdminClientProperties_t` to a reference.
///
/// # Safety
///
/// `props` must be a valid handle from an `AdminClientProperties` constructor.
unsafe fn properties_ref(props: *const kafka_admin_AdminClientProperties_t) -> &'static HashMap<String, String> {
    unsafe { &*(props as *const HashMap<String, String>) }
}

/// Casts a `*mut kafka_admin_AdminClientProperties_t` to a mutable reference.
///
/// # Safety
///
/// `props` must be a valid handle from an `AdminClientProperties` constructor.
unsafe fn properties_mut(props: *mut kafka_admin_AdminClientProperties_t) -> &'static mut HashMap<String, String> {
    unsafe { &mut *(props as *mut HashMap<String, String>) }
}

/// Creates an empty admin-client properties handle.
///
/// # Returns
///
/// A non-null opaque properties handle. The caller must free it with
/// [`kafka_admin_AdminClientProperties_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_AdminClientProperties_new() -> *mut kafka_admin_AdminClientProperties_t {
    let map: HashMap<String, String> = HashMap::new();
    Box::into_raw(Box::new(map)) as *mut kafka_admin_AdminClientProperties_t
}

/// Creates admin-client properties from a NULL-terminated flat array of C
/// strings.
///
/// The array contains alternating key-value pairs terminated by a NULL pointer:
/// `["key1", "val1", "key2", "val2", ..., NULL]`.
///
/// # Returns
///
/// A non-null handle on success, or NULL if `configs` is NULL or an odd number
/// of non-NULL entries is found. The caller must free a non-null handle with
/// [`kafka_admin_AdminClientProperties_destroy`].
///
/// # Safety
///
/// `configs` must be NULL or point to a NULL-terminated array of valid,
/// null-terminated C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientProperties_from_configs(
    configs: *const *const c_char,
) -> *mut kafka_admin_AdminClientProperties_t {
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
    Box::into_raw(Box::new(map)) as *mut kafka_admin_AdminClientProperties_t
}

/// Adds or overwrites a configuration key-value pair. No-op if any parameter is
/// null.
///
/// # Safety
///
/// `props` must be a valid handle; `key` and `value` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientProperties_put(
    props: *mut kafka_admin_AdminClientProperties_t,
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
/// `props` must be null or a valid handle from an `AdminClientProperties`
/// constructor. After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientProperties_destroy(props: *mut kafka_admin_AdminClientProperties_t) {
    if !props.is_null() {
        unsafe {
            drop(Box::from_raw(props as *mut HashMap<String, String>));
        }
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

/// Creates a new admin client connected to a real cluster.
///
/// Mirrors Java's `Admin.create(Properties)` / `AdminClient.create(Properties)`.
///
/// # Parameters
///
/// - `props`: Non-null properties handle. The caller retains ownership.
/// - `out_error`: Pointer where an error handle will be written on failure, or
///   null if the caller does not need error details.
///
/// # Returns
///
/// A non-null admin-client handle on success, or null on failure. If `out_error`
/// is non-null, `*out_error` is set to null on success or to a valid error
/// handle on failure (free it with `kafka_common_KafkaError_destroy`).
///
/// # Safety
///
/// `props` must be a valid, non-null properties handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_new(
    props: *const kafka_admin_AdminClientProperties_t,
    out_error: *mut *mut kafka_common_KafkaError_t,
) -> *mut kafka_admin_AdminClient_t {
    init_default_logger();
    if props.is_null() {
        if !out_error.is_null() {
            unsafe { *out_error = box_error(KafkaError::illegal_argument("properties handle must not be null")) };
        }
        return std::ptr::null_mut();
    }
    let map = unsafe { properties_ref(props) };
    let config = match AdminClientConfig::from_properties(map) {
        Ok(c) => c,
        Err(e) => {
            if !out_error.is_null() {
                unsafe { *out_error = box_error(e) };
            }
            return std::ptr::null_mut();
        },
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            if !out_error.is_null() {
                unsafe {
                    *out_error = box_error(KafkaError::illegal_state(format!(
                        "failed to create tokio runtime for AdminClient: {e}"
                    )))
                };
            }
            return std::ptr::null_mut();
        },
    };

    // Enter the runtime so `new_admin_client` can `tokio::spawn` the admin
    // background task. The guard is dropped before the handle is built.
    let admin = {
        let _guard = runtime.enter();
        match crate::admin::new_admin_client(config) {
            Ok(a) => a,
            Err(e) => {
                if !out_error.is_null() {
                    unsafe { *out_error = box_error(e) };
                }
                return std::ptr::null_mut();
            },
        }
    };

    if !out_error.is_null() {
        unsafe { *out_error = std::ptr::null_mut() };
    }
    build_admin_handle(AdminKind::Kafka(admin), runtime, false)
}

/// Creates a new mock admin client (broker-less, for tests).
///
/// Mirrors Java's `MockAdminClient.create().numBrokers(n).build()`: brokers are
/// `localhost:1000+id`, the controller is broker 0, the default partition count
/// is 1 and the default replication factor is `min(num_brokers, 3)`.
///
/// # Parameters
///
/// - `num_brokers`: Number of brokers to simulate; must be at least 1.
///
/// # Returns
///
/// A non-null admin-client handle, or null if the tokio runtime cannot be
/// created or `num_brokers < 1`. The caller must free a non-null handle with
/// [`kafka_admin_AdminClient_destroy`].
///
/// At least one broker is required because the mock places every partition
/// leader and the controller on broker 0. Java rejects it the same way, by
/// throwing: `MockAdminClient.Builder.build()` reads `brokers.get(0)` for the
/// controller and `createTopics` does likewise for each partition leader
/// (`MockAdminClient.java:210` / `:412` at kafka `a18251bae0b8`). Returning null
/// here is that throw expressed in the FFI's idiom — a Rust panic must not
/// unwind across the C boundary (CLAUDE.md §10.1).
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_MockAdminClient_new(num_brokers: i32) -> *mut kafka_admin_AdminClient_t {
    init_default_logger();
    if num_brokers < 1 {
        return std::ptr::null_mut();
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => return std::ptr::null_mut(),
    };
    let mock = MockAdminClient::create(num_brokers);
    build_admin_handle(AdminKind::Mock(Box::new(mock)), runtime, true)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Destroys an admin-client handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op). Destroying concurrently with an
/// in-flight `_async` operation is a C lifetime precondition the caller must
/// uphold (CLAUDE.md FFI §3).
///
/// # Safety
///
/// `admin` must be null or a valid handle from an admin-client constructor.
/// After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_destroy(admin: *mut kafka_admin_AdminClient_t) {
    if admin.is_null() {
        return;
    }
    let handle = unsafe { Box::from_raw(admin as *mut AdminHandle) };
    let AdminHandle { kind, runtime, completion_tx, dispatcher, .. } = *handle;

    // 1. Shut down the runtime first. This cancels any in-flight `_async`
    //    awaiter task that borrows the admin client, so it is no longer
    //    referenced when we drop it next.
    runtime.shutdown_background();
    // 2. Drop the admin client.
    drop(kind);
    // 3. Close the completion channel and detach the dispatcher (do NOT join —
    //    outstanding completion jobs may still hold a cloned `completion_tx`,
    //    and the dispatcher exits once all clones are released).
    drop(completion_tx);
    drop(dispatcher.into_inner().unwrap_or(None));
}

/// Converts a C millisecond timeout into a [`Duration`], treating a negative
/// value as "no timeout" — Java's no-argument `Admin.close()`, which delegates
/// to `close(Duration.ofMillis(Long.MAX_VALUE))`.
fn close_timeout(timeout_ms: i64) -> Duration {
    if timeout_ms < 0 {
        Duration::from_millis(i64::MAX as u64)
    } else {
        Duration::from_millis(timeout_ms as u64)
    }
}

/// Closes the admin client, awaiting the background task up to `timeout_ms`
/// (synchronous). The wait is bounded: this returns after `timeout_ms` whatever
/// the background task is doing, as Java's `thread.join(waitTimeMs)` does. Pass
/// a negative `timeout_ms` for Java's no-argument `close()` semantics — wait
/// indefinitely, which like Java means "capped at a year".
///
/// Returns nothing: Java's `Admin.close(Duration)` is `void`, and the Rust
/// `Admin::close` likewise returns `()`.
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_close(admin: *const kafka_admin_AdminClient_t, timeout_ms: i64) {
    if admin.is_null() {
        return;
    }
    let h = unsafe { handle_ref(admin) };
    let timeout = close_timeout(timeout_ms);
    h.runtime.block_on(h.admin().close(timeout));
}

/// Completion callback for [`kafka_admin_AdminClient_close_async`].
///
/// `error` is always null — Java's `Admin.close(Duration)` returns `void`. The
/// parameter is kept for signature uniformity with the other admin callbacks
/// (and so the Python layer can reuse one resolve/free pair); if it is ever
/// non-null the callback owns it.
pub type kafka_admin_AdminClient_close_callback_t =
    unsafe extern "C" fn(*mut kafka_common_KafkaError_t, *mut std::ffi::c_void);

/// Closes the admin client asynchronously. See
/// [`kafka_admin_AdminClient_close`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_close_async(
    admin: *const kafka_admin_AdminClient_t,
    timeout_ms: i64,
    callback: kafka_admin_AdminClient_close_callback_t,
    user_data: *mut c_void,
) {
    let timeout = close_timeout(timeout_ms);
    unsafe {
        admin_async_void_op(admin, callback, user_data, move |a| async move {
            a.close(timeout).await;
            Ok(())
        })
    };
}

// ---------------------------------------------------------------------------
// Async dispatch helpers
//
// Modelled on `async_void_op` / `async_value_op` in `src/ffi/consumer.rs`, minus
// the single-owner guard (see the module documentation).
//
// The critical invariant carried over from the consumer: C result handles are
// built inside the **completion closure that runs on the dispatcher thread**,
// never inside the spawned task. Raw pointers are `!Send`, so building them in
// the task would make its future `!Send` and it could not be spawned.
// ---------------------------------------------------------------------------

/// Async dispatch for a **void-returning** admin operation (currently only
/// `close`). `op` runs on the runtime and the callback fires on the dispatcher
/// thread — except for a NULL `admin`, which fires the callback inline on the
/// calling thread (see the module docs, *Callback thread*).
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
unsafe fn admin_async_void_op<F, Fut>(
    admin: *const kafka_admin_AdminClient_t,
    callback: OperationCallbackFn,
    user_data: *mut c_void,
    op: F,
) where
    F: FnOnce(&'static dyn Admin) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), KafkaError>> + Send,
{
    let target = OperationCallbackTarget { callback, user_data };
    if admin.is_null() {
        // Honor the callback obligation even for a null handle.
        unsafe {
            (target.callback)(
                box_error(KafkaError::illegal_argument("admin handle must not be null")),
                target.user_data,
            )
        };
        return;
    }
    let h = unsafe { handle_ref(admin) };
    let tx = h.completion_tx.clone();
    // `&'static dyn Admin` is `Send` because `Admin: Send + Sync`; resolving it
    // here keeps the (non-`Sync`) handle itself out of the spawned task.
    let client: &'static dyn Admin = h.admin();
    h.runtime_handle.spawn(async move {
        let target = target;
        let error = match op(client).await {
            Ok(()) => std::ptr::null_mut(),
            Err(e) => box_error(e),
        };
        let completion = OperationCompletion { callback: target.callback, user_data: target.user_data, error };
        let job: CompletionJob = Box::new(move || unsafe { completion.fire() });
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

/// Async dispatch for a **value-returning** admin RPC whose outcome is produced
/// by an arbitrary future.
///
/// `submit` runs on the **calling** thread (inside the runtime context, so the
/// RPC may `tokio::spawn` or notify the background task) and returns the future
/// to await. This mirrors Java, where `Admin.createTopics(...)` enqueues the
/// request on the caller's thread and returns immediately.
///
/// The spawned task only awaits that future. `complete` then runs on the
/// dispatcher thread, builds the C result handle from the `Ok` value (or an
/// error handle from the `Err`), and fires the typed C callback. Building the
/// handle inside `complete` (never inside the task) keeps the task's future free
/// of non-`Send` raw pointers.
///
/// Two paths never reach the dispatcher and run `complete` **inline on the
/// calling thread** instead: a NULL `admin`, and a `submit` that returns `Err`
/// (argument marshaling failed, so no RPC was issued and no task is spawned).
/// See the module docs, *Callback thread*.
///
/// Most RPCs award a single [`KafkaFuture`] and use the thin
/// [`admin_async_value_op`] wrapper below; this general form exists for
/// `describeCluster`, whose Java result holds four independent futures that must
/// all be awaited before one C handle can be built.
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
unsafe fn admin_async_future_op<T, S, Fut, C>(
    admin: *const kafka_admin_AdminClient_t,
    user_data: *mut c_void,
    submit: S,
    complete: C,
) where
    T: Send + 'static,
    S: FnOnce(&dyn Admin) -> Result<Fut, KafkaError>,
    Fut: std::future::Future<Output = Result<T, KafkaError>> + Send + 'static,
    C: FnOnce(Result<T, KafkaError>, *mut c_void) + Send + 'static,
{
    if admin.is_null() {
        // Honor the callback obligation even for a null handle.
        complete(Err(KafkaError::illegal_argument("admin handle must not be null")), user_data);
        return;
    }
    let h = unsafe { handle_ref(admin) };
    // Scoped so the runtime `EnterGuard` is dropped before we spawn.
    let submitted = {
        let _guard = h.runtime.enter();
        submit(h.admin())
    };
    let future = match submitted {
        Ok(f) => f,
        Err(e) => {
            // Argument marshaling failed: the RPC was never submitted, so fire
            // the callback inline rather than spawning.
            complete(Err(e), user_data);
            return;
        },
    };
    let tx = h.completion_tx.clone();
    let ud = SendUserData(user_data);
    h.runtime_handle.spawn(async move {
        let ud = ud;
        let result = future.await;
        let job: CompletionJob = Box::new(move || complete(result, ud.into_ptr()));
        enqueue_or_run_inline(&tx, job);
    });
}

/// Async dispatch for a value-returning admin RPC that resolves through one
/// [`KafkaFuture`] — normally `KafkaFuture::join_map_results(...)` over the
/// `*Result`'s per-key futures. Thin wrapper over [`admin_async_future_op`].
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
unsafe fn admin_async_value_op<T, S, C>(
    admin: *const kafka_admin_AdminClient_t,
    user_data: *mut c_void,
    submit: S,
    complete: C,
) where
    T: Clone + Send + Sync + 'static,
    S: FnOnce(&dyn Admin) -> Result<KafkaFuture<T>, KafkaError>,
    C: FnOnce(Result<T, KafkaError>, *mut c_void) + Send + 'static,
{
    unsafe {
        admin_async_future_op(
            admin,
            user_data,
            move |a| submit(a).map(|future| async move { future.get().await }),
            complete,
        )
    };
}

/// Synchronous dispatch for a value-returning admin RPC: runs `submit` on the
/// calling thread, then blocks until the returned future resolves.
///
/// This is the C equivalent of Java's `result.all().get()` in that it waits for
/// every per-key future; unlike `all()` it does not discard the per-key
/// outcomes, because the flattened result handle is the only channel C has for
/// them (`PLAN-bindings.md` D2).
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
unsafe fn admin_sync_future_op<T, S, Fut>(admin: *const kafka_admin_AdminClient_t, submit: S) -> Result<T, KafkaError>
where
    S: FnOnce(&dyn Admin) -> Result<Fut, KafkaError>,
    Fut: std::future::Future<Output = Result<T, KafkaError>>,
{
    if admin.is_null() {
        return Err(KafkaError::illegal_argument("admin handle must not be null"));
    }
    let h = unsafe { handle_ref(admin) };
    // Scoped so the runtime `EnterGuard` is dropped before `block_on`.
    let future = {
        let _guard = h.runtime.enter();
        submit(h.admin())?
    };
    h.runtime.block_on(future)
}

/// Synchronous dispatch for a value-returning admin RPC that resolves through
/// one [`KafkaFuture`]. Thin wrapper over [`admin_sync_future_op`].
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
unsafe fn admin_sync_value_op<T, S>(admin: *const kafka_admin_AdminClient_t, submit: S) -> Result<T, KafkaError>
where
    T: Clone + Send + Sync + 'static,
    S: FnOnce(&dyn Admin) -> Result<KafkaFuture<T>, KafkaError>,
{
    unsafe { admin_sync_future_op(admin, move |a| submit(a).map(|future| async move { future.get().await })) }
}

// ---------------------------------------------------------------------------
// Shared marshaling helpers
// ---------------------------------------------------------------------------

/// Builds a NUL-terminated [`CString`] from a Rust string, falling back to an
/// empty string if it contains an interior NUL (which Kafka identifiers, topic
/// ids and config keys never do).
fn to_cstring(s: &str) -> CString {
    CString::new(s.as_bytes()).unwrap_or_default()
}

/// Wraps a [`KafkaError`] for storage inside a result handle, so a getter can
/// hand out a borrowed `*const kafka_common_KafkaError_t` without a separate
/// heap allocation per key.
fn error_inner(error: KafkaError) -> KafkaErrorInner {
    let message_cstring = CString::new(error.message()).unwrap_or_default();
    KafkaErrorInner { error, message_cstring }
}

/// Returns a borrowed error pointer for `slot`, or null when the key succeeded.
fn error_ptr(slot: Option<&KafkaErrorInner>) -> *const kafka_common_KafkaError_t {
    match slot {
        Some(inner) => inner as *const KafkaErrorInner as *const kafka_common_KafkaError_t,
        None => std::ptr::null(),
    }
}

/// Returns the NUL-terminated bytes of the `index`th [`CString`], or null when
/// `index` is out of range.
fn cstring_at(strings: &[CString], index: i32) -> *const c_char {
    if index < 0 {
        return std::ptr::null();
    }
    match strings.get(index as usize) {
        Some(s) => s.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Reads `count` C strings into an owned `Vec<String>`, skipping NULL entries.
///
/// # Safety
///
/// `strings` must be null or have `count` entries, each NULL or a valid C string.
unsafe fn read_strings(strings: *const *const c_char, count: i32) -> Vec<String> {
    let n = count.max(0) as usize;
    if strings.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ptr = unsafe { *strings.add(i) };
        if ptr.is_null() {
            continue;
        }
        out.push(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string());
    }
    out
}

/// Reads `count` base64 topic-id strings (Java's `Uuid.toString()` form) into
/// [`Uuid`] values.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if any entry is NULL or not a valid
/// base64 UUID — mirroring Java's `Uuid.fromString`, which throws
/// `IllegalArgumentException`.
///
/// # Safety
///
/// `ids` must be null or have `count` entries, each NULL or a valid C string.
unsafe fn read_uuids(ids: *const *const c_char, count: i32) -> Result<Vec<Uuid>, KafkaError> {
    let n = count.max(0) as usize;
    if ids.is_null() {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ptr = unsafe { *ids.add(i) };
        if ptr.is_null() {
            return Err(KafkaError::illegal_argument(format!("topic id at index {i} must not be null")));
        }
        let text = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string();
        let uuid = Uuid::from_string(&text)
            .map_err(|e| KafkaError::illegal_argument(format!("invalid topic id `{text}` at index {i}: {e}")))?;
        out.push(uuid);
    }
    Ok(out)
}

/// Converts a C option timeout into the `Option<i32>` the `*Options` builders
/// take: a negative value means "unset", so the client's
/// `default.api.timeout.ms` applies (Java leaves `timeoutMs` null).
fn option_timeout(timeout_ms: i32) -> Option<i32> {
    if timeout_ms < 0 { None } else { Some(timeout_ms) }
}

/// The `UNKNOWN` wire code, which is `0` for every Kafka enum in this module
/// that has such a member: `ResourceType`, `PatternType`, `AclOperation`,
/// `AclPermissionType` and `ConfigResourceType`.
const UNKNOWN_ENUM_CODE: i8 = 0;

/// Narrows a C `int32_t` enum code to the `int8_t` Kafka defines its enums
/// over, returning `None` when the value does not fit.
///
/// **The rule for every `int32_t` enum code crossing this boundary: never
/// `as i8`.** A bare cast *truncates* — `259 as i8` is `3`, a valid code in
/// most of these enums — so a caller's out-of-range value would be read as a
/// different, legitimate member instead of being rejected or landing on
/// `UNKNOWN`.
///
/// What a miss means depends on the enum, and there are exactly two cases:
///
///   - **The enum has an `UNKNOWN` member.** Fall through to it with
///     [`enum_code_or_unknown`]. That extends the enum's own total function
///     (Java's `CODE_TO_VALUE.getOrDefault(code, UNKNOWN)`, mirrored by
///     `AclOperation::from_code` and `ConfigResourceType::for_id`) to the wider
///     C input type.
///   - **The enum has none** — the quota filter's `MATCH_TYPE_*` are bare wire
///     constants, and `AlterConfigOp::OpType::for_id` returns an `Option` —
///     so there is nothing to fall through to and the caller returns an
///     `IllegalArgument` naming the offending value.
fn narrow_enum_code(value: i32) -> Option<i8> {
    i8::try_from(value).ok()
}

/// [`narrow_enum_code`] for an enum that has an `UNKNOWN` member: anything that
/// does not fit in an `int8_t` becomes `UNKNOWN` rather than aliasing onto
/// another member.
fn enum_code_or_unknown(value: i32) -> i8 {
    narrow_enum_code(value).unwrap_or(UNKNOWN_ENUM_CODE)
}

/// Sorts a per-key outcome map into a deterministic, index-addressable order.
///
/// Java's `*Result` maps are unordered too, but C addresses entries by index, so
/// a stable order makes `_get_key(i)` / `_get_value(i)` / `_get_error(i)` line up
/// reproducibly across calls.
fn sorted_entries<K: Ord, V>(map: HashMap<K, V>) -> Vec<(K, V)> {
    let mut entries: Vec<(K, V)> = map.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

// ---------------------------------------------------------------------------
// NewTopic (input handle)
// ---------------------------------------------------------------------------

/// Opaque, mutable builder for a `NewTopic` request entry.
///
/// Java constructs `NewTopic` through overloaded constructors plus a fluent
/// `configs(...)`; C cannot express overloads, so this handle accumulates the
/// fields and [`NewTopicBuilder::build`] picks the matching Java constructor.
#[repr(C)]
pub struct kafka_admin_NewTopic_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_NewTopic_t`].
struct NewTopicBuilder {
    name: String,
    num_partitions: Option<i32>,
    replication_factor: Option<i16>,
    replicas_assignments: BTreeMap<i32, Vec<i32>>,
    configs: BTreeMap<String, String>,
}

impl NewTopicBuilder {
    /// Builds the [`NewTopic`], choosing the same constructor Java would:
    /// `NewTopic(name, replicasAssignments)` when at least one assignment was
    /// set, otherwise `NewTopic(name, Optional<Integer>, Optional<Short>)`.
    fn build(&self) -> NewTopic {
        let topic = if self.replicas_assignments.is_empty() {
            NewTopic::with_optional_defaults(self.name.clone(), self.num_partitions, self.replication_factor)
        } else {
            NewTopic::with_replicas_assignments(self.name.clone(), self.replicas_assignments.clone())
        };
        if self.configs.is_empty() {
            topic
        } else {
            topic.configs(self.configs.clone())
        }
    }
}

/// Casts a `*const kafka_admin_NewTopic_t` to a reference.
///
/// # Safety
///
/// `topic` must be a valid handle from [`kafka_admin_NewTopic_new`].
unsafe fn new_topic_ref(topic: *const kafka_admin_NewTopic_t) -> &'static NewTopicBuilder {
    unsafe { &*(topic as *const NewTopicBuilder) }
}

/// Casts a `*mut kafka_admin_NewTopic_t` to a mutable reference.
///
/// # Safety
///
/// `topic` must be a valid handle from [`kafka_admin_NewTopic_new`].
unsafe fn new_topic_mut(topic: *mut kafka_admin_NewTopic_t) -> &'static mut NewTopicBuilder {
    unsafe { &mut *(topic as *mut NewTopicBuilder) }
}

/// Creates a new-topic request entry.
///
/// Mirrors Java's `new NewTopic(name, Optional<Integer> numPartitions,
/// Optional<Short> replicationFactor)`: pass a negative `num_partitions` or
/// `replication_factor` to leave it unset, so the broker's `num.partitions` /
/// `default.replication.factor` applies.
///
/// # Returns
///
/// A non-null handle, or null if `name` is NULL. Free it with
/// [`kafka_admin_NewTopic_destroy`].
///
/// # Safety
///
/// `name` must be NULL or a valid, null-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_new(
    name: *const c_char,
    num_partitions: i32,
    replication_factor: i16,
) -> *mut kafka_admin_NewTopic_t {
    if name.is_null() {
        return std::ptr::null_mut();
    }
    let builder = NewTopicBuilder {
        name: unsafe { CStr::from_ptr(name) }.to_string_lossy().to_string(),
        num_partitions: if num_partitions < 0 { None } else { Some(num_partitions) },
        replication_factor: if replication_factor < 0 {
            None
        } else {
            Some(replication_factor)
        },
        replicas_assignments: BTreeMap::new(),
        configs: BTreeMap::new(),
    };
    Box::into_raw(Box::new(builder)) as *mut kafka_admin_NewTopic_t
}

/// Sets a topic-level configuration entry (Java's `NewTopic.configs(Map)`,
/// applied one key at a time). No-op if any parameter is null.
///
/// # Safety
///
/// `topic` must be a valid handle; `key` and `value` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_put_config(
    topic: *mut kafka_admin_NewTopic_t,
    key: *const c_char,
    value: *const c_char,
) {
    if topic.is_null() || key.is_null() || value.is_null() {
        return;
    }
    let builder = unsafe { new_topic_mut(topic) };
    let k = unsafe { CStr::from_ptr(key) }.to_string_lossy().to_string();
    let v = unsafe { CStr::from_ptr(value) }.to_string_lossy().to_string();
    builder.configs.insert(k, v);
}

/// Assigns the replicas (broker ids) for one partition.
///
/// Setting any assignment switches this entry to Java's
/// `new NewTopic(name, Map<Integer, List<Integer>> replicasAssignments)` form,
/// in which `num_partitions` / `replication_factor` are not sent. The first
/// broker id is the preferred leader. No-op if `topic` or `broker_ids` is null.
///
/// # Safety
///
/// `topic` must be a valid handle; `broker_ids` must have `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_set_replicas_assignment(
    topic: *mut kafka_admin_NewTopic_t,
    partition: i32,
    broker_ids: *const i32,
    count: i32,
) {
    if topic.is_null() || broker_ids.is_null() {
        return;
    }
    let builder = unsafe { new_topic_mut(topic) };
    let n = count.max(0) as usize;
    let mut replicas = Vec::with_capacity(n);
    for i in 0..n {
        replicas.push(unsafe { *broker_ids.add(i) });
    }
    builder.replicas_assignments.insert(partition, replicas);
}

/// Destroys a new-topic handle. Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// `topic` must be null or a valid handle from [`kafka_admin_NewTopic_new`].
/// After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_destroy(topic: *mut kafka_admin_NewTopic_t) {
    if !topic.is_null() {
        unsafe { drop(Box::from_raw(topic as *mut NewTopicBuilder)) };
    }
}

/// Builds the owned `Vec<NewTopic>` for a `createTopics` call from a C array of
/// new-topic handles, skipping NULL entries.
///
/// # Safety
///
/// `topics` must be null or have `count` entries, each NULL or a valid handle.
unsafe fn read_new_topics(topics: *const *const kafka_admin_NewTopic_t, count: i32) -> Vec<NewTopic> {
    let n = count.max(0) as usize;
    if topics.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ptr = unsafe { *topics.add(i) };
        if ptr.is_null() {
            continue;
        }
        out.push(unsafe { new_topic_ref(ptr) }.build());
    }
    out
}

// ---------------------------------------------------------------------------
// NewPartitions (input handle)
// ---------------------------------------------------------------------------

/// Opaque, mutable builder for a `NewPartitions` request entry.
///
/// Java offers two static factories (`NewPartitions.increaseTo(int)` and
/// `increaseTo(int, List<List<Integer>>)`); C cannot express overloads, so this
/// handle starts as the first form and switches to the second as soon as any
/// assignment is appended — exactly as [`kafka_admin_NewTopic_t`] does.
#[repr(C)]
pub struct kafka_admin_NewPartitions_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_NewPartitions_t`].
struct NewPartitionsBuilder {
    total_count: i32,
    /// One inner list of broker ids per *new* partition, in insertion order.
    new_assignments: Vec<Vec<i32>>,
}

impl NewPartitionsBuilder {
    /// Builds the [`NewPartitions`], choosing the same factory Java would.
    fn build(&self) -> NewPartitions {
        if self.new_assignments.is_empty() {
            NewPartitions::increase_to(self.total_count)
        } else {
            NewPartitions::increase_to_with_assignments(self.total_count, self.new_assignments.clone())
        }
    }
}

/// Casts a `*const kafka_admin_NewPartitions_t` to a reference.
///
/// # Safety
///
/// `partitions` must be a valid handle from [`kafka_admin_NewPartitions_new`].
unsafe fn new_partitions_ref(partitions: *const kafka_admin_NewPartitions_t) -> &'static NewPartitionsBuilder {
    unsafe { &*(partitions as *const NewPartitionsBuilder) }
}

/// Casts a `*mut kafka_admin_NewPartitions_t` to a mutable reference.
///
/// # Safety
///
/// `partitions` must be a valid handle from [`kafka_admin_NewPartitions_new`].
unsafe fn new_partitions_mut(partitions: *mut kafka_admin_NewPartitions_t) -> &'static mut NewPartitionsBuilder {
    unsafe { &mut *(partitions as *mut NewPartitionsBuilder) }
}

/// Creates a new-partitions request entry: increase the topic's partition count
/// to `total_count`, letting the broker decide the replica assignment.
///
/// Mirrors Java's `NewPartitions.increaseTo(int totalCount)`. `total_count` is
/// the total number of partitions *after* the operation, not the number added.
///
/// # Returns
///
/// A non-null handle. Free it with [`kafka_admin_NewPartitions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_NewPartitions_new(total_count: i32) -> *mut kafka_admin_NewPartitions_t {
    let builder = NewPartitionsBuilder { total_count, new_assignments: Vec::new() };
    Box::into_raw(Box::new(builder)) as *mut kafka_admin_NewPartitions_t
}

/// Appends the replica assignment (broker ids) for one *new* partition.
///
/// Appending any assignment switches this entry to Java's
/// `NewPartitions.increaseTo(int totalCount, List<List<Integer>> newAssignments)`
/// form. The number of appended lists should equal `total_count` minus the
/// topic's current partition count (existing partitions are not reassigned), and
/// each list should have `replication_factor` entries; the first broker id in a
/// list is the preferred leader. No-op if `partitions` or `broker_ids` is null.
///
/// # Safety
///
/// `partitions` must be a valid handle; `broker_ids` must have `count` valid
/// entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_add_assignment(
    partitions: *mut kafka_admin_NewPartitions_t,
    broker_ids: *const i32,
    count: i32,
) {
    if partitions.is_null() || broker_ids.is_null() {
        return;
    }
    let builder = unsafe { new_partitions_mut(partitions) };
    let n = count.max(0) as usize;
    let mut replicas = Vec::with_capacity(n);
    for i in 0..n {
        replicas.push(unsafe { *broker_ids.add(i) });
    }
    builder.new_assignments.push(replicas);
}

/// Destroys a new-partitions handle. Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// `partitions` must be null or a valid handle from
/// [`kafka_admin_NewPartitions_new`]. After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_destroy(partitions: *mut kafka_admin_NewPartitions_t) {
    if !partitions.is_null() {
        unsafe { drop(Box::from_raw(partitions as *mut NewPartitionsBuilder)) };
    }
}

/// Builds the owned `Map<String, NewPartitions>` for a `createPartitions` call
/// from two parallel C arrays.
///
/// Entry `i` pairs `topics[i]` with `new_partitions[i]`. A pair is skipped when
/// *either* side is NULL, so the two arrays cannot drift out of step (skipping
/// only one side would shift every later pairing).
///
/// # Safety
///
/// `topics` and `new_partitions` must be null or have `count` entries each,
/// every entry NULL or valid.
unsafe fn read_new_partitions(
    topics: *const *const c_char,
    new_partitions: *const *const kafka_admin_NewPartitions_t,
    count: i32,
) -> HashMap<String, NewPartitions> {
    let mut out = HashMap::new();
    if topics.is_null() || new_partitions.is_null() {
        return out;
    }
    for i in 0..count.max(0) as usize {
        let name_ptr = unsafe { *topics.add(i) };
        let spec_ptr = unsafe { *new_partitions.add(i) };
        if name_ptr.is_null() || spec_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        out.insert(name, unsafe { new_partitions_ref(spec_ptr) }.build());
    }
    out
}

/// Builds the owned `Map<TopicPartition, RecordsToDelete>` for a `deleteRecords`
/// call from three parallel C arrays.
///
/// Entry `i` is `(topics[i], partitions[i]) -> RecordsToDelete::before_offset(
/// before_offsets[i])`. `RecordsToDelete` carries only that offset, so it needs
/// no input handle of its own. An entry with a NULL topic is skipped.
///
/// # Safety
///
/// `topics`, `partitions` and `before_offsets` must be null or have `count`
/// entries each; every `topics` entry NULL or a valid C string.
unsafe fn read_records_to_delete(
    topics: *const *const c_char,
    partitions: *const i32,
    before_offsets: *const i64,
    count: i32,
) -> HashMap<TopicPartition, RecordsToDelete> {
    let mut out = HashMap::new();
    if topics.is_null() || partitions.is_null() || before_offsets.is_null() {
        return out;
    }
    for i in 0..count.max(0) as usize {
        let name_ptr = unsafe { *topics.add(i) };
        if name_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        let partition = unsafe { *partitions.add(i) };
        let offset = unsafe { *before_offsets.add(i) };
        out.insert(TopicPartition::new(name, partition), RecordsToDelete::before_offset(offset));
    }
    out
}

// ---------------------------------------------------------------------------
// Value types
//
// Each handle owns its strings in cached `CString`s and its nested entries in
// stable (boxed / vec-backed) allocations, so getters can return borrowed
// pointers valid until the owning result handle is destroyed.
// ---------------------------------------------------------------------------

/// One `ConfigEntry.ConfigSynonym` flattened for C.
///
/// Java models this as a nested class, but it carries three scalar fields and no
/// identity, so it is exposed through indexed accessors on
/// [`kafka_admin_ConfigEntry_t`] (`_synonym_name` / `_synonym_value` /
/// `_synonym_source`) rather than as an opaque handle of its own.
struct ConfigSynonymC {
    name_c: CString,
    /// `None` for a null value (Java's `ConfigSynonym.value()` is nullable).
    value_c: Option<CString>,
    source_c: CString,
}

/// Returns Java's implicit `Enum.name()` for a [`ConfigSource`].
///
/// `ConfigEntry.ConfigSource` is a plain Java enum with no numeric id (unlike
/// `ConfigResource.Type`, which has `id()`), so C receives the constant name
/// rather than an invented code.
fn config_source_name(source: ConfigSource) -> &'static str {
    match source {
        ConfigSource::DynamicTopicConfig => "DYNAMIC_TOPIC_CONFIG",
        ConfigSource::DynamicBrokerLoggerConfig => "DYNAMIC_BROKER_LOGGER_CONFIG",
        ConfigSource::DynamicBrokerConfig => "DYNAMIC_BROKER_CONFIG",
        ConfigSource::DynamicDefaultBrokerConfig => "DYNAMIC_DEFAULT_BROKER_CONFIG",
        ConfigSource::DynamicClientMetricsConfig => "DYNAMIC_CLIENT_METRICS_CONFIG",
        ConfigSource::DynamicGroupConfig => "DYNAMIC_GROUP_CONFIG",
        ConfigSource::StaticBrokerConfig => "STATIC_BROKER_CONFIG",
        ConfigSource::DefaultConfig => "DEFAULT_CONFIG",
        ConfigSource::Unknown => "UNKNOWN",
    }
}

/// Returns Java's implicit `Enum.name()` for a [`ConfigType`].
///
/// `ConfigEntry.ConfigType` is likewise a plain Java enum with no numeric id.
fn config_type_name(config_type: ConfigType) -> &'static str {
    match config_type {
        ConfigType::Unknown => "UNKNOWN",
        ConfigType::Boolean => "BOOLEAN",
        ConfigType::String => "STRING",
        ConfigType::Int => "INT",
        ConfigType::Short => "SHORT",
        ConfigType::Long => "LONG",
        ConfigType::Double => "DOUBLE",
        ConfigType::List => "LIST",
        ConfigType::Class => "CLASS",
        ConfigType::Password => "PASSWORD",
    }
}

/// A `ConfigEntry` flattened for C.
///
/// Every field Java's `ConfigEntry` exposes is carried. `createTopics` only
/// populates the first five (the broker's `CreateTopicsResponse` carries no
/// source, type, documentation or synonyms), which is why
/// [`kafka_admin_TopicMetadataAndConfig_t`] keeps its own flat `_config_*`
/// accessors for them; `describeConfigs` populates all of them and hands out a
/// [`kafka_admin_ConfigEntry_t`] instead.
struct ConfigEntryC {
    name_c: CString,
    /// `None` for a null config value (Java's `ConfigEntry.value()` is nullable).
    value_c: Option<CString>,
    is_default: bool,
    is_sensitive: bool,
    is_read_only: bool,
    source_c: CString,
    config_type_c: CString,
    /// `None` for null documentation (Java's `ConfigEntry.documentation()`).
    documentation_c: Option<CString>,
    synonyms: Vec<ConfigSynonymC>,
}

impl ConfigEntryC {
    /// Flattens one [`ConfigEntry`], preserving synonym order (Java's synonym
    /// list is ordered by precedence, so it must not be sorted).
    fn new(entry: &ConfigEntry) -> Self {
        Self {
            name_c: to_cstring(entry.name()),
            value_c: entry.value().map(to_cstring),
            is_default: entry.is_default(),
            is_sensitive: entry.is_sensitive(),
            is_read_only: entry.is_read_only(),
            source_c: to_cstring(config_source_name(entry.source())),
            config_type_c: to_cstring(config_type_name(entry.config_type())),
            documentation_c: entry.documentation().map(to_cstring),
            synonyms: entry
                .synonyms()
                .iter()
                .map(|s| ConfigSynonymC {
                    name_c: to_cstring(s.name()),
                    value_c: s.value().map(to_cstring),
                    source_c: to_cstring(config_source_name(s.source())),
                })
                .collect(),
        }
    }

    /// Flattens every entry of a [`Config`], sorted by name for stable indexing.
    fn from_config(config: &Config) -> Vec<ConfigEntryC> {
        let mut entries: Vec<ConfigEntryC> = config.entries().map(ConfigEntryC::new).collect();
        entries.sort_by(|a, b| a.name_c.cmp(&b.name_c));
        entries
    }
}

/// Opaque handle to a `Config` (the set of configuration entries of one
/// resource).
#[repr(C)]
pub struct kafka_admin_Config_t {
    _private: [u8; 0],
}

/// Opaque handle to a `ConfigEntry`.
#[repr(C)]
pub struct kafka_admin_ConfigEntry_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_Config_t`].
struct ConfigInner {
    entries: Vec<ConfigEntryC>,
}

impl ConfigInner {
    fn new(config: &Config) -> Self {
        Self { entries: ConfigEntryC::from_config(config) }
    }
}

/// Casts a `*const kafka_admin_Config_t` to a reference.
///
/// # Safety
///
/// `config` must be a non-null borrowed pointer from a result-handle getter.
unsafe fn config_ref(config: *const kafka_admin_Config_t) -> &'static ConfigInner {
    unsafe { &*(config as *const ConfigInner) }
}

/// Casts a `*const kafka_admin_ConfigEntry_t` to a reference.
///
/// # Safety
///
/// `entry` must be a non-null borrowed pointer from a [`kafka_admin_Config_t`]
/// getter.
unsafe fn config_entry_ref(entry: *const kafka_admin_ConfigEntry_t) -> &'static ConfigEntryC {
    unsafe { &*(entry as *const ConfigEntryC) }
}

/// Returns the number of configuration entries.
///
/// # Safety
///
/// `config` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_entry_count(config: *const kafka_admin_Config_t) -> i32 {
    unsafe { config_ref(config) }.entries.len() as i32
}

/// Returns the entry at `index` (borrowed), or null if out of range. Entries are
/// sorted by name.
///
/// # Safety
///
/// `config` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_get_entry(
    config: *const kafka_admin_Config_t,
    index: i32,
) -> *const kafka_admin_ConfigEntry_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { config_ref(config) }.entries.get(index as usize) {
        Some(entry) => entry as *const ConfigEntryC as *const kafka_admin_ConfigEntry_t,
        None => std::ptr::null(),
    }
}

/// Returns the entry named `name` (borrowed), or null if there is none.
/// Mirrors Java's `Config.get(String)`.
///
/// # Safety
///
/// `config` must be a valid borrowed pointer from a result-handle getter; `name`
/// must be null or a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_find_entry(
    config: *const kafka_admin_Config_t,
    name: *const c_char,
) -> *const kafka_admin_ConfigEntry_t {
    if name.is_null() {
        return std::ptr::null();
    }
    let wanted = unsafe { CStr::from_ptr(name) };
    match unsafe { config_ref(config) }
        .entries
        .iter()
        .find(|entry| entry.name_c.as_c_str() == wanted)
    {
        Some(entry) => entry as *const ConfigEntryC as *const kafka_admin_ConfigEntry_t,
        None => std::ptr::null(),
    }
}

/// Returns the entry name (borrowed).
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_name(entry: *const kafka_admin_ConfigEntry_t) -> *const c_char {
    unsafe { config_entry_ref(entry) }.name_c.as_ptr()
}

/// Returns the entry value (borrowed), or null when the value is null (Java's
/// `ConfigEntry.value()` is nullable — sensitive configs come back null).
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_value(entry: *const kafka_admin_ConfigEntry_t) -> *const c_char {
    match &unsafe { config_entry_ref(entry) }.value_c {
        Some(value) => value.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the config source as Java's enum constant name (borrowed), e.g.
/// `"DYNAMIC_TOPIC_CONFIG"` or `"DEFAULT_CONFIG"`.
///
/// `ConfigEntry.ConfigSource` has no numeric id in Java, so the name is the
/// contract.
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_source(entry: *const kafka_admin_ConfigEntry_t) -> *const c_char {
    unsafe { config_entry_ref(entry) }.source_c.as_ptr()
}

/// Returns whether the entry is a broker default (Java's `isDefault()`).
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_is_default(entry: *const kafka_admin_ConfigEntry_t) -> bool {
    unsafe { config_entry_ref(entry) }.is_default
}

/// Returns whether the entry is sensitive (its value is then null).
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_is_sensitive(entry: *const kafka_admin_ConfigEntry_t) -> bool {
    unsafe { config_entry_ref(entry) }.is_sensitive
}

/// Returns whether the entry is read-only.
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_is_read_only(entry: *const kafka_admin_ConfigEntry_t) -> bool {
    unsafe { config_entry_ref(entry) }.is_read_only
}

/// Returns the config type as Java's enum constant name (borrowed), e.g.
/// `"STRING"` or `"UNKNOWN"`.
///
/// `ConfigEntry.ConfigType` has no numeric id in Java, so the name is the
/// contract.
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_type(entry: *const kafka_admin_ConfigEntry_t) -> *const c_char {
    unsafe { config_entry_ref(entry) }.config_type_c.as_ptr()
}

/// Returns the entry documentation (borrowed), or null when the broker did not
/// report it (Java's `ConfigEntry.documentation()` is nullable).
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_documentation(
    entry: *const kafka_admin_ConfigEntry_t,
) -> *const c_char {
    match &unsafe { config_entry_ref(entry) }.documentation_c {
        Some(doc) => doc.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the number of synonyms of this entry.
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_synonym_count(entry: *const kafka_admin_ConfigEntry_t) -> i32 {
    unsafe { config_entry_ref(entry) }.synonyms.len() as i32
}

/// Returns the name of the synonym at `index` (borrowed), or null if out of
/// range. Synonyms keep Java's precedence order and are not sorted.
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_synonym_name(
    entry: *const kafka_admin_ConfigEntry_t,
    index: i32,
) -> *const c_char {
    match synonym_at(unsafe { config_entry_ref(entry) }, index) {
        Some(synonym) => synonym.name_c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the value of the synonym at `index` (borrowed), or null if the value
/// is null or `index` is out of range.
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_synonym_value(
    entry: *const kafka_admin_ConfigEntry_t,
    index: i32,
) -> *const c_char {
    match synonym_at(unsafe { config_entry_ref(entry) }, index) {
        Some(synonym) => match &synonym.value_c {
            Some(value) => value.as_ptr(),
            None => std::ptr::null(),
        },
        None => std::ptr::null(),
    }
}

/// Returns the source of the synonym at `index` as Java's enum constant name
/// (borrowed), or null if out of range.
///
/// # Safety
///
/// `entry` must be a valid borrowed pointer from a `Config` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_synonym_source(
    entry: *const kafka_admin_ConfigEntry_t,
    index: i32,
) -> *const c_char {
    match synonym_at(unsafe { config_entry_ref(entry) }, index) {
        Some(synonym) => synonym.source_c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the `index`th synonym of `entry`, or `None` if out of range.
fn synonym_at(entry: &ConfigEntryC, index: i32) -> Option<&ConfigSynonymC> {
    if index < 0 {
        return None;
    }
    entry.synonyms.get(index as usize)
}

/// Opaque handle to a `CreateTopicsResult.TopicMetadataAndConfig`.
#[repr(C)]
pub struct kafka_admin_TopicMetadataAndConfig_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_TopicMetadataAndConfig_t`].
struct TopicMetadataAndConfigInner {
    /// The exception Java's `ensureSuccess()` would rethrow from every accessor.
    /// `None` on success. This is *not* the per-key future error: the topic was
    /// created, but the broker did not return its metadata.
    error: Option<KafkaErrorInner>,
    topic_id_c: CString,
    num_partitions: i32,
    replication_factor: i32,
    configs: Vec<ConfigEntryC>,
}

impl TopicMetadataAndConfigInner {
    fn new(value: &TopicMetadataAndConfig) -> Self {
        // One `ensure_success`-equivalent probe: if the holder carries an
        // exception every accessor returns it, so the metadata is unavailable.
        match value.topic_id() {
            Ok(topic_id) => Self {
                error: None,
                topic_id_c: to_cstring(&topic_id.to_string()),
                // `unwrap_or` is unreachable here: all four accessors share the
                // same `ensure_success` gate, which just returned `Ok`.
                num_partitions: value.num_partitions().unwrap_or(-1),
                replication_factor: value.replication_factor().unwrap_or(-1),
                configs: value.config().map(|c| ConfigEntryC::from_config(&c)).unwrap_or_default(),
            },
            Err(e) => Self {
                error: Some(error_inner(e)),
                topic_id_c: CString::default(),
                // Matches `create_topics_result::UNKNOWN`.
                num_partitions: -1,
                replication_factor: -1,
                configs: Vec::new(),
            },
        }
    }
}

/// Casts a `*const kafka_admin_TopicMetadataAndConfig_t` to a reference.
///
/// # Safety
///
/// `mc` must be a non-null borrowed pointer from a result-handle getter.
unsafe fn metadata_ref(mc: *const kafka_admin_TopicMetadataAndConfig_t) -> &'static TopicMetadataAndConfigInner {
    unsafe { &*(mc as *const TopicMetadataAndConfigInner) }
}

/// Returns the error that the metadata accessors would raise, or null if the
/// metadata is available.
///
/// Mirrors Java's `TopicMetadataAndConfig.ensureSuccess()`: the topic creation
/// itself succeeded (so the per-key error from
/// `kafka_admin_CreateTopicsResult_get_error` is null), but the broker did not
/// return the topic's metadata. The returned pointer is **borrowed** — valid
/// until the owning result handle is destroyed; do not destroy it.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_error(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
) -> *const kafka_common_KafkaError_t {
    error_ptr(unsafe { metadata_ref(mc) }.error.as_ref())
}

/// Returns the topic id as a base64 string (Java's `Uuid.toString()`), or an
/// empty string if the metadata is unavailable (see
/// [`kafka_admin_TopicMetadataAndConfig_error`]). Borrowed.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_topic_id(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
) -> *const c_char {
    unsafe { metadata_ref(mc) }.topic_id_c.as_ptr()
}

/// Returns the number of partitions, or -1 if the metadata is unavailable.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_num_partitions(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
) -> i32 {
    unsafe { metadata_ref(mc) }.num_partitions
}

/// Returns the replication factor, or -1 if the metadata is unavailable.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_replication_factor(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
) -> i32 {
    unsafe { metadata_ref(mc) }.replication_factor
}

/// Returns the number of topic config entries (0 if the metadata is
/// unavailable).
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_config_count(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
) -> i32 {
    unsafe { metadata_ref(mc) }.configs.len() as i32
}

/// Returns the name of the config entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by name.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_config_name(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
    index: i32,
) -> *const c_char {
    match config_entry_at(unsafe { metadata_ref(mc) }, index) {
        Some(entry) => entry.name_c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the value of the config entry at `index` (borrowed), or null if out of
/// range **or** if the entry's value is null (Java's `ConfigEntry.value()` is
/// nullable).
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_config_value(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
    index: i32,
) -> *const c_char {
    match config_entry_at(unsafe { metadata_ref(mc) }, index).and_then(|e| e.value_c.as_ref()) {
        Some(value) => value.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns whether the config entry at `index` is the broker default
/// (`ConfigEntry.isDefault()`); `false` if out of range.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_config_is_default(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
    index: i32,
) -> bool {
    config_entry_at(unsafe { metadata_ref(mc) }, index).is_some_and(|e| e.is_default)
}

/// Returns whether the config entry at `index` is sensitive
/// (`ConfigEntry.isSensitive()`); `false` if out of range.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_config_is_sensitive(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
    index: i32,
) -> bool {
    config_entry_at(unsafe { metadata_ref(mc) }, index).is_some_and(|e| e.is_sensitive)
}

/// Returns whether the config entry at `index` is read-only
/// (`ConfigEntry.isReadOnly()`); `false` if out of range.
///
/// # Safety
///
/// `mc` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicMetadataAndConfig_config_is_read_only(
    mc: *const kafka_admin_TopicMetadataAndConfig_t,
    index: i32,
) -> bool {
    config_entry_at(unsafe { metadata_ref(mc) }, index).is_some_and(|e| e.is_read_only)
}

/// Bounds-checked lookup of a flattened config entry.
fn config_entry_at(inner: &TopicMetadataAndConfigInner, index: i32) -> Option<&ConfigEntryC> {
    if index < 0 {
        return None;
    }
    inner.configs.get(index as usize)
}

/// Opaque handle to a `TopicPartitionInfo`.
#[repr(C)]
pub struct kafka_admin_TopicPartitionInfo_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_TopicPartitionInfo_t`].
///
/// Node lists are owned here so [`kafka_common_Node_t`] pointers handed out by
/// the getters stay valid for the lifetime of the owning result handle.
struct TopicPartitionInfoInner {
    partition: i32,
    leader: Option<Node>,
    replicas: Vec<Node>,
    isr: Vec<Node>,
    /// `None` when the broker did not report an eligible-leader-replica set
    /// (Java's `TopicPartitionInfo.elr()` returns null).
    elr: Option<Vec<Node>>,
    last_known_elr: Option<Vec<Node>>,
}

impl TopicPartitionInfoInner {
    fn new(info: &TopicPartitionInfo) -> Self {
        Self {
            partition: info.partition(),
            leader: info.leader().cloned(),
            replicas: info.replicas().to_vec(),
            isr: info.isr().to_vec(),
            elr: info.elr().map(<[Node]>::to_vec),
            last_known_elr: info.last_known_elr().map(<[Node]>::to_vec),
        }
    }
}

/// Casts a `*const kafka_admin_TopicPartitionInfo_t` to a reference.
///
/// # Safety
///
/// `info` must be a non-null borrowed pointer from a `TopicDescription` getter.
unsafe fn partition_info_ref(info: *const kafka_admin_TopicPartitionInfo_t) -> &'static TopicPartitionInfoInner {
    unsafe { &*(info as *const TopicPartitionInfoInner) }
}

/// Returns a borrowed [`kafka_common_Node_t`] pointer for `nodes[index]`, or null
/// if out of range.
fn node_at(nodes: &[Node], index: i32) -> *const kafka_common_Node_t {
    if index < 0 {
        return std::ptr::null();
    }
    match nodes.get(index as usize) {
        Some(node) => node as *const Node as *const kafka_common_Node_t,
        None => std::ptr::null(),
    }
}

/// Returns the partition id.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_partition(
    info: *const kafka_admin_TopicPartitionInfo_t,
) -> i32 {
    unsafe { partition_info_ref(info) }.partition
}

/// Returns the partition leader (borrowed), or null if there is no leader.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_leader(
    info: *const kafka_admin_TopicPartitionInfo_t,
) -> *const kafka_common_Node_t {
    match unsafe { partition_info_ref(info) }.leader.as_ref() {
        Some(node) => node as *const Node as *const kafka_common_Node_t,
        None => std::ptr::null(),
    }
}

/// Returns the number of replicas.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_replica_count(
    info: *const kafka_admin_TopicPartitionInfo_t,
) -> i32 {
    unsafe { partition_info_ref(info) }.replicas.len() as i32
}

/// Returns the replica at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_replica(
    info: *const kafka_admin_TopicPartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    node_at(&unsafe { partition_info_ref(info) }.replicas, index)
}

/// Returns the number of in-sync replicas.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_isr_count(
    info: *const kafka_admin_TopicPartitionInfo_t,
) -> i32 {
    unsafe { partition_info_ref(info) }.isr.len() as i32
}

/// Returns the in-sync replica at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_isr(
    info: *const kafka_admin_TopicPartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    node_at(&unsafe { partition_info_ref(info) }.isr, index)
}

/// Returns the number of eligible leader replicas, or **-1** if the broker did
/// not report an ELR set (Java's `elr()` returns null). 0 means "reported, but
/// empty".
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_elr_count(
    info: *const kafka_admin_TopicPartitionInfo_t,
) -> i32 {
    match unsafe { partition_info_ref(info) }.elr.as_ref() {
        Some(nodes) => nodes.len() as i32,
        None => -1,
    }
}

/// Returns the eligible leader replica at `index` (borrowed), or null if absent
/// or out of range.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_elr(
    info: *const kafka_admin_TopicPartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    match unsafe { partition_info_ref(info) }.elr.as_ref() {
        Some(nodes) => node_at(nodes, index),
        None => std::ptr::null(),
    }
}

/// Returns the number of last-known eligible leader replicas, or **-1** if the
/// broker did not report the set (Java's `lastKnownElr()` returns null).
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_last_known_elr_count(
    info: *const kafka_admin_TopicPartitionInfo_t,
) -> i32 {
    match unsafe { partition_info_ref(info) }.last_known_elr.as_ref() {
        Some(nodes) => nodes.len() as i32,
        None => -1,
    }
}

/// Returns the last-known eligible leader replica at `index` (borrowed), or null
/// if absent or out of range.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a `TopicDescription` getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicPartitionInfo_last_known_elr(
    info: *const kafka_admin_TopicPartitionInfo_t,
    index: i32,
) -> *const kafka_common_Node_t {
    match unsafe { partition_info_ref(info) }.last_known_elr.as_ref() {
        Some(nodes) => node_at(nodes, index),
        None => std::ptr::null(),
    }
}

/// Opaque handle to a `TopicDescription`.
#[repr(C)]
pub struct kafka_admin_TopicDescription_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_TopicDescription_t`].
struct TopicDescriptionInner {
    name_c: CString,
    topic_id_c: CString,
    internal: bool,
    partitions: Vec<TopicPartitionInfoInner>,
    /// `AclOperation` wire codes (Java's `AclOperation.code()`), ascending.
    authorized_operations: Vec<i32>,
}

impl TopicDescriptionInner {
    fn new(description: &TopicDescription) -> Self {
        Self {
            name_c: to_cstring(description.name()),
            topic_id_c: to_cstring(&description.topic_id().to_string()),
            internal: description.is_internal(),
            partitions: description.partitions().iter().map(TopicPartitionInfoInner::new).collect(),
            authorized_operations: description
                .authorized_operations()
                .iter()
                .map(|op| i32::from(op.code()))
                .collect(),
        }
    }
}

/// Casts a `*const kafka_admin_TopicDescription_t` to a reference.
///
/// # Safety
///
/// `description` must be a non-null borrowed pointer from a result-handle getter.
unsafe fn description_ref(description: *const kafka_admin_TopicDescription_t) -> &'static TopicDescriptionInner {
    unsafe { &*(description as *const TopicDescriptionInner) }
}

/// Returns the topic name (borrowed).
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_name(
    description: *const kafka_admin_TopicDescription_t,
) -> *const c_char {
    unsafe { description_ref(description) }.name_c.as_ptr()
}

/// Returns the topic id as a base64 string (Java's `Uuid.toString()`), borrowed.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_topic_id(
    description: *const kafka_admin_TopicDescription_t,
) -> *const c_char {
    unsafe { description_ref(description) }.topic_id_c.as_ptr()
}

/// Returns whether the topic is internal (e.g. `__consumer_offsets`).
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_is_internal(
    description: *const kafka_admin_TopicDescription_t,
) -> bool {
    unsafe { description_ref(description) }.internal
}

/// Returns the number of partitions.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_partition_count(
    description: *const kafka_admin_TopicDescription_t,
) -> i32 {
    unsafe { description_ref(description) }.partitions.len() as i32
}

/// Returns the partition at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_partition(
    description: *const kafka_admin_TopicDescription_t,
    index: i32,
) -> *const kafka_admin_TopicPartitionInfo_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { description_ref(description) }.partitions.get(index as usize) {
        Some(info) => info as *const TopicPartitionInfoInner as *const kafka_admin_TopicPartitionInfo_t,
        None => std::ptr::null(),
    }
}

/// Returns the number of authorized operations reported for the topic (0 when
/// the request did not ask for them).
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_authorized_operation_count(
    description: *const kafka_admin_TopicDescription_t,
) -> i32 {
    unsafe { description_ref(description) }.authorized_operations.len() as i32
}

/// Returns the `AclOperation` wire code (Java's `AclOperation.code()`) of the
/// authorized operation at `index`, or -1 if out of range.
///
/// Codes are exposed rather than an opaque enum type because `AclOperation` is
/// a plain byte-coded enum on the wire; the named ACL types arrive with the ACL
/// slice, where `AclBinding` needs them.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_authorized_operation(
    description: *const kafka_admin_TopicDescription_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { description_ref(description) }
        .authorized_operations
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Opaque handle to a `TopicListing`.
#[repr(C)]
pub struct kafka_admin_TopicListing_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_TopicListing_t`].
struct TopicListingInner {
    name_c: CString,
    topic_id_c: CString,
    internal: bool,
}

impl TopicListingInner {
    fn new(listing: &TopicListing) -> Self {
        Self {
            name_c: to_cstring(listing.name()),
            topic_id_c: to_cstring(&listing.topic_id().to_string()),
            internal: listing.is_internal(),
        }
    }
}

/// Casts a `*const kafka_admin_TopicListing_t` to a reference.
///
/// # Safety
///
/// `listing` must be a non-null borrowed pointer from a result-handle getter.
unsafe fn listing_ref(listing: *const kafka_admin_TopicListing_t) -> &'static TopicListingInner {
    unsafe { &*(listing as *const TopicListingInner) }
}

/// Returns the topic name (borrowed).
///
/// # Safety
///
/// `listing` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_name(listing: *const kafka_admin_TopicListing_t) -> *const c_char {
    unsafe { listing_ref(listing) }.name_c.as_ptr()
}

/// Returns the topic id as a base64 string (Java's `Uuid.toString()`), borrowed.
///
/// # Safety
///
/// `listing` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_topic_id(
    listing: *const kafka_admin_TopicListing_t,
) -> *const c_char {
    unsafe { listing_ref(listing) }.topic_id_c.as_ptr()
}

/// Returns whether the topic is internal.
///
/// # Safety
///
/// `listing` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_is_internal(listing: *const kafka_admin_TopicListing_t) -> bool {
    unsafe { listing_ref(listing) }.internal
}

// ---------------------------------------------------------------------------
// Result handles
//
// One flattened handle per RPC (PLAN-bindings.md D2). Each owns parallel vecs
// for keys, values and per-key errors so `_get_key(i)` / `_get_value(i)` /
// `_get_error(i)` line up, and each returns *borrowed* sub-handles valid until
// the result is destroyed.
// ---------------------------------------------------------------------------

/// Opaque handle to a flattened `CreateTopicsResult`, keyed by topic name.
#[repr(C)]
pub struct kafka_admin_CreateTopicsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_CreateTopicsResult_t`].
struct CreateTopicsResultInner {
    keys: Vec<CString>,
    values: Vec<Option<TopicMetadataAndConfigInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-topic outcomes into the C handle.
fn box_create_topics_result(
    outcomes: HashMap<String, Result<TopicMetadataAndConfig, KafkaError>>,
) -> *mut kafka_admin_CreateTopicsResult_t {
    let entries = sorted_entries(outcomes);
    let mut keys = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (name, outcome) in entries {
        keys.push(to_cstring(&name));
        match outcome {
            Ok(metadata) => {
                values.push(Some(TopicMetadataAndConfigInner::new(&metadata)));
                errors.push(None);
            },
            Err(e) => {
                values.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(CreateTopicsResultInner { keys, values, errors })) as *mut kafka_admin_CreateTopicsResult_t
}

/// Casts a `*const kafka_admin_CreateTopicsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `create_topics` call.
unsafe fn create_topics_result_ref(
    result: *const kafka_admin_CreateTopicsResult_t,
) -> &'static CreateTopicsResultInner {
    unsafe { &*(result as *const CreateTopicsResultInner) }
}

/// Returns the number of requested topics.
///
/// # Safety
///
/// `result` must be a valid `create_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_count(result: *const kafka_admin_CreateTopicsResult_t) -> i32 {
    unsafe { create_topics_result_ref(result) }.keys.len() as i32
}

/// Returns the topic name at `index` (borrowed), or null if out of range.
/// Entries are sorted by topic name.
///
/// # Safety
///
/// `result` must be a valid `create_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_get_key(
    result: *const kafka_admin_CreateTopicsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { create_topics_result_ref(result) }.keys, index)
}

/// Returns the metadata for the topic at `index` (borrowed), or null if that
/// topic failed (see [`kafka_admin_CreateTopicsResult_get_error`]) or `index` is
/// out of range.
///
/// # Safety
///
/// `result` must be a valid `create_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_get_value(
    result: *const kafka_admin_CreateTopicsResult_t,
    index: i32,
) -> *const kafka_admin_TopicMetadataAndConfig_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { create_topics_result_ref(result) }.values.get(index as usize) {
        Some(Some(metadata)) => {
            metadata as *const TopicMetadataAndConfigInner as *const kafka_admin_TopicMetadataAndConfig_t
        },
        _ => std::ptr::null(),
    }
}

/// Returns the error for the topic at `index` (borrowed), or null if that topic
/// was created successfully or `index` is out of range.
///
/// The pointer is borrowed from the result handle — read it with the
/// `kafka_common_KafkaError_*` accessors, but do **not** destroy it.
///
/// # Safety
///
/// `result` must be a valid `create_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_get_error(
    result: *const kafka_admin_CreateTopicsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { create_topics_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `create_topics` result handle, invalidating every borrowed
/// sub-handle obtained from it. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `create_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_destroy(result: *mut kafka_admin_CreateTopicsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut CreateTopicsResultInner)) };
    }
}

/// Opaque handle to a flattened `DeleteTopicsResult`.
#[repr(C)]
pub struct kafka_admin_DeleteTopicsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DeleteTopicsResult_t`].
///
/// Java's `deleteTopics` result is keyed by topic name **or** topic id depending
/// on the `TopicCollection` passed in; there is no per-key value (the future is
/// `KafkaFuture<Void>`), only success or an error.
struct DeleteTopicsResultInner {
    keys: Vec<CString>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-key `deleteTopics` outcomes into the C handle. `key_text`
/// renders each key (topic name, or the base64 topic id).
fn box_delete_topics_result<K: Ord>(
    outcomes: HashMap<K, Result<(), KafkaError>>,
    key_text: impl Fn(&K) -> String,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    let entries = sorted_entries(outcomes);
    let mut keys = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (key, outcome) in entries {
        keys.push(to_cstring(&key_text(&key)));
        errors.push(outcome.err().map(error_inner));
    }
    Box::into_raw(Box::new(DeleteTopicsResultInner { keys, errors })) as *mut kafka_admin_DeleteTopicsResult_t
}

/// Casts a `*const kafka_admin_DeleteTopicsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `delete_topics` call.
unsafe fn delete_topics_result_ref(
    result: *const kafka_admin_DeleteTopicsResult_t,
) -> &'static DeleteTopicsResultInner {
    unsafe { &*(result as *const DeleteTopicsResultInner) }
}

/// Returns the number of requested topics.
///
/// # Safety
///
/// `result` must be a valid `delete_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_count(result: *const kafka_admin_DeleteTopicsResult_t) -> i32 {
    unsafe { delete_topics_result_ref(result) }.keys.len() as i32
}

/// Returns the key at `index` (borrowed): the topic name for
/// [`kafka_admin_AdminClient_delete_topics`], or the base64 topic id for
/// [`kafka_admin_AdminClient_delete_topics_by_ids`]. Null if out of range.
///
/// # Safety
///
/// `result` must be a valid `delete_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_get_key(
    result: *const kafka_admin_DeleteTopicsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { delete_topics_result_ref(result) }.keys, index)
}

/// Returns the error for the topic at `index` (borrowed), or null if that topic
/// was deleted successfully or `index` is out of range. Do not destroy it.
///
/// There is no `_get_value`: Java's per-key future is `KafkaFuture<Void>`, so a
/// null error *is* the success value.
///
/// # Safety
///
/// `result` must be a valid `delete_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_get_error(
    result: *const kafka_admin_DeleteTopicsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { delete_topics_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `delete_topics` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `delete_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_destroy(result: *mut kafka_admin_DeleteTopicsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DeleteTopicsResultInner)) };
    }
}

/// Opaque handle to a `ListTopicsResult`, keyed by topic name.
#[repr(C)]
pub struct kafka_admin_ListTopicsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListTopicsResult_t`].
///
/// Java's `listTopics` has a single `KafkaFuture<Map<String, TopicListing>>`, so
/// there are no per-key errors: the whole call either succeeds or fails, and the
/// failure is delivered as the call's error.
struct ListTopicsResultInner {
    keys: Vec<CString>,
    values: Vec<TopicListingInner>,
}

/// Flattens the topic listings into the C handle.
fn box_list_topics_result(listings: HashMap<String, TopicListing>) -> *mut kafka_admin_ListTopicsResult_t {
    let entries = sorted_entries(listings);
    let mut keys = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    for (name, listing) in entries {
        keys.push(to_cstring(&name));
        values.push(TopicListingInner::new(&listing));
    }
    Box::into_raw(Box::new(ListTopicsResultInner { keys, values })) as *mut kafka_admin_ListTopicsResult_t
}

/// Casts a `*const kafka_admin_ListTopicsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_topics` call.
unsafe fn list_topics_result_ref(result: *const kafka_admin_ListTopicsResult_t) -> &'static ListTopicsResultInner {
    unsafe { &*(result as *const ListTopicsResultInner) }
}

/// Returns the number of listed topics.
///
/// # Safety
///
/// `result` must be a valid `list_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_count(result: *const kafka_admin_ListTopicsResult_t) -> i32 {
    unsafe { list_topics_result_ref(result) }.keys.len() as i32
}

/// Returns the topic name at `index` (borrowed), or null if out of range.
/// Entries are sorted by topic name.
///
/// # Safety
///
/// `result` must be a valid `list_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_get_key(
    result: *const kafka_admin_ListTopicsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { list_topics_result_ref(result) }.keys, index)
}

/// Returns the listing at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `result` must be a valid `list_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_get_value(
    result: *const kafka_admin_ListTopicsResult_t,
    index: i32,
) -> *const kafka_admin_TopicListing_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_topics_result_ref(result) }.values.get(index as usize) {
        Some(listing) => listing as *const TopicListingInner as *const kafka_admin_TopicListing_t,
        None => std::ptr::null(),
    }
}

/// Destroys a `list_topics` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_destroy(result: *mut kafka_admin_ListTopicsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListTopicsResultInner)) };
    }
}

/// Opaque handle to a flattened `DescribeTopicsResult`.
#[repr(C)]
pub struct kafka_admin_DescribeTopicsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeTopicsResult_t`].
struct DescribeTopicsResultInner {
    keys: Vec<CString>,
    values: Vec<Option<TopicDescriptionInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-key `describeTopics` outcomes into the C handle. `key_text`
/// renders each key (topic name, or the base64 topic id).
fn box_describe_topics_result<K: Ord>(
    outcomes: HashMap<K, Result<TopicDescription, KafkaError>>,
    key_text: impl Fn(&K) -> String,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    let entries = sorted_entries(outcomes);
    let mut keys = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (key, outcome) in entries {
        keys.push(to_cstring(&key_text(&key)));
        match outcome {
            Ok(description) => {
                values.push(Some(TopicDescriptionInner::new(&description)));
                errors.push(None);
            },
            Err(e) => {
                values.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(DescribeTopicsResultInner { keys, values, errors }))
        as *mut kafka_admin_DescribeTopicsResult_t
}

/// Casts a `*const kafka_admin_DescribeTopicsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_topics` call.
unsafe fn describe_topics_result_ref(
    result: *const kafka_admin_DescribeTopicsResult_t,
) -> &'static DescribeTopicsResultInner {
    unsafe { &*(result as *const DescribeTopicsResultInner) }
}

/// Returns the number of requested topics.
///
/// # Safety
///
/// `result` must be a valid `describe_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_count(
    result: *const kafka_admin_DescribeTopicsResult_t,
) -> i32 {
    unsafe { describe_topics_result_ref(result) }.keys.len() as i32
}

/// Returns the key at `index` (borrowed): the topic name for
/// [`kafka_admin_AdminClient_describe_topics`], or the base64 topic id for
/// [`kafka_admin_AdminClient_describe_topics_by_ids`]. Null if out of range.
///
/// # Safety
///
/// `result` must be a valid `describe_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_get_key(
    result: *const kafka_admin_DescribeTopicsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { describe_topics_result_ref(result) }.keys, index)
}

/// Returns the description for the topic at `index` (borrowed), or null if that
/// topic failed (see [`kafka_admin_DescribeTopicsResult_get_error`]) or `index`
/// is out of range.
///
/// # Safety
///
/// `result` must be a valid `describe_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_get_value(
    result: *const kafka_admin_DescribeTopicsResult_t,
    index: i32,
) -> *const kafka_admin_TopicDescription_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_topics_result_ref(result) }.values.get(index as usize) {
        Some(Some(description)) => description as *const TopicDescriptionInner as *const kafka_admin_TopicDescription_t,
        _ => std::ptr::null(),
    }
}

/// Returns the error for the topic at `index` (borrowed), or null if that topic
/// was described successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_get_error(
    result: *const kafka_admin_DescribeTopicsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_topics_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `describe_topics` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_topics` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_destroy(result: *mut kafka_admin_DescribeTopicsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeTopicsResultInner)) };
    }
}

/// Opaque handle to a flattened `CreatePartitionsResult`, keyed by topic name.
#[repr(C)]
pub struct kafka_admin_CreatePartitionsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_CreatePartitionsResult_t`].
///
/// There is no per-key value: Java's per-topic future is `KafkaFuture<Void>`, so
/// a null error *is* the success value (as for `deleteTopics`).
struct CreatePartitionsResultInner {
    keys: Vec<CString>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-topic `createPartitions` outcomes into the C handle.
fn box_create_partitions_result(
    outcomes: HashMap<String, Result<(), KafkaError>>,
) -> *mut kafka_admin_CreatePartitionsResult_t {
    let entries = sorted_entries(outcomes);
    let mut keys = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (name, outcome) in entries {
        keys.push(to_cstring(&name));
        errors.push(outcome.err().map(error_inner));
    }
    Box::into_raw(Box::new(CreatePartitionsResultInner { keys, errors })) as *mut kafka_admin_CreatePartitionsResult_t
}

/// Casts a `*const kafka_admin_CreatePartitionsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `create_partitions` call.
unsafe fn create_partitions_result_ref(
    result: *const kafka_admin_CreatePartitionsResult_t,
) -> &'static CreatePartitionsResultInner {
    unsafe { &*(result as *const CreatePartitionsResultInner) }
}

/// Returns the number of requested topics.
///
/// # Safety
///
/// `result` must be a valid `create_partitions` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreatePartitionsResult_count(
    result: *const kafka_admin_CreatePartitionsResult_t,
) -> i32 {
    unsafe { create_partitions_result_ref(result) }.keys.len() as i32
}

/// Returns the topic name at `index` (borrowed), or null if out of range.
/// Entries are sorted by topic name.
///
/// # Safety
///
/// `result` must be a valid `create_partitions` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreatePartitionsResult_get_key(
    result: *const kafka_admin_CreatePartitionsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { create_partitions_result_ref(result) }.keys, index)
}

/// Returns the error for the topic at `index` (borrowed), or null if that
/// topic's partitions were created successfully or `index` is out of range. Do
/// not destroy it.
///
/// There is no `_get_value`: Java's per-topic future is `KafkaFuture<Void>`, so
/// a null error *is* the success value.
///
/// # Safety
///
/// `result` must be a valid `create_partitions` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreatePartitionsResult_get_error(
    result: *const kafka_admin_CreatePartitionsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { create_partitions_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `create_partitions` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `create_partitions` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreatePartitionsResult_destroy(result: *mut kafka_admin_CreatePartitionsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut CreatePartitionsResultInner)) };
    }
}

/// Opaque handle to a flattened `DeleteRecordsResult`, keyed by topic partition.
#[repr(C)]
pub struct kafka_admin_DeleteRecordsResult_t {
    _private: [u8; 0],
}

/// The low-watermark value reported for a partition whose deletion failed, or
/// for an out-of-range index. Real low watermarks are never negative.
const UNKNOWN_LOW_WATERMARK: i64 = -1;

/// Backing state for [`kafka_admin_DeleteRecordsResult_t`].
///
/// The key is a `TopicPartition`, which C reads as a topic name plus a partition
/// id (`_get_topic(i)` / `_get_partition(i)`) rather than through a dedicated
/// handle type. The per-key value is Java's `DeletedRecords`, whose only field is
/// the low watermark.
struct DeleteRecordsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    low_watermarks: Vec<i64>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-partition `deleteRecords` outcomes into the C handle.
///
/// Entries are sorted by `(topic, partition)`: `TopicPartition` is not `Ord`
/// (matching Java, where the map is unordered), but C addresses entries by index
/// so the order must be reproducible.
fn box_delete_records_result(
    outcomes: HashMap<TopicPartition, Result<DeletedRecords, KafkaError>>,
) -> *mut kafka_admin_DeleteRecordsResult_t {
    let mut entries: Vec<(TopicPartition, Result<DeletedRecords, KafkaError>)> = outcomes.into_iter().collect();
    entries.sort_by(|a, b| a.0.topic().cmp(b.0.topic()).then(a.0.partition().cmp(&b.0.partition())));

    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut low_watermarks = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (tp, outcome) in entries {
        topics.push(to_cstring(tp.topic()));
        partitions.push(tp.partition());
        match outcome {
            Ok(deleted) => {
                low_watermarks.push(deleted.low_watermark());
                errors.push(None);
            },
            Err(e) => {
                low_watermarks.push(UNKNOWN_LOW_WATERMARK);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(DeleteRecordsResultInner {
        topics,
        partitions,
        low_watermarks,
        errors,
    })) as *mut kafka_admin_DeleteRecordsResult_t
}

/// Casts a `*const kafka_admin_DeleteRecordsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `delete_records` call.
unsafe fn delete_records_result_ref(
    result: *const kafka_admin_DeleteRecordsResult_t,
) -> &'static DeleteRecordsResultInner {
    unsafe { &*(result as *const DeleteRecordsResultInner) }
}

/// Returns the number of requested partitions.
///
/// # Safety
///
/// `result` must be a valid `delete_records` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_count(
    result: *const kafka_admin_DeleteRecordsResult_t,
) -> i32 {
    unsafe { delete_records_result_ref(result) }.topics.len() as i32
}

/// Returns the topic name of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `result` must be a valid `delete_records` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_get_topic(
    result: *const kafka_admin_DeleteRecordsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { delete_records_result_ref(result) }.topics, index)
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `delete_records` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_get_partition(
    result: *const kafka_admin_DeleteRecordsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { delete_records_result_ref(result) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the partition's low watermark after the deletion (Java's
/// `DeletedRecords.lowWatermark()`), or -1 if that partition failed (see
/// [`kafka_admin_DeleteRecordsResult_get_error`]) or `index` is out of range.
///
/// # Safety
///
/// `result` must be a valid `delete_records` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_get_low_watermark(
    result: *const kafka_admin_DeleteRecordsResult_t,
    index: i32,
) -> i64 {
    if index < 0 {
        return UNKNOWN_LOW_WATERMARK;
    }
    unsafe { delete_records_result_ref(result) }
        .low_watermarks
        .get(index as usize)
        .copied()
        .unwrap_or(UNKNOWN_LOW_WATERMARK)
}

/// Returns the error for the entry at `index` (borrowed), or null if that
/// partition succeeded or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `delete_records` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_get_error(
    result: *const kafka_admin_DeleteRecordsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { delete_records_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `delete_records` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `delete_records` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_destroy(result: *mut kafka_admin_DeleteRecordsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DeleteRecordsResultInner)) };
    }
}

// ---------------------------------------------------------------------------
// RPC submission helpers
//
// Each returns the `KafkaFuture` whose resolution yields the per-key outcomes
// for one RPC. The `Admin` method itself is called here, on the caller's thread,
// exactly as in Java.
// ---------------------------------------------------------------------------

/// Per-key outcomes of `createTopics`.
type CreateTopicsOutcomes = HashMap<String, Result<TopicMetadataAndConfig, KafkaError>>;
/// Per-key outcomes of `deleteTopics`, keyed by `K` (topic name or topic id).
type DeleteTopicsOutcomes<K> = HashMap<K, Result<(), KafkaError>>;
/// Per-key outcomes of `describeTopics`, keyed by `K` (topic name or topic id).
type DescribeTopicsOutcomes<K> = HashMap<K, Result<TopicDescription, KafkaError>>;
/// Per-topic outcomes of `createPartitions`.
type CreatePartitionsOutcomes = HashMap<String, Result<(), KafkaError>>;
/// Per-partition outcomes of `deleteRecords`.
type DeleteRecordsOutcomes = HashMap<TopicPartition, Result<DeletedRecords, KafkaError>>;

/// Submits `createTopics` and returns the collect-all future over its per-topic
/// futures.
fn submit_create_topics(
    admin: &dyn Admin,
    new_topics: &[NewTopic],
    options: CreateTopicsOptions,
) -> KafkaFuture<CreateTopicsOutcomes> {
    let result = admin.create_topics(new_topics, options);
    let entries: Vec<(String, KafkaFuture<TopicMetadataAndConfig>)> =
        result.futures().iter().map(|(name, f)| (name.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Submits `deleteTopics(TopicCollection.ofTopicNames(...))`.
fn submit_delete_topics_by_names(
    admin: &dyn Admin,
    names: Vec<String>,
    options: DeleteTopicsOptions,
) -> Result<KafkaFuture<DeleteTopicsOutcomes<String>>, KafkaError> {
    let result = admin.delete_topics(TopicCollection::of_topic_names(names), options);
    let values = result
        .topic_name_values()
        .ok_or_else(|| KafkaError::illegal_state("deleteTopics(ofTopicNames) did not return name-keyed futures"))?;
    let entries: Vec<(String, KafkaFuture<()>)> = values.iter().map(|(name, f)| (name.clone(), f.clone())).collect();
    Ok(KafkaFuture::join_map_results(entries))
}

/// Submits `deleteTopics(TopicCollection.ofTopicIds(...))`.
fn submit_delete_topics_by_ids(
    admin: &dyn Admin,
    ids: Vec<Uuid>,
    options: DeleteTopicsOptions,
) -> Result<KafkaFuture<DeleteTopicsOutcomes<Uuid>>, KafkaError> {
    let result = admin.delete_topics(TopicCollection::of_topic_ids(ids), options);
    let values = result
        .topic_id_values()
        .ok_or_else(|| KafkaError::illegal_state("deleteTopics(ofTopicIds) did not return id-keyed futures"))?;
    let entries: Vec<(Uuid, KafkaFuture<()>)> = values.iter().map(|(id, f)| (*id, f.clone())).collect();
    Ok(KafkaFuture::join_map_results(entries))
}

/// Submits `describeTopics(TopicCollection.ofTopicNames(...))`.
fn submit_describe_topics_by_names(
    admin: &dyn Admin,
    names: Vec<String>,
    options: DescribeTopicsOptions,
) -> Result<KafkaFuture<DescribeTopicsOutcomes<String>>, KafkaError> {
    let result = admin.describe_topics(TopicCollection::of_topic_names(names), options);
    let values = result
        .topic_name_values()
        .ok_or_else(|| KafkaError::illegal_state("describeTopics(ofTopicNames) did not return name-keyed futures"))?;
    let entries: Vec<(String, KafkaFuture<TopicDescription>)> =
        values.iter().map(|(name, f)| (name.clone(), f.clone())).collect();
    Ok(KafkaFuture::join_map_results(entries))
}

/// Submits `createPartitions` and returns the collect-all future over its
/// per-topic futures.
fn submit_create_partitions(
    admin: &dyn Admin,
    new_partitions: &HashMap<String, NewPartitions>,
    options: CreatePartitionsOptions,
) -> KafkaFuture<CreatePartitionsOutcomes> {
    let result = admin.create_partitions(new_partitions, options);
    let entries: Vec<(String, KafkaFuture<()>)> =
        result.values().iter().map(|(name, f)| (name.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Submits `deleteRecords` and returns the collect-all future over its
/// per-partition futures.
fn submit_delete_records(
    admin: &dyn Admin,
    records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
    options: DeleteRecordsOptions,
) -> KafkaFuture<DeleteRecordsOutcomes> {
    let result = admin.delete_records(records_to_delete, options);
    let entries: Vec<(TopicPartition, KafkaFuture<DeletedRecords>)> =
        result.low_watermarks().iter().map(|(tp, f)| (tp.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Submits `describeTopics(TopicCollection.ofTopicIds(...))`.
fn submit_describe_topics_by_ids(
    admin: &dyn Admin,
    ids: Vec<Uuid>,
    options: DescribeTopicsOptions,
) -> Result<KafkaFuture<DescribeTopicsOutcomes<Uuid>>, KafkaError> {
    let result = admin.describe_topics(TopicCollection::of_topic_ids(ids), options);
    let values = result
        .topic_id_values()
        .ok_or_else(|| KafkaError::illegal_state("describeTopics(ofTopicIds) did not return id-keyed futures"))?;
    let entries: Vec<(Uuid, KafkaFuture<TopicDescription>)> = values.iter().map(|(id, f)| (*id, f.clone())).collect();
    Ok(KafkaFuture::join_map_results(entries))
}

/// Writes a boxed result handle to `out_result` and returns null, or returns the
/// boxed error and leaves `*out_result` untouched — the ownership contract every
/// sync FFI entry point in this crate follows.
///
/// A null `out_result` means the caller does not want the result, so the handle
/// is never built. (It must not be built and then freed here: `R` is the opaque
/// `#[repr(C)]` marker type, not the inner state, so there is no way to drop it
/// correctly from this generic context.)
///
/// # Safety
///
/// `out_result` must be null or a valid, writable pointer.
unsafe fn finish_sync<T, R>(
    outcome: Result<T, KafkaError>,
    out_result: *mut *mut R,
    box_result: impl FnOnce(T) -> *mut R,
) -> *mut kafka_common_KafkaError_t {
    match outcome {
        Ok(value) => {
            if !out_result.is_null() {
                unsafe { *out_result = box_result(value) };
            }
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

// ---------------------------------------------------------------------------
// createTopics
// ---------------------------------------------------------------------------

/// Builds `CreateTopicsOptions` from the flat C option parameters.
///
/// Java passes an options object; C passes the fields, mirroring how the
/// consumer FFI passes `CloseOptions`' timeout as a scalar. A negative
/// `timeout_ms` leaves `timeoutMs` unset so `default.api.timeout.ms` applies.
fn create_topics_options(timeout_ms: i32, validate_only: bool, retry_on_quota_violation: bool) -> CreateTopicsOptions {
    CreateTopicsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .validate_only(validate_only)
        .retry_on_quota_violation(retry_on_quota_violation)
}

/// Creates topics and blocks until every per-topic future has resolved
/// (synchronous).
///
/// On success writes a [`kafka_admin_CreateTopicsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_CreateTopicsResult_destroy`]) and returns null.
/// **A per-topic failure is not a call failure**: it is reported by
/// [`kafka_admin_CreateTopicsResult_get_error`] for that key, so a partially
/// failed batch still returns null here with a non-null result handle. A non-null
/// return means the request could not be submitted at all.
///
/// # Parameters
///
/// - `topics`: array of `count` [`kafka_admin_NewTopic_t`] handles; the caller
///   retains ownership of them.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `validate_only`: `CreateTopicsOptions.validateOnly` — validate without
///   creating.
/// - `retry_on_quota_violation`:
///   `CreateTopicsOptions.retryOnQuotaViolation`.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics` must have `count` valid entries;
/// `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create_topics(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const kafka_admin_NewTopic_t,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    retry_on_quota_violation: bool,
    out_result: *mut *mut kafka_admin_CreateTopicsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let new_topics = unsafe { read_new_topics(topics, count) };
    let options = create_topics_options(timeout_ms, validate_only, retry_on_quota_violation);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_create_topics(a, &new_topics, options))) };
    unsafe { finish_sync(outcome, out_result, box_create_topics_result) }
}

/// Completion callback for [`kafka_admin_AdminClient_create_topics_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_CreateTopicsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-topic failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_create_topics_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_CreateTopicsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Creates topics asynchronously. See [`kafka_admin_AdminClient_create_topics`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics` must have `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create_topics_async(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const kafka_admin_NewTopic_t,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    retry_on_quota_violation: bool,
    callback: kafka_admin_AdminClient_create_topics_callback_t,
    user_data: *mut c_void,
) {
    let new_topics = unsafe { read_new_topics(topics, count) };
    let options = create_topics_options(timeout_ms, validate_only, retry_on_quota_violation);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_create_topics(a, &new_topics, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_create_topics_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// deleteTopics
// ---------------------------------------------------------------------------

/// Builds `DeleteTopicsOptions` from the flat C option parameters.
fn delete_topics_options(timeout_ms: i32, retry_on_quota_violation: bool) -> DeleteTopicsOptions {
    DeleteTopicsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .retry_on_quota_violation(retry_on_quota_violation)
}

/// Completion callback for the `delete_topics` async entry points.
///
/// Shared by the by-names and by-ids variants (they are one Java method,
/// `deleteTopics(TopicCollection)`, and produce the same result shape). Exactly
/// one of `result` / `error` is non-null and the callback owns it.
pub type kafka_admin_AdminClient_delete_topics_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DeleteTopicsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Deletes topics **by name** and blocks until every per-topic future has
/// resolved (synchronous).
///
/// `TopicCollection` is topic-names xor topic-ids; rather than a runtime-checked
/// discriminated input struct, the two forms get separate entry points so the
/// invariant cannot be violated (`admin-client.md` §5). This is
/// `deleteTopics(TopicCollection.ofTopicNames(names))`.
///
/// On success writes a [`kafka_admin_DeleteTopicsResult_t`] to `*out_result`
/// (free with [`kafka_admin_DeleteTopicsResult_destroy`]) and returns null.
/// Per-topic failures are reported by
/// [`kafka_admin_DeleteTopicsResult_get_error`], not by the return value.
///
/// # Safety
///
/// `admin` must be a valid handle; `names` must have `count` valid C strings;
/// `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_topics(
    admin: *const kafka_admin_AdminClient_t,
    names: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    retry_on_quota_violation: bool,
    out_result: *mut *mut kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let topic_names = unsafe { read_strings(names, count) };
    let options = delete_topics_options(timeout_ms, retry_on_quota_violation);
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| submit_delete_topics_by_names(a, topic_names, options)) };
    unsafe { finish_sync(outcome, out_result, |o| box_delete_topics_result(o, String::clone)) }
}

/// Deletes topics **by name** asynchronously. See
/// [`kafka_admin_AdminClient_delete_topics`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `names` must have `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_topics_async(
    admin: *const kafka_admin_AdminClient_t,
    names: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    retry_on_quota_violation: bool,
    callback: kafka_admin_AdminClient_delete_topics_callback_t,
    user_data: *mut c_void,
) {
    let topic_names = unsafe { read_strings(names, count) };
    let options = delete_topics_options(timeout_ms, retry_on_quota_violation);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_delete_topics_by_names(a, topic_names, options),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_delete_topics_result(outcomes, String::clone), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

/// Deletes topics **by id** and blocks until every per-topic future has resolved
/// (synchronous). This is `deleteTopics(TopicCollection.ofTopicIds(ids))`.
///
/// `topic_ids` are base64 topic-id strings (Java's `Uuid.toString()` form); an
/// unparseable or NULL id makes the whole call fail with an illegal-argument
/// error, mirroring Java's `Uuid.fromString`. Result keys are the same base64
/// strings.
///
/// # Safety
///
/// `admin` must be a valid handle; `topic_ids` must have `count` valid C
/// strings; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_topics_by_ids(
    admin: *const kafka_admin_AdminClient_t,
    topic_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    retry_on_quota_violation: bool,
    out_result: *mut *mut kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let ids = match unsafe { read_uuids(topic_ids, count) } {
        Ok(ids) => ids,
        Err(e) => return box_error(e),
    };
    let options = delete_topics_options(timeout_ms, retry_on_quota_violation);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| submit_delete_topics_by_ids(a, ids, options)) };
    unsafe { finish_sync(outcome, out_result, |o| box_delete_topics_result(o, Uuid::to_string)) }
}

/// Deletes topics **by id** asynchronously. See
/// [`kafka_admin_AdminClient_delete_topics_by_ids`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, or an unparseable or NULL
/// base64 topic id). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `topic_ids` must have `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_topics_by_ids_async(
    admin: *const kafka_admin_AdminClient_t,
    topic_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    retry_on_quota_violation: bool,
    callback: kafka_admin_AdminClient_delete_topics_callback_t,
    user_data: *mut c_void,
) {
    let parsed = unsafe { read_uuids(topic_ids, count) };
    let options = delete_topics_options(timeout_ms, retry_on_quota_violation);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_delete_topics_by_ids(a, parsed?, options),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_delete_topics_result(outcomes, Uuid::to_string), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// listTopics
// ---------------------------------------------------------------------------

/// Completion callback for [`kafka_admin_AdminClient_list_topics_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it.
pub type kafka_admin_AdminClient_list_topics_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_ListTopicsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Lists the cluster's topics (synchronous).
///
/// On success writes a [`kafka_admin_ListTopicsResult_t`] to `*out_result` (free
/// with [`kafka_admin_ListTopicsResult_destroy`]) and returns null. Unlike the
/// per-key RPCs, `listTopics` has a single future in Java, so any failure is a
/// call failure and is returned here.
///
/// # Parameters
///
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `list_internal`: `ListTopicsOptions.listInternal` — include internal topics
///   such as `__consumer_offsets`.
///
/// # Safety
///
/// `admin` must be a valid handle; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_topics(
    admin: *const kafka_admin_AdminClient_t,
    timeout_ms: i32,
    list_internal: bool,
    out_result: *mut *mut kafka_admin_ListTopicsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let options = ListTopicsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .list_internal(list_internal);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(a.list_topics(options).names_to_listings())) };
    unsafe { finish_sync(outcome, out_result, box_list_topics_result) }
}

/// Lists the cluster's topics asynchronously. See
/// [`kafka_admin_AdminClient_list_topics`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_topics_async(
    admin: *const kafka_admin_AdminClient_t,
    timeout_ms: i32,
    list_internal: bool,
    callback: kafka_admin_AdminClient_list_topics_callback_t,
    user_data: *mut c_void,
) {
    let options = ListTopicsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .list_internal(list_internal);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(a.list_topics(options).names_to_listings()),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(listings) => (box_list_topics_result(listings), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeTopics
// ---------------------------------------------------------------------------

/// Builds `DescribeTopicsOptions` from the flat C option parameters. A negative
/// `partition_size_limit_per_response` keeps Java's default (2000).
fn describe_topics_options(
    timeout_ms: i32,
    include_authorized_operations: bool,
    partition_size_limit_per_response: i32,
) -> DescribeTopicsOptions {
    let options = DescribeTopicsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .include_authorized_operations(include_authorized_operations);
    if partition_size_limit_per_response < 0 {
        options
    } else {
        options.partition_size_limit_per_response(partition_size_limit_per_response)
    }
}

/// Completion callback for the `describe_topics` async entry points.
///
/// Shared by the by-names and by-ids variants (one Java method,
/// `describeTopics(TopicCollection)`). Exactly one of `result` / `error` is
/// non-null and the callback owns it.
pub type kafka_admin_AdminClient_describe_topics_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeTopicsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Describes topics **by name** and blocks until every per-topic future has
/// resolved (synchronous). This is
/// `describeTopics(TopicCollection.ofTopicNames(names))`.
///
/// On success writes a [`kafka_admin_DescribeTopicsResult_t`] to `*out_result`
/// (free with [`kafka_admin_DescribeTopicsResult_destroy`]) and returns null.
/// Per-topic failures (e.g. `UNKNOWN_TOPIC_OR_PARTITION`) are reported by
/// [`kafka_admin_DescribeTopicsResult_get_error`], not by the return value.
///
/// # Safety
///
/// `admin` must be a valid handle; `names` must have `count` valid C strings;
/// `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_topics(
    admin: *const kafka_admin_AdminClient_t,
    names: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    partition_size_limit_per_response: i32,
    out_result: *mut *mut kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let topic_names = unsafe { read_strings(names, count) };
    let options = describe_topics_options(timeout_ms, include_authorized_operations, partition_size_limit_per_response);
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| submit_describe_topics_by_names(a, topic_names, options)) };
    unsafe { finish_sync(outcome, out_result, |o| box_describe_topics_result(o, String::clone)) }
}

/// Describes topics **by name** asynchronously. See
/// [`kafka_admin_AdminClient_describe_topics`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `names` must have `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_topics_async(
    admin: *const kafka_admin_AdminClient_t,
    names: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    partition_size_limit_per_response: i32,
    callback: kafka_admin_AdminClient_describe_topics_callback_t,
    user_data: *mut c_void,
) {
    let topic_names = unsafe { read_strings(names, count) };
    let options = describe_topics_options(timeout_ms, include_authorized_operations, partition_size_limit_per_response);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_describe_topics_by_names(a, topic_names, options),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_describe_topics_result(outcomes, String::clone), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

/// Describes topics **by id** and blocks until every per-topic future has
/// resolved (synchronous). This is
/// `describeTopics(TopicCollection.ofTopicIds(ids))`.
///
/// `topic_ids` are base64 topic-id strings (Java's `Uuid.toString()` form); an
/// unparseable or NULL id makes the whole call fail with an illegal-argument
/// error. Result keys are the same base64 strings.
///
/// # Safety
///
/// `admin` must be a valid handle; `topic_ids` must have `count` valid C
/// strings; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_topics_by_ids(
    admin: *const kafka_admin_AdminClient_t,
    topic_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    partition_size_limit_per_response: i32,
    out_result: *mut *mut kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let ids = match unsafe { read_uuids(topic_ids, count) } {
        Ok(ids) => ids,
        Err(e) => return box_error(e),
    };
    let options = describe_topics_options(timeout_ms, include_authorized_operations, partition_size_limit_per_response);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| submit_describe_topics_by_ids(a, ids, options)) };
    unsafe { finish_sync(outcome, out_result, |o| box_describe_topics_result(o, Uuid::to_string)) }
}

/// Describes topics **by id** asynchronously. See
/// [`kafka_admin_AdminClient_describe_topics_by_ids`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, or an unparseable or NULL
/// base64 topic id). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `topic_ids` must have `count` valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_topics_by_ids_async(
    admin: *const kafka_admin_AdminClient_t,
    topic_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    partition_size_limit_per_response: i32,
    callback: kafka_admin_AdminClient_describe_topics_callback_t,
    user_data: *mut c_void,
) {
    let parsed = unsafe { read_uuids(topic_ids, count) };
    let options = describe_topics_options(timeout_ms, include_authorized_operations, partition_size_limit_per_response);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_describe_topics_by_ids(a, parsed?, options),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_describe_topics_result(outcomes, Uuid::to_string), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// createPartitions
// ---------------------------------------------------------------------------

/// Builds `CreatePartitionsOptions` from the flat C option parameters. A negative
/// `timeout_ms` leaves `timeoutMs` unset so `default.api.timeout.ms` applies.
fn create_partitions_options(
    timeout_ms: i32,
    validate_only: bool,
    retry_on_quota_violation: bool,
) -> CreatePartitionsOptions {
    CreatePartitionsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .validate_only(validate_only)
        .retry_on_quota_violation(retry_on_quota_violation)
}

/// Completion callback for [`kafka_admin_AdminClient_create_partitions_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_CreatePartitionsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-topic failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_create_partitions_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_CreatePartitionsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Increases the partition count of the given topics, blocking until every
/// per-topic future has resolved (synchronous).
///
/// This is `createPartitions(Map<String, NewPartitions>, CreatePartitionsOptions)`.
/// Java's map becomes two parallel arrays: entry `i` pairs `topics[i]` with
/// `new_partitions[i]`. A pair is skipped when either side is NULL, so the arrays
/// cannot drift out of step.
///
/// On success writes a [`kafka_admin_CreatePartitionsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_CreatePartitionsResult_destroy`]) and returns
/// null. **A per-topic failure is not a call failure**: it is reported by
/// [`kafka_admin_CreatePartitionsResult_get_error`] for that key, so a partially
/// failed batch still returns null here with a non-null result handle. A non-null
/// return means the request could not be submitted at all.
///
/// # Parameters
///
/// - `topics` / `new_partitions`: parallel arrays of `count` entries; the caller
///   retains ownership of the [`kafka_admin_NewPartitions_t`] handles.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `validate_only`: `CreatePartitionsOptions.validateOnly` — validate without
///   creating the partitions.
/// - `retry_on_quota_violation`:
///   `CreatePartitionsOptions.retryOnQuotaViolation`.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics` and `new_partitions` must have
/// `count` valid entries each; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create_partitions(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    new_partitions: *const *const kafka_admin_NewPartitions_t,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    retry_on_quota_violation: bool,
    out_result: *mut *mut kafka_admin_CreatePartitionsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let specs = unsafe { read_new_partitions(topics, new_partitions, count) };
    let options = create_partitions_options(timeout_ms, validate_only, retry_on_quota_violation);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_create_partitions(a, &specs, options))) };
    unsafe { finish_sync(outcome, out_result, box_create_partitions_result) }
}

/// Increases the partition count of the given topics asynchronously. See
/// [`kafka_admin_AdminClient_create_partitions`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics` and `new_partitions` must have
/// `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create_partitions_async(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    new_partitions: *const *const kafka_admin_NewPartitions_t,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    retry_on_quota_violation: bool,
    callback: kafka_admin_AdminClient_create_partitions_callback_t,
    user_data: *mut c_void,
) {
    let specs = unsafe { read_new_partitions(topics, new_partitions, count) };
    let options = create_partitions_options(timeout_ms, validate_only, retry_on_quota_violation);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_create_partitions(a, &specs, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_create_partitions_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// deleteRecords
// ---------------------------------------------------------------------------

/// Completion callback for [`kafka_admin_AdminClient_delete_records_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DeleteRecordsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-partition failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_delete_records_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DeleteRecordsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Deletes the records before the given offset of each partition, blocking until
/// every per-partition future has resolved (synchronous).
///
/// This is `deleteRecords(Map<TopicPartition, RecordsToDelete>,
/// DeleteRecordsOptions)`. Java's map becomes three parallel arrays: entry `i` is
/// `(topics[i], partitions[i]) -> RecordsToDelete.beforeOffset(
/// before_offsets[i])`. `RecordsToDelete` carries only that offset, so it needs
/// no input handle. An entry with a NULL topic is skipped. Pass `-1` as a
/// `before_offsets` entry to truncate that partition to its high watermark
/// (Java's documented `RecordsToDelete.beforeOffset(-1)` behavior).
///
/// On success writes a [`kafka_admin_DeleteRecordsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_DeleteRecordsResult_destroy`]) and returns null.
/// Per-partition failures are reported by
/// [`kafka_admin_DeleteRecordsResult_get_error`], not by the return value.
///
/// # Parameters
///
/// - `topics` / `partitions` / `before_offsets`: parallel arrays of `count`
///   entries.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///   `DeleteRecordsOptions` has no other field in Java.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics`, `partitions` and `before_offsets`
/// must have `count` valid entries each; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_records(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    before_offsets: *const i64,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DeleteRecordsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let records = unsafe { read_records_to_delete(topics, partitions, before_offsets, count) };
    let options = DeleteRecordsOptions::new().timeout_ms(option_timeout(timeout_ms));
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_delete_records(a, &records, options))) };
    unsafe { finish_sync(outcome, out_result, box_delete_records_result) }
}

/// Deletes records asynchronously. See
/// [`kafka_admin_AdminClient_delete_records`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics`, `partitions` and `before_offsets`
/// must have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_records_async(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    before_offsets: *const i64,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_delete_records_callback_t,
    user_data: *mut c_void,
) {
    let records = unsafe { read_records_to_delete(topics, partitions, before_offsets, count) };
    let options = DeleteRecordsOptions::new().timeout_ms(option_timeout(timeout_ms));
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_delete_records(a, &records, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_delete_records_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// Cluster / configs / log-dir marshaling helpers
// ---------------------------------------------------------------------------

/// Reads `count` 32-bit integers into an owned vector.
///
/// # Safety
///
/// `values` must be null or have `count` readable entries.
unsafe fn read_i32s(values: *const i32, count: i32) -> Vec<i32> {
    let n = count.max(0) as usize;
    if values.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(unsafe { *values.add(i) });
    }
    out
}

/// Reads `count` `(type_code, name)` pairs into [`ConfigResource`]s, skipping
/// entries whose name is NULL so the two arrays cannot drift out of step.
///
/// `type_codes` hold Java's `ConfigResource.Type.id()` values; an unrecognized
/// code becomes `ConfigResource.Type.UNKNOWN`, exactly as Java's
/// `ConfigResource.Type.forId` does.
///
/// # Safety
///
/// `type_codes` and `names` must be null or have `count` readable entries each,
/// every name NULL or a valid C string.
unsafe fn read_config_resources(
    type_codes: *const i32,
    names: *const *const c_char,
    count: i32,
) -> Vec<ConfigResource> {
    let n = count.max(0) as usize;
    if type_codes.is_null() || names.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let name_ptr = unsafe { *names.add(i) };
        if name_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        let resource_type = ConfigResourceType::for_id(enum_code_or_unknown(unsafe { *type_codes.add(i) }));
        out.push(ConfigResource::new(resource_type, name));
    }
    out
}

/// Reads `count` `(topic, partition, broker_id)` triples into
/// [`TopicPartitionReplica`]s, skipping entries whose topic is NULL.
///
/// # Safety
///
/// `topics`, `partitions` and `broker_ids` must be null or have `count` readable
/// entries each, every topic NULL or a valid C string.
unsafe fn read_replicas(
    topics: *const *const c_char,
    partitions: *const i32,
    broker_ids: *const i32,
    count: i32,
) -> Vec<TopicPartitionReplica> {
    let n = count.max(0) as usize;
    if topics.is_null() || partitions.is_null() || broker_ids.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        if topic_ptr.is_null() {
            continue;
        }
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        out.push(TopicPartitionReplica::new(topic, unsafe { *partitions.add(i) }, unsafe {
            *broker_ids.add(i)
        }));
    }
    out
}

/// Reads the flat `incrementalAlterConfigs` rows into Java's
/// `Map<ConfigResource, Collection<AlterConfigOp>>`.
///
/// Java's nested map becomes five parallel arrays, one row per *operation*:
/// row `i` applies `(config_names[i] -> config_values[i], op_types[i])` to the
/// resource `(resource_type_codes[i], resource_names[i])`. Rows for the same
/// resource are grouped, keeping their relative order (Java applies a
/// resource's ops in iteration order). A row with a NULL resource name or a NULL
/// config name is skipped; a NULL *value* is meaningful and becomes Java's null
/// value (which is what `DELETE` sends).
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if an op-type code is not one of
/// `AlterConfigOp.OpType.id()`.
///
/// # Safety
///
/// Every array must be null or have `count` readable entries.
unsafe fn read_alter_config_ops(
    resource_type_codes: *const i32,
    resource_names: *const *const c_char,
    config_names: *const *const c_char,
    config_values: *const *const c_char,
    op_type_codes: *const i32,
    count: i32,
) -> Result<HashMap<ConfigResource, Vec<AlterConfigOp>>, KafkaError> {
    let n = count.max(0) as usize;
    let mut out: HashMap<ConfigResource, Vec<AlterConfigOp>> = HashMap::new();
    if resource_type_codes.is_null() || resource_names.is_null() || config_names.is_null() || op_type_codes.is_null() {
        return Ok(out);
    }
    for i in 0..n {
        let resource_name_ptr = unsafe { *resource_names.add(i) };
        let config_name_ptr = unsafe { *config_names.add(i) };
        if resource_name_ptr.is_null() || config_name_ptr.is_null() {
            continue;
        }
        let op_code = unsafe { *op_type_codes.add(i) };
        // `OpType` has no UNKNOWN member, so a code that does not narrow to an
        // `int8_t` is rejected rather than folded onto a valid op (see
        // `narrow_enum_code`).
        let op_type = narrow_enum_code(op_code).and_then(OpType::for_id).ok_or_else(|| {
            KafkaError::illegal_argument(format!("unknown AlterConfigOp op type id {op_code} at index {i}"))
        })?;
        let resource_type = ConfigResourceType::for_id(enum_code_or_unknown(unsafe { *resource_type_codes.add(i) }));
        let resource_name = unsafe { CStr::from_ptr(resource_name_ptr) }.to_string_lossy().to_string();
        let config_name = unsafe { CStr::from_ptr(config_name_ptr) }.to_string_lossy().to_string();
        let value = if config_values.is_null() {
            None
        } else {
            let value_ptr = unsafe { *config_values.add(i) };
            if value_ptr.is_null() {
                None
            } else {
                Some(unsafe { CStr::from_ptr(value_ptr) }.to_string_lossy().to_string())
            }
        };
        out.entry(ConfigResource::new(resource_type, resource_name))
            .or_default()
            .push(AlterConfigOp::new(ConfigEntry::new(config_name, value), op_type));
    }
    Ok(out)
}

/// Sorts a per-`ConfigResource` outcome map by `(type id, name)`.
///
/// [`ConfigResource`] is not `Ord` (Java's map is unordered too), but C
/// addresses entries by index, so the order must be reproducible.
fn sorted_config_resource_entries<V>(map: HashMap<ConfigResource, V>) -> Vec<(ConfigResource, V)> {
    let mut entries: Vec<(ConfigResource, V)> = map.into_iter().collect();
    entries.sort_by(|a, b| {
        a.0.resource_type()
            .id()
            .cmp(&b.0.resource_type().id())
            .then_with(|| a.0.name().cmp(b.0.name()))
    });
    entries
}

/// Sorts a per-[`TopicPartitionReplica`] outcome map by
/// `(topic, partition, broker id)`, for the same reason.
fn sorted_replica_entries<V>(map: HashMap<TopicPartitionReplica, V>) -> Vec<(TopicPartitionReplica, V)> {
    let mut entries: Vec<(TopicPartitionReplica, V)> = map.into_iter().collect();
    entries.sort_by(|a, b| {
        a.0.topic()
            .cmp(b.0.topic())
            .then_with(|| a.0.partition().cmp(&b.0.partition()))
            .then_with(|| a.0.broker_id().cmp(&b.0.broker_id()))
    });
    entries
}

// ---------------------------------------------------------------------------
// Log-dir value types
// ---------------------------------------------------------------------------

/// Reported for a log dir whose total/usable size the broker did not send.
/// Mirrors `DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES`, which is what Java's
/// `LogDirDescription.totalBytes()` reports as an empty `OptionalLong`.
const UNKNOWN_VOLUME_BYTES: i64 = -1;

/// One `(TopicPartition, ReplicaInfo)` pair of a log dir, flattened for C.
///
/// Java's `LogDirDescription.replicaInfos()` is a map; C reads it as indexed
/// accessors on the owning [`kafka_admin_LogDirDescription_t`], the same shape
/// [`kafka_admin_DeleteRecordsResult_t`] uses for its `TopicPartition` keys.
struct ReplicaInfoC {
    topic_c: CString,
    partition: i32,
    size: i64,
    offset_lag: i64,
    is_future: bool,
}

/// Opaque handle to a `LogDirDescription`.
#[repr(C)]
pub struct kafka_admin_LogDirDescription_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_LogDirDescription_t`].
struct LogDirDescriptionInner {
    /// Java's `LogDirDescription.error()`: a per-log-dir error, distinct from
    /// the per-broker error of the enclosing future.
    error: Option<KafkaErrorInner>,
    total_bytes: i64,
    usable_bytes: i64,
    replicas: Vec<ReplicaInfoC>,
}

impl LogDirDescriptionInner {
    fn new(description: &LogDirDescription) -> Self {
        let mut replicas: Vec<ReplicaInfoC> = description
            .replica_infos()
            .iter()
            .map(|(tp, info)| ReplicaInfoC {
                topic_c: to_cstring(tp.topic()),
                partition: tp.partition(),
                size: info.size(),
                offset_lag: info.offset_lag(),
                is_future: info.is_future(),
            })
            .collect();
        // `replicaInfos()` is an unordered map in Java; C indexes it.
        replicas.sort_by(|a, b| a.topic_c.cmp(&b.topic_c).then_with(|| a.partition.cmp(&b.partition)));
        Self {
            error: description.error().cloned().map(error_inner),
            total_bytes: description.total_bytes().unwrap_or(UNKNOWN_VOLUME_BYTES),
            usable_bytes: description.usable_bytes().unwrap_or(UNKNOWN_VOLUME_BYTES),
            replicas,
        }
    }
}

/// Casts a `*const kafka_admin_LogDirDescription_t` to a reference.
///
/// # Safety
///
/// `description` must be a non-null borrowed pointer from a
/// [`kafka_admin_LogDirDescriptionMap_t`] getter.
unsafe fn log_dir_ref(description: *const kafka_admin_LogDirDescription_t) -> &'static LogDirDescriptionInner {
    unsafe { &*(description as *const LogDirDescriptionInner) }
}

/// Returns the log dir's own error (borrowed), or null if it reported none.
///
/// This is Java's `LogDirDescription.error()`. It is *not* the per-broker error
/// from [`kafka_admin_DescribeLogDirsResult_get_error`]: the broker answered, but
/// this particular directory is offline or unreadable. Do not destroy it.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_error(
    description: *const kafka_admin_LogDirDescription_t,
) -> *const kafka_common_KafkaError_t {
    error_ptr(unsafe { log_dir_ref(description) }.error.as_ref())
}

/// Returns the total size in bytes of the volume the log dir is on, or -1 if the
/// broker did not report it (Java's empty `OptionalLong`).
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_total_bytes(
    description: *const kafka_admin_LogDirDescription_t,
) -> i64 {
    unsafe { log_dir_ref(description) }.total_bytes
}

/// Returns the usable size in bytes of the volume the log dir is on, or -1 if
/// the broker did not report it.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_usable_bytes(
    description: *const kafka_admin_LogDirDescription_t,
) -> i64 {
    unsafe { log_dir_ref(description) }.usable_bytes
}

/// Returns the number of replicas hosted in this log dir.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_replica_count(
    description: *const kafka_admin_LogDirDescription_t,
) -> i32 {
    unsafe { log_dir_ref(description) }.replicas.len() as i32
}

/// Returns the topic of the replica at `index` (borrowed), or null if out of
/// range. Replicas are sorted by `(topic, partition)`.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_replica_topic(
    description: *const kafka_admin_LogDirDescription_t,
    index: i32,
) -> *const c_char {
    match replica_info_at(unsafe { log_dir_ref(description) }, index) {
        Some(replica) => replica.topic_c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the partition of the replica at `index`, or -1 if out of range.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_replica_partition(
    description: *const kafka_admin_LogDirDescription_t,
    index: i32,
) -> i32 {
    match replica_info_at(unsafe { log_dir_ref(description) }, index) {
        Some(replica) => replica.partition,
        None => -1,
    }
}

/// Returns the on-disk size in bytes of the replica at `index`, or -1 if out of
/// range.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_replica_size(
    description: *const kafka_admin_LogDirDescription_t,
    index: i32,
) -> i64 {
    match replica_info_at(unsafe { log_dir_ref(description) }, index) {
        Some(replica) => replica.size,
        None => -1,
    }
}

/// Returns the offset lag of the replica at `index`, or -1 if out of range.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_replica_offset_lag(
    description: *const kafka_admin_LogDirDescription_t,
    index: i32,
) -> i64 {
    match replica_info_at(unsafe { log_dir_ref(description) }, index) {
        Some(replica) => replica.offset_lag,
        None => -1,
    }
}

/// Returns whether the replica at `index` is a *future* replica (one being moved
/// into this log dir). False if out of range.
///
/// # Safety
///
/// `description` must be a valid borrowed pointer from a log-dir map getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_replica_is_future(
    description: *const kafka_admin_LogDirDescription_t,
    index: i32,
) -> bool {
    match replica_info_at(unsafe { log_dir_ref(description) }, index) {
        Some(replica) => replica.is_future,
        None => false,
    }
}

/// Returns the `index`th replica of `description`, or `None` if out of range.
fn replica_info_at(description: &LogDirDescriptionInner, index: i32) -> Option<&ReplicaInfoC> {
    if index < 0 {
        return None;
    }
    description.replicas.get(index as usize)
}

/// Opaque handle to one broker's `Map<String, LogDirDescription>`.
#[repr(C)]
pub struct kafka_admin_LogDirDescriptionMap_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_LogDirDescriptionMap_t`].
struct LogDirDescriptionMapInner {
    log_dirs: Vec<CString>,
    descriptions: Vec<LogDirDescriptionInner>,
}

impl LogDirDescriptionMapInner {
    fn new(map: &HashMap<String, LogDirDescription>) -> Self {
        let mut entries: Vec<(&String, &LogDirDescription)> = map.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let mut log_dirs = Vec::with_capacity(entries.len());
        let mut descriptions = Vec::with_capacity(entries.len());
        for (name, description) in entries {
            log_dirs.push(to_cstring(name));
            descriptions.push(LogDirDescriptionInner::new(description));
        }
        Self { log_dirs, descriptions }
    }
}

/// Casts a `*const kafka_admin_LogDirDescriptionMap_t` to a reference.
///
/// # Safety
///
/// `map` must be a non-null borrowed pointer from a `describe_log_dirs` result
/// getter.
unsafe fn log_dir_map_ref(map: *const kafka_admin_LogDirDescriptionMap_t) -> &'static LogDirDescriptionMapInner {
    unsafe { &*(map as *const LogDirDescriptionMapInner) }
}

/// Returns the number of log dirs reported by this broker.
///
/// # Safety
///
/// `map` must be a valid borrowed pointer from a `describe_log_dirs` result
/// getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescriptionMap_count(map: *const kafka_admin_LogDirDescriptionMap_t) -> i32 {
    unsafe { log_dir_map_ref(map) }.log_dirs.len() as i32
}

/// Returns the log-dir path at `index` (borrowed), or null if out of range.
/// Entries are sorted by path.
///
/// # Safety
///
/// `map` must be a valid borrowed pointer from a `describe_log_dirs` result
/// getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescriptionMap_get_key(
    map: *const kafka_admin_LogDirDescriptionMap_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { log_dir_map_ref(map) }.log_dirs, index)
}

/// Returns the description of the log dir at `index` (borrowed), or null if out
/// of range.
///
/// # Safety
///
/// `map` must be a valid borrowed pointer from a `describe_log_dirs` result
/// getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescriptionMap_get_value(
    map: *const kafka_admin_LogDirDescriptionMap_t,
    index: i32,
) -> *const kafka_admin_LogDirDescription_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { log_dir_map_ref(map) }.descriptions.get(index as usize) {
        Some(description) => description as *const LogDirDescriptionInner as *const kafka_admin_LogDirDescription_t,
        None => std::ptr::null(),
    }
}

/// Opaque handle to a `DescribeReplicaLogDirsResult.ReplicaLogDirInfo`.
#[repr(C)]
pub struct kafka_admin_ReplicaLogDirInfo_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ReplicaLogDirInfo_t`].
struct ReplicaLogDirInfoInner {
    /// `None` when no replica of this partition is found on the broker (Java
    /// returns null).
    current_log_dir_c: Option<CString>,
    current_offset_lag: i64,
    /// `None` when the replica is not being moved (Java returns null).
    future_log_dir_c: Option<CString>,
    future_offset_lag: i64,
}

impl ReplicaLogDirInfoInner {
    fn new(info: &ReplicaLogDirInfo) -> Self {
        Self {
            current_log_dir_c: info.current_replica_log_dir().map(to_cstring),
            current_offset_lag: info.current_replica_offset_lag(),
            future_log_dir_c: info.future_replica_log_dir().map(to_cstring),
            future_offset_lag: info.future_replica_offset_lag(),
        }
    }
}

/// Casts a `*const kafka_admin_ReplicaLogDirInfo_t` to a reference.
///
/// # Safety
///
/// `info` must be a non-null borrowed pointer from a result-handle getter.
unsafe fn replica_log_dir_info_ref(info: *const kafka_admin_ReplicaLogDirInfo_t) -> &'static ReplicaLogDirInfoInner {
    unsafe { &*(info as *const ReplicaLogDirInfoInner) }
}

/// Returns the replica's current log dir (borrowed), or null if the broker hosts
/// no replica of that partition.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaLogDirInfo_current_replica_log_dir(
    info: *const kafka_admin_ReplicaLogDirInfo_t,
) -> *const c_char {
    match &unsafe { replica_log_dir_info_ref(info) }.current_log_dir_c {
        Some(dir) => dir.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns `max(partition high watermark - replica log end offset, 0)` for the
/// current replica.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaLogDirInfo_current_replica_offset_lag(
    info: *const kafka_admin_ReplicaLogDirInfo_t,
) -> i64 {
    unsafe { replica_log_dir_info_ref(info) }.current_offset_lag
}

/// Returns the log dir the replica is being moved to (borrowed), or null if it
/// is not being moved.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaLogDirInfo_future_replica_log_dir(
    info: *const kafka_admin_ReplicaLogDirInfo_t,
) -> *const c_char {
    match &unsafe { replica_log_dir_info_ref(info) }.future_log_dir_c {
        Some(dir) => dir.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns `max(partition high watermark - future replica log end offset, 0)`.
///
/// # Safety
///
/// `info` must be a valid borrowed pointer from a result-handle getter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaLogDirInfo_future_replica_offset_lag(
    info: *const kafka_admin_ReplicaLogDirInfo_t,
) -> i64 {
    unsafe { replica_log_dir_info_ref(info) }.future_offset_lag
}

// ---------------------------------------------------------------------------
// describeCluster
// ---------------------------------------------------------------------------

/// The four independently-completable attributes of Java's
/// `DescribeClusterResult`, resolved into one value for the C handle.
struct DescribeClusterOutcome {
    nodes: Vec<Node>,
    controller: Option<Node>,
    cluster_id: String,
    /// `None` when the operations were not requested or the broker omitted them
    /// (Java returns null).
    authorized_operations: Option<BTreeSet<AclOperation>>,
}

/// Opaque handle to a `DescribeClusterResult`.
#[repr(C)]
pub struct kafka_admin_DescribeClusterResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeClusterResult_t`].
///
/// Unlike the per-key RPCs there is no key/value/error triple: Java's result is
/// four attributes of one cluster, not a map, so the handle exposes them
/// directly and any failure is a whole-call failure.
struct DescribeClusterResultInner {
    cluster_id_c: CString,
    nodes: Vec<Node>,
    controller: Option<Node>,
    /// `None` maps to a count of -1 (absent), matching how
    /// [`kafka_admin_TopicPartitionInfo_elr_count`] reports an absent set.
    authorized_operations: Option<Vec<i32>>,
}

/// Flattens the cluster description into the C handle.
fn box_describe_cluster_result(outcome: DescribeClusterOutcome) -> *mut kafka_admin_DescribeClusterResult_t {
    let inner = DescribeClusterResultInner {
        cluster_id_c: to_cstring(&outcome.cluster_id),
        nodes: outcome.nodes,
        controller: outcome.controller,
        authorized_operations: outcome
            .authorized_operations
            .map(|ops| ops.iter().map(|op| i32::from(op.code())).collect()),
    };
    Box::into_raw(Box::new(inner)) as *mut kafka_admin_DescribeClusterResult_t
}

/// Casts a `*const kafka_admin_DescribeClusterResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_cluster` call.
unsafe fn describe_cluster_result_ref(
    result: *const kafka_admin_DescribeClusterResult_t,
) -> &'static DescribeClusterResultInner {
    unsafe { &*(result as *const DescribeClusterResultInner) }
}

/// Returns the cluster id (borrowed).
///
/// # Safety
///
/// `result` must be a valid `describe_cluster` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_cluster_id(
    result: *const kafka_admin_DescribeClusterResult_t,
) -> *const c_char {
    unsafe { describe_cluster_result_ref(result) }.cluster_id_c.as_ptr()
}

/// Returns the number of nodes in the cluster.
///
/// # Safety
///
/// `result` must be a valid `describe_cluster` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_node_count(
    result: *const kafka_admin_DescribeClusterResult_t,
) -> i32 {
    unsafe { describe_cluster_result_ref(result) }.nodes.len() as i32
}

/// Returns the node at `index` (borrowed), or null if out of range. Read it with
/// the `kafka_common_Node_*` accessors; do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_cluster` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_get_node(
    result: *const kafka_admin_DescribeClusterResult_t,
    index: i32,
) -> *const kafka_common_Node_t {
    node_at(&unsafe { describe_cluster_result_ref(result) }.nodes, index)
}

/// Returns the current controller node (borrowed), or null if there is none
/// (Java's `controller()` yields null).
///
/// # Safety
///
/// `result` must be a valid `describe_cluster` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_controller(
    result: *const kafka_admin_DescribeClusterResult_t,
) -> *const kafka_common_Node_t {
    match &unsafe { describe_cluster_result_ref(result) }.controller {
        Some(node) => node as *const Node as *const kafka_common_Node_t,
        None => std::ptr::null(),
    }
}

/// Returns the number of authorized operations reported for the cluster, or
/// **-1** if the broker did not report them (Java yields null, which is distinct
/// from an empty set).
///
/// # Safety
///
/// `result` must be a valid `describe_cluster` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_authorized_operation_count(
    result: *const kafka_admin_DescribeClusterResult_t,
) -> i32 {
    match &unsafe { describe_cluster_result_ref(result) }.authorized_operations {
        Some(ops) => ops.len() as i32,
        None => -1,
    }
}

/// Returns the `AclOperation` wire code (Java's `AclOperation.code()`) of the
/// authorized operation at `index`, or -1 if out of range or absent.
///
/// # Safety
///
/// `result` must be a valid `describe_cluster` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_authorized_operation(
    result: *const kafka_admin_DescribeClusterResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    match &unsafe { describe_cluster_result_ref(result) }.authorized_operations {
        Some(ops) => ops.get(index as usize).copied().unwrap_or(-1),
        None => -1,
    }
}

/// Destroys a `describe_cluster` result handle, invalidating every borrowed
/// sub-handle obtained from it. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_cluster` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_destroy(result: *mut kafka_admin_DescribeClusterResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeClusterResultInner)) };
    }
}

/// Submits `describeCluster` and returns a future over its four attributes.
///
/// All four are awaited before any error is reported, so none is abandoned; if
/// more than one failed, the first in Java's declaration order (nodes,
/// controller, cluster id, authorized operations) is returned. C has one handle
/// per call, so a failure of any attribute is a whole-call failure.
fn submit_describe_cluster(
    admin: &dyn Admin,
    options: DescribeClusterOptions,
) -> impl std::future::Future<Output = Result<DescribeClusterOutcome, KafkaError>> + Send + use<> {
    let result = admin.describe_cluster(options);
    let nodes = result.nodes();
    let controller = result.controller();
    let cluster_id = result.cluster_id();
    let authorized_operations = result.authorized_operations();
    async move {
        let nodes = nodes.get().await;
        let controller = controller.get().await;
        let cluster_id = cluster_id.get().await;
        let authorized_operations = authorized_operations.get().await;
        Ok(DescribeClusterOutcome {
            nodes: nodes?,
            controller: controller?,
            cluster_id: cluster_id?,
            authorized_operations: authorized_operations?,
        })
    }
}

/// Builds `DescribeClusterOptions` from the flat C option parameters.
fn describe_cluster_options(
    timeout_ms: i32,
    include_authorized_operations: bool,
    include_fenced_brokers: bool,
) -> DescribeClusterOptions {
    DescribeClusterOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .include_authorized_operations(include_authorized_operations)
        .include_fenced_brokers(include_fenced_brokers)
}

/// Completion callback for [`kafka_admin_AdminClient_describe_cluster_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeClusterResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`.
pub type kafka_admin_AdminClient_describe_cluster_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeClusterResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Describes the cluster, blocking until every attribute future has resolved
/// (synchronous).
///
/// On success writes a [`kafka_admin_DescribeClusterResult_t`] to `*out_result`
/// (free it with [`kafka_admin_DescribeClusterResult_destroy`]) and returns null.
/// Java's result holds four independent futures rather than a per-key map, so
/// unlike the batch RPCs there are no per-key errors: any failure is returned
/// here.
///
/// # Parameters
///
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `include_authorized_operations`:
///   `DescribeClusterOptions.includeAuthorizedOperations`.
/// - `include_fenced_brokers`: `DescribeClusterOptions.includeFencedBrokers`.
///
/// # Safety
///
/// `admin` must be a valid handle; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_cluster(
    admin: *const kafka_admin_AdminClient_t,
    timeout_ms: i32,
    include_authorized_operations: bool,
    include_fenced_brokers: bool,
    out_result: *mut *mut kafka_admin_DescribeClusterResult_t,
) -> *mut kafka_common_KafkaError_t {
    let options = describe_cluster_options(timeout_ms, include_authorized_operations, include_fenced_brokers);
    let outcome = unsafe { admin_sync_future_op(admin, move |a| Ok(submit_describe_cluster(a, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_cluster_result) }
}

/// Describes the cluster asynchronously. See
/// [`kafka_admin_AdminClient_describe_cluster`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_cluster_async(
    admin: *const kafka_admin_AdminClient_t,
    timeout_ms: i32,
    include_authorized_operations: bool,
    include_fenced_brokers: bool,
    callback: kafka_admin_AdminClient_describe_cluster_callback_t,
    user_data: *mut c_void,
) {
    let options = describe_cluster_options(timeout_ms, include_authorized_operations, include_fenced_brokers);
    unsafe {
        admin_async_future_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_cluster(a, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcome) => (box_describe_cluster_result(outcome), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeConfigs
// ---------------------------------------------------------------------------

/// Per-resource outcomes of `describeConfigs`.
type DescribeConfigsOutcomes = HashMap<ConfigResource, Result<Config, KafkaError>>;

/// Opaque handle to a flattened `DescribeConfigsResult`, keyed by config
/// resource.
#[repr(C)]
pub struct kafka_admin_DescribeConfigsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeConfigsResult_t`].
///
/// The key is a `ConfigResource`, which C reads as a type code plus a name
/// (`_get_key_type(i)` / `_get_key_name(i)`) rather than through a dedicated
/// handle type — the shape [`kafka_admin_DeleteRecordsResult_t`] already uses for
/// its `TopicPartition` keys.
struct DescribeConfigsResultInner {
    key_types: Vec<i32>,
    key_names: Vec<CString>,
    values: Vec<Option<ConfigInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-resource `describeConfigs` outcomes into the C handle.
fn box_describe_configs_result(outcomes: DescribeConfigsOutcomes) -> *mut kafka_admin_DescribeConfigsResult_t {
    let entries = sorted_config_resource_entries(outcomes);
    let mut key_types = Vec::with_capacity(entries.len());
    let mut key_names = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (resource, outcome) in entries {
        key_types.push(i32::from(resource.resource_type().id()));
        key_names.push(to_cstring(resource.name()));
        match outcome {
            Ok(config) => {
                values.push(Some(ConfigInner::new(&config)));
                errors.push(None);
            },
            Err(e) => {
                values.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(DescribeConfigsResultInner { key_types, key_names, values, errors }))
        as *mut kafka_admin_DescribeConfigsResult_t
}

/// Casts a `*const kafka_admin_DescribeConfigsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_configs` call.
unsafe fn describe_configs_result_ref(
    result: *const kafka_admin_DescribeConfigsResult_t,
) -> &'static DescribeConfigsResultInner {
    unsafe { &*(result as *const DescribeConfigsResultInner) }
}

/// Returns the number of requested resources.
///
/// # Safety
///
/// `result` must be a valid `describe_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_count(
    result: *const kafka_admin_DescribeConfigsResult_t,
) -> i32 {
    unsafe { describe_configs_result_ref(result) }.key_names.len() as i32
}

/// Returns the `ConfigResource.Type.id()` of the resource at `index`, or -1 if
/// out of range. Entries are sorted by `(type id, name)`.
///
/// # Safety
///
/// `result` must be a valid `describe_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_get_key_type(
    result: *const kafka_admin_DescribeConfigsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { describe_configs_result_ref(result) }
        .key_types
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the name of the resource at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `result` must be a valid `describe_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_get_key_name(
    result: *const kafka_admin_DescribeConfigsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { describe_configs_result_ref(result) }.key_names, index)
}

/// Returns the config of the resource at `index` (borrowed), or null if that
/// resource failed (see [`kafka_admin_DescribeConfigsResult_get_error`]) or
/// `index` is out of range.
///
/// # Safety
///
/// `result` must be a valid `describe_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_get_value(
    result: *const kafka_admin_DescribeConfigsResult_t,
    index: i32,
) -> *const kafka_admin_Config_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_configs_result_ref(result) }.values.get(index as usize) {
        Some(Some(config)) => config as *const ConfigInner as *const kafka_admin_Config_t,
        _ => std::ptr::null(),
    }
}

/// Returns the error for the resource at `index` (borrowed), or null if it was
/// described successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_get_error(
    result: *const kafka_admin_DescribeConfigsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_configs_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `describe_configs` result handle, invalidating every borrowed
/// sub-handle obtained from it. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_destroy(result: *mut kafka_admin_DescribeConfigsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeConfigsResultInner)) };
    }
}

/// Submits `describeConfigs` and returns the collect-all future over its
/// per-resource futures.
fn submit_describe_configs(
    admin: &dyn Admin,
    resources: &[ConfigResource],
    options: DescribeConfigsOptions,
) -> KafkaFuture<DescribeConfigsOutcomes> {
    let result = admin.describe_configs(resources, options);
    let entries: Vec<(ConfigResource, KafkaFuture<Config>)> =
        result.values().iter().map(|(r, f)| (r.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Builds `DescribeConfigsOptions` from the flat C option parameters.
fn describe_configs_options(
    timeout_ms: i32,
    include_synonyms: bool,
    include_documentation: bool,
) -> DescribeConfigsOptions {
    DescribeConfigsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .include_synonyms(include_synonyms)
        .include_documentation(include_documentation)
}

/// Completion callback for [`kafka_admin_AdminClient_describe_configs_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeConfigsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-resource failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_describe_configs_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeConfigsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Describes the configuration of the given resources, blocking until every
/// per-resource future has resolved (synchronous).
///
/// Java's `Collection<ConfigResource>` becomes two parallel arrays: entry `i` is
/// the resource `(resource_types[i], resource_names[i])`, where the type is a
/// `ConfigResource.Type.id()` code (2 = TOPIC, 4 = BROKER, 8 = BROKER_LOGGER,
/// 16 = CLIENT_METRICS, 32 = GROUP). An entry with a NULL name is skipped.
///
/// On success writes a [`kafka_admin_DescribeConfigsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_DescribeConfigsResult_destroy`]) and returns null.
/// **A per-resource failure is not a call failure**: it is reported by
/// [`kafka_admin_DescribeConfigsResult_get_error`] for that key. A non-null return
/// means the request could not be submitted at all.
///
/// # Parameters
///
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `include_synonyms`: `DescribeConfigsOptions.includeSynonyms`.
/// - `include_documentation`: `DescribeConfigsOptions.includeDocumentation`.
///
/// # Safety
///
/// `admin` must be a valid handle; `resource_types` and `resource_names` must
/// have `count` valid entries each; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_configs(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_synonyms: bool,
    include_documentation: bool,
    out_result: *mut *mut kafka_admin_DescribeConfigsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let resources = unsafe { read_config_resources(resource_types, resource_names, count) };
    let options = describe_configs_options(timeout_ms, include_synonyms, include_documentation);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_describe_configs(a, &resources, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_configs_result) }
}

/// Describes resource configurations asynchronously. See
/// [`kafka_admin_AdminClient_describe_configs`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `resource_types` and `resource_names` must
/// have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_configs_async(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_synonyms: bool,
    include_documentation: bool,
    callback: kafka_admin_AdminClient_describe_configs_callback_t,
    user_data: *mut c_void,
) {
    let resources = unsafe { read_config_resources(resource_types, resource_names, count) };
    let options = describe_configs_options(timeout_ms, include_synonyms, include_documentation);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_configs(a, &resources, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_describe_configs_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// incrementalAlterConfigs
// ---------------------------------------------------------------------------

/// Per-resource outcomes of `incrementalAlterConfigs`.
type AlterConfigsOutcomes = HashMap<ConfigResource, Result<(), KafkaError>>;

/// Opaque handle to a flattened `AlterConfigsResult`, keyed by config resource.
#[repr(C)]
pub struct kafka_admin_AlterConfigsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_AlterConfigsResult_t`].
///
/// There is no per-key value: Java's per-resource future is `KafkaFuture<Void>`,
/// so a null error *is* the success value.
struct AlterConfigsResultInner {
    key_types: Vec<i32>,
    key_names: Vec<CString>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-resource `incrementalAlterConfigs` outcomes into the C
/// handle.
fn box_alter_configs_result(outcomes: AlterConfigsOutcomes) -> *mut kafka_admin_AlterConfigsResult_t {
    let entries = sorted_config_resource_entries(outcomes);
    let mut key_types = Vec::with_capacity(entries.len());
    let mut key_names = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (resource, outcome) in entries {
        key_types.push(i32::from(resource.resource_type().id()));
        key_names.push(to_cstring(resource.name()));
        errors.push(outcome.err().map(error_inner));
    }
    Box::into_raw(Box::new(AlterConfigsResultInner { key_types, key_names, errors }))
        as *mut kafka_admin_AlterConfigsResult_t
}

/// Casts a `*const kafka_admin_AlterConfigsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from an `incremental_alter_configs` call.
unsafe fn alter_configs_result_ref(
    result: *const kafka_admin_AlterConfigsResult_t,
) -> &'static AlterConfigsResultInner {
    unsafe { &*(result as *const AlterConfigsResultInner) }
}

/// Returns the number of altered resources.
///
/// # Safety
///
/// `result` must be a valid `incremental_alter_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_count(result: *const kafka_admin_AlterConfigsResult_t) -> i32 {
    unsafe { alter_configs_result_ref(result) }.key_names.len() as i32
}

/// Returns the `ConfigResource.Type.id()` of the resource at `index`, or -1 if
/// out of range. Entries are sorted by `(type id, name)`.
///
/// # Safety
///
/// `result` must be a valid `incremental_alter_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_get_key_type(
    result: *const kafka_admin_AlterConfigsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { alter_configs_result_ref(result) }
        .key_types
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the name of the resource at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `result` must be a valid `incremental_alter_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_get_key_name(
    result: *const kafka_admin_AlterConfigsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { alter_configs_result_ref(result) }.key_names, index)
}

/// Returns the error for the resource at `index` (borrowed), or null if it was
/// altered successfully or `index` is out of range. Do not destroy it.
///
/// There is no `_get_value`: Java's per-resource future is `KafkaFuture<Void>`,
/// so a null error *is* the success value.
///
/// # Safety
///
/// `result` must be a valid `incremental_alter_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_get_error(
    result: *const kafka_admin_AlterConfigsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { alter_configs_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys an `incremental_alter_configs` result handle. Safe with null
/// (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `incremental_alter_configs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_destroy(result: *mut kafka_admin_AlterConfigsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut AlterConfigsResultInner)) };
    }
}

/// Submits `incrementalAlterConfigs` and returns the collect-all future over its
/// per-resource futures.
fn submit_incremental_alter_configs(
    admin: &dyn Admin,
    configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
    options: AlterConfigsOptions,
) -> KafkaFuture<AlterConfigsOutcomes> {
    let result = admin.incremental_alter_configs(configs, options);
    let entries: Vec<(ConfigResource, KafkaFuture<()>)> =
        result.values().iter().map(|(r, f)| (r.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Completion callback for
/// [`kafka_admin_AdminClient_incremental_alter_configs_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_AlterConfigsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-resource failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_incremental_alter_configs_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_AlterConfigsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Incrementally alters resource configurations, blocking until every
/// per-resource future has resolved (synchronous).
///
/// Java's `Map<ConfigResource, Collection<AlterConfigOp>>` becomes five parallel
/// arrays, **one row per operation**: row `i` applies
/// `(config_names[i] -> config_values[i], op_types[i])` to the resource
/// `(resource_types[i], resource_names[i])`. Rows naming the same resource are
/// grouped in order. `resource_types` hold `ConfigResource.Type.id()` codes and
/// `op_types` hold `AlterConfigOp.OpType.id()` codes (0 = SET, 1 = DELETE,
/// 2 = APPEND, 3 = SUBTRACT). A row with a NULL resource name or config name is
/// skipped; a NULL `config_values` entry is the null value DELETE uses. An
/// unknown op-type code fails the whole call with an illegal-argument error.
///
/// On success writes a [`kafka_admin_AlterConfigsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_AlterConfigsResult_destroy`]) and returns null.
/// Per-resource failures are reported by
/// [`kafka_admin_AlterConfigsResult_get_error`], not by the return value.
///
/// # Parameters
///
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `validate_only`: `AlterConfigsOptions.validateOnly` — validate without
///   applying.
///
/// # Safety
///
/// `admin` must be a valid handle; every input array must have `count` valid
/// entries; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_incremental_alter_configs(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    config_names: *const *const c_char,
    config_values: *const *const c_char,
    op_types: *const i32,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    out_result: *mut *mut kafka_admin_AlterConfigsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let configs = match unsafe {
        read_alter_config_ops(resource_types, resource_names, config_names, config_values, op_types, count)
    } {
        Ok(configs) => configs,
        Err(e) => return box_error(e),
    };
    let options = AlterConfigsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .validate_only(validate_only);
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| Ok(submit_incremental_alter_configs(a, &configs, options))) };
    unsafe { finish_sync(outcome, out_result, box_alter_configs_result) }
}

/// Incrementally alters resource configurations asynchronously. See
/// [`kafka_admin_AdminClient_incremental_alter_configs`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, or an unknown `AlterConfigOp.OpType`
/// code). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; every input array must have `count` valid
/// entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_incremental_alter_configs_async(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    config_names: *const *const c_char,
    config_values: *const *const c_char,
    op_types: *const i32,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    callback: kafka_admin_AdminClient_incremental_alter_configs_callback_t,
    user_data: *mut c_void,
) {
    let parsed =
        unsafe { read_alter_config_ops(resource_types, resource_names, config_names, config_values, op_types, count) };
    let options = AlterConfigsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .validate_only(validate_only);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_incremental_alter_configs(a, &parsed?, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_alter_configs_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// listConfigResources
// ---------------------------------------------------------------------------

/// Opaque handle to a `ListConfigResourcesResult`.
#[repr(C)]
pub struct kafka_admin_ListConfigResourcesResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListConfigResourcesResult_t`].
///
/// Java's `listConfigResources` has a single `KafkaFuture<Collection<
/// ConfigResource>>`, so there are no per-key errors: the whole call either
/// succeeds or fails.
struct ListConfigResourcesResultInner {
    types: Vec<i32>,
    names: Vec<CString>,
}

/// Flattens the listed resources into the C handle, sorted by
/// `(type id, name)` — Java returns an unordered collection, but C indexes it.
fn box_list_config_resources_result(resources: Vec<ConfigResource>) -> *mut kafka_admin_ListConfigResourcesResult_t {
    let mut sorted = resources;
    sorted.sort_by(|a, b| {
        a.resource_type()
            .id()
            .cmp(&b.resource_type().id())
            .then_with(|| a.name().cmp(b.name()))
    });
    let mut types = Vec::with_capacity(sorted.len());
    let mut names = Vec::with_capacity(sorted.len());
    for resource in &sorted {
        types.push(i32::from(resource.resource_type().id()));
        names.push(to_cstring(resource.name()));
    }
    Box::into_raw(Box::new(ListConfigResourcesResultInner { types, names }))
        as *mut kafka_admin_ListConfigResourcesResult_t
}

/// Casts a `*const kafka_admin_ListConfigResourcesResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_config_resources` call.
unsafe fn list_config_resources_result_ref(
    result: *const kafka_admin_ListConfigResourcesResult_t,
) -> &'static ListConfigResourcesResultInner {
    unsafe { &*(result as *const ListConfigResourcesResultInner) }
}

/// Returns the number of listed config resources.
///
/// # Safety
///
/// `result` must be a valid `list_config_resources` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_count(
    result: *const kafka_admin_ListConfigResourcesResult_t,
) -> i32 {
    unsafe { list_config_resources_result_ref(result) }.names.len() as i32
}

/// Returns the `ConfigResource.Type.id()` of the resource at `index`, or -1 if
/// out of range. Entries are sorted by `(type id, name)`.
///
/// # Safety
///
/// `result` must be a valid `list_config_resources` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_get_type(
    result: *const kafka_admin_ListConfigResourcesResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { list_config_resources_result_ref(result) }
        .types
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the name of the resource at `index` (borrowed), or null if out of
/// range.
///
/// # Safety
///
/// `result` must be a valid `list_config_resources` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_get_name(
    result: *const kafka_admin_ListConfigResourcesResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { list_config_resources_result_ref(result) }.names, index)
}

/// Destroys a `list_config_resources` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_config_resources` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_destroy(
    result: *mut kafka_admin_ListConfigResourcesResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListConfigResourcesResultInner)) };
    }
}

/// Completion callback for
/// [`kafka_admin_AdminClient_list_config_resources_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it.
pub type kafka_admin_AdminClient_list_config_resources_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_ListConfigResourcesResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Lists the cluster's config resources whose type is in `resource_types`
/// (synchronous).
///
/// `resource_types` hold `ConfigResource.Type.id()` codes; pass NULL or
/// `count == 0` for Java's empty set, which means "every supported type".
///
/// On success writes a [`kafka_admin_ListConfigResourcesResult_t`] to
/// `*out_result` (free with
/// [`kafka_admin_ListConfigResourcesResult_destroy`]) and returns null. Java has
/// a single future here, so any failure is a call failure and is returned.
///
/// # Safety
///
/// `admin` must be a valid handle; `resource_types` must be null or have `count`
/// readable entries; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_config_resources(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_ListConfigResourcesResult_t,
) -> *mut kafka_common_KafkaError_t {
    let types = unsafe { read_config_resource_types(resource_types, count) };
    let options = ListConfigResourcesOptions::new().timeout_ms(option_timeout(timeout_ms));
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(a.list_config_resources(&types, options).all())) };
    unsafe { finish_sync(outcome, out_result, box_list_config_resources_result) }
}

/// Lists the cluster's config resources asynchronously. See
/// [`kafka_admin_AdminClient_list_config_resources`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `resource_types` must be null or have `count`
/// readable entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_config_resources_async(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_list_config_resources_callback_t,
    user_data: *mut c_void,
) {
    let types = unsafe { read_config_resource_types(resource_types, count) };
    let options = ListConfigResourcesOptions::new().timeout_ms(option_timeout(timeout_ms));
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(a.list_config_resources(&types, options).all()),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(resources) => (box_list_config_resources_result(resources), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

/// Reads `count` `ConfigResource.Type.id()` codes into the `HashSet` Java's
/// `listConfigResources` takes. An empty set means "every supported type".
///
/// # Safety
///
/// `type_codes` must be null or have `count` readable entries.
unsafe fn read_config_resource_types(type_codes: *const i32, count: i32) -> HashSet<ConfigResourceType> {
    unsafe { read_i32s(type_codes, count) }
        .into_iter()
        .map(|code| ConfigResourceType::for_id(enum_code_or_unknown(code)))
        .collect()
}

// ---------------------------------------------------------------------------
// listClientMetricsResources
// ---------------------------------------------------------------------------

/// Opaque handle to a `ListClientMetricsResourcesResult`.
#[repr(C)]
pub struct kafka_admin_ListClientMetricsResourcesResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListClientMetricsResourcesResult_t`].
///
/// Java's `ClientMetricsResourceListing` carries only a name, so the handle
/// exposes names directly instead of a sub-handle per listing. There is a single
/// future, hence no per-key errors.
struct ListClientMetricsResourcesResultInner {
    names: Vec<CString>,
}

/// Flattens the client-metrics resource listings into the C handle, sorted by
/// name (Java returns an unordered collection, but C indexes it).
#[allow(deprecated)]
fn box_list_client_metrics_resources_result(
    listings: Vec<ClientMetricsResourceListing>,
) -> *mut kafka_admin_ListClientMetricsResourcesResult_t {
    let mut names: Vec<CString> = listings.iter().map(|l| to_cstring(l.name())).collect();
    names.sort();
    Box::into_raw(Box::new(ListClientMetricsResourcesResultInner { names }))
        as *mut kafka_admin_ListClientMetricsResourcesResult_t
}

/// Casts a `*const kafka_admin_ListClientMetricsResourcesResult_t` to a
/// reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_client_metrics_resources`
/// call.
unsafe fn list_client_metrics_resources_result_ref(
    result: *const kafka_admin_ListClientMetricsResourcesResult_t,
) -> &'static ListClientMetricsResourcesResultInner {
    unsafe { &*(result as *const ListClientMetricsResourcesResultInner) }
}

/// Returns the number of client-metrics resources.
///
/// # Safety
///
/// `result` must be a valid `list_client_metrics_resources` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListClientMetricsResourcesResult_count(
    result: *const kafka_admin_ListClientMetricsResourcesResult_t,
) -> i32 {
    unsafe { list_client_metrics_resources_result_ref(result) }.names.len() as i32
}

/// Returns the name of the resource at `index` (borrowed), or null if out of
/// range. Entries are sorted by name.
///
/// # Safety
///
/// `result` must be a valid `list_client_metrics_resources` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListClientMetricsResourcesResult_get_name(
    result: *const kafka_admin_ListClientMetricsResourcesResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { list_client_metrics_resources_result_ref(result) }.names, index)
}

/// Destroys a `list_client_metrics_resources` result handle. Safe with null
/// (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_client_metrics_resources` result
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListClientMetricsResourcesResult_destroy(
    result: *mut kafka_admin_ListClientMetricsResourcesResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListClientMetricsResourcesResultInner)) };
    }
}

/// Completion callback for
/// [`kafka_admin_AdminClient_list_client_metrics_resources_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it.
pub type kafka_admin_AdminClient_list_client_metrics_resources_callback_t = unsafe extern "C" fn(
    *mut kafka_admin_ListClientMetricsResourcesResult_t,
    *mut kafka_common_KafkaError_t,
    *mut c_void,
);

/// Lists the cluster's client-metrics resources (synchronous).
///
/// Mirrors Java's `Admin.listClientMetricsResources`, which is **deprecated
/// since 4.1** in favour of `listConfigResources` filtered to
/// `CLIENT_METRICS`; it is exposed for parity. On success writes a
/// [`kafka_admin_ListClientMetricsResourcesResult_t`] to `*out_result` (free with
/// [`kafka_admin_ListClientMetricsResourcesResult_destroy`]) and returns null.
/// Java has a single future here, so any failure is a call failure and is
/// returned.
///
/// # Safety
///
/// `admin` must be a valid handle; `out_result` must be null or writable.
#[unsafe(no_mangle)]
#[allow(deprecated)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_client_metrics_resources(
    admin: *const kafka_admin_AdminClient_t,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_ListClientMetricsResourcesResult_t,
) -> *mut kafka_common_KafkaError_t {
    let options = ListClientMetricsResourcesOptions::new().timeout_ms(option_timeout(timeout_ms));
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(a.list_client_metrics_resources(options).all())) };
    unsafe { finish_sync(outcome, out_result, box_list_client_metrics_resources_result) }
}

/// Lists the cluster's client-metrics resources asynchronously. See
/// [`kafka_admin_AdminClient_list_client_metrics_resources`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle from an admin-client constructor.
#[unsafe(no_mangle)]
#[allow(deprecated)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_client_metrics_resources_async(
    admin: *const kafka_admin_AdminClient_t,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_list_client_metrics_resources_callback_t,
    user_data: *mut c_void,
) {
    let options = ListClientMetricsResourcesOptions::new().timeout_ms(option_timeout(timeout_ms));
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(a.list_client_metrics_resources(options).all()),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(listings) => (box_list_client_metrics_resources_result(listings), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeLogDirs
// ---------------------------------------------------------------------------

/// Per-broker outcomes of `describeLogDirs`.
type DescribeLogDirsOutcomes = HashMap<i32, Result<HashMap<String, LogDirDescription>, KafkaError>>;

/// Opaque handle to a flattened `DescribeLogDirsResult`, keyed by broker id.
#[repr(C)]
pub struct kafka_admin_DescribeLogDirsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeLogDirsResult_t`].
struct DescribeLogDirsResultInner {
    brokers: Vec<i32>,
    values: Vec<Option<LogDirDescriptionMapInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-broker `describeLogDirs` outcomes into the C handle.
fn box_describe_log_dirs_result(outcomes: DescribeLogDirsOutcomes) -> *mut kafka_admin_DescribeLogDirsResult_t {
    let entries = sorted_entries(outcomes);
    let mut brokers = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (broker, outcome) in entries {
        brokers.push(broker);
        match outcome {
            Ok(map) => {
                values.push(Some(LogDirDescriptionMapInner::new(&map)));
                errors.push(None);
            },
            Err(e) => {
                values.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(DescribeLogDirsResultInner { brokers, values, errors }))
        as *mut kafka_admin_DescribeLogDirsResult_t
}

/// Casts a `*const kafka_admin_DescribeLogDirsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_log_dirs` call.
unsafe fn describe_log_dirs_result_ref(
    result: *const kafka_admin_DescribeLogDirsResult_t,
) -> &'static DescribeLogDirsResultInner {
    unsafe { &*(result as *const DescribeLogDirsResultInner) }
}

/// Returns the number of queried brokers.
///
/// # Safety
///
/// `result` must be a valid `describe_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_count(
    result: *const kafka_admin_DescribeLogDirsResult_t,
) -> i32 {
    unsafe { describe_log_dirs_result_ref(result) }.brokers.len() as i32
}

/// Returns the broker id at `index`, or -1 if out of range. Entries are sorted
/// by broker id.
///
/// # Safety
///
/// `result` must be a valid `describe_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_get_broker(
    result: *const kafka_admin_DescribeLogDirsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { describe_log_dirs_result_ref(result) }
        .brokers
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the broker's log-dir map at `index` (borrowed), or null if that
/// broker failed (see [`kafka_admin_DescribeLogDirsResult_get_error`]) or `index`
/// is out of range.
///
/// # Safety
///
/// `result` must be a valid `describe_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_get_value(
    result: *const kafka_admin_DescribeLogDirsResult_t,
    index: i32,
) -> *const kafka_admin_LogDirDescriptionMap_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_log_dirs_result_ref(result) }.values.get(index as usize) {
        Some(Some(map)) => map as *const LogDirDescriptionMapInner as *const kafka_admin_LogDirDescriptionMap_t,
        _ => std::ptr::null(),
    }
}

/// Returns the error for the broker at `index` (borrowed), or null if it
/// answered or `index` is out of range. Do not destroy it.
///
/// A *per-log-dir* error is separate — see
/// [`kafka_admin_LogDirDescription_error`].
///
/// # Safety
///
/// `result` must be a valid `describe_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_get_error(
    result: *const kafka_admin_DescribeLogDirsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_log_dirs_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `describe_log_dirs` result handle, invalidating every borrowed
/// sub-handle obtained from it. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_destroy(result: *mut kafka_admin_DescribeLogDirsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeLogDirsResultInner)) };
    }
}

/// Submits `describeLogDirs` and returns the collect-all future over its
/// per-broker futures.
fn submit_describe_log_dirs(
    admin: &dyn Admin,
    brokers: &[i32],
    options: DescribeLogDirsOptions,
) -> KafkaFuture<DescribeLogDirsOutcomes> {
    let result = admin.describe_log_dirs(brokers, options);
    let entries: Vec<(i32, KafkaFuture<HashMap<String, LogDirDescription>>)> =
        result.descriptions().iter().map(|(b, f)| (*b, f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Completion callback for [`kafka_admin_AdminClient_describe_log_dirs_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeLogDirsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-broker failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_describe_log_dirs_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeLogDirsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Queries the log directories of the given brokers, blocking until every
/// per-broker future has resolved (synchronous).
///
/// On success writes a [`kafka_admin_DescribeLogDirsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_DescribeLogDirsResult_destroy`]) and returns null.
/// **A per-broker failure is not a call failure**: it is reported by
/// [`kafka_admin_DescribeLogDirsResult_get_error`] for that broker. A non-null
/// return means the request could not be submitted at all.
///
/// # Parameters
///
/// - `brokers`: array of `count` broker ids.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///   `DescribeLogDirsOptions` has no other field in Java.
///
/// # Safety
///
/// `admin` must be a valid handle; `brokers` must have `count` readable entries;
/// `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_log_dirs(
    admin: *const kafka_admin_AdminClient_t,
    brokers: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DescribeLogDirsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let broker_ids = unsafe { read_i32s(brokers, count) };
    let options = DescribeLogDirsOptions::new().timeout_ms(option_timeout(timeout_ms));
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_describe_log_dirs(a, &broker_ids, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_log_dirs_result) }
}

/// Queries broker log directories asynchronously. See
/// [`kafka_admin_AdminClient_describe_log_dirs`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `brokers` must have `count` readable entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_log_dirs_async(
    admin: *const kafka_admin_AdminClient_t,
    brokers: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_describe_log_dirs_callback_t,
    user_data: *mut c_void,
) {
    let broker_ids = unsafe { read_i32s(brokers, count) };
    let options = DescribeLogDirsOptions::new().timeout_ms(option_timeout(timeout_ms));
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_log_dirs(a, &broker_ids, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_describe_log_dirs_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// alterReplicaLogDirs
// ---------------------------------------------------------------------------

/// Per-replica outcomes of `alterReplicaLogDirs`.
type AlterReplicaLogDirsOutcomes = HashMap<TopicPartitionReplica, Result<(), KafkaError>>;

/// Opaque handle to a flattened `AlterReplicaLogDirsResult`, keyed by replica.
#[repr(C)]
pub struct kafka_admin_AlterReplicaLogDirsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_AlterReplicaLogDirsResult_t`].
///
/// The key is a `TopicPartitionReplica`, which C reads as a topic, a partition
/// and a broker id rather than through a dedicated handle type. There is no
/// per-key value: Java's per-replica future is `KafkaFuture<Void>`.
struct AlterReplicaLogDirsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    broker_ids: Vec<i32>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-replica `alterReplicaLogDirs` outcomes into the C handle.
fn box_alter_replica_log_dirs_result(
    outcomes: AlterReplicaLogDirsOutcomes,
) -> *mut kafka_admin_AlterReplicaLogDirsResult_t {
    let entries = sorted_replica_entries(outcomes);
    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut broker_ids = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (replica, outcome) in entries {
        topics.push(to_cstring(replica.topic()));
        partitions.push(replica.partition());
        broker_ids.push(replica.broker_id());
        errors.push(outcome.err().map(error_inner));
    }
    Box::into_raw(Box::new(AlterReplicaLogDirsResultInner {
        topics,
        partitions,
        broker_ids,
        errors,
    })) as *mut kafka_admin_AlterReplicaLogDirsResult_t
}

/// Casts a `*const kafka_admin_AlterReplicaLogDirsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from an `alter_replica_log_dirs` call.
unsafe fn alter_replica_log_dirs_result_ref(
    result: *const kafka_admin_AlterReplicaLogDirsResult_t,
) -> &'static AlterReplicaLogDirsResultInner {
    unsafe { &*(result as *const AlterReplicaLogDirsResultInner) }
}

/// Returns the number of requested replica moves.
///
/// # Safety
///
/// `result` must be a valid `alter_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_count(
    result: *const kafka_admin_AlterReplicaLogDirsResult_t,
) -> i32 {
    unsafe { alter_replica_log_dirs_result_ref(result) }.topics.len() as i32
}

/// Returns the topic of the replica at `index` (borrowed), or null if out of
/// range. Entries are sorted by `(topic, partition, broker id)`.
///
/// # Safety
///
/// `result` must be a valid `alter_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_get_topic(
    result: *const kafka_admin_AlterReplicaLogDirsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { alter_replica_log_dirs_result_ref(result) }.topics, index)
}

/// Returns the partition of the replica at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `alter_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_get_partition(
    result: *const kafka_admin_AlterReplicaLogDirsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { alter_replica_log_dirs_result_ref(result) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the broker id of the replica at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `alter_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_get_broker_id(
    result: *const kafka_admin_AlterReplicaLogDirsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { alter_replica_log_dirs_result_ref(result) }
        .broker_ids
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the error for the replica at `index` (borrowed), or null if the move
/// was accepted or `index` is out of range. Do not destroy it.
///
/// There is no `_get_value`: Java's per-replica future is `KafkaFuture<Void>`,
/// so a null error *is* the success value.
///
/// # Safety
///
/// `result` must be a valid `alter_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_get_error(
    result: *const kafka_admin_AlterReplicaLogDirsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { alter_replica_log_dirs_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys an `alter_replica_log_dirs` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `alter_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_destroy(
    result: *mut kafka_admin_AlterReplicaLogDirsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut AlterReplicaLogDirsResultInner)) };
    }
}

/// Reads the flat `alterReplicaLogDirs` rows into Java's
/// `Map<TopicPartitionReplica, String>`. A row with a NULL topic or NULL log dir
/// is skipped, so the arrays cannot drift out of step.
///
/// # Safety
///
/// Every array must be null or have `count` readable entries.
unsafe fn read_replica_assignment(
    topics: *const *const c_char,
    partitions: *const i32,
    broker_ids: *const i32,
    log_dirs: *const *const c_char,
    count: i32,
) -> HashMap<TopicPartitionReplica, String> {
    let n = count.max(0) as usize;
    let mut out = HashMap::new();
    if topics.is_null() || partitions.is_null() || broker_ids.is_null() || log_dirs.is_null() {
        return out;
    }
    for i in 0..n {
        let topic_ptr = unsafe { *topics.add(i) };
        let log_dir_ptr = unsafe { *log_dirs.add(i) };
        if topic_ptr.is_null() || log_dir_ptr.is_null() {
            continue;
        }
        let topic = unsafe { CStr::from_ptr(topic_ptr) }.to_string_lossy().to_string();
        let log_dir = unsafe { CStr::from_ptr(log_dir_ptr) }.to_string_lossy().to_string();
        out.insert(
            TopicPartitionReplica::new(topic, unsafe { *partitions.add(i) }, unsafe { *broker_ids.add(i) }),
            log_dir,
        );
    }
    out
}

/// Submits `alterReplicaLogDirs` and returns the collect-all future over its
/// per-replica futures.
fn submit_alter_replica_log_dirs(
    admin: &dyn Admin,
    replica_assignment: &HashMap<TopicPartitionReplica, String>,
    options: AlterReplicaLogDirsOptions,
) -> KafkaFuture<AlterReplicaLogDirsOutcomes> {
    let result = admin.alter_replica_log_dirs(replica_assignment, options);
    let entries: Vec<(TopicPartitionReplica, KafkaFuture<()>)> =
        result.values().iter().map(|(r, f)| (r.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Completion callback for
/// [`kafka_admin_AdminClient_alter_replica_log_dirs_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_AlterReplicaLogDirsResult_destroy`] or `error`
/// with `kafka_common_KafkaError_destroy`. A per-replica failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_alter_replica_log_dirs_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_AlterReplicaLogDirsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Moves the given replicas to new log directories, blocking until every
/// per-replica future has resolved (synchronous).
///
/// This is `alterReplicaLogDirs(Map<TopicPartitionReplica, String>,
/// AlterReplicaLogDirsOptions)`. Java's map becomes four parallel arrays: entry
/// `i` moves the replica `(topics[i], partitions[i], broker_ids[i])` to
/// `log_dirs[i]`. An entry with a NULL topic or NULL log dir is skipped.
///
/// On success writes a [`kafka_admin_AlterReplicaLogDirsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_AlterReplicaLogDirsResult_destroy`]) and returns null.
/// Per-replica failures are reported by
/// [`kafka_admin_AlterReplicaLogDirsResult_get_error`], not by the return value.
///
/// # Safety
///
/// `admin` must be a valid handle; every input array must have `count` valid
/// entries; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_replica_log_dirs(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    broker_ids: *const i32,
    log_dirs: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_AlterReplicaLogDirsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let assignment = unsafe { read_replica_assignment(topics, partitions, broker_ids, log_dirs, count) };
    let options = AlterReplicaLogDirsOptions::new().timeout_ms(option_timeout(timeout_ms));
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| Ok(submit_alter_replica_log_dirs(a, &assignment, options))) };
    unsafe { finish_sync(outcome, out_result, box_alter_replica_log_dirs_result) }
}

/// Moves replicas to new log directories asynchronously. See
/// [`kafka_admin_AdminClient_alter_replica_log_dirs`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; every input array must have `count` valid
/// entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_replica_log_dirs_async(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    broker_ids: *const i32,
    log_dirs: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_alter_replica_log_dirs_callback_t,
    user_data: *mut c_void,
) {
    let assignment = unsafe { read_replica_assignment(topics, partitions, broker_ids, log_dirs, count) };
    let options = AlterReplicaLogDirsOptions::new().timeout_ms(option_timeout(timeout_ms));
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_alter_replica_log_dirs(a, &assignment, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_alter_replica_log_dirs_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeReplicaLogDirs
// ---------------------------------------------------------------------------

/// Per-replica outcomes of `describeReplicaLogDirs`.
type DescribeReplicaLogDirsOutcomes = HashMap<TopicPartitionReplica, Result<ReplicaLogDirInfo, KafkaError>>;

/// Opaque handle to a flattened `DescribeReplicaLogDirsResult`, keyed by
/// replica.
#[repr(C)]
pub struct kafka_admin_DescribeReplicaLogDirsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeReplicaLogDirsResult_t`].
struct DescribeReplicaLogDirsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    broker_ids: Vec<i32>,
    values: Vec<Option<ReplicaLogDirInfoInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-replica `describeReplicaLogDirs` outcomes into the C handle.
fn box_describe_replica_log_dirs_result(
    outcomes: DescribeReplicaLogDirsOutcomes,
) -> *mut kafka_admin_DescribeReplicaLogDirsResult_t {
    let entries = sorted_replica_entries(outcomes);
    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut broker_ids = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (replica, outcome) in entries {
        topics.push(to_cstring(replica.topic()));
        partitions.push(replica.partition());
        broker_ids.push(replica.broker_id());
        match outcome {
            Ok(info) => {
                values.push(Some(ReplicaLogDirInfoInner::new(&info)));
                errors.push(None);
            },
            Err(e) => {
                values.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(DescribeReplicaLogDirsResultInner {
        topics,
        partitions,
        broker_ids,
        values,
        errors,
    })) as *mut kafka_admin_DescribeReplicaLogDirsResult_t
}

/// Casts a `*const kafka_admin_DescribeReplicaLogDirsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_replica_log_dirs` call.
unsafe fn describe_replica_log_dirs_result_ref(
    result: *const kafka_admin_DescribeReplicaLogDirsResult_t,
) -> &'static DescribeReplicaLogDirsResultInner {
    unsafe { &*(result as *const DescribeReplicaLogDirsResultInner) }
}

/// Returns the number of described replicas.
///
/// Against a real broker this equals the number of replicas requested:
/// `KafkaAdminClient.describeReplicaLogDirs` seeds one future per requested
/// replica (`KafkaAdminClient.java:3066-3068`) and completes every one of them
/// (`:3141-3145`). A replica whose topic the broker does not know is therefore
/// still *present*, holding a default `ReplicaLogDirInfo`: its
/// [`kafka_admin_ReplicaLogDirInfo_current_replica_log_dir`] is null and its
/// offset lags are `-1`. Absence from the result is not the signal for an
/// unknown topic; a null current log dir is.
///
/// `MockAdminClient.describeReplicaLogDirs` diverges: it skips replicas whose
/// topic it does not know (`MockAdminClient.java:1112`), so against the mock
/// this can be smaller than the number requested.
///
/// # Safety
///
/// `result` must be a valid `describe_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_count(
    result: *const kafka_admin_DescribeReplicaLogDirsResult_t,
) -> i32 {
    unsafe { describe_replica_log_dirs_result_ref(result) }.topics.len() as i32
}

/// Returns the topic of the replica at `index` (borrowed), or null if out of
/// range. Entries are sorted by `(topic, partition, broker id)`.
///
/// # Safety
///
/// `result` must be a valid `describe_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_get_topic(
    result: *const kafka_admin_DescribeReplicaLogDirsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { describe_replica_log_dirs_result_ref(result) }.topics, index)
}

/// Returns the partition of the replica at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `describe_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_get_partition(
    result: *const kafka_admin_DescribeReplicaLogDirsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { describe_replica_log_dirs_result_ref(result) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the broker id of the replica at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `describe_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_get_broker_id(
    result: *const kafka_admin_DescribeReplicaLogDirsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { describe_replica_log_dirs_result_ref(result) }
        .broker_ids
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the log-dir info of the replica at `index` (borrowed), or null if
/// that replica failed (see
/// [`kafka_admin_DescribeReplicaLogDirsResult_get_error`]) or `index` is out of
/// range.
///
/// # Safety
///
/// `result` must be a valid `describe_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_get_value(
    result: *const kafka_admin_DescribeReplicaLogDirsResult_t,
    index: i32,
) -> *const kafka_admin_ReplicaLogDirInfo_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_replica_log_dirs_result_ref(result) }
        .values
        .get(index as usize)
    {
        Some(Some(info)) => info as *const ReplicaLogDirInfoInner as *const kafka_admin_ReplicaLogDirInfo_t,
        _ => std::ptr::null(),
    }
}

/// Returns the error for the replica at `index` (borrowed), or null if it was
/// described successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_get_error(
    result: *const kafka_admin_DescribeReplicaLogDirsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_replica_log_dirs_result_ref(result) }
        .errors
        .get(index as usize)
    {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `describe_replica_log_dirs` result handle, invalidating every
/// borrowed sub-handle obtained from it. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_replica_log_dirs` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_destroy(
    result: *mut kafka_admin_DescribeReplicaLogDirsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeReplicaLogDirsResultInner)) };
    }
}

/// Submits `describeReplicaLogDirs` and returns the collect-all future over its
/// per-replica futures.
fn submit_describe_replica_log_dirs(
    admin: &dyn Admin,
    replicas: &[TopicPartitionReplica],
    options: DescribeReplicaLogDirsOptions,
) -> KafkaFuture<DescribeReplicaLogDirsOutcomes> {
    let result = admin.describe_replica_log_dirs(replicas, options);
    let entries: Vec<(TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>)> =
        result.values().iter().map(|(r, f)| (r.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Completion callback for
/// [`kafka_admin_AdminClient_describe_replica_log_dirs_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeReplicaLogDirsResult_destroy`] or `error`
/// with `kafka_common_KafkaError_destroy`. A per-replica failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_describe_replica_log_dirs_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeReplicaLogDirsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Queries the log directories of the given replicas, blocking until every
/// per-replica future has resolved (synchronous).
///
/// This is `describeReplicaLogDirs(Collection<TopicPartitionReplica>,
/// DescribeReplicaLogDirsOptions)`. Java's collection becomes three parallel
/// arrays: entry `i` is the replica `(topics[i], partitions[i],
/// broker_ids[i])`. An entry with a NULL topic is skipped.
///
/// On success writes a [`kafka_admin_DescribeReplicaLogDirsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_DescribeReplicaLogDirsResult_destroy`]) and returns null.
/// Per-replica failures are reported by
/// [`kafka_admin_DescribeReplicaLogDirsResult_get_error`], not by the return
/// value.
///
/// # Safety
///
/// `admin` must be a valid handle; every input array must have `count` valid
/// entries; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_replica_log_dirs(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    broker_ids: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DescribeReplicaLogDirsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let replicas = unsafe { read_replicas(topics, partitions, broker_ids, count) };
    let options = DescribeReplicaLogDirsOptions::new().timeout_ms(option_timeout(timeout_ms));
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| Ok(submit_describe_replica_log_dirs(a, &replicas, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_replica_log_dirs_result) }
}

/// Queries replica log directories asynchronously. See
/// [`kafka_admin_AdminClient_describe_replica_log_dirs`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; every input array must have `count` valid
/// entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_replica_log_dirs_async(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    broker_ids: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_describe_replica_log_dirs_callback_t,
    user_data: *mut c_void,
) {
    let replicas = unsafe { read_replicas(topics, partitions, broker_ids, count) };
    let options = DescribeReplicaLogDirsOptions::new().timeout_ms(option_timeout(timeout_ms));
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_replica_log_dirs(a, &replicas, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_describe_replica_log_dirs_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// Elections / reassignments / offsets value types
// ---------------------------------------------------------------------------

/// The broker id reported for an out-of-range replica index. Real broker ids are
/// never negative.
const UNKNOWN_BROKER_ID: i32 = -1;

/// Opaque handle to a `PartitionReassignment` (Java's
/// `org.apache.kafka.clients.admin.PartitionReassignment`).
///
/// Borrowed from the owning `listPartitionReassignments` result handle; valid
/// until that handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_PartitionReassignment_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_PartitionReassignment_t`].
///
/// Java's three accessors are `List<Integer>`, i.e. broker ids rather than
/// `Node`s, so they cross as plain `int32_t` count/index pairs (no
/// `kafka_common_Node_t` involved).
struct PartitionReassignmentInner {
    replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    removing_replicas: Vec<i32>,
}

impl PartitionReassignmentInner {
    fn new(reassignment: &PartitionReassignment) -> Self {
        Self {
            replicas: reassignment.replicas().to_vec(),
            adding_replicas: reassignment.adding_replicas().to_vec(),
            removing_replicas: reassignment.removing_replicas().to_vec(),
        }
    }
}

/// Returns `ids[index]`, or -1 when `index` is out of range.
fn broker_id_at(ids: &[i32], index: i32) -> i32 {
    if index < 0 {
        return UNKNOWN_BROKER_ID;
    }
    ids.get(index as usize).copied().unwrap_or(UNKNOWN_BROKER_ID)
}

/// Casts a `*const kafka_admin_PartitionReassignment_t` to a reference.
///
/// # Safety
///
/// `reassignment` must be a non-null borrowed pointer from a
/// `list_partition_reassignments` result handle.
unsafe fn partition_reassignment_ref(
    reassignment: *const kafka_admin_PartitionReassignment_t,
) -> &'static PartitionReassignmentInner {
    unsafe { &*(reassignment as *const PartitionReassignmentInner) }
}

/// Returns the number of current replicas (Java's `replicas()`).
///
/// # Safety
///
/// `reassignment` must be a valid borrowed partition-reassignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_replica_count(
    reassignment: *const kafka_admin_PartitionReassignment_t,
) -> i32 {
    unsafe { partition_reassignment_ref(reassignment) }.replicas.len() as i32
}

/// Returns the current replica broker id at `index`, or -1 if out of range.
///
/// # Safety
///
/// `reassignment` must be a valid borrowed partition-reassignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_replica(
    reassignment: *const kafka_admin_PartitionReassignment_t,
    index: i32,
) -> i32 {
    broker_id_at(&unsafe { partition_reassignment_ref(reassignment) }.replicas, index)
}

/// Returns the number of replicas being added (Java's `addingReplicas()`).
///
/// # Safety
///
/// `reassignment` must be a valid borrowed partition-reassignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_adding_replica_count(
    reassignment: *const kafka_admin_PartitionReassignment_t,
) -> i32 {
    unsafe { partition_reassignment_ref(reassignment) }.adding_replicas.len() as i32
}

/// Returns the broker id of the added replica at `index`, or -1 if out of range.
///
/// # Safety
///
/// `reassignment` must be a valid borrowed partition-reassignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_adding_replica(
    reassignment: *const kafka_admin_PartitionReassignment_t,
    index: i32,
) -> i32 {
    broker_id_at(&unsafe { partition_reassignment_ref(reassignment) }.adding_replicas, index)
}

/// Returns the number of replicas being removed (Java's `removingReplicas()`).
///
/// # Safety
///
/// `reassignment` must be a valid borrowed partition-reassignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_removing_replica_count(
    reassignment: *const kafka_admin_PartitionReassignment_t,
) -> i32 {
    unsafe { partition_reassignment_ref(reassignment) }.removing_replicas.len() as i32
}

/// Returns the broker id of the removed replica at `index`, or -1 if out of
/// range.
///
/// # Safety
///
/// `reassignment` must be a valid borrowed partition-reassignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_removing_replica(
    reassignment: *const kafka_admin_PartitionReassignment_t,
    index: i32,
) -> i32 {
    broker_id_at(&unsafe { partition_reassignment_ref(reassignment) }.removing_replicas, index)
}

/// Opaque handle to a `ListOffsetsResultInfo` (Java's
/// `ListOffsetsResult.ListOffsetsResultInfo`).
///
/// Borrowed from the owning `listOffsets` result handle; valid until that handle
/// is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_ListOffsetsResultInfo_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListOffsetsResultInfo_t`].
///
/// Java's `leaderEpoch()` is an `Optional<Integer>`, so it crosses through the
/// crate's usual out-param-plus-bool shape rather than a sentinel (precedent:
/// `kafka_consumer_OffsetAndMetadata_leader_epoch`).
struct ListOffsetsResultInfoInner {
    info: ListOffsetsResultInfo,
}

/// Casts a `*const kafka_admin_ListOffsetsResultInfo_t` to a reference.
///
/// # Safety
///
/// `info` must be a non-null borrowed pointer from a `list_offsets` result
/// handle.
unsafe fn list_offsets_info_ref(
    info: *const kafka_admin_ListOffsetsResultInfo_t,
) -> &'static ListOffsetsResultInfoInner {
    unsafe { &*(info as *const ListOffsetsResultInfoInner) }
}

/// Returns the offset (Java's `offset()`).
///
/// # Safety
///
/// `info` must be a valid borrowed list-offsets info pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResultInfo_offset(
    info: *const kafka_admin_ListOffsetsResultInfo_t,
) -> i64 {
    unsafe { list_offsets_info_ref(info) }.info.offset()
}

/// Returns the timestamp associated with the offset (Java's `timestamp()`).
/// `-1` means the broker reported no timestamp, which is what every non-
/// `forTimestamp` query returns.
///
/// # Safety
///
/// `info` must be a valid borrowed list-offsets info pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResultInfo_timestamp(
    info: *const kafka_admin_ListOffsetsResultInfo_t,
) -> i64 {
    unsafe { list_offsets_info_ref(info) }.info.timestamp()
}

/// Writes the leader epoch to `*out_epoch` and returns `true`, or returns
/// `false` when Java's `leaderEpoch()` is `Optional.empty()`.
///
/// # Safety
///
/// `info` must be a valid borrowed list-offsets info pointer; `out_epoch` must
/// be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResultInfo_leader_epoch(
    info: *const kafka_admin_ListOffsetsResultInfo_t,
    out_epoch: *mut i32,
) -> bool {
    match unsafe { list_offsets_info_ref(info) }.info.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                unsafe { *out_epoch = epoch };
            }
            true
        },
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Elections / reassignments / offsets input marshaling
//
// Every RPC in this group is keyed by `TopicPartition`, which crosses as two
// parallel arrays (`topics[i]`, `partitions[i]`) exactly as in `deleteRecords`
// and in the consumer FFI's `read_topic_partitions`. Where Java carries an
// `Optional`, C gets an explicit boolean discriminant beside the payload rather
// than an overloaded NULL or sentinel, so "absent" and "present but empty" stay
// distinguishable.
// ---------------------------------------------------------------------------

/// Reads `count` `(topic, partition)` pairs into [`TopicPartition`]s, skipping
/// entries whose topic is NULL so the two arrays cannot drift out of step.
///
/// This shares its name and shape with the consumer FFI's private
/// `read_topic_partitions` (`src/ffi/consumer.rs`), but **deliberately differs
/// in its NULL handling**: this one returns empty for a NULL array and skips a
/// NULL topic entry, where the consumer's does neither and would dereference a
/// NULL topic. Do not "unify" the two. Every admin entry point built on this
/// helper documents "an entry with a NULL topic is skipped" in its rustdoc, and
/// cbindgen ships that sentence into the public C header — delegating to the
/// consumer helper would make four shipped doc comments false. The two are
/// private to their own modules, so there is no conflict; if they are ever
/// merged, the merged helper must keep *these* guards.
///
/// # Safety
///
/// `topics` and `partitions` must be null or have `count` readable entries each,
/// every topic NULL or a valid C string.
unsafe fn read_topic_partitions(
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> Vec<TopicPartition> {
    let n = count.max(0) as usize;
    if topics.is_null() || partitions.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let name_ptr = unsafe { *topics.add(i) };
        if name_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        out.push(TopicPartition::new(name, unsafe { *partitions.add(i) }));
    }
    out
}

/// Reads the optional partition set that `electLeaders` and
/// `listPartitionReassignments` take.
///
/// `all_partitions` is the explicit discriminant for Java's absent set
/// (`electLeaders`' null `Set`, `listPartitionReassignments`' `Optional.empty()`),
/// which means "every partition". When it is true the arrays are not read at
/// all, so "all partitions" can never be confused with an empty selection.
///
/// # Safety
///
/// `topics` and `partitions` must be null or have `count` readable entries each,
/// every topic NULL or a valid C string.
unsafe fn read_optional_partition_set(
    all_partitions: bool,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
) -> Option<HashSet<TopicPartition>> {
    if all_partitions {
        return None;
    }
    Some(
        unsafe { read_topic_partitions(topics, partitions, count) }
            .into_iter()
            .collect(),
    )
}

/// Builds the owned `Map<TopicPartition, Optional<NewPartitionReassignment>>`
/// for an `alterPartitionReassignments` call.
///
/// `cancel[i]` is the explicit discriminant for Java's `Optional.empty()`, which
/// **reverts** the reassignment of that partition (`Admin.java:1142-1143`). When
/// it is false, `target_replicas[i]` / `target_replica_counts[i]` supply the new
/// `NewPartitionReassignment`. Keeping the two apart means an empty replica list
/// stays an error rather than silently becoming a cancellation.
///
/// An entry with a NULL topic is skipped.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if a non-cancelling entry supplies no
/// replicas — Java's `NewPartitionReassignment(List<Integer>)` throws
/// `IllegalArgumentException` there, before the RPC is issued.
///
/// # Safety
///
/// `topics`, `partitions`, `cancel`, `target_replicas` and
/// `target_replica_counts` must be null or have `count` readable entries each;
/// every topic NULL or a valid C string; every non-cancelled `target_replicas`
/// entry must have `target_replica_counts[i]` readable `int32_t`s.
unsafe fn read_reassignments(
    topics: *const *const c_char,
    partitions: *const i32,
    cancel: *const bool,
    target_replicas: *const *const i32,
    target_replica_counts: *const i32,
    count: i32,
) -> Result<HashMap<TopicPartition, Option<NewPartitionReassignment>>, KafkaError> {
    let mut out = HashMap::new();
    if topics.is_null() || partitions.is_null() || cancel.is_null() {
        return Ok(out);
    }
    for i in 0..count.max(0) as usize {
        let name_ptr = unsafe { *topics.add(i) };
        if name_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        let tp = TopicPartition::new(name, unsafe { *partitions.add(i) });
        if unsafe { *cancel.add(i) } {
            out.insert(tp, None);
            continue;
        }
        let replicas = if target_replicas.is_null() || target_replica_counts.is_null() {
            Vec::new()
        } else {
            unsafe { read_i32s(*target_replicas.add(i), *target_replica_counts.add(i)) }
        };
        let reassignment = NewPartitionReassignment::new(replicas).map_err(|e| {
            KafkaError::illegal_argument(format!("reassignment for {tp} at index {i}: {}", e.message()))
        })?;
        out.insert(tp, Some(reassignment));
    }
    Ok(out)
}

/// Builds the owned `Map<TopicPartition, OffsetSpec>` for a `listOffsets` call.
///
/// `is_timestamp[i]` is the explicit discriminant between Java's
/// `OffsetSpec.forTimestamp(t)` and the six no-argument factories. When it is
/// true, `spec_timestamps[i]` is the epoch-millisecond timestamp, whatever its
/// value. When it is false, `spec_timestamps[i]` selects a factory through the
/// `ListOffsets` wire sentinel Java's `KafkaAdminClient.getOffsetFromSpec`
/// (`KafkaAdminClient.java:5142-5156`) emits for it:
///
/// | `spec_timestamps[i]` | `OffsetSpec` factory        |
/// |----------------------|-----------------------------|
/// | -1                   | `latest()`                  |
/// | -2                   | `earliest()`                |
/// | -3                   | `maxTimestamp()`            |
/// | -4                   | `earliestLocal()`           |
/// | -5                   | `latestTiered()`            |
/// | -6                   | `earliestPendingUpload()`   |
///
/// The discriminant exists because `getOffsetFromSpec` is not injective:
/// `forTimestamp(-2)` and `earliest()` both project to `-2`. Java keeps them
/// apart until that point (and `MockAdminClient` distinguishes them), so C must
/// too.
///
/// An entry with a NULL topic is skipped.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if a non-timestamp entry carries a
/// value that is not one of the six sentinels.
///
/// # Safety
///
/// `topics`, `partitions`, `is_timestamp` and `spec_timestamps` must be null or
/// have `count` readable entries each, every topic NULL or a valid C string.
unsafe fn read_offset_specs(
    topics: *const *const c_char,
    partitions: *const i32,
    is_timestamp: *const bool,
    spec_timestamps: *const i64,
    count: i32,
) -> Result<HashMap<TopicPartition, OffsetSpec>, KafkaError> {
    let mut out = HashMap::new();
    if topics.is_null() || partitions.is_null() || is_timestamp.is_null() || spec_timestamps.is_null() {
        return Ok(out);
    }
    for i in 0..count.max(0) as usize {
        let name_ptr = unsafe { *topics.add(i) };
        if name_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        let tp = TopicPartition::new(name, unsafe { *partitions.add(i) });
        let value = unsafe { *spec_timestamps.add(i) };
        let spec = if unsafe { *is_timestamp.add(i) } {
            OffsetSpec::for_timestamp(value)
        } else {
            offset_spec_for_sentinel(value).ok_or_else(|| {
                KafkaError::illegal_argument(format!(
                    "offset spec for {tp} at index {i}: {value} is not a ListOffsets timestamp sentinel; \
                     pass is_timestamp=true to request OffsetSpec.forTimestamp({value})"
                ))
            })?
        };
        out.insert(tp, spec);
    }
    Ok(out)
}

/// Inverts `KafkaAdminClient.getOffsetFromSpec` for the six no-argument
/// `OffsetSpec` factories, or `None` for a value that is not a sentinel.
fn offset_spec_for_sentinel(value: i64) -> Option<OffsetSpec> {
    match value {
        LATEST_TIMESTAMP => Some(OffsetSpec::latest()),
        EARLIEST_TIMESTAMP => Some(OffsetSpec::earliest()),
        MAX_TIMESTAMP => Some(OffsetSpec::max_timestamp()),
        EARLIEST_LOCAL_TIMESTAMP => Some(OffsetSpec::earliest_local()),
        LATEST_TIERED_TIMESTAMP => Some(OffsetSpec::latest_tiered()),
        EARLIEST_PENDING_UPLOAD_TIMESTAMP => Some(OffsetSpec::earliest_pending_upload()),
        _ => None,
    }
}

/// Builds the `ElectLeadersOptions` for an `electLeaders` call.
fn elect_leaders_options(timeout_ms: i32) -> ElectLeadersOptions {
    ElectLeadersOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Builds the `AlterPartitionReassignmentsOptions` for an
/// `alterPartitionReassignments` call.
fn alter_partition_reassignments_options(
    timeout_ms: i32,
    allow_replication_factor_change: bool,
) -> AlterPartitionReassignmentsOptions {
    AlterPartitionReassignmentsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .allow_replication_factor_change(allow_replication_factor_change)
}

/// Builds the `ListPartitionReassignmentsOptions` for a
/// `listPartitionReassignments` call.
fn list_partition_reassignments_options(timeout_ms: i32) -> ListPartitionReassignmentsOptions {
    ListPartitionReassignmentsOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Builds the `ListOffsetsOptions` for a `listOffsets` call.
///
/// `isolation_level` carries Java's `IsolationLevel.id()` (0 =
/// `READ_UNCOMMITTED`, 1 = `READ_COMMITTED`). Any other value is rejected, as
/// Java's `IsolationLevel.forId` throws `IllegalArgumentException`.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] for an unknown isolation-level id.
fn list_offsets_options(timeout_ms: i32, isolation_level: i32) -> Result<ListOffsetsOptions, KafkaError> {
    let level = u8::try_from(isolation_level)
        .map_err(|_| KafkaError::illegal_argument(format!("Unknown isolation level {isolation_level}")))
        .and_then(IsolationLevel::for_id)?;
    Ok(ListOffsetsOptions::with_isolation_level(level).timeout_ms(option_timeout(timeout_ms)))
}

// ---------------------------------------------------------------------------
// Elections / reassignments / offsets result handles
// ---------------------------------------------------------------------------

/// Sorts a `TopicPartition`-keyed outcome map into a deterministic,
/// index-addressable order.
///
/// `TopicPartition` is not `Ord` (matching Java, whose result maps are
/// unordered), so this sorts by `(topic, partition)` the way
/// [`box_delete_records_result`] already does.
fn sorted_partition_entries<V>(map: HashMap<TopicPartition, V>) -> Vec<(TopicPartition, V)> {
    let mut entries: Vec<(TopicPartition, V)> = map.into_iter().collect();
    entries.sort_by(|a, b| a.0.topic().cmp(b.0.topic()).then(a.0.partition().cmp(&b.0.partition())));
    entries
}

/// Opaque handle to a flattened `ElectLeadersResult`, keyed by topic partition.
#[repr(C)]
pub struct kafka_admin_ElectLeadersResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ElectLeadersResult_t`].
///
/// Java's `ElectLeadersResult.partitions()` resolves to
/// `Map<TopicPartition, Optional<Throwable>>` — a per-partition *error* with no
/// per-partition value, so this handle has `_get_error(i)` and no `_get_value`.
struct ElectLeadersResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-partition `electLeaders` outcomes into the C handle.
fn box_elect_leaders_result(
    outcomes: HashMap<TopicPartition, Option<KafkaError>>,
) -> *mut kafka_admin_ElectLeadersResult_t {
    let entries = sorted_partition_entries(outcomes);
    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (tp, outcome) in entries {
        topics.push(to_cstring(tp.topic()));
        partitions.push(tp.partition());
        errors.push(outcome.map(error_inner));
    }
    Box::into_raw(Box::new(ElectLeadersResultInner { topics, partitions, errors }))
        as *mut kafka_admin_ElectLeadersResult_t
}

/// Casts a `*const kafka_admin_ElectLeadersResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from an `elect_leaders` call.
unsafe fn elect_leaders_result_ref(
    result: *const kafka_admin_ElectLeadersResult_t,
) -> &'static ElectLeadersResultInner {
    unsafe { &*(result as *const ElectLeadersResultInner) }
}

/// Returns the number of partitions an election was attempted for.
///
/// # Safety
///
/// `result` must be a valid `elect_leaders` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_count(result: *const kafka_admin_ElectLeadersResult_t) -> i32 {
    unsafe { elect_leaders_result_ref(result) }.topics.len() as i32
}

/// Returns the topic name of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `result` must be a valid `elect_leaders` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_get_topic(
    result: *const kafka_admin_ElectLeadersResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { elect_leaders_result_ref(result) }.topics, index)
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `elect_leaders` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_get_partition(
    result: *const kafka_admin_ElectLeadersResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { elect_leaders_result_ref(result) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the error for the entry at `index` (borrowed), or null if the
/// election succeeded for that partition or `index` is out of range. Do not
/// destroy it.
///
/// # Safety
///
/// `result` must be a valid `elect_leaders` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_get_error(
    result: *const kafka_admin_ElectLeadersResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { elect_leaders_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys an `elect_leaders` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `elect_leaders` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_destroy(result: *mut kafka_admin_ElectLeadersResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ElectLeadersResultInner)) };
    }
}

/// Opaque handle to a flattened `AlterPartitionReassignmentsResult`, keyed by
/// topic partition.
#[repr(C)]
pub struct kafka_admin_AlterPartitionReassignmentsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_AlterPartitionReassignmentsResult_t`].
///
/// Java's per-partition future is `KafkaFuture<Void>`, so there is no per-key
/// value: a null `_get_error(i)` is the success signal.
struct AlterPartitionReassignmentsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-partition `alterPartitionReassignments` outcomes into the C
/// handle.
fn box_alter_partition_reassignments_result(
    outcomes: HashMap<TopicPartition, Result<(), KafkaError>>,
) -> *mut kafka_admin_AlterPartitionReassignmentsResult_t {
    let entries = sorted_partition_entries(outcomes);
    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (tp, outcome) in entries {
        topics.push(to_cstring(tp.topic()));
        partitions.push(tp.partition());
        errors.push(outcome.err().map(error_inner));
    }
    Box::into_raw(Box::new(AlterPartitionReassignmentsResultInner { topics, partitions, errors }))
        as *mut kafka_admin_AlterPartitionReassignmentsResult_t
}

/// Casts a `*const kafka_admin_AlterPartitionReassignmentsResult_t` to a
/// reference.
///
/// # Safety
///
/// `result` must be a non-null handle from an `alter_partition_reassignments`
/// call.
unsafe fn alter_partition_reassignments_result_ref(
    result: *const kafka_admin_AlterPartitionReassignmentsResult_t,
) -> &'static AlterPartitionReassignmentsResultInner {
    unsafe { &*(result as *const AlterPartitionReassignmentsResultInner) }
}

/// Returns the number of requested partitions.
///
/// # Safety
///
/// `result` must be a valid `alter_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_count(
    result: *const kafka_admin_AlterPartitionReassignmentsResult_t,
) -> i32 {
    unsafe { alter_partition_reassignments_result_ref(result) }.topics.len() as i32
}

/// Returns the topic name of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `result` must be a valid `alter_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_get_topic(
    result: *const kafka_admin_AlterPartitionReassignmentsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { alter_partition_reassignments_result_ref(result) }.topics, index)
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `alter_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_get_partition(
    result: *const kafka_admin_AlterPartitionReassignmentsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { alter_partition_reassignments_result_ref(result) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the error for the entry at `index` (borrowed), or null if that
/// partition succeeded or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `alter_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_get_error(
    result: *const kafka_admin_AlterPartitionReassignmentsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { alter_partition_reassignments_result_ref(result) }
        .errors
        .get(index as usize)
    {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys an `alter_partition_reassignments` result handle. Safe with null
/// (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `alter_partition_reassignments` result
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_destroy(
    result: *mut kafka_admin_AlterPartitionReassignmentsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut AlterPartitionReassignmentsResultInner)) };
    }
}

/// Opaque handle to a flattened `ListPartitionReassignmentsResult`, keyed by
/// topic partition.
#[repr(C)]
pub struct kafka_admin_ListPartitionReassignmentsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListPartitionReassignmentsResult_t`].
///
/// Java's `reassignments()` is a *single* `KafkaFuture<Map<TopicPartition,
/// PartitionReassignment>>`, not one future per key, so there is no per-key
/// error to report: a failure fails the whole call. Hence `_get_value(i)` and no
/// `_get_error(i)` (same shape as `listTopics`).
struct ListPartitionReassignmentsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    values: Vec<PartitionReassignmentInner>,
}

/// Flattens the `listPartitionReassignments` map into the C handle.
fn box_list_partition_reassignments_result(
    reassignments: HashMap<TopicPartition, PartitionReassignment>,
) -> *mut kafka_admin_ListPartitionReassignmentsResult_t {
    let entries = sorted_partition_entries(reassignments);
    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    for (tp, reassignment) in entries {
        topics.push(to_cstring(tp.topic()));
        partitions.push(tp.partition());
        values.push(PartitionReassignmentInner::new(&reassignment));
    }
    Box::into_raw(Box::new(ListPartitionReassignmentsResultInner { topics, partitions, values }))
        as *mut kafka_admin_ListPartitionReassignmentsResult_t
}

/// Casts a `*const kafka_admin_ListPartitionReassignmentsResult_t` to a
/// reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_partition_reassignments`
/// call.
unsafe fn list_partition_reassignments_result_ref(
    result: *const kafka_admin_ListPartitionReassignmentsResult_t,
) -> &'static ListPartitionReassignmentsResultInner {
    unsafe { &*(result as *const ListPartitionReassignmentsResultInner) }
}

/// Returns the number of partitions with an ongoing reassignment.
///
/// # Safety
///
/// `result` must be a valid `list_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListPartitionReassignmentsResult_count(
    result: *const kafka_admin_ListPartitionReassignmentsResult_t,
) -> i32 {
    unsafe { list_partition_reassignments_result_ref(result) }.topics.len() as i32
}

/// Returns the topic name of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `result` must be a valid `list_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListPartitionReassignmentsResult_get_topic(
    result: *const kafka_admin_ListPartitionReassignmentsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { list_partition_reassignments_result_ref(result) }.topics, index)
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `list_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListPartitionReassignmentsResult_get_partition(
    result: *const kafka_admin_ListPartitionReassignmentsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { list_partition_reassignments_result_ref(result) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the reassignment of the entry at `index` (borrowed; valid until
/// `result` is destroyed), or null if out of range.
///
/// # Safety
///
/// `result` must be a valid `list_partition_reassignments` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListPartitionReassignmentsResult_get_value(
    result: *const kafka_admin_ListPartitionReassignmentsResult_t,
    index: i32,
) -> *const kafka_admin_PartitionReassignment_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_partition_reassignments_result_ref(result) }
        .values
        .get(index as usize)
    {
        Some(value) => value as *const PartitionReassignmentInner as *const kafka_admin_PartitionReassignment_t,
        None => std::ptr::null(),
    }
}

/// Destroys a `list_partition_reassignments` result handle. Safe with null
/// (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_partition_reassignments` result
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListPartitionReassignmentsResult_destroy(
    result: *mut kafka_admin_ListPartitionReassignmentsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListPartitionReassignmentsResultInner)) };
    }
}

/// Opaque handle to a flattened `ListOffsetsResult`, keyed by topic partition.
#[repr(C)]
pub struct kafka_admin_ListOffsetsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListOffsetsResult_t`].
///
/// Java holds one `KafkaFuture<ListOffsetsResultInfo>` per partition, so this is
/// the full D2 shape: a per-key value *and* a per-key error, exactly one of
/// which is present for each entry.
struct ListOffsetsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    values: Vec<Option<ListOffsetsResultInfoInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-partition `listOffsets` outcomes into the C handle.
fn box_list_offsets_result(
    outcomes: HashMap<TopicPartition, Result<ListOffsetsResultInfo, KafkaError>>,
) -> *mut kafka_admin_ListOffsetsResult_t {
    let entries = sorted_partition_entries(outcomes);
    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (tp, outcome) in entries {
        topics.push(to_cstring(tp.topic()));
        partitions.push(tp.partition());
        match outcome {
            Ok(info) => {
                values.push(Some(ListOffsetsResultInfoInner { info }));
                errors.push(None);
            },
            Err(e) => {
                values.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(ListOffsetsResultInner { topics, partitions, values, errors }))
        as *mut kafka_admin_ListOffsetsResult_t
}

/// Casts a `*const kafka_admin_ListOffsetsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_offsets` call.
unsafe fn list_offsets_result_ref(result: *const kafka_admin_ListOffsetsResult_t) -> &'static ListOffsetsResultInner {
    unsafe { &*(result as *const ListOffsetsResultInner) }
}

/// Returns the number of requested partitions.
///
/// # Safety
///
/// `result` must be a valid `list_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_count(result: *const kafka_admin_ListOffsetsResult_t) -> i32 {
    unsafe { list_offsets_result_ref(result) }.topics.len() as i32
}

/// Returns the topic name of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `result` must be a valid `list_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_get_topic(
    result: *const kafka_admin_ListOffsetsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { list_offsets_result_ref(result) }.topics, index)
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `list_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_get_partition(
    result: *const kafka_admin_ListOffsetsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { list_offsets_result_ref(result) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Returns the offset information for the entry at `index` (borrowed; valid
/// until `result` is destroyed), or null if that partition failed (see
/// [`kafka_admin_ListOffsetsResult_get_error`]) or `index` is out of range.
///
/// # Safety
///
/// `result` must be a valid `list_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_get_value(
    result: *const kafka_admin_ListOffsetsResult_t,
    index: i32,
) -> *const kafka_admin_ListOffsetsResultInfo_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_offsets_result_ref(result) }.values.get(index as usize) {
        Some(Some(value)) => value as *const ListOffsetsResultInfoInner as *const kafka_admin_ListOffsetsResultInfo_t,
        _ => std::ptr::null(),
    }
}

/// Returns the error for the entry at `index` (borrowed), or null if that
/// partition succeeded or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `list_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_get_error(
    result: *const kafka_admin_ListOffsetsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_offsets_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `list_offsets` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_destroy(result: *mut kafka_admin_ListOffsetsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListOffsetsResultInner)) };
    }
}

// ---------------------------------------------------------------------------
// Elections / reassignments / offsets submission helpers
// ---------------------------------------------------------------------------

/// Per-partition outcomes of `electLeaders`. Java's
/// `Map<TopicPartition, Optional<Throwable>>` maps to `Option<KafkaError>`.
type ElectLeadersOutcomes = HashMap<TopicPartition, Option<KafkaError>>;
/// Per-partition outcomes of `alterPartitionReassignments`.
type AlterPartitionReassignmentsOutcomes = HashMap<TopicPartition, Result<(), KafkaError>>;
/// The single `listPartitionReassignments` map.
type ListPartitionReassignmentsOutcomes = HashMap<TopicPartition, PartitionReassignment>;
/// Per-partition outcomes of `listOffsets`.
type ListOffsetsOutcomes = HashMap<TopicPartition, Result<ListOffsetsResultInfo, KafkaError>>;

/// Submits `electLeaders` and returns its single `partitions()` future.
///
/// Java exposes one future for the whole election, whose value already carries
/// the per-partition `Optional<Throwable>`, so there is nothing to join here.
fn submit_elect_leaders(
    admin: &dyn Admin,
    election_type: ElectionType,
    partitions: Option<HashSet<TopicPartition>>,
    options: ElectLeadersOptions,
) -> KafkaFuture<ElectLeadersOutcomes> {
    admin.elect_leaders(election_type, partitions, options).partitions()
}

/// Submits `alterPartitionReassignments` and returns the collect-all future over
/// its per-partition futures.
fn submit_alter_partition_reassignments(
    admin: &dyn Admin,
    reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
    options: AlterPartitionReassignmentsOptions,
) -> KafkaFuture<AlterPartitionReassignmentsOutcomes> {
    let result = admin.alter_partition_reassignments(reassignments, options);
    let entries: Vec<(TopicPartition, KafkaFuture<()>)> =
        result.values().iter().map(|(tp, f)| (tp.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Submits `listPartitionReassignments` and returns its single `reassignments()`
/// future.
fn submit_list_partition_reassignments(
    admin: &dyn Admin,
    partitions: Option<HashSet<TopicPartition>>,
    options: ListPartitionReassignmentsOptions,
) -> KafkaFuture<ListPartitionReassignmentsOutcomes> {
    admin.list_partition_reassignments(partitions, options).reassignments()
}

/// Submits `listOffsets` and returns the collect-all future over its
/// per-partition futures.
///
/// `ListOffsetsResult` exposes its futures through `partitionResult(tp)` rather
/// than as a map, so the requested keys drive the join. Both the production
/// client and the mock seed one future per requested partition, so no key is
/// missing; a `partition_result` error is nevertheless propagated rather than
/// dropped.
fn submit_list_offsets(
    admin: &dyn Admin,
    topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
    options: ListOffsetsOptions,
) -> Result<KafkaFuture<ListOffsetsOutcomes>, KafkaError> {
    let result = admin.list_offsets(topic_partition_offsets, options);
    let mut entries: Vec<(TopicPartition, KafkaFuture<ListOffsetsResultInfo>)> =
        Vec::with_capacity(topic_partition_offsets.len());
    for tp in topic_partition_offsets.keys() {
        entries.push((tp.clone(), result.partition_result(tp)?));
    }
    Ok(KafkaFuture::join_map_results(entries))
}

// ---------------------------------------------------------------------------
// electLeaders
// ---------------------------------------------------------------------------

/// Completion callback for [`kafka_admin_AdminClient_elect_leaders_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_ElectLeadersResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-partition failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_elect_leaders_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_ElectLeadersResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Elects a leader for the given partitions, blocking until the election future
/// has resolved (synchronous).
///
/// This is `electLeaders(ElectionType, Set<TopicPartition>, ElectLeadersOptions)`.
///
/// On success writes a [`kafka_admin_ElectLeadersResult_t`] to `*out_result`
/// (free it with [`kafka_admin_ElectLeadersResult_destroy`]) and returns null.
/// **A per-partition failure is not a call failure**: it is reported by
/// [`kafka_admin_ElectLeadersResult_get_error`]. A non-null return means the
/// election could not be run at all, and `*out_result` is left untouched.
///
/// # Parameters
///
/// - `election_type`: Java's `ElectionType` byte value — `0` = `PREFERRED`,
///   `1` = `UNCLEAN`. Any other value is rejected
///   (`ElectionType.valueOf(byte)` throws `IllegalArgumentException`).
/// - `all_partitions`: pass `true` for Java's **null** partition set, i.e.
///   "conduct an election for every partition in the cluster"
///   (`Admin.java:1096-1097`). The `topics` / `partitions` / `count` arguments
///   are then ignored. Pass `false` to elect leaders only for the listed
///   partitions — an explicit flag, so "all partitions" and "an empty
///   selection" stay distinguishable.
/// - `topics` / `partitions`: parallel arrays of `count` entries; entry `i` is
///   `(topics[i], partitions[i])`. An entry with a NULL topic is skipped.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///   `ElectLeadersOptions` has no other field in Java.
///
/// # Safety
///
/// `admin` must be a valid handle; unless `all_partitions` is true, `topics` and
/// `partitions` must have `count` valid entries each; `out_result` must be null
/// or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_elect_leaders(
    admin: *const kafka_admin_AdminClient_t,
    election_type: i32,
    all_partitions: bool,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_ElectLeadersResult_t,
) -> *mut kafka_common_KafkaError_t {
    let selection = unsafe { read_optional_partition_set(all_partitions, topics, partitions, count) };
    let options = elect_leaders_options(timeout_ms);
    let outcome = unsafe {
        admin_sync_value_op(admin, move |a| {
            let election_type = read_election_type(election_type)?;
            Ok(submit_elect_leaders(a, election_type, selection, options))
        })
    };
    unsafe { finish_sync(outcome, out_result, box_elect_leaders_result) }
}

/// Elects leaders asynchronously. See [`kafka_admin_AdminClient_elect_leaders`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, or an `election_type` that is
/// neither 0 nor 1). And it runs on a **tokio worker thread** if the
/// dispatcher's completion queue can no longer be reached when the result
/// arrives. Destroying the handle does not cause that — an outstanding operation
/// holds its own sender, so it cannot disconnect the queue; what remains is a
/// dispatcher thread that terminated abnormally, i.e. a panic inside an earlier
/// callback. So callbacks are not guaranteed to be serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; unless `all_partitions` is true, `topics` and
/// `partitions` must have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_elect_leaders_async(
    admin: *const kafka_admin_AdminClient_t,
    election_type: i32,
    all_partitions: bool,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_elect_leaders_callback_t,
    user_data: *mut c_void,
) {
    let selection = unsafe { read_optional_partition_set(all_partitions, topics, partitions, count) };
    let options = elect_leaders_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| {
                let election_type = read_election_type(election_type)?;
                Ok(submit_elect_leaders(a, election_type, selection, options))
            },
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_elect_leaders_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

/// Converts the C `election_type` code into an [`ElectionType`].
///
/// The code is Java's `ElectionType` byte `value` (0 = `PREFERRED`,
/// 1 = `UNCLEAN`), so the mapping is Java's `ElectionType.valueOf(byte)`.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] for any other value, mirroring Java's
/// `IllegalArgumentException`.
fn read_election_type(election_type: i32) -> Result<ElectionType, KafkaError> {
    i8::try_from(election_type)
        .map_err(|_| KafkaError::illegal_argument(format!("Value {election_type} must be one of [PREFERRED, UNCLEAN]")))
        .and_then(ElectionType::value_of)
}

// ---------------------------------------------------------------------------
// alterPartitionReassignments
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_alter_partition_reassignments_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_AlterPartitionReassignmentsResult_destroy`] or
/// `error` with `kafka_common_KafkaError_destroy`. A per-partition failure
/// arrives inside `result`, not as `error`.
pub type kafka_admin_AdminClient_alter_partition_reassignments_callback_t = unsafe extern "C" fn(
    *mut kafka_admin_AlterPartitionReassignmentsResult_t,
    *mut kafka_common_KafkaError_t,
    *mut c_void,
);

/// Changes the reassignments of one or more partitions, blocking until every
/// per-partition future has resolved (synchronous).
///
/// This is `alterPartitionReassignments(Map<TopicPartition,
/// Optional<NewPartitionReassignment>>, AlterPartitionReassignmentsOptions)`.
/// Java's map becomes parallel arrays: entry `i` is `(topics[i],
/// partitions[i])`.
///
/// On success writes a [`kafka_admin_AlterPartitionReassignmentsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_AlterPartitionReassignmentsResult_destroy`]) and returns null.
/// **A per-partition failure is not a call failure**: it is reported by
/// [`kafka_admin_AlterPartitionReassignmentsResult_get_error`]. A non-null
/// return means the request could not be submitted at all, and `*out_result` is
/// left untouched.
///
/// # Parameters
///
/// - `topics` / `partitions`: parallel arrays of `count` entries. An entry with
///   a NULL topic is skipped.
/// - `cancel`: `count` flags. `cancel[i] != false` **reverts** the reassignment
///   of that partition — Java's empty `Optional`
///   (`Admin.java:1142-1143`) — and `target_replicas[i]` /
///   `target_replica_counts[i]` are then not read. A separate flag rather than a
///   NULL replica pointer, so cancelling stays distinct from "present but
///   empty", which Java rejects.
/// - `target_replicas` / `target_replica_counts`: for each non-cancelled entry,
///   `target_replicas[i]` points at `target_replica_counts[i]` broker ids
///   forming Java's `NewPartitionReassignment(List<Integer>)`. An empty list is
///   an error, exactly as in Java.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `allow_replication_factor_change`: Java's
///   `AlterPartitionReassignmentsOptions.allowReplicationFactorChange(boolean)`,
///   which defaults to `true`.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics`, `partitions`, `cancel`,
/// `target_replicas` and `target_replica_counts` must have `count` valid entries
/// each; every non-cancelled `target_replicas[i]` must point at
/// `target_replica_counts[i]` readable ids; `out_result` must be null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_partition_reassignments(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    cancel: *const bool,
    target_replicas: *const *const i32,
    target_replica_counts: *const i32,
    count: i32,
    timeout_ms: i32,
    allow_replication_factor_change: bool,
    out_result: *mut *mut kafka_admin_AlterPartitionReassignmentsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let reassignments =
        unsafe { read_reassignments(topics, partitions, cancel, target_replicas, target_replica_counts, count) };
    let options = alter_partition_reassignments_options(timeout_ms, allow_replication_factor_change);
    let outcome = unsafe {
        admin_sync_value_op(admin, move |a| {
            Ok(submit_alter_partition_reassignments(a, &reassignments?, options))
        })
    };
    unsafe { finish_sync(outcome, out_result, box_alter_partition_reassignments_result) }
}

/// Changes partition reassignments asynchronously. See
/// [`kafka_admin_AdminClient_alter_partition_reassignments`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, or a non-cancelled entry with no
/// target replicas). And it runs on a **tokio worker thread** if the
/// dispatcher's completion queue can no longer be reached when the result
/// arrives. Destroying the handle does not cause that — an outstanding operation
/// holds its own sender, so it cannot disconnect the queue; what remains is a
/// dispatcher thread that terminated abnormally, i.e. a panic inside an earlier
/// callback. So callbacks are not guaranteed to be serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics`, `partitions`, `cancel`,
/// `target_replicas` and `target_replica_counts` must have `count` valid entries
/// each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_partition_reassignments_async(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    cancel: *const bool,
    target_replicas: *const *const i32,
    target_replica_counts: *const i32,
    count: i32,
    timeout_ms: i32,
    allow_replication_factor_change: bool,
    callback: kafka_admin_AdminClient_alter_partition_reassignments_callback_t,
    user_data: *mut c_void,
) {
    let reassignments =
        unsafe { read_reassignments(topics, partitions, cancel, target_replicas, target_replica_counts, count) };
    let options = alter_partition_reassignments_options(timeout_ms, allow_replication_factor_change);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_alter_partition_reassignments(a, &reassignments?, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_alter_partition_reassignments_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// listPartitionReassignments
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_list_partition_reassignments_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_ListPartitionReassignmentsResult_destroy`] or
/// `error` with `kafka_common_KafkaError_destroy`. Java exposes one future for
/// the whole listing, so *any* failure arrives as `error`.
pub type kafka_admin_AdminClient_list_partition_reassignments_callback_t = unsafe extern "C" fn(
    *mut kafka_admin_ListPartitionReassignmentsResult_t,
    *mut kafka_common_KafkaError_t,
    *mut c_void,
);

/// Lists the ongoing partition reassignments, blocking until the listing future
/// has resolved (synchronous).
///
/// This is `listPartitionReassignments(Optional<Set<TopicPartition>>,
/// ListPartitionReassignmentsOptions)`.
///
/// On success writes a [`kafka_admin_ListPartitionReassignmentsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_ListPartitionReassignmentsResult_destroy`]) and returns null.
/// Java holds a **single** future here rather than one per partition, so unlike
/// the per-key RPCs there is no `_get_error(i)`: any failure is a call failure
/// and is returned, leaving `*out_result` untouched.
///
/// Only partitions with an ongoing reassignment appear in the result, so it can
/// be shorter than the request.
///
/// # Parameters
///
/// - `all_partitions`: pass `true` for Java's `Optional.empty()`, i.e. "list
///   every ongoing reassignment in the cluster" (`Admin.java:1246-1247`). The
///   `topics` / `partitions` / `count` arguments are then ignored. Pass `false`
///   to restrict the listing to the given partitions — an explicit flag, so
///   "all partitions" and "an empty selection" stay distinguishable.
/// - `topics` / `partitions`: parallel arrays of `count` entries. An entry with
///   a NULL topic is skipped.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///   `ListPartitionReassignmentsOptions` has no other field in Java.
///
/// # Safety
///
/// `admin` must be a valid handle; unless `all_partitions` is true, `topics` and
/// `partitions` must have `count` valid entries each; `out_result` must be null
/// or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_partition_reassignments(
    admin: *const kafka_admin_AdminClient_t,
    all_partitions: bool,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_ListPartitionReassignmentsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let selection = unsafe { read_optional_partition_set(all_partitions, topics, partitions, count) };
    let options = list_partition_reassignments_options(timeout_ms);
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| Ok(submit_list_partition_reassignments(a, selection, options))) };
    unsafe { finish_sync(outcome, out_result, box_list_partition_reassignments_result) }
}

/// Lists partition reassignments asynchronously. See
/// [`kafka_admin_AdminClient_list_partition_reassignments`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e. a
/// panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; unless `all_partitions` is true, `topics` and
/// `partitions` must have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_partition_reassignments_async(
    admin: *const kafka_admin_AdminClient_t,
    all_partitions: bool,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_list_partition_reassignments_callback_t,
    user_data: *mut c_void,
) {
    let selection = unsafe { read_optional_partition_set(all_partitions, topics, partitions, count) };
    let options = list_partition_reassignments_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_list_partition_reassignments(a, selection, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(reassignments) => (box_list_partition_reassignments_result(reassignments), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// listOffsets
// ---------------------------------------------------------------------------

/// Completion callback for [`kafka_admin_AdminClient_list_offsets_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_ListOffsetsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-partition failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_list_offsets_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_ListOffsetsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Lists the offsets of the given partitions, blocking until every
/// per-partition future has resolved (synchronous).
///
/// This is `listOffsets(Map<TopicPartition, OffsetSpec>, ListOffsetsOptions)`.
/// Java's map becomes parallel arrays: entry `i` is `(topics[i], partitions[i])`
/// with the `OffsetSpec` described below.
///
/// On success writes a [`kafka_admin_ListOffsetsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_ListOffsetsResult_destroy`]) and returns null.
/// **A per-partition failure is not a call failure**: it is reported by
/// [`kafka_admin_ListOffsetsResult_get_error`]. A non-null return means the
/// request could not be submitted at all, and `*out_result` is left untouched.
///
/// # Parameters
///
/// - `topics` / `partitions`: parallel arrays of `count` entries. An entry with
///   a NULL topic is skipped.
/// - `is_timestamp` / `spec_timestamps`: the `OffsetSpec` for entry `i`. When
///   `is_timestamp[i]` is true the spec is
///   `OffsetSpec.forTimestamp(spec_timestamps[i])` for any value at all;
///   otherwise `spec_timestamps[i]` selects one of the six no-argument
///   factories through the `ListOffsets` wire sentinel Java's
///   `KafkaAdminClient.getOffsetFromSpec` emits for it: `-1` = `latest()`,
///   `-2` = `earliest()`, `-3` = `maxTimestamp()`, `-4` = `earliestLocal()`,
///   `-5` = `latestTiered()`, `-6` = `earliestPendingUpload()`. Any other value
///   with `is_timestamp[i]` false is rejected. The flag is needed because that
///   projection is not injective — `forTimestamp(-2)` and `earliest()` both
///   yield `-2`, yet Java treats them differently up to that point.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `isolation_level`: Java's `IsolationLevel.id()` — `0` =
///   `READ_UNCOMMITTED` (Java's `ListOffsetsOptions` default), `1` =
///   `READ_COMMITTED`. Any other value is rejected.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics`, `partitions`, `is_timestamp` and
/// `spec_timestamps` must have `count` valid entries each; `out_result` must be
/// null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_offsets(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    is_timestamp: *const bool,
    spec_timestamps: *const i64,
    count: i32,
    timeout_ms: i32,
    isolation_level: i32,
    out_result: *mut *mut kafka_admin_ListOffsetsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let specs = unsafe { read_offset_specs(topics, partitions, is_timestamp, spec_timestamps, count) };
    let outcome = unsafe {
        admin_sync_value_op(admin, move |a| {
            submit_list_offsets(a, &specs?, list_offsets_options(timeout_ms, isolation_level)?)
        })
    };
    unsafe { finish_sync(outcome, out_result, box_list_offsets_result) }
}

/// Lists offsets asynchronously. See [`kafka_admin_AdminClient_list_offsets`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, an unknown `isolation_level`, or a
/// `spec_timestamps` entry that is neither flagged as a timestamp nor a
/// recognised sentinel). And it runs on a **tokio worker thread** if the
/// dispatcher's completion queue can no longer be reached when the result
/// arrives. Destroying the handle does not cause that — an outstanding operation
/// holds its own sender, so it cannot disconnect the queue; what remains is a
/// dispatcher thread that terminated abnormally, i.e. a panic inside an earlier
/// callback. So callbacks are not guaranteed to be serialised on one thread.
/// Do not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `topics`, `partitions`, `is_timestamp` and
/// `spec_timestamps` must have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_offsets_async(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    is_timestamp: *const bool,
    spec_timestamps: *const i64,
    count: i32,
    timeout_ms: i32,
    isolation_level: i32,
    callback: kafka_admin_AdminClient_list_offsets_callback_t,
    user_data: *mut c_void,
) {
    let specs = unsafe { read_offset_specs(topics, partitions, is_timestamp, spec_timestamps, count) };
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_list_offsets(a, &specs?, list_offsets_options(timeout_ms, isolation_level)?),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_list_offsets_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// Group value types
//
// Java's group descriptions nest three levels deep
// (`ConsumerGroupDescription` -> `MemberDescription` -> `MemberAssignment`).
// Each level that is a real Java class gets its own borrowed opaque handle; a
// nested value that is *not* a Java class is flattened into indexed accessors
// on its parent, as B2 already did for `LogDirDescription.ReplicaInfo`.
//
// Every `Optional` field crosses with an explicit discriminant, following B3:
// an optional string is a null `const char *`, an optional number is a
// `bool fn(handle, T *out)`, and an optional nested handle is a null pointer.
// ---------------------------------------------------------------------------

/// Returns the NUL-terminated bytes of an optional [`CString`], or null when it
/// is absent — Java's `Optional.empty()` or a null string.
fn optional_cstring_ptr(value: &Option<CString>) -> *const c_char {
    match value {
        Some(text) => text.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Writes `value` to `out` when it is present, returning whether it was.
///
/// The crate's shape for a Java `Optional<Integer>` / `OptionalLong` output
/// (precedent: `kafka_consumer_OffsetAndMetadata_leader_epoch`).
///
/// # Safety
///
/// `out` must be null or writable.
unsafe fn write_optional<T: Copy>(value: Option<T>, out: *mut T) -> bool {
    match value {
        Some(value) => {
            if !out.is_null() {
                unsafe { *out = value };
            }
            true
        },
        None => false,
    }
}

/// Opaque handle to a `GroupListing` (Java's
/// `org.apache.kafka.clients.admin.GroupListing`).
///
/// Borrowed from the owning `list_groups` result handle; valid until that
/// handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_GroupListing_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_GroupListing_t`].
///
/// `type()` and `groupState()` are `Optional` in Java, so they cross as
/// nullable strings. Neither `GroupType` nor `GroupState` has a numeric `id()`
/// in Java, so — per the B2 rule — the enum's `toString()` name is the
/// contract rather than an invented code.
struct GroupListingInner {
    group_id_c: CString,
    group_type_c: Option<CString>,
    protocol_c: CString,
    group_state_c: Option<CString>,
    is_simple_consumer_group: bool,
}

impl GroupListingInner {
    fn new(listing: &GroupListing) -> Self {
        Self {
            group_id_c: to_cstring(listing.group_id()),
            group_type_c: listing.group_type().map(|t| to_cstring(t.name())),
            protocol_c: to_cstring(listing.protocol()),
            group_state_c: listing.group_state().map(|s| to_cstring(s.name())),
            is_simple_consumer_group: listing.is_simple_consumer_group(),
        }
    }
}

/// Casts a `*const kafka_admin_GroupListing_t` to a reference.
///
/// # Safety
///
/// `listing` must be a non-null borrowed pointer from a `list_groups` result
/// getter.
unsafe fn group_listing_ref(listing: *const kafka_admin_GroupListing_t) -> &'static GroupListingInner {
    unsafe { &*(listing as *const GroupListingInner) }
}

/// Returns the group id (borrowed). Java's `groupId()`.
///
/// # Safety
///
/// `listing` must be a valid borrowed group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_group_id(
    listing: *const kafka_admin_GroupListing_t,
) -> *const c_char {
    unsafe { group_listing_ref(listing) }.group_id_c.as_ptr()
}

/// Returns the `GroupType` name (borrowed) — Java's `toString()` value, i.e.
/// `"Consumer"`, `"Classic"`, `"Share"`, `"Streams"` or `"Unknown"` — or null
/// when Java's `type()` is `Optional.empty()`.
///
/// `GroupType` has no numeric id in Java, so its `toString()` name is the
/// contract.
///
/// # Safety
///
/// `listing` must be a valid borrowed group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_group_type(
    listing: *const kafka_admin_GroupListing_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { group_listing_ref(listing) }.group_type_c)
}

/// Returns the group protocol (borrowed). Java's `protocol()`; the empty string
/// for a classic group that is not using a protocol.
///
/// # Safety
///
/// `listing` must be a valid borrowed group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_protocol(
    listing: *const kafka_admin_GroupListing_t,
) -> *const c_char {
    unsafe { group_listing_ref(listing) }.protocol_c.as_ptr()
}

/// Returns the `GroupState` name (borrowed), e.g. `"Stable"` or `"Empty"`, or
/// null when Java's `groupState()` is `Optional.empty()`.
///
/// `GroupState` has no numeric id in Java, so its `toString()` name is the
/// contract.
///
/// # Safety
///
/// `listing` must be a valid borrowed group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_group_state(
    listing: *const kafka_admin_GroupListing_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { group_listing_ref(listing) }.group_state_c)
}

/// Returns whether this is a simple consumer group. Java's
/// `isSimpleConsumerGroup()`: a classic group with an empty protocol.
///
/// # Safety
///
/// `listing` must be a valid borrowed group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_is_simple_consumer_group(
    listing: *const kafka_admin_GroupListing_t,
) -> bool {
    unsafe { group_listing_ref(listing) }.is_simple_consumer_group
}

/// Opaque handle to a `ConsumerGroupListing` (Java's
/// `org.apache.kafka.clients.admin.ConsumerGroupListing`, deprecated since 4.1
/// in favour of `GroupListing`).
///
/// Borrowed from the owning `list_consumer_groups` result handle; valid until
/// that handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_ConsumerGroupListing_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ConsumerGroupListing_t`].
struct ConsumerGroupListingInner {
    group_id_c: CString,
    is_simple_consumer_group: bool,
    group_state_c: Option<CString>,
    state_c: Option<CString>,
    group_type_c: Option<CString>,
}

impl ConsumerGroupListingInner {
    #[allow(deprecated)]
    fn new(listing: &ConsumerGroupListing) -> Self {
        Self {
            group_id_c: to_cstring(listing.group_id()),
            is_simple_consumer_group: listing.is_simple_consumer_group(),
            group_state_c: listing.group_state().map(|s| to_cstring(s.name())),
            state_c: listing.state().map(|s| to_cstring(s.name())),
            group_type_c: listing.group_type().map(|t| to_cstring(t.name())),
        }
    }
}

/// Casts a `*const kafka_admin_ConsumerGroupListing_t` to a reference.
///
/// # Safety
///
/// `listing` must be a non-null borrowed pointer from a `list_consumer_groups`
/// result getter.
unsafe fn consumer_group_listing_ref(
    listing: *const kafka_admin_ConsumerGroupListing_t,
) -> &'static ConsumerGroupListingInner {
    unsafe { &*(listing as *const ConsumerGroupListingInner) }
}

/// Returns the consumer group id (borrowed). Java's `groupId()`.
///
/// # Safety
///
/// `listing` must be a valid borrowed consumer-group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupListing_group_id(
    listing: *const kafka_admin_ConsumerGroupListing_t,
) -> *const c_char {
    unsafe { consumer_group_listing_ref(listing) }.group_id_c.as_ptr()
}

/// Returns whether the group is simple. Java's `isSimpleConsumerGroup()`.
///
/// # Safety
///
/// `listing` must be a valid borrowed consumer-group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupListing_is_simple_consumer_group(
    listing: *const kafka_admin_ConsumerGroupListing_t,
) -> bool {
    unsafe { consumer_group_listing_ref(listing) }.is_simple_consumer_group
}

/// Returns the `GroupState` name (borrowed), or null when Java's
/// `groupState()` is `Optional.empty()`.
///
/// # Safety
///
/// `listing` must be a valid borrowed consumer-group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupListing_group_state(
    listing: *const kafka_admin_ConsumerGroupListing_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { consumer_group_listing_ref(listing) }.group_state_c)
}

/// Returns the deprecated `ConsumerGroupState` name (borrowed), or null when
/// Java's `state()` is `Optional.empty()`.
///
/// This is Java's deprecated `state()`, which maps `groupState()` through
/// `ConsumerGroupState.parse(...)`; the two therefore differ only for the group
/// states `ConsumerGroupState` does not model.
///
/// # Safety
///
/// `listing` must be a valid borrowed consumer-group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupListing_state(
    listing: *const kafka_admin_ConsumerGroupListing_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { consumer_group_listing_ref(listing) }.state_c)
}

/// Returns the `GroupType` name (borrowed), or null when Java's `type()` is
/// `Optional.empty()`.
///
/// # Safety
///
/// `listing` must be a valid borrowed consumer-group-listing pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupListing_group_type(
    listing: *const kafka_admin_ConsumerGroupListing_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { consumer_group_listing_ref(listing) }.group_type_c)
}

/// Opaque handle to a `MemberAssignment` (Java's
/// `org.apache.kafka.clients.admin.MemberAssignment`).
///
/// Borrowed from the owning `MemberDescription`; valid until the enclosing
/// result handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_MemberAssignment_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_MemberAssignment_t`].
///
/// Java's `topicPartitions()` is an unordered `Set`, so C sorts it by
/// `(topic, partition)` to make index addressing reproducible.
struct MemberAssignmentInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
}

impl MemberAssignmentInner {
    fn new(assignment: &MemberAssignment) -> Self {
        let mut sorted: Vec<&TopicPartition> = assignment.topic_partitions().iter().collect();
        sorted.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
        Self {
            topics: sorted.iter().map(|tp| to_cstring(tp.topic())).collect(),
            partitions: sorted.iter().map(|tp| tp.partition()).collect(),
        }
    }
}

/// Casts a `*const kafka_admin_MemberAssignment_t` to a reference.
///
/// # Safety
///
/// `assignment` must be a non-null borrowed pointer from a `MemberDescription`
/// getter.
unsafe fn member_assignment_ref(assignment: *const kafka_admin_MemberAssignment_t) -> &'static MemberAssignmentInner {
    unsafe { &*(assignment as *const MemberAssignmentInner) }
}

/// Returns the number of assigned partitions. Java's `topicPartitions()` size.
///
/// # Safety
///
/// `assignment` must be a valid borrowed member-assignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberAssignment_count(assignment: *const kafka_admin_MemberAssignment_t) -> i32 {
    unsafe { member_assignment_ref(assignment) }.topics.len() as i32
}

/// Returns the topic of the assigned partition at `index` (borrowed), or null
/// if out of range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `assignment` must be a valid borrowed member-assignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberAssignment_get_topic(
    assignment: *const kafka_admin_MemberAssignment_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { member_assignment_ref(assignment) }.topics, index)
}

/// Returns the id of the assigned partition at `index`, or -1 if out of range.
///
/// # Safety
///
/// `assignment` must be a valid borrowed member-assignment pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberAssignment_get_partition(
    assignment: *const kafka_admin_MemberAssignment_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return -1;
    }
    unsafe { member_assignment_ref(assignment) }
        .partitions
        .get(index as usize)
        .copied()
        .unwrap_or(-1)
}

/// Opaque handle to a `MemberDescription` (Java's
/// `org.apache.kafka.clients.admin.MemberDescription`).
///
/// Borrowed from the owning group description; valid until the enclosing result
/// handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_MemberDescription_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_MemberDescription_t`].
struct MemberDescriptionInner {
    consumer_id_c: CString,
    group_instance_id_c: Option<CString>,
    rack_id_c: Option<CString>,
    client_id_c: CString,
    host_c: CString,
    assignment: MemberAssignmentInner,
    target_assignment: Option<MemberAssignmentInner>,
    member_epoch: Option<i32>,
    upgraded: Option<bool>,
}

impl MemberDescriptionInner {
    fn new(member: &MemberDescription) -> Self {
        Self {
            consumer_id_c: to_cstring(member.consumer_id()),
            group_instance_id_c: member.group_instance_id().map(to_cstring),
            rack_id_c: member.rack_id().map(to_cstring),
            client_id_c: to_cstring(member.client_id()),
            host_c: to_cstring(member.host()),
            assignment: MemberAssignmentInner::new(member.assignment()),
            target_assignment: member.target_assignment().map(MemberAssignmentInner::new),
            member_epoch: member.member_epoch(),
            upgraded: member.upgraded(),
        }
    }
}

/// Casts a `*const kafka_admin_MemberDescription_t` to a reference.
///
/// # Safety
///
/// `member` must be a non-null borrowed pointer from a group-description
/// getter.
unsafe fn member_description_ref(member: *const kafka_admin_MemberDescription_t) -> &'static MemberDescriptionInner {
    unsafe { &*(member as *const MemberDescriptionInner) }
}

/// Returns the consumer id (borrowed). Java's `consumerId()`.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_consumer_id(
    member: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    unsafe { member_description_ref(member) }.consumer_id_c.as_ptr()
}

/// Returns the group instance id (borrowed), or null when Java's
/// `groupInstanceId()` is `Optional.empty()` — i.e. the member is not a static
/// member.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_group_instance_id(
    member: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { member_description_ref(member) }.group_instance_id_c)
}

/// Returns the rack id (borrowed), or null when Java's `rackId()` is
/// `Optional.empty()`.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_rack_id(
    member: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { member_description_ref(member) }.rack_id_c)
}

/// Returns the client id (borrowed). Java's `clientId()`.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_client_id(
    member: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    unsafe { member_description_ref(member) }.client_id_c.as_ptr()
}

/// Returns the member host (borrowed). Java's `host()`.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_host(
    member: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    unsafe { member_description_ref(member) }.host_c.as_ptr()
}

/// Returns the member's current assignment (borrowed, never null). Java's
/// `assignment()`, which is a non-optional `MemberAssignment` — an unassigned
/// member has an assignment with zero partitions.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_assignment(
    member: *const kafka_admin_MemberDescription_t,
) -> *const kafka_admin_MemberAssignment_t {
    &unsafe { member_description_ref(member) }.assignment as *const MemberAssignmentInner
        as *const kafka_admin_MemberAssignment_t
}

/// Returns the member's target assignment (borrowed), or null when Java's
/// `targetAssignment()` is `Optional.empty()` — which is the case for every
/// classic-protocol member. A null return is therefore "no target assignment
/// was reported", distinct from a non-null handle whose count is 0.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_target_assignment(
    member: *const kafka_admin_MemberDescription_t,
) -> *const kafka_admin_MemberAssignment_t {
    match &unsafe { member_description_ref(member) }.target_assignment {
        Some(assignment) => assignment as *const MemberAssignmentInner as *const kafka_admin_MemberAssignment_t,
        None => std::ptr::null(),
    }
}

/// Writes the member epoch to `*out_epoch` and returns `true`, or returns
/// `false` when Java's `memberEpoch()` is `Optional.empty()`.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer; `out_epoch`
/// must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_member_epoch(
    member: *const kafka_admin_MemberDescription_t,
    out_epoch: *mut i32,
) -> bool {
    unsafe { write_optional(member_description_ref(member).member_epoch, out_epoch) }
}

/// Writes whether the member has been upgraded to the consumer protocol to
/// `*out_upgraded` and returns `true`, or returns `false` when Java's
/// `upgraded()` is `Optional.empty()`.
///
/// # Safety
///
/// `member` must be a valid borrowed member-description pointer;
/// `out_upgraded` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_upgraded(
    member: *const kafka_admin_MemberDescription_t,
    out_upgraded: *mut bool,
) -> bool {
    unsafe { write_optional(member_description_ref(member).upgraded, out_upgraded) }
}

/// Returns the borrowed member pointer at `index` in `members`, or null when
/// `index` is out of range.
fn member_at(members: &[MemberDescriptionInner], index: i32) -> *const kafka_admin_MemberDescription_t {
    if index < 0 {
        return std::ptr::null();
    }
    match members.get(index as usize) {
        Some(member) => member as *const MemberDescriptionInner as *const kafka_admin_MemberDescription_t,
        None => std::ptr::null(),
    }
}

/// Returns the `AclOperation` code at `index`, or -1 when out of range.
fn authorized_operation_at(codes: &[i32], index: i32) -> i32 {
    if index < 0 {
        return -1;
    }
    codes.get(index as usize).copied().unwrap_or(-1)
}

/// Returns a borrowed [`kafka_common_Node_t`] for an optional coordinator.
fn optional_node_ptr(node: Option<&Node>) -> *const kafka_common_Node_t {
    match node {
        Some(node) => node as *const Node as *const kafka_common_Node_t,
        None => std::ptr::null(),
    }
}

/// Opaque handle to a `ConsumerGroupDescription` (Java's
/// `org.apache.kafka.clients.admin.ConsumerGroupDescription`).
///
/// Borrowed from the owning `describe_consumer_groups` result handle; valid
/// until that handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_ConsumerGroupDescription_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ConsumerGroupDescription_t`].
struct ConsumerGroupDescriptionInner {
    group_id_c: CString,
    is_simple_consumer_group: bool,
    members: Vec<MemberDescriptionInner>,
    partition_assignor_c: CString,
    group_type_c: CString,
    state_c: CString,
    group_state_c: CString,
    coordinator: Option<Node>,
    /// `AclOperation` wire codes (Java's `AclOperation.code()`), ascending.
    authorized_operations: Vec<i32>,
    group_epoch: Option<i32>,
    target_assignment_epoch: Option<i32>,
}

impl ConsumerGroupDescriptionInner {
    fn new(description: &ConsumerGroupDescription) -> Self {
        Self {
            group_id_c: to_cstring(description.group_id()),
            is_simple_consumer_group: description.is_simple_consumer_group(),
            members: description.members().iter().map(MemberDescriptionInner::new).collect(),
            partition_assignor_c: to_cstring(description.partition_assignor()),
            group_type_c: to_cstring(description.group_type().name()),
            state_c: to_cstring(description.state().name()),
            group_state_c: to_cstring(description.group_state().name()),
            coordinator: description.coordinator().cloned(),
            authorized_operations: description
                .authorized_operations()
                .iter()
                .map(|op| i32::from(op.code()))
                .collect(),
            group_epoch: description.group_epoch(),
            target_assignment_epoch: description.target_assignment_epoch(),
        }
    }
}

/// Casts a `*const kafka_admin_ConsumerGroupDescription_t` to a reference.
///
/// # Safety
///
/// `description` must be a non-null borrowed pointer from a
/// `describe_consumer_groups` result getter.
unsafe fn consumer_group_description_ref(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> &'static ConsumerGroupDescriptionInner {
    unsafe { &*(description as *const ConsumerGroupDescriptionInner) }
}

/// Returns the group id (borrowed). Java's `groupId()`.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_group_id(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const c_char {
    unsafe { consumer_group_description_ref(description) }.group_id_c.as_ptr()
}

/// Returns whether the group is simple. Java's `isSimpleConsumerGroup()`.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_is_simple_consumer_group(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> bool {
    unsafe { consumer_group_description_ref(description) }.is_simple_consumer_group
}

/// Returns the number of members in the group. Java's `members()` size.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_member_count(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> i32 {
    unsafe { consumer_group_description_ref(description) }.members.len() as i32
}

/// Returns the member at `index` (borrowed), or null if out of range. Members
/// keep the order Java's `members()` reports them in.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_get_member(
    description: *const kafka_admin_ConsumerGroupDescription_t,
    index: i32,
) -> *const kafka_admin_MemberDescription_t {
    member_at(&unsafe { consumer_group_description_ref(description) }.members, index)
}

/// Returns the partition assignor name (borrowed). Java's
/// `partitionAssignor()`; the empty string for a consumer-protocol group, which
/// assigns partitions server-side.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_partition_assignor(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const c_char {
    unsafe { consumer_group_description_ref(description) }
        .partition_assignor_c
        .as_ptr()
}

/// Returns the `GroupType` name (borrowed) — Java's `toString()` value, i.e.
/// `"Consumer"`, `"Classic"`, `"Share"`, `"Streams"` or `"Unknown"`. Java's
/// `type()`, which is non-optional here.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_group_type(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const c_char {
    unsafe { consumer_group_description_ref(description) }.group_type_c.as_ptr()
}

/// Returns the deprecated `ConsumerGroupState` name (borrowed), e.g.
/// `"Stable"`. Java's deprecated `state()`, which maps `groupState()` through
/// `ConsumerGroupState.parse(...)`.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_state(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const c_char {
    unsafe { consumer_group_description_ref(description) }.state_c.as_ptr()
}

/// Returns the `GroupState` name (borrowed), e.g. `"Stable"`. Java's
/// `groupState()`.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_group_state(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const c_char {
    unsafe { consumer_group_description_ref(description) }.group_state_c.as_ptr()
}

/// Returns the group coordinator (borrowed), or null when Java's
/// `coordinator()` reported none.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_coordinator(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const kafka_common_Node_t {
    optional_node_ptr(unsafe { consumer_group_description_ref(description) }.coordinator.as_ref())
}

/// Returns the number of authorized operations reported for the group (0 when
/// the request did not ask for them).
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_authorized_operation_count(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> i32 {
    unsafe { consumer_group_description_ref(description) }
        .authorized_operations
        .len() as i32
}

/// Returns the `AclOperation` wire code (Java's `AclOperation.code()`) of the
/// authorized operation at `index`, or -1 if out of range.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_authorized_operation(
    description: *const kafka_admin_ConsumerGroupDescription_t,
    index: i32,
) -> i32 {
    authorized_operation_at(
        &unsafe { consumer_group_description_ref(description) }.authorized_operations,
        index,
    )
}

/// Writes the group epoch to `*out_epoch` and returns `true`, or returns
/// `false` when Java's `groupEpoch()` is `Optional.empty()` (a classic group).
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer;
/// `out_epoch` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_group_epoch(
    description: *const kafka_admin_ConsumerGroupDescription_t,
    out_epoch: *mut i32,
) -> bool {
    unsafe { write_optional(consumer_group_description_ref(description).group_epoch, out_epoch) }
}

/// Writes the target assignment epoch to `*out_epoch` and returns `true`, or
/// returns `false` when Java's `targetAssignmentEpoch()` is `Optional.empty()`.
///
/// # Safety
///
/// `description` must be a valid borrowed consumer-group-description pointer;
/// `out_epoch` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_target_assignment_epoch(
    description: *const kafka_admin_ConsumerGroupDescription_t,
    out_epoch: *mut i32,
) -> bool {
    unsafe { write_optional(consumer_group_description_ref(description).target_assignment_epoch, out_epoch) }
}

/// Opaque handle to a `ClassicGroupDescription` (Java's
/// `org.apache.kafka.clients.admin.ClassicGroupDescription`).
///
/// Borrowed from the owning `describe_classic_groups` result handle; valid
/// until that handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_ClassicGroupDescription_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ClassicGroupDescription_t`].
struct ClassicGroupDescriptionInner {
    group_id_c: CString,
    protocol_c: CString,
    protocol_data_c: CString,
    is_simple_consumer_group: bool,
    members: Vec<MemberDescriptionInner>,
    state_c: CString,
    coordinator: Option<Node>,
    /// `AclOperation` wire codes (Java's `AclOperation.code()`), ascending.
    authorized_operations: Vec<i32>,
}

impl ClassicGroupDescriptionInner {
    fn new(description: &ClassicGroupDescription) -> Self {
        Self {
            group_id_c: to_cstring(description.group_id()),
            protocol_c: to_cstring(description.protocol()),
            protocol_data_c: to_cstring(description.protocol_data()),
            is_simple_consumer_group: description.is_simple_consumer_group(),
            members: description.members().iter().map(MemberDescriptionInner::new).collect(),
            state_c: to_cstring(description.state().name()),
            coordinator: description.coordinator().cloned(),
            authorized_operations: description
                .authorized_operations()
                .iter()
                .map(|op| i32::from(op.code()))
                .collect(),
        }
    }
}

/// Casts a `*const kafka_admin_ClassicGroupDescription_t` to a reference.
///
/// # Safety
///
/// `description` must be a non-null borrowed pointer from a
/// `describe_classic_groups` result getter.
unsafe fn classic_group_description_ref(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> &'static ClassicGroupDescriptionInner {
    unsafe { &*(description as *const ClassicGroupDescriptionInner) }
}

/// Returns the group id (borrowed). Java's `groupId()`.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_group_id(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    unsafe { classic_group_description_ref(description) }.group_id_c.as_ptr()
}

/// Returns the group protocol type (borrowed). Java's `protocol()`.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_protocol(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    unsafe { classic_group_description_ref(description) }.protocol_c.as_ptr()
}

/// Returns the protocol data (borrowed), i.e. the assignment strategy the group
/// selected. Java's `protocolData()`.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_protocol_data(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    unsafe { classic_group_description_ref(description) }.protocol_data_c.as_ptr()
}

/// Returns whether the group is simple. Java's `isSimpleConsumerGroup()`.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_is_simple_consumer_group(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> bool {
    unsafe { classic_group_description_ref(description) }.is_simple_consumer_group
}

/// Returns the number of members in the group. Java's `members()` size.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_member_count(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> i32 {
    unsafe { classic_group_description_ref(description) }.members.len() as i32
}

/// Returns the member at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_get_member(
    description: *const kafka_admin_ClassicGroupDescription_t,
    index: i32,
) -> *const kafka_admin_MemberDescription_t {
    member_at(&unsafe { classic_group_description_ref(description) }.members, index)
}

/// Returns the `ClassicGroupState` name (borrowed), e.g. `"Stable"`. Java's
/// `state()`.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_state(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    unsafe { classic_group_description_ref(description) }.state_c.as_ptr()
}

/// Returns the group coordinator (borrowed), or null when Java's
/// `coordinator()` reported none.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_coordinator(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> *const kafka_common_Node_t {
    optional_node_ptr(unsafe { classic_group_description_ref(description) }.coordinator.as_ref())
}

/// Returns the number of authorized operations reported for the group (0 when
/// the request did not ask for them).
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_authorized_operation_count(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> i32 {
    unsafe { classic_group_description_ref(description) }
        .authorized_operations
        .len() as i32
}

/// Returns the `AclOperation` wire code (Java's `AclOperation.code()`) of the
/// authorized operation at `index`, or -1 if out of range.
///
/// # Safety
///
/// `description` must be a valid borrowed classic-group-description pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_authorized_operation(
    description: *const kafka_admin_ClassicGroupDescription_t,
    index: i32,
) -> i32 {
    authorized_operation_at(
        &unsafe { classic_group_description_ref(description) }.authorized_operations,
        index,
    )
}

/// Opaque handle to one group's committed offsets: Java's
/// `Map<TopicPartition, OffsetAndMetadata>`, the value of a
/// `listConsumerGroupOffsets` per-group future.
///
/// There is no `kafka_admin_OffsetAndMetadata_t`: `OffsetAndMetadata` is
/// `org.apache.kafka.clients.consumer.OffsetAndMetadata`, so an
/// `kafka_admin_`-prefixed handle would be mis-namespaced (CLAUDE.md §3), and
/// the crate's existing `kafka_consumer_OffsetAndMetadata_t` is private to the
/// consumer FFI module. Its three fields are therefore flattened into indexed
/// accessors on this map handle, exactly as B2 flattened
/// `LogDirDescription.ReplicaInfo` onto [`kafka_admin_LogDirDescription_t`].
///
/// Borrowed from the owning `list_consumer_group_offsets` result handle; valid
/// until that handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_admin_OffsetAndMetadataMap_t {
    _private: [u8; 0],
}

/// One `(TopicPartition, OffsetAndMetadata)` pair, flattened for C.
///
/// Java's map value is nullable: a partition the group has no committed offset
/// for is present with a null value. `offset` is therefore `Option`, and
/// [`kafka_admin_OffsetAndMetadataMap_has_offset`] is the discriminant.
struct GroupOffsetEntry {
    topic_c: CString,
    partition: i32,
    offset: Option<OffsetAndMetadata>,
    metadata_c: Option<CString>,
}

/// Backing state for [`kafka_admin_OffsetAndMetadataMap_t`].
struct OffsetAndMetadataMapInner {
    entries: Vec<GroupOffsetEntry>,
}

impl OffsetAndMetadataMapInner {
    fn new(offsets: GroupOffsets) -> Self {
        let entries: Vec<GroupOffsetEntry> = sorted_partition_entries(offsets)
            .into_iter()
            .map(|(tp, offset)| GroupOffsetEntry {
                topic_c: to_cstring(tp.topic()),
                partition: tp.partition(),
                metadata_c: offset.as_ref().map(|o| to_cstring(o.metadata())),
                offset,
            })
            .collect();
        Self { entries }
    }
}

/// Casts a `*const kafka_admin_OffsetAndMetadataMap_t` to a reference.
///
/// # Safety
///
/// `map` must be a non-null borrowed pointer from a
/// `list_consumer_group_offsets` result getter.
unsafe fn offset_and_metadata_map_ref(
    map: *const kafka_admin_OffsetAndMetadataMap_t,
) -> &'static OffsetAndMetadataMapInner {
    unsafe { &*(map as *const OffsetAndMetadataMapInner) }
}

/// Returns the entry at `index`, or `None` when out of range.
fn group_offset_at(map: &OffsetAndMetadataMapInner, index: i32) -> Option<&GroupOffsetEntry> {
    if index < 0 {
        return None;
    }
    map.entries.get(index as usize)
}

/// Returns the number of partitions in this group's offset map.
///
/// # Safety
///
/// `map` must be a valid borrowed offset-map pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetAndMetadataMap_count(map: *const kafka_admin_OffsetAndMetadataMap_t) -> i32 {
    unsafe { offset_and_metadata_map_ref(map) }.entries.len() as i32
}

/// Returns the topic of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `map` must be a valid borrowed offset-map pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetAndMetadataMap_get_topic(
    map: *const kafka_admin_OffsetAndMetadataMap_t,
    index: i32,
) -> *const c_char {
    match group_offset_at(unsafe { offset_and_metadata_map_ref(map) }, index) {
        Some(entry) => entry.topic_c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `map` must be a valid borrowed offset-map pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetAndMetadataMap_get_partition(
    map: *const kafka_admin_OffsetAndMetadataMap_t,
    index: i32,
) -> i32 {
    match group_offset_at(unsafe { offset_and_metadata_map_ref(map) }, index) {
        Some(entry) => entry.partition,
        None => -1,
    }
}

/// Returns whether the entry at `index` carries a committed offset.
///
/// Java's map value is nullable: `listConsumerGroupOffsets` reports a requested
/// partition the group has never committed for as present with a **null**
/// `OffsetAndMetadata`. `false` therefore means "no committed offset", which is
/// distinct from a committed offset of 0. When this returns `false`,
/// [`kafka_admin_OffsetAndMetadataMap_get_offset`] returns -1,
/// [`kafka_admin_OffsetAndMetadataMap_get_metadata`] returns null and
/// [`kafka_admin_OffsetAndMetadataMap_get_leader_epoch`] returns `false`.
///
/// # Safety
///
/// `map` must be a valid borrowed offset-map pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetAndMetadataMap_has_offset(
    map: *const kafka_admin_OffsetAndMetadataMap_t,
    index: i32,
) -> bool {
    match group_offset_at(unsafe { offset_and_metadata_map_ref(map) }, index) {
        Some(entry) => entry.offset.is_some(),
        None => false,
    }
}

/// Returns the committed offset of the entry at `index`, or -1 if out of range
/// or the group has no committed offset for it (see
/// [`kafka_admin_OffsetAndMetadataMap_has_offset`]). Committed offsets are
/// never negative — Java's `OffsetAndMetadata` constructor rejects them — so -1
/// is unambiguous.
///
/// # Safety
///
/// `map` must be a valid borrowed offset-map pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetAndMetadataMap_get_offset(
    map: *const kafka_admin_OffsetAndMetadataMap_t,
    index: i32,
) -> i64 {
    match group_offset_at(unsafe { offset_and_metadata_map_ref(map) }, index) {
        Some(entry) => entry.offset.as_ref().map_or(-1, OffsetAndMetadata::offset),
        None => -1,
    }
}

/// Returns the commit metadata of the entry at `index` (borrowed), or null if
/// out of range or the group has no committed offset for it. Java normalises an
/// absent metadata string to `""`, so a committed offset always yields a
/// non-null (possibly empty) string.
///
/// # Safety
///
/// `map` must be a valid borrowed offset-map pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetAndMetadataMap_get_metadata(
    map: *const kafka_admin_OffsetAndMetadataMap_t,
    index: i32,
) -> *const c_char {
    match group_offset_at(unsafe { offset_and_metadata_map_ref(map) }, index) {
        Some(entry) => optional_cstring_ptr(&entry.metadata_c),
        None => std::ptr::null(),
    }
}

/// Writes the leader epoch of the entry at `index` to `*out_epoch` and returns
/// `true`, or returns `false` when out of range, when the group has no
/// committed offset for it, or when Java's `leaderEpoch()` is
/// `Optional.empty()`.
///
/// # Safety
///
/// `map` must be a valid borrowed offset-map pointer; `out_epoch` must be null
/// or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetAndMetadataMap_get_leader_epoch(
    map: *const kafka_admin_OffsetAndMetadataMap_t,
    index: i32,
    out_epoch: *mut i32,
) -> bool {
    let epoch = group_offset_at(unsafe { offset_and_metadata_map_ref(map) }, index)
        .and_then(|entry| entry.offset.as_ref())
        .and_then(OffsetAndMetadata::leader_epoch);
    unsafe { write_optional(epoch, out_epoch) }
}

// ---------------------------------------------------------------------------
// Group input marshaling and submission helpers
//
// Group RPCs are keyed by group id rather than by topic partition, so keys are
// plain C strings. As in B3, every Java `Optional` gets an explicit boolean
// discriminant beside the payload (`all_partitions[i]` for a per-group
// `ListConsumerGroupOffsetsSpec`, `remove_all` for the no-members
// `RemoveMembersFromConsumerGroupOptions` constructor, `has_leader_epoch[i]`
// for an `OffsetAndMetadata`'s leader epoch), so "absent" and "present but
// empty" stay distinguishable.
// ---------------------------------------------------------------------------

/// Per-key `KafkaFuture<Void>` outcomes keyed by a string (group id or group
/// instance id).
type GroupVoidOutcomes = HashMap<String, Result<(), KafkaError>>;
/// Per-partition `KafkaFuture<Void>` outcomes.
type PartitionVoidOutcomes = HashMap<TopicPartition, Result<(), KafkaError>>;
/// The `valid()` listings and unkeyed `errors()` of `listGroups`.
type ListGroupsOutcome = (Vec<GroupListing>, Vec<KafkaError>);
/// The `valid()` listings and unkeyed `errors()` of `listConsumerGroups`.
#[allow(deprecated)]
type ListConsumerGroupsOutcome = (Vec<ConsumerGroupListing>, Vec<KafkaError>);
/// Per-group outcomes of `describeConsumerGroups`.
type DescribeConsumerGroupsOutcomes = HashMap<String, Result<ConsumerGroupDescription, KafkaError>>;
/// Per-group outcomes of `describeClassicGroups`.
type DescribeClassicGroupsOutcomes = HashMap<String, Result<ClassicGroupDescription, KafkaError>>;
/// Per-group outcomes of `listConsumerGroupOffsets`.
type ListConsumerGroupOffsetsOutcomes = HashMap<String, Result<GroupOffsets, KafkaError>>;

/// Reads a required C string parameter.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] when `text` is NULL. Java's group-id
/// parameters are non-null by contract, and a NULL here would otherwise be
/// dereferenced; reporting it is cheaper than the alternative of silently
/// substituting the empty string, which the broker would reject with a much
/// less specific error.
///
/// # Safety
///
/// `text` must be null or a valid C string.
unsafe fn read_required_string(text: *const c_char, parameter: &str) -> Result<String, KafkaError> {
    if text.is_null() {
        return Err(KafkaError::illegal_argument(format!("{parameter} must not be null")));
    }
    Ok(unsafe { CStr::from_ptr(text) }.to_string_lossy().to_string())
}

/// Returns `partitions[index]`, or -1 when `index` is out of range.
fn partition_at(partitions: &[i32], index: i32) -> i32 {
    if index < 0 {
        return -1;
    }
    partitions.get(index as usize).copied().unwrap_or(-1)
}

/// Returns a borrowed error pointer for `errors[index]`, or null when the key
/// succeeded or `index` is out of range.
fn optional_error_at(errors: &[Option<KafkaErrorInner>], index: i32) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Splits string-keyed void outcomes into the parallel key / error vectors a
/// per-key-error-only result handle stores.
fn flatten_keyed_void_outcomes(outcomes: GroupVoidOutcomes) -> (Vec<CString>, Vec<Option<KafkaErrorInner>>) {
    let entries = sorted_entries(outcomes);
    let mut keys = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (key, outcome) in entries {
        keys.push(to_cstring(&key));
        errors.push(outcome.err().map(error_inner));
    }
    (keys, errors)
}

/// Splits partition-keyed void outcomes into the parallel topic / partition /
/// error vectors a per-key-error-only result handle stores.
fn flatten_partition_void_outcomes(
    outcomes: PartitionVoidOutcomes,
) -> (Vec<CString>, Vec<i32>, Vec<Option<KafkaErrorInner>>) {
    let entries = sorted_partition_entries(outcomes);
    let mut topics = Vec::with_capacity(entries.len());
    let mut partitions = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (tp, outcome) in entries {
        topics.push(to_cstring(tp.topic()));
        partitions.push(tp.partition());
        errors.push(outcome.err().map(error_inner));
    }
    (topics, partitions, errors)
}

/// Parses `count` `GroupState` names into a set.
///
/// Names are Java's `GroupState.toString()` values (`"Stable"`, `"Empty"`, …);
/// matching is case-insensitive and an unrecognised name becomes
/// `GroupState.UNKNOWN`, exactly as Java's `GroupState.parse(String)` does. An
/// empty or NULL array leaves the filter unset, i.e. "every state".
///
/// # Safety
///
/// `names` must be null or have `count` entries, each NULL or a valid C string.
unsafe fn read_group_states(names: *const *const c_char, count: i32) -> HashSet<GroupState> {
    unsafe { read_strings(names, count) }
        .iter()
        .map(|name| GroupState::parse(name))
        .collect()
}

/// Parses `count` `GroupType` names into a set.
///
/// Names are Java's `GroupType.toString()` values (`"Consumer"`, `"Classic"`,
/// `"Share"`, `"Streams"`); matching is case-insensitive and an unrecognised
/// name becomes `GroupType.UNKNOWN`, as in Java's `GroupType.parse(String)`.
///
/// # Safety
///
/// `names` must be null or have `count` entries, each NULL or a valid C string.
unsafe fn read_group_types(names: *const *const c_char, count: i32) -> HashSet<GroupType> {
    unsafe { read_strings(names, count) }
        .iter()
        .map(|name| GroupType::parse(name))
        .collect()
}

/// Builds `ListGroupsOptions` from the flat C option parameters.
///
/// # Safety
///
/// The three name arrays must be null or have their stated counts.
unsafe fn list_groups_options(
    group_states: *const *const c_char,
    group_state_count: i32,
    protocol_types: *const *const c_char,
    protocol_type_count: i32,
    types: *const *const c_char,
    type_count: i32,
    timeout_ms: i32,
) -> ListGroupsOptions {
    ListGroupsOptions::new()
        .in_group_states(unsafe { read_group_states(group_states, group_state_count) })
        .with_protocol_types(
            unsafe { read_strings(protocol_types, protocol_type_count) }
                .into_iter()
                .collect(),
        )
        .with_types(unsafe { read_group_types(types, type_count) })
        .timeout_ms(option_timeout(timeout_ms))
}

/// Builds `ListConsumerGroupsOptions` from the flat C option parameters.
///
/// Java also has the deprecated `inStates(Set<ConsumerGroupState>)`, which is
/// defined as `inGroupStates(states.map(s -> GroupState.parse(s.toString())))`.
/// The two therefore accept the same strings here, so C exposes only
/// `group_states`; a caller holding `ConsumerGroupState` names passes them in
/// the same array.
///
/// # Safety
///
/// The two name arrays must be null or have their stated counts.
#[allow(deprecated)]
unsafe fn list_consumer_groups_options(
    group_states: *const *const c_char,
    group_state_count: i32,
    types: *const *const c_char,
    type_count: i32,
    timeout_ms: i32,
) -> ListConsumerGroupsOptions {
    ListConsumerGroupsOptions::new()
        .in_group_states(unsafe { read_group_states(group_states, group_state_count) })
        .with_types(unsafe { read_group_types(types, type_count) })
        .timeout_ms(option_timeout(timeout_ms))
}

/// Builds `DescribeConsumerGroupsOptions` from the flat C option parameters.
fn describe_consumer_groups_options(
    timeout_ms: i32,
    include_authorized_operations: bool,
) -> DescribeConsumerGroupsOptions {
    DescribeConsumerGroupsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .include_authorized_operations(include_authorized_operations)
}

/// Builds `DescribeClassicGroupsOptions` from the flat C option parameters.
fn describe_classic_groups_options(
    timeout_ms: i32,
    include_authorized_operations: bool,
) -> DescribeClassicGroupsOptions {
    DescribeClassicGroupsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .include_authorized_operations(include_authorized_operations)
}

/// Builds `ListConsumerGroupOffsetsOptions` from the flat C option parameters.
fn list_consumer_group_offsets_options(timeout_ms: i32, require_stable: bool) -> ListConsumerGroupOffsetsOptions {
    ListConsumerGroupOffsetsOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .require_stable(require_stable)
}

/// Builds `AlterConsumerGroupOffsetsOptions` from the flat C option parameters.
fn alter_consumer_group_offsets_options(timeout_ms: i32) -> AlterConsumerGroupOffsetsOptions {
    AlterConsumerGroupOffsetsOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Builds `DeleteConsumerGroupOffsetsOptions` from the flat C option parameters.
fn delete_consumer_group_offsets_options(timeout_ms: i32) -> DeleteConsumerGroupOffsetsOptions {
    DeleteConsumerGroupOffsetsOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Builds `DeleteConsumerGroupsOptions` from the flat C option parameters.
fn delete_consumer_groups_options(timeout_ms: i32) -> DeleteConsumerGroupsOptions {
    DeleteConsumerGroupsOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Builds `RemoveMembersFromConsumerGroupOptions` from the flat C option
/// parameters.
///
/// `remove_all` selects Java's no-argument constructor ("remove every member");
/// otherwise the `Collection<MemberToRemove>` constructor is used, which throws
/// `IllegalArgumentException("Invalid empty members has been provided")` on an
/// empty collection. A NULL `reason` is Java's unset reason.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] when `remove_all` is false and no
/// group instance id was supplied, mirroring Java.
///
/// # Safety
///
/// `group_instance_ids` must be null or have `member_count` entries, each NULL
/// or a valid C string; `reason` must be null or a valid C string.
unsafe fn remove_members_options(
    remove_all: bool,
    group_instance_ids: *const *const c_char,
    member_count: i32,
    reason: *const c_char,
    timeout_ms: i32,
) -> Result<RemoveMembersFromConsumerGroupOptions, KafkaError> {
    let mut options = if remove_all {
        // Java's `RemoveMembersFromConsumerGroupOptions()`: removeAll mode.
        RemoveMembersFromConsumerGroupOptions::default()
    } else {
        let members = unsafe { read_strings(group_instance_ids, member_count) }
            .into_iter()
            .map(MemberToRemove::new);
        RemoveMembersFromConsumerGroupOptions::new(members)?
    };
    if !reason.is_null() {
        options.reason(unsafe { CStr::from_ptr(reason) }.to_string_lossy().to_string());
    }
    Ok(options.timeout_ms(option_timeout(timeout_ms)))
}

/// Reads the per-group `ListConsumerGroupOffsetsSpec` map that
/// `listConsumerGroupOffsets` takes.
///
/// Entry `i` describes group `group_ids[i]`: when `all_partitions[i]` is true
/// the spec's topic partitions stay unset (Java's null `Collection`, meaning
/// "every partition the group has committed offsets for"); otherwise
/// `topics[i]` / `partitions[i]` hold `partition_counts[i]` parallel entries.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if a group id is NULL, or if the
/// same group id appears twice — Java takes a `Map`, where the second entry
/// would silently have replaced the first.
///
/// # Safety
///
/// `group_ids`, `all_partitions`, `topics`, `partitions` and `partition_counts`
/// must be null or have `group_count` readable entries each; for a group whose
/// `all_partitions` flag is false, `topics[i]` and `partitions[i]` must have
/// `partition_counts[i]` readable entries.
unsafe fn read_group_offsets_specs(
    group_ids: *const *const c_char,
    all_partitions: *const bool,
    topics: *const *const *const c_char,
    partitions: *const *const i32,
    partition_counts: *const i32,
    group_count: i32,
) -> Result<HashMap<String, ListConsumerGroupOffsetsSpec>, KafkaError> {
    let n = group_count.max(0) as usize;
    let mut specs = HashMap::with_capacity(n);
    if group_ids.is_null() || all_partitions.is_null() {
        return Ok(specs);
    }
    for i in 0..n {
        let id_ptr = unsafe { *group_ids.add(i) };
        if id_ptr.is_null() {
            return Err(KafkaError::illegal_argument(format!("group id at index {i} must not be null")));
        }
        let group_id = unsafe { CStr::from_ptr(id_ptr) }.to_string_lossy().to_string();
        let spec = if unsafe { *all_partitions.add(i) } {
            ListConsumerGroupOffsetsSpec::new()
        } else {
            let count = if partition_counts.is_null() {
                0
            } else {
                unsafe { *partition_counts.add(i) }
            };
            let group_topics = if topics.is_null() {
                std::ptr::null()
            } else {
                unsafe { *topics.add(i) }
            };
            let group_partitions = if partitions.is_null() {
                std::ptr::null()
            } else {
                unsafe { *partitions.add(i) }
            };
            ListConsumerGroupOffsetsSpec::new()
                .topic_partitions(Some(unsafe { read_topic_partitions(group_topics, group_partitions, count) }))
        };
        if specs.insert(group_id.clone(), spec).is_some() {
            return Err(KafkaError::illegal_argument(format!(
                "group id `{group_id}` appears more than once at index {i}"
            )));
        }
    }
    Ok(specs)
}

/// Reads the `Map<TopicPartition, OffsetAndMetadata>` that
/// `alterConsumerGroupOffsets` takes.
///
/// Entry `i` is `(topics[i], partitions[i]) -> OffsetAndMetadata(offsets[i],
/// leader_epochs[i] if has_leader_epoch[i], metadata[i])`. A NULL `metadata[i]`
/// is Java's null metadata, which the `OffsetAndMetadata` constructor
/// normalises to the empty string. `has_leader_epoch[i]` is the explicit
/// discriminant for Java's `Optional<Integer> leaderEpoch`, so an epoch of 0
/// stays distinguishable from an absent one.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if a topic entry is NULL or an
/// offset is negative — the latter mirroring Java's `OffsetAndMetadata`
/// constructor, which throws `IllegalArgumentException` for a negative offset.
///
/// # Safety
///
/// Every non-null array must have `count` readable entries; every topic and
/// metadata entry must be NULL or a valid C string.
unsafe fn read_alter_group_offsets(
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    metadata: *const *const c_char,
    leader_epochs: *const i32,
    has_leader_epoch: *const bool,
    count: i32,
) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
    let n = count.max(0) as usize;
    let mut out = HashMap::with_capacity(n);
    if topics.is_null() || partitions.is_null() || offsets.is_null() {
        return Ok(out);
    }
    for i in 0..n {
        let name_ptr = unsafe { *topics.add(i) };
        if name_ptr.is_null() {
            return Err(KafkaError::illegal_argument(format!("topic at index {i} must not be null")));
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        let tp = TopicPartition::new(name, unsafe { *partitions.add(i) });
        let epoch = if !has_leader_epoch.is_null() && unsafe { *has_leader_epoch.add(i) } && !leader_epochs.is_null() {
            Some(unsafe { *leader_epochs.add(i) })
        } else {
            None
        };
        let text = if metadata.is_null() {
            String::new()
        } else {
            let text_ptr = unsafe { *metadata.add(i) };
            if text_ptr.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(text_ptr) }.to_string_lossy().to_string()
            }
        };
        let offset = OffsetAndMetadata::with_leader_epoch(unsafe { *offsets.add(i) }, epoch, text)
            .map_err(|e| KafkaError::illegal_argument(format!("offset at index {i}: {}", e.message())))?;
        out.insert(tp, offset);
    }
    Ok(out)
}

/// Submits `listGroups` and returns a future over its `valid()` / `errors()`
/// split.
///
/// Java's `ListGroupsResult` derives both views from one source future, so
/// both are awaited before either is inspected — the `describeCluster`
/// discipline, which keeps a derived future from being abandoned.
fn submit_list_groups(
    admin: &dyn Admin,
    options: ListGroupsOptions,
) -> impl std::future::Future<Output = Result<ListGroupsOutcome, KafkaError>> + Send + use<> {
    let result = admin.list_groups(options);
    let valid = result.valid();
    let errors = result.errors();
    async move {
        let valid = valid.get().await;
        let errors = errors.get().await;
        Ok((valid?, errors?))
    }
}

/// Submits `listConsumerGroups` and returns a future over its `valid()` /
/// `errors()` split.
#[allow(deprecated)]
fn submit_list_consumer_groups(
    admin: &dyn Admin,
    options: ListConsumerGroupsOptions,
) -> impl std::future::Future<Output = Result<ListConsumerGroupsOutcome, KafkaError>> + Send + use<> {
    let result = admin.list_consumer_groups(options);
    let valid = result.valid();
    let errors = result.errors();
    async move {
        let valid = valid.get().await;
        let errors = errors.get().await;
        Ok((valid?, errors?))
    }
}

/// Submits `describeConsumerGroups` and returns the collect-all future over its
/// per-group futures.
fn submit_describe_consumer_groups(
    admin: &dyn Admin,
    group_ids: &[String],
    options: DescribeConsumerGroupsOptions,
) -> KafkaFuture<DescribeConsumerGroupsOutcomes> {
    let result = admin.describe_consumer_groups(group_ids, options);
    KafkaFuture::join_map_results(result.described_groups().into_iter().collect())
}

/// Submits `describeClassicGroups` and returns the collect-all future over its
/// per-group futures.
fn submit_describe_classic_groups(
    admin: &dyn Admin,
    group_ids: &[String],
    options: DescribeClassicGroupsOptions,
) -> KafkaFuture<DescribeClassicGroupsOutcomes> {
    let result = admin.describe_classic_groups(group_ids, options);
    KafkaFuture::join_map_results(result.described_groups().into_iter().collect())
}

/// Submits `listConsumerGroupOffsets` and returns the collect-all future over
/// its per-group futures.
///
/// `ListConsumerGroupOffsetsResult` exposes its futures only through
/// `partitionsToOffsetAndMetadata(groupId)`, so the requested group ids drive
/// the join. That accessor throws for a group that was not requested, which
/// cannot happen here; the error is propagated rather than dropped.
fn submit_list_consumer_group_offsets(
    admin: &dyn Admin,
    group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
    options: ListConsumerGroupOffsetsOptions,
) -> Result<KafkaFuture<ListConsumerGroupOffsetsOutcomes>, KafkaError> {
    let result = admin.list_consumer_group_offsets(group_specs, options);
    let mut entries: Vec<(String, KafkaFuture<GroupOffsets>)> = Vec::with_capacity(group_specs.len());
    for group_id in group_specs.keys() {
        entries.push((group_id.clone(), result.partitions_to_offset_and_metadata_for_group(group_id)?));
    }
    Ok(KafkaFuture::join_map_results(entries))
}

/// Turns a single whole-call future into an empty per-key outcome map.
///
/// Three of the group RPCs back every per-key future with **one** source
/// future. When the request selected no keys there is no per-key slot for a
/// failure to occupy, so the source future is awaited directly and its error
/// becomes the call's error — which mirrors Java, where `all()` is then the
/// only observable outcome.
fn empty_outcomes<K, V>(all: KafkaFuture<()>) -> KafkaFuture<HashMap<K, V>>
where
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    all.then_apply(|()| HashMap::new())
}

/// Submits `alterConsumerGroupOffsets` and returns the collect-all future over
/// its per-partition futures.
fn submit_alter_consumer_group_offsets(
    admin: &dyn Admin,
    group_id: &str,
    offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    options: AlterConsumerGroupOffsetsOptions,
) -> KafkaFuture<PartitionVoidOutcomes> {
    let result = admin.alter_consumer_group_offsets(group_id, offsets, options);
    if offsets.is_empty() {
        return empty_outcomes(result.all());
    }
    let entries: Vec<(TopicPartition, KafkaFuture<()>)> =
        offsets.keys().map(|tp| (tp.clone(), result.partition_result(tp))).collect();
    KafkaFuture::join_map_results(entries)
}

/// Submits `deleteConsumerGroupOffsets` and returns the collect-all future over
/// its per-partition futures.
///
/// `partitionResult(tp)` throws for a partition that was not requested, which
/// cannot happen here since the requested set drives the join; the error is
/// propagated rather than dropped.
fn submit_delete_consumer_group_offsets(
    admin: &dyn Admin,
    group_id: &str,
    partitions: &HashSet<TopicPartition>,
    options: DeleteConsumerGroupOffsetsOptions,
) -> Result<KafkaFuture<PartitionVoidOutcomes>, KafkaError> {
    let result = admin.delete_consumer_group_offsets(group_id, partitions, options);
    if partitions.is_empty() {
        return Ok(empty_outcomes(result.all()));
    }
    let mut entries: Vec<(TopicPartition, KafkaFuture<()>)> = Vec::with_capacity(partitions.len());
    for tp in partitions {
        entries.push((tp.clone(), result.partition_result(tp)?));
    }
    Ok(KafkaFuture::join_map_results(entries))
}

/// Submits `deleteConsumerGroups` and returns the collect-all future over its
/// per-group futures.
fn submit_delete_consumer_groups(
    admin: &dyn Admin,
    group_ids: &[String],
    options: DeleteConsumerGroupsOptions,
) -> KafkaFuture<GroupVoidOutcomes> {
    let result = admin.delete_consumer_groups(group_ids, options);
    KafkaFuture::join_map_results(result.deleted_groups().into_iter().collect())
}

/// Submits `removeMembersFromConsumerGroup` and returns the collect-all future
/// over its per-member futures, keyed by group instance id.
///
/// In `removeAll` mode Java's `memberResult` is not applicable at all, so the
/// single `all()` future is the outcome and the C result carries no keys.
fn submit_remove_members_from_consumer_group(
    admin: &dyn Admin,
    group_id: &str,
    options: RemoveMembersFromConsumerGroupOptions,
) -> Result<KafkaFuture<GroupVoidOutcomes>, KafkaError> {
    let members: Vec<MemberToRemove> = options.members().iter().cloned().collect();
    let result = admin.remove_members_from_consumer_group(group_id, options);
    if members.is_empty() {
        return Ok(empty_outcomes(result.all()));
    }
    let mut entries: Vec<(String, KafkaFuture<()>)> = Vec::with_capacity(members.len());
    for member in &members {
        entries.push((member.group_instance_id().to_string(), result.member_result(member)?));
    }
    Ok(KafkaFuture::join_map_results(entries))
}

// ---------------------------------------------------------------------------
// Group result handles
//
// Accessors follow each Java `*Result`'s future shape (`PLAN-bindings.md` D2 as
// amended after B3), not a fixed template:
//
//   - `Map<K, KafkaFuture<V>>`      -> `_get_value(i)` and `_get_error(i)`
//     (`describeConsumerGroups`, `describeClassicGroups`,
//      `listConsumerGroupOffsets`)
//   - `Map<K, KafkaFuture<Void>>` / one future over `Map<K, Errors>`
//                                   -> `_get_error(i)` only
//     (`deleteConsumerGroups`, `alterConsumerGroupOffsets`,
//      `deleteConsumerGroupOffsets`, `removeMembersFromConsumerGroup`)
//   - one future split into `valid()` + `errors()`
//                                   -> two independent lists, no key at all
//     (`listGroups`, `listConsumerGroups`)
// ---------------------------------------------------------------------------

/// Opaque handle to a flattened `ListGroupsResult`.
#[repr(C)]
pub struct kafka_admin_ListGroupsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListGroupsResult_t`].
///
/// Java's `ListGroupsResult` has no per-key future at all: one source future
/// resolves to a mixed collection, which `valid()` and `errors()` split into a
/// listing list and an unkeyed error list (`ListGroupsResult.java:82,95`). The
/// two lists are independent and generally of different lengths, so this handle
/// exposes them as two separate sequences rather than as parallel arrays.
struct ListGroupsResultInner {
    valid: Vec<GroupListingInner>,
    errors: Vec<KafkaErrorInner>,
}

/// Flattens the `listGroups` outcome into the C handle.
fn box_list_groups_result(outcome: ListGroupsOutcome) -> *mut kafka_admin_ListGroupsResult_t {
    let (valid, errors) = outcome;
    Box::into_raw(Box::new(ListGroupsResultInner {
        valid: valid.iter().map(GroupListingInner::new).collect(),
        errors: errors.into_iter().map(error_inner).collect(),
    })) as *mut kafka_admin_ListGroupsResult_t
}

/// Casts a `*const kafka_admin_ListGroupsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_groups` call.
unsafe fn list_groups_result_ref(result: *const kafka_admin_ListGroupsResult_t) -> &'static ListGroupsResultInner {
    unsafe { &*(result as *const ListGroupsResultInner) }
}

/// Returns the number of successfully listed groups (Java's `valid()`).
///
/// # Safety
///
/// `result` must be a valid `list_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_valid_count(
    result: *const kafka_admin_ListGroupsResult_t,
) -> i32 {
    unsafe { list_groups_result_ref(result) }.valid.len() as i32
}

/// Returns the listing at `index` (borrowed), or null if out of range. Listings
/// keep the order the brokers reported them in, as Java's `valid()` does.
///
/// # Safety
///
/// `result` must be a valid `list_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_get_valid(
    result: *const kafka_admin_ListGroupsResult_t,
    index: i32,
) -> *const kafka_admin_GroupListing_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_groups_result_ref(result) }.valid.get(index as usize) {
        Some(listing) => listing as *const GroupListingInner as *const kafka_admin_GroupListing_t,
        None => std::ptr::null(),
    }
}

/// Returns the number of per-broker errors (Java's `errors()`).
///
/// # Safety
///
/// `result` must be a valid `list_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_error_count(
    result: *const kafka_admin_ListGroupsResult_t,
) -> i32 {
    unsafe { list_groups_result_ref(result) }.errors.len() as i32
}

/// Returns the error at `index` (borrowed), or null if out of range. Do not
/// destroy it.
///
/// **This list is not parallel to the listings.** Java's `errors()` is an
/// unkeyed `Collection<Throwable>` of the failures some brokers reported, while
/// `valid()` holds the listings the others returned; the two are independent
/// and generally of different lengths. Index this list with
/// [`kafka_admin_ListGroupsResult_error_count`], never with
/// [`kafka_admin_ListGroupsResult_valid_count`]. A non-empty error list plus a
/// non-empty listing list is Java's normal partial-success outcome, which is
/// exactly what `all()` would have thrown on.
///
/// # Safety
///
/// `result` must be a valid `list_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_get_error(
    result: *const kafka_admin_ListGroupsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    error_ptr(unsafe { list_groups_result_ref(result) }.errors.get(index as usize))
}

/// Destroys a `list_groups` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_destroy(result: *mut kafka_admin_ListGroupsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListGroupsResultInner)) };
    }
}

/// Opaque handle to a flattened `ListConsumerGroupsResult`.
#[repr(C)]
pub struct kafka_admin_ListConsumerGroupsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListConsumerGroupsResult_t`]. Same
/// `valid()` / `errors()` split as [`ListGroupsResultInner`].
struct ListConsumerGroupsResultInner {
    valid: Vec<ConsumerGroupListingInner>,
    errors: Vec<KafkaErrorInner>,
}

/// Flattens the `listConsumerGroups` outcome into the C handle.
fn box_list_consumer_groups_result(outcome: ListConsumerGroupsOutcome) -> *mut kafka_admin_ListConsumerGroupsResult_t {
    let (valid, errors) = outcome;
    Box::into_raw(Box::new(ListConsumerGroupsResultInner {
        valid: valid.iter().map(ConsumerGroupListingInner::new).collect(),
        errors: errors.into_iter().map(error_inner).collect(),
    })) as *mut kafka_admin_ListConsumerGroupsResult_t
}

/// Casts a `*const kafka_admin_ListConsumerGroupsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_consumer_groups` call.
unsafe fn list_consumer_groups_result_ref(
    result: *const kafka_admin_ListConsumerGroupsResult_t,
) -> &'static ListConsumerGroupsResultInner {
    unsafe { &*(result as *const ListConsumerGroupsResultInner) }
}

/// Returns the number of successfully listed consumer groups (Java's
/// `valid()`).
///
/// # Safety
///
/// `result` must be a valid `list_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupsResult_valid_count(
    result: *const kafka_admin_ListConsumerGroupsResult_t,
) -> i32 {
    unsafe { list_consumer_groups_result_ref(result) }.valid.len() as i32
}

/// Returns the listing at `index` (borrowed), or null if out of range.
///
/// # Safety
///
/// `result` must be a valid `list_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupsResult_get_valid(
    result: *const kafka_admin_ListConsumerGroupsResult_t,
    index: i32,
) -> *const kafka_admin_ConsumerGroupListing_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_consumer_groups_result_ref(result) }.valid.get(index as usize) {
        Some(listing) => listing as *const ConsumerGroupListingInner as *const kafka_admin_ConsumerGroupListing_t,
        None => std::ptr::null(),
    }
}

/// Returns the number of per-broker errors (Java's `errors()`).
///
/// # Safety
///
/// `result` must be a valid `list_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupsResult_error_count(
    result: *const kafka_admin_ListConsumerGroupsResult_t,
) -> i32 {
    unsafe { list_consumer_groups_result_ref(result) }.errors.len() as i32
}

/// Returns the error at `index` (borrowed), or null if out of range. Do not
/// destroy it.
///
/// **This list is not parallel to the listings** — see
/// [`kafka_admin_ListGroupsResult_get_error`], which has the same shape.
///
/// # Safety
///
/// `result` must be a valid `list_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupsResult_get_error(
    result: *const kafka_admin_ListConsumerGroupsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    error_ptr(unsafe { list_consumer_groups_result_ref(result) }.errors.get(index as usize))
}

/// Destroys a `list_consumer_groups` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupsResult_destroy(
    result: *mut kafka_admin_ListConsumerGroupsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListConsumerGroupsResultInner)) };
    }
}

/// Opaque handle to a flattened `DescribeConsumerGroupsResult`, keyed by group
/// id.
#[repr(C)]
pub struct kafka_admin_DescribeConsumerGroupsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeConsumerGroupsResult_t`].
///
/// Java's `describedGroups()` is `Map<String, KafkaFuture<ConsumerGroupDescription>>`
/// — one future per group carrying a value — so this handle has both
/// `_get_value(i)` and `_get_error(i)`.
struct DescribeConsumerGroupsResultInner {
    group_ids: Vec<CString>,
    descriptions: Vec<Option<ConsumerGroupDescriptionInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-group `describeConsumerGroups` outcomes into the C handle.
fn box_describe_consumer_groups_result(
    outcomes: DescribeConsumerGroupsOutcomes,
) -> *mut kafka_admin_DescribeConsumerGroupsResult_t {
    let entries = sorted_entries(outcomes);
    let mut group_ids = Vec::with_capacity(entries.len());
    let mut descriptions = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (group_id, outcome) in entries {
        group_ids.push(to_cstring(&group_id));
        match outcome {
            Ok(description) => {
                descriptions.push(Some(ConsumerGroupDescriptionInner::new(&description)));
                errors.push(None);
            },
            Err(e) => {
                descriptions.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(DescribeConsumerGroupsResultInner { group_ids, descriptions, errors }))
        as *mut kafka_admin_DescribeConsumerGroupsResult_t
}

/// Casts a `*const kafka_admin_DescribeConsumerGroupsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_consumer_groups` call.
unsafe fn describe_consumer_groups_result_ref(
    result: *const kafka_admin_DescribeConsumerGroupsResult_t,
) -> &'static DescribeConsumerGroupsResultInner {
    unsafe { &*(result as *const DescribeConsumerGroupsResultInner) }
}

/// Returns the number of described groups.
///
/// # Safety
///
/// `result` must be a valid `describe_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConsumerGroupsResult_count(
    result: *const kafka_admin_DescribeConsumerGroupsResult_t,
) -> i32 {
    unsafe { describe_consumer_groups_result_ref(result) }.group_ids.len() as i32
}

/// Returns the group id of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by group id.
///
/// # Safety
///
/// `result` must be a valid `describe_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConsumerGroupsResult_get_group_id(
    result: *const kafka_admin_DescribeConsumerGroupsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { describe_consumer_groups_result_ref(result) }.group_ids, index)
}

/// Returns the description of the entry at `index` (borrowed), or null if the
/// group failed or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConsumerGroupsResult_get_value(
    result: *const kafka_admin_DescribeConsumerGroupsResult_t,
    index: i32,
) -> *const kafka_admin_ConsumerGroupDescription_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_consumer_groups_result_ref(result) }
        .descriptions
        .get(index as usize)
    {
        Some(Some(description)) => {
            description as *const ConsumerGroupDescriptionInner as *const kafka_admin_ConsumerGroupDescription_t
        },
        _ => std::ptr::null(),
    }
}

/// Returns the error for the entry at `index` (borrowed), or null if the group
/// was described successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConsumerGroupsResult_get_error(
    result: *const kafka_admin_DescribeConsumerGroupsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_consumer_groups_result_ref(result) }
        .errors
        .get(index as usize)
    {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `describe_consumer_groups` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConsumerGroupsResult_destroy(
    result: *mut kafka_admin_DescribeConsumerGroupsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeConsumerGroupsResultInner)) };
    }
}

/// Opaque handle to a flattened `DescribeClassicGroupsResult`, keyed by group
/// id.
#[repr(C)]
pub struct kafka_admin_DescribeClassicGroupsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeClassicGroupsResult_t`]. Same
/// per-key value-and-error shape as
/// [`DescribeConsumerGroupsResultInner`], with `ClassicGroupDescription` as the
/// value.
struct DescribeClassicGroupsResultInner {
    group_ids: Vec<CString>,
    descriptions: Vec<Option<ClassicGroupDescriptionInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-group `describeClassicGroups` outcomes into the C handle.
fn box_describe_classic_groups_result(
    outcomes: DescribeClassicGroupsOutcomes,
) -> *mut kafka_admin_DescribeClassicGroupsResult_t {
    let entries = sorted_entries(outcomes);
    let mut group_ids = Vec::with_capacity(entries.len());
    let mut descriptions = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (group_id, outcome) in entries {
        group_ids.push(to_cstring(&group_id));
        match outcome {
            Ok(description) => {
                descriptions.push(Some(ClassicGroupDescriptionInner::new(&description)));
                errors.push(None);
            },
            Err(e) => {
                descriptions.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(DescribeClassicGroupsResultInner { group_ids, descriptions, errors }))
        as *mut kafka_admin_DescribeClassicGroupsResult_t
}

/// Casts a `*const kafka_admin_DescribeClassicGroupsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_classic_groups` call.
unsafe fn describe_classic_groups_result_ref(
    result: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> &'static DescribeClassicGroupsResultInner {
    unsafe { &*(result as *const DescribeClassicGroupsResultInner) }
}

/// Returns the number of described groups.
///
/// # Safety
///
/// `result` must be a valid `describe_classic_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_count(
    result: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> i32 {
    unsafe { describe_classic_groups_result_ref(result) }.group_ids.len() as i32
}

/// Returns the group id of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by group id.
///
/// # Safety
///
/// `result` must be a valid `describe_classic_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_get_group_id(
    result: *const kafka_admin_DescribeClassicGroupsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { describe_classic_groups_result_ref(result) }.group_ids, index)
}

/// Returns the description of the entry at `index` (borrowed), or null if the
/// group failed or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_classic_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_get_value(
    result: *const kafka_admin_DescribeClassicGroupsResult_t,
    index: i32,
) -> *const kafka_admin_ClassicGroupDescription_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_classic_groups_result_ref(result) }
        .descriptions
        .get(index as usize)
    {
        Some(Some(description)) => {
            description as *const ClassicGroupDescriptionInner as *const kafka_admin_ClassicGroupDescription_t
        },
        _ => std::ptr::null(),
    }
}

/// Returns the error for the entry at `index` (borrowed), or null if the group
/// was described successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `describe_classic_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_get_error(
    result: *const kafka_admin_DescribeClassicGroupsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_classic_groups_result_ref(result) }.errors.get(index as usize) {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `describe_classic_groups` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_classic_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_destroy(
    result: *mut kafka_admin_DescribeClassicGroupsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeClassicGroupsResultInner)) };
    }
}

/// Opaque handle to a flattened `ListConsumerGroupOffsetsResult`, keyed by
/// group id.
#[repr(C)]
pub struct kafka_admin_ListConsumerGroupOffsetsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_ListConsumerGroupOffsetsResult_t`].
///
/// Two-level: Java's per-group future carries a whole
/// `Map<TopicPartition, OffsetAndMetadata>`, so `_get_value(i)` hands out a
/// borrowed [`kafka_admin_OffsetAndMetadataMap_t`] rather than a scalar.
struct ListConsumerGroupOffsetsResultInner {
    group_ids: Vec<CString>,
    offsets: Vec<Option<OffsetAndMetadataMapInner>>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-group `listConsumerGroupOffsets` outcomes into the C handle.
fn box_list_consumer_group_offsets_result(
    outcomes: ListConsumerGroupOffsetsOutcomes,
) -> *mut kafka_admin_ListConsumerGroupOffsetsResult_t {
    let entries = sorted_entries(outcomes);
    let mut group_ids = Vec::with_capacity(entries.len());
    let mut offsets = Vec::with_capacity(entries.len());
    let mut errors = Vec::with_capacity(entries.len());
    for (group_id, outcome) in entries {
        group_ids.push(to_cstring(&group_id));
        match outcome {
            Ok(group_offsets) => {
                offsets.push(Some(OffsetAndMetadataMapInner::new(group_offsets)));
                errors.push(None);
            },
            Err(e) => {
                offsets.push(None);
                errors.push(Some(error_inner(e)));
            },
        }
    }
    Box::into_raw(Box::new(ListConsumerGroupOffsetsResultInner { group_ids, offsets, errors }))
        as *mut kafka_admin_ListConsumerGroupOffsetsResult_t
}

/// Casts a `*const kafka_admin_ListConsumerGroupOffsetsResult_t` to a
/// reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `list_consumer_group_offsets`
/// call.
unsafe fn list_consumer_group_offsets_result_ref(
    result: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
) -> &'static ListConsumerGroupOffsetsResultInner {
    unsafe { &*(result as *const ListConsumerGroupOffsetsResultInner) }
}

/// Returns the number of groups offsets were listed for.
///
/// # Safety
///
/// `result` must be a valid `list_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_count(
    result: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
) -> i32 {
    unsafe { list_consumer_group_offsets_result_ref(result) }.group_ids.len() as i32
}

/// Returns the group id of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by group id.
///
/// # Safety
///
/// `result` must be a valid `list_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_get_group_id(
    result: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { list_consumer_group_offsets_result_ref(result) }.group_ids, index)
}

/// Returns the group's committed offsets at `index` (borrowed), or null if the
/// group failed or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `list_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_get_value(
    result: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
    index: i32,
) -> *const kafka_admin_OffsetAndMetadataMap_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_consumer_group_offsets_result_ref(result) }
        .offsets
        .get(index as usize)
    {
        Some(Some(map)) => map as *const OffsetAndMetadataMapInner as *const kafka_admin_OffsetAndMetadataMap_t,
        _ => std::ptr::null(),
    }
}

/// Returns the error for the entry at `index` (borrowed), or null if the
/// group's offsets were listed successfully or `index` is out of range. Do not
/// destroy it.
///
/// # Safety
///
/// `result` must be a valid `list_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_get_error(
    result: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { list_consumer_group_offsets_result_ref(result) }
        .errors
        .get(index as usize)
    {
        Some(slot) => error_ptr(slot.as_ref()),
        None => std::ptr::null(),
    }
}

/// Destroys a `list_consumer_group_offsets` result handle. Safe with null
/// (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `list_consumer_group_offsets` result
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_destroy(
    result: *mut kafka_admin_ListConsumerGroupOffsetsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut ListConsumerGroupOffsetsResultInner)) };
    }
}

/// Opaque handle to a flattened `AlterConsumerGroupOffsetsResult`, keyed by
/// topic partition.
#[repr(C)]
pub struct kafka_admin_AlterConsumerGroupOffsetsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_AlterConsumerGroupOffsetsResult_t`].
///
/// Java's `partitionResult(tp)` is a `KafkaFuture<Void>`, so there is no
/// per-key value: a null `_get_error(i)` is the success signal.
struct AlterConsumerGroupOffsetsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-partition `alterConsumerGroupOffsets` outcomes into the C
/// handle.
fn box_alter_consumer_group_offsets_result(
    outcomes: PartitionVoidOutcomes,
) -> *mut kafka_admin_AlterConsumerGroupOffsetsResult_t {
    let (topics, partitions, errors) = flatten_partition_void_outcomes(outcomes);
    Box::into_raw(Box::new(AlterConsumerGroupOffsetsResultInner { topics, partitions, errors }))
        as *mut kafka_admin_AlterConsumerGroupOffsetsResult_t
}

/// Casts a `*const kafka_admin_AlterConsumerGroupOffsetsResult_t` to a
/// reference.
///
/// # Safety
///
/// `result` must be a non-null handle from an `alter_consumer_group_offsets`
/// call.
unsafe fn alter_consumer_group_offsets_result_ref(
    result: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
) -> &'static AlterConsumerGroupOffsetsResultInner {
    unsafe { &*(result as *const AlterConsumerGroupOffsetsResultInner) }
}

/// Returns the number of partitions whose offsets were altered.
///
/// # Safety
///
/// `result` must be a valid `alter_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_count(
    result: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
) -> i32 {
    unsafe { alter_consumer_group_offsets_result_ref(result) }.topics.len() as i32
}

/// Returns the topic name of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `result` must be a valid `alter_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_get_topic(
    result: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { alter_consumer_group_offsets_result_ref(result) }.topics, index)
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `alter_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_get_partition(
    result: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
    index: i32,
) -> i32 {
    partition_at(&unsafe { alter_consumer_group_offsets_result_ref(result) }.partitions, index)
}

/// Returns the error for the entry at `index` (borrowed), or null if the
/// partition's offset was altered successfully or `index` is out of range. Do
/// not destroy it.
///
/// # Safety
///
/// `result` must be a valid `alter_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_get_error(
    result: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    optional_error_at(&unsafe { alter_consumer_group_offsets_result_ref(result) }.errors, index)
}

/// Destroys an `alter_consumer_group_offsets` result handle. Safe with null
/// (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `alter_consumer_group_offsets` result
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_destroy(
    result: *mut kafka_admin_AlterConsumerGroupOffsetsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut AlterConsumerGroupOffsetsResultInner)) };
    }
}

/// Opaque handle to a flattened `DeleteConsumerGroupOffsetsResult`, keyed by
/// topic partition.
#[repr(C)]
pub struct kafka_admin_DeleteConsumerGroupOffsetsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DeleteConsumerGroupOffsetsResult_t`]. Same
/// per-key-error-only shape as [`AlterConsumerGroupOffsetsResultInner`].
struct DeleteConsumerGroupOffsetsResultInner {
    topics: Vec<CString>,
    partitions: Vec<i32>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-partition `deleteConsumerGroupOffsets` outcomes into the C
/// handle.
fn box_delete_consumer_group_offsets_result(
    outcomes: PartitionVoidOutcomes,
) -> *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t {
    let (topics, partitions, errors) = flatten_partition_void_outcomes(outcomes);
    Box::into_raw(Box::new(DeleteConsumerGroupOffsetsResultInner { topics, partitions, errors }))
        as *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t
}

/// Casts a `*const kafka_admin_DeleteConsumerGroupOffsetsResult_t` to a
/// reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `delete_consumer_group_offsets`
/// call.
unsafe fn delete_consumer_group_offsets_result_ref(
    result: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
) -> &'static DeleteConsumerGroupOffsetsResultInner {
    unsafe { &*(result as *const DeleteConsumerGroupOffsetsResultInner) }
}

/// Returns the number of partitions whose offsets were deleted.
///
/// # Safety
///
/// `result` must be a valid `delete_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_count(
    result: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
) -> i32 {
    unsafe { delete_consumer_group_offsets_result_ref(result) }.topics.len() as i32
}

/// Returns the topic name of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by topic name then partition id.
///
/// # Safety
///
/// `result` must be a valid `delete_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_get_topic(
    result: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { delete_consumer_group_offsets_result_ref(result) }.topics, index)
}

/// Returns the partition id of the entry at `index`, or -1 if out of range.
///
/// # Safety
///
/// `result` must be a valid `delete_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_get_partition(
    result: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
    index: i32,
) -> i32 {
    partition_at(&unsafe { delete_consumer_group_offsets_result_ref(result) }.partitions, index)
}

/// Returns the error for the entry at `index` (borrowed), or null if the
/// partition's offset was deleted successfully or `index` is out of range. Do
/// not destroy it.
///
/// # Safety
///
/// `result` must be a valid `delete_consumer_group_offsets` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_get_error(
    result: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    optional_error_at(&unsafe { delete_consumer_group_offsets_result_ref(result) }.errors, index)
}

/// Destroys a `delete_consumer_group_offsets` result handle. Safe with null
/// (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `delete_consumer_group_offsets` result
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(
    result: *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DeleteConsumerGroupOffsetsResultInner)) };
    }
}

/// Opaque handle to a flattened `DeleteConsumerGroupsResult`, keyed by group id.
#[repr(C)]
pub struct kafka_admin_DeleteConsumerGroupsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DeleteConsumerGroupsResult_t`].
///
/// Java's `deletedGroups()` is `Map<String, KafkaFuture<Void>>`, so there is no
/// per-key value: a null `_get_error(i)` is the success signal.
struct DeleteConsumerGroupsResultInner {
    group_ids: Vec<CString>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-group `deleteConsumerGroups` outcomes into the C handle.
fn box_delete_consumer_groups_result(outcomes: GroupVoidOutcomes) -> *mut kafka_admin_DeleteConsumerGroupsResult_t {
    let (group_ids, errors) = flatten_keyed_void_outcomes(outcomes);
    Box::into_raw(Box::new(DeleteConsumerGroupsResultInner { group_ids, errors }))
        as *mut kafka_admin_DeleteConsumerGroupsResult_t
}

/// Casts a `*const kafka_admin_DeleteConsumerGroupsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `delete_consumer_groups` call.
unsafe fn delete_consumer_groups_result_ref(
    result: *const kafka_admin_DeleteConsumerGroupsResult_t,
) -> &'static DeleteConsumerGroupsResultInner {
    unsafe { &*(result as *const DeleteConsumerGroupsResultInner) }
}

/// Returns the number of groups a deletion was attempted for.
///
/// # Safety
///
/// `result` must be a valid `delete_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupsResult_count(
    result: *const kafka_admin_DeleteConsumerGroupsResult_t,
) -> i32 {
    unsafe { delete_consumer_groups_result_ref(result) }.group_ids.len() as i32
}

/// Returns the group id of the entry at `index` (borrowed), or null if out of
/// range. Entries are sorted by group id.
///
/// # Safety
///
/// `result` must be a valid `delete_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupsResult_get_group_id(
    result: *const kafka_admin_DeleteConsumerGroupsResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { delete_consumer_groups_result_ref(result) }.group_ids, index)
}

/// Returns the error for the entry at `index` (borrowed), or null if the group
/// was deleted successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `delete_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupsResult_get_error(
    result: *const kafka_admin_DeleteConsumerGroupsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    optional_error_at(&unsafe { delete_consumer_groups_result_ref(result) }.errors, index)
}

/// Destroys a `delete_consumer_groups` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `delete_consumer_groups` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupsResult_destroy(
    result: *mut kafka_admin_DeleteConsumerGroupsResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DeleteConsumerGroupsResultInner)) };
    }
}

/// Opaque handle to a flattened `RemoveMembersFromConsumerGroupResult`, keyed
/// by group instance id.
#[repr(C)]
pub struct kafka_admin_RemoveMembersFromConsumerGroupResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_RemoveMembersFromConsumerGroupResult_t`].
///
/// Java's `memberResult(MemberToRemove)` is a `KafkaFuture<Void>`, so there is
/// no per-key value. In `removeAll` mode Java *refuses* `memberResult` entirely
/// ("The method: memberResult is not applicable in 'removeAll' mode") and
/// `all()` is the only observable, so the C handle is then empty and the
/// outcome is the call's error.
struct RemoveMembersFromConsumerGroupResultInner {
    group_instance_ids: Vec<CString>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-member `removeMembersFromConsumerGroup` outcomes into the C
/// handle.
fn box_remove_members_from_consumer_group_result(
    outcomes: GroupVoidOutcomes,
) -> *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t {
    let (group_instance_ids, errors) = flatten_keyed_void_outcomes(outcomes);
    Box::into_raw(Box::new(RemoveMembersFromConsumerGroupResultInner {
        group_instance_ids,
        errors,
    })) as *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t
}

/// Casts a `*const kafka_admin_RemoveMembersFromConsumerGroupResult_t` to a
/// reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a
/// `remove_members_from_consumer_group` call.
unsafe fn remove_members_result_ref(
    result: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) -> &'static RemoveMembersFromConsumerGroupResultInner {
    unsafe { &*(result as *const RemoveMembersFromConsumerGroupResultInner) }
}

/// Returns the number of members a removal was attempted for. This is 0 in
/// `remove_all` mode, where Java exposes no per-member outcome.
///
/// # Safety
///
/// `result` must be a valid `remove_members_from_consumer_group` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_count(
    result: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) -> i32 {
    unsafe { remove_members_result_ref(result) }.group_instance_ids.len() as i32
}

/// Returns the group instance id of the entry at `index` (borrowed), or null if
/// out of range. Entries are sorted by group instance id.
///
/// # Safety
///
/// `result` must be a valid `remove_members_from_consumer_group` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_get_group_instance_id(
    result: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { remove_members_result_ref(result) }.group_instance_ids, index)
}

/// Returns the error for the entry at `index` (borrowed), or null if the member
/// was removed successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `remove_members_from_consumer_group` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_get_error(
    result: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    optional_error_at(&unsafe { remove_members_result_ref(result) }.errors, index)
}

/// Destroys a `remove_members_from_consumer_group` result handle. Safe with
/// null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `remove_members_from_consumer_group` result
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(
    result: *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut RemoveMembersFromConsumerGroupResultInner)) };
    }
}

// ---------------------------------------------------------------------------
// listGroups
// ---------------------------------------------------------------------------

/// Completion callback for [`kafka_admin_AdminClient_list_groups_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_ListGroupsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-broker listing failure arrives
/// inside `result` (see [`kafka_admin_ListGroupsResult_get_error`]), not as
/// `error`.
pub type kafka_admin_AdminClient_list_groups_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_ListGroupsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Lists the groups in the cluster, blocking until the listing has resolved
/// (synchronous).
///
/// This is `listGroups(ListGroupsOptions)`.
///
/// On success writes a [`kafka_admin_ListGroupsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_ListGroupsResult_destroy`]) and returns null.
/// **A per-broker failure is not a call failure**: Java splits the outcome into
/// `valid()` listings and an unkeyed `errors()` collection, which the result
/// handle exposes as two independent lists. A non-null return means the listing
/// could not be run at all, and `*out_result` is left untouched.
///
/// # Parameters
///
/// - `group_states` / `group_state_count`: filter by `GroupState`, using Java's
///   `toString()` names (`"Stable"`, `"Empty"`, `"PreparingRebalance"`, …).
///   Matching is case-insensitive and an unrecognised name becomes `UNKNOWN`,
///   as in `GroupState.parse(String)`. Pass NULL / 0 for every state.
/// - `protocol_types` / `protocol_type_count`: filter by protocol type, e.g.
///   `"consumer"` — the wire protocol-type string, which is lower-case and
///   unrelated to the `GroupType` names above. Pass NULL / 0 for every
///   protocol type.
/// - `types` / `type_count`: filter by `GroupType`, using Java's `toString()`
///   names (`"Consumer"`, `"Classic"`, `"Share"`, `"Streams"`). Pass NULL / 0
///   for every type. Java's `ListGroupsOptions.forConsumerGroups()` and
///   friends are convenience presets over exactly these two filters.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// # Safety
///
/// `admin` must be a valid handle; each name array must be null or have its
/// stated number of valid C strings; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_groups(
    admin: *const kafka_admin_AdminClient_t,
    group_states: *const *const c_char,
    group_state_count: i32,
    protocol_types: *const *const c_char,
    protocol_type_count: i32,
    types: *const *const c_char,
    type_count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_ListGroupsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let options = unsafe {
        list_groups_options(
            group_states,
            group_state_count,
            protocol_types,
            protocol_type_count,
            types,
            type_count,
            timeout_ms,
        )
    };
    let outcome = unsafe { admin_sync_future_op(admin, move |a| Ok(submit_list_groups(a, options))) };
    unsafe { finish_sync(outcome, out_result, box_list_groups_result) }
}

/// Lists the groups asynchronously. See [`kafka_admin_AdminClient_list_groups`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; each name array must be null or have its
/// stated number of valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_groups_async(
    admin: *const kafka_admin_AdminClient_t,
    group_states: *const *const c_char,
    group_state_count: i32,
    protocol_types: *const *const c_char,
    protocol_type_count: i32,
    types: *const *const c_char,
    type_count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_list_groups_callback_t,
    user_data: *mut c_void,
) {
    let options = unsafe {
        list_groups_options(
            group_states,
            group_state_count,
            protocol_types,
            protocol_type_count,
            types,
            type_count,
            timeout_ms,
        )
    };
    unsafe {
        admin_async_future_op(
            admin,
            user_data,
            move |a| Ok(submit_list_groups(a, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcome) => (box_list_groups_result(outcome), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// listConsumerGroups
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_list_consumer_groups_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_ListConsumerGroupsResult_destroy`] or `error`
/// with `kafka_common_KafkaError_destroy`.
pub type kafka_admin_AdminClient_list_consumer_groups_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_ListConsumerGroupsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Lists the consumer groups in the cluster, blocking until the listing has
/// resolved (synchronous).
///
/// This is `listConsumerGroups(ListConsumerGroupsOptions)`, which is
/// **deprecated since Kafka 4.1** in favour of
/// [`kafka_admin_AdminClient_list_groups`] — that call returns `GroupListing`s
/// covering every group type, not just consumer groups. It is exposed here for
/// parity with the Java `Admin` surface, which still declares it.
///
/// On success writes a [`kafka_admin_ListConsumerGroupsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_ListConsumerGroupsResult_destroy`]) and returns null. A
/// per-broker failure is not a call failure; see
/// [`kafka_admin_AdminClient_list_groups`], which has the same result shape.
///
/// # Parameters
///
/// - `group_states` / `group_state_count`: filter by `GroupState`, using Java's
///   `toString()` names. Java's deprecated `inStates(Set<ConsumerGroupState>)`
///   is defined as `inGroupStates(...)` over `GroupState.parse(...)` of those
///   same names, so a caller holding `ConsumerGroupState` names passes them in
///   this array too.
/// - `types` / `type_count`: filter by `GroupType`, using Java's `toString()`
///   names.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// # Safety
///
/// `admin` must be a valid handle; each name array must be null or have its
/// stated number of valid C strings; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_consumer_groups(
    admin: *const kafka_admin_AdminClient_t,
    group_states: *const *const c_char,
    group_state_count: i32,
    types: *const *const c_char,
    type_count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_ListConsumerGroupsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let options =
        unsafe { list_consumer_groups_options(group_states, group_state_count, types, type_count, timeout_ms) };
    let outcome = unsafe { admin_sync_future_op(admin, move |a| Ok(submit_list_consumer_groups(a, options))) };
    unsafe { finish_sync(outcome, out_result, box_list_consumer_groups_result) }
}

/// Lists the consumer groups asynchronously. See
/// [`kafka_admin_AdminClient_list_consumer_groups`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; each name array must be null or have its
/// stated number of valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_consumer_groups_async(
    admin: *const kafka_admin_AdminClient_t,
    group_states: *const *const c_char,
    group_state_count: i32,
    types: *const *const c_char,
    type_count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_list_consumer_groups_callback_t,
    user_data: *mut c_void,
) {
    let options =
        unsafe { list_consumer_groups_options(group_states, group_state_count, types, type_count, timeout_ms) };
    unsafe {
        admin_async_future_op(
            admin,
            user_data,
            move |a| Ok(submit_list_consumer_groups(a, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcome) => (box_list_consumer_groups_result(outcome), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeConsumerGroups
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_describe_consumer_groups_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeConsumerGroupsResult_destroy`] or
/// `error` with `kafka_common_KafkaError_destroy`. A per-group failure arrives
/// inside `result`, not as `error`.
pub type kafka_admin_AdminClient_describe_consumer_groups_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeConsumerGroupsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Describes consumer groups, blocking until every per-group future has
/// resolved (synchronous).
///
/// This is
/// `describeConsumerGroups(Collection<String>, DescribeConsumerGroupsOptions)`.
/// It describes both classic and consumer (KIP-848) protocol groups; the
/// classic-only sibling is
/// [`kafka_admin_AdminClient_describe_classic_groups`].
///
/// On success writes a [`kafka_admin_DescribeConsumerGroupsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_DescribeConsumerGroupsResult_destroy`]) and returns null.
/// **A per-group failure is not a call failure**: it is reported by
/// [`kafka_admin_DescribeConsumerGroupsResult_get_error`] for that key. A
/// non-null return means the request could not be submitted at all, and
/// `*out_result` is left untouched.
///
/// # Parameters
///
/// - `group_ids` / `count`: the group ids to describe. A NULL entry is skipped.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `include_authorized_operations`: Java's
///   `DescribeConsumerGroupsOptions.includeAuthorizedOperations(boolean)`. When
///   false, [`kafka_admin_ConsumerGroupDescription_authorized_operation_count`]
///   is 0.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_ids` must be null or have `count`
/// entries, each NULL or a valid C string; `out_result` must be null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_consumer_groups(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    out_result: *mut *mut kafka_admin_DescribeConsumerGroupsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let ids = unsafe { read_strings(group_ids, count) };
    let options = describe_consumer_groups_options(timeout_ms, include_authorized_operations);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_describe_consumer_groups(a, &ids, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_consumer_groups_result) }
}

/// Describes consumer groups asynchronously. See
/// [`kafka_admin_AdminClient_describe_consumer_groups`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_ids` must be null or have `count`
/// entries, each NULL or a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_consumer_groups_async(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    callback: kafka_admin_AdminClient_describe_consumer_groups_callback_t,
    user_data: *mut c_void,
) {
    let ids = unsafe { read_strings(group_ids, count) };
    let options = describe_consumer_groups_options(timeout_ms, include_authorized_operations);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_consumer_groups(a, &ids, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_describe_consumer_groups_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeClassicGroups
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_describe_classic_groups_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeClassicGroupsResult_destroy`] or `error`
/// with `kafka_common_KafkaError_destroy`. A per-group failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_describe_classic_groups_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeClassicGroupsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Describes classic groups, blocking until every per-group future has resolved
/// (synchronous).
///
/// This is
/// `describeClassicGroups(Collection<String>, DescribeClassicGroupsOptions)`.
/// Unlike [`kafka_admin_AdminClient_describe_consumer_groups`] it covers only
/// classic-protocol groups, and its description carries the group's `protocol`
/// and `protocolData` rather than a group epoch.
///
/// On success writes a [`kafka_admin_DescribeClassicGroupsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_DescribeClassicGroupsResult_destroy`]) and returns null. A
/// per-group failure is not a call failure; it is reported by
/// [`kafka_admin_DescribeClassicGroupsResult_get_error`] for that key.
///
/// # Parameters
///
/// - `group_ids` / `count`: the group ids to describe. A NULL entry is skipped.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `include_authorized_operations`: Java's
///   `DescribeClassicGroupsOptions.includeAuthorizedOperations(boolean)`.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_ids` must be null or have `count`
/// entries, each NULL or a valid C string; `out_result` must be null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_classic_groups(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    out_result: *mut *mut kafka_admin_DescribeClassicGroupsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let ids = unsafe { read_strings(group_ids, count) };
    let options = describe_classic_groups_options(timeout_ms, include_authorized_operations);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_describe_classic_groups(a, &ids, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_classic_groups_result) }
}

/// Describes classic groups asynchronously. See
/// [`kafka_admin_AdminClient_describe_classic_groups`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_ids` must be null or have `count`
/// entries, each NULL or a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_classic_groups_async(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    include_authorized_operations: bool,
    callback: kafka_admin_AdminClient_describe_classic_groups_callback_t,
    user_data: *mut c_void,
) {
    let ids = unsafe { read_strings(group_ids, count) };
    let options = describe_classic_groups_options(timeout_ms, include_authorized_operations);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_classic_groups(a, &ids, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_describe_classic_groups_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// listConsumerGroupOffsets
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_list_consumer_group_offsets_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_ListConsumerGroupOffsetsResult_destroy`] or
/// `error` with `kafka_common_KafkaError_destroy`. A per-group failure arrives
/// inside `result`, not as `error`.
pub type kafka_admin_AdminClient_list_consumer_group_offsets_callback_t = unsafe extern "C" fn(
    *mut kafka_admin_ListConsumerGroupOffsetsResult_t,
    *mut kafka_common_KafkaError_t,
    *mut c_void,
);

/// Lists committed offsets for one or more consumer groups, blocking until
/// every per-group future has resolved (synchronous).
///
/// This is
/// `listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>, ListConsumerGroupOffsetsOptions)`.
///
/// The request is two-level — a set of groups, each with its own partition
/// selection — so the arrays are ragged: index `i` addresses group `i`, and
/// `topics[i]` / `partitions[i]` are that group's own arrays of
/// `partition_counts[i]` entries. The result is two-level too:
/// [`kafka_admin_ListConsumerGroupOffsetsResult_get_value`] hands out a
/// [`kafka_admin_OffsetAndMetadataMap_t`] per group.
///
/// On success writes a [`kafka_admin_ListConsumerGroupOffsetsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_ListConsumerGroupOffsetsResult_destroy`]) and returns null. A
/// per-group failure is not a call failure; it is reported by
/// [`kafka_admin_ListConsumerGroupOffsetsResult_get_error`] for that key.
///
/// # Parameters
///
/// - `group_ids` / `group_count`: the groups to query. A NULL group id is
///   rejected, and so is a duplicate one — Java takes a `Map`, where the second
///   entry would silently have replaced the first.
/// - `all_partitions`: per group, pass `true` for Java's unset
///   `ListConsumerGroupOffsetsSpec.topicPartitions()`, i.e. "every partition
///   the group has committed offsets for". `topics[i]` / `partitions[i]` /
///   `partition_counts[i]` are then ignored. An explicit flag, so "all
///   partitions" and "an empty selection" stay distinguishable.
/// - `topics` / `partitions` / `partition_counts`: per group `i`, parallel
///   arrays of `partition_counts[i]` entries; entry `j` is
///   `(topics[i][j], partitions[i][j])`. An entry with a NULL topic is skipped.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
/// - `require_stable`: Java's
///   `ListConsumerGroupOffsetsOptions.requireStable(boolean)`.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_ids`, `all_partitions`, `topics`,
/// `partitions` and `partition_counts` must be null or have `group_count`
/// entries each; for a group whose `all_partitions` flag is false, `topics[i]`
/// and `partitions[i]` must have `partition_counts[i]` valid entries;
/// `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_consumer_group_offsets(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    all_partitions: *const bool,
    topics: *const *const *const c_char,
    partitions: *const *const i32,
    partition_counts: *const i32,
    group_count: i32,
    timeout_ms: i32,
    require_stable: bool,
    out_result: *mut *mut kafka_admin_ListConsumerGroupOffsetsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let specs = unsafe {
        read_group_offsets_specs(group_ids, all_partitions, topics, partitions, partition_counts, group_count)
    };
    let options = list_consumer_group_offsets_options(timeout_ms, require_stable);
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| submit_list_consumer_group_offsets(a, &specs?, options)) };
    unsafe { finish_sync(outcome, out_result, box_list_consumer_group_offsets_result) }
}

/// Lists consumer group offsets asynchronously. See
/// [`kafka_admin_AdminClient_list_consumer_group_offsets`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, a NULL group id, or the same group
/// id twice). And it runs on a **tokio worker thread** if the dispatcher's
/// completion queue can no longer be reached when the result arrives.
/// Destroying the handle does not cause that — an outstanding operation holds
/// its own sender, so it cannot disconnect the queue; what remains is a
/// dispatcher thread that terminated abnormally, i.e. a panic inside an earlier
/// callback. So callbacks are not guaranteed to be serialised on one thread. Do
/// not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; the group arrays must be null or have
/// `group_count` entries each, and each group's partition arrays must have that
/// group's `partition_counts` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_list_consumer_group_offsets_async(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    all_partitions: *const bool,
    topics: *const *const *const c_char,
    partitions: *const *const i32,
    partition_counts: *const i32,
    group_count: i32,
    timeout_ms: i32,
    require_stable: bool,
    callback: kafka_admin_AdminClient_list_consumer_group_offsets_callback_t,
    user_data: *mut c_void,
) {
    let specs = unsafe {
        read_group_offsets_specs(group_ids, all_partitions, topics, partitions, partition_counts, group_count)
    };
    let options = list_consumer_group_offsets_options(timeout_ms, require_stable);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_list_consumer_group_offsets(a, &specs?, options),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_list_consumer_group_offsets_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// alterConsumerGroupOffsets
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_alter_consumer_group_offsets_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_AlterConsumerGroupOffsetsResult_destroy`] or
/// `error` with `kafka_common_KafkaError_destroy`. A per-partition failure
/// arrives inside `result`, not as `error`.
pub type kafka_admin_AdminClient_alter_consumer_group_offsets_callback_t = unsafe extern "C" fn(
    *mut kafka_admin_AlterConsumerGroupOffsetsResult_t,
    *mut kafka_common_KafkaError_t,
    *mut c_void,
);

/// Commits offsets on behalf of a consumer group, blocking until every
/// per-partition future has resolved (synchronous).
///
/// This is
/// `alterConsumerGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>, AlterConsumerGroupOffsetsOptions)`.
///
/// On success writes a [`kafka_admin_AlterConsumerGroupOffsetsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_AlterConsumerGroupOffsetsResult_destroy`]) and returns null.
/// **A per-partition failure is not a call failure**: it is reported by
/// [`kafka_admin_AlterConsumerGroupOffsetsResult_get_error`] for that key. A
/// non-null return means the request could not be submitted at all — or that
/// `count` was 0, in which case there is no per-key slot for the outcome and
/// the whole-request error is returned instead, mirroring Java, where `all()`
/// is then the only observable.
///
/// # Parameters
///
/// - `group_id`: the consumer group whose offsets are being committed.
/// - `topics` / `partitions` / `offsets`: parallel arrays of `count` entries;
///   entry `i` commits `offsets[i]` for `(topics[i], partitions[i])`. A NULL
///   topic or a negative offset is rejected, the latter mirroring Java's
///   `OffsetAndMetadata` constructor.
/// - `metadata`: per entry, the commit metadata, or NULL for Java's null
///   metadata (which its constructor normalises to `""`). The whole array may
///   also be NULL.
/// - `leader_epochs` / `has_leader_epoch`: per entry, Java's
///   `Optional<Integer> leaderEpoch`. The flag is the discriminant, so epoch 0
///   stays distinguishable from an absent epoch; `leader_epochs[i]` is read
///   only when `has_leader_epoch[i]` is true.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_id` must be a valid C string; every
/// non-null array must have `count` valid entries; `out_result` must be null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_consumer_group_offsets(
    admin: *const kafka_admin_AdminClient_t,
    group_id: *const c_char,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    metadata: *const *const c_char,
    leader_epochs: *const i32,
    has_leader_epoch: *const bool,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_AlterConsumerGroupOffsetsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let group = unsafe { read_required_string(group_id, "group_id") };
    let parsed = unsafe {
        read_alter_group_offsets(topics, partitions, offsets, metadata, leader_epochs, has_leader_epoch, count)
    };
    let options = alter_consumer_group_offsets_options(timeout_ms);
    let outcome = unsafe {
        admin_sync_value_op(admin, move |a| {
            Ok(submit_alter_consumer_group_offsets(a, &group?, &parsed?, options))
        })
    };
    unsafe { finish_sync(outcome, out_result, box_alter_consumer_group_offsets_result) }
}

/// Commits consumer group offsets asynchronously. See
/// [`kafka_admin_AdminClient_alter_consumer_group_offsets`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, a NULL `group_id`, a NULL topic
/// entry, or a negative offset). And it runs on a **tokio worker thread** if the dispatcher's
/// completion queue can no longer be reached when the result arrives.
/// Destroying the handle does not cause that — an outstanding operation holds
/// its own sender, so it cannot disconnect the queue; what remains is a
/// dispatcher thread that terminated abnormally, i.e. a panic inside an earlier
/// callback. So callbacks are not guaranteed to be serialised on one thread. Do
/// not hold a lock across this call and re-acquire it in the callback, and
/// publish everything the callback needs (including `user_data`) before calling
/// rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_id` must be a valid C string; every
/// non-null array must have `count` valid entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_consumer_group_offsets_async(
    admin: *const kafka_admin_AdminClient_t,
    group_id: *const c_char,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    metadata: *const *const c_char,
    leader_epochs: *const i32,
    has_leader_epoch: *const bool,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_alter_consumer_group_offsets_callback_t,
    user_data: *mut c_void,
) {
    let group = unsafe { read_required_string(group_id, "group_id") };
    let parsed = unsafe {
        read_alter_group_offsets(topics, partitions, offsets, metadata, leader_epochs, has_leader_epoch, count)
    };
    let options = alter_consumer_group_offsets_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_alter_consumer_group_offsets(a, &group?, &parsed?, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_alter_consumer_group_offsets_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// deleteConsumerGroupOffsets
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_delete_consumer_group_offsets_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DeleteConsumerGroupOffsetsResult_destroy`] or
/// `error` with `kafka_common_KafkaError_destroy`. A per-partition failure
/// arrives inside `result`, not as `error`.
pub type kafka_admin_AdminClient_delete_consumer_group_offsets_callback_t = unsafe extern "C" fn(
    *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t,
    *mut kafka_common_KafkaError_t,
    *mut c_void,
);

/// Deletes committed offsets for a set of partitions in a consumer group,
/// blocking until every per-partition future has resolved (synchronous).
///
/// This is
/// `deleteConsumerGroupOffsets(String, Set<TopicPartition>, DeleteConsumerGroupOffsetsOptions)`.
///
/// On success writes a [`kafka_admin_DeleteConsumerGroupOffsetsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_DeleteConsumerGroupOffsetsResult_destroy`]) and returns null.
/// **A per-partition failure is not a call failure**: it is reported by
/// [`kafka_admin_DeleteConsumerGroupOffsetsResult_get_error`] for that key. A
/// non-null return means the request could not be submitted at all — or that
/// `count` was 0, in which case there is no per-key slot for the outcome and
/// the whole-request error is returned instead.
///
/// # Parameters
///
/// - `group_id`: the consumer group whose offsets are being deleted.
/// - `topics` / `partitions` / `count`: parallel arrays; entry `i` is
///   `(topics[i], partitions[i])`. An entry with a NULL topic is skipped.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///   `DeleteConsumerGroupOffsetsOptions` has no other field in Java.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_id` must be a valid C string;
/// `topics` and `partitions` must be null or have `count` valid entries each;
/// `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_consumer_group_offsets(
    admin: *const kafka_admin_AdminClient_t,
    group_id: *const c_char,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let group = unsafe { read_required_string(group_id, "group_id") };
    let selection: HashSet<TopicPartition> = unsafe { read_topic_partitions(topics, partitions, count) }
        .into_iter()
        .collect();
    let options = delete_consumer_group_offsets_options(timeout_ms);
    let outcome = unsafe {
        admin_sync_value_op(admin, move |a| {
            submit_delete_consumer_group_offsets(a, &group?, &selection, options)
        })
    };
    unsafe { finish_sync(outcome, out_result, box_delete_consumer_group_offsets_result) }
}

/// Deletes consumer group offsets asynchronously. See
/// [`kafka_admin_AdminClient_delete_consumer_group_offsets`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_id` must be a valid C string;
/// `topics` and `partitions` must be null or have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_consumer_group_offsets_async(
    admin: *const kafka_admin_AdminClient_t,
    group_id: *const c_char,
    topics: *const *const c_char,
    partitions: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_delete_consumer_group_offsets_callback_t,
    user_data: *mut c_void,
) {
    let group = unsafe { read_required_string(group_id, "group_id") };
    let selection: HashSet<TopicPartition> = unsafe { read_topic_partitions(topics, partitions, count) }
        .into_iter()
        .collect();
    let options = delete_consumer_group_offsets_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_delete_consumer_group_offsets(a, &group?, &selection, options),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_delete_consumer_group_offsets_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// deleteConsumerGroups
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_delete_consumer_groups_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DeleteConsumerGroupsResult_destroy`] or `error`
/// with `kafka_common_KafkaError_destroy`. A per-group failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_delete_consumer_groups_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DeleteConsumerGroupsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Deletes consumer groups, blocking until every per-group future has resolved
/// (synchronous).
///
/// This is
/// `deleteConsumerGroups(Collection<String>, DeleteConsumerGroupsOptions)`.
///
/// On success writes a [`kafka_admin_DeleteConsumerGroupsResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_DeleteConsumerGroupsResult_destroy`]) and returns null.
/// **A per-group failure is not a call failure**: it is reported by
/// [`kafka_admin_DeleteConsumerGroupsResult_get_error`] for that key. A
/// non-null return means the request could not be submitted at all.
///
/// # Parameters
///
/// - `group_ids` / `count`: the group ids to delete. A NULL entry is skipped.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///   `DeleteConsumerGroupsOptions` has no other field in Java.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_ids` must be null or have `count`
/// entries, each NULL or a valid C string; `out_result` must be null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_consumer_groups(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DeleteConsumerGroupsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let ids = unsafe { read_strings(group_ids, count) };
    let options = delete_consumer_groups_options(timeout_ms);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_delete_consumer_groups(a, &ids, options))) };
    unsafe { finish_sync(outcome, out_result, box_delete_consumer_groups_result) }
}

/// Deletes consumer groups asynchronously. See
/// [`kafka_admin_AdminClient_delete_consumer_groups`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_ids` must be null or have `count`
/// entries, each NULL or a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_consumer_groups_async(
    admin: *const kafka_admin_AdminClient_t,
    group_ids: *const *const c_char,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_delete_consumer_groups_callback_t,
    user_data: *mut c_void,
) {
    let ids = unsafe { read_strings(group_ids, count) };
    let options = delete_consumer_groups_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_delete_consumer_groups(a, &ids, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_delete_consumer_groups_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// removeMembersFromConsumerGroup
// ---------------------------------------------------------------------------

/// Completion callback for
/// [`kafka_admin_AdminClient_remove_members_from_consumer_group_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_RemoveMembersFromConsumerGroupResult_destroy`]
/// or `error` with `kafka_common_KafkaError_destroy`. A per-member failure
/// arrives inside `result`, not as `error`.
pub type kafka_admin_AdminClient_remove_members_from_consumer_group_callback_t = unsafe extern "C" fn(
    *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t,
    *mut kafka_common_KafkaError_t,
    *mut c_void,
);

/// Removes members from a consumer group, blocking until every per-member
/// future has resolved (synchronous).
///
/// This is
/// `removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)`.
///
/// On success writes a
/// [`kafka_admin_RemoveMembersFromConsumerGroupResult_t`] to `*out_result`
/// (free it with
/// [`kafka_admin_RemoveMembersFromConsumerGroupResult_destroy`]) and returns
/// null. **A per-member failure is not a call failure**: it is reported by
/// [`kafka_admin_RemoveMembersFromConsumerGroupResult_get_error`] for that
/// member. A non-null return means the request could not be submitted at all —
/// or that `remove_all` was true, where Java exposes no per-member outcome at
/// all and `all()` is the only observable, so the result handle is empty and
/// any failure is returned here.
///
/// # Parameters
///
/// - `group_id`: the consumer group to remove members from.
/// - `remove_all`: pass `true` for Java's no-argument
///   `RemoveMembersFromConsumerGroupOptions()` constructor, i.e. "remove every
///   member of the group"; `group_instance_ids` and `member_count` are then
///   ignored. Pass `false` to remove only the listed members. An explicit flag
///   rather than an empty array, because Java's `Collection` constructor
///   *rejects* an empty collection
///   (`IllegalArgumentException("Invalid empty members has been provided")`),
///   so an empty array must not silently mean "remove everything".
/// - `group_instance_ids` / `member_count`: the static members to remove, by
///   `group.instance.id`. A NULL entry is skipped; if that leaves none and
///   `remove_all` is false, Java's empty-members
///   `IllegalArgumentException` is returned.
/// - `reason`: Java's `RemoveMembersFromConsumerGroupOptions.reason(String)`,
///   or NULL to leave it unset.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_id` must be a valid C string; unless
/// `remove_all` is true, `group_instance_ids` must be null or have
/// `member_count` entries, each NULL or a valid C string; `reason` must be null
/// or a valid C string; `out_result` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_remove_members_from_consumer_group(
    admin: *const kafka_admin_AdminClient_t,
    group_id: *const c_char,
    remove_all: bool,
    group_instance_ids: *const *const c_char,
    member_count: i32,
    reason: *const c_char,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) -> *mut kafka_common_KafkaError_t {
    let group = unsafe { read_required_string(group_id, "group_id") };
    let options = unsafe { remove_members_options(remove_all, group_instance_ids, member_count, reason, timeout_ms) };
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| submit_remove_members_from_consumer_group(a, &group?, options?)) };
    unsafe { finish_sync(outcome, out_result, box_remove_members_from_consumer_group_result) }
}

/// Removes members from a consumer group asynchronously. See
/// [`kafka_admin_AdminClient_remove_members_from_consumer_group`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, a NULL `group_id`, or `remove_all`
/// false with no group instance id supplied). And it runs on a **tokio worker thread** if the
/// dispatcher's completion queue can no longer be reached when the result
/// arrives. Destroying the handle does not cause that — an outstanding
/// operation holds its own sender, so it cannot disconnect the queue; what
/// remains is a dispatcher thread that terminated abnormally, i.e. a panic
/// inside an earlier callback. So callbacks are not guaranteed to be serialised
/// on one thread. Do not hold a lock across this call and re-acquire it in the
/// callback, and publish everything the callback needs (including `user_data`)
/// before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; `group_id` must be a valid C string; unless
/// `remove_all` is true, `group_instance_ids` must be null or have
/// `member_count` entries, each NULL or a valid C string; `reason` must be null
/// or a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_remove_members_from_consumer_group_async(
    admin: *const kafka_admin_AdminClient_t,
    group_id: *const c_char,
    remove_all: bool,
    group_instance_ids: *const *const c_char,
    member_count: i32,
    reason: *const c_char,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_remove_members_from_consumer_group_callback_t,
    user_data: *mut c_void,
) {
    let group = unsafe { read_required_string(group_id, "group_id") };
    let options = unsafe { remove_members_options(remove_all, group_instance_ids, member_count, reason, timeout_ms) };
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| submit_remove_members_from_consumer_group(a, &group?, options?),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_remove_members_from_consumer_group_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// B5a — ACL and client-quota value types
//
// Every Java class bound here lives in `org.apache.kafka.common` (`.acl`,
// `.resource`, `.quota`), never in `clients.admin`, so per CLAUDE.md §3 the C
// spelling is `kafka_common_*`. `kafka_common_Node_t` and
// `kafka_common_KafkaError_t` are the existing precedent. Naming these
// `kafka_admin_*` would repeat the `kafka_consumer_TopicPartition_t` mistake
// in a second public surface.
//
// The three handles below are **output-only and borrowed**: they are interior
// references into the owning result handle's allocation, so they live until
// that result is destroyed and must never be freed. Request-side ACL bindings,
// filters and quota entities cross as parallel arrays instead — the shape
// `alterPartitionReassignments`, `alterConsumerGroupOffsets` and
// `listConsumerGroupOffsets` already use — which keeps ownership unambiguous:
// no handle in this module is ever both caller-owned and borrowed.
//
// `AclBinding.pattern()` (a `ResourcePattern`) and `.entry()` (an
// `AccessControlEntry`) are flattened onto the binding rather than getting
// handles of their own, following B2's treatment of
// `LogDirDescription.ReplicaInfo`. The accessor names keep Java's field names,
// so `kafka_common_AclBinding_resource_name` is `pattern().name()` and
// `..._principal` is `entry().principal()`.
//
// All four ACL enums have a numeric `code()` in Java
// (`AclOperation.code()`, `AclPermissionType.code()`, `ResourceType.code()`,
// `PatternType.code()`), so per the B2 rule they cross as `int32_t` codes
// rather than as `toString()` names. The codes are Java's, listed on each
// accessor.
// ---------------------------------------------------------------------------

/// Opaque handle to an `AclBinding` (Java's
/// `org.apache.kafka.common.acl.AclBinding`).
///
/// Borrowed from the owning `create_acls` / `describe_acls` / `delete_acls`
/// result handle; valid until that handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_common_AclBinding_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_common_AclBinding_t`].
///
/// `AclBinding`'s two components are flattened: `pattern()`'s three fields and
/// `entry()`'s four. Every string is non-nullable on a *binding* (only a
/// *filter* has nullable ones), because `AccessControlEntry::new` always stores
/// a principal and a host and `ResourcePattern` always stores a name.
struct AclBindingInner {
    resource_type: i32,
    resource_name_c: CString,
    pattern_type: i32,
    principal_c: CString,
    host_c: CString,
    operation: i32,
    permission_type: i32,
}

impl AclBindingInner {
    fn new(binding: &AclBinding) -> Self {
        let pattern = binding.pattern();
        let entry = binding.entry();
        Self {
            resource_type: i32::from(pattern.resource_type().code()),
            resource_name_c: to_cstring(pattern.name()),
            pattern_type: i32::from(pattern.pattern_type().code()),
            principal_c: to_cstring(entry.principal()),
            host_c: to_cstring(entry.host()),
            operation: i32::from(entry.operation().code()),
            permission_type: i32::from(entry.permission_type().code()),
        }
    }

    /// Deterministic ordering key. Java's `*Result` maps are unordered, but C
    /// addresses entries by index, so the flattened entries are sorted;
    /// `AclBinding` is `Hash + Eq` in Java and here, but not `Ord`.
    fn sort_key(&self) -> (i32, &str, i32, &str, &str, i32, i32) {
        (
            self.resource_type,
            self.resource_name_c.to_str().unwrap_or_default(),
            self.pattern_type,
            self.principal_c.to_str().unwrap_or_default(),
            self.host_c.to_str().unwrap_or_default(),
            self.operation,
            self.permission_type,
        )
    }

    fn as_ptr(&self) -> *const kafka_common_AclBinding_t {
        self as *const AclBindingInner as *const kafka_common_AclBinding_t
    }
}

/// Casts a `*const kafka_common_AclBinding_t` to a reference.
///
/// # Safety
///
/// `binding` must be a non-null borrowed pointer from an ACL result getter.
unsafe fn acl_binding_ref(binding: *const kafka_common_AclBinding_t) -> &'static AclBindingInner {
    unsafe { &*(binding as *const AclBindingInner) }
}

/// Returns `pattern().resourceType().code()`: UNKNOWN=0, ANY=1, TOPIC=2,
/// GROUP=3, CLUSTER=4, TRANSACTIONAL_ID=5, DELEGATION_TOKEN=6, USER=7.
///
/// A binding never carries ANY (`ResourcePattern` rejects it), but UNKNOWN is
/// possible when the broker reports a resource type this client does not know.
///
/// # Safety
///
/// `binding` must be a valid borrowed ACL-binding pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBinding_resource_type(binding: *const kafka_common_AclBinding_t) -> i32 {
    unsafe { acl_binding_ref(binding) }.resource_type
}

/// Returns `pattern().name()` (borrowed). Never null on a binding.
///
/// # Safety
///
/// `binding` must be a valid borrowed ACL-binding pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBinding_resource_name(
    binding: *const kafka_common_AclBinding_t,
) -> *const c_char {
    unsafe { acl_binding_ref(binding) }.resource_name_c.as_ptr()
}

/// Returns `pattern().patternType().code()`: UNKNOWN=0, ANY=1, MATCH=2,
/// LITERAL=3, PREFIXED=4.
///
/// A binding never carries ANY or MATCH (`ResourcePattern` rejects both);
/// those are filter-only pattern types.
///
/// # Safety
///
/// `binding` must be a valid borrowed ACL-binding pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBinding_pattern_type(binding: *const kafka_common_AclBinding_t) -> i32 {
    unsafe { acl_binding_ref(binding) }.pattern_type
}

/// Returns `entry().principal()` (borrowed), e.g. `"User:alice"`. Never null on
/// a binding.
///
/// # Safety
///
/// `binding` must be a valid borrowed ACL-binding pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBinding_principal(binding: *const kafka_common_AclBinding_t) -> *const c_char {
    unsafe { acl_binding_ref(binding) }.principal_c.as_ptr()
}

/// Returns `entry().host()` (borrowed), e.g. `"*"`. Never null on a binding.
///
/// # Safety
///
/// `binding` must be a valid borrowed ACL-binding pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBinding_host(binding: *const kafka_common_AclBinding_t) -> *const c_char {
    unsafe { acl_binding_ref(binding) }.host_c.as_ptr()
}

/// Returns `entry().operation().code()`: UNKNOWN=0, ANY=1, ALL=2, READ=3,
/// WRITE=4, CREATE=5, DELETE=6, ALTER=7, DESCRIBE=8, CLUSTER_ACTION=9,
/// DESCRIBE_CONFIGS=10, ALTER_CONFIGS=11, IDEMPOTENT_WRITE=12,
/// CREATE_TOKENS=13, DESCRIBE_TOKENS=14, TWO_PHASE_COMMIT=15.
///
/// A binding never carries ANY (`AccessControlEntry` rejects it).
///
/// # Safety
///
/// `binding` must be a valid borrowed ACL-binding pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBinding_operation(binding: *const kafka_common_AclBinding_t) -> i32 {
    unsafe { acl_binding_ref(binding) }.operation
}

/// Returns `entry().permissionType().code()`: UNKNOWN=0, ANY=1, DENY=2,
/// ALLOW=3.
///
/// A binding never carries ANY (`AccessControlEntry` rejects it).
///
/// # Safety
///
/// `binding` must be a valid borrowed ACL-binding pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBinding_permission_type(binding: *const kafka_common_AclBinding_t) -> i32 {
    unsafe { acl_binding_ref(binding) }.permission_type
}

/// Opaque handle to an `AclBindingFilter` (Java's
/// `org.apache.kafka.common.acl.AclBindingFilter`).
///
/// Borrowed from the owning `delete_acls` result handle; valid until that
/// handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_common_AclBindingFilter_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_common_AclBindingFilter_t`].
///
/// A *filter* differs from a binding in exactly two ways, both of which the C
/// surface has to preserve: its three string fields are genuinely nullable
/// (Java's `ResourcePatternFilter.name()` / `AccessControlEntryFilter
/// .principal()` / `.host()` return null to mean "match any"), and its enums
/// may be ANY or MATCH.
///
/// A null `const char *` is the right encoding for those three, and needs no
/// companion discriminant: it is distinguishable from a pointer to `""`, and an
/// empty name is a legal, distinct filter. That is the B3 rule applied, not
/// waived — a discriminant is required only where the sentinel would *collide*
/// with a real value, as it would for a nullable number.
struct AclBindingFilterInner {
    resource_type: i32,
    resource_name_c: Option<CString>,
    pattern_type: i32,
    principal_c: Option<CString>,
    host_c: Option<CString>,
    operation: i32,
    permission_type: i32,
}

impl AclBindingFilterInner {
    fn new(filter: &AclBindingFilter) -> Self {
        let pattern = filter.pattern_filter();
        let entry = filter.entry_filter();
        Self {
            resource_type: i32::from(pattern.resource_type().code()),
            resource_name_c: pattern.name().map(to_cstring),
            pattern_type: i32::from(pattern.pattern_type().code()),
            principal_c: entry.principal().map(to_cstring),
            host_c: entry.host().map(to_cstring),
            operation: i32::from(entry.operation().code()),
            permission_type: i32::from(entry.permission_type().code()),
        }
    }

    /// Deterministic ordering key; see [`AclBindingInner::sort_key`]. An absent
    /// (match-any) string sorts before any present one.
    fn sort_key(&self) -> (i32, Option<&str>, i32, Option<&str>, Option<&str>, i32, i32) {
        fn text(value: &Option<CString>) -> Option<&str> {
            value.as_ref().map(|s| s.to_str().unwrap_or_default())
        }
        (
            self.resource_type,
            text(&self.resource_name_c),
            self.pattern_type,
            text(&self.principal_c),
            text(&self.host_c),
            self.operation,
            self.permission_type,
        )
    }
}

/// Casts a `*const kafka_common_AclBindingFilter_t` to a reference.
///
/// # Safety
///
/// `filter` must be a non-null borrowed pointer from a `delete_acls` result
/// getter.
unsafe fn acl_binding_filter_ref(filter: *const kafka_common_AclBindingFilter_t) -> &'static AclBindingFilterInner {
    unsafe { &*(filter as *const AclBindingFilterInner) }
}

/// Returns `patternFilter().resourceType().code()`. See
/// [`kafka_common_AclBinding_resource_type`] for the codes; a filter may also
/// carry ANY=1, which matches every resource type.
///
/// # Safety
///
/// `filter` must be a valid borrowed ACL-filter pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBindingFilter_resource_type(
    filter: *const kafka_common_AclBindingFilter_t,
) -> i32 {
    unsafe { acl_binding_filter_ref(filter) }.resource_type
}

/// Returns `patternFilter().name()` (borrowed), or null when the filter matches
/// any resource name (Java's null name). Null is distinct from a pointer to the
/// empty string, which filters on the name `""`.
///
/// # Safety
///
/// `filter` must be a valid borrowed ACL-filter pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBindingFilter_resource_name(
    filter: *const kafka_common_AclBindingFilter_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { acl_binding_filter_ref(filter) }.resource_name_c)
}

/// Returns `patternFilter().patternType().code()`. See
/// [`kafka_common_AclBinding_pattern_type`] for the codes; a filter may also
/// carry ANY=1 (any pattern type) and MATCH=2 (literal, prefixed and wildcard
/// patterns that would match the name).
///
/// # Safety
///
/// `filter` must be a valid borrowed ACL-filter pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBindingFilter_pattern_type(
    filter: *const kafka_common_AclBindingFilter_t,
) -> i32 {
    unsafe { acl_binding_filter_ref(filter) }.pattern_type
}

/// Returns `entryFilter().principal()` (borrowed), or null when the filter
/// matches any principal.
///
/// # Safety
///
/// `filter` must be a valid borrowed ACL-filter pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBindingFilter_principal(
    filter: *const kafka_common_AclBindingFilter_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { acl_binding_filter_ref(filter) }.principal_c)
}

/// Returns `entryFilter().host()` (borrowed), or null when the filter matches
/// any host.
///
/// # Safety
///
/// `filter` must be a valid borrowed ACL-filter pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBindingFilter_host(
    filter: *const kafka_common_AclBindingFilter_t,
) -> *const c_char {
    optional_cstring_ptr(&unsafe { acl_binding_filter_ref(filter) }.host_c)
}

/// Returns `entryFilter().operation().code()`. See
/// [`kafka_common_AclBinding_operation`] for the codes; a filter may also carry
/// ANY=1.
///
/// # Safety
///
/// `filter` must be a valid borrowed ACL-filter pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBindingFilter_operation(
    filter: *const kafka_common_AclBindingFilter_t,
) -> i32 {
    unsafe { acl_binding_filter_ref(filter) }.operation
}

/// Returns `entryFilter().permissionType().code()`. See
/// [`kafka_common_AclBinding_permission_type`] for the codes; a filter may also
/// carry ANY=1.
///
/// # Safety
///
/// `filter` must be a valid borrowed ACL-filter pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_AclBindingFilter_permission_type(
    filter: *const kafka_common_AclBindingFilter_t,
) -> i32 {
    unsafe { acl_binding_filter_ref(filter) }.permission_type
}

/// Opaque handle to a `ClientQuotaEntity` (Java's
/// `org.apache.kafka.common.quota.ClientQuotaEntity`).
///
/// Borrowed from the owning `describe_client_quotas` / `alter_client_quotas`
/// result handle; valid until that handle is destroyed. Do not free it.
#[repr(C)]
pub struct kafka_common_ClientQuotaEntity_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_common_ClientQuotaEntity_t`].
///
/// Java's entity is a `Map<String, String>` from entity type (`"user"`,
/// `"client-id"`, `"ip"`) to entity name, with a **null value meaning the
/// built-in default entity** for that type — the `--entity-default` of the
/// command-line tools — which is not the same as the type being absent from the
/// map, and not the same as the name `""`.
///
/// C gets the map as an indexed sequence sorted by entity type. Absence is
/// expressed by the type simply not appearing; "default entity" is a null name
/// pointer; the name `""` is a pointer to an empty string. All three stay
/// distinct without an extra discriminant, because a null pointer cannot
/// collide with a pointer to `""`.
struct ClientQuotaEntityInner {
    entry_types_c: Vec<CString>,
    entry_names_c: Vec<Option<CString>>,
}

impl ClientQuotaEntityInner {
    fn new(entity: &ClientQuotaEntity) -> Self {
        // Sorted by entity type so C's index addressing is reproducible; Java's
        // map is unordered.
        let mut entries: Vec<(&String, &Option<String>)> = entity.entries().iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        Self {
            entry_types_c: entries.iter().map(|(t, _)| to_cstring(t)).collect(),
            entry_names_c: entries.iter().map(|(_, n)| n.as_deref().map(to_cstring)).collect(),
        }
    }

    /// Deterministic ordering key across entities. `ClientQuotaEntity` is
    /// `Hash + Eq` but not `Ord`, and its entries are already type-sorted, so
    /// the pair sequence orders entities reproducibly.
    fn sort_key(&self) -> Vec<(&str, Option<&str>)> {
        self.entry_types_c
            .iter()
            .zip(&self.entry_names_c)
            .map(|(t, n)| {
                (
                    t.to_str().unwrap_or_default(),
                    n.as_ref().map(|n| n.to_str().unwrap_or_default()),
                )
            })
            .collect()
    }

    fn as_ptr(&self) -> *const kafka_common_ClientQuotaEntity_t {
        self as *const ClientQuotaEntityInner as *const kafka_common_ClientQuotaEntity_t
    }
}

/// Casts a `*const kafka_common_ClientQuotaEntity_t` to a reference.
///
/// # Safety
///
/// `entity` must be a non-null borrowed pointer from a client-quota result
/// getter.
unsafe fn client_quota_entity_ref(entity: *const kafka_common_ClientQuotaEntity_t) -> &'static ClientQuotaEntityInner {
    unsafe { &*(entity as *const ClientQuotaEntityInner) }
}

/// Returns the number of entity-type entries (the size of Java's
/// `entries()` map).
///
/// # Safety
///
/// `entity` must be a valid borrowed client-quota-entity pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClientQuotaEntity_entry_count(
    entity: *const kafka_common_ClientQuotaEntity_t,
) -> i32 {
    unsafe { client_quota_entity_ref(entity) }.entry_types_c.len() as i32
}

/// Returns the entity type at `index` (borrowed) — `"user"`, `"client-id"` or
/// `"ip"` — or null if out of range. Entries are sorted by entity type.
///
/// # Safety
///
/// `entity` must be a valid borrowed client-quota-entity pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClientQuotaEntity_get_entry_type(
    entity: *const kafka_common_ClientQuotaEntity_t,
    index: i32,
) -> *const c_char {
    cstring_at(&unsafe { client_quota_entity_ref(entity) }.entry_types_c, index)
}

/// Returns the entity name at `index` (borrowed), or null if out of range **or
/// if the entry names the built-in default entity** for its type.
///
/// The two nulls are told apart by [`kafka_common_ClientQuotaEntity_entry_count`]:
/// an `index` below the count always denotes a present entry, so a null there
/// means "default entity", Java's null map value. A name of `""` is a real,
/// distinct name and comes back as a pointer to an empty string.
///
/// # Safety
///
/// `entity` must be a valid borrowed client-quota-entity pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClientQuotaEntity_get_entry_name(
    entity: *const kafka_common_ClientQuotaEntity_t,
    index: i32,
) -> *const c_char {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { client_quota_entity_ref(entity) }.entry_names_c.get(index as usize) {
        Some(name) => optional_cstring_ptr(name),
        None => std::ptr::null(),
    }
}

// ---------------------------------------------------------------------------
// B5a — ACL and client-quota input marshaling and submission helpers
//
// Request-side ACL bindings, filters and quota entities cross as parallel
// arrays, following `alterPartitionReassignments` /
// `alterConsumerGroupOffsets` (flat) and `listConsumerGroupOffsets` (ragged
// two-level). Per CLAUDE.md §3 a NULL *required array* is a caller
// programming error and is not diagnosed; it is read as "no entries", exactly
// as `read_alter_group_offsets` already does. A NULL *element* of a
// non-nullable string array is diagnosed, because it is indistinguishable from
// a legitimate absent value otherwise and Java's constructor would reject it.
// ---------------------------------------------------------------------------

/// Per-binding outcomes of `createAcls`.
type CreateAclsOutcomes = HashMap<AclBinding, Result<(), KafkaError>>;

/// Per-filter outcomes of `deleteAcls`.
type DeleteAclsOutcomes = HashMap<AclBindingFilter, Result<FilterResults, KafkaError>>;

/// Per-entity outcomes of `alterClientQuotas`.
type AlterClientQuotasOutcomes = HashMap<ClientQuotaEntity, Result<(), KafkaError>>;

/// The whole-map outcome of `describeClientQuotas`.
type DescribeClientQuotasOutcome = HashMap<ClientQuotaEntity, HashMap<String, f64>>;

/// Reads the `index`th entry of a *required* C string array.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] naming `field` and `index` when the
/// entry is NULL.
///
/// # Safety
///
/// `strings` must be non-null with at least `index + 1` entries.
unsafe fn required_string_at(strings: *const *const c_char, index: usize, field: &str) -> Result<String, KafkaError> {
    let ptr = unsafe { *strings.add(index) };
    if ptr.is_null() {
        return Err(KafkaError::illegal_argument(format!(
            "{field} at index {index} must not be null"
        )));
    }
    Ok(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string())
}

/// Reads the `index`th entry of a *nullable* C string array, preserving NULL as
/// `None`.
///
/// Unlike [`read_strings`], which drops NULL entries, this keeps the array
/// index aligned with its siblings — essential for parallel arrays, where a
/// dropped entry would silently shift every later field onto the wrong row.
///
/// # Safety
///
/// `strings` must be null, or non-null with at least `index + 1` entries.
unsafe fn optional_string_at(strings: *const *const c_char, index: usize) -> Option<String> {
    if strings.is_null() {
        return None;
    }
    let ptr = unsafe { *strings.add(index) };
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string())
}

/// Builds one [`AclBinding`] from the `index`th row of the request arrays.
///
/// # Errors
///
/// Propagates the `IllegalArgumentException`s Java's `ResourcePattern` and
/// `AccessControlEntry` constructors throw — an ANY resource type, an ANY or
/// MATCH pattern type, an ANY operation or an ANY permission type — prefixed
/// with the row index, since C has no other way to say which entry was wrong.
///
/// # Safety
///
/// Every array must be non-null with at least `index + 1` entries.
#[allow(clippy::too_many_arguments)]
unsafe fn read_acl_binding_at(
    resource_types: *const i32,
    resource_names: *const *const c_char,
    pattern_types: *const i32,
    principals: *const *const c_char,
    hosts: *const *const c_char,
    operations: *const i32,
    permission_types: *const i32,
    index: usize,
) -> Result<AclBinding, KafkaError> {
    let context = |e: KafkaError| KafkaError::illegal_argument(format!("acl at index {index}: {}", e.message()));
    let pattern = ResourcePattern::new(
        ResourceType::from_code(enum_code_or_unknown(unsafe { *resource_types.add(index) })),
        unsafe { required_string_at(resource_names, index, "resource name")? },
        PatternType::from_code(enum_code_or_unknown(unsafe { *pattern_types.add(index) })),
    )
    .map_err(context)?;
    let entry = AccessControlEntry::new(
        unsafe { required_string_at(principals, index, "principal")? },
        unsafe { required_string_at(hosts, index, "host")? },
        AclOperation::from_code(enum_code_or_unknown(unsafe { *operations.add(index) })),
        AclPermissionType::from_code(enum_code_or_unknown(unsafe { *permission_types.add(index) })),
    )
    .map_err(context)?;
    Ok(AclBinding::new(pattern, entry))
}

/// Reads `count` rows of parallel arrays into [`AclBinding`]s.
///
/// # Safety
///
/// Each array must be null, or have `count` entries; string entries must be
/// NULL or valid C strings.
#[allow(clippy::too_many_arguments)]
unsafe fn read_acl_bindings(
    resource_types: *const i32,
    resource_names: *const *const c_char,
    pattern_types: *const i32,
    principals: *const *const c_char,
    hosts: *const *const c_char,
    operations: *const i32,
    permission_types: *const i32,
    count: i32,
) -> Result<Vec<AclBinding>, KafkaError> {
    let n = count.max(0) as usize;
    if resource_types.is_null()
        || resource_names.is_null()
        || pattern_types.is_null()
        || principals.is_null()
        || hosts.is_null()
        || operations.is_null()
        || permission_types.is_null()
    {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(n);
    for index in 0..n {
        out.push(unsafe {
            read_acl_binding_at(
                resource_types,
                resource_names,
                pattern_types,
                principals,
                hosts,
                operations,
                permission_types,
                index,
            )?
        });
    }
    Ok(out)
}

/// Builds one [`AclBindingFilter`] from scalar fields.
///
/// Unlike a binding this is infallible: Java's filter constructors accept ANY
/// and MATCH, which is the whole point of a filter, and their three strings are
/// nullable (null = match any).
///
/// # Safety
///
/// The three string pointers must be NULL or valid C strings.
unsafe fn build_acl_binding_filter(
    resource_type: i32,
    resource_name: *const c_char,
    pattern_type: i32,
    principal: *const c_char,
    host: *const c_char,
    operation: i32,
    permission_type: i32,
) -> AclBindingFilter {
    let text = |ptr: *const c_char| {
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string())
        }
    };
    AclBindingFilter::new(
        ResourcePatternFilter::new(
            ResourceType::from_code(enum_code_or_unknown(resource_type)),
            text(resource_name),
            PatternType::from_code(enum_code_or_unknown(pattern_type)),
        ),
        AccessControlEntryFilter::new(
            text(principal),
            text(host),
            AclOperation::from_code(enum_code_or_unknown(operation)),
            AclPermissionType::from_code(enum_code_or_unknown(permission_type)),
        ),
    )
}

/// Reads `count` rows of parallel arrays into [`AclBindingFilter`]s.
///
/// # Safety
///
/// Each array must be null, or have `count` entries; string entries must be
/// NULL or valid C strings.
#[allow(clippy::too_many_arguments)]
unsafe fn read_acl_binding_filters(
    resource_types: *const i32,
    resource_names: *const *const c_char,
    pattern_types: *const i32,
    principals: *const *const c_char,
    hosts: *const *const c_char,
    operations: *const i32,
    permission_types: *const i32,
    count: i32,
) -> Vec<AclBindingFilter> {
    let n = count.max(0) as usize;
    if resource_types.is_null() || pattern_types.is_null() || operations.is_null() || permission_types.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for index in 0..n {
        out.push(unsafe {
            build_acl_binding_filter(
                *resource_types.add(index),
                if resource_names.is_null() {
                    std::ptr::null()
                } else {
                    *resource_names.add(index)
                },
                *pattern_types.add(index),
                if principals.is_null() {
                    std::ptr::null()
                } else {
                    *principals.add(index)
                },
                if hosts.is_null() {
                    std::ptr::null()
                } else {
                    *hosts.add(index)
                },
                *operations.add(index),
                *permission_types.add(index),
            )
        });
    }
    out
}

/// Reads `count` filter components into a [`ClientQuotaFilter`].
///
/// `match_types` carries the wire match-type constants
/// (`MATCH_TYPE_EXACT` = 0, `MATCH_TYPE_DEFAULT` = 1,
/// `MATCH_TYPE_SPECIFIED` = 2), which are the real Kafka protocol values, not
/// invented codes. They are needed as an explicit discriminant because a
/// component's match is a genuine tri-state (Java
/// `ClientQuotaFilterComponent.match()` is `Optional<String>` that may also be
/// null) whose DEFAULT and ANY arms both carry no name — so a null name alone
/// could not tell them apart, unlike the nullable strings elsewhere in this
/// slice.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] for an unrecognised match type, or
/// for an EXACT component with no name.
///
/// # Safety
///
/// Each array must be null, or have `count` entries; string entries must be
/// NULL or valid C strings.
unsafe fn read_client_quota_filter(
    entity_types: *const *const c_char,
    match_types: *const i32,
    match_names: *const *const c_char,
    count: i32,
    strict: bool,
) -> Result<ClientQuotaFilter, KafkaError> {
    let n = count.max(0) as usize;
    if entity_types.is_null() || match_types.is_null() {
        // No components at all: Java's `ClientQuotaFilter.all()` when not
        // strict, or `containsOnly([])` when strict — which matches only the
        // entity with no components.
        return Ok(if strict {
            ClientQuotaFilter::contains_only(Vec::new())
        } else {
            ClientQuotaFilter::all()
        });
    }
    let mut components = Vec::with_capacity(n);
    for index in 0..n {
        let entity_type = unsafe { required_string_at(entity_types, index, "entity type")? };
        let match_type = unsafe { *match_types.add(index) };
        let name = unsafe { optional_string_at(match_names, index) };
        // The `MATCH_TYPE_*` constants are bare wire values with no UNKNOWN
        // member, so a code that does not narrow is rejected rather than folded
        // onto a valid match type (see `narrow_enum_code`).
        let component = match narrow_enum_code(match_type) {
            Some(MATCH_TYPE_EXACT) => {
                let name = name.ok_or_else(|| {
                    KafkaError::illegal_argument(format!(
                        "quota filter component at index {index} has match type EXACT but no match name"
                    ))
                })?;
                ClientQuotaFilterComponent::of_entity(entity_type, name)
            },
            Some(MATCH_TYPE_DEFAULT) => ClientQuotaFilterComponent::of_default_entity(entity_type),
            Some(MATCH_TYPE_SPECIFIED) => ClientQuotaFilterComponent::of_entity_type(entity_type),
            _ => {
                return Err(KafkaError::illegal_argument(format!(
                    "quota filter component at index {index} has unknown match type {match_type}"
                )));
            },
        };
        components.push(component);
    }
    Ok(if strict {
        ClientQuotaFilter::contains_only(components)
    } else {
        ClientQuotaFilter::contains(components)
    })
}

/// Reads one ragged row of `(entity type, entity name)` pairs into a
/// [`ClientQuotaEntity`].
///
/// A NULL name entry is Java's null map value: the built-in **default entity**
/// for that type, not an absent entry and not the empty name.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] when a type is NULL, or when the row
/// repeats an entity type — Java takes a `Map`, so a duplicate key could only
/// be silently dropped otherwise.
///
/// # Safety
///
/// `types` must be non-null with `count` entries; `names` must be null or have
/// `count` entries.
unsafe fn read_client_quota_entity(
    types: *const *const c_char,
    names: *const *const c_char,
    count: i32,
    row: usize,
) -> Result<ClientQuotaEntity, KafkaError> {
    let n = count.max(0) as usize;
    let mut entries: HashMap<String, Option<String>> = HashMap::with_capacity(n);
    for index in 0..n {
        let entity_type = unsafe { required_string_at(types, index, "entity type")? };
        let name = unsafe { optional_string_at(names, index) };
        if entries.insert(entity_type.clone(), name).is_some() {
            return Err(KafkaError::illegal_argument(format!(
                "quota alteration at index {row} repeats entity type `{entity_type}`"
            )));
        }
    }
    Ok(ClientQuotaEntity::new(entries))
}

/// Reads `count` ragged rows into [`ClientQuotaAlteration`]s.
///
/// The two-level shape follows `listConsumerGroupOffsets`: an array of arrays
/// plus a per-row count. `op_has_values` is the explicit discriminant for
/// Java's nullable `Double` op value — a nullable *number* needs one, because
/// every sentinel `double` is also a legal quota value — and `false` means
/// **remove this quota**, Java's `ClientQuotaAlteration.Op(key, null)`.
///
/// # Errors
///
/// Propagates the per-row errors of [`read_client_quota_entity`], and rejects a
/// duplicate entity across rows.
///
/// **Deviation from Java, deliberate.** Java does *not* reject: it keeps the
/// `Collection<ClientQuotaAlteration>` intact and hands it verbatim to
/// `new AlterClientQuotasRequest.Builder(entries, ...)`, so both alterations
/// reach the broker; only the *future* map collapses, because
/// `futures.put(entry.entity(), ...)` overwrites the earlier entry
/// (`KafkaAdminClient.java:4301-4313`). `src/admin/kafka_admin_client.rs`
/// mirrors that. The C layer is stricter because the collapse is not
/// attributable here: the result crosses as a flat, index-addressed array of
/// entities built from that map, so a duplicate silently yields fewer rows than
/// the request had and the caller — who passed parallel arrays, not a map —
/// has no way to learn which of its two rows the surviving outcome describes.
/// A Java caller holds the map and can see it shrink. Rejecting at the exact
/// row index turns unattributable data loss into a named error; the cost is
/// that a C caller cannot express "send two alterations for one entity and let
/// the broker apply both in order", which is the only thing Java can do here
/// that this cannot.
///
/// # Safety
///
/// Each array must be null, or have `count` entries, each of which is null or
/// has the matching per-row count of entries.
#[allow(clippy::too_many_arguments)]
unsafe fn read_client_quota_alterations(
    entity_types: *const *const *const c_char,
    entity_names: *const *const *const c_char,
    entity_counts: *const i32,
    op_keys: *const *const *const c_char,
    op_values: *const *const f64,
    op_has_values: *const *const bool,
    op_counts: *const i32,
    count: i32,
) -> Result<Vec<ClientQuotaAlteration>, KafkaError> {
    let n = count.max(0) as usize;
    if entity_types.is_null() || entity_counts.is_null() {
        return Ok(Vec::new());
    }
    let mut out: Vec<ClientQuotaAlteration> = Vec::with_capacity(n);
    let mut seen: HashSet<ClientQuotaEntity> = HashSet::with_capacity(n);
    for row in 0..n {
        let types = unsafe { *entity_types.add(row) };
        if types.is_null() {
            return Err(KafkaError::illegal_argument(format!(
                "quota alteration at index {row} has no entity types"
            )));
        }
        let names = if entity_names.is_null() {
            std::ptr::null()
        } else {
            unsafe { *entity_names.add(row) }
        };
        let entity = unsafe { read_client_quota_entity(types, names, *entity_counts.add(row), row)? };
        if !seen.insert(entity.clone()) {
            return Err(KafkaError::illegal_argument(format!(
                "quota alteration at index {row} repeats an entity already altered by an earlier entry"
            )));
        }

        let op_count = if op_counts.is_null() {
            0
        } else {
            unsafe { *op_counts.add(row) }
        };
        let keys = if op_keys.is_null() {
            std::ptr::null()
        } else {
            unsafe { *op_keys.add(row) }
        };
        let values = if op_values.is_null() {
            std::ptr::null()
        } else {
            unsafe { *op_values.add(row) }
        };
        let has_values = if op_has_values.is_null() {
            std::ptr::null()
        } else {
            unsafe { *op_has_values.add(row) }
        };
        let mut ops = Vec::with_capacity(op_count.max(0) as usize);
        if !keys.is_null() {
            for index in 0..op_count.max(0) as usize {
                let key = unsafe { required_string_at(keys, index, "quota op key")? };
                let present = !has_values.is_null() && unsafe { *has_values.add(index) };
                let value = if present && !values.is_null() {
                    Some(unsafe { *values.add(index) })
                } else {
                    None
                };
                ops.push(ClientQuotaOp::new(key, value));
            }
        }
        out.push(ClientQuotaAlteration::new(entity, ops));
    }
    Ok(out)
}

/// Submits `createAcls` and returns the collect-all future over its per-binding
/// futures.
fn submit_create_acls(
    admin: &dyn Admin,
    acls: &[AclBinding],
    options: CreateAclsOptions,
) -> KafkaFuture<CreateAclsOutcomes> {
    let result = admin.create_acls(acls, options);
    // Driven from the result's own map (Java's `values()`), which is the
    // authority on which bindings got a future.
    let entries: Vec<(AclBinding, KafkaFuture<()>)> =
        result.values().iter().map(|(b, f)| (b.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Submits `describeAcls` and returns its single listing future.
fn submit_describe_acls(
    admin: &dyn Admin,
    filter: &AclBindingFilter,
    options: DescribeAclsOptions,
) -> KafkaFuture<Vec<AclBinding>> {
    admin.describe_acls(filter, options).values().clone()
}

/// Submits `deleteAcls` and returns the collect-all future over its per-filter
/// futures.
fn submit_delete_acls(
    admin: &dyn Admin,
    filters: &[AclBindingFilter],
    options: DeleteAclsOptions,
) -> KafkaFuture<DeleteAclsOutcomes> {
    let result = admin.delete_acls(filters, options);
    let entries: Vec<(AclBindingFilter, KafkaFuture<FilterResults>)> =
        result.values().iter().map(|(f, fut)| (f.clone(), fut.clone())).collect();
    KafkaFuture::join_map_results(entries)
}

/// Submits `describeClientQuotas` and returns its single whole-map future.
fn submit_describe_client_quotas(
    admin: &dyn Admin,
    filter: &ClientQuotaFilter,
    options: DescribeClientQuotasOptions,
) -> KafkaFuture<DescribeClientQuotasOutcome> {
    admin.describe_client_quotas(filter, options).entities().clone()
}

/// Submits `alterClientQuotas` and returns the collect-all future over its
/// per-entity futures.
fn submit_alter_client_quotas(
    admin: &dyn Admin,
    entries: &[ClientQuotaAlteration],
    options: AlterClientQuotasOptions,
) -> KafkaFuture<AlterClientQuotasOutcomes> {
    let result = admin.alter_client_quotas(entries, options);
    let futures: Vec<(ClientQuotaEntity, KafkaFuture<()>)> =
        result.values().iter().map(|(e, f)| (e.clone(), f.clone())).collect();
    KafkaFuture::join_map_results(futures)
}

// ---------------------------------------------------------------------------
// B5a — ACL and client-quota result handles
//
// Accessors follow each Java `*Result`'s future shape (`PLAN-bindings.md` D2 as
// amended after B4), not a fixed template:
//
//   - `Map<K, KafkaFuture<Void>>`   -> `_get_error(i)` only, no value
//     (`createAcls`, `alterClientQuotas`)
//   - one `KafkaFuture<Collection<V>>` / `KafkaFuture<Map<K, V>>` for the whole
//     call                          -> `_count` plus value accessors, and **no**
//     `_get_error`: a failure is the call's error
//     (`describeAcls`, `describeClientQuotas`)
//   - `Map<K, KafkaFuture<V>>` where `V` is itself a collection
//                                   -> `_get_error(i)` for the key's future
//     plus a second index level over `V`
//     (`deleteAcls`, whose `FilterResults` holds one `FilterResult` per matched
//      ACL, each carrying *either* a binding *or* its own exception)
// ---------------------------------------------------------------------------

// [`sorted_entries`] cannot order these results: it needs `K: Ord`, which
// `AclBinding`, `AclBindingFilter` and `ClientQuotaEntity` are not — they are
// `Hash + Eq` in Java too, and inventing an `Ord` for a Java type that has none
// would be a fabricated contract. Each `box_*` below instead sorts the
// already-flattened rows by their C-visible field tuple (`sort_key`), which
// gives the same reproducible index addressing. The comparison closure is
// written out at each site rather than shared, because a `Fn(&I) -> K` helper
// cannot express that `K` borrows from `I`.

/// Opaque handle to a flattened `CreateAclsResult`.
#[repr(C)]
pub struct kafka_admin_CreateAclsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_CreateAclsResult_t`].
///
/// `CreateAclsResult.values()` is `Map<AclBinding, KafkaFuture<Void>>`: a
/// per-binding future that carries no value, so the handle exposes the binding
/// and its error and nothing else.
struct CreateAclsResultInner {
    bindings: Vec<AclBindingInner>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-binding `createAcls` outcomes into the C handle.
fn box_create_acls_result(outcomes: CreateAclsOutcomes) -> *mut kafka_admin_CreateAclsResult_t {
    let mut rows: Vec<(AclBindingInner, Option<KafkaErrorInner>)> = outcomes
        .into_iter()
        .map(|(binding, outcome)| (AclBindingInner::new(&binding), outcome.err().map(error_inner)))
        .collect();
    rows.sort_by(|a, b| a.0.sort_key().cmp(&b.0.sort_key()));
    let (bindings, errors) = rows.into_iter().unzip();
    Box::into_raw(Box::new(CreateAclsResultInner { bindings, errors })) as *mut kafka_admin_CreateAclsResult_t
}

/// Casts a `*const kafka_admin_CreateAclsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `create_acls` call.
unsafe fn create_acls_result_ref(result: *const kafka_admin_CreateAclsResult_t) -> &'static CreateAclsResultInner {
    unsafe { &*(result as *const CreateAclsResultInner) }
}

/// Returns the number of ACL bindings in the result.
///
/// # Safety
///
/// `result` must be a valid `create_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateAclsResult_count(result: *const kafka_admin_CreateAclsResult_t) -> i32 {
    unsafe { create_acls_result_ref(result) }.bindings.len() as i32
}

/// Returns the binding at `index` (borrowed), or null if out of range. Do not
/// free it; it dies with the result handle. Entries are sorted by resource
/// type, resource name, pattern type, principal, host, operation and permission
/// type.
///
/// # Safety
///
/// `result` must be a valid `create_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateAclsResult_get_binding(
    result: *const kafka_admin_CreateAclsResult_t,
    index: i32,
) -> *const kafka_common_AclBinding_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { create_acls_result_ref(result) }.bindings.get(index as usize) {
        Some(binding) => binding.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the error for the binding at `index` (borrowed), or null if it was
/// created successfully or `index` is out of range. Do not destroy it.
///
/// # Safety
///
/// `result` must be a valid `create_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateAclsResult_get_error(
    result: *const kafka_admin_CreateAclsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    optional_error_at(&unsafe { create_acls_result_ref(result) }.errors, index)
}

/// Destroys a `create_acls` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `create_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateAclsResult_destroy(result: *mut kafka_admin_CreateAclsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut CreateAclsResultInner)) };
    }
}

/// Opaque handle to a flattened `DescribeAclsResult`.
#[repr(C)]
pub struct kafka_admin_DescribeAclsResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeAclsResult_t`].
///
/// `DescribeAclsResult` holds a single `KafkaFuture<Collection<AclBinding>>`
/// and has no `all()` and no per-key future at all, so there is nothing for a
/// `_get_error(i)` to report: any failure is the call's error.
struct DescribeAclsResultInner {
    bindings: Vec<AclBindingInner>,
}

/// Flattens the described bindings into the C handle.
fn box_describe_acls_result(bindings: Vec<AclBinding>) -> *mut kafka_admin_DescribeAclsResult_t {
    Box::into_raw(Box::new(DescribeAclsResultInner {
        bindings: bindings.iter().map(AclBindingInner::new).collect(),
    })) as *mut kafka_admin_DescribeAclsResult_t
}

/// Casts a `*const kafka_admin_DescribeAclsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_acls` call.
unsafe fn describe_acls_result_ref(
    result: *const kafka_admin_DescribeAclsResult_t,
) -> &'static DescribeAclsResultInner {
    unsafe { &*(result as *const DescribeAclsResultInner) }
}

/// Returns the number of matching ACL bindings.
///
/// # Safety
///
/// `result` must be a valid `describe_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeAclsResult_count(result: *const kafka_admin_DescribeAclsResult_t) -> i32 {
    unsafe { describe_acls_result_ref(result) }.bindings.len() as i32
}

/// Returns the binding at `index` (borrowed), or null if out of range. Do not
/// free it. Bindings keep the order the broker reported them in, as Java's
/// `values()` collection does.
///
/// # Safety
///
/// `result` must be a valid `describe_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeAclsResult_get_binding(
    result: *const kafka_admin_DescribeAclsResult_t,
    index: i32,
) -> *const kafka_common_AclBinding_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_acls_result_ref(result) }.bindings.get(index as usize) {
        Some(binding) => binding.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Destroys a `describe_acls` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeAclsResult_destroy(result: *mut kafka_admin_DescribeAclsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeAclsResultInner)) };
    }
}

/// Opaque handle to a flattened `DeleteAclsResult`.
#[repr(C)]
pub struct kafka_admin_DeleteAclsResult_t {
    _private: [u8; 0],
}

/// One `DeleteAclsResult.FilterResult`: exactly one of the two is present.
struct DeleteAclsFilterResultInner {
    binding: Option<AclBindingInner>,
    error: Option<KafkaErrorInner>,
}

/// Backing state for [`kafka_admin_DeleteAclsResult_t`].
///
/// `DeleteAclsResult.values()` is `Map<AclBindingFilter,
/// KafkaFuture<FilterResults>>`, so there are two index levels: the filter, and
/// within it the ACLs that filter matched. `errors[i]` is the *filter's* future
/// failing (nothing was deleted for it); a per-ACL failure lives inside
/// `results[i][j]` instead, which is Java's `FilterResult.exception()`.
struct DeleteAclsResultInner {
    filters: Vec<AclBindingFilterInner>,
    errors: Vec<Option<KafkaErrorInner>>,
    results: Vec<Vec<DeleteAclsFilterResultInner>>,
}

/// Flattens the per-filter `deleteAcls` outcomes into the C handle.
fn box_delete_acls_result(outcomes: DeleteAclsOutcomes) -> *mut kafka_admin_DeleteAclsResult_t {
    type Row = (
        AclBindingFilterInner,
        (Option<KafkaErrorInner>, Vec<DeleteAclsFilterResultInner>),
    );
    let mut rows: Vec<Row> = outcomes
        .into_iter()
        .map(|(filter, outcome)| {
            let flattened = match outcome {
                Ok(results) => (
                    None,
                    results
                        .values()
                        .iter()
                        .map(|r| DeleteAclsFilterResultInner {
                            binding: r.binding().map(AclBindingInner::new),
                            error: r.exception().cloned().map(error_inner),
                        })
                        .collect(),
                ),
                Err(e) => (Some(error_inner(e)), Vec::new()),
            };
            (AclBindingFilterInner::new(&filter), flattened)
        })
        .collect();
    rows.sort_by(|a, b| a.0.sort_key().cmp(&b.0.sort_key()));

    let mut filters = Vec::with_capacity(rows.len());
    let mut errors = Vec::with_capacity(rows.len());
    let mut results = Vec::with_capacity(rows.len());
    for (filter, (error, filter_results)) in rows {
        filters.push(filter);
        errors.push(error);
        results.push(filter_results);
    }
    Box::into_raw(Box::new(DeleteAclsResultInner { filters, errors, results })) as *mut kafka_admin_DeleteAclsResult_t
}

/// Casts a `*const kafka_admin_DeleteAclsResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `delete_acls` call.
unsafe fn delete_acls_result_ref(result: *const kafka_admin_DeleteAclsResult_t) -> &'static DeleteAclsResultInner {
    unsafe { &*(result as *const DeleteAclsResultInner) }
}

/// Returns the number of filters in the result.
///
/// # Safety
///
/// `result` must be a valid `delete_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_count(result: *const kafka_admin_DeleteAclsResult_t) -> i32 {
    unsafe { delete_acls_result_ref(result) }.filters.len() as i32
}

/// Returns the filter at `index` (borrowed), or null if out of range. Do not
/// free it. Entries are sorted by the filter's fields.
///
/// # Safety
///
/// `result` must be a valid `delete_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_get_filter(
    result: *const kafka_admin_DeleteAclsResult_t,
    index: i32,
) -> *const kafka_common_AclBindingFilter_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { delete_acls_result_ref(result) }.filters.get(index as usize) {
        Some(filter) => filter as *const AclBindingFilterInner as *const kafka_common_AclBindingFilter_t,
        None => std::ptr::null(),
    }
}

/// Returns the error for the filter at `index` (borrowed), or null if the
/// filter was applied successfully or `index` is out of range. Do not destroy
/// it.
///
/// This is the *filter's* future failing, meaning nothing was deleted for it.
/// An individual matched ACL that could not be deleted is reported by
/// [`kafka_admin_DeleteAclsResult_get_result_error`] instead, and leaves this
/// null.
///
/// # Safety
///
/// `result` must be a valid `delete_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_get_error(
    result: *const kafka_admin_DeleteAclsResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    optional_error_at(&unsafe { delete_acls_result_ref(result) }.errors, index)
}

/// Returns how many ACLs the filter at `index` matched (the size of Java's
/// `FilterResults.values()`), or 0 if the filter failed or `index` is out of
/// range.
///
/// # Safety
///
/// `result` must be a valid `delete_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_get_result_count(
    result: *const kafka_admin_DeleteAclsResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return 0;
    }
    match unsafe { delete_acls_result_ref(result) }.results.get(index as usize) {
        Some(results) => results.len() as i32,
        None => 0,
    }
}

/// Returns the ACL binding deleted by the filter at `index`, entry
/// `result_index` (borrowed), or null when that entry carries an exception
/// instead, or when either index is out of range. Do not free it.
///
/// Java's `FilterResult` holds exactly one of a binding or an exception, so
/// this and [`kafka_admin_DeleteAclsResult_get_result_error`] are
/// complementary: for an in-range entry, precisely one of them is non-null.
///
/// # Safety
///
/// `result` must be a valid `delete_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_get_binding(
    result: *const kafka_admin_DeleteAclsResult_t,
    index: i32,
    result_index: i32,
) -> *const kafka_common_AclBinding_t {
    match unsafe { delete_acls_filter_result_at(result, index, result_index) } {
        Some(entry) => match &entry.binding {
            Some(binding) => binding.as_ptr(),
            None => std::ptr::null(),
        },
        None => std::ptr::null(),
    }
}

/// Returns the exception for the filter at `index`, entry `result_index`
/// (borrowed), or null when that entry carries a deleted binding instead, or
/// when either index is out of range. Do not destroy it.
///
/// This is Java's `FilterResult.exception()`: the filter matched this ACL but
/// deleting it failed. It is independent of
/// [`kafka_admin_DeleteAclsResult_get_error`], which reports the whole filter
/// failing.
///
/// # Safety
///
/// `result` must be a valid `delete_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_get_result_error(
    result: *const kafka_admin_DeleteAclsResult_t,
    index: i32,
    result_index: i32,
) -> *const kafka_common_KafkaError_t {
    match unsafe { delete_acls_filter_result_at(result, index, result_index) } {
        Some(entry) => error_ptr(entry.error.as_ref()),
        None => std::ptr::null(),
    }
}

/// Looks up one `FilterResult` by its two indices, or `None` if either is out
/// of range.
///
/// # Safety
///
/// `result` must be a valid `delete_acls` result handle.
unsafe fn delete_acls_filter_result_at(
    result: *const kafka_admin_DeleteAclsResult_t,
    index: i32,
    result_index: i32,
) -> Option<&'static DeleteAclsFilterResultInner> {
    if index < 0 || result_index < 0 {
        return None;
    }
    unsafe { delete_acls_result_ref(result) }
        .results
        .get(index as usize)?
        .get(result_index as usize)
}

/// Destroys a `delete_acls` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `delete_acls` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_destroy(result: *mut kafka_admin_DeleteAclsResult_t) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DeleteAclsResultInner)) };
    }
}

/// Opaque handle to a flattened `DescribeClientQuotasResult`.
#[repr(C)]
pub struct kafka_admin_DescribeClientQuotasResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_DescribeClientQuotasResult_t`].
///
/// `DescribeClientQuotasResult` holds a single
/// `KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>>` for the whole
/// call, so there is no per-entity error to expose: a failure is the call's
/// error. The inner quota map is flattened into parallel key/value vectors —
/// it is a plain `Map<String, Double>`, not a Java class, so it gets indexed
/// accessors on the parent rather than a handle (the B2 `ReplicaInfo` rule).
struct DescribeClientQuotasResultInner {
    entities: Vec<ClientQuotaEntityInner>,
    quota_keys: Vec<Vec<CString>>,
    quota_values: Vec<Vec<f64>>,
}

/// Flattens the described quotas into the C handle.
fn box_describe_client_quotas_result(
    outcome: DescribeClientQuotasOutcome,
) -> *mut kafka_admin_DescribeClientQuotasResult_t {
    let mut rows: Vec<(ClientQuotaEntityInner, HashMap<String, f64>)> = outcome
        .into_iter()
        .map(|(entity, quotas)| (ClientQuotaEntityInner::new(&entity), quotas))
        .collect();
    rows.sort_by(|a, b| a.0.sort_key().cmp(&b.0.sort_key()));

    let mut entities = Vec::with_capacity(rows.len());
    let mut quota_keys = Vec::with_capacity(rows.len());
    let mut quota_values = Vec::with_capacity(rows.len());
    for (entity, quotas) in rows {
        // Sorted by quota key so index addressing is reproducible.
        let sorted = sorted_entries(quotas);
        entities.push(entity);
        quota_keys.push(sorted.iter().map(|(k, _)| to_cstring(k)).collect());
        quota_values.push(sorted.iter().map(|(_, v)| *v).collect());
    }
    Box::into_raw(Box::new(DescribeClientQuotasResultInner { entities, quota_keys, quota_values }))
        as *mut kafka_admin_DescribeClientQuotasResult_t
}

/// Casts a `*const kafka_admin_DescribeClientQuotasResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from a `describe_client_quotas` call.
unsafe fn describe_client_quotas_result_ref(
    result: *const kafka_admin_DescribeClientQuotasResult_t,
) -> &'static DescribeClientQuotasResultInner {
    unsafe { &*(result as *const DescribeClientQuotasResultInner) }
}

/// Returns the number of entities that matched the filter.
///
/// # Safety
///
/// `result` must be a valid `describe_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_count(
    result: *const kafka_admin_DescribeClientQuotasResult_t,
) -> i32 {
    unsafe { describe_client_quotas_result_ref(result) }.entities.len() as i32
}

/// Returns the entity at `index` (borrowed), or null if out of range. Do not
/// free it. Entities are sorted by their `(entity type, entity name)` pairs.
///
/// # Safety
///
/// `result` must be a valid `describe_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_get_entity(
    result: *const kafka_admin_DescribeClientQuotasResult_t,
    index: i32,
) -> *const kafka_common_ClientQuotaEntity_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_client_quotas_result_ref(result) }
        .entities
        .get(index as usize)
    {
        Some(entity) => entity.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns how many quota values the entity at `index` has, or 0 if out of
/// range. A quota type the entity has no value for is simply absent.
///
/// # Safety
///
/// `result` must be a valid `describe_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_get_quota_count(
    result: *const kafka_admin_DescribeClientQuotasResult_t,
    index: i32,
) -> i32 {
    if index < 0 {
        return 0;
    }
    match unsafe { describe_client_quotas_result_ref(result) }
        .quota_keys
        .get(index as usize)
    {
        Some(keys) => keys.len() as i32,
        None => 0,
    }
}

/// Returns the quota key at `(index, quota_index)` (borrowed) — e.g.
/// `"producer_byte_rate"`, `"consumer_byte_rate"`, `"request_percentage"`,
/// `"controller_mutation_rate"` — or null if either index is out of range.
/// Keys are sorted.
///
/// Quota keys are opaque broker-defined strings in Java too; there is no enum.
///
/// # Safety
///
/// `result` must be a valid `describe_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_get_quota_key(
    result: *const kafka_admin_DescribeClientQuotasResult_t,
    index: i32,
    quota_index: i32,
) -> *const c_char {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { describe_client_quotas_result_ref(result) }
        .quota_keys
        .get(index as usize)
    {
        Some(keys) => cstring_at(keys, quota_index),
        None => std::ptr::null(),
    }
}

/// Writes the quota value at `(index, quota_index)` to `out`, returning whether
/// both indices were in range.
///
/// Unlike the other index accessors this cannot signal "out of range" with a
/// sentinel: every `double`, including every negative one and 0, is a legal
/// quota value. So it takes an out-parameter, as
/// [`kafka_admin_ListOffsetsResultInfo_leader_epoch`] does for an optional
/// number.
///
/// # Safety
///
/// `result` must be a valid `describe_client_quotas` result handle; `out` must
/// be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_get_quota_value(
    result: *const kafka_admin_DescribeClientQuotasResult_t,
    index: i32,
    quota_index: i32,
    out: *mut f64,
) -> bool {
    if index < 0 || quota_index < 0 {
        return false;
    }
    let value = unsafe { describe_client_quotas_result_ref(result) }
        .quota_values
        .get(index as usize)
        .and_then(|values| values.get(quota_index as usize))
        .copied();
    unsafe { write_optional(value, out) }
}

/// Destroys a `describe_client_quotas` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `describe_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_destroy(
    result: *mut kafka_admin_DescribeClientQuotasResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut DescribeClientQuotasResultInner)) };
    }
}

/// Opaque handle to a flattened `AlterClientQuotasResult`.
#[repr(C)]
pub struct kafka_admin_AlterClientQuotasResult_t {
    _private: [u8; 0],
}

/// Backing state for [`kafka_admin_AlterClientQuotasResult_t`].
///
/// `AlterClientQuotasResult.values()` is `Map<ClientQuotaEntity,
/// KafkaFuture<Void>>` — the mirror image of `describeClientQuotas`, which has
/// one future for the whole map — so this handle exposes a per-entity error and
/// no value.
struct AlterClientQuotasResultInner {
    entities: Vec<ClientQuotaEntityInner>,
    errors: Vec<Option<KafkaErrorInner>>,
}

/// Flattens the per-entity `alterClientQuotas` outcomes into the C handle.
fn box_alter_client_quotas_result(outcomes: AlterClientQuotasOutcomes) -> *mut kafka_admin_AlterClientQuotasResult_t {
    let mut rows: Vec<(ClientQuotaEntityInner, Option<KafkaErrorInner>)> = outcomes
        .into_iter()
        .map(|(entity, outcome)| (ClientQuotaEntityInner::new(&entity), outcome.err().map(error_inner)))
        .collect();
    rows.sort_by(|a, b| a.0.sort_key().cmp(&b.0.sort_key()));
    let (entities, errors) = rows.into_iter().unzip();
    Box::into_raw(Box::new(AlterClientQuotasResultInner { entities, errors }))
        as *mut kafka_admin_AlterClientQuotasResult_t
}

/// Casts a `*const kafka_admin_AlterClientQuotasResult_t` to a reference.
///
/// # Safety
///
/// `result` must be a non-null handle from an `alter_client_quotas` call.
unsafe fn alter_client_quotas_result_ref(
    result: *const kafka_admin_AlterClientQuotasResult_t,
) -> &'static AlterClientQuotasResultInner {
    unsafe { &*(result as *const AlterClientQuotasResultInner) }
}

/// Returns the number of entities whose quotas were altered.
///
/// # Safety
///
/// `result` must be a valid `alter_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_count(
    result: *const kafka_admin_AlterClientQuotasResult_t,
) -> i32 {
    unsafe { alter_client_quotas_result_ref(result) }.entities.len() as i32
}

/// Returns the entity at `index` (borrowed), or null if out of range. Do not
/// free it. Entities are sorted by their `(entity type, entity name)` pairs.
///
/// # Safety
///
/// `result` must be a valid `alter_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_get_entity(
    result: *const kafka_admin_AlterClientQuotasResult_t,
    index: i32,
) -> *const kafka_common_ClientQuotaEntity_t {
    if index < 0 {
        return std::ptr::null();
    }
    match unsafe { alter_client_quotas_result_ref(result) }.entities.get(index as usize) {
        Some(entity) => entity.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns the error for the entity at `index` (borrowed), or null if its
/// quotas were altered successfully or `index` is out of range. Do not destroy
/// it.
///
/// # Safety
///
/// `result` must be a valid `alter_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_get_error(
    result: *const kafka_admin_AlterClientQuotasResult_t,
    index: i32,
) -> *const kafka_common_KafkaError_t {
    optional_error_at(&unsafe { alter_client_quotas_result_ref(result) }.errors, index)
}

/// Destroys an `alter_client_quotas` result handle. Safe with null (no-op).
///
/// # Safety
///
/// `result` must be null or a valid `alter_client_quotas` result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_destroy(
    result: *mut kafka_admin_AlterClientQuotasResult_t,
) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result as *mut AlterClientQuotasResultInner)) };
    }
}

// ---------------------------------------------------------------------------
// createAcls
// ---------------------------------------------------------------------------

/// Builds `CreateAclsOptions` from the flat C option parameters.
fn create_acls_options(timeout_ms: i32) -> CreateAclsOptions {
    CreateAclsOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Completion callback for [`kafka_admin_AdminClient_create_acls_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_CreateAclsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-binding failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_create_acls_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_CreateAclsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Creates ACL bindings, blocking until every per-binding future has resolved
/// (synchronous).
///
/// This is `createAcls(Collection<AclBinding>, CreateAclsOptions)`. The
/// bindings cross as seven parallel arrays rather than as handles, following
/// `alterPartitionReassignments` and `alterConsumerGroupOffsets`; row `i` of
/// each array describes one `AclBinding`.
///
/// On success writes a [`kafka_admin_CreateAclsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_CreateAclsResult_destroy`]) and returns null.
/// **A per-binding failure is not a call failure**: it is reported by
/// [`kafka_admin_CreateAclsResult_get_error`] for that binding. A non-null
/// return means the request could not be submitted at all, and `*out_result` is
/// left untouched.
///
/// # Parameters
///
/// - `resource_types`: `ResourceType` codes; see
///   [`kafka_common_AclBinding_resource_type`]. ANY (1) is rejected, as Java's
///   `ResourcePattern` constructor rejects it.
/// - `resource_names`: resource names; a NULL entry is rejected.
/// - `pattern_types`: `PatternType` codes; ANY (1) and MATCH (2) are rejected,
///   as Java's `ResourcePattern` constructor rejects them.
/// - `principals` / `hosts`: e.g. `"User:alice"` and `"*"`; a NULL entry is
///   rejected.
/// - `operations`: `AclOperation` codes; ANY (1) is rejected, as Java's
///   `AccessControlEntry` constructor rejects it.
/// - `permission_types`: `AclPermissionType` codes; ANY (1) is likewise
///   rejected.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// An unrecognised enum code becomes UNKNOWN, exactly as Java's `fromCode`
/// does, and UNKNOWN is accepted by the constructors — the broker rejects it.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null array must have `count`
/// entries, with string entries NULL or valid C strings; `out_result` must be
/// null or writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create_acls(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    pattern_types: *const i32,
    principals: *const *const c_char,
    hosts: *const *const c_char,
    operations: *const i32,
    permission_types: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_CreateAclsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let acls = unsafe {
        read_acl_bindings(
            resource_types,
            resource_names,
            pattern_types,
            principals,
            hosts,
            operations,
            permission_types,
            count,
        )
    };
    let options = create_acls_options(timeout_ms);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_create_acls(a, &acls?, options))) };
    unsafe { finish_sync(outcome, out_result, box_create_acls_result) }
}

/// Creates ACL bindings asynchronously. See
/// [`kafka_admin_AdminClient_create_acls`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, a NULL resource name, principal or
/// host entry, or an enum code Java's `ResourcePattern` /
/// `AccessControlEntry` constructor rejects). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null array must have `count`
/// entries, with string entries NULL or valid C strings.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create_acls_async(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    pattern_types: *const i32,
    principals: *const *const c_char,
    hosts: *const *const c_char,
    operations: *const i32,
    permission_types: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_create_acls_callback_t,
    user_data: *mut c_void,
) {
    let acls = unsafe {
        read_acl_bindings(
            resource_types,
            resource_names,
            pattern_types,
            principals,
            hosts,
            operations,
            permission_types,
            count,
        )
    };
    let options = create_acls_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_create_acls(a, &acls?, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_create_acls_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeAcls
// ---------------------------------------------------------------------------

/// Builds `DescribeAclsOptions` from the flat C option parameters.
fn describe_acls_options(timeout_ms: i32) -> DescribeAclsOptions {
    DescribeAclsOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Completion callback for [`kafka_admin_AdminClient_describe_acls_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeAclsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. `describeAcls` has a single future for
/// the whole call, so **any** failure arrives as `error`.
pub type kafka_admin_AdminClient_describe_acls_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeAclsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Describes the ACL bindings matching one filter, blocking until the result
/// arrives (synchronous).
///
/// This is `describeAcls(AclBindingFilter, DescribeAclsOptions)`. Java takes a
/// single filter, not a collection, so the seven fields cross as scalars.
///
/// On success writes a [`kafka_admin_DescribeAclsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_DescribeAclsResult_destroy`]) and returns null.
/// Unlike the keyed RPCs there is no per-key error here: `DescribeAclsResult`
/// holds one future for the whole call, so any failure is returned from this
/// function and `*out_result` is left untouched.
///
/// # Parameters
///
/// - `resource_type`: a `ResourceType` code; ANY (1) matches every type.
/// - `resource_name`: the resource name, or NULL to match any name. NULL is
///   distinct from a pointer to `""`, which filters on the empty name.
/// - `pattern_type`: a `PatternType` code; ANY (1) matches every pattern type
///   and MATCH (2) selects literal, prefixed and wildcard patterns that would
///   match the name.
/// - `principal` / `host`: or NULL to match any.
/// - `operation` / `permission_type`: codes; ANY (1) matches every value.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// Unlike [`kafka_admin_AdminClient_create_acls`], no combination is rejected:
/// Java's filter constructors accept ANY and MATCH, which is what a filter is
/// for.
///
/// # Safety
///
/// `admin` must be a valid handle; the three string parameters must be NULL or
/// valid C strings; `out_result` must be null or writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_acls(
    admin: *const kafka_admin_AdminClient_t,
    resource_type: i32,
    resource_name: *const c_char,
    pattern_type: i32,
    principal: *const c_char,
    host: *const c_char,
    operation: i32,
    permission_type: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DescribeAclsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let filter = unsafe {
        build_acl_binding_filter(
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        )
    };
    let options = describe_acls_options(timeout_ms);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_describe_acls(a, &filter, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_acls_result) }
}

/// Describes ACL bindings asynchronously. See
/// [`kafka_admin_AdminClient_describe_acls`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; the three string parameters must be NULL or
/// valid C strings.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_acls_async(
    admin: *const kafka_admin_AdminClient_t,
    resource_type: i32,
    resource_name: *const c_char,
    pattern_type: i32,
    principal: *const c_char,
    host: *const c_char,
    operation: i32,
    permission_type: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_describe_acls_callback_t,
    user_data: *mut c_void,
) {
    let filter = unsafe {
        build_acl_binding_filter(
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        )
    };
    let options = describe_acls_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_acls(a, &filter, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(bindings) => (box_describe_acls_result(bindings), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// deleteAcls
// ---------------------------------------------------------------------------

/// Builds `DeleteAclsOptions` from the flat C option parameters.
fn delete_acls_options(timeout_ms: i32) -> DeleteAclsOptions {
    DeleteAclsOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Completion callback for [`kafka_admin_AdminClient_delete_acls_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DeleteAclsResult_destroy`] or `error` with
/// `kafka_common_KafkaError_destroy`. A per-filter failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_delete_acls_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DeleteAclsResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Deletes the ACL bindings matching each filter, blocking until every
/// per-filter future has resolved (synchronous).
///
/// This is `deleteAcls(Collection<AclBindingFilter>, DeleteAclsOptions)`. The
/// filters cross as seven parallel arrays; row `i` describes one
/// `AclBindingFilter`. As with
/// [`kafka_admin_AdminClient_describe_acls`], a NULL string entry means "match
/// any" and no enum combination is rejected.
///
/// On success writes a [`kafka_admin_DeleteAclsResult_t`] to `*out_result`
/// (free it with [`kafka_admin_DeleteAclsResult_destroy`]) and returns null.
/// **Neither a per-filter nor a per-ACL failure is a call failure**: the first
/// is reported by [`kafka_admin_DeleteAclsResult_get_error`], the second by
/// [`kafka_admin_DeleteAclsResult_get_result_error`]. A non-null return means
/// the request could not be submitted at all.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null array must have `count`
/// entries, with string entries NULL or valid C strings; `out_result` must be
/// null or writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_acls(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    pattern_types: *const i32,
    principals: *const *const c_char,
    hosts: *const *const c_char,
    operations: *const i32,
    permission_types: *const i32,
    count: i32,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DeleteAclsResult_t,
) -> *mut kafka_common_KafkaError_t {
    let filters = unsafe {
        read_acl_binding_filters(
            resource_types,
            resource_names,
            pattern_types,
            principals,
            hosts,
            operations,
            permission_types,
            count,
        )
    };
    let options = delete_acls_options(timeout_ms);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_delete_acls(a, &filters, options))) };
    unsafe { finish_sync(outcome, out_result, box_delete_acls_result) }
}

/// Deletes ACL bindings asynchronously. See
/// [`kafka_admin_AdminClient_delete_acls`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle). And it runs on a **tokio worker
/// thread** if the dispatcher's completion queue can no longer be reached when
/// the result arrives. Destroying the handle does not cause that — an
/// outstanding operation holds its own sender, so it cannot disconnect the
/// queue; what remains is a dispatcher thread that terminated abnormally, i.e.
/// a panic inside an earlier callback. So callbacks are not guaranteed to be
/// serialised on one thread. Do not hold a lock across this call and re-acquire
/// it in the callback, and publish everything the callback needs (including
/// `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null array must have `count`
/// entries, with string entries NULL or valid C strings.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_delete_acls_async(
    admin: *const kafka_admin_AdminClient_t,
    resource_types: *const i32,
    resource_names: *const *const c_char,
    pattern_types: *const i32,
    principals: *const *const c_char,
    hosts: *const *const c_char,
    operations: *const i32,
    permission_types: *const i32,
    count: i32,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_delete_acls_callback_t,
    user_data: *mut c_void,
) {
    let filters = unsafe {
        read_acl_binding_filters(
            resource_types,
            resource_names,
            pattern_types,
            principals,
            hosts,
            operations,
            permission_types,
            count,
        )
    };
    let options = delete_acls_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_delete_acls(a, &filters, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_delete_acls_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// describeClientQuotas
// ---------------------------------------------------------------------------

/// Builds `DescribeClientQuotasOptions` from the flat C option parameters.
fn describe_client_quotas_options(timeout_ms: i32) -> DescribeClientQuotasOptions {
    DescribeClientQuotasOptions::new().timeout_ms(option_timeout(timeout_ms))
}

/// Completion callback for
/// [`kafka_admin_AdminClient_describe_client_quotas_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_DescribeClientQuotasResult_destroy`] or `error`
/// with `kafka_common_KafkaError_destroy`. `describeClientQuotas` has a single
/// future for the whole call, so **any** failure arrives as `error`.
pub type kafka_admin_AdminClient_describe_client_quotas_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_DescribeClientQuotasResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Describes the client quotas matching a filter, blocking until the result
/// arrives (synchronous).
///
/// This is `describeClientQuotas(ClientQuotaFilter,
/// DescribeClientQuotasOptions)`.
///
/// On success writes a [`kafka_admin_DescribeClientQuotasResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_DescribeClientQuotasResult_destroy`]) and returns null.
/// `DescribeClientQuotasResult` holds one future for the whole call, so any
/// failure is returned from this function and `*out_result` is left untouched.
///
/// # Parameters
///
/// - `entity_types` / `match_types` / `match_names` / `count`: the filter's
///   components. `match_types[i]` is the wire match type — 0 = EXACT (match
///   `match_names[i]` exactly), 1 = DEFAULT (match the built-in default entity
///   for the type), 2 = SPECIFIED (match any *named* entity of the type). These
///   are Kafka's own protocol constants. The discriminant is required because
///   DEFAULT and SPECIFIED both carry no name, so a null name alone could not
///   tell them apart. `match_names[i]` is read only for EXACT, and a NULL there
///   is rejected.
/// - `strict`: Java's `ClientQuotaFilter.containsOnly(...)` rather than
///   `contains(...)` — the entity must have *only* the given components.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// Passing `count` 0 with `strict` false is Java's `ClientQuotaFilter.all()`.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null array must have `count`
/// entries, with string entries NULL or valid C strings; `out_result` must be
/// null or writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_client_quotas(
    admin: *const kafka_admin_AdminClient_t,
    entity_types: *const *const c_char,
    match_types: *const i32,
    match_names: *const *const c_char,
    count: i32,
    strict: bool,
    timeout_ms: i32,
    out_result: *mut *mut kafka_admin_DescribeClientQuotasResult_t,
) -> *mut kafka_common_KafkaError_t {
    let filter = unsafe { read_client_quota_filter(entity_types, match_types, match_names, count, strict) };
    let options = describe_client_quotas_options(timeout_ms);
    let outcome =
        unsafe { admin_sync_value_op(admin, move |a| Ok(submit_describe_client_quotas(a, &filter?, options))) };
    unsafe { finish_sync(outcome, out_result, box_describe_client_quotas_result) }
}

/// Describes client quotas asynchronously. See
/// [`kafka_admin_AdminClient_describe_client_quotas`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, a NULL entity type, an unknown
/// match type, or an EXACT component with no match name). And it runs on a
/// **tokio worker thread** if the dispatcher's completion queue can no longer
/// be reached when the result arrives. Destroying the handle does not cause
/// that — an outstanding operation holds its own sender, so it cannot
/// disconnect the queue; what remains is a dispatcher thread that terminated
/// abnormally, i.e. a panic inside an earlier callback. So callbacks are not
/// guaranteed to be serialised on one thread. Do not hold a lock across this
/// call and re-acquire it in the callback, and publish everything the callback
/// needs (including `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null array must have `count`
/// entries, with string entries NULL or valid C strings.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_describe_client_quotas_async(
    admin: *const kafka_admin_AdminClient_t,
    entity_types: *const *const c_char,
    match_types: *const i32,
    match_names: *const *const c_char,
    count: i32,
    strict: bool,
    timeout_ms: i32,
    callback: kafka_admin_AdminClient_describe_client_quotas_callback_t,
    user_data: *mut c_void,
) {
    let filter = unsafe { read_client_quota_filter(entity_types, match_types, match_names, count, strict) };
    let options = describe_client_quotas_options(timeout_ms);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_describe_client_quotas(a, &filter?, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(entities) => (box_describe_client_quotas_result(entities), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// alterClientQuotas
// ---------------------------------------------------------------------------

/// Builds `AlterClientQuotasOptions` from the flat C option parameters.
fn alter_client_quotas_options(timeout_ms: i32, validate_only: bool) -> AlterClientQuotasOptions {
    AlterClientQuotasOptions::new()
        .timeout_ms(option_timeout(timeout_ms))
        .validate_only(validate_only)
}

/// Completion callback for
/// [`kafka_admin_AdminClient_alter_client_quotas_async`].
///
/// Exactly one of `result` / `error` is non-null and the callback owns it: free
/// `result` with [`kafka_admin_AlterClientQuotasResult_destroy`] or `error`
/// with `kafka_common_KafkaError_destroy`. A per-entity failure arrives inside
/// `result`, not as `error`.
pub type kafka_admin_AdminClient_alter_client_quotas_callback_t =
    unsafe extern "C" fn(*mut kafka_admin_AlterClientQuotasResult_t, *mut kafka_common_KafkaError_t, *mut c_void);

/// Alters client quotas, blocking until every per-entity future has resolved
/// (synchronous).
///
/// This is `alterClientQuotas(Collection<ClientQuotaAlteration>,
/// AlterClientQuotasOptions)`. Each alteration is a quota entity plus a list of
/// operations, so both levels cross as arrays of arrays with per-row counts,
/// following `listConsumerGroupOffsets`.
///
/// On success writes a [`kafka_admin_AlterClientQuotasResult_t`] to
/// `*out_result` (free it with
/// [`kafka_admin_AlterClientQuotasResult_destroy`]) and returns null. **A
/// per-entity failure is not a call failure**: it is reported by
/// [`kafka_admin_AlterClientQuotasResult_get_error`]. A non-null return means
/// the request could not be submitted at all.
///
/// # Parameters
///
/// - `entity_types[i]` / `entity_names[i]` / `entity_counts[i]`: the entity of
///   alteration `i`, as `entity_counts[i]` `(type, name)` pairs. A NULL
///   `entity_names[i][j]` is Java's null map value: the **built-in default
///   entity** for that type, which is not the same as omitting the type and not
///   the same as the name `""`.
/// - `op_keys[i]` / `op_values[i]` / `op_has_values[i]` / `op_counts[i]`: the
///   operations of alteration `i`. `op_has_values[i][j] == false` is Java's
///   `Op(key, null)`: **remove** that quota rather than set it. The flag is
///   required because every `double`, including 0, is a legal quota value, so
///   no sentinel could carry the distinction.
/// - `validate_only`: `AlterClientQuotasOptions.validateOnly` — validate
///   without applying.
/// - `timeout_ms`: per-request timeout, or negative for the client default.
///
/// A repeated entity type within one alteration, or a repeated entity across
/// alterations, is rejected: Java keys both by a `Map`, so a duplicate could
/// only be silently dropped.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null outer array must have `count`
/// entries, and each non-null inner array the matching per-row count of
/// entries; string entries must be NULL or valid C strings; `out_result` must
/// be null or writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_client_quotas(
    admin: *const kafka_admin_AdminClient_t,
    entity_types: *const *const *const c_char,
    entity_names: *const *const *const c_char,
    entity_counts: *const i32,
    op_keys: *const *const *const c_char,
    op_values: *const *const f64,
    op_has_values: *const *const bool,
    op_counts: *const i32,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    out_result: *mut *mut kafka_admin_AlterClientQuotasResult_t,
) -> *mut kafka_common_KafkaError_t {
    let entries = unsafe {
        read_client_quota_alterations(
            entity_types,
            entity_names,
            entity_counts,
            op_keys,
            op_values,
            op_has_values,
            op_counts,
            count,
        )
    };
    let options = alter_client_quotas_options(timeout_ms, validate_only);
    let outcome = unsafe { admin_sync_value_op(admin, move |a| Ok(submit_alter_client_quotas(a, &entries?, options))) };
    unsafe { finish_sync(outcome, out_result, box_alter_client_quotas_result) }
}

/// Alters client quotas asynchronously. See
/// [`kafka_admin_AdminClient_alter_client_quotas`].
///
/// The callback fires exactly once, but not always on the same thread. It
/// normally runs on the handle's dispatcher thread. It runs **synchronously on
/// the calling thread, before this function returns**, when the RPC cannot be
/// submitted at all (a NULL `admin` handle, a NULL entity type or op key, an
/// alteration with no entity types, or a repeated entity type or entity). And
/// it runs on a **tokio worker thread** if the dispatcher's completion queue
/// can no longer be reached when the result arrives. Destroying the handle does
/// not cause that — an outstanding operation holds its own sender, so it cannot
/// disconnect the queue; what remains is a dispatcher thread that terminated
/// abnormally, i.e. a panic inside an earlier callback. So callbacks are not
/// guaranteed to be serialised on one thread. Do not hold a lock across this
/// call and re-acquire it in the callback, and publish everything the callback
/// needs (including `user_data`) before calling rather than after.
///
/// # Safety
///
/// `admin` must be a valid handle; every non-null outer array must have `count`
/// entries, and each non-null inner array the matching per-row count of
/// entries; string entries must be NULL or valid C strings.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kafka_admin_AdminClient_alter_client_quotas_async(
    admin: *const kafka_admin_AdminClient_t,
    entity_types: *const *const *const c_char,
    entity_names: *const *const *const c_char,
    entity_counts: *const i32,
    op_keys: *const *const *const c_char,
    op_values: *const *const f64,
    op_has_values: *const *const bool,
    op_counts: *const i32,
    count: i32,
    timeout_ms: i32,
    validate_only: bool,
    callback: kafka_admin_AdminClient_alter_client_quotas_callback_t,
    user_data: *mut c_void,
) {
    let entries = unsafe {
        read_client_quota_alterations(
            entity_types,
            entity_names,
            entity_counts,
            op_keys,
            op_values,
            op_has_values,
            op_counts,
            count,
        )
    };
    let options = alter_client_quotas_options(timeout_ms, validate_only);
    unsafe {
        admin_async_value_op(
            admin,
            user_data,
            move |a| Ok(submit_alter_client_quotas(a, &entries?, options)),
            move |outcome, ud| {
                let (result, error) = match outcome {
                    Ok(outcomes) => (box_alter_client_quotas_result(outcomes), std::ptr::null_mut()),
                    Err(e) => (std::ptr::null_mut(), box_error(e)),
                };
                callback(result, error, ud);
            },
        )
    };
}

// ---------------------------------------------------------------------------
// MockAdminClient drivers
//
// Mock-only configuration methods (inherent on `MockAdminClient`, not part of
// the `Admin` trait), mirroring the consumer FFI's `kafka_consumer_MockConsumer_*`
// drivers. They take the same handle and return an error if it does not wrap a
// mock.
//
// A driver is exposed once a landed slice's tests need it. `add_topic` and
// `mark_topic_for_deletion` additionally need `TopicPartitionInfo` input
// marshaling and a non-panicking Rust surface (both currently `panic!` on
// duplicate/missing topics, mirroring Java's `IllegalArgumentException`, which
// must not cross the FFI boundary — CLAUDE.md §10.1), so they arrive with the
// slice whose tests need them.
// ---------------------------------------------------------------------------

/// Returns the mock client behind `admin`, or an error if it wraps the
/// production client.
///
/// # Safety
///
/// `admin` must be null or a valid handle from an admin-client constructor.
unsafe fn mock_ref(admin: *const kafka_admin_AdminClient_t) -> Result<&'static MockAdminClient, KafkaError> {
    if admin.is_null() {
        return Err(KafkaError::illegal_argument("admin handle must not be null"));
    }
    let h = unsafe { handle_ref(admin) };
    match (&h.kind, h.is_mock) {
        (AdminKind::Mock(mock), true) => Ok(mock.as_ref()),
        _ => Err(KafkaError::illegal_state(
            "this operation is only supported on a MockAdminClient",
        )),
    }
}

/// Causes the next `number_of_requests` mock operations to fail with a timeout.
///
/// Mirrors `MockAdminClient.timeoutNextRequest(int)`.
///
/// # Returns
///
/// Null on success, or a non-null error handle if `admin` does not wrap a mock
/// (free it with `kafka_common_KafkaError_destroy`).
///
/// # Safety
///
/// `admin` must be null or a valid handle from an admin-client constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_timeout_next_request(
    admin: *const kafka_admin_AdminClient_t,
    number_of_requests: i32,
) -> *mut kafka_common_KafkaError_t {
    match unsafe { mock_ref(admin) } {
        Ok(mock) => {
            mock.timeout_next_request(number_of_requests);
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Seeds the beginning offsets the mock's `listOffsets` reports for
/// `OffsetSpec.earliest()`.
///
/// Mirrors `MockAdminClient.updateBeginningOffsets(Map<TopicPartition, Long>)`,
/// which merges into (rather than replaces) the existing map. Entry `i` is
/// `(topics[i], partitions[i]) -> offsets[i]`; an entry with a NULL topic is
/// skipped.
///
/// # Returns
///
/// Null on success, or a non-null error handle if `admin` does not wrap a mock
/// (free it with `kafka_common_KafkaError_destroy`).
///
/// # Safety
///
/// `admin` must be null or a valid handle from an admin-client constructor;
/// `topics`, `partitions` and `offsets` must have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_beginning_offsets(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    match unsafe { mock_ref(admin) } {
        Ok(mock) => {
            mock.update_beginning_offsets(unsafe { read_partition_offsets(topics, partitions, offsets, count) });
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Seeds the end offsets the mock's `listOffsets` reports for every
/// `OffsetSpec` other than `earliest()` and `forTimestamp(...)`.
///
/// Mirrors `MockAdminClient.updateEndOffsets(Map<TopicPartition, Long>)`, which
/// merges into (rather than replaces) the existing map. Entry `i` is
/// `(topics[i], partitions[i]) -> offsets[i]`; an entry with a NULL topic is
/// skipped.
///
/// # Returns
///
/// Null on success, or a non-null error handle if `admin` does not wrap a mock
/// (free it with `kafka_common_KafkaError_destroy`).
///
/// # Safety
///
/// `admin` must be null or a valid handle from an admin-client constructor;
/// `topics`, `partitions` and `offsets` must have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_end_offsets(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    match unsafe { mock_ref(admin) } {
        Ok(mock) => {
            mock.update_end_offsets(unsafe { read_partition_offsets(topics, partitions, offsets, count) });
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Seeds the committed offsets the mock's `listConsumerGroupOffsets` reports.
///
/// Mirrors `MockAdminClient.updateConsumerGroupOffsets(Map<TopicPartition, Long>)`
/// (`MockAdminClient.java:1493-1495`), which **merges** into rather than
/// replaces the existing map. Entry `i` is
/// `(topics[i], partitions[i]) -> offsets[i]`; an entry with a NULL topic is
/// skipped.
///
/// `MockAdminClient` keys its committed offsets by partition only and ignores
/// the group id — its `listConsumerGroupOffsets` answers every request from one
/// shared map, and throws `UnsupportedOperationException` for more than one
/// requested group (`MockAdminClient.java:748-760`) — so there is no group
/// parameter here. The production client has no such restriction.
///
/// # Returns
///
/// Null on success, or a non-null error handle if `admin` does not wrap a mock
/// (free it with `kafka_common_KafkaError_destroy`).
///
/// # Safety
///
/// `admin` must be null or a valid handle from an admin-client constructor;
/// `topics`, `partitions` and `offsets` must have `count` valid entries each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_consumer_group_offsets(
    admin: *const kafka_admin_AdminClient_t,
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    count: i32,
) -> *mut kafka_common_KafkaError_t {
    match unsafe { mock_ref(admin) } {
        Ok(mock) => {
            mock.update_consumer_group_offsets(unsafe { read_partition_offsets(topics, partitions, offsets, count) });
            std::ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// Reads `count` `(topic, partition) -> offset` triples, skipping entries whose
/// topic is NULL.
///
/// # Safety
///
/// `topics`, `partitions` and `offsets` must be null or have `count` readable
/// entries each, every topic NULL or a valid C string.
unsafe fn read_partition_offsets(
    topics: *const *const c_char,
    partitions: *const i32,
    offsets: *const i64,
    count: i32,
) -> HashMap<TopicPartition, i64> {
    let mut out = HashMap::new();
    if topics.is_null() || partitions.is_null() || offsets.is_null() {
        return out;
    }
    for i in 0..count.max(0) as usize {
        let name_ptr = unsafe { *topics.add(i) };
        if name_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy().to_string();
        out.insert(TopicPartition::new(name, unsafe { *partitions.add(i) }), unsafe {
            *offsets.add(i)
        });
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
//
// These exercise the pure marshaling helpers directly rather than end-to-end
// through `MockAdminClient`, because the mock cannot reach most of them:
//
//   - It never reads its `options` argument (`MockAdminClient.java:340-360` for
//     `describeCluster`, `:897-916` for `incrementalAlterConfigs`), so an
//     end-to-end test cannot tell two boolean option flags apart. Every option
//     builder below is therefore called with *asymmetric* flag values, so that
//     transposing any two of them fails an assertion here.
//   - It builds config entries with the two-argument `ConfigEntry(name, value)`
//     constructor (`MockAdminClient.java:889-895`), so `source` and `type` are
//     always `UNKNOWN`, `documentation` is always null and `synonyms` is always
//     empty. 17 of the 19 enum constant names and the whole synonym flattening
//     path are unreachable from it.
//   - It reports no volume sizes and no `LogDirDescription.error`.
//
// The helpers are pure functions of their inputs, so hand-built fixtures cover
// what the mock cannot.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{ConfigSynonym, FilterResult, ReplicaInfo};
    use crate::common::{ClassicGroupState, Errors, KafkaGenericError};

    fn text(value: &CString) -> &str {
        value.to_str().expect("CString holds UTF-8")
    }

    fn opt_text(value: &Option<CString>) -> Option<&str> {
        value.as_ref().map(text)
    }

    // -- option_timeout -----------------------------------------------------

    #[test]
    fn option_timeout_maps_negative_to_unset() {
        // Negative means "unset" so the client's default.api.timeout.ms applies
        // (Java leaves `timeoutMs` null); zero is a real timeout, not unset.
        assert_eq!(option_timeout(-1), None);
        assert_eq!(option_timeout(i32::MIN), None);
        assert_eq!(option_timeout(0), Some(0));
        assert_eq!(option_timeout(30_000), Some(30_000));
    }

    // -- Option builders ----------------------------------------------------
    //
    // Each flag is set to a value distinct from its neighbours so that swapping
    // two parameters, or wiring one to the wrong `*Options` setter, is caught.

    #[test]
    fn describe_cluster_options_maps_each_flag_to_its_own_field() {
        let options = describe_cluster_options(1_000, true, false);
        assert_eq!(options.timeout(), Some(1_000));
        assert!(options.should_include_authorized_operations());
        assert!(!options.should_include_fenced_brokers());

        // Reversed, so a transposition cannot satisfy both cases.
        let options = describe_cluster_options(-1, false, true);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_include_authorized_operations());
        assert!(options.should_include_fenced_brokers());
    }

    #[test]
    fn describe_configs_options_maps_each_flag_to_its_own_field() {
        let options = describe_configs_options(2_000, true, false);
        assert_eq!(options.timeout(), Some(2_000));
        assert!(options.should_include_synonyms());
        assert!(!options.should_include_documentation());

        let options = describe_configs_options(-1, false, true);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_include_synonyms());
        assert!(options.should_include_documentation());
    }

    #[test]
    fn describe_topics_options_maps_each_flag_to_its_own_field() {
        let options = describe_topics_options(3_000, true, 25);
        assert_eq!(options.timeout(), Some(3_000));
        assert!(options.should_include_authorized_operations());
        assert_eq!(options.partition_size_limit(), 25);

        // A negative partition-size limit leaves Java's default in place rather
        // than forwarding the sentinel to the setter.
        let default_limit = DescribeTopicsOptions::new().partition_size_limit();
        let options = describe_topics_options(-1, false, -1);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_include_authorized_operations());
        assert_eq!(options.partition_size_limit(), default_limit);
    }

    #[test]
    fn create_topics_options_maps_each_flag_to_its_own_field() {
        let options = create_topics_options(4_000, true, false);
        assert_eq!(options.timeout(), Some(4_000));
        assert!(options.should_validate_only());
        assert!(!options.should_retry_on_quota_violation());

        let options = create_topics_options(-1, false, true);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_validate_only());
        assert!(options.should_retry_on_quota_violation());
    }

    #[test]
    fn create_partitions_options_maps_each_flag_to_its_own_field() {
        let options = create_partitions_options(5_000, true, false);
        assert_eq!(options.timeout(), Some(5_000));
        assert!(options.should_validate_only());
        assert!(!options.should_retry_on_quota_violation());

        let options = create_partitions_options(-1, false, true);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_validate_only());
        assert!(options.should_retry_on_quota_violation());
    }

    #[test]
    fn delete_topics_options_maps_each_flag_to_its_own_field() {
        let options = delete_topics_options(6_000, true);
        assert_eq!(options.timeout(), Some(6_000));
        assert!(options.should_retry_on_quota_violation());

        let options = delete_topics_options(-1, false);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_retry_on_quota_violation());
    }

    // -- Enum constant names ------------------------------------------------

    #[test]
    fn config_source_name_matches_java_enum_constant_names() {
        // Java's `ConfigEntry.ConfigSource` has no `id()`, so the constant name
        // is the C contract. Asserted exhaustively because the mock only ever
        // produces UNKNOWN.
        let expected = [
            (ConfigSource::DynamicTopicConfig, "DYNAMIC_TOPIC_CONFIG"),
            (ConfigSource::DynamicBrokerLoggerConfig, "DYNAMIC_BROKER_LOGGER_CONFIG"),
            (ConfigSource::DynamicBrokerConfig, "DYNAMIC_BROKER_CONFIG"),
            (ConfigSource::DynamicDefaultBrokerConfig, "DYNAMIC_DEFAULT_BROKER_CONFIG"),
            (ConfigSource::DynamicClientMetricsConfig, "DYNAMIC_CLIENT_METRICS_CONFIG"),
            (ConfigSource::DynamicGroupConfig, "DYNAMIC_GROUP_CONFIG"),
            (ConfigSource::StaticBrokerConfig, "STATIC_BROKER_CONFIG"),
            (ConfigSource::DefaultConfig, "DEFAULT_CONFIG"),
            (ConfigSource::Unknown, "UNKNOWN"),
        ];
        for (source, name) in expected {
            assert_eq!(config_source_name(source), name, "wrong name for {source:?}");
        }
    }

    #[test]
    fn config_type_name_matches_java_enum_constant_names() {
        let expected = [
            (ConfigType::Unknown, "UNKNOWN"),
            (ConfigType::Boolean, "BOOLEAN"),
            (ConfigType::String, "STRING"),
            (ConfigType::Int, "INT"),
            (ConfigType::Short, "SHORT"),
            (ConfigType::Long, "LONG"),
            (ConfigType::Double, "DOUBLE"),
            (ConfigType::List, "LIST"),
            (ConfigType::Class, "CLASS"),
            (ConfigType::Password, "PASSWORD"),
        ];
        for (config_type, name) in expected {
            assert_eq!(config_type_name(config_type), name, "wrong name for {config_type:?}");
        }
    }

    // -- ConfigEntryC -------------------------------------------------------

    #[test]
    fn config_entry_c_carries_every_field_including_synonyms() {
        let entry = ConfigEntry::with_metadata(
            "retention.ms".to_string(),
            Some("604800000".to_string()),
            ConfigSource::DynamicTopicConfig,
            true,
            false,
            vec![
                // Ordered by precedence in Java; the flattener must not sort.
                ConfigSynonym::new(
                    "retention.ms".to_string(),
                    Some("604800000".to_string()),
                    ConfigSource::DynamicTopicConfig,
                ),
                ConfigSynonym::new("log.retention.ms".to_string(), None, ConfigSource::StaticBrokerConfig),
            ],
            ConfigType::Long,
            Some("The retention window.".to_string()),
        );

        let flat = ConfigEntryC::new(&entry);

        assert_eq!(text(&flat.name_c), "retention.ms");
        assert_eq!(opt_text(&flat.value_c), Some("604800000"));
        // `isDefault()` is derived from the source, not stored separately.
        assert!(!flat.is_default);
        assert!(flat.is_sensitive);
        assert!(!flat.is_read_only);
        assert_eq!(text(&flat.source_c), "DYNAMIC_TOPIC_CONFIG");
        assert_eq!(text(&flat.config_type_c), "LONG");
        assert_eq!(opt_text(&flat.documentation_c), Some("The retention window."));

        assert_eq!(flat.synonyms.len(), 2);
        assert_eq!(text(&flat.synonyms[0].name_c), "retention.ms");
        assert_eq!(opt_text(&flat.synonyms[0].value_c), Some("604800000"));
        assert_eq!(text(&flat.synonyms[0].source_c), "DYNAMIC_TOPIC_CONFIG");
        assert_eq!(text(&flat.synonyms[1].name_c), "log.retention.ms");
        assert_eq!(opt_text(&flat.synonyms[1].value_c), None);
        assert_eq!(text(&flat.synonyms[1].source_c), "STATIC_BROKER_CONFIG");
    }

    #[test]
    fn config_entry_c_preserves_null_value_and_documentation() {
        // Java's `ConfigEntry.value()` and `.documentation()` are both nullable;
        // C must see null, not an empty string.
        let flat = ConfigEntryC::new(&ConfigEntry::new("sensitive.config".to_string(), None));
        assert_eq!(opt_text(&flat.value_c), None);
        assert_eq!(opt_text(&flat.documentation_c), None);
        assert!(flat.synonyms.is_empty());
        assert_eq!(text(&flat.source_c), "UNKNOWN");
        assert_eq!(text(&flat.config_type_c), "UNKNOWN");
    }

    #[test]
    fn config_entry_c_is_default_tracks_the_default_config_source() {
        let flat = ConfigEntryC::new(&ConfigEntry::with_metadata(
            "k".to_string(),
            Some("v".to_string()),
            ConfigSource::DefaultConfig,
            false,
            true,
            Vec::new(),
            ConfigType::String,
            None,
        ));
        assert!(flat.is_default);
        assert!(flat.is_read_only);
        assert_eq!(text(&flat.source_c), "DEFAULT_CONFIG");
    }

    #[test]
    fn config_entry_c_from_config_sorts_by_name_for_stable_indexing() {
        // `Config.entries()` iterates a HashMap; C addresses entries by index.
        let config = Config::new(vec![
            ConfigEntry::new("zzz".to_string(), Some("3".to_string())),
            ConfigEntry::new("aaa".to_string(), Some("1".to_string())),
            ConfigEntry::new("mmm".to_string(), Some("2".to_string())),
        ]);
        let entries = ConfigEntryC::from_config(&config);
        let names: Vec<&str> = entries.iter().map(|e| text(&e.name_c)).collect();
        assert_eq!(names, ["aaa", "mmm", "zzz"]);
    }

    // -- LogDirDescriptionInner ---------------------------------------------

    #[test]
    fn log_dir_description_carries_error_and_volume_bytes() {
        let mut replicas = HashMap::new();
        replicas.insert(TopicPartition::new("t".to_string(), 0), ReplicaInfo::new(100, 5, false));
        let description = LogDirDescription::with_volume_bytes(
            Some(KafkaError::Generic(KafkaGenericError::new(Errors::KafkaStorageError))),
            replicas,
            2_000,
            1_000,
        );

        let flat = LogDirDescriptionInner::new(&description);

        let error = flat.error.as_ref().expect("log dir reported an error");
        assert_eq!(error.error.code(), Errors::KafkaStorageError.code());
        assert_eq!(flat.total_bytes, 2_000);
        assert_eq!(flat.usable_bytes, 1_000);
        assert_eq!(flat.replicas.len(), 1);
        assert_eq!(flat.replicas[0].size, 100);
        assert_eq!(flat.replicas[0].offset_lag, 5);
        assert!(!flat.replicas[0].is_future);
    }

    #[test]
    fn log_dir_description_reports_absent_volume_bytes_as_unknown() {
        // Java's empty `OptionalLong` becomes
        // `DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES` (-1) over the boundary.
        let flat = LogDirDescriptionInner::new(&LogDirDescription::new(None, HashMap::new()));
        assert!(flat.error.is_none());
        assert_eq!(flat.total_bytes, UNKNOWN_VOLUME_BYTES);
        assert_eq!(flat.usable_bytes, UNKNOWN_VOLUME_BYTES);
        assert!(flat.replicas.is_empty());
    }

    #[test]
    fn log_dir_description_sorts_replicas_by_topic_then_partition() {
        let mut replicas = HashMap::new();
        for (topic, partition) in [("b", 0), ("a", 10), ("a", 2)] {
            replicas.insert(TopicPartition::new(topic.to_string(), partition), ReplicaInfo::new(0, 0, false));
        }
        let flat = LogDirDescriptionInner::new(&LogDirDescription::new(None, replicas));
        let order: Vec<(&str, i32)> = flat.replicas.iter().map(|r| (text(&r.topic_c), r.partition)).collect();
        // Partition ordering is numeric, not lexicographic: 2 before 10.
        assert_eq!(order, [("a", 2), ("a", 10), ("b", 0)]);
    }

    #[test]
    fn log_dir_description_map_sorts_by_path() {
        let mut map = HashMap::new();
        for path in ["/data/2", "/data/1"] {
            map.insert(path.to_string(), LogDirDescription::new(None, HashMap::new()));
        }
        let flat = LogDirDescriptionMapInner::new(&map);
        let paths: Vec<&str> = flat.log_dirs.iter().map(text).collect();
        assert_eq!(paths, ["/data/1", "/data/2"]);
        assert_eq!(flat.descriptions.len(), 2);
    }

    // -- ReplicaLogDirInfoInner ---------------------------------------------

    #[test]
    fn replica_log_dir_info_carries_both_log_dirs() {
        let flat = ReplicaLogDirInfoInner::new(&ReplicaLogDirInfo::new(
            Some("/data/current".to_string()),
            7,
            Some("/data/future".to_string()),
            3,
        ));
        assert_eq!(opt_text(&flat.current_log_dir_c), Some("/data/current"));
        assert_eq!(flat.current_offset_lag, 7);
        assert_eq!(opt_text(&flat.future_log_dir_c), Some("/data/future"));
        assert_eq!(flat.future_offset_lag, 3);
    }

    #[test]
    fn replica_log_dir_info_preserves_null_log_dirs() {
        // This is what a real broker returns for a replica whose topic it does
        // not know (`KafkaAdminClient.java:3066-3068` seeds a default
        // `ReplicaLogDirInfo`), and for a replica that is not being moved.
        let flat = ReplicaLogDirInfoInner::new(&ReplicaLogDirInfo::new(None, -1, None, -1));
        assert_eq!(opt_text(&flat.current_log_dir_c), None);
        assert_eq!(opt_text(&flat.future_log_dir_c), None);
        assert_eq!(flat.current_offset_lag, -1);
        assert_eq!(flat.future_offset_lag, -1);
    }

    // -- B3: elections / reassignments / offsets ----------------------------
    //
    // The mock ignores every `*Options` argument, so as above each option
    // builder is called twice with *asymmetric* values; a transposition cannot
    // satisfy both cases. The input readers get their own tests because they
    // carry the three `Optional` discriminants (all-partitions, cancel,
    // is-timestamp) that no end-to-end test through the mock can distinguish.

    #[test]
    fn elect_leaders_options_maps_the_timeout() {
        assert_eq!(elect_leaders_options(1_500).timeout(), Some(1_500));
        assert_eq!(elect_leaders_options(-1).timeout(), None);
    }

    #[test]
    fn alter_partition_reassignments_options_maps_each_flag_to_its_own_field() {
        let options = alter_partition_reassignments_options(2_500, false);
        assert_eq!(options.timeout(), Some(2_500));
        assert!(!options.should_allow_replication_factor_change());

        // Reversed, so a transposition cannot satisfy both cases.
        let options = alter_partition_reassignments_options(-1, true);
        assert_eq!(options.timeout(), None);
        assert!(options.should_allow_replication_factor_change());
    }

    #[test]
    fn list_partition_reassignments_options_maps_the_timeout() {
        assert_eq!(list_partition_reassignments_options(3_500).timeout(), Some(3_500));
        assert_eq!(list_partition_reassignments_options(-7).timeout(), None);
    }

    #[test]
    fn list_offsets_options_maps_each_field_to_its_own_slot() {
        let options = list_offsets_options(4_500, 1).unwrap();
        assert_eq!(options.timeout(), Some(4_500));
        assert_eq!(options.isolation_level(), IsolationLevel::ReadCommitted);

        // Reversed, so wiring the timeout into the isolation level (or vice
        // versa) cannot satisfy both cases.
        let options = list_offsets_options(-1, 0).unwrap();
        assert_eq!(options.timeout(), None);
        assert_eq!(options.isolation_level(), IsolationLevel::ReadUncommitted);
    }

    #[test]
    fn list_offsets_options_rejects_an_unknown_isolation_level() {
        // Mirrors Java's `IsolationLevel.forId` IllegalArgumentException.
        assert_eq!(list_offsets_options(0, 2).unwrap_err().message(), "Unknown isolation level 2");
        // Out of u8 range, so `for_id` is never reached; same wording.
        assert_eq!(list_offsets_options(0, -1).unwrap_err().message(), "Unknown isolation level -1");
        assert_eq!(
            list_offsets_options(0, 300).unwrap_err().message(),
            "Unknown isolation level 300"
        );
    }

    #[test]
    fn read_election_type_maps_javas_byte_values() {
        assert_eq!(read_election_type(0).unwrap(), ElectionType::Preferred);
        assert_eq!(read_election_type(1).unwrap(), ElectionType::Unclean);
        // Mirrors Java's `ElectionType.valueOf(byte)` IllegalArgumentException,
        // both inside and outside the i8 range.
        assert_eq!(
            read_election_type(2).unwrap_err().message(),
            "Value 2 must be one of [PREFERRED, UNCLEAN]"
        );
        assert_eq!(
            read_election_type(1_000).unwrap_err().message(),
            "Value 1000 must be one of [PREFERRED, UNCLEAN]"
        );
    }

    #[test]
    fn offset_spec_for_sentinel_inverts_get_offset_from_spec() {
        // Every value here is the `ListOffsets` sentinel Java's
        // `KafkaAdminClient.getOffsetFromSpec` emits for that factory, so a
        // transposed pair would make one of these fail.
        assert_eq!(offset_spec_for_sentinel(-1), Some(OffsetSpec::Latest));
        assert_eq!(offset_spec_for_sentinel(-2), Some(OffsetSpec::Earliest));
        assert_eq!(offset_spec_for_sentinel(-3), Some(OffsetSpec::MaxTimestamp));
        assert_eq!(offset_spec_for_sentinel(-4), Some(OffsetSpec::EarliestLocal));
        assert_eq!(offset_spec_for_sentinel(-5), Some(OffsetSpec::LatestTiered));
        assert_eq!(offset_spec_for_sentinel(-6), Some(OffsetSpec::EarliestPendingUpload));
        assert_eq!(offset_spec_for_sentinel(-7), None);
        assert_eq!(offset_spec_for_sentinel(0), None);
        assert_eq!(offset_spec_for_sentinel(1_700_000_000_000), None);
    }

    /// Builds the `*const *const c_char` array a C caller would pass, keeping
    /// the `CString`s alive for the duration of the call.
    fn topic_array(names: &[&str]) -> (Vec<CString>, Vec<*const c_char>) {
        let owned: Vec<CString> = names.iter().map(|n| CString::new(*n).unwrap()).collect();
        let ptrs: Vec<*const c_char> = owned.iter().map(|c| c.as_ptr()).collect();
        (owned, ptrs)
    }

    #[test]
    fn read_optional_partition_set_distinguishes_all_from_empty() {
        let (_owned, ptrs) = topic_array(&["t"]);
        let partitions = [3i32];

        // all_partitions = true is Java's absent Set / Optional.empty(): the
        // arrays are not read at all.
        let all = unsafe { read_optional_partition_set(true, ptrs.as_ptr(), partitions.as_ptr(), 1) };
        assert_eq!(all, None);

        let selected = unsafe { read_optional_partition_set(false, ptrs.as_ptr(), partitions.as_ptr(), 1) };
        assert_eq!(selected, Some(HashSet::from([TopicPartition::new("t".to_string(), 3)])));

        // An empty selection is Some(empty), not None — the distinction the
        // flag exists for.
        let empty = unsafe { read_optional_partition_set(false, ptrs.as_ptr(), partitions.as_ptr(), 0) };
        assert_eq!(empty, Some(HashSet::new()));
    }

    #[test]
    fn read_reassignments_maps_cancel_to_an_empty_optional() {
        let (_owned, ptrs) = topic_array(&["t", "t"]);
        let partitions = [0i32, 1];
        let cancel = [false, true];
        let replicas = [2i32, 3];
        // The cancelled entry deliberately supplies a non-empty replica list;
        // the flag must win, and the list must not be read.
        let replica_ptrs = [replicas.as_ptr(), replicas.as_ptr()];
        let replica_counts = [2i32, 2];

        let out = unsafe {
            read_reassignments(
                ptrs.as_ptr(),
                partitions.as_ptr(),
                cancel.as_ptr(),
                replica_ptrs.as_ptr(),
                replica_counts.as_ptr(),
                2,
            )
        }
        .unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(
            out[&TopicPartition::new("t".to_string(), 0)]
                .as_ref()
                .unwrap()
                .target_replicas(),
            &[2, 3]
        );
        assert!(out[&TopicPartition::new("t".to_string(), 1)].is_none());
    }

    #[test]
    fn read_reassignments_rejects_an_empty_non_cancelled_replica_list() {
        let (_owned, ptrs) = topic_array(&["t"]);
        let partitions = [0i32];
        let cancel = [false];
        let replicas = [0i32];
        let replica_ptrs = [replicas.as_ptr()];
        let replica_counts = [0i32];

        // Java's `NewPartitionReassignment(List<Integer>)` throws here, so an
        // empty list must stay an error rather than becoming a cancellation.
        let error = unsafe {
            read_reassignments(
                ptrs.as_ptr(),
                partitions.as_ptr(),
                cancel.as_ptr(),
                replica_ptrs.as_ptr(),
                replica_counts.as_ptr(),
                1,
            )
        }
        .unwrap_err();
        assert_eq!(
            error.message(),
            "reassignment for t-0 at index 0: Cannot create a new partition reassignment without any replicas"
        );
    }

    #[test]
    fn read_offset_specs_distinguishes_a_timestamp_from_a_sentinel() {
        let (_owned, ptrs) = topic_array(&["t", "t", "t"]);
        let partitions = [0i32, 1, 2];
        let is_timestamp = [false, true, true];
        // Entry 0 and entry 1 carry the same value: without the flag they would
        // be indistinguishable, which is the whole reason it exists.
        let values = [EARLIEST_TIMESTAMP, EARLIEST_TIMESTAMP, 1_700_000_000_000];

        let out =
            unsafe { read_offset_specs(ptrs.as_ptr(), partitions.as_ptr(), is_timestamp.as_ptr(), values.as_ptr(), 3) }
                .unwrap();

        assert_eq!(out[&TopicPartition::new("t".to_string(), 0)], OffsetSpec::Earliest);
        assert_eq!(
            out[&TopicPartition::new("t".to_string(), 1)],
            OffsetSpec::Timestamp(EARLIEST_TIMESTAMP)
        );
        assert_eq!(
            out[&TopicPartition::new("t".to_string(), 2)],
            OffsetSpec::Timestamp(1_700_000_000_000)
        );
    }

    #[test]
    fn read_offset_specs_rejects_a_non_sentinel_without_the_flag() {
        let (_owned, ptrs) = topic_array(&["t"]);
        let partitions = [4i32];
        let is_timestamp = [false];
        let values = [42i64];

        let error =
            unsafe { read_offset_specs(ptrs.as_ptr(), partitions.as_ptr(), is_timestamp.as_ptr(), values.as_ptr(), 1) }
                .unwrap_err();
        assert_eq!(
            error.message(),
            "offset spec for t-4 at index 0: 42 is not a ListOffsets timestamp sentinel; \
             pass is_timestamp=true to request OffsetSpec.forTimestamp(42)"
        );
    }

    // -- Result flatteners --------------------------------------------------
    //
    // Driven through the public getters, i.e. exactly as a C caller sees them,
    // so index addressing and the borrowed-pointer contract are covered too.

    #[test]
    fn elect_leaders_result_reports_per_partition_errors_in_sorted_order() {
        let outcomes = HashMap::from([
            (TopicPartition::new("b".to_string(), 0), None),
            (
                TopicPartition::new("a".to_string(), 10),
                Some(KafkaError::new(Errors::LeaderNotAvailable)),
            ),
            (TopicPartition::new("a".to_string(), 2), None),
        ]);
        let result = box_elect_leaders_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_ElectLeadersResult_count(result), 3);
            // Sorted by topic then *numeric* partition: 2 before 10.
            let keys: Vec<(String, i32)> = (0..3)
                .map(|i| {
                    (
                        CStr::from_ptr(kafka_admin_ElectLeadersResult_get_topic(result, i))
                            .to_string_lossy()
                            .to_string(),
                        kafka_admin_ElectLeadersResult_get_partition(result, i),
                    )
                })
                .collect();
            assert_eq!(keys, [("a".to_string(), 2), ("a".to_string(), 10), ("b".to_string(), 0)]);

            assert!(kafka_admin_ElectLeadersResult_get_error(result, 0).is_null());
            let failed = kafka_admin_ElectLeadersResult_get_error(result, 1);
            assert!(!failed.is_null());
            assert_eq!(
                common::kafka_common_KafkaError_code(failed),
                Errors::LeaderNotAvailable.code() as i32
            );
            assert!(kafka_admin_ElectLeadersResult_get_error(result, 2).is_null());

            // Out-of-range indices are null / -1, never a crash.
            assert!(kafka_admin_ElectLeadersResult_get_topic(result, 3).is_null());
            assert!(kafka_admin_ElectLeadersResult_get_topic(result, -1).is_null());
            assert_eq!(kafka_admin_ElectLeadersResult_get_partition(result, -1), -1);
            assert!(kafka_admin_ElectLeadersResult_get_error(result, 3).is_null());
            kafka_admin_ElectLeadersResult_destroy(result);
        }
    }

    #[test]
    fn alter_partition_reassignments_result_reports_per_partition_errors() {
        let outcomes = HashMap::from([
            (TopicPartition::new("t".to_string(), 0), Ok(())),
            (
                TopicPartition::new("t".to_string(), 1),
                Err(KafkaError::new(Errors::UnknownTopicOrPartition)),
            ),
        ]);
        let result = box_alter_partition_reassignments_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_AlterPartitionReassignmentsResult_count(result), 2);
            assert_eq!(kafka_admin_AlterPartitionReassignmentsResult_get_partition(result, 0), 0);
            assert!(kafka_admin_AlterPartitionReassignmentsResult_get_error(result, 0).is_null());
            let failed = kafka_admin_AlterPartitionReassignmentsResult_get_error(result, 1);
            assert!(!failed.is_null());
            assert_eq!(
                common::kafka_common_KafkaError_code(failed),
                Errors::UnknownTopicOrPartition.code() as i32
            );
            kafka_admin_AlterPartitionReassignmentsResult_destroy(result);
        }
    }

    #[test]
    fn list_partition_reassignments_result_carries_all_three_replica_lists() {
        let reassignments = HashMap::from([(
            TopicPartition::new("t".to_string(), 0),
            PartitionReassignment::new(vec![0, 1, 2], vec![3], vec![0, 2]),
        )]);
        let result = box_list_partition_reassignments_result(reassignments);
        unsafe {
            assert_eq!(kafka_admin_ListPartitionReassignmentsResult_count(result), 1);
            let value = kafka_admin_ListPartitionReassignmentsResult_get_value(result, 0);
            assert!(!value.is_null());

            // Asserting all three lists with distinct contents catches a
            // transposition between them.
            assert_eq!(kafka_admin_PartitionReassignment_replica_count(value), 3);
            let replicas: Vec<i32> = (0..3).map(|i| kafka_admin_PartitionReassignment_replica(value, i)).collect();
            assert_eq!(replicas, [0, 1, 2]);

            assert_eq!(kafka_admin_PartitionReassignment_adding_replica_count(value), 1);
            assert_eq!(kafka_admin_PartitionReassignment_adding_replica(value, 0), 3);

            assert_eq!(kafka_admin_PartitionReassignment_removing_replica_count(value), 2);
            let removing: Vec<i32> = (0..2)
                .map(|i| kafka_admin_PartitionReassignment_removing_replica(value, i))
                .collect();
            assert_eq!(removing, [0, 2]);

            // Out-of-range broker indices are -1, never a crash.
            assert_eq!(kafka_admin_PartitionReassignment_replica(value, 3), -1);
            assert_eq!(kafka_admin_PartitionReassignment_adding_replica(value, -1), -1);
            assert_eq!(kafka_admin_PartitionReassignment_removing_replica(value, 9), -1);
            assert!(kafka_admin_ListPartitionReassignmentsResult_get_value(result, 1).is_null());
            kafka_admin_ListPartitionReassignmentsResult_destroy(result);
        }
    }

    #[test]
    fn list_offsets_result_carries_value_and_error_per_partition() {
        let outcomes = HashMap::from([
            (
                TopicPartition::new("t".to_string(), 0),
                Ok(ListOffsetsResultInfo::new(42, 1_700_000_000_000, Some(7))),
            ),
            (
                TopicPartition::new("t".to_string(), 1),
                Ok(ListOffsetsResultInfo::new(5, -1, None)),
            ),
            (
                TopicPartition::new("t".to_string(), 2),
                Err(KafkaError::new(Errors::UnknownTopicOrPartition)),
            ),
        ]);
        let result = box_list_offsets_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_ListOffsetsResult_count(result), 3);

            // Distinct offset and timestamp catch a transposition between them.
            let info = kafka_admin_ListOffsetsResult_get_value(result, 0);
            assert!(!info.is_null());
            assert_eq!(kafka_admin_ListOffsetsResultInfo_offset(info), 42);
            assert_eq!(kafka_admin_ListOffsetsResultInfo_timestamp(info), 1_700_000_000_000);
            let mut epoch = -99i32;
            assert!(kafka_admin_ListOffsetsResultInfo_leader_epoch(info, &mut epoch));
            assert_eq!(epoch, 7);
            assert!(kafka_admin_ListOffsetsResult_get_error(result, 0).is_null());

            // Java's `Optional.empty()` leader epoch: false, out-param untouched.
            let info = kafka_admin_ListOffsetsResult_get_value(result, 1);
            assert_eq!(kafka_admin_ListOffsetsResultInfo_offset(info), 5);
            assert_eq!(kafka_admin_ListOffsetsResultInfo_timestamp(info), -1);
            let mut epoch = -99i32;
            assert!(!kafka_admin_ListOffsetsResultInfo_leader_epoch(info, &mut epoch));
            assert_eq!(epoch, -99);
            // A null out-param is tolerated.
            assert!(!kafka_admin_ListOffsetsResultInfo_leader_epoch(info, std::ptr::null_mut()));

            // The failed partition has an error and *no* value.
            assert!(kafka_admin_ListOffsetsResult_get_value(result, 2).is_null());
            let failed = kafka_admin_ListOffsetsResult_get_error(result, 2);
            assert!(!failed.is_null());
            assert_eq!(
                common::kafka_common_KafkaError_code(failed),
                Errors::UnknownTopicOrPartition.code() as i32
            );

            assert!(kafka_admin_ListOffsetsResult_get_value(result, 3).is_null());
            assert!(kafka_admin_ListOffsetsResult_get_error(result, -1).is_null());
            kafka_admin_ListOffsetsResult_destroy(result);
        }
    }

    // -- B4: group option builders, input marshaling and flatteners ---------

    /// Builds the `*const *const c_char` array a C caller would pass.
    fn c_strings(values: &[&str]) -> Vec<CString> {
        values.iter().map(|v| CString::new(*v).unwrap()).collect()
    }

    fn c_ptrs(values: &[CString]) -> Vec<*const c_char> {
        values.iter().map(|v| v.as_ptr()).collect()
    }

    #[test]
    fn list_groups_options_routes_each_array_to_its_own_filter() {
        // Deliberately different lengths and values, so swapping any two of the
        // three arrays at the call site changes at least one assertion.
        let states = c_strings(&["Stable"]);
        let protocols = c_strings(&["consumer", "connect"]);
        let types = c_strings(&["Classic", "Consumer", "Share"]);
        let (sp, pp, tp) = (c_ptrs(&states), c_ptrs(&protocols), c_ptrs(&types));

        let options = unsafe { list_groups_options(sp.as_ptr(), 1, pp.as_ptr(), 2, tp.as_ptr(), 3, 7_000) };
        assert_eq!(options.group_states(), &HashSet::from([GroupState::Stable]));
        assert_eq!(
            options.protocol_types(),
            &HashSet::from(["consumer".to_string(), "connect".to_string()])
        );
        assert_eq!(
            options.types(),
            &HashSet::from([GroupType::Classic, GroupType::Consumer, GroupType::Share])
        );
        assert_eq!(options.timeout(), Some(7_000));

        // Null arrays leave every filter empty, i.e. "everything".
        let options = unsafe { list_groups_options(std::ptr::null(), 0, std::ptr::null(), 0, std::ptr::null(), 0, -1) };
        assert!(options.group_states().is_empty());
        assert!(options.protocol_types().is_empty());
        assert!(options.types().is_empty());
        assert_eq!(options.timeout(), None);
    }

    #[test]
    #[allow(deprecated)]
    fn list_consumer_groups_options_routes_each_array_to_its_own_filter() {
        let states = c_strings(&["Empty"]);
        let types = c_strings(&["Consumer", "Classic"]);
        let (sp, tp) = (c_ptrs(&states), c_ptrs(&types));

        let options = unsafe { list_consumer_groups_options(sp.as_ptr(), 1, tp.as_ptr(), 2, 8_000) };
        assert_eq!(options.group_states(), &HashSet::from([GroupState::Empty]));
        assert_eq!(options.types(), &HashSet::from([GroupType::Consumer, GroupType::Classic]));
        assert_eq!(options.timeout(), Some(8_000));
    }

    #[test]
    fn read_group_states_matches_java_parse_including_the_unknown_fallback() {
        // Java's `GroupState.parse` upper-cases before lookup and falls back to
        // UNKNOWN, so casing is irrelevant and a bogus name is not an error.
        let names = c_strings(&["stable", "PREPARINGREBALANCE", "NotReady", "not-a-state"]);
        let ptrs = c_ptrs(&names);
        let parsed = unsafe { read_group_states(ptrs.as_ptr(), 4) };
        assert_eq!(
            parsed,
            HashSet::from([
                GroupState::Stable,
                GroupState::PreparingRebalance,
                GroupState::NotReady,
                GroupState::Unknown,
            ])
        );
    }

    #[test]
    fn read_group_types_matches_java_parse_including_the_unknown_fallback() {
        let names = c_strings(&["CONSUMER", "streams", "nope"]);
        let ptrs = c_ptrs(&names);
        let parsed = unsafe { read_group_types(ptrs.as_ptr(), 3) };
        assert_eq!(
            parsed,
            HashSet::from([GroupType::Consumer, GroupType::Streams, GroupType::Unknown])
        );
    }

    #[test]
    fn describe_group_options_map_each_flag_to_its_own_field() {
        let options = describe_consumer_groups_options(1_500, true);
        assert_eq!(options.timeout(), Some(1_500));
        assert!(options.should_include_authorized_operations());
        let options = describe_consumer_groups_options(-1, false);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_include_authorized_operations());

        let options = describe_classic_groups_options(2_500, true);
        assert_eq!(options.timeout(), Some(2_500));
        assert!(options.should_include_authorized_operations());
        let options = describe_classic_groups_options(-1, false);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_include_authorized_operations());
    }

    #[test]
    fn list_consumer_group_offsets_options_maps_each_flag_to_its_own_field() {
        let options = list_consumer_group_offsets_options(9_000, true);
        assert_eq!(options.timeout(), Some(9_000));
        assert!(options.should_require_stable());

        let options = list_consumer_group_offsets_options(-1, false);
        assert_eq!(options.timeout(), None);
        assert!(!options.should_require_stable());
    }

    #[test]
    fn single_field_group_options_carry_only_the_timeout() {
        assert_eq!(alter_consumer_group_offsets_options(11_000).timeout(), Some(11_000));
        assert_eq!(alter_consumer_group_offsets_options(-1).timeout(), None);
        assert_eq!(delete_consumer_group_offsets_options(12_000).timeout(), Some(12_000));
        assert_eq!(delete_consumer_group_offsets_options(-1).timeout(), None);
        assert_eq!(delete_consumer_groups_options(13_000).timeout(), Some(13_000));
        assert_eq!(delete_consumer_groups_options(-1).timeout(), None);
    }

    #[test]
    fn remove_members_options_distinguishes_remove_all_from_a_member_list() {
        let ids = c_strings(&["instance-1", "instance-2"]);
        let ptrs = c_ptrs(&ids);
        let reason = CString::new("rolling restart").unwrap();

        let options =
            unsafe { remove_members_options(false, ptrs.as_ptr(), 2, reason.as_ptr(), 14_000) }.expect("valid members");
        assert!(!options.remove_all());
        assert_eq!(
            options.members(),
            &HashSet::from([MemberToRemove::new("instance-1"), MemberToRemove::new("instance-2")])
        );
        assert_eq!(options.reason_value(), Some("rolling restart"));
        assert_eq!(options.timeout(), Some(14_000));

        // `remove_all` ignores the member array entirely: Java's no-argument
        // constructor. A NULL reason leaves it unset.
        let options = unsafe { remove_members_options(true, ptrs.as_ptr(), 2, std::ptr::null(), -1) }
            .expect("remove-all is always valid");
        assert!(options.remove_all());
        assert!(options.members().is_empty());
        assert_eq!(options.reason_value(), None);
        assert_eq!(options.timeout(), None);
    }

    #[test]
    fn remove_members_options_rejects_an_empty_member_list() {
        // Java's `RemoveMembersFromConsumerGroupOptions(Collection)` throws for
        // an empty collection, so an empty array must not silently mean
        // "remove everything".
        let error = unsafe { remove_members_options(false, std::ptr::null(), 0, std::ptr::null(), -1) }
            .expect_err("empty members is rejected");
        assert_eq!(error.message(), "Invalid empty members has been provided");
    }

    #[test]
    fn read_group_offsets_specs_gives_each_group_its_own_selection() {
        let group_names = c_strings(&["g-all", "g-some"]);
        let group_ptrs = c_ptrs(&group_names);
        let all_partitions = [true, false];

        let some_topics = c_strings(&["t1", "t2"]);
        let some_topic_ptrs = c_ptrs(&some_topics);
        let some_partitions = [3i32, 4];
        // The first group is in `all_partitions` mode, so its arrays are never
        // read; NULL proves it.
        let topics: [*const *const c_char; 2] = [std::ptr::null(), some_topic_ptrs.as_ptr()];
        let partitions: [*const i32; 2] = [std::ptr::null(), some_partitions.as_ptr()];
        let counts = [0i32, 2];

        let specs = unsafe {
            read_group_offsets_specs(
                group_ptrs.as_ptr(),
                all_partitions.as_ptr(),
                topics.as_ptr(),
                partitions.as_ptr(),
                counts.as_ptr(),
                2,
            )
        }
        .expect("well-formed request");
        assert_eq!(specs.len(), 2);
        // Java's unset `topicPartitions()`: every partition.
        assert_eq!(specs["g-all"].get_topic_partitions(), None);
        assert_eq!(
            specs["g-some"].get_topic_partitions(),
            Some([TopicPartition::new("t1", 3), TopicPartition::new("t2", 4)].as_slice())
        );
    }

    #[test]
    fn read_group_offsets_specs_rejects_a_null_or_duplicate_group_id() {
        let all_partitions = [true, true];
        let good = CString::new("g").unwrap();

        let with_null: [*const c_char; 2] = [good.as_ptr(), std::ptr::null()];
        let error = unsafe {
            read_group_offsets_specs(
                with_null.as_ptr(),
                all_partitions.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                2,
            )
        }
        .expect_err("a null group id is rejected");
        assert_eq!(error.message(), "group id at index 1 must not be null");

        // Java takes a Map, where the second entry would silently replace the
        // first, so a duplicate is a marshaling error rather than a silent drop.
        let duplicated: [*const c_char; 2] = [good.as_ptr(), good.as_ptr()];
        let error = unsafe {
            read_group_offsets_specs(
                duplicated.as_ptr(),
                all_partitions.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                2,
            )
        }
        .expect_err("a duplicate group id is rejected");
        assert_eq!(error.message(), "group id `g` appears more than once at index 1");
    }

    #[test]
    fn read_alter_group_offsets_keeps_the_leader_epoch_flag_load_bearing() {
        let topics = c_strings(&["t", "t"]);
        let topic_ptrs = c_ptrs(&topics);
        let partitions = [0i32, 1];
        let offsets = [10i64, 20];
        let meta = c_strings(&["first", "second"]);
        let meta_ptrs = c_ptrs(&meta);
        // Identical epoch values with opposite flags: the flag, not the value,
        // has to decide. Epoch 0 is a real epoch, which a sentinel could not
        // express.
        let epochs = [0i32, 0];
        let has_epoch = [true, false];

        let parsed = unsafe {
            read_alter_group_offsets(
                topic_ptrs.as_ptr(),
                partitions.as_ptr(),
                offsets.as_ptr(),
                meta_ptrs.as_ptr(),
                epochs.as_ptr(),
                has_epoch.as_ptr(),
                2,
            )
        }
        .expect("well-formed offsets");

        let first = &parsed[&TopicPartition::new("t", 0)];
        assert_eq!(first.offset(), 10);
        assert_eq!(first.metadata(), "first");
        assert_eq!(first.leader_epoch(), Some(0));

        let second = &parsed[&TopicPartition::new("t", 1)];
        assert_eq!(second.offset(), 20);
        assert_eq!(second.metadata(), "second");
        assert_eq!(second.leader_epoch(), None);
    }

    #[test]
    fn read_alter_group_offsets_maps_null_metadata_to_the_empty_string() {
        let topics = c_strings(&["t"]);
        let topic_ptrs = c_ptrs(&topics);
        let partitions = [0i32];
        let offsets = [7i64];
        let meta: [*const c_char; 1] = [std::ptr::null()];

        let parsed = unsafe {
            read_alter_group_offsets(
                topic_ptrs.as_ptr(),
                partitions.as_ptr(),
                offsets.as_ptr(),
                meta.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                1,
            )
        }
        .expect("well-formed offsets");
        // Java's `OffsetAndMetadata` constructor normalises null metadata to "".
        assert_eq!(parsed[&TopicPartition::new("t", 0)].metadata(), "");
    }

    #[test]
    fn read_alter_group_offsets_rejects_a_null_topic_and_a_negative_offset() {
        let partitions = [0i32];
        let offsets = [-1i64];
        let null_topic: [*const c_char; 1] = [std::ptr::null()];
        let error = unsafe {
            read_alter_group_offsets(
                null_topic.as_ptr(),
                partitions.as_ptr(),
                offsets.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                1,
            )
        }
        .expect_err("a null topic is rejected");
        assert_eq!(error.message(), "topic at index 0 must not be null");

        let topics = c_strings(&["t"]);
        let topic_ptrs = c_ptrs(&topics);
        let error = unsafe {
            read_alter_group_offsets(
                topic_ptrs.as_ptr(),
                partitions.as_ptr(),
                offsets.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                1,
            )
        }
        .expect_err("a negative offset is rejected");
        // Java's `OffsetAndMetadata` constructor throws for a negative offset
        // with exactly this message; the index prefix tells a C caller which
        // array entry was at fault, which a Java `Map` call site does not need.
        assert_eq!(error.message(), "offset at index 0: Invalid negative offset");
    }

    #[test]
    fn read_required_string_rejects_null_and_names_the_parameter() {
        let error = unsafe { read_required_string(std::ptr::null(), "group_id") }.expect_err("null is rejected");
        assert_eq!(error.message(), "group_id must not be null");
        let good = CString::new("g1").unwrap();
        assert_eq!(unsafe { read_required_string(good.as_ptr(), "group_id") }.unwrap(), "g1");
    }

    // -- B4 flatteners ------------------------------------------------------

    /// A member with every optional field populated, so a transposition between
    /// two same-typed accessors changes an assertion.
    fn member_fixture() -> MemberDescription {
        MemberDescription::new(
            "consumer-7",
            Some("instance-7".to_string()),
            Some("rack-7".to_string()),
            "client-7",
            "host-7",
            MemberAssignment::new(HashSet::from([TopicPartition::new("tb", 1), TopicPartition::new("ta", 0)])),
            Some(MemberAssignment::new(HashSet::from([TopicPartition::new("tc", 2)]))),
            Some(17),
            Some(true),
        )
    }

    #[test]
    fn member_description_exposes_every_field_and_both_assignments() {
        let inner = MemberDescriptionInner::new(&member_fixture());
        let member = &inner as *const MemberDescriptionInner as *const kafka_admin_MemberDescription_t;
        unsafe {
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberDescription_consumer_id(member)).to_str(),
                Ok("consumer-7")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberDescription_group_instance_id(member)).to_str(),
                Ok("instance-7")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberDescription_rack_id(member)).to_str(),
                Ok("rack-7")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberDescription_client_id(member)).to_str(),
                Ok("client-7")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberDescription_host(member)).to_str(),
                Ok("host-7")
            );

            // The current assignment is sorted by (topic, partition).
            let assignment = kafka_admin_MemberDescription_assignment(member);
            assert_eq!(kafka_admin_MemberAssignment_count(assignment), 2);
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberAssignment_get_topic(assignment, 0)).to_str(),
                Ok("ta")
            );
            assert_eq!(kafka_admin_MemberAssignment_get_partition(assignment, 0), 0);
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberAssignment_get_topic(assignment, 1)).to_str(),
                Ok("tb")
            );
            assert_eq!(kafka_admin_MemberAssignment_get_partition(assignment, 1), 1);
            assert!(kafka_admin_MemberAssignment_get_topic(assignment, 2).is_null());
            assert_eq!(kafka_admin_MemberAssignment_get_partition(assignment, -1), -1);

            // The target assignment is a *different* handle with different
            // contents, so returning the wrong one is caught.
            let target = kafka_admin_MemberDescription_target_assignment(member);
            assert!(!target.is_null());
            assert_eq!(kafka_admin_MemberAssignment_count(target), 1);
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberAssignment_get_topic(target, 0)).to_str(),
                Ok("tc")
            );
            assert_eq!(kafka_admin_MemberAssignment_get_partition(target, 0), 2);

            let mut epoch = -99i32;
            assert!(kafka_admin_MemberDescription_member_epoch(member, &mut epoch));
            assert_eq!(epoch, 17);
            let mut upgraded = false;
            assert!(kafka_admin_MemberDescription_upgraded(member, &mut upgraded));
            assert!(upgraded);
        }
    }

    #[test]
    fn member_description_reports_absent_optionals_as_null_or_false() {
        let member = MemberDescription::new(
            "c",
            None,
            None,
            "cid",
            "h",
            MemberAssignment::new(HashSet::new()),
            None,
            None,
            None,
        );
        let inner = MemberDescriptionInner::new(&member);
        let ptr = &inner as *const MemberDescriptionInner as *const kafka_admin_MemberDescription_t;
        unsafe {
            assert!(kafka_admin_MemberDescription_group_instance_id(ptr).is_null());
            assert!(kafka_admin_MemberDescription_rack_id(ptr).is_null());
            // An absent target assignment is a null handle, distinct from a
            // present-but-empty one (the current assignment below).
            assert!(kafka_admin_MemberDescription_target_assignment(ptr).is_null());
            let assignment = kafka_admin_MemberDescription_assignment(ptr);
            assert!(!assignment.is_null());
            assert_eq!(kafka_admin_MemberAssignment_count(assignment), 0);

            let mut epoch = -99i32;
            assert!(!kafka_admin_MemberDescription_member_epoch(ptr, &mut epoch));
            assert_eq!(epoch, -99);
            let mut upgraded = true;
            assert!(!kafka_admin_MemberDescription_upgraded(ptr, &mut upgraded));
            assert!(upgraded);
            // A null out-param is tolerated.
            assert!(!kafka_admin_MemberDescription_member_epoch(ptr, std::ptr::null_mut()));
            assert!(!kafka_admin_MemberDescription_upgraded(ptr, std::ptr::null_mut()));
        }
    }

    #[test]
    fn describe_consumer_groups_result_carries_values_and_errors_per_group() {
        let description = ConsumerGroupDescription::new(
            "g-ok",
            false,
            vec![member_fixture()],
            "range",
            GroupType::Consumer,
            GroupState::Stable,
            Some(Node::new(3, "h3".to_string(), 9093)),
            BTreeSet::from([AclOperation::Describe, AclOperation::Read]),
            Some(11),
            Some(12),
        );
        let outcomes = HashMap::from([
            ("g-ok".to_string(), Ok(description)),
            ("g-bad".to_string(), Err(KafkaError::new(Errors::GroupIdNotFound))),
        ]);
        let result = box_describe_consumer_groups_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_DescribeConsumerGroupsResult_count(result), 2);
            // Sorted by group id: "g-bad" then "g-ok".
            assert_eq!(
                CStr::from_ptr(kafka_admin_DescribeConsumerGroupsResult_get_group_id(result, 0)).to_str(),
                Ok("g-bad")
            );
            assert!(kafka_admin_DescribeConsumerGroupsResult_get_value(result, 0).is_null());
            let error = kafka_admin_DescribeConsumerGroupsResult_get_error(result, 0);
            assert_eq!(
                common::kafka_common_KafkaError_code(error),
                Errors::GroupIdNotFound.code() as i32
            );

            let value = kafka_admin_DescribeConsumerGroupsResult_get_value(result, 1);
            assert!(!value.is_null());
            assert!(kafka_admin_DescribeConsumerGroupsResult_get_error(result, 1).is_null());
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupDescription_group_id(value)).to_str(),
                Ok("g-ok")
            );
            assert!(!kafka_admin_ConsumerGroupDescription_is_simple_consumer_group(value));
            assert_eq!(kafka_admin_ConsumerGroupDescription_member_count(value), 1);
            assert!(!kafka_admin_ConsumerGroupDescription_get_member(value, 0).is_null());
            assert!(kafka_admin_ConsumerGroupDescription_get_member(value, 1).is_null());
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupDescription_partition_assignor(value)).to_str(),
                Ok("range")
            );
            // `type()`, `state()` and `groupState()` are three distinct strings.
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupDescription_group_type(value)).to_str(),
                Ok("Consumer")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupDescription_state(value)).to_str(),
                Ok("Stable")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupDescription_group_state(value)).to_str(),
                Ok("Stable")
            );
            assert!(!kafka_admin_ConsumerGroupDescription_coordinator(value).is_null());
            assert_eq!(kafka_admin_ConsumerGroupDescription_authorized_operation_count(value), 2);
            assert_eq!(
                kafka_admin_ConsumerGroupDescription_authorized_operation(value, 0),
                i32::from(AclOperation::Read.code())
            );
            assert_eq!(kafka_admin_ConsumerGroupDescription_authorized_operation(value, 2), -1);

            // Distinct epoch values catch a transposition between the two.
            let mut epoch = -99i32;
            assert!(kafka_admin_ConsumerGroupDescription_group_epoch(value, &mut epoch));
            assert_eq!(epoch, 11);
            assert!(kafka_admin_ConsumerGroupDescription_target_assignment_epoch(value, &mut epoch));
            assert_eq!(epoch, 12);

            assert!(kafka_admin_DescribeConsumerGroupsResult_get_group_id(result, 2).is_null());
            assert!(kafka_admin_DescribeConsumerGroupsResult_get_error(result, -1).is_null());
            kafka_admin_DescribeConsumerGroupsResult_destroy(result);
        }
    }

    #[test]
    fn describe_classic_groups_result_carries_protocol_and_protocol_data() {
        let description = ClassicGroupDescription::new(
            "cg",
            "consumer",
            "range",
            vec![member_fixture()],
            ClassicGroupState::Stable,
            Some(Node::new(1, "h1".to_string(), 9091)),
            BTreeSet::from([AclOperation::Delete]),
        );
        let outcomes = HashMap::from([("cg".to_string(), Ok(description))]);
        let result = box_describe_classic_groups_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_DescribeClassicGroupsResult_count(result), 1);
            let value = kafka_admin_DescribeClassicGroupsResult_get_value(result, 0);
            assert!(!value.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_admin_ClassicGroupDescription_group_id(value)).to_str(),
                Ok("cg")
            );
            // `protocol` and `protocolData` are different strings, so swapping
            // them at the call site fails here.
            assert_eq!(
                CStr::from_ptr(kafka_admin_ClassicGroupDescription_protocol(value)).to_str(),
                Ok("consumer")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_ClassicGroupDescription_protocol_data(value)).to_str(),
                Ok("range")
            );
            assert!(!kafka_admin_ClassicGroupDescription_is_simple_consumer_group(value));
            assert_eq!(kafka_admin_ClassicGroupDescription_member_count(value), 1);
            assert_eq!(
                CStr::from_ptr(kafka_admin_ClassicGroupDescription_state(value)).to_str(),
                Ok("Stable")
            );
            assert!(!kafka_admin_ClassicGroupDescription_coordinator(value).is_null());
            assert_eq!(kafka_admin_ClassicGroupDescription_authorized_operation_count(value), 1);
            assert_eq!(
                kafka_admin_ClassicGroupDescription_authorized_operation(value, 0),
                i32::from(AclOperation::Delete.code())
            );
            assert!(kafka_admin_DescribeClassicGroupsResult_get_error(result, 0).is_null());
            kafka_admin_DescribeClassicGroupsResult_destroy(result);
        }
    }

    #[test]
    fn list_groups_result_keeps_the_valid_and_error_lists_independent() {
        // Deliberately different lengths: two listings, one error. A caller who
        // indexed the errors with the listing count would read past the end.
        let outcome = (
            vec![
                GroupListing::new("g1", Some(GroupType::Consumer), "consumer", Some(GroupState::Stable)),
                GroupListing::new("g2", None, "consumer", None),
                GroupListing::new("g3", Some(GroupType::Classic), "", Some(GroupState::Empty)),
            ],
            vec![KafkaError::new(Errors::CoordinatorNotAvailable)],
        );
        let result = box_list_groups_result(outcome);
        unsafe {
            assert_eq!(kafka_admin_ListGroupsResult_valid_count(result), 3);
            assert_eq!(kafka_admin_ListGroupsResult_error_count(result), 1);

            let first = kafka_admin_ListGroupsResult_get_valid(result, 0);
            assert_eq!(CStr::from_ptr(kafka_admin_GroupListing_group_id(first)).to_str(), Ok("g1"));
            assert_eq!(
                CStr::from_ptr(kafka_admin_GroupListing_group_type(first)).to_str(),
                Ok("Consumer")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_GroupListing_protocol(first)).to_str(),
                Ok("consumer")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_GroupListing_group_state(first)).to_str(),
                Ok("Stable")
            );
            assert!(!kafka_admin_GroupListing_is_simple_consumer_group(first));

            // Absent `Optional`s are null strings.
            let second = kafka_admin_ListGroupsResult_get_valid(result, 1);
            assert_eq!(CStr::from_ptr(kafka_admin_GroupListing_group_id(second)).to_str(), Ok("g2"));
            assert!(kafka_admin_GroupListing_group_type(second).is_null());
            assert!(kafka_admin_GroupListing_group_state(second).is_null());
            assert_eq!(
                CStr::from_ptr(kafka_admin_GroupListing_protocol(second)).to_str(),
                Ok("consumer")
            );
            assert!(!kafka_admin_GroupListing_is_simple_consumer_group(second));

            // Java's `isSimpleConsumerGroup()` is "a CLASSIC group with an empty
            // protocol", so it needs both, not just the empty protocol.
            let third = kafka_admin_ListGroupsResult_get_valid(result, 2);
            assert_eq!(
                CStr::from_ptr(kafka_admin_GroupListing_group_type(third)).to_str(),
                Ok("Classic")
            );
            assert_eq!(CStr::from_ptr(kafka_admin_GroupListing_protocol(third)).to_str(), Ok(""));
            assert_eq!(
                CStr::from_ptr(kafka_admin_GroupListing_group_state(third)).to_str(),
                Ok("Empty")
            );
            assert!(kafka_admin_GroupListing_is_simple_consumer_group(third));

            let error = kafka_admin_ListGroupsResult_get_error(result, 0);
            assert_eq!(
                common::kafka_common_KafkaError_code(error),
                Errors::CoordinatorNotAvailable.code() as i32
            );
            // Index 1 is a valid listing index but not a valid error index.
            assert!(kafka_admin_ListGroupsResult_get_error(result, 1).is_null());
            assert!(kafka_admin_ListGroupsResult_get_valid(result, 3).is_null());
            assert!(kafka_admin_ListGroupsResult_get_valid(result, -1).is_null());
            kafka_admin_ListGroupsResult_destroy(result);
        }
    }

    #[test]
    #[allow(deprecated)]
    fn list_consumer_groups_result_exposes_both_state_views() {
        let outcome = (
            vec![ConsumerGroupListing::new(
                "cg1",
                Some(GroupState::Stable),
                Some(GroupType::Classic),
                true,
            )],
            vec![KafkaError::new(Errors::GroupAuthorizationFailed)],
        );
        let result = box_list_consumer_groups_result(outcome);
        unsafe {
            assert_eq!(kafka_admin_ListConsumerGroupsResult_valid_count(result), 1);
            assert_eq!(kafka_admin_ListConsumerGroupsResult_error_count(result), 1);
            let listing = kafka_admin_ListConsumerGroupsResult_get_valid(result, 0);
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupListing_group_id(listing)).to_str(),
                Ok("cg1")
            );
            assert!(kafka_admin_ConsumerGroupListing_is_simple_consumer_group(listing));
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupListing_group_state(listing)).to_str(),
                Ok("Stable")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupListing_state(listing)).to_str(),
                Ok("Stable")
            );
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConsumerGroupListing_group_type(listing)).to_str(),
                Ok("Classic")
            );
            let error = kafka_admin_ListConsumerGroupsResult_get_error(result, 0);
            assert_eq!(
                common::kafka_common_KafkaError_code(error),
                Errors::GroupAuthorizationFailed.code() as i32
            );
            kafka_admin_ListConsumerGroupsResult_destroy(result);
        }
    }

    #[test]
    fn list_consumer_group_offsets_result_is_two_level_and_keeps_null_offsets() {
        let offsets: GroupOffsets = HashMap::from([
            (
                TopicPartition::new("ta", 0),
                Some(OffsetAndMetadata::with_leader_epoch(100, Some(4), "meta-a").unwrap()),
            ),
            // Java reports a requested partition the group never committed for
            // as present with a null value.
            (TopicPartition::new("tb", 1), None),
        ]);
        let outcomes = HashMap::from([
            ("g-ok".to_string(), Ok(offsets)),
            ("g-bad".to_string(), Err(KafkaError::unsupported_version("Not implemented yet"))),
        ]);
        let result = box_list_consumer_group_offsets_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_ListConsumerGroupOffsetsResult_count(result), 2);
            // Sorted by group id: "g-bad" then "g-ok".
            assert!(kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, 0).is_null());
            let error = kafka_admin_ListConsumerGroupOffsetsResult_get_error(result, 0);
            assert_eq!(
                CStr::from_ptr(common::kafka_common_KafkaError_message(error)).to_str(),
                Ok("Not implemented yet")
            );

            assert_eq!(
                CStr::from_ptr(kafka_admin_ListConsumerGroupOffsetsResult_get_group_id(result, 1)).to_str(),
                Ok("g-ok")
            );
            assert!(kafka_admin_ListConsumerGroupOffsetsResult_get_error(result, 1).is_null());
            let map = kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, 1);
            assert!(!map.is_null());
            assert_eq!(kafka_admin_OffsetAndMetadataMap_count(map), 2);

            assert_eq!(
                CStr::from_ptr(kafka_admin_OffsetAndMetadataMap_get_topic(map, 0)).to_str(),
                Ok("ta")
            );
            assert_eq!(kafka_admin_OffsetAndMetadataMap_get_partition(map, 0), 0);
            assert!(kafka_admin_OffsetAndMetadataMap_has_offset(map, 0));
            assert_eq!(kafka_admin_OffsetAndMetadataMap_get_offset(map, 0), 100);
            assert_eq!(
                CStr::from_ptr(kafka_admin_OffsetAndMetadataMap_get_metadata(map, 0)).to_str(),
                Ok("meta-a")
            );
            let mut epoch = -99i32;
            assert!(kafka_admin_OffsetAndMetadataMap_get_leader_epoch(map, 0, &mut epoch));
            assert_eq!(epoch, 4);

            // The uncommitted partition is present with no offset: `has_offset`
            // is the discriminant, and the other accessors report absence.
            assert_eq!(
                CStr::from_ptr(kafka_admin_OffsetAndMetadataMap_get_topic(map, 1)).to_str(),
                Ok("tb")
            );
            assert_eq!(kafka_admin_OffsetAndMetadataMap_get_partition(map, 1), 1);
            assert!(!kafka_admin_OffsetAndMetadataMap_has_offset(map, 1));
            assert_eq!(kafka_admin_OffsetAndMetadataMap_get_offset(map, 1), -1);
            assert!(kafka_admin_OffsetAndMetadataMap_get_metadata(map, 1).is_null());
            assert!(!kafka_admin_OffsetAndMetadataMap_get_leader_epoch(map, 1, &mut epoch));

            assert!(!kafka_admin_OffsetAndMetadataMap_has_offset(map, 2));
            assert_eq!(kafka_admin_OffsetAndMetadataMap_get_partition(map, -1), -1);
            kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);
        }
    }

    #[test]
    fn partition_keyed_void_results_report_success_as_a_null_error() {
        let outcomes: PartitionVoidOutcomes = HashMap::from([
            (TopicPartition::new("t", 0), Ok(())),
            (TopicPartition::new("t", 1), Err(KafkaError::new(Errors::UnknownMemberId))),
        ]);
        let altered = box_alter_consumer_group_offsets_result(outcomes.clone());
        let deleted = box_delete_consumer_group_offsets_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_AlterConsumerGroupOffsetsResult_count(altered), 2);
            assert_eq!(
                CStr::from_ptr(kafka_admin_AlterConsumerGroupOffsetsResult_get_topic(altered, 0)).to_str(),
                Ok("t")
            );
            assert_eq!(kafka_admin_AlterConsumerGroupOffsetsResult_get_partition(altered, 0), 0);
            assert!(kafka_admin_AlterConsumerGroupOffsetsResult_get_error(altered, 0).is_null());
            assert_eq!(kafka_admin_AlterConsumerGroupOffsetsResult_get_partition(altered, 1), 1);
            assert_eq!(
                common::kafka_common_KafkaError_code(kafka_admin_AlterConsumerGroupOffsetsResult_get_error(altered, 1)),
                Errors::UnknownMemberId.code() as i32
            );
            assert_eq!(kafka_admin_AlterConsumerGroupOffsetsResult_get_partition(altered, 2), -1);
            assert!(kafka_admin_AlterConsumerGroupOffsetsResult_get_topic(altered, -1).is_null());
            kafka_admin_AlterConsumerGroupOffsetsResult_destroy(altered);

            assert_eq!(kafka_admin_DeleteConsumerGroupOffsetsResult_count(deleted), 2);
            assert!(kafka_admin_DeleteConsumerGroupOffsetsResult_get_error(deleted, 0).is_null());
            assert_eq!(
                common::kafka_common_KafkaError_code(kafka_admin_DeleteConsumerGroupOffsetsResult_get_error(
                    deleted, 1
                )),
                Errors::UnknownMemberId.code() as i32
            );
            assert_eq!(kafka_admin_DeleteConsumerGroupOffsetsResult_get_partition(deleted, -1), -1);
            kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(deleted);
        }
    }

    #[test]
    fn string_keyed_void_results_report_success_as_a_null_error() {
        let outcomes: GroupVoidOutcomes = HashMap::from([
            ("a".to_string(), Ok(())),
            ("b".to_string(), Err(KafkaError::new(Errors::GroupIdNotFound))),
        ]);
        let groups = box_delete_consumer_groups_result(outcomes.clone());
        let members = box_remove_members_from_consumer_group_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_DeleteConsumerGroupsResult_count(groups), 2);
            assert_eq!(
                CStr::from_ptr(kafka_admin_DeleteConsumerGroupsResult_get_group_id(groups, 0)).to_str(),
                Ok("a")
            );
            assert!(kafka_admin_DeleteConsumerGroupsResult_get_error(groups, 0).is_null());
            assert_eq!(
                common::kafka_common_KafkaError_code(kafka_admin_DeleteConsumerGroupsResult_get_error(groups, 1)),
                Errors::GroupIdNotFound.code() as i32
            );
            assert!(kafka_admin_DeleteConsumerGroupsResult_get_group_id(groups, 2).is_null());
            kafka_admin_DeleteConsumerGroupsResult_destroy(groups);

            assert_eq!(kafka_admin_RemoveMembersFromConsumerGroupResult_count(members), 2);
            assert_eq!(
                CStr::from_ptr(kafka_admin_RemoveMembersFromConsumerGroupResult_get_group_instance_id(
                    members, 1
                ))
                .to_str(),
                Ok("b")
            );
            assert!(kafka_admin_RemoveMembersFromConsumerGroupResult_get_error(members, 0).is_null());
            assert!(!kafka_admin_RemoveMembersFromConsumerGroupResult_get_error(members, 1).is_null());
            assert!(kafka_admin_RemoveMembersFromConsumerGroupResult_get_error(members, -1).is_null());
            kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(members);
        }
    }

    // -- B5a: ACLs and client quotas ----------------------------------------
    //
    // Java's own `MockAdminClient` throws `UnsupportedOperationException` for
    // all five of these RPCs — `createAcls` (MockAdminClient.java:806),
    // `describeAcls` (:811), `deleteAcls` (:816), `describeClientQuotas`
    // (:1243) and `alterClientQuotas` (:1248) — so the Rust mock fails every
    // future it hands out and **the success path of every drain in this slice
    // is unreachable end-to-end**. Hand-built fixtures against the pure
    // marshaling helpers are therefore the only coverage those paths can have.

    /// Builds a binding whose seven fields are all distinct, so that a
    /// transposition of any two of them fails an assertion.
    fn acl_binding(name: &str, principal: &str) -> AclBinding {
        AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, name, PatternType::Prefixed).expect("valid pattern"),
            AccessControlEntry::new(principal, "10.0.0.1", AclOperation::Write, AclPermissionType::Deny)
                .expect("valid entry"),
        )
    }

    fn quota_entity(pairs: &[(&str, Option<&str>)]) -> ClientQuotaEntity {
        ClientQuotaEntity::new(pairs.iter().map(|(t, n)| ((*t).to_string(), n.map(str::to_string))).collect())
    }

    /// Turns a slice of `&str` into the NUL-terminated pointer array the C
    /// entry points take. The returned `CString`s must outlive the pointers.
    fn c_array(values: &[&str]) -> (Vec<CString>, Vec<*const c_char>) {
        let owned: Vec<CString> = values.iter().map(|v| to_cstring(v)).collect();
        let ptrs = owned.iter().map(|c| c.as_ptr()).collect();
        (owned, ptrs)
    }

    /// Same, but a `None` becomes a NULL entry rather than being dropped.
    fn c_array_opt(values: &[Option<&str>]) -> (Vec<Option<CString>>, Vec<*const c_char>) {
        let owned: Vec<Option<CString>> = values.iter().map(|v| v.map(to_cstring)).collect();
        let ptrs = owned
            .iter()
            .map(|c| match c {
                Some(s) => s.as_ptr(),
                None => std::ptr::null(),
            })
            .collect();
        (owned, ptrs)
    }

    // -- Option builders ----------------------------------------------------

    #[test]
    fn acl_and_quota_options_map_the_timeout_to_its_own_field() {
        assert_eq!(create_acls_options(1_000).timeout(), Some(1_000));
        assert_eq!(create_acls_options(-1).timeout(), None);
        assert_eq!(describe_acls_options(2_000).timeout(), Some(2_000));
        assert_eq!(describe_acls_options(-1).timeout(), None);
        assert_eq!(delete_acls_options(3_000).timeout(), Some(3_000));
        assert_eq!(delete_acls_options(-1).timeout(), None);
        assert_eq!(describe_client_quotas_options(4_000).timeout(), Some(4_000));
        assert_eq!(describe_client_quotas_options(-1).timeout(), None);
    }

    #[test]
    fn alter_client_quotas_options_maps_each_flag_to_its_own_field() {
        let options = alter_client_quotas_options(5_000, true);
        assert_eq!(options.timeout(), Some(5_000));
        assert!(options.is_validate_only());

        // Reversed, so a transposition cannot satisfy both cases.
        let options = alter_client_quotas_options(-1, false);
        assert_eq!(options.timeout(), None);
        assert!(!options.is_validate_only());
    }

    // -- AclBinding / AclBindingFilter value handles -------------------------

    #[test]
    fn acl_binding_exposes_all_seven_java_fields_with_their_code_values() {
        let inner = AclBindingInner::new(&acl_binding("orders-", "User:alice"));
        let b = inner.as_ptr();
        unsafe {
            assert_eq!(kafka_common_AclBinding_resource_type(b), i32::from(ResourceType::Topic.code()));
            assert_eq!(CStr::from_ptr(kafka_common_AclBinding_resource_name(b)).to_str(), Ok("orders-"));
            assert_eq!(kafka_common_AclBinding_pattern_type(b), i32::from(PatternType::Prefixed.code()));
            assert_eq!(CStr::from_ptr(kafka_common_AclBinding_principal(b)).to_str(), Ok("User:alice"));
            assert_eq!(CStr::from_ptr(kafka_common_AclBinding_host(b)).to_str(), Ok("10.0.0.1"));
            assert_eq!(kafka_common_AclBinding_operation(b), i32::from(AclOperation::Write.code()));
            assert_eq!(
                kafka_common_AclBinding_permission_type(b),
                i32::from(AclPermissionType::Deny.code())
            );
        }
    }

    #[test]
    fn acl_binding_codes_are_javas_and_are_pairwise_distinct() {
        // The four enums cross as `code()` values because Java defines one for
        // each; these are the constants a C caller compares against. Asserted
        // as literals so a renumbering is caught here rather than on the wire.
        assert_eq!(
            (
                ResourceType::Unknown.code(),
                ResourceType::Any.code(),
                ResourceType::Topic.code(),
                ResourceType::Group.code(),
                ResourceType::Cluster.code(),
                ResourceType::TransactionalId.code(),
                ResourceType::DelegationToken.code(),
                ResourceType::User.code(),
            ),
            (0, 1, 2, 3, 4, 5, 6, 7)
        );
        assert_eq!(
            (
                PatternType::Unknown.code(),
                PatternType::Any.code(),
                PatternType::Match.code(),
                PatternType::Literal.code(),
                PatternType::Prefixed.code(),
            ),
            (0, 1, 2, 3, 4)
        );
        assert_eq!(
            (
                AclPermissionType::Unknown.code(),
                AclPermissionType::Any.code(),
                AclPermissionType::Deny.code(),
                AclPermissionType::Allow.code(),
            ),
            (0, 1, 2, 3)
        );
        assert_eq!(
            (
                AclOperation::Unknown.code(),
                AclOperation::Any.code(),
                AclOperation::All.code(),
                AclOperation::Read.code(),
                AclOperation::Write.code(),
                AclOperation::Describe.code(),
                AclOperation::TwoPhaseCommit.code(),
            ),
            (0, 1, 2, 3, 4, 8, 15)
        );
    }

    #[test]
    fn acl_binding_filter_distinguishes_a_null_string_from_an_empty_one() {
        // Java's filter strings are nullable: null means "match any". The empty
        // string is a real, different filter, which is why a null pointer is a
        // sufficient encoding and no extra discriminant is needed.
        let any = AclBindingFilterInner::new(&AclBindingFilter::any());
        let empty = AclBindingFilterInner::new(&AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Topic, Some(String::new()), PatternType::Literal),
            AccessControlEntryFilter::new(
                Some(String::new()),
                Some(String::new()),
                AclOperation::Read,
                AclPermissionType::Allow,
            ),
        ));
        unsafe {
            let a = &any as *const AclBindingFilterInner as *const kafka_common_AclBindingFilter_t;
            assert!(kafka_common_AclBindingFilter_resource_name(a).is_null());
            assert!(kafka_common_AclBindingFilter_principal(a).is_null());
            assert!(kafka_common_AclBindingFilter_host(a).is_null());
            // `AclBindingFilter::any()` is ANY on all four enums.
            assert_eq!(
                kafka_common_AclBindingFilter_resource_type(a),
                i32::from(ResourceType::Any.code())
            );
            assert_eq!(
                kafka_common_AclBindingFilter_pattern_type(a),
                i32::from(PatternType::Any.code())
            );
            assert_eq!(kafka_common_AclBindingFilter_operation(a), i32::from(AclOperation::Any.code()));
            assert_eq!(
                kafka_common_AclBindingFilter_permission_type(a),
                i32::from(AclPermissionType::Any.code())
            );

            let e = &empty as *const AclBindingFilterInner as *const kafka_common_AclBindingFilter_t;
            assert!(!kafka_common_AclBindingFilter_resource_name(e).is_null());
            assert_eq!(CStr::from_ptr(kafka_common_AclBindingFilter_resource_name(e)).to_str(), Ok(""));
            assert_eq!(CStr::from_ptr(kafka_common_AclBindingFilter_principal(e)).to_str(), Ok(""));
            assert_eq!(CStr::from_ptr(kafka_common_AclBindingFilter_host(e)).to_str(), Ok(""));
        }
    }

    // -- ACL request marshaling ---------------------------------------------

    #[test]
    fn read_acl_bindings_keeps_each_parallel_array_on_its_own_field() {
        let (_names, name_ptrs) = c_array(&["topic-a", "topic-b"]);
        let (_principals, principal_ptrs) = c_array(&["User:alice", "User:bob"]);
        let (_hosts, host_ptrs) = c_array(&["10.0.0.1", "10.0.0.2"]);
        // Every column carries a different value in each row, and no two
        // columns share a value, so a swapped pair of arguments is visible.
        let resource_types = [ResourceType::Topic.code() as i32, ResourceType::Group.code() as i32];
        let pattern_types = [PatternType::Literal.code() as i32, PatternType::Prefixed.code() as i32];
        let operations = [AclOperation::Read.code() as i32, AclOperation::Write.code() as i32];
        let permission_types = [
            AclPermissionType::Allow.code() as i32,
            AclPermissionType::Deny.code() as i32,
        ];

        let acls = unsafe {
            read_acl_bindings(
                resource_types.as_ptr(),
                name_ptrs.as_ptr(),
                pattern_types.as_ptr(),
                principal_ptrs.as_ptr(),
                host_ptrs.as_ptr(),
                operations.as_ptr(),
                permission_types.as_ptr(),
                2,
            )
        }
        .expect("valid bindings");

        assert_eq!(acls.len(), 2);
        assert_eq!(acls[0].pattern().resource_type(), ResourceType::Topic);
        assert_eq!(acls[0].pattern().name(), "topic-a");
        assert_eq!(acls[0].pattern().pattern_type(), PatternType::Literal);
        assert_eq!(acls[0].entry().principal(), "User:alice");
        assert_eq!(acls[0].entry().host(), "10.0.0.1");
        assert_eq!(acls[0].entry().operation(), AclOperation::Read);
        assert_eq!(acls[0].entry().permission_type(), AclPermissionType::Allow);
        assert_eq!(acls[1].pattern().resource_type(), ResourceType::Group);
        assert_eq!(acls[1].pattern().name(), "topic-b");
        assert_eq!(acls[1].pattern().pattern_type(), PatternType::Prefixed);
        assert_eq!(acls[1].entry().principal(), "User:bob");
        assert_eq!(acls[1].entry().host(), "10.0.0.2");
        assert_eq!(acls[1].entry().operation(), AclOperation::Write);
        assert_eq!(acls[1].entry().permission_type(), AclPermissionType::Deny);
    }

    #[test]
    fn read_acl_bindings_propagates_javas_constructor_messages_with_the_row_index() {
        let (_names, name_ptrs) = c_array(&["t0", "t1"]);
        let (_principals, principal_ptrs) = c_array(&["User:a", "User:b"]);
        let (_hosts, host_ptrs) = c_array(&["*", "*"]);
        let literal = PatternType::Literal.code() as i32;
        let allow = AclPermissionType::Allow.code() as i32;
        let read = AclOperation::Read.code() as i32;
        let topic = ResourceType::Topic.code() as i32;

        // (expected message, resource types, pattern types, operations, permission types)
        type Case = (&'static str, [i32; 2], [i32; 2], [i32; 2], [i32; 2]);
        let cases: [Case; 4] = [
            // ANY resource type, on row 1.
            (
                "acl at index 1: resourceType must not be ANY",
                [topic, ResourceType::Any.code() as i32],
                [literal, literal],
                [read, read],
                [allow, allow],
            ),
            // MATCH pattern type, on row 0.
            (
                "acl at index 0: patternType must not be MATCH",
                [topic, topic],
                [PatternType::Match.code() as i32, literal],
                [read, read],
                [allow, allow],
            ),
            // ANY operation, on row 1.
            (
                "acl at index 1: operation must not be ANY",
                [topic, topic],
                [literal, literal],
                [read, AclOperation::Any.code() as i32],
                [allow, allow],
            ),
            // ANY permission type, on row 0.
            (
                "acl at index 0: permissionType must not be ANY",
                [topic, topic],
                [literal, literal],
                [read, read],
                [AclPermissionType::Any.code() as i32, allow],
            ),
        ];
        for (expected, resource_types, pattern_types, operations, permission_types) in cases {
            let err = unsafe {
                read_acl_bindings(
                    resource_types.as_ptr(),
                    name_ptrs.as_ptr(),
                    pattern_types.as_ptr(),
                    principal_ptrs.as_ptr(),
                    host_ptrs.as_ptr(),
                    operations.as_ptr(),
                    permission_types.as_ptr(),
                    2,
                )
            }
            .expect_err("rejected");
            assert!(matches!(err, KafkaError::IllegalArgument(_)));
            assert_eq!(err.message(), expected);
        }
    }

    #[test]
    fn read_acl_bindings_rejects_a_null_entry_in_a_non_nullable_string_array() {
        let (_names, name_ptrs) = c_array_opt(&[Some("t0"), None]);
        let (_principals, principal_ptrs) = c_array(&["User:a", "User:b"]);
        let (_hosts, host_ptrs) = c_array(&["*", "*"]);
        let topic = [ResourceType::Topic.code() as i32; 2];
        let literal = [PatternType::Literal.code() as i32; 2];
        let read = [AclOperation::Read.code() as i32; 2];
        let allow = [AclPermissionType::Allow.code() as i32; 2];

        let err = unsafe {
            read_acl_bindings(
                topic.as_ptr(),
                name_ptrs.as_ptr(),
                literal.as_ptr(),
                principal_ptrs.as_ptr(),
                host_ptrs.as_ptr(),
                read.as_ptr(),
                allow.as_ptr(),
                2,
            )
        }
        .expect_err("rejected");
        assert_eq!(err.message(), "resource name at index 1 must not be null");
    }

    #[test]
    fn read_acl_bindings_maps_an_unrecognised_code_to_unknown_as_java_does() {
        // Java's `fromCode` returns UNKNOWN rather than throwing, and the
        // constructors accept UNKNOWN (only ANY is rejected). The broker is
        // what refuses it.
        let (_names, name_ptrs) = c_array(&["t"]);
        let (_principals, principal_ptrs) = c_array(&["User:a"]);
        let (_hosts, host_ptrs) = c_array(&["*"]);
        let acls = unsafe {
            read_acl_bindings(
                [99i32].as_ptr(),
                name_ptrs.as_ptr(),
                [PatternType::Literal.code() as i32].as_ptr(),
                principal_ptrs.as_ptr(),
                host_ptrs.as_ptr(),
                [98i32].as_ptr(),
                [AclPermissionType::Allow.code() as i32].as_ptr(),
                1,
            )
        }
        .expect("UNKNOWN is accepted");
        assert_eq!(acls[0].pattern().resource_type(), ResourceType::Unknown);
        assert_eq!(acls[0].entry().operation(), AclOperation::Unknown);
    }

    #[test]
    fn read_acl_binding_filters_preserve_null_entries_rather_than_dropping_them() {
        // The critical difference from `read_strings`, which skips NULLs: a
        // dropped entry would shift every later row's fields onto the wrong
        // filter. Row 0 has a null name, row 1 a null principal.
        let (_names, name_ptrs) = c_array_opt(&[None, Some("topic-b")]);
        let (_principals, principal_ptrs) = c_array_opt(&[Some("User:alice"), None]);
        let (_hosts, host_ptrs) = c_array_opt(&[Some("10.0.0.1"), None]);
        let resource_types = [ResourceType::Any.code() as i32, ResourceType::Topic.code() as i32];
        let pattern_types = [PatternType::Match.code() as i32, PatternType::Literal.code() as i32];
        let operations = [AclOperation::Any.code() as i32, AclOperation::Describe.code() as i32];
        let permission_types = [
            AclPermissionType::Any.code() as i32,
            AclPermissionType::Allow.code() as i32,
        ];

        let filters = unsafe {
            read_acl_binding_filters(
                resource_types.as_ptr(),
                name_ptrs.as_ptr(),
                pattern_types.as_ptr(),
                principal_ptrs.as_ptr(),
                host_ptrs.as_ptr(),
                operations.as_ptr(),
                permission_types.as_ptr(),
                2,
            )
        };
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].pattern_filter().resource_type(), ResourceType::Any);
        assert_eq!(filters[0].pattern_filter().name(), None);
        assert_eq!(filters[0].pattern_filter().pattern_type(), PatternType::Match);
        assert_eq!(filters[0].entry_filter().principal(), Some("User:alice"));
        assert_eq!(filters[0].entry_filter().host(), Some("10.0.0.1"));
        assert_eq!(filters[1].pattern_filter().name(), Some("topic-b"));
        assert_eq!(filters[1].pattern_filter().pattern_type(), PatternType::Literal);
        assert_eq!(filters[1].entry_filter().principal(), None);
        assert_eq!(filters[1].entry_filter().host(), None);
        assert_eq!(filters[1].entry_filter().operation(), AclOperation::Describe);
    }

    // -- ACL result flattening ----------------------------------------------

    #[test]
    fn create_acls_result_reports_success_as_a_null_error_and_sorts_by_binding() {
        let ok = acl_binding("a-topic", "User:alice");
        let failed = acl_binding("z-topic", "User:zoe");
        let outcomes: CreateAclsOutcomes = HashMap::from([
            (ok.clone(), Ok(())),
            (failed.clone(), Err(KafkaError::new(Errors::SecurityDisabled))),
        ]);
        let result = box_create_acls_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_CreateAclsResult_count(result), 2);
            // Sorted by resource name, so "a-topic" comes first.
            let first = kafka_admin_CreateAclsResult_get_binding(result, 0);
            assert_eq!(
                CStr::from_ptr(kafka_common_AclBinding_resource_name(first)).to_str(),
                Ok("a-topic")
            );
            assert!(kafka_admin_CreateAclsResult_get_error(result, 0).is_null());

            let second = kafka_admin_CreateAclsResult_get_binding(result, 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_AclBinding_principal(second)).to_str(),
                Ok("User:zoe")
            );
            assert_eq!(
                common::kafka_common_KafkaError_code(kafka_admin_CreateAclsResult_get_error(result, 1)),
                Errors::SecurityDisabled.code() as i32
            );

            assert!(kafka_admin_CreateAclsResult_get_binding(result, 2).is_null());
            assert!(kafka_admin_CreateAclsResult_get_binding(result, -1).is_null());
            assert!(kafka_admin_CreateAclsResult_get_error(result, -1).is_null());
            kafka_admin_CreateAclsResult_destroy(result);
        }
    }

    #[test]
    fn describe_acls_result_keeps_the_broker_order_and_has_no_per_key_error() {
        // `DescribeAclsResult` holds one future for the whole call, so the
        // listing is unsorted (broker order) and there is no `_get_error`.
        let result =
            box_describe_acls_result(vec![acl_binding("z-topic", "User:zoe"), acl_binding("a-topic", "User:alice")]);
        unsafe {
            assert_eq!(kafka_admin_DescribeAclsResult_count(result), 2);
            assert_eq!(
                CStr::from_ptr(kafka_common_AclBinding_resource_name(
                    kafka_admin_DescribeAclsResult_get_binding(result, 0)
                ))
                .to_str(),
                Ok("z-topic")
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_AclBinding_resource_name(
                    kafka_admin_DescribeAclsResult_get_binding(result, 1)
                ))
                .to_str(),
                Ok("a-topic")
            );
            assert!(kafka_admin_DescribeAclsResult_get_binding(result, 2).is_null());
            assert!(kafka_admin_DescribeAclsResult_get_binding(result, -1).is_null());
            kafka_admin_DescribeAclsResult_destroy(result);
        }
    }

    #[test]
    fn delete_acls_result_separates_a_filter_failure_from_a_per_acl_failure() {
        // Three filters exercising all three outcomes Java can report: the
        // filter's own future failing, a matched ACL that could not be deleted,
        // and a matched ACL that was.
        let deleted = acl_binding("deleted-topic", "User:alice");
        let matched_but_failed = acl_binding("stuck-topic", "User:bob");

        fn filter(name: &str) -> AclBindingFilter {
            AclBindingFilter::new(
                ResourcePatternFilter::new(ResourceType::Topic, Some(name.to_string()), PatternType::Literal),
                AccessControlEntryFilter::any(),
            )
        }
        let outcomes: DeleteAclsOutcomes = HashMap::from([
            (
                filter("a-filter"),
                Ok(FilterResults::new(vec![
                    FilterResult::new(Some(deleted.clone()), None),
                    FilterResult::new(None, Some(KafkaError::new(Errors::SecurityDisabled))),
                ])),
            ),
            (
                filter("m-filter"),
                Ok(FilterResults::new(vec![FilterResult::new(Some(matched_but_failed), None)])),
            ),
            (filter("z-filter"), Err(KafkaError::new(Errors::ClusterAuthorizationFailed))),
        ]);
        let result = box_delete_acls_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_DeleteAclsResult_count(result), 3);

            // Sorted by the filter's fields, so "a-filter" is index 0.
            let f0 = kafka_admin_DeleteAclsResult_get_filter(result, 0);
            assert_eq!(
                CStr::from_ptr(kafka_common_AclBindingFilter_resource_name(f0)).to_str(),
                Ok("a-filter")
            );
            assert!(kafka_admin_DeleteAclsResult_get_error(result, 0).is_null());
            assert_eq!(kafka_admin_DeleteAclsResult_get_result_count(result, 0), 2);
            // Entry 0: a binding, no exception.
            assert_eq!(
                CStr::from_ptr(kafka_common_AclBinding_resource_name(kafka_admin_DeleteAclsResult_get_binding(
                    result, 0, 0
                )))
                .to_str(),
                Ok("deleted-topic")
            );
            assert!(kafka_admin_DeleteAclsResult_get_result_error(result, 0, 0).is_null());
            // Entry 1: an exception, no binding. The two are complementary.
            assert!(kafka_admin_DeleteAclsResult_get_binding(result, 0, 1).is_null());
            assert_eq!(
                common::kafka_common_KafkaError_code(kafka_admin_DeleteAclsResult_get_result_error(result, 0, 1)),
                Errors::SecurityDisabled.code() as i32
            );

            // Filter 2 failed outright: its error is set and it has no results,
            // which is a different thing from a filter that matched nothing.
            let f2 = kafka_admin_DeleteAclsResult_get_filter(result, 2);
            assert_eq!(
                CStr::from_ptr(kafka_common_AclBindingFilter_resource_name(f2)).to_str(),
                Ok("z-filter")
            );
            assert_eq!(
                common::kafka_common_KafkaError_code(kafka_admin_DeleteAclsResult_get_error(result, 2)),
                Errors::ClusterAuthorizationFailed.code() as i32
            );
            assert_eq!(kafka_admin_DeleteAclsResult_get_result_count(result, 2), 0);

            // Out of range on either index.
            assert_eq!(kafka_admin_DeleteAclsResult_get_result_count(result, 3), 0);
            assert_eq!(kafka_admin_DeleteAclsResult_get_result_count(result, -1), 0);
            assert!(kafka_admin_DeleteAclsResult_get_filter(result, 3).is_null());
            assert!(kafka_admin_DeleteAclsResult_get_binding(result, 0, 2).is_null());
            assert!(kafka_admin_DeleteAclsResult_get_binding(result, 0, -1).is_null());
            assert!(kafka_admin_DeleteAclsResult_get_result_error(result, 5, 0).is_null());
            kafka_admin_DeleteAclsResult_destroy(result);
        }
    }

    // -- Client-quota request marshaling -------------------------------------

    #[test]
    fn read_client_quota_filter_maps_each_wire_match_type_to_its_own_arm() {
        use crate::common::quota::ClientQuotaMatch;
        let (_types, type_ptrs) = c_array(&["user", "client-id", "ip"]);
        let (_names, name_ptrs) = c_array_opt(&[Some("alice"), None, None]);
        let match_types = [
            i32::from(MATCH_TYPE_EXACT),
            i32::from(MATCH_TYPE_DEFAULT),
            i32::from(MATCH_TYPE_SPECIFIED),
        ];
        let filter =
            unsafe { read_client_quota_filter(type_ptrs.as_ptr(), match_types.as_ptr(), name_ptrs.as_ptr(), 3, false) }
                .expect("valid filter");

        assert!(!filter.strict());
        let components = filter.components();
        assert_eq!(components.len(), 3);
        assert_eq!(components[0].entity_type(), "user");
        assert_eq!(components[0].match_spec(), &ClientQuotaMatch::Exact("alice".to_string()));
        assert_eq!(components[1].entity_type(), "client-id");
        // DEFAULT and SPECIFIED both carry no name; the discriminant is the
        // only thing separating them, and conflating them would change both
        // equality and the wire match-type byte.
        assert_eq!(components[1].match_spec(), &ClientQuotaMatch::Default);
        assert_eq!(components[2].entity_type(), "ip");
        assert_eq!(components[2].match_spec(), &ClientQuotaMatch::Any);
        assert_ne!(components[1].match_spec(), components[2].match_spec());
    }

    #[test]
    fn read_client_quota_filter_honours_strict_and_the_no_component_case() {
        let (_types, type_ptrs) = c_array(&["user"]);
        let (_names, name_ptrs) = c_array_opt(&[Some("alice")]);
        let exact = [i32::from(MATCH_TYPE_EXACT)];

        let strict =
            unsafe { read_client_quota_filter(type_ptrs.as_ptr(), exact.as_ptr(), name_ptrs.as_ptr(), 1, true) }
                .expect("valid filter");
        assert!(strict.strict());
        assert_eq!(strict.components().len(), 1);

        // No components, not strict: Java's `ClientQuotaFilter.all()`.
        let all = unsafe { read_client_quota_filter(std::ptr::null(), std::ptr::null(), std::ptr::null(), 0, false) }
            .expect("valid filter");
        assert_eq!(all, ClientQuotaFilter::all());
        // No components, strict: `containsOnly([])`, a different filter.
        let none = unsafe { read_client_quota_filter(std::ptr::null(), std::ptr::null(), std::ptr::null(), 0, true) }
            .expect("valid filter");
        assert_eq!(none, ClientQuotaFilter::contains_only(Vec::new()));
        assert_ne!(all, none);
    }

    #[test]
    fn read_client_quota_filter_rejects_a_bad_match_type_or_a_nameless_exact() {
        let (_types, type_ptrs) = c_array(&["user"]);
        let (_names, name_ptrs) = c_array_opt(&[None]);
        let err = unsafe {
            read_client_quota_filter(
                type_ptrs.as_ptr(),
                [i32::from(MATCH_TYPE_EXACT)].as_ptr(),
                name_ptrs.as_ptr(),
                1,
                false,
            )
        }
        .expect_err("rejected");
        assert_eq!(
            err.message(),
            "quota filter component at index 0 has match type EXACT but no match name"
        );

        let err =
            unsafe { read_client_quota_filter(type_ptrs.as_ptr(), [7i32].as_ptr(), name_ptrs.as_ptr(), 1, false) }
                .expect_err("rejected");
        assert_eq!(err.message(), "quota filter component at index 0 has unknown match type 7");
    }

    #[test]
    fn read_client_quota_alterations_maps_both_ragged_levels_onto_their_own_rows() {
        let (_t0, t0) = c_array(&["user", "client-id"]);
        let (_t1, t1) = c_array(&["ip"]);
        let (_n0, n0) = c_array_opt(&[Some("alice"), None]);
        let (_n1, n1) = c_array_opt(&[Some("10.0.0.1")]);
        let (_k0, k0) = c_array(&["producer_byte_rate"]);
        let (_k1, k1) = c_array(&["consumer_byte_rate", "request_percentage"]);

        let entity_types = [t0.as_ptr(), t1.as_ptr()];
        let entity_names = [n0.as_ptr(), n1.as_ptr()];
        let entity_counts = [2i32, 1];
        let op_keys = [k0.as_ptr(), k1.as_ptr()];
        let v0 = [1024.0f64];
        let v1 = [0.0f64, 50.0];
        let op_values = [v0.as_ptr(), v1.as_ptr()];
        // Row 1 op 0 has no value: Java's `Op(key, null)`, i.e. remove. Its
        // slot in `op_values` holds 0.0, a perfectly legal quota value, which
        // is exactly why the flag rather than a sentinel carries the meaning.
        //
        // The entity counts (2, 1) and op counts (1, 2) are deliberately
        // different per row, so swapping the two `*const i32` count arrays is
        // visible rather than a no-op.
        let h0 = [true];
        let h1 = [false, true];
        let op_has_values = [h0.as_ptr(), h1.as_ptr()];
        let op_counts = [1i32, 2];

        let alterations = unsafe {
            read_client_quota_alterations(
                entity_types.as_ptr(),
                entity_names.as_ptr(),
                entity_counts.as_ptr(),
                op_keys.as_ptr(),
                op_values.as_ptr(),
                op_has_values.as_ptr(),
                op_counts.as_ptr(),
                2,
            )
        }
        .expect("valid alterations");

        assert_eq!(alterations.len(), 2);
        let first = &alterations[0];
        assert_eq!(first.entity().entries().len(), 2);
        assert_eq!(first.entity().entries().get("user"), Some(&Some("alice".to_string())));
        // A null name is the built-in DEFAULT entity for that type, not an
        // absent entry and not the empty name.
        assert_eq!(first.entity().entries().get("client-id"), Some(&None));
        assert_eq!(first.ops().len(), 1);
        assert_eq!(first.ops()[0].key(), "producer_byte_rate");
        assert_eq!(first.ops()[0].value(), Some(1024.0));

        let second = &alterations[1];
        assert_eq!(second.entity().entries().get("ip"), Some(&Some("10.0.0.1".to_string())));
        assert_eq!(second.ops().len(), 2);
        assert_eq!(second.ops()[0].key(), "consumer_byte_rate");
        assert_eq!(second.ops()[0].value(), None);
        assert_eq!(second.ops()[1].key(), "request_percentage");
        assert_eq!(second.ops()[1].value(), Some(50.0));
    }

    #[test]
    fn read_client_quota_alterations_rejects_duplicate_entity_types_and_entities() {
        let (_dup, dup) = c_array(&["user", "user"]);
        let (_names, names) = c_array_opt(&[Some("alice"), Some("bob")]);
        let entity_types = [dup.as_ptr()];
        let entity_names = [names.as_ptr()];
        let err = unsafe {
            read_client_quota_alterations(
                entity_types.as_ptr(),
                entity_names.as_ptr(),
                [2i32].as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                1,
            )
        }
        .expect_err("rejected");
        assert_eq!(err.message(), "quota alteration at index 0 repeats entity type `user`");

        // The same entity twice across two alterations. Java accepts it and
        // sends both; this layer rejects because the flat C result could not
        // attribute the one surviving outcome to either row.
        let (_t, t) = c_array(&["user"]);
        let (_n, n) = c_array_opt(&[Some("alice")]);
        let entity_types = [t.as_ptr(), t.as_ptr()];
        let entity_names = [n.as_ptr(), n.as_ptr()];
        let err = unsafe {
            read_client_quota_alterations(
                entity_types.as_ptr(),
                entity_names.as_ptr(),
                [1i32, 1].as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                2,
            )
        }
        .expect_err("rejected");
        assert_eq!(
            err.message(),
            "quota alteration at index 1 repeats an entity already altered by an earlier entry"
        );
    }

    // -- Client-quota result flattening --------------------------------------

    #[test]
    fn client_quota_entity_distinguishes_the_default_entity_from_the_empty_name() {
        let inner = ClientQuotaEntityInner::new(&quota_entity(&[
            ("user", None),
            ("client-id", Some("")),
            ("ip", Some("10.0.0.1")),
        ]));
        let e = inner.as_ptr();
        unsafe {
            assert_eq!(kafka_common_ClientQuotaEntity_entry_count(e), 3);
            // Sorted by entity type: client-id, ip, user.
            assert_eq!(
                CStr::from_ptr(kafka_common_ClientQuotaEntity_get_entry_type(e, 0)).to_str(),
                Ok("client-id")
            );
            // Present but empty: a pointer to "", not null.
            let empty = kafka_common_ClientQuotaEntity_get_entry_name(e, 0);
            assert!(!empty.is_null());
            assert_eq!(CStr::from_ptr(empty).to_str(), Ok(""));

            assert_eq!(
                CStr::from_ptr(kafka_common_ClientQuotaEntity_get_entry_type(e, 1)).to_str(),
                Ok("ip")
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_ClientQuotaEntity_get_entry_name(e, 1)).to_str(),
                Ok("10.0.0.1")
            );

            assert_eq!(
                CStr::from_ptr(kafka_common_ClientQuotaEntity_get_entry_type(e, 2)).to_str(),
                Ok("user")
            );
            // The default entity: a null name at an in-range index.
            assert!(kafka_common_ClientQuotaEntity_get_entry_name(e, 2).is_null());

            assert!(kafka_common_ClientQuotaEntity_get_entry_type(e, 3).is_null());
            assert!(kafka_common_ClientQuotaEntity_get_entry_name(e, 3).is_null());
            assert!(kafka_common_ClientQuotaEntity_get_entry_type(e, -1).is_null());
        }
    }

    #[test]
    fn describe_client_quotas_result_flattens_the_nested_quota_map() {
        let outcome: DescribeClientQuotasOutcome = HashMap::from([
            (
                quota_entity(&[("user", Some("alice"))]),
                HashMap::from([
                    ("producer_byte_rate".to_string(), 1024.0),
                    ("consumer_byte_rate".to_string(), 2048.0),
                ]),
            ),
            (quota_entity(&[("user", Some("bob"))]), HashMap::new()),
        ]);
        let result = box_describe_client_quotas_result(outcome);
        unsafe {
            assert_eq!(kafka_admin_DescribeClientQuotasResult_count(result), 2);

            // Entities sorted by their (type, name) pairs: alice before bob.
            let alice = kafka_admin_DescribeClientQuotasResult_get_entity(result, 0);
            assert_eq!(
                CStr::from_ptr(kafka_common_ClientQuotaEntity_get_entry_name(alice, 0)).to_str(),
                Ok("alice")
            );
            assert_eq!(kafka_admin_DescribeClientQuotasResult_get_quota_count(result, 0), 2);
            // Quota keys sorted: consumer_byte_rate before producer_byte_rate.
            assert_eq!(
                CStr::from_ptr(kafka_admin_DescribeClientQuotasResult_get_quota_key(result, 0, 0)).to_str(),
                Ok("consumer_byte_rate")
            );
            let mut value = 0.0f64;
            assert!(kafka_admin_DescribeClientQuotasResult_get_quota_value(result, 0, 0, &mut value));
            assert_eq!(value, 2048.0);
            assert!(kafka_admin_DescribeClientQuotasResult_get_quota_value(result, 0, 1, &mut value));
            assert_eq!(value, 1024.0);

            // An entity with no quota values at all.
            assert_eq!(kafka_admin_DescribeClientQuotasResult_get_quota_count(result, 1), 0);

            // Out of range: the value accessor reports false and leaves `out`
            // untouched, because no `double` sentinel could be unambiguous.
            value = -12.5;
            assert!(!kafka_admin_DescribeClientQuotasResult_get_quota_value(
                result, 0, 2, &mut value
            ));
            assert_eq!(value, -12.5);
            assert!(!kafka_admin_DescribeClientQuotasResult_get_quota_value(
                result, 9, 0, &mut value
            ));
            assert!(!kafka_admin_DescribeClientQuotasResult_get_quota_value(
                result, -1, 0, &mut value
            ));
            assert!(!kafka_admin_DescribeClientQuotasResult_get_quota_value(
                result, 0, -1, &mut value
            ));
            assert_eq!(value, -12.5);
            assert!(kafka_admin_DescribeClientQuotasResult_get_quota_key(result, 5, 0).is_null());
            assert!(kafka_admin_DescribeClientQuotasResult_get_entity(result, 2).is_null());
            assert_eq!(kafka_admin_DescribeClientQuotasResult_get_quota_count(result, -1), 0);
            kafka_admin_DescribeClientQuotasResult_destroy(result);
        }
    }

    #[test]
    fn alter_client_quotas_result_reports_success_as_a_null_error() {
        let outcomes: AlterClientQuotasOutcomes = HashMap::from([
            (quota_entity(&[("user", Some("alice"))]), Ok(())),
            (
                quota_entity(&[("user", Some("bob"))]),
                Err(KafkaError::new(Errors::InvalidRequest)),
            ),
        ]);
        let result = box_alter_client_quotas_result(outcomes);
        unsafe {
            assert_eq!(kafka_admin_AlterClientQuotasResult_count(result), 2);
            let alice = kafka_admin_AlterClientQuotasResult_get_entity(result, 0);
            assert_eq!(
                CStr::from_ptr(kafka_common_ClientQuotaEntity_get_entry_name(alice, 0)).to_str(),
                Ok("alice")
            );
            assert!(kafka_admin_AlterClientQuotasResult_get_error(result, 0).is_null());
            assert_eq!(
                common::kafka_common_KafkaError_code(kafka_admin_AlterClientQuotasResult_get_error(result, 1)),
                Errors::InvalidRequest.code() as i32
            );
            assert!(kafka_admin_AlterClientQuotasResult_get_entity(result, 2).is_null());
            assert!(kafka_admin_AlterClientQuotasResult_get_entity(result, -1).is_null());
            assert!(kafka_admin_AlterClientQuotasResult_get_error(result, -1).is_null());
            kafka_admin_AlterClientQuotasResult_destroy(result);
        }
    }
}
