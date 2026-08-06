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
//!   argument marshaling fails (for example an unparseable base64 topic id
//!   passed to `kafka_admin_AdminClient_delete_topics_by_ids_async` /
//!   `_describe_topics_by_ids_async`). This is plain bad input, not only a
//!   programming error, so a caller must not assume the entry point has returned
//!   by the time the callback runs.
//! - On a **tokio worker thread**, if the dispatcher has already been torn down
//!   when the result arrives (only reachable while the handle is being
//!   destroyed).
//!
//! Firing inline keeps the callback obligation total — no path drops it — but it
//! means a caller must not hold a lock across `..._async(...)` and re-acquire it
//! in the callback, and must publish anything the callback needs (including
//! `user_data`) *before* the submit rather than after it.
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

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::Mutex;
use std::time::Duration;

use crate::admin::{
    Admin, AdminClientConfig, Config, CreateTopicsOptions, DeleteTopicsOptions, DescribeTopicsOptions,
    ListTopicsOptions, MockAdminClient, NewTopic, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
use crate::common::{KafkaError, KafkaFuture, Node, TopicCollection, TopicPartitionInfo, Uuid};

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
/// (synchronous). Pass a negative `timeout_ms` for Java's no-argument
/// `close()` semantics (wait indefinitely).
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
/// The callback fires exactly once: normally on the handle's dispatcher thread,
/// but **synchronously on the calling thread** if `admin` is NULL (see the
/// module-level *Callback thread* section).
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
/// calling thread (module docs, *Callback thread*).
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

/// Async dispatch for a **value-returning** admin RPC.
///
/// `submit` runs on the **calling** thread (inside the runtime context, so the
/// RPC may `tokio::spawn` or notify the background task) and returns the
/// [`KafkaFuture`] to await — normally `KafkaFuture::join_map_results(...)` over
/// the `*Result`'s per-key futures. This mirrors Java, where
/// `Admin.createTopics(...)` enqueues the request on the caller's thread and
/// returns immediately.
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
        let result = future.get().await;
        let job: CompletionJob = Box::new(move || complete(result, ud.into_ptr()));
        enqueue_or_run_inline(&tx, job);
    });
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
unsafe fn admin_sync_value_op<T, S>(admin: *const kafka_admin_AdminClient_t, submit: S) -> Result<T, KafkaError>
where
    T: Clone + Send + Sync + 'static,
    S: FnOnce(&dyn Admin) -> Result<KafkaFuture<T>, KafkaError>,
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
    h.runtime.block_on(future.get())
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
// Value types
//
// Each handle owns its strings in cached `CString`s and its nested entries in
// stable (boxed / vec-backed) allocations, so getters can return borrowed
// pointers valid until the owning result handle is destroyed.
// ---------------------------------------------------------------------------

/// A `ConfigEntry` flattened for C.
///
/// `createTopics` is the only B1 RPC that carries configs, and the broker's
/// `CreateTopicsResponse` populates only these fields. The full `Config` /
/// `ConfigEntry` C surface (synonyms, config type, documentation, config source)
/// arrives with the `describeConfigs` slice, where it is the primary payload.
struct ConfigEntryC {
    name_c: CString,
    /// `None` for a null config value (Java's `ConfigEntry.value()` is nullable).
    value_c: Option<CString>,
    is_default: bool,
    is_sensitive: bool,
    is_read_only: bool,
}

impl ConfigEntryC {
    /// Flattens every entry of a [`Config`], sorted by name for stable indexing.
    fn from_config(config: &Config) -> Vec<ConfigEntryC> {
        let mut entries: Vec<ConfigEntryC> = config
            .entries()
            .map(|entry| ConfigEntryC {
                name_c: to_cstring(entry.name()),
                value_c: entry.value().map(to_cstring),
                is_default: entry.is_default(),
                is_sensitive: entry.is_sensitive(),
                is_read_only: entry.is_read_only(),
            })
            .collect();
        entries.sort_by(|a, b| a.name_c.cmp(&b.name_c));
        entries
    }
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
/// The callback fires exactly once: normally on the handle's dispatcher thread,
/// but **synchronously on the calling thread** if `admin` is NULL (see the
/// module-level *Callback thread* section).
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
/// An unparseable or NULL id fires the callback with that error **synchronously,
/// on the calling thread, before this function returns**, because the RPC is
/// never submitted (module docs, *Callback thread*).
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
/// An unparseable or NULL id fires the callback with that error **synchronously,
/// on the calling thread, before this function returns**, because the RPC is
/// never submitted (module docs, *Callback thread*).
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
// MockAdminClient drivers
//
// Mock-only configuration methods (inherent on `MockAdminClient`, not part of
// the `Admin` trait), mirroring the consumer FFI's `kafka_consumer_MockConsumer_*`
// drivers. They take the same handle and return an error if it does not wrap a
// mock.
//
// Only the drivers the B1 topic RPCs can exercise are exposed. `add_topic` and
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
