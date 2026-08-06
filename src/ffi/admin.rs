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
//!   delivers the same flattened result handle through a C callback fired on the
//!   per-handle dispatcher thread.
//!
//! In both cases the RPC method itself is invoked on the **calling** thread, so
//! the request is enqueued as promptly as in Java; only the awaiting of the
//! per-key futures moves to the tokio runtime.
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

use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;
use std::time::Duration;

use crate::admin::{Admin, AdminClientConfig, MockAdminClient};
use crate::common::KafkaError;

use super::common::{
    self, CompletionJob, OperationCallbackFn, OperationCallbackTarget, OperationCompletion, box_error,
    enqueue_or_run_inline, init_default_logger, kafka_common_KafkaError_t,
};

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
    #[allow(dead_code)]
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
/// - `num_brokers`: Number of brokers to simulate. Values below 0 are treated
///   as 0.
///
/// # Returns
///
/// A non-null admin-client handle, or null if the tokio runtime cannot be
/// created. The caller must free it with [`kafka_admin_AdminClient_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_MockAdminClient_new(num_brokers: i32) -> *mut kafka_admin_AdminClient_t {
    init_default_logger();
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => return std::ptr::null_mut(),
    };
    let mock = MockAdminClient::create(num_brokers.max(0));
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
/// The callback fires exactly once, on the handle's dispatcher thread.
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
/// `close`). `op` runs on the runtime; the callback fires on the dispatcher
/// thread.
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
