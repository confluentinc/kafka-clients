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

//! C bindings for `org.apache.kafka.clients.admin` (CLAUDE.md §4).
//!
//! # Shape
//!
//! `kafka_admin_Admin_t` is the `Admin` trait: one invoker per method,
//! taking the interface handle first. `kafka_admin_AdminClient_create`
//! returns an owned `Admin_t` (Rust's `Box<dyn Admin>`), freed with
//! `kafka_admin_Admin_destroy`; `kafka_admin_MockAdminClient_create` returns
//! the class handle [`kafka_admin_MockAdminClient_t`], freed with its own
//! `_destroy` and reached as an `Admin` through
//! `kafka_admin_MockAdminClient__as_Admin`, a borrowed view valid until the
//! class handle is destroyed and never passed to `Admin_destroy`
//! (CLAUDE.md §4 rule 3).
//!
//! # Results are Java-faithful: per-key `kafka_common_KafkaFuture_t`
//!
//! Every RPC is a plain, non-blocking function returning the Java `*Result`
//! handle (`admin-client.md` §1): the network I/O happens on the client's
//! background task and the caller awaits the `kafka_common_KafkaFuture_t`s
//! the result exposes, exactly as Java's `KafkaFuture.get()`. A result
//! exposes what Java exposes: `values()` as a `kafka_Map_t` of owned
//! futures, `all()`, and the typed refinements; each documents the element
//! type behind the future's `void *`. The futures are bound to the client's
//! runtime, so their blocking `get` drives the client from the calling
//! thread and their `get_cb` completions are queued on the client's callback
//! vector. A result handle may be destroyed before the futures taken from it
//! resolve; the futures keep what they need alive.
//!
//! # Blocking and `_cb` entry points (§4 rule 5)
//!
//! `close` and `close_with_timeout` are the only `async` methods of the Rust
//! `Admin` trait, so they are the only ones with a `_cb` twin. Their
//! completion carries no value and no error (Java's `close` is `void` and
//! throws nothing), so the `_cb_t` takes only the opaque pointer.
//! `kafka_admin_Admin_execute_callbacks` runs the queued callbacks serially
//! on the calling thread and `kafka_admin_Admin_set_callbacks_notify`
//! installs the hook fired once each time the vector goes from empty to
//! non-empty. A blocking entry point drives the runtime from the calling
//! thread with `block_on`, so it must not be called from a callback running
//! on a runtime worker; the `_cb` forms are always safe to call from there.
//!
//! # Destroy
//!
//! `_destroy` waits for the tasks spawned by `_cb` calls, runs the
//! still-pending callbacks so each fires exactly once (§4 rule 5), drops the
//! client and shuts the runtime down. It does not `close` the client: as in
//! Rust, a `KafkaAdminClient` dropped without `close` stops its background
//! task without waiting for in-flight work, so a caller that wants a
//! graceful shutdown calls `close` first.

#![expect(non_camel_case_types)]

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::admin::{Admin, MockAdminClient};
use crate::common::{Error, KafkaFuture};
use crate::ffi::callback_queue::{CallbackQueue, SendPtr};
use crate::ffi::common::{box_error, init_default_logger, kafka_common_Error_t};
use crate::ffi::kafka_future::{
    FfiFuture, FfiValue, block_on, box_future, kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_t,
    map_future, map_future_handle, map_void_future,
};
use crate::ffi::util::{
    ElementDestroy, KeyEq, box_map, box_string_keyed_map, destroy_string_element, into_c_string, kafka_List_destroy,
    kafka_List_t, kafka_Map_destroy, kafka_Map_t,
};

pub(crate) mod abort_transaction_result;
pub(crate) mod abort_transaction_spec;
pub(crate) mod admin_client;
pub(crate) mod admin_client_config;
pub(crate) mod alter_client_quotas_result;
pub(crate) mod alter_config_op;
pub(crate) mod alter_configs_result;
pub(crate) mod alter_consumer_group_offsets_result;
pub(crate) mod alter_partition_reassignments_result;
pub(crate) mod alter_replica_log_dirs_result;
pub(crate) mod alter_user_scram_credentials_result;
pub(crate) mod classic_group_description;
pub(crate) mod config;
pub(crate) mod config_entry;
pub(crate) mod consumer_group_description;
pub(crate) mod create_acls_result;
pub(crate) mod create_delegation_token_result;
pub(crate) mod create_partitions_result;
pub(crate) mod create_topics_result;
pub(crate) mod delete_acls_result;
pub(crate) mod delete_consumer_group_offsets_result;
pub(crate) mod delete_consumer_groups_result;
pub(crate) mod delete_records_result;
pub(crate) mod delete_topics_result;
pub(crate) mod deleted_records;
pub(crate) mod describe_acls_result;
pub(crate) mod describe_classic_groups_result;
pub(crate) mod describe_client_quotas_result;
pub(crate) mod describe_cluster_result;
pub(crate) mod describe_configs_result;
pub(crate) mod describe_consumer_groups_result;
pub(crate) mod describe_delegation_token_result;
pub(crate) mod describe_features_result;
pub(crate) mod describe_log_dirs_result;
pub(crate) mod describe_producers_result;
pub(crate) mod describe_replica_log_dirs_result;
pub(crate) mod describe_topics_result;
pub(crate) mod describe_transactions_result;
pub(crate) mod describe_user_scram_credentials_result;
pub(crate) mod elect_leaders_result;
pub(crate) mod expire_delegation_token_result;
pub(crate) mod feature_metadata;
pub(crate) mod feature_update;
pub(crate) mod fence_producers_result;
pub(crate) mod finalized_version_range;
pub(crate) mod group_listing;
pub(crate) mod list_config_resources_result;
pub(crate) mod list_consumer_group_offsets_result;
pub(crate) mod list_consumer_group_offsets_spec;
pub(crate) mod list_groups_result;
pub(crate) mod list_offsets_result;
pub(crate) mod list_partition_reassignments_result;
pub(crate) mod list_topics_result;
pub(crate) mod list_transactions_result;
pub(crate) mod log_dir_description;
pub(crate) mod member_assignment;
pub(crate) mod member_description;
pub(crate) mod member_to_remove;
pub(crate) mod mock_admin_client;
pub(crate) mod new_partition_reassignment;
pub(crate) mod new_partitions;
pub(crate) mod new_topic;
pub(crate) mod offset_spec;
pub(crate) mod options;
pub(crate) mod partition_reassignment;
pub(crate) mod producer_state;
pub(crate) mod records_to_delete;
pub(crate) mod remove_members_from_consumer_group_result;
pub(crate) mod renew_delegation_token_result;
pub(crate) mod replica_info;
pub(crate) mod rpc;
pub(crate) mod scram_credential_info;
pub(crate) mod scram_mechanism;
pub(crate) mod supported_version_range;
pub(crate) mod terminate_transaction_result;
pub(crate) mod topic_description;
pub(crate) mod topic_listing;
pub(crate) mod transaction_description;
pub(crate) mod transaction_listing;
pub(crate) mod transaction_state;
pub(crate) mod update_features_result;
pub(crate) mod user_scram_credential_alteration;
pub(crate) mod user_scram_credential_deletion;
pub(crate) mod user_scram_credential_upsertion;
pub(crate) mod user_scram_credentials_description;

/// Opaque handle to the `Admin` interface: owned when returned by
/// `kafka_admin_AdminClient_create` (freed with [`kafka_admin_Admin_destroy`]),
/// a borrowed view when obtained from `kafka_admin_MockAdminClient__as_Admin`
/// (valid until that class handle is destroyed).
#[repr(C)]
pub struct kafka_admin_Admin_t {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------------
// Client state shared by the handle, its tasks and the futures it produced
// ---------------------------------------------------------------------------

/// The state a `kafka_admin_Admin_t` points at, shared through an `Arc`
/// between the class handle and every task it spawned, so a task never
/// outlives what it uses.
pub(crate) struct Client {
    admin: Arc<dyn Admin>,
    runtime: tokio::runtime::Handle,
    queue: Arc<CallbackQueue>,
    /// Tasks spawned by `_cb` calls, awaited by `destroy`.
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Client {
    /// Runs `f` against the `Admin` inside the runtime's context, so an
    /// RPC that spawns is driven by the client's runtime.
    pub(crate) fn call<R>(&self, f: impl FnOnce(&dyn Admin) -> R) -> R {
        let _enter = self.runtime.enter();
        f(&*self.admin)
    }

    /// The runtime and callback vector a future produced by this client is
    /// bound to.
    pub(crate) fn future_ctx(&self) -> FutureCtx {
        FutureCtx { runtime: Some(self.runtime.clone()), queue: Some(Arc::clone(&self.queue)) }
    }

    /// Spawns `future` on the client's runtime and remembers it for `destroy`.
    fn spawn<F>(self: &Arc<Self>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = self.runtime.spawn(future);
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    /// Drives `op` to completion on the calling thread.
    fn block_on<F>(&self, op: F) -> F::Output
    where
        F: Future,
    {
        block_on(Some(&self.runtime), op)
    }

    /// Runs `op` on the runtime and queues `cb(opaque)` once it completes.
    fn run_cb<F, Fut>(self: &Arc<Self>, cb: kafka_admin_Admin_close_cb_t, opaque: *mut c_void, op: F)
    where
        F: FnOnce(Arc<dyn Admin>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let opaque = SendPtr(opaque);
        let admin = Arc::clone(&self.admin);
        let queue = Arc::clone(&self.queue);
        self.spawn(async move {
            op(admin).await;
            queue.push(Box::new(move || unsafe { cb(opaque.get()) }));
        });
    }
}

/// The class handle behind an owned `kafka_admin_Admin_t` and behind
/// `kafka_admin_MockAdminClient_t`: the shared client state, the runtime
/// that drives it and, for the mock, the concrete client its inherent
/// methods need. `#[repr(C)]` with the client first, so the
/// `kafka_admin_Admin_t` view is the handle's own address.
#[repr(C)]
pub(crate) struct AdminClassHandle {
    /// The `kafka_admin_Admin_t` view is this field's address.
    client: Arc<Client>,
    runtime: tokio::runtime::Runtime,
    mock: Option<Arc<MockAdminClient>>,
}

impl AdminClassHandle {
    /// Builds the client with `make`, inside the context of a fresh
    /// multi-thread runtime (a `KafkaAdminClient` spawns its background
    /// task on construction), and wraps it with the runtime.
    pub(crate) fn new(make: impl FnOnce() -> Result<Box<dyn Admin>, Error>) -> Result<Box<Self>, Error> {
        Self::build(|| make().map(|admin| (Arc::from(admin), None)))
    }

    /// Builds a `MockAdminClient` with `make` and wraps it like [`Self::new`],
    /// keeping the concrete client for the mock's inherent methods.
    pub(crate) fn new_mock(make: impl FnOnce() -> Result<MockAdminClient, Error>) -> Result<Box<Self>, Error> {
        Self::build(|| {
            let mock = Arc::new(make()?);
            Ok((Arc::clone(&mock) as Arc<dyn Admin>, Some(mock)))
        })
    }

    fn build(
        make: impl FnOnce() -> Result<(Arc<dyn Admin>, Option<Arc<MockAdminClient>>), Error>,
    ) -> Result<Box<Self>, Error> {
        init_default_logger();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("kafka-admin-ffi")
            .build()
            .expect("tokio runtime");
        let (admin, mock) = {
            let _enter = runtime.enter();
            make()?
        };
        let client = Arc::new(Client {
            admin,
            runtime: runtime.handle().clone(),
            queue: Arc::new(CallbackQueue::new()),
            tasks: Mutex::new(Vec::new()),
        });
        Ok(Box::new(Self { client, runtime, mock }))
    }

    /// The `Admin` view of this handle (see the module docs).
    pub(crate) fn as_admin(&self) -> *const kafka_admin_Admin_t {
        &self.client as *const Arc<Client> as *const kafka_admin_Admin_t
    }

    /// The concrete mock, when this handle was built by [`Self::new_mock`].
    pub(crate) fn mock(&self) -> Option<&MockAdminClient> {
        self.mock.as_deref()
    }

    /// Awaits every spawned task, fires the pending callbacks and drops the
    /// client (see the module docs).
    pub(crate) fn destroy(self) {
        let Self { client, runtime, mock } = self;
        loop {
            let tasks: Vec<JoinHandle<()>> = std::mem::take(&mut *client.tasks.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            runtime.block_on(async {
                for task in tasks {
                    let _ = task.await;
                }
            });
        }
        client.queue.execute();
        // A `KafkaAdminClient` needs the runtime context to stop its
        // background task on drop; the runtime is shut down last.
        {
            let _enter = runtime.enter();
            drop(mock);
            drop(client);
        }
        runtime.shutdown_timeout(Duration::from_secs(5));
    }
}

/// The client behind an interface handle.
///
/// # Safety
///
/// `admin` must be a live handle or view.
pub(crate) unsafe fn client_ref<'a>(admin: *const kafka_admin_Admin_t) -> &'a Arc<Client> {
    unsafe { &*(admin as *const Arc<Client>) }
}

/// Delivers `result` through the error slot and `out`.
///
/// # Safety
///
/// `out` must be a valid slot.
pub(crate) unsafe fn out_slot<T, P>(
    result: Result<T, Error>,
    out: *mut P,
    to_value: impl FnOnce(T) -> P,
) -> *mut kafka_common_Error_t {
    match result {
        Ok(value) => {
            unsafe { *out = to_value(value) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// Translates a `Result<(), Error>` to the error slot.
pub(crate) fn error_slot(result: Result<(), Error>) -> *mut kafka_common_Error_t {
    result.err().map_or(std::ptr::null_mut(), box_error)
}

// ---------------------------------------------------------------------------
// Futures and results
// ---------------------------------------------------------------------------

/// The runtime and callback vector the futures handed out by a result are
/// bound to (see the module docs): a blocking `get` drives this runtime, a
/// `get_cb` completion is queued on this vector.
#[derive(Clone)]
pub(crate) struct FutureCtx {
    runtime: Option<tokio::runtime::Handle>,
    queue: Option<Arc<CallbackQueue>>,
}

impl FutureCtx {
    /// The context of a future that belongs to no client: a result built by
    /// C through a factory such as `kafka_admin_DescribeTopicsResult_by_topic_name`.
    pub(crate) fn detached() -> Self {
        Self { runtime: None, queue: None }
    }

    /// Hands an already-mapped future to C.
    pub(crate) fn boxed(&self, future: FfiFuture) -> *mut kafka_common_KafkaFuture_t {
        box_future(future, self.runtime.clone(), self.queue.clone())
    }

    /// A `KafkaFuture<Void>`: `get` delivers `NULL`.
    pub(crate) fn void_future(&self, future: &KafkaFuture<()>) -> *mut kafka_common_KafkaFuture_t {
        self.boxed(map_void_future(future))
    }

    /// A future resolving to the owned C handle `boxer` builds, freed with
    /// `destroy` (the handle type's `_destroy` as an [`ElementDestroy`]) when
    /// the last future sharing it is destroyed.
    pub(crate) fn handle_future<T, F>(
        &self,
        future: &KafkaFuture<T>,
        boxer: F,
        destroy: ElementDestroy,
    ) -> *mut kafka_common_KafkaFuture_t
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(T) -> *mut c_void + Send + Sync + 'static,
    {
        self.boxed(map_future_handle(future, boxer, destroy))
    }

    /// A future resolving to a scalar, delivered boxed as a `T *`
    /// (`int32_t *`, `int64_t *`, ...) owned by the future.
    pub(crate) fn value_future<T>(&self, future: &KafkaFuture<T>) -> *mut kafka_common_KafkaFuture_t
    where
        T: Clone + Send + Sync + 'static,
    {
        self.boxed(map_future(future))
    }

    /// A future resolving to a `String`, delivered as a `char *` owned by the
    /// future.
    pub(crate) fn string_future(&self, future: &KafkaFuture<String>) -> *mut kafka_common_KafkaFuture_t {
        self.boxed(future.then_apply(|s| FfiValue::handle(into_c_string(&s) as *mut c_void, destroy_string_element)))
    }

    /// Java's `Map<String, KafkaFuture<T>>`: an owned map of owned `char *`
    /// keys (compared by content in `kafka_Map_get`) to the owned futures
    /// `to_future` builds, sorted by key so the encoding is deterministic.
    pub(crate) fn string_keyed_future_map<'a, T, I, F>(&self, entries: I, to_future: F) -> *mut kafka_Map_t
    where
        T: Send + 'static,
        I: IntoIterator<Item = (&'a String, &'a KafkaFuture<T>)>,
        F: Fn(&FutureCtx, &KafkaFuture<T>) -> *mut kafka_common_KafkaFuture_t,
    {
        let sorted: BTreeMap<&String, &KafkaFuture<T>> = entries.into_iter().collect();
        box_string_keyed_map(
            sorted.into_iter().map(|(k, f)| (k, to_future(self, f) as *mut c_void)),
            Some(destroy_future_element),
        )
    }

    /// Java's `Map<K, KafkaFuture<T>>` for a non-string `K`: an owned map of
    /// the owned keys `key_boxer` builds (freed with `key_destroy`, compared
    /// with `key_eq` in `kafka_Map_get`) to the owned futures `to_future`
    /// builds. `entries` should already be in a deterministic order.
    pub(crate) fn keyed_future_map<'a, K, T, I, KB, F>(
        &self,
        entries: I,
        key_boxer: KB,
        key_destroy: ElementDestroy,
        key_eq: KeyEq,
        to_future: F,
    ) -> *mut kafka_Map_t
    where
        K: 'a,
        T: Send + 'static,
        I: IntoIterator<Item = (&'a K, &'a KafkaFuture<T>)>,
        KB: Fn(&K) -> *mut c_void,
        F: Fn(&FutureCtx, &KafkaFuture<T>) -> *mut kafka_common_KafkaFuture_t,
    {
        let entries = entries
            .into_iter()
            .map(|(k, f)| (key_boxer(k), to_future(self, f) as *mut c_void))
            .collect();
        box_map(entries, Some(key_destroy), Some(destroy_future_element), Some(key_eq))
    }
}

/// Frees a `kafka_common_KafkaFuture_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned future handle.
pub(crate) unsafe fn destroy_future_element(element: *mut c_void) {
    unsafe { kafka_common_KafkaFuture_destroy(element as *mut kafka_common_KafkaFuture_t) }
}

/// Frees a `kafka_List_t *` element of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned list handle.
pub(crate) unsafe fn destroy_list_element(element: *mut c_void) {
    unsafe { kafka_List_destroy(element as *mut kafka_List_t) }
}

/// Frees a `kafka_Map_t *` element of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned map handle.
pub(crate) unsafe fn destroy_map_element(element: *mut c_void) {
    unsafe { kafka_Map_destroy(element as *mut kafka_Map_t) }
}

/// What a `kafka_admin_<Rpc>Result_t` handle points at: the Rust result and
/// the context its futures are bound to.
pub(crate) struct ResultHandle<R> {
    pub(crate) result: R,
    pub(crate) ctx: FutureCtx,
}

/// Hands `result` to C as an owned `P` handle (`kafka_admin_<Rpc>Result_t`).
pub(crate) fn box_result<R, P>(result: R, ctx: &FutureCtx) -> *mut P {
    Box::into_raw(Box::new(ResultHandle { result, ctx: ctx.clone() })) as *mut P
}

/// The result behind a handle built by [`box_result`].
///
/// # Safety
///
/// `handle` must be a live handle built by `box_result::<R, P>`.
pub(crate) unsafe fn result_ref<'a, R, P>(handle: *const P) -> &'a ResultHandle<R> {
    unsafe { &*(handle as *const ResultHandle<R>) }
}

/// Frees a handle built by [`box_result`]; null is a no-op.
///
/// # Safety
///
/// `handle` must be null or a handle built by `box_result::<R, P>` and not
/// yet destroyed.
pub(crate) unsafe fn destroy_result<R, P>(handle: *mut P) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle as *mut ResultHandle<R>) });
    }
}

// ---------------------------------------------------------------------------
// Callback typedefs
// ---------------------------------------------------------------------------

/// Completion of `close`: Java's `close()` is `void` and throws nothing, so
/// the callback carries only the opaque pointer.
pub type kafka_admin_Admin_close_cb_t = unsafe extern "C" fn(opaque: *mut c_void);
/// Completion of `close(Duration)`: Java's `close(Duration)` is `void` and
/// throws nothing, so the callback carries only the opaque pointer.
pub type kafka_admin_Admin_close_with_timeout_cb_t = unsafe extern "C" fn(opaque: *mut c_void);

/// The hook fired once each time the callback vector goes from empty to
/// non-empty, from a Rust task: it may only schedule a later
/// [`kafka_admin_Admin_execute_callbacks`], never run callbacks.
pub type kafka_admin_Admin_callbacks_notify_fn_t = unsafe extern "C" fn(opaque: *mut c_void);

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

/// `Admin.close()`, blocking: waits for the background task to finish its
/// in-flight work (`Long.MAX_VALUE` milliseconds, as Java).
///
/// # Safety
///
/// `self_` must be a live handle or view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close(self_: *const kafka_admin_Admin_t) {
    let client = unsafe { client_ref(self_) };
    client.block_on(client.admin.close());
}

/// `Admin.close()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle or view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close_cb(
    self_: *const kafka_admin_Admin_t,
    cb: kafka_admin_Admin_close_cb_t,
    opaque: *mut c_void,
) {
    unsafe { client_ref(self_) }.run_cb(cb, opaque, |admin| async move { admin.close().await });
}

/// `Admin.close(Duration timeout)`, blocking; `timeout` in milliseconds.
/// Java's `Duration` cannot be negative and the Rust `close_with_timeout`
/// takes an unsigned `Duration`, so a negative `timeout` is clamped to `0`
/// (close without waiting for in-flight work); the 365-day ceiling Java
/// applies is applied by the Rust client.
///
/// # Safety
///
/// `self_` must be a live handle or view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close_with_timeout(self_: *const kafka_admin_Admin_t, timeout: i64) {
    let client = unsafe { client_ref(self_) };
    client.block_on(client.admin.close_with_timeout(close_timeout(timeout)));
}

/// `Admin.close(Duration timeout)`, completion queued; `timeout` as in
/// [`kafka_admin_Admin_close_with_timeout`].
///
/// # Safety
///
/// `self_` must be a live handle or view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close_with_timeout_cb(
    self_: *const kafka_admin_Admin_t,
    timeout: i64,
    cb: kafka_admin_Admin_close_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let timeout = close_timeout(timeout);
    unsafe { client_ref(self_) }.run_cb(
        cb,
        opaque,
        move |admin| async move { admin.close_with_timeout(timeout).await },
    );
}

fn close_timeout(timeout: i64) -> Duration {
    Duration::from_millis(u64::try_from(timeout).unwrap_or(0))
}

// ---------------------------------------------------------------------------
// Callback pump
// ---------------------------------------------------------------------------

/// Runs the queued callbacks serially on the calling thread and returns how
/// many ran (CLAUDE.md §4 rule 5).
///
/// # Safety
///
/// `self_` must be a live handle or view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_execute_callbacks(self_: *const kafka_admin_Admin_t) -> i32 {
    unsafe { client_ref(self_) }.queue.execute()
}

/// Installs the hook fired once each time the callback vector goes from
/// empty to non-empty (see [`kafka_admin_Admin_callbacks_notify_fn_t`]).
///
/// # Safety
///
/// `self_` must be a live handle or view; `opaque` stays valid while the
/// hook is installed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_set_callbacks_notify(
    self_: *const kafka_admin_Admin_t,
    notify: kafka_admin_Admin_callbacks_notify_fn_t,
    opaque: *mut c_void,
) {
    unsafe { client_ref(self_) }.queue.set_notify(Some(notify), opaque);
}

// ---------------------------------------------------------------------------
// Destroy
// ---------------------------------------------------------------------------

/// Frees an `Admin_t` returned by `kafka_admin_AdminClient_create` (see the
/// module docs: waits for the `_cb` tasks, fires the pending callbacks, drops
/// the client); null is a no-op. Never pass a view obtained from an
/// `__as_Admin` function: that class handle has its own `_destroy`.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_destroy(self_: *mut kafka_admin_Admin_t) {
    if !self_.is_null() {
        unsafe { Box::from_raw(self_ as *mut AdminClassHandle) }.destroy();
    }
}
