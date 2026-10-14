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
//! `kafka_admin_Admin__execute_callbacks` runs the queued callbacks serially
//! on the calling thread and `kafka_admin_Admin__set_callbacks_notify`
//! installs the hook fired once each time the vector goes from empty to
//! non-empty. A blocking entry point drives the runtime from the calling
//! thread with `block_on`, so it must not be called from a callback running
//! on a runtime worker; the `_cb` forms are always safe to call from there.
//!
//! # Destroy
//!
//! `_destroy` waits for the tasks spawned by `_cb` calls (so a
//! `close_with_timeout_cb` still running keeps its grace period), then closes
//! the client with no grace period (Java's `close(Duration.ZERO)` deadline),
//! so the calls still pending fail with `kafka_common_Error_is_timeout_error`,
//! "The AdminClient thread has exited."; a caller that wants in-flight work to
//! finish calls `close` with a timeout first. It then runs the still-pending
//! callbacks so each fires exactly once (§4 rule 5), drops the client and
//! shuts the runtime down; the `kafka_common_KafkaFuture_get_cb` calls still
//! waiting on its futures fire last, once each (see `ffi::kafka_future`).
//!
//! The close joins the background task for a bounded time only: a task
//! inside a TCP connect the broker never answers cannot reach the step that
//! fails the pending calls, and is cancelled with the runtime. Its calls'
//! futures then never complete: a `get_cb` on one fires, and a blocking
//! `kafka_common_KafkaFuture_get` on one returns, with the
//! `kafka_common_Error_LOCAL_ILLEGAL_STATE` error "The client was destroyed
//! before the future completed".
//!
//! Called inside a tokio runtime (on a worker thread, or inside a
//! `block_on`), where it cannot wait, `_destroy` returns at once and all of
//! this happens on a new thread, the pending callbacks included. If that
//! thread cannot be started, the handle is leaked and an error logged.

#![expect(non_camel_case_types)]

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::admin::{Admin, MockAdminClient};
use crate::common::{Error, KafkaFuture};
use crate::ffi::callback_queue::{CallbackJob, CallbackQueue, SendPtr};
use crate::ffi::common::{
    catch_task_panic, deliver_panic_opaque, in_tokio_runtime, init_default_logger, spawn_callback_task, spawn_teardown,
};
use crate::ffi::ffi_guard;
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
pub(crate) mod unregister_controller_result;
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
    ///
    /// The registration has two phases, so nothing that can panic runs once the
    /// task exists: the `tasks` lock and the room for the handle are taken first,
    /// the spawn aborts the process if it panics ([`spawn_callback_task`]), and
    /// the push cannot reallocate. A `_cb` entry point's guard can then queue its
    /// completion on a panic without a task already spawned queueing a second
    /// one, and `destroy` never misses a task (§4 item 6 of
    /// `design/current/appsec-7665-4521-ffi-panic-guard.md`). A spawned task that
    /// takes this lock itself waits only for the push.
    fn spawn<F>(self: &Arc<Self>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        tasks.retain(|task| !task.is_finished());
        tasks.reserve(1);
        tasks.push(spawn_callback_task(&self.runtime, future));
    }

    /// Drives `op` to completion on the calling thread.
    fn block_on<F>(&self, op: F) -> F::Output
    where
        F: Future,
    {
        block_on(&self.runtime, op)
    }

    /// Runs `op` on the runtime and queues `cb(opaque)` once it completes.
    /// A panic in `op` is logged as the panic of `fn_name`, the entry point,
    /// and `cb(opaque)` is still queued: the callback carries no error.
    fn run_cb<F, Fut>(
        self: &Arc<Self>,
        fn_name: &'static str,
        cb: kafka_admin_Admin_close_cb_t,
        opaque: *mut c_void,
        op: F,
    ) where
        F: FnOnce(Arc<dyn Admin>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        #[cfg(test)]
        crate::ffi::common::fail_point::hit();
        let opaque = SendPtr(opaque);
        let admin = Arc::clone(&self.admin);
        let queue = Arc::clone(&self.queue);
        self.spawn(async move {
            // The error is already logged by `catch_task_panic`, and the
            // callback has no error parameter to carry it.
            let _ = catch_task_panic(fn_name, async {
                op(admin).await;
                Ok(())
            })
            .await;
            // SAFETY: `cb` and `opaque` are the pair the C caller passed to the `_cb` entry point
            // (`kafka_admin_Admin_close_cb` or `kafka_admin_Admin_close_with_timeout_cb`), passed
            // on unchanged and moved into this one task, which pushes this one job, so calling `cb`
            // with `opaque` is the completion the caller is waiting for. The job is a `FnOnce`, so
            // `cb` fires exactly once.
            queue.push(Box::new(move || unsafe { cb(opaque.get()) }));
        });
    }
}

/// How long `destroy` waits for the background task to exit, and then for
/// the runtime to stop.
const DESTROY_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

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

    /// Tears the handle down ([`Self::teardown`]) on the calling thread, or,
    /// called inside a tokio runtime, where `teardown`'s `block_on` would
    /// panic, on a new thread, returning at once (see [`spawn_teardown`]).
    pub(crate) fn destroy(self: Box<Self>) {
        if in_tokio_runtime() {
            /// # Safety
            ///
            /// `handle` must be the `Box::into_raw` of an `AdminClassHandle`
            /// whose only teardown this is.
            unsafe fn teardown(handle: usize) {
                // SAFETY: Per this function's `# Safety`, `handle` is the `Box::into_raw` of an
                // `AdminClassHandle` torn down only here, so taking it back frees it exactly once.
                unsafe { Box::from_raw(handle as *mut AdminClassHandle) }.teardown();
            }
            // SAFETY: `teardown`'s `# Safety` holds: the address is the `Box::into_raw` just taken,
            // and this is its only teardown, since the box is consumed here. `teardown` waits with
            // `block_on`, which is sound on the new thread, outside any runtime.
            unsafe { spawn_teardown("kafka-admin-destroy", Box::into_raw(self) as usize, teardown) };
        } else {
            self.teardown();
        }
    }

    /// Awaits every spawned task, closes the client with no grace period,
    /// fires the pending callbacks and drops the client (see the module
    /// docs), then stops the runtime, releasing the blocking `KafkaFuture_get`
    /// calls still waiting, and fires the completions of the
    /// `KafkaFuture_get_cb` calls still waiting, each exactly once.
    ///
    /// A panic caught earlier may have poisoned `tasks`; it is taken
    /// regardless, so the handle is still freed and every pending callback
    /// still fires exactly once.
    fn teardown(self) {
        let Self { client, runtime, mock } = self;
        // The `_cb` tasks first: they are the two `close` twins, each bounded by
        // its own timeout, so a `close_with_timeout_cb` the caller started keeps
        // its grace period instead of being cut short by the close below.
        loop {
            let tasks: Vec<JoinHandle<()>> =
                std::mem::take(&mut *client.tasks.lock().unwrap_or_else(PoisonError::into_inner));
            if tasks.is_empty() {
                break;
            }
            runtime.block_on(async {
                for task in tasks {
                    let _ = task.await;
                }
            });
        }
        // Shutting the runtime down below cancels the background task where it
        // stands, so a call still pending would never complete. Close with the
        // hard-shutdown deadline at once: the task's `finally`
        // (`AdminClientRunnable.run`, `KafkaAdminClient.java:1557-1562`) fails
        // every pending call with `TimeoutException("The AdminClient thread has
        // exited.")`. The zero close only installs the deadline (it does not
        // join, see `close_with_timeout`); the second keeps that earlier deadline
        // and joins the task, bounded. Both are no-ops after a `close` of the
        // caller's.
        runtime.block_on(async {
            client.admin.close_with_timeout(Duration::ZERO).await;
            client.admin.close_with_timeout(DESTROY_JOIN_TIMEOUT).await;
        });
        client.queue.execute();
        // A `KafkaAdminClient` needs the runtime context to stop its
        // background task on drop; the runtime is shut down last.
        let queue = Arc::clone(&client.queue);
        {
            let _enter = runtime.enter();
            drop(mock);
            drop(client);
        }
        // A background task the join above missed (inside a TCP connect no
        // shutdown deadline interrupts) is cancelled with the runtime, so the
        // futures of its pending calls never complete: release the blocking
        // `KafkaFuture_get` calls waiting on them, before the runtime's timers
        // stop under a `get` with a timeout (see `ffi::kafka_future`).
        queue.mark_client_stopped();
        runtime.shutdown_timeout(DESTROY_JOIN_TIMEOUT);
        // Shutting the runtime down dropped the `KafkaFuture_get_cb` tasks
        // still waiting, each queuing its completion as it went (see
        // `ffi::kafka_future`): run them, closing the queue first so a later
        // `get_cb` on a future of this client runs inline.
        queue.close();
        queue.execute();
    }
}

/// Where the guard of a `_cb` entry point delivers a caught panic: the
/// client's callbacks vector (see [`deliver_panic_opaque`]).
///
/// # Safety
///
/// `admin` must be a live handle or view, as [`client_ref`] describes; the
/// sink holds its own `Arc` to the client, so it may outlive the call.
unsafe fn completion_sink(admin: *const kafka_admin_Admin_t) -> impl FnOnce(CallbackJob) {
    // SAFETY: Per this function's `# Safety`, `admin` is a live handle or view, as `client_ref`
    // requires; the reference is used only to clone the `Arc`, so the sink does not borrow the
    // handle.
    let client = Arc::clone(unsafe { client_ref(admin) });
    move |job| client.queue.push(job)
}

/// The client behind an interface handle.
///
/// # Safety
///
/// `admin` must point at a live [`AdminClassHandle`], kept alive for `'a`:
/// a handle from `kafka_admin_AdminClient_create` or a
/// `kafka_admin_MockAdminClient__as_Admin` view (the same pointer, see
/// [`AdminClassHandle::as_admin`]).
pub(crate) unsafe fn client_ref<'a>(admin: *const kafka_admin_Admin_t) -> &'a Arc<Client> {
    // SAFETY: Per this function's `# Safety`, `admin` points at a live `AdminClassHandle` kept
    // alive for `'a`: the `Box<AdminClassHandle>` `kafka_admin_AdminClient_create` delivers is that
    // handle's address, and so is a `kafka_admin_MockAdminClient__as_Admin` view
    // (`AdminClassHandle::as_admin`). `AdminClassHandle` is `#[repr(C)]` with `client: Arc<Client>`
    // as its first field, so the address is that field's, and a shared reference to it is valid for
    // `'a`. Nothing takes a `&mut` to the handle while it is live: only its destroy takes it back,
    // after the handle's last use.
    unsafe { &*(admin as *const Arc<Client>) }
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
    ///
    /// # Safety
    ///
    /// The caller gives each handle `boxer` makes the guarantees
    /// [`map_future_handle`] asks of its caller (those of [`FfiValue::handle`]):
    /// `destroy` frees exactly that handle, being the destructor paired with
    /// the helper that made it (a `box_*` helper and its `_destroy`), and the
    /// handle may be freed from any thread, and read through its accessors from
    /// several threads at once. `boxer` may also return null, which is never
    /// freed.
    pub(crate) unsafe fn handle_future<T, F>(
        &self,
        future: &KafkaFuture<T>,
        boxer: F,
        destroy: ElementDestroy,
    ) -> *mut kafka_common_KafkaFuture_t
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(T) -> *mut c_void + Send + Sync + 'static,
    {
        // SAFETY: Per this function's `# Safety`, each pointer `boxer` returns is null or an owned
        // handle `destroy` frees exactly, which may be freed from any thread and read from several
        // threads at once, which is what `map_future_handle` requires.
        self.boxed(unsafe { map_future_handle(future, boxer, destroy) })
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
        self.boxed(future.then_apply(|s| {
            // SAFETY: `into_c_string` returns an owned `char *` that `destroy_string_element`, its
            // paired destructor, frees exactly, and nothing else frees it. A C string has no
            // thread affinity: it may be freed from any thread and read from several at once.
            Ok(unsafe { FfiValue::handle(into_c_string(&s) as *mut c_void, destroy_string_element) })
        }))
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
/// `element` must be an owned future handle not yet destroyed.
pub(crate) unsafe fn destroy_future_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned future handle not yet
    // destroyed, which is what `kafka_common_KafkaFuture_destroy` requires.
    unsafe { kafka_common_KafkaFuture_destroy(element as *mut kafka_common_KafkaFuture_t) }
}

/// Frees a `kafka_List_t *` element of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned list handle not yet destroyed.
pub(crate) unsafe fn destroy_list_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned list handle not yet destroyed,
    // which is what `kafka_List_destroy` requires.
    unsafe { kafka_List_destroy(element as *mut kafka_List_t) }
}

/// Frees a `kafka_Map_t *` element of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned map handle not yet destroyed.
pub(crate) unsafe fn destroy_map_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned map handle not yet destroyed,
    // which is what `kafka_Map_destroy` requires.
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
/// `handle` must be a live handle built by `box_result::<R, P>`, kept alive
/// for `'a`.
pub(crate) unsafe fn result_ref<'a, R, P>(handle: *const P) -> &'a ResultHandle<R> {
    // SAFETY: Per this function's `# Safety`, `handle` is a live handle built by
    // `box_result::<R, P>`, i.e. the `Box::into_raw` of a `Box<ResultHandle<R>>`, kept alive for
    // `'a`, so the shared reference is to a live `ResultHandle<R>`. Nothing mutates a result handle
    // after `box_result`.
    unsafe { &*(handle as *const ResultHandle<R>) }
}

/// Frees a handle built by [`box_result`]; null is a no-op.
///
/// # Safety
///
/// `handle` must be null or a handle built by `box_result::<R, P>` and not
/// yet destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_result<R, P>(handle: *mut P) {
    if !handle.is_null() {
        // SAFETY: `handle` is non-null (checked above) and, per this function's `# Safety`, a
        // handle built by `box_result::<R, P>` and not yet destroyed, i.e. the `Box::into_raw` of a
        // `Box<ResultHandle<R>>`, so `from_raw` reclaims that allocation, once. No other call uses
        // it meanwhile or afterwards, so no reference from `result_ref` outlives the drop.
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
/// [`kafka_admin_Admin__execute_callbacks`], never run callbacks.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close(self_: *const kafka_admin_Admin_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as `client_ref`
    // requires; the reference is used only during this call.
    let client = unsafe { client_ref(self_) };
    client.block_on(client.admin.close());
}

/// `Admin.close()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle or view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as
    // `completion_sink` requires. The guard runs this only when the body did not return. The body
    // hands `cb` over only as the last step of `Client::run_cb`, to the task `Client::spawn`
    // registers, after which nothing can panic. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_opaque` requires.
    unsafe { deliver_panic_opaque(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close_cb(
    self_: *const kafka_admin_Admin_t,
    cb: kafka_admin_Admin_close_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as `client_ref`
    // requires; the reference is used only during this call, the task getting its own `Arc`s.
    unsafe { client_ref(self_) }.run_cb("kafka_admin_Admin_close_cb", cb, opaque, |admin| async move {
        admin.close().await
    });
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close_with_timeout(self_: *const kafka_admin_Admin_t, timeout: i64) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as `client_ref`
    // requires; the reference is used only during this call.
    let client = unsafe { client_ref(self_) };
    client.block_on(client.admin.close_with_timeout(close_timeout(timeout)));
}

/// `Admin.close(Duration timeout)`, completion queued; `timeout` as in
/// [`kafka_admin_Admin_close_with_timeout`].
///
/// # Safety
///
/// `self_` must be a live handle or view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as
    // `completion_sink` requires. The guard runs this only when the body did not return. The body
    // hands `cb` over only as the last step of `Client::run_cb`, to the task `Client::spawn`
    // registers, after which nothing can panic. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_opaque` requires.
    unsafe { deliver_panic_opaque(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_close_with_timeout_cb(
    self_: *const kafka_admin_Admin_t,
    timeout: i64,
    cb: kafka_admin_Admin_close_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let timeout = close_timeout(timeout);
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as `client_ref`
    // requires; the reference is used only during this call, the task getting its own `Arc`s.
    unsafe { client_ref(self_) }.run_cb(
        "kafka_admin_Admin_close_with_timeout_cb",
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin__execute_callbacks(self_: *const kafka_admin_Admin_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as `client_ref`
    // requires; the reference is used only during this call.
    unsafe { client_ref(self_) }.queue.execute()
}

/// Installs the hook fired once each time the callback vector goes from
/// empty to non-empty (see [`kafka_admin_Admin_callbacks_notify_fn_t`]).
///
/// # Safety
///
/// `self_` must be a live handle or view; `opaque` stays valid while the
/// hook is installed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin__set_callbacks_notify(
    self_: *const kafka_admin_Admin_t,
    notify: kafka_admin_Admin_callbacks_notify_fn_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, as `client_ref`
    // requires; the reference is used only during this call. `opaque` stays valid while the hook is
    // installed, as the doc requires.
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
/// `self_` must be null or an owned handle from
/// `kafka_admin_AdminClient_create` not yet destroyed, that no other call
/// uses at the same time; it is not used afterwards, by the callbacks this
/// call runs included.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_destroy(self_: *mut kafka_admin_Admin_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. the `Box<AdminClassHandle>`
        // `kafka_admin_AdminClient_create` delivered, that no other call uses at the same time, so
        // taking it back frees it exactly once, through `destroy`, which awaits the tasks still
        // holding the client. `self_` is not used afterwards, by the callbacks `destroy` runs
        // included, as the doc requires.
        unsafe { Box::from_raw(self_ as *mut AdminClassHandle) }.destroy();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::admin::admin_client::kafka_admin_AdminClient_create;
    use crate::ffi::admin::admin_client_config::{
        kafka_admin_AdminClientConfig_destroy, kafka_admin_AdminClientConfig_new,
    };
    use crate::ffi::admin::list_topics_result::{
        kafka_admin_ListTopicsResult_destroy, kafka_admin_ListTopicsResult_names,
    };
    use crate::ffi::admin::mock_admin_client::{
        kafka_admin_MockAdminClient__as_Admin, kafka_admin_MockAdminClient_create, kafka_admin_MockAdminClient_destroy,
        kafka_admin_MockAdminClient_set_using_raft_controller,
    };
    use crate::ffi::admin::options::unregister_controller_options::{
        kafka_admin_UnregisterControllerOptions_destroy, kafka_admin_UnregisterControllerOptions_new,
        kafka_admin_UnregisterControllerOptions_set_timeout_ms, kafka_admin_UnregisterControllerOptions_timeout_ms,
    };
    use crate::ffi::admin::rpc::kafka_admin_Admin_list_topics;
    use crate::ffi::admin::rpc::{
        kafka_admin_Admin_unregister_controller, kafka_admin_Admin_unregister_controller_with_options,
    };
    use crate::ffi::admin::unregister_controller_result::{
        kafka_admin_UnregisterControllerResult_all, kafka_admin_UnregisterControllerResult_destroy,
    };
    use crate::ffi::common::fail_point;
    use crate::ffi::common::panic_probe::{Probe, on_opaque};
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message, kafka_common_Error_t};
    use crate::ffi::error_predicates::kafka_common_Error_is_unsupported_version_error;
    use crate::ffi::error_predicates::{
        kafka_common_Error_is_local_illegal_state_error, kafka_common_Error_is_timeout_error,
    };
    use crate::ffi::kafka_future::{
        kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_get_cb,
        kafka_common_KafkaFuture_get_with_timeout,
    };
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_new, kafka_Map_put};
    use std::sync::Mutex;

    /// A `close` `_cb` whose body panics before handing its completion over
    /// still fires `cb` once, queued: the callback carries no error, so the
    /// caller gets its `opaque` back and the panic is only logged.
    #[test]
    fn a_panicking_close_cb_queues_its_completion_once() {
        // SAFETY: `err` is a valid slot pointing at null; the mock is live until it is
        // destroyed below.
        unsafe {
            let mut err = std::ptr::null_mut();
            let mock = kafka_admin_MockAdminClient_create(1, &mut err);
            assert!(err.is_null());
            let admin = kafka_admin_MockAdminClient__as_Admin(mock);

            let close = Probe::default();
            fail_point::arm();
            kafka_admin_Admin_close_cb(admin, on_opaque, close.as_opaque());
            let close_with_timeout = Probe::default();
            fail_point::arm();
            kafka_admin_Admin_close_with_timeout_cb(admin, 0, on_opaque, close_with_timeout.as_opaque());

            assert!(close.fired().is_empty(), "queued, not fired");
            assert!(close_with_timeout.fired().is_empty(), "queued, not fired");
            assert_eq!(kafka_admin_Admin__execute_callbacks(admin), 2);
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(kafka_admin_Admin__execute_callbacks(admin), 0);
            assert_eq!(close.fired(), ["<no error>"]);
            assert_eq!(close_with_timeout.fired(), ["<no error>"]);
            kafka_admin_MockAdminClient_destroy(mock);
        }
    }

    /// `MockAdminClient.unregisterController` (KAFKA-20395) through C: without a
    /// Raft controller quorum the future fails with Java's
    /// `UnsupportedVersionException("")`; `set_using_raft_controller` makes it
    /// succeed, through the options form too.
    #[test]
    fn unregister_controller_follows_the_mock_raft_controller_setting() {
        // SAFETY: Test code: `err` is a valid slot pointing at null before each call that takes
        // it; the mock is live until it is destroyed last, and `admin` is its view; every result,
        // future, options and error handle is owned and destroyed once, after its last use.
        unsafe {
            let mut err = std::ptr::null_mut();
            let mock = kafka_admin_MockAdminClient_create(1, &mut err);
            assert!(err.is_null());
            let admin = kafka_admin_MockAdminClient__as_Admin(mock);

            let result = kafka_admin_Admin_unregister_controller(admin, 0);
            let all = kafka_admin_UnregisterControllerResult_all(result);
            assert!(kafka_common_KafkaFuture_get(all, &mut err).is_null());
            assert_eq!(kafka_common_Error_is_unsupported_version_error(err), 1);
            assert_eq!(std::ffi::CStr::from_ptr(kafka_common_Error_message(err)).to_str(), Ok(""));
            kafka_common_Error_destroy(err);
            err = std::ptr::null_mut();
            kafka_common_KafkaFuture_destroy(all);
            kafka_admin_UnregisterControllerResult_destroy(result);

            kafka_admin_MockAdminClient_set_using_raft_controller(mock, 1);
            let options = kafka_admin_UnregisterControllerOptions_new();
            assert_eq!(kafka_admin_UnregisterControllerOptions_timeout_ms(options), -1);
            kafka_admin_UnregisterControllerOptions_set_timeout_ms(options, 1000);
            assert_eq!(kafka_admin_UnregisterControllerOptions_timeout_ms(options), 1000);
            let result = kafka_admin_Admin_unregister_controller_with_options(admin, 0, options);
            kafka_admin_UnregisterControllerOptions_destroy(options);
            let all = kafka_admin_UnregisterControllerResult_all(result);
            assert!(kafka_common_KafkaFuture_get(all, &mut err).is_null());
            assert!(err.is_null());
            kafka_common_KafkaFuture_destroy(all);
            kafka_admin_UnregisterControllerResult_destroy(result);
            kafka_admin_UnregisterControllerOptions_destroy(std::ptr::null_mut());
            kafka_admin_UnregisterControllerResult_destroy(std::ptr::null_mut());

            kafka_admin_MockAdminClient_set_using_raft_controller(mock, 0);
            let result = kafka_admin_Admin_unregister_controller(admin, 0);
            let all = kafka_admin_UnregisterControllerResult_all(result);
            assert!(kafka_common_KafkaFuture_get(all, &mut err).is_null());
            assert_eq!(kafka_common_Error_is_unsupported_version_error(err), 1);
            kafka_common_Error_destroy(err);
            kafka_common_KafkaFuture_destroy(all);
            kafka_admin_UnregisterControllerResult_destroy(result);
            kafka_admin_MockAdminClient_destroy(mock);
        }
    }

    /// What a `get_cb` completion fired with: `(is_timeout_error,
    /// is_local_illegal_state_error, message)`, or `None` for a value.
    type Fired = Mutex<Vec<Option<(i8, i8, String)>>>;

    unsafe extern "C" fn record_completion(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        // SAFETY: Test code: `opaque` is the `Fired` the test keeps alive until the completion
        // fired; a completion owns the error handle it receives.
        unsafe {
            let fired = &*(opaque as *const Fired);
            if error.is_null() {
                assert!(!value.is_null());
                fired.lock().unwrap().push(None);
            } else {
                assert!(value.is_null());
                let message = std::ffi::CStr::from_ptr(kafka_common_Error_message(error))
                    .to_string_lossy()
                    .into_owned();
                fired.lock().unwrap().push(Some((
                    kafka_common_Error_is_timeout_error(error),
                    kafka_common_Error_is_local_illegal_state_error(error),
                    message,
                )));
                kafka_common_Error_destroy(error);
            }
        }
    }

    /// Java `KafkaAdminClient.close` fails the calls still pending with
    /// `TimeoutException("The AdminClient thread has exited.")`
    /// (`KafkaAdminClient.java:1557-1562,1692-1694`): destroying an admin whose
    /// broker is unreachable, with a `listTopics` still waiting and a blocking
    /// `get` on it in another thread, returns promptly, and both the `get_cb`
    /// waiting on the call and the blocking `get` see the call fail.
    #[test]
    fn destroy_fails_the_rpc_still_waiting_for_an_unreachable_broker() {
        // SAFETY: Test code: every map, config, admin, result and future handle is live until it
        // is destroyed once below; `err` is a valid slot pointing at null; `fired` outlives the
        // destroy, which fires the completion.
        unsafe {
            let props = kafka_Map_new();
            let (bootstrap, address) = (c"bootstrap.servers", c"127.0.0.1:1");
            kafka_Map_put(props, bootstrap.as_ptr() as *mut c_void, address.as_ptr() as *mut c_void);
            let mut err = std::ptr::null_mut();
            let config = kafka_admin_AdminClientConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let admin = kafka_admin_AdminClient_create(config, &mut err);
            assert!(err.is_null());
            kafka_admin_AdminClientConfig_destroy(config);

            let result = kafka_admin_Admin_list_topics(admin);
            let names = kafka_admin_ListTopicsResult_names(result);
            let fired = Fired::default();
            kafka_common_KafkaFuture_get_cb(names, record_completion, &fired as *const Fired as *mut c_void);
            let blocking = names as usize;
            let getter = std::thread::spawn(move || {
                let mut err = std::ptr::null_mut();
                let value = kafka_common_KafkaFuture_get(blocking as *const kafka_common_KafkaFuture_t, &mut err);
                assert!(value.is_null());
                let message = std::ffi::CStr::from_ptr(kafka_common_Error_message(err))
                    .to_string_lossy()
                    .into_owned();
                let timeout = kafka_common_Error_is_timeout_error(err);
                kafka_common_Error_destroy(err);
                (timeout, message)
            });
            // Let the call reach the pending list.
            std::thread::sleep(Duration::from_millis(300));

            let started = std::time::Instant::now();
            kafka_admin_Admin_destroy(admin);
            let elapsed = started.elapsed();
            let (timeout, message) = getter.join().unwrap();
            assert!(elapsed < Duration::from_secs(5), "destroy took {elapsed:?}");
            let exited = "The AdminClient thread has exited. Call: listTopics";
            assert_eq!((timeout, message.as_str()), (1, exited));
            assert_eq!(*fired.lock().unwrap(), [Some((1, 0, exited.to_string()))]);
            kafka_common_KafkaFuture_destroy(names);
            kafka_admin_ListTopicsResult_destroy(result);
        }
    }

    /// A destroy whose background task is stuck inside a TCP connect the
    /// broker never answers cannot fail the call still pending: the bounded
    /// join misses and the runtime cancels the task where it stands. The
    /// blocking `get` waiting on the call's future still returns, the `get_cb`
    /// still fires, both with the "client destroyed" error, and a `get` after
    /// the destroy returns at once.
    #[test]
    fn destroy_releases_a_blocking_get_whose_call_is_stuck_in_the_connect() {
        // A listener whose accept queue is full: Linux drops the SYNs it gets
        // next, so a connect to it waits out the SYN retries, about two minutes.
        let listener_runtime = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();
        let listener = listener_runtime.block_on(async {
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            socket.listen(0).unwrap()
        });
        let address = listener.local_addr().unwrap();
        let fillers: Vec<std::net::TcpStream> = (0..4)
            .filter_map(|_| std::net::TcpStream::connect_timeout(&address, Duration::from_millis(200)).ok())
            .collect();
        assert!(!fillers.is_empty(), "the accept queue takes one connection");
        let bootstrap_address = std::ffi::CString::new(address.to_string()).unwrap();
        // SAFETY: Test code: every map, config, admin, result and future handle is live until it
        // is destroyed once below; `err` is a valid slot pointing at null; `fired` outlives the
        // destroy, which fires the completion; the getter threads are joined before `names` is
        // destroyed.
        unsafe {
            let props = kafka_Map_new();
            let bootstrap = c"bootstrap.servers";
            kafka_Map_put(
                props,
                bootstrap.as_ptr() as *mut c_void,
                bootstrap_address.as_ptr() as *mut c_void,
            );
            let mut err = std::ptr::null_mut();
            let config = kafka_admin_AdminClientConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let admin = kafka_admin_AdminClient_create(config, &mut err);
            assert!(err.is_null());
            kafka_admin_AdminClientConfig_destroy(config);

            let result = kafka_admin_Admin_list_topics(admin);
            let names = kafka_admin_ListTopicsResult_names(result);
            let fired = Fired::default();
            kafka_common_KafkaFuture_get_cb(names, record_completion, &fired as *const Fired as *mut c_void);
            let blocking = names as usize;
            let (got_tx, got_rx) = std::sync::mpsc::channel();
            // A `get` and a `get_with_timeout`: the timed one waits on a timer of
            // the runtime the destroy stops, so it must be released before that.
            let getters: Vec<_> = [None, Some(120_000)]
                .into_iter()
                .map(|timeout| {
                    let got_tx = got_tx.clone();
                    std::thread::spawn(move || {
                        let future = blocking as *const kafka_common_KafkaFuture_t;
                        let mut err = std::ptr::null_mut();
                        let value = match timeout {
                            None => kafka_common_KafkaFuture_get(future, &mut err),
                            Some(timeout) => kafka_common_KafkaFuture_get_with_timeout(future, timeout, &mut err),
                        };
                        assert!(value.is_null());
                        let message = std::ffi::CStr::from_ptr(kafka_common_Error_message(err))
                            .to_string_lossy()
                            .into_owned();
                        let illegal_state = kafka_common_Error_is_local_illegal_state_error(err);
                        kafka_common_Error_destroy(err);
                        let _ = got_tx.send((timeout, illegal_state, message));
                    })
                })
                .collect();
            // Let the background task reach the connect.
            std::thread::sleep(Duration::from_millis(500));

            kafka_admin_Admin_destroy(admin);
            let destroyed = "The client was destroyed before the future completed";
            let mut got: Vec<_> = (0..2)
                .map(|_| got_rx.recv_timeout(Duration::from_secs(10)).expect("the blocking get returned"))
                .collect();
            got.sort();
            for getter in getters {
                getter.join().unwrap();
            }
            assert_eq!(
                got,
                [
                    (None, 1, destroyed.to_string()),
                    (Some(120_000), 1, destroyed.to_string())
                ]
            );
            assert_eq!(*fired.lock().unwrap(), [Some((0, 1, destroyed.to_string()))]);

            let started = std::time::Instant::now();
            assert!(kafka_common_KafkaFuture_get(names, &mut err).is_null());
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "a get after the destroy waits for nothing"
            );
            assert_eq!(kafka_common_Error_is_local_illegal_state_error(err), 1);
            kafka_common_Error_destroy(err);
            kafka_common_KafkaFuture_destroy(names);
            kafka_admin_ListTopicsResult_destroy(result);
        }
        drop(fillers);
        listener_runtime.block_on(async move { drop(listener) });
    }

    /// A `close_with_timeout_cb` still running when `_destroy` is called keeps
    /// its grace period: the destroy waits for it before closing with none, so
    /// the call still pending is given the whole timeout, and the completion
    /// fires once.
    #[test]
    fn destroy_lets_a_running_close_with_timeout_cb_keep_its_grace_period() {
        // SAFETY: Test code: every map, config, admin and result handle is live until it is
        // destroyed once below; `err` is a valid slot pointing at null; `close` outlives the
        // destroy, which fires the completion.
        unsafe {
            let props = kafka_Map_new();
            let (bootstrap, address) = (c"bootstrap.servers", c"127.0.0.1:1");
            kafka_Map_put(props, bootstrap.as_ptr() as *mut c_void, address.as_ptr() as *mut c_void);
            let mut err = std::ptr::null_mut();
            let config = kafka_admin_AdminClientConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let admin = kafka_admin_AdminClient_create(config, &mut err);
            assert!(err.is_null());
            kafka_admin_AdminClientConfig_destroy(config);

            let result = kafka_admin_Admin_list_topics(admin);
            let close = Probe::default();
            let started = std::time::Instant::now();
            kafka_admin_Admin_close_with_timeout_cb(admin, 1500, on_opaque, close.as_opaque());
            kafka_admin_Admin_destroy(admin);
            let elapsed = started.elapsed();
            assert!(
                elapsed >= Duration::from_millis(1400),
                "destroy cut the close short: {elapsed:?}"
            );
            assert_eq!(close.fired(), ["<no error>"]);
            kafka_admin_ListTopicsResult_destroy(result);
        }
    }

    /// A destroy inside a tokio runtime, where `teardown`'s `block_on` would
    /// panic, returns at once and tears the handle down on its own thread: the
    /// queued `close_cb` completion fires there, once.
    #[test]
    fn destroy_inside_a_tokio_runtime_tears_down_on_its_own_thread() {
        use crate::ffi::common::teardown_probe::{Fired, on_fired};
        let fired = Fired::new();
        // SAFETY: Test code: `err` is a valid slot pointing at null; the mock is live until it is
        // destroyed, once, inside the runtime; `fired` outlives the completion it waits for.
        unsafe {
            let mut err = std::ptr::null_mut();
            let mock = kafka_admin_MockAdminClient_create(1, &mut err);
            assert!(err.is_null());
            kafka_admin_Admin_close_cb(kafka_admin_MockAdminClient__as_Admin(mock), complete_close, fired.as_opaque());
            let mock = mock as usize;
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            runtime.block_on(async move { kafka_admin_MockAdminClient_destroy(mock as *mut _) });
        }
        assert_eq!(fired.wait(), (Some("kafka-admin-destroy".to_string()), true));

        /// `close_cb`'s `(opaque)` completion, reported as an error-free firing.
        unsafe extern "C" fn complete_close(opaque: *mut c_void) {
            // SAFETY: Test code: `opaque` is the `Fired` above, alive until it has waited.
            unsafe { on_fired(std::ptr::null_mut(), opaque) }
        }
    }
}
