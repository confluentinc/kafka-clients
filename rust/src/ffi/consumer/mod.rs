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

//! C bindings for `org.apache.kafka.clients.consumer` (CLAUDE.md §4).
//!
//! # Handles
//!
//! `kafka_consumer_Consumer_t` is the interface handle, every operation of
//! the `Consumer` trait takes it. It is never owned: the class handles
//! `kafka_consumer_KafkaConsumer_t` and `kafka_consumer_MockConsumer_t` are,
//! freed with `kafka_consumer_KafkaConsumer_destroy` /
//! `kafka_consumer_MockConsumer_destroy`, and their `__as_Consumer` returns a
//! borrowed view valid until the class handle is destroyed. All three point
//! at the same thing: a [`ConsumerClassHandle`] whose first field is the
//! shared [`Client`].
//!
//! # Threading
//!
//! Java's `KafkaConsumer` is not safe for multi-threaded access and reports
//! a concurrent call with `ConcurrentModificationException`; the handle
//! keeps that contract with a single-owner flag ([`Client::acquire`]). Every
//! operation that takes `&mut self` in Rust takes `kafka_consumer_Consumer_t *`
//! and holds the flag for its duration — the blocking form while it runs on
//! the calling thread, the `_cb` form until its operation ends, released just
//! before the completion is queued, so a caller that runs the completion and
//! makes its next call never finds the flag still held. The
//! sync getters Java guards with `acquire()` (`assignment`, `subscription`,
//! `paused`, `currentLag`, `groupMetadata`) take the flag for the call and,
//! when another call holds it, fail with the same
//! `LOCAL_CONCURRENT_MODIFICATION` through their trailing `out_err`, returning
//! their value otherwise. `metrics`, which Java does not guard, reads through
//! the `ConsumerHandle`. `wakeup` and the `ConsumerHandle` never touch the
//! consumer itself and are callable at any time.
//!
//! # Callbacks (CLAUDE.md §4 rule 5)
//!
//! A blocking operation invokes the interface methods it triggers
//! (`ConsumerRebalanceListener`, `OffsetCommitCallback`) directly on the
//! calling thread, as Java does; a `_cb` operation queues them on the
//! client's callbacks vector, which [`kafka_consumer_Consumer__execute_callbacks`]
//! runs on the pumping thread, and queues its own completion the same way.
//! Those interface methods are `async` in Rust, so their C functions return
//! `void`, take a trailing `int64_t callback_id` and report through
//! [`kafka_consumer_Consumer__set_callback_result`], from inside the
//! function or later from any thread; the operation waits for that report
//! (a C implementation that never reports hangs it, as a Java listener that
//! never returns would). Destroying the class handle wakes the
//! operation still waiting (it fails with `kafka_common_Error_is_wakeup_error`)
//! and runs the callbacks still pending so each fires exactly once.
//!
//! # Generic types
//!
//! `K` and `V` are `void *` produced by the deserializers passed to
//! `kafka_consumer_KafkaConsumer_new`; with a `NULL` deserializer they are
//! `kafka_Bytes_t *` owned by the records (see `consumer_record`).

#![expect(non_camel_case_types)]

pub(crate) mod close_options;
pub(crate) mod consumer_config;
pub(crate) mod consumer_group_metadata;
pub(crate) mod consumer_handle;
pub(crate) mod consumer_partition_assignor;
pub(crate) mod consumer_rebalance_listener;
pub(crate) mod consumer_record;
pub(crate) mod consumer_records;
pub(crate) mod group_protocol;
pub(crate) mod kafka_consumer;
pub(crate) mod mock_consumer;
pub(crate) mod offset_and_metadata;
pub(crate) mod offset_and_timestamp;
pub(crate) mod offset_commit_callback;
pub(crate) mod subscription_pattern;

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ffi::{CString, c_char, c_void};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::common::{Error, TopicPartition};
use crate::consumer::{ConsumerHandle, KafkaConsumer, MockConsumer};
use crate::ffi::callback_queue::{CallbackJob, CallbackQueue, SendPtr};
use crate::ffi::callback_result::{CallbackResults, INVOKER_RESULTS};
use crate::ffi::common::metrics::box_metrics_map;
use crate::ffi::common::partition_info::partition_info_list;
use crate::ffi::common::topic_partition::{
    kafka_common_TopicPartition_t, list_topic_partitions, map_topic_partition_i64, sorted_topic_partition_list,
    topic_partition_i64_map, topic_partition_ref,
};
use crate::ffi::common::{
    box_error, catch_task_panic, deliver_panic, deliver_panic_i64, deliver_panic_value, in_tokio_runtime,
    init_default_logger, kafka_common_Error_t, spawn_callback_task, spawn_teardown, take_error,
};
use crate::ffi::common::{deliver_unit, deliver_value};
use crate::ffi::consumer::close_options::{close_options_ref, kafka_consumer_CloseOptions_t};
use crate::ffi::consumer::consumer_group_metadata::box_group_metadata;
pub(crate) use crate::ffi::consumer::consumer_group_metadata::{
    group_metadata_ref, kafka_consumer_ConsumerGroupMetadata_t,
};
use crate::ffi::consumer::consumer_handle::{box_consumer_handle, kafka_consumer_ConsumerHandle_t};
use crate::ffi::consumer::consumer_rebalance_listener::{kafka_consumer_ConsumerRebalanceListener_t, listener_adapter};
use crate::ffi::consumer::consumer_records::{box_consumer_records, kafka_consumer_ConsumerRecords_t};
pub(crate) use crate::ffi::consumer::offset_and_metadata::{
    OffsetAndMetadataInner, box_offset_and_metadata, kafka_consumer_OffsetAndMetadata_destroy,
    kafka_consumer_OffsetAndMetadata_t, offset_and_metadata_ref,
};
use crate::ffi::consumer::offset_and_metadata::{map_offset_and_metadata, offset_and_metadata_map};
use crate::ffi::consumer::offset_and_timestamp::offset_and_timestamp_map;
use crate::ffi::consumer::offset_commit_callback::{commit_callback_adapter, kafka_consumer_OffsetCommitCallback_t};
use crate::ffi::consumer::subscription_pattern::{kafka_consumer_SubscriptionPattern_t, subscription_pattern_ref};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::block_on;
use crate::ffi::util::{
    GenericValue, box_string_keyed_map, c_str_to_string, kafka_List_destroy, kafka_List_t, kafka_Map_t, list_strings,
    owned_c_string, sorted_string_list,
};

/// Opaque handle to a `Consumer<K, V>` (see the module docs).
#[repr(C)]
pub struct kafka_consumer_Consumer_t {
    _private: [u8; 0],
}

/// The consumer behind the boundary: keys and values are the C caller's
/// `void *`s. The dyn-compatible [`crate::consumer::DynConsumer`], since
/// `Consumer` returns unboxed futures and is not dyn-compatible.
pub(crate) type DynConsumer = dyn crate::consumer::DynConsumer<GenericValue, GenericValue>;

/// A pinned, sendable operation future borrowing the consumer.
pub(crate) type OpFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// What a handle wraps: the `KafkaConsumer`, or the mock, kept concrete so
/// `kafka_consumer_MockConsumer_*` can reach its inherent methods.
pub(crate) enum ConsumerKind {
    Async(Box<KafkaConsumer<GenericValue, GenericValue>>),
    Mock(Box<MockConsumer<GenericValue, GenericValue>>),
}

impl ConsumerKind {
    fn as_dyn(&self) -> &DynConsumer {
        match self {
            Self::Async(consumer) => &**consumer,
            Self::Mock(mock) => &**mock,
        }
    }

    fn as_dyn_mut(&mut self) -> &mut DynConsumer {
        match self {
            Self::Async(consumer) => &mut **consumer,
            Self::Mock(mock) => &mut **mock,
        }
    }
}

/// How the interface methods an operation triggers reach C (CLAUDE.md §4
/// rule 5): directly on the calling thread while a blocking operation runs,
/// queued otherwise. Shared with the listener and callback adapters, which
/// hold it instead of the [`Client`] so they never keep the consumer alive.
pub(crate) struct Delivery {
    queue: CallbackQueue,
    blocking: AtomicBool,
    /// The consumer's pending interface calls, by the id they report with
    /// through [`kafka_consumer_Consumer__set_callback_result`]. An `Arc`,
    /// so a report and a queued invocation keep it alive without keeping
    /// the rest of the delivery (and its queue) alive.
    results: Arc<CallbackResults>,
}

/// Where [`Delivery::invoke`] sends the interface methods a
/// `ConsumerHandle` operation triggers, set for the task that drives the
/// operation (`consumer_handle`'s `HandleInner::run_blocking` /
/// `run_cb`).
///
/// No Java counterpart (DoD #7): a Java callback calling the consumer back
/// runs the nested callbacks on its own thread, inside the call
/// (`AsyncKafkaConsumer.commit()` runs `executeCallbacks()` first). A
/// handle operation runs on a task of the runtime instead (a nested
/// `block_on` is forbidden there), so without a route the callbacks its
/// commit drains would run on that task's worker thread whenever a blocking
/// consumer operation has the blocking flag set.
#[derive(Clone)]
pub(crate) enum InvokeRoute {
    /// To the thread waiting for a blocking handle operation, which runs it
    /// there.
    Caller(Arc<dyn Fn(CallbackJob) + Send + Sync>),
    /// To the callbacks vector, for a `_cb` handle operation.
    Queue,
}

tokio::task_local! {
    /// The route of the handle operation the current task drives, if any.
    pub(crate) static INVOKE_ROUTE: InvokeRoute;
}

impl Delivery {
    /// Runs `job` now when a blocking operation is on the calling thread,
    /// queues it otherwise. Inside a `ConsumerHandle` operation the
    /// operation's [`InvokeRoute`] decides instead: the thread waiting for a
    /// blocking one runs it, a `_cb` one queues it.
    pub(crate) fn invoke(&self, job: CallbackJob) {
        match INVOKE_ROUTE.try_with(InvokeRoute::clone) {
            Ok(InvokeRoute::Caller(send)) => send(job),
            Ok(InvokeRoute::Queue) => self.queue.push(job),
            Err(_) if self.blocking.load(Ordering::Acquire) => job(),
            Err(_) => self.queue.push(job),
        }
    }

    /// The registry the invocations this delivery runs report to.
    pub(crate) fn results(&self) -> &Arc<CallbackResults> {
        &self.results
    }

    #[cfg(test)]
    pub(crate) fn queue(&self) -> &CallbackQueue {
        &self.queue
    }

    #[cfg(test)]
    pub(crate) fn for_tests(queue: CallbackQueue) -> Self {
        Self {
            queue,
            blocking: AtomicBool::new(false),
            results: Arc::new(CallbackResults::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_blocking_for_tests(&self, blocking: bool) {
        self.blocking.store(blocking, Ordering::Release);
    }
}

/// The state a `kafka_consumer_Consumer_t` points at, shared through an
/// `Arc` between the class handle and the tasks `_cb` operations spawn, so
/// a task never outlives what it uses.
pub(crate) struct Client {
    /// Exclusive access is granted by [`Self::acquire`], never by the type.
    consumer: UnsafeCell<ConsumerKind>,
    /// Java's single-owner check (`KafkaConsumer.acquire`).
    busy: AtomicBool,
    delivery: Arc<Delivery>,
    runtime: tokio::runtime::Handle,
    /// Tasks spawned by `_cb` calls, awaited by `destroy`.
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// The consumer's reentrancy handle, also what `wakeup` goes through
    /// so it never touches the consumer an operation may be borrowing.
    handle: ConsumerHandle,
    client_id: CString,
    /// Whether the `void *`s are `kafka_Bytes_t`s the records own (a `NULL`
    /// deserializer on that side).
    owns: Owns,
}

/// Which of a record's `void *`s are `kafka_Bytes_t`s the record owns.
#[derive(Clone, Copy)]
pub(crate) struct Owns {
    pub(crate) key: bool,
    pub(crate) value: bool,
}

// SAFETY: `Send` is a bound every field already meets on its own: `UnsafeCell<ConsumerKind>` is
// `Send` because `ConsumerKind` is, and so is every other field, so this impl only restates what
// the compiler derives.
unsafe impl Send for Client {}
// SAFETY: `Sync` is the bound `UnsafeCell<ConsumerKind>` lacks; every other field is `Sync` on its
// own. The cell is reached only through `BusyGuard::kind_mut`, and a `BusyGuard` exists only while
// it holds `busy`: `acquire` takes the flag with a compare-and-swap (`AcqRel`) and the guard's drop
// clears it (`Release`), so at most one thread reaches the cell at a time and each sees the
// previous holder's writes. `ConsumerKind` is `Send`, so that thread may be any thread.
unsafe impl Sync for Client {}

/// Releases the single-owner flag when dropped.
pub(crate) struct BusyGuard(Arc<Client>);

impl BusyGuard {
    /// The consumer this guard grants exclusive access to; the borrow is
    /// tied to the guard, so it cannot outlive the single-owner flag.
    pub(crate) fn consumer_mut(&mut self) -> &mut DynConsumer {
        self.kind_mut().as_dyn_mut()
    }

    /// The concrete consumer, for the mock's inherent methods.
    pub(crate) fn kind_mut(&mut self) -> &mut ConsumerKind {
        // SAFETY: `acquire` hands out one guard at a time, and `&mut self`
        // makes this the only borrow taken through it.
        unsafe { &mut *self.0.consumer.get() }
    }
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.0.busy.store(false, Ordering::Release);
    }
}

impl Client {
    /// Takes the single-owner flag, or fails with the
    /// `ConcurrentModificationException` translation Java throws.
    pub(crate) fn acquire(self: &Arc<Self>) -> Result<BusyGuard, Error> {
        match self.busy.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(BusyGuard(Arc::clone(self))),
            Err(_) => Err(Error::local_concurrent_modification(
                "KafkaConsumer is not safe for multi-threaded access.",
            )),
        }
    }

    pub(crate) fn delivery(&self) -> &Arc<Delivery> {
        &self.delivery
    }

    pub(crate) fn handle(&self) -> &ConsumerHandle {
        &self.handle
    }

    pub(crate) fn owns(&self) -> Owns {
        self.owns
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
    pub(crate) fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        tasks.retain(|task| !task.is_finished());
        tasks.reserve(1);
        tasks.push(spawn_callback_task(&self.runtime, future));
    }

    /// Drives `future` on the calling thread with the blocking flag set, so
    /// the interface methods it triggers run there too (see the module docs).
    /// The flag is cleared even when `future` panics.
    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _blocking = BlockingGuard::set(&self.delivery);
        block_on(&self.runtime, future)
    }
}

/// Holds [`Delivery`]'s blocking flag for one [`Client::block_on`] and clears
/// it when dropped.
///
/// No Java counterpart (DoD #7): Java calls the listener on the thread
/// inside the blocking method and has nothing to reset. Here the flag routes
/// every interface method an operation triggers, and a panic inside the
/// operation unwinds to the entry point's `#[ffi_guard]`, which keeps the
/// handle usable. A reset written after the `block_on` would be skipped by
/// that unwind, leaving the flag set, and every later listener or
/// `OffsetCommitCallback` would then run inline on whatever thread delivered
/// it, a tokio worker for a `_cb` operation, instead of being queued for
/// `__execute_callbacks` (CLAUDE.md §4).
struct BlockingGuard<'a>(&'a Delivery);

impl<'a> BlockingGuard<'a> {
    fn set(delivery: &'a Delivery) -> Self {
        delivery.blocking.store(true, Ordering::Release);
        Self(delivery)
    }
}

impl Drop for BlockingGuard<'_> {
    fn drop(&mut self) {
        self.0.blocking.store(false, Ordering::Release);
    }
}

/// The class handle behind `kafka_consumer_KafkaConsumer_t`,
/// `kafka_consumer_MockConsumer_t` and their `kafka_consumer_Consumer_t`
/// views: the shared client and the runtime that drives it. `#[repr(C)]`
/// with the client first, so the `Consumer_t` pointer is the handle pointer and
/// [`client_ref`] reads the `Arc` straight from it.
#[repr(C)]
pub(crate) struct ConsumerClassHandle {
    client: Arc<Client>,
    runtime: tokio::runtime::Runtime,
}

impl ConsumerClassHandle {
    /// Builds the consumer with `make`, inside the context of a fresh
    /// multi-thread runtime (an `AsyncKafkaConsumer` spawns its background
    /// task on construction).
    pub(crate) fn new(owns: Owns, make: impl FnOnce() -> Result<ConsumerKind, Error>) -> Result<Box<Self>, Error> {
        init_default_logger();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("kafka-consumer-ffi")
            .build()
            .expect("tokio runtime");
        let kind = {
            let _enter = runtime.enter();
            make()?
        };
        let handle = kind.as_dyn().handle();
        let client_id = owned_c_string(kind.as_dyn().client_id());
        let client = Arc::new(Client {
            consumer: UnsafeCell::new(kind),
            busy: AtomicBool::new(false),
            delivery: Arc::new(Delivery {
                queue: CallbackQueue::new(),
                blocking: AtomicBool::new(false),
                results: Arc::new(CallbackResults::new()),
            }),
            runtime: runtime.handle().clone(),
            tasks: Mutex::new(Vec::new()),
            handle,
            client_id,
            owns,
        });
        Ok(Box::new(Self { client, runtime }))
    }

    /// The `Consumer` view of this handle (see the module docs).
    pub(crate) fn as_consumer(&self) -> *mut kafka_consumer_Consumer_t {
        self as *const Self as *mut kafka_consumer_Consumer_t
    }

    /// Tears the handle down ([`Self::teardown`]) on the calling thread, or,
    /// called inside a tokio runtime, where `teardown`'s `block_on` would
    /// panic, on a new thread, returning at once (see [`spawn_teardown`]);
    /// the pending callbacks then fire on that thread.
    ///
    /// # Safety
    ///
    /// As for [`Self::teardown`].
    pub(crate) unsafe fn destroy(handle: *mut Self) {
        if in_tokio_runtime() {
            /// # Safety
            ///
            /// As for [`ConsumerClassHandle::teardown`], on `handle as *mut
            /// ConsumerClassHandle`.
            unsafe fn teardown(handle: usize) {
                // SAFETY: Per this function's `# Safety`, the address meets `teardown`'s.
                unsafe { ConsumerClassHandle::teardown(handle as *mut ConsumerClassHandle) };
            }
            // SAFETY: Per this function's `# Safety`, `handle` meets `teardown`'s `# Safety`, and
            // this is its only teardown; `teardown` waits with `block_on`, which is sound on the
            // new thread, outside any runtime.
            unsafe { spawn_teardown("kafka-consumer-destroy", handle as usize, teardown) };
        } else {
            // SAFETY: Per this function's `# Safety`, as `teardown` requires.
            unsafe { Self::teardown(handle) };
        }
    }

    /// Wakes and awaits every spawned task, fires the pending callbacks, drops
    /// the consumer inside the runtime context, shuts the runtime down and frees
    /// the handle.
    ///
    /// The queue is drained *while* the tasks are awaited: a `_cb` task may
    /// be waiting for the report of a listener or commit-callback invocation
    /// that sits queued for `__execute_callbacks`, so awaiting it first would
    /// never return. Every pending callback thus fires exactly once, on the
    /// destroying thread (CLAUDE.md §4 rule 5).
    ///
    /// The handle is reached through shared references until then and freed
    /// only at the end, because those callbacks report through
    /// `__set_callback_result` on this very handle, from the destroying
    /// thread or another one. A report reads the handle only to clone the
    /// registry's `Arc`, before it delivers, and the task waiting for that
    /// delivery holds this call up, so no report still reads the handle once
    /// it is freed.
    ///
    /// A panic caught earlier may have poisoned `tasks`; it is taken
    /// regardless, so the handle is still freed and every pending callback
    /// still fires exactly once.
    ///
    /// # Safety
    ///
    /// `handle` must be an owned handle, the `Box::into_raw` of a
    /// `Box<ConsumerClassHandle>` not yet destroyed, that no other call uses
    /// at the same time except the `__set_callback_result` reports of the
    /// callbacks this call runs or waits for; it is freed here.
    unsafe fn teardown(handle: *mut Self) {
        {
            // SAFETY: Per this function's `# Safety`, `handle` is a live `Box<ConsumerClassHandle>`
            // not yet destroyed, and the only other users are reports, which take shared references
            // too, so a shared reference to it is valid until the `Box::from_raw` below, which comes
            // after this block's last use of it.
            let Self { client, runtime } = unsafe { &*handle };
            loop {
                let tasks: Vec<JoinHandle<()>> =
                    std::mem::take(&mut *client.tasks.lock().unwrap_or_else(PoisonError::into_inner));
                if tasks.is_empty() {
                    break;
                }
                runtime.block_on(async {
                    for mut task in tasks {
                        // Wake the operation the task awaits (`wakeup` goes
                        // through the reentrancy handle, never the consumer the
                        // task borrows), so a `poll_cb` with a long timeout
                        // returns `WakeupError` instead of holding destroy up
                        // for the whole timeout. Once per task: the one after
                        // it may start its wait only once this one returns.
                        client.handle.wakeup();
                        loop {
                            client.delivery.queue.execute();
                            // Both branches are cancellation-safe: polling a
                            // `JoinHandle` has no side effect, and a push that
                            // races the `notified()` leaves its permit behind.
                            tokio::select! {
                                _ = &mut task => break,
                                () = client.delivery.queue.pushed() => {},
                            }
                        }
                    }
                });
            }
            client.delivery.queue.execute();
        }
        // SAFETY: Per this function's `# Safety`, `handle` is the `Box::into_raw` of a
        // `Box<ConsumerClassHandle>` not yet destroyed, so taking it back frees it exactly once. The
        // shared reference above is no longer used, and no report still reads the handle: every
        // task is finished, so the reports they waited for were delivered, and a report reads the
        // handle only before it delivers (see the docs above).
        let Self { client, runtime } = *unsafe { Box::from_raw(handle) };
        {
            let _enter = runtime.enter();
            drop(client);
        }
        runtime.shutdown_timeout(Duration::from_secs(5));
    }
}

/// The client behind a consumer handle.
///
/// # Safety
///
/// `consumer` must point at a live [`ConsumerClassHandle`], kept alive for
/// `'a`: an `__as_Consumer` view, or a `kafka_consumer_KafkaConsumer_t` /
/// `kafka_consumer_MockConsumer_t` cast to `kafka_consumer_Consumer_t` (the
/// same pointer, see [`ConsumerClassHandle::as_consumer`]).
pub(crate) unsafe fn client_ref<'a>(consumer: *const kafka_consumer_Consumer_t) -> &'a Arc<Client> {
    // SAFETY: Per this function's `# Safety`, `consumer` points at a live `ConsumerClassHandle`
    // kept alive for `'a`: every pointer the doc lists is that handle's address.
    // `ConsumerClassHandle` is `#[repr(C)]` with `client: Arc<Client>` as its first field, so the
    // address is that field's, and a shared reference to it is valid for `'a`. Nothing takes a
    // `&mut` to the handle while it is live: only its destroy takes it back, which the destroy docs
    // exclude while another call uses the handle.
    unsafe { &*(consumer as *const Arc<Client>) }
}

/// Where the guard of a `_cb` entry point delivers a caught panic: the
/// consumer's callbacks vector (see [`deliver_panic`]).
///
/// # Safety
///
/// `consumer` must be a live handle, as [`client_ref`] describes; the sink
/// holds its own `Arc` to the client, so it may outlive the call.
pub(crate) unsafe fn completion_sink(consumer: *const kafka_consumer_Consumer_t) -> impl FnOnce(CallbackJob) {
    // SAFETY: Per this function's `# Safety`, `consumer` is a live handle, as `client_ref`
    // requires; the reference is used only to clone the `Arc`, so the sink does not borrow the
    // handle.
    let client = Arc::clone(unsafe { client_ref(consumer) });
    move |job| client.delivery.queue.push(job)
}

pub(crate) fn error_slot(result: Result<(), Error>) -> *mut kafka_common_Error_t {
    result.err().map_or(std::ptr::null_mut(), box_error)
}

/// A Java `Duration` in milliseconds; negative fails as Java's
/// `IllegalArgumentException("Timeout must not be negative")`.
pub(crate) fn ms(timeout: i64) -> Result<Duration, Error> {
    u64::try_from(timeout)
        .map(Duration::from_millis)
        .map_err(|_| Error::local_illegal_argument("Timeout must not be negative"))
}

// ---------------------------------------------------------------------------
// Callback results (CLAUDE.md §4 rule 3: async interface methods)
// ---------------------------------------------------------------------------

/// Reports the result of an async interface method (`ConsumerRebalanceListener`,
/// `OffsetCommitCallback`) invoked with `callback_id` (CLAUDE.md §4): once the
/// callback finishes, the Rust task that invoked it completes with this
/// report. The double underscore marks a function derived by convention, with
/// no Java counterpart.
///
/// `error` is `NULL` for success; otherwise it is an owned
/// `kafka_common_Error_t *` that Rust takes over, and `result` is then
/// ignored and stays the caller's. On success `result` is the method's return
/// value, which Rust takes over as the method's `_fn_t` documents; for a
/// `void` method it is ignored, so pass `NULL`. Every method the consumer
/// invokes is `void` in Java (`ConsumerRebalanceListener`'s three methods,
/// `OffsetCommitCallback.onComplete`).
///
/// `self_` is the consumer that invoked the method: each consumer keeps its
/// own ids. A method invoked through `kafka_consumer_ConsumerRebalanceListener_on_partitions_*`
/// or `kafka_consumer_OffsetCommitCallback_on_complete*`, which have no
/// consumer, is reported the same way; its id says so and `self_` is then
/// not read, so it may be any consumer or `NULL`.
///
/// Callable from inside the method or later, from any thread, including
/// while `kafka_consumer_KafkaConsumer_destroy` or `kafka_consumer_MockConsumer_destroy`
/// runs the consumer's pending callbacks, but never once it returned. A
/// second report for the same id, or one for an id the consumer did not
/// issue, is ignored: its error is freed and its `result` is not taken.
///
/// # Safety
///
/// `self_` must be the live consumer handle that issued `callback_id`
/// (unless an invoker issued it, see above), `error` null or a valid owned
/// error not used afterwards, and `result` what the method's `_fn_t`
/// documents for a successful report.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer__set_callback_result(
    self_: *const kafka_consumer_Consumer_t,
    callback_id: i64,
    result: *mut c_void,
    error: *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `error` is null or a valid owned error not used
    // afterwards, i.e. a `box_error` handle the C side hands over here, as `take_error` requires.
    let error = unsafe { take_error(error) };
    // `result` is only passed on: the sink registered under `callback_id` takes it over as its
    // method's `_fn_t` documents, which this function's `# Safety` has the caller follow.
    if CallbackResults::is_tagged(callback_id) {
        INVOKER_RESULTS.complete(callback_id, result, error);
    } else {
        // SAFETY: Per this function's `# Safety`, `self_` is the live consumer handle that issued
        // the untagged `callback_id`, as `client_ref` requires, possibly while its destroy runs, which
        // reaches the handle through shared references only until it frees it. The reference is used
        // only to clone the registry's `Arc`, before the report is delivered, so the handle is not
        // read once the sink ran, which is what lets that destroy free it afterwards.
        let results = Arc::clone(unsafe { client_ref(self_) }.delivery.results());
        results.complete(callback_id, result, error);
    }
}

// ---------------------------------------------------------------------------
// Operation plumbing
// ---------------------------------------------------------------------------

/// Runs an operation on the calling thread under the single-owner flag.
///
/// # Safety
///
/// `self_` must be a live handle, as [`client_ref`] describes, kept alive for
/// this call.
unsafe fn run<T, F>(self_: *mut kafka_consumer_Consumer_t, f: F) -> Result<T, Error>
where
    F: for<'a> FnOnce(&'a mut DynConsumer) -> OpFuture<'a, T>,
{
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `client_ref` requires. Exclusive access to the consumer comes from `acquire`, not from
    // this reference.
    let client = unsafe { client_ref(self_) };
    let mut guard = client.acquire()?;
    client.block_on(f(guard.consumer_mut()))
}

/// Runs an operation on the client's runtime under the single-owner flag
/// and hands its result to `deliver`, which queues the completion; a
/// rejected call is delivered the same way, and a panic inside the operation
/// is delivered as the [`panic_error`](crate::ffi::common::panic_error) of
/// `fn_name`, the entry point.
///
/// # Safety
///
/// `self_` must be a live handle, as [`client_ref`] describes, kept alive for
/// this call; the task it spawns holds its own `Arc` to the client.
unsafe fn run_cb<T, F, D>(fn_name: &'static str, self_: *mut kafka_consumer_Consumer_t, f: F, deliver: D)
where
    T: Send + 'static,
    F: for<'a> FnOnce(&'a mut DynConsumer) -> OpFuture<'a, T> + Send + 'static,
    D: FnOnce(&Client, Result<T, Error>) + Send + 'static,
{
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call, as
    // `client_ref` requires; the reference is used only before this function returns: `acquire`,
    // `deliver` on a rejection, and the `Arc` clones the task holds.
    let client = unsafe { client_ref(self_) };
    match client.acquire() {
        Err(error) => deliver(client, Err(error)),
        Ok(mut guard) => {
            let client = Arc::clone(client);
            let task_client = Arc::clone(&client);
            client.spawn(async move {
                let result = catch_task_panic(fn_name, f(guard.consumer_mut())).await;
                // Release the flag before queuing: the queue's notify hook can wake a caller
                // that runs this completion and calls again before this task would get past
                // the push, and that call must not be rejected as concurrent.
                drop(guard);
                deliver(&task_client, result);
            });
        },
    }
}

type VoidCb = unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
type I64Cb = unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

fn deliver_void(client: &Client, cb: VoidCb, opaque: SendPtr, result: Result<(), Error>) {
    client
        .delivery
        .queue
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to a `_cb` entry point: every
        // caller of `deliver_void` (the `deliver` closure a `_cb` entry point hands `run_cb` or
        // `HandleInner::run_cb`, which run it at most once, and
        // `kafka_consumer_MockConsumer_rebalance_cb`, on one path only) passes them on unchanged,
        // so calling `cb` with `opaque` is the completion that caller waits for, and `cb` is still
        // valid, as each entry point's `# Safety` requires it to stay valid until it fires.
        // `error_slot` returns null or a fresh `box_error` handle, owned by no one else, so handing
        // it to `cb` transfers its ownership, as the entry point's `_cb_t` documents; the job is a
        // `FnOnce`, so `cb` fires exactly once.
        .push(Box::new(move || unsafe { cb(error_slot(result), opaque.get()) }));
}

fn deliver_i64(client: &Client, cb: I64Cb, opaque: SendPtr, result: Result<i64, Error>) {
    client.delivery.queue.push(Box::new(move || match result {
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to a `_cb` entry point: every
        // caller of `deliver_i64` (the `deliver` closure a `_cb` entry point hands `run_cb` or
        // `HandleInner::run_cb`, which run it at most once) passes them on unchanged, so calling
        // `cb` with `opaque` is the completion that caller waits for, and `cb` is still valid, as
        // each entry point's `# Safety` requires it to stay valid until it fires. The job is a
        // `FnOnce` and runs one arm, so `cb` fires exactly once; here with the value and a null
        // error.
        Ok(value) => unsafe { cb(value, std::ptr::null_mut(), opaque.get()) },
        // SAFETY: As the `Ok` arm; the error is the fresh `box_error` handle, owned by no one else,
        // so handing it to `cb` transfers its ownership, as the entry point's `_cb_t` documents.
        Err(error) => unsafe { cb(-1, box_error(error), opaque.get()) },
    }));
}

fn deliver_ptr<T, P>(
    client: &Client,
    cb: unsafe extern "C" fn(value: *mut P, error: *mut kafka_common_Error_t, opaque: *mut c_void),
    opaque: SendPtr,
    result: Result<T, Error>,
    to_ptr: impl FnOnce(T) -> *mut P + Send + 'static,
) where
    T: Send + 'static,
    P: 'static,
{
    client.delivery.queue.push(Box::new(move || match result {
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to a `_cb` entry point: every
        // caller of `deliver_ptr` (the `deliver` closure a `_cb` entry point hands `run_cb` or
        // `HandleInner::run_cb`, which run it at most once) passes them on unchanged, so calling
        // `cb` with `opaque` is the completion that caller waits for, and `cb` is still valid, as
        // each entry point's `# Safety` requires it to stay valid until it fires. Each caller's
        // `to_ptr` boxes a fresh handle (`box_consumer_records` or one of the list and map
        // builders), owned by no one else, so handing it to `cb` with a null error transfers its
        // ownership, as the entry point's `_cb_t` documents. The job is a `FnOnce` and runs one
        // arm, so `cb` fires exactly once.
        Ok(value) => unsafe { cb(to_ptr(value), std::ptr::null_mut(), opaque.get()) },
        // SAFETY: As the `Ok` arm, with a null value; the error is the fresh `box_error` handle,
        // owned by no one else, so handing it to `cb` transfers its ownership, as the entry point's
        // `_cb_t` documents.
        Err(error) => unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    }));
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// `assign(Collection<TopicPartition>)`: `partitions` is a list of
/// `kafka_common_TopicPartition_t *`, copied during the call.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.assign(partitions).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_assign_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_assign_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_assign_cb",
            self_,
            move |c| Box::pin(async move { c.assign(partitions).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `subscribe(Collection<String> topics)`: `topics` is a list of
/// `char *`, copied during the call. Replaces the listener registered by
/// an earlier `subscribe_*`, releasing its `self`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `topics` must be a valid list of NUL-terminated
/// `char *`. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `topics` is a valid list of NUL-terminated `char *`,
    // as `list_strings` requires; the strings are copied, so the borrow lasts for this call only.
    let topics = unsafe { list_strings(topics) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.subscribe_with_topics(topics).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_topics_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
    cb: kafka_consumer_Consumer_subscribe_with_topics_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topics` is a valid list of NUL-terminated `char *`,
    // as `list_strings` requires; the strings are copied, so the borrow lasts for this call only.
    let topics = unsafe { list_strings(topics) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_subscribe_with_topics_cb",
            self_,
            move |c| Box::pin(async move { c.subscribe_with_topics(topics).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `subscribe(Collection<String> topics, ConsumerRebalanceListener listener)`:
/// the listener registration is copied during the call (its handle may
/// be destroyed afterwards); its `self` must stay alive until the next
/// `subscribe_*`, `unsubscribe` or the consumer's destruction releases it.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `topics` must be a valid list of NUL-terminated
/// `char *`. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics_listener(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is a valid list of NUL-terminated `char *`,
    // as `list_strings` requires; the strings are copied, so the borrow lasts for this call only.
    let topics = unsafe { list_strings(topics) };
    // SAFETY: Per this function's `# Safety`, `listener` is a live listener handle, as
    // `listener_adapter` requires; the registration is copied, so the handle is borrowed for this
    // call only. Its `self` stays alive until the registration is released, as
    // `kafka_consumer_ConsumerRebalanceListener_new` documents.
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.subscribe_with_topics_listener(topics, listener).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_topics_listener_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics_listener_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    cb: kafka_consumer_Consumer_subscribe_with_topics_listener_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is a valid list of NUL-terminated `char *`,
    // as `list_strings` requires; the strings are copied, so the borrow lasts for this call only.
    let topics = unsafe { list_strings(topics) };
    // SAFETY: Per this function's `# Safety`, `listener` is a live listener handle, as
    // `listener_adapter` requires; the registration is copied, so the handle is borrowed for this
    // call only. Its `self` stays alive until the registration is released, as
    // `kafka_consumer_ConsumerRebalanceListener_new` documents.
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_subscribe_with_topics_listener_cb",
            self_,
            move |c| Box::pin(async move { c.subscribe_with_topics_listener(topics, listener).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `subscribe(SubscriptionPattern pattern)`: the pattern is copied
/// during the call.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `pattern` is a valid subscription-pattern handle,
    // alive for this expression, the only use of the reference, as `subscription_pattern_ref`
    // requires; the value is cloned.
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.subscribe_with_pattern(pattern).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_pattern_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern_cb(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
    cb: kafka_consumer_Consumer_subscribe_with_pattern_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `pattern` is a valid subscription-pattern handle,
    // alive for this expression, the only use of the reference, as `subscription_pattern_ref`
    // requires; the value is cloned.
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_subscribe_with_pattern_cb",
            self_,
            move |c| Box::pin(async move { c.subscribe_with_pattern(pattern).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener listener)`;
/// see `subscribe_with_topics_listener` for the listener's lifetime.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern_listener(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `pattern` is a valid subscription-pattern handle,
    // alive for this expression, the only use of the reference, as `subscription_pattern_ref`
    // requires; the value is cloned.
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    // SAFETY: Per this function's `# Safety`, `listener` is a live listener handle, as
    // `listener_adapter` requires; the registration is copied, so the handle is borrowed for this
    // call only. Its `self` stays alive until the registration is released, as
    // `kafka_consumer_ConsumerRebalanceListener_new` documents.
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.subscribe_with_pattern_listener(pattern, listener).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_pattern_listener_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern_listener_cb(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    cb: kafka_consumer_Consumer_subscribe_with_pattern_listener_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `pattern` is a valid subscription-pattern handle,
    // alive for this expression, the only use of the reference, as `subscription_pattern_ref`
    // requires; the value is cloned.
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    // SAFETY: Per this function's `# Safety`, `listener` is a live listener handle, as
    // `listener_adapter` requires; the registration is copied, so the handle is borrowed for this
    // call only. Its `self` stays alive until the registration is released, as
    // `kafka_consumer_ConsumerRebalanceListener_new` documents.
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_subscribe_with_pattern_listener_cb",
            self_,
            move |c| Box::pin(async move { c.subscribe_with_pattern_listener(pattern, listener).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `unsubscribe()`: releases the registered listener after invoking
/// `onPartitionsLost` / `onPartitionsRevoked` as Java does.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe(
    self_: *mut kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe { deliver_unit(run(self_, move |c| Box::pin(async move { c.unsubscribe().await })), out_err) }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_unsubscribe_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_unsubscribe_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_unsubscribe_cb",
            self_,
            move |c| Box::pin(async move { c.unsubscribe().await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

// ---------------------------------------------------------------------------
// Poll
// ---------------------------------------------------------------------------

/// `poll(Duration timeout)`: `timeout` in milliseconds (negative fails as
/// Java's `IllegalArgumentException`); the records are owned by the
/// caller (`kafka_consumer_ConsumerRecords_destroy`).
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_ConsumerRecords_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let owns = client.owns();
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.poll(ms(timeout)?).await })) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the result to,
    // as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |records| box_consumer_records(records, owns.key, owns.value)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_poll_cb_t = unsafe extern "C" fn(
    value: *mut kafka_consumer_ConsumerRecords_t,
    error: *mut kafka_common_Error_t,
    opaque: *mut c_void,
);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_poll_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let owns = client.owns();
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_poll_cb",
            self_,
            move |c| Box::pin(async move { c.poll(ms(timeout)?).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |records| {
                    box_consumer_records(records, owns.key, owns.value)
                })
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

/// `commitSync()`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync(
    self_: *mut kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe { deliver_unit(run(self_, move |c| Box::pin(async move { c.commit_sync().await })), out_err) }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_commit_sync_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_commit_sync_cb",
            self_,
            move |c| Box::pin(async move { c.commit_sync().await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `commitSync(Duration timeout)`: `timeout` in milliseconds.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.commit_sync_with_timeout(ms(timeout)?).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_with_timeout_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_commit_sync_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_commit_sync_with_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.commit_sync_with_timeout(ms(timeout)?).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)`: a map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`,
/// copied during the call.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `offsets` must be a valid map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one. `out_err` must be a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied,
    // so the borrow lasts for this call only.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.commit_sync_with_offsets(offsets).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_with_offsets_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets_cb(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    cb: kafka_consumer_Consumer_commit_sync_with_offsets_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied,
    // so the borrow lasts for this call only.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_commit_sync_with_offsets_cb",
            self_,
            move |c| Box::pin(async move { c.commit_sync_with_offsets(offsets).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `offsets` must be a valid map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one. `out_err` must be a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied,
    // so the borrow lasts for this call only.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.commit_sync_with_offsets_timeout(offsets, ms(timeout)?).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied,
    // so the borrow lasts for this call only.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.commit_sync_with_offsets_timeout(offsets, ms(timeout)?).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `commitAsync()`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async(
    self_: *mut kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe { deliver_unit(run(self_, move |c| Box::pin(async move { c.commit_async().await })), out_err) }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_async_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_commit_async_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_commit_async_cb",
            self_,
            move |c| Box::pin(async move { c.commit_async().await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `commitAsync(OffsetCommitCallback callback)`: the callback
/// registration is copied during the call; its `self` must stay alive
/// until `onComplete` has fired (or the consumer is destroyed).
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_callback(
    self_: *mut kafka_consumer_Consumer_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.commit_async_with_callback(callback).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_async_with_callback_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_callback_cb(
    self_: *mut kafka_consumer_Consumer_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    cb: kafka_consumer_Consumer_commit_async_with_callback_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_commit_async_with_callback_cb",
            self_,
            move |c| Box::pin(async move { c.commit_async_with_callback(callback).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `offsets` must be a valid map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one. `out_err` must be a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_offsets_callback(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied,
    // so the borrow lasts for this call only.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.commit_async_with_offsets_callback(offsets, callback).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_async_with_offsets_callback_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_offsets_callback_cb(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    cb: kafka_consumer_Consumer_commit_async_with_offsets_callback_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied,
    // so the borrow lasts for this call only.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_commit_async_with_offsets_callback_cb",
            self_,
            move |c| Box::pin(async move { c.commit_async_with_offsets_callback(offsets, callback).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

// ---------------------------------------------------------------------------
// Seek / position
// ---------------------------------------------------------------------------

/// `seek(TopicPartition partition, long offset)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.seek_with_offset(partition, offset).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_with_offset_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
    cb: kafka_consumer_Consumer_seek_with_offset_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_seek_with_offset_cb",
            self_,
            move |c| Box::pin(async move { c.seek_with_offset(partition, offset).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset_and_metadata(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    // SAFETY: Per this function's `# Safety`, `offset_and_metadata` is a valid offset-and-metadata
    // handle, alive for this expression, the only use of the reference, as
    // `offset_and_metadata_ref` requires; the value is cloned.
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.seek_with_offset_and_metadata(partition, offset_and_metadata).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_with_offset_and_metadata_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset_and_metadata_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
    cb: kafka_consumer_Consumer_seek_with_offset_and_metadata_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    // SAFETY: Per this function's `# Safety`, `offset_and_metadata` is a valid offset-and-metadata
    // handle, alive for this expression, the only use of the reference, as
    // `offset_and_metadata_ref` requires; the value is cloned.
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_seek_with_offset_and_metadata_cb",
            self_,
            move |c| Box::pin(async move { c.seek_with_offset_and_metadata(partition, offset_and_metadata).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `seekToBeginning(Collection<TopicPartition>)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.seek_to_beginning(&partitions).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_to_beginning_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_seek_to_beginning_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_seek_to_beginning_cb",
            self_,
            move |c| Box::pin(async move { c.seek_to_beginning(&partitions).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `seekToEnd(Collection<TopicPartition>)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.seek_to_end(&partitions).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_to_end_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_seek_to_end_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_seek_to_end_cb",
            self_,
            move |c| Box::pin(async move { c.seek_to_end(&partitions).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `position(TopicPartition partition)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the value is returned; on failure the error
/// is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.position(&partition).await })) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the result
    // to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |v| v) }
}

/// The completion of the `_cb` twin: `value` on success, `-1` beside
/// an owned `error` otherwise.
pub type kafka_consumer_Consumer_position_cb_t =
    unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_i64` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_i64` requires.
    unsafe { deliver_panic_i64(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    cb: kafka_consumer_Consumer_position_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_position_cb",
            self_,
            move |c| Box::pin(async move { c.position(&partition).await }),
            move |client, result| deliver_i64(client, cb, opaque, result),
        )
    }
}

/// `position(TopicPartition partition, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the value is returned; on failure the error
/// is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.position_with_timeout(&partition, ms(timeout)?).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to
    // write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |v| v) }
}

/// The completion of the `_cb` twin: `value` on success, `-1` beside
/// an owned `error` otherwise.
pub type kafka_consumer_Consumer_position_with_timeout_cb_t =
    unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_i64` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_i64` requires.
    unsafe { deliver_panic_i64(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_position_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, the only use of the reference, as `topic_partition_ref` requires; the
    // value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_position_with_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.position_with_timeout(&partition, ms(timeout)?).await }),
            move |client, result| deliver_i64(client, cb, opaque, result),
        )
    }
}

/// `committed(Set<TopicPartition> partitions)`: an owned map of owned
/// `kafka_common_TopicPartition_t *` to owned
/// `kafka_consumer_OffsetAndMetadata_t *`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.committed(&partitions).await })) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| offset_and_metadata_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_committed_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_committed_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_committed_cb",
            self_,
            move |c| Box::pin(async move { c.committed(&partitions).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_metadata_map(&offsets))
            },
        )
    }
}

/// `committed(Set<TopicPartition> partitions, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.committed_with_timeout(&partitions, ms(timeout)?).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to
    // write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| offset_and_metadata_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_committed_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_committed_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_committed_with_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.committed_with_timeout(&partitions, ms(timeout)?).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_metadata_map(&offsets))
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// `Map<String, List<PartitionInfo>>` as an owned map of owned `char *` to
/// owned `kafka_List_t *` of owned `kafka_common_PartitionInfo_t *`, topics
/// sorted.
fn topic_partition_info_map(topics: HashMap<String, Vec<crate::common::PartitionInfo>>) -> *mut kafka_Map_t {
    let mut entries: Vec<_> = topics.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    box_string_keyed_map(
        entries
            .into_iter()
            .map(|(topic, infos)| (topic, partition_info_list(&infos) as *mut c_void)),
        Some(destroy_list_element),
    )
}

/// Frees a list handle held as a `void *` container element.
///
/// # Safety
///
/// `element` must be an owned list handle not yet destroyed.
unsafe fn destroy_list_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned list handle not yet destroyed,
    // which is what `kafka_List_destroy` requires.
    unsafe { kafka_List_destroy(element as *mut kafka_List_t) }
}

/// `partitionsFor(String topic)`: an owned list of owned
/// `kafka_common_PartitionInfo_t *`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `topic` must be a NUL-terminated string. `out_err`
/// must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.partitions_for(&topic).await })) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |infos| partition_info_list(&infos)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_partitions_for_cb_t =
    unsafe extern "C" fn(value: *mut kafka_List_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    cb: kafka_consumer_Consumer_partitions_for_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let topic = unsafe { c_str_to_string(topic) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_partitions_for_cb",
            self_,
            move |c| Box::pin(async move { c.partitions_for(&topic).await }),
            move |client, result| deliver_ptr(client, cb, opaque, result, move |infos| partition_info_list(&infos)),
        )
    }
}

/// `partitionsFor(String topic, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `topic` must be a NUL-terminated string. `out_err`
/// must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.partitions_for_with_timeout(&topic, ms(timeout)?).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer
    // to write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |infos| partition_info_list(&infos)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_partitions_for_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_List_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    timeout: i64,
    cb: kafka_consumer_Consumer_partitions_for_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let topic = unsafe { c_str_to_string(topic) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_partitions_for_with_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.partitions_for_with_timeout(&topic, ms(timeout)?).await }),
            move |client, result| deliver_ptr(client, cb, opaque, result, move |infos| partition_info_list(&infos)),
        )
    }
}

/// `listTopics()`: an owned map of owned `char *` to owned `kafka_List_t *`
/// of owned `kafka_common_PartitionInfo_t *`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics(
    self_: *mut kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.list_topics().await })) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, topic_partition_info_map) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_list_topics_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_list_topics_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_list_topics_cb",
            self_,
            move |c| Box::pin(async move { c.list_topics().await }),
            move |client, result| deliver_ptr(client, cb, opaque, result, topic_partition_info_map),
        )
    }
}

/// `listTopics(Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.list_topics_with_timeout(ms(timeout)?).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to
    // write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, topic_partition_info_map) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_list_topics_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_list_topics_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_list_topics_with_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.list_topics_with_timeout(ms(timeout)?).await }),
            move |client, result| deliver_ptr(client, cb, opaque, result, topic_partition_info_map),
        )
    }
}

/// `offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch)`: the
/// input maps `kafka_common_TopicPartition_t *` to `int64_t *`; the owned
/// result maps owned `kafka_common_TopicPartition_t *` to owned
/// `kafka_consumer_OffsetAndTimestamp_t *`, unresolved partitions absent.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `timestamps_to_search` must be a valid map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`. `out_err` must be a valid
/// slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `timestamps_to_search` is a valid map of
    // `kafka_common_TopicPartition_t *` to `int64_t *`, as `map_topic_partition_i64` requires; the
    // entries are copied, so the borrow lasts for this call only.
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.offsets_for_times(timestamps_to_search).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write
    // the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| offset_and_timestamp_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_offsets_for_times_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    cb: kafka_consumer_Consumer_offsets_for_times_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `timestamps_to_search` is a valid map of
    // `kafka_common_TopicPartition_t *` to `int64_t *`, as `map_topic_partition_i64` requires; the
    // entries are copied, so the borrow lasts for this call only.
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_offsets_for_times_cb",
            self_,
            move |c| Box::pin(async move { c.offsets_for_times(timestamps_to_search).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_timestamp_map(&offsets))
            },
        )
    }
}

/// `offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `timestamps_to_search` must be a valid map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`. `out_err` must be a valid
/// slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `timestamps_to_search` is a valid map of
    // `kafka_common_TopicPartition_t *` to `int64_t *`, as `map_topic_partition_i64` requires; the
    // entries are copied, so the borrow lasts for this call only.
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.offsets_for_times_with_timeout(timestamps_to_search, ms(timeout)?).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid
    // pointer to write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| offset_and_timestamp_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_offsets_for_times_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_offsets_for_times_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `timestamps_to_search` is a valid map of
    // `kafka_common_TopicPartition_t *` to `int64_t *`, as `map_topic_partition_i64` requires; the
    // entries are copied, so the borrow lasts for this call only.
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_offsets_for_times_with_timeout_cb",
            self_,
            move |c| {
                Box::pin(async move { c.offsets_for_times_with_timeout(timestamps_to_search, ms(timeout)?).await })
            },
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_timestamp_map(&offsets))
            },
        )
    }
}

/// `beginningOffsets(Collection<TopicPartition> partitions)`: an owned
/// map of owned `kafka_common_TopicPartition_t *` to owned `int64_t *`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.beginning_offsets(&partitions).await })) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write
    // the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_beginning_offsets_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_beginning_offsets_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_beginning_offsets_cb",
            self_,
            move |c| Box::pin(async move { c.beginning_offsets(&partitions).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets))
            },
        )
    }
}

/// `beginningOffsets(Collection<TopicPartition> partitions, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.beginning_offsets_with_timeout(&partitions, ms(timeout)?).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid
    // pointer to write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_beginning_offsets_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_beginning_offsets_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_beginning_offsets_with_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.beginning_offsets_with_timeout(&partitions, ms(timeout)?).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets))
            },
        )
    }
}

/// `endOffsets(Collection<TopicPartition> partitions)`: an owned map of
/// owned `kafka_common_TopicPartition_t *` to owned `int64_t *`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.end_offsets(&partitions).await })) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_end_offsets_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_end_offsets_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_end_offsets_cb",
            self_,
            move |c| Box::pin(async move { c.end_offsets(&partitions).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets))
            },
        )
    }
}

/// `endOffsets(Collection<TopicPartition> partitions, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is returned; on failure the
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.end_offsets_with_timeout(&partitions, ms(timeout)?).await })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to
    // write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_end_offsets_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_ptr` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_end_offsets_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_end_offsets_with_timeout_cb",
            self_,
            move |c| Box::pin(async move { c.end_offsets_with_timeout(&partitions, ms(timeout)?).await }),
            move |client, result| {
                deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets))
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Flow control / lifecycle
// ---------------------------------------------------------------------------

/// `pause(Collection<TopicPartition>)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.pause(&partitions).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_pause_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_pause_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_pause_cb",
            self_,
            move |c| Box::pin(async move { c.pause(&partitions).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `resume(Collection<TopicPartition>)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `partitions` must be a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.resume(&partitions).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_resume_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_resume_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_resume_cb",
            self_,
            move |c| Box::pin(async move { c.resume(&partitions).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `enforceRebalance()`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance(
    self_: *mut kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.enforce_rebalance().await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_enforce_rebalance_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_enforce_rebalance_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_enforce_rebalance_cb",
            self_,
            move |c| Box::pin(async move { c.enforce_rebalance().await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `enforceRebalance(String reason)`.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `reason` must be a NUL-terminated string. `out_err`
/// must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance_with_reason(
    self_: *mut kafka_consumer_Consumer_t,
    reason: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `reason` is a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let reason = unsafe { c_str_to_string(reason) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| {
                Box::pin(async move { c.enforce_rebalance_with_reason(&reason).await })
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_enforce_rebalance_with_reason_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance_with_reason_cb(
    self_: *mut kafka_consumer_Consumer_t,
    reason: *const c_char,
    cb: kafka_consumer_Consumer_enforce_rebalance_with_reason_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `reason` is a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let reason = unsafe { c_str_to_string(reason) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_enforce_rebalance_with_reason_cb",
            self_,
            move |c| Box::pin(async move { c.enforce_rebalance_with_reason(&reason).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `close()`: `close(CloseOptions)` with the defaults. The handle stays
/// valid, to be destroyed afterwards.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close(
    self_: *mut kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe { deliver_unit(run(self_, move |c| Box::pin(async move { c.close().await })), out_err) }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_close_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_close_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_close_cb",
            self_,
            move |c| Box::pin(async move { c.close().await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `close(CloseOptions options)`: the options are copied during the call.
///
/// Blocking (CLAUDE.md §4 rule 5): the interface methods it triggers
/// run on the calling thread. A call while another operation is in
/// flight fails with the `ConcurrentModificationException`
/// translation (`LocalConcurrentModification`).
///
/// # Safety
///
/// `self_` must be a live consumer handle and the other parameters valid for
/// their documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_with_options(
    self_: *mut kafka_consumer_Consumer_t,
    options: *const kafka_consumer_CloseOptions_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `options` is a valid close-options handle, alive for
    // this expression, the only use of the reference; no setter runs on it meanwhile, since the
    // setters' `# Safety` excludes any other call using the handle at the same time, so
    // `close_options_ref`'s requirement holds. The value is cloned.
    let options = unsafe { close_options_ref(options) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, kept alive for
    // this call, as `run` requires.
    unsafe {
        deliver_unit(
            run(self_, move |c| Box::pin(async move { c.close_with_options(options).await })),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_close_with_options_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of `run_cb`: to
    // the job `deliver_void` queues when `acquire` fails, or to the task `Client::spawn` registers;
    // nothing that can panic runs after either. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_with_options_cb(
    self_: *mut kafka_consumer_Consumer_t,
    options: *const kafka_consumer_CloseOptions_t,
    cb: kafka_consumer_Consumer_close_with_options_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `options` is a valid close-options handle, alive for
    // this expression, the only use of the reference; no setter runs on it meanwhile, since the
    // setters' `# Safety` excludes any other call using the handle at the same time, so
    // `close_options_ref`'s requirement holds. The value is cloned.
    let options = unsafe { close_options_ref(options) }.clone();
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer handle, as `run_cb`
    // requires; `run_cb` uses it only before it returns, cloning the `Arc` for the task it spawns.
    unsafe {
        run_cb(
            "kafka_consumer_Consumer_close_with_options_cb",
            self_,
            move |c| Box::pin(async move { c.close_with_options(options).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

// ---------------------------------------------------------------------------
// Sync methods
// ---------------------------------------------------------------------------

/// Reads the consumer under the single-owner flag, or fails with the
/// `LOCAL_CONCURRENT_MODIFICATION` Java's `acquire()` throws when another
/// call holds it (see the module docs).
///
/// # Safety
///
/// `self_` must be a live handle, as [`client_ref`] describes, kept alive for
/// this call.
unsafe fn read<T>(self_: *const kafka_consumer_Consumer_t, f: impl FnOnce(&DynConsumer) -> T) -> Result<T, Error> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `client_ref` requires. Exclusive access to the consumer comes from `acquire`, not from
    // this reference.
    let client = unsafe { client_ref(self_) };
    let mut guard = client.acquire()?;
    Ok(f(guard.consumer_mut()))
}

/// `assignment()`: an owned list of owned `kafka_common_TopicPartition_t *`,
/// sorted by topic and partition; stores `LOCAL_CONCURRENT_MODIFICATION` in
/// `*out_err` and returns `NULL` while another call holds the consumer, as
/// Java's `acquire()` throws `ConcurrentModificationException`.
///
/// # Safety
///
/// `self_` must be a live handle and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assignment(
    self_: *const kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `read` requires.
    let assignment = unsafe { read(self_, |c| c.assignment()) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(assignment, out_err, |a| sorted_topic_partition_list(a.iter())) }
}

/// `subscription()`: an owned list of owned `char *`, sorted; stores
/// `LOCAL_CONCURRENT_MODIFICATION` in `*out_err` and returns `NULL` while
/// another call holds the consumer, as Java's `acquire()` throws
/// `ConcurrentModificationException`.
///
/// # Safety
///
/// `self_` must be a live handle and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscription(
    self_: *const kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `read` requires.
    let subscription = unsafe { read(self_, |c| c.subscription()) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(subscription, out_err, |s| sorted_string_list(s.iter())) }
}

/// `paused()`: an owned list of owned `kafka_common_TopicPartition_t *`,
/// sorted; stores `LOCAL_CONCURRENT_MODIFICATION` in `*out_err` and returns
/// `NULL` while another call holds the consumer, as Java's `acquire()` throws
/// `ConcurrentModificationException`.
///
/// # Safety
///
/// `self_` must be a live handle and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_paused(
    self_: *const kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `read` requires.
    let paused = unsafe { read(self_, |c| c.paused()) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the result
    // to, as `deliver_value` requires.
    unsafe { deliver_value(paused, out_err, |p| sorted_topic_partition_list(p.iter())) }
}

/// `clientId()`: borrowed from the handle for its lifetime.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_client_id(self_: *const kafka_consumer_Consumer_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // as `client_ref` requires. The pointer returned is into the `CString` the client owns, which
    // lives at least as long as the handle holds the client, as the docs above state.
    unsafe { client_ref(self_) }.client_id.as_ptr()
}

/// `currentLag(TopicPartition topicPartition)`: the lag, or `-1` for
/// `OptionalLong.empty()`; stores `LOCAL_CONCURRENT_MODIFICATION` in `*out_err`
/// and returns -1 while another call holds the consumer, as Java's `acquire()`
/// throws `ConcurrentModificationException`. Since -1 is also the empty lag,
/// the caller tells the two apart by `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle, `topic_partition` a valid handle and
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_current_lag(
    self_: *const kafka_consumer_Consumer_t,
    topic_partition: *const kafka_common_TopicPartition_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `topic_partition` is a valid topic-partition handle,
    // kept alive for this call, which bounds every use of `tp`, as `topic_partition_ref` requires.
    let tp: &TopicPartition = unsafe { topic_partition_ref(topic_partition) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `read` requires.
    let lag = unsafe { read(self_, |c| c.current_lag(tp).unwrap_or(-1)) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(lag, out_err, |l| l) }
}

/// `metrics()`: an owned `kafka_Map_t` of owned `kafka_common_MetricName_t *`
/// to owned `kafka_common_metrics_KafkaMetric_t *`, names compared by value
/// in `kafka_Map_get`. Read through the consumer's handle, not the
/// single-owner flag, so it answers while another call holds the consumer:
/// Java's `AsyncKafkaConsumer.metrics()` takes no `acquire()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_metrics(self_: *const kafka_consumer_Consumer_t) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // as `client_ref` requires; the handle it reads through is shared state, never the consumer an
    // operation may be borrowing.
    box_metrics_map(unsafe { client_ref(self_) }.handle().metrics())
}

/// `groupMetadata()`: an owned handle freed with
/// `kafka_consumer_ConsumerGroupMetadata_destroy`; stores
/// `LOCAL_CONCURRENT_MODIFICATION` in `*out_err` and returns `NULL` while
/// another call holds the consumer, as Java's `acquire()` throws
/// `ConcurrentModificationException`.
///
/// # Safety
///
/// `self_` must be a live handle and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_group_metadata(
    self_: *const kafka_consumer_Consumer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `read` requires.
    let metadata = unsafe { read(self_, |c| box_group_metadata(c.group_metadata())) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(metadata, out_err, |m| m) }
}

/// `wakeup()`: interrupts the operation in flight (or the next one) with
/// the `WakeupException` translation. Callable from any thread, at any time.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_wakeup(self_: *const kafka_consumer_Consumer_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // as `client_ref` requires. `wakeup` goes through the reentrancy handle, never the consumer, so
    // it needs no `acquire` and may run while an operation holds the guard; `Client` is `Sync`.
    unsafe { client_ref(self_) }.handle.wakeup();
}

/// `handle()`: the reentrancy handle a listener calls the consumer back
/// through (consumer-threading.md §31, §41), owned by the caller
/// (`kafka_consumer_ConsumerHandle_destroy`) and usable after the consumer
/// is destroyed, when its operations fail.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_handle(
    self_: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_ConsumerHandle_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call, as
    // `client_ref` requires; `box_consumer_handle` keeps only a `Weak` to the client, so the new
    // handle does not borrow `self_`.
    box_consumer_handle(unsafe { client_ref(self_) })
}

// ---------------------------------------------------------------------------
// Callback pump (CLAUDE.md §4 rule 5)
// ---------------------------------------------------------------------------

/// Fired once, from a Rust task, each time the client's callbacks vector
/// goes from empty to non-empty; it may only schedule a call to
/// [`kafka_consumer_Consumer__execute_callbacks`], never run callbacks.
pub type kafka_consumer_Consumer_callbacks_notify_fn_t = unsafe extern "C" fn(opaque: *mut c_void);

/// Runs every queued callback serially on the calling thread and returns
/// how many ran. Concurrent calls are serialized; a nested call from inside
/// a callback returns 0.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer__execute_callbacks(self_: *const kafka_consumer_Consumer_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `client_ref` requires; a callback that runs here and calls the consumer again takes its
    // own reference, and none takes a `&mut` to the handle (its destroy is excluded while this call
    // uses it).
    unsafe { client_ref(self_) }.delivery.queue.execute()
}

/// Installs the hook fired once each time the callback vector goes from
/// empty to non-empty (see
/// [`kafka_consumer_Consumer_callbacks_notify_fn_t`]).
///
/// # Safety
///
/// `self_` must be a live handle; `opaque` stays valid while the hook is
/// installed and until a call of it in flight returns: a later call of this
/// function does not wait for one.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer__set_callbacks_notify(
    self_: *const kafka_consumer_Consumer_t,
    notify: kafka_consumer_Consumer_callbacks_notify_fn_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // as `client_ref` requires. `notify` and `opaque` are stored as a pair, and `opaque` stays
    // valid while the hook is installed and until a call in flight returns, which
    // `CallbackQueue::notify_if` relies on when it fires the hook after releasing the
    // registration's lock.
    unsafe { client_ref(self_) }.delivery.queue.set_notify(Some(notify), opaque);
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::callback_result::{ResultSink, void_sink};
    use crate::ffi::consumer::mock_consumer::{
        kafka_consumer_MockConsumer__as_Consumer, kafka_consumer_MockConsumer_destroy, kafka_consumer_MockConsumer_new,
        kafka_consumer_MockConsumer_t,
    };

    /// A mock consumer handle and its `Consumer` view.
    fn new_mock() -> (*mut kafka_consumer_MockConsumer_t, *const kafka_consumer_Consumer_t) {
        let mut error = ptr::null_mut();
        // SAFETY: Test code: a NUL-terminated literal and a valid slot pointing at null.
        let mock = unsafe { kafka_consumer_MockConsumer_new(c"earliest".as_ptr(), &mut error) };
        assert!(error.is_null());
        // SAFETY: Test code: `mock` is the live handle just created.
        (mock, unsafe { kafka_consumer_MockConsumer__as_Consumer(mock) })
    }

    /// Registers `sink` with the consumer's own registry, as its adapters do.
    fn register(consumer: *const kafka_consumer_Consumer_t, sink: ResultSink) -> i64 {
        // SAFETY: Test code: `consumer` is a live handle for the expression.
        unsafe { client_ref(consumer) }.delivery.results().register(sink)
    }

    fn channel_sink() -> (ResultSink, std::sync::mpsc::Receiver<Result<(), Error>>) {
        let (tx, rx) = std::sync::mpsc::channel();
        (void_sink(move |r| tx.send(r).unwrap()), rx)
    }

    #[test]
    fn callback_results_are_delivered_once() {
        let (mock, consumer) = new_mock();
        let (sink, rx) = channel_sink();
        let id = register(consumer, sink);
        // SAFETY: Test code: `consumer` is the live handle that issued `id`, `error` is null and the
        // void sink does not take `result`.
        unsafe { kafka_consumer_Consumer__set_callback_result(consumer, id, ptr::null_mut(), ptr::null_mut()) };
        assert!(rx.recv().unwrap().is_ok());
        // A repeated report is ignored and its error freed.
        // SAFETY: Test code: as above, with `error` an owned handle, handed over.
        unsafe {
            kafka_consumer_Consumer__set_callback_result(
                consumer,
                id,
                ptr::null_mut(),
                box_error(Error::timeout("repeated")),
            )
        };
        assert!(rx.try_recv().is_err());
        // SAFETY: Test code: `mock` is a live handle, destroyed once here.
        unsafe { kafka_consumer_MockConsumer_destroy(mock) };
    }

    #[test]
    fn a_reported_error_wins_over_the_value() {
        let (mock, consumer) = new_mock();
        let (sink, rx) = channel_sink();
        let id = register(consumer, sink);
        let mut value = 0_u8;
        // SAFETY: Test code: `consumer` is the live handle that issued `id`; `error` is an owned
        // handle, handed over, and the sink ignores `result`, which stays this test's.
        unsafe {
            kafka_consumer_Consumer__set_callback_result(
                consumer,
                id,
                ptr::addr_of_mut!(value) as *mut c_void,
                box_error(Error::timeout("failed")),
            )
        };
        assert_eq!(rx.recv().unwrap().unwrap_err().message(), "failed");
        // SAFETY: Test code: `mock` is a live handle, destroyed once here.
        unsafe { kafka_consumer_MockConsumer_destroy(mock) };
    }

    /// Each consumer keeps its own ids: one reported to another consumer is
    /// ignored there and stays pending on its own.
    #[test]
    fn ids_are_per_consumer() {
        let (mock_a, a) = new_mock();
        let (mock_b, b) = new_mock();
        let (sink, rx) = channel_sink();
        let id = register(a, sink);
        // SAFETY: Test code: `b` is a live handle; the id is not one of its own, which the function
        // ignores, freeing the owned error.
        unsafe {
            kafka_consumer_Consumer__set_callback_result(b, id, ptr::null_mut(), box_error(Error::timeout("wrong")))
        };
        assert!(rx.try_recv().is_err(), "not delivered through another consumer");
        // SAFETY: Test code: `a` is the live handle that issued `id`, with a null error and value.
        unsafe { kafka_consumer_Consumer__set_callback_result(a, id, ptr::null_mut(), ptr::null_mut()) };
        assert!(rx.recv().unwrap().is_ok());
        // SAFETY: Test code: each handle is live and destroyed once here.
        unsafe {
            kafka_consumer_MockConsumer_destroy(mock_a);
            kafka_consumer_MockConsumer_destroy(mock_b);
        }
    }

    /// An invoker's id is routed by its tag, whatever consumer, or `NULL`,
    /// the report names.
    #[test]
    fn invoker_ids_need_no_consumer() {
        let (mock, consumer) = new_mock();
        for named in [ptr::null(), consumer] {
            let (sink, rx) = channel_sink();
            let id = INVOKER_RESULTS.register(sink);
            // SAFETY: Test code: `id` is an invoker's, so the consumer handle is not read; `error` is
            // null and the void sink does not take `result`.
            unsafe { kafka_consumer_Consumer__set_callback_result(named, id, ptr::null_mut(), ptr::null_mut()) };
            assert!(rx.recv().unwrap().is_ok());
        }
        // SAFETY: Test code: `mock` is a live handle, destroyed once here.
        unsafe { kafka_consumer_MockConsumer_destroy(mock) };
    }

    #[test]
    fn negative_timeout_is_the_java_illegal_argument() {
        let error = ms(-1).unwrap_err();
        assert!(error.is_local_illegal_argument_error());
        assert_eq!(error.message(), "Timeout must not be negative");
        assert_eq!(ms(10).unwrap(), Duration::from_millis(10));
    }
}
