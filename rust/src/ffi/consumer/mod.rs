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
//! `kafka_consumer_Consumer_t` is the interface handle, Rust's
//! `Box<dyn Consumer>`. `kafka_consumer_KafkaConsumer_new` returns one owned
//! by the caller (`KafkaConsumer::new` returns the delegate it chose, not a
//! `KafkaConsumer`), freed with [`kafka_consumer_Consumer_destroy`];
//! `kafka_consumer_MockConsumer__as_Consumer` returns a borrowed view of a
//! `kafka_consumer_MockConsumer_t`, valid until that class handle is
//! destroyed and never passed to `Consumer_destroy`. Both point at the same
//! thing: a [`ConsumerClassHandle`] whose first field is the shared
//! [`Client`].
//!
//! # Threading
//!
//! Java's `KafkaConsumer` is not safe for multi-threaded access and reports
//! a concurrent call with `ConcurrentModificationException`; the handle
//! keeps that contract with a single-owner flag ([`Client::acquire`]). Every
//! operation that takes `&mut self` in Rust takes `kafka_consumer_Consumer_t *`
//! and holds the flag for its duration — the blocking form while it runs on
//! the calling thread, the `_cb` form until its completion is queued. The
//! sync getters (`assignment`, `metrics`, ...) take the flag for the call
//! and, when an operation is in flight, return the empty value
//! (`-1` / `NULL` for scalars and handles), there being no error slot to
//! report the Java exception through. `wakeup` and the `ConsumerHandle`
//! never touch the consumer itself and are callable at any time.
//!
//! # Callbacks (CLAUDE.md §4 rule 5)
//!
//! A blocking operation invokes the interface methods it triggers
//! (`ConsumerRebalanceListener`, `OffsetCommitCallback`) directly on the
//! calling thread, as Java does; a `_cb` operation queues them on the
//! client's callbacks vector, which [`kafka_consumer_Consumer_execute_callbacks`]
//! runs on the pumping thread, and queues its own completion the same way.
//! Those interface methods are `async` in Rust, so their C functions return
//! `void`, take a trailing `int64_t callback_id` and report through
//! [`kafka_consumer_Consumer_set_callback_result`], from inside the
//! function or later from any thread; the operation waits for that report
//! (a C implementation that never reports hangs it, as a Java listener that
//! never returns would). [`kafka_consumer_Consumer_destroy`] runs the
//! callbacks still pending so each fires exactly once.
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
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::common::{Error, TopicPartition};
use crate::consumer::{Consumer, ConsumerHandle, MockConsumer};
use crate::ffi::callback_queue::{CallbackJob, CallbackQueue, SendPtr};
use crate::ffi::common::metric_name::{box_metric_name, kafka_common_MetricName_destroy, metric_name_eq};
use crate::ffi::common::metrics::kafka_metric::{box_kafka_metric, kafka_common_metrics_KafkaMetric_destroy};
use crate::ffi::common::partition_info::partition_info_list;
use crate::ffi::common::topic_partition::{
    kafka_common_TopicPartition_t, list_topic_partitions, map_topic_partition_i64, sorted_topic_partition_list,
    topic_partition_i64_map, topic_partition_ref,
};
use crate::ffi::common::{box_error, init_default_logger, kafka_common_Error_t, take_error};
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
use crate::ffi::kafka_future::block_on;
use crate::ffi::util::{
    GenericValue, box_map, box_string_keyed_map, c_str_to_string, kafka_List_destroy, kafka_List_t, kafka_Map_t,
    list_strings, owned_c_string, sorted_string_list,
};

/// Opaque handle to a `Consumer<K, V>` (see the module docs).
#[repr(C)]
pub struct kafka_consumer_Consumer_t {
    _private: [u8; 0],
}

/// The consumer behind the boundary: keys and values are the C caller's
/// `void *`s.
pub(crate) type DynConsumer = dyn Consumer<GenericValue, GenericValue>;

/// A pinned, sendable operation future borrowing the consumer.
pub(crate) type OpFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// What a handle wraps: the delegate `KafkaConsumer::new` chose, or the
/// mock, kept concrete so `kafka_consumer_MockConsumer_*` can reach its
/// inherent methods.
pub(crate) enum ConsumerKind {
    Async(Box<DynConsumer>),
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
}

impl Delivery {
    /// Runs `job` now when a blocking operation is on the calling thread,
    /// queues it otherwise.
    pub(crate) fn invoke(&self, job: CallbackJob) {
        if self.blocking.load(Ordering::Acquire) {
            job();
        } else {
            self.queue.push(job);
        }
    }

    #[cfg(test)]
    pub(crate) fn queue(&self) -> &CallbackQueue {
        &self.queue
    }

    #[cfg(test)]
    pub(crate) fn for_tests(queue: CallbackQueue) -> Self {
        Self { queue, blocking: AtomicBool::new(false) }
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

// SAFETY: the consumer is only reached through `acquire`, which hands out
// exclusive access one caller at a time, and everything else is `Sync`.
unsafe impl Send for Client {}
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

    pub(crate) fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }

    pub(crate) fn handle(&self) -> &ConsumerHandle {
        &self.handle
    }

    pub(crate) fn owns(&self) -> Owns {
        self.owns
    }

    /// Spawns `future` on the client's runtime and remembers it for `destroy`.
    pub(crate) fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = self.runtime.spawn(future);
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    /// Drives `future` on the calling thread with the blocking flag set, so
    /// the interface methods it triggers run there too (see the module docs).
    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.delivery.blocking.store(true, Ordering::Release);
        let output = block_on(Some(&self.runtime), future);
        self.delivery.blocking.store(false, Ordering::Release);
        output
    }
}

/// The class handle behind `kafka_consumer_Consumer_t` (from
/// `KafkaConsumer_new`) and `kafka_consumer_MockConsumer_t`: the shared
/// client and the runtime that drives it. `#[repr(C)]` with the client
/// first, so the `Consumer_t` pointer is the handle pointer and
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
            delivery: Arc::new(Delivery { queue: CallbackQueue::new(), blocking: AtomicBool::new(false) }),
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

    /// Awaits every spawned task, fires the pending callbacks, drops the
    /// consumer inside the runtime context and shuts the runtime down.
    ///
    /// The queue is drained *while* the tasks are awaited: a `_cb` task may
    /// be waiting for the report of a listener or commit-callback invocation
    /// that sits queued for `_execute_callbacks`, so awaiting it first would
    /// never return. Every pending callback thus fires exactly once, on the
    /// destroying thread (CLAUDE.md §4 rule 5).
    pub(crate) fn destroy(self) {
        let Self { client, runtime } = self;
        loop {
            let tasks: Vec<JoinHandle<()>> = std::mem::take(&mut *client.tasks.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            runtime.block_on(async {
                for mut task in tasks {
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
/// `consumer` must be a live handle from `KafkaConsumer_new` or an
/// `__as_Consumer` view.
pub(crate) unsafe fn client_ref<'a>(consumer: *const kafka_consumer_Consumer_t) -> &'a Arc<Client> {
    unsafe { &*(consumer as *const Arc<Client>) }
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

/// What a pending async interface call does with its result.
pub(crate) type ResultSink = Box<dyn FnOnce(Result<(), Error>) + Send>;

static NEXT_CALLBACK_ID: AtomicI64 = AtomicI64::new(1);
/// The sinks awaiting a `set_callback_result`, by id. A `Vec` because it
/// can be built in a `const` context; it holds the in-flight callbacks
/// only, a handful at most.
static PENDING: Mutex<Vec<(i64, ResultSink)>> = Mutex::new(Vec::new());

/// Registers `sink` and returns the id C reports with.
pub(crate) fn register_callback_result(sink: ResultSink) -> i64 {
    let id = NEXT_CALLBACK_ID.fetch_add(1, Ordering::Relaxed);
    PENDING.lock().unwrap().push((id, sink));
    id
}

/// Delivers `result` to the sink registered under `id`; `false` for an
/// unknown (or already reported) id.
pub(crate) fn complete_callback_result(id: i64, result: Result<(), Error>) -> bool {
    let sink = {
        let mut pending = PENDING.lock().unwrap();
        pending
            .iter()
            .position(|(pending_id, _)| *pending_id == id)
            .map(|index| pending.swap_remove(index).1)
    };
    match sink {
        Some(sink) => {
            sink(result);
            true
        },
        None => false,
    }
}

/// Reports the result of an async interface method (`ConsumerRebalanceListener`,
/// `OffsetCommitCallback`) invoked with `callback_id` (CLAUDE.md §4 rule 3):
/// `result` is `NULL` for success or an owned `kafka_common_Error_t *` for
/// failure, taken over by Rust. Callable from inside the method or later,
/// from any thread; the ids are process-unique, so `self_` only names the
/// consumer that issued the call. A second report for the same id, or one
/// for an unknown id, is ignored (its error freed).
///
/// # Safety
///
/// `result` must be null or a valid owned error not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_set_callback_result(
    self_: *const kafka_consumer_Consumer_t,
    callback_id: i64,
    result: *mut c_void,
) {
    let _ = self_;
    let result = match unsafe { take_error(result as *mut kafka_common_Error_t) } {
        Some(error) => Err(error),
        None => Ok(()),
    };
    complete_callback_result(callback_id, result);
}

// ---------------------------------------------------------------------------
// Operation plumbing
// ---------------------------------------------------------------------------

/// Runs an operation on the calling thread under the single-owner flag.
///
/// # Safety
///
/// `self_` must be a live handle.
unsafe fn run<T, F>(self_: *mut kafka_consumer_Consumer_t, f: F) -> Result<T, Error>
where
    F: for<'a> FnOnce(&'a mut DynConsumer) -> OpFuture<'a, T>,
{
    let client = unsafe { client_ref(self_) };
    let mut guard = client.acquire()?;
    client.block_on(f(guard.consumer_mut()))
}

/// Runs an operation on the client's runtime under the single-owner flag
/// and hands its result to `deliver`, which queues the completion; a
/// rejected call is delivered the same way.
///
/// # Safety
///
/// `self_` must be a live handle.
unsafe fn run_cb<T, F, D>(self_: *mut kafka_consumer_Consumer_t, f: F, deliver: D)
where
    T: Send + 'static,
    F: for<'a> FnOnce(&'a mut DynConsumer) -> OpFuture<'a, T> + Send + 'static,
    D: FnOnce(&Client, Result<T, Error>) + Send + 'static,
{
    let client = unsafe { client_ref(self_) };
    match client.acquire() {
        Err(error) => deliver(client, Err(error)),
        Ok(mut guard) => {
            let client = Arc::clone(client);
            let task_client = Arc::clone(&client);
            client.spawn(async move {
                let result = f(guard.consumer_mut()).await;
                deliver(&task_client, result);
                drop(guard);
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
        .push(Box::new(move || unsafe { cb(error_slot(result), opaque.get()) }));
}

fn deliver_i64(client: &Client, cb: I64Cb, opaque: SendPtr, result: Result<i64, Error>) {
    client.delivery.queue.push(Box::new(move || match result {
        Ok(value) => unsafe { cb(value, std::ptr::null_mut(), opaque.get()) },
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
        Ok(value) => unsafe { cb(to_ptr(value), std::ptr::null_mut(), opaque.get()) },
        Err(error) => unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    }));
}

/// Delivers a blocking operation's value through `out`, or returns its
/// error (CLAUDE.md §4 error slot).
///
/// # Safety
///
/// `out` must be a valid pointer.
unsafe fn out_slot<T, P>(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.assign(partitions).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_assign_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assign_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_assign_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let topics = unsafe { list_strings(topics) };
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.subscribe_with_topics(topics).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_topics_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
    cb: kafka_consumer_Consumer_subscribe_with_topics_cb_t,
    opaque: *mut c_void,
) {
    let topics = unsafe { list_strings(topics) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics_listener(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
) -> *mut kafka_common_Error_t {
    let client = unsafe { client_ref(self_) };
    let topics = unsafe { list_strings(topics) };
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.subscribe_with_topics_listener(topics, listener).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_topics_listener_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_topics_listener_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topics: *const kafka_List_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    cb: kafka_consumer_Consumer_subscribe_with_topics_listener_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    let topics = unsafe { list_strings(topics) };
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
) -> *mut kafka_common_Error_t {
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.subscribe_with_pattern(pattern).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_pattern_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern_cb(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
    cb: kafka_consumer_Consumer_subscribe_with_pattern_cb_t,
    opaque: *mut c_void,
) {
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern_listener(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
) -> *mut kafka_common_Error_t {
    let client = unsafe { client_ref(self_) };
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.subscribe_with_pattern_listener(pattern, listener).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_subscribe_with_pattern_listener_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscribe_with_pattern_listener_cb(
    self_: *mut kafka_consumer_Consumer_t,
    pattern: *const kafka_consumer_SubscriptionPattern_t,
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    cb: kafka_consumer_Consumer_subscribe_with_pattern_listener_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    let pattern = unsafe { subscription_pattern_ref(pattern) }.clone();
    let listener = unsafe { listener_adapter(listener, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe(
    self_: *mut kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.unsubscribe().await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_unsubscribe_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_unsubscribe_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_unsubscribe_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    out_poll: *mut *mut kafka_consumer_ConsumerRecords_t,
) -> *mut kafka_common_Error_t {
    let client = unsafe { client_ref(self_) };
    let owns = client.owns();
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.poll(ms(timeout)?).await })) };
    unsafe { out_slot(result, out_poll, |records| box_consumer_records(records, owns.key, owns.value)) }
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
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_poll_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_poll_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    let owns = client.owns();
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync(
    self_: *mut kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.commit_sync().await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_commit_sync_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.commit_sync_with_timeout(ms(timeout)?).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_with_timeout_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_commit_sync_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.commit_sync_with_offsets(offsets).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_with_offsets_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets_cb(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    cb: kafka_consumer_Consumer_commit_sync_with_offsets_cb_t,
    opaque: *mut c_void,
) {
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    timeout: i64,
) -> *mut kafka_common_Error_t {
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.commit_sync_with_offsets_timeout(offsets, ms(timeout)?).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb_t,
    opaque: *mut c_void,
) {
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async(
    self_: *mut kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.commit_async().await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_async_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_commit_async_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_callback(
    self_: *mut kafka_consumer_Consumer_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
) -> *mut kafka_common_Error_t {
    let client = unsafe { client_ref(self_) };
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.commit_async_with_callback(callback).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_async_with_callback_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_callback_cb(
    self_: *mut kafka_consumer_Consumer_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    cb: kafka_consumer_Consumer_commit_async_with_callback_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_offsets_callback(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
) -> *mut kafka_common_Error_t {
    let client = unsafe { client_ref(self_) };
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.commit_async_with_offsets_callback(offsets, callback).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_commit_async_with_offsets_callback_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_commit_async_with_offsets_callback_cb(
    self_: *mut kafka_consumer_Consumer_t,
    offsets: *const kafka_Map_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    cb: kafka_consumer_Consumer_commit_async_with_offsets_callback_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
) -> *mut kafka_common_Error_t {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.seek_with_offset(partition, offset).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_with_offset_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
    cb: kafka_consumer_Consumer_seek_with_offset_cb_t,
    opaque: *mut c_void,
) {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset_and_metadata(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
) -> *mut kafka_common_Error_t {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.seek_with_offset_and_metadata(partition, offset_and_metadata).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_with_offset_and_metadata_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_with_offset_and_metadata_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
    cb: kafka_consumer_Consumer_seek_with_offset_and_metadata_cb_t,
    opaque: *mut c_void,
) {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.seek_to_beginning(&partitions).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_to_beginning_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_beginning_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_seek_to_beginning_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.seek_to_end(&partitions).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_seek_to_end_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_seek_to_end_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_seek_to_end_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
            self_,
            move |c| Box::pin(async move { c.seek_to_end(&partitions).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

/// `position(TopicPartition partition)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the value is delivered through the
/// trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    out_position: *mut i64,
) -> *mut kafka_common_Error_t {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.position(&partition).await })) };
    unsafe { out_slot(result, out_position, |v| v) }
}

/// The completion of the `_cb` twin: `value` on success, `-1` beside
/// an owned `error` otherwise.
pub type kafka_consumer_Consumer_position_cb_t =
    unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    cb: kafka_consumer_Consumer_position_cb_t,
    opaque: *mut c_void,
) {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
            self_,
            move |c| Box::pin(async move { c.position(&partition).await }),
            move |client, result| deliver_i64(client, cb, opaque, result),
        )
    }
}

/// `position(TopicPartition partition, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the value is delivered through the
/// trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    out_position_with_timeout: *mut i64,
) -> *mut kafka_common_Error_t {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.position_with_timeout(&partition, ms(timeout)?).await })
        })
    };
    unsafe { out_slot(result, out_position_with_timeout, |v| v) }
}

/// The completion of the `_cb` twin: `value` on success, `-1` beside
/// an owned `error` otherwise.
pub type kafka_consumer_Consumer_position_with_timeout_cb_t =
    unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_position_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_position_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_committed: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.committed(&partitions).await })) };
    unsafe { out_slot(result, out_committed, |offsets| offset_and_metadata_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_committed_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_committed_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    out_committed_with_timeout: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.committed_with_timeout(&partitions, ms(timeout)?).await })
        })
    };
    unsafe { out_slot(result, out_committed_with_timeout, |offsets| offset_and_metadata_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_committed_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_committed_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_committed_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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

unsafe fn destroy_list_element(element: *mut c_void) {
    unsafe { kafka_List_destroy(element as *mut kafka_List_t) }
}

/// `partitionsFor(String topic)`: an owned list of owned
/// `kafka_common_PartitionInfo_t *`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    out_partitions_for: *mut *mut kafka_List_t,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.partitions_for(&topic).await })) };
    unsafe { out_slot(result, out_partitions_for, |infos| partition_info_list(&infos)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_partitions_for_cb_t =
    unsafe extern "C" fn(value: *mut kafka_List_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    cb: kafka_consumer_Consumer_partitions_for_cb_t,
    opaque: *mut c_void,
) {
    let topic = unsafe { c_str_to_string(topic) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
            self_,
            move |c| Box::pin(async move { c.partitions_for(&topic).await }),
            move |client, result| deliver_ptr(client, cb, opaque, result, move |infos| partition_info_list(&infos)),
        )
    }
}

/// `partitionsFor(String topic, Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    timeout: i64,
    out_partitions_for_with_timeout: *mut *mut kafka_List_t,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.partitions_for_with_timeout(&topic, ms(timeout)?).await })
        })
    };
    unsafe { out_slot(result, out_partitions_for_with_timeout, |infos| partition_info_list(&infos)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_partitions_for_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_List_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_partitions_for_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    topic: *const c_char,
    timeout: i64,
    cb: kafka_consumer_Consumer_partitions_for_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let topic = unsafe { c_str_to_string(topic) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
            self_,
            move |c| Box::pin(async move { c.partitions_for_with_timeout(&topic, ms(timeout)?).await }),
            move |client, result| deliver_ptr(client, cb, opaque, result, move |infos| partition_info_list(&infos)),
        )
    }
}

/// `listTopics()`: an owned map of owned `char *` to owned `kafka_List_t *`
/// of owned `kafka_common_PartitionInfo_t *`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics(
    self_: *mut kafka_consumer_Consumer_t,
    out_list_topics: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.list_topics().await })) };
    unsafe { out_slot(result, out_list_topics, topic_partition_info_map) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_list_topics_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_list_topics_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
            self_,
            move |c| Box::pin(async move { c.list_topics().await }),
            move |client, result| deliver_ptr(client, cb, opaque, result, topic_partition_info_map),
        )
    }
}

/// `listTopics(Duration timeout)`.
///
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    out_list_topics_with_timeout: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.list_topics_with_timeout(ms(timeout)?).await })
        })
    };
    unsafe { out_slot(result, out_list_topics_with_timeout, topic_partition_info_map) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_list_topics_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_list_topics_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_list_topics_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    out_offsets_for_times: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.offsets_for_times(timestamps_to_search).await })
        })
    };
    unsafe { out_slot(result, out_offsets_for_times, |offsets| offset_and_timestamp_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_offsets_for_times_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    cb: kafka_consumer_Consumer_offsets_for_times_cb_t,
    opaque: *mut c_void,
) {
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    timeout: i64,
    out_offsets_for_times_with_timeout: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.offsets_for_times_with_timeout(timestamps_to_search, ms(timeout)?).await })
        })
    };
    unsafe {
        out_slot(result, out_offsets_for_times_with_timeout, |offsets| {
            offset_and_timestamp_map(&offsets)
        })
    }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_offsets_for_times_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_offsets_for_times_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    timestamps_to_search: *const kafka_Map_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_offsets_for_times_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_beginning_offsets: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.beginning_offsets(&partitions).await })) };
    unsafe { out_slot(result, out_beginning_offsets, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_beginning_offsets_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_beginning_offsets_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    out_beginning_offsets_with_timeout: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.beginning_offsets_with_timeout(&partitions, ms(timeout)?).await })
        })
    };
    unsafe {
        out_slot(result, out_beginning_offsets_with_timeout, |offsets| {
            topic_partition_i64_map(&offsets)
        })
    }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_beginning_offsets_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_beginning_offsets_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_beginning_offsets_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    out_end_offsets: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let result = unsafe { run(self_, move |c| Box::pin(async move { c.end_offsets(&partitions).await })) };
    unsafe { out_slot(result, out_end_offsets, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_end_offsets_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_end_offsets_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// Blocking (CLAUDE.md §4 rule 5); the owned value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live consumer handle, the other parameters valid
/// for their documented types and the `out_` pointer valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_with_timeout(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    out_end_offsets_with_timeout: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let result = unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.end_offsets_with_timeout(&partitions, ms(timeout)?).await })
        })
    };
    unsafe {
        out_slot(result, out_end_offsets_with_timeout, |offsets| {
            topic_partition_i64_map(&offsets)
        })
    }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_Consumer_end_offsets_with_timeout_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_end_offsets_with_timeout_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    timeout: i64,
    cb: kafka_consumer_Consumer_end_offsets_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.pause(&partitions).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_pause_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_pause_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_pause_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.resume(&partitions).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_resume_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_resume_cb(
    self_: *mut kafka_consumer_Consumer_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_Consumer_resume_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance(
    self_: *mut kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.enforce_rebalance().await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_enforce_rebalance_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_enforce_rebalance_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance_with_reason(
    self_: *mut kafka_consumer_Consumer_t,
    reason: *const c_char,
) -> *mut kafka_common_Error_t {
    let reason = unsafe { c_str_to_string(reason) };
    error_slot(unsafe {
        run(self_, move |c| {
            Box::pin(async move { c.enforce_rebalance_with_reason(&reason).await })
        })
    })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_enforce_rebalance_with_reason_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_enforce_rebalance_with_reason_cb(
    self_: *mut kafka_consumer_Consumer_t,
    reason: *const c_char,
    cb: kafka_consumer_Consumer_enforce_rebalance_with_reason_cb_t,
    opaque: *mut c_void,
) {
    let reason = unsafe { c_str_to_string(reason) };
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close(
    self_: *mut kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.close().await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_close_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_cb(
    self_: *mut kafka_consumer_Consumer_t,
    cb: kafka_consumer_Consumer_close_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
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
/// `self_` must be a live consumer handle and the other parameters
/// valid for their documented types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_with_options(
    self_: *mut kafka_consumer_Consumer_t,
    options: *const kafka_consumer_CloseOptions_t,
) -> *mut kafka_common_Error_t {
    let options = unsafe { close_options_ref(options) }.clone();
    error_slot(unsafe { run(self_, move |c| Box::pin(async move { c.close_with_options(options).await })) })
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_Consumer_close_with_options_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin (CLAUDE.md §4 rule 5): the operation runs
/// on the client's runtime, the interface methods it triggers and
/// `cb` itself are queued for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_close_with_options_cb(
    self_: *mut kafka_consumer_Consumer_t,
    options: *const kafka_consumer_CloseOptions_t,
    cb: kafka_consumer_Consumer_close_with_options_cb_t,
    opaque: *mut c_void,
) {
    let options = unsafe { close_options_ref(options) }.clone();
    let opaque = SendPtr(opaque);
    unsafe {
        run_cb(
            self_,
            move |c| Box::pin(async move { c.close_with_options(options).await }),
            move |client, result| deliver_void(client, cb, opaque, result),
        )
    }
}

// ---------------------------------------------------------------------------
// Sync methods
// ---------------------------------------------------------------------------

/// Reads the consumer under the single-owner flag, or returns `busy` when an
/// operation is in flight (see the module docs).
///
/// # Safety
///
/// `self_` must be a live handle.
unsafe fn read<T>(
    self_: *const kafka_consumer_Consumer_t,
    busy: impl FnOnce() -> T,
    f: impl FnOnce(&DynConsumer) -> T,
) -> T {
    let client = unsafe { client_ref(self_) };
    match client.acquire() {
        Ok(mut guard) => f(guard.consumer_mut()),
        Err(_) => busy(),
    }
}

/// `assignment()`: an owned list of owned `kafka_common_TopicPartition_t *`,
/// sorted by topic and partition; empty while an operation is in flight.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_assignment(
    self_: *const kafka_consumer_Consumer_t,
) -> *mut kafka_List_t {
    let assignment = unsafe { read(self_, Default::default, |c| c.assignment()) };
    sorted_topic_partition_list(assignment.iter())
}

/// `subscription()`: an owned list of owned `char *`, sorted; empty while an
/// operation is in flight.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_subscription(
    self_: *const kafka_consumer_Consumer_t,
) -> *mut kafka_List_t {
    let subscription = unsafe { read(self_, Default::default, |c| c.subscription()) };
    sorted_string_list(subscription.iter())
}

/// `paused()`: an owned list of owned `kafka_common_TopicPartition_t *`,
/// sorted; empty while an operation is in flight.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_paused(self_: *const kafka_consumer_Consumer_t) -> *mut kafka_List_t {
    let paused = unsafe { read(self_, Default::default, |c| c.paused()) };
    sorted_topic_partition_list(paused.iter())
}

/// `clientId()`: borrowed from the handle for its lifetime.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_client_id(self_: *const kafka_consumer_Consumer_t) -> *const c_char {
    unsafe { client_ref(self_) }.client_id.as_ptr()
}

/// `currentLag(TopicPartition topicPartition)`: the lag, or `-1` for
/// `OptionalLong.empty()` (and while an operation is in flight).
///
/// # Safety
///
/// `self_` must be a live handle and `topic_partition` a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_current_lag(
    self_: *const kafka_consumer_Consumer_t,
    topic_partition: *const kafka_common_TopicPartition_t,
) -> i64 {
    let tp: &TopicPartition = unsafe { topic_partition_ref(topic_partition) };
    unsafe { read(self_, || -1, |c| c.current_lag(tp).unwrap_or(-1)) }
}

/// `metrics()`: an owned `kafka_Map_t` of owned `kafka_common_MetricName_t *`
/// to owned `kafka_common_metrics_KafkaMetric_t *`, names compared by value
/// in `kafka_Map_get`; empty while an operation is in flight.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_metrics(self_: *const kafka_consumer_Consumer_t) -> *mut kafka_Map_t {
    let mut metrics: Vec<_> = unsafe { read(self_, Default::default, |c| c.metrics()) }.into_iter().collect();
    // Deterministic order for a map Java leaves unordered.
    metrics.sort_by(|(a, _), (b, _)| a.name().cmp(b.name()).then_with(|| a.group().cmp(b.group())));
    let entries = metrics
        .into_iter()
        .map(|(name, metric)| (box_metric_name(name) as *mut c_void, box_kafka_metric(metric) as *mut c_void))
        .collect();
    box_map(
        entries,
        Some(destroy_metric_name),
        Some(destroy_kafka_metric),
        Some(metric_name_eq),
    )
}

unsafe fn destroy_metric_name(element: *mut c_void) {
    unsafe { kafka_common_MetricName_destroy(element as *mut _) }
}

unsafe fn destroy_kafka_metric(element: *mut c_void) {
    unsafe { kafka_common_metrics_KafkaMetric_destroy(element as *mut _) }
}

/// `groupMetadata()`: an owned handle freed with
/// `kafka_consumer_ConsumerGroupMetadata_destroy`, or `NULL` while an
/// operation is in flight.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_group_metadata(
    self_: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
    unsafe { read(self_, std::ptr::null_mut, |c| box_group_metadata(c.group_metadata())) }
}

/// `wakeup()`: interrupts the operation in flight (or the next one) with
/// the `WakeupException` translation. Callable from any thread, at any time.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_wakeup(self_: *const kafka_consumer_Consumer_t) {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_handle(
    self_: *const kafka_consumer_Consumer_t,
) -> *mut kafka_consumer_ConsumerHandle_t {
    box_consumer_handle(unsafe { client_ref(self_) })
}

// ---------------------------------------------------------------------------
// Callback pump (CLAUDE.md §4 rule 5)
// ---------------------------------------------------------------------------

/// Fired once, from a Rust task, each time the client's callbacks vector
/// goes from empty to non-empty; it may only schedule a call to
/// [`kafka_consumer_Consumer_execute_callbacks`], never run callbacks.
pub type kafka_consumer_Consumer_callbacks_notify_fn_t = unsafe extern "C" fn(opaque: *mut c_void);

/// Runs every queued callback serially on the calling thread and returns
/// how many ran. Concurrent calls are serialized; a nested call from inside
/// a callback returns 0.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_execute_callbacks(self_: *const kafka_consumer_Consumer_t) -> i32 {
    unsafe { client_ref(self_) }.delivery.queue.execute()
}

/// Installs the hook fired once each time the callback vector goes from
/// empty to non-empty (see
/// [`kafka_consumer_Consumer_callbacks_notify_fn_t`]).
///
/// # Safety
///
/// `self_` must be a live handle; `opaque` stays valid while the hook is
/// installed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_set_callbacks_notify(
    self_: *const kafka_consumer_Consumer_t,
    notify: kafka_consumer_Consumer_callbacks_notify_fn_t,
    opaque: *mut c_void,
) {
    unsafe { client_ref(self_) }.delivery.queue.set_notify(Some(notify), opaque);
}

/// Frees a handle returned by `kafka_consumer_KafkaConsumer_new`: awaits the
/// `_cb` operations in flight, runs every pending callback (so each fires
/// exactly once), drops the consumer (which closes it if `close` was never
/// called, as Java's finalizer-less contract expects the caller to do) and
/// shuts its runtime down. Never called on an `__as_Consumer` view; a null
/// pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a live handle from `KafkaConsumer_new`, not
/// used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_Consumer_destroy(self_: *mut kafka_consumer_Consumer_t) {
    if !self_.is_null() {
        unsafe { Box::from_raw(self_ as *mut ConsumerClassHandle) }.destroy();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_results_are_delivered_once() {
        let (tx, rx) = std::sync::mpsc::channel();
        let id = register_callback_result(Box::new(move |r| tx.send(r).unwrap()));
        assert!(complete_callback_result(id, Ok(())));
        assert!(rx.recv().unwrap().is_ok());
        assert!(!complete_callback_result(id, Ok(())));
        assert!(!complete_callback_result(id + 1_000_000, Ok(())));
    }

    #[test]
    fn negative_timeout_is_the_java_illegal_argument() {
        let error = ms(-1).unwrap_err();
        assert!(error.is_local_illegal_argument_error());
        assert_eq!(error.message(), "Timeout must not be negative");
        assert_eq!(ms(10).unwrap(), Duration::from_millis(10));
    }
}
