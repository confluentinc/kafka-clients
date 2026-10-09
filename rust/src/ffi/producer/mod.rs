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

//! C bindings for `org.apache.kafka.clients.producer` (CLAUDE.md §4).
//!
//! # Shape
//!
//! `kafka_producer_Producer_t` is the `Producer` trait: one invoker per
//! method, taking the interface handle first. Nothing constructs an owned
//! `Producer_t`, so it has no `_new` and no `_destroy`: the two classes
//! implementing it, [`kafka_producer_KafkaProducer_t`] and
//! [`kafka_producer_MockProducer_t`], are built by their own constructors,
//! freed by their own `_destroy`, and reached as a `Producer` through their
//! `__as_Producer` borrowed view (CLAUDE.md §4 rule 3). The class handles
//! also expose the inherent delegates Java declares on the class
//! (`KafkaProducer_init_transactions`, ...), which forward to the interface
//! invokers.
//!
//! Key and value are `void *` (§4, "Generic types"). A producer built with
//! `NULL` serializers reads them as `kafka_Bytes_t *`; otherwise the C
//! `kafka_common_serialization_Serializer_t` passed at construction turns
//! them into bytes.
//!
//! # Blocking and `_cb` entry points (§4 rule 5)
//!
//! Every method that is `async` in Rust has a blocking form, driven to
//! completion on the calling thread, and a `_cb` twin that queues the
//! completion onto the producer's callback vector:
//! `kafka_producer_Producer__execute_callbacks` runs the queued callbacks
//! serially on the calling thread and
//! `kafka_producer_Producer__set_callbacks_notify` installs the hook fired
//! once each time the vector goes from empty to non-empty. Delivery
//! callbacks (`kafka_producer_Callback_t`) are fired by the producer's
//! background task and are therefore always queued, whichever `send` form
//! registered them. A blocking entry point invokes interface methods
//! (serializers, partitioner) directly on the calling thread; a `_cb` entry
//! point invokes them on a runtime worker, so C implementations must be
//! thread-safe.
//!
//! The blocking forms drive the runtime from the calling thread with
//! `block_on`; a C interface method (a serializer, the partitioner, a
//! delivery callback) that is itself running on a runtime worker must not
//! call a blocking entry point of the same producer, as a nested `block_on`
//! on a worker is a programming error (it panics in Tokio). The `_cb` forms
//! are always safe to call from there.
//!
//! # Ordering between `send_cb` and the control operations
//!
//! `send_cb` only *queues* a record: the real `producer.send()` runs later on
//! a per-producer submission task, in FIFO order. `flush`, `close` and every
//! transaction-control operation first drain that queue, so every `send_cb`
//! that had returned to the caller before the operation began is registered
//! with the producer first: `commit_transaction` commits those records and
//! `abort_transaction` discards them, through the producer's own accumulator
//! handling (`producer-transactions.md` §13). A `send_cb` racing concurrently
//! on another thread is not ordered against the operation, the same ambiguity
//! Java has for a `send` racing `commitTransaction`. A `send_cb` keeps the
//! key and value `void *` (and, without serializers, the bytes they point
//! at) borrowed until its completion callback fires.
//!
//! # Destroy
//!
//! `_destroy` on a class handle stops the submission task, waits for the
//! tasks spawned by `_cb` calls, runs the still-pending callbacks so each
//! fires exactly once (§4 rule 5), and drops the producer. It does not
//! `close` the producer: as in Rust, a `KafkaProducer` dropped without
//! `close` is force-closed with a warning, so a caller that wants a graceful
//! shutdown calls `close` first.

#![expect(non_camel_case_types)]

pub(crate) mod callback;
pub(crate) mod kafka_producer;
pub(crate) mod mock_producer;
pub(crate) mod partitioner;
pub(crate) mod producer_config;
pub(crate) mod producer_record;
pub(crate) mod record_metadata;
pub(crate) mod round_robin_partitioner;

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::common::{Error, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::ffi::callback_queue::{CallbackQueue, SendPtr};
use crate::ffi::common::metric_name::{box_metric_name, kafka_common_MetricName_destroy, metric_name_eq};
use crate::ffi::common::metrics::kafka_metric::{box_kafka_metric, kafka_common_metrics_KafkaMetric_destroy};
use crate::ffi::common::partition_info::{box_partition_info, kafka_common_PartitionInfo_destroy};
use crate::ffi::common::topic_partition::topic_partition_ref;
use crate::ffi::common::{box_error, init_default_logger, kafka_common_Error_t};
use crate::ffi::consumer::{
    group_metadata_ref, kafka_consumer_ConsumerGroupMetadata_t, kafka_consumer_OffsetAndMetadata_t,
    offset_and_metadata_ref,
};
use crate::ffi::kafka_future::{block_on, box_future, kafka_common_KafkaFuture_t, map_future_handle};
use crate::ffi::producer::callback::{callback_registration, kafka_producer_Callback_t};
use crate::ffi::producer::producer_record::{GenericRecord, kafka_producer_ProducerRecord_t, producer_record_ref};
use crate::ffi::producer::record_metadata::{box_record_metadata, kafka_producer_RecordMetadata_destroy};
use crate::ffi::util::{GenericValue, box_list, box_map, c_str_to_string, kafka_List_t, kafka_Map_t, map_entries};
use crate::producer::{Callback, DynProducer};

/// The `Producer` every C handle drives: `void *` key and value.
pub(crate) type DynProducerGv = dyn DynProducer<GenericValue, GenericValue>;

/// Opaque handle to the `Producer` interface: a borrowed view obtained from
/// `kafka_producer_KafkaProducer__as_Producer` or
/// `kafka_producer_MockProducer__as_Producer`, valid until that class handle
/// is destroyed.
#[repr(C)]
pub struct kafka_producer_Producer_t {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------------
// Client state shared by the class handle and its tasks
// ---------------------------------------------------------------------------

/// What the submission task receives.
enum SubmitRequest {
    /// A `send_cb` to hand to the producer.
    Send {
        record: GenericRecord,
        callback: Option<Callback>,
        cb: kafka_producer_Producer_send_cb_t,
        opaque: SendPtr,
    },
    /// A marker placed behind a set of queued sends.
    ///
    /// Signals `ack` once reached. FIFO delivery is what makes it a barrier:
    /// the task fully finishes each send before taking the next item, so
    /// dequeuing this marker means everything ahead of it is done.
    Barrier { ack: tokio::sync::oneshot::Sender<()> },
}

/// The state a `kafka_producer_Producer_t` points at, shared through an
/// `Arc` between the class handle and every task it spawned, so a task
/// never outlives what it uses.
pub(crate) struct Client {
    producer: Arc<DynProducerGv>,
    runtime: tokio::runtime::Handle,
    queue: Arc<CallbackQueue>,
    /// `None` once `destroy` has asked the submission task to stop.
    submit_tx: Mutex<Option<UnboundedSender<SubmitRequest>>>,
    /// Sends queued by `send_cb` and not yet handed to the producer.
    queued_sends: AtomicUsize,
    /// Java's `TransactionManager` is not safe for concurrent control calls;
    /// the flag turns a concurrent call into a `ConcurrentModificationException`.
    txn_control_busy: AtomicBool,
    /// Tasks spawned by `_cb` calls, awaited by `destroy`.
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Client {
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

    /// Waits until every `send_cb` that had returned before this call was
    /// handed to the producer (see the module docs).
    async fn drain_submitted_sends(&self) -> Result<(), Error> {
        if self.queued_sends.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let sent = match &*self.submit_tx.lock().unwrap() {
            Some(tx) => tx.send(SubmitRequest::Barrier { ack: ack_tx }).is_ok(),
            None => false,
        };
        if !sent {
            return Err(Error::local_illegal_state(
                "the producer's send-submission task has stopped; records queued by send_cb were dropped without \
                 being produced and their callbacks will never fire",
            ));
        }
        ack_rx.await.map_err(|_| {
            Error::local_illegal_state(
                "the producer's send-submission task stopped while ordering queued sends; records queued by send_cb \
                 may not have been produced",
            )
        })
    }

    /// Takes the transaction-control flag, or fails as Java does when two
    /// control calls overlap.
    fn acquire_txn_control(&self) -> Result<(), Error> {
        self.txn_control_busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                Error::local_concurrent_modification(
                    "Transactional methods of KafkaProducer are not safe for concurrent access.",
                )
            })
    }

    /// Queues the completion of a `void` operation.
    fn deliver(&self, cb: kafka_producer_Producer_init_transactions_cb_t, opaque: SendPtr, result: Result<(), Error>) {
        let error = result.err().map_or(std::ptr::null_mut(), box_error);
        let error = SendPtr(error as *mut c_void);
        self.queue.push(Box::new(move || unsafe {
            cb(error.get() as *mut kafka_common_Error_t, opaque.get())
        }));
    }
}

/// Releases the transaction-control flag on every exit path, a panic
/// included.
struct TxnControlGuard(Arc<Client>);

impl Drop for TxnControlGuard {
    fn drop(&mut self) {
        self.0.txn_control_busy.store(false, Ordering::Release);
    }
}

/// Decrements `queued_sends` once the submission task is done with a request,
/// produced or not, so the counter cannot drift: a drifted counter would make
/// every later barrier wait for a request that no longer exists.
struct QueueDepthGuard<'a>(&'a Client);

impl Drop for QueueDepthGuard<'_> {
    fn drop(&mut self) {
        self.0.queued_sends.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The per-producer submission task: hands `send_cb` records to the producer
/// in FIFO order (one task per producer, not a per-record spawn, CLAUDE.md
/// §13). Ends when the class handle is destroyed and drops the sender.
async fn submission_loop(client: Arc<Client>, mut rx: UnboundedReceiver<SubmitRequest>) {
    while let Some(request) = rx.recv().await {
        let (record, callback, cb, opaque) = match request {
            SubmitRequest::Send { record, callback, cb, opaque } => (record, callback, cb, opaque),
            SubmitRequest::Barrier { ack } => {
                let _ = ack.send(());
                continue;
            },
        };
        // Decremented only once the send below has fully completed, so the
        // counter means "queued or in flight": a barrier has to wait for an
        // in-flight handover too, not merely for the queue to empty.
        let _depth = QueueDepthGuard(&client);
        let result = client.producer.send_with_callback(record, callback).await;
        deliver_send(&client, cb, opaque, result);
    }
}

/// Queues the completion of a `send`: the future handle on success, the
/// error otherwise. On `Err` the Rust producer owns the delivery-callback
/// obligation (it fires the callback itself when the record was registered),
/// so nothing is fired here.
fn deliver_send(
    client: &Arc<Client>,
    cb: kafka_producer_Producer_send_cb_t,
    opaque: SendPtr,
    result: Result<crate::common::KafkaFuture<crate::producer::RecordMetadata>, Error>,
) {
    let (value, error) = match result {
        Ok(future) => (
            box_future(
                metadata_future(&future),
                Some(client.runtime.clone()),
                Some(Arc::clone(&client.queue)),
            ),
            std::ptr::null_mut(),
        ),
        Err(error) => (std::ptr::null_mut(), box_error(error)),
    };
    let value = SendPtr(value as *mut c_void);
    let error = SendPtr(error as *mut c_void);
    client.queue.push(Box::new(move || unsafe {
        cb(
            value.get() as *mut kafka_common_KafkaFuture_t,
            error.get() as *mut kafka_common_Error_t,
            opaque.get(),
        )
    }));
}

/// The C view of a `KafkaFuture<RecordMetadata>`: resolves to a
/// `kafka_producer_RecordMetadata_t *` the future owns.
fn metadata_future(
    future: &crate::common::KafkaFuture<crate::producer::RecordMetadata>,
) -> crate::ffi::kafka_future::FfiFuture {
    map_future_handle(
        future,
        |metadata| box_record_metadata(metadata) as *mut c_void,
        destroy_record_metadata,
    )
}

unsafe fn destroy_record_metadata(element: *mut c_void) {
    unsafe { kafka_producer_RecordMetadata_destroy(element as *mut _) }
}

/// The class handle behind `kafka_producer_KafkaProducer_t` and
/// `kafka_producer_MockProducer_t`: the concrete producer, the shared client
/// state and the runtime that drives both.
pub(crate) struct ProducerHandle<P: ?Sized> {
    producer: Arc<P>,
    /// The `kafka_producer_Producer_t` view is this field's address.
    client: Arc<Client>,
    runtime: tokio::runtime::Runtime,
}

impl<P> ProducerHandle<P>
where
    P: DynProducer<GenericValue, GenericValue> + 'static,
{
    /// Builds the producer with `make`, inside the context of a fresh
    /// multi-thread runtime (a `KafkaProducer` spawns its Sender task on
    /// construction), and wraps it with the runtime and the submission task.
    pub(crate) fn new(make: impl FnOnce() -> Result<P, Error>) -> Result<Box<Self>, Error> {
        init_default_logger();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("kafka-producer-ffi")
            .build()
            .expect("tokio runtime");
        let producer = {
            let _enter = runtime.enter();
            Arc::new(make()?)
        };
        let (submit_tx, submit_rx) = tokio::sync::mpsc::unbounded_channel();
        let client = Arc::new(Client {
            producer: Arc::clone(&producer) as Arc<DynProducerGv>,
            runtime: runtime.handle().clone(),
            queue: Arc::new(CallbackQueue::new()),
            submit_tx: Mutex::new(Some(submit_tx)),
            queued_sends: AtomicUsize::new(0),
            txn_control_busy: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
        });
        client.spawn(submission_loop(Arc::clone(&client), submit_rx));
        Ok(Box::new(Self { producer, client, runtime }))
    }

    /// The concrete producer.
    pub(crate) fn producer(&self) -> &P {
        &self.producer
    }

    /// The `Producer` view of this handle (see the module docs).
    pub(crate) fn as_producer(&self) -> *const kafka_producer_Producer_t {
        &self.client as *const Arc<Client> as *const kafka_producer_Producer_t
    }

    /// Stops the submission task, awaits every spawned task, fires the
    /// pending callbacks and drops the producer (see the module docs).
    pub(crate) fn destroy(self) {
        let Self { producer, client, runtime } = self;
        drop(client.submit_tx.lock().unwrap().take());
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
        // The producer's `Drop` force-closes it (a `KafkaProducer` needs the
        // runtime context for that) and fires the delivery callbacks of the
        // records still in flight into the queue, which runs once more so
        // each fires exactly once; the runtime is shut down last.
        {
            let _enter = runtime.enter();
            drop(producer);
        }
        client.queue.execute();
        drop(client);
        runtime.shutdown_timeout(Duration::from_secs(5));
    }
}

/// The client behind an interface handle.
///
/// # Safety
///
/// `producer` must be a live view from an `__as_Producer` function.
unsafe fn client_ref<'a>(producer: *const kafka_producer_Producer_t) -> &'a Arc<Client> {
    unsafe { &*(producer as *const Arc<Client>) }
}

fn error_slot(result: Result<(), Error>) -> *mut kafka_common_Error_t {
    result.err().map_or(std::ptr::null_mut(), box_error)
}

/// Runs a transaction-control operation on the calling thread: takes the
/// flag, drains the submission queue, runs `op`.
fn with_txn_control<F, Fut>(client: &Arc<Client>, op: F) -> *mut kafka_common_Error_t
where
    F: FnOnce(Arc<DynProducerGv>) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    if let Err(error) = client.acquire_txn_control() {
        return box_error(error);
    }
    let _guard = TxnControlGuard(Arc::clone(client));
    let producer = Arc::clone(&client.producer);
    error_slot(block_on(Some(&client.runtime), async {
        client.drain_submitted_sends().await?;
        op(producer).await
    }))
}

/// The `_cb` form of [`with_txn_control`]: the drain and `op` run on a
/// spawned task and the result is queued.
fn with_txn_control_cb<F, Fut>(
    client: &Arc<Client>,
    cb: kafka_producer_Producer_init_transactions_cb_t,
    opaque: *mut c_void,
    op: F,
) where
    F: FnOnce(Arc<DynProducerGv>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    let opaque = SendPtr(opaque);
    if let Err(error) = client.acquire_txn_control() {
        client.deliver(cb, opaque, Err(error));
        return;
    }
    let guard = TxnControlGuard(Arc::clone(client));
    let task_client = Arc::clone(client);
    client.spawn(async move {
        let _guard = guard;
        let result = match task_client.drain_submitted_sends().await {
            Ok(()) => op(Arc::clone(&task_client.producer)).await,
            Err(error) => Err(error),
        };
        task_client.deliver(cb, opaque, result);
    });
}

/// Runs a non-transactional `void` operation after draining the submission
/// queue, on the calling thread.
fn with_drain<F, Fut>(client: &Arc<Client>, op: F) -> *mut kafka_common_Error_t
where
    F: FnOnce(Arc<DynProducerGv>) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let producer = Arc::clone(&client.producer);
    error_slot(block_on(Some(&client.runtime), async {
        client.drain_submitted_sends().await?;
        op(producer).await
    }))
}

/// The `_cb` form of [`with_drain`].
fn with_drain_cb<F, Fut>(
    client: &Arc<Client>,
    cb: kafka_producer_Producer_init_transactions_cb_t,
    opaque: *mut c_void,
    op: F,
) where
    F: FnOnce(Arc<DynProducerGv>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    let opaque = SendPtr(opaque);
    let task_client = Arc::clone(client);
    client.spawn(async move {
        let result = match task_client.drain_submitted_sends().await {
            Ok(()) => op(Arc::clone(&task_client.producer)).await,
            Err(error) => Err(error),
        };
        task_client.deliver(cb, opaque, result);
    });
}

/// Reads a `kafka_Map_t` of `kafka_common_TopicPartition_t *` to
/// `kafka_consumer_OffsetAndMetadata_t *`.
///
/// # Safety
///
/// `offsets` must be a live map with those element types.
unsafe fn offsets_map(offsets: *const kafka_Map_t) -> HashMap<TopicPartition, OffsetAndMetadata> {
    unsafe { map_entries(offsets) }
        .iter()
        .map(|&(tp, oam)| {
            (
                unsafe { topic_partition_ref(tp as *const _) }.clone(),
                unsafe { offset_and_metadata_ref(oam as *const kafka_consumer_OffsetAndMetadata_t) }.clone(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Callback typedefs
// ---------------------------------------------------------------------------

/// Completion of a `void` operation: `error` is `NULL` on success and owned
/// by the callee otherwise.
pub type kafka_producer_Producer_init_transactions_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` operation: `error` is `NULL` on success and owned
/// by the callee otherwise.
pub type kafka_producer_Producer_send_offsets_to_transaction_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` operation: `error` is `NULL` on success and owned
/// by the callee otherwise.
pub type kafka_producer_Producer_commit_transaction_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` operation: `error` is `NULL` on success and owned
/// by the callee otherwise.
pub type kafka_producer_Producer_abort_transaction_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` operation: `error` is `NULL` on success and owned
/// by the callee otherwise.
pub type kafka_producer_Producer_flush_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` operation: `error` is `NULL` on success and owned
/// by the callee otherwise.
pub type kafka_producer_Producer_close_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` operation: `error` is `NULL` on success and owned
/// by the callee otherwise.
pub type kafka_producer_Producer_close_with_timeout_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `send`: `value` is the owned
/// `kafka_common_KafkaFuture_t *` resolving to a
/// `kafka_producer_RecordMetadata_t *` borrowed from the future (`NULL` on
/// failure), `error` the owned failure (`NULL` on success).
pub type kafka_producer_Producer_send_cb_t =
    unsafe extern "C" fn(value: *mut kafka_common_KafkaFuture_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `send`: `value` is the owned
/// `kafka_common_KafkaFuture_t *` resolving to a
/// `kafka_producer_RecordMetadata_t *` borrowed from the future (`NULL` on
/// failure), `error` the owned failure (`NULL` on success).
pub type kafka_producer_Producer_send_with_callback_cb_t =
    unsafe extern "C" fn(value: *mut kafka_common_KafkaFuture_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of `partitions_for`: `value` is the owned `kafka_List_t *` of
/// owned `kafka_common_PartitionInfo_t *` (`NULL` on failure).
pub type kafka_producer_Producer_partitions_for_cb_t =
    unsafe extern "C" fn(value: *mut kafka_List_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The hook fired once each time the callback vector goes from empty to
/// non-empty, from a Rust task: it may only schedule a later
/// [`kafka_producer_Producer__execute_callbacks`], never run callbacks.
pub type kafka_producer_Producer_callbacks_notify_fn_t = unsafe extern "C" fn(opaque: *mut c_void);

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/// `Producer.initTransactions()`, blocking.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions(
    self_: *const kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    with_txn_control(unsafe { client_ref(self_) }, |p| async move { p.init_transactions().await })
}

/// `Producer.initTransactions()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_init_transactions_cb_t,
    opaque: *mut c_void,
) {
    with_txn_control_cb(unsafe { client_ref(self_) }, cb, opaque, |p| async move {
        p.init_transactions().await
    });
}

/// `Producer.beginTransaction()`: synchronous in Java and Rust, still
/// ordered after the queued `send_cb`s like every control operation.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_begin_transaction(
    self_: *const kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    with_txn_control(unsafe { client_ref(self_) }, |p| async move { p.begin_transaction() })
}

/// `Producer.sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>,
/// ConsumerGroupMetadata)`, blocking: `offsets` maps
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// and is copied during the call.
///
/// # Safety
///
/// `self_`, `offsets` and `group_metadata` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_offsets_to_transaction(
    self_: *const kafka_producer_Producer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *mut kafka_common_Error_t {
    let offsets = unsafe { offsets_map(offsets) };
    let group_metadata = Arc::clone(unsafe { group_metadata_ref(group_metadata) });
    with_txn_control(unsafe { client_ref(self_) }, |p| async move {
        p.send_offsets_to_transaction(offsets, &*group_metadata).await
    })
}

/// `Producer.sendOffsetsToTransaction(...)`, completion queued.
///
/// # Safety
///
/// `self_`, `offsets` and `group_metadata` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_offsets_to_transaction_cb(
    self_: *const kafka_producer_Producer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    cb: kafka_producer_Producer_send_offsets_to_transaction_cb_t,
    opaque: *mut c_void,
) {
    let offsets = unsafe { offsets_map(offsets) };
    let group_metadata = Arc::clone(unsafe { group_metadata_ref(group_metadata) });
    with_txn_control_cb(unsafe { client_ref(self_) }, cb, opaque, |p| async move {
        p.send_offsets_to_transaction(offsets, &*group_metadata).await
    });
}

/// `Producer.commitTransaction()`, blocking.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction(
    self_: *const kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    with_txn_control(unsafe { client_ref(self_) }, |p| async move { p.commit_transaction().await })
}

/// `Producer.commitTransaction()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_commit_transaction_cb_t,
    opaque: *mut c_void,
) {
    with_txn_control_cb(unsafe { client_ref(self_) }, cb, opaque, |p| async move {
        p.commit_transaction().await
    });
}

/// `Producer.abortTransaction()`, blocking.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction(
    self_: *const kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    with_txn_control(unsafe { client_ref(self_) }, |p| async move { p.abort_transaction().await })
}

/// `Producer.abortTransaction()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_abort_transaction_cb_t,
    opaque: *mut c_void,
) {
    with_txn_control_cb(unsafe { client_ref(self_) }, cb, opaque, |p| async move {
        p.abort_transaction().await
    });
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

/// `Producer.send(ProducerRecord)`, blocking until the record is registered
/// (serialized, partitioned and appended to a batch): delivers the owned
/// `kafka_common_KafkaFuture_t` resolving to a
/// `kafka_producer_RecordMetadata_t *` borrowed from the future. The record
/// stays the caller's; its key and value `void *` are read during the call.
///
/// # Safety
///
/// `self_` and `record` must be live handles and `out_send` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    out_send: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    unsafe { kafka_producer_Producer_send_with_callback(self_, record, std::ptr::null(), out_send) }
}

/// `Producer.send(ProducerRecord)`, completion queued: the record is copied
/// and handed to the producer by the submission task (see the module docs);
/// its key and value `void *` stay borrowed until `cb` fires.
///
/// # Safety
///
/// `self_` and `record` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_cb(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    cb: kafka_producer_Producer_send_cb_t,
    opaque: *mut c_void,
) {
    unsafe { kafka_producer_Producer_send_with_callback_cb(self_, record, std::ptr::null(), cb, opaque) }
}

/// `Producer.send(ProducerRecord, Callback)`, blocking as
/// [`kafka_producer_Producer_send`]; `callback` (`NULL` for none) is a
/// `kafka_producer_Callback_t` whose `on_completion` is queued on the
/// callback vector when the record is acknowledged or fails, exactly once.
/// The callback's `self` must stay alive until then; the registration
/// handle itself is read during the call.
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle, `out_send_with_callback` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_with_callback(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *const kafka_producer_Callback_t,
    out_send_with_callback: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let client = unsafe { client_ref(self_) };
    let record = unsafe { producer_record_ref(record) }.clone();
    let callback = unsafe { delivery_callback(client, callback) };
    let producer = Arc::clone(&client.producer);
    match block_on(Some(&client.runtime), async move {
        producer.send_with_callback(record, callback).await
    }) {
        Ok(future) => {
            unsafe {
                *out_send_with_callback = box_future(
                    metadata_future(&future),
                    Some(client.runtime.clone()),
                    Some(Arc::clone(&client.queue)),
                );
            }
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `Producer.send(ProducerRecord, Callback)`, completion queued as
/// [`kafka_producer_Producer_send_cb`].
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_with_callback_cb(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *const kafka_producer_Callback_t,
    cb: kafka_producer_Producer_send_with_callback_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    let record = unsafe { producer_record_ref(record) }.clone();
    let callback = unsafe { delivery_callback(client, callback) };
    let opaque = SendPtr(opaque);
    client.queued_sends.fetch_add(1, Ordering::AcqRel);
    let sent = match &*client.submit_tx.lock().unwrap() {
        Some(tx) => tx.send(SubmitRequest::Send { record, callback, cb, opaque }).is_ok(),
        None => false,
    };
    if !sent {
        client.queued_sends.fetch_sub(1, Ordering::AcqRel);
        deliver_send(
            client,
            cb,
            opaque,
            Err(Error::local_illegal_state(
                "Cannot perform operation after producer has been closed",
            )),
        );
    }
}

/// The Rust delivery callback queuing a C `on_completion`, or `None`.
unsafe fn delivery_callback(client: &Arc<Client>, callback: *const kafka_producer_Callback_t) -> Option<Callback> {
    (!callback.is_null()).then(|| unsafe { callback_registration(callback) }.into_callback(Arc::clone(&client.queue)))
}

// ---------------------------------------------------------------------------
// Flush, partitions, metrics, close
// ---------------------------------------------------------------------------

/// `Producer.flush()`, blocking; ordered after the queued `send_cb`s.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_flush(
    self_: *const kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    with_drain(unsafe { client_ref(self_) }, |p| async move { p.flush().await })
}

/// `Producer.flush()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_flush_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_flush_cb_t,
    opaque: *mut c_void,
) {
    with_drain_cb(unsafe { client_ref(self_) }, cb, opaque, |p| async move { p.flush().await });
}

/// `Producer.partitionsFor(String)`, blocking: delivers an owned
/// `kafka_List_t` of owned `kafka_common_PartitionInfo_t *`.
///
/// # Safety
///
/// `self_` must be a live view, `topic` a NUL-terminated string and
/// `out_partitions_for` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for(
    self_: *const kafka_producer_Producer_t,
    topic: *const c_char,
    out_partitions_for: *mut *mut kafka_List_t,
) -> *mut kafka_common_Error_t {
    let client = unsafe { client_ref(self_) };
    let topic = unsafe { c_str_to_string(topic) };
    let producer = Arc::clone(&client.producer);
    match block_on(Some(&client.runtime), async move { producer.partitions_for(&topic).await }) {
        Ok(infos) => {
            unsafe { *out_partitions_for = partition_info_list(infos) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `Producer.partitionsFor(String)`, completion queued.
///
/// # Safety
///
/// `self_` must be a live view and `topic` a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for_cb(
    self_: *const kafka_producer_Producer_t,
    topic: *const c_char,
    cb: kafka_producer_Producer_partitions_for_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    let topic = unsafe { c_str_to_string(topic) };
    let opaque = SendPtr(opaque);
    let task_client = Arc::clone(client);
    client.spawn(async move {
        let result = task_client.producer.partitions_for(&topic).await;
        let (value, error) = match result {
            Ok(infos) => (partition_info_list(infos), std::ptr::null_mut()),
            Err(error) => (std::ptr::null_mut(), box_error(error)),
        };
        let value = SendPtr(value as *mut c_void);
        let error = SendPtr(error as *mut c_void);
        task_client.queue.push(Box::new(move || unsafe {
            cb(
                value.get() as *mut kafka_List_t,
                error.get() as *mut kafka_common_Error_t,
                opaque.get(),
            )
        }));
    });
}

fn partition_info_list(infos: Vec<crate::common::PartitionInfo>) -> *mut kafka_List_t {
    let elements = infos.into_iter().map(|info| box_partition_info(info) as *mut c_void).collect();
    box_list(elements, Some(destroy_partition_info))
}

unsafe fn destroy_partition_info(element: *mut c_void) {
    unsafe { kafka_common_PartitionInfo_destroy(element as *mut _) }
}

/// `Producer.metrics()`: an owned `kafka_Map_t` of owned
/// `kafka_common_MetricName_t *` to owned `kafka_common_metrics_KafkaMetric_t *`;
/// `kafka_Map_get` compares names by value.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_metrics(self_: *const kafka_producer_Producer_t) -> *mut kafka_Map_t {
    let client = unsafe { client_ref(self_) };
    let mut metrics: Vec<_> = client.producer.metrics().into_iter().collect();
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

/// `Producer.close()`, blocking: `close(Duration.ofMillis(Long.MAX_VALUE))`,
/// ordered after the queued `send_cb`s. The handle stays valid, to be
/// destroyed afterwards.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close(
    self_: *const kafka_producer_Producer_t,
) -> *mut kafka_common_Error_t {
    with_drain(unsafe { client_ref(self_) }, |p| async move { p.close().await })
}

/// `Producer.close()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_close_cb_t,
    opaque: *mut c_void,
) {
    with_drain_cb(unsafe { client_ref(self_) }, cb, opaque, |p| async move { p.close().await });
}

/// `Producer.close(Duration timeout)`, blocking; `timeout` in milliseconds,
/// negative values rejected as in Java.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close_with_timeout(
    self_: *const kafka_producer_Producer_t,
    timeout: i64,
) -> *mut kafka_common_Error_t {
    let timeout = match close_timeout(timeout) {
        Ok(timeout) => timeout,
        Err(error) => return box_error(error),
    };
    with_drain(unsafe { client_ref(self_) }, move |p| async move {
        p.close_with_timeout(timeout).await
    })
}

/// `Producer.close(Duration timeout)`, completion queued.
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close_with_timeout_cb(
    self_: *const kafka_producer_Producer_t,
    timeout: i64,
    cb: kafka_producer_Producer_close_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let client = unsafe { client_ref(self_) };
    match close_timeout(timeout) {
        Ok(timeout) => {
            with_drain_cb(client, cb, opaque, move |p| async move { p.close_with_timeout(timeout).await });
        },
        Err(error) => client.deliver(cb, SendPtr(opaque), Err(error)),
    }
}

fn close_timeout(timeout: i64) -> Result<Duration, Error> {
    u64::try_from(timeout)
        .map(Duration::from_millis)
        .map_err(|_| Error::local_illegal_argument("The timeout cannot be negative."))
}

// ---------------------------------------------------------------------------
// Callback pump
// ---------------------------------------------------------------------------

/// Runs the queued callbacks serially on the calling thread and returns how
/// many ran (CLAUDE.md §4 rule 5).
///
/// # Safety
///
/// `self_` must be a live view.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer__execute_callbacks(self_: *const kafka_producer_Producer_t) -> i32 {
    unsafe { client_ref(self_) }.queue.execute()
}

/// Installs the hook fired once each time the callback vector goes from
/// empty to non-empty (see
/// [`kafka_producer_Producer_callbacks_notify_fn_t`]).
///
/// # Safety
///
/// `self_` must be a live view; `opaque` stays valid while the hook is
/// installed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer__set_callbacks_notify(
    self_: *const kafka_producer_Producer_t,
    notify: kafka_producer_Producer_callbacks_notify_fn_t,
    opaque: *mut c_void,
) {
    unsafe { client_ref(self_) }.queue.set_notify(Some(notify), opaque);
}
