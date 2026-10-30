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
//! point invokes C serializers on the runtime, never on the calling thread
//! (see "`send_cb` tries the producer first" for the partitioner and the
//! built-in serializers), so C implementations must be thread-safe.
//!
//! The blocking forms drive the runtime from the calling thread with
//! `block_on`; a C interface method (a serializer, the partitioner) that is
//! itself running on a runtime worker must not call a blocking entry point
//! of the same producer, as a nested `block_on` on a worker is a programming
//! error (it panics in Tokio). The `_cb` forms are always safe to call from
//! there. A delivery callback runs on the thread pumping the queue, never on
//! a worker, so it may call either form, within what Java allows from a
//! callback (below).
//!
//! # Delivery callbacks report (`ffi::producer::callback`)
//!
//! Java runs `Callback.onCompletion` on the producer's I/O thread before the
//! record's future completes, so `flush()`, `commitTransaction()`,
//! `abortTransaction()` and `close()` return after the callbacks of the
//! records they wait for. C's callback runs later, on the pumping thread, so
//! it reports that it finished through
//! [`kafka_producer_Producer__set_callback_result`], with the `callback_id`
//! it was invoked with; the producer keeps one registry of those ids
//! (`ffi::callback_result::CallbackResults`). With a callback, the future a
//! `send_with_callback(_cb)` hands out completes only on that report, with
//! the record's own outcome; `flush` and the commit and abort wait, once the
//! Rust operation succeeded, for the reports of every callback queued before
//! it ended, and `close` waits for them without a bound once the producer
//! is closed, as Java's `close` joins its I/O thread after `forceClose()`.
//! While a blocking form waits it runs the queued callbacks on the calling
//! thread, as does `kafka_common_KafkaFuture_get` on a future a callback
//! backs, so a caller pumping the queue on that one thread does not wait for
//! itself; the `_cb` forms only wait.
//!
//! From inside a delivery callback, as from Java's I/O thread: a
//! `KafkaProducer`'s `flush` fails (Java's "KafkaProducer.flush() invocation
//! inside a callback is not permitted because it may lead to deadlock."),
//! its `close` with a positive timeout closes as for zero, with Java's
//! warning, and `close` does not wait for the reports, nor do the blocking
//! commit and abort, since the callback running cannot report before it
//! returns. A blocking `get` of the callback's own record's future waits
//! forever, as Java's does.
//!
//! # Ordering between `send_cb` and the control operations
//!
//! When both of the producer's serializers are built-in, `send_cb` first
//! tries the producer on the calling thread, without waiting: a record the
//! producer takes, or refuses, at once is done there, and `cb` fires inside
//! the call (see below). Only a record the producer would have to wait for,
//! on metadata or on buffer memory, is *queued*. When either serializer is a
//! C implementation, every record is queued and `cb` fires later, through the
//! callback vector, a closed producer's refusal included; only a send made
//! while `_destroy` runs completes inside the call.
//! A per-producer submission task takes the queued records in FIFO order and
//! tries each again on the producer without waiting. One it still has to
//! wait for moves to a task of its own, which runs Java's blocking `send` for
//! it, so the records behind it are not held back: Java's `send` blocking on
//! one thread holds back no other thread's records. Such a task is a
//! per-record spawn, but only for a record that has to wait, as many as Java
//! would have blocked threads.
//!
//! `flush`, `close` and every transaction-control operation first drain the
//! queue, so every `send_cb` that had returned to the caller before the
//! operation began has been handed to the producer: registered, refused, or
//! waiting inside it on its own task. A registered record is in the
//! operation: `commit_transaction` commits it and `abort_transaction`
//! discards it, through the producer's own accumulator handling
//! (`producer-transactions.md` §13). A waiting record is not: it is a `send`
//! still in progress, which the operation does not wait for, as Java's
//! `flush()` and `commitTransaction()` do not wait for a `send` blocked on
//! another thread, and `close` fails it with "Producer closed while send in
//! progress", as Java's does. A caller that wants each record in the next
//! operation, and its records reaching the producer in sending order, waits
//! for a record's completion before it goes on, as Java's `send` makes its
//! caller wait; the Python client does. A `send_cb` racing concurrently on
//! another thread is not ordered against the operation, the same ambiguity
//! Java has for a `send` racing `commitTransaction`. A `send_cb` keeps the
//! key and value `void *` (and, without serializers, the bytes they point
//! at) borrowed until its completion callback fires.
//!
//! A timed `close(Duration)` spends at most its timeout in total, the drain
//! included, as Java's timeout bounds the caller's whole wait: it drains for at
//! most the timeout and then closes with the time left. A record still queued
//! when the drain gives up reaches a closed producer and fails through its
//! completion, as a Java `send` after `close` does, and one waiting inside the
//! producer when it closes fails with "Producer closed while send in
//! progress". A plain `close()` (`Long.MAX_VALUE`) and `flush` drain without a
//! bound.
//!
//! # `send_cb` tries the producer first
//!
//! Java's `send` blocks for up to `max.block.ms` on metadata and on buffer
//! memory. `send_cb` and `send_with_callback_cb` spare the caller that wait
//! without a thread hop and a queued completion for every record: when both
//! of the producer's serializers are built-in (a `NULL` handle or a built-in
//! class, which never wait), they run the producer's non-blocking `try_send`
//! (crate-internal, with no Java counterpart) on the calling thread, invoking
//! the partitioner there, and
//!
//!   - fire `cb` inside the call, with the owned future, when the producer
//!     took the record at once (`TrySendOutcome::Accepted`);
//!   - queue the record for the submission task when the producer would have
//!     had to wait (`TrySendOutcome::WouldBlock`): the task tries it again
//!     and otherwise waits for it on a task of its own, and `cb` fires later,
//!     through the callback vector, with the future or the error;
//!   - fire `cb` inside the call, with the error, when the producer refused
//!     the record (a closed producer, a serialization failure).
//!
//! So `cb` fires exactly once either way, and a caller tells a record done
//! inside the call from a queued one only by when `cb` fires. Fired inside
//! the call, `cb` runs on the calling thread, which may hold what the caller
//! holds across the call: the Python binding holds its interpreter lock and
//! re-enters it there. The key and value `void *` are read during the call
//! when the record is taken or refused at once, and stay borrowed until `cb`
//! fires when it is queued. A caught panic in the body is reported through
//! `cb` too, queued, as by every `_cb` entry point.
//!
//! When either serializer is a C implementation, the calling thread does not
//! try the producer: a serializer registered with
//! `kafka_common_serialization_Serializer_new_async` may report from the very
//! thread that called `send_cb` (a single-threaded event loop), so
//! `try_send`, which awaits both serializers, would block the caller until
//! they are done, or forever. Every record then goes to the submission task, which awaits the
//! serializers on the runtime, and `cb` fires later, through the callback
//! vector. The choice is made once, when the producer is built.
//!
//! When it is tried first, the producer is tried whatever the queue holds, so
//! a record taken at once can reach the producer before one queued earlier,
//! by this caller or by another. That is Java's order: records are ordered
//! by when they reach the accumulator, and a `send` blocking on metadata or
//! on buffer memory holds back its own caller's next record, nothing else. A
//! caller that wants its records in sending order therefore waits for a
//! queued record's `cb` before its next send, as Java's `send` would have
//! made it wait; the queue then holds at most one record per such caller.
//!
//! # Destroy
//!
//! `_destroy` on a class handle waits for the tasks spawned by `_cb` calls,
//! then stops the submission task and waits for it and for the records it
//! left waiting on tasks of their own, closes a `KafkaProducer` the caller did
//! not close (below), runs the still-pending callbacks so each fires exactly
//! once (§4 rule 5), and drops the producer; the
//! `kafka_common_KafkaFuture_get_cb` calls still waiting on its futures fire
//! last, once each (see `ffi::kafka_future`). The delivery callbacks it runs
//! may report inline; once it ran them it completes the ones still
//! unreported as if each had reported success, so no future and no
//! `flush_cb` or `commit_transaction_cb` waits forever, and
//! `kafka_producer_Producer__set_callback_result` must not be called for it
//! from elsewhere once `_destroy` began.
//!
//! A `KafkaProducer` the caller did not close is closed as
//! `kafka_producer_Producer_close` closes it: the records it holds are sent,
//! and their delivery callbacks fire with the outcome before `_destroy`
//! returns, as no callback may fire after it. With no broker reachable that
//! waits up to `delivery.timeout.ms`. Java has no `destroy`; a Java producer is
//! released by `close()`, and one nothing references keeps sending on its
//! daemon I/O thread.
//!
//! Called inside a tokio runtime (on a worker thread, or inside a
//! `block_on`), where it cannot wait, `_destroy` returns at once and all of
//! this happens on a new thread, the pending callbacks included. If that
//! thread cannot be started, the handle is leaked and an error logged.

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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::common::internals::KafkaFutureImpl;
use crate::common::{Error, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::ffi::callback_queue::{CallbackJob, CallbackQueue, SendPtr};
use crate::ffi::callback_result::{CallbackResults, INVOKER_RESULTS, Mark};
use crate::ffi::common::deliver_unit;
use crate::ffi::common::fail_with;
use crate::ffi::common::metrics::metrics_view::{box_metrics_view, kafka_common_metrics_MetricsView_t};
use crate::ffi::common::partition_info::{box_partition_info, kafka_common_PartitionInfo_destroy};
use crate::ffi::common::store_error;
use crate::ffi::common::topic_partition::topic_partition_ref;
use crate::ffi::common::{
    box_error, catch_task_panic, deliver_panic, deliver_panic_value, in_tokio_runtime, init_default_logger,
    kafka_common_Error_t, spawn_callback_task, spawn_teardown, take_error,
};
use crate::ffi::consumer::{
    group_metadata_ref, kafka_consumer_ConsumerGroupMetadata_t, kafka_consumer_OffsetAndMetadata_t,
    offset_and_metadata_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::{
    block_on, box_future, box_report_future, kafka_common_KafkaFuture_t, map_future_handle,
};
use crate::ffi::producer::callback::{kafka_producer_Callback_t, take_callback};
use crate::ffi::producer::producer_record::{GenericRecord, kafka_producer_ProducerRecord_t, producer_record_ref};
use crate::ffi::producer::record_metadata::{box_record_metadata, kafka_producer_RecordMetadata_destroy};
use crate::ffi::util::{GenericValue, box_list, c_str_to_string, kafka_List_t, kafka_Map_t, map_entries};
use crate::producer::{Callback, DynTrySend, RecordMetadata, TrySendOutcome};

/// The `Producer` every C handle drives: `void *` key and value, with the
/// non-blocking send of [`DynTrySend`] beside the
/// [`DynProducer`](crate::producer::DynProducer) methods.
pub(crate) type DynProducerGv = dyn DynTrySend<GenericValue, GenericValue>;

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
        callback: Option<Box<dyn Callback>>,
        /// The future `callback`'s report completes (see [`delivery_callback`]).
        completion: Option<KafkaFutureImpl<RecordMetadata>>,
        cb: kafka_producer_Producer_send_cb_t,
        opaque: SendPtr,
    },
    /// A marker placed behind a set of queued sends.
    ///
    /// Signals `ack` once reached. FIFO delivery is what makes it a barrier:
    /// the task is done with each send before it takes the next item, the
    /// record registered, refused or moved to a task of its own to wait
    /// inside the producer (see [`submission_loop`]), so dequeuing this marker
    /// means everything ahead of it has been handed to the producer. It does
    /// not wait for a record still waiting there: that is a `send` in
    /// progress, which Java's `flush()` and `commitTransaction()` do not wait
    /// for either.
    Barrier { ack: tokio::sync::oneshot::Sender<()> },
}

/// The state a `kafka_producer_Producer_t` points at, shared through an
/// `Arc` between the class handle and every task it spawned, so a task
/// never outlives what it uses.
pub(crate) struct Client {
    /// The class handle's producer. Weak, so `destroy` drops the producer
    /// itself, before it stops the runtime, while this state, which the
    /// `__set_callback_result` reports of the callbacks it runs still read,
    /// lives on to the end (see [`ProducerHandle::destroy`]); it upgrades for
    /// as long as the class handle is live.
    producer: Weak<DynProducerGv>,
    runtime: tokio::runtime::Handle,
    queue: Arc<CallbackQueue>,
    /// The delivery callbacks awaiting their `__set_callback_result` report
    /// (see `ffi::producer::callback`).
    results: Arc<CallbackResults>,
    /// `None` once `destroy` has asked the submission task to stop.
    submit_tx: Mutex<Option<UnboundedSender<SubmitRequest>>>,
    /// Test-only: a test holding this lock parks the submission task before
    /// each handover, with the request dequeued, to observe what happens
    /// while a send is queued (see
    /// `tests::send_cb_takes_a_record_at_once_while_one_is_queued_before_it`).
    #[cfg(test)]
    submission_gate: tokio::sync::Mutex<()>,
    /// Java's `TransactionManager` is not safe for concurrent control calls;
    /// the flag turns a concurrent call into a `ConcurrentModificationException`.
    txn_control_busy: AtomicBool,
    /// Tasks spawned by `_cb` calls, and the [`send_waiter`]s of the records
    /// the submission task left waiting, awaited by `destroy`.
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Whether `close(Duration)` checks its timeout as `KafkaProducer.close`
    /// does: it rejects a negative one (`KafkaProducer#close`) and, called
    /// from a delivery callback, overrides a positive one to zero
    /// (`KafkaProducer#close`). Java's `MockProducer.close(Duration)`
    /// never reads the timeout (`MockProducer#close(Duration)`), so the mock's
    /// view passes a negative one on as zero and never overrides it.
    follows_kafka_producer_close: bool,
    /// Whether `send_cb` tries the producer on the calling thread before
    /// queueing the record: true when both serializers are built-in (a
    /// `NULL` handle, a built-in class), which complete at once; false when
    /// either is a C implementation, which `send_cb` must not wait for on the
    /// calling thread (see the module docs, "`send_cb` tries the producer
    /// first"). Fixed when the producer is built, so no record pays the check.
    tries_send_inline: bool,
}

impl Client {
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

    /// The producer, while the class handle is live, which every entry point
    /// but `__set_callback_result` requires; after `destroy` dropped it, a
    /// `LocalIllegalState` error.
    fn producer(&self) -> Result<Arc<DynProducerGv>, Error> {
        self.producer
            .upgrade()
            .ok_or_else(|| Error::local_illegal_state("Cannot perform operation after producer has been destroyed"))
    }

    /// Whether the calling thread is running one of this producer's queued
    /// callbacks: Java's `Thread.currentThread() == this.ioThread`, the
    /// thread Java's delivery callbacks run on (`KafkaProducer#flush`,
    /// `KafkaProducer#close(Duration)`).
    fn in_callback(&self) -> bool {
        self.queue.is_executing_on_current_thread()
    }

    /// Fails as `KafkaProducer.flush()` does when called from a delivery
    /// callback (`KafkaProducer#flush`), logging the message as Java
    /// does; `MockProducer.flush()` makes no such check. A flush there would
    /// wait for the report of the callback it runs in, which cannot come
    /// before the callback returns.
    fn check_flush_outside_callback(&self, in_callback: bool) -> Result<(), Error> {
        const MESSAGE: &str =
            "KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.";
        if in_callback && self.follows_kafka_producer_close {
            log::error!("{MESSAGE}");
            return Err(Error::kafka_message(MESSAGE));
        }
        Ok(())
    }

    /// Waits, on the calling thread, until every delivery callback queued
    /// before `mark` has reported, running the queued callbacks on this
    /// thread meanwhile, outside the runtime, so a caller that pumps the
    /// queue itself on one thread does not wait for its own pump.
    fn wait_reported_blocking(&self, mark: Mark) {
        loop {
            self.queue.execute();
            let reported = block_on(&self.runtime, async {
                // Both branches are cancellation-safe: `wait_reported` leaves nothing behind, and a
                // push that races `pushed` leaves its permit for the next round.
                tokio::select! {
                    biased;
                    () = self.results.wait_reported(mark) => true,
                    () = self.queue.pushed() => false,
                }
            });
            if reported {
                return;
            }
        }
    }

    /// Completes, after `result`, the wait `wait` asks for on the calling
    /// thread (see [`ReportWait`]); from inside a delivery callback it does
    /// not wait, since that callback cannot report before it returns.
    fn wait_for_reports_blocking(
        &self,
        result: Result<(), Error>,
        wait: ReportWait,
        in_callback: bool,
    ) -> Result<(), Error> {
        if wait.applies(&result) && !in_callback {
            self.wait_reported_blocking(self.results.mark());
        }
        result
    }

    /// The `_cb` form of [`Self::wait_for_reports_blocking`]: waits on the
    /// task, without running the queued callbacks, which the caller pumps.
    async fn wait_for_reports(
        &self,
        result: Result<(), Error>,
        wait: ReportWait,
        in_callback: bool,
    ) -> Result<(), Error> {
        if wait.applies(&result) && !in_callback {
            self.results.wait_reported(self.results.mark()).await;
        }
        result
    }

    /// Waits until every `send_cb` that had returned before this call was
    /// handed to the producer: registered, refused, or waiting inside it on a
    /// task of its own, which this does not wait for (see the module docs).
    async fn drain_submitted_sends(&self) -> Result<(), Error> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        // A poisoned lock still holds a valid sender: its holders only read
        // or take it.
        let sent = match &*self.submit_tx.lock().unwrap_or_else(PoisonError::into_inner) {
            Some(tx) => tx.send(SubmitRequest::Barrier { ack: ack_tx }).is_ok(),
            // `destroy` stopped the queue once no `_cb` task was left, and
            // only the callbacks it runs call in after that, by when the
            // submission task has handed over everything queued: there is
            // nothing left to wait for.
            None => return Ok(()),
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
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to a `_cb` entry point: each
        // caller of `deliver` (`with_txn_control_cb`, `with_drain_cb`,
        // `kafka_producer_Producer_close_with_timeout_cb`) passes them on unchanged and delivers
        // them on one path only, so calling `cb` with `opaque` is the completion the caller is
        // waiting for. `error` is null or the fresh `box_error` handle above, owned by no one else,
        // so handing it to `cb` transfers its ownership, as
        // `kafka_producer_Producer_init_transactions_cb_t` documents; the job is a `FnOnce`, so
        // `cb` fires exactly once.
        self.queue.push(Box::new(move || unsafe {
            cb(error.get() as *mut kafka_common_Error_t, opaque.get())
        }));
    }
}

/// Whether an operation, once done, waits for the reports of the delivery
/// callbacks queued before it ended (see `ffi::producer::callback`): Java runs
/// the callbacks on the I/O thread before a record's future completes, so
/// `flush()` and `commitTransaction()` / `abortTransaction()` (which flush)
/// return after them, and `close()` joins that thread.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReportWait {
    /// No wait (`initTransactions`, `beginTransaction`,
    /// `sendOffsetsToTransaction`).
    No,
    /// After a successful operation (`flush`, `commitTransaction`,
    /// `abortTransaction`).
    OnSuccess,
    /// Whatever the operation's outcome, without a bound: Java's `close`
    /// joins the I/O thread after `forceClose()` (`KafkaProducer#close`),
    /// a failure being thrown after it.
    Always,
}

impl ReportWait {
    fn applies(self, result: &Result<(), Error>) -> bool {
        match self {
            ReportWait::No => false,
            ReportWait::OnSuccess => result.is_ok(),
            ReportWait::Always => true,
        }
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

/// The per-producer submission task: hands the `send_cb` records to the
/// producer in FIFO order, one task per producer (CLAUDE.md §13). It tries
/// each record without waiting: a record the producer takes, or refuses, is
/// delivered here, and one it would have to wait for moves to a
/// [`send_waiter`] of its own, so the task never awaits a record's wait and
/// the records behind it are not held back. Ends when the class handle is
/// destroyed and drops the sender.
async fn submission_loop(client: Arc<Client>, mut rx: UnboundedReceiver<SubmitRequest>) {
    while let Some(request) = rx.recv().await {
        let (record, callback, completion, cb, opaque) = match request {
            SubmitRequest::Send { record, callback, completion, cb, opaque } => {
                (record, callback, completion, cb, opaque)
            },
            SubmitRequest::Barrier { ack } => {
                let _ = ack.send(());
                continue;
            },
        };
        // Test-only: parks here while a test holds the gate (see `Client`).
        #[cfg(test)]
        drop(client.submission_gate.lock().await);
        // Kept for the waiter, since `try_send` consumes the record and hands
        // back only the callback; the key and value are `void *`, so the clone
        // copies the topic and the headers, on a path that already pays a
        // thread hop per record.
        let waiting = record.clone();
        let outcome = catch_task_panic("kafka_producer_Producer_send_with_callback_cb", async {
            client.producer()?.try_send(record, callback).await
        })
        .await;
        match outcome {
            Ok(TrySendOutcome::Accepted(future)) => deliver_send(&client, cb, opaque, Ok((future, completion))),
            Ok(TrySendOutcome::WouldBlock(callback)) => {
                client.spawn(send_waiter(Arc::clone(&client), waiting, callback, completion, cb, opaque))
            },
            Err(error) => deliver_send(&client, cb, opaque, Err(error)),
        }
    }
}

/// A record the producer would have had to wait for, on metadata or on buffer
/// memory: Java's blocking `send`, for up to `max.block.ms`, run on a task of
/// its own so that it holds back no other record, as a Java `send` blocking on
/// one thread holds back no other thread's (see [`submission_loop`]). The
/// drains do not wait for it, since Java's `flush()` and `commitTransaction()`
/// do not wait for a `send` in progress either, and `close` fails it as Java's
/// does, with "Producer closed while send in progress". A spawn per record,
/// but only for a record that has to wait, never for one the producer takes at
/// once; registered in [`Client::tasks`], so `destroy` waits for it.
///
/// `callback` is the one `try_send` handed back un-invoked.
async fn send_waiter(
    client: Arc<Client>,
    record: GenericRecord,
    callback: Option<Box<dyn Callback>>,
    completion: Option<KafkaFutureImpl<RecordMetadata>>,
    cb: kafka_producer_Producer_send_cb_t,
    opaque: SendPtr,
) {
    let result = catch_task_panic("kafka_producer_Producer_send_with_callback_cb", async {
        client.producer()?.send_with_callback(record, callback).await
    })
    .await;
    deliver_send(&client, cb, opaque, result.map(|future| (future, completion)));
}

/// The record's future type the Rust producer returns.
type RecordFuture = crate::common::KafkaFuture<RecordMetadata>;

/// Queues the completion of a `send`: the future handle on success, the
/// error otherwise. On `Err` the Rust producer owns the delivery-callback
/// obligation (it fires the callback itself when the record was registered),
/// so nothing is fired here.
fn deliver_send(
    client: &Arc<Client>,
    cb: kafka_producer_Producer_send_cb_t,
    opaque: SendPtr,
    result: Result<(RecordFuture, Option<KafkaFutureImpl<RecordMetadata>>), Error>,
) {
    let (value, error) = match result {
        Ok((future, completion)) => (client.box_send_future(&future, completion), std::ptr::null_mut()),
        Err(error) => (std::ptr::null_mut(), box_error(error)),
    };
    let value = SendPtr(value as *mut c_void);
    let error = SendPtr(error as *mut c_void);
    // SAFETY: `cb` and `opaque` are the pair the C caller passed to
    // `kafka_producer_Producer_send_with_callback_cb` (or to `send_cb`, which forwards to it),
    // which, the producer having had to wait for the record, `queue_send` puts in one
    // `SubmitRequest::Send`, which `submission_loop` delivers here once, itself or through the
    // one `send_waiter` it moves the record to (when the channel refuses the request,
    // `queue_send` calls `cb` itself instead), so calling `cb` with `opaque` is the completion
    // the caller is waiting for. `value` and `error` are each null or
    // the fresh `box_future` / `box_error` handle above, owned by no one else, so handing them to
    // `cb` transfers their ownership, as `kafka_producer_Producer_send_cb_t` documents; the job is
    // a `FnOnce`, so `cb` fires exactly once.
    client.queue.push(Box::new(move || unsafe {
        cb(
            value.get() as *mut kafka_common_KafkaFuture_t,
            error.get() as *mut kafka_common_Error_t,
            opaque.get(),
        )
    }));
}

impl Client {
    /// The C future a `send` hands out: the producer's own future for a
    /// record sent without a delivery callback, or, with one, the future its
    /// report completes (`completion`, see [`delivery_callback`]), whose
    /// blocking `get` runs the queued callbacks while it waits.
    fn box_send_future(
        &self,
        future: &RecordFuture,
        completion: Option<KafkaFutureImpl<RecordMetadata>>,
    ) -> *mut kafka_common_KafkaFuture_t {
        match completion {
            None => box_future(
                metadata_future(future),
                Some(self.runtime.clone()),
                Some(Arc::clone(&self.queue)),
            ),
            Some(completion) => box_report_future(
                metadata_future(&completion.future()),
                self.runtime.clone(),
                Arc::clone(&self.queue),
            ),
        }
    }
}

/// The C view of a `KafkaFuture<RecordMetadata>`: resolves to a
/// `kafka_producer_RecordMetadata_t *` the future owns.
fn metadata_future(future: &RecordFuture) -> crate::ffi::kafka_future::FfiFuture {
    // SAFETY: `box_record_metadata` returns an owned record-metadata handle that
    // `destroy_record_metadata` frees exactly, and nothing else frees it. The handle is an
    // immutable Rust value behind a `Box` (`Send + Sync`), so it may be freed from any thread and
    // read through its accessors from several at once.
    unsafe {
        map_future_handle(
            future,
            |metadata| box_record_metadata(metadata) as *mut c_void,
            destroy_record_metadata,
        )
    }
}

/// Frees a record-metadata handle held as a future's value (see
/// [`map_future_handle`]).
///
/// # Safety
///
/// `element` must be an owned record-metadata handle not yet destroyed.
unsafe fn destroy_record_metadata(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned record-metadata handle not yet
    // destroyed, which is what `kafka_producer_RecordMetadata_destroy` requires.
    unsafe { kafka_producer_RecordMetadata_destroy(element as *mut _) }
}

/// The class handle behind `kafka_producer_KafkaProducer_t` and
/// `kafka_producer_MockProducer_t`: the concrete producer, the shared client
/// state and the runtime that drives both.
pub(crate) struct ProducerHandle<P: ?Sized> {
    /// The producer's one strong reference ([`Client::producer`] is weak).
    producer: Arc<P>,
    /// The `kafka_producer_Producer_t` view is the address of this field in
    /// the boxed handle, which `destroy` keeps where it is until its very end,
    /// so the callbacks it runs can still report through the view.
    client: Arc<Client>,
    runtime: tokio::runtime::Runtime,
    /// The submission task, awaited by `destroy` once the `_cb` tasks have
    /// ended (see [`Self::destroy`]); kept apart from `Client::tasks` for that.
    submission: JoinHandle<()>,
}

impl<P> ProducerHandle<P>
where
    P: DynTrySend<GenericValue, GenericValue> + 'static,
{
    /// Builds the producer with `make`, inside the context of a fresh
    /// multi-thread runtime (a `KafkaProducer` spawns its Sender task on
    /// construction), and wraps it with the runtime and the submission task.
    ///
    /// `follows_kafka_producer_close` is true for a `KafkaProducer`, whose
    /// `close(Duration)` rejects a negative timeout and, from a delivery
    /// callback, overrides a positive one to zero, and whose `flush()` fails
    /// from a delivery callback; false for the mock, which does neither (see
    /// [`Client::follows_kafka_producer_close`]).
    ///
    /// `tries_send_inline` is false when a serializer of the producer is a C
    /// implementation (see [`Client::tries_send_inline`]).
    pub(crate) fn new(
        make: impl FnOnce() -> Result<P, Error>,
        follows_kafka_producer_close: bool,
        tries_send_inline: bool,
    ) -> Result<Box<Self>, Error> {
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
        let dyn_producer: Arc<DynProducerGv> = Arc::clone(&producer) as Arc<DynProducerGv>;
        let client = Arc::new(Client {
            producer: Arc::downgrade(&dyn_producer),
            runtime: runtime.handle().clone(),
            queue: Arc::new(CallbackQueue::new()),
            results: Arc::new(CallbackResults::new()),
            submit_tx: Mutex::new(Some(submit_tx)),
            #[cfg(test)]
            submission_gate: tokio::sync::Mutex::new(()),
            txn_control_busy: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            follows_kafka_producer_close,
            tries_send_inline,
        });
        let submission = spawn_callback_task(runtime.handle(), submission_loop(Arc::clone(&client), submit_rx));
        Ok(Box::new(Self { producer, client, runtime, submission }))
    }

    /// The concrete producer.
    pub(crate) fn producer(&self) -> &P {
        &self.producer
    }

    /// The `Producer` view of this handle (see the module docs).
    pub(crate) fn as_producer(&self) -> *const kafka_producer_Producer_t {
        &self.client as *const Arc<Client> as *const kafka_producer_Producer_t
    }

    /// Tears the handle down ([`Self::teardown`]) on the calling thread, or,
    /// called inside a tokio runtime, where `teardown`'s `block_on` would
    /// panic, on a new thread, returning at once (see [`spawn_teardown`]);
    /// the pending callbacks then fire on that thread.
    pub(crate) fn destroy(self: Box<Self>) {
        if in_tokio_runtime() {
            /// # Safety
            ///
            /// `handle` must be the `Box::into_raw` of a `ProducerHandle<P>`
            /// whose only teardown this is.
            unsafe fn teardown<P>(handle: usize)
            where
                P: DynTrySend<GenericValue, GenericValue> + 'static,
            {
                // SAFETY: Per this function's `# Safety`, `handle` is the `Box::into_raw` of a
                // `ProducerHandle<P>` torn down only here, so taking it back frees it exactly once.
                unsafe { Box::from_raw(handle as *mut ProducerHandle<P>) }.teardown();
            }
            // SAFETY: `teardown`'s `# Safety` holds: the address is the `Box::into_raw` of this
            // `ProducerHandle<P>`, just taken, and this is its only teardown, since the box is
            // consumed here. `teardown` waits with `block_on`, which is sound on the new thread,
            // outside any runtime.
            unsafe { spawn_teardown("kafka-producer-destroy", Box::into_raw(self) as usize, teardown::<P>) };
        } else {
            self.teardown();
        }
    }

    /// Awaits every task spawned by a `_cb` call or for a waiting record, then
    /// stops the submission task and awaits it and the records it left
    /// waiting, closes a `KafkaProducer` the caller did not close, fires the
    /// pending callbacks and drops the producer (see the module docs), then
    /// stops the runtime and fires the completions of the
    /// `KafkaFuture_get_cb` calls still waiting, each exactly once.
    ///
    /// The submission queue stays open until the `_cb` tasks have ended: a
    /// task that has not reached its drain yet must still be able to queue its
    /// barrier. A forced `close_with_timeout_cb(0)` queued behind an untimed
    /// `close_cb` (Python's cancelled async `close()`) is such a task: closing
    /// the queue first failed its drain, so it never closed the producer, and
    /// the untimed close, the record it waited for and so `destroy` itself
    /// waited out `max.block.ms`.
    ///
    /// While it awaits the tasks it runs the queued callbacks, and completes
    /// the delivery callbacks still unreported as if each had reported
    /// success: a `flush_cb` or `commit_transaction_cb` task waits for those
    /// reports, which nothing but `destroy` is left to give, since no report
    /// may arrive from another thread once `destroy` began. The callbacks it
    /// runs may report inline, through the view, which therefore stays valid
    /// until the very end: the handle's client field it points at is dropped
    /// last, with the handle's allocation, after the runtime stopped; the
    /// producer itself is dropped before, through its one strong reference.
    ///
    /// A panic caught earlier may have poisoned `submit_tx` or `tasks`; both
    /// are taken regardless, so the handle is still freed and every pending
    /// callback still fires exactly once.
    // The box is load-bearing: the view points into it, and the callbacks
    // `destroy` runs report through the view until it returns.
    #[expect(clippy::boxed_local)]
    fn teardown(self: Box<Self>) {
        // Moved out of the box field by field, so `client`, which the view
        // points at, stays in place until the box is freed at the end.
        let producer = self.producer;
        let runtime = self.runtime;
        let submission = self.submission;
        let client = &self.client;
        await_tasks(client, &runtime);
        // No `_cb` task is left, and none can start: the handle is gone. The
        // submission task may still leave records waiting on tasks of their
        // own while it hands over the last ones queued; they are awaited once
        // it ended.
        drop(client.submit_tx.lock().unwrap_or_else(PoisonError::into_inner).take());
        let _ = runtime.block_on(submission);
        await_tasks(client, &runtime);
        client.queue.execute();
        // A `KafkaProducer` the caller did not close is closed as `close()`
        // closes it, now that every queued `send_cb` was handed over: its sender
        // sends the records it holds before it stops. Leaving them to the sender
        // task, as dropping the producer does, is not possible here: the
        // shutdown below would cancel it, and a delivery callback must not fire
        // after `destroy` returns.
        if producer.has_unclosed_sender() {
            let closed = runtime.block_on(catch_task_panic("kafka_producer_KafkaProducer_destroy", producer.close()));
            if let Err(error) = closed {
                log::warn!("Closing the producer on destroy failed: {error}");
            }
        }
        // Dropping the producer then has no sender left to stop (a
        // `KafkaProducer` needs the runtime context to drop). The queue runs
        // once more for the delivery callbacks the close fired.
        {
            let _enter = runtime.enter();
            drop(producer);
        }
        client.queue.execute();
        // A `KafkaFuture_get_cb` task waiting on a record's future gets its
        // value before the runtime stops.
        complete_unreported(&client.results);
        // A blocking `KafkaFuture_get` with a timeout drops its timer before
        // the runtime's timers stop, and waits on (see `ffi::kafka_future`).
        client.queue.mark_client_stopping();
        runtime.shutdown_timeout(Duration::from_secs(5));
        // Shutting the runtime down dropped the `KafkaFuture_get_cb` tasks
        // still waiting, each queuing its completion as it went (see
        // `ffi::kafka_future`): run them, closing the queue first so a later
        // `get_cb` on a future of this producer runs inline. The client is
        // stopped before they run, so a blocking `KafkaFuture_get` one of them
        // makes, or any still waiting, answers what its future holds rather
        // than waiting on a drain that waits on it.
        client.queue.close();
        client.queue.mark_client_stopped();
        client.queue.execute();
        complete_unreported(&client.results);
    }
}

/// Awaits the tasks registered in [`Client::tasks`] until none is left, the
/// ones they register meanwhile included, running the queued callbacks and
/// completing the unreported ones while it waits (see
/// [`ProducerHandle::teardown`]).
fn await_tasks(client: &Client, runtime: &tokio::runtime::Runtime) {
    loop {
        let tasks: Vec<JoinHandle<()>> =
            std::mem::take(&mut *client.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        if tasks.is_empty() {
            return;
        }
        for mut task in tasks {
            loop {
                client.queue.execute();
                complete_unreported(&client.results);
                let finished = runtime.block_on(async {
                    // Both branches are cancellation-safe: a `JoinHandle` is polled by `&mut`,
                    // and a push that races `pushed` leaves its permit for the next round.
                    tokio::select! {
                        biased;
                        _ = &mut task => true,
                        () = client.queue.pushed() => false,
                    }
                });
                if finished {
                    break;
                }
            }
        }
    }
}

/// Completes the delivery callbacks still awaiting their report as if each
/// had reported success, for a producer being destroyed (see
/// [`ProducerHandle::destroy`]): each record's future then completes with the
/// record's outcome.
fn complete_unreported(results: &CallbackResults) {
    for sink in results.drain() {
        sink(Ok(std::ptr::null_mut()));
    }
}

/// The client behind an interface handle.
///
/// # Safety
///
/// `producer` must be a live view from an `__as_Producer` function, kept
/// alive for `'a`.
unsafe fn client_ref<'a>(producer: *const kafka_producer_Producer_t) -> &'a Arc<Client> {
    // SAFETY: Per this function's `# Safety`, `producer` is a live view from an `__as_Producer`
    // function, kept alive for `'a`. Both such functions return `ProducerHandle::as_producer`, the
    // address of the `Arc<Client>` boxed in the class handle's `client` field, so the cast reads
    // that `Arc<Client>`, which lives as long as the class handle and, while its `_destroy` runs,
    // until that returns (`ProducerHandle::destroy` keeps the box to its end). It is only ever
    // read, never written, so the shared reference is sound.
    unsafe { &*(producer as *const Arc<Client>) }
}

/// Where the guard of a `_cb` entry point delivers a caught panic: the
/// producer's callbacks vector (see [`deliver_panic`]).
///
/// # Safety
///
/// `producer` must be a live view from an `__as_Producer` function.
pub(crate) unsafe fn completion_sink(producer: *const kafka_producer_Producer_t) -> impl FnOnce(CallbackJob) {
    // SAFETY: Per this function's `# Safety`, `producer` is a live view, as `client_ref` requires;
    // the reference is used only to clone the `Arc`, within this statement.
    let client = Arc::clone(unsafe { client_ref(producer) });
    move |job| client.queue.push(job)
}

/// Runs a transaction-control operation on the calling thread: takes the
/// flag, drains the submission queue, runs `op`, then releases the flag and
/// waits as `wait` asks (see [`Client::wait_for_reports_blocking`]). An
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `out_err` must be as `store_error` requires.
unsafe fn with_txn_control<F, Fut>(
    client: &Arc<Client>,
    wait: ReportWait,
    out_err: *mut *mut kafka_common_Error_t,
    op: F,
) where
    F: FnOnce(Arc<DynProducerGv>) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    if let Err(error) = client.acquire_txn_control() {
        // SAFETY: Per this function's `# Safety`, `out_err` is as `store_error` requires.
        return unsafe { store_error(out_err, error) };
    }
    let in_callback = client.in_callback();
    let result = {
        let _guard = TxnControlGuard(Arc::clone(client));
        client.producer().and_then(|producer| {
            block_on(&client.runtime, async {
                client.drain_submitted_sends().await?;
                op(producer).await
            })
        })
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(client.wait_for_reports_blocking(result, wait, in_callback), out_err) }
}

/// The `_cb` form of [`with_txn_control`]: the drain, `op` and the wait run
/// on a spawned task and the result is queued; a panic on the task is queued
/// as the [`panic_error`](crate::ffi::common::panic_error) of `fn_name`, the
/// entry point. The wait does not run the queued callbacks (the caller pumps
/// them) and so happens from a delivery callback too: it cannot block it.
fn with_txn_control_cb<F, Fut>(
    fn_name: &'static str,
    client: &Arc<Client>,
    wait: ReportWait,
    cb: kafka_producer_Producer_init_transactions_cb_t,
    opaque: *mut c_void,
    op: F,
) where
    F: FnOnce(Arc<DynProducerGv>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    let opaque = SendPtr(opaque);
    if let Err(error) = client.acquire_txn_control() {
        client.deliver(cb, opaque, Err(error));
        return;
    }
    let guard = TxnControlGuard(Arc::clone(client));
    let task_client = Arc::clone(client);
    client.spawn(async move {
        let result = catch_task_panic(fn_name, async {
            let _guard = guard;
            task_client.drain_submitted_sends().await?;
            op(task_client.producer()?).await
        })
        .await;
        let result = task_client.wait_for_reports(result, wait, false).await;
        task_client.deliver(cb, opaque, result);
    });
}

/// How long a [`with_drain`] operation may wait for the submission queue.
#[derive(Clone, Copy)]
enum DrainBound {
    /// Until every queued `send_cb` was handed over (`flush`, `close()`).
    Unbounded,
    /// At most this long; the operation gets the time left (`close(Duration)`).
    Within(Duration),
}

impl Client {
    /// Drains the submission queue within `bound` and returns the time left
    /// of it (`Duration::MAX` when unbounded).
    ///
    /// Giving up on the drain is cancel-safe: the drain has only queued a
    /// `Barrier` and awaits its oneshot, so a late ack is discarded, and the
    /// records still queued are handed over afterwards, to a producer that by
    /// then refuses them.
    async fn drain_within(&self, bound: DrainBound) -> Result<Duration, Error> {
        match bound {
            DrainBound::Unbounded => self.drain_submitted_sends().await.map(|()| Duration::MAX),
            DrainBound::Within(timeout) => {
                let start = Instant::now();
                if let Ok(result) = tokio::time::timeout(timeout, self.drain_submitted_sends()).await {
                    result?;
                }
                Ok(timeout.saturating_sub(start.elapsed()))
            },
        }
    }
}

/// Runs a non-transactional `void` operation after draining the submission
/// queue within `bound`, on the calling thread, then waits as `wait` asks
/// (see [`Client::wait_for_reports_blocking`]); `op` gets the time left of
/// `bound`. `in_callback` is [`Client::in_callback`], read by the caller.
fn with_drain<F, Fut>(
    client: &Arc<Client>,
    bound: DrainBound,
    wait: ReportWait,
    in_callback: bool,
    op: F,
) -> Result<(), Error>
where
    F: FnOnce(Arc<DynProducerGv>, Duration) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let result = client.producer().and_then(|producer| {
        block_on(&client.runtime, async {
            let left = client.drain_within(bound).await?;
            op(producer, left).await
        })
    });
    client.wait_for_reports_blocking(result, wait, in_callback)
}

/// The `_cb` form of [`with_drain`], whose wait runs on the task without
/// running the queued callbacks; a panic on the task is queued as the
/// [`panic_error`](crate::ffi::common::panic_error) of `fn_name`, the entry
/// point.
#[expect(clippy::too_many_arguments)]
fn with_drain_cb<F, Fut>(
    fn_name: &'static str,
    client: &Arc<Client>,
    bound: DrainBound,
    wait: ReportWait,
    in_callback: bool,
    cb: kafka_producer_Producer_init_transactions_cb_t,
    opaque: *mut c_void,
    op: F,
) where
    F: FnOnce(Arc<DynProducerGv>, Duration) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    let opaque = SendPtr(opaque);
    let task_client = Arc::clone(client);
    client.spawn(async move {
        let result = catch_task_panic(fn_name, async {
            let left = task_client.drain_within(bound).await?;
            op(task_client.producer()?, left).await
        })
        .await;
        let result = task_client.wait_for_reports(result, wait, in_callback).await;
        task_client.deliver(cb, opaque, result);
    });
}

/// Reads a `kafka_Map_t` of `kafka_common_TopicPartition_t *` to
/// `kafka_consumer_OffsetAndMetadata_t *`.
///
/// # Safety
///
/// `offsets` must be a live map whose keys and values are live handles of
/// those types, and no other call may mutate it during this one.
unsafe fn offsets_map(offsets: *const kafka_Map_t) -> HashMap<TopicPartition, OffsetAndMetadata> {
    // SAFETY: Per this function's `# Safety`, `offsets` is a live map that no other call mutates
    // during this one, as `map_entries` requires; the entries are read only within this expression.
    unsafe { map_entries(offsets) }
        .iter()
        .map(|&(tp, oam)| {
            (
                // SAFETY: Per this function's `# Safety`, every key of `offsets` is a live
                // topic-partition handle, as `topic_partition_ref` requires; the reference is only
                // cloned.
                unsafe { topic_partition_ref(tp as *const _) }.clone(),
                // SAFETY: Per this function's `# Safety`, every value of `offsets` is a live
                // offset-and-metadata handle, as `offset_and_metadata_ref` requires; the reference
                // is only cloned.
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
pub type kafka_producer_Producer_begin_transaction_cb_t =
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
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no `transactional.id` has been configured
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` error indicating that the configured
///   transactional.id is not authorized, or the idempotent producer id is unavailable. See the
///   error for more details. User may retry this function call after fixing the permission.
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal error or
///   for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the time taken for initialize the transaction has
///   surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
///
/// # Safety
///
/// `self_` must be a live view. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions(
    self_: *const kafka_producer_Producer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires; the
    // reference does not outlive the call, and `out_err` is as `with_txn_control` requires.
    unsafe {
        with_txn_control(client_ref(self_), ReportWait::No, out_err, |p| async move {
            p.init_transactions().await
        })
    }
}

/// `Producer.initTransactions()`, completion queued.
///
/// # Parameters
///
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no `transactional.id` has been configured
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` error indicating that the configured
///   transactional.id is not authorized, or the idempotent producer id is unavailable. See the
///   error for more details. User may retry this function call after fixing the permission.
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal error or
///   for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the time taken for initialize the transaction has
///   surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. The body hands `cb` over
    // only as the last step of `with_txn_control_cb`: to `Client::deliver` before its early return,
    // or to the task `Client::spawn` registers, after which nothing can panic. So `cb` has not
    // fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_init_transactions_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_init_transactions_cb_t,
    opaque: *mut c_void,
) {
    with_txn_control_cb(
        "kafka_producer_Producer_init_transactions_cb",
        // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
        // `with_txn_control_cb` uses the reference only before it returns, cloning the `Arc` for
        // the task it spawns.
        unsafe { client_ref(self_) },
        ReportWait::No,
        cb,
        opaque,
        |p| async move { p.init_transactions().await },
    );
}

/// `Producer.beginTransaction()`: synchronous in Java and Rust, still
/// ordered after the queued `send_cb`s like every control operation.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no `transactional.id` has been configured
///   or if `kafka_producer_Producer_init_transactions` has not yet been invoked
/// - `kafka_common_Error_is_producer_fenced_error` if another producer with the same
///   transactional.id is active
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   `transactional.id` is not authorized. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal error or
///   for any other unexpected error
///
/// # Safety
///
/// `self_` must be a live view. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_begin_transaction(
    self_: *const kafka_producer_Producer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires; the
    // reference does not outlive the call, and `out_err` is as `with_txn_control` requires.
    unsafe {
        with_txn_control(
            client_ref(self_),
            ReportWait::No,
            out_err,
            |p| async move { p.begin_transaction() },
        )
    }
}

/// `Producer.beginTransaction()`, completion queued: ordered after the queued
/// `send_cb`s like the blocking form, without blocking the caller on them.
///
/// # Parameters
///
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no `transactional.id` has been configured
///   or if `kafka_producer_Producer_init_transactions` has not yet been invoked
/// - `kafka_common_Error_is_producer_fenced_error` if another producer with the same
///   transactional.id is active
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   `transactional.id` is not authorized. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal error or
///   for any other unexpected error
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. The body hands `cb` over
    // only as the last step of `with_txn_control_cb`: to `Client::deliver` before its early return,
    // or to the task `Client::spawn` registers, after which nothing can panic. So `cb` has not
    // fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_begin_transaction_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_begin_transaction_cb_t,
    opaque: *mut c_void,
) {
    with_txn_control_cb(
        "kafka_producer_Producer_begin_transaction_cb",
        // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
        // `with_txn_control_cb` uses the reference only before it returns, cloning the `Arc` for
        // the task it spawns.
        unsafe { client_ref(self_) },
        ReportWait::No,
        cb,
        opaque,
        |p| async move { p.begin_transaction() },
    );
}

/// `Producer.sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>,
/// ConsumerGroupMetadata)`, blocking: `offsets` maps
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// and is copied during the call.
///
/// # Parameters
///
/// - `offsets` - the offsets to commit, the next message your application will consume for each
///   partition
/// - `group_metadata` - the consumer group metadata, extracted from the consumer via
///   `kafka_consumer_Consumer_group_metadata`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no transactional.id has been configured
///   or no transaction has been started.
/// - `kafka_common_Error_is_producer_fenced_error` fatal error indicating another producer with the
///   same transactional.id is active
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0) or the broker doesn't
///   support the latest version of transactional API with all consumer group metadata (i.e. if its
///   version is lower than 2.5.0).
/// - `kafka_common_Error_is_unsupported_for_message_format_error` fatal error indicating the
///   message format used for the offsets topic on the broker does not support transactions
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   transactional.id is not authorized, or the consumer group id is not authorized.
/// - `kafka_common_Error_is_consumer_commit_failed_error` if the commit failed and cannot be
///   retried (e.g. if the consumer has been kicked out of the group). Users should handle this by
///   aborting the transaction.
/// - `kafka_common_Error_is_fenced_instance_id_error` if this producer instance gets fenced by
///   broker due to a mis-configured consumer instance id within group metadata.
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal or
///   abortable error, or for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the combined time taken for resolving topic metadata
///   and sending the offsets has surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
///
/// # Safety
///
/// `self_` must be a live view, `offsets` a live map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one, and `group_metadata` a live
/// handle. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_offsets_to_transaction(
    self_: *const kafka_producer_Producer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `offsets` is a live map of topic-partition to
    // offset-and-metadata handles that no other call mutates during this one, as `offsets_map`
    // requires.
    let offsets = unsafe { offsets_map(offsets) };
    // SAFETY: Per this function's `# Safety`, `group_metadata` is a live handle, as
    // `group_metadata_ref` requires; the reference is only cloned, within this statement.
    let group_metadata = Arc::clone(unsafe { group_metadata_ref(group_metadata) });
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires; the
    // reference does not outlive the call, and `out_err` is as `with_txn_control` requires.
    unsafe {
        with_txn_control(client_ref(self_), ReportWait::No, out_err, |p| async move {
            p.send_offsets_to_transaction(offsets, &*group_metadata).await
        })
    }
}

/// `Producer.sendOffsetsToTransaction(...)`, completion queued.
///
/// # Parameters
///
/// - `offsets` - the offsets to commit, the next message your application will consume for each
///   partition
/// - `group_metadata` - the consumer group metadata, extracted from the consumer via
///   `kafka_consumer_Consumer_group_metadata`
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no transactional.id has been configured
///   or no transaction has been started.
/// - `kafka_common_Error_is_producer_fenced_error` fatal error indicating another producer with the
///   same transactional.id is active
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0) or the broker doesn't
///   support the latest version of transactional API with all consumer group metadata (i.e. if its
///   version is lower than 2.5.0).
/// - `kafka_common_Error_is_unsupported_for_message_format_error` fatal error indicating the
///   message format used for the offsets topic on the broker does not support transactions
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   transactional.id is not authorized, or the consumer group id is not authorized.
/// - `kafka_common_Error_is_consumer_commit_failed_error` if the commit failed and cannot be
///   retried (e.g. if the consumer has been kicked out of the group). Users should handle this by
///   aborting the transaction.
/// - `kafka_common_Error_is_fenced_instance_id_error` if this producer instance gets fenced by
///   broker due to a mis-configured consumer instance id within group metadata.
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal or
///   abortable error, or for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the combined time taken for resolving topic metadata
///   and sending the offsets has surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
///
/// # Safety
///
/// `self_` must be a live view, `offsets` a live map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one, and `group_metadata` a live
/// handle.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading `offsets` and
    // `group_metadata` comes first and hands nothing over; the body hands `cb` over only as the
    // last step of `with_txn_control_cb`: to `Client::deliver` before its early return, or to the
    // task `Client::spawn` registers, after which nothing can panic. So `cb` has not fired for this
    // `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_offsets_to_transaction_cb(
    self_: *const kafka_producer_Producer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    cb: kafka_producer_Producer_send_offsets_to_transaction_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `offsets` is a live map of topic-partition to
    // offset-and-metadata handles that no other call mutates during this one, as `offsets_map`
    // requires.
    let offsets = unsafe { offsets_map(offsets) };
    // SAFETY: Per this function's `# Safety`, `group_metadata` is a live handle, as
    // `group_metadata_ref` requires; the reference is only cloned, within this statement.
    let group_metadata = Arc::clone(unsafe { group_metadata_ref(group_metadata) });
    with_txn_control_cb(
        "kafka_producer_Producer_send_offsets_to_transaction_cb",
        // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
        // `with_txn_control_cb` uses the reference only before it returns, cloning the `Arc` for
        // the task it spawns.
        unsafe { client_ref(self_) },
        ReportWait::No,
        cb,
        opaque,
        |p| async move { p.send_offsets_to_transaction(offsets, &*group_metadata).await },
    );
}

/// `Producer.commitTransaction()`, blocking.
///
/// Once the commit succeeded it waits for the delivery callbacks queued
/// before it ended to report through
/// [`kafka_producer_Producer__set_callback_result`], running the queued
/// callbacks on the calling thread meanwhile, as Java's `commitTransaction` returns
/// after the callbacks of the records it flushes; called from inside a
/// delivery callback it does not wait for them, since that callback cannot
/// report before it returns.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no transactional.id has been configured
///   or no transaction has been started
/// - `kafka_common_Error_is_producer_fenced_error` fatal error indicating another producer with the
///   same transactional.id is active
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   transactional.id is not authorized. See the error for more details
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal or
///   abortable error, or for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the time taken for committing the transaction has
///   surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
///
/// # Safety
///
/// `self_` must be a live view. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction(
    self_: *const kafka_producer_Producer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires; the
    // reference does not outlive the call, and `out_err` is as `with_txn_control` requires.
    unsafe {
        with_txn_control(client_ref(self_), ReportWait::OnSuccess, out_err, |p| async move {
            p.commit_transaction().await
        })
    }
}

/// `Producer.commitTransaction()`, completion queued.
///
/// Once the commit succeeded the completion waits for the reports of the
/// delivery callbacks queued before it ended, made through
/// [`kafka_producer_Producer__set_callback_result`]; it does not run them.
///
/// # Parameters
///
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no transactional.id has been configured
///   or no transaction has been started
/// - `kafka_common_Error_is_producer_fenced_error` fatal error indicating another producer with the
///   same transactional.id is active
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   transactional.id is not authorized. See the error for more details
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal or
///   abortable error, or for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the time taken for committing the transaction has
///   surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. The body hands `cb` over
    // only as the last step of `with_txn_control_cb`: to `Client::deliver` before its early return,
    // or to the task `Client::spawn` registers, after which nothing can panic. So `cb` has not
    // fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_commit_transaction_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_commit_transaction_cb_t,
    opaque: *mut c_void,
) {
    with_txn_control_cb(
        "kafka_producer_Producer_commit_transaction_cb",
        // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
        // `with_txn_control_cb` uses the reference only before it returns, cloning the `Arc` for
        // the task it spawns.
        unsafe { client_ref(self_) },
        ReportWait::OnSuccess,
        cb,
        opaque,
        |p| async move { p.commit_transaction().await },
    );
}

/// `Producer.abortTransaction()`, blocking.
///
/// Once the abort succeeded it waits for the delivery callbacks queued
/// before it ended to report through
/// [`kafka_producer_Producer__set_callback_result`], running the queued
/// callbacks on the calling thread meanwhile, as Java's `abortTransaction` returns
/// after the callbacks of the records it aborts; called from inside a
/// delivery callback it does not wait for them, since that callback cannot
/// report before it returns.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no transactional.id has been configured
///   or no transaction has been started
/// - `kafka_common_Error_is_producer_fenced_error` fatal error indicating another producer with the
///   same transactional.id is active
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   transactional.id is not authorized. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal error or
///   for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the time taken for aborting the transaction has
///   surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
///
/// # Safety
///
/// `self_` must be a live view. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction(
    self_: *const kafka_producer_Producer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires; the
    // reference does not outlive the call, and `out_err` is as `with_txn_control` requires.
    unsafe {
        with_txn_control(client_ref(self_), ReportWait::OnSuccess, out_err, |p| async move {
            p.abort_transaction().await
        })
    }
}

/// `Producer.abortTransaction()`, completion queued.
///
/// Once the abort succeeded the completion waits for the reports of the
/// delivery callbacks queued before it ended, made through
/// [`kafka_producer_Producer__set_callback_result`]; it does not run them.
///
/// # Parameters
///
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if no transactional.id has been configured
///   or no transaction has been started
/// - `kafka_common_Error_is_producer_fenced_error` fatal error indicating another producer with the
///   same transactional.id is active
/// - `kafka_common_Error_is_invalid_producer_epoch_error` if the producer has attempted to produce
///   with an old epoch to the partition leader. See the error for more details
/// - `kafka_common_Error_is_unsupported_version_error` fatal error indicating the broker does not
///   support transactions (i.e. if its version is lower than 0.11.0.0)
/// - `kafka_common_Error_is_authorization_error` fatal error indicating that the configured
///   transactional.id is not authorized. See the error for more details
/// - `kafka_common_Error_is_kafka_error` if the producer has encountered a previous fatal error or
///   for any other unexpected error
/// - `kafka_common_Error_is_timeout_error` if the time taken for aborting the transaction has
///   surpassed `max.block.ms`.
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. The body hands `cb` over
    // only as the last step of `with_txn_control_cb`: to `Client::deliver` before its early return,
    // or to the task `Client::spawn` registers, after which nothing can panic. So `cb` has not
    // fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_abort_transaction_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_abort_transaction_cb_t,
    opaque: *mut c_void,
) {
    with_txn_control_cb(
        "kafka_producer_Producer_abort_transaction_cb",
        // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
        // `with_txn_control_cb` uses the reference only before it returns, cloning the `Arc` for
        // the task it spawns.
        unsafe { client_ref(self_) },
        ReportWait::OnSuccess,
        cb,
        opaque,
        |p| async move { p.abort_transaction().await },
    );
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

/// `Producer.send(ProducerRecord)`, blocking until the record is registered
/// (serialized, partitioned and appended to a batch): returns a
/// `kafka_common_KafkaFuture_t` resolving to a
/// `kafka_producer_RecordMetadata_t *`.
///
/// # Parameters
///
/// - `record` - The record to send. If the topic or the partition specified in it cannot be found
///   in metadata within `max.block.ms`, the returned future will time out when retrieved.
///
/// # Returns
///
/// A future which will eventually contain the response information.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if a transactional.id has been configured
///   and no transaction has been started, or when send is invoked after producer has been closed.
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked
/// - `kafka_common_Error_is_serialization_error` If the key or value are not valid objects given
///   the configured serializers
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
/// - `kafka_common_Error_is_kafka_error` If a Kafka related error occurs that does not belong to
///   the public API errors.
///
/// # Safety
///
/// `self_` and `record` must be live handles and `out_err` a valid slot
/// pointing at null.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`; the
/// `kafka_producer_RecordMetadata_t *` it resolves to is borrowed from the future, valid until the
/// future is destroyed. The record stays the caller's; its key and value `void *` are read during
/// the call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, `record` a live handle and
    // `out_err` a valid slot, and a null `callback` is allowed, which is what
    // `kafka_producer_Producer_send_with_callback`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_send_with_callback(self_, record, std::ptr::null_mut(), out_err) }
}

/// `Producer.send(ProducerRecord)`, completed through `cb` with the owned
/// future, or the error: when both serializers are built-in, the producer is
/// tried on the calling thread first, and `cb` fires inside this call when it
/// takes or refuses the record at once; a record it would have to wait for,
/// and every record of a producer with a C serializer, is copied and queued
/// for the submission task, and `cb` fires later, through the callback
/// vector, once the record was handed to the producer (see
/// [`kafka_producer_Producer_send_with_callback_cb`]). A
/// C serializer is thus never invoked on the calling thread, and an
/// asynchronous one may report from it once this call returned. The
/// record's key and value `void *` are read during the call in the first
/// case and stay borrowed until `cb` fires in the second.
///
/// # Parameters
///
/// - `record` - The record to send. If the topic or the partition specified in it cannot be found
///   in metadata within `max.block.ms`, the returned future will time out when retrieved.
/// - `cb` - the completion, receiving the future or the error
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if a transactional.id has been configured
///   and no transaction has been started, or when send is invoked after producer has been closed.
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked
/// - `kafka_common_Error_is_serialization_error` If the key or value are not valid objects given
///   the configured serializers
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
/// - `kafka_common_Error_is_kafka_error` If a Kafka related error occurs that does not belong to
///   the public API errors.
///
/// # Safety
///
/// `self_` and `record` must be live handles.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. The body's one step, the
    // call to `kafka_producer_Producer_send_with_callback_cb`, delivers `cb` itself (a panic inside
    // it included) before it returns. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_cb(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    cb: kafka_producer_Producer_send_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view and `record` a live handle,
    // and a null `callback` is allowed, which is what
    // `kafka_producer_Producer_send_with_callback_cb`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_send_with_callback_cb(self_, record, std::ptr::null_mut(), cb, opaque) }
}

/// `Producer.send(ProducerRecord, Callback)`, blocking as
/// [`kafka_producer_Producer_send`]; `callback` (`NULL` for none) is a
/// `kafka_producer_Callback_t` whose `on_completion` is queued on the
/// callback vector when the record is acknowledged or fails, exactly once,
/// with a `callback_id` it reports through
/// [`kafka_producer_Producer__set_callback_result`] once it finished. The
/// callback's `self` must stay alive until then. The registration handle
/// `callback` is consumed by the call, whatever its outcome (an error in
/// `*out_err` included): the caller must not use it or pass it to
/// `kafka_producer_Callback_destroy` afterwards.
///
/// With a callback, the delivered future completes with the record's
/// outcome only once the callback reported, as Java completes the future
/// after `onCompletion` returned; an error the callback reports is logged
/// and leaves that outcome unchanged. A `kafka_common_KafkaFuture_get` on it
/// runs the queued callbacks on the calling thread while it waits; from
/// inside the record's own callback it waits forever, as Java's `get` does
/// on the I/O thread.
///
/// When this call stores an error in `*out_err`, the callback may still fire
/// later, exactly once, if the record had reached the accumulator: as in Java's
/// `doSend`, the record is appended with its callback before
/// `maybeAddPartition` fails (for example a send outside a transaction), and it
/// completes with its batch. A record rejected before the append never fires
/// it. The caller cannot tell the two apart, so after an error the callback's
/// `self` must stay alive until `_destroy`.
///
/// # Parameters
///
/// - `record` - The record to send. If the topic or the partition specified in it cannot be found
///   in metadata within `max.block.ms`, the returned future will time out when retrieved.
/// - `callback` - A user-supplied callback to execute when the record has been acknowledged by the
///   server (`NULL` indicates no callback)
///
/// # Returns
///
/// A future which will eventually contain the response information.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if a transactional.id has been configured
///   and no transaction has been started, or when send is invoked after producer has been closed.
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked
/// - `kafka_common_Error_is_serialization_error` If the key or value are not valid objects given
///   the configured serializers
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
/// - `kafka_common_Error_is_kafka_error` If a Kafka related error occurs that does not belong to
///   the public API errors.
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live handle
/// (consumed), `out_err` a valid slot pointing at null.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`; the
/// `kafka_producer_RecordMetadata_t *` it resolves to is borrowed from the future, valid until the
/// future is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_with_callback(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *mut kafka_producer_Callback_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `record` is a live handle, as `producer_record_ref`
    // requires; the reference is only cloned, within this statement.
    let record = unsafe { producer_record_ref(record) }.clone();
    // SAFETY: Per this function's `# Safety`, `callback` is null or a live handle, as
    // `delivery_callback` requires.
    let (callback, completion) = unsafe { delivery_callback(client, callback) }.unzip();
    let sent = client.producer().and_then(|producer| {
        block_on(
            &client.runtime,
            async move { producer.send_with_callback(record, callback).await },
        )
    });
    match sent {
        Ok(future) => client.box_send_future(&future, completion),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `Producer.send(ProducerRecord, Callback)`, completed through `cb` as
/// [`kafka_producer_Producer_send_cb`]: when both serializers are built-in,
/// the producer is tried on the calling thread first, without waiting, and
/// `cb` fires inside this call, on the calling thread, with the future when
/// the producer takes the record at once and with the error when it refuses
/// it (a closed producer among the refusals); a record the producer would
/// have to wait for is queued, and `cb` fires later, through the callback
/// vector, once the submission task handed it over. When either serializer
/// is a C implementation, every record is queued that way, so a C serializer
/// is never invoked or waited for on the calling thread: an asynchronous one
/// may report from it after this call returned.
///
/// When `cb` receives an error, `callback` may still fire later, exactly
/// once, if the record had reached the accumulator, as for
/// [`kafka_producer_Producer_send_with_callback`]; so after an error the
/// callback's `self` must stay alive until `_destroy`. A record the producer
/// would have had to wait for is queued with its callback un-invoked: the
/// callback fires, once, when the queued send resolves the record. With a
/// callback, the future `cb` receives completes once the callback reported,
/// as for [`kafka_producer_Producer_send_with_callback`].
///
/// As there, the registration handle `callback` is consumed by the call,
/// whatever its outcome (an error delivered to `cb` included): the caller must
/// not use it or pass it to `kafka_producer_Callback_destroy` afterwards.
///
/// # Parameters
///
/// - `record` - The record to send. If the topic or the partition specified in it cannot be found
///   in metadata within `max.block.ms`, the returned future will time out when retrieved.
/// - `callback` - A user-supplied callback to execute when the record has been acknowledged by the
///   server (`NULL` indicates no callback)
/// - `cb` - the completion, receiving the future or the error
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_state_error` if a transactional.id has been configured
///   and no transaction has been started, or when send is invoked after producer has been closed.
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked
/// - `kafka_common_Error_is_serialization_error` If the key or value are not valid objects given
///   the configured serializers
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
/// - `kafka_common_Error_is_kafka_error` If a Kafka related error occurs that does not belong to
///   the public API errors.
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle (consumed).
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Every path through the
    // body ends by firing `cb` once (`complete_send_now`) or by handing it over once to
    // `queue_send`, after which nothing can panic: in the request the submission channel
    // accepts or, when the channel refuses it (dropping it unfired), by calling it. So `cb` has
    // not fired for this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_send_with_callback_cb(
    self_: *const kafka_producer_Producer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *mut kafka_producer_Callback_t,
    cb: kafka_producer_Producer_send_with_callback_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call.
    let client = unsafe { client_ref(self_) };
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    // SAFETY: Per this function's `# Safety`, `record` is a live handle, as `producer_record_ref`
    // requires; the reference is only cloned, within this call.
    let record = unsafe { producer_record_ref(record) };
    // SAFETY: Per this function's `# Safety`, `callback` is null or a live handle, as
    // `delivery_callback` requires.
    let (callback, completion) = unsafe { delivery_callback(client, callback) }.unzip();
    let producer = match client.producer() {
        Ok(producer) => producer,
        // SAFETY: `cb` and `opaque` are the pair the C caller passed, neither fired nor handed
        // over yet, as `complete_send_now` requires.
        Err(error) => return unsafe { complete_send_now(cb, opaque, Err(error)) },
    };
    if !client.tries_send_inline {
        // A C serializer is waited for on the runtime, by the submission task,
        // never on this thread (see the module docs, "`send_cb` tries the
        // producer first").
        return queue_send(client, record.clone(), callback, completion, cb, opaque);
    }
    // Tried first whatever the queue holds: a queued record does not hold this
    // one back (see the module docs, "`send_cb` tries the producer first").
    let taken = record.clone();
    let outcome = block_on(&client.runtime, async move { producer.try_send(taken, callback).await });
    let callback = match outcome {
        Ok(TrySendOutcome::Accepted(future)) => {
            let future = client.box_send_future(&future, completion);
            // SAFETY: as above: `cb` and `opaque` are the caller's pair, neither fired nor handed
            // over yet.
            return unsafe { complete_send_now(cb, opaque, Ok(future)) };
        },
        Ok(TrySendOutcome::WouldBlock(callback)) => callback,
        // SAFETY: as above.
        Err(error) => return unsafe { complete_send_now(cb, opaque, Err(error)) },
    };
    // The producer would have waited: the record goes to the submission task,
    // which tries it again and otherwise waits for it on a task of its own,
    // and `cb` fires once the record was handed over or refused.
    queue_send(client, record.clone(), callback, completion, cb, opaque);
}

/// Queues a record the producer would have had to wait for, for the
/// submission task, which tries it again on the producer and delivers `cb`
/// (see [`submission_loop`]); when the task is gone, the producer having been
/// destroyed, delivers `cb` the error itself, during the call.
fn queue_send(
    client: &Arc<Client>,
    record: GenericRecord,
    callback: Option<Box<dyn Callback>>,
    completion: Option<KafkaFutureImpl<RecordMetadata>>,
    cb: kafka_producer_Producer_send_cb_t,
    opaque: *mut c_void,
) {
    let opaque = SendPtr(opaque);
    // A poisoned lock still holds a valid sender (see `drain_submitted_sends`).
    let sent = match &*client.submit_tx.lock().unwrap_or_else(PoisonError::into_inner) {
        Some(tx) => tx
            .send(SubmitRequest::Send { record, callback, completion, cb, opaque })
            .is_ok(),
        None => false,
    };
    if !sent {
        let error = Error::local_illegal_state("Cannot perform operation after producer has been closed");
        // SAFETY: `cb` and `opaque` are the pair the C caller passed, and the channel refused the
        // request that carried them (dropping it unfired), so they are neither fired nor handed
        // over, as `complete_send_now` requires.
        unsafe { complete_send_now(cb, opaque.get(), Err(error)) };
    }
}

/// Fires `cb` during the call, on the calling thread, for a record that is
/// done without the submission task: with the owned future handle when the
/// producer took it at once (`Ok`), with the owned error when the producer,
/// or a submission channel already gone, refused it (`Err`). Not through the
/// callback vector: a caller that holds its interpreter lock across the call
/// (the Python binding does) then never queues a completion, whose notify
/// hook would wait for that lock, from inside it.
///
/// # Safety
///
/// `cb` and `opaque` must be the pair a C caller passed to
/// [`kafka_producer_Producer_send_with_callback_cb`], not yet fired and not
/// handed over to the submission task.
unsafe fn complete_send_now(
    cb: kafka_producer_Producer_send_cb_t,
    opaque: *mut c_void,
    result: Result<*mut kafka_common_KafkaFuture_t, Error>,
) {
    let (value, error) = match result {
        Ok(future) => (future, std::ptr::null_mut()),
        Err(error) => (std::ptr::null_mut(), box_error(error)),
    };
    // SAFETY: Per this function's `# Safety`, `cb` and `opaque` are the caller's pair, neither
    // fired nor handed over, so this is their one completion. `value` and `error` are each null
    // or a fresh handle owned by no one else, so handing them to `cb` transfers their ownership,
    // as `kafka_producer_Producer_send_cb_t` documents.
    unsafe { cb(value, error, opaque) };
}

/// The Rust delivery callback queuing a C `on_completion`, with the
/// completion its report resolves, which backs the record's C future (see
/// `ffi::producer::callback`); `None` without a callback.
///
/// # Safety
///
/// `callback` must be null or a live handle, consumed here.
unsafe fn delivery_callback(
    client: &Arc<Client>,
    callback: *mut kafka_producer_Callback_t,
) -> Option<(Box<dyn Callback>, KafkaFutureImpl<RecordMetadata>)> {
    if callback.is_null() {
        return None;
    }
    let completion = KafkaFutureImpl::new();
    // SAFETY: Per this function's `# Safety`, `callback` is a live handle, since it is not null, which
    // the caller does not use again, as `take_callback` requires.
    let callback = unsafe { take_callback(callback) }.into_callback(
        Arc::clone(&client.queue),
        Arc::clone(&client.results),
        completion.clone(),
    );
    Some((callback, completion))
}

// ---------------------------------------------------------------------------
// Flush, partitions, metrics, close
// ---------------------------------------------------------------------------

/// `Producer.flush()`, blocking; ordered after the queued `send_cb`s.
///
/// Returns once every record sent before it completed and every delivery
/// callback of those records reported through
/// [`kafka_producer_Producer__set_callback_result`], as Java's `flush()`
/// returns after the callbacks ran on the I/O thread. While it waits it runs
/// the queued callbacks on the calling thread, as
/// [`kafka_producer_Producer__execute_callbacks`] does, so a caller pumping
/// the callbacks on this one thread is not left waiting for itself.
///
/// From inside a delivery callback of a `KafkaProducer` it fails with
/// `kafka_common_Error_KAFKA` ("KafkaProducer.flush() invocation inside a
/// callback is not permitted because it may lead to deadlock."), as in
/// Java; a `MockProducer`, whose Java `flush()` makes no such check,
/// completes its records and returns without waiting for the reports.
///
/// # Errors
///
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked
/// - `kafka_common_Error_is_kafka_error` If the method is invoked inside a `kafka_producer_Producer_send_with_callback` callback
///
/// # Safety
///
/// `self_` must be a live view. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_flush(
    self_: *const kafka_producer_Producer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires; the
    // reference does not outlive the call.
    let client = unsafe { client_ref(self_) };
    let in_callback = client.in_callback();
    if let Err(error) = client.check_flush_outside_callback(in_callback) {
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        return unsafe { fail_with(out_err, error) };
    }
    let result = with_drain(
        client,
        DrainBound::Unbounded,
        ReportWait::OnSuccess,
        in_callback,
        |p, _| async move { p.flush().await },
    );
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `Producer.flush()`, completion queued: `cb` fires once every record sent
/// before it completed and every delivery callback of those records
/// reported, as for [`kafka_producer_Producer_flush`]. The wait does not run
/// the queued callbacks: the caller pumps them. Called from inside a
/// delivery callback of a `KafkaProducer`, `cb` receives the error the
/// blocking form returns there.
///
/// # Parameters
///
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked
/// - `kafka_common_Error_is_kafka_error` If the method is invoked inside a `kafka_producer_Producer_send_with_callback` callback
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. The body hands `cb` over
    // only as its last step: to `Client::deliver` before its early return, or as the last step of
    // `with_drain_cb`, to the task `Client::spawn` registers, after which nothing can panic. So `cb`
    // has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_flush_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_flush_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `with_drain_cb` uses the reference only before it returns, cloning the `Arc` for the task it
    // spawns.
    let client = unsafe { client_ref(self_) };
    if let Err(error) = client.check_flush_outside_callback(client.in_callback()) {
        client.deliver(cb, SendPtr(opaque), Err(error));
        return;
    }
    with_drain_cb(
        "kafka_producer_Producer_flush_cb",
        client,
        DrainBound::Unbounded,
        ReportWait::OnSuccess,
        false,
        cb,
        opaque,
        |p, _| async move { p.flush().await },
    );
}

/// Get the partition metadata for the given topic. This can be used for custom partitioning.
///
/// This will attempt to refresh metadata until it finds the topic in it, or the configured
/// `max.block.ms` expires.
///
/// # Parameters
///
/// - `topic` - The topic to get partition metadata for
///
/// # Returns
///
/// The list of partitions, of `kafka_common_PartitionInfo_t *`.
///
/// # Errors
///
/// - `kafka_common_Error_is_authentication_error` if authentication fails. See the error for more
///   details
/// - `kafka_common_Error_is_authorization_error` if not authorized to the specified topic. See the
///   error for more details
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
/// - `kafka_common_Error_is_timeout_error` if the topic cannot be found in metadata within
///   `max.block.ms`
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
/// - `kafka_common_Error_is_kafka_error` for all Kafka-related errors, including the case where
///   this method is called after producer close
///
/// # Safety
///
/// `self_` must be a live view, `topic` a NUL-terminated string and `out_err` a
/// valid slot pointing at null. The returned list and its elements are owned by
/// the caller.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for(
    self_: *const kafka_producer_Producer_t,
    topic: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topic` is a NUL-terminated string, as
    // `c_str_to_string` requires.
    let topic = unsafe { c_str_to_string(topic) };
    let infos = client
        .producer()
        .and_then(|producer| block_on(&client.runtime, async move { producer.partitions_for(&topic).await }));
    match infos {
        Ok(infos) => partition_info_list(infos),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Get the partition metadata for the given topic. This can be used for custom partitioning.
///
/// This will attempt to refresh metadata until it finds the topic in it, or the configured
/// `max.block.ms` expires.
///
/// # Parameters
///
/// - `topic` - The topic to get partition metadata for
/// - `cb` - the completion, receiving the list of partitions or the error
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_authentication_error` if authentication fails. See the error for more
///   details
/// - `kafka_common_Error_is_authorization_error` if not authorized to the specified topic. See the
///   error for more details
/// - `kafka_common_Error_is_interrupt_error` if the thread is interrupted while blocked
/// - `kafka_common_Error_is_timeout_error` if the topic cannot be found in metadata within
///   `max.block.ms`
/// - `kafka_common_Error_is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers
///   fails within `bootstrap.resolve.timeout.ms`
/// - `kafka_common_Error_is_kafka_error` for all Kafka-related errors, including the case where
///   this method is called after producer close
///
/// # Safety
///
/// `self_` must be a live view and `topic` a NUL-terminated string. The list `cb`
/// receives and its elements are owned by `cb`.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading `self_` and `topic`
    // comes first and hands nothing over; the body hands `cb` over only as its last step, to the
    // task `Client::spawn` registers, after which nothing can panic. So `cb` has not fired for this
    // `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_partitions_for_cb(
    self_: *const kafka_producer_Producer_t,
    topic: *const c_char,
    cb: kafka_producer_Producer_partitions_for_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call, the task getting its own `Arc`.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topic` is a NUL-terminated string, as
    // `c_str_to_string` requires.
    let topic = unsafe { c_str_to_string(topic) };
    let opaque = SendPtr(opaque);
    let task_client = Arc::clone(client);
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    client.spawn(async move {
        let result = catch_task_panic("kafka_producer_Producer_partitions_for_cb", async {
            task_client.producer()?.partitions_for(&topic).await
        })
        .await;
        let (value, error) = match result {
            Ok(infos) => (partition_info_list(infos), std::ptr::null_mut()),
            Err(error) => (std::ptr::null_mut(), box_error(error)),
        };
        let value = SendPtr(value as *mut c_void);
        let error = SendPtr(error as *mut c_void);
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to this entry point, moved
        // into this one task, which pushes this one job, so calling `cb` with `opaque` is the
        // completion the caller is waiting for. `value` and `error` are each null or the fresh
        // `box_list` / `box_error` handle above, owned by no one else, so handing them to `cb`
        // transfers their ownership, as `kafka_producer_Producer_partitions_for_cb_t` documents;
        // the job is a `FnOnce`, so `cb` fires exactly once.
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

/// Frees a partition-info handle held as a `void *` container element.
///
/// # Safety
///
/// `element` must be an owned partition-info handle not yet destroyed.
unsafe fn destroy_partition_info(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned partition-info handle not yet
    // destroyed, which is what `kafka_common_PartitionInfo_destroy` requires.
    unsafe { kafka_common_PartitionInfo_destroy(element as *mut _) }
}

/// Get the full set of internal metrics maintained by the producer.
///
/// The returned map is an unmodifiable live view of the metrics. Changes to the
/// underlying metrics will be reflected in the returned map.
///
/// # Returns
///
/// An unmodifiable live view of the map of metrics currently maintained by the
/// producer, from `kafka_common_MetricName_t *` to
/// `kafka_common_metrics_KafkaMetric_t *`.
///
/// # Safety
///
/// `self_` must be a live view. The returned view is owned by the caller, freed
/// with `kafka_common_metrics_MetricsView_destroy`, and stays valid after the
/// producer is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_metrics(
    self_: *const kafka_producer_Producer_t,
) -> *mut kafka_common_metrics_MetricsView_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call.
    let client = unsafe { client_ref(self_) };
    // After `_destroy` began, the producer's metrics are gone with it.
    box_metrics_view(client.producer().map(|producer| producer.metrics()).unwrap_or_default())
}

/// `Producer.close()`, blocking: `close(Duration.ofMillis(Long.MAX_VALUE))`,
/// ordered after the queued `send_cb`s. The handle stays valid, to be
/// destroyed afterwards.
///
/// Once the producer is closed it waits, without a bound, until every
/// delivery callback queued by then reported through
/// [`kafka_producer_Producer__set_callback_result`], as Java's `close`
/// joins the I/O thread its callbacks run on, running the queued callbacks
/// on the calling thread meanwhile, as [`kafka_producer_Producer_flush`]
/// does. From inside a delivery callback it does not wait, and a
/// `KafkaProducer` closes as for a zero timeout, as Java does there (see
/// [`kafka_producer_Producer_close_with_timeout`]).
///
/// # Errors
///
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked.
/// - `kafka_common_Error_is_kafka_error` If an unexpected error occurs while trying to close the
///   client, this error should be treated as fatal and indicate the client is no longer usable.
///
/// # Safety
///
/// `self_` must be a live view. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close(
    self_: *const kafka_producer_Producer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call.
    let client = unsafe { client_ref(self_) };
    let in_callback = client.in_callback();
    let result = match untimed_close_override(client, in_callback) {
        None => with_drain(
            client,
            DrainBound::Unbounded,
            ReportWait::Always,
            in_callback,
            |p, _| async move { p.close().await },
        ),
        Some(timeout) => with_drain(
            client,
            DrainBound::Within(timeout),
            ReportWait::Always,
            in_callback,
            |p, left| async move { p.close_with_timeout(left).await },
        ),
    };
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `Producer.close()`, completion queued: `cb` fires once the producer is
/// closed and every delivery callback queued by then reported, as for
/// [`kafka_producer_Producer_close`]; that wait does not run the queued
/// callbacks, which the caller pumps. Called from inside a delivery
/// callback, `cb` does not wait for the reports, and a `KafkaProducer`
/// closes as for a zero timeout.
///
/// # Parameters
///
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked.
/// - `kafka_common_Error_is_kafka_error` If an unexpected error occurs while trying to close the
///   client, this error should be treated as fatal and indicate the client is no longer usable.
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. The body hands `cb` over
    // only as the last step of `with_drain_cb`, to the task `Client::spawn` registers, after which
    // nothing can panic. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close_cb(
    self_: *const kafka_producer_Producer_t,
    cb: kafka_producer_Producer_close_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `with_drain_cb` uses the reference only before it returns, cloning the `Arc` for the task it
    // spawns.
    let client = unsafe { client_ref(self_) };
    let in_callback = client.in_callback();
    match untimed_close_override(client, in_callback) {
        None => with_drain_cb(
            "kafka_producer_Producer_close_cb",
            client,
            DrainBound::Unbounded,
            ReportWait::Always,
            in_callback,
            cb,
            opaque,
            |p, _| async move { p.close().await },
        ),
        Some(timeout) => with_drain_cb(
            "kafka_producer_Producer_close_cb",
            client,
            DrainBound::Within(timeout),
            ReportWait::Always,
            in_callback,
            cb,
            opaque,
            |p, left| async move { p.close_with_timeout(left).await },
        ),
    }
}

/// `Producer.close(Duration timeout)`, blocking; `timeout` in milliseconds.
///
/// This method waits up to `timeout` for the producer to complete the sending of all incomplete
/// requests.
///
/// If the producer is unable to complete all requests before the timeout expires, this method will
/// fail any unsent and unacknowledged records immediately. It will also abort the ongoing transaction
/// if it's not already completing.
///
/// If invoked from within a `Callback` this method will not block and will be equivalent to a zero
/// `timeout`. This is done since no further sending will happen while blocking the I/O thread of the
/// producer.
///
/// # Parameters
///
/// - `timeout` - The maximum time to wait for producer to complete any pending requests. The value
///   should be non-negative. Specifying a timeout of zero means do not wait for pending send
///   requests to complete.
///
/// # Errors
///
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked.
/// - `kafka_common_Error_is_kafka_error` If an unexpected error occurs while trying to close the
///   client, this error should be treated as fatal and indicate the client is no longer usable.
/// - `kafka_common_Error_is_local_illegal_argument_error` If the `timeout` is negative.
///
/// # Translation notes
///
/// The queued `send_cb`s are drained within `timeout`, then the producer is closed with the time
/// left. It then waits without a bound for the delivery callbacks queued by then to report through
/// [`kafka_producer_Producer__set_callback_result`], running them on the calling thread meanwhile,
/// except from inside one. A `MockProducer` reads a negative `timeout` as zero.
///
/// # Safety
///
/// `self_` must be a live view. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close_with_timeout(
    self_: *const kafka_producer_Producer_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call.
    let client = unsafe { client_ref(self_) };
    let in_callback = client.in_callback();
    let timeout = match close_timeout(client, timeout, in_callback) {
        Ok(timeout) => timeout,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => return unsafe { fail_with(out_err, error) },
    };
    let result = with_drain(
        client,
        DrainBound::Within(timeout),
        ReportWait::Always,
        in_callback,
        |p, left| async move { p.close_with_timeout(left).await },
    );
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `Producer.close(Duration timeout)`, completion queued; the bounded drain,
/// a negative `timeout` and a call from inside a delivery callback are
/// handled as by [`kafka_producer_Producer_close_with_timeout`], a rejected
/// timeout delivered through `cb`. The wait for the reports does not run the
/// queued callbacks: the caller pumps them.
///
/// # Parameters
///
/// - `timeout` - The maximum time to wait for producer to complete any pending requests. The value
///   should be non-negative. Specifying a timeout of zero means do not wait for pending send
///   requests to complete.
/// - `cb` - the completion, receiving the error or `NULL`
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_interrupt_error` If the thread is interrupted while blocked.
/// - `kafka_common_Error_is_kafka_error` If an unexpected error occurs while trying to close the
///   client, this error should be treated as fatal and indicate the client is no longer usable.
/// - `kafka_common_Error_is_local_illegal_argument_error` If the `timeout` is negative.
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Computing the timeout hands
    // nothing over; the body hands `cb` over only as its last step: as the last step of
    // `with_drain_cb`, to the task `Client::spawn` registers, after which nothing can panic, or,
    // for a rejected timeout, to `Client::deliver`. So `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer_close_with_timeout_cb(
    self_: *const kafka_producer_Producer_t,
    timeout: i64,
    cb: kafka_producer_Producer_close_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires;
    // `client` is used only during the call.
    let client = unsafe { client_ref(self_) };
    let in_callback = client.in_callback();
    match close_timeout(client, timeout, in_callback) {
        Ok(timeout) => {
            with_drain_cb(
                "kafka_producer_Producer_close_with_timeout_cb",
                client,
                DrainBound::Within(timeout),
                ReportWait::Always,
                in_callback,
                cb,
                opaque,
                |p, left| async move { p.close_with_timeout(left).await },
            );
        },
        Err(error) => client.deliver(cb, SendPtr(opaque), Err(error)),
    }
}

/// The `Duration` for a `close(Duration)` timeout in milliseconds, as
/// `KafkaProducer.close(Duration, boolean)` reads it
/// (`KafkaProducer#close`): a negative one is an error, and a
/// positive one from inside a delivery callback is overridden to zero, with
/// Java's warning. The mock passes a negative one on as zero and never
/// overrides (see [`Client::follows_kafka_producer_close`]).
fn close_timeout(client: &Client, timeout: i64, in_callback: bool) -> Result<Duration, Error> {
    match u64::try_from(timeout) {
        Ok(millis) if millis > 0 && in_callback && client.follows_kafka_producer_close => {
            warn_close_timeout_override(timeout);
            Ok(Duration::ZERO)
        },
        Ok(millis) => Ok(Duration::from_millis(millis)),
        Err(_) if client.follows_kafka_producer_close => {
            Err(Error::local_illegal_argument("The timeout cannot be negative."))
        },
        Err(_) => Ok(Duration::ZERO),
    }
}

/// The timeout an untimed `close()` closes with instead, if any: Java's
/// `close()` is `close(Duration.ofMillis(Long.MAX_VALUE))`
/// (`KafkaProducer#close`), so from inside a delivery callback of a
/// `KafkaProducer` it is overridden to zero like any positive timeout.
fn untimed_close_override(client: &Client, in_callback: bool) -> Option<Duration> {
    (in_callback && client.follows_kafka_producer_close).then(|| {
        warn_close_timeout_override(i64::MAX);
        Duration::ZERO
    })
}

/// Java's warning when a delivery callback closes its producer with a
/// positive timeout (`KafkaProducer#close(Duration)`).
fn warn_close_timeout_override(timeout_ms: i64) {
    log::warn!(
        "Overriding close timeout {timeout_ms} ms to 0 ms in order to prevent useless blocking due to self-join. \
         This means you have incorrectly invoked close with a non-zero timeout from the producer call-back."
    );
}

// ---------------------------------------------------------------------------
// Callback pump
// ---------------------------------------------------------------------------

/// Runs the queued callbacks serially on the calling thread.
///
/// # Returns
///
/// How many callbacks ran.
///
/// # Safety
///
/// `self_` must be a live view.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer__execute_callbacks(self_: *const kafka_producer_Producer_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires; the
    // reference does not outlive the call.
    unsafe { client_ref(self_) }.queue.execute()
}

/// Installs the hook fired once each time the callback vector goes from
/// empty to non-empty (see
/// [`kafka_producer_Producer_callbacks_notify_fn_t`]).
///
/// # Parameters
///
/// - `notify` - the hook
/// - `opaque` - passed to `notify`
///
/// # Safety
///
/// `self_` must be a live view; `opaque` stays valid while the hook is
/// installed and until a call of it in flight returns: a later call of this
/// function does not wait for one.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer__set_callbacks_notify(
    self_: *const kafka_producer_Producer_t,
    notify: kafka_producer_Producer_callbacks_notify_fn_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live view, as `client_ref` requires, and
    // `opaque` stays valid while the hook is installed and until a call in flight returns, which is
    // what the hook call in `CallbackQueue::notify_if`, made after the registration's lock is
    // released, relies on: `notify` and `opaque` are stored together.
    unsafe { client_ref(self_) }.queue.set_notify(Some(notify), opaque);
}

/// Reports the result of a delivery callback invoked with `callback_id`:
/// the `kafka_producer_Callback_on_completion_fn_t` of a
/// `send_with_callback` / `send_with_callback_cb`. The double underscore
/// marks a function derived by convention, with no Java counterpart.
///
/// `error` is `NULL` when the callback returned normally; otherwise it is an
/// owned `kafka_common_Error_t *` that Rust takes over: the error the
/// callback raised, Java's `onCompletion` throwing, which is logged
/// ("Error executing user-provided callback on message for topic-partition
/// '<topic>-<partition>'") and freed, and fails nothing, as in Java's
/// `ProducerBatch.completeFutureAndFireCallbacks`. `Callback.onCompletion`
/// is `void`, so `result` is ignored: pass `NULL`.
///
/// The report completes the record's C future (with the record's own
/// outcome, whatever the report) and releases the waits of `flush`,
/// `commit_transaction`, `abort_transaction` and `close` for this callback.
///
/// `self_` is the producer that invoked the callback: each producer keeps
/// its own ids. A callback invoked through `kafka_producer_Callback_on_completion*`,
/// which has no producer, is reported the same way; its id says so and
/// `self_` is then not read, so it may be any producer or `NULL`.
///
/// Callable from inside the callback or later, from any thread, including
/// while `kafka_producer_KafkaProducer_destroy` or
/// `kafka_producer_MockProducer_destroy` runs the producer's pending
/// callbacks, but never once it began otherwise: `_destroy` completes the
/// callbacks still unreported as if each had reported success. A second
/// report for the same id, or one for an id the producer did not issue, is
/// ignored: its error is freed.
///
/// # Parameters
///
/// - `callback_id` - the id the callback was invoked with
/// - `result` - ignored, as `Callback.onCompletion` is `void`: pass `NULL`
/// - `error` - `NULL`, or the error the callback raised
///
/// # Safety
///
/// `self_` must be a live view of the producer that issued `callback_id`
/// (unless an invoker issued it, see above), and `error` null or a valid
/// owned error not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Producer__set_callback_result(
    self_: *const kafka_producer_Producer_t,
    callback_id: i64,
    result: *mut c_void,
    error: *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `error` is null or a valid owned error not used
    // afterwards, i.e. a `box_error` handle the C side hands over here, as `take_error` requires.
    let error = unsafe { take_error(error) };
    // `result` is only passed on: a delivery callback's sink ignores it (`void_sink`).
    if CallbackResults::is_tagged(callback_id) {
        INVOKER_RESULTS.complete(callback_id, result, error);
    } else {
        // SAFETY: Per this function's `# Safety`, `self_` is a live view of the producer that
        // issued the untagged `callback_id`, as `client_ref` requires, possibly while its destroy
        // runs, which keeps the view valid to its end (`ProducerHandle::destroy`). The reference is
        // used only to clone the registry's `Arc`, before the report is delivered.
        let results = Arc::clone(&unsafe { client_ref(self_) }.results);
        results.complete(callback_id, result, error);
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::atomic::AtomicI32;

    use super::*;
    use crate::ffi::common::fail_point;
    use crate::ffi::common::panic_probe::{Probe, on_error, on_value};
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::kafka_future::{
        kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_get_cb,
        kafka_common_KafkaFuture_get_with_timeout, kafka_common_KafkaFuture_t,
    };
    use crate::ffi::producer::callback::kafka_producer_Callback_new;
    use crate::ffi::producer::mock_producer::{
        kafka_producer_MockProducer__as_Producer, kafka_producer_MockProducer_closed,
        kafka_producer_MockProducer_destroy, kafka_producer_MockProducer_history, kafka_producer_MockProducer_t,
        kafka_producer_MockProducer_with_auto_complete,
    };
    use crate::ffi::producer::producer_record::{
        kafka_producer_ProducerRecord_destroy, kafka_producer_ProducerRecord_new, kafka_producer_ProducerRecord_value,
    };
    use crate::ffi::producer::record_metadata::{
        kafka_producer_RecordMetadata_offset, kafka_producer_RecordMetadata_t,
    };
    use crate::ffi::util::{c_str_to_string, kafka_List_destroy, kafka_List_get, kafka_List_size};

    /// Records a `void` completion's outcome: `None` for success, the
    /// error's message otherwise.
    unsafe extern "C" fn record_outcome(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        let outcome = if error.is_null() {
            None
        } else {
            // SAFETY: a non-null `error` is owned by this completion.
            let message = unsafe { c_str_to_string(kafka_common_Error_message(error)) };
            // SAFETY: as above; freed once.
            unsafe { kafka_common_Error_destroy(error) };
            Some(message)
        };
        // SAFETY: `opaque` is the test's slot, alive until the callbacks ran.
        *unsafe { &*(opaque as *const Mutex<Vec<Option<String>>>) }.lock().unwrap() = vec![outcome];
    }

    /// Java's `MockProducer.close(Duration)` never reads the timeout
    /// (`MockProducer#close(Duration)`), so the mock closes on a negative one
    /// instead of rejecting it as `KafkaProducer` does; both forms.
    #[test]
    fn mock_close_with_timeout_treats_a_negative_timeout_as_zero() {
        // SAFETY: every handle is live until it is destroyed below, and `err` is a valid slot
        // pointing at null.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_close_with_timeout(view, -1, &raw mut err);
            assert!(err.is_null());
            assert_eq!(kafka_producer_MockProducer_closed(mock), 1);
            kafka_producer_MockProducer_destroy(mock);

            let outcome = Mutex::new(Vec::new());
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            kafka_producer_Producer_close_with_timeout_cb(
                view,
                -1,
                record_outcome,
                &outcome as *const Mutex<Vec<Option<String>>> as *mut c_void,
            );
            while outcome.lock().unwrap().is_empty() {
                kafka_producer_Producer__execute_callbacks(view);
                std::thread::yield_now();
            }
            assert_eq!(*outcome.lock().unwrap(), [None]);
            assert_eq!(kafka_producer_MockProducer_closed(mock), 1);
            kafka_producer_MockProducer_destroy(mock);
        }
    }

    unsafe extern "C" fn count_send(
        value: *mut kafka_common_KafkaFuture_t,
        error: *mut kafka_common_Error_t,
        opaque: *mut c_void,
    ) {
        assert!(error.is_null());
        // SAFETY: `value` is the future this completion owns.
        unsafe { kafka_common_KafkaFuture_destroy(value) };
        // SAFETY: `opaque` is the test's counter, alive until destroy returns.
        unsafe { &*(opaque as *const AtomicI32) }.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn record_refused_send(
        value: *mut kafka_common_KafkaFuture_t,
        error: *mut kafka_common_Error_t,
        opaque: *mut c_void,
    ) {
        assert!(value.is_null());
        // SAFETY: `error` is the error this completion owns; it is read, then freed once.
        let message = unsafe { c_str_to_string(kafka_common_Error_message(error)) };
        // SAFETY: as above.
        unsafe { kafka_common_Error_destroy(error) };
        // SAFETY: `opaque` is the test's slot, alive until the test ends.
        let fired = unsafe { &*(opaque as *const Mutex<Vec<(String, std::thread::ThreadId)>>) };
        fired.lock().unwrap().push((message, std::thread::current().id()));
    }

    /// A panic that poisoned the client's locks does not stop `_destroy`: it
    /// still stops the submission task, awaits it and fires the completion of
    /// a queued send exactly once. The record is queued through `queue_send`,
    /// as `send_cb` queues one the producer would have waited for; the mock
    /// takes every record at once.
    #[test]
    fn destroy_tolerates_poisoned_locks() {
        let sends = AtomicI32::new(0);
        let topic = CString::new("topic").unwrap();
        // SAFETY: every handle is live until it is destroyed below.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());
            queue_send(
                client_ref(view),
                producer_record_ref(record).clone(),
                None,
                None,
                count_send,
                &sends as *const AtomicI32 as *mut c_void,
            );

            let client = Arc::clone(client_ref(view));
            let poisoner = std::thread::spawn(move || {
                let _tasks = client.tasks.lock().unwrap();
                let _submit_tx = client.submit_tx.lock().unwrap();
                panic!("poison the client's locks");
            });
            assert!(poisoner.join().is_err());
            let client = client_ref(view);
            assert!(client.tasks.is_poisoned());
            assert!(client.submit_tx.is_poisoned());

            kafka_producer_MockProducer_destroy(mock);
            assert_eq!(sends.load(Ordering::SeqCst), 1);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// A `_cb` body that panics before handing its completion over delivers
    /// the panic once, queued: nothing fires until `__execute_callbacks`, which
    /// runs exactly one completion, of each callback shape the producer has.
    #[test]
    fn a_panicking_cb_body_queues_one_panic_completion() {
        let topic = CString::new("topic").unwrap();
        // SAFETY: every handle is live until it is destroyed below, and `err` is a valid slot
        // pointing at null.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());

            let flush = Probe::default();
            fail_point::arm();
            kafka_producer_Producer_flush_cb(view, on_error, flush.as_opaque());
            let commit = Probe::default();
            fail_point::arm();
            kafka_producer_Producer_commit_transaction_cb(view, on_error, commit.as_opaque());
            let partitions = Probe::default();
            fail_point::arm();
            kafka_producer_Producer_partitions_for_cb(view, topic.as_ptr(), on_value, partitions.as_opaque());
            let send = Probe::default();
            fail_point::arm();
            kafka_producer_Producer_send_cb(view, record, on_value, send.as_opaque());

            for probe in [&flush, &commit, &partitions, &send] {
                assert!(probe.fired().is_empty(), "queued, not fired");
            }
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 4);
            flush.assert_one_panic("kafka_producer_Producer_flush_cb", fail_point::MESSAGE);
            commit.assert_one_panic("kafka_producer_Producer_commit_transaction_cb", fail_point::MESSAGE);
            partitions.assert_one_panic("kafka_producer_Producer_partitions_for_cb", fail_point::MESSAGE);
            send.assert_one_panic("kafka_producer_Producer_send_with_callback_cb", fail_point::MESSAGE);

            // The send panicked before it was counted, so a flush does not
            // wait for it, and the transaction-control flag was never taken.
            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_flush(view, &raw mut err);
            assert!(err.is_null());
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0);
            kafka_producer_MockProducer_destroy(mock);
            for probe in [&flush, &commit, &partitions, &send] {
                assert_eq!(probe.fired().len(), 1, "no second completion");
            }
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// A record queued after a panic poisoned the submission lock is still
    /// submitted: the lock guards only the sender, which a panic cannot leave
    /// invalid. Queued through `queue_send`, as `send_cb` queues a record the
    /// producer would have waited for; the mock takes every record at once.
    #[test]
    fn a_queued_send_tolerates_a_poisoned_submission_lock() {
        let sends = AtomicI32::new(0);
        let topic = CString::new("topic").unwrap();
        // SAFETY: every handle is live until it is destroyed below, and `err` is a valid slot
        // pointing at null.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let client = Arc::clone(client_ref(view));
            let poisoner = std::thread::spawn(move || {
                let _submit_tx = client.submit_tx.lock().unwrap();
                panic!("poison the submission lock");
            });
            assert!(poisoner.join().is_err());
            assert!(client_ref(view).submit_tx.is_poisoned());

            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());
            queue_send(
                client_ref(view),
                producer_record_ref(record).clone(),
                None,
                None,
                count_send,
                &sends as *const AtomicI32 as *mut c_void,
            );
            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_flush(view, &raw mut err);
            assert!(err.is_null());
            while sends.load(Ordering::SeqCst) == 0 {
                kafka_producer_Producer__execute_callbacks(view);
                std::thread::yield_now();
            }
            kafka_producer_MockProducer_destroy(mock);
            assert_eq!(sends.load(Ordering::SeqCst), 1);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// A `KafkaFuture_get_cb` still waiting on a send the mock never completes
    /// fires once, from the producer's `_destroy`, with the destroyed-client
    /// error; a `get_cb` after it fires inline with the same error.
    #[test]
    fn destroy_fires_a_waiting_future_get_cb_once() {
        let topic = CString::new("topic").unwrap();
        let seen: Mutex<Vec<Option<String>>> = Mutex::new(Vec::new());
        // SAFETY: every handle is live until it is destroyed below, and `err` is a valid slot
        // pointing at null.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(0);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());
            let mut err = std::ptr::null_mut();
            let future = kafka_producer_Producer_send(view, record, &mut err);
            assert!(err.is_null());
            let opaque = &seen as *const Mutex<Vec<Option<String>>> as *mut c_void;
            kafka_common_KafkaFuture_get_cb(future, record_future, opaque);
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0, "still waiting");
            assert!(seen.lock().unwrap().is_empty());

            kafka_producer_MockProducer_destroy(mock);
            let destroyed = Some("The client was destroyed before the future completed".to_string());
            assert_eq!(*seen.lock().unwrap(), std::slice::from_ref(&destroyed));
            kafka_common_KafkaFuture_get_cb(future, record_future, opaque);
            assert_eq!(*seen.lock().unwrap(), [destroyed.clone(), destroyed]);
            kafka_common_KafkaFuture_destroy(future);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// A blocking `KafkaFuture_get`, timed or not, still waiting on a send the
    /// mock never completes returns from the producer's `_destroy` with the
    /// destroyed-client error: the timed one drops its timer before the
    /// runtime's timers stop, instead of polling it into the shutdown panic.
    #[test]
    fn destroy_releases_a_blocking_get_on_a_pending_record() {
        // `send` waits on the record alone; `send_with_callback` drains the
        // callback queue meanwhile (`resolve_draining`).
        for with_callback in [false, true] {
            destroy_releases_a_blocking_get_on_a_pending_record_with(with_callback);
        }
    }

    /// A delivery callback that never reports (the mock never completes the
    /// record, so it never fires either).
    unsafe extern "C" fn unreported(
        _self: *mut c_void,
        _metadata: *const kafka_producer_RecordMetadata_t,
        _error: *const kafka_common_Error_t,
        _callback_id: i64,
    ) {
    }

    fn destroy_releases_a_blocking_get_on_a_pending_record_with(with_callback: bool) {
        struct Future(*mut kafka_common_KafkaFuture_t);
        // SAFETY: a future handle may be used from any thread (`ffi::kafka_future`).
        unsafe impl Send for Future {}
        // SAFETY: as above; the getters only borrow it.
        unsafe impl Sync for Future {}

        let topic = CString::new("topic").unwrap();
        // SAFETY: every handle is live until it is destroyed below, and each `err` is a valid
        // slot pointing at null. The future outlives both getter threads, joined before it is
        // destroyed.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(0);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());
            let mut err = std::ptr::null_mut();
            let future = Future(match with_callback {
                true => {
                    let callback = kafka_producer_Callback_new(std::ptr::null_mut(), unreported);
                    kafka_producer_Producer_send_with_callback(view, record, callback, &mut err)
                },
                false => kafka_producer_Producer_send(view, record, &mut err),
            });
            assert!(err.is_null());
            let outcome = |error: *mut kafka_common_Error_t| {
                assert!(!error.is_null(), "the record never completes");
                // A non-null `error` is owned by the caller; freed once.
                let message = c_str_to_string(kafka_common_Error_message(error));
                kafka_common_Error_destroy(error);
                message
            };
            let (untimed, timed) = std::thread::scope(|scope| {
                let future = &future;
                let untimed = scope.spawn(move || {
                    let mut err = std::ptr::null_mut();
                    // `future` is live until the scope ends; `err` points at null.
                    let value = kafka_common_KafkaFuture_get(future.0, &mut err);
                    assert!(value.is_null());
                    outcome(err)
                });
                let timed = scope.spawn(move || {
                    let mut err = std::ptr::null_mut();
                    let value = kafka_common_KafkaFuture_get_with_timeout(future.0, 60_000, &mut err);
                    assert!(value.is_null());
                    outcome(err)
                });
                std::thread::sleep(Duration::from_millis(300));
                // Checked after the destroy: the scope joins the getters, which only it releases.
                let waiting = !untimed.is_finished() && !timed.is_finished();
                kafka_producer_MockProducer_destroy(mock);
                assert!(waiting, "still waiting before the destroy");
                (untimed.join().unwrap(), timed.join().unwrap())
            });
            let destroyed = "The client was destroyed before the future completed";
            assert_eq!(untimed, destroyed, "with_callback={with_callback}");
            assert_eq!(timed, destroyed, "with_callback={with_callback}");
            kafka_common_KafkaFuture_destroy(future.0);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// The `KafkaFuture_get_cb` completions the producer's `_destroy` runs
    /// last may block on another pending record of the producer: each such
    /// `get`, timed or not, answers the destroyed-client error instead of
    /// waiting on the drain that runs it.
    #[test]
    fn destroy_releases_a_blocking_get_made_by_a_completion_it_runs() {
        struct Nested {
            other: *mut kafka_common_KafkaFuture_t,
            seen: Mutex<Vec<String>>,
        }
        /// Blocks on `other` from inside the completion, both ways.
        unsafe extern "C" fn nested_get(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
            assert!(value.is_null());
            // SAFETY: `opaque` is the test's `Nested`, alive until the destroy returned; `other`
            // is live until then too; a non-null `error` is owned here, freed once.
            unsafe {
                let nested = &*(opaque as *const Nested);
                let mut seen = vec![c_str_to_string(kafka_common_Error_message(error))];
                kafka_common_Error_destroy(error);
                for timeout in [None, Some(1_000), Some(0)] {
                    let mut err = std::ptr::null_mut();
                    let value = match timeout {
                        None => kafka_common_KafkaFuture_get(nested.other, &mut err),
                        Some(ms) => kafka_common_KafkaFuture_get_with_timeout(nested.other, ms, &mut err),
                    };
                    assert!(value.is_null() && !err.is_null());
                    seen.push(c_str_to_string(kafka_common_Error_message(err)));
                    kafka_common_Error_destroy(err);
                }
                nested.seen.lock().unwrap().extend(seen);
            }
        }
        struct Handle(*mut kafka_producer_MockProducer_t);
        // SAFETY: a producer handle may be destroyed from any thread.
        unsafe impl Send for Handle {}

        let topic = CString::new("topic").unwrap();
        // SAFETY: every handle is live until it is destroyed below, and each `err` is a valid
        // slot pointing at null; `nested` outlives the destroy, which runs its completion.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(0);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());
            let mut err = std::ptr::null_mut();
            let first = kafka_producer_Producer_send(view, record, &mut err);
            let other = kafka_producer_Producer_send(view, record, &mut err);
            assert!(err.is_null());
            let nested = Nested { other, seen: Mutex::new(Vec::new()) };
            kafka_common_KafkaFuture_get_cb(first, nested_get, &nested as *const Nested as *mut c_void);
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0, "still waiting");

            // A hang fails the test instead of stalling it.
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let mock = Handle(mock);
            std::thread::spawn(move || {
                let mock = mock;
                kafka_producer_MockProducer_destroy(mock.0);
                let _ = done_tx.send(());
            });
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the destroy returns although a completion it runs blocks on a record");
            let destroyed = "The client was destroyed before the future completed";
            assert_eq!(*nested.seen.lock().unwrap(), [destroyed; 4]);
            kafka_common_KafkaFuture_destroy(first);
            kafka_common_KafkaFuture_destroy(other);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// Records a `KafkaFuture_get_cb` completion's error message, or `None`.
    unsafe extern "C" fn record_future(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        let outcome = if error.is_null() {
            None
        } else {
            assert!(value.is_null());
            // SAFETY: a non-null `error` is owned by this completion.
            let message = unsafe { c_str_to_string(kafka_common_Error_message(error)) };
            // SAFETY: as above; freed once.
            unsafe { kafka_common_Error_destroy(error) };
            Some(message)
        };
        // SAFETY: `opaque` is the test's slot, alive until the test ends.
        unsafe { &*(opaque as *const Mutex<Vec<Option<String>>>) }
            .lock()
            .unwrap()
            .push(outcome);
    }

    /// `send_cb` on a producer that can take the record at once — the mock
    /// always can — fires `cb` inside the call, on the calling thread, with
    /// the record's future; nothing is queued, so `__execute_callbacks` has
    /// nothing to run.
    #[test]
    fn send_cb_fires_its_completion_inside_the_call_for_a_record_taken_at_once() {
        let topic = CString::new("topic").unwrap();
        let seen: Mutex<Vec<i64>> = Mutex::new(Vec::new());
        // SAFETY: every handle is live until it is destroyed below, and `seen` outlives the one
        // completion that writes it.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());
            kafka_producer_Producer_send_cb(
                view,
                record,
                record_send_offset,
                &seen as *const Mutex<Vec<i64>> as *mut c_void,
            );
            assert_eq!(
                *seen.lock().unwrap(),
                [0],
                "taken at once: `cb` fired inside the call, with the done future"
            );

            let history = kafka_producer_MockProducer_history(mock);
            assert_eq!(kafka_List_size(history), 1);
            kafka_List_destroy(history);
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0, "nothing was queued");
            kafka_producer_MockProducer_destroy(mock);
            assert_eq!(seen.lock().unwrap().len(), 1, "not a second time on destroy");
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// `send_cb` on a closed producer refuses the record inside the call:
    /// `cb` receives the error on the calling thread, before the call
    /// returns, and nothing is queued, so a caller holding its interpreter
    /// lock across the call never fires the notify hook from inside it.
    #[test]
    fn send_cb_on_a_closed_producer_fires_its_completion_inside_the_call() {
        let topic = CString::new("topic").unwrap();
        let fired = Mutex::new(Vec::<(String, std::thread::ThreadId)>::new());
        // SAFETY: every handle is live until it is destroyed below, `err` is a valid slot pointing
        // at null, and `fired` outlives the one completion that writes it.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_close(view, &raw mut err);
            assert!(err.is_null());
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());
            kafka_producer_Producer_send_cb(
                view,
                record,
                record_refused_send,
                &fired as *const Mutex<Vec<(String, std::thread::ThreadId)>> as *mut c_void,
            );
            assert_eq!(
                *fired.lock().unwrap(),
                [("MockProducer is already closed.".to_owned(), std::thread::current().id())],
                "refused at once: `cb` fired inside the call, on this thread"
            );
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0, "nothing was queued");
            kafka_producer_MockProducer_destroy(mock);
            assert_eq!(fired.lock().unwrap().len(), 1, "not a second time on destroy");
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// Records the offset of a `send_cb` completion's future, which the mock
    /// assigns in handover order.
    unsafe extern "C" fn record_send_offset(
        value: *mut kafka_common_KafkaFuture_t,
        error: *mut kafka_common_Error_t,
        opaque: *mut c_void,
    ) {
        assert!(error.is_null());
        let mut err = std::ptr::null_mut();
        // SAFETY: `value` is the done future this completion owns; the metadata is borrowed from
        // it until it is destroyed, once, here.
        let offset = unsafe {
            let metadata = kafka_common_KafkaFuture_get(value, &raw mut err) as *const kafka_producer_RecordMetadata_t;
            assert!(err.is_null());
            let offset = kafka_producer_RecordMetadata_offset(metadata);
            kafka_common_KafkaFuture_destroy(value);
            offset
        };
        // SAFETY: `opaque` is the test's list, alive until the callbacks ran.
        unsafe { &*(opaque as *const Mutex<Vec<i64>>) }.lock().unwrap().push(offset);
    }

    /// `send_cb` tries the producer first whatever the queue holds: while a
    /// record the producer had to wait for sits in the submission queue, the
    /// next record is taken at once, the producer (the mock) being able to
    /// take it, and reaches the producer before the queued one. The order is
    /// Java's, the one records reach the producer in; a caller that wants its
    /// own records in sending order waits for a queued record's `cb` before
    /// its next send. The mock never has to wait, so the first record is
    /// queued through `queue_send`, as `send_with_callback_cb` queues one the
    /// producer would have waited for.
    #[test]
    fn send_cb_takes_a_record_at_once_while_one_is_queued_before_it() {
        let topic = CString::new("topic").unwrap();
        let (first, second) = (1_i32, 2_i32);
        let seen: Mutex<Vec<i64>> = Mutex::new(Vec::new());
        // SAFETY: every handle is live until it is destroyed below, `first`, `second` and `seen`
        // outlive every callback and every read of the mock's history, and the gate guard is
        // dropped before the mock is destroyed.
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let client = client_ref(view);
            let records = [
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &first as *const i32 as *const c_void),
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &second as *const i32 as *const c_void),
            ];
            let seen_ptr = &seen as *const Mutex<Vec<i64>> as *mut c_void;

            // The gate parks the submission task on the first record: queued,
            // dequeued, not handed over.
            let gate = client.submission_gate.try_lock().expect("the gate is free");
            queue_send(
                client,
                producer_record_ref(records[0]).clone(),
                None,
                None,
                record_send_offset,
                seen_ptr,
            );

            // The mock takes the second record at once, although one is queued before it.
            kafka_producer_Producer_send_cb(view, records[1], record_send_offset, seen_ptr);
            assert_eq!(
                *seen.lock().unwrap(),
                [0],
                "taken at once, not queued behind the first record: the first to reach the producer"
            );
            let history = kafka_producer_MockProducer_history(mock);
            assert_eq!(kafka_List_size(history), 1, "only the second record reached the producer");
            assert_eq!(
                kafka_producer_ProducerRecord_value(kafka_List_get(history, 0) as *const _),
                &second as *const i32 as *mut c_void
            );
            kafka_List_destroy(history);
            assert_eq!(
                kafka_producer_Producer__execute_callbacks(view),
                0,
                "the first record is still parked"
            );

            // Released, the task hands the first record over: the second to reach the producer.
            drop(gate);
            while seen.lock().unwrap().len() < 2 {
                kafka_producer_Producer__execute_callbacks(view);
                std::thread::yield_now();
            }
            assert_eq!(*seen.lock().unwrap(), [0, 1], "the first record got the second offset");
            let history = kafka_producer_MockProducer_history(mock);
            assert_eq!(kafka_List_size(history), 2);
            assert_eq!(
                kafka_producer_ProducerRecord_value(kafka_List_get(history, 1) as *const _),
                &first as *const i32 as *mut c_void
            );
            kafka_List_destroy(history);

            kafka_producer_MockProducer_destroy(mock);
            for record in records {
                kafka_producer_ProducerRecord_destroy(record);
            }
        }
    }
}
