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

//! C interface for `org.apache.kafka.clients.consumer.ConsumerRebalanceListener`
//! (CLAUDE.md §4 rule 3).
//!
//! The three methods are `async fn`s in Rust, so their C counterparts return
//! `void`, take a trailing `int64_t callback_id` and report through
//! `kafka_consumer_Consumer__set_callback_result`, from inside the method or
//! later from any thread. How a consumer reaches the implementation depends
//! on the entry point that triggered the rebalance (module docs of
//! `ffi::consumer`): directly on the calling thread for a blocking one,
//! through `kafka_consumer_Consumer__execute_callbacks` for a `_cb` one. The
//! membership state machine does not advance until the result is reported
//! (consumer-threading.md §31), exactly as Java waits for the listener to
//! return.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use async_trait::async_trait;

use crate::common::{Error, TopicPartition};
use crate::consumer::ConsumerRebalanceListener;
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::callback_result::{CallbackResults, INVOKER_RESULTS, void_sink};
use crate::ffi::common::deliver_unit;
use crate::ffi::common::topic_partition::{list_topic_partitions, sorted_topic_partition_list};
use crate::ffi::common::{deliver_panic, kafka_common_Error_t, run_inline};
use crate::ffi::consumer::{Delivery, error_slot};
use crate::ffi::ffi_guard;
use crate::ffi::util::{kafka_List_destroy, kafka_List_t};

/// Opaque handle to a `ConsumerRebalanceListener` implementation registered
/// with [`kafka_consumer_ConsumerRebalanceListener_new`].
#[repr(C)]
pub struct kafka_consumer_ConsumerRebalanceListener_t {
    _private: [u8; 0],
}

/// `onPartitionsRevoked(Collection<TopicPartition>)` of a C implementation:
/// `partitions` is a list of `kafka_common_TopicPartition_t *`, sorted by
/// topic and partition and borrowed for the call. The method is `async` in
/// Rust: it reports its result with
/// `kafka_consumer_Consumer__set_callback_result(consumer, callback_id, NULL, error)`,
/// `error` `NULL` for success or an owned `kafka_common_Error_t *` for
/// failure; the method is `void` in Java, so `result` is `NULL`.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64);

/// `onPartitionsAssigned(Collection<TopicPartition>)` of a C implementation;
/// see the `on_partitions_revoked` pointer for the contract.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64);

/// `onPartitionsLost(Collection<TopicPartition>)` of a C implementation;
/// see the `on_partitions_revoked` pointer for the contract. `NULL` is the
/// Java default: `onPartitionsRevoked(partitions)`.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64)>;

/// What a listener handle points at: the C implementation's `self` and
/// method pointers. `Copy` so a consumer takes its own registration and the
/// handle may be destroyed afterwards; `self` itself stays the caller's
/// until the consumer releases it.
#[derive(Clone, Copy)]
pub(crate) struct ListenerRegistration {
    self_: SendPtr,
    on_partitions_revoked: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_fn_t,
    on_partitions_assigned: kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_fn_t,
    on_partitions_lost: kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_fn_t,
}

/// One of the three C methods, resolved (the `lost` default applied).
type Method = unsafe extern "C" fn(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64);

impl ListenerRegistration {
    fn lost_or_revoked(self) -> Method {
        self.on_partitions_lost.unwrap_or(self.on_partitions_revoked)
    }

    /// Invokes `method` on the calling thread with `partitions` and a fresh
    /// callback id of `results` whose report reaches `sink`; the list is
    /// freed after the method returned (C copies what it keeps). The id is
    /// registered only once the list is built, right before the call, so a
    /// `_cb` entry point's guard that reports a panic before then is the only
    /// completion.
    fn fire(
        self,
        method: Method,
        partitions: &[TopicPartition],
        results: &CallbackResults,
        sink: Box<dyn FnOnce(Result<(), Error>) + Send>,
    ) {
        let list = sorted_topic_partition_list(partitions.iter());
        let callback_id = results.register(void_sink(sink));
        // SAFETY: `method` is one of the method pointers registered with `self.self_` through
        // `kafka_consumer_ConsumerRebalanceListener_new` (every caller passes a field of `self` or
        // `lost_or_revoked`), whose rustdoc has the caller keep `self_` alive until the handle is
        // destroyed and, once passed to a `subscribe_*`, until a later successful `subscribe_*`
        // replaces it (on a `KafkaConsumer`, one with a non-empty topic list or a pattern), or the
        // consumer is destroyed, or a `KafkaConsumer`'s `close` returns (a `MockConsumer` still calls
        // it after `close`). A registration is copied only
        // out of a live handle: by an invoker, which fires it within its own call, or into a
        // consumer's adapter, whose invocations run inside the operation that triggered them
        // (inline, from the callbacks vector while that operation waits for the report, or as
        // `_destroy` drains the vector), which another operation cannot overlap to release the
        // listener; so this call falls within that window. `list` is the owned list
        // `sorted_topic_partition_list` just built, borrowed by the C method for the call, as the
        // `_fn_t` rustdoc states, then freed here exactly once.
        unsafe {
            method(self.self_.get(), list, callback_id);
            kafka_List_destroy(list);
        }
    }
}

/// The `ConsumerRebalanceListener` a consumer holds for a C registration:
/// each method hands the invocation to [`Delivery`] (inline or queued, per
/// the entry point) and awaits the result C reports.
struct CRebalanceListener {
    registration: ListenerRegistration,
    delivery: Arc<Delivery>,
}

impl CRebalanceListener {
    async fn invoke(&self, method: Method, partitions: &[TopicPartition]) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let registration = self.registration;
        let partitions = partitions.to_vec();
        let results = Arc::clone(self.delivery.results());
        self.delivery.invoke(Box::new(move || {
            registration.fire(
                method,
                &partitions,
                &results,
                Box::new(move |result| {
                    // The awaiting side may have been dropped with the consumer.
                    let _ = tx.send(result);
                }),
            );
        }));
        rx.await
            .unwrap_or_else(|_| Err(Error::local_illegal_state("consumer destroyed before the listener reported")))
    }
}

#[async_trait]
impl ConsumerRebalanceListener for CRebalanceListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.invoke(self.registration.on_partitions_revoked, partitions).await
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.invoke(self.registration.on_partitions_assigned, partitions).await
    }

    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.invoke(self.registration.lost_or_revoked(), partitions).await
    }
}

/// The registration behind a handle.
///
/// # Safety
///
/// `listener` must be a live listener handle.
pub(crate) unsafe fn listener_registration(
    listener: *const kafka_consumer_ConsumerRebalanceListener_t,
) -> ListenerRegistration {
    // SAFETY: Per this function's `# Safety`, `listener` is a live listener handle, i.e. the
    // `Box<ListenerRegistration>` of `kafka_consumer_ConsumerRebalanceListener_new`, not yet
    // destroyed; the reference is used only to copy the `Copy` registration out, within this
    // expression.
    *unsafe { &*(listener as *const ListenerRegistration) }
}

/// The Rust listener a consumer registers for the C implementation behind
/// `listener`, delivering through `delivery`.
///
/// # Safety
///
/// `listener` must be a live listener handle.
pub(crate) unsafe fn listener_adapter(
    listener: *mut kafka_consumer_ConsumerRebalanceListener_t,
    delivery: Arc<Delivery>,
) -> Arc<dyn ConsumerRebalanceListener> {
    // SAFETY: Per this function's `# Safety`, `listener` is a live listener handle, as
    // `listener_registration` requires.
    Arc::new(CRebalanceListener { registration: unsafe { listener_registration(listener) }, delivery })
}

/// Registers a C implementation of `ConsumerRebalanceListener`.
///
/// # Parameters
///
/// - `on_partitions_revoked` - `onPartitionsRevoked`
/// - `on_partitions_assigned` - `onPartitionsAssigned`
/// - `on_partitions_lost` - `onPartitionsLost`, `NULL` for the Java default
///
/// # Returns
///
/// The listener handle.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_consumer_ConsumerRebalanceListener_destroy`. The caller owns `self_` and keeps it alive
/// until the handle is destroyed and, once passed to a `subscribe_*`, until a later successful
/// `subscribe_*` replaces it, or the consumer is destroyed. A `KafkaConsumer` also releases it
/// when `close` returns; a `MockConsumer` keeps it, since its `rebalance` and `lose_partitions`
/// still call it after `close`, as Java's mock does. `unsubscribe` and `assign` keep it, and so,
/// on a `KafkaConsumer`, does a `subscribe_*` with an empty topic list.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_ConsumerRebalanceListener_new(
    self_: *mut c_void,
    on_partitions_revoked: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_fn_t,
    on_partitions_assigned: kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_fn_t,
    on_partitions_lost: kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_fn_t,
) -> *mut kafka_consumer_ConsumerRebalanceListener_t {
    Box::into_raw(Box::new(ListenerRegistration {
        self_: SendPtr(self_),
        on_partitions_revoked,
        on_partitions_assigned,
        on_partitions_lost,
    })) as *mut kafka_consumer_ConsumerRebalanceListener_t
}

/// Invokes `method` on the calling thread and waits for the report; its
/// error is stored in `*out_err`.
///
/// # Safety
///
/// `partitions` must be null or a valid list whose elements are
/// topic-partition handles, and `out_err` as `deliver_unit` requires.
unsafe fn invoke_blocking(
    registration: ListenerRegistration,
    method: Method,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list whose elements
    // are topic-partition handles, as `list_topic_partitions` requires.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let (tx, rx) = std::sync::mpsc::channel();
    registration.fire(
        method,
        &partitions,
        &INVOKER_RESULTS,
        Box::new(move |result| {
            let _ = tx.send(result);
        }),
    );
    let result = rx
        .recv()
        .unwrap_or_else(|_| Err(Error::local_illegal_state("listener result dropped")));
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// Invokes `method` on the calling thread; `cb` fires on whichever thread
/// reports the result, from inside the method or later.
///
/// # Safety
///
/// `partitions` must be null or a valid list whose elements are
/// topic-partition handles, and `cb` and `opaque` the pair the C caller
/// passed to the `_cb` entry point, which has not fired `cb`.
unsafe fn invoke_cb(
    registration: ListenerRegistration,
    method: Method,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list whose elements
    // are topic-partition handles, as `list_topic_partitions` requires.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    registration.fire(
        method,
        &partitions,
        &INVOKER_RESULTS,
        // SAFETY: Per this function's `# Safety`, `cb` and `opaque` are the pair the C caller
        // passed to the `_cb` entry point, which has not fired `cb`; `cb` fires only from this
        // sink, which `INVOKER_RESULTS` holds under a fresh id and runs at most once, as a
        // `FnOnce`, when the report for that id arrives. `error_slot` returns null or a fresh owned
        // error, handed over to `cb`.
        Box::new(move |result| unsafe { cb(error_slot(result), opaque.get()) }),
    );
}

/// Invokes the implementation's `onPartitionsRevoked` on the calling thread
/// with a list of `kafka_common_TopicPartition_t *` (copied) and waits for
/// its `__set_callback_result` report; stores the reported error in
/// `*out_err`.
///
/// # Parameters
///
/// - `partitions` - The list of partitions that were assigned to the consumer and now need to be
///   revoked. This will include the full assignment under the Classic/Eager protocol, given that it
///   revokes all partitions. It will only include the subset to revoke under the
///   Classic/Cooperative and Consumer protocols.
///
/// # Errors
///
/// - `kafka_common_Error_is_wakeup_error` If raised from a nested call to `KafkaConsumer`
/// - `kafka_common_Error_is_interrupt_error` If raised from a nested call to `KafkaConsumer`
///
/// # Safety
///
/// `self_` must be a live listener handle and `partitions` a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live listener handle, as
    // `listener_registration` requires.
    let registration = unsafe { listener_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of topic-partition
    // handles, and `out_err` is as `invoke_blocking` requires; `registration` was just copied out of the live handle
    // `self_`, and the method is one of its own pointers.
    unsafe { invoke_blocking(registration, registration.on_partitions_revoked, partitions, out_err) }
}

/// Completion of the `_cb` invokers: `error` is `NULL` on success, owned
/// otherwise.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `on_partitions_revoked`: the method runs on the calling
/// thread, `cb` fires when its result is reported (a standalone listener
/// has no callbacks vector to queue on, so `cb` runs on the reporting
/// thread).
///
/// # Parameters
///
/// - `partitions` - The list of partitions that were assigned to the consumer and now need to be
///   revoked. This will include the full assignment under the Classic/Eager protocol, given that it
///   revokes all partitions. It will only include the subset to revoke under the
///   Classic/Cooperative and Consumer protocols.
/// - `cb` - the completion, receiving the error, `NULL` on success
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_wakeup_error` If raised from a nested call to `KafkaConsumer`
/// - `kafka_common_Error_is_interrupt_error` If raised from a nested call to `KafkaConsumer`
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as the last step, in `invoke_cb`, to the sink `fire` registers right before calling the C
    // method; nothing after that can panic, as the C method cannot unwind into Rust and
    // `kafka_List_destroy` is guarded. So `cb` has not fired for this `opaque`, as `deliver_panic`
    // requires; `run_inline` fires it on this thread.
    unsafe { deliver_panic(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live listener handle, as
    // `listener_registration` requires.
    let registration = unsafe { listener_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of topic-partition
    // handles; `cb` and `opaque` are this call's own arguments, so `cb` has not fired yet. Both are
    // what `invoke_cb` requires. `registration` was just copied out of the live handle `self_`, and
    // the method is one of its own pointers.
    unsafe { invoke_cb(registration, registration.on_partitions_revoked, partitions, cb, opaque) }
}

/// Invokes the implementation's `onPartitionsAssigned`; see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked`].
///
/// # Parameters
///
/// - `partitions` - Partitions that have been added to the assignment as a result of the rebalance.
///   Note that partitions that were already owned by this consumer and remain assigned are not
///   included in this list under the Classic/Cooperative or Consumer protocols. THe full assignment
///   will be received under the Classic/Eager protocol.
///
/// # Errors
///
/// - `kafka_common_Error_is_wakeup_error` If raised from a nested call to `KafkaConsumer`
/// - `kafka_common_Error_is_interrupt_error` If raised from a nested call to `KafkaConsumer`
///
/// # Safety
///
/// `self_` must be a live listener handle and `partitions` a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live listener handle, as
    // `listener_registration` requires.
    let registration = unsafe { listener_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of topic-partition
    // handles, and `out_err` is as `invoke_blocking` requires; `registration` was just copied out of the live handle
    // `self_`, and the method is one of its own pointers.
    unsafe { invoke_blocking(registration, registration.on_partitions_assigned, partitions, out_err) }
}

/// Completion of `on_partitions_assigned_cb`.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `on_partitions_assigned`; see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb`].
///
/// # Parameters
///
/// - `partitions` - Partitions that have been added to the assignment as a result of the rebalance.
///   Note that partitions that were already owned by this consumer and remain assigned are not
///   included in this list under the Classic/Cooperative or Consumer protocols. THe full assignment
///   will be received under the Classic/Eager protocol.
/// - `cb` - the completion, receiving the error, `NULL` on success
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_wakeup_error` If raised from a nested call to `KafkaConsumer`
/// - `kafka_common_Error_is_interrupt_error` If raised from a nested call to `KafkaConsumer`
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as the last step, in `invoke_cb`, to the sink `fire` registers right before calling the C
    // method; nothing after that can panic, as the C method cannot unwind into Rust and
    // `kafka_List_destroy` is guarded. So `cb` has not fired for this `opaque`, as `deliver_panic`
    // requires; `run_inline` fires it on this thread.
    unsafe { deliver_panic(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live listener handle, as
    // `listener_registration` requires.
    let registration = unsafe { listener_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of topic-partition
    // handles; `cb` and `opaque` are this call's own arguments, so `cb` has not fired yet. Both are
    // what `invoke_cb` requires. `registration` was just copied out of the live handle `self_`, and
    // the method is one of its own pointers.
    unsafe { invoke_cb(registration, registration.on_partitions_assigned, partitions, cb, opaque) }
}

/// Invokes the implementation's `onPartitionsLost` (its `onPartitionsRevoked`
/// when registered as `NULL`); see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked`].
///
/// # Parameters
///
/// - `partitions` - The list of partitions that were assigned to the consumer and now have been
///   reassigned to other consumers. With both, the Classic and Consumer protocols, this will always
///   include all partitions that were previously assigned to the consumer.
///
/// # Errors
///
/// - `kafka_common_Error_is_wakeup_error` If raised from a nested call to `KafkaConsumer`
/// - `kafka_common_Error_is_interrupt_error` If raised from a nested call to `KafkaConsumer`
///
/// # Safety
///
/// `self_` must be a live listener handle and `partitions` a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live listener handle, as
    // `listener_registration` requires.
    let registration = unsafe { listener_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of topic-partition
    // handles, and `out_err` is as `invoke_blocking` requires; `registration` was just copied out of the live handle
    // `self_`, and the method is one of its own pointers.
    unsafe { invoke_blocking(registration, registration.lost_or_revoked(), partitions, out_err) }
}

/// Completion of `on_partitions_lost_cb`.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `on_partitions_lost`; see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb`].
///
/// # Parameters
///
/// - `partitions` - The list of partitions that were assigned to the consumer and now have been
///   reassigned to other consumers. With both, the Classic and Consumer protocols, this will always
///   include all partitions that were previously assigned to the consumer.
/// - `cb` - the completion, receiving the error, `NULL` on success
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_wakeup_error` If raised from a nested call to `KafkaConsumer`
/// - `kafka_common_Error_is_interrupt_error` If raised from a nested call to `KafkaConsumer`
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as the last step, in `invoke_cb`, to the sink `fire` registers right before calling the C
    // method; nothing after that can panic, as the C method cannot unwind into Rust and
    // `kafka_List_destroy` is guarded. So `cb` has not fired for this `opaque`, as `deliver_panic`
    // requires; `run_inline` fires it on this thread.
    unsafe { deliver_panic(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live listener handle, as
    // `listener_registration` requires.
    let registration = unsafe { listener_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of topic-partition
    // handles; `cb` and `opaque` are this call's own arguments, so `cb` has not fired yet. Both are
    // what `invoke_cb` requires. `registration` was just copied out of the live handle `self_`, and
    // the method is one of its own pointers.
    unsafe { invoke_cb(registration, registration.lost_or_revoked(), partitions, cb, opaque) }
}

/// Frees the handle; the consumer it was registered with keeps its own copy
/// of the registration. A null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a live handle not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_destroy(
    self_: *mut kafka_consumer_ConsumerRebalanceListener_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is a live
        // handle not used afterwards, i.e. the `Box<ListenerRegistration>` of
        // `kafka_consumer_ConsumerRebalanceListener_new`, so taking it back and dropping it frees
        // it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ListenerRegistration) });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::ffi::callback_queue::CallbackQueue;
    use crate::ffi::common::fail_point;
    use crate::ffi::common::panic_probe::{Probe, on_error};
    use crate::ffi::common::topic_partition::{
        kafka_common_TopicPartition_partition, kafka_common_TopicPartition_topic,
    };
    use crate::ffi::common::{box_error, error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::{c_str_to_string, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size};

    /// One logged call: the method name and the partitions it received.
    type LoggedCall = (&'static str, Vec<(String, i32)>);

    /// A C implementation logging its calls and reporting the configured way.
    #[derive(Default)]
    struct Impl {
        calls: Mutex<Vec<LoggedCall>>,
        /// `Some(id)`: do not report, store the id for the test to report later.
        deferred: Mutex<Option<i64>>,
        defer: AtomicBool,
        fail: AtomicBool,
        /// The registry of the delivery an adapter invokes through; the
        /// invokers' ids are tagged and go to `INVOKER_RESULTS` instead, as
        /// `kafka_consumer_Consumer__set_callback_result` routes them.
        results: Mutex<Option<Arc<CallbackResults>>>,
    }

    impl Impl {
        fn report(&self, callback_id: i64, error: Option<Error>) {
            let delivered = if CallbackResults::is_tagged(callback_id) {
                INVOKER_RESULTS.complete(callback_id, std::ptr::null_mut(), error)
            } else {
                let results = self.results.lock().unwrap().clone().expect("an adapter's registry");
                results.complete(callback_id, std::ptr::null_mut(), error)
            };
            assert!(delivered, "a pending id");
        }
    }

    unsafe fn record(self_: *mut c_void, name: &'static str, partitions: *const kafka_List_t, callback_id: i64) {
        // SAFETY: Test code: `self_` is the `&imp` the test registered, alive for the whole test.
        let imp = unsafe { &*(self_ as *const Impl) };
        // SAFETY: Test code: `partitions` is the list `fire` passes, live for the call.
        let n = unsafe { kafka_List_size(partitions) };
        let tps = (0..n)
            .map(|i| {
                // SAFETY: Test code: `partitions` is the list `fire` passes, live for the call, and
                // `i` is in range.
                let tp = unsafe { kafka_List_get(partitions, i) } as *const _;
                // SAFETY: Test code: `tp` is an element of that list, a topic-partition handle, and
                // its topic is a NUL-terminated string it owns.
                unsafe {
                    (
                        c_str_to_string(kafka_common_TopicPartition_topic(tp)),
                        kafka_common_TopicPartition_partition(tp),
                    )
                }
            })
            .collect();
        imp.calls.lock().unwrap().push((name, tps));
        if imp.defer.load(Ordering::Relaxed) {
            *imp.deferred.lock().unwrap() = Some(callback_id);
        } else if imp.fail.load(Ordering::Relaxed) {
            imp.report(callback_id, Some(Error::local_illegal_state("listener failed")));
        } else {
            imp.report(callback_id, None);
        }
    }

    unsafe extern "C" fn revoked(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
        // SAFETY: Test code: forwards the arguments the C contract gives this method, which
        // `record` requires.
        unsafe { record(self_, "revoked", partitions, callback_id) }
    }

    unsafe extern "C" fn assigned(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
        // SAFETY: Test code: forwards the arguments the C contract gives this method, which
        // `record` requires.
        unsafe { record(self_, "assigned", partitions, callback_id) }
    }

    fn handle(imp: &Impl) -> *mut kafka_consumer_ConsumerRebalanceListener_t {
        kafka_consumer_ConsumerRebalanceListener_new(imp as *const Impl as *mut c_void, revoked, assigned, None)
    }

    #[test]
    fn invokers_report_synchronously_and_lost_defaults_to_revoked() {
        let imp = Impl::default();
        let listener = handle(&imp);
        let partitions = sorted_topic_partition_list([TopicPartition::new("t", 1), TopicPartition::new("a", 0)].iter());
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `listener` and `partitions` are live handles this test made, `err` is a
        // valid slot pointing at null before each call, and the error stored in it is owned; each
        // is destroyed once, after its last use.
        unsafe {
            kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned(listener, partitions, &mut err);
            assert!(err.is_null());
            kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(listener, partitions, &mut err);
            assert!(err.is_null());
            imp.fail.store(true, Ordering::Relaxed);
            kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked(listener, partitions, &mut err);
            assert!(!err.is_null());
            assert_eq!(error_ref(err).error.message(), "listener failed");
            kafka_common_Error_destroy(err);
            kafka_List_destroy(partitions);
            kafka_consumer_ConsumerRebalanceListener_destroy(listener);
        }
        let sorted = vec![("a".to_string(), 0), ("t".to_string(), 1)];
        assert_eq!(
            *imp.calls.lock().unwrap(),
            vec![
                ("assigned", sorted.clone()),
                ("revoked", sorted.clone()),
                ("revoked", sorted)
            ]
        );
    }

    #[test]
    fn cb_invoker_fires_when_the_result_is_reported_later() {
        let imp = Impl::default();
        imp.defer.store(true, Ordering::Relaxed);
        let listener = handle(&imp);
        let fired: Mutex<Option<bool>> = Mutex::new(None);
        unsafe extern "C" fn cb(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
            // SAFETY: Test code: `opaque` is `&fired`, alive until the test ends.
            let fired = unsafe { &*(opaque as *const Mutex<Option<bool>>) };
            *fired.lock().unwrap() = Some(error.is_null());
            // SAFETY: Test code: `error` is null or the owned error the completion hands over,
            // destroyed once here.
            unsafe { kafka_common_Error_destroy(error) };
        }
        let partitions = sorted_topic_partition_list([TopicPartition::new("t", 0)].iter());
        // SAFETY: Test code: `listener` and `partitions` are live handles this test made, `cb`
        // matches the completion type and `&fired` outlives the call; `partitions` is destroyed
        // once, after its use.
        unsafe {
            kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb(
                listener,
                partitions,
                cb,
                &fired as *const _ as *mut c_void,
            );
            kafka_List_destroy(partitions);
        }
        assert_eq!(*fired.lock().unwrap(), None, "not reported yet");
        let id = imp.deferred.lock().unwrap().take().unwrap();
        let error = box_error(Error::timeout("late"));
        // SAFETY: Test code: `id` is the pending id the deferred method stored, issued by an invoker,
        // so the function does not read its consumer handle and null is fine; `error` is an owned
        // error, handed over.
        unsafe {
            crate::ffi::consumer::kafka_consumer_Consumer__set_callback_result(
                std::ptr::null(),
                id,
                std::ptr::null_mut(),
                error,
            )
        };
        assert_eq!(*fired.lock().unwrap(), Some(false));
        // SAFETY: Test code: `listener` is a live handle this test made, destroyed once here.
        unsafe { kafka_consumer_ConsumerRebalanceListener_destroy(listener) };
    }

    #[test]
    fn adapter_runs_inline_when_blocking_and_queues_otherwise() {
        let imp = Impl::default();
        let listener = handle(&imp);
        let delivery = Arc::new(Delivery::for_tests(CallbackQueue::new()));
        // SAFETY: Test code: `listener` is a live handle this test made; the adapter copies the
        // registration, and `imp` outlives the adapter.
        let adapter = unsafe { listener_adapter(listener, Arc::clone(&delivery)) };
        *imp.results.lock().unwrap() = Some(Arc::clone(delivery.results()));
        // SAFETY: Test code: `listener` is a live handle, destroyed once here; the adapter keeps
        // its own copy.
        unsafe { kafka_consumer_ConsumerRebalanceListener_destroy(listener) };
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let partitions = [TopicPartition::new("t", 0)];

        delivery.set_blocking_for_tests(true);
        runtime.block_on(adapter.on_partitions_assigned(&partitions)).unwrap();
        assert_eq!(imp.calls.lock().unwrap().len(), 1, "invoked inline");

        delivery.set_blocking_for_tests(false);
        let queue_delivery = Arc::clone(&delivery);
        let pump = std::thread::spawn(move || {
            while queue_delivery.queue().is_empty() {
                std::thread::yield_now();
            }
            queue_delivery.queue().execute()
        });
        runtime.block_on(adapter.on_partitions_revoked(&partitions)).unwrap();
        assert_eq!(pump.join().unwrap(), 1, "invoked through the queue");
        assert_eq!(imp.calls.lock().unwrap().len(), 2);
    }

    /// A standalone listener has no callbacks vector, so a panic before the
    /// method is invoked fires `cb` once on the calling thread, before the
    /// call returns, and the C method never runs.
    #[test]
    fn a_panicking_cb_invoker_fires_cb_once_inline() {
        let imp = Impl::default();
        let listener = handle(&imp);
        let partitions = kafka_List_new();
        for (export, invoke) in [
            (
                "kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb",
                kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb as unsafe extern "C" fn(_, _, _, _),
            ),
            (
                "kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb",
                kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb,
            ),
            (
                "kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb",
                kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb,
            ),
        ] {
            let probe = Probe::default();
            fail_point::arm();
            // SAFETY: Test code: `listener` and `partitions` are live handles this test made,
            // `on_error` matches the completion type and the probe outlives the call.
            unsafe { invoke(listener, partitions, on_error, probe.as_opaque()) };
            probe.assert_one_panic(export, fail_point::MESSAGE);
        }
        assert!(imp.calls.lock().unwrap().is_empty(), "no method ran");
        // SAFETY: Test code: `partitions` and `listener` are live handles this test made, destroyed
        // once here.
        unsafe {
            kafka_List_destroy(partitions);
            kafka_consumer_ConsumerRebalanceListener_destroy(listener);
        }
    }
}
