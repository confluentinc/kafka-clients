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
//! through `kafka_consumer_Consumer_execute_callbacks` for a `_cb` one. The
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
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{list_topic_partitions, sorted_topic_partition_list};
use crate::ffi::consumer::{Delivery, error_slot, register_callback_result};
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
/// Rust (CLAUDE.md §4 rule 3): it reports its result with
/// `kafka_consumer_Consumer__set_callback_result(consumer, callback_id, result)`,
/// `NULL` for success or an owned `kafka_common_Error_t *` for failure.
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
/// (CLAUDE.md §4 rule 3) until the consumer releases it.
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
    /// callback id whose report reaches `sink`; the list is freed after the
    /// method returned (C copies what it keeps).
    fn fire(self, method: Method, partitions: &[TopicPartition], sink: Box<dyn FnOnce(Result<(), Error>) + Send>) {
        let callback_id = register_callback_result(sink);
        let list = sorted_topic_partition_list(partitions.iter());
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
        self.delivery.invoke(Box::new(move || {
            registration.fire(
                method,
                &partitions,
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
    Arc::new(CRebalanceListener { registration: unsafe { listener_registration(listener) }, delivery })
}

/// Registers a C implementation of `ConsumerRebalanceListener`;
/// `on_partitions_lost` may be `NULL` for the Java default. The caller owns
/// `self_` and keeps it alive until the handle is destroyed and, once passed
/// to a `subscribe_*`, until the next `subscribe_*`, `unsubscribe` or the
/// consumer's destruction releases it.
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

/// Invokes `method` on the calling thread and waits for the report.
///
/// # Safety
///
/// `partitions` must be a valid list of topic partitions.
unsafe fn invoke_blocking(
    registration: ListenerRegistration,
    method: Method,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let (tx, rx) = std::sync::mpsc::channel();
    registration.fire(
        method,
        &partitions,
        Box::new(move |result| {
            let _ = tx.send(result);
        }),
    );
    error_slot(
        rx.recv()
            .unwrap_or_else(|_| Err(Error::local_illegal_state("listener result dropped"))),
    )
}

/// Invokes `method` on the calling thread; `cb` fires on whichever thread
/// reports the result, from inside the method or later.
///
/// # Safety
///
/// `partitions` must be a valid list of topic partitions.
unsafe fn invoke_cb(
    registration: ListenerRegistration,
    method: Method,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(partitions) };
    let opaque = SendPtr(opaque);
    registration.fire(
        method,
        &partitions,
        Box::new(move |result| unsafe { cb(error_slot(result), opaque.get()) }),
    );
}

/// Invokes the implementation's `onPartitionsRevoked` on the calling thread
/// with a list of `kafka_common_TopicPartition_t *` (copied) and waits for
/// its `__set_callback_result` report; returns the reported error, or `NULL`.
///
/// # Safety
///
/// `self_` must be a live listener handle and `partitions` a valid list.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let registration = unsafe { listener_registration(self_) };
    unsafe { invoke_blocking(registration, registration.on_partitions_revoked, partitions) }
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
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb_t,
    opaque: *mut c_void,
) {
    let registration = unsafe { listener_registration(self_) };
    unsafe { invoke_cb(registration, registration.on_partitions_revoked, partitions, cb, opaque) }
}

/// Invokes the implementation's `onPartitionsAssigned`; see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked`].
///
/// # Safety
///
/// `self_` must be a live listener handle and `partitions` a valid list.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let registration = unsafe { listener_registration(self_) };
    unsafe { invoke_blocking(registration, registration.on_partitions_assigned, partitions) }
}

/// Completion of `on_partitions_assigned_cb`.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `on_partitions_assigned`; see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb`].
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb_t,
    opaque: *mut c_void,
) {
    let registration = unsafe { listener_registration(self_) };
    unsafe { invoke_cb(registration, registration.on_partitions_assigned, partitions, cb, opaque) }
}

/// Invokes the implementation's `onPartitionsLost` (its `onPartitionsRevoked`
/// when registered as `NULL`); see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked`].
///
/// # Safety
///
/// `self_` must be a live listener handle and `partitions` a valid list.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let registration = unsafe { listener_registration(self_) };
    unsafe { invoke_blocking(registration, registration.lost_or_revoked(), partitions) }
}

/// Completion of `on_partitions_lost_cb`.
pub type kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `on_partitions_lost`; see
/// [`kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb`].
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb(
    self_: *const kafka_consumer_ConsumerRebalanceListener_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb_t,
    opaque: *mut c_void,
) {
    let registration = unsafe { listener_registration(self_) };
    unsafe { invoke_cb(registration, registration.lost_or_revoked(), partitions, cb, opaque) }
}

/// Frees the handle; the consumer it was registered with keeps its own copy
/// of the registration. A null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a live handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRebalanceListener_destroy(
    self_: *mut kafka_consumer_ConsumerRebalanceListener_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ListenerRegistration) });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::ffi::callback_queue::CallbackQueue;
    use crate::ffi::common::topic_partition::{
        kafka_common_TopicPartition_partition, kafka_common_TopicPartition_topic,
    };
    use crate::ffi::common::{box_error, error_ref, kafka_common_Error_destroy};
    use crate::ffi::consumer::complete_callback_result;
    use crate::ffi::util::{c_str_to_string, kafka_List_get, kafka_List_size};

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
    }

    unsafe fn record(self_: *mut c_void, name: &'static str, partitions: *const kafka_List_t, callback_id: i64) {
        let imp = unsafe { &*(self_ as *const Impl) };
        let n = unsafe { kafka_List_size(partitions) };
        let tps = (0..n)
            .map(|i| {
                let tp = unsafe { kafka_List_get(partitions, i) } as *const _;
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
            complete_callback_result(callback_id, Err(Error::local_illegal_state("listener failed")));
        } else {
            complete_callback_result(callback_id, Ok(()));
        }
    }

    unsafe extern "C" fn revoked(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
        unsafe { record(self_, "revoked", partitions, callback_id) }
    }

    unsafe extern "C" fn assigned(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
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
        unsafe {
            assert!(kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned(listener, partitions).is_null());
            assert!(kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(listener, partitions).is_null());
            imp.fail.store(true, Ordering::Relaxed);
            let error = kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked(listener, partitions);
            assert_eq!(error_ref(error).error.message(), "listener failed");
            kafka_common_Error_destroy(error);
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
            let fired = unsafe { &*(opaque as *const Mutex<Option<bool>>) };
            *fired.lock().unwrap() = Some(error.is_null());
            unsafe { kafka_common_Error_destroy(error) };
        }
        let partitions = sorted_topic_partition_list([TopicPartition::new("t", 0)].iter());
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
        unsafe {
            crate::ffi::consumer::kafka_consumer_Consumer__set_callback_result(
                std::ptr::null(),
                id,
                error as *mut c_void,
            )
        };
        assert_eq!(*fired.lock().unwrap(), Some(false));
        unsafe { kafka_consumer_ConsumerRebalanceListener_destroy(listener) };
    }

    #[test]
    fn adapter_runs_inline_when_blocking_and_queues_otherwise() {
        let imp = Impl::default();
        let listener = handle(&imp);
        let delivery = Arc::new(Delivery::for_tests(CallbackQueue::new()));
        let adapter = unsafe { listener_adapter(listener, Arc::clone(&delivery)) };
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
}
