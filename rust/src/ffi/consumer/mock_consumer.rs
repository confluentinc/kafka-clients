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

//! C bindings for `org.apache.kafka.clients.consumer.MockConsumer`
//! (CLAUDE.md §4).
//!
//! `kafka_consumer_MockConsumer_t` is a class handle, owned by the caller
//! and freed with [`kafka_consumer_MockConsumer_destroy`];
//! [`kafka_consumer_MockConsumer__as_Consumer`] is its borrowed `Consumer`
//! view (rule 3), through which every `kafka_consumer_Consumer_*` function
//! applies. The mock-specific methods below take the class handle (rule 9).
//!
//! The mock shares the consumer's single-owner flag: a mock method called
//! while an operation is in flight (a `_cb` one, or a blocking one on
//! another thread) is rejected the way Java's `ConcurrentModificationException`
//! rejects it — returned through the error slot where the method has one;
//! the `void` setters log and ignore the call and the getters return `-1`.

use std::ffi::c_char;
use std::ffi::c_void;
use std::sync::Arc;

use log::warn;

use crate::common::Error;
use crate::consumer::MockConsumer;
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::common::partition_info::list_partition_infos;
use crate::ffi::common::topic_partition::{list_topic_partitions, map_topic_partition_i64};
use crate::ffi::common::{kafka_common_Error_t, take_error};
use crate::ffi::consumer::consumer_record::{consumer_record_ref, kafka_consumer_ConsumerRecord_t};
use crate::ffi::consumer::{
    BusyGuard, ConsumerClassHandle, ConsumerKind, Owns, client_ref, deliver_void, error_slot,
    kafka_consumer_Consumer_t, out_slot,
};
use crate::ffi::util::{GenericValue, c_str_to_string, kafka_List_t, kafka_Map_t};

/// Opaque handle to a [`MockConsumer`] over `void *` key and value.
#[repr(C)]
pub struct kafka_consumer_MockConsumer_t {
    _private: [u8; 0],
}

type Mock = MockConsumer<GenericValue, GenericValue>;

/// The mock behind a guard; a `Consumer` handle from `KafkaConsumer_new`
/// is never a mock, so the other arm cannot be reached through this type.
fn mock_of(guard: &mut BusyGuard) -> Result<&mut Mock, Error> {
    match guard.kind_mut() {
        ConsumerKind::Mock(mock) => Ok(&mut **mock),
        ConsumerKind::Async(_) => Err(Error::local_illegal_state("not a MockConsumer")),
    }
}

/// Runs `f` on the mock under the single-owner flag.
///
/// # Safety
///
/// `self_` must be a live mock handle.
unsafe fn with_mock<T>(
    self_: *const kafka_consumer_MockConsumer_t,
    f: impl FnOnce(&mut Mock) -> T,
) -> Result<T, Error> {
    let mut guard = unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) }.acquire()?;
    Ok(f(mock_of(&mut guard)?))
}

/// A void mock method: rejected calls are logged, there being no slot to
/// report them through.
///
/// # Safety
///
/// `self_` must be a live mock handle.
unsafe fn with_mock_void(self_: *mut kafka_consumer_MockConsumer_t, name: &str, f: impl FnOnce(&mut Mock)) {
    if let Err(error) = unsafe { with_mock(self_, f) } {
        warn!("MockConsumer.{name} ignored: {error}");
    }
}

/// `new MockConsumer(String offsetResetStrategy)`: delivers the owned class
/// handle or returns the owned error (an unknown strategy).
///
/// # Safety
///
/// `offset_reset_strategy` must be a valid string and `out_new` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_new(
    offset_reset_strategy: *const c_char,
    out_new: *mut *mut kafka_consumer_MockConsumer_t,
) -> *mut kafka_common_Error_t {
    let strategy = unsafe { c_str_to_string(offset_reset_strategy) };
    // No deserializers: the `void *`s are whatever `add_record` was given.
    let result = ConsumerClassHandle::new(Owns { key: false, value: false }, || {
        Mock::new(&strategy).map(|mock| ConsumerKind::Mock(Box::new(mock)))
    });
    unsafe {
        out_slot(result, out_new, |handle| {
            Box::into_raw(handle) as *mut kafka_consumer_MockConsumer_t
        })
    }
}

/// The class as a `Consumer`: a borrowed view valid until the class handle
/// is destroyed, never passed to `kafka_consumer_Consumer_destroy`
/// (CLAUDE.md §4 rule 3). `*mut` because `poll` and the other operations
/// take `&mut self`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer__as_Consumer(
    self_: *mut kafka_consumer_MockConsumer_t,
) -> *mut kafka_consumer_Consumer_t {
    unsafe { &*(self_ as *const ConsumerClassHandle) }.as_consumer()
}

/// Frees the handle as `kafka_consumer_Consumer_destroy` does; null is a
/// no-op.
///
/// # Safety
///
/// `self_` must be null or a live handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_destroy(self_: *mut kafka_consumer_MockConsumer_t) {
    if !self_.is_null() {
        unsafe { Box::from_raw(self_ as *mut ConsumerClassHandle) }.destroy();
    }
}

/// `addRecord(ConsumerRecord<K, V> record)`: the record is copied (its
/// `void *` key and value are shared with the caller, who keeps them alive
/// until the records holding them are destroyed).
///
/// # Safety
///
/// `self_` must be a live handle and `record` a live record.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_add_record(
    self_: *mut kafka_consumer_MockConsumer_t,
    record: *const kafka_consumer_ConsumerRecord_t,
) -> *mut kafka_common_Error_t {
    let record = unsafe { consumer_record_ref(record) }.clone();
    error_slot(unsafe { with_mock(self_, |mock| mock.add_record(record)) }.and_then(|r| r))
}

/// `updateBeginningOffsets(Map<TopicPartition, Long>)`: a map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`, copied.
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` a valid map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_beginning_offsets(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_offsets: *const kafka_Map_t,
) {
    let offsets = unsafe { map_topic_partition_i64(new_offsets) };
    unsafe { with_mock_void(self_, "updateBeginningOffsets", |mock| mock.update_beginning_offsets(offsets)) }
}

/// `updateEndOffsets(Map<TopicPartition, Long>)`.
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` a valid map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_end_offsets(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_offsets: *const kafka_Map_t,
) {
    let offsets = unsafe { map_topic_partition_i64(new_offsets) };
    unsafe { with_mock_void(self_, "updateEndOffsets", |mock| mock.update_end_offsets(offsets)) }
}

/// `updateDurationOffsets(Map<TopicPartition, Long>)`.
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` a valid map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_duration_offsets(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_offsets: *const kafka_Map_t,
) {
    let offsets = unsafe { map_topic_partition_i64(new_offsets) };
    unsafe { with_mock_void(self_, "updateDurationOffsets", |mock| mock.update_duration_offsets(offsets)) }
}

/// `updatePartitions(String topic, List<PartitionInfo> partitions)`: a list
/// of `kafka_common_PartitionInfo_t *`, copied.
///
/// # Safety
///
/// `self_` must be a live handle, `topic` a valid string and `partitions`
/// a valid list.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_partitions(
    self_: *mut kafka_consumer_MockConsumer_t,
    topic: *const c_char,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let partitions = unsafe { list_partition_infos(partitions) };
    error_slot(unsafe { with_mock(self_, |mock| mock.update_partitions(&topic, partitions)) }.and_then(|r| r))
}

/// `setPollException(KafkaException)`: the next `poll` fails with `error`,
/// which Rust takes over.
///
/// # Safety
///
/// `self_` must be a live handle and `error` an owned error not used
/// afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_poll_error(
    self_: *mut kafka_consumer_MockConsumer_t,
    error: *mut kafka_common_Error_t,
) {
    let Some(error) = (unsafe { take_error(error) }) else {
        return;
    };
    unsafe { with_mock_void(self_, "setPollException", |mock| mock.set_poll_error(error)) }
}

/// `setOffsetsException(KafkaException)`: the next offsets query fails with
/// `error`, which Rust takes over.
///
/// # Safety
///
/// `self_` must be a live handle and `error` an owned error not used
/// afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_offsets_error(
    self_: *mut kafka_consumer_MockConsumer_t,
    error: *mut kafka_common_Error_t,
) {
    let Some(error) = (unsafe { take_error(error) }) else {
        return;
    };
    unsafe { with_mock_void(self_, "setOffsetsException", |mock| mock.set_offsets_error(error)) }
}

/// `setMaxPollRecords(long maxPollRecords)`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_max_poll_records(
    self_: *mut kafka_consumer_MockConsumer_t,
    max_poll_records: i64,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe { with_mock(self_, |mock| mock.set_max_poll_records(max_poll_records)) }.and_then(|r| r))
}

/// `rebalance(Collection<TopicPartition> newAssignment)`: blocking, the
/// registered listener's callbacks run on the calling thread
/// (CLAUDE.md §4 rule 5).
///
/// # Safety
///
/// `self_` must be a live handle and `new_assignment` a valid list.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_rebalance(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_assignment: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let partitions = unsafe { list_topic_partitions(new_assignment) };
    let client = unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) };
    let result = client.acquire().and_then(|mut guard| {
        let mock = mock_of(&mut guard)?;
        client.block_on(mock.rebalance(&partitions))
    });
    error_slot(result)
}

/// The completion of [`kafka_consumer_MockConsumer_rebalance_cb`].
pub type kafka_consumer_MockConsumer_rebalance_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `rebalance`: the listener's callbacks and `cb` are queued
/// for `kafka_consumer_Consumer_execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_rebalance_cb(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_assignment: *const kafka_List_t,
    cb: kafka_consumer_MockConsumer_rebalance_cb_t,
    opaque: *mut c_void,
) {
    let partitions = unsafe { list_topic_partitions(new_assignment) };
    let opaque = SendPtr(opaque);
    let client = unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) };
    match client.acquire() {
        Err(error) => deliver_void(client, cb, opaque, Err(error)),
        Ok(mut guard) => {
            let client = Arc::clone(client);
            let task_client = Arc::clone(&client);
            client.spawn(async move {
                let result = match mock_of(&mut guard) {
                    Ok(mock) => mock.rebalance(&partitions).await,
                    Err(error) => Err(error),
                };
                deliver_void(&task_client, cb, opaque, result);
                drop(guard);
            });
        },
    }
}

/// `closed()`: `1` once `close` ran, `-1` while an operation is in flight.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_closed(self_: *const kafka_consumer_MockConsumer_t) -> i8 {
    unsafe { with_mock(self_, |mock| i8::from(mock.closed())) }.unwrap_or(-1)
}

/// `shouldRebalance()`: `-1` while an operation is in flight.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_should_rebalance(
    self_: *const kafka_consumer_MockConsumer_t,
) -> i8 {
    unsafe { with_mock(self_, |mock| i8::from(mock.should_rebalance())) }.unwrap_or(-1)
}

/// `resetShouldRebalance()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_reset_should_rebalance(self_: *mut kafka_consumer_MockConsumer_t) {
    unsafe { with_mock_void(self_, "resetShouldRebalance", Mock::reset_should_rebalance) }
}

/// `lastPollTimeout()`: the last `poll` timeout in milliseconds, `-1` when
/// `poll` never ran (and while an operation is in flight).
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_last_poll_timeout(
    self_: *const kafka_consumer_MockConsumer_t,
) -> i64 {
    unsafe {
        with_mock(self_, |mock| {
            mock.last_poll_timeout()
                .map_or(-1, |timeout| i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX))
        })
    }
    .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicI32, AtomicI64};
    use std::thread::{self, ThreadId};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::ffi::common::topic_partition::{
        kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_new, kafka_common_TopicPartition_t,
    };
    use crate::ffi::consumer::consumer_rebalance_listener::{
        kafka_consumer_ConsumerRebalanceListener_destroy, kafka_consumer_ConsumerRebalanceListener_new,
    };
    use crate::ffi::consumer::consumer_record::{
        kafka_consumer_ConsumerRecord_destroy, kafka_consumer_ConsumerRecord_key, kafka_consumer_ConsumerRecord_new,
        kafka_consumer_ConsumerRecord_value,
    };
    use crate::ffi::consumer::consumer_records::{
        kafka_consumer_ConsumerRecords_count, kafka_consumer_ConsumerRecords_destroy,
        kafka_consumer_ConsumerRecords_records_with_partition,
    };
    use crate::ffi::consumer::{
        kafka_consumer_Consumer_assign, kafka_consumer_Consumer_commit_sync, kafka_consumer_Consumer_execute_callbacks,
        kafka_consumer_Consumer_poll, kafka_consumer_Consumer_set_callback_result,
        kafka_consumer_Consumer_set_callbacks_notify, kafka_consumer_Consumer_subscribe_with_topics_listener,
    };
    use crate::ffi::util::{
        kafka_Bytes_t, kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size,
        kafka_Map_destroy, kafka_Map_new, kafka_Map_put,
    };

    /// A mock over `void *` records and its borrowed `Consumer` view.
    fn new_mock() -> (*mut kafka_consumer_MockConsumer_t, *mut kafka_consumer_Consumer_t) {
        let mut mock = ptr::null_mut();
        let error = unsafe { kafka_consumer_MockConsumer_new(c"earliest".as_ptr(), &mut mock) };
        assert!(error.is_null());
        (mock, unsafe { kafka_consumer_MockConsumer__as_Consumer(mock) })
    }

    /// A C-built list holding one topic-partition; the caller destroys both.
    fn partition_list(topic: &CStr, partition: i32) -> (*mut kafka_List_t, *mut kafka_common_TopicPartition_t) {
        let tp = unsafe { kafka_common_TopicPartition_new(topic.as_ptr(), partition) };
        let list = kafka_List_new();
        unsafe { kafka_List_add(list, tp as *mut c_void) };
        (list, tp)
    }

    fn wait_until(what: &str, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// The `self` of a C listener: counts the partitions each method saw,
    /// records the invoking thread and either reports at once through
    /// `set_callback_result` or leaves the `callback_id` for the test.
    struct ListenerProbe {
        report: bool,
        thread: Mutex<Option<ThreadId>>,
        assigned: AtomicI32,
        revoked: AtomicI32,
        pending_id: AtomicI64,
    }

    impl ListenerProbe {
        fn new(report: bool) -> Self {
            Self {
                report,
                thread: Mutex::new(None),
                assigned: AtomicI32::new(0),
                revoked: AtomicI32::new(0),
                pending_id: AtomicI64::new(0),
            }
        }

        fn as_self(&self) -> *mut c_void {
            self as *const Self as *mut c_void
        }

        fn ran_on_this_thread(&self) -> bool {
            *self.thread.lock().unwrap() == Some(thread::current().id())
        }

        unsafe fn saw(&self, counter: &AtomicI32, partitions: *const kafka_List_t, callback_id: i64) {
            *self.thread.lock().unwrap() = Some(thread::current().id());
            counter.fetch_add(unsafe { kafka_List_size(partitions) }, SeqCst);
            if self.report {
                unsafe { kafka_consumer_Consumer_set_callback_result(ptr::null(), callback_id, ptr::null_mut()) };
            } else {
                self.pending_id.store(callback_id, SeqCst);
            }
        }
    }

    unsafe extern "C" fn probe_revoked(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
        let probe = unsafe { &*(self_ as *const ListenerProbe) };
        unsafe { probe.saw(&probe.revoked, partitions, callback_id) };
    }

    unsafe extern "C" fn probe_assigned(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
        let probe = unsafe { &*(self_ as *const ListenerProbe) };
        unsafe { probe.saw(&probe.assigned, partitions, callback_id) };
    }

    /// Subscribes the consumer to `topic` with a listener backed by `probe`;
    /// the listener handle is destroyed at once, its registration having
    /// been copied.
    unsafe fn subscribe(consumer: *mut kafka_consumer_Consumer_t, probe: &ListenerProbe) {
        let listener =
            kafka_consumer_ConsumerRebalanceListener_new(probe.as_self(), probe_revoked, probe_assigned, None);
        let topics = kafka_List_new();
        unsafe { kafka_List_add(topics, c"topic".as_ptr() as *mut c_void) };
        let error = unsafe { kafka_consumer_Consumer_subscribe_with_topics_listener(consumer, topics, listener) };
        assert!(error.is_null());
        unsafe {
            kafka_List_destroy(topics);
            kafka_consumer_ConsumerRebalanceListener_destroy(listener);
        }
    }

    unsafe extern "C" fn count_notify(opaque: *mut c_void) {
        unsafe { &*(opaque as *const AtomicI32) }.fetch_add(1, SeqCst);
    }

    unsafe extern "C" fn count_completion(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        assert!(error.is_null(), "{:?}", unsafe { take_error(error) });
        unsafe { &*(opaque as *const AtomicI32) }.fetch_add(1, SeqCst);
    }

    #[test]
    fn blocking_rebalance_invokes_the_listener_on_the_calling_thread() {
        let (mock, consumer) = new_mock();
        let probe = ListenerProbe::new(true);
        unsafe { subscribe(consumer, &probe) };

        let (assignment, tp) = partition_list(c"topic", 0);
        assert!(unsafe { kafka_consumer_MockConsumer_rebalance(mock, assignment) }.is_null());
        assert_eq!(probe.assigned.load(SeqCst), 1);
        assert!(
            probe.ran_on_this_thread(),
            "a blocking entry point invokes the listener directly"
        );

        // Rebalancing the partition away revokes it, on this thread too.
        let empty = kafka_List_new();
        assert!(unsafe { kafka_consumer_MockConsumer_rebalance(mock, empty) }.is_null());
        assert_eq!(probe.revoked.load(SeqCst), 1);
        assert!(probe.ran_on_this_thread());

        unsafe {
            kafka_List_destroy(empty);
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    #[test]
    fn rebalance_cb_queues_the_listener_and_its_completion_for_the_pump() {
        let (mock, consumer) = new_mock();
        let notifies = AtomicI32::new(0);
        unsafe {
            kafka_consumer_Consumer_set_callbacks_notify(
                consumer,
                count_notify,
                &notifies as *const AtomicI32 as *mut c_void,
            )
        };
        let probe = ListenerProbe::new(false);
        unsafe { subscribe(consumer, &probe) };

        let completed = AtomicI32::new(0);
        let (assignment, tp) = partition_list(c"topic", 0);
        unsafe {
            kafka_consumer_MockConsumer_rebalance_cb(
                mock,
                assignment,
                count_completion,
                &completed as *const AtomicI32 as *mut c_void,
            )
        };
        wait_until("the listener invocation to be queued", || notifies.load(SeqCst) >= 1);
        assert_eq!(probe.assigned.load(SeqCst), 0, "queued, not run, until the pump runs it");

        assert_eq!(unsafe { kafka_consumer_Consumer_execute_callbacks(consumer) }, 1);
        assert_eq!(probe.assigned.load(SeqCst), 1);
        assert!(probe.ran_on_this_thread(), "the pump runs it on the pumping thread");
        assert_eq!(completed.load(SeqCst), 0, "the operation waits for the report");

        // Reported later, from another thread.
        let id = probe.pending_id.load(SeqCst);
        thread::spawn(move || unsafe { kafka_consumer_Consumer_set_callback_result(ptr::null(), id, ptr::null_mut()) })
            .join()
            .unwrap();
        wait_until("the completion to be queued", || notifies.load(SeqCst) >= 2);
        assert_eq!(unsafe { kafka_consumer_Consumer_execute_callbacks(consumer) }, 1);
        assert_eq!(completed.load(SeqCst), 1);
        assert_eq!(unsafe { kafka_consumer_Consumer_execute_callbacks(consumer) }, 0);

        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    #[test]
    fn destroy_runs_the_pending_listener_and_completion_exactly_once() {
        let (mock, consumer) = new_mock();
        let probe = ListenerProbe::new(true);
        unsafe { subscribe(consumer, &probe) };

        let completed = AtomicI32::new(0);
        let (assignment, tp) = partition_list(c"topic", 0);
        unsafe {
            kafka_consumer_MockConsumer_rebalance_cb(
                mock,
                assignment,
                count_completion,
                &completed as *const AtomicI32 as *mut c_void,
            )
        };
        // Never pumped: `destroy` must run the queued listener invocation
        // (whose report lets the operation finish) and then its completion.
        unsafe { kafka_consumer_MockConsumer_destroy(mock) };
        assert_eq!(probe.assigned.load(SeqCst), 1);
        assert!(probe.ran_on_this_thread(), "fired on the destroying thread");
        assert_eq!(completed.load(SeqCst), 1);

        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
        }
    }

    #[test]
    fn poll_hands_back_the_record_pointers_add_record_was_given() {
        let (mock, consumer) = new_mock();
        let (assignment, tp) = partition_list(c"topic", 0);
        assert!(unsafe { kafka_consumer_Consumer_assign(consumer, assignment) }.is_null());

        let mut zero = 0i64;
        let offsets = kafka_Map_new();
        unsafe {
            kafka_Map_put(offsets, tp as *mut c_void, &mut zero as *mut i64 as *mut c_void);
            kafka_consumer_MockConsumer_update_beginning_offsets(mock, offsets);
            kafka_Map_destroy(offsets);
        }

        // With no deserializer the `void *`s are `kafka_Bytes_t *` the
        // caller owns; the mock hands the very same pointers back.
        let key = kafka_Bytes_t { data: b"k".as_ptr(), len: 1 };
        let value = kafka_Bytes_t { data: b"value".as_ptr(), len: 5 };
        let record = unsafe {
            kafka_consumer_ConsumerRecord_new(
                c"topic".as_ptr(),
                0,
                0,
                &key as *const kafka_Bytes_t as *const c_void,
                &value as *const kafka_Bytes_t as *const c_void,
            )
        };
        assert!(unsafe { kafka_consumer_MockConsumer_add_record(mock, record) }.is_null());
        unsafe { kafka_consumer_ConsumerRecord_destroy(record) };

        let mut records = ptr::null_mut();
        assert!(unsafe { kafka_consumer_Consumer_poll(consumer, 0, &mut records) }.is_null());
        assert_eq!(unsafe { kafka_consumer_ConsumerRecords_count(records) }, 1);
        let list = unsafe { kafka_consumer_ConsumerRecords_records_with_partition(records, tp) };
        let first = unsafe { kafka_List_get(list, 0) } as *const kafka_consumer_ConsumerRecord_t;
        assert_eq!(
            unsafe { kafka_consumer_ConsumerRecord_key(first) },
            &key as *const kafka_Bytes_t as *mut c_void
        );
        assert_eq!(
            unsafe { kafka_consumer_ConsumerRecord_value(first) },
            &value as *const kafka_Bytes_t as *mut c_void
        );
        assert_eq!(unsafe { kafka_consumer_MockConsumer_last_poll_timeout(mock) }, 0);

        unsafe {
            kafka_List_destroy(list);
            kafka_consumer_ConsumerRecords_destroy(records);
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    #[test]
    fn a_call_while_an_operation_is_in_flight_is_the_java_concurrent_modification() {
        let (mock, consumer) = new_mock();
        let probe = ListenerProbe::new(false);
        unsafe { subscribe(consumer, &probe) };

        let completed = AtomicI32::new(0);
        let (assignment, tp) = partition_list(c"topic", 0);
        unsafe {
            kafka_consumer_MockConsumer_rebalance_cb(
                mock,
                assignment,
                count_completion,
                &completed as *const AtomicI32 as *mut c_void,
            )
        };
        // In flight until the listener invocation is pumped and reported.
        let error = unsafe { take_error(kafka_consumer_Consumer_commit_sync(consumer)) }.expect("rejected");
        assert!(error.is_local_concurrent_modification_error());
        assert_eq!(error.message(), "KafkaConsumer is not safe for multi-threaded access.");
        assert_eq!(
            unsafe { kafka_consumer_MockConsumer_closed(mock) },
            -1,
            "getters answer -1 meanwhile"
        );

        wait_until("the listener invocation to run", || {
            unsafe { kafka_consumer_Consumer_execute_callbacks(consumer) };
            probe.pending_id.load(SeqCst) != 0
        });
        let id = probe.pending_id.load(SeqCst);
        unsafe { kafka_consumer_Consumer_set_callback_result(ptr::null(), id, ptr::null_mut()) };
        wait_until("the completion to run", || {
            unsafe { kafka_consumer_Consumer_execute_callbacks(consumer) };
            completed.load(SeqCst) == 1
        });
        assert!(unsafe { kafka_consumer_Consumer_commit_sync(consumer) }.is_null(), "free again");

        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }
}
