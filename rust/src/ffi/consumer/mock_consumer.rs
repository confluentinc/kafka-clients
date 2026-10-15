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
use crate::consumer::{ConsumerHandle, MockConsumer};
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::common::partition_info::list_partition_infos;
use crate::ffi::common::topic_partition::{list_topic_partitions, map_topic_partition_i64};
use crate::ffi::common::{catch_task_panic, deliver_panic, kafka_common_Error_t, take_error};
use crate::ffi::common::{deliver_unit, deliver_value};
use crate::ffi::consumer::consumer_record::{consumer_record_ref, kafka_consumer_ConsumerRecord_t};
use crate::ffi::consumer::{
    BusyGuard, ConsumerClassHandle, ConsumerKind, Owns, client_ref, completion_sink, deliver_void,
    kafka_consumer_Consumer_t,
};
use crate::ffi::ffi_guard;
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, kept alive for this
    // call; the `kafka_consumer_MockConsumer_t` pointer is the `ConsumerClassHandle` pointer, the
    // same one its `__as_Consumer` view is (`ConsumerClassHandle::as_consumer`), as `client_ref`
    // requires. Exclusive access to the consumer comes from `acquire`, not from this reference.
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock`
    // requires.
    if let Err(error) = unsafe { with_mock(self_, f) } {
        warn!("MockConsumer.{name} ignored: {error}");
    }
}

/// `new MockConsumer(String offsetResetStrategy)`: returns the owned class
/// handle, or stores the owned error (an unknown strategy) in `*out_err` and
/// returns `NULL`.
///
/// # Safety
///
/// `offset_reset_strategy` must be a valid string and `out_err` a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_new(
    offset_reset_strategy: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_MockConsumer_t {
    // SAFETY: Per this function's `# Safety`, `offset_reset_strategy` is a valid NUL-terminated
    // string, as `c_str_to_string` requires; it is copied, so the borrow lasts for this call only.
    let strategy = unsafe { c_str_to_string(offset_reset_strategy) };
    // No deserializers: the `void *`s are whatever `add_record` was given.
    let result = ConsumerClassHandle::new(Owns { key: false, value: false }, || {
        Mock::new(&strategy).map(|mock| ConsumerKind::Mock(Box::new(mock)))
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the result to,
    // as `deliver_value` requires.
    unsafe {
        deliver_value(result, out_err, |handle| {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer__as_Consumer(
    self_: *mut kafka_consumer_MockConsumer_t,
) -> *mut kafka_consumer_Consumer_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, i.e. a
    // `Box<ConsumerClassHandle>` from `kafka_consumer_MockConsumer_new` not yet destroyed, alive
    // for this expression, the only use of the reference; `as_consumer` only takes its address.
    unsafe { &*(self_ as *const ConsumerClassHandle) }.as_consumer()
}

/// Frees the handle as `kafka_consumer_Consumer_destroy` does; null is a
/// no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle from
/// [`kafka_consumer_MockConsumer_new`] not yet destroyed, that no other call
/// uses at the same time; the callbacks this call runs may use its
/// `__as_Consumer` view only to report through
/// `kafka_consumer_Consumer__set_callback_result`, and neither is used once
/// this call returned.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_destroy(self_: *mut kafka_consumer_MockConsumer_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of the `Box<ConsumerClassHandle>`
        // `kafka_consumer_MockConsumer_new` delivered, that no other call uses at the same time
        // except the reports of the callbacks this call runs, as `ConsumerClassHandle::destroy`
        // requires; it frees the handle exactly once, after awaiting the tasks still holding the
        // client.
        unsafe { ConsumerClassHandle::destroy(self_ as *mut ConsumerClassHandle) };
    }
}

/// `addRecord(ConsumerRecord<K, V> record)`: the record is copied (its
/// `void *` key and value are shared with the caller, who keeps them alive
/// until the records holding them are destroyed).
///
/// # Safety
///
/// `self_` must be a live handle and `record` a live record. `out_err` must be
/// a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_add_record(
    self_: *mut kafka_consumer_MockConsumer_t,
    record: *const kafka_consumer_ConsumerRecord_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `record` is a live record handle, alive for this
    // expression, the only use of the reference, as `consumer_record_ref` requires. The clone
    // copies the `void *`s without owning them: the mock was built with
    // `Owns { key: false, value: false }`.
    let record = unsafe { consumer_record_ref(record) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock`
    // requires.
    let result = unsafe { with_mock(self_, |mock| mock.add_record(record)) }.and_then(|r| r);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `updateBeginningOffsets(Map<TopicPartition, Long>)`: a map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`, copied.
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` a valid map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_beginning_offsets(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_offsets: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `new_offsets` is a valid map of topic-partition
    // handles to `int64_t *`, as `map_topic_partition_i64` requires; the entries are copied, so the
    // borrow lasts for this call only.
    let offsets = unsafe { map_topic_partition_i64(new_offsets) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock_void`
    // requires.
    unsafe { with_mock_void(self_, "updateBeginningOffsets", |mock| mock.update_beginning_offsets(offsets)) }
}

/// `updateEndOffsets(Map<TopicPartition, Long>)`.
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` a valid map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_end_offsets(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_offsets: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `new_offsets` is a valid map of topic-partition
    // handles to `int64_t *`, as `map_topic_partition_i64` requires; the entries are copied, so the
    // borrow lasts for this call only.
    let offsets = unsafe { map_topic_partition_i64(new_offsets) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock_void`
    // requires.
    unsafe { with_mock_void(self_, "updateEndOffsets", |mock| mock.update_end_offsets(offsets)) }
}

/// `updateDurationOffsets(Map<TopicPartition, Long>)`.
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` a valid map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_duration_offsets(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_offsets: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `new_offsets` is a valid map of topic-partition
    // handles to `int64_t *`, as `map_topic_partition_i64` requires; the entries are copied, so the
    // borrow lasts for this call only.
    let offsets = unsafe { map_topic_partition_i64(new_offsets) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock_void`
    // requires.
    unsafe { with_mock_void(self_, "updateDurationOffsets", |mock| mock.update_duration_offsets(offsets)) }
}

/// `updatePartitions(String topic, List<PartitionInfo> partitions)`: a list
/// of `kafka_common_PartitionInfo_t *`, copied.
///
/// # Safety
///
/// `self_` must be a live handle, `topic` a valid string and `partitions` a
/// valid list of `kafka_common_PartitionInfo_t *`. `out_err` must be a valid
/// slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_update_partitions(
    self_: *mut kafka_consumer_MockConsumer_t,
    topic: *const c_char,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid list of
    // `kafka_common_PartitionInfo_t *`, as `list_partition_infos` requires; the values are copied,
    // so the borrow lasts for this call only.
    let partitions = unsafe { list_partition_infos(partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock`
    // requires.
    let result = unsafe { with_mock(self_, |mock| mock.update_partitions(&topic, partitions)) }.and_then(|r| r);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `setPollException(KafkaException)`: the next `poll` fails with `error`,
/// which Rust takes over.
///
/// # Safety
///
/// `self_` must be a live handle and `error` an owned error not used
/// afterwards, or NULL to clear the pending error as Java's `null` does.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_poll_error(
    self_: *mut kafka_consumer_MockConsumer_t,
    error: *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error not used
    // afterwards, as `take_error` requires.
    let error = unsafe { take_error(error) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock_void`
    // requires.
    unsafe { with_mock_void(self_, "setPollException", |mock| mock.set_poll_error(error)) }
}

/// `setOffsetsException(KafkaException)`: the next offsets query fails with
/// `error`, which Rust takes over.
///
/// # Safety
///
/// `self_` must be a live handle and `error` an owned error not used
/// afterwards, or NULL to clear the pending error as Java's `null` does.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_offsets_error(
    self_: *mut kafka_consumer_MockConsumer_t,
    error: *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error not used
    // afterwards, as `take_error` requires.
    let error = unsafe { take_error(error) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock_void`
    // requires.
    unsafe { with_mock_void(self_, "setOffsetsException", |mock| mock.set_offsets_error(error)) }
}

/// `setMaxPollRecords(long maxPollRecords)`.
///
/// # Safety
///
/// `self_` must be a live handle. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_set_max_poll_records(
    self_: *mut kafka_consumer_MockConsumer_t,
    max_poll_records: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock`
    // requires.
    let result = unsafe { with_mock(self_, |mock| mock.set_max_poll_records(max_poll_records)) }.and_then(|r| r);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `rebalance(Collection<TopicPartition> newAssignment)`: blocking, the
/// registered listener's callbacks run on the calling thread
/// (CLAUDE.md §4 rule 5).
///
/// # Safety
///
/// `self_` must be a live handle and `new_assignment` null or a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_rebalance(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_assignment: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `new_assignment` is null or a valid list of
    // topic-partition handles, as `list_topic_partitions` requires; the values are cloned, so the
    // borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(new_assignment) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, kept alive for this
    // call; the `kafka_consumer_MockConsumer_t` pointer is the `ConsumerClassHandle` pointer, the
    // same one its `__as_Consumer` view is (`ConsumerClassHandle::as_consumer`), as `client_ref`
    // requires. Exclusive access to the consumer comes from `acquire`, not from this reference.
    let client = unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) };
    let result = client.acquire().and_then(|mut guard| {
        let mock = mock_of(&mut guard)?;
        client.block_on(mock.rebalance(&partitions))
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// The completion of [`kafka_consumer_MockConsumer_rebalance_cb`].
pub type kafka_consumer_MockConsumer_rebalance_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `rebalance`: the listener's callbacks and `cb` are queued
/// for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, whose pointer is its
    // `__as_Consumer` view's, as `completion_sink` requires. The guard runs this only when the body
    // did not return. Reading `new_assignment` comes first and hands nothing over; the body hands
    // `cb` over only as its last step: to `deliver_void` when `acquire` fails, or to the task
    // `Client::spawn` registers, after which nothing can panic. So `cb` has not fired for this
    // `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_ as *const kafka_consumer_Consumer_t), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_rebalance_cb(
    self_: *mut kafka_consumer_MockConsumer_t,
    new_assignment: *const kafka_List_t,
    cb: kafka_consumer_MockConsumer_rebalance_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `new_assignment` is null or a valid list of
    // topic-partition handles, as `list_topic_partitions` requires; the values are cloned, so the
    // borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(new_assignment) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, kept alive for this
    // call; the `kafka_consumer_MockConsumer_t` pointer is the `ConsumerClassHandle` pointer, the
    // same one its `__as_Consumer` view is (`ConsumerClassHandle::as_consumer`), as `client_ref`
    // requires. The spawned task holds its own `Arc` clones, so it does not depend on the handle
    // staying alive.
    let client = unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) };
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    match client.acquire() {
        Err(error) => deliver_void(client, cb, opaque, Err(error)),
        Ok(mut guard) => {
            let client = Arc::clone(client);
            let task_client = Arc::clone(&client);
            client.spawn(async move {
                let result = catch_task_panic("kafka_consumer_MockConsumer_rebalance_cb", async {
                    match mock_of(&mut guard) {
                        Ok(mock) => mock.rebalance(&partitions).await,
                        Err(error) => Err(error),
                    }
                })
                .await;
                // Release the flag before queuing, as `run_cb` does.
                drop(guard);
                deliver_void(&task_client, cb, opaque, result);
            });
        },
    }
}

/// `losePartitions(Collection<TopicPartition> partitionsLost)`
/// (KAFKA-20575): blocking, the registered listener's `on_partitions_lost`
/// runs on the calling thread (CLAUDE.md §4 rule 5). Fails with Java's
/// `IllegalStateException` when a partition is not currently assigned,
/// before any state changes.
///
/// # Safety
///
/// `self_` must be a live handle and `partitions_lost` null or a valid list of
/// `kafka_common_TopicPartition_t *`. `out_err` must be a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_lose_partitions(
    self_: *mut kafka_consumer_MockConsumer_t,
    partitions_lost: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `partitions_lost` is null or a valid list of
    // topic-partition handles, as `list_topic_partitions` requires; the values are cloned, so the
    // borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions_lost) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, kept alive for this
    // call; the `kafka_consumer_MockConsumer_t` pointer is the `ConsumerClassHandle` pointer, the
    // same one its `__as_Consumer` view is (`ConsumerClassHandle::as_consumer`), as `client_ref`
    // requires. Exclusive access to the consumer comes from `acquire`, not from this reference.
    let client = unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) };
    let result = client.acquire().and_then(|mut guard| {
        let mock = mock_of(&mut guard)?;
        client.block_on(mock.lose_partitions(&partitions))
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// The completion of [`kafka_consumer_MockConsumer_lose_partitions_cb`].
pub type kafka_consumer_MockConsumer_lose_partitions_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking `lose_partitions`: the listener's callbacks and `cb` are queued
/// for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, whose pointer is its
    // `__as_Consumer` view's, as `completion_sink` requires. The guard runs this only when the body
    // did not return. Reading `partitions_lost` comes first and hands nothing over; the body hands
    // `cb` over only as its last step: to `deliver_void` when `acquire` fails, or to the task
    // `Client::spawn` registers, after which nothing can panic. So `cb` has not fired for this
    // `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_ as *const kafka_consumer_Consumer_t), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_lose_partitions_cb(
    self_: *mut kafka_consumer_MockConsumer_t,
    partitions_lost: *const kafka_List_t,
    cb: kafka_consumer_MockConsumer_lose_partitions_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `partitions_lost` is null or a valid list of
    // topic-partition handles, as `list_topic_partitions` requires; the values are cloned, so the
    // borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions_lost) };
    let opaque = SendPtr(opaque);
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, kept alive for this
    // call; the `kafka_consumer_MockConsumer_t` pointer is the `ConsumerClassHandle` pointer, the
    // same one its `__as_Consumer` view is (`ConsumerClassHandle::as_consumer`), as `client_ref`
    // requires. The spawned task holds its own `Arc` clones, so it does not depend on the handle
    // staying alive.
    let client = unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) };
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    match client.acquire() {
        Err(error) => deliver_void(client, cb, opaque, Err(error)),
        Ok(mut guard) => {
            let client = Arc::clone(client);
            let task_client = Arc::clone(&client);
            client.spawn(async move {
                let result = catch_task_panic("kafka_consumer_MockConsumer_lose_partitions_cb", async {
                    match mock_of(&mut guard) {
                        Ok(mock) => mock.lose_partitions(&partitions).await,
                        Err(error) => Err(error),
                    }
                })
                .await;
                // Release the flag before queuing, as `run_cb` does.
                drop(guard);
                deliver_void(&task_client, cb, opaque, result);
            });
        },
    }
}

/// The mock's reentrancy handle, which reads `closed`, `shouldRebalance`
/// and `lastPollTimeout` without the single-owner flag.
///
/// # Safety
///
/// `self_` must be a live mock handle, kept alive for `'a`.
unsafe fn mock_handle<'a>(self_: *const kafka_consumer_MockConsumer_t) -> &'a ConsumerHandle {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, kept alive for `'a`;
    // the `kafka_consumer_MockConsumer_t` pointer is the `ConsumerClassHandle` pointer, the same
    // one its `__as_Consumer` view is (`ConsumerClassHandle::as_consumer`), as `client_ref`
    // requires. The handle is shared state, never the mock an operation may be borrowing.
    unsafe { client_ref(self_ as *const kafka_consumer_Consumer_t) }.handle()
}

/// `closed()`: `1` once `close` ran, else `0`. Read through the mock's
/// handle, not the single-owner flag, so it answers while another call
/// holds the mock: Java's `MockConsumer.closed()` waits on the monitor and
/// never throws.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_closed(self_: *const kafka_consumer_MockConsumer_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, alive for this
    // expression, as `mock_handle` requires.
    let handle = unsafe { mock_handle(self_) };
    // A `kafka_consumer_MockConsumer_t` always wraps a mock, so the `None` arm is unreachable.
    handle.mock_closed().map_or(-1, i8::from)
}

/// `shouldRebalance()`: `1` while an `enforceRebalance` is pending, else
/// `0`. Read through the mock's handle, not the single-owner flag, so it
/// answers while another call holds the mock, as Java's unsynchronized
/// `MockConsumer.shouldRebalance()` does.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_should_rebalance(
    self_: *const kafka_consumer_MockConsumer_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, alive for this
    // expression, as `mock_handle` requires.
    let handle = unsafe { mock_handle(self_) };
    // A `kafka_consumer_MockConsumer_t` always wraps a mock, so the `None` arm is unreachable.
    handle.mock_should_rebalance().map_or(-1, i8::from)
}

/// `resetShouldRebalance()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_reset_should_rebalance(self_: *mut kafka_consumer_MockConsumer_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock_void`
    // requires.
    unsafe { with_mock_void(self_, "resetShouldRebalance", Mock::reset_should_rebalance) }
}

/// `scheduleNopPollTask()`: queues a task that does nothing, consuming the
/// next `poll`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_schedule_nop_poll_task(self_: *mut kafka_consumer_MockConsumer_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, as `with_mock_void`
    // requires.
    unsafe { with_mock_void(self_, "scheduleNopPollTask", Mock::schedule_nop_poll_task) }
}

/// `lastPollTimeout()`: the last `poll` timeout in milliseconds, `-1` when
/// `poll` never ran (Java's `null`). Read through the mock's handle, not the
/// single-owner flag, so it answers while another call holds the mock, as
/// Java's unsynchronized `MockConsumer.lastPollTimeout()` does.
///
/// A caught Rust panic returns -2, which is no poll timeout: `Consumer.poll` rejects a negative one.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_MockConsumer_last_poll_timeout(
    self_: *const kafka_consumer_MockConsumer_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live mock handle, alive for this
    // expression, as `mock_handle` requires.
    let handle = unsafe { mock_handle(self_) };
    // A `kafka_consumer_MockConsumer_t` always wraps a mock, so the outer `None` is unreachable.
    handle
        .mock_last_poll_timeout()
        .flatten()
        .map_or(-1, |timeout| i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicI32, AtomicI64, AtomicPtr};
    use std::thread::{self, ThreadId};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::ffi::common::fail_point;
    use crate::ffi::common::panic_probe::{Probe, on_error, on_i64, on_value};
    use crate::ffi::common::take_error;
    use crate::ffi::common::topic_partition::{
        kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_new, kafka_common_TopicPartition_t,
    };
    use crate::ffi::consumer::consumer_handle::{
        kafka_consumer_ConsumerHandle_destroy, kafka_consumer_ConsumerHandle_pause,
        kafka_consumer_ConsumerHandle_pause_cb, kafka_consumer_ConsumerHandle_position_cb,
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
        kafka_consumer_Consumer__execute_callbacks, kafka_consumer_Consumer__set_callback_result,
        kafka_consumer_Consumer__set_callbacks_notify, kafka_consumer_Consumer_assign,
        kafka_consumer_Consumer_assignment, kafka_consumer_Consumer_commit_sync,
        kafka_consumer_Consumer_commit_sync_cb, kafka_consumer_Consumer_current_lag,
        kafka_consumer_Consumer_group_metadata, kafka_consumer_Consumer_handle, kafka_consumer_Consumer_metrics,
        kafka_consumer_Consumer_paused, kafka_consumer_Consumer_poll, kafka_consumer_Consumer_poll_cb,
        kafka_consumer_Consumer_position_cb, kafka_consumer_Consumer_subscribe_with_topics_listener,
        kafka_consumer_Consumer_subscription,
    };
    use crate::ffi::util::{
        kafka_Bytes_t, kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size,
        kafka_Map_destroy, kafka_Map_new, kafka_Map_put,
    };

    /// Calls `f` with an error slot pointing at null, as `out_err` requires,
    /// and returns its value with the error it stored there, taken.
    fn call<T>(f: impl FnOnce(*mut *mut kafka_common_Error_t) -> T) -> (T, Option<crate::common::Error>) {
        let mut err = ptr::null_mut();
        let value = f(&mut err);
        // SAFETY: Test code: `err` is null or the owned error `f` stored there, taken once.
        (value, unsafe { take_error(err) })
    }

    /// [`call`] for a call that must succeed: returns its value.
    fn ok<T>(f: impl FnOnce(*mut *mut kafka_common_Error_t) -> T) -> T {
        let (value, error) = call(f);
        assert!(error.is_none(), "{error:?}");
        value
    }

    /// A mock over `void *` records and its borrowed `Consumer` view.
    fn new_mock() -> (*mut kafka_consumer_MockConsumer_t, *mut kafka_consumer_Consumer_t) {
        // SAFETY: Test code: a NUL-terminated literal and the valid error slot `ok` passes.
        let mock = ok(|e| unsafe { kafka_consumer_MockConsumer_new(c"earliest".as_ptr(), e) });
        // SAFETY: Test code: `mock` is the live handle just created.
        (mock, unsafe { kafka_consumer_MockConsumer__as_Consumer(mock) })
    }

    /// A C-built list holding one topic-partition; the caller destroys both.
    fn partition_list(topic: &CStr, partition: i32) -> (*mut kafka_List_t, *mut kafka_common_TopicPartition_t) {
        // SAFETY: Test code: a NUL-terminated literal.
        let tp = unsafe { kafka_common_TopicPartition_new(topic.as_ptr(), partition) };
        let list = kafka_List_new();
        // SAFETY: Test code: `list` and `tp` are the live handles just created.
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
    /// `__set_callback_result` or leaves the `callback_id` for the test.
    struct ListenerProbe {
        report: bool,
        thread: Mutex<Option<ThreadId>>,
        assigned: AtomicI32,
        revoked: AtomicI32,
        pending_id: AtomicI64,
        /// The consumer `subscribe` registered the listener with, which the
        /// reports name.
        consumer: AtomicPtr<kafka_consumer_Consumer_t>,
    }

    impl ListenerProbe {
        fn new(report: bool) -> Self {
            Self {
                report,
                thread: Mutex::new(None),
                assigned: AtomicI32::new(0),
                revoked: AtomicI32::new(0),
                pending_id: AtomicI64::new(0),
                consumer: AtomicPtr::new(ptr::null_mut()),
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
            // SAFETY: Test code: `partitions` is the live list Rust passed to the listener method.
            counter.fetch_add(unsafe { kafka_List_size(partitions) }, SeqCst);
            if self.report {
                // SAFETY: Test code: the consumer is the live handle that invoked this method, and the
                // null error is a success report, with a null value since the method is void.
                unsafe {
                    kafka_consumer_Consumer__set_callback_result(
                        self.consumer.load(SeqCst),
                        callback_id,
                        ptr::null_mut(),
                        ptr::null_mut(),
                    )
                };
            } else {
                self.pending_id.store(callback_id, SeqCst);
            }
        }
    }

    unsafe extern "C" fn probe_revoked(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
        // SAFETY: Test code: `self_` is the `ListenerProbe` the test registered, alive until after
        // the consumer is destroyed.
        let probe = unsafe { &*(self_ as *const ListenerProbe) };
        // SAFETY: Test code: `partitions` is the live list Rust passed in.
        unsafe { probe.saw(&probe.revoked, partitions, callback_id) };
    }

    unsafe extern "C" fn probe_assigned(self_: *mut c_void, partitions: *const kafka_List_t, callback_id: i64) {
        // SAFETY: Test code: `self_` is the `ListenerProbe` the test registered, alive until after
        // the consumer is destroyed.
        let probe = unsafe { &*(self_ as *const ListenerProbe) };
        // SAFETY: Test code: `partitions` is the live list Rust passed in.
        unsafe { probe.saw(&probe.assigned, partitions, callback_id) };
    }

    /// Subscribes the consumer to `topic` with a listener backed by `probe`;
    /// the listener handle is destroyed at once, its registration having
    /// been copied.
    unsafe fn subscribe(consumer: *mut kafka_consumer_Consumer_t, probe: &ListenerProbe) {
        probe.consumer.store(consumer, SeqCst);
        let listener =
            kafka_consumer_ConsumerRebalanceListener_new(probe.as_self(), probe_revoked, probe_assigned, None);
        let topics = kafka_List_new();
        // SAFETY: Test code: `topics` is a live list and the element a static NUL-terminated
        // literal.
        unsafe { kafka_List_add(topics, c"topic".as_ptr() as *mut c_void) };
        // SAFETY: Test code: `consumer`, `topics` and `listener` are live handles and `e` the valid
        // error slot `ok` passes.
        ok(|e| unsafe { kafka_consumer_Consumer_subscribe_with_topics_listener(consumer, topics, listener, e) });
        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
        unsafe {
            kafka_List_destroy(topics);
            kafka_consumer_ConsumerRebalanceListener_destroy(listener);
        }
    }

    unsafe extern "C" fn count_notify(opaque: *mut c_void) {
        // SAFETY: Test code: `opaque` is the test's `AtomicI32`, alive until after the consumer is
        // destroyed.
        unsafe { &*(opaque as *const AtomicI32) }.fetch_add(1, SeqCst);
    }

    unsafe extern "C" fn count_completion(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        // SAFETY: Test code: `error` is null or an owned error, taken once.
        assert!(error.is_null(), "{:?}", unsafe { take_error(error) });
        // SAFETY: Test code: `opaque` is the test's `AtomicI32`, alive until after the completion
        // fires.
        unsafe { &*(opaque as *const AtomicI32) }.fetch_add(1, SeqCst);
    }

    #[test]
    fn blocking_rebalance_invokes_the_listener_on_the_calling_thread() {
        let (mock, consumer) = new_mock();
        let probe = ListenerProbe::new(true);
        // SAFETY: Test code: `consumer` is live and `probe` outlives the consumer.
        unsafe { subscribe(consumer, &probe) };

        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `e` the valid error slot
        // `ok` passes.
        ok(|e| unsafe { kafka_consumer_MockConsumer_rebalance(mock, assignment, e) });
        assert_eq!(probe.assigned.load(SeqCst), 1);
        assert!(
            probe.ran_on_this_thread(),
            "a blocking entry point invokes the listener directly"
        );

        // Rebalancing the partition away revokes it, on this thread too.
        let empty = kafka_List_new();
        // SAFETY: Test code: `mock` and `empty` are live handles and `e` the valid error slot `ok`
        // passes.
        ok(|e| unsafe { kafka_consumer_MockConsumer_rebalance(mock, empty, e) });
        assert_eq!(probe.revoked.load(SeqCst), 1);
        assert!(probe.ran_on_this_thread());

        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
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
        // SAFETY: Test code: `consumer` is live and `notifies` outlives the consumer.
        unsafe {
            kafka_consumer_Consumer__set_callbacks_notify(
                consumer,
                count_notify,
                &notifies as *const AtomicI32 as *mut c_void,
            )
        };
        let probe = ListenerProbe::new(false);
        // SAFETY: Test code: `consumer` is live and `probe` outlives the consumer.
        unsafe { subscribe(consumer, &probe) };

        let completed = AtomicI32::new(0);
        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `completed` outlives the
        // completion.
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

        // SAFETY: Test code: `consumer` is a live handle.
        assert_eq!(unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) }, 1);
        assert_eq!(probe.assigned.load(SeqCst), 1);
        assert!(probe.ran_on_this_thread(), "the pump runs it on the pumping thread");
        assert_eq!(completed.load(SeqCst), 0, "the operation waits for the report");

        // Reported later, from another thread.
        let id = probe.pending_id.load(SeqCst);
        let reporter = SendPtr(consumer as *mut c_void);
        // SAFETY: Test code: the consumer is the live handle that issued `id`, alive until the
        // thread is joined, and the null error is a success report, with a null value since the
        // method is void.
        thread::spawn(move || unsafe {
            kafka_consumer_Consumer__set_callback_result(
                reporter.get() as *const kafka_consumer_Consumer_t,
                id,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        })
        .join()
        .unwrap();
        wait_until("the completion to be queued", || notifies.load(SeqCst) >= 2);
        // SAFETY: Test code: `consumer` is a live handle.
        assert_eq!(unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) }, 1);
        assert_eq!(completed.load(SeqCst), 1);
        // SAFETY: Test code: `consumer` is a live handle.
        assert_eq!(unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) }, 0);

        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    /// `MockConsumer.losePartitions` (KAFKA-20575) through C: the listener's
    /// `on_partitions_lost` (here the Java default, forwarding to
    /// `on_partitions_revoked`) runs on the calling thread, and losing a
    /// partition that is not assigned fails with Java's message.
    #[test]
    fn blocking_lose_partitions_invokes_the_listener_on_the_calling_thread() {
        let (mock, consumer) = new_mock();
        let probe = ListenerProbe::new(true);
        // SAFETY: Test code: `consumer` is live and `probe` outlives the consumer.
        unsafe { subscribe(consumer, &probe) };

        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `e` the valid error slot
        // `ok` passes.
        ok(|e| unsafe { kafka_consumer_MockConsumer_rebalance(mock, assignment, e) });
        assert_eq!(probe.revoked.load(SeqCst), 0);

        // SAFETY: Test code: `mock` and `assignment` are live handles and `e` the valid error slot
        // `ok` passes.
        ok(|e| unsafe { kafka_consumer_MockConsumer_lose_partitions(mock, assignment, e) });
        assert_eq!(probe.revoked.load(SeqCst), 1, "the default on_partitions_lost revokes");
        assert!(
            probe.ran_on_this_thread(),
            "a blocking entry point invokes the listener directly"
        );

        // The partition is no longer assigned, so losing it again fails, before the listener.
        // SAFETY: Test code: `mock` and `assignment` are live handles and `e` the valid error slot
        // `call` passes.
        let error = call(|e| unsafe { kafka_consumer_MockConsumer_lose_partitions(mock, assignment, e) })
            .1
            .expect("not assigned");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!(
            error.message(),
            "Cannot lose partitions that are not currently assigned: [topic-0]"
        );
        assert_eq!(probe.revoked.load(SeqCst), 1);

        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    unsafe extern "C" fn store_completion(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        // SAFETY: Test code: `opaque` is the test's slot, alive until after the completion fires;
        // `error` is null or an owned error, taken once.
        unsafe { *(*(opaque as *const Mutex<Option<Option<Error>>>)).lock().unwrap() = Some(take_error(error)) };
    }

    #[test]
    fn lose_partitions_cb_queues_the_listener_and_its_completion_for_the_pump() {
        let (mock, consumer) = new_mock();
        let notifies = AtomicI32::new(0);
        // SAFETY: Test code: `consumer` is live and `notifies` outlives the consumer.
        unsafe {
            kafka_consumer_Consumer__set_callbacks_notify(
                consumer,
                count_notify,
                &notifies as *const AtomicI32 as *mut c_void,
            )
        };
        let probe = ListenerProbe::new(true);
        // SAFETY: Test code: `consumer` is live and `probe` outlives the consumer.
        unsafe { subscribe(consumer, &probe) };
        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `e` the valid error slot
        // `ok` passes.
        ok(|e| unsafe { kafka_consumer_MockConsumer_rebalance(mock, assignment, e) });

        let completed = AtomicI32::new(0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `completed` outlives the
        // completion.
        unsafe {
            kafka_consumer_MockConsumer_lose_partitions_cb(
                mock,
                assignment,
                count_completion,
                &completed as *const AtomicI32 as *mut c_void,
            )
        };
        wait_until("the listener invocation to be queued", || notifies.load(SeqCst) >= 1);
        assert_eq!(probe.revoked.load(SeqCst), 0, "queued, not run, until the pump runs it");
        // SAFETY: Test code: `consumer` is a live handle.
        assert_eq!(unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) }, 1);
        assert_eq!(probe.revoked.load(SeqCst), 1);
        assert!(probe.ran_on_this_thread(), "the pump runs it on the pumping thread");
        wait_until("the completion to be queued", || notifies.load(SeqCst) >= 2);
        // SAFETY: Test code: `consumer` is a live handle.
        assert_eq!(unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) }, 1);
        assert_eq!(completed.load(SeqCst), 1);

        // Losing it again fails through the completion, with no listener call.
        let failed: Mutex<Option<Option<Error>>> = Mutex::new(None);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `failed` outlives the
        // completion.
        unsafe {
            kafka_consumer_MockConsumer_lose_partitions_cb(
                mock,
                assignment,
                store_completion,
                &failed as *const Mutex<Option<Option<Error>>> as *mut c_void,
            )
        };
        wait_until("the failed completion to be queued", || notifies.load(SeqCst) >= 3);
        // SAFETY: Test code: `consumer` is a live handle.
        assert_eq!(unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) }, 1);
        let error = failed.lock().unwrap().take().expect("fired").expect("an error");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!(
            error.message(),
            "Cannot lose partitions that are not currently assigned: [topic-0]"
        );
        assert_eq!(probe.revoked.load(SeqCst), 1);

        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
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
        // SAFETY: Test code: `consumer` is live and `probe` outlives the consumer.
        unsafe { subscribe(consumer, &probe) };

        let completed = AtomicI32::new(0);
        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `completed` outlives the
        // completion.
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
        // SAFETY: Test code: `mock` is live, destroyed once, and not used afterwards.
        unsafe { kafka_consumer_MockConsumer_destroy(mock) };
        assert_eq!(probe.assigned.load(SeqCst), 1);
        assert!(probe.ran_on_this_thread(), "fired on the destroying thread");
        assert_eq!(completed.load(SeqCst), 1);

        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
        }
    }

    #[test]
    fn poll_hands_back_the_record_pointers_add_record_was_given() {
        let (mock, consumer) = new_mock();
        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `consumer` and `assignment` are live handles and `e` the valid error
        // slot `ok` passes.
        ok(|e| unsafe { kafka_consumer_Consumer_assign(consumer, assignment, e) });

        let mut zero = 0i64;
        let offsets = kafka_Map_new();
        // SAFETY: Test code: `offsets`, `tp` and `mock` are live handles, `zero` outlives the call,
        // and `offsets` is destroyed once.
        unsafe {
            kafka_Map_put(offsets, tp as *mut c_void, &mut zero as *mut i64 as *mut c_void);
            kafka_consumer_MockConsumer_update_beginning_offsets(mock, offsets);
            kafka_Map_destroy(offsets);
        }

        // With no deserializer the `void *`s are `kafka_Bytes_t *` the
        // caller owns; the mock hands the very same pointers back.
        let key = kafka_Bytes_t { data: b"k".as_ptr(), len: 1 };
        let value = kafka_Bytes_t { data: b"value".as_ptr(), len: 5 };
        // SAFETY: Test code: the literal is NUL-terminated, and `key` and `value` outlive every
        // record holding them.
        let record = unsafe {
            kafka_consumer_ConsumerRecord_new(
                c"topic".as_ptr(),
                0,
                0,
                &key as *const kafka_Bytes_t as *const c_void,
                &value as *const kafka_Bytes_t as *const c_void,
            )
        };
        // SAFETY: Test code: `mock` and `record` are live handles and `e` the valid error slot `ok`
        // passes.
        ok(|e| unsafe { kafka_consumer_MockConsumer_add_record(mock, record, e) });
        // SAFETY: Test code: `record` is live and destroyed once.
        unsafe { kafka_consumer_ConsumerRecord_destroy(record) };

        // SAFETY: Test code: `consumer` is live and `e` the valid error slot `ok` passes.
        let records = ok(|e| unsafe { kafka_consumer_Consumer_poll(consumer, 0, e) });
        // SAFETY: Test code: `records` is a live handle.
        assert_eq!(unsafe { kafka_consumer_ConsumerRecords_count(records) }, 1);
        // SAFETY: Test code: `records` and `tp` are live handles.
        let list = unsafe { kafka_consumer_ConsumerRecords_records_with_partition(records, tp) };
        // SAFETY: Test code: `list` is a live list with one element.
        let first = unsafe { kafka_List_get(list, 0) } as *const kafka_consumer_ConsumerRecord_t;
        assert_eq!(
            // SAFETY: Test code: `first` is a live record borrowed from `records`.
            unsafe { kafka_consumer_ConsumerRecord_key(first) },
            &key as *const kafka_Bytes_t as *mut c_void
        );
        assert_eq!(
            // SAFETY: Test code: `first` is a live record borrowed from `records`.
            unsafe { kafka_consumer_ConsumerRecord_value(first) },
            &value as *const kafka_Bytes_t as *mut c_void
        );
        // SAFETY: Test code: `mock` is a live handle.
        assert_eq!(unsafe { kafka_consumer_MockConsumer_last_poll_timeout(mock) }, 0);

        // SAFETY: Test code: each handle is live and destroyed once, after its last use; `list`
        // before the `records` it borrows from.
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
        // SAFETY: Test code: `consumer` is live and `probe` outlives the consumer.
        unsafe { subscribe(consumer, &probe) };

        let completed = AtomicI32::new(0);
        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `completed` outlives the
        // completion.
        unsafe {
            kafka_consumer_MockConsumer_rebalance_cb(
                mock,
                assignment,
                count_completion,
                &completed as *const AtomicI32 as *mut c_void,
            )
        };
        // In flight until the listener invocation is pumped and reported.
        // SAFETY: Test code: `consumer` is a live handle and `e` the valid error slot `call` passes.
        let error = call(|e| unsafe { kafka_consumer_Consumer_commit_sync(consumer, e) })
            .1
            .expect("rejected");
        assert!(error.is_local_concurrent_modification_error());
        assert_eq!(error.message(), "KafkaConsumer is not safe for multi-threaded access.");
        assert_eq!(
            // SAFETY: Test code: `mock` is a live handle.
            unsafe { kafka_consumer_MockConsumer_closed(mock) },
            0,
            "the mock's own getters still answer"
        );
        // The reads Java guards with `acquire()` fail the same way, so a caller
        // can tell "busy" from an empty answer.
        // SAFETY: Test code: `consumer` and `tp` are live handles and `e` the valid error slot
        // `call` passes.
        let (lists, errors): (Vec<_>, Vec<_>) = unsafe {
            [
                call(|e| kafka_consumer_Consumer_assignment(consumer, e)),
                call(|e| kafka_consumer_Consumer_subscription(consumer, e)),
                call(|e| kafka_consumer_Consumer_paused(consumer, e)),
            ]
        }
        .into_iter()
        .unzip();
        // SAFETY: As above.
        let (lag, lag_error) = call(|e| unsafe { kafka_consumer_Consumer_current_lag(consumer, tp, e) });
        // SAFETY: As above.
        let (metadata, metadata_error) = call(|e| unsafe { kafka_consumer_Consumer_group_metadata(consumer, e) });
        for error in errors.into_iter().chain([lag_error, metadata_error]) {
            let error = error.expect("refused while the rebalance is in flight");
            assert!(error.is_local_concurrent_modification_error());
            assert_eq!(error.message(), "KafkaConsumer is not safe for multi-threaded access.");
        }
        assert!(
            lists.iter().all(|list| list.is_null()) && metadata.is_null() && lag == -1,
            "each returns its failure value"
        );
        // Java's `metrics()` takes no `acquire()`: it still answers.
        // SAFETY: Test code: `consumer` is a live handle; the map is owned and destroyed once.
        unsafe {
            let metrics = kafka_consumer_Consumer_metrics(consumer);
            assert!(!metrics.is_null());
            kafka_Map_destroy(metrics);
        }

        wait_until("the listener invocation to run", || {
            // SAFETY: Test code: `consumer` is a live handle.
            unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) };
            probe.pending_id.load(SeqCst) != 0
        });
        let id = probe.pending_id.load(SeqCst);
        // SAFETY: Test code: `consumer` is the live handle that issued `id`, and the null error is a
        // success report, with a null value since the method is void.
        unsafe { kafka_consumer_Consumer__set_callback_result(consumer, id, ptr::null_mut(), ptr::null_mut()) };
        wait_until("the completion to run", || {
            // SAFETY: Test code: `consumer` is a live handle.
            unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) };
            completed.load(SeqCst) == 1
        });
        // SAFETY: Test code: `consumer` is a live handle and `e` the valid error slot `ok` passes.
        ok(|e| unsafe { kafka_consumer_Consumer_commit_sync(consumer, e) });
        // SAFETY: Test code: `consumer` is a live handle, `e` the valid error slot `ok` passes, and
        // the returned list is owned and destroyed once.
        unsafe {
            let list = ok(|e| kafka_consumer_Consumer_assignment(consumer, e));
            assert_eq!(kafka_List_size(list), 1);
            kafka_List_destroy(list);
        }

        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    /// Java's `MockConsumer.closed()`, `shouldRebalance()` and
    /// `lastPollTimeout()` never throw, so their C forms answer the real value
    /// while another call holds the mock, not the old `-1` "busy" answer.
    #[test]
    fn mock_getters_answer_their_value_while_an_operation_is_in_flight() {
        let (mock, consumer) = new_mock();
        let probe = ListenerProbe::new(false);
        // SAFETY: Test code: `consumer` is live and `probe` outlives the consumer.
        unsafe { subscribe(consumer, &probe) };
        // A non-default value for each field, so no fallback can pass for it: a 250 ms poll,
        // a pending rebalance and a closed mock. `rebalance` checks no closed state, as Java's
        // does not, so it can still run after `close`.
        // SAFETY: Test code: `consumer` is a live handle, `e` the valid error slot `ok` passes, and
        // the returned records are owned and released once.
        unsafe {
            let records = ok(|e| kafka_consumer_Consumer_poll(consumer, 250, e));
            kafka_consumer_ConsumerRecords_destroy(records);
            ok(|e| crate::ffi::consumer::kafka_consumer_Consumer_enforce_rebalance(consumer, e));
            ok(|e| crate::ffi::consumer::kafka_consumer_Consumer_close(consumer, e));
        }

        let completed = AtomicI32::new(0);
        let (assignment, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: `mock` and `assignment` are live handles and `completed` outlives the
        // completion.
        unsafe {
            kafka_consumer_MockConsumer_rebalance_cb(
                mock,
                assignment,
                count_completion,
                &completed as *const AtomicI32 as *mut c_void,
            )
        };
        // In flight until the listener invocation is pumped and reported: a guarded call is refused.
        // SAFETY: Test code: `consumer` is a live handle and `e` the valid error slot `call` passes.
        let error = call(|e| unsafe { kafka_consumer_Consumer_commit_sync(consumer, e) })
            .1
            .expect("rejected");
        assert!(error.is_local_concurrent_modification_error());
        // SAFETY: Test code: `mock` is a live handle.
        unsafe {
            assert_eq!(kafka_consumer_MockConsumer_closed(mock), 1, "closed() while busy");
            assert_eq!(
                kafka_consumer_MockConsumer_should_rebalance(mock),
                1,
                "shouldRebalance() while busy"
            );
            assert_eq!(
                kafka_consumer_MockConsumer_last_poll_timeout(mock),
                250,
                "lastPollTimeout() while busy"
            );
        }

        wait_until("the listener invocation to run", || {
            // SAFETY: Test code: `consumer` is a live handle.
            unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) };
            probe.pending_id.load(SeqCst) != 0
        });
        let id = probe.pending_id.load(SeqCst);
        // SAFETY: Test code: `consumer` is the live handle that issued `id`, and the null error is a
        // success report, with a null value since the method is void.
        unsafe { kafka_consumer_Consumer__set_callback_result(consumer, id, ptr::null_mut(), ptr::null_mut()) };
        wait_until("the completion to run", || {
            // SAFETY: Test code: `consumer` is a live handle.
            unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) };
            completed.load(SeqCst) == 1
        });

        // SAFETY: Test code: each handle is live and destroyed once, after its last use.
        unsafe {
            kafka_List_destroy(assignment);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    /// `scheduleNopPollTask()` queues a real (empty) poll task: the first
    /// `poll` consumes it, so a task scheduled after it runs on the second.
    #[test]
    fn schedule_nop_poll_task_consumes_one_poll() {
        let (mock, consumer) = new_mock();
        let ran = std::sync::Arc::new(AtomicI32::new(0));
        let counter = std::sync::Arc::clone(&ran);
        // SAFETY: Test code: `mock` and its `consumer` view stay live until the mock is
        // destroyed last; each `records` is a fresh handle destroyed once.
        unsafe {
            kafka_consumer_MockConsumer_schedule_nop_poll_task(mock);
            with_mock(mock, move |m| {
                m.schedule_poll_task(Box::new(move |_| {
                    counter.fetch_add(1, SeqCst);
                }))
            })
            .expect("no other call in flight");

            for expected in [0, 1] {
                let records = ok(|e| kafka_consumer_Consumer_poll(consumer, 0, e));
                assert_eq!(kafka_consumer_ConsumerRecords_count(records), 0);
                kafka_consumer_ConsumerRecords_destroy(records);
                assert_eq!(ran.load(SeqCst), expected);
            }
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    /// Runs the consumer's queued callbacks until `probe` saw one completion,
    /// then checks no second one arrives.
    fn drain_one(consumer: *mut kafka_consumer_Consumer_t, probe: &Probe) {
        wait_until("the completion", || {
            // SAFETY: Test code: `consumer` is a live handle.
            unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) };
            !probe.fired().is_empty()
        });
        thread::sleep(Duration::from_millis(20));
        // SAFETY: Test code: `consumer` is a live handle.
        assert_eq!(unsafe { kafka_consumer_Consumer__execute_callbacks(consumer) }, 0);
    }

    /// A `_cb` body that panics before handing its completion over delivers
    /// the panic once, queued: nothing fires until `__execute_callbacks`, which
    /// runs exactly one completion, of each callback shape the consumer has.
    #[test]
    fn a_panicking_cb_body_queues_one_panic_completion() {
        let (mock, consumer) = new_mock();
        let (partitions, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: the handles stay live until destroyed at the end.
        unsafe {
            let void = Probe::default();
            fail_point::arm();
            kafka_consumer_Consumer_commit_sync_cb(consumer, on_error, void.as_opaque());
            assert!(void.fired().is_empty(), "queued, not fired");
            assert_eq!(kafka_consumer_Consumer__execute_callbacks(consumer), 1);
            void.assert_one_panic("kafka_consumer_Consumer_commit_sync_cb", fail_point::MESSAGE);
            assert_eq!(kafka_consumer_Consumer__execute_callbacks(consumer), 0);

            let int = Probe::default();
            fail_point::arm();
            kafka_consumer_Consumer_position_cb(consumer, tp, on_i64, int.as_opaque());
            assert!(int.fired().is_empty(), "queued, not fired");
            assert_eq!(kafka_consumer_Consumer__execute_callbacks(consumer), 1);
            int.assert_one_panic("kafka_consumer_Consumer_position_cb", fail_point::MESSAGE);

            let value = Probe::default();
            fail_point::arm();
            kafka_consumer_Consumer_poll_cb(consumer, 0, on_value, value.as_opaque());
            assert!(value.fired().is_empty(), "queued, not fired");
            assert_eq!(kafka_consumer_Consumer__execute_callbacks(consumer), 1);
            value.assert_one_panic("kafka_consumer_Consumer_poll_cb", fail_point::MESSAGE);

            let mock_void = Probe::default();
            fail_point::arm();
            kafka_consumer_MockConsumer_rebalance_cb(mock, partitions, on_error, mock_void.as_opaque());
            assert!(mock_void.fired().is_empty(), "queued, not fired");
            assert_eq!(kafka_consumer_Consumer__execute_callbacks(consumer), 1);
            mock_void.assert_one_panic("kafka_consumer_MockConsumer_rebalance_cb", fail_point::MESSAGE);

            // The panics came before the single-owner flag was taken, so the
            // consumer is still free.
            ok(|e| kafka_consumer_Consumer_commit_sync(consumer, e));
            for probe in [&void, &int, &value, &mock_void] {
                assert_eq!(probe.fired().len(), 1, "no second completion");
            }

            kafka_List_destroy(partitions);
            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    /// A panic on the task a `_cb` entry point spawned, after the operation
    /// took the single-owner flag, is delivered once through the completion,
    /// queued, and the flag is released.
    #[test]
    fn a_panicking_cb_task_delivers_one_panic_completion() {
        let (mock, consumer) = new_mock();
        // SAFETY: Test code: the handles stay live until destroyed at the end.
        unsafe {
            with_mock(mock, |m| m.schedule_poll_task(Box::new(|_| panic!("poll task panicked"))))
                .expect("no other call in flight");
            let probe = Probe::default();
            kafka_consumer_Consumer_poll_cb(consumer, 0, on_value, probe.as_opaque());
            drain_one(consumer, &probe);
            probe.assert_one_panic("kafka_consumer_Consumer_poll_cb", "poll task panicked");

            let records = ok(|e| kafka_consumer_Consumer_poll(consumer, 0, e));
            kafka_consumer_ConsumerRecords_destroy(records);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    /// The same for the reentrancy handle: queued on the consumer's callbacks
    /// vector while the consumer lives, inline once it is destroyed, as the
    /// handle delivers every completion.
    #[test]
    fn a_panicking_handle_cb_delivers_one_panic_completion() {
        let (mock, consumer) = new_mock();
        let (partitions, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: the handles stay live until destroyed at the end; the
        // reentrancy handle outlives the consumer by design.
        unsafe {
            let handle = kafka_consumer_Consumer_handle(consumer);
            let queued = Probe::default();
            fail_point::arm();
            kafka_consumer_ConsumerHandle_pause_cb(handle, partitions, on_error, queued.as_opaque());
            let int = Probe::default();
            fail_point::arm();
            kafka_consumer_ConsumerHandle_position_cb(handle, tp, on_i64, int.as_opaque());
            assert!(queued.fired().is_empty() && int.fired().is_empty(), "queued, not fired");
            assert_eq!(kafka_consumer_Consumer__execute_callbacks(consumer), 2);
            queued.assert_one_panic("kafka_consumer_ConsumerHandle_pause_cb", fail_point::MESSAGE);
            int.assert_one_panic("kafka_consumer_ConsumerHandle_position_cb", fail_point::MESSAGE);
            kafka_consumer_MockConsumer_destroy(mock);

            let inline = Probe::default();
            fail_point::arm();
            kafka_consumer_ConsumerHandle_pause_cb(handle, partitions, on_error, inline.as_opaque());
            inline.assert_one_panic("kafka_consumer_ConsumerHandle_pause_cb", fail_point::MESSAGE);
            assert_eq!(queued.fired().len() + int.fired().len(), 2, "no second completion");

            kafka_consumer_ConsumerHandle_destroy(handle);
            kafka_List_destroy(partitions);
            kafka_common_TopicPartition_destroy(tp);
        }
    }

    /// A panic on the task a blocking handle operation drives is returned as
    /// that operation's panic error, not as the consumer being destroyed, and
    /// leaves the handle and the consumer usable.
    #[test]
    fn a_panicking_blocking_handle_task_returns_the_panic_error() {
        let (mock, consumer) = new_mock();
        let (partitions, tp) = partition_list(c"topic", 0);
        // SAFETY: Test code: the handles stay live until destroyed at the end.
        unsafe {
            let handle = kafka_consumer_Consumer_handle(consumer);
            fail_point::arm();
            let error = call(|e| kafka_consumer_ConsumerHandle_pause(handle, partitions, e))
                .1
                .expect("the panic");
            assert!(error.is_local_illegal_state_error(), "{error:?}");
            assert_eq!(
                error.message(),
                format!(
                    "Rust panic caught at the FFI boundary in kafka_consumer_ConsumerHandle_pause: {}",
                    fail_point::MESSAGE
                )
            );

            // Disarmed: the mock's handle answers as it always does.
            let error = call(|e| kafka_consumer_ConsumerHandle_pause(handle, partitions, e))
                .1
                .expect("mock error");
            assert!(!error.message().starts_with("Rust panic"), "{error:?}");
            // The consumer is free.
            ok(|e| kafka_consumer_Consumer_commit_sync(consumer, e));

            kafka_consumer_MockConsumer_destroy(mock);
            kafka_consumer_ConsumerHandle_destroy(handle);
            kafka_List_destroy(partitions);
            kafka_common_TopicPartition_destroy(tp);
        }
    }

    /// A blocking operation that panics clears the blocking flag on the way
    /// out, so a listener job delivered afterwards is queued for the pump
    /// rather than run inline on the delivering thread.
    #[test]
    fn a_panicking_blocking_operation_leaves_later_listener_jobs_queued() {
        let (mock, consumer) = new_mock();
        // SAFETY: Test code: the handles stay live until destroyed at the end.
        unsafe {
            with_mock(mock, |m| m.schedule_poll_task(Box::new(|_| panic!("poll task panicked"))))
                .expect("no other call in flight");
            let (records, error) = call(|e| kafka_consumer_Consumer_poll(consumer, 0, e));
            assert!(records.is_null(), "the failure value");
            let error = error.expect("the panic");
            assert_eq!(
                error.message(),
                "Rust panic caught at the FFI boundary in kafka_consumer_Consumer_poll: poll task panicked"
            );

            let ran = std::sync::Arc::new(AtomicI32::new(0));
            let counter = std::sync::Arc::clone(&ran);
            client_ref(consumer).delivery().invoke(Box::new(move || {
                counter.fetch_add(1, SeqCst);
            }));
            assert_eq!(ran.load(SeqCst), 0, "queued, not run inline");
            assert_eq!(kafka_consumer_Consumer__execute_callbacks(consumer), 1);
            assert_eq!(ran.load(SeqCst), 1);
            kafka_consumer_MockConsumer_destroy(mock);
        }
    }

    /// A destroy inside a tokio runtime, where `teardown`'s `block_on` would
    /// panic, returns at once and tears the handle down on its own thread: the
    /// queued `close_cb` completion fires there, once.
    #[test]
    fn destroy_inside_a_tokio_runtime_tears_down_on_its_own_thread() {
        use crate::ffi::common::teardown_probe::{Fired, on_fired};
        use crate::ffi::consumer::kafka_consumer_Consumer_close_cb;
        let fired = Fired::new();
        // SAFETY: Test code: the strategy is a NUL-terminated literal and `e` the valid slot `ok`
        // hands in.
        let mock = ok(|e| unsafe { kafka_consumer_MockConsumer_new(c"earliest".as_ptr(), e) });
        // SAFETY: Test code: the mock is live until it is destroyed, once, inside the runtime;
        // `fired` outlives the completion it waits for.
        unsafe {
            kafka_consumer_Consumer_close_cb(
                kafka_consumer_MockConsumer__as_Consumer(mock),
                on_fired,
                fired.as_opaque(),
            );
            let mock = mock as usize;
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            runtime.block_on(async move { kafka_consumer_MockConsumer_destroy(mock as *mut _) });
        }
        assert_eq!(fired.wait(), (Some("kafka-consumer-destroy".to_string()), true));
    }
}
