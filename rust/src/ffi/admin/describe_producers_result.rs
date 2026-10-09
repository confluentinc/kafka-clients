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

//! `kafka_admin_DescribeProducersResult_t` and its nested
//! `kafka_admin_DescribeProducersResult_PartitionProducerState_t`:
//! `org.apache.kafka.clients.admin.DescribeProducersResult` (CLAUDE.md §4).

use std::collections::HashMap;
use std::ffi::{c_char, c_void};

use crate::admin::{DescribeProducersResult, PartitionProducerState};
use crate::common::TopicPartition;
use crate::ffi::admin::producer_state::{
    box_producer_state, destroy_producer_state_element, kafka_admin_ProducerState_t, producer_state_ref,
};
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, out_slot, result_ref,
};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
    topic_partition_ref,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_list, box_map, into_c_string, kafka_List_t, list_elements};

// ---------------------------------------------------------------------------
// DescribeProducersResult.PartitionProducerState
// ---------------------------------------------------------------------------

/// Opaque handle to a [`PartitionProducerState`], owned and freed with
/// [`kafka_admin_DescribeProducersResult_PartitionProducerState_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeProducersResult_PartitionProducerState_t {
    _private: [u8; 0],
}

/// Hands `state` to C as an owned handle.
pub(crate) fn box_partition_producer_state(
    state: PartitionProducerState,
) -> *mut kafka_admin_DescribeProducersResult_PartitionProducerState_t {
    Box::into_raw(Box::new(state)) as *mut kafka_admin_DescribeProducersResult_PartitionProducerState_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `state` must be a live partition-producer-state handle.
pub(crate) unsafe fn partition_producer_state_ref<'a>(
    state: *const kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) -> &'a PartitionProducerState {
    unsafe { &*(state as *const PartitionProducerState) }
}

/// Frees a `kafka_admin_DescribeProducersResult_PartitionProducerState_t *`
/// element of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned partition-producer-state handle.
pub(crate) unsafe fn destroy_partition_producer_state_element(element: *mut c_void) {
    unsafe {
        kafka_admin_DescribeProducersResult_PartitionProducerState_destroy(
            element as *mut kafka_admin_DescribeProducersResult_PartitionProducerState_t,
        )
    }
}

/// `new PartitionProducerState(List<ProducerState> activeProducers)`:
/// `active_producers` is a borrowed list of `const kafka_admin_ProducerState_t *`,
/// copied during the call (null reads as empty). Owned, freed with
/// [`kafka_admin_DescribeProducersResult_PartitionProducerState_destroy`].
///
/// # Safety
///
/// `active_producers` must be null or a valid list of producer-state handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_new(
    active_producers: *const kafka_List_t,
) -> *mut kafka_admin_DescribeProducersResult_PartitionProducerState_t {
    let active_producers = unsafe { list_elements(active_producers) }
        .iter()
        .map(|&p| unsafe { producer_state_ref(p as *const kafka_admin_ProducerState_t) }.clone())
        .collect();
    box_partition_producer_state(PartitionProducerState::new(active_producers))
}

/// `activeProducers()`: an owned `kafka_List_t` (freed with
/// `kafka_List_destroy`, which frees its elements) of owned
/// `kafka_admin_ProducerState_t *` copies, in Java's list order.
///
/// # Safety
///
/// `self_` must be a live partition-producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_active_producers(
    self_: *const kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) -> *mut kafka_List_t {
    let state = unsafe { partition_producer_state_ref(self_) };
    let elements = state
        .active_producers()
        .iter()
        .map(|p| box_producer_state(p.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_producer_state_element))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a live partition-producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_to_string(
    self_: *const kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) -> *mut c_char {
    into_c_string(&unsafe { partition_producer_state_ref(self_) }.to_string())
}

/// Frees an owned partition-producer-state handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_destroy(
    self_: *mut kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut PartitionProducerState) });
    }
}

// ---------------------------------------------------------------------------
// DescribeProducersResult
// ---------------------------------------------------------------------------

/// Opaque handle to a [`DescribeProducersResult`], owned and freed with
/// [`kafka_admin_DescribeProducersResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeProducersResult_t {
    _private: [u8; 0],
}

/// Frees a `kafka_common_TopicPartition_t *` key of an owned map.
unsafe fn destroy_topic_partition_element(element: *mut c_void) {
    unsafe { kafka_common_TopicPartition_destroy(element as *mut kafka_common_TopicPartition_t) }
}

/// Java's `Map<TopicPartition, PartitionProducerState>` as an owned map of
/// owned `kafka_common_TopicPartition_t *` keys (compared by value) to owned
/// `kafka_admin_DescribeProducersResult_PartitionProducerState_t *`, ordered
/// by topic then partition.
fn states_map(map: HashMap<TopicPartition, PartitionProducerState>) -> *mut c_void {
    let mut entries: Vec<(TopicPartition, PartitionProducerState)> = map.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, s)| {
            (
                box_topic_partition(tp) as *mut c_void,
                box_partition_producer_state(s) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition_element),
        Some(destroy_partition_producer_state_element),
        Some(topic_partition_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_producers_result(
    result: DescribeProducersResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeProducersResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_DescribeProducersResult_t) -> &'a ResultHandle<DescribeProducersResult> {
    unsafe { result_ref(self_) }
}

/// `partitionResult(TopicPartition partition)`: delivers through
/// `out_partition_result` an owned future (freed with
/// `kafka_common_KafkaFuture_destroy`) whose `get` yields a
/// `kafka_admin_DescribeProducersResult_PartitionProducerState_t *` owned by
/// the future, or returns the owned `IllegalArgumentError`
/// when `partition` was not part of the request. `partition` is borrowed
/// for the call.
///
/// # Safety
///
/// `self_` must be a live result handle, `partition` a valid topic-partition
/// handle and `out_partition_result` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_partition_result(
    self_: *const kafka_admin_DescribeProducersResult_t,
    partition: *const kafka_common_TopicPartition_t,
    out_partition_result: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let partition = unsafe { topic_partition_ref(partition) };
    unsafe {
        out_slot(h.result.partition_result(partition), out_partition_result, |f| {
            h.ctx.handle_future(
                &f,
                |s| box_partition_producer_state(s) as *mut c_void,
                destroy_partition_producer_state_element,
            )
        })
    }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_Map_t *` (owned by the future) of
/// `kafka_common_TopicPartition_t *` keys, ordered by topic then partition
/// and compared by value in `kafka_Map_get`, to
/// `kafka_admin_DescribeProducersResult_PartitionProducerState_t *` values.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_all(
    self_: *const kafka_admin_DescribeProducersResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.all(), states_map, destroy_map_element)
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_destroy(
    self_: *mut kafka_admin_DescribeProducersResult_t,
) {
    unsafe { destroy_result::<DescribeProducersResult, _>(self_) }
}
