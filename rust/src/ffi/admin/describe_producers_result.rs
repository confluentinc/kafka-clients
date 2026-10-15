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
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
    topic_partition_ref,
};
use crate::ffi::ffi_guard;
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
/// `state` must be a live partition-producer-state handle, kept alive for
/// `'a`.
pub(crate) unsafe fn partition_producer_state_ref<'a>(
    state: *const kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) -> &'a PartitionProducerState {
    // SAFETY: Per this function's `# Safety`, `state` is a live partition-producer-state handle
    // kept alive for `'a`. Every `kafka_admin_DescribeProducersResult_PartitionProducerState_t` is
    // the `Box::into_raw` of a `Box<PartitionProducerState>` (`box_partition_producer_state`, the
    // only place one is made, for an owned handle or a value a map or future owns), so it points at
    // a live `PartitionProducerState`. Nothing takes a `&mut` to it: only the destroy takes the box
    // back, and its `# Safety` excludes any other use at the same time or afterwards.
    unsafe { &*(state as *const PartitionProducerState) }
}

/// Frees a `kafka_admin_DescribeProducersResult_PartitionProducerState_t *`
/// element of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned partition-producer-state handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_partition_producer_state_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned partition-producer-state handle
    // not yet destroyed, that no other call uses at the same time and that is not used afterwards,
    // as `kafka_admin_DescribeProducersResult_PartitionProducerState_destroy` requires.
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
/// `active_producers` must be null or a valid list handle, not mutated
/// during the call, whose elements are live producer-state handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_new(
    active_producers: *const kafka_List_t,
) -> *mut kafka_admin_DescribeProducersResult_PartitionProducerState_t {
    // SAFETY: Per this function's `# Safety`, `active_producers` is null or a valid list handle,
    // which the caller keeps alive for this call and nothing mutates meanwhile, as `list_elements`
    // requires; the borrow ends with the call.
    let active_producers = unsafe { list_elements(active_producers) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element `p` is a live producer-state
        // handle, as `producer_state_ref` requires; the borrow ends with the `clone`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_active_producers(
    self_: *const kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live partition-producer-state handle,
    // kept alive for this call, as `partition_producer_state_ref` requires; the borrow ends once
    // the copies are made.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_to_string(
    self_: *const kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live partition-producer-state handle,
    // kept alive for this call, as `partition_producer_state_ref` requires; the borrow ends with
    // `to_string`.
    into_c_string(&unsafe { partition_producer_state_ref(self_) }.to_string())
}

/// Frees an owned partition-producer-state handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_PartitionProducerState_destroy(
    self_: *mut kafka_admin_DescribeProducersResult_PartitionProducerState_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every such handle is the `Box::into_raw` of a `Box<PartitionProducerState>`
        // (`box_partition_producer_state`), so `from_raw` reclaims that allocation, once.
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
///
/// # Safety
///
/// `element` must be an owned topic-partition handle not yet destroyed.
unsafe fn destroy_topic_partition_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned topic-partition handle not yet
    // destroyed, as `kafka_common_TopicPartition_destroy` requires.
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

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_DescribeProducersResult_t) -> &'a ResultHandle<DescribeProducersResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeProducersResult_t` is built by
    // `box_result::<DescribeProducersResult, _>` (in `box_describe_producers_result`), as
    // `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// `partitionResult(TopicPartition partition)`: returns an owned future (freed
/// with `kafka_common_KafkaFuture_destroy`) whose `get` yields a
/// `kafka_admin_DescribeProducersResult_PartitionProducerState_t *` owned by
/// the future, or stores the owned `IllegalArgumentError` in `*out_err` and
/// returns `NULL` when `partition` was not part of the request. `partition` is
/// borrowed for the call.
///
/// # Safety
///
/// `self_` must be a live result handle, `partition` a valid topic-partition
/// handle and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_partition_result(
    self_: *const kafka_admin_DescribeProducersResult_t,
    partition: *const kafka_common_TopicPartition_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, as
    // `topic_partition_ref` requires; the caller keeps it alive for this call, which the borrow
    // does not outlive.
    let partition = unsafe { topic_partition_ref(partition) };
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires. For `handle_future`, `destroy_partition_producer_state_element` frees exactly the
    // handle `box_partition_producer_state` makes, and the handle holds only owned data and no
    // thread-bound state, so it may be freed from any thread and read from several at once, as
    // `handle_future` requires.
    unsafe {
        deliver_value(h.result.partition_result(partition), out_err, |f| {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_all(
    self_: *const kafka_admin_DescribeProducersResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `states_map` makes through `box_map`, and
    // the handle holds only owned data and no thread-bound state, so it may be freed from any
    // thread and read from several at once, as `handle_future` requires.
    unsafe { h.ctx.handle_future(&h.result.all(), states_map, destroy_map_element) }
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeProducersResult_destroy(
    self_: *mut kafka_admin_DescribeProducersResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeProducersResult_t` is built by
    // `box_result::<DescribeProducersResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<DescribeProducersResult, _>(self_) }
}
