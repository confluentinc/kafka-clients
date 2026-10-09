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

//! `kafka_admin_ListOffsetsResult_t` and its nested
//! `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t`:
//! `org.apache.kafka.clients.admin.ListOffsetsResult` (CLAUDE.md §4).

use std::collections::HashMap;
use std::ffi::{c_char, c_void};

use crate::admin::{ListOffsetsResult, ListOffsetsResultInfo};
use crate::common::TopicPartition;
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, out_slot, result_ref,
};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
    topic_partition_ref,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_map, into_c_string};

// ---------------------------------------------------------------------------
// ListOffsetsResult.ListOffsetsResultInfo
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ListOffsetsResultInfo`], owned and freed with
/// [`kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy`].
#[repr(C)]
pub struct kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t {
    _private: [u8; 0],
}

/// Hands `info` to C as an owned handle.
pub(crate) fn box_list_offsets_result_info(
    info: ListOffsetsResultInfo,
) -> *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t {
    Box::into_raw(Box::new(info)) as *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `info` must be a live list-offsets-result-info handle.
pub(crate) unsafe fn list_offsets_result_info_ref<'a>(
    info: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> &'a ListOffsetsResultInfo {
    unsafe { &*(info as *const ListOffsetsResultInfo) }
}

/// Frees a `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *` element
/// of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned list-offsets-result-info handle.
pub(crate) unsafe fn destroy_list_offsets_result_info_element(element: *mut c_void) {
    unsafe {
        kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy(
            element as *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
        )
    }
}

/// `new ListOffsetsResultInfo(long offset, long timestamp, Optional<Integer>
/// leaderEpoch)`: `leader_epoch` is `-1` for Java's empty `Optional`. Owned,
/// freed with [`kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_new(
    offset: i64,
    timestamp: i64,
    leader_epoch: i32,
) -> *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t {
    let leader_epoch = if leader_epoch == -1 { None } else { Some(leader_epoch) };
    box_list_offsets_result_info(ListOffsetsResultInfo::new(offset, timestamp, leader_epoch))
}

/// `offset()`.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> i64 {
    unsafe { list_offsets_result_info_ref(self_) }.offset()
}

/// `timestamp()`.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> i64 {
    unsafe { list_offsets_result_info_ref(self_) }.timestamp()
}

/// `leaderEpoch()`: `-1` for Java's empty `Optional`.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> i32 {
    unsafe { list_offsets_result_info_ref(self_) }.leader_epoch().unwrap_or(-1)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_to_string(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> *mut c_char {
    into_c_string(&unsafe { list_offsets_result_info_ref(self_) }.to_string())
}

/// Frees an owned list-offsets-result-info handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy(
    self_: *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ListOffsetsResultInfo) });
    }
}

// ---------------------------------------------------------------------------
// ListOffsetsResult
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ListOffsetsResult`], owned and freed with
/// [`kafka_admin_ListOffsetsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListOffsetsResult_t {
    _private: [u8; 0],
}

/// Frees a `kafka_common_TopicPartition_t *` key of an owned map.
unsafe fn destroy_topic_partition_element(element: *mut c_void) {
    unsafe { kafka_common_TopicPartition_destroy(element as *mut kafka_common_TopicPartition_t) }
}

/// Java's `Map<TopicPartition, ListOffsetsResultInfo>` as an owned map of
/// owned `kafka_common_TopicPartition_t *` keys (compared by value) to owned
/// `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *`, ordered by
/// topic then partition.
fn infos_map(map: HashMap<TopicPartition, ListOffsetsResultInfo>) -> *mut c_void {
    let mut entries: Vec<(TopicPartition, ListOffsetsResultInfo)> = map.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, i)| {
            (
                box_topic_partition(tp) as *mut c_void,
                box_list_offsets_result_info(i) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition_element),
        Some(destroy_list_offsets_result_info_element),
        Some(topic_partition_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_offsets_result(
    result: ListOffsetsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ListOffsetsResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_ListOffsetsResult_t) -> &'a ResultHandle<ListOffsetsResult> {
    unsafe { result_ref(self_) }
}

/// `partitionResult(TopicPartition partition)`: delivers through
/// `out_partition_result` an owned future (freed with
/// `kafka_common_KafkaFuture_destroy`) whose `get` yields a
/// `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *` owned by the
/// future, or returns the owned `IllegalArgumentError` when
/// `partition` was not part of the request. `partition` is borrowed for the
/// call.
///
/// # Safety
///
/// `self_` must be a live result handle, `partition` a valid topic-partition
/// handle and `out_partition_result` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_partition_result(
    self_: *const kafka_admin_ListOffsetsResult_t,
    partition: *const kafka_common_TopicPartition_t,
    out_partition_result: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let partition = unsafe { topic_partition_ref(partition) };
    unsafe {
        out_slot(h.result.partition_result(partition), out_partition_result, |f| {
            h.ctx.handle_future(
                &f,
                |i| box_list_offsets_result_info(i) as *mut c_void,
                destroy_list_offsets_result_info_element,
            )
        })
    }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_Map_t *` (owned by the future) of
/// `kafka_common_TopicPartition_t *` keys, ordered by topic then partition
/// and compared by value in `kafka_Map_get`, to
/// `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *` values.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_all(
    self_: *const kafka_admin_ListOffsetsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.all(), infos_map, destroy_map_element)
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_destroy(self_: *mut kafka_admin_ListOffsetsResult_t) {
    unsafe { destroy_result::<ListOffsetsResult, _>(self_) }
}
