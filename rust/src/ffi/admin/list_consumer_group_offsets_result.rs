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

//! `kafka_admin_ListConsumerGroupOffsetsResult_t`:
//! `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult`
//! (CLAUDE.md §4).

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};

use crate::admin::{GroupOffsets, ListConsumerGroupOffsetsResult};
use crate::common::TopicPartition;
use crate::consumer::OffsetAndMetadata;
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, out_slot, result_ref,
};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
};
use crate::ffi::consumer::offset_and_metadata::{
    box_offset_and_metadata, kafka_consumer_OffsetAndMetadata_destroy, kafka_consumer_OffsetAndMetadata_t,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_map, box_string_keyed_map, c_str_to_string};

/// Opaque handle to a [`ListConsumerGroupOffsetsResult`], owned and freed
/// with [`kafka_admin_ListConsumerGroupOffsetsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListConsumerGroupOffsetsResult_t {
    _private: [u8; 0],
}

/// Frees a `kafka_common_TopicPartition_t *` key of an owned map.
unsafe fn destroy_topic_partition_element(element: *mut c_void) {
    unsafe { kafka_common_TopicPartition_destroy(element as *mut kafka_common_TopicPartition_t) }
}

/// Frees a `kafka_consumer_OffsetAndMetadata_t *` value of an owned map.
unsafe fn destroy_offset_and_metadata_element(element: *mut c_void) {
    unsafe { kafka_consumer_OffsetAndMetadata_destroy(element as *mut kafka_consumer_OffsetAndMetadata_t) }
}

/// Java's `Map<TopicPartition, OffsetAndMetadata>` as an owned map of owned
/// `kafka_common_TopicPartition_t *` keys (compared by value) to an owned
/// `kafka_consumer_OffsetAndMetadata_t *` or `NULL` for a partition without
/// a committed offset, ordered by topic then partition.
fn group_offsets_map(offsets: GroupOffsets) -> *mut c_void {
    let mut entries: Vec<(TopicPartition, Option<OffsetAndMetadata>)> = offsets.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, oam)| {
            (
                box_topic_partition(tp) as *mut c_void,
                oam.map_or(std::ptr::null_mut(), |oam| box_offset_and_metadata(oam) as *mut c_void),
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition_element),
        Some(destroy_offset_and_metadata_element),
        Some(topic_partition_key_eq),
    ) as *mut c_void
}

/// Java's `Map<String, Map<TopicPartition, OffsetAndMetadata>>` as an owned
/// map of owned `char *` group ids, sorted, to owned `kafka_Map_t *` built
/// by [`group_offsets_map`].
fn all_map(map: HashMap<String, GroupOffsets>) -> *mut c_void {
    let sorted: BTreeMap<String, GroupOffsets> = map.into_iter().collect();
    box_string_keyed_map(
        sorted
            .into_iter()
            .map(|(group_id, offsets)| (group_id, group_offsets_map(offsets))),
        Some(destroy_map_element),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_consumer_group_offsets_result(
    result: ListConsumerGroupOffsetsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ListConsumerGroupOffsetsResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(
    self_: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
) -> &'a ResultHandle<ListConsumerGroupOffsetsResult> {
    unsafe { result_ref(self_) }
}

/// `partitionsToOffsetAndMetadata()`: when exactly one group was requested,
/// delivers through `out_partitions_to_offset_and_metadata` an owned future
/// (freed with `kafka_common_KafkaFuture_destroy`) whose `get` yields a
/// `kafka_Map_t *` (owned by the future) of `kafka_common_TopicPartition_t *`
/// keys, ordered by topic then partition and compared by value in
/// `kafka_Map_get`, to `kafka_consumer_OffsetAndMetadata_t *` values or
/// `NULL` for a partition without a committed offset; otherwise returns the
/// owned `IllegalStateError`.
///
/// # Safety
///
/// `self_` must be a live result handle and
/// `out_partitions_to_offset_and_metadata` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata(
    self_: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
    out_partitions_to_offset_and_metadata: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    unsafe {
        out_slot(
            h.result.partitions_to_offset_and_metadata(),
            out_partitions_to_offset_and_metadata,
            |f| h.ctx.handle_future(&f, group_offsets_map, destroy_map_element),
        )
    }
}

/// `partitionsToOffsetAndMetadata(String groupId)`: delivers through
/// `out_partitions_to_offset_and_metadata_with_group_id` the future
/// described in
/// [`kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata`]
/// for `group_id`, or returns the owned `IllegalArgumentError`
/// translation when that group was not part of the request.
///
/// # Safety
///
/// `self_` must be a live result handle, `group_id` a valid NUL-terminated
/// string and `out_partitions_to_offset_and_metadata_with_group_id` a valid
/// slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(
    self_: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
    group_id: *const c_char,
    out_partitions_to_offset_and_metadata_with_group_id: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let group_id = unsafe { c_str_to_string(group_id) };
    unsafe {
        out_slot(
            h.result.partitions_to_offset_and_metadata_with_group_id(&group_id),
            out_partitions_to_offset_and_metadata_with_group_id,
            |f| h.ctx.handle_future(&f, group_offsets_map, destroy_map_element),
        )
    }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_Map_t *` (owned by the future) of `char *`
/// group ids, sorted, to the per-group `kafka_Map_t *` described in
/// [`kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata`].
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_all(
    self_: *const kafka_admin_ListConsumerGroupOffsetsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.all(), all_map, destroy_map_element)
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsResult_destroy(
    self_: *mut kafka_admin_ListConsumerGroupOffsetsResult_t,
) {
    unsafe { destroy_result::<ListConsumerGroupOffsetsResult, _>(self_) }
}
