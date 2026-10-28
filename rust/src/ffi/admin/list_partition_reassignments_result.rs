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

//! `kafka_admin_ListPartitionReassignmentsResult_t`:
//! `org.apache.kafka.clients.admin.ListPartitionReassignmentsResult`
//! (CLAUDE.md §4).

use std::collections::HashMap;
use std::ffi::c_void;

use crate::admin::{ListPartitionReassignmentsResult, PartitionReassignment};
use crate::common::TopicPartition;
use crate::ffi::admin::partition_reassignment::{box_partition_reassignment, destroy_partition_reassignment_element};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::box_map;

/// Opaque handle to a [`ListPartitionReassignmentsResult`], owned and freed
/// with [`kafka_admin_ListPartitionReassignmentsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListPartitionReassignmentsResult_t {
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

/// Java's `Map<TopicPartition, PartitionReassignment>` as an owned map of
/// owned `kafka_common_TopicPartition_t *` keys (compared by value) to owned
/// `kafka_admin_PartitionReassignment_t *`, ordered by topic then partition.
fn reassignments_map(map: HashMap<TopicPartition, PartitionReassignment>) -> *mut c_void {
    let mut entries: Vec<(TopicPartition, PartitionReassignment)> = map.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, r)| {
            (
                box_topic_partition(tp) as *mut c_void,
                box_partition_reassignment(r) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition_element),
        Some(destroy_partition_reassignment_element),
        Some(topic_partition_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_partition_reassignments_result(
    result: ListPartitionReassignmentsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ListPartitionReassignmentsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_ListPartitionReassignmentsResult_t,
) -> &'a ResultHandle<ListPartitionReassignmentsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_ListPartitionReassignmentsResult_t` is built by
    // `box_result::<ListPartitionReassignmentsResult, _>` (in
    // `box_list_partition_reassignments_result`), as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// Return a future which yields a map containing each partition's reassignments
///
/// # Returns
///
/// A future; its `get` delivers a `kafka_Map_t *` (owned by the future) of
/// `kafka_common_TopicPartition_t *` keys, ordered by topic then partition and compared by value in
/// `kafka_Map_get`, to `kafka_admin_PartitionReassignment_t *` values.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListPartitionReassignmentsResult_reassignments(
    self_: *const kafka_admin_ListPartitionReassignmentsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `reassignments_map` makes through
    // `box_map`, and the handle holds only owned data and no thread-bound state, so it may be freed
    // from any thread and read from several at once, as `handle_future` requires.
    unsafe {
        h.ctx
            .handle_future(&h.result.reassignments(), reassignments_map, destroy_map_element)
    }
}

/// Frees a result handle; `NULL` is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListPartitionReassignmentsResult_destroy(
    self_: *mut kafka_admin_ListPartitionReassignmentsResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_ListPartitionReassignmentsResult_t` is built by
    // `box_result::<ListPartitionReassignmentsResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<ListPartitionReassignmentsResult, _>(self_) }
}
