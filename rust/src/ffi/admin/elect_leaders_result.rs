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

//! `kafka_admin_ElectLeadersResult_t`:
//! `org.apache.kafka.clients.admin.ElectLeadersResult` (CLAUDE.md §4).

use std::collections::HashMap;
use std::ffi::c_void;

use crate::admin::ElectLeadersResult;
use crate::common::{Error, TopicPartition};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
};
use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_t};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::box_map;

/// Opaque handle to an [`ElectLeadersResult`], owned and freed with
/// [`kafka_admin_ElectLeadersResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ElectLeadersResult_t {
    _private: [u8; 0],
}

/// Frees a `kafka_common_TopicPartition_t *` key of an owned map.
unsafe fn destroy_topic_partition_element(element: *mut c_void) {
    unsafe { kafka_common_TopicPartition_destroy(element as *mut kafka_common_TopicPartition_t) }
}

/// Frees a `kafka_common_Error_t *` value of an owned map.
unsafe fn destroy_error_element(element: *mut c_void) {
    unsafe { kafka_common_Error_destroy(element as *mut kafka_common_Error_t) }
}

/// Java's `Map<TopicPartition, Optional<Throwable>>` as an owned map of
/// owned `kafka_common_TopicPartition_t *` keys (compared by value) to an
/// owned `kafka_common_Error_t *` or `NULL` for an election that succeeded,
/// ordered by topic then partition.
fn partitions_map(map: HashMap<TopicPartition, Option<Error>>) -> *mut c_void {
    let mut entries: Vec<(TopicPartition, Option<Error>)> = map.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, error)| {
            (
                box_topic_partition(tp) as *mut c_void,
                error.map_or(std::ptr::null_mut(), |e| box_error(e) as *mut c_void),
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition_element),
        Some(destroy_error_element),
        Some(topic_partition_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_elect_leaders_result(
    result: ElectLeadersResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ElectLeadersResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_ElectLeadersResult_t) -> &'a ResultHandle<ElectLeadersResult> {
    unsafe { result_ref(self_) }
}

/// `partitions()`: an owned future freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a `kafka_Map_t *`
/// (owned by the future) of `kafka_common_TopicPartition_t *` keys, ordered
/// by topic then partition and compared by value in `kafka_Map_get`, to a
/// `kafka_common_Error_t *` for a partition whose election failed or `NULL`
/// for one that succeeded (Java's `Optional<Throwable>`).
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_partitions(
    self_: *const kafka_admin_ElectLeadersResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.partitions(), partitions_map, destroy_map_element)
}

/// `all()`: an owned `KafkaFuture<Void>` freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers `NULL` when every
/// election succeeded, or the first partition's error.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_all(
    self_: *const kafka_admin_ElectLeadersResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ElectLeadersResult_destroy(self_: *mut kafka_admin_ElectLeadersResult_t) {
    unsafe { destroy_result::<ElectLeadersResult, _>(self_) }
}
