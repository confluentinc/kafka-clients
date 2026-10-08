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

//! C bindings for `org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult`.

use std::ffi::c_void;

use crate::admin::AlterPartitionReassignmentsResult;
use crate::common::{KafkaFuture, TopicPartition};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to an [`AlterPartitionReassignmentsResult`], owned by the
/// caller and freed with [`kafka_admin_AlterPartitionReassignmentsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_AlterPartitionReassignmentsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_alter_partition_reassignments_result(
    result: AlterPartitionReassignmentsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_AlterPartitionReassignmentsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_AlterPartitionReassignmentsResult_t,
) -> &'a ResultHandle<AlterPartitionReassignmentsResult> {
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_TopicPartition_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_TopicPartition_t *`.
unsafe fn destroy_topic_partition_element(element: *mut c_void) {
    unsafe { kafka_common_TopicPartition_destroy(element as *mut kafka_common_TopicPartition_t) }
}

/// `AlterPartitionReassignmentsResult.values()`: an owned map, sorted by
/// topic then partition, of owned `kafka_common_TopicPartition_t *` keys
/// (compared by value in `kafka_Map_get`) to owned
/// `kafka_common_KafkaFuture_t *` (`KafkaFuture<Void>`: `get` delivers
/// `NULL`). Freed with `kafka_Map_destroy`, which frees the keys and the
/// futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_values(
    self_: *const kafka_admin_AlterPartitionReassignmentsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&TopicPartition, &KafkaFuture<()>)> = h.result.values().iter().collect();
    entries.sort_by(|a, b| (a.0.topic(), a.0.partition()).cmp(&(b.0.topic(), b.0.partition())));
    h.ctx.keyed_future_map(
        entries,
        |tp| box_topic_partition(tp.clone()) as *mut c_void,
        destroy_topic_partition_element,
        topic_partition_key_eq,
        FutureCtx::void_future,
    )
}

/// `AlterPartitionReassignmentsResult.all()`: an owned `KafkaFuture<Void>`
/// (its `get` delivers `NULL`) that succeeds once every reassignment was
/// accepted, freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_all(
    self_: *const kafka_admin_AlterPartitionReassignmentsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterPartitionReassignmentsResult_destroy(
    self_: *mut kafka_admin_AlterPartitionReassignmentsResult_t,
) {
    unsafe { destroy_result::<AlterPartitionReassignmentsResult, _>(self_) }
}
