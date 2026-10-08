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

//! C bindings for `org.apache.kafka.clients.admin.DeleteRecordsResult`.

use std::ffi::c_void;

use crate::admin::{DeleteRecordsResult, DeletedRecords};
use crate::common::{KafkaFuture, TopicPartition};
use crate::ffi::admin::deleted_records::{box_deleted_records, destroy_deleted_records_element};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to a [`DeleteRecordsResult`], owned by the caller and
/// freed with [`kafka_admin_DeleteRecordsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteRecordsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_delete_records_result(
    result: DeleteRecordsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DeleteRecordsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_DeleteRecordsResult_t) -> &'a ResultHandle<DeleteRecordsResult> {
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

/// `DeleteRecordsResult.lowWatermarks()`: an owned map, sorted by topic
/// then partition, of owned `kafka_common_TopicPartition_t *` keys
/// (compared by value in `kafka_Map_get`) to owned
/// `kafka_common_KafkaFuture_t *` whose `get` delivers a
/// `kafka_admin_DeletedRecords_t *` owned by the future. Freed with
/// `kafka_Map_destroy`, which frees the keys and the futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_low_watermarks(
    self_: *const kafka_admin_DeleteRecordsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&TopicPartition, &KafkaFuture<DeletedRecords>)> = h.result.low_watermarks().iter().collect();
    entries.sort_by(|a, b| (a.0.topic(), a.0.partition()).cmp(&(b.0.topic(), b.0.partition())));
    h.ctx.keyed_future_map(
        entries,
        |tp| box_topic_partition(tp.clone()) as *mut c_void,
        destroy_topic_partition_element,
        topic_partition_key_eq,
        |ctx, future| {
            ctx.handle_future(
                future,
                |records| box_deleted_records(records) as *mut c_void,
                destroy_deleted_records_element,
            )
        },
    )
}

/// `DeleteRecordsResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once every deletion succeeded, freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_all(
    self_: *const kafka_admin_DeleteRecordsResult_t,
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
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_destroy(self_: *mut kafka_admin_DeleteRecordsResult_t) {
    unsafe { destroy_result::<DeleteRecordsResult, _>(self_) }
}
