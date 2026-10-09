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

//! C bindings for `org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult`.

use crate::admin::DeleteConsumerGroupOffsetsResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, out_slot, result_ref};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{kafka_common_TopicPartition_t, topic_partition_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;

/// Opaque handle to a [`DeleteConsumerGroupOffsetsResult`], owned by the
/// caller and freed with [`kafka_admin_DeleteConsumerGroupOffsetsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteConsumerGroupOffsetsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_delete_consumer_group_offsets_result(
    result: DeleteConsumerGroupOffsetsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
) -> &'a ResultHandle<DeleteConsumerGroupOffsetsResult> {
    unsafe { result_ref(self_) }
}

/// `DeleteConsumerGroupOffsetsResult.partitionResult(TopicPartition partition)`:
/// stores in `*out_partition_result` an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once the offset of `partition` was
/// deleted, freed with `kafka_common_KafkaFuture_destroy`. Returns `NULL`,
/// or the owned translation of `IllegalArgumentException` when `partition`
/// was not part of the request (Java throws it synchronously, so it is
/// returned rather than carried by the future). `partition` is borrowed and
/// copied during the call.
///
/// # Safety
///
/// `self_` must be a live result handle, `partition` a valid
/// topic-partition handle and `out_partition_result` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result(
    self_: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
    partition: *const kafka_common_TopicPartition_t,
    out_partition_result: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let result = h.result.partition_result(unsafe { topic_partition_ref(partition) });
    unsafe { out_slot(result, out_partition_result, |future| h.ctx.void_future(&future)) }
}

/// `DeleteConsumerGroupOffsetsResult.all()`: an owned `KafkaFuture<Void>`
/// (its `get` delivers `NULL`) that succeeds once every offset was deleted,
/// freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_all(
    self_: *const kafka_admin_DeleteConsumerGroupOffsetsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Futures taken from the result
/// stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(
    self_: *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t,
) {
    unsafe { destroy_result::<DeleteConsumerGroupOffsetsResult, _>(self_) }
}
