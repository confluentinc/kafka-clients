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

//! C bindings for `org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult`.

use crate::admin::AlterConsumerGroupOffsetsResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::topic_partition::{kafka_common_TopicPartition_t, topic_partition_ref};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;

/// Opaque handle to an [`AlterConsumerGroupOffsetsResult`], owned by the
/// caller and freed with [`kafka_admin_AlterConsumerGroupOffsetsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_AlterConsumerGroupOffsetsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_alter_consumer_group_offsets_result(
    result: AlterConsumerGroupOffsetsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_AlterConsumerGroupOffsetsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
) -> &'a ResultHandle<AlterConsumerGroupOffsetsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_AlterConsumerGroupOffsetsResult_t` is built by
    // `box_result::<AlterConsumerGroupOffsetsResult, _>` (in
    // `box_alter_consumer_group_offsets_result`), as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// `AlterConsumerGroupOffsetsResult.partitionResult(TopicPartition partition)`:
/// an owned `KafkaFuture<Void>` (its `get` delivers `NULL`) that succeeds
/// once the offset of `partition` was altered, and fails with the
/// translation of `IllegalArgumentException` when `partition` was not part
/// of the request. `partition` is borrowed and copied during the call. Freed
/// with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle and `partition` a valid
/// topic-partition handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_partition_result(
    self_: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
    partition: *const kafka_common_TopicPartition_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    h.ctx
        // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, as
        // `topic_partition_ref` requires; the borrow ends with the `partition_result` call, during
        // which the caller keeps the handle alive.
        .void_future(&h.result.partition_result(unsafe { topic_partition_ref(partition) }))
}

/// `AlterConsumerGroupOffsetsResult.all()`: an owned `KafkaFuture<Void>`
/// (its `get` delivers `NULL`) that succeeds once every offset was altered,
/// freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_all(
    self_: *const kafka_admin_AlterConsumerGroupOffsetsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Futures taken from the result
/// stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConsumerGroupOffsetsResult_destroy(
    self_: *mut kafka_admin_AlterConsumerGroupOffsetsResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_AlterConsumerGroupOffsetsResult_t` is built by
    // `box_result::<AlterConsumerGroupOffsetsResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<AlterConsumerGroupOffsetsResult, _>(self_) }
}
