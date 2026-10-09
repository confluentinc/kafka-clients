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

//! C bindings for `org.apache.kafka.clients.admin.DeleteConsumerGroupsResult`.

use crate::admin::DeleteConsumerGroupsResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to a [`DeleteConsumerGroupsResult`], owned by the caller
/// and freed with [`kafka_admin_DeleteConsumerGroupsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteConsumerGroupsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_delete_consumer_groups_result(
    result: DeleteConsumerGroupsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DeleteConsumerGroupsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DeleteConsumerGroupsResult_t,
) -> &'a ResultHandle<DeleteConsumerGroupsResult> {
    unsafe { result_ref(self_) }
}

/// `DeleteConsumerGroupsResult.deletedGroups()`: an owned map, sorted by
/// group id, of owned `char *` group ids to owned
/// `kafka_common_KafkaFuture_t *` (`KafkaFuture<Void>`: `get` delivers
/// `NULL`). Freed with `kafka_Map_destroy`, which frees the keys and the
/// futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupsResult_deleted_groups(
    self_: *const kafka_admin_DeleteConsumerGroupsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let groups = h.result.deleted_groups();
    h.ctx.string_keyed_future_map(groups.iter(), FutureCtx::void_future)
}

/// `DeleteConsumerGroupsResult.all()`: an owned `KafkaFuture<Void>` (its
/// `get` delivers `NULL`) that succeeds once every group was deleted, freed
/// with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupsResult_all(
    self_: *const kafka_admin_DeleteConsumerGroupsResult_t,
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
pub unsafe extern "C" fn kafka_admin_DeleteConsumerGroupsResult_destroy(
    self_: *mut kafka_admin_DeleteConsumerGroupsResult_t,
) {
    unsafe { destroy_result::<DeleteConsumerGroupsResult, _>(self_) }
}
