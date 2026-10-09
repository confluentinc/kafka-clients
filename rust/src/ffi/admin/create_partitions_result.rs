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

//! C bindings for `org.apache.kafka.clients.admin.CreatePartitionsResult`.

use crate::admin::CreatePartitionsResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to a [`CreatePartitionsResult`], owned by the caller and
/// freed with [`kafka_admin_CreatePartitionsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_CreatePartitionsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_create_partitions_result(
    result: CreatePartitionsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_CreatePartitionsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_CreatePartitionsResult_t) -> &'a ResultHandle<CreatePartitionsResult> {
    unsafe { result_ref(self_) }
}

/// `CreatePartitionsResult.values()`: an owned map, sorted by topic name,
/// of owned `char *` topic names to owned `kafka_common_KafkaFuture_t *`
/// (`KafkaFuture<Void>`: `get` delivers `NULL`). Freed with
/// `kafka_Map_destroy`, which frees the keys and the futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreatePartitionsResult_values(
    self_: *const kafka_admin_CreatePartitionsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    h.ctx.string_keyed_future_map(h.result.values().iter(), FutureCtx::void_future)
}

/// `CreatePartitionsResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once every partition creation succeeded,
/// freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreatePartitionsResult_all(
    self_: *const kafka_admin_CreatePartitionsResult_t,
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
pub unsafe extern "C" fn kafka_admin_CreatePartitionsResult_destroy(self_: *mut kafka_admin_CreatePartitionsResult_t) {
    unsafe { destroy_result::<CreatePartitionsResult, _>(self_) }
}
