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

//! C bindings for `org.apache.kafka.clients.admin.AbortTransactionResult`.

use crate::admin::AbortTransactionResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;

/// Opaque handle to an [`AbortTransactionResult`], owned by the caller and
/// freed with [`kafka_admin_AbortTransactionResult_destroy`].
#[repr(C)]
pub struct kafka_admin_AbortTransactionResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_abort_transaction_result(
    result: AbortTransactionResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_AbortTransactionResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_AbortTransactionResult_t) -> &'a ResultHandle<AbortTransactionResult> {
    unsafe { result_ref(self_) }
}

/// `AbortTransactionResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that completes once the transaction was aborted, freed
/// with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionResult_all(
    self_: *const kafka_admin_AbortTransactionResult_t,
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
pub unsafe extern "C" fn kafka_admin_AbortTransactionResult_destroy(self_: *mut kafka_admin_AbortTransactionResult_t) {
    unsafe { destroy_result::<AbortTransactionResult, _>(self_) }
}
