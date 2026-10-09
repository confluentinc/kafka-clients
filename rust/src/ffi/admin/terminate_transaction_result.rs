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

//! `kafka_admin_TerminateTransactionResult_t`:
//! `org.apache.kafka.clients.admin.TerminateTransactionResult` (CLAUDE.md §4).

use crate::admin::TerminateTransactionResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;

/// Opaque handle to a [`TerminateTransactionResult`], owned and freed with
/// [`kafka_admin_TerminateTransactionResult_destroy`].
#[repr(C)]
pub struct kafka_admin_TerminateTransactionResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_terminate_transaction_result(
    result: TerminateTransactionResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_TerminateTransactionResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(
    self_: *const kafka_admin_TerminateTransactionResult_t,
) -> &'a ResultHandle<TerminateTransactionResult> {
    unsafe { result_ref(self_) }
}

/// `result()`: an owned `KafkaFuture<Void>` freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers `NULL` once the
/// transaction has been terminated.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TerminateTransactionResult_result(
    self_: *const kafka_admin_TerminateTransactionResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.result())
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TerminateTransactionResult_destroy(
    self_: *mut kafka_admin_TerminateTransactionResult_t,
) {
    unsafe { destroy_result::<TerminateTransactionResult, _>(self_) }
}
