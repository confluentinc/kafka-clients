// Copyright 2026 Confluent Inc.
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

//! C bindings for `org.apache.kafka.clients.admin.UnregisterControllerResult`.

use crate::admin::UnregisterControllerResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;

/// Opaque handle to an [`UnregisterControllerResult`], owned by the caller and
/// freed with [`kafka_admin_UnregisterControllerResult_destroy`].
#[repr(C)]
pub struct kafka_admin_UnregisterControllerResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_unregister_controller_result(
    result: UnregisterControllerResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_UnregisterControllerResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_UnregisterControllerResult_t,
) -> &'a ResultHandle<UnregisterControllerResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_UnregisterControllerResult_t` is built by
    // `box_result::<UnregisterControllerResult, _>` (in `box_unregister_controller_result`), as
    // `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// `UnregisterControllerResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that completes once the controller was unregistered, freed
/// with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UnregisterControllerResult_all(
    self_: *const kafka_admin_UnregisterControllerResult_t,
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
pub unsafe extern "C" fn kafka_admin_UnregisterControllerResult_destroy(
    self_: *mut kafka_admin_UnregisterControllerResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_UnregisterControllerResult_t` is built by `box_result::<UnregisterControllerResult, _>`,
    // as `destroy_result` requires.
    unsafe { destroy_result::<UnregisterControllerResult, _>(self_) }
}
