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

//! `kafka_admin_UpdateFeaturesResult_t`:
//! `org.apache.kafka.clients.admin.UpdateFeaturesResult` (CLAUDE.md §4).

use crate::admin::UpdateFeaturesResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to an [`UpdateFeaturesResult`], owned and freed with
/// [`kafka_admin_UpdateFeaturesResult_destroy`].
#[repr(C)]
pub struct kafka_admin_UpdateFeaturesResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_update_features_result(
    result: UpdateFeaturesResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_UpdateFeaturesResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_UpdateFeaturesResult_t) -> &'a ResultHandle<UpdateFeaturesResult> {
    unsafe { result_ref(self_) }
}

/// `values()`: an owned `kafka_Map_t` (freed with `kafka_Map_destroy`,
/// which frees its keys and values) of owned `char *` feature names, sorted,
/// to owned `kafka_common_KafkaFuture_t *` of `Void` (`get` delivers `NULL`
/// once that feature's update has been applied).
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UpdateFeaturesResult_values(
    self_: *const kafka_admin_UpdateFeaturesResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    h.ctx
        .string_keyed_future_map(h.result.values().iter(), |ctx, f| ctx.void_future(f))
}

/// `all()`: an owned `KafkaFuture<Void>` freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers `NULL` once every
/// feature update has been applied, or the first failure.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UpdateFeaturesResult_all(
    self_: *const kafka_admin_UpdateFeaturesResult_t,
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
pub unsafe extern "C" fn kafka_admin_UpdateFeaturesResult_destroy(self_: *mut kafka_admin_UpdateFeaturesResult_t) {
    unsafe { destroy_result::<UpdateFeaturesResult, _>(self_) }
}
