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

//! `kafka_admin_FenceProducersResult_t`:
//! `org.apache.kafka.clients.admin.FenceProducersResult` (CLAUDE.md §4).

use std::ffi::c_char;

use crate::admin::FenceProducersResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{c_str_to_string, kafka_Map_t};

/// Opaque handle to a [`FenceProducersResult`], owned and freed with
/// [`kafka_admin_FenceProducersResult_destroy`].
#[repr(C)]
pub struct kafka_admin_FenceProducersResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_fence_producers_result(
    result: FenceProducersResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_FenceProducersResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_FenceProducersResult_t) -> &'a ResultHandle<FenceProducersResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_FenceProducersResult_t` is built by
    // `box_result::<FenceProducersResult, _>` (in `box_fence_producers_result`), as `result_ref`
    // requires.
    unsafe { result_ref(self_) }
}

/// Return a map from transactional ID to futures which can be used to check the status of
/// individual fencings.
///
/// # Returns
///
/// A `kafka_Map_t` of owned `char *` transactional ids, sorted, to owned `kafka_common_KafkaFuture_t *` of `Void`
/// (`get` delivers `NULL` once that producer has been fenced).
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned map is owned by the caller, freed with `kafka_Map_destroy`, which frees its keys
/// and values.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_fenced_producers(
    self_: *const kafka_admin_FenceProducersResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    let fenced = h.result.fenced_producers();
    h.ctx.string_keyed_future_map(fenced.iter(), |ctx, f| ctx.void_future(f))
}

/// Returns a future that provides the producer ID generated while initializing the given
/// transaction when the request completes.
///
/// # Parameters
///
/// - `transactional_id` - the transactional id
///
/// # Returns
///
/// A future whose `get` yields an `int64_t *` owned by the future, or `NULL` on failure.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` - `transactional_id` was not part of the request
///
/// # Safety
///
/// `self_` must be a live result handle, `transactional_id` a valid
/// NUL-terminated string and `out_err` a valid slot pointing at `NULL`. The
/// returned future is owned and freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_producer_id(
    self_: *const kafka_admin_FenceProducersResult_t,
    transactional_id: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_id` is a valid NUL-terminated string,
    // as `c_str_to_string` requires; it is copied into a `String` during the call.
    let transactional_id = unsafe { c_str_to_string(transactional_id) };
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(h.result.producer_id(&transactional_id), out_err, |f| h.ctx.value_future(&f)) }
}

/// Returns a future that provides the epoch ID generated while initializing the given transaction
/// when the request completes.
///
/// # Parameters
///
/// - `transactional_id` - the transactional id
///
/// # Returns
///
/// A future whose `get` yields an `int16_t *` owned by the future, or `NULL` on failure.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` - `transactional_id` was not part of the request
///
/// # Safety
///
/// `self_` must be a live result handle, `transactional_id` a valid
/// NUL-terminated string and `out_err` a valid slot pointing at `NULL`. The
/// returned future is owned and freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_epoch_id(
    self_: *const kafka_admin_FenceProducersResult_t,
    transactional_id: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_id` is a valid NUL-terminated string,
    // as `c_str_to_string` requires; it is copied into a `String` during the call.
    let transactional_id = unsafe { c_str_to_string(transactional_id) };
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(h.result.epoch_id(&transactional_id), out_err, |f| h.ctx.value_future(&f)) }
}

/// Return a future which succeeds only if all the producer fencings succeed.
///
/// # Returns
///
/// A `KafkaFuture<Void>`; its `get` delivers `NULL` once every producer has been fenced, or the
/// first failure.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_all(
    self_: *const kafka_admin_FenceProducersResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; `NULL` is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_destroy(self_: *mut kafka_admin_FenceProducersResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_FenceProducersResult_t` is built by `box_result::<FenceProducersResult, _>`, as
    // `destroy_result` requires.
    unsafe { destroy_result::<FenceProducersResult, _>(self_) }
}
