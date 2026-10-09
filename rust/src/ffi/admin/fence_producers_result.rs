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
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, out_slot, result_ref};
use crate::ffi::common::kafka_common_Error_t;
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

unsafe fn handle<'a>(self_: *const kafka_admin_FenceProducersResult_t) -> &'a ResultHandle<FenceProducersResult> {
    unsafe { result_ref(self_) }
}

/// `fencedProducers()`: an owned `kafka_Map_t` (freed with
/// `kafka_Map_destroy`, which frees its keys and values) of owned `char *`
/// transactional ids, sorted, to owned `kafka_common_KafkaFuture_t *` of
/// `Void` (`get` delivers `NULL` once that producer has been fenced).
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_fenced_producers(
    self_: *const kafka_admin_FenceProducersResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let fenced = h.result.fenced_producers();
    h.ctx.string_keyed_future_map(fenced.iter(), |ctx, f| ctx.void_future(f))
}

/// `producerId(String transactionalId)`: delivers through `out_producer_id`
/// an owned future (freed with `kafka_common_KafkaFuture_destroy`) whose
/// `get` yields an `int64_t *` owned by the future, or returns the owned
/// `IllegalArgumentError` when `transactional_id` was not
/// part of the request.
///
/// # Safety
///
/// `self_` must be a live result handle, `transactional_id` a valid
/// NUL-terminated string and `out_producer_id` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_producer_id(
    self_: *const kafka_admin_FenceProducersResult_t,
    transactional_id: *const c_char,
    out_producer_id: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let transactional_id = unsafe { c_str_to_string(transactional_id) };
    unsafe {
        out_slot(h.result.producer_id(&transactional_id), out_producer_id, |f| {
            h.ctx.value_future(&f)
        })
    }
}

/// `epochId(String transactionalId)`: delivers through `out_epoch_id` an
/// owned future (freed with `kafka_common_KafkaFuture_destroy`) whose `get`
/// yields an `int16_t *` owned by the future, or returns the owned
/// `IllegalArgumentError` when `transactional_id` was not
/// part of the request.
///
/// # Safety
///
/// `self_` must be a live result handle, `transactional_id` a valid
/// NUL-terminated string and `out_epoch_id` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_epoch_id(
    self_: *const kafka_admin_FenceProducersResult_t,
    transactional_id: *const c_char,
    out_epoch_id: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let transactional_id = unsafe { c_str_to_string(transactional_id) };
    unsafe { out_slot(h.result.epoch_id(&transactional_id), out_epoch_id, |f| h.ctx.value_future(&f)) }
}

/// `all()`: an owned `KafkaFuture<Void>` freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers `NULL` once every
/// producer has been fenced, or the first failure.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_all(
    self_: *const kafka_admin_FenceProducersResult_t,
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
pub unsafe extern "C" fn kafka_admin_FenceProducersResult_destroy(self_: *mut kafka_admin_FenceProducersResult_t) {
    unsafe { destroy_result::<FenceProducersResult, _>(self_) }
}
