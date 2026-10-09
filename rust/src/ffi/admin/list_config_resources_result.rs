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

//! `kafka_admin_ListConfigResourcesResult_t`:
//! `org.apache.kafka.clients.admin.ListConfigResourcesResult` (CLAUDE.md §4).

use std::ffi::c_void;

use crate::admin::ListConfigResourcesResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_result, result_ref};
use crate::ffi::common::config::config_resource::{
    box_config_resource, kafka_common_config_ConfigResource_destroy, kafka_common_config_ConfigResource_t,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::box_list;

/// Opaque handle to a [`ListConfigResourcesResult`], owned and freed with
/// [`kafka_admin_ListConfigResourcesResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListConfigResourcesResult_t {
    _private: [u8; 0],
}

/// Frees a `kafka_common_config_ConfigResource_t *` element of an owned list.
unsafe fn destroy_config_resource_element(element: *mut c_void) {
    unsafe { kafka_common_config_ConfigResource_destroy(element as *mut kafka_common_config_ConfigResource_t) }
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_config_resources_result(
    result: ListConfigResourcesResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ListConfigResourcesResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(
    self_: *const kafka_admin_ListConfigResourcesResult_t,
) -> &'a ResultHandle<ListConfigResourcesResult> {
    unsafe { result_ref(self_) }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of
/// `kafka_common_config_ConfigResource_t *`, in the order the broker
/// returned them.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_all(
    self_: *const kafka_admin_ListConfigResourcesResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.all(),
        |resources| {
            let elements = resources.into_iter().map(|r| box_config_resource(r) as *mut c_void).collect();
            box_list(elements, Some(destroy_config_resource_element)) as *mut c_void
        },
        destroy_list_element,
    )
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_destroy(
    self_: *mut kafka_admin_ListConfigResourcesResult_t,
) {
    unsafe { destroy_result::<ListConfigResourcesResult, _>(self_) }
}
