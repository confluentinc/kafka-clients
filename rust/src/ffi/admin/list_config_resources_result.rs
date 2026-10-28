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
use crate::ffi::common::config::config_resource::{box_config_resource, destroy_config_resource_element};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::box_list;

/// Opaque handle to a [`ListConfigResourcesResult`], owned and freed with
/// [`kafka_admin_ListConfigResourcesResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListConfigResourcesResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_config_resources_result(
    result: ListConfigResourcesResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ListConfigResourcesResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_ListConfigResourcesResult_t,
) -> &'a ResultHandle<ListConfigResourcesResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_ListConfigResourcesResult_t` is built by
    // `box_result::<ListConfigResourcesResult, _>` (in `box_list_config_resources_result`), as
    // `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// Returns a future that yields either an error, or the full set of config resources. In the
/// event of a failure, the future yields nothing but the first error which occurred.
///
/// # Returns
///
/// A future; its `get` delivers a `kafka_List_t *` (owned by the future) of
/// `kafka_common_config_ConfigResource_t *`, in the order the broker returned them.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_all(
    self_: *const kafka_admin_ListConfigResourcesResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_list_element` frees exactly the list `box_list` makes, and the handle holds
    // only owned data and no thread-bound state, so it may be freed from any thread and read from
    // several at once, as `handle_future` requires.
    unsafe {
        h.ctx.handle_future(
            &h.result.all(),
            |resources| {
                let elements = resources.into_iter().map(|r| box_config_resource(r) as *mut c_void).collect();
                box_list(elements, Some(destroy_config_resource_element)) as *mut c_void
            },
            destroy_list_element,
        )
    }
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
pub unsafe extern "C" fn kafka_admin_ListConfigResourcesResult_destroy(
    self_: *mut kafka_admin_ListConfigResourcesResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_ListConfigResourcesResult_t` is built by
    // `box_result::<ListConfigResourcesResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<ListConfigResourcesResult, _>(self_) }
}
