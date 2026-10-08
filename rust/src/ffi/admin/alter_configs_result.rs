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

//! C bindings for `org.apache.kafka.clients.admin.AlterConfigsResult`.

use std::ffi::c_void;

use crate::admin::AlterConfigsResult;
use crate::common::KafkaFuture;
use crate::common::config::ConfigResource;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::config::config_resource::{
    box_config_resource, config_resource_ref, kafka_common_config_ConfigResource_destroy,
    kafka_common_config_ConfigResource_t,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to an [`AlterConfigsResult`], owned by the caller and
/// freed with [`kafka_admin_AlterConfigsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_AlterConfigsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_alter_configs_result(
    result: AlterConfigsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_AlterConfigsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_AlterConfigsResult_t) -> &'a ResultHandle<AlterConfigsResult> {
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_config_ConfigResource_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_config_ConfigResource_t *`.
unsafe fn destroy_config_resource_element(element: *mut c_void) {
    unsafe { kafka_common_config_ConfigResource_destroy(element as *mut kafka_common_config_ConfigResource_t) }
}

/// Compares two `kafka_common_config_ConfigResource_t *` keys by value.
///
/// # Safety
///
/// `a` and `b` must be valid `kafka_common_config_ConfigResource_t *`.
unsafe fn config_resource_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    unsafe {
        config_resource_ref(a as *const kafka_common_config_ConfigResource_t)
            == config_resource_ref(b as *const kafka_common_config_ConfigResource_t)
    }
}

/// `AlterConfigsResult.values()`: an owned map, sorted by the resource's
/// `toString()`, of owned `kafka_common_config_ConfigResource_t *` keys
/// (compared by value in `kafka_Map_get`) to owned
/// `kafka_common_KafkaFuture_t *` (`KafkaFuture<Void>`: `get` delivers
/// `NULL`). Freed with `kafka_Map_destroy`, which frees the keys and the
/// futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_values(
    self_: *const kafka_admin_AlterConfigsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&ConfigResource, &KafkaFuture<()>)> = h.result.values().iter().collect();
    entries.sort_by_cached_key(|(resource, _)| resource.to_string());
    h.ctx.keyed_future_map(
        entries,
        |resource| box_config_resource(resource.clone()) as *mut c_void,
        destroy_config_resource_element,
        config_resource_key_eq,
        FutureCtx::void_future,
    )
}

/// `AlterConfigsResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once every config alteration succeeded,
/// freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_all(
    self_: *const kafka_admin_AlterConfigsResult_t,
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
pub unsafe extern "C" fn kafka_admin_AlterConfigsResult_destroy(self_: *mut kafka_admin_AlterConfigsResult_t) {
    unsafe { destroy_result::<AlterConfigsResult, _>(self_) }
}
