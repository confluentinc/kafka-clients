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

//! C bindings for `org.apache.kafka.clients.admin.CreateAclsResult`.

use std::ffi::c_void;

use crate::admin::CreateAclsResult;
use crate::common::KafkaFuture;
use crate::common::acl::AclBinding;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::acl::acl_binding::{
    acl_binding_ref, box_acl_binding, kafka_common_acl_AclBinding_destroy, kafka_common_acl_AclBinding_t,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to a [`CreateAclsResult`], owned by the caller and freed
/// with [`kafka_admin_CreateAclsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_CreateAclsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_create_acls_result(result: CreateAclsResult, ctx: &FutureCtx) -> *mut kafka_admin_CreateAclsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_CreateAclsResult_t) -> &'a ResultHandle<CreateAclsResult> {
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_acl_AclBinding_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_acl_AclBinding_t *`.
unsafe fn destroy_acl_binding_element(element: *mut c_void) {
    unsafe { kafka_common_acl_AclBinding_destroy(element as *mut kafka_common_acl_AclBinding_t) }
}

/// Compares two `kafka_common_acl_AclBinding_t *` keys by value.
///
/// # Safety
///
/// `a` and `b` must be valid `kafka_common_acl_AclBinding_t *`.
unsafe fn acl_binding_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    unsafe {
        acl_binding_ref(a as *const kafka_common_acl_AclBinding_t)
            == acl_binding_ref(b as *const kafka_common_acl_AclBinding_t)
    }
}

/// `CreateAclsResult.values()`: an owned map, sorted by the binding's
/// `toString()`, of owned `kafka_common_acl_AclBinding_t *` keys (compared
/// by value in `kafka_Map_get`) to owned `kafka_common_KafkaFuture_t *`
/// (`KafkaFuture<Void>`: `get` delivers `NULL`). Freed with
/// `kafka_Map_destroy`, which frees the keys and the futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateAclsResult_values(
    self_: *const kafka_admin_CreateAclsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&AclBinding, &KafkaFuture<()>)> = h.result.values().iter().collect();
    entries.sort_by_cached_key(|(binding, _)| binding.to_string());
    h.ctx.keyed_future_map(
        entries,
        |binding| box_acl_binding(binding.clone()) as *mut c_void,
        destroy_acl_binding_element,
        acl_binding_key_eq,
        FutureCtx::void_future,
    )
}

/// `CreateAclsResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once every ACL was created, freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateAclsResult_all(
    self_: *const kafka_admin_CreateAclsResult_t,
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
pub unsafe extern "C" fn kafka_admin_CreateAclsResult_destroy(self_: *mut kafka_admin_CreateAclsResult_t) {
    unsafe { destroy_result::<CreateAclsResult, _>(self_) }
}
