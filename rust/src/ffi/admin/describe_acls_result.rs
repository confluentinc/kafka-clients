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

//! C bindings for `org.apache.kafka.clients.admin.DescribeAclsResult`.

use std::ffi::c_void;

use crate::admin::DescribeAclsResult;
use crate::common::acl::AclBinding;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_result, result_ref};
use crate::ffi::common::acl::acl_binding::{
    box_acl_binding, kafka_common_acl_AclBinding_destroy, kafka_common_acl_AclBinding_t,
};
use crate::ffi::kafka_future::{kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_t};
use crate::ffi::util::{box_list, kafka_List_t};

/// Opaque handle to a [`DescribeAclsResult`], owned by the caller and freed
/// with [`kafka_admin_DescribeAclsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeAclsResult_t {
    _private: [u8; 0],
}

/// What the handle holds: Java's `values()` returns the result's one
/// future itself, so the C future is built once and borrowed out, owned by
/// the handle (the Rust future lives on inside it).
struct DescribeAclsResultInner {
    values: *mut kafka_common_KafkaFuture_t,
}

impl Drop for DescribeAclsResultInner {
    fn drop(&mut self) {
        // SAFETY: `values` is the owned future handle built in
        // `box_describe_acls_result`, destroyed exactly here.
        unsafe { kafka_common_KafkaFuture_destroy(self.values) }
    }
}

/// Frees a `kafka_common_acl_AclBinding_t *` element of an owned list.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_acl_AclBinding_t *`.
unsafe fn destroy_acl_binding_element(element: *mut c_void) {
    unsafe { kafka_common_acl_AclBinding_destroy(element as *mut kafka_common_acl_AclBinding_t) }
}

/// An owned list of owned `kafka_common_acl_AclBinding_t *`, in the
/// response's order.
fn acl_binding_list(bindings: Vec<AclBinding>) -> *mut kafka_List_t {
    let elements = bindings
        .into_iter()
        .map(|binding| box_acl_binding(binding) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_acl_binding_element))
}

/// Hands `result` to C, binding the future it exposes to `ctx`.
pub(crate) fn box_describe_acls_result(
    result: DescribeAclsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeAclsResult_t {
    let values = ctx.handle_future(
        result.values(),
        |bindings| acl_binding_list(bindings) as *mut c_void,
        destroy_list_element,
    );
    box_result(DescribeAclsResultInner { values }, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_DescribeAclsResult_t) -> &'a ResultHandle<DescribeAclsResultInner> {
    unsafe { result_ref(self_) }
}

/// `DescribeAclsResult.values()`: the future of the matching ACL bindings,
/// borrowed from the result handle (valid until it is destroyed, never
/// passed to `kafka_common_KafkaFuture_destroy`). Its `get` delivers a
/// `kafka_List_t *` owned by the future, of `kafka_common_acl_AclBinding_t *`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeAclsResult_values(
    self_: *const kafka_admin_DescribeAclsResult_t,
) -> *const kafka_common_KafkaFuture_t {
    unsafe { handle(self_) }.result.values
}

/// Frees a result handle and the future borrowed from it; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeAclsResult_destroy(self_: *mut kafka_admin_DescribeAclsResult_t) {
    unsafe { destroy_result::<DescribeAclsResultInner, _>(self_) }
}
