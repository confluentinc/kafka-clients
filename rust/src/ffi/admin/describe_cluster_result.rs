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

//! C bindings for `org.apache.kafka.clients.admin.DescribeClusterResult`.

use std::ffi::c_void;
use std::ptr;

use crate::admin::DescribeClusterResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_result, result_ref};
use crate::ffi::common::acl::acl_operation::singleton as acl_operation_singleton;
use crate::ffi::common::node::{box_node, kafka_common_Node_destroy, kafka_common_Node_t, node_list};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::box_list;

/// Opaque handle to a [`DescribeClusterResult`], owned by the caller and
/// freed with [`kafka_admin_DescribeClusterResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeClusterResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_describe_cluster_result(
    result: DescribeClusterResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeClusterResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_DescribeClusterResult_t) -> &'a ResultHandle<DescribeClusterResult> {
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_Node_t *` a future delivered.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_Node_t *`.
unsafe fn destroy_node_element(element: *mut c_void) {
    unsafe { kafka_common_Node_destroy(element as *mut kafka_common_Node_t) }
}

/// `DescribeClusterResult.nodes()`: an owned future whose `get` delivers a
/// `kafka_List_t *` owned by the future, of `kafka_common_Node_t *` (the
/// brokers, in response order). Freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_nodes(
    self_: *const kafka_admin_DescribeClusterResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.nodes(),
        |nodes| node_list(&nodes) as *mut c_void,
        destroy_list_element,
    )
}

/// `DescribeClusterResult.controller()`: an owned future whose `get`
/// delivers a `kafka_common_Node_t *` owned by the future, or `NULL` when
/// the controller is unknown (Java's `null`). Freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_controller(
    self_: *const kafka_admin_DescribeClusterResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.controller(),
        |controller| controller.map_or(ptr::null_mut(), |node| box_node(node) as *mut c_void),
        destroy_node_element,
    )
}

/// `DescribeClusterResult.clusterId()`: an owned future whose `get`
/// delivers a `char *` owned by the future. Freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_cluster_id(
    self_: *const kafka_admin_DescribeClusterResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.string_future(&h.result.cluster_id())
}

/// `DescribeClusterResult.authorizedOperations()`: an owned future whose
/// `get` delivers a `kafka_List_t *` owned by the future, of the borrowed
/// `kafka_common_acl_AclOperation_t` singletons (in declaration order,
/// never freed), or `NULL` when the operations were not requested
/// (`DescribeClusterOptions.includeAuthorizedOperations(false)`, Java's
/// `null`). Freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_authorized_operations(
    self_: *const kafka_admin_DescribeClusterResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.authorized_operations(),
        |operations| {
            operations.map_or(ptr::null_mut(), |operations| {
                let elements = operations
                    .iter()
                    .map(|&op| acl_operation_singleton(op) as *mut c_void)
                    .collect();
                box_list(elements, None) as *mut c_void
            })
        },
        destroy_list_element,
    )
}

/// Frees a result handle; null is a no-op. Futures taken from the result
/// stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClusterResult_destroy(self_: *mut kafka_admin_DescribeClusterResult_t) {
    unsafe { destroy_result::<DescribeClusterResult, _>(self_) }
}
