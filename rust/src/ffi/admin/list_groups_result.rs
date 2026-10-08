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

//! `kafka_admin_ListGroupsResult_t`:
//! `org.apache.kafka.clients.admin.ListGroupsResult` (CLAUDE.md §4).

use std::ffi::c_void;

use crate::admin::{GroupListing, ListGroupsResult};
use crate::common::Error;
use crate::ffi::admin::group_listing::{box_group_listing, destroy_group_listing_element};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_result, result_ref};
use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_t};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::box_list;

/// Opaque handle to a [`ListGroupsResult`], owned and freed with
/// [`kafka_admin_ListGroupsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListGroupsResult_t {
    _private: [u8; 0],
}

/// Frees a `kafka_common_Error_t *` element of an owned list.
unsafe fn destroy_error_element(element: *mut c_void) {
    unsafe { kafka_common_Error_destroy(element as *mut kafka_common_Error_t) }
}

/// Java's `Collection<GroupListing>` as an owned list of owned
/// `kafka_admin_GroupListing_t *`, in the order the brokers returned them.
fn listings_list(listings: Vec<GroupListing>) -> *mut c_void {
    let elements = listings.into_iter().map(|l| box_group_listing(l) as *mut c_void).collect();
    box_list(elements, Some(destroy_group_listing_element)) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_groups_result(result: ListGroupsResult, ctx: &FutureCtx) -> *mut kafka_admin_ListGroupsResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_ListGroupsResult_t) -> &'a ResultHandle<ListGroupsResult> {
    unsafe { result_ref(self_) }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of
/// `kafka_admin_GroupListing_t *`, or fails with the first broker's error
/// when any broker failed.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_all(
    self_: *const kafka_admin_ListGroupsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.all(), listings_list, destroy_list_element)
}

/// `valid()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of the
/// `kafka_admin_GroupListing_t *` the brokers that succeeded returned,
/// never failing because of a broker that did not.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_valid(
    self_: *const kafka_admin_ListGroupsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.valid(), listings_list, destroy_list_element)
}

/// `errors()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of
/// `kafka_common_Error_t *`, one per broker that failed.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_errors(
    self_: *const kafka_admin_ListGroupsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.errors(),
        |errors: Vec<Error>| {
            let elements = errors.into_iter().map(|e| box_error(e) as *mut c_void).collect();
            box_list(elements, Some(destroy_error_element)) as *mut c_void
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
pub unsafe extern "C" fn kafka_admin_ListGroupsResult_destroy(self_: *mut kafka_admin_ListGroupsResult_t) {
    unsafe { destroy_result::<ListGroupsResult, _>(self_) }
}
