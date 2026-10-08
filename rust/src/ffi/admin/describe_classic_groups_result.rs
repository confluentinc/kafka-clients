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

//! C bindings for `org.apache.kafka.clients.admin.DescribeClassicGroupsResult`.

use std::collections::{BTreeMap, HashMap};
use std::ffi::c_void;

use crate::admin::{ClassicGroupDescription, DescribeClassicGroupsResult};
use crate::ffi::admin::classic_group_description::{
    box_classic_group_description, destroy_classic_group_description_element,
};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_string_keyed_map, kafka_Map_t};

/// Opaque handle to a [`DescribeClassicGroupsResult`], owned by the caller
/// and freed with [`kafka_admin_DescribeClassicGroupsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeClassicGroupsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_describe_classic_groups_result(
    result: DescribeClassicGroupsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeClassicGroupsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> &'a ResultHandle<DescribeClassicGroupsResult> {
    unsafe { result_ref(self_) }
}

/// The C shape of `all()`'s value: an owned map, sorted by group id, of
/// owned `char *` to owned `kafka_admin_ClassicGroupDescription_t *`.
fn box_descriptions(descriptions: HashMap<String, ClassicGroupDescription>) -> *mut kafka_Map_t {
    let sorted: BTreeMap<String, ClassicGroupDescription> = descriptions.into_iter().collect();
    box_string_keyed_map(
        sorted
            .into_iter()
            .map(|(group, description)| (group, box_classic_group_description(description) as *mut c_void)),
        Some(destroy_classic_group_description_element),
    )
}

/// `DescribeClassicGroupsResult.describedGroups()`: an owned map, sorted by
/// group id, of owned `char *` group ids to owned
/// `kafka_common_KafkaFuture_t *` whose `get` delivers a
/// `kafka_admin_ClassicGroupDescription_t *` owned by the future. Freed with
/// `kafka_Map_destroy`, which frees the keys and the futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_described_groups(
    self_: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let groups = h.result.described_groups();
    h.ctx.string_keyed_future_map(groups.iter(), |ctx, future| {
        ctx.handle_future(
            future,
            |description| box_classic_group_description(description) as *mut c_void,
            destroy_classic_group_description_element,
        )
    })
}

/// `DescribeClassicGroupsResult.all()`: an owned future whose `get`
/// delivers a `kafka_Map_t *` owned by the future, of `char *` group ids
/// (sorted) to `kafka_admin_ClassicGroupDescription_t *`; the future fails
/// when any description failed. Freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_all(
    self_: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.all(),
        |groups| box_descriptions(groups) as *mut c_void,
        destroy_map_element,
    )
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_destroy(
    self_: *mut kafka_admin_DescribeClassicGroupsResult_t,
) {
    unsafe { destroy_result::<DescribeClassicGroupsResult, _>(self_) }
}
