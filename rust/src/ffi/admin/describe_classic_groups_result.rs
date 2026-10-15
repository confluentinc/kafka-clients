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
use std::ffi::{c_char, c_void};

use crate::admin::{ClassicGroupDescription, DescribeClassicGroupsResult};
use crate::ffi::admin::classic_group_description::{
    box_classic_group_description, classic_group_description_ref, destroy_classic_group_description_element,
    kafka_admin_ClassicGroupDescription_t,
};
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, resolved_future, result_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_string_keyed_map, c_str_to_string, kafka_Map_t, map_entries};

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
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> &'a ResultHandle<DescribeClassicGroupsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeClassicGroupsResult_t` is built by
    // `box_result::<DescribeClassicGroupsResult, _>` (in `box_describe_classic_groups_result`), as
    // `result_ref` requires.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_described_groups(
    self_: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    let groups = h.result.described_groups();
    h.ctx.string_keyed_future_map(groups.iter(), |ctx, future| {
        // SAFETY: `destroy_classic_group_description_element` frees exactly the handle
        // `box_classic_group_description` makes, and the handle holds only owned data and no
        // thread-bound state, so it may be freed from any thread and read from several at once, as
        // `handle_future` requires.
        unsafe {
            ctx.handle_future(
                future,
                |description| box_classic_group_description(description) as *mut c_void,
                destroy_classic_group_description_element,
            )
        }
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_all(
    self_: *const kafka_admin_DescribeClassicGroupsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `box_descriptions` makes through
    // `box_string_keyed_map`, and the handle holds only owned data and no thread-bound state, so it
    // may be freed from any thread and read from several at once, as `handle_future` requires.
    unsafe {
        h.ctx.handle_future(
            &h.result.all(),
            |groups| box_descriptions(groups) as *mut c_void,
            destroy_map_element,
        )
    }
}

/// `new DescribeClassicGroupsResult(Map<String, KafkaFuture<ClassicGroupDescription>> futures)`:
/// `futures` is a borrowed map of `const char *` group ids to
/// `const kafka_common_KafkaFuture_t *` whose value is a `kafka_admin_ClassicGroupDescription_t *`,
/// copied during the call; the caller keeps ownership of the map, its keys,
/// its futures and their values. Only futures that are already complete
/// (`kafka_common_KafkaFuture_is_done`) can be copied across the boundary:
/// the outcome of each is taken as it stands, and a future still pending
/// stands as one failed with the translation of `IllegalArgumentException`.
/// Owned, freed with [`kafka_admin_DescribeClassicGroupsResult_destroy`].
///
/// # Safety
///
/// `futures` must be null or a valid map whose keys are valid NUL-terminated strings
/// and whose values are valid future handles, each successful one's value a
/// valid `kafka_admin_ClassicGroupDescription_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_new(
    futures: *const kafka_Map_t,
) -> *mut kafka_admin_DescribeClassicGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `futures` is null or a valid map handle. The borrow
    // of its entries ends with this call, while the caller keeps the map alive, and nothing mutates
    // it meanwhile: `kafka_Map_put`, its one mutator, requires that no other call uses the map at
    // the same time. That is what `map_entries` requires.
    let futures = unsafe { map_entries(futures) }
        .iter()
        .map(|&(k, v)| {
            (
                // SAFETY: Per this function's `# Safety`, the key `k` is a valid NUL-terminated
                // string, which the caller keeps alive for this call, as `c_str_to_string`
                // requires; it is copied.
                unsafe { c_str_to_string(k as *const c_char) },
                // SAFETY: Per this function's `# Safety`, the value `v` is a valid future handle,
                // alive for this call, and a successful one's value a valid `kafka_admin_ClassicGroupDescription_t *`
                // borrowed from it, as `classic_group_description_ref` requires; the value is cloned, so the borrow
                // ends within the call, as `resolved_future` requires.
                unsafe {
                    resolved_future(v as *const kafka_common_KafkaFuture_t, "DescribeClassicGroupsResult", |value| {
                        classic_group_description_ref(value as *const kafka_admin_ClassicGroupDescription_t).clone()
                    })
                },
            )
        })
        .collect();
    box_result(DescribeClassicGroupsResult::new(futures), &FutureCtx::detached())
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClassicGroupsResult_destroy(
    self_: *mut kafka_admin_DescribeClassicGroupsResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeClassicGroupsResult_t` is built by
    // `box_result::<DescribeClassicGroupsResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<DescribeClassicGroupsResult, _>(self_) }
}
