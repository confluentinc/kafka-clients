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

//! `kafka_admin_ListGroupsOptions_t`: `org.apache.kafka.clients.admin.ListGroupsOptions`
//! (CLAUDE.md §4).
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `in_group_states(self, states)` on the handle is
//! Java's `options.inGroupStates(states)` with the result stored back. The
//! group states and types cross as borrowed `kafka_common_GroupState_t` /
//! `kafka_common_GroupType_t` singletons both ways; the Java `Set`s come
//! back as lists sorted by name so a C caller sees a deterministic order.

use std::collections::HashSet;
use std::ffi::c_void;

use crate::admin::ListGroupsOptions;
use crate::common::{GroupState, GroupType};
use crate::ffi::common::group_state::{self as group_state_ffi, kafka_common_GroupState_t};
use crate::ffi::common::group_type::{self as group_type_ffi, kafka_common_GroupType_t};
use crate::ffi::util::{box_list, kafka_List_t, list_elements, list_string_set, sorted_string_list};

/// Opaque handle to a [`ListGroupsOptions`], owned by the caller and freed
/// with [`kafka_admin_ListGroupsOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_ListGroupsOptions_t {
    _private: [u8; 0],
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle.
pub(crate) unsafe fn list_groups_options_ref<'a>(
    options: *const kafka_admin_ListGroupsOptions_t,
) -> &'a ListGroupsOptions {
    unsafe { &*(options as *const ListGroupsOptions) }
}

/// Mutable access to the options behind a handle, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle and no other reference to it may be live.
unsafe fn options_mut<'a>(options: *mut kafka_admin_ListGroupsOptions_t) -> &'a mut ListGroupsOptions {
    unsafe { &mut *(options as *mut ListGroupsOptions) }
}

fn boxed(options: ListGroupsOptions) -> *mut kafka_admin_ListGroupsOptions_t {
    Box::into_raw(Box::new(options)) as *mut kafka_admin_ListGroupsOptions_t
}

/// The group states behind a borrowed list of singletons.
///
/// # Safety
///
/// `list` must be `NULL` or a list of `kafka_common_GroupState_t` singletons.
unsafe fn list_group_states(list: *const kafka_List_t) -> HashSet<GroupState> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { group_state_ffi::value_of(element as *const kafka_common_GroupState_t) })
        .collect()
}

/// The group types behind a borrowed list of singletons.
///
/// # Safety
///
/// `list` must be `NULL` or a list of `kafka_common_GroupType_t` singletons.
unsafe fn list_group_types(list: *const kafka_List_t) -> HashSet<GroupType> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { group_type_ffi::value_of(element as *const kafka_common_GroupType_t) })
        .collect()
}

/// A Java `Set<GroupState>` as a list of borrowed singletons sorted by name.
fn group_state_list(states: &HashSet<GroupState>) -> *mut kafka_List_t {
    let mut states: Vec<GroupState> = states.iter().copied().collect();
    states.sort_unstable_by_key(|state| state.name());
    box_list(
        states
            .into_iter()
            .map(|state| group_state_ffi::singleton(state) as *mut c_void)
            .collect(),
        None,
    )
}

/// A Java `Set<GroupType>` as a list of borrowed singletons sorted by name.
fn group_type_list(types: &HashSet<GroupType>) -> *mut kafka_List_t {
    let mut types: Vec<GroupType> = types.iter().copied().collect();
    types.sort_unstable_by_key(|group_type| group_type.name());
    box_list(
        types
            .into_iter()
            .map(|group_type| group_type_ffi::singleton(group_type) as *mut c_void)
            .collect(),
        None,
    )
}

/// `new ListGroupsOptions()`: no filter, with the client's default API
/// timeout. Owned, freed with [`kafka_admin_ListGroupsOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListGroupsOptions_new() -> *mut kafka_admin_ListGroupsOptions_t {
    boxed(ListGroupsOptions::new())
}

/// `ListGroupsOptions.forConsumerGroups()`: only consumer groups (the
/// `CLASSIC` and `CONSUMER` types with the empty and `consumer` protocol
/// types). Owned, freed with [`kafka_admin_ListGroupsOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListGroupsOptions_for_consumer_groups() -> *mut kafka_admin_ListGroupsOptions_t {
    boxed(ListGroupsOptions::for_consumer_groups())
}

/// `ListGroupsOptions.forShareGroups()`: only share groups. Owned, freed
/// with [`kafka_admin_ListGroupsOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListGroupsOptions_for_share_groups() -> *mut kafka_admin_ListGroupsOptions_t {
    boxed(ListGroupsOptions::for_share_groups())
}

/// `ListGroupsOptions.forStreamsGroups()`: only streams groups. Owned, freed
/// with [`kafka_admin_ListGroupsOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListGroupsOptions_for_streams_groups() -> *mut kafka_admin_ListGroupsOptions_t {
    boxed(ListGroupsOptions::for_streams_groups())
}

/// `ListGroupsOptions.inGroupStates(Set<GroupState> groupStates)`:
/// `group_states` is a borrowed `kafka_List_t` of `kafka_common_GroupState_t`
/// singletons, copied during the call (`NULL` reads as empty: no filter).
///
/// # Safety
///
/// `self_` must be a live handle; `group_states` must be `NULL` or a list of
/// group-state singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_in_group_states(
    self_: *mut kafka_admin_ListGroupsOptions_t,
    group_states: *const kafka_List_t,
) {
    let group_states = unsafe { list_group_states(group_states) };
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).in_group_states(group_states);
}

/// `ListGroupsOptions.withProtocolTypes(Set<String> protocolTypes)`:
/// `protocol_types` is a borrowed `kafka_List_t` of `const char *`, copied
/// during the call (`NULL` reads as empty: no filter).
///
/// # Safety
///
/// `self_` must be a live handle; `protocol_types` must be `NULL` or a list
/// of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_with_protocol_types(
    self_: *mut kafka_admin_ListGroupsOptions_t,
    protocol_types: *const kafka_List_t,
) {
    let protocol_types = unsafe { list_string_set(protocol_types) };
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).with_protocol_types(protocol_types);
}

/// `ListGroupsOptions.withTypes(Set<GroupType> types)`: `types` is a
/// borrowed `kafka_List_t` of `kafka_common_GroupType_t` singletons, copied
/// during the call (`NULL` reads as empty: no filter).
///
/// # Safety
///
/// `self_` must be a live handle; `types` must be `NULL` or a list of
/// group-type singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_with_types(
    self_: *mut kafka_admin_ListGroupsOptions_t,
    types: *const kafka_List_t,
) {
    let types = unsafe { list_group_types(types) };
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).with_types(types);
}

/// `AbstractOptions.timeoutMs(Integer timeoutMs)`: `-1` (any negative value)
/// stands for Java's `null`, the client's default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_set_timeout_ms(
    self_: *mut kafka_admin_ListGroupsOptions_t,
    timeout_ms: i32,
) {
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms));
}

/// `ListGroupsOptions.groupStates()`: an owned `kafka_List_t` of borrowed
/// `kafka_common_GroupState_t` singletons sorted by name, freed with
/// `kafka_List_destroy` (the singletons are never freed).
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_group_states(
    self_: *const kafka_admin_ListGroupsOptions_t,
) -> *mut kafka_List_t {
    group_state_list(unsafe { list_groups_options_ref(self_) }.group_states())
}

/// `ListGroupsOptions.protocolTypes()`: an owned, sorted `kafka_List_t` of
/// owned `char *`, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_protocol_types(
    self_: *const kafka_admin_ListGroupsOptions_t,
) -> *mut kafka_List_t {
    sorted_string_list(unsafe { list_groups_options_ref(self_) }.protocol_types())
}

/// `ListGroupsOptions.types()`: an owned `kafka_List_t` of borrowed
/// `kafka_common_GroupType_t` singletons sorted by name, freed with
/// `kafka_List_destroy` (the singletons are never freed).
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_types(
    self_: *const kafka_admin_ListGroupsOptions_t,
) -> *mut kafka_List_t {
    group_type_list(unsafe { list_groups_options_ref(self_) }.types())
}

/// `AbstractOptions.timeoutMs()`: the timeout in milliseconds, `-1` when the
/// client's default API timeout applies.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_timeout_ms(
    self_: *const kafka_admin_ListGroupsOptions_t,
) -> i32 {
    unsafe { list_groups_options_ref(self_) }.timeout_ms().unwrap_or(-1)
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListGroupsOptions_destroy(self_: *mut kafka_admin_ListGroupsOptions_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ListGroupsOptions) });
    }
}
