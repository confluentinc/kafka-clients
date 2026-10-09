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

//! `kafka_admin_ClassicGroupDescription_t`:
//! `org.apache.kafka.clients.admin.ClassicGroupDescription` (CLAUDE.md §4).
//!
//! The coordinator getter returns a borrowed node valid as long as the
//! description (null for Java's null); `authorizedOperations()` is null when
//! the broker did not report them.

use std::ffi::{CString, c_char, c_void};

use crate::admin::{ClassicGroupDescription, MemberDescription};
use crate::ffi::admin::member_description::{
    box_member_description, destroy_member_description_element, kafka_admin_MemberDescription_t, member_description_ref,
};
use crate::ffi::admin::topic_description::{acl_operation_list, list_acl_operations};
use crate::ffi::common::classic_group_state::{self, kafka_common_ClassicGroupState_t};
use crate::ffi::common::node::{NodeInner, kafka_common_Node_t, optional_node, optional_node_ptr};
use crate::ffi::util::{box_list, c_str_to_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`ClassicGroupDescription`].
#[repr(C)]
pub struct kafka_admin_ClassicGroupDescription_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_ClassicGroupDescription_t`] points at: the value plus
/// the NUL-terminated strings and the coordinator handle its getters borrow
/// out.
pub(crate) struct ClassicGroupDescriptionInner {
    description: ClassicGroupDescription,
    group_id_c: CString,
    protocol_c: CString,
    protocol_data_c: CString,
    coordinator: Option<NodeInner>,
}

impl ClassicGroupDescriptionInner {
    fn new(description: ClassicGroupDescription) -> Self {
        let group_id_c = owned_c_string(description.group_id());
        let protocol_c = owned_c_string(description.protocol());
        let protocol_data_c = owned_c_string(description.protocol_data());
        let coordinator = description.coordinator().cloned().map(NodeInner::new);
        Self { description, group_id_c, protocol_c, protocol_data_c, coordinator }
    }
}

unsafe fn inner_ref<'a>(description: *const kafka_admin_ClassicGroupDescription_t) -> &'a ClassicGroupDescriptionInner {
    unsafe { &*(description as *const ClassicGroupDescriptionInner) }
}

/// Hands `description` to C as an owned handle, freed with
/// [`kafka_admin_ClassicGroupDescription_destroy`].
pub(crate) fn box_classic_group_description(
    description: ClassicGroupDescription,
) -> *mut kafka_admin_ClassicGroupDescription_t {
    Box::into_raw(Box::new(ClassicGroupDescriptionInner::new(description)))
        as *mut kafka_admin_ClassicGroupDescription_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `description` must be a live classic-group-description handle.
pub(crate) unsafe fn classic_group_description_ref<'a>(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> &'a ClassicGroupDescription {
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_ClassicGroupDescription_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned classic-group-description handle.
pub(crate) unsafe fn destroy_classic_group_description_element(element: *mut c_void) {
    unsafe { kafka_admin_ClassicGroupDescription_destroy(element as *mut kafka_admin_ClassicGroupDescription_t) }
}

/// Reads a list of `const kafka_admin_MemberDescription_t *` into owned
/// copies; null reads as empty.
unsafe fn list_member_descriptions(list: *const kafka_List_t) -> Vec<MemberDescription> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { member_description_ref(element as *const kafka_admin_MemberDescription_t) }.clone())
        .collect()
}

/// `new ClassicGroupDescription(String groupId, String protocol, String
/// protocolData, Collection<MemberDescription> members, ClassicGroupState
/// state, Node coordinator, Set<AclOperation> authorizedOperations)`.
/// `members` is a borrowed list of `const kafka_admin_MemberDescription_t *`,
/// copied (null reads as empty); `state` is a singleton; `coordinator` is
/// nullable and copied; `authorized_operations` is a borrowed list of
/// `const kafka_common_acl_AclOperation_t *` singletons, copied, null being
/// Java's null set. Owned, freed with
/// [`kafka_admin_ClassicGroupDescription_destroy`].
///
/// # Safety
///
/// The strings must be valid NUL-terminated strings, the lists null or valid
/// lists of the documented handles, `state` a classic-group-state singleton
/// and `coordinator` null or a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_new(
    group_id: *const c_char,
    protocol: *const c_char,
    protocol_data: *const c_char,
    members: *const kafka_List_t,
    state: *const kafka_common_ClassicGroupState_t,
    coordinator: *const kafka_common_Node_t,
    authorized_operations: *const kafka_List_t,
) -> *mut kafka_admin_ClassicGroupDescription_t {
    box_classic_group_description(ClassicGroupDescription::new(
        unsafe { c_str_to_string(group_id) },
        unsafe { c_str_to_string(protocol) },
        unsafe { c_str_to_string(protocol_data) },
        unsafe { list_member_descriptions(members) },
        unsafe { classic_group_state::value_of(state) },
        unsafe { optional_node(coordinator) },
        unsafe { list_acl_operations(authorized_operations) },
    ))
}

/// `groupId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_group_id(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.group_id_c.as_ptr()
}

/// `protocol()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_protocol(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.protocol_c.as_ptr()
}

/// `protocolData()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_protocol_data(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.protocol_data_c.as_ptr()
}

/// `isSimpleConsumerGroup()`: whether the protocol is empty.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_is_simple_consumer_group(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> i8 {
    i8::from(unsafe { classic_group_description_ref(self_) }.is_simple_consumer_group())
}

/// `members()`: an owned list of owned `kafka_admin_MemberDescription_t *`
/// copies, in Java's order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_members(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *mut kafka_List_t {
    let elements = unsafe { classic_group_description_ref(self_) }
        .members()
        .iter()
        .map(|m| box_member_description(m.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_member_description_element))
}

/// `state()`: the `kafka_common_ClassicGroupState_t` singleton, never freed.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_state(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const kafka_common_ClassicGroupState_t {
    classic_group_state::singleton(unsafe { classic_group_description_ref(self_) }.state())
}

/// `coordinator()`: a borrowed node valid as long as the description (never
/// passed to `kafka_common_Node_destroy`), or null for Java's null.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_coordinator(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const kafka_common_Node_t {
    optional_node_ptr(unsafe { inner_ref(self_) }.coordinator.as_ref())
}

/// `authorizedOperations()`: an owned list (freed with `kafka_List_destroy`)
/// of borrowed `const kafka_common_acl_AclOperation_t *` singletons, or null
/// when the broker did not report them (Java's null).
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_authorized_operations(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *mut kafka_List_t {
    acl_operation_list(unsafe { classic_group_description_ref(self_) }.authorized_operations())
}

/// Frees an owned classic-group-description handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_destroy(
    self_: *mut kafka_admin_ClassicGroupDescription_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ClassicGroupDescriptionInner) });
    }
}
