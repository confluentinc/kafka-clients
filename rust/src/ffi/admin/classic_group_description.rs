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
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string};

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

/// The inner value behind a handle.
///
/// # Safety
///
/// `description` must be a live classic-group-description handle, kept
/// alive for `'a`.
unsafe fn inner_ref<'a>(description: *const kafka_admin_ClassicGroupDescription_t) -> &'a ClassicGroupDescriptionInner {
    // SAFETY: Per this function's `# Safety`, `description` is a live classic-group-description
    // handle kept alive for `'a`. Every `kafka_admin_ClassicGroupDescription_t` comes from
    // `box_classic_group_description`, which boxes a `ClassicGroupDescriptionInner`, so the cast
    // yields a valid shared reference. The type has no setters, so nothing mutates it while
    // borrowed.
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
/// `description` must be a live classic-group-description handle, kept
/// alive for `'a`.
pub(crate) unsafe fn classic_group_description_ref<'a>(
    description: *const kafka_admin_ClassicGroupDescription_t,
) -> &'a ClassicGroupDescription {
    // SAFETY: Per this function's `# Safety`, `description` is a live classic-group-description
    // handle kept alive for `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_ClassicGroupDescription_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned classic-group-description handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_classic_group_description_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned classic-group-description
    // handle not yet destroyed, that no other call uses at the same time and that is not used
    // afterwards, as `kafka_admin_ClassicGroupDescription_destroy` requires.
    unsafe { kafka_admin_ClassicGroupDescription_destroy(element as *mut kafka_admin_ClassicGroupDescription_t) }
}

/// Reads a list of `const kafka_admin_MemberDescription_t *` into owned
/// copies; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are member-description
/// handles, alive and not mutated for this call.
unsafe fn list_member_descriptions(list: *const kafka_List_t) -> Vec<MemberDescription> {
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list handle, alive and not
    // mutated for this call, as `list_elements` requires; the borrowed slice does not outlive the
    // call.
    unsafe { list_elements(list) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element of `list` is a member-description
        // handle alive for this call, as `member_description_ref` requires; the borrow ends with
        // the clone.
        .map(|&element| unsafe { member_description_ref(element as *const kafka_admin_MemberDescription_t) }.clone())
        .collect()
}

/// `new ClassicGroupDescription(String groupId, String protocol, String
/// protocolData, Collection<MemberDescription> members, ClassicGroupState
/// state, Node coordinator)`, which passes `Set.of()`, an empty set, as the
/// authorized operations. `members` is a borrowed list of
/// `const kafka_admin_MemberDescription_t *`, copied (null reads as empty);
/// `state` is a singleton; `coordinator` is nullable and copied. Owned, freed
/// with [`kafka_admin_ClassicGroupDescription_destroy`].
///
/// # Safety
///
/// The strings must be valid NUL-terminated strings, `members` null or a valid
/// list of the documented handles, `state` a classic-group-state singleton and
/// `coordinator` null or a valid node handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_new(
    group_id: *const c_char,
    protocol: *const c_char,
    protocol_data: *const c_char,
    members: *const kafka_List_t,
    state: *const kafka_common_ClassicGroupState_t,
    coordinator: *const kafka_common_Node_t,
) -> *mut kafka_admin_ClassicGroupDescription_t {
    box_classic_group_description(ClassicGroupDescription::new(
        // SAFETY: Per this function's `# Safety`, `group_id` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(group_id) },
        // SAFETY: Per this function's `# Safety`, `protocol` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(protocol) },
        // SAFETY: Per this function's `# Safety`, `protocol_data` is a valid NUL-terminated string,
        // as `c_str_to_string` requires.
        unsafe { c_str_to_string(protocol_data) },
        // SAFETY: Per this function's `# Safety`, `members` is null or a valid list of the
        // documented `const kafka_admin_MemberDescription_t *` handles, alive and not mutated for
        // this call, as `list_member_descriptions` requires.
        unsafe { list_member_descriptions(members) },
        // SAFETY: Per this function's `# Safety`, `state` is a `kafka_common_ClassicGroupState_t`
        // singleton, as `classic_group_state::value_of` requires.
        unsafe { classic_group_state::value_of(state) },
        // SAFETY: Per this function's `# Safety`, `coordinator` is null or a valid node handle, as
        // `optional_node` requires.
        unsafe { optional_node(coordinator) },
    ))
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_with_authorized_operations(
    group_id: *const c_char,
    protocol: *const c_char,
    protocol_data: *const c_char,
    members: *const kafka_List_t,
    state: *const kafka_common_ClassicGroupState_t,
    coordinator: *const kafka_common_Node_t,
    authorized_operations: *const kafka_List_t,
) -> *mut kafka_admin_ClassicGroupDescription_t {
    box_classic_group_description(ClassicGroupDescription::with_authorized_operations(
        // SAFETY: Per this function's `# Safety`, `group_id` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(group_id) },
        // SAFETY: Per this function's `# Safety`, `protocol` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(protocol) },
        // SAFETY: Per this function's `# Safety`, `protocol_data` is a valid NUL-terminated string,
        // as `c_str_to_string` requires.
        unsafe { c_str_to_string(protocol_data) },
        // SAFETY: Per this function's `# Safety`, `members` is null or a valid list of the
        // documented `const kafka_admin_MemberDescription_t *` handles, alive and not mutated for
        // this call, as `list_member_descriptions` requires.
        unsafe { list_member_descriptions(members) },
        // SAFETY: Per this function's `# Safety`, `state` is a `kafka_common_ClassicGroupState_t`
        // singleton, as `classic_group_state::value_of` requires.
        unsafe { classic_group_state::value_of(state) },
        // SAFETY: Per this function's `# Safety`, `coordinator` is null or a valid node handle, as
        // `optional_node` requires.
        unsafe { optional_node(coordinator) },
        // SAFETY: Per this function's `# Safety`, `authorized_operations` is null or a valid list
        // of the documented `const kafka_common_acl_AclOperation_t *` singletons, as
        // `list_acl_operations` requires.
        unsafe { list_acl_operations(authorized_operations) },
    ))
}

/// `groupId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_group_id(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `inner_ref` requires. The returned string is a `CString` the
    // handle owns, so it lives as long as the handle.
    unsafe { inner_ref(self_) }.group_id_c.as_ptr()
}

/// `protocol()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_protocol(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `inner_ref` requires. The returned string is a `CString` the
    // handle owns, so it lives as long as the handle.
    unsafe { inner_ref(self_) }.protocol_c.as_ptr()
}

/// `protocolData()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_protocol_data(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `inner_ref` requires. The returned string is a `CString` the
    // handle owns, so it lives as long as the handle.
    unsafe { inner_ref(self_) }.protocol_data_c.as_ptr()
}

/// `isSimpleConsumerGroup()`: whether the protocol is empty.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_is_simple_consumer_group(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `classic_group_description_ref` requires.
    i8::from(unsafe { classic_group_description_ref(self_) }.is_simple_consumer_group())
}

/// `members()`: an owned list of owned `kafka_admin_MemberDescription_t *`
/// copies, in Java's order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_members(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `classic_group_description_ref` requires.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_state(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const kafka_common_ClassicGroupState_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `classic_group_description_ref` requires.
    classic_group_state::singleton(unsafe { classic_group_description_ref(self_) }.state())
}

/// `coordinator()`: a borrowed node valid as long as the description (never
/// passed to `kafka_common_Node_destroy`), or null for Java's null.
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_coordinator(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `inner_ref` requires. The returned node points at the
    // `NodeInner` the handle owns, so it lives as long as the handle.
    optional_node_ptr(unsafe { inner_ref(self_) }.coordinator.as_ref())
}

/// `authorizedOperations()`: an owned list (freed with `kafka_List_destroy`)
/// of borrowed `const kafka_common_acl_AclOperation_t *` singletons, or null
/// when the broker did not report them (Java's null).
///
/// # Safety
///
/// `self_` must be a valid classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_authorized_operations(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid classic-group-description handle,
    // kept alive for this call, as `classic_group_description_ref` requires.
    acl_operation_list(unsafe { classic_group_description_ref(self_) }.authorized_operations())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a live classic-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_to_string(
    self_: *const kafka_admin_ClassicGroupDescription_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live classic-group-description handle, kept alive for
    // this call, as `classic_group_description_ref` requires.
    into_c_string(&unsafe { classic_group_description_ref(self_) }.to_string())
}

/// Frees an owned classic-group-description handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ClassicGroupDescription_destroy(
    self_: *mut kafka_admin_ClassicGroupDescription_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every classic-group-description handle comes from
        // `box_classic_group_description`, which boxes a `ClassicGroupDescriptionInner`, so
        // `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut ClassicGroupDescriptionInner) });
    }
}
