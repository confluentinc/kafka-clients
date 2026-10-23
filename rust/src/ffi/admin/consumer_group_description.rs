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

//! `kafka_admin_ConsumerGroupDescription_t`:
//! `org.apache.kafka.clients.admin.ConsumerGroupDescription` (CLAUDE.md §4).
//!
//! The coordinator getter returns a borrowed node valid as long as the
//! description (null for Java's null); `authorizedOperations()` is null when
//! the broker did not report them; `Optional<Integer> groupEpoch()` and
//! `targetAssignmentEpoch()` cross as `-1` when empty.

use std::ffi::{CString, c_char, c_void};

use crate::admin::{ConsumerGroupDescription, MemberDescription};
use crate::ffi::admin::member_description::{
    box_member_description, destroy_member_description_element, kafka_admin_MemberDescription_t, member_description_ref,
};
use crate::ffi::admin::topic_description::{acl_operation_list, list_acl_operations};
use crate::ffi::common::group_state::{self, kafka_common_GroupState_t};
use crate::ffi::common::group_type::{self, kafka_common_GroupType_t};
use crate::ffi::common::node::{NodeInner, kafka_common_Node_t, optional_node, optional_node_ptr};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`ConsumerGroupDescription`].
#[repr(C)]
pub struct kafka_admin_ConsumerGroupDescription_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_ConsumerGroupDescription_t`] points at: the value plus
/// the NUL-terminated strings and the coordinator handle its getters borrow
/// out.
pub(crate) struct ConsumerGroupDescriptionInner {
    description: ConsumerGroupDescription,
    group_id_c: CString,
    partition_assignor_c: CString,
    coordinator: Option<NodeInner>,
}

impl ConsumerGroupDescriptionInner {
    fn new(description: ConsumerGroupDescription) -> Self {
        let group_id_c = owned_c_string(description.group_id());
        let partition_assignor_c = owned_c_string(description.partition_assignor());
        let coordinator = description.coordinator().cloned().map(NodeInner::new);
        Self { description, group_id_c, partition_assignor_c, coordinator }
    }
}

/// The inner value behind a handle.
///
/// # Safety
///
/// `description` must be a live consumer-group-description handle, kept
/// alive for `'a`.
unsafe fn inner_ref<'a>(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> &'a ConsumerGroupDescriptionInner {
    // SAFETY: Per this function's `# Safety`, `description` is a live consumer-group-description
    // handle kept alive for `'a`. Every `kafka_admin_ConsumerGroupDescription_t` comes from
    // `box_consumer_group_description`, which boxes a `ConsumerGroupDescriptionInner`, so the cast
    // yields a valid shared reference. The type has no setters, so nothing mutates it while
    // borrowed.
    unsafe { &*(description as *const ConsumerGroupDescriptionInner) }
}

/// Hands `description` to C as an owned handle, freed with
/// [`kafka_admin_ConsumerGroupDescription_destroy`].
pub(crate) fn box_consumer_group_description(
    description: ConsumerGroupDescription,
) -> *mut kafka_admin_ConsumerGroupDescription_t {
    Box::into_raw(Box::new(ConsumerGroupDescriptionInner::new(description)))
        as *mut kafka_admin_ConsumerGroupDescription_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `description` must be a live consumer-group-description handle, kept
/// alive for `'a`.
pub(crate) unsafe fn consumer_group_description_ref<'a>(
    description: *const kafka_admin_ConsumerGroupDescription_t,
) -> &'a ConsumerGroupDescription {
    // SAFETY: Per this function's `# Safety`, `description` is a live consumer-group-description
    // handle kept alive for `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_ConsumerGroupDescription_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned consumer-group-description handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_consumer_group_description_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned consumer-group-description
    // handle not yet destroyed, that no other call uses at the same time and that is not used
    // afterwards, as `kafka_admin_ConsumerGroupDescription_destroy` requires.
    unsafe { kafka_admin_ConsumerGroupDescription_destroy(element as *mut kafka_admin_ConsumerGroupDescription_t) }
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

/// A Java `Collection<MemberDescription>` as an owned list of owned
/// `kafka_admin_MemberDescription_t *` copies.
fn member_description_list(members: &[MemberDescription]) -> *mut kafka_List_t {
    let elements = members
        .iter()
        .map(|m| box_member_description(m.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_member_description_element))
}

/// `new ConsumerGroupDescription(String groupId, boolean isSimpleConsumerGroup,
/// Collection<MemberDescription> members, String partitionAssignor, GroupType
/// type, GroupState groupState, Node coordinator, Set<AclOperation>
/// authorizedOperations, Optional<Integer> groupEpoch, Optional<Integer>
/// targetAssignmentEpoch)`. `members` is a borrowed list of
/// `const kafka_admin_MemberDescription_t *`, copied (null reads as empty);
/// `group_type` and `group_state` are singletons; `coordinator` is nullable
/// and copied; `authorized_operations` is a borrowed list of
/// `const kafka_common_acl_AclOperation_t *` singletons, copied, null being
/// Java's null set; the two epochs are `-1` for `Optional.empty()`. Owned,
/// freed with [`kafka_admin_ConsumerGroupDescription_destroy`].
///
/// # Safety
///
/// The strings must be valid NUL-terminated strings, the lists null or valid
/// lists of the documented handles, the enums singletons of their types and
/// `coordinator` null or a valid node handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_new(
    group_id: *const c_char,
    is_simple_consumer_group: i8,
    members: *const kafka_List_t,
    partition_assignor: *const c_char,
    group_type: *const kafka_common_GroupType_t,
    group_state: *const kafka_common_GroupState_t,
    coordinator: *const kafka_common_Node_t,
    authorized_operations: *const kafka_List_t,
    group_epoch: i32,
    target_assignment_epoch: i32,
) -> *mut kafka_admin_ConsumerGroupDescription_t {
    box_consumer_group_description(ConsumerGroupDescription::new(
        // SAFETY: Per this function's `# Safety`, `group_id` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(group_id) },
        is_simple_consumer_group != 0,
        // SAFETY: Per this function's `# Safety`, `members` is null or a valid list of the
        // documented `const kafka_admin_MemberDescription_t *` handles, alive and not mutated for
        // this call, as `list_member_descriptions` requires.
        unsafe { list_member_descriptions(members) },
        // SAFETY: Per this function's `# Safety`, `partition_assignor` is a valid NUL-terminated
        // string, as `c_str_to_string` requires.
        unsafe { c_str_to_string(partition_assignor) },
        // SAFETY: Per this function's `# Safety`, `group_type` is a `kafka_common_GroupType_t`
        // singleton, as `group_type::value_of` requires.
        unsafe { group_type::value_of(group_type) },
        // SAFETY: Per this function's `# Safety`, `group_state` is a `kafka_common_GroupState_t`
        // singleton, as `group_state::value_of` requires.
        unsafe { group_state::value_of(group_state) },
        // SAFETY: Per this function's `# Safety`, `coordinator` is null or a valid node handle, as
        // `optional_node` requires.
        unsafe { optional_node(coordinator) },
        // SAFETY: Per this function's `# Safety`, `authorized_operations` is null or a valid list
        // of the documented `const kafka_common_acl_AclOperation_t *` singletons, as
        // `list_acl_operations` requires.
        unsafe { list_acl_operations(authorized_operations) },
        (group_epoch >= 0).then_some(group_epoch),
        (target_assignment_epoch >= 0).then_some(target_assignment_epoch),
    ))
}

/// `groupId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_group_id(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `inner_ref` requires. The returned string is a `CString` the
    // handle owns, so it lives as long as the handle.
    unsafe { inner_ref(self_) }.group_id_c.as_ptr()
}

/// `isSimpleConsumerGroup()`.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_is_simple_consumer_group(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `consumer_group_description_ref` requires.
    i8::from(unsafe { consumer_group_description_ref(self_) }.is_simple_consumer_group())
}

/// `members()`: an owned list of owned `kafka_admin_MemberDescription_t *`
/// copies, in Java's order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_members(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `consumer_group_description_ref` requires.
    member_description_list(unsafe { consumer_group_description_ref(self_) }.members())
}

/// `partitionAssignor()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_partition_assignor(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `inner_ref` requires. The returned string is a `CString` the
    // handle owns, so it lives as long as the handle.
    unsafe { inner_ref(self_) }.partition_assignor_c.as_ptr()
}

/// `type()`: the `kafka_common_GroupType_t` singleton, never freed.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_type(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const kafka_common_GroupType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `consumer_group_description_ref` requires.
    group_type::singleton(unsafe { consumer_group_description_ref(self_) }.r#type())
}

/// `groupState()`: the `kafka_common_GroupState_t` singleton, never freed.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_group_state(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const kafka_common_GroupState_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `consumer_group_description_ref` requires.
    group_state::singleton(unsafe { consumer_group_description_ref(self_) }.group_state())
}

/// `coordinator()`: a borrowed node valid as long as the description (never
/// passed to `kafka_common_Node_destroy`), or null for Java's null.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_coordinator(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
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
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_authorized_operations(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `consumer_group_description_ref` requires.
    acl_operation_list(unsafe { consumer_group_description_ref(self_) }.authorized_operations())
}

/// `groupEpoch()`: the epoch, or `-1` for `Optional.empty()`.
///
/// A caught Rust panic returns -2, which is no group epoch.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_group_epoch(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `consumer_group_description_ref` requires.
    unsafe { consumer_group_description_ref(self_) }.group_epoch().unwrap_or(-1)
}

/// `targetAssignmentEpoch()`: the epoch, or `-1` for `Optional.empty()`.
///
/// A caught Rust panic returns -2, which is no assignment epoch.
///
/// # Safety
///
/// `self_` must be a valid consumer-group-description handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_target_assignment_epoch(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid consumer-group-description handle,
    // kept alive for this call, as `consumer_group_description_ref` requires.
    unsafe { consumer_group_description_ref(self_) }
        .target_assignment_epoch()
        .unwrap_or(-1)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a live consumer-group-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_to_string(
    self_: *const kafka_admin_ConsumerGroupDescription_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live consumer-group-description handle, kept alive for
    // this call, as `consumer_group_description_ref` requires.
    into_c_string(&unsafe { consumer_group_description_ref(self_) }.to_string())
}

/// Frees an owned consumer-group-description handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConsumerGroupDescription_destroy(
    self_: *mut kafka_admin_ConsumerGroupDescription_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every consumer-group-description handle comes from
        // `box_consumer_group_description`, which boxes a `ConsumerGroupDescriptionInner`, so
        // `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut ConsumerGroupDescriptionInner) });
    }
}
