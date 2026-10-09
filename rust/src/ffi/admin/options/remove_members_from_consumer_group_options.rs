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

//! `kafka_admin_RemoveMembersFromConsumerGroupOptions_t`:
//! `org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions`
//! (CLAUDE.md §4).
//!
//! Rust's fluent `set_timeout_ms` takes `self` by value and returns it; C
//! mutates the handle in place with the result stored back. The constructor
//! is fallible (Java's `IllegalArgumentException` on an empty member list),
//! so it follows the error-slot shape. The handle keeps a NUL-terminated copy
//! of the reason so its getter borrows.

use std::ffi::{CString, c_char, c_void};
use std::ptr;

use crate::admin::{MemberToRemove, RemoveMembersFromConsumerGroupOptions};
use crate::ffi::admin::member_to_remove::{
    box_member_to_remove, destroy_member_to_remove_element, kafka_admin_MemberToRemove_t, member_to_remove_ref,
};
use crate::ffi::admin::out_slot;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::util::{box_list, c_str_to_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`RemoveMembersFromConsumerGroupOptions`], owned by
/// the caller and freed with
/// [`kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_RemoveMembersFromConsumerGroupOptions_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_RemoveMembersFromConsumerGroupOptions_t`] points at:
/// the options plus the NUL-terminated reason its getter borrows out.
struct RemoveMembersFromConsumerGroupOptionsInner {
    options: RemoveMembersFromConsumerGroupOptions,
    reason_c: Option<CString>,
}

impl RemoveMembersFromConsumerGroupOptionsInner {
    fn new(options: RemoveMembersFromConsumerGroupOptions) -> Self {
        let reason_c = options.reason().map(owned_c_string);
        Self { options, reason_c }
    }

    /// Replaces the options, refreshing the cached reason.
    fn replace(
        &mut self,
        update: impl FnOnce(RemoveMembersFromConsumerGroupOptions) -> RemoveMembersFromConsumerGroupOptions,
    ) {
        *self = Self::new(update(std::mem::take(&mut self.options)));
    }
}

unsafe fn inner_ref<'a>(
    options: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> &'a RemoveMembersFromConsumerGroupOptionsInner {
    unsafe { &*(options as *const RemoveMembersFromConsumerGroupOptionsInner) }
}

/// Mutable access to the handle's state, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle and no other reference to it may be live.
unsafe fn inner_mut<'a>(
    options: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> &'a mut RemoveMembersFromConsumerGroupOptionsInner {
    unsafe { &mut *(options as *mut RemoveMembersFromConsumerGroupOptionsInner) }
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle.
pub(crate) unsafe fn remove_members_from_consumer_group_options_ref<'a>(
    options: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> &'a RemoveMembersFromConsumerGroupOptions {
    &unsafe { inner_ref(options) }.options
}

fn boxed(options: RemoveMembersFromConsumerGroupOptions) -> *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t {
    Box::into_raw(Box::new(RemoveMembersFromConsumerGroupOptionsInner::new(options)))
        as *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t
}

/// `new RemoveMembersFromConsumerGroupOptions()`: options removing every
/// member of the group (`remove_all()` is true). Owned, freed with
/// [`kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_new()
-> *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t {
    boxed(RemoveMembersFromConsumerGroupOptions::new())
}

/// `new RemoveMembersFromConsumerGroupOptions(Collection<MemberToRemove> members)`:
/// `members` is a borrowed `kafka_List_t` of borrowed
/// `kafka_admin_MemberToRemove_t *`, copied during the call. Fails with
/// Java's `IllegalArgumentException` ("Invalid empty members has been
/// provided") when the list is empty or `NULL`; on success `out_with_members`
/// receives an owned handle, freed with
/// [`kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy`]. The
/// returned error is owned by the caller, `NULL` on success.
///
/// # Safety
///
/// `members` must be `NULL` or a list of live member handles;
/// `out_with_members` must be a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(
    members: *const kafka_List_t,
    out_with_members: *mut *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> *mut kafka_common_Error_t {
    let members: Vec<MemberToRemove> = unsafe { list_elements(members) }
        .iter()
        .map(|&element| unsafe { member_to_remove_ref(element as *const kafka_admin_MemberToRemove_t) }.clone())
        .collect();
    unsafe {
        out_slot(
            RemoveMembersFromConsumerGroupOptions::with_members(members),
            out_with_members,
            boxed,
        )
    }
}

/// `RemoveMembersFromConsumerGroupOptions.reason(String reason)`: `reason`
/// is copied during the call; `NULL` reads as an empty reason, because the
/// Rust setter always records one.
///
/// # Safety
///
/// `self_` must be a live handle; `reason` must be `NULL` or a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
    reason: *const c_char,
) {
    let reason = unsafe { c_str_to_string(reason) };
    unsafe { inner_mut(self_) }.replace(|mut options| {
        options.set_reason(reason);
        options
    });
}

/// `AbstractOptions.timeoutMs(Integer timeoutMs)`: `-1` (any negative value)
/// stands for Java's `null`, the client's default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_set_timeout_ms(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
    timeout_ms: i32,
) {
    unsafe { inner_mut(self_) }.replace(|options| options.set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms)));
}

/// `RemoveMembersFromConsumerGroupOptions.members()`: an owned `kafka_List_t`
/// of owned `kafka_admin_MemberToRemove_t *` sorted by group instance id,
/// freed with `kafka_List_destroy`; empty when all members are removed.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_members(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> *mut kafka_List_t {
    let mut members: Vec<&MemberToRemove> = unsafe { remove_members_from_consumer_group_options_ref(self_) }
        .members()
        .iter()
        .collect();
    members.sort_unstable_by_key(|member| member.group_instance_id());
    box_list(
        members
            .into_iter()
            .map(|member| box_member_to_remove(member.clone()) as *mut c_void)
            .collect(),
        Some(destroy_member_to_remove_element),
    )
}

/// `RemoveMembersFromConsumerGroupOptions.reason()`: a borrowed string valid
/// until the handle is destroyed or the reason is set again, `NULL` when no
/// reason is set.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_reason(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }
        .reason_c
        .as_ref()
        .map_or(ptr::null(), |reason| reason.as_ptr())
}

/// `RemoveMembersFromConsumerGroupOptions.removeAll()`: whether every member
/// of the group is removed, that is, no specific member was given.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_remove_all(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> i8 {
    i8::from(unsafe { remove_members_from_consumer_group_options_ref(self_) }.remove_all())
}

/// `AbstractOptions.timeoutMs()`: the timeout in milliseconds, `-1` when the
/// client's default API timeout applies.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_timeout_ms(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> i32 {
    unsafe { remove_members_from_consumer_group_options_ref(self_) }
        .timeout_ms()
        .unwrap_or(-1)
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut RemoveMembersFromConsumerGroupOptionsInner) });
    }
}
