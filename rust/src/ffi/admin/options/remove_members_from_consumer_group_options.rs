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
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_option, kafka_List_t, list_elements, owned_c_string};

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

/// Shared access to the handle's state.
///
/// # Safety
///
/// `options` must be a live handle, kept alive for `'a`, during which no
/// setter runs on it.
unsafe fn inner_ref<'a>(
    options: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> &'a RemoveMembersFromConsumerGroupOptionsInner {
    // SAFETY: Per this function's `# Safety`, `options` is a live handle kept alive for `'a`,
    // during which no setter runs on it. Every
    // `kafka_admin_RemoveMembersFromConsumerGroupOptions_t` is the `Box::into_raw` of a
    // `Box<RemoveMembersFromConsumerGroupOptionsInner>` (`boxed`), so the shared reference is to a
    // live `RemoveMembersFromConsumerGroupOptionsInner` that nothing mutates while it is used.
    unsafe { &*(options as *const RemoveMembersFromConsumerGroupOptionsInner) }
}

/// Mutable access to the handle's state, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle, kept alive for `'a`, to which no other
/// reference is live meanwhile.
unsafe fn inner_mut<'a>(
    options: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> &'a mut RemoveMembersFromConsumerGroupOptionsInner {
    // SAFETY: Per this function's `# Safety`, `options` is a live handle kept alive for `'a`, to
    // which no other reference is live meanwhile. Every
    // `kafka_admin_RemoveMembersFromConsumerGroupOptions_t` is the `Box::into_raw` of a
    // `Box<RemoveMembersFromConsumerGroupOptionsInner>` (`boxed`), so the exclusive reference is to
    // a live `RemoveMembersFromConsumerGroupOptionsInner`.
    unsafe { &mut *(options as *mut RemoveMembersFromConsumerGroupOptionsInner) }
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle, kept alive for `'a`, during which no
/// setter runs on it.
pub(crate) unsafe fn remove_members_from_consumer_group_options_ref<'a>(
    options: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> &'a RemoveMembersFromConsumerGroupOptions {
    // SAFETY: Per this function's `# Safety`, `options` is a live handle kept alive for `'a`,
    // during which no setter runs on it, as `inner_ref` requires.
    &unsafe { inner_ref(options) }.options
}

fn boxed(options: RemoveMembersFromConsumerGroupOptions) -> *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t {
    Box::into_raw(Box::new(RemoveMembersFromConsumerGroupOptionsInner::new(options)))
        as *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t
}

/// `new RemoveMembersFromConsumerGroupOptions()`: options removing every
/// member of the group (`remove_all()` is true). Owned, freed with
/// [`kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_new()
-> *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t {
    boxed(RemoveMembersFromConsumerGroupOptions::new())
}

/// `new RemoveMembersFromConsumerGroupOptions(Collection<MemberToRemove>
/// members)`: `members` is a borrowed `kafka_List_t` of borrowed
/// `kafka_admin_MemberToRemove_t *`, copied during the call. Returns an owned
/// handle, freed with
/// [`kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy`]. When the list
/// is empty or `NULL` it stores the translation of Java\'s
/// `IllegalArgumentException` ("Invalid empty members has been provided"),
/// owned by the caller, in `*out_err` and returns `NULL`.
///
/// # Safety
///
/// `members` must be `NULL` or a valid list, not mutated during the call, whose
/// elements are live member-to-remove handles; `out_err` a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(
    members: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t {
    // SAFETY: Per this function's `# Safety`, `members` is null or a valid list, not mutated during
    // the call, which is as long as the borrowed elements are used, as `list_elements` requires.
    let members: Vec<MemberToRemove> = unsafe { list_elements(members) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element of `members` is a live
        // member-to-remove handle, as `member_to_remove_ref` requires; the borrow ends with the
        // clone, during this call, while the caller keeps the handle alive.
        .map(|&element| unsafe { member_to_remove_ref(element as *const kafka_admin_MemberToRemove_t) }.clone())
        .collect();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(RemoveMembersFromConsumerGroupOptions::with_members(members), out_err, boxed) }
}

/// `RemoveMembersFromConsumerGroupOptions.reason(String reason)`: `reason`
/// is copied during the call; `NULL` is Java's `null`, which clears a reason
/// set earlier.
///
/// # Safety
///
/// `self_` must be a live handle that no other call uses at the same time;
/// `reason` must be `NULL` or a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
    reason: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `reason` is null or a valid NUL-terminated string, as
    // `c_str_to_option` requires; it is copied.
    let reason = unsafe { c_str_to_option(reason) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle that no other call uses at
    // the same time, so no other reference to it is live while the options are replaced, as
    // `inner_mut` requires.
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
/// `self_` must be a live handle that no other call uses at the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_set_timeout_ms(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
    timeout_ms: i32,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle that no other call uses at
    // the same time, so no other reference to it is live while the options are replaced, as
    // `inner_mut` requires.
    unsafe { inner_mut(self_) }.replace(|options| options.set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms)));
}

/// `RemoveMembersFromConsumerGroupOptions.members()`: an owned `kafka_List_t`
/// of owned `kafka_admin_MemberToRemove_t *` sorted by group instance id,
/// freed with `kafka_List_destroy`; empty when all members are removed.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_members(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `remove_members_from_consumer_group_options_ref`
    // requires.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_reason(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `inner_ref` requires. The returned pointer borrows
    // the cached reason, which stays put until the handle is destroyed or the reason is set again,
    // as documented above.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_remove_all(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `remove_members_from_consumer_group_options_ref`
    // requires.
    i8::from(unsafe { remove_members_from_consumer_group_options_ref(self_) }.remove_all())
}

/// `AbstractOptions.timeoutMs()`: the timeout in milliseconds, `-1` when the
/// client's default API timeout applies.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_timeout_ms(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `remove_members_from_consumer_group_options_ref`
    // requires.
    unsafe { remove_members_from_consumer_group_options_ref(self_) }
        .timeout_ms()
        .unwrap_or(-1)
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed, that no other call
/// uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, a handle
        // not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every `kafka_admin_RemoveMembersFromConsumerGroupOptions_t` is the
        // `Box::into_raw` of a `Box<RemoveMembersFromConsumerGroupOptionsInner>` (`boxed`), so
        // `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut RemoveMembersFromConsumerGroupOptionsInner) });
    }
}
