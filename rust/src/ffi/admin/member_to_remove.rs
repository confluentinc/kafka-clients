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

//! `kafka_admin_MemberToRemove_t`:
//! `org.apache.kafka.clients.admin.MemberToRemove` (CLAUDE.md §4).

use std::ffi::{CString, c_char, c_void};

use crate::admin::MemberToRemove;
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, owned_c_string};

/// Opaque handle to a [`MemberToRemove`].
#[repr(C)]
pub struct kafka_admin_MemberToRemove_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_MemberToRemove_t`] points at: the member plus the
/// NUL-terminated instance id the string getter borrows out.
pub(crate) struct MemberToRemoveInner {
    member: MemberToRemove,
    group_instance_id_c: CString,
}

impl MemberToRemoveInner {
    pub(crate) fn new(member: MemberToRemove) -> Self {
        let group_instance_id_c = owned_c_string(member.group_instance_id());
        Self { member, group_instance_id_c }
    }
}

/// The inner value behind a handle.
///
/// # Safety
///
/// `member` must be a valid member-to-remove handle, kept alive for `'a`.
unsafe fn inner_ref<'a>(member: *const kafka_admin_MemberToRemove_t) -> &'a MemberToRemoveInner {
    // SAFETY: Per this function's `# Safety`, `member` is a valid member-to-remove handle kept
    // alive for `'a`. Every `kafka_admin_MemberToRemove_t` comes from `box_member_to_remove`, which
    // boxes a `MemberToRemoveInner`, so the cast yields a valid shared reference. The type has no
    // setters, so nothing mutates it while borrowed.
    unsafe { &*(member as *const MemberToRemoveInner) }
}

/// The member behind a handle.
///
/// # Safety
///
/// `member` must be a valid member-to-remove handle, kept alive for `'a`.
pub(crate) unsafe fn member_to_remove_ref<'a>(member: *const kafka_admin_MemberToRemove_t) -> &'a MemberToRemove {
    // SAFETY: Per this function's `# Safety`, `member` is a valid member-to-remove handle kept
    // alive for `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(member) }.member
}

/// Hands `member` to C as an owned handle, freed with
/// [`kafka_admin_MemberToRemove_destroy`].
pub(crate) fn box_member_to_remove(member: MemberToRemove) -> *mut kafka_admin_MemberToRemove_t {
    Box::into_raw(Box::new(MemberToRemoveInner::new(member))) as *mut kafka_admin_MemberToRemove_t
}

/// Frees a `kafka_admin_MemberToRemove_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned member-to-remove handle not yet destroyed,
/// that no other call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_member_to_remove_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned member-to-remove handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_MemberToRemove_destroy` requires.
    unsafe { kafka_admin_MemberToRemove_destroy(element as *mut kafka_admin_MemberToRemove_t) };
}

/// `new MemberToRemove(String groupInstanceId)`: copied. Owned, freed with
/// [`kafka_admin_MemberToRemove_destroy`].
///
/// # Safety
///
/// `group_instance_id` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberToRemove_new(
    group_instance_id: *const c_char,
) -> *mut kafka_admin_MemberToRemove_t {
    // SAFETY: Per this function's `# Safety`, `group_instance_id` is a valid NUL-terminated string,
    // as `c_str_to_string` requires.
    box_member_to_remove(MemberToRemove::new(unsafe { c_str_to_string(group_instance_id) }))
}

/// `groupInstanceId()`: a borrowed string valid as long as the member, never
/// passed to `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid member-to-remove handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberToRemove_group_instance_id(
    self_: *const kafka_admin_MemberToRemove_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid member-to-remove handle, kept
    // alive for this call, as `inner_ref` requires. The returned string is a `CString` the handle
    // owns, so it lives as long as the handle.
    unsafe { inner_ref(self_) }.group_instance_id_c.as_ptr()
}

/// Frees an owned member handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned member handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberToRemove_destroy(self_: *mut kafka_admin_MemberToRemove_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // member handle not yet destroyed, that no other call uses at the same time and that is not
        // used afterwards. Every member-to-remove handle comes from `box_member_to_remove`, which
        // boxes a `MemberToRemoveInner`, so `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut MemberToRemoveInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;

    #[test]
    fn constructor_copies_and_getter_borrows() {
        // SAFETY: Test code: the string is a static NUL-terminated literal, `member` is live until
        // its destroy, and the borrowed instance id is read before it; destroying null is a no-op.
        unsafe {
            let member = kafka_admin_MemberToRemove_new(c"instance-1".as_ptr());
            assert_eq!(*member_to_remove_ref(member), MemberToRemove::new("instance-1"));
            assert_eq!(
                CStr::from_ptr(kafka_admin_MemberToRemove_group_instance_id(member))
                    .to_str()
                    .unwrap(),
                "instance-1"
            );
            kafka_admin_MemberToRemove_destroy(member);
            kafka_admin_MemberToRemove_destroy(ptr::null_mut());
        }
    }
}
