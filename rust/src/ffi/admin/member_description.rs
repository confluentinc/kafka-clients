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

//! `kafka_admin_MemberDescription_t`:
//! `org.apache.kafka.clients.admin.MemberDescription` (CLAUDE.md §4).
//!
//! Java's `Optional` fields cross as nullable pointers (`groupInstanceId`,
//! `rackId`, `targetAssignment`), as `-1` (`memberEpoch`) and as `-1`/`0`/`1`
//! (`upgraded`). The two assignment getters return borrowed handles valid as
//! long as the description.

use std::ffi::{CString, c_char, c_void};
use std::ptr;

use crate::admin::MemberDescription;
use crate::ffi::admin::member_assignment::{
    kafka_admin_MemberAssignment_t, member_assignment_ptr, member_assignment_ref,
};
use crate::ffi::util::{c_str_to_option, c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`MemberDescription`].
#[repr(C)]
pub struct kafka_admin_MemberDescription_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_MemberDescription_t`] points at: the value plus the
/// NUL-terminated strings its getters borrow out.
pub(crate) struct MemberDescriptionInner {
    description: MemberDescription,
    member_id_c: CString,
    group_instance_id_c: Option<CString>,
    rack_id_c: Option<CString>,
    client_id_c: CString,
    host_c: CString,
}

impl MemberDescriptionInner {
    fn new(description: MemberDescription) -> Self {
        let member_id_c = owned_c_string(description.consumer_id());
        let group_instance_id_c = description.group_instance_id().map(owned_c_string);
        let rack_id_c = description.rack_id().map(owned_c_string);
        let client_id_c = owned_c_string(description.client_id());
        let host_c = owned_c_string(description.host());
        Self { description, member_id_c, group_instance_id_c, rack_id_c, client_id_c, host_c }
    }
}

unsafe fn inner_ref<'a>(description: *const kafka_admin_MemberDescription_t) -> &'a MemberDescriptionInner {
    unsafe { &*(description as *const MemberDescriptionInner) }
}

/// Hands `description` to C as an owned handle, freed with
/// [`kafka_admin_MemberDescription_destroy`].
pub(crate) fn box_member_description(description: MemberDescription) -> *mut kafka_admin_MemberDescription_t {
    Box::into_raw(Box::new(MemberDescriptionInner::new(description))) as *mut kafka_admin_MemberDescription_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `description` must be a live member-description handle.
pub(crate) unsafe fn member_description_ref<'a>(
    description: *const kafka_admin_MemberDescription_t,
) -> &'a MemberDescription {
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_MemberDescription_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned member-description handle.
pub(crate) unsafe fn destroy_member_description_element(element: *mut c_void) {
    unsafe { kafka_admin_MemberDescription_destroy(element as *mut kafka_admin_MemberDescription_t) }
}

fn optional_c_str(s: Option<&CString>) -> *const c_char {
    s.map_or(ptr::null(), |s| s.as_ptr())
}

/// `new MemberDescription(String memberId, Optional<String> groupInstanceId,
/// Optional<String> rackId, String clientId, String host, MemberAssignment
/// assignment, Optional<MemberAssignment> targetAssignment, Optional<Integer>
/// memberEpoch, Optional<Boolean> upgraded)`. `group_instance_id`, `rack_id`
/// and `target_assignment` are nullable (null = `Optional.empty()`);
/// `member_epoch` is `-1` for `Optional.empty()`; `upgraded` is `-1` for
/// `Optional.empty()`, else `0`/`1`. The assignments are copied, the caller
/// keeps its handles. Owned, freed with [`kafka_admin_MemberDescription_destroy`].
///
/// # Safety
///
/// The strings must be null (where nullable) or valid NUL-terminated
/// strings; `assignment` must be a valid member-assignment handle and
/// `target_assignment` null or a valid one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_new(
    member_id: *const c_char,
    group_instance_id: *const c_char,
    rack_id: *const c_char,
    client_id: *const c_char,
    host: *const c_char,
    assignment: *const kafka_admin_MemberAssignment_t,
    target_assignment: *const kafka_admin_MemberAssignment_t,
    member_epoch: i32,
    upgraded: i8,
) -> *mut kafka_admin_MemberDescription_t {
    let target_assignment = if target_assignment.is_null() {
        None
    } else {
        Some(unsafe { member_assignment_ref(target_assignment) }.clone())
    };
    box_member_description(MemberDescription::new(
        unsafe { c_str_to_string(member_id) },
        unsafe { c_str_to_option(group_instance_id) },
        unsafe { c_str_to_option(rack_id) },
        unsafe { c_str_to_string(client_id) },
        unsafe { c_str_to_string(host) },
        unsafe { member_assignment_ref(assignment) }.clone(),
        target_assignment,
        (member_epoch >= 0).then_some(member_epoch),
        (upgraded >= 0).then_some(upgraded != 0),
    ))
}

/// `consumerId()`: the member id, borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_consumer_id(
    self_: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.member_id_c.as_ptr()
}

/// `groupInstanceId()`: borrowed from the handle, or null for
/// `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_group_instance_id(
    self_: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    optional_c_str(unsafe { inner_ref(self_) }.group_instance_id_c.as_ref())
}

/// `rackId()`: borrowed from the handle, or null for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_rack_id(
    self_: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    optional_c_str(unsafe { inner_ref(self_) }.rack_id_c.as_ref())
}

/// `clientId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_client_id(
    self_: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.client_id_c.as_ptr()
}

/// `host()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_host(
    self_: *const kafka_admin_MemberDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.host_c.as_ptr()
}

/// `assignment()`: a borrowed handle valid as long as the description, never
/// passed to `kafka_admin_MemberAssignment_destroy`.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_assignment(
    self_: *const kafka_admin_MemberDescription_t,
) -> *const kafka_admin_MemberAssignment_t {
    member_assignment_ptr(unsafe { member_description_ref(self_) }.assignment())
}

/// `targetAssignment()`: a borrowed handle valid as long as the description
/// (never passed to `kafka_admin_MemberAssignment_destroy`), or null for
/// `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_target_assignment(
    self_: *const kafka_admin_MemberDescription_t,
) -> *const kafka_admin_MemberAssignment_t {
    unsafe { member_description_ref(self_) }
        .target_assignment()
        .map_or(ptr::null(), member_assignment_ptr)
}

/// `memberEpoch()`: the member epoch, or `-1` for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_member_epoch(
    self_: *const kafka_admin_MemberDescription_t,
) -> i32 {
    unsafe { member_description_ref(self_) }.member_epoch().unwrap_or(-1)
}

/// `upgraded()`: `1` or `0` for the present value, `-1` for
/// `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_upgraded(self_: *const kafka_admin_MemberDescription_t) -> i8 {
    unsafe { member_description_ref(self_) }.upgraded().map_or(-1, i8::from)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid member-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_to_string(
    self_: *const kafka_admin_MemberDescription_t,
) -> *mut c_char {
    into_c_string(&unsafe { member_description_ref(self_) }.to_string())
}

/// Frees an owned member-description handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberDescription_destroy(self_: *mut kafka_admin_MemberDescription_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut MemberDescriptionInner) });
    }
}
