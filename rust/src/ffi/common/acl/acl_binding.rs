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

//! `kafka_common_acl_AclBinding_t`: `org.apache.kafka.common.acl.AclBinding`
//! (CLAUDE.md §4). The pattern and entry getters return borrowed handles
//! valid as long as the binding.

use std::ffi::c_char;

use crate::common::acl::AclBinding;
use crate::ffi::common::acl::access_control_entry::{
    AccessControlEntryInner, access_control_entry_ref, kafka_common_acl_AccessControlEntry_t,
};
use crate::ffi::common::acl::acl_binding_filter::{box_acl_binding_filter, kafka_common_acl_AclBindingFilter_t};
use crate::ffi::common::resource::resource_pattern::{
    ResourcePatternInner, kafka_common_resource_ResourcePattern_t, resource_pattern_ref,
};
use crate::ffi::util::into_c_string;

/// Opaque handle to an [`AclBinding`].
#[repr(C)]
pub struct kafka_common_acl_AclBinding_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_acl_AclBinding_t`] points at: the binding plus the
/// handles of its two parts, which the getters borrow out.
pub(crate) struct AclBindingInner {
    binding: AclBinding,
    pattern: ResourcePatternInner,
    entry: AccessControlEntryInner,
}

impl AclBindingInner {
    pub(crate) fn new(binding: AclBinding) -> Self {
        let pattern = ResourcePatternInner::new(binding.pattern().clone());
        let entry = AccessControlEntryInner::new(binding.entry().clone());
        Self { binding, pattern, entry }
    }

    /// A borrowed handle on this binding, valid as long as `self` stays where
    /// it is: a result handle keeps its bindings in place and hands this out.
    // handed out by the admin result handles once the admin slice lands
    #[expect(dead_code)]
    pub(crate) fn as_ptr(&self) -> *const kafka_common_acl_AclBinding_t {
        self as *const AclBindingInner as *const kafka_common_acl_AclBinding_t
    }
}

unsafe fn inner_ref<'a>(binding: *const kafka_common_acl_AclBinding_t) -> &'a AclBindingInner {
    unsafe { &*(binding as *const AclBindingInner) }
}

/// The binding behind a handle.
///
/// # Safety
///
/// `binding` must be a valid ACL-binding handle.
pub(crate) unsafe fn acl_binding_ref<'a>(binding: *const kafka_common_acl_AclBinding_t) -> &'a AclBinding {
    &unsafe { inner_ref(binding) }.binding
}

/// Hands `binding` to C as an owned handle, freed with
/// [`kafka_common_acl_AclBinding_destroy`].
pub(crate) fn box_acl_binding(binding: AclBinding) -> *mut kafka_common_acl_AclBinding_t {
    Box::into_raw(Box::new(AclBindingInner::new(binding))) as *mut kafka_common_acl_AclBinding_t
}

/// `new AclBinding(ResourcePattern pattern, AccessControlEntry entry)`: both
/// are copied, the caller keeps its handles. Owned, freed with
/// [`kafka_common_acl_AclBinding_destroy`].
///
/// # Safety
///
/// `pattern` must be a valid resource-pattern handle and `entry` a valid
/// access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_new(
    pattern: *const kafka_common_resource_ResourcePattern_t,
    entry: *const kafka_common_acl_AccessControlEntry_t,
) -> *mut kafka_common_acl_AclBinding_t {
    box_acl_binding(AclBinding::new(
        unsafe { resource_pattern_ref(pattern) }.clone(),
        unsafe { access_control_entry_ref(entry) }.clone(),
    ))
}

/// `isUnknown()`: whether the pattern or the entry has an `UNKNOWN`
/// component.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_is_unknown(self_: *const kafka_common_acl_AclBinding_t) -> i8 {
    i8::from(unsafe { acl_binding_ref(self_) }.is_unknown())
}

/// `pattern()`: a borrowed handle valid as long as the binding, never
/// passed to `kafka_common_resource_ResourcePattern_destroy`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_pattern(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *const kafka_common_resource_ResourcePattern_t {
    unsafe { inner_ref(self_) }.pattern.as_ptr()
}

/// `entry()`: a borrowed handle valid as long as the binding, never passed
/// to `kafka_common_acl_AccessControlEntry_destroy`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_entry(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *const kafka_common_acl_AccessControlEntry_t {
    unsafe { inner_ref(self_) }.entry.as_ptr()
}

/// `toFilter()`: an owned filter matching exactly this binding, freed with
/// `kafka_common_acl_AclBindingFilter_destroy`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_to_filter(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *mut kafka_common_acl_AclBindingFilter_t {
    box_acl_binding_filter(unsafe { acl_binding_ref(self_) }.to_filter())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_to_string(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *mut c_char {
    into_c_string(&unsafe { acl_binding_ref(self_) }.to_string())
}

/// Frees an owned binding handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned binding handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_destroy(self_: *mut kafka_common_acl_AclBinding_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut AclBindingInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::acl::{AccessControlEntry, AclOperation, AclPermissionType};
    use crate::common::resource::{PatternType, ResourcePattern, ResourceType};
    use crate::ffi::common::acl::access_control_entry::{
        box_access_control_entry, kafka_common_acl_AccessControlEntry_destroy, kafka_common_acl_AccessControlEntry_host,
    };
    use crate::ffi::common::acl::acl_binding_filter::{
        acl_binding_filter_ref, kafka_common_acl_AclBindingFilter_destroy, kafka_common_acl_AclBindingFilter_matches,
    };
    use crate::ffi::common::resource::resource_pattern::{
        box_resource_pattern, kafka_common_resource_ResourcePattern_destroy, kafka_common_resource_ResourcePattern_name,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn binding_copies_its_parts_and_borrows_them_out() {
        let pattern = ResourcePattern::new(ResourceType::Topic, "orders", PatternType::Literal).unwrap();
        let entry = AccessControlEntry::new("User:alice", "*", AclOperation::Read, AclPermissionType::Allow).unwrap();
        let pattern_handle = box_resource_pattern(pattern.clone());
        let entry_handle = box_access_control_entry(entry.clone());
        unsafe {
            let binding = kafka_common_acl_AclBinding_new(pattern_handle, entry_handle);
            // The parts were copied: the caller's handles are still its own.
            kafka_common_resource_ResourcePattern_destroy(pattern_handle);
            kafka_common_acl_AccessControlEntry_destroy(entry_handle);
            let expected = AclBinding::new(pattern, entry);
            assert_eq!(*acl_binding_ref(binding), expected);
            assert_eq!(kafka_common_acl_AclBinding_is_unknown(binding), 0);
            assert_eq!(
                CStr::from_ptr(kafka_common_resource_ResourcePattern_name(kafka_common_acl_AclBinding_pattern(
                    binding
                )))
                .to_str()
                .unwrap(),
                "orders"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_acl_AccessControlEntry_host(kafka_common_acl_AclBinding_entry(
                    binding
                )))
                .to_str()
                .unwrap(),
                "*"
            );
            let s = kafka_common_acl_AclBinding_to_string(binding);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);

            let filter = kafka_common_acl_AclBinding_to_filter(binding);
            assert_eq!(*acl_binding_filter_ref(filter), expected.to_filter());
            assert_eq!(kafka_common_acl_AclBindingFilter_matches(filter, binding), 1);
            kafka_common_acl_AclBindingFilter_destroy(filter);
            kafka_common_acl_AclBinding_destroy(binding);
            kafka_common_acl_AclBinding_destroy(ptr::null_mut());
        }
    }
}
