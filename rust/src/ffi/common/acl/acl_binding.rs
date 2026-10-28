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
use crate::ffi::ffi_guard;
use crate::ffi::util::into_c_string;

/// Opaque handle to an [`AclBinding`].
#[repr(C)]
pub struct kafka_common_acl_AclBinding_t {
    _private: [u8; 0],
}

/// What a `kafka_common_acl_AclBinding_t` points at: the binding plus the
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

/// What a handle points at.
///
/// # Safety
///
/// `binding` must be a valid ACL-binding handle, alive for `'a`.
unsafe fn inner_ref<'a>(binding: *const kafka_common_acl_AclBinding_t) -> &'a AclBindingInner {
    // SAFETY: Per this function's `# Safety`, `binding` is a valid ACL-binding handle alive for
    // `'a`, so it points at a live `AclBindingInner`: the `Box::into_raw` of `box_acl_binding`, or
    // one a parent handle embeds and hands out a view on, which lives as long as that parent.
    // Nothing mutates an `AclBindingInner` while a handle to it exists, so the shared reference is
    // sound for `'a`.
    unsafe { &*(binding as *const AclBindingInner) }
}

/// The binding behind a handle.
///
/// # Safety
///
/// `binding` must be a valid ACL-binding handle, alive for `'a`.
pub(crate) unsafe fn acl_binding_ref<'a>(binding: *const kafka_common_acl_AclBinding_t) -> &'a AclBinding {
    // SAFETY: Per this function's `# Safety`, `binding` is a valid ACL-binding handle, as
    // `inner_ref` requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(binding) }.binding
}

/// Hands `binding` to C as an owned handle, freed with
/// `kafka_common_acl_AclBinding_destroy`.
pub(crate) fn box_acl_binding(binding: AclBinding) -> *mut kafka_common_acl_AclBinding_t {
    Box::into_raw(Box::new(AclBindingInner::new(binding))) as *mut kafka_common_acl_AclBinding_t
}

/// Create an instance of this class with the provided parameters.
///
/// # Parameters
///
/// - `pattern` - non-`NULL` resource pattern.
/// - `entry` - non-`NULL` entry
///
/// # Safety
///
/// `pattern` must be a valid resource-pattern handle and `entry` a valid
/// access-control-entry handle; both are copied, the caller keeps its handles.
/// The returned binding is owned, freed with `kafka_common_acl_AclBinding_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_new(
    pattern: *const kafka_common_resource_ResourcePattern_t,
    entry: *const kafka_common_acl_AccessControlEntry_t,
) -> *mut kafka_common_acl_AclBinding_t {
    box_acl_binding(AclBinding::new(
        // SAFETY: Per this function's `# Safety`, `pattern` is a valid resource-pattern handle, as
        // `resource_pattern_ref` requires; it is cloned, so the borrow lasts for this call only.
        unsafe { resource_pattern_ref(pattern) }.clone(),
        // SAFETY: Per this function's `# Safety`, `entry` is a valid access-control-entry handle,
        // as `access_control_entry_ref` requires; it is cloned, so the borrow lasts for this call
        // only.
        unsafe { access_control_entry_ref(entry) }.clone(),
    ))
}

/// `isUnknown()`.
///
/// # Returns
///
/// 1 if this binding has any UNKNOWN components, else 0.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_is_unknown(self_: *const kafka_common_acl_AclBinding_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid ACL-binding handle, as
    // `acl_binding_ref` requires; the reference is used only within this call.
    i8::from(unsafe { acl_binding_ref(self_) }.is_unknown())
}

/// `pattern()`.
///
/// # Returns
///
/// the resource pattern for this binding.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
/// The returned handle is borrowed, valid as long as the binding.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_pattern(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *const kafka_common_resource_ResourcePattern_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid ACL-binding handle, as `inner_ref`
    // requires. The pattern handle returned points at the `ResourcePatternInner` embedded in the
    // binding's `AclBindingInner`, so it lives as long as the binding; that is the rustdoc's "valid
    // as long as the binding".
    unsafe { inner_ref(self_) }.pattern.as_ptr()
}

/// `entry()`.
///
/// # Returns
///
/// the access control entry for this binding.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
/// The returned handle is borrowed, valid as long as the binding.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_entry(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *const kafka_common_acl_AccessControlEntry_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid ACL-binding handle, as `inner_ref`
    // requires. The entry handle returned points at the `AccessControlEntryInner` embedded in the
    // binding's `AclBindingInner`, so it lives as long as the binding; that is the rustdoc's "valid
    // as long as the binding".
    unsafe { inner_ref(self_) }.entry.as_ptr()
}

/// Create a filter which matches only this AclBinding.
///
/// # Returns
///
/// The filter.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
/// The returned filter is owned, freed with `kafka_common_acl_AclBindingFilter_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_to_filter(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *mut kafka_common_acl_AclBindingFilter_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid ACL-binding handle, as
    // `acl_binding_ref` requires; the result is copied into an owned value before return, so the
    // borrow lasts for this call only.
    box_acl_binding_filter(unsafe { acl_binding_ref(self_) }.to_filter())
}

/// Returns the text form of the binding.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_to_string(
    self_: *const kafka_common_acl_AclBinding_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid ACL-binding handle, as
    // `acl_binding_ref` requires; the result is copied into an owned value before return, so the
    // borrow lasts for this call only.
    into_c_string(&unsafe { acl_binding_ref(self_) }.to_string())
}

/// Frees an owned binding handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned binding handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBinding_destroy(self_: *mut kafka_common_acl_AclBinding_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of an `AclBindingInner` in
        // `box_acl_binding` (a view, which points into its parent, is never passed here), so
        // `from_raw` reclaims that allocation, once.
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
        // SAFETY: Test code: `pattern_handle` and `entry_handle` come from `box_resource_pattern`
        // and `box_access_control_entry` and are destroyed once, after the constructor copies them;
        // the binding and the filter come from this module's functions; the pattern and entry views
        // are used only while the binding is alive; every owned handle (the binding, the filter,
        // the string) is destroyed once.
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
