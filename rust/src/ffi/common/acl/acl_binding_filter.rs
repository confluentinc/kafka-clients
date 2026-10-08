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

//! `kafka_common_acl_AclBindingFilter_t`:
//! `org.apache.kafka.common.acl.AclBindingFilter` (CLAUDE.md §4). The two
//! filter getters return borrowed handles valid as long as this filter.

use std::ffi::c_char;
use std::ptr;

use crate::common::acl::AclBindingFilter;
use crate::ffi::common::acl::access_control_entry_filter::{
    AccessControlEntryFilterInner, access_control_entry_filter_ref, kafka_common_acl_AccessControlEntryFilter_t,
};
use crate::ffi::common::acl::acl_binding::{acl_binding_ref, kafka_common_acl_AclBinding_t};
use crate::ffi::common::resource::resource_pattern_filter::{
    ResourcePatternFilterInner, kafka_common_resource_ResourcePatternFilter_t, resource_pattern_filter_ref,
};
use crate::ffi::util::into_c_string;

/// Opaque handle to an [`AclBindingFilter`].
#[repr(C)]
pub struct kafka_common_acl_AclBindingFilter_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_acl_AclBindingFilter_t`] points at: the filter plus
/// the handles of its two parts, which the getters borrow out.
pub(crate) struct AclBindingFilterInner {
    filter: AclBindingFilter,
    pattern_filter: ResourcePatternFilterInner,
    entry_filter: AccessControlEntryFilterInner,
}

impl AclBindingFilterInner {
    pub(crate) fn new(filter: AclBindingFilter) -> Self {
        let pattern_filter = ResourcePatternFilterInner::new(filter.pattern_filter().clone());
        let entry_filter = AccessControlEntryFilterInner::new(filter.entry_filter().clone());
        Self { filter, pattern_filter, entry_filter }
    }
}

unsafe fn inner_ref<'a>(filter: *const kafka_common_acl_AclBindingFilter_t) -> &'a AclBindingFilterInner {
    unsafe { &*(filter as *const AclBindingFilterInner) }
}

/// The filter behind a handle.
///
/// # Safety
///
/// `filter` must be a valid ACL-binding-filter handle.
pub(crate) unsafe fn acl_binding_filter_ref<'a>(
    filter: *const kafka_common_acl_AclBindingFilter_t,
) -> &'a AclBindingFilter {
    &unsafe { inner_ref(filter) }.filter
}

/// Hands `filter` to C as an owned handle, freed with
/// [`kafka_common_acl_AclBindingFilter_destroy`].
pub(crate) fn box_acl_binding_filter(filter: AclBindingFilter) -> *mut kafka_common_acl_AclBindingFilter_t {
    Box::into_raw(Box::new(AclBindingFilterInner::new(filter))) as *mut kafka_common_acl_AclBindingFilter_t
}

/// `new AclBindingFilter(ResourcePatternFilter patternFilter, AccessControlEntryFilter entryFilter)`:
/// both are copied, the caller keeps its handles. Owned, freed with
/// [`kafka_common_acl_AclBindingFilter_destroy`].
///
/// # Safety
///
/// `pattern_filter` must be a valid resource-pattern-filter handle and
/// `entry_filter` a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_new(
    pattern_filter: *const kafka_common_resource_ResourcePatternFilter_t,
    entry_filter: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> *mut kafka_common_acl_AclBindingFilter_t {
    box_acl_binding_filter(AclBindingFilter::new(
        unsafe { resource_pattern_filter_ref(pattern_filter) }.clone(),
        unsafe { access_control_entry_filter_ref(entry_filter) }.clone(),
    ))
}

/// `AclBindingFilter.ANY`: a filter matching every binding. Owned, as every
/// handle of this type.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclBindingFilter_any() -> *mut kafka_common_acl_AclBindingFilter_t {
    box_acl_binding_filter(AclBindingFilter::any())
}

/// `isUnknown()`: whether either part has an `UNKNOWN` component.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_is_unknown(
    self_: *const kafka_common_acl_AclBindingFilter_t,
) -> i8 {
    i8::from(unsafe { acl_binding_filter_ref(self_) }.is_unknown())
}

/// `patternFilter()`: a borrowed handle valid as long as this filter, never
/// passed to `kafka_common_resource_ResourcePatternFilter_destroy`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_pattern_filter(
    self_: *const kafka_common_acl_AclBindingFilter_t,
) -> *const kafka_common_resource_ResourcePatternFilter_t {
    unsafe { inner_ref(self_) }.pattern_filter.as_ptr()
}

/// `entryFilter()`: a borrowed handle valid as long as this filter, never
/// passed to `kafka_common_acl_AccessControlEntryFilter_destroy`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_entry_filter(
    self_: *const kafka_common_acl_AclBindingFilter_t,
) -> *const kafka_common_acl_AccessControlEntryFilter_t {
    unsafe { inner_ref(self_) }.entry_filter.as_ptr()
}

/// `matchesAtMostOne()`: whether both parts match at most one value.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_matches_at_most_one(
    self_: *const kafka_common_acl_AclBindingFilter_t,
) -> i8 {
    i8::from(unsafe { acl_binding_filter_ref(self_) }.matches_at_most_one())
}

/// `findIndefiniteField()`: a description of the first field that keeps
/// the filter from matching at most one binding, as an owned string freed
/// with `kafka_string_destroy`, or null when there is none.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_find_indefinite_field(
    self_: *const kafka_common_acl_AclBindingFilter_t,
) -> *mut c_char {
    unsafe { acl_binding_filter_ref(self_) }
        .find_indefinite_field()
        .map_or(ptr::null_mut(), |field| into_c_string(&field))
}

/// `matches(AclBinding binding)`: whether the filter matches `binding`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding-filter handle and `binding` a valid
/// ACL-binding handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_matches(
    self_: *const kafka_common_acl_AclBindingFilter_t,
    binding: *const kafka_common_acl_AclBinding_t,
) -> i8 {
    i8::from(unsafe { acl_binding_filter_ref(self_) }.matches(unsafe { acl_binding_ref(binding) }))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid ACL-binding-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_to_string(
    self_: *const kafka_common_acl_AclBindingFilter_t,
) -> *mut c_char {
    into_c_string(&unsafe { acl_binding_filter_ref(self_) }.to_string())
}

/// Frees an owned filter handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned filter handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclBindingFilter_destroy(self_: *mut kafka_common_acl_AclBindingFilter_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut AclBindingFilterInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::acl::{
        AccessControlEntry, AccessControlEntryFilter, AclBinding, AclOperation, AclPermissionType,
    };
    use crate::common::resource::{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType};
    use crate::ffi::common::acl::access_control_entry_filter::{
        box_access_control_entry_filter, kafka_common_acl_AccessControlEntryFilter_destroy,
        kafka_common_acl_AccessControlEntryFilter_principal,
    };
    use crate::ffi::common::acl::acl_binding::{box_acl_binding, kafka_common_acl_AclBinding_destroy};
    use crate::ffi::common::resource::resource_pattern_filter::{
        box_resource_pattern_filter, kafka_common_resource_ResourcePatternFilter_destroy,
        kafka_common_resource_ResourcePatternFilter_name,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn filter_copies_its_parts_and_matches_like_java() {
        let binding = box_acl_binding(AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, "orders", PatternType::Literal).unwrap(),
            AccessControlEntry::new("User:alice", "*", AclOperation::Read, AclPermissionType::Allow).unwrap(),
        ));
        let pattern_filter = ResourcePatternFilter::new(ResourceType::Topic, Some("orders".into()), PatternType::Any);
        let entry_filter =
            AccessControlEntryFilter::new(Some("User:alice".into()), None, AclOperation::Any, AclPermissionType::Allow);
        let pattern_handle = box_resource_pattern_filter(pattern_filter.clone());
        let entry_handle = box_access_control_entry_filter(entry_filter.clone());
        unsafe {
            let filter = kafka_common_acl_AclBindingFilter_new(pattern_handle, entry_handle);
            kafka_common_resource_ResourcePatternFilter_destroy(pattern_handle);
            kafka_common_acl_AccessControlEntryFilter_destroy(entry_handle);
            let expected = AclBindingFilter::new(pattern_filter, entry_filter);
            assert_eq!(*acl_binding_filter_ref(filter), expected);
            assert_eq!(kafka_common_acl_AclBindingFilter_is_unknown(filter), 0);
            assert_eq!(kafka_common_acl_AclBindingFilter_matches(filter, binding), 1);
            assert_eq!(kafka_common_acl_AclBindingFilter_matches_at_most_one(filter), 0);
            assert_eq!(
                CStr::from_ptr(kafka_common_resource_ResourcePatternFilter_name(
                    kafka_common_acl_AclBindingFilter_pattern_filter(filter)
                ))
                .to_str()
                .unwrap(),
                "orders"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_acl_AccessControlEntryFilter_principal(
                    kafka_common_acl_AclBindingFilter_entry_filter(filter)
                ))
                .to_str()
                .unwrap(),
                "User:alice"
            );
            let field = kafka_common_acl_AclBindingFilter_find_indefinite_field(filter);
            assert_eq!(
                CStr::from_ptr(field).to_str().unwrap(),
                expected.find_indefinite_field().unwrap()
            );
            kafka_string_destroy(field);
            let s = kafka_common_acl_AclBindingFilter_to_string(filter);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_common_acl_AclBindingFilter_destroy(filter);

            let any = kafka_common_acl_AclBindingFilter_any();
            assert_eq!(*acl_binding_filter_ref(any), AclBindingFilter::any());
            assert_eq!(kafka_common_acl_AclBindingFilter_matches(any, binding), 1);
            kafka_common_acl_AclBindingFilter_destroy(any);
            kafka_common_acl_AclBindingFilter_destroy(ptr::null_mut());
            kafka_common_acl_AclBinding_destroy(binding);
        }
    }
}
