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

//! `kafka_common_acl_AccessControlEntryFilter_t`:
//! `org.apache.kafka.common.acl.AccessControlEntryFilter` (CLAUDE.md §4). A
//! null principal or host is Java's null: "matches any".

use std::ffi::{CString, c_char};
use std::ptr;

use crate::common::acl::AccessControlEntryFilter;
use crate::ffi::common::acl::access_control_entry::{access_control_entry_ref, kafka_common_acl_AccessControlEntry_t};
use crate::ffi::common::acl::acl_operation::{
    kafka_common_acl_AclOperation_t, singleton as operation_singleton, value_of as operation_of,
};
use crate::ffi::common::acl::acl_permission_type::{
    kafka_common_acl_AclPermissionType_t, singleton as permission_type_singleton, value_of as permission_type_of,
};
use crate::ffi::util::{c_str_to_option, into_c_string, owned_c_string};

/// Opaque handle to an [`AccessControlEntryFilter`].
#[repr(C)]
pub struct kafka_common_acl_AccessControlEntryFilter_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_acl_AccessControlEntryFilter_t`] points at: the
/// filter plus the NUL-terminated strings its getters borrow out (`None`
/// for Java's null).
pub(crate) struct AccessControlEntryFilterInner {
    filter: AccessControlEntryFilter,
    principal_c: Option<CString>,
    host_c: Option<CString>,
}

impl AccessControlEntryFilterInner {
    pub(crate) fn new(filter: AccessControlEntryFilter) -> Self {
        let principal_c = filter.principal().map(owned_c_string);
        let host_c = filter.host().map(owned_c_string);
        Self { filter, principal_c, host_c }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_acl_AccessControlEntryFilter_t {
        self as *const Self as *const kafka_common_acl_AccessControlEntryFilter_t
    }
}

unsafe fn inner_ref<'a>(
    filter: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> &'a AccessControlEntryFilterInner {
    unsafe { &*(filter as *const AccessControlEntryFilterInner) }
}

/// The filter behind a handle.
///
/// # Safety
///
/// `filter` must be a valid access-control-entry-filter handle.
pub(crate) unsafe fn access_control_entry_filter_ref<'a>(
    filter: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> &'a AccessControlEntryFilter {
    &unsafe { inner_ref(filter) }.filter
}

/// Hands `filter` to C as an owned handle, freed with
/// [`kafka_common_acl_AccessControlEntryFilter_destroy`].
pub(crate) fn box_access_control_entry_filter(
    filter: AccessControlEntryFilter,
) -> *mut kafka_common_acl_AccessControlEntryFilter_t {
    Box::into_raw(Box::new(AccessControlEntryFilterInner::new(filter)))
        as *mut kafka_common_acl_AccessControlEntryFilter_t
}

/// `new AccessControlEntryFilter(String principal, String host, AclOperation operation, AclPermissionType permissionType)`:
/// a null `principal` or `host` matches any. Owned, freed with
/// [`kafka_common_acl_AccessControlEntryFilter_destroy`].
///
/// # Safety
///
/// `principal` and `host` must be null or valid NUL-terminated strings,
/// `operation` and `permission_type` singletons of their enums.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_new(
    principal: *const c_char,
    host: *const c_char,
    operation: *const kafka_common_acl_AclOperation_t,
    permission_type: *const kafka_common_acl_AclPermissionType_t,
) -> *mut kafka_common_acl_AccessControlEntryFilter_t {
    box_access_control_entry_filter(AccessControlEntryFilter::new(
        unsafe { c_str_to_option(principal) },
        unsafe { c_str_to_option(host) },
        unsafe { operation_of(operation) },
        unsafe { permission_type_of(permission_type) },
    ))
}

/// `AccessControlEntryFilter.ANY`: a filter matching every entry. Owned, as
/// every handle of this type.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AccessControlEntryFilter_any() -> *mut kafka_common_acl_AccessControlEntryFilter_t {
    box_access_control_entry_filter(AccessControlEntryFilter::any())
}

/// `principal()`: borrowed from the handle, or null for Java's null.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_principal(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }
        .principal_c
        .as_ref()
        .map_or(ptr::null(), |s| s.as_ptr())
}

/// `host()`: borrowed from the handle, or null for Java's null. The value
/// `*` means any host.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_host(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.host_c.as_ref().map_or(ptr::null(), |s| s.as_ptr())
}

/// `operation()`: the borrowed `AclOperation` singleton.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_operation(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> *const kafka_common_acl_AclOperation_t {
    operation_singleton(unsafe { access_control_entry_filter_ref(self_) }.operation())
}

/// `permissionType()`: the borrowed `AclPermissionType` singleton.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_permission_type(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> *const kafka_common_acl_AclPermissionType_t {
    permission_type_singleton(unsafe { access_control_entry_filter_ref(self_) }.permission_type())
}

/// `isUnknown()`: whether the operation or the permission type is `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_is_unknown(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> i8 {
    i8::from(unsafe { access_control_entry_filter_ref(self_) }.is_unknown())
}

/// `matches(AccessControlEntry other)`: whether the filter matches `other`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle and `other` a
/// valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_matches(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
    other: *const kafka_common_acl_AccessControlEntry_t,
) -> i8 {
    i8::from(unsafe { access_control_entry_filter_ref(self_) }.matches(unsafe { access_control_entry_ref(other) }))
}

/// `matchesAtMostOne()`: whether the filter can match at most one entry
/// (no `ANY`, `UNKNOWN` or null field).
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_matches_at_most_one(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> i8 {
    i8::from(unsafe { access_control_entry_filter_ref(self_) }.matches_at_most_one())
}

/// `findIndefiniteField()`: a description of the first field that keeps
/// the filter from matching at most one entry, as an owned string freed
/// with `kafka_string_destroy`, or null when there is none.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_find_indefinite_field(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> *mut c_char {
    unsafe { access_control_entry_filter_ref(self_) }
        .find_indefinite_field()
        .map_or(ptr::null_mut(), |field| into_c_string(&field))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_to_string(
    self_: *const kafka_common_acl_AccessControlEntryFilter_t,
) -> *mut c_char {
    into_c_string(&unsafe { access_control_entry_filter_ref(self_) }.to_string())
}

/// Frees an owned filter handle. Null is a no-op; the filter borrowed from
/// a `kafka_common_acl_AclBindingFilter_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned filter handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntryFilter_destroy(
    self_: *mut kafka_common_acl_AccessControlEntryFilter_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut AccessControlEntryFilterInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::acl::{AccessControlEntry, AclOperation, AclPermissionType};
    use crate::ffi::common::acl::access_control_entry::box_access_control_entry;
    use crate::ffi::common::acl::acl_operation::{
        kafka_common_acl_AclOperation_any, kafka_common_acl_AclOperation_read,
    };
    use crate::ffi::common::acl::acl_permission_type::{
        kafka_common_acl_AclPermissionType_allow, kafka_common_acl_AclPermissionType_any,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn null_fields_match_any_and_are_indefinite() {
        let principal = CString::new("User:alice").unwrap();
        let entry = box_access_control_entry(
            AccessControlEntry::new("User:alice", "host1", AclOperation::Read, AclPermissionType::Allow).unwrap(),
        );
        unsafe {
            let filter = kafka_common_acl_AccessControlEntryFilter_new(
                principal.as_ptr(),
                ptr::null(),
                kafka_common_acl_AclOperation_read(),
                kafka_common_acl_AclPermissionType_any(),
            );
            assert_eq!(
                *access_control_entry_filter_ref(filter),
                AccessControlEntryFilter::new(
                    Some("User:alice".into()),
                    None,
                    AclOperation::Read,
                    AclPermissionType::Any
                )
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_acl_AccessControlEntryFilter_principal(filter))
                    .to_str()
                    .unwrap(),
                "User:alice"
            );
            assert!(kafka_common_acl_AccessControlEntryFilter_host(filter).is_null());
            assert_eq!(
                kafka_common_acl_AccessControlEntryFilter_operation(filter),
                kafka_common_acl_AclOperation_read()
            );
            assert_eq!(
                kafka_common_acl_AccessControlEntryFilter_permission_type(filter),
                kafka_common_acl_AclPermissionType_any()
            );
            assert_eq!(kafka_common_acl_AccessControlEntryFilter_is_unknown(filter), 0);
            assert_eq!(kafka_common_acl_AccessControlEntryFilter_matches(filter, entry), 1);
            assert_eq!(kafka_common_acl_AccessControlEntryFilter_matches_at_most_one(filter), 0);
            let field = kafka_common_acl_AccessControlEntryFilter_find_indefinite_field(filter);
            assert_eq!(
                CStr::from_ptr(field).to_str().unwrap(),
                access_control_entry_filter_ref(filter).find_indefinite_field().unwrap()
            );
            kafka_string_destroy(field);
            let s = kafka_common_acl_AccessControlEntryFilter_to_string(filter);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                access_control_entry_filter_ref(filter).to_string()
            );
            kafka_string_destroy(s);
            kafka_common_acl_AccessControlEntryFilter_destroy(filter);

            let any = kafka_common_acl_AccessControlEntryFilter_any();
            assert_eq!(*access_control_entry_filter_ref(any), AccessControlEntryFilter::any());
            assert!(kafka_common_acl_AccessControlEntryFilter_principal(any).is_null());
            assert_eq!(
                kafka_common_acl_AccessControlEntryFilter_operation(any),
                kafka_common_acl_AclOperation_any()
            );
            assert_eq!(kafka_common_acl_AccessControlEntryFilter_matches(any, entry), 1);
            kafka_common_acl_AccessControlEntryFilter_destroy(any);

            // Every field set: definite, and `find_indefinite_field` is null.
            let host = CString::new("host1").unwrap();
            let exact = kafka_common_acl_AccessControlEntryFilter_new(
                principal.as_ptr(),
                host.as_ptr(),
                kafka_common_acl_AclOperation_read(),
                kafka_common_acl_AclPermissionType_allow(),
            );
            assert_eq!(kafka_common_acl_AccessControlEntryFilter_matches_at_most_one(exact), 1);
            assert!(kafka_common_acl_AccessControlEntryFilter_find_indefinite_field(exact).is_null());
            kafka_common_acl_AccessControlEntryFilter_destroy(exact);
            kafka_common_acl_AccessControlEntryFilter_destroy(ptr::null_mut());
            crate::ffi::common::acl::access_control_entry::kafka_common_acl_AccessControlEntry_destroy(entry);
        }
    }
}
