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

//! `kafka_common_acl_AccessControlEntry_t`:
//! `org.apache.kafka.common.acl.AccessControlEntry` (CLAUDE.md §4).

use std::ffi::{CString, c_char};
use std::ptr;

use crate::common::acl::AccessControlEntry;
use crate::ffi::common::acl::access_control_entry_filter::{
    box_access_control_entry_filter, kafka_common_acl_AccessControlEntryFilter_t,
};
use crate::ffi::common::acl::acl_operation::{
    kafka_common_acl_AclOperation_t, singleton as operation_singleton, value_of as operation_of,
};
use crate::ffi::common::acl::acl_permission_type::{
    kafka_common_acl_AclPermissionType_t, singleton as permission_type_singleton, value_of as permission_type_of,
};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to an [`AccessControlEntry`].
#[repr(C)]
pub struct kafka_common_acl_AccessControlEntry_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_acl_AccessControlEntry_t`] points at: the entry plus
/// the NUL-terminated strings its getters borrow out.
pub(crate) struct AccessControlEntryInner {
    entry: AccessControlEntry,
    principal_c: CString,
    host_c: CString,
}

impl AccessControlEntryInner {
    pub(crate) fn new(entry: AccessControlEntry) -> Self {
        let principal_c = owned_c_string(entry.principal());
        let host_c = owned_c_string(entry.host());
        Self { entry, principal_c, host_c }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_acl_AccessControlEntry_t {
        self as *const Self as *const kafka_common_acl_AccessControlEntry_t
    }
}

unsafe fn inner_ref<'a>(entry: *const kafka_common_acl_AccessControlEntry_t) -> &'a AccessControlEntryInner {
    unsafe { &*(entry as *const AccessControlEntryInner) }
}

/// The entry behind a handle.
///
/// # Safety
///
/// `entry` must be a valid access-control-entry handle.
pub(crate) unsafe fn access_control_entry_ref<'a>(
    entry: *const kafka_common_acl_AccessControlEntry_t,
) -> &'a AccessControlEntry {
    &unsafe { inner_ref(entry) }.entry
}

/// Hands `entry` to C as an owned handle, freed with
/// [`kafka_common_acl_AccessControlEntry_destroy`].
pub(crate) fn box_access_control_entry(entry: AccessControlEntry) -> *mut kafka_common_acl_AccessControlEntry_t {
    Box::into_raw(Box::new(AccessControlEntryInner::new(entry))) as *mut kafka_common_acl_AccessControlEntry_t
}

/// `new AccessControlEntry(String principal, String host, AclOperation operation, AclPermissionType permissionType)`:
/// delivers the owned entry through `out_entry`, or returns the owned
/// `IllegalArgumentException` translation when `operation` or
/// `permission_type` is `ANY`.
///
/// # Safety
///
/// `principal` and `host` must be valid NUL-terminated strings, `operation`
/// and `permission_type` singletons of their enums and `out_entry` a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_new(
    principal: *const c_char,
    host: *const c_char,
    operation: *const kafka_common_acl_AclOperation_t,
    permission_type: *const kafka_common_acl_AclPermissionType_t,
    out_entry: *mut *mut kafka_common_acl_AccessControlEntry_t,
) -> *mut kafka_common_Error_t {
    let entry = AccessControlEntry::new(
        unsafe { c_str_to_string(principal) },
        unsafe { c_str_to_string(host) },
        unsafe { operation_of(operation) },
        unsafe { permission_type_of(permission_type) },
    );
    match entry {
        Ok(entry) => {
            unsafe { *out_entry = box_access_control_entry(entry) };
            ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `principal()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_principal(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.principal_c.as_ptr()
}

/// `host()`: borrowed from the handle; `*` means any host.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_host(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.host_c.as_ptr()
}

/// `operation()`: the borrowed `AclOperation` singleton.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_operation(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const kafka_common_acl_AclOperation_t {
    operation_singleton(unsafe { access_control_entry_ref(self_) }.operation())
}

/// `permissionType()`: the borrowed `AclPermissionType` singleton.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_permission_type(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const kafka_common_acl_AclPermissionType_t {
    permission_type_singleton(unsafe { access_control_entry_ref(self_) }.permission_type())
}

/// `toFilter()`: an owned filter matching exactly this entry, freed with
/// `kafka_common_acl_AccessControlEntryFilter_destroy`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_to_filter(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *mut kafka_common_acl_AccessControlEntryFilter_t {
    box_access_control_entry_filter(unsafe { access_control_entry_ref(self_) }.to_filter())
}

/// `isUnknown()`: whether the operation or the permission type is `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_is_unknown(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> i8 {
    i8::from(unsafe { access_control_entry_ref(self_) }.is_unknown())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_to_string(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *mut c_char {
    into_c_string(&unsafe { access_control_entry_ref(self_) }.entry_to_string())
}

trait EntryToString {
    fn entry_to_string(&self) -> String;
}

impl EntryToString for AccessControlEntry {
    fn entry_to_string(&self) -> String {
        self.to_string()
    }
}

/// Frees an owned entry handle. Null is a no-op; the entry borrowed from a
/// `kafka_common_acl_AclBinding_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned entry handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_destroy(
    self_: *mut kafka_common_acl_AccessControlEntry_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut AccessControlEntryInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::acl::{AclOperation, AclPermissionType};
    use crate::ffi::common::acl::access_control_entry_filter::{
        access_control_entry_filter_ref, kafka_common_acl_AccessControlEntryFilter_destroy,
        kafka_common_acl_AccessControlEntryFilter_matches,
    };
    use crate::ffi::common::acl::acl_operation::{
        kafka_common_acl_AclOperation_any, kafka_common_acl_AclOperation_read,
    };
    use crate::ffi::common::acl::acl_permission_type::{
        kafka_common_acl_AclPermissionType_allow, kafka_common_acl_AclPermissionType_any,
    };
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_argument_error;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructor_getters_filter_and_to_string_follow_java() {
        let principal = CString::new("User:alice").unwrap();
        let host = CString::new("*").unwrap();
        unsafe {
            let mut entry = ptr::null_mut();
            assert!(
                kafka_common_acl_AccessControlEntry_new(
                    principal.as_ptr(),
                    host.as_ptr(),
                    kafka_common_acl_AclOperation_read(),
                    kafka_common_acl_AclPermissionType_allow(),
                    &mut entry
                )
                .is_null()
            );
            let expected =
                AccessControlEntry::new("User:alice", "*", AclOperation::Read, AclPermissionType::Allow).unwrap();
            assert_eq!(*access_control_entry_ref(entry), expected);
            assert_eq!(
                CStr::from_ptr(kafka_common_acl_AccessControlEntry_principal(entry))
                    .to_str()
                    .unwrap(),
                "User:alice"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_acl_AccessControlEntry_host(entry))
                    .to_str()
                    .unwrap(),
                "*"
            );
            assert_eq!(
                kafka_common_acl_AccessControlEntry_operation(entry),
                kafka_common_acl_AclOperation_read()
            );
            assert_eq!(
                kafka_common_acl_AccessControlEntry_permission_type(entry),
                kafka_common_acl_AclPermissionType_allow()
            );
            assert_eq!(kafka_common_acl_AccessControlEntry_is_unknown(entry), 0);
            let s = kafka_common_acl_AccessControlEntry_to_string(entry);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);

            let filter = kafka_common_acl_AccessControlEntry_to_filter(entry);
            assert_eq!(*access_control_entry_filter_ref(filter), expected.to_filter());
            assert_eq!(kafka_common_acl_AccessControlEntryFilter_matches(filter, entry), 1);
            kafka_common_acl_AccessControlEntryFilter_destroy(filter);
            kafka_common_acl_AccessControlEntry_destroy(entry);
            kafka_common_acl_AccessControlEntry_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn any_operation_or_permission_type_is_rejected() {
        let principal = CString::new("User:alice").unwrap();
        let host = CString::new("*").unwrap();
        unsafe {
            let mut entry = ptr::null_mut();
            let error = kafka_common_acl_AccessControlEntry_new(
                principal.as_ptr(),
                host.as_ptr(),
                kafka_common_acl_AclOperation_any(),
                kafka_common_acl_AclPermissionType_allow(),
                &mut entry,
            );
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "operation must not be ANY"
            );
            kafka_common_Error_destroy(error);
            let error = kafka_common_acl_AccessControlEntry_new(
                principal.as_ptr(),
                host.as_ptr(),
                kafka_common_acl_AclOperation_read(),
                kafka_common_acl_AclPermissionType_any(),
                &mut entry,
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "permissionType must not be ANY"
            );
            kafka_common_Error_destroy(error);
            assert!(entry.is_null(), "nothing is delivered on failure");
        }
    }
}
