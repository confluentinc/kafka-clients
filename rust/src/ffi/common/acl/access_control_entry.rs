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
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
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

/// What a handle points at.
///
/// # Safety
///
/// `entry` must be a valid access-control-entry handle, alive for `'a`.
unsafe fn inner_ref<'a>(entry: *const kafka_common_acl_AccessControlEntry_t) -> &'a AccessControlEntryInner {
    // SAFETY: Per this function's `# Safety`, `entry` is a valid access-control-entry handle alive
    // for `'a`, so it points at a live `AccessControlEntryInner`: the `Box::into_raw` of
    // `box_access_control_entry`, or one a parent handle embeds and hands out a view on, which
    // lives as long as that parent. Nothing mutates an `AccessControlEntryInner` while a handle to
    // it exists, so the shared reference is sound for `'a`.
    unsafe { &*(entry as *const AccessControlEntryInner) }
}

/// The entry behind a handle.
///
/// # Safety
///
/// `entry` must be a valid access-control-entry handle, alive for `'a`.
pub(crate) unsafe fn access_control_entry_ref<'a>(
    entry: *const kafka_common_acl_AccessControlEntry_t,
) -> &'a AccessControlEntry {
    // SAFETY: Per this function's `# Safety`, `entry` is a valid access-control-entry handle, as
    // `inner_ref` requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(entry) }.entry
}

/// Hands `entry` to C as an owned handle, freed with
/// [`kafka_common_acl_AccessControlEntry_destroy`].
pub(crate) fn box_access_control_entry(entry: AccessControlEntry) -> *mut kafka_common_acl_AccessControlEntry_t {
    Box::into_raw(Box::new(AccessControlEntryInner::new(entry))) as *mut kafka_common_acl_AccessControlEntry_t
}

/// `new AccessControlEntry(String principal, String host, AclOperation
/// operation, AclPermissionType permissionType)`: returns the owned entry, or
/// stores the owned `IllegalArgumentException` translation in `*out_err` and
/// returns `NULL` when `operation` or `permission_type` is `ANY`.
///
/// # Safety
///
/// `principal` and `host` must be valid NUL-terminated strings, `operation` and
/// `permission_type` singletons of their enums and `out_err` a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_new(
    principal: *const c_char,
    host: *const c_char,
    operation: *const kafka_common_acl_AclOperation_t,
    permission_type: *const kafka_common_acl_AclPermissionType_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_acl_AccessControlEntry_t {
    let entry = AccessControlEntry::new(
        // SAFETY: Per this function's `# Safety`, `principal` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(principal) },
        // SAFETY: Per this function's `# Safety`, `host` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(host) },
        // SAFETY: Per this function's `# Safety`, `operation` is a singleton of its enum, i.e. one
        // returned by the `acl_operation` module, as `operation_of` (`acl_operation::value_of`)
        // requires; the value is copied out.
        unsafe { operation_of(operation) },
        // SAFETY: Per this function's `# Safety`, `permission_type` is a singleton of its enum,
        // i.e. one returned by the `acl_permission_type` module, as `permission_type_of`
        // (`acl_permission_type::value_of`) requires; the value is copied out.
        unsafe { permission_type_of(permission_type) },
    );
    match entry {
        Ok(entry) => box_access_control_entry(entry),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `principal()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_principal(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid access-control-entry handle, as
    // `inner_ref` requires; the string returned is the handle's own C string, so it lives as long
    // as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.principal_c.as_ptr()
}

/// `host()`: borrowed from the handle; `*` means any host.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_host(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid access-control-entry handle, as
    // `inner_ref` requires; the string returned is the handle's own C string, so it lives as long
    // as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.host_c.as_ptr()
}

/// `operation()`: the borrowed `AclOperation` singleton.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_operation(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const kafka_common_acl_AclOperation_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid access-control-entry handle, as
    // `access_control_entry_ref` requires; the value is copied out and mapped to its static
    // singleton, so the borrow lasts for this call only.
    operation_singleton(unsafe { access_control_entry_ref(self_) }.operation())
}

/// `permissionType()`: the borrowed `AclPermissionType` singleton.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_permission_type(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *const kafka_common_acl_AclPermissionType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid access-control-entry handle, as
    // `access_control_entry_ref` requires; the value is copied out and mapped to its static
    // singleton, so the borrow lasts for this call only.
    permission_type_singleton(unsafe { access_control_entry_ref(self_) }.permission_type())
}

/// `toFilter()`: an owned filter matching exactly this entry, freed with
/// `kafka_common_acl_AccessControlEntryFilter_destroy`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_to_filter(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *mut kafka_common_acl_AccessControlEntryFilter_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid access-control-entry handle, as
    // `access_control_entry_ref` requires; the result is copied into an owned value before return,
    // so the borrow lasts for this call only.
    box_access_control_entry_filter(unsafe { access_control_entry_ref(self_) }.to_filter())
}

/// `isUnknown()`: whether the operation or the permission type is `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_is_unknown(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid access-control-entry handle, as
    // `access_control_entry_ref` requires; the reference is used only within this call.
    i8::from(unsafe { access_control_entry_ref(self_) }.is_unknown())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid access-control-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_to_string(
    self_: *const kafka_common_acl_AccessControlEntry_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid access-control-entry handle, as
    // `access_control_entry_ref` requires; the result is copied into an owned value before return,
    // so the borrow lasts for this call only.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AccessControlEntry_destroy(
    self_: *mut kafka_common_acl_AccessControlEntry_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of an `AccessControlEntryInner` in
        // `box_access_control_entry` (a view, which points into its parent, is never passed here),
        // so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut AccessControlEntryInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

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
        // SAFETY: Test code: `principal` and `host` are `CString`s that outlive every call reading
        // them, `err` is a valid slot pointing at null, the operation and permission-type
        // arguments are singletons from their enums' getters, and every owned handle (the entry
        // the constructor returns, the filter, the string) is destroyed once.
        unsafe {
            let mut err = ptr::null_mut();
            let entry = kafka_common_acl_AccessControlEntry_new(
                principal.as_ptr(),
                host.as_ptr(),
                kafka_common_acl_AclOperation_read(),
                kafka_common_acl_AclPermissionType_allow(),
                &mut err,
            );
            assert!(err.is_null());
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
        // SAFETY: Test code: `principal` and `host` are `CString`s that outlive every call reading
        // them, `error` is a valid slot pointing at null before each call, the operation and
        // permission-type arguments are singletons from their enums' getters, and each error the
        // failing constructor stores is destroyed once, after its message is read, and the slot
        // reset.
        unsafe {
            let mut error = ptr::null_mut();
            let entry = kafka_common_acl_AccessControlEntry_new(
                principal.as_ptr(),
                host.as_ptr(),
                kafka_common_acl_AclOperation_any(),
                kafka_common_acl_AclPermissionType_allow(),
                &mut error,
            );
            assert!(entry.is_null(), "nothing is returned on failure");
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "operation must not be ANY"
            );
            kafka_common_Error_destroy(error);
            error = ptr::null_mut();
            let entry = kafka_common_acl_AccessControlEntry_new(
                principal.as_ptr(),
                host.as_ptr(),
                kafka_common_acl_AclOperation_read(),
                kafka_common_acl_AclPermissionType_any(),
                &mut error,
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "permissionType must not be ANY"
            );
            kafka_common_Error_destroy(error);
            assert!(entry.is_null(), "nothing is returned on failure");
        }
    }
}
