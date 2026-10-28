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

//! `kafka_common_security_auth_KafkaPrincipal_t`:
//! `org.apache.kafka.common.security.auth.KafkaPrincipal` (CLAUDE.md §4).

use std::ffi::{CString, c_char};

use crate::common::security::auth::KafkaPrincipal;
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`KafkaPrincipal`].
#[repr(C)]
pub struct kafka_common_security_auth_KafkaPrincipal_t {
    _private: [u8; 0],
}

/// What a `kafka_common_security_auth_KafkaPrincipal_t` points at: the
/// principal plus the NUL-terminated type and name its getters borrow out.
pub(crate) struct KafkaPrincipalInner {
    principal: KafkaPrincipal,
    principal_type_c: CString,
    name_c: CString,
}

impl KafkaPrincipalInner {
    pub(crate) fn new(principal: KafkaPrincipal) -> Self {
        let principal_type_c = owned_c_string(principal.principal_type());
        let name_c = owned_c_string(principal.name());
        Self { principal, principal_type_c, name_c }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_security_auth_KafkaPrincipal_t {
        self as *const Self as *const kafka_common_security_auth_KafkaPrincipal_t
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `principal` must be a valid principal handle, alive for `'a`.
unsafe fn inner_ref<'a>(principal: *const kafka_common_security_auth_KafkaPrincipal_t) -> &'a KafkaPrincipalInner {
    // SAFETY: Per this function's `# Safety`, `principal` is a valid principal handle alive for
    // `'a`, so it points at a live `KafkaPrincipalInner`: the `Box::into_raw` of
    // `box_kafka_principal`, or one a parent embeds and hands out through
    // `KafkaPrincipalInner::as_ptr` (a `TokenInformation`'s owner or token requester, a
    // `CreateDelegationTokenOptions`' owner), which lives as long as that parent. The only call
    // that mutates a `KafkaPrincipalInner`,
    // `kafka_common_security_auth_KafkaPrincipal_set_token_authenticated`, takes an owned handle
    // that no other call uses at the same time, so the shared reference is sound for `'a`.
    unsafe { &*(principal as *const KafkaPrincipalInner) }
}

/// The principal behind a handle.
///
/// # Safety
///
/// `principal` must be a valid principal handle, alive for `'a`.
pub(crate) unsafe fn kafka_principal_ref<'a>(
    principal: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> &'a KafkaPrincipal {
    // SAFETY: Per this function's `# Safety`, `principal` is a valid principal handle, as
    // `inner_ref` requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(principal) }.principal
}

/// Hands `principal` to C as an owned handle, freed with
/// `kafka_common_security_auth_KafkaPrincipal_destroy`.
pub(crate) fn box_kafka_principal(principal: KafkaPrincipal) -> *mut kafka_common_security_auth_KafkaPrincipal_t {
    Box::into_raw(Box::new(KafkaPrincipalInner::new(principal))) as *mut kafka_common_security_auth_KafkaPrincipal_t
}

/// Copies the principals out of a C list of borrowed principal handles.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are principal handles.
pub(crate) unsafe fn list_kafka_principals(list: *const kafka_List_t) -> Vec<KafkaPrincipal> {
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list, which the caller
    // keeps alive and does not mutate for this call, as `list_elements` requires; the borrow of its
    // elements ends when the principals are collected.
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| {
            // SAFETY: Per this function's `# Safety`, every element of `list` is a principal
            // handle, as `kafka_principal_ref` requires; the principal is cloned, so the borrow
            // lasts for this call only.
            unsafe { kafka_principal_ref(element as *const kafka_common_security_auth_KafkaPrincipal_t) }.clone()
        })
        .collect()
}

/// Creates a principal that is not token authenticated.
///
/// # Parameters
///
/// - `principal_type` - the principal type, e.g. `User`
/// - `name` - the principal name
///
/// # Safety
///
/// `principal_type` and `name` must be valid NUL-terminated strings, copied. The returned handle is
/// owned, freed with `kafka_common_security_auth_KafkaPrincipal_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_new(
    principal_type: *const c_char,
    name: *const c_char,
) -> *mut kafka_common_security_auth_KafkaPrincipal_t {
    // SAFETY: Per this function's `# Safety`, `principal_type` and `name` are each a valid
    // NUL-terminated string, as `c_str_to_string` requires; both are copied into owned `String`s,
    // so nothing borrows them after this call.
    box_kafka_principal(KafkaPrincipal::new(unsafe { c_str_to_string(principal_type) }, unsafe {
        c_str_to_string(name)
    }))
}

/// Creates a principal.
///
/// # Parameters
///
/// - `principal_type` - the principal type, e.g. `User`
/// - `name` - the principal name
/// - `token_authenticated` - 1 if the principal authenticated with a delegation token, else 0
///
/// # Safety
///
/// `principal_type` and `name` must be valid NUL-terminated strings, copied. The returned handle is
/// owned, freed with `kafka_common_security_auth_KafkaPrincipal_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_with_token_authenticated(
    principal_type: *const c_char,
    name: *const c_char,
    token_authenticated: i8,
) -> *mut kafka_common_security_auth_KafkaPrincipal_t {
    box_kafka_principal(KafkaPrincipal::with_token_authenticated(
        // SAFETY: Per this function's `# Safety`, `principal_type` is a valid NUL-terminated
        // string, as `c_str_to_string` requires; it is copied into an owned `String`, so nothing
        // borrows it after this call.
        unsafe { c_str_to_string(principal_type) },
        // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(name) },
        token_authenticated != 0,
    ))
}

/// Returns `KafkaPrincipal.ANONYMOUS`, the `User:ANONYMOUS` principal.
///
/// # Safety
///
/// The returned handle is owned, freed with `kafka_common_security_auth_KafkaPrincipal_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_KafkaPrincipal_anonymous()
-> *mut kafka_common_security_auth_KafkaPrincipal_t {
    box_kafka_principal(KafkaPrincipal::anonymous())
}

/// Returns the principal name.
///
/// # Safety
///
/// `self_` must be a valid principal handle. The returned string is borrowed, valid as long as the
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_name(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid principal handle, as `inner_ref`
    // requires; the string returned is the handle's own C string, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// Returns the principal type, e.g. `User`.
///
/// # Safety
///
/// `self_` must be a valid principal handle. The returned string is borrowed, valid as long as the
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_principal_type(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid principal handle, as `inner_ref`
    // requires; the string returned is the handle's own C string, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.principal_type_c.as_ptr()
}

/// Records whether this principal authenticated with a delegation token.
///
/// # Parameters
///
/// - `token_authenticated` - 1 if it did, else 0
///
/// # Safety
///
/// `self_` must be a valid owned principal handle that no other call uses at
/// the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_set_token_authenticated(
    self_: *mut kafka_common_security_auth_KafkaPrincipal_t,
    token_authenticated: i8,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned principal handle, the
    // `Box::into_raw` of a `KafkaPrincipalInner` in `box_kafka_principal`, that no other call uses
    // at the same time, so the exclusive reference aliases nothing for the length of this call.
    // Only the `principal` field is written; the C strings earlier getters returned point into the
    // `CString` heap buffers, which this does not touch.
    unsafe { &mut *(self_ as *mut KafkaPrincipalInner) }
        .principal
        .set_token_authenticated(token_authenticated != 0);
}

/// Returns 1 if this principal authenticated with a delegation token, else 0.
///
/// # Safety
///
/// `self_` must be a valid principal handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_token_authenticated(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid principal handle, as
    // `kafka_principal_ref` requires; the reference is used only within this call.
    i8::from(unsafe { kafka_principal_ref(self_) }.token_authenticated())
}

/// Returns `<type>:<name>`.
///
/// # Safety
///
/// `self_` must be a valid principal handle. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_to_string(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid principal handle, as
    // `kafka_principal_ref` requires; the result is copied into an owned value before return, so
    // the borrow lasts for this call only.
    into_c_string(&unsafe { kafka_principal_ref(self_) }.to_string())
}

/// Frees an owned principal handle. Null is a no-op; a principal borrowed
/// from a `kafka_common_security_token_delegation_TokenInformation_t` is
/// never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned principal handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_destroy(
    self_: *mut kafka_common_security_auth_KafkaPrincipal_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `KafkaPrincipalInner` in
        // `box_kafka_principal` (a view, which points into its parent, is never passed here), so
        // `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut KafkaPrincipalInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructors_getters_and_setter_follow_java() {
        let user = CString::new("User").unwrap();
        let alice = CString::new("alice").unwrap();
        // SAFETY: Test code: `user` and `alice` are `CString`s that outlive the constructors
        // copying them, every principal comes from this module's constructors and is destroyed
        // once, the name and type views are read only while their principal is alive, the setter is
        // called on an owned handle no other call uses, and the string from `to_string` is
        // destroyed once.
        unsafe {
            let plain = kafka_common_security_auth_KafkaPrincipal_new(user.as_ptr(), alice.as_ptr());
            assert_eq!(*kafka_principal_ref(plain), KafkaPrincipal::new("User", "alice"));
            assert_eq!(
                CStr::from_ptr(kafka_common_security_auth_KafkaPrincipal_principal_type(plain)).to_str(),
                Ok("User")
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_security_auth_KafkaPrincipal_name(plain)).to_str(),
                Ok("alice")
            );
            assert_eq!(kafka_common_security_auth_KafkaPrincipal_token_authenticated(plain), 0);
            kafka_common_security_auth_KafkaPrincipal_set_token_authenticated(plain, 1);
            assert_eq!(kafka_common_security_auth_KafkaPrincipal_token_authenticated(plain), 1);
            let s = kafka_common_security_auth_KafkaPrincipal_to_string(plain);
            assert_eq!(CStr::from_ptr(s).to_str(), Ok("User:alice"));
            kafka_string_destroy(s);
            kafka_common_security_auth_KafkaPrincipal_destroy(plain);

            let token =
                kafka_common_security_auth_KafkaPrincipal_with_token_authenticated(user.as_ptr(), alice.as_ptr(), 1);
            assert_eq!(
                *kafka_principal_ref(token),
                KafkaPrincipal::with_token_authenticated("User", "alice", true)
            );
            assert_eq!(kafka_common_security_auth_KafkaPrincipal_token_authenticated(token), 1);
            kafka_common_security_auth_KafkaPrincipal_destroy(token);

            let anonymous = kafka_common_security_auth_KafkaPrincipal_anonymous();
            assert_eq!(*kafka_principal_ref(anonymous), KafkaPrincipal::anonymous());
            assert_eq!(
                CStr::from_ptr(kafka_common_security_auth_KafkaPrincipal_name(anonymous)).to_str(),
                Ok("ANONYMOUS")
            );
            kafka_common_security_auth_KafkaPrincipal_destroy(anonymous);
            kafka_common_security_auth_KafkaPrincipal_destroy(ptr::null_mut());
        }
    }
}
