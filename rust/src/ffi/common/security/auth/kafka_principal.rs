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
use crate::ffi::util::{c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`KafkaPrincipal`].
#[repr(C)]
pub struct kafka_common_security_auth_KafkaPrincipal_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_security_auth_KafkaPrincipal_t`] points at: the
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

unsafe fn inner_ref<'a>(principal: *const kafka_common_security_auth_KafkaPrincipal_t) -> &'a KafkaPrincipalInner {
    unsafe { &*(principal as *const KafkaPrincipalInner) }
}

/// The principal behind a handle.
///
/// # Safety
///
/// `principal` must be a valid principal handle.
pub(crate) unsafe fn kafka_principal_ref<'a>(
    principal: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> &'a KafkaPrincipal {
    &unsafe { inner_ref(principal) }.principal
}

/// Hands `principal` to C as an owned handle, freed with
/// [`kafka_common_security_auth_KafkaPrincipal_destroy`].
pub(crate) fn box_kafka_principal(principal: KafkaPrincipal) -> *mut kafka_common_security_auth_KafkaPrincipal_t {
    Box::into_raw(Box::new(KafkaPrincipalInner::new(principal))) as *mut kafka_common_security_auth_KafkaPrincipal_t
}

/// Copies the principals out of a C list of borrowed principal handles.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are principal handles.
pub(crate) unsafe fn list_kafka_principals(list: *const kafka_List_t) -> Vec<KafkaPrincipal> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| {
            unsafe { kafka_principal_ref(element as *const kafka_common_security_auth_KafkaPrincipal_t) }.clone()
        })
        .collect()
}

/// `new KafkaPrincipal(String principalType, String name)`: not token
/// authenticated. Owned, freed with
/// [`kafka_common_security_auth_KafkaPrincipal_destroy`]; the strings are
/// copied.
///
/// # Safety
///
/// `principal_type` and `name` must be valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_new(
    principal_type: *const c_char,
    name: *const c_char,
) -> *mut kafka_common_security_auth_KafkaPrincipal_t {
    box_kafka_principal(KafkaPrincipal::new(unsafe { c_str_to_string(principal_type) }, unsafe {
        c_str_to_string(name)
    }))
}

/// `new KafkaPrincipal(String principalType, String name, boolean tokenAuthenticated)`.
/// Owned; the strings are copied.
///
/// # Safety
///
/// `principal_type` and `name` must be valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_with_token_authenticated(
    principal_type: *const c_char,
    name: *const c_char,
    token_authenticated: i8,
) -> *mut kafka_common_security_auth_KafkaPrincipal_t {
    box_kafka_principal(KafkaPrincipal::with_token_authenticated(
        unsafe { c_str_to_string(principal_type) },
        unsafe { c_str_to_string(name) },
        token_authenticated != 0,
    ))
}

/// `KafkaPrincipal.ANONYMOUS`: the `User:ANONYMOUS` principal. Owned.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_KafkaPrincipal_anonymous()
-> *mut kafka_common_security_auth_KafkaPrincipal_t {
    box_kafka_principal(KafkaPrincipal::anonymous())
}

/// `getName()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid principal handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_name(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `getPrincipalType()`, e.g. `User`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid principal handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_principal_type(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.principal_type_c.as_ptr()
}

/// `tokenAuthenticated(boolean)`: records whether this principal
/// authenticated with a delegation token.
///
/// # Safety
///
/// `self_` must be a valid owned principal handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_set_token_authenticated(
    self_: *mut kafka_common_security_auth_KafkaPrincipal_t,
    token_authenticated: i8,
) {
    unsafe { &mut *(self_ as *mut KafkaPrincipalInner) }
        .principal
        .set_token_authenticated(token_authenticated != 0);
}

/// `tokenAuthenticated()`: whether this principal authenticated with a
/// delegation token.
///
/// # Safety
///
/// `self_` must be a valid principal handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_token_authenticated(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> i8 {
    i8::from(unsafe { kafka_principal_ref(self_) }.token_authenticated())
}

/// `toString()`, `<type>:<name>`, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid principal handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_to_string(
    self_: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> *mut c_char {
    into_c_string(&unsafe { kafka_principal_ref(self_) }.to_string())
}

/// Frees an owned principal handle. Null is a no-op; a principal borrowed
/// from a `kafka_common_security_token_delegation_TokenInformation_t` is
/// never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned principal handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_KafkaPrincipal_destroy(
    self_: *mut kafka_common_security_auth_KafkaPrincipal_t,
) {
    if !self_.is_null() {
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
