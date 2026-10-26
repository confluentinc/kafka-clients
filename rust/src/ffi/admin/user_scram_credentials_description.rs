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

//! `kafka_admin_UserScramCredentialsDescription_t`:
//! `org.apache.kafka.clients.admin.UserScramCredentialsDescription`
//! (CLAUDE.md §4).

use std::ffi::{CString, c_char, c_void};

use crate::admin::{ScramCredentialInfo, UserScramCredentialsDescription};
use crate::ffi::admin::scram_credential_info::{
    box_scram_credential_info, destroy_scram_credential_info_element, kafka_admin_ScramCredentialInfo_t,
    scram_credential_info_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`UserScramCredentialsDescription`].
#[repr(C)]
pub struct kafka_admin_UserScramCredentialsDescription_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_UserScramCredentialsDescription_t`] points at: the
/// value plus the NUL-terminated name its getter borrows out.
pub(crate) struct UserScramCredentialsDescriptionInner {
    description: UserScramCredentialsDescription,
    name_c: CString,
}

impl UserScramCredentialsDescriptionInner {
    fn new(description: UserScramCredentialsDescription) -> Self {
        let name_c = owned_c_string(description.name());
        Self { description, name_c }
    }
}

/// The inner value behind a handle.
///
/// # Safety
///
/// `description` must be a live user-SCRAM-credentials-description handle,
/// kept alive for `'a`.
unsafe fn inner_ref<'a>(
    description: *const kafka_admin_UserScramCredentialsDescription_t,
) -> &'a UserScramCredentialsDescriptionInner {
    // SAFETY: Per this function's `# Safety`, `description` is a live
    // user-SCRAM-credentials-description handle kept alive for `'a`. Every
    // `kafka_admin_UserScramCredentialsDescription_t` comes from
    // `box_user_scram_credentials_description`, which boxes a
    // `UserScramCredentialsDescriptionInner`, so the cast yields a valid shared reference. The type
    // has no setters, so nothing mutates it while borrowed.
    unsafe { &*(description as *const UserScramCredentialsDescriptionInner) }
}

/// Hands `description` to C as an owned handle, freed with
/// [`kafka_admin_UserScramCredentialsDescription_destroy`].
pub(crate) fn box_user_scram_credentials_description(
    description: UserScramCredentialsDescription,
) -> *mut kafka_admin_UserScramCredentialsDescription_t {
    Box::into_raw(Box::new(UserScramCredentialsDescriptionInner::new(description)))
        as *mut kafka_admin_UserScramCredentialsDescription_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `description` must be a live user-SCRAM-credentials-description handle,
/// kept alive for `'a`.
pub(crate) unsafe fn user_scram_credentials_description_ref<'a>(
    description: *const kafka_admin_UserScramCredentialsDescription_t,
) -> &'a UserScramCredentialsDescription {
    // SAFETY: Per this function's `# Safety`, `description` is a live
    // user-SCRAM-credentials-description handle kept alive for `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_UserScramCredentialsDescription_t *` element of an
/// owned container.
///
/// # Safety
///
/// `element` must be an owned user-SCRAM-credentials-description handle not
/// yet destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_user_scram_credentials_description_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned
    // user-SCRAM-credentials-description handle not yet destroyed, that no other call uses at the
    // same time and that is not used afterwards, as
    // `kafka_admin_UserScramCredentialsDescription_destroy` requires.
    unsafe {
        kafka_admin_UserScramCredentialsDescription_destroy(
            element as *mut kafka_admin_UserScramCredentialsDescription_t,
        )
    }
}

/// Creates the description.
///
/// # Parameters
///
/// - `name` - the required user name
/// - `credential_infos` - the required SASL/SCRAM credential representations for the user
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `credential_infos` `NULL`, read as empty, or
/// a valid borrowed list of `const kafka_admin_ScramCredentialInfo_t *`, copied. The returned
/// handle is owned, freed with [`kafka_admin_UserScramCredentialsDescription_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_new(
    name: *const c_char,
    credential_infos: *const kafka_List_t,
) -> *mut kafka_admin_UserScramCredentialsDescription_t {
    // SAFETY: Per this function's `# Safety`, `credential_infos` is null or a valid list, alive and
    // not mutated for this call, as `list_elements` requires; the borrowed slice does not outlive
    // the call.
    let infos: Vec<ScramCredentialInfo> = unsafe { list_elements(credential_infos) }
        .iter()
        .map(|&element| {
            // SAFETY: Per this function's `# Safety`, every element of `credential_infos` is a
            // SCRAM-credential-info handle alive for this call, as `scram_credential_info_ref`
            // requires; the borrow ends with the clone.
            unsafe { scram_credential_info_ref(element as *const kafka_admin_ScramCredentialInfo_t) }.clone()
        })
        .collect();
    box_user_scram_credentials_description(UserScramCredentialsDescription::new(
        // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(name) },
        infos,
    ))
}

/// # Returns
///
/// the user name
///
/// # Safety
///
/// `self_` must be a valid user-SCRAM-credentials-description handle. The returned string is
/// borrowed, valid as long as the description.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_name(
    self_: *const kafka_admin_UserScramCredentialsDescription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid user-SCRAM-credentials-description
    // handle, kept alive for this call, as `inner_ref` requires. The returned string is a `CString`
    // the handle owns, so it lives as long as the handle.
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// # Returns
///
/// the always non-`NULL`/unmodifiable list of SASL/SCRAM credential representations for the user
///
/// # Safety
///
/// `self_` must be a valid user-SCRAM-credentials-description handle. The returned list of
/// `kafka_admin_ScramCredentialInfo_t *` copies, in Java's order, is owned, freed with
/// `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_credential_infos(
    self_: *const kafka_admin_UserScramCredentialsDescription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid user-SCRAM-credentials-description
    // handle, kept alive for this call, as `user_scram_credentials_description_ref` requires.
    let elements = unsafe { user_scram_credentials_description_ref(self_) }
        .credential_infos()
        .iter()
        .map(|info| box_scram_credential_info(info.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_scram_credential_info_element))
}

/// Returns the text form of the description.
///
/// # Safety
///
/// `self_` must be a valid user-SCRAM-credentials-description handle. The returned string is
/// owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_to_string(
    self_: *const kafka_admin_UserScramCredentialsDescription_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid user-SCRAM-credentials-description
    // handle, kept alive for this call, as `user_scram_credentials_description_ref` requires.
    into_c_string(&unsafe { user_scram_credentials_description_ref(self_) }.to_string())
}

/// Frees an owned user-SCRAM-credentials-description handle; `NULL` is a
/// no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_destroy(
    self_: *mut kafka_admin_UserScramCredentialsDescription_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every user-SCRAM-credentials-description handle comes from
        // `box_user_scram_credentials_description`, which boxes a
        // `UserScramCredentialsDescriptionInner`, so `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut UserScramCredentialsDescriptionInner) });
    }
}
