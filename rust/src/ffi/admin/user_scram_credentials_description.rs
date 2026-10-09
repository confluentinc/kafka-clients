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

unsafe fn inner_ref<'a>(
    description: *const kafka_admin_UserScramCredentialsDescription_t,
) -> &'a UserScramCredentialsDescriptionInner {
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
/// `description` must be a live user-SCRAM-credentials-description handle.
pub(crate) unsafe fn user_scram_credentials_description_ref<'a>(
    description: *const kafka_admin_UserScramCredentialsDescription_t,
) -> &'a UserScramCredentialsDescription {
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_UserScramCredentialsDescription_t *` element of an
/// owned container.
///
/// # Safety
///
/// `element` must be an owned user-SCRAM-credentials-description handle.
pub(crate) unsafe fn destroy_user_scram_credentials_description_element(element: *mut c_void) {
    unsafe {
        kafka_admin_UserScramCredentialsDescription_destroy(
            element as *mut kafka_admin_UserScramCredentialsDescription_t,
        )
    }
}

/// `new UserScramCredentialsDescription(String name, List<ScramCredentialInfo>
/// credentialInfos)`: a borrowed list of `const kafka_admin_ScramCredentialInfo_t *`,
/// copied (null reads as empty). Owned, freed with
/// [`kafka_admin_UserScramCredentialsDescription_destroy`].
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `credential_infos` null
/// or a valid list of SCRAM-credential-info handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_new(
    name: *const c_char,
    credential_infos: *const kafka_List_t,
) -> *mut kafka_admin_UserScramCredentialsDescription_t {
    let infos: Vec<ScramCredentialInfo> = unsafe { list_elements(credential_infos) }
        .iter()
        .map(|&element| {
            unsafe { scram_credential_info_ref(element as *const kafka_admin_ScramCredentialInfo_t) }.clone()
        })
        .collect();
    box_user_scram_credentials_description(UserScramCredentialsDescription::new(
        unsafe { c_str_to_string(name) },
        infos,
    ))
}

/// `name()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid user-SCRAM-credentials-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_name(
    self_: *const kafka_admin_UserScramCredentialsDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `credentialInfos()`: an owned list of owned `kafka_admin_ScramCredentialInfo_t *`
/// copies, in Java's order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid user-SCRAM-credentials-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_credential_infos(
    self_: *const kafka_admin_UserScramCredentialsDescription_t,
) -> *mut kafka_List_t {
    let elements = unsafe { user_scram_credentials_description_ref(self_) }
        .credential_infos()
        .iter()
        .map(|info| box_scram_credential_info(info.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_scram_credential_info_element))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid user-SCRAM-credentials-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_to_string(
    self_: *const kafka_admin_UserScramCredentialsDescription_t,
) -> *mut c_char {
    into_c_string(&unsafe { user_scram_credentials_description_ref(self_) }.to_string())
}

/// Frees an owned user-SCRAM-credentials-description handle; null is a
/// no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialsDescription_destroy(
    self_: *mut kafka_admin_UserScramCredentialsDescription_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut UserScramCredentialsDescriptionInner) });
    }
}
