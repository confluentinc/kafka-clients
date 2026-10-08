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

//! `kafka_admin_UserScramCredentialAlteration_t`:
//! `org.apache.kafka.clients.admin.UserScramCredentialAlteration` (CLAUDE.md §4).
//!
//! Java's abstract class has two subclasses, `UserScramCredentialUpsertion`
//! and `UserScramCredentialDeletion`; the Rust enum's two data-carrying
//! variants follow the enum rule for variants with data (§4, "Enums"): each
//! is built from a copy of the subclass handle and returned owned, beside
//! `kafka_admin_UserScramCredentialAlteration_e` and `__enum` for a `switch`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char};

use crate::admin::UserScramCredentialAlteration;
use crate::ffi::admin::user_scram_credential_deletion::{
    kafka_admin_UserScramCredentialDeletion_t, user_scram_credential_deletion_ref,
};
use crate::ffi::admin::user_scram_credential_upsertion::{
    kafka_admin_UserScramCredentialUpsertion_t, user_scram_credential_upsertion_ref,
};
use crate::ffi::util::owned_c_string;

/// Opaque handle to a [`UserScramCredentialAlteration`].
#[repr(C)]
pub struct kafka_admin_UserScramCredentialAlteration_t {
    _private: [u8; 0],
}

/// The subclass behind a [`kafka_admin_UserScramCredentialAlteration_t`], for
/// a `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_UserScramCredentialAlteration_e {
    /// A `UserScramCredentialUpsertion`.
    kafka_admin_UserScramCredentialAlteration_UPSERTION,
    /// A `UserScramCredentialDeletion`.
    kafka_admin_UserScramCredentialAlteration_DELETION,
}

/// What a [`kafka_admin_UserScramCredentialAlteration_t`] points at: the
/// value plus the NUL-terminated user its getter borrows out.
pub(crate) struct UserScramCredentialAlterationInner {
    alteration: UserScramCredentialAlteration,
    user_c: CString,
}

impl UserScramCredentialAlterationInner {
    fn new(alteration: UserScramCredentialAlteration) -> Self {
        let user_c = owned_c_string(alteration.user());
        Self { alteration, user_c }
    }
}

unsafe fn inner_ref<'a>(
    alteration: *const kafka_admin_UserScramCredentialAlteration_t,
) -> &'a UserScramCredentialAlterationInner {
    unsafe { &*(alteration as *const UserScramCredentialAlterationInner) }
}

/// Hands `alteration` to C as an owned handle, freed with
/// [`kafka_admin_UserScramCredentialAlteration_destroy`].
pub(crate) fn box_user_scram_credential_alteration(
    alteration: UserScramCredentialAlteration,
) -> *mut kafka_admin_UserScramCredentialAlteration_t {
    Box::into_raw(Box::new(UserScramCredentialAlterationInner::new(alteration)))
        as *mut kafka_admin_UserScramCredentialAlteration_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `alteration` must be a live alteration handle.
pub(crate) unsafe fn user_scram_credential_alteration_ref<'a>(
    alteration: *const kafka_admin_UserScramCredentialAlteration_t,
) -> &'a UserScramCredentialAlteration {
    &unsafe { inner_ref(alteration) }.alteration
}

/// Which subclass the alteration is, for a `switch`.
///
/// # Safety
///
/// `self_` must be a valid alteration handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialAlteration__enum(
    self_: *const kafka_admin_UserScramCredentialAlteration_t,
) -> kafka_admin_UserScramCredentialAlteration_e {
    match unsafe { user_scram_credential_alteration_ref(self_) } {
        UserScramCredentialAlteration::Upsertion(_) => {
            kafka_admin_UserScramCredentialAlteration_e::kafka_admin_UserScramCredentialAlteration_UPSERTION
        },
        UserScramCredentialAlteration::Deletion(_) => {
            kafka_admin_UserScramCredentialAlteration_e::kafka_admin_UserScramCredentialAlteration_DELETION
        },
    }
}

/// The alteration standing for a `UserScramCredentialUpsertion` (the
/// subclass, viewed as its `UserScramCredentialAlteration` base): `value` is
/// copied, the caller keeps its handle. Owned, freed with
/// [`kafka_admin_UserScramCredentialAlteration_destroy`].
///
/// # Safety
///
/// `value` must be a valid upsertion handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialAlteration_upsertion(
    value: *const kafka_admin_UserScramCredentialUpsertion_t,
) -> *mut kafka_admin_UserScramCredentialAlteration_t {
    box_user_scram_credential_alteration(UserScramCredentialAlteration::Upsertion(
        unsafe { user_scram_credential_upsertion_ref(value) }.clone(),
    ))
}

/// The alteration standing for a `UserScramCredentialDeletion` (the subclass,
/// viewed as its `UserScramCredentialAlteration` base): `value` is copied,
/// the caller keeps its handle. Owned, freed with
/// [`kafka_admin_UserScramCredentialAlteration_destroy`].
///
/// # Safety
///
/// `value` must be a valid deletion handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialAlteration_deletion(
    value: *const kafka_admin_UserScramCredentialDeletion_t,
) -> *mut kafka_admin_UserScramCredentialAlteration_t {
    box_user_scram_credential_alteration(UserScramCredentialAlteration::Deletion(
        unsafe { user_scram_credential_deletion_ref(value) }.clone(),
    ))
}

/// `user()`: the user of the upsertion or deletion, borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid alteration handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialAlteration_user(
    self_: *const kafka_admin_UserScramCredentialAlteration_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.user_c.as_ptr()
}

/// Frees an owned alteration handle; null is a no-op. Both variants carry
/// data, so every handle is owned and there is no singleton to skip.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialAlteration_destroy(
    self_: *mut kafka_admin_UserScramCredentialAlteration_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut UserScramCredentialAlterationInner) });
    }
}
