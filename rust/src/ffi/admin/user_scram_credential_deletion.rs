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

//! `kafka_admin_UserScramCredentialDeletion_t`:
//! `org.apache.kafka.clients.admin.UserScramCredentialDeletion` (CLAUDE.md §4).

use std::ffi::{CString, c_char};

use crate::admin::UserScramCredentialDeletion;
use crate::ffi::admin::scram_mechanism::{
    kafka_admin_ScramMechanism_t, scram_mechanism_singleton, scram_mechanism_value_of,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, owned_c_string};

/// Opaque handle to a [`UserScramCredentialDeletion`].
#[repr(C)]
pub struct kafka_admin_UserScramCredentialDeletion_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_UserScramCredentialDeletion_t`] points at: the value
/// plus the NUL-terminated user its getter borrows out.
pub(crate) struct UserScramCredentialDeletionInner {
    deletion: UserScramCredentialDeletion,
    user_c: CString,
}

impl UserScramCredentialDeletionInner {
    fn new(deletion: UserScramCredentialDeletion) -> Self {
        let user_c = owned_c_string(deletion.user());
        Self { deletion, user_c }
    }
}

/// The inner value behind a handle.
///
/// # Safety
///
/// `deletion` must be a live deletion handle, kept alive for `'a`.
unsafe fn inner_ref<'a>(
    deletion: *const kafka_admin_UserScramCredentialDeletion_t,
) -> &'a UserScramCredentialDeletionInner {
    // SAFETY: Per this function's `# Safety`, `deletion` is a live deletion handle kept alive for
    // `'a`. Every `kafka_admin_UserScramCredentialDeletion_t` comes from
    // `box_user_scram_credential_deletion`, which boxes a `UserScramCredentialDeletionInner`, so
    // the cast yields a valid shared reference. The type has no setters, so nothing mutates it
    // while borrowed.
    unsafe { &*(deletion as *const UserScramCredentialDeletionInner) }
}

/// Hands `deletion` to C as an owned handle, freed with
/// [`kafka_admin_UserScramCredentialDeletion_destroy`].
pub(crate) fn box_user_scram_credential_deletion(
    deletion: UserScramCredentialDeletion,
) -> *mut kafka_admin_UserScramCredentialDeletion_t {
    Box::into_raw(Box::new(UserScramCredentialDeletionInner::new(deletion)))
        as *mut kafka_admin_UserScramCredentialDeletion_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `deletion` must be a live deletion handle, kept alive for `'a`.
pub(crate) unsafe fn user_scram_credential_deletion_ref<'a>(
    deletion: *const kafka_admin_UserScramCredentialDeletion_t,
) -> &'a UserScramCredentialDeletion {
    // SAFETY: Per this function's `# Safety`, `deletion` is a live deletion handle kept alive for
    // `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(deletion) }.deletion
}

/// `new UserScramCredentialDeletion(String user, ScramMechanism mechanism)`;
/// `mechanism` is a `kafka_admin_ScramMechanism_t` singleton. Owned, freed
/// with [`kafka_admin_UserScramCredentialDeletion_destroy`].
///
/// # Safety
///
/// `user` must be a valid NUL-terminated string and `mechanism` a
/// SCRAM-mechanism singleton.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialDeletion_new(
    user: *const c_char,
    mechanism: *const kafka_admin_ScramMechanism_t,
) -> *mut kafka_admin_UserScramCredentialDeletion_t {
    // SAFETY: Per this function's `# Safety`, `user` is a valid NUL-terminated string, as
    // `c_str_to_string` requires, and `mechanism` a SCRAM-mechanism singleton, which only the
    // `scram_mechanism` module hands out, as `scram_mechanism_value_of` requires.
    box_user_scram_credential_deletion(UserScramCredentialDeletion::new(unsafe { c_str_to_string(user) }, unsafe {
        scram_mechanism_value_of(mechanism)
    }))
}

/// `user()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid deletion handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialDeletion_user(
    self_: *const kafka_admin_UserScramCredentialDeletion_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deletion handle, kept alive for
    // this call, as `inner_ref` requires. The returned string is a `CString` the handle owns, so it
    // lives as long as the handle.
    unsafe { inner_ref(self_) }.user_c.as_ptr()
}

/// `mechanism()`: the `kafka_admin_ScramMechanism_t` singleton, never freed.
///
/// # Safety
///
/// `self_` must be a valid deletion handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialDeletion_mechanism(
    self_: *const kafka_admin_UserScramCredentialDeletion_t,
) -> *const kafka_admin_ScramMechanism_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deletion handle, kept alive for
    // this call, as `user_scram_credential_deletion_ref` requires.
    scram_mechanism_singleton(unsafe { user_scram_credential_deletion_ref(self_) }.mechanism())
}

/// Frees an owned deletion handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialDeletion_destroy(
    self_: *mut kafka_admin_UserScramCredentialDeletion_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every deletion handle comes from `box_user_scram_credential_deletion`, which
        // boxes a `UserScramCredentialDeletionInner`, so `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut UserScramCredentialDeletionInner) });
    }
}
