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

//! `kafka_admin_UserScramCredentialUpsertion_t`:
//! `org.apache.kafka.clients.admin.UserScramCredentialUpsertion` (CLAUDE.md §4).
//!
//! Java's three constructors are the three `with_*` factories; the `byte[]`
//! password and salt cross as `kafka_Bytes_t` views, borrowed from the handle
//! on the way out and copied on the way in.

use std::ffi::{CString, c_char};

use crate::admin::UserScramCredentialUpsertion;
use crate::ffi::admin::scram_credential_info::{
    kafka_admin_ScramCredentialInfo_t, scram_credential_info_ptr, scram_credential_info_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_Bytes_t, owned_c_string};

/// Opaque handle to a [`UserScramCredentialUpsertion`].
#[repr(C)]
pub struct kafka_admin_UserScramCredentialUpsertion_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_UserScramCredentialUpsertion_t`] points at: the value
/// plus the NUL-terminated user its getter borrows out.
pub(crate) struct UserScramCredentialUpsertionInner {
    upsertion: UserScramCredentialUpsertion,
    user_c: CString,
}

impl UserScramCredentialUpsertionInner {
    fn new(upsertion: UserScramCredentialUpsertion) -> Self {
        let user_c = owned_c_string(upsertion.user());
        Self { upsertion, user_c }
    }
}

/// The inner value behind a handle.
///
/// # Safety
///
/// `upsertion` must be a live upsertion handle, kept alive for `'a`.
unsafe fn inner_ref<'a>(
    upsertion: *const kafka_admin_UserScramCredentialUpsertion_t,
) -> &'a UserScramCredentialUpsertionInner {
    // SAFETY: Per this function's `# Safety`, `upsertion` is a live upsertion handle kept alive for
    // `'a`. Every `kafka_admin_UserScramCredentialUpsertion_t` comes from
    // `box_user_scram_credential_upsertion`, which boxes a `UserScramCredentialUpsertionInner`, so
    // the cast yields a valid shared reference. The type has no setters, so nothing mutates it
    // while borrowed.
    unsafe { &*(upsertion as *const UserScramCredentialUpsertionInner) }
}

/// Hands `upsertion` to C as an owned handle, freed with
/// [`kafka_admin_UserScramCredentialUpsertion_destroy`].
pub(crate) fn box_user_scram_credential_upsertion(
    upsertion: UserScramCredentialUpsertion,
) -> *mut kafka_admin_UserScramCredentialUpsertion_t {
    Box::into_raw(Box::new(UserScramCredentialUpsertionInner::new(upsertion)))
        as *mut kafka_admin_UserScramCredentialUpsertion_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `upsertion` must be a live upsertion handle, kept alive for `'a`.
pub(crate) unsafe fn user_scram_credential_upsertion_ref<'a>(
    upsertion: *const kafka_admin_UserScramCredentialUpsertion_t,
) -> &'a UserScramCredentialUpsertion {
    // SAFETY: Per this function's `# Safety`, `upsertion` is a live upsertion handle kept alive for
    // `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(upsertion) }.upsertion
}

/// Copies a borrowed `kafka_Bytes_t`; a null `data` (Java's null array) reads
/// as empty.
///
/// # Safety
///
/// `bytes.data` must be null or point to `bytes.len` readable bytes that are
/// not written during this call.
unsafe fn bytes_to_vec(bytes: kafka_Bytes_t) -> Vec<u8> {
    // SAFETY: Per this function's `# Safety`, `bytes.data` is null or points to `bytes.len`
    // readable bytes not written during this call, as `as_slice` requires; the borrowed slice is
    // copied before return.
    unsafe { bytes.as_slice() }.map(<[u8]>::to_vec).unwrap_or_default()
}

/// `new UserScramCredentialUpsertion(String user, ScramCredentialInfo
/// credentialInfo, String password)`: the password is the UTF-8 bytes of the
/// string, and a random salt is generated as in Java. `credential_info` is
/// copied, the caller keeps its handle. Owned, freed with
/// [`kafka_admin_UserScramCredentialUpsertion_destroy`].
///
/// # Safety
///
/// `user` and `password` must be valid NUL-terminated strings and
/// `credential_info` a valid SCRAM-credential-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_with_str(
    user: *const c_char,
    credential_info: *const kafka_admin_ScramCredentialInfo_t,
    password: *const c_char,
) -> *mut kafka_admin_UserScramCredentialUpsertion_t {
    box_user_scram_credential_upsertion(UserScramCredentialUpsertion::with_str(
        // SAFETY: Per this function's `# Safety`, `user` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(user) },
        // SAFETY: Per this function's `# Safety`, `credential_info` is a valid
        // SCRAM-credential-info handle, kept alive for this call, as `scram_credential_info_ref`
        // requires; the borrow ends with the clone.
        unsafe { scram_credential_info_ref(credential_info) }.clone(),
        // SAFETY: Per this function's `# Safety`, `password` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        &unsafe { c_str_to_string(password) },
    ))
}

/// `new UserScramCredentialUpsertion(String user, ScramCredentialInfo
/// credentialInfo, byte[] password)`: the password bytes are copied and a
/// random salt is generated as in Java. `credential_info` is copied, the
/// caller keeps its handle. Owned, freed with
/// [`kafka_admin_UserScramCredentialUpsertion_destroy`].
///
/// # Safety
///
/// `user` must be a valid NUL-terminated string, `credential_info` a valid
/// SCRAM-credential-info handle and `password` a view over readable bytes
/// that are not written during the call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_with_bytes(
    user: *const c_char,
    credential_info: *const kafka_admin_ScramCredentialInfo_t,
    password: kafka_Bytes_t,
) -> *mut kafka_admin_UserScramCredentialUpsertion_t {
    box_user_scram_credential_upsertion(UserScramCredentialUpsertion::with_bytes(
        // SAFETY: Per this function's `# Safety`, `user` is a valid NUL-terminated string, as
        // `c_str_to_string` requires.
        unsafe { c_str_to_string(user) },
        // SAFETY: Per this function's `# Safety`, `credential_info` is a valid
        // SCRAM-credential-info handle, kept alive for this call, as `scram_credential_info_ref`
        // requires; the borrow ends with the clone.
        unsafe { scram_credential_info_ref(credential_info) }.clone(),
        // SAFETY: Per this function's `# Safety`, `password` is a view over readable bytes not
        // written during this call, as `bytes_to_vec` requires.
        unsafe { bytes_to_vec(password) },
    ))
}

/// `new UserScramCredentialUpsertion(String user, ScramCredentialInfo
/// credentialInfo, byte[] password, byte[] salt)`: both byte arrays are
/// copied. `credential_info` is copied, the caller keeps its handle. Owned,
/// freed with [`kafka_admin_UserScramCredentialUpsertion_destroy`].
///
/// # Safety
///
/// As [`kafka_admin_UserScramCredentialUpsertion_with_bytes`], plus `salt` a
/// view over readable bytes that are not written during the call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_with_salt(
    user: *const c_char,
    credential_info: *const kafka_admin_ScramCredentialInfo_t,
    password: kafka_Bytes_t,
    salt: kafka_Bytes_t,
) -> *mut kafka_admin_UserScramCredentialUpsertion_t {
    box_user_scram_credential_upsertion(UserScramCredentialUpsertion::with_salt(
        // SAFETY: Per this function's `# Safety` (by reference to
        // `kafka_admin_UserScramCredentialUpsertion_with_bytes`), `user` is a valid NUL-terminated
        // string, as `c_str_to_string` requires.
        unsafe { c_str_to_string(user) },
        // SAFETY: Per this function's `# Safety` (by reference to
        // `kafka_admin_UserScramCredentialUpsertion_with_bytes`), `credential_info` is a valid
        // SCRAM-credential-info handle, kept alive for this call, as `scram_credential_info_ref`
        // requires; the borrow ends with the clone.
        unsafe { scram_credential_info_ref(credential_info) }.clone(),
        // SAFETY: Per this function's `# Safety` (by reference to
        // `kafka_admin_UserScramCredentialUpsertion_with_bytes`), `password` is a view over
        // readable bytes not written during this call, as `bytes_to_vec` requires.
        unsafe { bytes_to_vec(password) },
        // SAFETY: Per this function's `# Safety`, `salt` is a view over readable bytes not written
        // during this call, as `bytes_to_vec` requires.
        unsafe { bytes_to_vec(salt) },
    ))
}

/// `user()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid upsertion handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_user(
    self_: *const kafka_admin_UserScramCredentialUpsertion_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid upsertion handle, kept alive for
    // this call, as `inner_ref` requires. The returned string is a `CString` the handle owns, so it
    // lives as long as the handle.
    unsafe { inner_ref(self_) }.user_c.as_ptr()
}

/// `credentialInfo()`: a borrowed handle valid as long as the upsertion,
/// never passed to `kafka_admin_ScramCredentialInfo_destroy`.
///
/// # Safety
///
/// `self_` must be a valid upsertion handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_credential_info(
    self_: *const kafka_admin_UserScramCredentialUpsertion_t,
) -> *const kafka_admin_ScramCredentialInfo_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid upsertion handle, kept alive for
    // this call, as `user_scram_credential_upsertion_ref` requires. The returned view points at the
    // `ScramCredentialInfo` inside the handle, so it lives as long as the handle.
    scram_credential_info_ptr(unsafe { user_scram_credential_upsertion_ref(self_) }.credential_info())
}

/// `salt()`: a view over the salt bytes, borrowed from the handle and valid
/// as long as it.
///
/// # Safety
///
/// `self_` must be a valid upsertion handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_salt(
    self_: *const kafka_admin_UserScramCredentialUpsertion_t,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid upsertion handle, kept alive for
    // this call, as `user_scram_credential_upsertion_ref` requires. The returned view borrows the
    // salt bytes the handle owns, so it lives as long as the handle.
    kafka_Bytes_t::from_slice(unsafe { user_scram_credential_upsertion_ref(self_) }.salt())
}

/// `password()`: a view over the password bytes, borrowed from the handle
/// and valid as long as it.
///
/// # Safety
///
/// `self_` must be a valid upsertion handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_password(
    self_: *const kafka_admin_UserScramCredentialUpsertion_t,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid upsertion handle, kept alive for
    // this call, as `user_scram_credential_upsertion_ref` requires. The returned view borrows the
    // password bytes the handle owns, so it lives as long as the handle.
    kafka_Bytes_t::from_slice(unsafe { user_scram_credential_upsertion_ref(self_) }.password())
}

/// Frees an owned upsertion handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_UserScramCredentialUpsertion_destroy(
    self_: *mut kafka_admin_UserScramCredentialUpsertion_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every upsertion handle comes from `box_user_scram_credential_upsertion`,
        // which boxes a `UserScramCredentialUpsertionInner`, so `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut UserScramCredentialUpsertionInner) });
    }
}
