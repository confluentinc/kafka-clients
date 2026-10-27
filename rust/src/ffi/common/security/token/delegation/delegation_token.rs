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

//! `kafka_common_security_token_delegation_DelegationToken_t`:
//! `org.apache.kafka.common.security.token.delegation.DelegationToken`
//! (CLAUDE.md §4).

use std::ffi::c_char;

use crate::common::security::token::delegation::DelegationToken;
use crate::ffi::common::security::token::delegation::token_information::{
    TokenInformationInner, kafka_common_security_token_delegation_TokenInformation_t, token_information_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{into_c_string, kafka_Bytes_t};

/// Opaque handle to a [`DelegationToken`].
#[repr(C)]
pub struct kafka_common_security_token_delegation_DelegationToken_t {
    _private: [u8; 0],
}

/// What a `kafka_common_security_token_delegation_DelegationToken_t` points
/// at: the token plus the token-information handle its getter borrows out.
///
/// The HMAC is raw bytes, not a string: it is a SHA-512 MAC and can contain
/// NULs, so it crosses as a `kafka_Bytes_t` borrowed from the token rather
/// than as a NUL-terminated string.
pub(crate) struct DelegationTokenInner {
    token: DelegationToken,
    token_info: TokenInformationInner,
}

impl DelegationTokenInner {
    pub(crate) fn new(token: DelegationToken) -> Self {
        let token_info = TokenInformationInner::new(token.token_info().clone());
        Self { token, token_info }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    // handed out by the admin result handles once the admin slice lands
    #[expect(dead_code)]
    pub(crate) fn as_ptr(&self) -> *const kafka_common_security_token_delegation_DelegationToken_t {
        self as *const Self as *const kafka_common_security_token_delegation_DelegationToken_t
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `token` must be a valid delegation-token handle, alive for `'a`.
unsafe fn inner_ref<'a>(
    token: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> &'a DelegationTokenInner {
    // SAFETY: Per this function's `# Safety`, `token` is a valid delegation-token handle alive for
    // `'a`, so it points at a live `DelegationTokenInner`: the `Box::into_raw` of
    // `box_delegation_token`, or one a parent embeds and hands out through
    // `DelegationTokenInner::as_ptr`, which lives as long as that parent (no parent does yet; the
    // admin results will). No call mutates a `DelegationTokenInner`, so the shared reference is
    // sound for `'a`.
    unsafe { &*(token as *const DelegationTokenInner) }
}

/// The token behind a handle.
///
/// # Safety
///
/// `token` must be a valid delegation-token handle, alive for `'a`.
pub(crate) unsafe fn delegation_token_ref<'a>(
    token: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> &'a DelegationToken {
    // SAFETY: Per this function's `# Safety`, `token` is a valid delegation-token handle, as
    // `inner_ref` requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(token) }.token
}

/// Hands `token` to C as an owned handle, freed with
/// `kafka_common_security_token_delegation_DelegationToken_destroy`.
pub(crate) fn box_delegation_token(
    token: DelegationToken,
) -> *mut kafka_common_security_token_delegation_DelegationToken_t {
    Box::into_raw(Box::new(DelegationTokenInner::new(token)))
        as *mut kafka_common_security_token_delegation_DelegationToken_t
}

/// Creates a delegation token.
///
/// # Parameters
///
/// - `token_information` - the token's details, copied
/// - `hmac` - the token's MAC, copied; a `NULL` `hmac.data` is an empty MAC
///
/// # Safety
///
/// `token_information` must be a valid token-information handle and `hmac.data` `NULL` or valid
/// for `hmac.len` bytes. The returned handle is owned, freed with
/// `kafka_common_security_token_delegation_DelegationToken_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_new(
    token_information: *const kafka_common_security_token_delegation_TokenInformation_t,
    hmac: kafka_Bytes_t,
) -> *mut kafka_common_security_token_delegation_DelegationToken_t {
    box_delegation_token(DelegationToken::new(
        // SAFETY: Per this function's `# Safety`, `token_information` is a valid token-information
        // handle, as `token_information_ref` requires; it is cloned, so the borrow lasts for this
        // call only.
        unsafe { token_information_ref(token_information) }.clone(),
        // SAFETY: Per this function's `# Safety`, `hmac.data` is null or valid for `hmac.len`
        // bytes, as `kafka_Bytes_t::as_slice` requires; the bytes are copied into an owned `Vec`,
        // so nothing borrows them after this call.
        unsafe { hmac.as_slice() }.map(<[u8]>::to_vec).unwrap_or_default(),
    ))
}

/// Returns the token's details.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle. The returned handle is borrowed, valid as long
/// as the token, and never destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_token_info(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> *const kafka_common_security_token_delegation_TokenInformation_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid delegation-token handle, as
    // `inner_ref` requires. The handle returned is a view on the `TokenInformationInner` the token
    // embeds, so it lives as long as the token handle; that is the rustdoc's "borrowed from the
    // handle; never destroyed".
    unsafe { inner_ref(self_) }.token_info.as_ptr()
}

/// Returns the raw MAC bytes. They are **not** NUL-terminated and may contain NUL, so `len` is the
/// only way to know how many there are. Pass them back verbatim to `renewDelegationToken` /
/// `expireDelegationToken`.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle. The returned bytes are borrowed, valid as long
/// as the token.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_hmac(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid delegation-token handle, as
    // `delegation_token_ref` requires. The bytes returned are a view on the token's own HMAC, so
    // they live as long as the handle; that is the rustdoc's "borrowed from the handle".
    kafka_Bytes_t::from_slice(unsafe { delegation_token_ref(self_) }.hmac())
}

/// Returns the MAC in Base64, the form most tooling passes back to `renewDelegationToken`.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid delegation-token handle, as
    // `delegation_token_ref` requires; the result is copied into an owned value before return, so
    // the borrow lasts for this call only.
    into_c_string(&unsafe { delegation_token_ref(self_) }.hmac_as_base64_string())
}

/// Returns the text form of the token.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_to_string(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid delegation-token handle, as
    // `delegation_token_ref` requires; the result is copied into an owned value before return, so
    // the borrow lasts for this call only.
    into_c_string(&unsafe { delegation_token_ref(self_) }.to_string())
}

/// Frees an owned delegation-token handle. Null is a no-op; a token borrowed
/// from an admin result is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned delegation-token handle not yet
/// destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_destroy(
    self_: *mut kafka_common_security_token_delegation_DelegationToken_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `DelegationTokenInner` in
        // `box_delegation_token` (a view, which points into its parent, is never passed here), so
        // `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut DelegationTokenInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::security::auth::KafkaPrincipal;
    use crate::common::security::token::delegation::TokenInformation;
    use crate::ffi::common::security::auth::kafka_principal::kafka_common_security_auth_KafkaPrincipal_name;
    use crate::ffi::common::security::token::delegation::token_information::{
        box_token_information, kafka_common_security_token_delegation_TokenInformation_destroy,
        kafka_common_security_token_delegation_TokenInformation_owner,
        kafka_common_security_token_delegation_TokenInformation_token_id,
    };
    use crate::ffi::util::kafka_string_destroy;

    fn sample_info() -> TokenInformation {
        TokenInformation::new(
            "token-id-1",
            KafkaPrincipal::new("User", "owner"),
            Vec::new(),
            1_000,
            9_000,
            5_000,
        )
    }

    #[test]
    fn hmac_crosses_with_its_length_and_the_info_is_borrowed() {
        // The HMAC contains an interior NUL, so only the length says how long
        // it is -- a C string would have truncated it to one byte.
        let token = DelegationToken::new(sample_info(), vec![0x01, 0x00, 0x02]);
        let base64 = token.hmac_as_base64_string();
        let handle = box_delegation_token(token.clone());
        // SAFETY: Test code: `handle` comes from `box_delegation_token` and is destroyed once,
        // after the views read from it (the HMAC bytes, the token information and its owner); each
        // string from `hmac_as_base64_string` and `to_string` is destroyed once, after it is read.
        unsafe {
            let hmac = kafka_common_security_token_delegation_DelegationToken_hmac(handle);
            assert_eq!(hmac.len, 3);
            assert_eq!(hmac.as_slice(), Some(&[0x01u8, 0x00, 0x02][..]));
            let s = kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string(handle);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), base64);
            kafka_string_destroy(s);

            let info = kafka_common_security_token_delegation_DelegationToken_token_info(handle);
            assert_eq!(
                CStr::from_ptr(kafka_common_security_token_delegation_TokenInformation_token_id(info)).to_str(),
                Ok("token-id-1")
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_security_auth_KafkaPrincipal_name(
                    kafka_common_security_token_delegation_TokenInformation_owner(info)
                ))
                .to_str(),
                Ok("owner")
            );
            let s = kafka_common_security_token_delegation_DelegationToken_to_string(handle);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), token.to_string());
            kafka_string_destroy(s);
            kafka_common_security_token_delegation_DelegationToken_destroy(handle);
            kafka_common_security_token_delegation_DelegationToken_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn constructor_copies_the_info_and_the_bytes() {
        let bytes = [0xffu8, 0x00, 0x10];
        // SAFETY: Test code: each token-information handle comes from `box_token_information` and
        // is destroyed once, after the constructor copies it; `bytes` outlives the constructor
        // reading it; each token comes from the constructor and is destroyed once, after it is
        // read.
        unsafe {
            let info = box_token_information(sample_info());
            let token =
                kafka_common_security_token_delegation_DelegationToken_new(info, kafka_Bytes_t::from_slice(&bytes));
            kafka_common_security_token_delegation_TokenInformation_destroy(info);
            assert_eq!(
                *delegation_token_ref(token),
                DelegationToken::new(sample_info(), bytes.to_vec())
            );
            kafka_common_security_token_delegation_DelegationToken_destroy(token);

            // A null `data` is an empty MAC, not a crash.
            let info = box_token_information(sample_info());
            let empty = kafka_common_security_token_delegation_DelegationToken_new(info, kafka_Bytes_t::NULL);
            kafka_common_security_token_delegation_TokenInformation_destroy(info);
            assert_eq!(delegation_token_ref(empty).hmac(), &[] as &[u8]);
            kafka_common_security_token_delegation_DelegationToken_destroy(empty);
        }
    }
}
