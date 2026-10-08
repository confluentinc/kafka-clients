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
use crate::ffi::util::{into_c_string, kafka_Bytes_t};

/// Opaque handle to a [`DelegationToken`].
#[repr(C)]
pub struct kafka_common_security_token_delegation_DelegationToken_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_security_token_delegation_DelegationToken_t`] points
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
    pub(crate) fn as_ptr(&self) -> *const kafka_common_security_token_delegation_DelegationToken_t {
        self as *const Self as *const kafka_common_security_token_delegation_DelegationToken_t
    }
}

unsafe fn inner_ref<'a>(
    token: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> &'a DelegationTokenInner {
    unsafe { &*(token as *const DelegationTokenInner) }
}

/// The token behind a handle.
///
/// # Safety
///
/// `token` must be a valid delegation-token handle.
pub(crate) unsafe fn delegation_token_ref<'a>(
    token: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> &'a DelegationToken {
    &unsafe { inner_ref(token) }.token
}

/// Hands `token` to C as an owned handle, freed with
/// [`kafka_common_security_token_delegation_DelegationToken_destroy`].
pub(crate) fn box_delegation_token(
    token: DelegationToken,
) -> *mut kafka_common_security_token_delegation_DelegationToken_t {
    Box::into_raw(Box::new(DelegationTokenInner::new(token)))
        as *mut kafka_common_security_token_delegation_DelegationToken_t
}

/// `new DelegationToken(TokenInformation tokenInformation, byte[] hmac)`:
/// `token_information` and the bytes behind `hmac` are copied during the
/// call; a null `hmac.data` is an empty MAC. Owned, freed with
/// [`kafka_common_security_token_delegation_DelegationToken_destroy`].
///
/// # Safety
///
/// `token_information` must be a valid token-information handle and
/// `hmac.data` null or valid for `hmac.len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_new(
    token_information: *const kafka_common_security_token_delegation_TokenInformation_t,
    hmac: kafka_Bytes_t,
) -> *mut kafka_common_security_token_delegation_DelegationToken_t {
    box_delegation_token(DelegationToken::new(
        unsafe { token_information_ref(token_information) }.clone(),
        unsafe { hmac.as_slice() }.map(<[u8]>::to_vec).unwrap_or_default(),
    ))
}

/// `tokenInfo()`: borrowed from the handle; never destroyed.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_token_info(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> *const kafka_common_security_token_delegation_TokenInformation_t {
    unsafe { inner_ref(self_) }.token_info.as_ptr()
}

/// `hmac()`: the raw MAC bytes, borrowed from the handle. They are **not**
/// NUL-terminated and may contain NUL, so `len` is the only way to know how
/// many there are. Pass them back verbatim to `renewDelegationToken` /
/// `expireDelegationToken`.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_hmac(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> kafka_Bytes_t {
    kafka_Bytes_t::from_slice(unsafe { delegation_token_ref(self_) }.hmac())
}

/// `hmacAsBase64String()`, as an owned string freed with
/// `kafka_string_destroy`: the form most tooling passes back to
/// `renewDelegationToken`.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> *mut c_char {
    into_c_string(&unsafe { delegation_token_ref(self_) }.hmac_as_base64_string())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid delegation-token handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_to_string(
    self_: *const kafka_common_security_token_delegation_DelegationToken_t,
) -> *mut c_char {
    into_c_string(&unsafe { delegation_token_ref(self_) }.to_string())
}

/// Frees an owned delegation-token handle. Null is a no-op; a token borrowed
/// from an admin result is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned delegation-token handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_DelegationToken_destroy(
    self_: *mut kafka_common_security_token_delegation_DelegationToken_t,
) {
    if !self_.is_null() {
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
