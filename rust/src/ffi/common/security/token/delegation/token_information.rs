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

//! `kafka_common_security_token_delegation_TokenInformation_t`:
//! `org.apache.kafka.common.security.token.delegation.TokenInformation`
//! (CLAUDE.md §4).

use std::ffi::{CString, c_char, c_void};

use crate::common::security::token::delegation::TokenInformation;
use crate::ffi::common::security::auth::kafka_principal::{
    KafkaPrincipalInner, box_kafka_principal, kafka_common_security_auth_KafkaPrincipal_t, kafka_principal_ref,
    list_kafka_principals,
};
use crate::ffi::util::{
    box_list, box_string_list, c_str_to_string, destroy_boxed, into_c_string, kafka_List_t, owned_c_string,
};

/// Opaque handle to a [`TokenInformation`].
#[repr(C)]
pub struct kafka_common_security_token_delegation_TokenInformation_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_security_token_delegation_TokenInformation_t`]
/// points at: the information plus the NUL-terminated token id and the two
/// principal handles its getters borrow out. The owner and requester are
/// immutable in Java, so the copies inside the principal handles never drift
/// from `info`.
pub(crate) struct TokenInformationInner {
    info: TokenInformation,
    token_id_c: CString,
    owner: KafkaPrincipalInner,
    token_requester: KafkaPrincipalInner,
}

impl TokenInformationInner {
    pub(crate) fn new(info: TokenInformation) -> Self {
        let token_id_c = owned_c_string(info.token_id());
        let owner = KafkaPrincipalInner::new(info.owner().clone());
        let token_requester = KafkaPrincipalInner::new(info.token_requester().clone());
        Self { info, token_id_c, owner, token_requester }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_security_token_delegation_TokenInformation_t {
        self as *const Self as *const kafka_common_security_token_delegation_TokenInformation_t
    }
}

unsafe fn inner_ref<'a>(
    info: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> &'a TokenInformationInner {
    unsafe { &*(info as *const TokenInformationInner) }
}

/// The information behind a handle.
///
/// # Safety
///
/// `info` must be a valid token-information handle.
pub(crate) unsafe fn token_information_ref<'a>(
    info: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> &'a TokenInformation {
    &unsafe { inner_ref(info) }.info
}

/// Hands `info` to C as an owned handle, freed with
/// [`kafka_common_security_token_delegation_TokenInformation_destroy`].
pub(crate) fn box_token_information(
    info: TokenInformation,
) -> *mut kafka_common_security_token_delegation_TokenInformation_t {
    Box::into_raw(Box::new(TokenInformationInner::new(info)))
        as *mut kafka_common_security_token_delegation_TokenInformation_t
}

/// `new TokenInformation(String tokenId, KafkaPrincipal owner, Collection<KafkaPrincipal> renewers, long issueTimestamp, long maxTimestamp, long expiryTimestamp)`:
/// the requester is the owner. `owner` and the borrowed
/// `kafka_common_security_auth_KafkaPrincipal_t` handles in `renewers` are
/// copied during the call. Owned, freed with
/// [`kafka_common_security_token_delegation_TokenInformation_destroy`].
///
/// # Safety
///
/// `token_id` must be a valid NUL-terminated string, `owner` a valid
/// principal handle and `renewers` null or a valid list of principal handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_new(
    token_id: *const c_char,
    owner: *const kafka_common_security_auth_KafkaPrincipal_t,
    renewers: *const kafka_List_t,
    issue_timestamp: i64,
    max_timestamp: i64,
    expiry_timestamp: i64,
) -> *mut kafka_common_security_token_delegation_TokenInformation_t {
    box_token_information(TokenInformation::new(
        unsafe { c_str_to_string(token_id) },
        unsafe { kafka_principal_ref(owner) }.clone(),
        unsafe { list_kafka_principals(renewers) },
        issue_timestamp,
        max_timestamp,
        expiry_timestamp,
    ))
}

/// `new TokenInformation(String tokenId, KafkaPrincipal owner, KafkaPrincipal tokenRequester, Collection<KafkaPrincipal> renewers, long issueTimestamp, long maxTimestamp, long expiryTimestamp)`
/// (KIP-373): the requester may differ from the owner when a superuser creates
/// a token on another principal's behalf. Same copying and ownership as
/// [`kafka_common_security_token_delegation_TokenInformation_new`].
///
/// # Safety
///
/// `token_id` must be a valid NUL-terminated string, `owner` and
/// `token_requester` valid principal handles and `renewers` null or a valid
/// list of principal handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_with_token_requester(
    token_id: *const c_char,
    owner: *const kafka_common_security_auth_KafkaPrincipal_t,
    token_requester: *const kafka_common_security_auth_KafkaPrincipal_t,
    renewers: *const kafka_List_t,
    issue_timestamp: i64,
    max_timestamp: i64,
    expiry_timestamp: i64,
) -> *mut kafka_common_security_token_delegation_TokenInformation_t {
    box_token_information(TokenInformation::with_token_requester(
        unsafe { c_str_to_string(token_id) },
        unsafe { kafka_principal_ref(owner) }.clone(),
        unsafe { kafka_principal_ref(token_requester) }.clone(),
        unsafe { list_kafka_principals(renewers) },
        issue_timestamp,
        max_timestamp,
        expiry_timestamp,
    ))
}

/// `owner()`: borrowed from the handle; never destroyed.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_owner(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *const kafka_common_security_auth_KafkaPrincipal_t {
    unsafe { inner_ref(self_) }.owner.as_ptr()
}

/// `ownerAsString()`, `<type>:<name>`, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_owner_as_string(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *mut c_char {
    into_c_string(&unsafe { token_information_ref(self_) }.owner_as_string())
}

/// `tokenRequester()`: the principal that *asked* for the token, borrowed
/// from the handle; never destroyed.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_token_requester(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *const kafka_common_security_auth_KafkaPrincipal_t {
    unsafe { inner_ref(self_) }.token_requester.as_ptr()
}

/// `tokenRequesterAsString()`, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_token_requester_as_string(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *mut c_char {
    into_c_string(&unsafe { token_information_ref(self_) }.token_requester_as_string())
}

/// `renewers()`: an owned list of owned
/// `kafka_common_security_auth_KafkaPrincipal_t` copies, in the order the
/// broker reported. Freed with `kafka_List_destroy`, which also frees the
/// principals.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_renewers(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *mut kafka_List_t {
    let elements = unsafe { token_information_ref(self_) }
        .renewers()
        .iter()
        .map(|renewer| box_kafka_principal(renewer.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_boxed::<KafkaPrincipalInner>))
}

/// `renewersAsString()`: an owned list of owned `<type>:<name>` strings.
/// Freed with `kafka_List_destroy`, which also frees the strings.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_renewers_as_string(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *mut kafka_List_t {
    box_string_list(unsafe { token_information_ref(self_) }.renewers_as_string())
}

/// `issueTimestamp()`, in milliseconds since the epoch.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_issue_timestamp(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> i64 {
    unsafe { token_information_ref(self_) }.issue_timestamp()
}

/// `expiryTimestamp()`, in milliseconds since the epoch.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> i64 {
    unsafe { token_information_ref(self_) }.expiry_timestamp()
}

/// `setExpiryTimestamp(long expiryTimestamp)`.
///
/// # Safety
///
/// `self_` must be a valid owned token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_set_expiry_timestamp(
    self_: *mut kafka_common_security_token_delegation_TokenInformation_t,
    expiry_timestamp: i64,
) {
    unsafe { &mut *(self_ as *mut TokenInformationInner) }
        .info
        .set_expiry_timestamp(expiry_timestamp);
}

/// `tokenId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_token_id(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.token_id_c.as_ptr()
}

/// `maxTimestamp()`, in milliseconds since the epoch: the latest the token
/// can be renewed to, whatever the renewal period asks for.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_max_timestamp(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> i64 {
    unsafe { token_information_ref(self_) }.max_timestamp()
}

/// `ownerOrRenewer(KafkaPrincipal principal)`: whether `principal` is the
/// owner or one of the renewers.
///
/// # Safety
///
/// `self_` must be a valid token-information handle and `principal` a valid
/// principal handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_owner_or_renewer(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
    principal: *const kafka_common_security_auth_KafkaPrincipal_t,
) -> i8 {
    i8::from(unsafe { token_information_ref(self_) }.owner_or_renewer(unsafe { kafka_principal_ref(principal) }))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid token-information handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_to_string(
    self_: *const kafka_common_security_token_delegation_TokenInformation_t,
) -> *mut c_char {
    into_c_string(&unsafe { token_information_ref(self_) }.to_string())
}

/// Frees an owned token-information handle. Null is a no-op; the information
/// borrowed from a `kafka_common_security_token_delegation_DelegationToken_t`
/// is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned token-information handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_token_delegation_TokenInformation_destroy(
    self_: *mut kafka_common_security_token_delegation_TokenInformation_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TokenInformationInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::security::auth::KafkaPrincipal;
    use crate::ffi::common::security::auth::kafka_principal::{
        kafka_common_security_auth_KafkaPrincipal_destroy, kafka_common_security_auth_KafkaPrincipal_name,
        kafka_common_security_auth_KafkaPrincipal_new, kafka_common_security_auth_KafkaPrincipal_principal_type,
    };
    use crate::ffi::util::{
        kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size, kafka_string_destroy,
    };

    pub(crate) fn sample_info() -> TokenInformation {
        TokenInformation::with_token_requester(
            "token-id-1",
            KafkaPrincipal::new("User", "owner"),
            KafkaPrincipal::new("User", "requester"),
            vec![
                KafkaPrincipal::new("User", "renewer-1"),
                KafkaPrincipal::new("Group", "renewer-2"),
            ],
            1_000,
            9_000,
            5_000,
        )
    }

    unsafe fn principal_text(principal: *const kafka_common_security_auth_KafkaPrincipal_t) -> (String, String) {
        let t = unsafe { CStr::from_ptr(kafka_common_security_auth_KafkaPrincipal_principal_type(principal)) };
        let n = unsafe { CStr::from_ptr(kafka_common_security_auth_KafkaPrincipal_name(principal)) };
        (t.to_str().unwrap().to_string(), n.to_str().unwrap().to_string())
    }

    #[test]
    fn getters_expose_the_whole_java_chain() {
        let expected = sample_info();
        let handle = box_token_information(expected.clone());
        unsafe {
            assert_eq!(
                CStr::from_ptr(kafka_common_security_token_delegation_TokenInformation_token_id(handle)).to_str(),
                Ok("token-id-1")
            );
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_issue_timestamp(handle),
                1_000
            );
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_max_timestamp(handle),
                9_000
            );
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(handle),
                5_000
            );

            // The three principals are distinct, so a transposition of owner
            // / requester / renewer is caught.
            let owner = kafka_common_security_token_delegation_TokenInformation_owner(handle);
            assert_eq!(principal_text(owner), ("User".to_string(), "owner".to_string()));
            let requester = kafka_common_security_token_delegation_TokenInformation_token_requester(handle);
            assert_eq!(principal_text(requester), ("User".to_string(), "requester".to_string()));
            let s = kafka_common_security_token_delegation_TokenInformation_owner_as_string(handle);
            assert_eq!(CStr::from_ptr(s).to_str(), Ok("User:owner"));
            kafka_string_destroy(s);
            let s = kafka_common_security_token_delegation_TokenInformation_token_requester_as_string(handle);
            assert_eq!(CStr::from_ptr(s).to_str(), Ok("User:requester"));
            kafka_string_destroy(s);

            let renewers = kafka_common_security_token_delegation_TokenInformation_renewers(handle);
            assert_eq!(kafka_List_size(renewers), 2);
            assert_eq!(
                principal_text(kafka_List_get(renewers, 0) as *const kafka_common_security_auth_KafkaPrincipal_t),
                ("User".to_string(), "renewer-1".to_string())
            );
            assert_eq!(
                principal_text(kafka_List_get(renewers, 1) as *const kafka_common_security_auth_KafkaPrincipal_t),
                ("Group".to_string(), "renewer-2".to_string())
            );
            kafka_List_destroy(renewers);
            let names = kafka_common_security_token_delegation_TokenInformation_renewers_as_string(handle);
            assert_eq!(kafka_List_size(names), 2);
            assert_eq!(
                CStr::from_ptr(kafka_List_get(names, 1) as *const c_char).to_str(),
                Ok("Group:renewer-2")
            );
            kafka_List_destroy(names);

            // Java's `ownerOrRenewer` matches the owner, the requester and
            // every renewer; a stranger matches none of them.
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_owner_or_renewer(handle, owner),
                1
            );
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_owner_or_renewer(handle, requester),
                1
            );
            let stranger = box_kafka_principal(KafkaPrincipal::new("User", "stranger"));
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_owner_or_renewer(handle, stranger),
                0
            );
            kafka_common_security_auth_KafkaPrincipal_destroy(stranger);

            kafka_common_security_token_delegation_TokenInformation_set_expiry_timestamp(handle, 6_000);
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(handle),
                6_000
            );
            let s = kafka_common_security_token_delegation_TokenInformation_to_string(handle);
            let mut renewed = expected;
            renewed.set_expiry_timestamp(6_000);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), renewed.to_string());
            kafka_string_destroy(s);
            kafka_common_security_token_delegation_TokenInformation_destroy(handle);
            kafka_common_security_token_delegation_TokenInformation_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn constructors_copy_their_principals() {
        let id = CString::new("token-id-2").unwrap();
        let user = CString::new("User").unwrap();
        let owner_name = CString::new("owner").unwrap();
        let requester_name = CString::new("requester").unwrap();
        let renewer_name = CString::new("renewer").unwrap();
        unsafe {
            let owner = kafka_common_security_auth_KafkaPrincipal_new(user.as_ptr(), owner_name.as_ptr());
            let requester = kafka_common_security_auth_KafkaPrincipal_new(user.as_ptr(), requester_name.as_ptr());
            let renewer = kafka_common_security_auth_KafkaPrincipal_new(user.as_ptr(), renewer_name.as_ptr());
            let renewers = kafka_List_new();
            kafka_List_add(renewers, renewer as *mut c_void);

            let plain =
                kafka_common_security_token_delegation_TokenInformation_new(id.as_ptr(), owner, renewers, 10, 30, 20);
            let requested = kafka_common_security_token_delegation_TokenInformation_with_token_requester(
                id.as_ptr(),
                owner,
                requester,
                renewers,
                10,
                30,
                20,
            );
            // The inputs were copied: the caller frees them first.
            kafka_List_destroy(renewers);
            kafka_common_security_auth_KafkaPrincipal_destroy(renewer);
            kafka_common_security_auth_KafkaPrincipal_destroy(requester);
            kafka_common_security_auth_KafkaPrincipal_destroy(owner);

            let renewers = vec![KafkaPrincipal::new("User", "renewer")];
            assert_eq!(
                *token_information_ref(plain),
                TokenInformation::new("token-id-2", KafkaPrincipal::new("User", "owner"), renewers.clone(), 10, 30, 20)
            );
            // Java's `equals` ignores the expiry, so it is checked by its getter.
            assert_eq!(
                kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(plain),
                20
            );
            assert_eq!(
                token_information_ref(plain).token_requester(),
                &KafkaPrincipal::new("User", "owner")
            );
            assert_eq!(
                *token_information_ref(requested),
                TokenInformation::with_token_requester(
                    "token-id-2",
                    KafkaPrincipal::new("User", "owner"),
                    KafkaPrincipal::new("User", "requester"),
                    renewers,
                    10,
                    30,
                    20
                )
            );
            assert_eq!(
                token_information_ref(requested).token_requester(),
                &KafkaPrincipal::new("User", "requester")
            );
            kafka_common_security_token_delegation_TokenInformation_destroy(plain);
            kafka_common_security_token_delegation_TokenInformation_destroy(requested);
        }
    }

    #[test]
    fn no_renewers_is_a_legal_token() {
        let handle = box_token_information(TokenInformation::new(
            "token-id-3",
            KafkaPrincipal::new("User", "owner"),
            Vec::new(),
            10,
            30,
            20,
        ));
        unsafe {
            let renewers = kafka_common_security_token_delegation_TokenInformation_renewers(handle);
            assert_eq!(kafka_List_size(renewers), 0);
            kafka_List_destroy(renewers);
            kafka_common_security_token_delegation_TokenInformation_destroy(handle);
        }
    }
}
