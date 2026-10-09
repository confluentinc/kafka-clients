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

//! `kafka_common_quota_ClientQuotaMatch_t`: the Rust-only [`ClientQuotaMatch`]
//! that stands for the three states of Java's
//! `ClientQuotaFilterComponent.match()` (`Optional.of(name)`,
//! `Optional.empty()` and `null`), CLAUDE.md §4 rule 2.
//!
//! `Default` and `Any` carry no data and are borrowed singletons comparable
//! with `==`; `Exact` carries the name and is built owned through
//! [`kafka_common_quota_ClientQuotaMatch_exact`]. The match borrowed from a
//! component through `kafka_common_quota_ClientQuotaFilterComponent_match`
//! is the singleton when it carries no data, so `==` against the singletons
//! works there too.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::common::quota::ClientQuotaMatch;
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`ClientQuotaMatch`].
// Rust-only: Java expresses the three states with an `Optional` that may be null.
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_quota_ClientQuotaMatch_t {
    _private: [u8; 0],
}

/// The variant behind a [`kafka_common_quota_ClientQuotaMatch_t`], for a
/// `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_quota_ClientQuotaMatch_e {
    kafka_common_quota_ClientQuotaMatch_DEFAULT,
    kafka_common_quota_ClientQuotaMatch_ANY,
    kafka_common_quota_ClientQuotaMatch_EXACT,
}

static DEFAULT: ClientQuotaMatch = ClientQuotaMatch::Default;
static ANY: ClientQuotaMatch = ClientQuotaMatch::Any;

/// A borrowed handle on `value`, valid as long as `value`: the singleton for
/// a data-less variant, so it compares equal to the per-variant functions,
/// and `value` itself for `Exact`.
pub(crate) fn client_quota_match_ptr(value: &ClientQuotaMatch) -> *const kafka_common_quota_ClientQuotaMatch_t {
    let target: &ClientQuotaMatch = match value {
        ClientQuotaMatch::Default => &DEFAULT,
        ClientQuotaMatch::Any => &ANY,
        ClientQuotaMatch::Exact(_) => value,
    };
    target as *const ClientQuotaMatch as *const kafka_common_quota_ClientQuotaMatch_t
}

/// The match behind a handle.
///
/// # Safety
///
/// `value` must be a valid match handle.
pub(crate) unsafe fn client_quota_match_ref<'a>(
    value: *const kafka_common_quota_ClientQuotaMatch_t,
) -> &'a ClientQuotaMatch {
    unsafe { &*(value as *const ClientQuotaMatch) }
}

/// The C enumerator of the match behind `self_`.
///
/// # Safety
///
/// `self_` must be a valid match handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaMatch__enum(
    self_: *const kafka_common_quota_ClientQuotaMatch_t,
) -> kafka_common_quota_ClientQuotaMatch_e {
    match unsafe { client_quota_match_ref(self_) } {
        ClientQuotaMatch::Default => kafka_common_quota_ClientQuotaMatch_e::kafka_common_quota_ClientQuotaMatch_DEFAULT,
        ClientQuotaMatch::Any => kafka_common_quota_ClientQuotaMatch_e::kafka_common_quota_ClientQuotaMatch_ANY,
        ClientQuotaMatch::Exact(_) => kafka_common_quota_ClientQuotaMatch_e::kafka_common_quota_ClientQuotaMatch_EXACT,
    }
}

/// `ClientQuotaMatch::Default`: matches the built-in default entity name
/// (Java's `Optional.empty()`). A borrowed singleton, never freed.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_quota_ClientQuotaMatch_default() -> *const kafka_common_quota_ClientQuotaMatch_t {
    client_quota_match_ptr(&DEFAULT)
}

/// `ClientQuotaMatch::Any`: matches any specified name for the entity type
/// (Java's `null`). A borrowed singleton, never freed.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_quota_ClientQuotaMatch_any() -> *const kafka_common_quota_ClientQuotaMatch_t {
    client_quota_match_ptr(&ANY)
}

/// `ClientQuotaMatch::Exact(value)`: matches `value` exactly (Java's
/// `Optional.of(name)`). Owned, freed with
/// [`kafka_common_quota_ClientQuotaMatch_destroy`]; `value` is copied.
///
/// # Safety
///
/// `value` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaMatch_exact(
    value: *const c_char,
) -> *mut kafka_common_quota_ClientQuotaMatch_t {
    Box::into_raw(Box::new(ClientQuotaMatch::Exact(unsafe { c_str_to_string(value) })))
        as *mut kafka_common_quota_ClientQuotaMatch_t
}

/// Frees a match built by [`kafka_common_quota_ClientQuotaMatch_exact`]. Null
/// is a no-op; the singletons and a match borrowed from a component are never
/// passed here.
///
/// # Safety
///
/// `self_` must be null or an owned match handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaMatch_destroy(
    self_: *mut kafka_common_quota_ClientQuotaMatch_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ClientQuotaMatch) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;

    use super::*;

    #[test]
    fn singletons_compare_equal_and_switch_on_their_enumerator() {
        unsafe {
            assert_eq!(
                kafka_common_quota_ClientQuotaMatch_default(),
                kafka_common_quota_ClientQuotaMatch_default()
            );
            assert_ne!(
                kafka_common_quota_ClientQuotaMatch_default(),
                kafka_common_quota_ClientQuotaMatch_any()
            );
            assert_eq!(
                kafka_common_quota_ClientQuotaMatch__enum(kafka_common_quota_ClientQuotaMatch_default()),
                kafka_common_quota_ClientQuotaMatch_e::kafka_common_quota_ClientQuotaMatch_DEFAULT
            );
            assert_eq!(
                kafka_common_quota_ClientQuotaMatch__enum(kafka_common_quota_ClientQuotaMatch_any()),
                kafka_common_quota_ClientQuotaMatch_e::kafka_common_quota_ClientQuotaMatch_ANY
            );
            // A borrowed data-less value resolves to the singleton.
            assert_eq!(
                client_quota_match_ptr(&ClientQuotaMatch::Any),
                kafka_common_quota_ClientQuotaMatch_any()
            );
        }
    }

    #[test]
    fn exact_is_owned_and_carries_its_name() {
        let name = CString::new("alice").unwrap();
        unsafe {
            let exact = kafka_common_quota_ClientQuotaMatch_exact(name.as_ptr());
            assert_eq!(
                kafka_common_quota_ClientQuotaMatch__enum(exact),
                kafka_common_quota_ClientQuotaMatch_e::kafka_common_quota_ClientQuotaMatch_EXACT
            );
            assert_eq!(*client_quota_match_ref(exact), ClientQuotaMatch::Exact("alice".to_string()));
            // A borrowed `Exact` is the value itself, not a singleton.
            let value = ClientQuotaMatch::Exact("bob".to_string());
            assert_eq!(client_quota_match_ptr(&value), &value as *const ClientQuotaMatch as *const _);
            kafka_common_quota_ClientQuotaMatch_destroy(exact);
            kafka_common_quota_ClientQuotaMatch_destroy(ptr::null_mut());
        }
    }
}
