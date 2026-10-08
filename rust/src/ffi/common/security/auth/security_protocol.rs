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

//! `kafka_common_security_auth_SecurityProtocol_t`:
//! `org.apache.kafka.common.security.auth.SecurityProtocol` as borrowed
//! singletons (CLAUDE.md §4 rule 2).

#![expect(non_camel_case_types)]

use std::ffi::{CStr, c_char};
use std::ptr;

use crate::common::security::auth::SecurityProtocol;
use crate::ffi::util::{box_string_list, c_str_to_string, into_c_string, kafka_List_t};

/// Opaque handle to a [`SecurityProtocol`] value; only the four singletons
/// exist, never freed, comparable with `==`.
#[repr(C)]
pub struct kafka_common_security_auth_SecurityProtocol_t {
    _private: [u8; 0],
}

/// The value behind a [`kafka_common_security_auth_SecurityProtocol_t`], for a
/// `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_security_auth_SecurityProtocol_e {
    plaintext,
    ssl,
    sasl_plaintext,
    sasl_ssl,
}

static VARIANTS: [SecurityProtocol; 4] = [
    SecurityProtocol::Plaintext,
    SecurityProtocol::Ssl,
    SecurityProtocol::SaslPlaintext,
    SecurityProtocol::SaslSsl,
];

/// `name()` of each variant, in `VARIANTS` order; Java's enum constant names.
static NAMES: [&CStr; 4] = [c"PLAINTEXT", c"SSL", c"SASL_PLAINTEXT", c"SASL_SSL"];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(value: SecurityProtocol) -> kafka_common_security_auth_SecurityProtocol_e {
    match value {
        SecurityProtocol::Plaintext => kafka_common_security_auth_SecurityProtocol_e::plaintext,
        SecurityProtocol::Ssl => kafka_common_security_auth_SecurityProtocol_e::ssl,
        SecurityProtocol::SaslPlaintext => kafka_common_security_auth_SecurityProtocol_e::sasl_plaintext,
        SecurityProtocol::SaslSsl => kafka_common_security_auth_SecurityProtocol_e::sasl_ssl,
    }
}

/// The borrowed singleton for `value`.
pub(crate) fn singleton(value: SecurityProtocol) -> *const kafka_common_security_auth_SecurityProtocol_t {
    &VARIANTS[enum_of(value) as usize] as *const SecurityProtocol
        as *const kafka_common_security_auth_SecurityProtocol_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `value` must be one of the singletons.
pub(crate) unsafe fn value_of(value: *const kafka_common_security_auth_SecurityProtocol_t) -> SecurityProtocol {
    unsafe { *(value as *const SecurityProtocol) }
}

/// The singleton for an optional value, null for `None`.
fn optional_singleton(value: Option<SecurityProtocol>) -> *const kafka_common_security_auth_SecurityProtocol_t {
    value.map_or(ptr::null(), singleton)
}

/// `SecurityProtocol.PLAINTEXT`: un-authenticated, non-encrypted channel.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_SecurityProtocol_plaintext()
-> *const kafka_common_security_auth_SecurityProtocol_t {
    singleton(SecurityProtocol::Plaintext)
}

/// `SecurityProtocol.SSL`: SSL channel.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_SecurityProtocol_ssl()
-> *const kafka_common_security_auth_SecurityProtocol_t {
    singleton(SecurityProtocol::Ssl)
}

/// `SecurityProtocol.SASL_PLAINTEXT`: SASL authenticated, non-encrypted channel.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_SecurityProtocol_sasl_plaintext()
-> *const kafka_common_security_auth_SecurityProtocol_t {
    singleton(SecurityProtocol::SaslPlaintext)
}

/// `SecurityProtocol.SASL_SSL`: SASL authenticated, SSL channel.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_SecurityProtocol_sasl_ssl()
-> *const kafka_common_security_auth_SecurityProtocol_t {
    singleton(SecurityProtocol::SaslSsl)
}

/// The C enumerator of the value behind `self_`.
///
/// # Safety
///
/// `self_` must be one of the singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_SecurityProtocol__enum(
    self_: *const kafka_common_security_auth_SecurityProtocol_t,
) -> kafka_common_security_auth_SecurityProtocol_e {
    enum_of(unsafe { value_of(self_) })
}

/// `id`: the permanent and immutable id of the protocol, used on the wire.
///
/// # Safety
///
/// `self_` must be one of the singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_SecurityProtocol_id(
    self_: *const kafka_common_security_auth_SecurityProtocol_t,
) -> i16 {
    unsafe { value_of(self_) }.id()
}

/// `name`: the enum constant's name, e.g. `SASL_SSL`; a static string, never
/// freed.
///
/// # Safety
///
/// `self_` must be one of the singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_SecurityProtocol_name(
    self_: *const kafka_common_security_auth_SecurityProtocol_t,
) -> *const c_char {
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// `forId(short id)`: the singleton with that id, or null for an unknown id.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_SecurityProtocol_for_id(
    id: i16,
) -> *const kafka_common_security_auth_SecurityProtocol_t {
    optional_singleton(SecurityProtocol::for_id(id))
}

/// `forName(String name)`: the singleton with that name, compared
/// case-insensitively, or null for an unknown name.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_SecurityProtocol_for_name(
    name: *const c_char,
) -> *const kafka_common_security_auth_SecurityProtocol_t {
    optional_singleton(SecurityProtocol::for_name(&unsafe { c_str_to_string(name) }))
}

/// `names()`: an owned list of owned NUL-terminated names, in id order.
/// Freed with `kafka_List_destroy`, which also frees the strings.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_security_auth_SecurityProtocol_names() -> *mut kafka_List_t {
    box_string_list(SecurityProtocol::names())
}

/// `toString()`, the same text as `name`, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be one of the singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_security_auth_SecurityProtocol_to_string(
    self_: *const kafka_common_security_auth_SecurityProtocol_t,
) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_string_destroy};

    #[test]
    fn singletons_round_trip_through_id_name_and_enum() {
        let all = [
            (
                kafka_common_security_auth_SecurityProtocol_plaintext(),
                kafka_common_security_auth_SecurityProtocol_e::plaintext,
                0,
                "PLAINTEXT",
            ),
            (
                kafka_common_security_auth_SecurityProtocol_ssl(),
                kafka_common_security_auth_SecurityProtocol_e::ssl,
                1,
                "SSL",
            ),
            (
                kafka_common_security_auth_SecurityProtocol_sasl_plaintext(),
                kafka_common_security_auth_SecurityProtocol_e::sasl_plaintext,
                2,
                "SASL_PLAINTEXT",
            ),
            (
                kafka_common_security_auth_SecurityProtocol_sasl_ssl(),
                kafka_common_security_auth_SecurityProtocol_e::sasl_ssl,
                3,
                "SASL_SSL",
            ),
        ];
        for (value, enumerator, id, name) in all {
            unsafe {
                assert_eq!(kafka_common_security_auth_SecurityProtocol__enum(value), enumerator);
                assert_eq!(kafka_common_security_auth_SecurityProtocol_id(value), id);
                assert_eq!(
                    CStr::from_ptr(kafka_common_security_auth_SecurityProtocol_name(value)).to_str(),
                    Ok(name)
                );
                assert_eq!(kafka_common_security_auth_SecurityProtocol_for_id(id), value);
                let lower = CString::new(name.to_lowercase()).unwrap();
                assert_eq!(kafka_common_security_auth_SecurityProtocol_for_name(lower.as_ptr()), value);
                let s = kafka_common_security_auth_SecurityProtocol_to_string(value);
                assert_eq!(CStr::from_ptr(s).to_str(), Ok(name));
                kafka_string_destroy(s);
            }
        }
    }

    #[test]
    fn unknown_id_and_name_are_null_and_names_lists_all_four() {
        let bogus = CString::new("KERBEROS").unwrap();
        unsafe {
            assert!(kafka_common_security_auth_SecurityProtocol_for_id(4).is_null());
            assert!(kafka_common_security_auth_SecurityProtocol_for_id(-1).is_null());
            assert!(kafka_common_security_auth_SecurityProtocol_for_name(bogus.as_ptr()).is_null());
            let names = kafka_common_security_auth_SecurityProtocol_names();
            assert_eq!(kafka_List_size(names), 4);
            let listed: Vec<&str> = (0..4)
                .map(|i| CStr::from_ptr(kafka_List_get(names, i) as *const c_char).to_str().unwrap())
                .collect();
            assert_eq!(listed, SecurityProtocol::names());
            kafka_List_destroy(names);
        }
    }
}
