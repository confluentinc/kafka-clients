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

//! `kafka_common_acl_AclPermissionType_t`: `org.apache.kafka.common.acl.AclPermissionType`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_acl_AclPermissionType_e`.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::common::acl::AclPermissionType;
use crate::ffi::util::{c_str_to_string, into_c_string};

/// Opaque handle to a [`AclPermissionType`] singleton.
#[repr(C)]
pub struct kafka_common_acl_AclPermissionType_t {
    _private: [u8; 0],
}

/// The values of [`AclPermissionType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_acl_AclPermissionType_e {
    /// `AclPermissionType::Unknown`: Java's `UNKNOWN`.
    unknown,
    /// `AclPermissionType::Any`: Java's `ANY`.
    any,
    /// `AclPermissionType::Deny`: Java's `DENY`.
    deny,
    /// `AclPermissionType::Allow`: Java's `ALLOW`.
    allow,
}

/// One static instance per value, indexed by [`kafka_common_acl_AclPermissionType_e`].
static VARIANTS: [AclPermissionType; 4] = [
    AclPermissionType::Unknown,
    AclPermissionType::Any,
    AclPermissionType::Deny,
    AclPermissionType::Allow,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(value: AclPermissionType) -> kafka_common_acl_AclPermissionType_e {
    match value {
        AclPermissionType::Unknown => kafka_common_acl_AclPermissionType_e::unknown,
        AclPermissionType::Any => kafka_common_acl_AclPermissionType_e::any,
        AclPermissionType::Deny => kafka_common_acl_AclPermissionType_e::deny,
        AclPermissionType::Allow => kafka_common_acl_AclPermissionType_e::allow,
    }
}

/// The borrowed singleton standing for `value`.
pub(crate) fn singleton(value: AclPermissionType) -> *const kafka_common_acl_AclPermissionType_t {
    &VARIANTS[enum_of(value) as usize] as *const AclPermissionType as *const kafka_common_acl_AclPermissionType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `value` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(value: *const kafka_common_acl_AclPermissionType_t) -> AclPermissionType {
    unsafe { *(value as *const AclPermissionType) }
}

/// `AclPermissionType.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclPermissionType_unknown() -> *const kafka_common_acl_AclPermissionType_t {
    singleton(AclPermissionType::Unknown)
}

/// `AclPermissionType.ANY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclPermissionType_any() -> *const kafka_common_acl_AclPermissionType_t {
    singleton(AclPermissionType::Any)
}

/// `AclPermissionType.DENY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclPermissionType_deny() -> *const kafka_common_acl_AclPermissionType_t {
    singleton(AclPermissionType::Deny)
}

/// `AclPermissionType.ALLOW`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclPermissionType_allow() -> *const kafka_common_acl_AclPermissionType_t {
    singleton(AclPermissionType::Allow)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclPermissionType__enum(
    self_: *const kafka_common_acl_AclPermissionType_t,
) -> kafka_common_acl_AclPermissionType_e {
    enum_of(unsafe { value_of(self_) })
}

/// `code()`: the wire-protocol byte.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclPermissionType_code(
    self_: *const kafka_common_acl_AclPermissionType_t,
) -> i8 {
    unsafe { value_of(self_) }.code()
}

/// `fromCode(byte code)`: the singleton for a wire-protocol byte, `UNKNOWN`
/// for one this client does not know.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclPermissionType_from_code(
    code: i8,
) -> *const kafka_common_acl_AclPermissionType_t {
    singleton(AclPermissionType::from_code(code))
}

/// `fromString(String)`: the singleton named by `str` (Java's constant name),
/// `UNKNOWN` for any other string.
///
/// # Safety
///
/// `str` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclPermissionType_from_string(
    str: *const c_char,
) -> *const kafka_common_acl_AclPermissionType_t {
    singleton(AclPermissionType::from_string(&unsafe { c_str_to_string(str) }))
}

/// `isUnknown()`: whether this is the `UNKNOWN` value.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclPermissionType_is_unknown(
    self_: *const kafka_common_acl_AclPermissionType_t,
) -> i8 {
    i8::from(unsafe { value_of(self_) }.is_unknown())
}

/// `toString()`: Java's constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclPermissionType_to_string(
    self_: *const kafka_common_acl_AclPermissionType_t,
) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &value) in VARIANTS.iter().enumerate() {
            let handle = singleton(value);
            unsafe {
                assert_eq!(value_of(handle), value);
                assert_eq!(kafka_common_acl_AclPermissionType__enum(handle) as usize, index);
            }
        }
        assert_eq!(
            kafka_common_acl_AclPermissionType_unknown(),
            singleton(AclPermissionType::Unknown)
        );
        assert_eq!(kafka_common_acl_AclPermissionType_allow(), singleton(AclPermissionType::Allow));
    }

    #[test]
    fn methods_follow_the_rust_enum() {
        unsafe {
            for &value in &VARIANTS {
                let handle = singleton(value);
                assert_eq!(kafka_common_acl_AclPermissionType_code(handle), value.code());
                assert_eq!(kafka_common_acl_AclPermissionType_from_code(value.code()), handle);
            }
            assert_eq!(
                kafka_common_acl_AclPermissionType_from_code(-1),
                kafka_common_acl_AclPermissionType_unknown()
            );
            assert_eq!(
                kafka_common_acl_AclPermissionType_is_unknown(kafka_common_acl_AclPermissionType_unknown()),
                1
            );
            assert_eq!(
                kafka_common_acl_AclPermissionType_is_unknown(kafka_common_acl_AclPermissionType_any()),
                0
            );
            for &value in &VARIANTS {
                let s = kafka_common_acl_AclPermissionType_to_string(singleton(value));
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), value.to_string());
                assert_eq!(
                    kafka_common_acl_AclPermissionType_from_string(s),
                    singleton(value),
                    "toString round-trips through fromString"
                );
                kafka_string_destroy(s);
            }
            let bogus = std::ffi::CString::new("bogus").unwrap();
            assert_eq!(
                kafka_common_acl_AclPermissionType_from_string(bogus.as_ptr()),
                kafka_common_acl_AclPermissionType_unknown()
            );
        }
    }
}
