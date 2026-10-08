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

//! `kafka_common_resource_ResourceType_t`: `org.apache.kafka.common.resource.ResourceType`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_resource_ResourceType_e`.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::common::resource::ResourceType;
use crate::ffi::util::{c_str_to_string, into_c_string};

/// Opaque handle to a [`ResourceType`] singleton.
#[repr(C)]
pub struct kafka_common_resource_ResourceType_t {
    _private: [u8; 0],
}

/// The values of [`ResourceType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_resource_ResourceType_e {
    /// `ResourceType::Unknown`: Java's `UNKNOWN`.
    unknown,
    /// `ResourceType::Any`: Java's `ANY`.
    any,
    /// `ResourceType::Topic`: Java's `TOPIC`.
    topic,
    /// `ResourceType::Group`: Java's `GROUP`.
    group,
    /// `ResourceType::Cluster`: Java's `CLUSTER`.
    cluster,
    /// `ResourceType::TransactionalId`: Java's `TRANSACTIONAL_ID`.
    transactional_id,
    /// `ResourceType::DelegationToken`: Java's `DELEGATION_TOKEN`.
    delegation_token,
    /// `ResourceType::User`: Java's `USER`.
    user,
}

/// One static instance per value, indexed by [`kafka_common_resource_ResourceType_e`].
static VARIANTS: [ResourceType; 8] = [
    ResourceType::Unknown,
    ResourceType::Any,
    ResourceType::Topic,
    ResourceType::Group,
    ResourceType::Cluster,
    ResourceType::TransactionalId,
    ResourceType::DelegationToken,
    ResourceType::User,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(value: ResourceType) -> kafka_common_resource_ResourceType_e {
    match value {
        ResourceType::Unknown => kafka_common_resource_ResourceType_e::unknown,
        ResourceType::Any => kafka_common_resource_ResourceType_e::any,
        ResourceType::Topic => kafka_common_resource_ResourceType_e::topic,
        ResourceType::Group => kafka_common_resource_ResourceType_e::group,
        ResourceType::Cluster => kafka_common_resource_ResourceType_e::cluster,
        ResourceType::TransactionalId => kafka_common_resource_ResourceType_e::transactional_id,
        ResourceType::DelegationToken => kafka_common_resource_ResourceType_e::delegation_token,
        ResourceType::User => kafka_common_resource_ResourceType_e::user,
    }
}

/// The borrowed singleton standing for `value`.
pub(crate) fn singleton(value: ResourceType) -> *const kafka_common_resource_ResourceType_t {
    &VARIANTS[enum_of(value) as usize] as *const ResourceType as *const kafka_common_resource_ResourceType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `value` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(value: *const kafka_common_resource_ResourceType_t) -> ResourceType {
    unsafe { *(value as *const ResourceType) }
}

/// `ResourceType.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_unknown() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Unknown)
}

/// `ResourceType.ANY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_any() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Any)
}

/// `ResourceType.TOPIC`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_topic() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Topic)
}

/// `ResourceType.GROUP`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_group() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Group)
}

/// `ResourceType.CLUSTER`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_cluster() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Cluster)
}

/// `ResourceType.TRANSACTIONAL_ID`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_transactional_id() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::TransactionalId)
}

/// `ResourceType.DELEGATION_TOKEN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_delegation_token() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::DelegationToken)
}

/// `ResourceType.USER`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_user() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::User)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType__enum(
    self_: *const kafka_common_resource_ResourceType_t,
) -> kafka_common_resource_ResourceType_e {
    enum_of(unsafe { value_of(self_) })
}

/// `code()`: the wire-protocol byte.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType_code(
    self_: *const kafka_common_resource_ResourceType_t,
) -> i8 {
    unsafe { value_of(self_) }.code()
}

/// `fromCode(byte code)`: the singleton for a wire-protocol byte, `UNKNOWN`
/// for one this client does not know.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_from_code(
    code: i8,
) -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::from_code(code))
}

/// `fromString(String)`: the singleton named by `str` (Java's constant name),
/// `UNKNOWN` for any other string.
///
/// # Safety
///
/// `str` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType_from_string(
    str: *const c_char,
) -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::from_string(&unsafe { c_str_to_string(str) }))
}

/// `isUnknown()`: whether this is the `UNKNOWN` value.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType_is_unknown(
    self_: *const kafka_common_resource_ResourceType_t,
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
pub unsafe extern "C" fn kafka_common_resource_ResourceType_to_string(
    self_: *const kafka_common_resource_ResourceType_t,
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
                assert_eq!(kafka_common_resource_ResourceType__enum(handle) as usize, index);
            }
        }
        assert_eq!(kafka_common_resource_ResourceType_unknown(), singleton(ResourceType::Unknown));
        assert_eq!(kafka_common_resource_ResourceType_user(), singleton(ResourceType::User));
    }

    #[test]
    fn methods_follow_the_rust_enum() {
        unsafe {
            for &value in &VARIANTS {
                let handle = singleton(value);
                assert_eq!(kafka_common_resource_ResourceType_code(handle), value.code());
                assert_eq!(kafka_common_resource_ResourceType_from_code(value.code()), handle);
            }
            assert_eq!(
                kafka_common_resource_ResourceType_from_code(-1),
                kafka_common_resource_ResourceType_unknown()
            );
            assert_eq!(
                kafka_common_resource_ResourceType_is_unknown(kafka_common_resource_ResourceType_unknown()),
                1
            );
            assert_eq!(
                kafka_common_resource_ResourceType_is_unknown(kafka_common_resource_ResourceType_any()),
                0
            );
            for &value in &VARIANTS {
                let s = kafka_common_resource_ResourceType_to_string(singleton(value));
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), value.to_string());
                assert_eq!(
                    kafka_common_resource_ResourceType_from_string(s),
                    singleton(value),
                    "toString round-trips through fromString"
                );
                kafka_string_destroy(s);
            }
            let bogus = std::ffi::CString::new("bogus").unwrap();
            assert_eq!(
                kafka_common_resource_ResourceType_from_string(bogus.as_ptr()),
                kafka_common_resource_ResourceType_unknown()
            );
        }
    }
}
