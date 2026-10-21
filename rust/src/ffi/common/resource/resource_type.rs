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

use std::ffi::{c_char, c_void};

use crate::common::resource::ResourceType;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t};

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
    kafka_common_resource_ResourceType_UNKNOWN,
    /// `ResourceType::Any`: Java's `ANY`.
    kafka_common_resource_ResourceType_ANY,
    /// `ResourceType::Topic`: Java's `TOPIC`.
    kafka_common_resource_ResourceType_TOPIC,
    /// `ResourceType::Group`: Java's `GROUP`.
    kafka_common_resource_ResourceType_GROUP,
    /// `ResourceType::Cluster`: Java's `CLUSTER`.
    kafka_common_resource_ResourceType_CLUSTER,
    /// `ResourceType::TransactionalId`: Java's `TRANSACTIONAL_ID`.
    kafka_common_resource_ResourceType_TRANSACTIONAL_ID,
    /// `ResourceType::DelegationToken`: Java's `DELEGATION_TOKEN`.
    kafka_common_resource_ResourceType_DELEGATION_TOKEN,
    /// `ResourceType::User`: Java's `USER`.
    kafka_common_resource_ResourceType_USER,
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
        ResourceType::Unknown => kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_UNKNOWN,
        ResourceType::Any => kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_ANY,
        ResourceType::Topic => kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_TOPIC,
        ResourceType::Group => kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_GROUP,
        ResourceType::Cluster => kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_CLUSTER,
        ResourceType::TransactionalId => {
            kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_TRANSACTIONAL_ID
        },
        ResourceType::DelegationToken => {
            kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_DELEGATION_TOKEN
        },
        ResourceType::User => kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_USER,
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
    // SAFETY: Per this function's `# Safety`, `value` is a singleton returned by this module, i.e.
    // a pointer to an element of the `static` `VARIANTS` array (`singleton`), so it points at a
    // live, initialised `ResourceType`, which is `Copy` and read out here.
    unsafe { *(value as *const ResourceType) }
}

/// `ResourceType.UNKNOWN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_unknown() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Unknown)
}

/// `ResourceType.ANY`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_any() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Any)
}

/// `ResourceType.TOPIC`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_topic() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Topic)
}

/// `ResourceType.GROUP`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_group() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Group)
}

/// `ResourceType.CLUSTER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_cluster() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::Cluster)
}

/// `ResourceType.TRANSACTIONAL_ID`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_transactional_id() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::TransactionalId)
}

/// `ResourceType.DELEGATION_TOKEN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_delegation_token() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::DelegationToken)
}

/// `ResourceType.USER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_user() -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::User)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_common_resource_ResourceType_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_common_resource_ResourceType_e::kafka_common_resource_ResourceType_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType__enum(
    self_: *const kafka_common_resource_ResourceType_t,
) -> kafka_common_resource_ResourceType_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    enum_of(unsafe { value_of(self_) })
}

/// `code()`: the wire-protocol byte.
///
/// A caught Rust panic returns -1, which is no code of this Java `byte`
/// enum: its 0 is the valid `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType_code(
    self_: *const kafka_common_resource_ResourceType_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    unsafe { value_of(self_) }.code()
}

/// `fromCode(byte code)`: the singleton for a wire-protocol byte, `UNKNOWN`
/// for one this client does not know.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_from_code(
    code: i8,
) -> *const kafka_common_resource_ResourceType_t {
    singleton(ResourceType::from_code(code))
}

/// `ResourceType.values()`: an owned list of the borrowed singletons in
/// declaration order, freed with `kafka_List_destroy` (the elements are
/// never freed).
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourceType_values() -> *mut kafka_List_t {
    let elements = ResourceType::values().iter().map(|&t| singleton(t) as *mut c_void).collect();
    box_list(elements, None)
}

/// `fromString(String)`: the singleton named by `str` (Java's constant name),
/// `UNKNOWN` for any other string.
///
/// # Safety
///
/// `str` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType_from_string(
    str: *const c_char,
) -> *const kafka_common_resource_ResourceType_t {
    // SAFETY: Per this function's `# Safety`, `str` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    singleton(ResourceType::from_string(&unsafe { c_str_to_string(str) }))
}

/// `isUnknown()`: whether this is the `UNKNOWN` value.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType_is_unknown(
    self_: *const kafka_common_resource_ResourceType_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    i8::from(unsafe { value_of(self_) }.is_unknown())
}

/// `toString()`: Java's constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourceType_to_string(
    self_: *const kafka_common_resource_ResourceType_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_string_destroy};

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &value) in VARIANTS.iter().enumerate() {
            let handle = singleton(value);
            // SAFETY: Test code: every handle passed is a singleton from `singleton`, which points
            // into the static `VARIANTS` array and is never freed.
            unsafe {
                assert_eq!(value_of(handle), value);
                assert_eq!(kafka_common_resource_ResourceType__enum(handle) as usize, index);
            }
        }
        assert_eq!(kafka_common_resource_ResourceType_unknown(), singleton(ResourceType::Unknown));
        assert_eq!(kafka_common_resource_ResourceType_user(), singleton(ResourceType::User));
    }

    #[test]
    fn values_lists_the_singletons_in_declaration_order() {
        // SAFETY: Test code: the owned list from `values` is destroyed once, after its elements,
        // borrowed singletons, are read.
        unsafe {
            let values = kafka_common_resource_ResourceType_values();
            assert_eq!(kafka_List_size(values), 8);
            for (index, &value) in ResourceType::values().iter().enumerate() {
                assert_eq!(
                    kafka_List_get(values, index as i32) as *const kafka_common_resource_ResourceType_t,
                    singleton(value)
                );
            }
            kafka_List_destroy(values);
        }
    }

    #[test]
    fn methods_follow_the_rust_enum() {
        // SAFETY: Test code: every handle passed is a singleton from `singleton` or this module's
        // getters, which point into the static `VARIANTS` array and are never freed; each string
        // from `to_string` is destroyed once, after `from_string` reads it; `bogus` is a `CString`
        // that outlives the call reading it.
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
