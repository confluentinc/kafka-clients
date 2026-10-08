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

//! `kafka_common_resource_PatternType_t`: `org.apache.kafka.common.resource.PatternType`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_resource_PatternType_e`.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::common::resource::PatternType;
use crate::ffi::util::{c_str_to_string, into_c_string};

/// Opaque handle to a [`PatternType`] singleton.
#[repr(C)]
pub struct kafka_common_resource_PatternType_t {
    _private: [u8; 0],
}

/// The values of [`PatternType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_resource_PatternType_e {
    /// `PatternType::Unknown`: Java's `UNKNOWN`.
    kafka_common_resource_PatternType_UNKNOWN,
    /// `PatternType::Any`: Java's `ANY`.
    kafka_common_resource_PatternType_ANY,
    /// `PatternType::Match`: Java's `MATCH`.
    kafka_common_resource_PatternType_MATCH,
    /// `PatternType::Literal`: Java's `LITERAL`.
    kafka_common_resource_PatternType_LITERAL,
    /// `PatternType::Prefixed`: Java's `PREFIXED`.
    kafka_common_resource_PatternType_PREFIXED,
}

/// One static instance per value, indexed by [`kafka_common_resource_PatternType_e`].
static VARIANTS: [PatternType; 5] = [
    PatternType::Unknown,
    PatternType::Any,
    PatternType::Match,
    PatternType::Literal,
    PatternType::Prefixed,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(value: PatternType) -> kafka_common_resource_PatternType_e {
    match value {
        PatternType::Unknown => kafka_common_resource_PatternType_e::kafka_common_resource_PatternType_UNKNOWN,
        PatternType::Any => kafka_common_resource_PatternType_e::kafka_common_resource_PatternType_ANY,
        PatternType::Match => kafka_common_resource_PatternType_e::kafka_common_resource_PatternType_MATCH,
        PatternType::Literal => kafka_common_resource_PatternType_e::kafka_common_resource_PatternType_LITERAL,
        PatternType::Prefixed => kafka_common_resource_PatternType_e::kafka_common_resource_PatternType_PREFIXED,
    }
}

/// The borrowed singleton standing for `value`.
pub(crate) fn singleton(value: PatternType) -> *const kafka_common_resource_PatternType_t {
    &VARIANTS[enum_of(value) as usize] as *const PatternType as *const kafka_common_resource_PatternType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `value` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(value: *const kafka_common_resource_PatternType_t) -> PatternType {
    unsafe { *(value as *const PatternType) }
}

/// `PatternType.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_PatternType_unknown() -> *const kafka_common_resource_PatternType_t {
    singleton(PatternType::Unknown)
}

/// `PatternType.ANY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_PatternType_any() -> *const kafka_common_resource_PatternType_t {
    singleton(PatternType::Any)
}

/// `PatternType.MATCH`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_PatternType_match() -> *const kafka_common_resource_PatternType_t {
    singleton(PatternType::Match)
}

/// `PatternType.LITERAL`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_PatternType_literal() -> *const kafka_common_resource_PatternType_t {
    singleton(PatternType::Literal)
}

/// `PatternType.PREFIXED`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_PatternType_prefixed() -> *const kafka_common_resource_PatternType_t {
    singleton(PatternType::Prefixed)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_PatternType__enum(
    self_: *const kafka_common_resource_PatternType_t,
) -> kafka_common_resource_PatternType_e {
    enum_of(unsafe { value_of(self_) })
}

/// `code()`: the wire-protocol byte.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_PatternType_code(
    self_: *const kafka_common_resource_PatternType_t,
) -> i8 {
    unsafe { value_of(self_) }.code()
}

/// `isUnknown()`: whether this is the `UNKNOWN` value.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_PatternType_is_unknown(
    self_: *const kafka_common_resource_PatternType_t,
) -> i8 {
    i8::from(unsafe { value_of(self_) }.is_unknown())
}

/// `isSpecific()`: whether the value names a concrete pattern type rather
/// than a filter (`ANY`, `MATCH`, `UNKNOWN`).
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_PatternType_is_specific(
    self_: *const kafka_common_resource_PatternType_t,
) -> i8 {
    i8::from(unsafe { value_of(self_) }.is_specific())
}

/// `fromCode(byte code)`: the singleton for a wire-protocol byte, `UNKNOWN`
/// for one this client does not know.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_PatternType_from_code(code: i8) -> *const kafka_common_resource_PatternType_t {
    singleton(PatternType::from_code(code))
}

/// `fromString(String)`: the singleton named by `name` (Java's constant name),
/// `UNKNOWN` for any other string.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_PatternType_from_string(
    name: *const c_char,
) -> *const kafka_common_resource_PatternType_t {
    singleton(PatternType::from_string(&unsafe { c_str_to_string(name) }))
}

/// `toString()`: Java's constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_PatternType_to_string(
    self_: *const kafka_common_resource_PatternType_t,
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
                assert_eq!(kafka_common_resource_PatternType__enum(handle) as usize, index);
            }
        }
        assert_eq!(kafka_common_resource_PatternType_unknown(), singleton(PatternType::Unknown));
        assert_eq!(kafka_common_resource_PatternType_prefixed(), singleton(PatternType::Prefixed));
    }

    #[test]
    fn methods_follow_the_rust_enum() {
        unsafe {
            for &value in &VARIANTS {
                let handle = singleton(value);
                assert_eq!(kafka_common_resource_PatternType_code(handle), value.code());
                assert_eq!(kafka_common_resource_PatternType_from_code(value.code()), handle);
            }
            assert_eq!(
                kafka_common_resource_PatternType_from_code(-1),
                kafka_common_resource_PatternType_unknown()
            );
            assert_eq!(
                kafka_common_resource_PatternType_is_unknown(kafka_common_resource_PatternType_unknown()),
                1
            );
            assert_eq!(
                kafka_common_resource_PatternType_is_unknown(kafka_common_resource_PatternType_any()),
                0
            );
            assert_eq!(
                kafka_common_resource_PatternType_is_specific(kafka_common_resource_PatternType_literal()),
                1
            );
            assert_eq!(
                kafka_common_resource_PatternType_is_specific(kafka_common_resource_PatternType_prefixed()),
                1
            );
            assert_eq!(
                kafka_common_resource_PatternType_is_specific(kafka_common_resource_PatternType_any()),
                0
            );
            assert_eq!(
                kafka_common_resource_PatternType_is_specific(kafka_common_resource_PatternType_match()),
                0
            );
            for &value in &VARIANTS {
                let s = kafka_common_resource_PatternType_to_string(singleton(value));
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), value.to_string());
                assert_eq!(
                    kafka_common_resource_PatternType_from_string(s),
                    singleton(value),
                    "toString round-trips through fromString"
                );
                kafka_string_destroy(s);
            }
            let bogus = std::ffi::CString::new("bogus").unwrap();
            assert_eq!(
                kafka_common_resource_PatternType_from_string(bogus.as_ptr()),
                kafka_common_resource_PatternType_unknown()
            );
        }
    }
}
