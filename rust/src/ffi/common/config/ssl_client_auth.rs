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

//! `kafka_common_config_SslClientAuth_t`: `org.apache.kafka.common.config.SslClientAuth`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_config_SslClientAuth_e`.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::common::config::SslClientAuth;
use crate::ffi::util::{c_str_to_option, into_c_string};

/// Opaque handle to a [`SslClientAuth`] singleton.
#[repr(C)]
pub struct kafka_common_config_SslClientAuth_t {
    _private: [u8; 0],
}

/// The values of [`SslClientAuth`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_config_SslClientAuth_e {
    /// `SslClientAuth::Required`: Java's `REQUIRED`.
    kafka_common_config_SslClientAuth_REQUIRED,
    /// `SslClientAuth::Requested`: Java's `REQUESTED`.
    kafka_common_config_SslClientAuth_REQUESTED,
    /// `SslClientAuth::None`: Java's `NONE`.
    kafka_common_config_SslClientAuth_NONE,
}

/// One static instance per value, indexed by [`kafka_common_config_SslClientAuth_e`].
static VARIANTS: [SslClientAuth; 3] = [SslClientAuth::Required, SslClientAuth::Requested, SslClientAuth::None];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(value: SslClientAuth) -> kafka_common_config_SslClientAuth_e {
    match value {
        SslClientAuth::Required => kafka_common_config_SslClientAuth_e::kafka_common_config_SslClientAuth_REQUIRED,
        SslClientAuth::Requested => kafka_common_config_SslClientAuth_e::kafka_common_config_SslClientAuth_REQUESTED,
        SslClientAuth::None => kafka_common_config_SslClientAuth_e::kafka_common_config_SslClientAuth_NONE,
    }
}

/// The borrowed singleton standing for `value`.
pub(crate) fn singleton(value: SslClientAuth) -> *const kafka_common_config_SslClientAuth_t {
    &VARIANTS[enum_of(value) as usize] as *const SslClientAuth as *const kafka_common_config_SslClientAuth_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `value` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(value: *const kafka_common_config_SslClientAuth_t) -> SslClientAuth {
    unsafe { *(value as *const SslClientAuth) }
}

/// `SslClientAuth.REQUIRED`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_SslClientAuth_required() -> *const kafka_common_config_SslClientAuth_t {
    singleton(SslClientAuth::Required)
}

/// `SslClientAuth.REQUESTED`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_SslClientAuth_requested() -> *const kafka_common_config_SslClientAuth_t {
    singleton(SslClientAuth::Requested)
}

/// `SslClientAuth.NONE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_SslClientAuth_none() -> *const kafka_common_config_SslClientAuth_t {
    singleton(SslClientAuth::None)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_SslClientAuth__enum(
    self_: *const kafka_common_config_SslClientAuth_t,
) -> kafka_common_config_SslClientAuth_e {
    enum_of(unsafe { value_of(self_) })
}

/// `forConfig(String key)`: the singleton for a configuration value, case
/// insensitively; a null `key` is Java's null, giving `NONE`, and a value
/// Java does not know gives `NULL`.
///
/// # Safety
///
/// `key` must be null or a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_SslClientAuth_for_config(
    key: *const c_char,
) -> *const kafka_common_config_SslClientAuth_t {
    let key = unsafe { c_str_to_option(key) };
    SslClientAuth::for_config(key.as_deref()).map_or(std::ptr::null(), singleton)
}

/// `toString()`: Java's constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_SslClientAuth_to_string(
    self_: *const kafka_common_config_SslClientAuth_t,
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
                assert_eq!(kafka_common_config_SslClientAuth__enum(handle) as usize, index);
            }
        }
        assert_eq!(kafka_common_config_SslClientAuth_required(), singleton(SslClientAuth::Required));
        assert_eq!(kafka_common_config_SslClientAuth_none(), singleton(SslClientAuth::None));
    }

    #[test]
    fn methods_follow_the_rust_enum() {
        unsafe {
            for &value in &VARIANTS {
                let s = kafka_common_config_SslClientAuth_to_string(singleton(value));
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), value.to_string());
                assert_eq!(
                    kafka_common_config_SslClientAuth_for_config(s),
                    singleton(value),
                    "toString round-trips through forConfig"
                );
                kafka_string_destroy(s);
            }
            assert_eq!(
                kafka_common_config_SslClientAuth_for_config(std::ptr::null()),
                kafka_common_config_SslClientAuth_none(),
                "Java's null key is NONE"
            );
            let lower = std::ffi::CString::new("requested").unwrap();
            assert_eq!(
                kafka_common_config_SslClientAuth_for_config(lower.as_ptr()),
                kafka_common_config_SslClientAuth_requested(),
                "case insensitive"
            );
            let bogus = std::ffi::CString::new("bogus").unwrap();
            assert!(
                kafka_common_config_SslClientAuth_for_config(bogus.as_ptr()).is_null(),
                "an unknown value is Java's null"
            );
        }
    }
}
