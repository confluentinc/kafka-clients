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

//! `kafka_admin_ScramMechanism_t`:
//! `org.apache.kafka.clients.admin.ScramMechanism` (CLAUDE.md §4, "Enums"):
//! borrowed per-value singletons plus the C enum
//! `kafka_admin_ScramMechanism_e`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char};
use std::sync::LazyLock;

use crate::admin::ScramMechanism;
use crate::ffi::util::{c_str_to_string, owned_c_string};

/// Opaque handle to a [`ScramMechanism`] singleton.
#[repr(C)]
pub struct kafka_admin_ScramMechanism_t {
    _private: [u8; 0],
}

/// The values of [`ScramMechanism`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_ScramMechanism_e {
    unknown,
    scram_sha256,
    scram_sha512,
}

/// One static instance per value, indexed by [`kafka_admin_ScramMechanism_e`].
static VARIANTS: [ScramMechanism; 3] = [
    ScramMechanism::Unknown,
    ScramMechanism::ScramSha256,
    ScramMechanism::ScramSha512,
];

/// The NUL-terminated `mechanismName()` of each value, borrowed out for the
/// life of the process.
static MECHANISM_NAMES: LazyLock<Vec<CString>> =
    LazyLock::new(|| VARIANTS.iter().map(|m| owned_c_string(m.mechanism_name())).collect());

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(mechanism: ScramMechanism) -> kafka_admin_ScramMechanism_e {
    match mechanism {
        ScramMechanism::Unknown => kafka_admin_ScramMechanism_e::unknown,
        ScramMechanism::ScramSha256 => kafka_admin_ScramMechanism_e::scram_sha256,
        ScramMechanism::ScramSha512 => kafka_admin_ScramMechanism_e::scram_sha512,
    }
}

/// The borrowed singleton standing for `mechanism`.
pub(crate) fn scram_mechanism_singleton(mechanism: ScramMechanism) -> *const kafka_admin_ScramMechanism_t {
    &VARIANTS[enum_of(mechanism) as usize] as *const ScramMechanism as *const kafka_admin_ScramMechanism_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `mechanism` must be a singleton returned by this module.
pub(crate) unsafe fn scram_mechanism_value_of(mechanism: *const kafka_admin_ScramMechanism_t) -> ScramMechanism {
    unsafe { *(mechanism as *const ScramMechanism) }
}

/// `ScramMechanism.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_unknown() -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::Unknown)
}

/// `ScramMechanism.SCRAM_SHA_256`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_scram_sha256() -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::ScramSha256)
}

/// `ScramMechanism.SCRAM_SHA_512`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_scram_sha512() -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::ScramSha512)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism__enum(
    self_: *const kafka_admin_ScramMechanism_t,
) -> kafka_admin_ScramMechanism_e {
    enum_of(unsafe { scram_mechanism_value_of(self_) })
}

/// `ScramMechanism.fromType(byte type)`: the borrowed singleton with that
/// wire type, `UNKNOWN` for a type Java does not know.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_from_type(r#type: i8) -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::from_type(r#type))
}

/// `ScramMechanism.fromMechanismName(String mechanismName)`: the borrowed
/// singleton with that SASL mechanism name (`"SCRAM-SHA-256"`, ...),
/// `UNKNOWN` for a name Java does not know.
///
/// # Safety
///
/// `mechanism_name` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism_from_mechanism_name(
    mechanism_name: *const c_char,
) -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::from_mechanism_name(&unsafe { c_str_to_string(mechanism_name) }))
}

/// `mechanismName()`: the SASL mechanism name, a borrowed string valid for
/// the life of the process and never passed to `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism_mechanism_name(
    self_: *const kafka_admin_ScramMechanism_t,
) -> *const c_char {
    MECHANISM_NAMES[enum_of(unsafe { scram_mechanism_value_of(self_) }) as usize].as_ptr()
}

/// `type()`: the wire-protocol byte of the mechanism.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism_type(self_: *const kafka_admin_ScramMechanism_t) -> i8 {
    unsafe { scram_mechanism_value_of(self_) }.r#type()
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    #[test]
    fn singletons_round_trip_through_type_and_name() {
        for (index, &mechanism) in VARIANTS.iter().enumerate() {
            let handle = scram_mechanism_singleton(mechanism);
            unsafe {
                assert_eq!(scram_mechanism_value_of(handle), mechanism);
                assert_eq!(kafka_admin_ScramMechanism__enum(handle) as usize, index);
                assert_eq!(
                    kafka_admin_ScramMechanism_from_type(kafka_admin_ScramMechanism_type(handle)),
                    handle
                );
                let name = kafka_admin_ScramMechanism_mechanism_name(handle);
                assert_eq!(CStr::from_ptr(name).to_str().unwrap(), mechanism.mechanism_name());
                assert_eq!(kafka_admin_ScramMechanism_from_mechanism_name(name), handle);
            }
        }
        assert_eq!(
            kafka_admin_ScramMechanism_scram_sha512(),
            scram_mechanism_singleton(ScramMechanism::ScramSha512)
        );
        assert_eq!(kafka_admin_ScramMechanism_from_type(9), kafka_admin_ScramMechanism_unknown());
        assert_eq!(
            unsafe { kafka_admin_ScramMechanism_from_mechanism_name(c"scram-sha-256".as_ptr()) },
            kafka_admin_ScramMechanism_unknown()
        );
        assert_eq!(
            unsafe { kafka_admin_ScramMechanism_type(kafka_admin_ScramMechanism_scram_sha256()) },
            1
        );
    }
}
