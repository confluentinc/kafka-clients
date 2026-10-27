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

use crate::ffi::util::into_c_string;
use std::ffi::{CString, c_char};
use std::sync::LazyLock;

use crate::admin::ScramMechanism;
use crate::ffi::ffi_guard;
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
    kafka_admin_ScramMechanism_UNKNOWN,
    kafka_admin_ScramMechanism_SCRAM_SHA_256,
    kafka_admin_ScramMechanism_SCRAM_SHA_512,
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
        ScramMechanism::Unknown => kafka_admin_ScramMechanism_e::kafka_admin_ScramMechanism_UNKNOWN,
        ScramMechanism::ScramSha256 => kafka_admin_ScramMechanism_e::kafka_admin_ScramMechanism_SCRAM_SHA_256,
        ScramMechanism::ScramSha512 => kafka_admin_ScramMechanism_e::kafka_admin_ScramMechanism_SCRAM_SHA_512,
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
    // SAFETY: Per this function's `# Safety`, `mechanism` is a singleton returned by this module.
    // `scram_mechanism_singleton` is the only producer and points it at an element of the static
    // `VARIANTS`, a `ScramMechanism` that is never freed or mutated, so the read copies a live
    // value.
    unsafe { *(mechanism as *const ScramMechanism) }
}

/// Returns `ScramMechanism.UNKNOWN`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_unknown() -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::Unknown)
}

/// Returns `ScramMechanism.SCRAM_SHA_256`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_scram_sha_256() -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::ScramSha256)
}

/// Returns `ScramMechanism.SCRAM_SHA_512`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_scram_sha_512() -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::ScramSha512)
}

/// Returns the C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_admin_ScramMechanism_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_admin_ScramMechanism_e::kafka_admin_ScramMechanism_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism__enum(
    self_: *const kafka_admin_ScramMechanism_t,
) -> kafka_admin_ScramMechanism_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `scram_mechanism_value_of` requires.
    enum_of(unsafe { scram_mechanism_value_of(self_) })
}

/// Returns the constant's name, as Java's inherited `Enum.toString()`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism_to_string(
    self_: *const kafka_admin_ScramMechanism_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `scram_mechanism_value_of` requires; the value is copied out and its name copied into an owned string.
    into_c_string(&unsafe { scram_mechanism_value_of(self_) }.to_string())
}

/// # Parameters
///
/// - `type` - the type indicator
///
/// # Returns
///
/// the instance corresponding to the given type indicator, otherwise `UNKNOWN`
///
/// # Safety
///
/// The returned singleton is borrowed and never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ScramMechanism_from_type(r#type: i8) -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(ScramMechanism::from_type(r#type))
}

/// `ScramMechanism.fromMechanismName(String mechanismName)`: the singleton with that SASL
/// mechanism name (`"SCRAM-SHA-256"`, ...),
/// `UNKNOWN` for a name Java does not know.
///
/// # Parameters
///
/// - `mechanism_name` - the SASL SCRAM mechanism name
///
/// # Returns
///
/// the corresponding SASL SCRAM mechanism enum, otherwise `UNKNOWN`
///
/// # Safety
///
/// `mechanism_name` must be a valid NUL-terminated string.
///
/// The returned singleton is borrowed and never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism_from_mechanism_name(
    mechanism_name: *const c_char,
) -> *const kafka_admin_ScramMechanism_t {
    // SAFETY: Per this function's `# Safety`, `mechanism_name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires.
    scram_mechanism_singleton(ScramMechanism::from_mechanism_name(&unsafe { c_str_to_string(mechanism_name) }))
}

/// `mechanismName()`: the SASL mechanism name.
///
/// # Returns
///
/// the corresponding SASL SCRAM mechanism name
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
///
/// The returned string is borrowed, valid for the life of the process, and is never passed to
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism_mechanism_name(
    self_: *const kafka_admin_ScramMechanism_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `scram_mechanism_value_of` requires. The returned name is a `CString` in the static
    // `MECHANISM_NAMES`, never freed.
    MECHANISM_NAMES[enum_of(unsafe { scram_mechanism_value_of(self_) }) as usize].as_ptr()
}

/// # Returns
///
/// the type indicator for this SASL SCRAM mechanism
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. A caught Rust panic returns -1, which is
/// no code of this Java `byte` enum: its 0 is the valid `UNKNOWN`.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramMechanism_type(self_: *const kafka_admin_ScramMechanism_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `scram_mechanism_value_of` requires.
    unsafe { scram_mechanism_value_of(self_) }.r#type()
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    #[test]
    fn singletons_round_trip_through_type_and_name() {
        // SAFETY: Test code: every handle is a singleton from this module, as the reads require;
        // each `name` is a static NUL-terminated string, and the parsed literal is one too.
        unsafe {
            for (index, &mechanism) in VARIANTS.iter().enumerate() {
                let handle = scram_mechanism_singleton(mechanism);
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
            assert_eq!(
                kafka_admin_ScramMechanism_scram_sha_512(),
                scram_mechanism_singleton(ScramMechanism::ScramSha512)
            );
            assert_eq!(kafka_admin_ScramMechanism_from_type(9), kafka_admin_ScramMechanism_unknown());
            assert_eq!(
                kafka_admin_ScramMechanism_from_mechanism_name(c"scram-sha-256".as_ptr()),
                kafka_admin_ScramMechanism_unknown()
            );
            assert_eq!(kafka_admin_ScramMechanism_type(kafka_admin_ScramMechanism_scram_sha_256()), 1);
        }
    }
}
