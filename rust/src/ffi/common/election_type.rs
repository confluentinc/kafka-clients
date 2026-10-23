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

//! `kafka_common_ElectionType_t`: `org.apache.kafka.common.ElectionType`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_ElectionType_e`.

#![expect(non_camel_case_types)]

use crate::ffi::util::into_c_string;
use std::ffi::c_char;
use std::ffi::c_void;

use crate::common::ElectionType;
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, kafka_List_t};

/// Opaque handle to an [`ElectionType`] singleton.
#[repr(C)]
pub struct kafka_common_ElectionType_t {
    _private: [u8; 0],
}

/// The values of [`ElectionType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_ElectionType_e {
    kafka_common_ElectionType_PREFERRED,
    kafka_common_ElectionType_UNCLEAN,
}

/// One static instance per value, indexed by [`kafka_common_ElectionType_e`].
static VARIANTS: [ElectionType; 2] = [ElectionType::Preferred, ElectionType::Unclean];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(election_type: ElectionType) -> kafka_common_ElectionType_e {
    match election_type {
        ElectionType::Preferred => kafka_common_ElectionType_e::kafka_common_ElectionType_PREFERRED,
        ElectionType::Unclean => kafka_common_ElectionType_e::kafka_common_ElectionType_UNCLEAN,
    }
}

/// The borrowed singleton standing for `election_type`.
pub(crate) fn singleton(election_type: ElectionType) -> *const kafka_common_ElectionType_t {
    &VARIANTS[enum_of(election_type) as usize] as *const ElectionType as *const kafka_common_ElectionType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `election_type` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(election_type: *const kafka_common_ElectionType_t) -> ElectionType {
    // SAFETY: Per this function's `# Safety`, `election_type` is a singleton returned by this
    // module, i.e. a pointer to an element of the `static` `VARIANTS` array (`singleton`), so it
    // points at a live, initialised `ElectionType`, which is `Copy` and read out here.
    unsafe { *(election_type as *const ElectionType) }
}

/// `ElectionType.PREFERRED`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ElectionType_preferred() -> *const kafka_common_ElectionType_t {
    singleton(ElectionType::Preferred)
}

/// `ElectionType.UNCLEAN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ElectionType_unclean() -> *const kafka_common_ElectionType_t {
    singleton(ElectionType::Unclean)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_common_ElectionType_PREFERRED`, the first enumerator: the
/// enum has no `UNKNOWN` constant, so the panic cannot be told apart from that value, and is only
/// logged.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_common_ElectionType_e::kafka_common_ElectionType_PREFERRED)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ElectionType__enum(
    self_: *const kafka_common_ElectionType_t,
) -> kafka_common_ElectionType_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    enum_of(unsafe { value_of(self_) })
}

/// `toString()`, Java's inherited `Enum.toString()`: the constant's name, as an owned string freed
/// with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ElectionType_to_string(self_: *const kafka_common_ElectionType_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out and its name copied into an owned string.
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

/// `value`: the wire-protocol byte.
///
/// A caught Rust panic returns -1, which is no code of this Java `byte`
/// enum: its 0 is the valid `PREFERRED`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ElectionType_value(self_: *const kafka_common_ElectionType_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    unsafe { value_of(self_) }.value()
}

/// `ElectionType.valueOf(byte value)`: returns the singleton, or stores the
/// owned `IllegalArgumentException` translation in `*out_err` and returns
/// `NULL` for an unknown value.
///
/// # Safety
///
/// `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ElectionType_value_of(
    value: i8,
    out_err: *mut *mut kafka_common_Error_t,
) -> *const kafka_common_ElectionType_t {
    match ElectionType::value_of(value) {
        Ok(election_type) => singleton(election_type),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `ElectionType.values()`: an owned list of the borrowed singletons in
/// declaration order, freed with `kafka_List_destroy` (the elements are
/// never freed).
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ElectionType_values() -> *mut kafka_List_t {
    let elements = ElectionType::values().iter().map(|&t| singleton(t) as *mut c_void).collect();
    box_list(elements, None)
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_argument_error;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size};

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &election_type) in VARIANTS.iter().enumerate() {
            let handle = singleton(election_type);
            // SAFETY: Test code: every handle passed is a singleton from `singleton`, which points
            // into the static `VARIANTS` array and is never freed.
            unsafe {
                assert_eq!(value_of(handle), election_type);
                assert_eq!(kafka_common_ElectionType__enum(handle) as usize, index);
            }
        }
        assert_eq!(kafka_common_ElectionType_preferred(), singleton(ElectionType::Preferred));
        assert_eq!(kafka_common_ElectionType_unclean(), singleton(ElectionType::Unclean));
    }

    #[test]
    fn value_value_of_and_values_follow_java() {
        // SAFETY: Test code: the singletons are never freed, `error` is a valid slot pointing at
        // null before each call, the error from the unknown value is destroyed once, and the
        // owned list from `values` is destroyed once, after its elements are read.
        unsafe {
            assert_eq!(kafka_common_ElectionType_value(kafka_common_ElectionType_unclean()), 1);
            let mut error = ptr::null_mut();
            let election_type = kafka_common_ElectionType_value_of(0, &mut error);
            assert!(error.is_null());
            assert_eq!(election_type, kafka_common_ElectionType_preferred());
            let unknown = kafka_common_ElectionType_value_of(9, &mut error);
            assert!(unknown.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);

            let values = kafka_common_ElectionType_values();
            assert_eq!(kafka_List_size(values), 2);
            assert_eq!(
                kafka_List_get(values, 0) as *const kafka_common_ElectionType_t,
                kafka_common_ElectionType_preferred()
            );
            assert_eq!(
                kafka_List_get(values, 1) as *const kafka_common_ElectionType_t,
                kafka_common_ElectionType_unclean()
            );
            kafka_List_destroy(values);
        }
    }
}
