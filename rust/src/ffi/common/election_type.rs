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

use std::ffi::c_void;

use crate::common::ElectionType;
use crate::ffi::common::{box_error, kafka_common_Error_t};
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
    unsafe { *(election_type as *const ElectionType) }
}

/// `ElectionType.PREFERRED`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ElectionType_preferred() -> *const kafka_common_ElectionType_t {
    singleton(ElectionType::Preferred)
}

/// `ElectionType.UNCLEAN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ElectionType_unclean() -> *const kafka_common_ElectionType_t {
    singleton(ElectionType::Unclean)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ElectionType__enum(
    self_: *const kafka_common_ElectionType_t,
) -> kafka_common_ElectionType_e {
    enum_of(unsafe { value_of(self_) })
}

/// `value`: the wire-protocol byte.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ElectionType_value(self_: *const kafka_common_ElectionType_t) -> i8 {
    unsafe { value_of(self_) }.value()
}

/// `ElectionType.valueOf(byte value)`: delivers the singleton through
/// `out_value_of`, or returns the owned `IllegalArgumentException`
/// translation for an unknown value.
///
/// # Safety
///
/// `out_value_of` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ElectionType_value_of(
    value: i8,
    out_value_of: *mut *const kafka_common_ElectionType_t,
) -> *mut kafka_common_Error_t {
    match ElectionType::value_of(value) {
        Ok(election_type) => {
            unsafe { *out_value_of = singleton(election_type) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `ElectionType.values()`: an owned list of the borrowed singletons in
/// declaration order, freed with `kafka_List_destroy` (the elements are
/// never freed).
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
        unsafe {
            assert_eq!(kafka_common_ElectionType_value(kafka_common_ElectionType_unclean()), 1);
            let mut election_type = ptr::null();
            assert!(kafka_common_ElectionType_value_of(0, &mut election_type).is_null());
            assert_eq!(election_type, kafka_common_ElectionType_preferred());
            let error = kafka_common_ElectionType_value_of(9, &mut election_type);
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
