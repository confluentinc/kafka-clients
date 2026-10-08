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

//! `kafka_common_IsolationLevel_t`: `org.apache.kafka.common.IsolationLevel`
//! (CLAUDE.md §4, "Enums").
//!
//! A unit enum crosses as borrowed per-value singletons: each value's
//! function returns a static instance, never freed and comparable with `==`,
//! `kafka_common_IsolationLevel_e` is the C enum for a `switch`, and
//! `__enum` maps a singleton to it.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::common::IsolationLevel;
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::into_c_string;

/// Opaque handle to an [`IsolationLevel`] singleton.
#[repr(C)]
pub struct kafka_common_IsolationLevel_t {
    _private: [u8; 0],
}

/// The values of [`IsolationLevel`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_IsolationLevel_e {
    read_uncommitted,
    read_committed,
}

/// One static instance per value, indexed by [`kafka_common_IsolationLevel_e`].
static VARIANTS: [IsolationLevel; 2] = [IsolationLevel::ReadUncommitted, IsolationLevel::ReadCommitted];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(level: IsolationLevel) -> kafka_common_IsolationLevel_e {
    match level {
        IsolationLevel::ReadUncommitted => kafka_common_IsolationLevel_e::read_uncommitted,
        IsolationLevel::ReadCommitted => kafka_common_IsolationLevel_e::read_committed,
    }
}

/// The borrowed singleton standing for `level`.
pub(crate) fn singleton(level: IsolationLevel) -> *const kafka_common_IsolationLevel_t {
    &VARIANTS[enum_of(level) as usize] as *const IsolationLevel as *const kafka_common_IsolationLevel_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `level` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(level: *const kafka_common_IsolationLevel_t) -> IsolationLevel {
    unsafe { *(level as *const IsolationLevel) }
}

/// `IsolationLevel.READ_UNCOMMITTED`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_IsolationLevel_read_uncommitted() -> *const kafka_common_IsolationLevel_t {
    singleton(IsolationLevel::ReadUncommitted)
}

/// `IsolationLevel.READ_COMMITTED`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_IsolationLevel_read_committed() -> *const kafka_common_IsolationLevel_t {
    singleton(IsolationLevel::ReadCommitted)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_IsolationLevel__enum(
    self_: *const kafka_common_IsolationLevel_t,
) -> kafka_common_IsolationLevel_e {
    enum_of(unsafe { value_of(self_) })
}

/// `id()`: the wire-protocol byte.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_IsolationLevel_id(self_: *const kafka_common_IsolationLevel_t) -> i8 {
    unsafe { value_of(self_) }.id() as i8
}

/// `IsolationLevel.forId(byte id)`: delivers the singleton through
/// `out_for_id`, or returns the owned `IllegalArgumentException` translation
/// for an unknown id.
///
/// # Safety
///
/// `out_for_id` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_IsolationLevel_for_id(
    id: i8,
    out_for_id: *mut *const kafka_common_IsolationLevel_t,
) -> *mut kafka_common_Error_t {
    match IsolationLevel::for_id(id as u8) {
        Ok(level) => {
            unsafe { *out_for_id = singleton(level) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_IsolationLevel_to_string(
    self_: *const kafka_common_IsolationLevel_t,
) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_argument_error;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &level) in VARIANTS.iter().enumerate() {
            let handle = singleton(level);
            unsafe {
                assert_eq!(value_of(handle), level);
                assert_eq!(kafka_common_IsolationLevel__enum(handle) as usize, index);
            }
        }
        assert_eq!(
            kafka_common_IsolationLevel_read_uncommitted(),
            singleton(IsolationLevel::ReadUncommitted)
        );
        assert_eq!(
            kafka_common_IsolationLevel_read_committed(),
            singleton(IsolationLevel::ReadCommitted)
        );
    }

    #[test]
    fn id_for_id_and_to_string_follow_java() {
        unsafe {
            assert_eq!(kafka_common_IsolationLevel_id(kafka_common_IsolationLevel_read_committed()), 1);
            let mut level = ptr::null();
            assert!(kafka_common_IsolationLevel_for_id(0, &mut level).is_null());
            assert_eq!(level, kafka_common_IsolationLevel_read_uncommitted());
            let error = kafka_common_IsolationLevel_for_id(7, &mut level);
            assert!(!error.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
            let s = kafka_common_IsolationLevel_to_string(kafka_common_IsolationLevel_read_committed());
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), IsolationLevel::ReadCommitted.to_string());
            kafka_string_destroy(s);
        }
    }
}
