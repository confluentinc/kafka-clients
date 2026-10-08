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

//! `kafka_common_ClassicGroupState_t`:
//! `org.apache.kafka.common.ClassicGroupState` (CLAUDE.md §4, "Enums"):
//! borrowed per-value singletons plus the C enum
//! `kafka_common_ClassicGroupState_e`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char};
use std::sync::LazyLock;

use crate::common::ClassicGroupState;
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`ClassicGroupState`] singleton.
#[repr(C)]
pub struct kafka_common_ClassicGroupState_t {
    _private: [u8; 0],
}

/// The values of [`ClassicGroupState`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_ClassicGroupState_e {
    unknown,
    preparing_rebalance,
    completing_rebalance,
    stable,
    dead,
    empty,
}

/// One static instance per value, indexed by
/// [`kafka_common_ClassicGroupState_e`].
static VARIANTS: [ClassicGroupState; 6] = [
    ClassicGroupState::Unknown,
    ClassicGroupState::PreparingRebalance,
    ClassicGroupState::CompletingRebalance,
    ClassicGroupState::Stable,
    ClassicGroupState::Dead,
    ClassicGroupState::Empty,
];

/// `name()` of each value, NUL-terminated, indexed like [`VARIANTS`].
static NAMES: LazyLock<Vec<CString>> = LazyLock::new(|| VARIANTS.iter().map(|s| owned_c_string(s.name())).collect());

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(state: ClassicGroupState) -> kafka_common_ClassicGroupState_e {
    match state {
        ClassicGroupState::Unknown => kafka_common_ClassicGroupState_e::unknown,
        ClassicGroupState::PreparingRebalance => kafka_common_ClassicGroupState_e::preparing_rebalance,
        ClassicGroupState::CompletingRebalance => kafka_common_ClassicGroupState_e::completing_rebalance,
        ClassicGroupState::Stable => kafka_common_ClassicGroupState_e::stable,
        ClassicGroupState::Dead => kafka_common_ClassicGroupState_e::dead,
        ClassicGroupState::Empty => kafka_common_ClassicGroupState_e::empty,
    }
}

/// The borrowed singleton standing for `state`.
pub(crate) fn singleton(state: ClassicGroupState) -> *const kafka_common_ClassicGroupState_t {
    &VARIANTS[enum_of(state) as usize] as *const ClassicGroupState as *const kafka_common_ClassicGroupState_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `state` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(state: *const kafka_common_ClassicGroupState_t) -> ClassicGroupState {
    unsafe { *(state as *const ClassicGroupState) }
}

/// `ClassicGroupState.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ClassicGroupState_unknown() -> *const kafka_common_ClassicGroupState_t {
    singleton(ClassicGroupState::Unknown)
}

/// `ClassicGroupState.PREPARING_REBALANCE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ClassicGroupState_preparing_rebalance() -> *const kafka_common_ClassicGroupState_t {
    singleton(ClassicGroupState::PreparingRebalance)
}

/// `ClassicGroupState.COMPLETING_REBALANCE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ClassicGroupState_completing_rebalance() -> *const kafka_common_ClassicGroupState_t {
    singleton(ClassicGroupState::CompletingRebalance)
}

/// `ClassicGroupState.STABLE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ClassicGroupState_stable() -> *const kafka_common_ClassicGroupState_t {
    singleton(ClassicGroupState::Stable)
}

/// `ClassicGroupState.DEAD`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ClassicGroupState_dead() -> *const kafka_common_ClassicGroupState_t {
    singleton(ClassicGroupState::Dead)
}

/// `ClassicGroupState.EMPTY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ClassicGroupState_empty() -> *const kafka_common_ClassicGroupState_t {
    singleton(ClassicGroupState::Empty)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClassicGroupState__enum(
    self_: *const kafka_common_ClassicGroupState_t,
) -> kafka_common_ClassicGroupState_e {
    enum_of(unsafe { value_of(self_) })
}

/// `name()`: a static string, never freed.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClassicGroupState_name(
    self_: *const kafka_common_ClassicGroupState_t,
) -> *const c_char {
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// `ClassicGroupState.parse(String name)`: case-insensitive, `UNKNOWN` for an
/// unrecognised name, as in Java.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClassicGroupState_parse(
    name: *const c_char,
) -> *const kafka_common_ClassicGroupState_t {
    singleton(ClassicGroupState::parse(&unsafe { c_str_to_string(name) }))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClassicGroupState_to_string(
    self_: *const kafka_common_ClassicGroupState_t,
) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &state) in VARIANTS.iter().enumerate() {
            let handle = singleton(state);
            unsafe {
                assert_eq!(value_of(handle), state);
                assert_eq!(kafka_common_ClassicGroupState__enum(handle) as usize, index);
                assert_eq!(
                    CStr::from_ptr(kafka_common_ClassicGroupState_name(handle)).to_str().unwrap(),
                    state.name()
                );
            }
        }
        assert_eq!(kafka_common_ClassicGroupState_unknown(), singleton(ClassicGroupState::Unknown));
        assert_eq!(
            kafka_common_ClassicGroupState_preparing_rebalance(),
            singleton(ClassicGroupState::PreparingRebalance)
        );
        assert_eq!(
            kafka_common_ClassicGroupState_completing_rebalance(),
            singleton(ClassicGroupState::CompletingRebalance)
        );
        assert_eq!(kafka_common_ClassicGroupState_stable(), singleton(ClassicGroupState::Stable));
        assert_eq!(kafka_common_ClassicGroupState_dead(), singleton(ClassicGroupState::Dead));
        assert_eq!(kafka_common_ClassicGroupState_empty(), singleton(ClassicGroupState::Empty));
    }

    #[test]
    fn parse_and_to_string_follow_java() {
        let stable = CString::new("stable").unwrap();
        let bogus = CString::new("bogus").unwrap();
        unsafe {
            assert_eq!(
                kafka_common_ClassicGroupState_parse(stable.as_ptr()),
                kafka_common_ClassicGroupState_stable()
            );
            assert_eq!(
                kafka_common_ClassicGroupState_parse(bogus.as_ptr()),
                kafka_common_ClassicGroupState_unknown()
            );
            let s = kafka_common_ClassicGroupState_to_string(kafka_common_ClassicGroupState_dead());
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), ClassicGroupState::Dead.to_string());
            kafka_string_destroy(s);
        }
    }
}
