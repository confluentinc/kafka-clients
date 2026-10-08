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

//! `kafka_common_GroupState_t`: `org.apache.kafka.common.GroupState`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_GroupState_e`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char, c_void};
use std::sync::LazyLock;

use crate::common::{GroupState, GroupType};
use crate::ffi::common::group_type::{kafka_common_GroupType_t, value_of as group_type_of};
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t, owned_c_string};

/// Opaque handle to a [`GroupState`] singleton.
#[repr(C)]
pub struct kafka_common_GroupState_t {
    _private: [u8; 0],
}

/// The values of [`GroupState`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_GroupState_e {
    unknown,
    preparing_rebalance,
    completing_rebalance,
    stable,
    dead,
    empty,
    assigning,
    reconciling,
    not_ready,
}

/// One static instance per value, indexed by [`kafka_common_GroupState_e`].
static VARIANTS: [GroupState; 9] = [
    GroupState::Unknown,
    GroupState::PreparingRebalance,
    GroupState::CompletingRebalance,
    GroupState::Stable,
    GroupState::Dead,
    GroupState::Empty,
    GroupState::Assigning,
    GroupState::Reconciling,
    GroupState::NotReady,
];

/// `name()` of each value, NUL-terminated, indexed like [`VARIANTS`].
static NAMES: LazyLock<Vec<CString>> = LazyLock::new(|| VARIANTS.iter().map(|s| owned_c_string(s.name())).collect());

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(state: GroupState) -> kafka_common_GroupState_e {
    match state {
        GroupState::Unknown => kafka_common_GroupState_e::unknown,
        GroupState::PreparingRebalance => kafka_common_GroupState_e::preparing_rebalance,
        GroupState::CompletingRebalance => kafka_common_GroupState_e::completing_rebalance,
        GroupState::Stable => kafka_common_GroupState_e::stable,
        GroupState::Dead => kafka_common_GroupState_e::dead,
        GroupState::Empty => kafka_common_GroupState_e::empty,
        GroupState::Assigning => kafka_common_GroupState_e::assigning,
        GroupState::Reconciling => kafka_common_GroupState_e::reconciling,
        GroupState::NotReady => kafka_common_GroupState_e::not_ready,
    }
}

/// The borrowed singleton standing for `state`.
pub(crate) fn singleton(state: GroupState) -> *const kafka_common_GroupState_t {
    &VARIANTS[enum_of(state) as usize] as *const GroupState as *const kafka_common_GroupState_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `state` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(state: *const kafka_common_GroupState_t) -> GroupState {
    unsafe { *(state as *const GroupState) }
}

/// `GroupState.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_unknown() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Unknown)
}

/// `GroupState.PREPARING_REBALANCE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_preparing_rebalance() -> *const kafka_common_GroupState_t {
    singleton(GroupState::PreparingRebalance)
}

/// `GroupState.COMPLETING_REBALANCE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_completing_rebalance() -> *const kafka_common_GroupState_t {
    singleton(GroupState::CompletingRebalance)
}

/// `GroupState.STABLE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_stable() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Stable)
}

/// `GroupState.DEAD`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_dead() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Dead)
}

/// `GroupState.EMPTY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_empty() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Empty)
}

/// `GroupState.ASSIGNING`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_assigning() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Assigning)
}

/// `GroupState.RECONCILING`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_reconciling() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Reconciling)
}

/// `GroupState.NOT_READY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_not_ready() -> *const kafka_common_GroupState_t {
    singleton(GroupState::NotReady)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState__enum(
    self_: *const kafka_common_GroupState_t,
) -> kafka_common_GroupState_e {
    enum_of(unsafe { value_of(self_) })
}

/// `name()`: a static string, never freed.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_name(self_: *const kafka_common_GroupState_t) -> *const c_char {
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// `GroupState.parse(String name)`: case-insensitive, `UNKNOWN` for an
/// unrecognised name, as in Java.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_parse(name: *const c_char) -> *const kafka_common_GroupState_t {
    singleton(GroupState::parse(&unsafe { c_str_to_string(name) }))
}

/// `GroupState.groupStatesForType(GroupType type)`: an owned list of the
/// borrowed singletons in declaration order, freed with `kafka_List_destroy`
/// (the elements are never freed).
///
/// Java throws `IllegalArgumentException("Group type not known")` for
/// `GroupType.UNKNOWN`; the Rust translation panics there, and a panic must
/// not cross the C boundary, so this returns null for that one input.
///
/// # Safety
///
/// `group_type` must be a `kafka_common_GroupType_t` singleton.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_group_states_for_type(
    group_type: *const kafka_common_GroupType_t,
) -> *mut kafka_List_t {
    let group_type = unsafe { group_type_of(group_type) };
    if group_type == GroupType::Unknown {
        return std::ptr::null_mut();
    }
    let states = GroupState::group_states_for_type(group_type);
    // A `HashSet` has no order; emit the values in declaration order so the
    // list is deterministic.
    let elements = VARIANTS
        .iter()
        .filter(|state| states.contains(state))
        .map(|&state| singleton(state) as *mut c_void)
        .collect();
    box_list(elements, None)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_to_string(self_: *const kafka_common_GroupState_t) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::common::group_type::{kafka_common_GroupType_share, kafka_common_GroupType_unknown};
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_string_destroy};

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &state) in VARIANTS.iter().enumerate() {
            let handle = singleton(state);
            unsafe {
                assert_eq!(value_of(handle), state);
                assert_eq!(kafka_common_GroupState__enum(handle) as usize, index);
                assert_eq!(
                    CStr::from_ptr(kafka_common_GroupState_name(handle)).to_str().unwrap(),
                    state.name()
                );
            }
        }
        assert_eq!(kafka_common_GroupState_unknown(), singleton(GroupState::Unknown));
        assert_eq!(
            kafka_common_GroupState_preparing_rebalance(),
            singleton(GroupState::PreparingRebalance)
        );
        assert_eq!(
            kafka_common_GroupState_completing_rebalance(),
            singleton(GroupState::CompletingRebalance)
        );
        assert_eq!(kafka_common_GroupState_stable(), singleton(GroupState::Stable));
        assert_eq!(kafka_common_GroupState_dead(), singleton(GroupState::Dead));
        assert_eq!(kafka_common_GroupState_empty(), singleton(GroupState::Empty));
        assert_eq!(kafka_common_GroupState_assigning(), singleton(GroupState::Assigning));
        assert_eq!(kafka_common_GroupState_reconciling(), singleton(GroupState::Reconciling));
        assert_eq!(kafka_common_GroupState_not_ready(), singleton(GroupState::NotReady));
    }

    #[test]
    fn parse_group_states_for_type_and_to_string_follow_java() {
        let stable = CString::new("Stable").unwrap();
        let bogus = CString::new("bogus").unwrap();
        unsafe {
            assert_eq!(kafka_common_GroupState_parse(stable.as_ptr()), kafka_common_GroupState_stable());
            assert_eq!(kafka_common_GroupState_parse(bogus.as_ptr()), kafka_common_GroupState_unknown());

            let share = kafka_common_GroupState_group_states_for_type(kafka_common_GroupType_share());
            assert_eq!(kafka_List_size(share), 3);
            assert_eq!(
                kafka_List_get(share, 0) as *const kafka_common_GroupState_t,
                kafka_common_GroupState_stable()
            );
            assert_eq!(
                kafka_List_get(share, 1) as *const kafka_common_GroupState_t,
                kafka_common_GroupState_dead()
            );
            assert_eq!(
                kafka_List_get(share, 2) as *const kafka_common_GroupState_t,
                kafka_common_GroupState_empty()
            );
            kafka_List_destroy(share);
            assert!(kafka_common_GroupState_group_states_for_type(kafka_common_GroupType_unknown()).is_null());

            let s = kafka_common_GroupState_to_string(kafka_common_GroupState_not_ready());
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), GroupState::NotReady.to_string());
            kafka_string_destroy(s);
        }
    }
}
