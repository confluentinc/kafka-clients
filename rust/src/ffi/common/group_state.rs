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
use crate::ffi::ffi_guard;
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
    kafka_common_GroupState_UNKNOWN,
    kafka_common_GroupState_PREPARING_REBALANCE,
    kafka_common_GroupState_COMPLETING_REBALANCE,
    kafka_common_GroupState_STABLE,
    kafka_common_GroupState_DEAD,
    kafka_common_GroupState_EMPTY,
    kafka_common_GroupState_ASSIGNING,
    kafka_common_GroupState_RECONCILING,
    kafka_common_GroupState_NOT_READY,
}

/// One static instance per value, indexed by `kafka_common_GroupState_e`.
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
        GroupState::Unknown => kafka_common_GroupState_e::kafka_common_GroupState_UNKNOWN,
        GroupState::PreparingRebalance => kafka_common_GroupState_e::kafka_common_GroupState_PREPARING_REBALANCE,
        GroupState::CompletingRebalance => kafka_common_GroupState_e::kafka_common_GroupState_COMPLETING_REBALANCE,
        GroupState::Stable => kafka_common_GroupState_e::kafka_common_GroupState_STABLE,
        GroupState::Dead => kafka_common_GroupState_e::kafka_common_GroupState_DEAD,
        GroupState::Empty => kafka_common_GroupState_e::kafka_common_GroupState_EMPTY,
        GroupState::Assigning => kafka_common_GroupState_e::kafka_common_GroupState_ASSIGNING,
        GroupState::Reconciling => kafka_common_GroupState_e::kafka_common_GroupState_RECONCILING,
        GroupState::NotReady => kafka_common_GroupState_e::kafka_common_GroupState_NOT_READY,
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
    // SAFETY: Per this function's `# Safety`, `state` is a singleton returned by this module, i.e.
    // a pointer to an element of the `static` `VARIANTS` array (`singleton`), so it points at a
    // live, initialised `GroupState`, which is `Copy` and read out here.
    unsafe { *(state as *const GroupState) }
}

/// Returns `GroupState.UNKNOWN`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_unknown() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Unknown)
}

/// Returns `GroupState.PREPARING_REBALANCE`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_preparing_rebalance() -> *const kafka_common_GroupState_t {
    singleton(GroupState::PreparingRebalance)
}

/// Returns `GroupState.COMPLETING_REBALANCE`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_completing_rebalance() -> *const kafka_common_GroupState_t {
    singleton(GroupState::CompletingRebalance)
}

/// Returns `GroupState.STABLE`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_stable() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Stable)
}

/// Returns `GroupState.DEAD`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_dead() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Dead)
}

/// Returns `GroupState.EMPTY`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_empty() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Empty)
}

/// Returns `GroupState.ASSIGNING`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_assigning() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Assigning)
}

/// Returns `GroupState.RECONCILING`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_reconciling() -> *const kafka_common_GroupState_t {
    singleton(GroupState::Reconciling)
}

/// Returns `GroupState.NOT_READY`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupState_not_ready() -> *const kafka_common_GroupState_t {
    singleton(GroupState::NotReady)
}

/// Returns the C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_common_GroupState_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_common_GroupState_e::kafka_common_GroupState_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState__enum(
    self_: *const kafka_common_GroupState_t,
) -> kafka_common_GroupState_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    enum_of(unsafe { value_of(self_) })
}

/// Returns the name of this enum constant, exactly as declared in Java (`PREPARING_REBALANCE`), Java's
/// `Enum.name()`. The text `kafka_common_GroupState_parse` reads (`PreparingRebalance`) is `kafka_common_GroupState_to_string`'s.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. The returned string is static, never
/// freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_name(self_: *const kafka_common_GroupState_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out, and the name returned is owned by the `static`
    // `NAMES`, which is never dropped, so it is never freed, as the rustdoc states.
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// Case-insensitive group state lookup by string name.
///
/// # Parameters
///
/// - `name` - the state's name
///
/// # Returns
///
/// The state, or `UNKNOWN` for an unrecognised name.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string. The returned singleton is static, never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_parse(name: *const c_char) -> *const kafka_common_GroupState_t {
    // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    singleton(GroupState::parse(&unsafe { c_str_to_string(name) }))
}

/// Returns the states a group of `group_type` can be in, in declaration order, or `NULL` for
/// `GroupType.UNKNOWN`, where Java throws `IllegalArgumentException("Group type not known")`.
///
/// # Parameters
///
/// - `group_type` - the group type
///
/// # Safety
///
/// `group_type` must be a `kafka_common_GroupType_t` singleton. The returned list is owned, freed
/// with `kafka_List_destroy`; its elements are static singletons, never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_group_states_for_type(
    group_type: *const kafka_common_GroupType_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `group_type` is a `kafka_common_GroupType_t`
    // singleton, as `group_type_of` (`group_type::value_of`) requires; the value is copied out.
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

/// Returns the state's name, the text the broker sends and `kafka_common_GroupState_parse` reads (`PreparingRebalance`).
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupState_to_string(self_: *const kafka_common_GroupState_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out and its text copied into an owned string.
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
            // SAFETY: Test code: every handle passed is a singleton from `singleton`, which points
            // into the static `VARIANTS` array and is never freed, and the names read are owned by
            // the static `NAMES`.
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
        // SAFETY: Test code: the `CString`s outlive the calls reading them, the singletons (of this
        // module and of `group_type`) are never freed, the owned list from `group_states_for_type`
        // is destroyed once, after its elements are read, and the string from `to_string` is
        // destroyed once, after it is read.
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

    /// New test, no Java original: `_name` is Java's `Enum.name()` and
    /// `_to_string` Java's `toString()`, the text `_parse` reads back; the
    /// constant name with an underscore does not parse.
    #[test]
    fn name_is_the_constant_and_to_string_the_java_text() {
        // SAFETY: Test code: the singletons are never freed, the names `_name` returns are owned
        // by the static `NAMES`, each string `_to_string` returns is destroyed once, after it is
        // read and parsed back, and the `CStr` literals outlive the calls reading them.
        unsafe {
            for (handle, name, text) in [
                (
                    kafka_common_GroupState_preparing_rebalance(),
                    "PREPARING_REBALANCE",
                    "PreparingRebalance",
                ),
                (kafka_common_GroupState_not_ready(), "NOT_READY", "NotReady"),
            ] {
                assert_eq!(CStr::from_ptr(kafka_common_GroupState_name(handle)).to_str().unwrap(), name);
                let s = kafka_common_GroupState_to_string(handle);
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), text);
                assert_eq!(kafka_common_GroupState_parse(s), handle);
                kafka_string_destroy(s);
            }
            assert_eq!(
                kafka_common_GroupState_parse(c"NOT_READY".as_ptr()),
                kafka_common_GroupState_unknown()
            );
        }
    }
}
