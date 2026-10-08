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

//! `kafka_admin_TransactionState_t`:
//! `org.apache.kafka.clients.admin.TransactionState` (CLAUDE.md §4,
//! "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_admin_TransactionState_e`.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::admin::TransactionState;
use crate::ffi::util::{c_str_to_string, into_c_string};

/// Opaque handle to a [`TransactionState`] singleton.
#[repr(C)]
pub struct kafka_admin_TransactionState_t {
    _private: [u8; 0],
}

/// The values of [`TransactionState`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_TransactionState_e {
    ongoing,
    prepare_abort,
    prepare_commit,
    complete_abort,
    complete_commit,
    empty,
    prepare_epoch_fence,
    unknown,
}

/// One static instance per value, indexed by
/// [`kafka_admin_TransactionState_e`].
static VARIANTS: [TransactionState; 8] = [
    TransactionState::Ongoing,
    TransactionState::PrepareAbort,
    TransactionState::PrepareCommit,
    TransactionState::CompleteAbort,
    TransactionState::CompleteCommit,
    TransactionState::Empty,
    TransactionState::PrepareEpochFence,
    TransactionState::Unknown,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(state: TransactionState) -> kafka_admin_TransactionState_e {
    match state {
        TransactionState::Ongoing => kafka_admin_TransactionState_e::ongoing,
        TransactionState::PrepareAbort => kafka_admin_TransactionState_e::prepare_abort,
        TransactionState::PrepareCommit => kafka_admin_TransactionState_e::prepare_commit,
        TransactionState::CompleteAbort => kafka_admin_TransactionState_e::complete_abort,
        TransactionState::CompleteCommit => kafka_admin_TransactionState_e::complete_commit,
        TransactionState::Empty => kafka_admin_TransactionState_e::empty,
        TransactionState::PrepareEpochFence => kafka_admin_TransactionState_e::prepare_epoch_fence,
        TransactionState::Unknown => kafka_admin_TransactionState_e::unknown,
    }
}

/// The borrowed singleton standing for `state`.
pub(crate) fn transaction_state_singleton(state: TransactionState) -> *const kafka_admin_TransactionState_t {
    &VARIANTS[enum_of(state) as usize] as *const TransactionState as *const kafka_admin_TransactionState_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `state` must be a singleton returned by this module.
pub(crate) unsafe fn transaction_state_value_of(state: *const kafka_admin_TransactionState_t) -> TransactionState {
    unsafe { *(state as *const TransactionState) }
}

/// `TransactionState.ONGOING`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_ongoing() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::Ongoing)
}

/// `TransactionState.PREPARE_ABORT`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_prepare_abort() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::PrepareAbort)
}

/// `TransactionState.PREPARE_COMMIT`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_prepare_commit() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::PrepareCommit)
}

/// `TransactionState.COMPLETE_ABORT`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_complete_abort() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::CompleteAbort)
}

/// `TransactionState.COMPLETE_COMMIT`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_complete_commit() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::CompleteCommit)
}

/// `TransactionState.EMPTY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_empty() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::Empty)
}

/// `TransactionState.PREPARE_EPOCH_FENCE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_prepare_epoch_fence() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::PrepareEpochFence)
}

/// `TransactionState.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_TransactionState_unknown() -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::Unknown)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionState__enum(
    self_: *const kafka_admin_TransactionState_t,
) -> kafka_admin_TransactionState_e {
    enum_of(unsafe { transaction_state_value_of(self_) })
}

/// `TransactionState.parse(String name)`: the borrowed singleton named
/// `name`, `UNKNOWN` for a name Java does not know.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionState_parse(
    name: *const c_char,
) -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(TransactionState::parse(&unsafe { c_str_to_string(name) }))
}

/// `toString()`: the Java name of the state (`"CompleteCommit"`, ...), as an
/// owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionState_to_string(
    self_: *const kafka_admin_TransactionState_t,
) -> *mut c_char {
    into_c_string(&unsafe { transaction_state_value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &state) in VARIANTS.iter().enumerate() {
            let handle = transaction_state_singleton(state);
            unsafe {
                assert_eq!(transaction_state_value_of(handle), state);
                assert_eq!(kafka_admin_TransactionState__enum(handle) as usize, index);
                let name = kafka_admin_TransactionState_to_string(handle);
                assert_eq!(kafka_admin_TransactionState_parse(name), handle);
                assert_eq!(CStr::from_ptr(name).to_str().unwrap(), state.to_string());
                kafka_string_destroy(name);
            }
        }
        assert_eq!(
            kafka_admin_TransactionState_ongoing(),
            transaction_state_singleton(TransactionState::Ongoing)
        );
        assert_eq!(
            kafka_admin_TransactionState_prepare_epoch_fence(),
            transaction_state_singleton(TransactionState::PrepareEpochFence)
        );
        assert_eq!(
            unsafe { kafka_admin_TransactionState_parse(c"NotAState".as_ptr()) },
            kafka_admin_TransactionState_unknown()
        );
    }
}
