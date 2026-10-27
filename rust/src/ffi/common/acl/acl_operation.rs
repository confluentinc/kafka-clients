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

//! `kafka_common_acl_AclOperation_t`: `org.apache.kafka.common.acl.AclOperation`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_acl_AclOperation_e`.

#![expect(non_camel_case_types)]

use std::ffi::{c_char, c_void};

use crate::common::acl::AclOperation;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t};

/// Opaque handle to a [`AclOperation`] singleton.
#[repr(C)]
pub struct kafka_common_acl_AclOperation_t {
    _private: [u8; 0],
}

/// The values of [`AclOperation`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_acl_AclOperation_e {
    /// `AclOperation::Unknown`: Java's `UNKNOWN`.
    kafka_common_acl_AclOperation_UNKNOWN,
    /// `AclOperation::Any`: Java's `ANY`.
    kafka_common_acl_AclOperation_ANY,
    /// `AclOperation::All`: Java's `ALL`.
    kafka_common_acl_AclOperation_ALL,
    /// `AclOperation::Read`: Java's `READ`.
    kafka_common_acl_AclOperation_READ,
    /// `AclOperation::Write`: Java's `WRITE`.
    kafka_common_acl_AclOperation_WRITE,
    /// `AclOperation::Create`: Java's `CREATE`.
    kafka_common_acl_AclOperation_CREATE,
    /// `AclOperation::Delete`: Java's `DELETE`.
    kafka_common_acl_AclOperation_DELETE,
    /// `AclOperation::Alter`: Java's `ALTER`.
    kafka_common_acl_AclOperation_ALTER,
    /// `AclOperation::Describe`: Java's `DESCRIBE`.
    kafka_common_acl_AclOperation_DESCRIBE,
    /// `AclOperation::ClusterAction`: Java's `CLUSTER_ACTION`.
    kafka_common_acl_AclOperation_CLUSTER_ACTION,
    /// `AclOperation::DescribeConfigs`: Java's `DESCRIBE_CONFIGS`.
    kafka_common_acl_AclOperation_DESCRIBE_CONFIGS,
    /// `AclOperation::AlterConfigs`: Java's `ALTER_CONFIGS`.
    kafka_common_acl_AclOperation_ALTER_CONFIGS,
    /// `AclOperation::IdempotentWrite`: Java's `IDEMPOTENT_WRITE`.
    kafka_common_acl_AclOperation_IDEMPOTENT_WRITE,
    /// `AclOperation::CreateTokens`: Java's `CREATE_TOKENS`.
    kafka_common_acl_AclOperation_CREATE_TOKENS,
    /// `AclOperation::DescribeTokens`: Java's `DESCRIBE_TOKENS`.
    kafka_common_acl_AclOperation_DESCRIBE_TOKENS,
    /// `AclOperation::TwoPhaseCommit`: Java's `TWO_PHASE_COMMIT`.
    kafka_common_acl_AclOperation_TWO_PHASE_COMMIT,
}

/// One static instance per value, indexed by [`kafka_common_acl_AclOperation_e`].
static VARIANTS: [AclOperation; 16] = [
    AclOperation::Unknown,
    AclOperation::Any,
    AclOperation::All,
    AclOperation::Read,
    AclOperation::Write,
    AclOperation::Create,
    AclOperation::Delete,
    AclOperation::Alter,
    AclOperation::Describe,
    AclOperation::ClusterAction,
    AclOperation::DescribeConfigs,
    AclOperation::AlterConfigs,
    AclOperation::IdempotentWrite,
    AclOperation::CreateTokens,
    AclOperation::DescribeTokens,
    AclOperation::TwoPhaseCommit,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(value: AclOperation) -> kafka_common_acl_AclOperation_e {
    match value {
        AclOperation::Unknown => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_UNKNOWN,
        AclOperation::Any => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_ANY,
        AclOperation::All => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_ALL,
        AclOperation::Read => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_READ,
        AclOperation::Write => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_WRITE,
        AclOperation::Create => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_CREATE,
        AclOperation::Delete => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_DELETE,
        AclOperation::Alter => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_ALTER,
        AclOperation::Describe => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_DESCRIBE,
        AclOperation::ClusterAction => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_CLUSTER_ACTION,
        AclOperation::DescribeConfigs => {
            kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_DESCRIBE_CONFIGS
        },
        AclOperation::AlterConfigs => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_ALTER_CONFIGS,
        AclOperation::IdempotentWrite => {
            kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_IDEMPOTENT_WRITE
        },
        AclOperation::CreateTokens => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_CREATE_TOKENS,
        AclOperation::DescribeTokens => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_DESCRIBE_TOKENS,
        AclOperation::TwoPhaseCommit => kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_TWO_PHASE_COMMIT,
    }
}

/// The borrowed singleton standing for `value`.
pub(crate) fn singleton(value: AclOperation) -> *const kafka_common_acl_AclOperation_t {
    &VARIANTS[enum_of(value) as usize] as *const AclOperation as *const kafka_common_acl_AclOperation_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `value` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(value: *const kafka_common_acl_AclOperation_t) -> AclOperation {
    // SAFETY: Per this function's `# Safety`, `value` is a singleton returned by this module, i.e.
    // a pointer to an element of the `static` `VARIANTS` array (`singleton`), so it points at a
    // live, initialised `AclOperation`, which is `Copy` and read out here.
    unsafe { *(value as *const AclOperation) }
}

/// `AclOperation.UNKNOWN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_unknown() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Unknown)
}

/// `AclOperation.ANY`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_any() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Any)
}

/// `AclOperation.ALL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_all() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::All)
}

/// `AclOperation.READ`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_read() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Read)
}

/// `AclOperation.WRITE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_write() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Write)
}

/// `AclOperation.CREATE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_create() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Create)
}

/// `AclOperation.DELETE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_delete() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Delete)
}

/// `AclOperation.ALTER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_alter() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Alter)
}

/// `AclOperation.DESCRIBE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_describe() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Describe)
}

/// `AclOperation.CLUSTER_ACTION`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_cluster_action() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::ClusterAction)
}

/// `AclOperation.DESCRIBE_CONFIGS`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_describe_configs() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::DescribeConfigs)
}

/// `AclOperation.ALTER_CONFIGS`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_alter_configs() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::AlterConfigs)
}

/// `AclOperation.IDEMPOTENT_WRITE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_idempotent_write() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::IdempotentWrite)
}

/// `AclOperation.CREATE_TOKENS`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_create_tokens() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::CreateTokens)
}

/// `AclOperation.DESCRIBE_TOKENS`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_describe_tokens() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::DescribeTokens)
}

/// `AclOperation.TWO_PHASE_COMMIT`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_two_phase_commit() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::TwoPhaseCommit)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_common_acl_AclOperation_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_common_acl_AclOperation_e::kafka_common_acl_AclOperation_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation__enum(
    self_: *const kafka_common_acl_AclOperation_t,
) -> kafka_common_acl_AclOperation_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    enum_of(unsafe { value_of(self_) })
}

/// `code()`: the wire-protocol byte.
///
/// A caught Rust panic returns -1, which is no code of this Java `byte`
/// enum: its 0 is the valid `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_code(self_: *const kafka_common_acl_AclOperation_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    unsafe { value_of(self_) }.code()
}

/// `AclOperation.values()`: an owned list of the borrowed singletons in
/// declaration order, freed with `kafka_List_destroy` (the elements are
/// never freed).
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_values() -> *mut kafka_List_t {
    let elements = AclOperation::values()
        .iter()
        .map(|&value| singleton(value) as *mut c_void)
        .collect();
    box_list(elements, None)
}

/// `fromCode(byte code)`: the singleton for a wire-protocol byte, `UNKNOWN`
/// for one this client does not know.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_from_code(code: i8) -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::from_code(code))
}

/// `fromString(String)`: the singleton named by `str` (Java's constant name),
/// `UNKNOWN` for any other string.
///
/// # Safety
///
/// `str` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_from_string(
    str: *const c_char,
) -> *const kafka_common_acl_AclOperation_t {
    // SAFETY: Per this function's `# Safety`, `str` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    singleton(AclOperation::from_string(&unsafe { c_str_to_string(str) }))
}

/// `isUnknown()`: whether this is the `UNKNOWN` value.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_is_unknown(self_: *const kafka_common_acl_AclOperation_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    i8::from(unsafe { value_of(self_) }.is_unknown())
}

/// `toString()`: Java's constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_to_string(
    self_: *const kafka_common_acl_AclOperation_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_string_destroy};

    #[test]
    fn values_lists_the_singletons_in_declaration_order() {
        // SAFETY: Test code: the owned list from `values` is destroyed once, after its elements,
        // borrowed singletons, are read.
        unsafe {
            let values = kafka_common_acl_AclOperation_values();
            assert_eq!(kafka_List_size(values), 16);
            for (index, &value) in AclOperation::values().iter().enumerate() {
                assert_eq!(
                    kafka_List_get(values, index as i32) as *const kafka_common_acl_AclOperation_t,
                    singleton(value)
                );
            }
            kafka_List_destroy(values);
        }
    }

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &value) in VARIANTS.iter().enumerate() {
            let handle = singleton(value);
            // SAFETY: Test code: every handle passed is a singleton from `singleton`, which points
            // into the static `VARIANTS` array and is never freed.
            unsafe {
                assert_eq!(value_of(handle), value);
                assert_eq!(kafka_common_acl_AclOperation__enum(handle) as usize, index);
            }
        }
        assert_eq!(kafka_common_acl_AclOperation_unknown(), singleton(AclOperation::Unknown));
        assert_eq!(
            kafka_common_acl_AclOperation_two_phase_commit(),
            singleton(AclOperation::TwoPhaseCommit)
        );
    }

    #[test]
    fn methods_follow_the_rust_enum() {
        // SAFETY: Test code: every handle passed is a singleton from `singleton` or this module's
        // getters, which point into the static `VARIANTS` array and are never freed; each string
        // from `to_string` is destroyed once, after `from_string` reads it; `bogus` is a `CString`
        // that outlives the call reading it.
        unsafe {
            for &value in &VARIANTS {
                let handle = singleton(value);
                assert_eq!(kafka_common_acl_AclOperation_code(handle), value.code());
                assert_eq!(kafka_common_acl_AclOperation_from_code(value.code()), handle);
            }
            assert_eq!(
                kafka_common_acl_AclOperation_from_code(-1),
                kafka_common_acl_AclOperation_unknown()
            );
            assert_eq!(
                kafka_common_acl_AclOperation_is_unknown(kafka_common_acl_AclOperation_unknown()),
                1
            );
            assert_eq!(kafka_common_acl_AclOperation_is_unknown(kafka_common_acl_AclOperation_any()), 0);
            for &value in &VARIANTS {
                let s = kafka_common_acl_AclOperation_to_string(singleton(value));
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), value.to_string());
                assert_eq!(
                    kafka_common_acl_AclOperation_from_string(s),
                    singleton(value),
                    "toString round-trips through fromString"
                );
                kafka_string_destroy(s);
            }
            let bogus = std::ffi::CString::new("bogus").unwrap();
            assert_eq!(
                kafka_common_acl_AclOperation_from_string(bogus.as_ptr()),
                kafka_common_acl_AclOperation_unknown()
            );
        }
    }
}
