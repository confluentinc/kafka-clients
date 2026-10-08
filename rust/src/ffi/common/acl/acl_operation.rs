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

use std::ffi::c_char;

use crate::common::acl::AclOperation;
use crate::ffi::util::{c_str_to_string, into_c_string};

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
    unknown,
    /// `AclOperation::Any`: Java's `ANY`.
    any,
    /// `AclOperation::All`: Java's `ALL`.
    all,
    /// `AclOperation::Read`: Java's `READ`.
    read,
    /// `AclOperation::Write`: Java's `WRITE`.
    write,
    /// `AclOperation::Create`: Java's `CREATE`.
    create,
    /// `AclOperation::Delete`: Java's `DELETE`.
    delete,
    /// `AclOperation::Alter`: Java's `ALTER`.
    alter,
    /// `AclOperation::Describe`: Java's `DESCRIBE`.
    describe,
    /// `AclOperation::ClusterAction`: Java's `CLUSTER_ACTION`.
    cluster_action,
    /// `AclOperation::DescribeConfigs`: Java's `DESCRIBE_CONFIGS`.
    describe_configs,
    /// `AclOperation::AlterConfigs`: Java's `ALTER_CONFIGS`.
    alter_configs,
    /// `AclOperation::IdempotentWrite`: Java's `IDEMPOTENT_WRITE`.
    idempotent_write,
    /// `AclOperation::CreateTokens`: Java's `CREATE_TOKENS`.
    create_tokens,
    /// `AclOperation::DescribeTokens`: Java's `DESCRIBE_TOKENS`.
    describe_tokens,
    /// `AclOperation::TwoPhaseCommit`: Java's `TWO_PHASE_COMMIT`.
    two_phase_commit,
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
        AclOperation::Unknown => kafka_common_acl_AclOperation_e::unknown,
        AclOperation::Any => kafka_common_acl_AclOperation_e::any,
        AclOperation::All => kafka_common_acl_AclOperation_e::all,
        AclOperation::Read => kafka_common_acl_AclOperation_e::read,
        AclOperation::Write => kafka_common_acl_AclOperation_e::write,
        AclOperation::Create => kafka_common_acl_AclOperation_e::create,
        AclOperation::Delete => kafka_common_acl_AclOperation_e::delete,
        AclOperation::Alter => kafka_common_acl_AclOperation_e::alter,
        AclOperation::Describe => kafka_common_acl_AclOperation_e::describe,
        AclOperation::ClusterAction => kafka_common_acl_AclOperation_e::cluster_action,
        AclOperation::DescribeConfigs => kafka_common_acl_AclOperation_e::describe_configs,
        AclOperation::AlterConfigs => kafka_common_acl_AclOperation_e::alter_configs,
        AclOperation::IdempotentWrite => kafka_common_acl_AclOperation_e::idempotent_write,
        AclOperation::CreateTokens => kafka_common_acl_AclOperation_e::create_tokens,
        AclOperation::DescribeTokens => kafka_common_acl_AclOperation_e::describe_tokens,
        AclOperation::TwoPhaseCommit => kafka_common_acl_AclOperation_e::two_phase_commit,
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
    unsafe { *(value as *const AclOperation) }
}

/// `AclOperation.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_unknown() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Unknown)
}

/// `AclOperation.ANY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_any() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Any)
}

/// `AclOperation.ALL`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_all() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::All)
}

/// `AclOperation.READ`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_read() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Read)
}

/// `AclOperation.WRITE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_write() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Write)
}

/// `AclOperation.CREATE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_create() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Create)
}

/// `AclOperation.DELETE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_delete() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Delete)
}

/// `AclOperation.ALTER`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_alter() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Alter)
}

/// `AclOperation.DESCRIBE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_describe() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::Describe)
}

/// `AclOperation.CLUSTER_ACTION`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_cluster_action() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::ClusterAction)
}

/// `AclOperation.DESCRIBE_CONFIGS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_describe_configs() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::DescribeConfigs)
}

/// `AclOperation.ALTER_CONFIGS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_alter_configs() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::AlterConfigs)
}

/// `AclOperation.IDEMPOTENT_WRITE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_idempotent_write() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::IdempotentWrite)
}

/// `AclOperation.CREATE_TOKENS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_create_tokens() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::CreateTokens)
}

/// `AclOperation.DESCRIBE_TOKENS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_describe_tokens() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::DescribeTokens)
}

/// `AclOperation.TWO_PHASE_COMMIT`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_acl_AclOperation_two_phase_commit() -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::TwoPhaseCommit)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation__enum(
    self_: *const kafka_common_acl_AclOperation_t,
) -> kafka_common_acl_AclOperation_e {
    enum_of(unsafe { value_of(self_) })
}

/// `code()`: the wire-protocol byte.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_code(self_: *const kafka_common_acl_AclOperation_t) -> i8 {
    unsafe { value_of(self_) }.code()
}

/// `fromCode(byte code)`: the singleton for a wire-protocol byte, `UNKNOWN`
/// for one this client does not know.
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_from_string(
    str: *const c_char,
) -> *const kafka_common_acl_AclOperation_t {
    singleton(AclOperation::from_string(&unsafe { c_str_to_string(str) }))
}

/// `isUnknown()`: whether this is the `UNKNOWN` value.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_is_unknown(self_: *const kafka_common_acl_AclOperation_t) -> i8 {
    i8::from(unsafe { value_of(self_) }.is_unknown())
}

/// `toString()`: Java's constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_acl_AclOperation_to_string(
    self_: *const kafka_common_acl_AclOperation_t,
) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &value) in VARIANTS.iter().enumerate() {
            let handle = singleton(value);
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
