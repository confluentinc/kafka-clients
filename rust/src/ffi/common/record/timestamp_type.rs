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

//! `kafka_common_record_TimestampType_t`:
//! `org.apache.kafka.common.record.TimestampType` (CLAUDE.md §4, "Enums"):
//! borrowed per-value singletons plus the C enum
//! `kafka_common_record_TimestampType_e`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char};
use std::sync::LazyLock;

use crate::common::record::TimestampType;
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`TimestampType`] singleton.
#[repr(C)]
pub struct kafka_common_record_TimestampType_t {
    _private: [u8; 0],
}

/// The values of [`TimestampType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_record_TimestampType_e {
    no_timestamp_type,
    create_time,
    log_append_time,
}

/// One static instance per value, indexed by
/// [`kafka_common_record_TimestampType_e`].
static VARIANTS: [TimestampType; 3] = [
    TimestampType::NoTimestampType,
    TimestampType::CreateTime,
    TimestampType::LogAppendTime,
];

/// `name()` of each value, NUL-terminated, indexed like [`VARIANTS`].
static NAMES: LazyLock<Vec<CString>> = LazyLock::new(|| VARIANTS.iter().map(|t| owned_c_string(t.name())).collect());

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(timestamp_type: TimestampType) -> kafka_common_record_TimestampType_e {
    match timestamp_type {
        TimestampType::NoTimestampType => kafka_common_record_TimestampType_e::no_timestamp_type,
        TimestampType::CreateTime => kafka_common_record_TimestampType_e::create_time,
        TimestampType::LogAppendTime => kafka_common_record_TimestampType_e::log_append_time,
    }
}

/// The borrowed singleton standing for `timestamp_type`.
pub(crate) fn singleton(timestamp_type: TimestampType) -> *const kafka_common_record_TimestampType_t {
    &VARIANTS[enum_of(timestamp_type) as usize] as *const TimestampType as *const kafka_common_record_TimestampType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `timestamp_type` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(timestamp_type: *const kafka_common_record_TimestampType_t) -> TimestampType {
    unsafe { *(timestamp_type as *const TimestampType) }
}

/// `TimestampType.NO_TIMESTAMP_TYPE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_record_TimestampType_no_timestamp_type() -> *const kafka_common_record_TimestampType_t {
    singleton(TimestampType::NoTimestampType)
}

/// `TimestampType.CREATE_TIME`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_record_TimestampType_create_time() -> *const kafka_common_record_TimestampType_t {
    singleton(TimestampType::CreateTime)
}

/// `TimestampType.LOG_APPEND_TIME`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_record_TimestampType_log_append_time() -> *const kafka_common_record_TimestampType_t {
    singleton(TimestampType::LogAppendTime)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_record_TimestampType__enum(
    self_: *const kafka_common_record_TimestampType_t,
) -> kafka_common_record_TimestampType_e {
    enum_of(unsafe { value_of(self_) })
}

/// `id`: `-1`, `0` or `1`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_record_TimestampType_id(
    self_: *const kafka_common_record_TimestampType_t,
) -> i32 {
    unsafe { value_of(self_) }.id()
}

/// `name`: a static string, never freed.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_record_TimestampType_name(
    self_: *const kafka_common_record_TimestampType_t,
) -> *const c_char {
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// `TimestampType.forName(String name)`: delivers the singleton through
/// `out_for_name`, or returns the owned `NoSuchElementException` translation
/// for an unknown name.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `out_for_name` a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_record_TimestampType_for_name(
    name: *const c_char,
    out_for_name: *mut *const kafka_common_record_TimestampType_t,
) -> *mut kafka_common_Error_t {
    match TimestampType::for_name(&unsafe { c_str_to_string(name) }) {
        Ok(timestamp_type) => {
            unsafe { *out_for_name = singleton(timestamp_type) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `toString()`: the name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_record_TimestampType_to_string(
    self_: *const kafka_common_record_TimestampType_t,
) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &timestamp_type) in VARIANTS.iter().enumerate() {
            let handle = singleton(timestamp_type);
            unsafe {
                assert_eq!(value_of(handle), timestamp_type);
                assert_eq!(kafka_common_record_TimestampType__enum(handle) as usize, index);
                assert_eq!(kafka_common_record_TimestampType_id(handle), timestamp_type.id());
                assert_eq!(
                    CStr::from_ptr(kafka_common_record_TimestampType_name(handle)).to_str().unwrap(),
                    timestamp_type.name()
                );
            }
        }
        assert_eq!(
            kafka_common_record_TimestampType_no_timestamp_type(),
            singleton(TimestampType::NoTimestampType)
        );
        assert_eq!(
            kafka_common_record_TimestampType_create_time(),
            singleton(TimestampType::CreateTime)
        );
        assert_eq!(
            kafka_common_record_TimestampType_log_append_time(),
            singleton(TimestampType::LogAppendTime)
        );
    }

    #[test]
    fn for_name_and_to_string_follow_java() {
        let create_time = CString::new("CreateTime").unwrap();
        let bogus = CString::new("bogus").unwrap();
        unsafe {
            let mut timestamp_type = ptr::null();
            assert!(kafka_common_record_TimestampType_for_name(create_time.as_ptr(), &mut timestamp_type).is_null());
            assert_eq!(timestamp_type, kafka_common_record_TimestampType_create_time());
            let error = kafka_common_record_TimestampType_for_name(bogus.as_ptr(), &mut timestamp_type);
            assert!(!error.is_null());
            kafka_common_Error_destroy(error);

            let s = kafka_common_record_TimestampType_to_string(kafka_common_record_TimestampType_log_append_time());
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), TimestampType::LogAppendTime.to_string());
            kafka_string_destroy(s);
        }
    }
}
