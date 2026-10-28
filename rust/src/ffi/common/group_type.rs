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

//! `kafka_common_GroupType_t`: `org.apache.kafka.common.GroupType`
//! (CLAUDE.md §4, "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_common_GroupType_e`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char};
use std::sync::LazyLock;

use crate::common::GroupType;
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`GroupType`] singleton.
#[repr(C)]
pub struct kafka_common_GroupType_t {
    _private: [u8; 0],
}

/// The values of [`GroupType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_GroupType_e {
    kafka_common_GroupType_UNKNOWN,
    kafka_common_GroupType_CONSUMER,
    kafka_common_GroupType_CLASSIC,
    kafka_common_GroupType_SHARE,
    kafka_common_GroupType_STREAMS,
}

/// One static instance per value, indexed by [`kafka_common_GroupType_e`].
static VARIANTS: [GroupType; 5] = [
    GroupType::Unknown,
    GroupType::Consumer,
    GroupType::Classic,
    GroupType::Share,
    GroupType::Streams,
];

/// `name()` of each value, NUL-terminated, indexed like [`VARIANTS`].
static NAMES: LazyLock<Vec<CString>> = LazyLock::new(|| VARIANTS.iter().map(|t| owned_c_string(t.name())).collect());

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(group_type: GroupType) -> kafka_common_GroupType_e {
    match group_type {
        GroupType::Unknown => kafka_common_GroupType_e::kafka_common_GroupType_UNKNOWN,
        GroupType::Consumer => kafka_common_GroupType_e::kafka_common_GroupType_CONSUMER,
        GroupType::Classic => kafka_common_GroupType_e::kafka_common_GroupType_CLASSIC,
        GroupType::Share => kafka_common_GroupType_e::kafka_common_GroupType_SHARE,
        GroupType::Streams => kafka_common_GroupType_e::kafka_common_GroupType_STREAMS,
    }
}

/// The borrowed singleton standing for `group_type`.
pub(crate) fn singleton(group_type: GroupType) -> *const kafka_common_GroupType_t {
    &VARIANTS[enum_of(group_type) as usize] as *const GroupType as *const kafka_common_GroupType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `group_type` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(group_type: *const kafka_common_GroupType_t) -> GroupType {
    // SAFETY: Per this function's `# Safety`, `group_type` is a singleton returned by this module,
    // i.e. a pointer to an element of the `static` `VARIANTS` array (`singleton`), so it points at
    // a live, initialised `GroupType`, which is `Copy` and read out here.
    unsafe { *(group_type as *const GroupType) }
}

/// `GroupType.UNKNOWN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupType_unknown() -> *const kafka_common_GroupType_t {
    singleton(GroupType::Unknown)
}

/// `GroupType.CONSUMER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupType_consumer() -> *const kafka_common_GroupType_t {
    singleton(GroupType::Consumer)
}

/// `GroupType.CLASSIC`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupType_classic() -> *const kafka_common_GroupType_t {
    singleton(GroupType::Classic)
}

/// `GroupType.SHARE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupType_share() -> *const kafka_common_GroupType_t {
    singleton(GroupType::Share)
}

/// `GroupType.STREAMS`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupType_streams() -> *const kafka_common_GroupType_t {
    singleton(GroupType::Streams)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_common_GroupType_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_common_GroupType_e::kafka_common_GroupType_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupType__enum(
    self_: *const kafka_common_GroupType_t,
) -> kafka_common_GroupType_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    enum_of(unsafe { value_of(self_) })
}

/// `name()`: Java's `Enum.name()`, the constant name (`CONSUMER`), as a
/// static string, never freed. The text `parse` reads (`Consumer`) is
/// `toString()`'s.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupType_name(self_: *const kafka_common_GroupType_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out, and the name returned is owned by the `static`
    // `NAMES`, which is never dropped, so it is never freed, as the rustdoc states.
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// `GroupType.parse(String name)`: case-insensitive, `UNKNOWN` for an
/// unrecognised name, as in Java.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupType_parse(name: *const c_char) -> *const kafka_common_GroupType_t {
    // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    singleton(GroupType::parse(&unsafe { c_str_to_string(name) }))
}

/// `toString()`: the text the broker sends and `parse` reads (`Consumer`), as
/// an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupType_to_string(self_: *const kafka_common_GroupType_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out and its text copied into an owned string.
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &group_type) in VARIANTS.iter().enumerate() {
            let handle = singleton(group_type);
            // SAFETY: Test code: every handle passed is a singleton from `singleton`, which points
            // into the static `VARIANTS` array and is never freed, and the names read are owned by
            // the static `NAMES`.
            unsafe {
                assert_eq!(value_of(handle), group_type);
                assert_eq!(kafka_common_GroupType__enum(handle) as usize, index);
                assert_eq!(
                    CStr::from_ptr(kafka_common_GroupType_name(handle)).to_str().unwrap(),
                    group_type.name()
                );
            }
        }
        assert_eq!(kafka_common_GroupType_unknown(), singleton(GroupType::Unknown));
        assert_eq!(kafka_common_GroupType_consumer(), singleton(GroupType::Consumer));
        assert_eq!(kafka_common_GroupType_classic(), singleton(GroupType::Classic));
        assert_eq!(kafka_common_GroupType_share(), singleton(GroupType::Share));
        assert_eq!(kafka_common_GroupType_streams(), singleton(GroupType::Streams));
    }

    #[test]
    fn parse_and_to_string_follow_java() {
        let consumer = CString::new("consumer").unwrap();
        let bogus = CString::new("bogus").unwrap();
        // SAFETY: Test code: the `CString`s outlive the calls reading them, the singletons are
        // never freed, and the string from `to_string` is destroyed once, after it is read.
        unsafe {
            assert_eq!(
                kafka_common_GroupType_parse(consumer.as_ptr()),
                kafka_common_GroupType_consumer()
            );
            assert_eq!(kafka_common_GroupType_parse(bogus.as_ptr()), kafka_common_GroupType_unknown());
            let s = kafka_common_GroupType_to_string(kafka_common_GroupType_share());
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), GroupType::Share.to_string());
            kafka_string_destroy(s);
        }
    }

    /// New test, no Java original: `_name` is Java's `Enum.name()` and
    /// `_to_string` Java's `toString()`, the text `_parse` reads back. No
    /// `GroupType` constant has an underscore, so Java's case-insensitive
    /// `parse` reads the constant names too.
    #[test]
    fn name_is_the_constant_and_to_string_the_java_text() {
        // SAFETY: Test code: the singletons are never freed, the names `_name` returns are owned
        // by the static `NAMES`, each string `_to_string` returns is destroyed once, after it is
        // read and parsed back, and the `CStr` literals outlive the calls reading them.
        unsafe {
            for (handle, name, text) in [
                (kafka_common_GroupType_consumer(), "CONSUMER", "Consumer"),
                (kafka_common_GroupType_streams(), "STREAMS", "Streams"),
            ] {
                assert_eq!(CStr::from_ptr(kafka_common_GroupType_name(handle)).to_str().unwrap(), name);
                let s = kafka_common_GroupType_to_string(handle);
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), text);
                assert_eq!(kafka_common_GroupType_parse(s), handle);
                kafka_string_destroy(s);
            }
            assert_eq!(
                kafka_common_GroupType_parse(c"STREAMS".as_ptr()),
                kafka_common_GroupType_streams()
            );
        }
    }
}
