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

//! `kafka_consumer_GroupProtocol_t`:
//! `org.apache.kafka.clients.consumer.GroupProtocol` (CLAUDE.md §4,
//! "Enums"): borrowed per-value singletons plus the C enum
//! `kafka_consumer_GroupProtocol_e`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char};
use std::sync::LazyLock;

use crate::consumer::GroupProtocol;
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`GroupProtocol`] singleton.
#[repr(C)]
pub struct kafka_consumer_GroupProtocol_t {
    _private: [u8; 0],
}

/// The values of [`GroupProtocol`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_consumer_GroupProtocol_e {
    classic,
    consumer,
}

/// One static instance per value, indexed by [`kafka_consumer_GroupProtocol_e`].
static VARIANTS: [GroupProtocol; 2] = [GroupProtocol::Classic, GroupProtocol::Consumer];

/// `name()` of each value, NUL-terminated, indexed like [`VARIANTS`].
static NAMES: LazyLock<Vec<CString>> = LazyLock::new(|| VARIANTS.iter().map(|p| owned_c_string(p.name())).collect());

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(protocol: GroupProtocol) -> kafka_consumer_GroupProtocol_e {
    match protocol {
        GroupProtocol::Classic => kafka_consumer_GroupProtocol_e::classic,
        GroupProtocol::Consumer => kafka_consumer_GroupProtocol_e::consumer,
    }
}

/// The borrowed singleton standing for `protocol`.
pub(crate) fn singleton(protocol: GroupProtocol) -> *const kafka_consumer_GroupProtocol_t {
    &VARIANTS[enum_of(protocol) as usize] as *const GroupProtocol as *const kafka_consumer_GroupProtocol_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `protocol` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(protocol: *const kafka_consumer_GroupProtocol_t) -> GroupProtocol {
    unsafe { *(protocol as *const GroupProtocol) }
}

/// `GroupProtocol.CLASSIC`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_GroupProtocol_classic() -> *const kafka_consumer_GroupProtocol_t {
    singleton(GroupProtocol::Classic)
}

/// `GroupProtocol.CONSUMER`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_GroupProtocol_consumer() -> *const kafka_consumer_GroupProtocol_t {
    singleton(GroupProtocol::Consumer)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol__enum(
    self_: *const kafka_consumer_GroupProtocol_t,
) -> kafka_consumer_GroupProtocol_e {
    enum_of(unsafe { value_of(self_) })
}

/// `name`: the upper-case Java enum name, a static string never freed.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol_name(
    self_: *const kafka_consumer_GroupProtocol_t,
) -> *const c_char {
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// `GroupProtocol.of(String name)`, case-insensitive: delivers the singleton
/// through `out_of`, or returns the owned `IllegalArgumentException`
/// translation for an unknown name.
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `out_of` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol_of(
    name: *const c_char,
    out_of: *mut *const kafka_consumer_GroupProtocol_t,
) -> *mut kafka_common_Error_t {
    match GroupProtocol::of(&unsafe { c_str_to_string(name) }) {
        Ok(protocol) => {
            unsafe { *out_of = singleton(protocol) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// Java `toString()`: the lower-case name, an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol_to_string(
    self_: *const kafka_consumer_GroupProtocol_t,
) -> *mut c_char {
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn singletons_enumerators_and_names() {
        let classic = kafka_consumer_GroupProtocol_classic();
        let consumer = kafka_consumer_GroupProtocol_consumer();
        assert_ne!(classic, consumer);
        assert_eq!(classic, kafka_consumer_GroupProtocol_classic());
        unsafe {
            assert_eq!(
                kafka_consumer_GroupProtocol__enum(classic),
                kafka_consumer_GroupProtocol_e::classic
            );
            assert_eq!(
                kafka_consumer_GroupProtocol__enum(consumer),
                kafka_consumer_GroupProtocol_e::consumer
            );
            assert_eq!(
                CStr::from_ptr(kafka_consumer_GroupProtocol_name(consumer)).to_str().unwrap(),
                "CONSUMER"
            );
            let s = kafka_consumer_GroupProtocol_to_string(classic);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), "classic");
            kafka_string_destroy(s);
        }
    }

    #[test]
    fn of_is_case_insensitive_and_fails_like_java() {
        let mut out = std::ptr::null();
        assert!(unsafe { kafka_consumer_GroupProtocol_of(c"Consumer".as_ptr(), &mut out) }.is_null());
        assert_eq!(out, kafka_consumer_GroupProtocol_consumer());
        let error = unsafe { kafka_consumer_GroupProtocol_of(c"share".as_ptr(), &mut out) };
        assert!(!error.is_null());
        let inner = unsafe { error_ref(error) };
        assert!(inner.error.is_local_illegal_argument_error());
        assert_eq!(
            inner.error.message(),
            "No enum constant org.apache.kafka.clients.consumer.GroupProtocol.share"
        );
        unsafe { kafka_common_Error_destroy(error) };
    }
}
