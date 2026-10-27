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
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
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
    kafka_consumer_GroupProtocol_CLASSIC,
    kafka_consumer_GroupProtocol_CONSUMER,
}

/// One static instance per value, indexed by [`kafka_consumer_GroupProtocol_e`].
static VARIANTS: [GroupProtocol; 2] = [GroupProtocol::Classic, GroupProtocol::Consumer];

/// `name()` of each value, NUL-terminated, indexed like [`VARIANTS`].
static NAMES: LazyLock<Vec<CString>> = LazyLock::new(|| VARIANTS.iter().map(|p| owned_c_string(p.name())).collect());

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(protocol: GroupProtocol) -> kafka_consumer_GroupProtocol_e {
    match protocol {
        GroupProtocol::Classic => kafka_consumer_GroupProtocol_e::kafka_consumer_GroupProtocol_CLASSIC,
        GroupProtocol::Consumer => kafka_consumer_GroupProtocol_e::kafka_consumer_GroupProtocol_CONSUMER,
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
    // SAFETY: Per this function's `# Safety`, `protocol` is a singleton returned by this module,
    // i.e. a pointer to an element of the `static` `VARIANTS` array (`singleton`), so it points at
    // a live, initialised `GroupProtocol`, which is `Copy` and read out here.
    unsafe { *(protocol as *const GroupProtocol) }
}

/// Returns `GroupProtocol.CLASSIC`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_GroupProtocol_classic() -> *const kafka_consumer_GroupProtocol_t {
    singleton(GroupProtocol::Classic)
}

/// Returns `GroupProtocol.CONSUMER`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_GroupProtocol_consumer() -> *const kafka_consumer_GroupProtocol_t {
    singleton(GroupProtocol::Consumer)
}

/// Returns the C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_consumer_GroupProtocol_CLASSIC`, the first enumerator: the
/// enum has no `UNKNOWN` constant, so the panic cannot be told apart from that value, and is only
/// logged.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_consumer_GroupProtocol_e::kafka_consumer_GroupProtocol_CLASSIC)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol__enum(
    self_: *const kafka_consumer_GroupProtocol_t,
) -> kafka_consumer_GroupProtocol_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    enum_of(unsafe { value_of(self_) })
}

/// String representation of the group protocol.
///
/// # Returns
///
/// The upper-case name (e.g. `"CLASSIC"`).
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. The string is static,
/// never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol_name(
    self_: *const kafka_consumer_GroupProtocol_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out, and the pointer returned is into the `static`
    // `NAMES`, never freed.
    NAMES[enum_of(unsafe { value_of(self_) }) as usize].as_ptr()
}

/// Case-insensitive group protocol lookup by string name.
///
/// # Parameters
///
/// - `name` - The name of the group protocol
///
/// # Returns
///
/// The corresponding GroupProtocol
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` If the name does not match any protocol
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `out_err` a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol_of(
    name: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *const kafka_consumer_GroupProtocol_t {
    // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    match GroupProtocol::of(&unsafe { c_str_to_string(name) }) {
        Ok(protocol) => singleton(protocol),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Java `toString()`.
///
/// # Returns
///
/// The lower-case name.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. The string is owned by the
/// caller, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_GroupProtocol_to_string(
    self_: *const kafka_consumer_GroupProtocol_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out and its text copied into an owned string.
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
        // SAFETY: Test code: `classic` and `consumer` are singletons of this module, never freed,
        // and the string from `to_string` is destroyed once, after it is read.
        unsafe {
            assert_eq!(
                kafka_consumer_GroupProtocol__enum(classic),
                kafka_consumer_GroupProtocol_e::kafka_consumer_GroupProtocol_CLASSIC
            );
            assert_eq!(
                kafka_consumer_GroupProtocol__enum(consumer),
                kafka_consumer_GroupProtocol_e::kafka_consumer_GroupProtocol_CONSUMER
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
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: the name is a C string literal and `error` a valid slot pointing at
        // null.
        let out = unsafe { kafka_consumer_GroupProtocol_of(c"Consumer".as_ptr(), &mut error) };
        assert!(error.is_null());
        assert_eq!(out, kafka_consumer_GroupProtocol_consumer());
        // SAFETY: Test code: the name is a C string literal and `error` a valid slot pointing at
        // null (the call above left it untouched).
        let out = unsafe { kafka_consumer_GroupProtocol_of(c"share".as_ptr(), &mut error) };
        assert!(!error.is_null());
        assert!(out.is_null());
        // SAFETY: Test code: `error` is the owned error just returned (non-null, asserted above),
        // destroyed only after `inner`'s last use.
        let inner = unsafe { error_ref(error) };
        assert!(inner.error.is_local_illegal_argument_error());
        assert_eq!(
            inner.error.message(),
            "No enum constant org.apache.kafka.clients.consumer.GroupProtocol.share"
        );
        // SAFETY: Test code: `error` is the owned error returned above, destroyed once.
        unsafe { kafka_common_Error_destroy(error) };
    }
}
