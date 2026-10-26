// Copyright 2026 Confluent Inc.
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

//! `kafka_admin_EndpointType_t`:
//! `org.apache.kafka.clients.admin.EndpointType` (CLAUDE.md §4, "Enums"):
//! borrowed per-value singletons plus the C enum `kafka_admin_EndpointType_e`.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::admin::EndpointType;
use crate::ffi::ffi_guard;
use crate::ffi::util::into_c_string;

/// Opaque handle to an [`EndpointType`] singleton.
#[repr(C)]
pub struct kafka_admin_EndpointType_t {
    _private: [u8; 0],
}

/// The values of [`EndpointType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_EndpointType_e {
    kafka_admin_EndpointType_UNKNOWN,
    kafka_admin_EndpointType_BROKER,
    kafka_admin_EndpointType_CONTROLLER,
}

/// One static instance per value, indexed by [`kafka_admin_EndpointType_e`].
static VARIANTS: [EndpointType; 3] = [EndpointType::Unknown, EndpointType::Broker, EndpointType::Controller];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(endpoint_type: EndpointType) -> kafka_admin_EndpointType_e {
    match endpoint_type {
        EndpointType::Unknown => kafka_admin_EndpointType_e::kafka_admin_EndpointType_UNKNOWN,
        EndpointType::Broker => kafka_admin_EndpointType_e::kafka_admin_EndpointType_BROKER,
        EndpointType::Controller => kafka_admin_EndpointType_e::kafka_admin_EndpointType_CONTROLLER,
    }
}

/// The borrowed singleton standing for `endpoint_type`.
fn endpoint_type_singleton(endpoint_type: EndpointType) -> *const kafka_admin_EndpointType_t {
    &VARIANTS[enum_of(endpoint_type) as usize] as *const EndpointType as *const kafka_admin_EndpointType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `endpoint_type` must be a singleton returned by this module.
unsafe fn endpoint_type_value_of(endpoint_type: *const kafka_admin_EndpointType_t) -> EndpointType {
    // SAFETY: Per this function's `# Safety`, `endpoint_type` is a singleton returned by this
    // module. `endpoint_type_singleton` is the only producer and points it at an element of the
    // static `VARIANTS`, an `EndpointType` that is never freed or mutated, so the read copies a
    // live value.
    unsafe { *(endpoint_type as *const EndpointType) }
}

/// `EndpointType.UNKNOWN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_EndpointType_unknown() -> *const kafka_admin_EndpointType_t {
    endpoint_type_singleton(EndpointType::Unknown)
}

/// `EndpointType.BROKER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_EndpointType_broker() -> *const kafka_admin_EndpointType_t {
    endpoint_type_singleton(EndpointType::Broker)
}

/// `EndpointType.CONTROLLER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_EndpointType_controller() -> *const kafka_admin_EndpointType_t {
    endpoint_type_singleton(EndpointType::Controller)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_admin_EndpointType_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_admin_EndpointType_e::kafka_admin_EndpointType_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_EndpointType__enum(
    self_: *const kafka_admin_EndpointType_t,
) -> kafka_admin_EndpointType_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `endpoint_type_value_of` requires.
    enum_of(unsafe { endpoint_type_value_of(self_) })
}

/// `toString()`, Java's inherited `Enum.toString()`: the constant's name, as an owned string freed
/// with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_EndpointType_to_string(self_: *const kafka_admin_EndpointType_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `endpoint_type_value_of` requires; the value is copied out and its name copied into an owned
    // string.
    into_c_string(&unsafe { endpoint_type_value_of(self_) }.to_string())
}

/// `EndpointType.id()`: the wire-protocol byte of the endpoint type.
///
/// A caught Rust panic returns -1, which is no id of this Java `byte` enum: its
/// 0 is the valid `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_EndpointType_id(self_: *const kafka_admin_EndpointType_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `endpoint_type_value_of` requires.
    unsafe { endpoint_type_value_of(self_) }.id()
}

/// `EndpointType.fromId(byte id)`: the borrowed singleton with that wire byte,
/// `UNKNOWN` for an id Java does not know.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_EndpointType_from_id(id: i8) -> *const kafka_admin_EndpointType_t {
    endpoint_type_singleton(EndpointType::from_id(id))
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    #[test]
    fn singletons_round_trip_through_id_and_name() {
        // SAFETY: Test code: every handle is a singleton from this module, as the reads require;
        // each owned string is freed with `kafka_string_destroy`.
        unsafe {
            for (index, (&endpoint_type, name)) in VARIANTS.iter().zip(["UNKNOWN", "BROKER", "CONTROLLER"]).enumerate()
            {
                let handle = endpoint_type_singleton(endpoint_type);
                assert_eq!(endpoint_type_value_of(handle), endpoint_type);
                assert_eq!(kafka_admin_EndpointType__enum(handle) as usize, index);
                assert_eq!(kafka_admin_EndpointType_from_id(kafka_admin_EndpointType_id(handle)), handle);
                let text = kafka_admin_EndpointType_to_string(handle);
                assert_eq!(CStr::from_ptr(text).to_str().unwrap(), name);
                crate::ffi::util::kafka_string_destroy(text);
            }
            assert_eq!(
                kafka_admin_EndpointType_unknown(),
                endpoint_type_singleton(EndpointType::Unknown)
            );
            assert_eq!(kafka_admin_EndpointType_broker(), endpoint_type_singleton(EndpointType::Broker));
            assert_eq!(
                kafka_admin_EndpointType_controller(),
                endpoint_type_singleton(EndpointType::Controller)
            );
            assert_eq!(kafka_admin_EndpointType_id(kafka_admin_EndpointType_controller()), 2);
            assert_eq!(kafka_admin_EndpointType_from_id(3), kafka_admin_EndpointType_unknown());
            assert_eq!(kafka_admin_EndpointType_from_id(-1), kafka_admin_EndpointType_unknown());
        }
    }
}
