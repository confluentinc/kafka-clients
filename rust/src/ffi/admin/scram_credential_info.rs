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

//! `kafka_admin_ScramCredentialInfo_t`:
//! `org.apache.kafka.clients.admin.ScramCredentialInfo` (CLAUDE.md §4). The
//! handle points at the Rust value, so a `ScramCredentialInfo` held inside a
//! `kafka_admin_UserScramCredentialUpsertion_t` is borrowed out by address.

use std::ffi::{c_char, c_void};

use crate::admin::ScramCredentialInfo;
use crate::ffi::admin::scram_mechanism::{
    kafka_admin_ScramMechanism_t, scram_mechanism_singleton, scram_mechanism_value_of,
};
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`ScramCredentialInfo`].
#[repr(C)]
pub struct kafka_admin_ScramCredentialInfo_t {
    _private: [u8; 0],
}

/// Hands `info` to C as an owned handle, freed with
/// [`kafka_admin_ScramCredentialInfo_destroy`].
pub(crate) fn box_scram_credential_info(info: ScramCredentialInfo) -> *mut kafka_admin_ScramCredentialInfo_t {
    Box::into_raw(Box::new(info)) as *mut kafka_admin_ScramCredentialInfo_t
}

/// A borrowed handle on `info`, valid as long as the value stays where it is
/// (inside a boxed parent handle).
pub(crate) fn scram_credential_info_ptr(info: &ScramCredentialInfo) -> *const kafka_admin_ScramCredentialInfo_t {
    info as *const ScramCredentialInfo as *const kafka_admin_ScramCredentialInfo_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `info` must be a live SCRAM-credential-info handle.
pub(crate) unsafe fn scram_credential_info_ref<'a>(
    info: *const kafka_admin_ScramCredentialInfo_t,
) -> &'a ScramCredentialInfo {
    unsafe { &*(info as *const ScramCredentialInfo) }
}

/// Frees a `kafka_admin_ScramCredentialInfo_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned SCRAM-credential-info handle.
pub(crate) unsafe fn destroy_scram_credential_info_element(element: *mut c_void) {
    unsafe { kafka_admin_ScramCredentialInfo_destroy(element as *mut kafka_admin_ScramCredentialInfo_t) }
}

/// `new ScramCredentialInfo(ScramMechanism mechanism, int iterations)`;
/// `mechanism` is a `kafka_admin_ScramMechanism_t` singleton. Owned, freed
/// with [`kafka_admin_ScramCredentialInfo_destroy`].
///
/// # Safety
///
/// `mechanism` must be a `kafka_admin_ScramMechanism_t` singleton.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramCredentialInfo_new(
    mechanism: *const kafka_admin_ScramMechanism_t,
    iterations: i32,
) -> *mut kafka_admin_ScramCredentialInfo_t {
    box_scram_credential_info(ScramCredentialInfo::new(
        unsafe { scram_mechanism_value_of(mechanism) },
        iterations,
    ))
}

/// `mechanism()`: the `kafka_admin_ScramMechanism_t` singleton, never freed.
///
/// # Safety
///
/// `self_` must be a valid SCRAM-credential-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramCredentialInfo_mechanism(
    self_: *const kafka_admin_ScramCredentialInfo_t,
) -> *const kafka_admin_ScramMechanism_t {
    scram_mechanism_singleton(unsafe { scram_credential_info_ref(self_) }.mechanism())
}

/// `iterations()`: the number of iterations used when creating the
/// credential.
///
/// # Safety
///
/// `self_` must be a valid SCRAM-credential-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramCredentialInfo_iterations(
    self_: *const kafka_admin_ScramCredentialInfo_t,
) -> i32 {
    unsafe { scram_credential_info_ref(self_) }.iterations()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid SCRAM-credential-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramCredentialInfo_to_string(
    self_: *const kafka_admin_ScramCredentialInfo_t,
) -> *mut c_char {
    into_c_string(&unsafe { scram_credential_info_ref(self_) }.to_string())
}

/// Frees an owned SCRAM-credential-info handle. Null is a no-op; a handle
/// borrowed from a `kafka_admin_UserScramCredentialUpsertion_t` is never
/// passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ScramCredentialInfo_destroy(self_: *mut kafka_admin_ScramCredentialInfo_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ScramCredentialInfo) });
    }
}
