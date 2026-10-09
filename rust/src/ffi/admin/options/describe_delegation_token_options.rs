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

//! `kafka_admin_DescribeDelegationTokenOptions_t`:
//! `org.apache.kafka.clients.admin.DescribeDelegationTokenOptions` (CLAUDE.md §4).
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `set_x(self, x)` on the handle is Java's
//! `options.x(x)` with the result stored back. Java's `owners` is a nullable
//! list: `NULL` stands for it both ways.

use std::ffi::c_void;
use std::ptr;

use crate::admin::DescribeDelegationTokenOptions;
use crate::ffi::common::security::auth::kafka_principal::{
    box_kafka_principal, kafka_common_security_auth_KafkaPrincipal_destroy,
    kafka_common_security_auth_KafkaPrincipal_t, list_kafka_principals,
};
use crate::ffi::util::{box_list, kafka_List_t};

/// Opaque handle to a [`DescribeDelegationTokenOptions`], owned by the
/// caller and freed with [`kafka_admin_DescribeDelegationTokenOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeDelegationTokenOptions_t {
    _private: [u8; 0],
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle.
pub(crate) unsafe fn describe_delegation_token_options_ref<'a>(
    options: *const kafka_admin_DescribeDelegationTokenOptions_t,
) -> &'a DescribeDelegationTokenOptions {
    unsafe { &*(options as *const DescribeDelegationTokenOptions) }
}

/// Mutable access to the options behind a handle, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle and no other reference to it may be live.
unsafe fn options_mut<'a>(
    options: *mut kafka_admin_DescribeDelegationTokenOptions_t,
) -> &'a mut DescribeDelegationTokenOptions {
    unsafe { &mut *(options as *mut DescribeDelegationTokenOptions) }
}

fn boxed(options: DescribeDelegationTokenOptions) -> *mut kafka_admin_DescribeDelegationTokenOptions_t {
    Box::into_raw(Box::new(options)) as *mut kafka_admin_DescribeDelegationTokenOptions_t
}

/// Frees a `kafka_common_security_auth_KafkaPrincipal_t *` element of an
/// owned list.
unsafe fn destroy_kafka_principal_element(element: *mut c_void) {
    unsafe {
        kafka_common_security_auth_KafkaPrincipal_destroy(element as *mut kafka_common_security_auth_KafkaPrincipal_t)
    }
}

/// `new DescribeDelegationTokenOptions()`: every authorized token, with the
/// client's default API timeout. Owned, freed with
/// [`kafka_admin_DescribeDelegationTokenOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_DescribeDelegationTokenOptions_new() -> *mut kafka_admin_DescribeDelegationTokenOptions_t
{
    boxed(DescribeDelegationTokenOptions::new())
}

/// `DescribeDelegationTokenOptions.owners(List<KafkaPrincipal> owners)`:
/// `owners` is a borrowed `kafka_List_t` of borrowed
/// `kafka_common_security_auth_KafkaPrincipal_t *`, copied during the call;
/// `NULL` is Java's `null`, describing all the tokens the caller owns or may
/// describe.
///
/// # Safety
///
/// `self_` must be a live handle; `owners` must be `NULL` or a list of live
/// principal handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_set_owners(
    self_: *mut kafka_admin_DescribeDelegationTokenOptions_t,
    owners: *const kafka_List_t,
) {
    let owners = if owners.is_null() {
        None
    } else {
        Some(unsafe { list_kafka_principals(owners) })
    };
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_owners(owners);
}

/// `DescribeDelegationTokenOptions.owners()`: `NULL` when unset (Java's
/// `null`), else an owned `kafka_List_t` of owned
/// `kafka_common_security_auth_KafkaPrincipal_t *` in the order they were set,
/// freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_owners(
    self_: *const kafka_admin_DescribeDelegationTokenOptions_t,
) -> *mut kafka_List_t {
    match unsafe { describe_delegation_token_options_ref(self_) }.owners() {
        None => ptr::null_mut(),
        Some(owners) => box_list(
            owners
                .iter()
                .map(|owner| box_kafka_principal(owner.clone()) as *mut c_void)
                .collect(),
            Some(destroy_kafka_principal_element),
        ),
    }
}

/// `AbstractOptions.timeoutMs(Integer timeoutMs)`: `-1` (any negative value)
/// stands for Java's `null`, the client's default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_set_timeout_ms(
    self_: *mut kafka_admin_DescribeDelegationTokenOptions_t,
    timeout_ms: i32,
) {
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms));
}

/// `AbstractOptions.timeoutMs()`: the timeout in milliseconds, `-1` when the
/// client's default API timeout applies.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_timeout_ms(
    self_: *const kafka_admin_DescribeDelegationTokenOptions_t,
) -> i32 {
    unsafe { describe_delegation_token_options_ref(self_) }
        .timeout_ms()
        .unwrap_or(-1)
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_destroy(
    self_: *mut kafka_admin_DescribeDelegationTokenOptions_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut DescribeDelegationTokenOptions) });
    }
}
