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
use crate::ffi::ffi_guard;
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
/// `options` must be a live handle, kept alive for `'a`, during which no
/// setter runs on it.
pub(crate) unsafe fn describe_delegation_token_options_ref<'a>(
    options: *const kafka_admin_DescribeDelegationTokenOptions_t,
) -> &'a DescribeDelegationTokenOptions {
    // SAFETY: Per this function's `# Safety`, `options` is a live handle kept alive for `'a`,
    // during which no setter runs on it. Every `kafka_admin_DescribeDelegationTokenOptions_t` is
    // the `Box::into_raw` of a `Box<DescribeDelegationTokenOptions>` (`boxed`), so the shared
    // reference is to a live `DescribeDelegationTokenOptions` that nothing mutates while it is
    // used.
    unsafe { &*(options as *const DescribeDelegationTokenOptions) }
}

/// Mutable access to the options behind a handle, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle, kept alive for `'a`, to which no other
/// reference is live meanwhile.
unsafe fn options_mut<'a>(
    options: *mut kafka_admin_DescribeDelegationTokenOptions_t,
) -> &'a mut DescribeDelegationTokenOptions {
    // SAFETY: Per this function's `# Safety`, `options` is a live handle kept alive for `'a`, to
    // which no other reference is live meanwhile. Every
    // `kafka_admin_DescribeDelegationTokenOptions_t` is the `Box::into_raw` of a
    // `Box<DescribeDelegationTokenOptions>` (`boxed`), so the exclusive reference is to a live
    // `DescribeDelegationTokenOptions`.
    unsafe { &mut *(options as *mut DescribeDelegationTokenOptions) }
}

fn boxed(options: DescribeDelegationTokenOptions) -> *mut kafka_admin_DescribeDelegationTokenOptions_t {
    Box::into_raw(Box::new(options)) as *mut kafka_admin_DescribeDelegationTokenOptions_t
}

/// Frees a `kafka_common_security_auth_KafkaPrincipal_t *` element of an
/// owned list.
///
/// # Safety
///
/// `element` must be an owned principal handle not yet destroyed.
unsafe fn destroy_kafka_principal_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned principal handle not yet
    // destroyed, as `kafka_common_security_auth_KafkaPrincipal_destroy` requires.
    unsafe {
        kafka_common_security_auth_KafkaPrincipal_destroy(element as *mut kafka_common_security_auth_KafkaPrincipal_t)
    }
}

/// `new DescribeDelegationTokenOptions()`: every authorized token, with the client's default API
/// timeout.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_admin_DescribeDelegationTokenOptions_destroy`.
#[ffi_guard]
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
/// # Parameters
///
/// - `owners` - The owners that we want to describe delegation tokens for
///
/// # Safety
///
/// `self_` must be a live handle that no other call uses at the same time;
/// `owners` must be `NULL` or a valid list whose elements are live
/// principal handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_set_owners(
    self_: *mut kafka_admin_DescribeDelegationTokenOptions_t,
    owners: *const kafka_List_t,
) {
    let owners = if owners.is_null() {
        None
    } else {
        // SAFETY: `owners` is non-null (checked above) and, per this function's `# Safety`, a valid
        // list whose elements are live principal handles, as `list_kafka_principals` requires; they
        // are copied during the call.
        Some(unsafe { list_kafka_principals(owners) })
    };
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle that no other call uses at
    // the same time, so no other reference to it is live while `options` is used, as `options_mut`
    // requires.
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_owners(owners);
}

/// `DescribeDelegationTokenOptions.owners()`.
///
/// # Returns
///
/// The owners, a `kafka_List_t` of `kafka_common_security_auth_KafkaPrincipal_t *` in the order
/// they were set, or `NULL` when unset (Java's `null`).
///
/// # Safety
///
/// `self_` must be a live handle. The list and its elements are owned by the caller, freed with
/// `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_owners(
    self_: *const kafka_admin_DescribeDelegationTokenOptions_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `describe_delegation_token_options_ref` requires.
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

/// Set the timeout in milliseconds for this operation or `-1` if the default API timeout for the
/// AdminClient should be used.
///
/// # Parameters
///
/// - `timeout_ms` - the timeout in milliseconds; any negative value stands for Java's `null`, the
///   default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle that no other call uses at the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_set_timeout_ms(
    self_: *mut kafka_admin_DescribeDelegationTokenOptions_t,
    timeout_ms: i32,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle that no other call uses at
    // the same time, so no other reference to it is live while `options` is used, as `options_mut`
    // requires.
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms));
}

/// The timeout in milliseconds for this operation or `-1` if the default API timeout for the
/// AdminClient should be used.
///
/// # Returns
///
/// The timeout in milliseconds, or `-1` for the default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_timeout_ms(
    self_: *const kafka_admin_DescribeDelegationTokenOptions_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `describe_delegation_token_options_ref` requires.
    unsafe { describe_delegation_token_options_ref(self_) }
        .timeout_ms()
        .unwrap_or(-1)
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed, that no other call
/// uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenOptions_destroy(
    self_: *mut kafka_admin_DescribeDelegationTokenOptions_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, a handle
        // not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every `kafka_admin_DescribeDelegationTokenOptions_t` is the `Box::into_raw`
        // of a `Box<DescribeDelegationTokenOptions>` (`boxed`), so `from_raw` reclaims that
        // allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut DescribeDelegationTokenOptions) });
    }
}
