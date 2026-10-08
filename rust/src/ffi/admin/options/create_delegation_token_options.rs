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

//! `kafka_admin_CreateDelegationTokenOptions_t`:
//! `org.apache.kafka.clients.admin.CreateDelegationTokenOptions` (CLAUDE.md §4).
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `set_x(self, x)` on the handle is Java's
//! `options.x(x)` with the result stored back. The handle keeps a cached
//! owner so `owner()` is a borrowed principal valid as long as the handle.

use std::ffi::c_void;
use std::ptr;

use crate::admin::CreateDelegationTokenOptions;
use crate::ffi::common::security::auth::kafka_principal::{
    KafkaPrincipalInner, box_kafka_principal, kafka_common_security_auth_KafkaPrincipal_destroy,
    kafka_common_security_auth_KafkaPrincipal_t, kafka_principal_ref, list_kafka_principals,
};
use crate::ffi::util::{box_list, kafka_List_t};

/// Opaque handle to a [`CreateDelegationTokenOptions`], owned by the caller
/// and freed with [`kafka_admin_CreateDelegationTokenOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_CreateDelegationTokenOptions_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_CreateDelegationTokenOptions_t`] points at: the
/// options plus the cached owner `owner()` borrows out.
struct CreateDelegationTokenOptionsInner {
    options: CreateDelegationTokenOptions,
    owner: Option<KafkaPrincipalInner>,
}

impl CreateDelegationTokenOptionsInner {
    fn new(options: CreateDelegationTokenOptions) -> Self {
        let owner = options.owner().cloned().map(KafkaPrincipalInner::new);
        Self { options, owner }
    }

    /// Replaces the options, refreshing the cached owner.
    fn replace(&mut self, update: impl FnOnce(CreateDelegationTokenOptions) -> CreateDelegationTokenOptions) {
        *self = Self::new(update(std::mem::take(&mut self.options)));
    }
}

unsafe fn inner_ref<'a>(
    options: *const kafka_admin_CreateDelegationTokenOptions_t,
) -> &'a CreateDelegationTokenOptionsInner {
    unsafe { &*(options as *const CreateDelegationTokenOptionsInner) }
}

/// Mutable access to the handle's state, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle and no other reference to it may be live.
unsafe fn inner_mut<'a>(
    options: *mut kafka_admin_CreateDelegationTokenOptions_t,
) -> &'a mut CreateDelegationTokenOptionsInner {
    unsafe { &mut *(options as *mut CreateDelegationTokenOptionsInner) }
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle.
pub(crate) unsafe fn create_delegation_token_options_ref<'a>(
    options: *const kafka_admin_CreateDelegationTokenOptions_t,
) -> &'a CreateDelegationTokenOptions {
    &unsafe { inner_ref(options) }.options
}

fn boxed(options: CreateDelegationTokenOptions) -> *mut kafka_admin_CreateDelegationTokenOptions_t {
    Box::into_raw(Box::new(CreateDelegationTokenOptionsInner::new(options)))
        as *mut kafka_admin_CreateDelegationTokenOptions_t
}

/// Frees a `kafka_common_security_auth_KafkaPrincipal_t *` element of an
/// owned list.
unsafe fn destroy_kafka_principal_element(element: *mut c_void) {
    unsafe {
        kafka_common_security_auth_KafkaPrincipal_destroy(element as *mut kafka_common_security_auth_KafkaPrincipal_t)
    }
}

/// `new CreateDelegationTokenOptions()`: the server-default lifetime, no
/// renewers, the request principal as owner, the client's default API
/// timeout. Owned, freed with [`kafka_admin_CreateDelegationTokenOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_CreateDelegationTokenOptions_new() -> *mut kafka_admin_CreateDelegationTokenOptions_t {
    boxed(CreateDelegationTokenOptions::new())
}

/// `CreateDelegationTokenOptions.renewers(List<KafkaPrincipal> renewers)`:
/// `renewers` is a borrowed `kafka_List_t` of borrowed
/// `kafka_common_security_auth_KafkaPrincipal_t *`, copied during the call
/// (`NULL` reads as an empty list).
///
/// # Safety
///
/// `self_` must be a live handle; `renewers` must be `NULL` or a list of
/// live principal handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_set_renewers(
    self_: *mut kafka_admin_CreateDelegationTokenOptions_t,
    renewers: *const kafka_List_t,
) {
    let renewers = unsafe { list_kafka_principals(renewers) };
    unsafe { inner_mut(self_) }.replace(|options| options.set_renewers(renewers));
}

/// `CreateDelegationTokenOptions.renewers()`: an owned `kafka_List_t` of
/// owned `kafka_common_security_auth_KafkaPrincipal_t *` in the order they
/// were set, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_renewers(
    self_: *const kafka_admin_CreateDelegationTokenOptions_t,
) -> *mut kafka_List_t {
    let renewers = unsafe { create_delegation_token_options_ref(self_) }.renewers();
    box_list(
        renewers
            .iter()
            .map(|renewer| box_kafka_principal(renewer.clone()) as *mut c_void)
            .collect(),
        Some(destroy_kafka_principal_element),
    )
}

/// `CreateDelegationTokenOptions.owner(KafkaPrincipal owner)`: `owner` is
/// borrowed and copied during the call.
///
/// # Safety
///
/// `self_` must be a live handle; `owner` must be a live principal handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_set_owner(
    self_: *mut kafka_admin_CreateDelegationTokenOptions_t,
    owner: *const kafka_common_security_auth_KafkaPrincipal_t,
) {
    let owner = unsafe { kafka_principal_ref(owner) }.clone();
    unsafe { inner_mut(self_) }.replace(|options| options.set_owner(owner));
}

/// `CreateDelegationTokenOptions.owner()`: a borrowed principal valid until
/// the handle is destroyed or the owner is set again, `NULL` when unset
/// (Java's empty `Optional`: the request principal owns the token).
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_owner(
    self_: *const kafka_admin_CreateDelegationTokenOptions_t,
) -> *const kafka_common_security_auth_KafkaPrincipal_t {
    unsafe { inner_ref(self_) }
        .owner
        .as_ref()
        .map_or(ptr::null(), KafkaPrincipalInner::as_ptr)
}

/// `CreateDelegationTokenOptions.maxlifeTimeMs(long maxLifeTimeMs)`: the
/// maximum lifetime of the token in milliseconds, `-1` for the server
/// default.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_set_max_lifetime_ms(
    self_: *mut kafka_admin_CreateDelegationTokenOptions_t,
    max_lifetime_ms: i64,
) {
    unsafe { inner_mut(self_) }.replace(|options| options.set_max_lifetime_ms(max_lifetime_ms));
}

/// `CreateDelegationTokenOptions.maxlifeTimeMs()`: the maximum lifetime of
/// the token in milliseconds.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_max_lifetime_ms(
    self_: *const kafka_admin_CreateDelegationTokenOptions_t,
) -> i64 {
    unsafe { create_delegation_token_options_ref(self_) }.max_lifetime_ms()
}

/// `AbstractOptions.timeoutMs(Integer timeoutMs)`: `-1` (any negative value)
/// stands for Java's `null`, the client's default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_set_timeout_ms(
    self_: *mut kafka_admin_CreateDelegationTokenOptions_t,
    timeout_ms: i32,
) {
    unsafe { inner_mut(self_) }.replace(|options| options.set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms)));
}

/// `AbstractOptions.timeoutMs()`: the timeout in milliseconds, `-1` when the
/// client's default API timeout applies.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_timeout_ms(
    self_: *const kafka_admin_CreateDelegationTokenOptions_t,
) -> i32 {
    unsafe { create_delegation_token_options_ref(self_) }.timeout_ms().unwrap_or(-1)
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenOptions_destroy(
    self_: *mut kafka_admin_CreateDelegationTokenOptions_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut CreateDelegationTokenOptionsInner) });
    }
}
