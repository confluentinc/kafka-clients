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

//! C bindings for `org.apache.kafka.clients.admin.CreateDelegationTokenResult`.

use std::ffi::c_void;

use crate::admin::CreateDelegationTokenResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::security::token::delegation::delegation_token::{
    box_delegation_token, kafka_common_security_token_delegation_DelegationToken_destroy,
    kafka_common_security_token_delegation_DelegationToken_t,
};
use crate::ffi::kafka_future::{kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_t};

/// Opaque handle to a [`CreateDelegationTokenResult`], owned by the caller
/// and freed with [`kafka_admin_CreateDelegationTokenResult_destroy`].
#[repr(C)]
pub struct kafka_admin_CreateDelegationTokenResult_t {
    _private: [u8; 0],
}

/// What the handle holds: Java's `delegationToken()` returns the result's
/// one future itself, so the C future is built once and borrowed out, owned
/// by the handle (the Rust future lives on inside it).
struct CreateDelegationTokenResultInner {
    delegation_token: *mut kafka_common_KafkaFuture_t,
}

impl Drop for CreateDelegationTokenResultInner {
    fn drop(&mut self) {
        // SAFETY: `delegation_token` is the owned future handle built in
        // `box_create_delegation_token_result`, destroyed exactly here.
        unsafe { kafka_common_KafkaFuture_destroy(self.delegation_token) }
    }
}

/// Frees a `kafka_common_security_token_delegation_DelegationToken_t *` a
/// future delivered.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_security_token_delegation_DelegationToken_t *`.
unsafe fn destroy_delegation_token_element(element: *mut c_void) {
    unsafe {
        kafka_common_security_token_delegation_DelegationToken_destroy(
            element as *mut kafka_common_security_token_delegation_DelegationToken_t,
        )
    }
}

/// Hands `result` to C, binding the future it exposes to `ctx`.
pub(crate) fn box_create_delegation_token_result(
    result: CreateDelegationTokenResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_CreateDelegationTokenResult_t {
    let delegation_token = ctx.handle_future(
        result.delegation_token(),
        |token| box_delegation_token(token) as *mut c_void,
        destroy_delegation_token_element,
    );
    box_result(CreateDelegationTokenResultInner { delegation_token }, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_CreateDelegationTokenResult_t,
) -> &'a ResultHandle<CreateDelegationTokenResultInner> {
    unsafe { result_ref(self_) }
}

/// `CreateDelegationTokenResult.delegationToken()`: the future of the
/// created token, borrowed from the result handle (valid until it is
/// destroyed, never passed to `kafka_common_KafkaFuture_destroy`). Its `get`
/// delivers a `kafka_common_security_token_delegation_DelegationToken_t *`
/// owned by the future.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenResult_delegation_token(
    self_: *const kafka_admin_CreateDelegationTokenResult_t,
) -> *const kafka_common_KafkaFuture_t {
    unsafe { handle(self_) }.result.delegation_token
}

/// Frees a result handle and the future borrowed from it; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenResult_destroy(
    self_: *mut kafka_admin_CreateDelegationTokenResult_t,
) {
    unsafe { destroy_result::<CreateDelegationTokenResultInner, _>(self_) }
}
