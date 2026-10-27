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
use crate::ffi::ffi_guard;
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
/// `element` must be an owned delegation-token handle not yet destroyed.
unsafe fn destroy_delegation_token_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned delegation-token handle not yet
    // destroyed, as `kafka_common_security_token_delegation_DelegationToken_destroy` requires.
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
    // SAFETY: `destroy_delegation_token_element` frees exactly the handle `box_delegation_token`
    // makes, and the handle holds only owned data and no thread-bound state, so it may be freed
    // from any thread and read from several at once, as `handle_future` requires.
    let delegation_token = unsafe {
        ctx.handle_future(
            result.delegation_token(),
            |token| box_delegation_token(token) as *mut c_void,
            destroy_delegation_token_element,
        )
    };
    box_result(CreateDelegationTokenResultInner { delegation_token }, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_CreateDelegationTokenResult_t,
) -> &'a ResultHandle<CreateDelegationTokenResultInner> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_CreateDelegationTokenResult_t` is built by
    // `box_result::<CreateDelegationTokenResultInner, _>`, as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// Returns a future which yields a delegation token
///
/// # Returns
///
/// The future of the created token. Its `get` delivers a
/// `kafka_common_security_token_delegation_DelegationToken_t *` owned by the future.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is borrowed from `self_`, valid until it is destroyed, and is never passed
/// to `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenResult_delegation_token(
    self_: *const kafka_admin_CreateDelegationTokenResult_t,
) -> *const kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    unsafe { handle(self_) }.result.delegation_token
}

/// Frees a result handle and the future borrowed from it; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateDelegationTokenResult_destroy(
    self_: *mut kafka_admin_CreateDelegationTokenResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_CreateDelegationTokenResult_t` is built by
    // `box_result::<CreateDelegationTokenResultInner, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<CreateDelegationTokenResultInner, _>(self_) }
}
