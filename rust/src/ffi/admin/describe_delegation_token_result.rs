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

//! `kafka_admin_DescribeDelegationTokenResult_t`:
//! `org.apache.kafka.clients.admin.DescribeDelegationTokenResult` (CLAUDE.md
//! §4). The Rust getter borrows its future out of the result, so the C
//! getter returns a borrowed future handle that lives as long as the result.

use std::ffi::c_void;

use crate::admin::DescribeDelegationTokenResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_result, result_ref};
use crate::ffi::common::security::token::delegation::delegation_token::{
    box_delegation_token, kafka_common_security_token_delegation_DelegationToken_destroy,
    kafka_common_security_token_delegation_DelegationToken_t,
};
use crate::ffi::kafka_future::{kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_t};
use crate::ffi::util::box_list;

/// Opaque handle to a [`DescribeDelegationTokenResult`], owned and freed
/// with [`kafka_admin_DescribeDelegationTokenResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeDelegationTokenResult_t {
    _private: [u8; 0],
}

/// What the handle holds: the future `delegationTokens()` borrows out,
/// built once when the result is handed to C and freed with the result.
struct Held {
    delegation_tokens: *mut kafka_common_KafkaFuture_t,
}

impl Drop for Held {
    fn drop(&mut self) {
        unsafe { kafka_common_KafkaFuture_destroy(self.delegation_tokens) }
    }
}

/// Frees a `kafka_common_security_token_delegation_DelegationToken_t *`
/// element of an owned list.
unsafe fn destroy_delegation_token_element(element: *mut c_void) {
    unsafe {
        kafka_common_security_token_delegation_DelegationToken_destroy(
            element as *mut kafka_common_security_token_delegation_DelegationToken_t,
        )
    }
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_delegation_token_result(
    result: DescribeDelegationTokenResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeDelegationTokenResult_t {
    let delegation_tokens = ctx.handle_future(
        result.delegation_tokens(),
        |tokens| {
            let elements = tokens.into_iter().map(|t| box_delegation_token(t) as *mut c_void).collect();
            box_list(elements, Some(destroy_delegation_token_element)) as *mut c_void
        },
        destroy_list_element,
    );
    box_result(Held { delegation_tokens }, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_DescribeDelegationTokenResult_t) -> &'a ResultHandle<Held> {
    unsafe { result_ref(self_) }
}

/// `delegationTokens()`: a borrowed future, valid until the result is
/// destroyed and never passed to `kafka_common_KafkaFuture_destroy`. Its
/// `get` delivers a `kafka_List_t *` (owned by the future) of
/// `kafka_common_security_token_delegation_DelegationToken_t *`, in the
/// order the broker returned them.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenResult_delegation_tokens(
    self_: *const kafka_admin_DescribeDelegationTokenResult_t,
) -> *const kafka_common_KafkaFuture_t {
    unsafe { handle(self_) }.result.delegation_tokens
}

/// Frees a result handle, and with it the future `delegation_tokens`
/// borrows out; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeDelegationTokenResult_destroy(
    self_: *mut kafka_admin_DescribeDelegationTokenResult_t,
) {
    unsafe { destroy_result::<Held, _>(self_) }
}
