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

//! `kafka_admin_ExpireDelegationTokenResult_t`:
//! `org.apache.kafka.clients.admin.ExpireDelegationTokenResult` (CLAUDE.md
//! §4). The Rust getter borrows its future out of the result, so the C
//! getter returns a borrowed future handle that lives as long as the result.

use crate::admin::ExpireDelegationTokenResult;
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::kafka_future::{kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_t};

/// Opaque handle to an [`ExpireDelegationTokenResult`], owned and freed
/// with [`kafka_admin_ExpireDelegationTokenResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ExpireDelegationTokenResult_t {
    _private: [u8; 0],
}

/// What the handle holds: the future `expiryTimestamp()` borrows out, built
/// once when the result is handed to C and freed with the result.
struct Held {
    expiry_timestamp: *mut kafka_common_KafkaFuture_t,
}

impl Drop for Held {
    fn drop(&mut self) {
        unsafe { kafka_common_KafkaFuture_destroy(self.expiry_timestamp) }
    }
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_expire_delegation_token_result(
    result: ExpireDelegationTokenResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ExpireDelegationTokenResult_t {
    let expiry_timestamp = ctx.value_future(result.expiry_timestamp());
    box_result(Held { expiry_timestamp }, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_ExpireDelegationTokenResult_t) -> &'a ResultHandle<Held> {
    unsafe { result_ref(self_) }
}

/// `expiryTimestamp()`: a borrowed future, valid until the result is
/// destroyed and never passed to `kafka_common_KafkaFuture_destroy`. Its
/// `get` delivers an `int64_t *` (owned by the future): the new expiry time
/// in milliseconds.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(
    self_: *const kafka_admin_ExpireDelegationTokenResult_t,
) -> *const kafka_common_KafkaFuture_t {
    unsafe { handle(self_) }.result.expiry_timestamp
}

/// Frees a result handle, and with it the future `expiry_timestamp` borrows
/// out; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ExpireDelegationTokenResult_destroy(
    self_: *mut kafka_admin_ExpireDelegationTokenResult_t,
) {
    unsafe { destroy_result::<Held, _>(self_) }
}
