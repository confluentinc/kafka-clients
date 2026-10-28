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
use crate::ffi::ffi_guard;
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
        // SAFETY: `expiry_timestamp` is the owned future handle
        // `box_expire_delegation_token_result` built with `value_future`. The accessor only lends
        // it out, and `Held` is dropped once, with the result, so the handle is not yet destroyed,
        // as `kafka_common_KafkaFuture_destroy` requires, and is destroyed exactly here.
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

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_ExpireDelegationTokenResult_t) -> &'a ResultHandle<Held> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_ExpireDelegationTokenResult_t` is built by `box_result::<Held, _>`, as
    // `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// Returns a future which yields expiry timestamp
///
/// # Returns
///
/// A future. Its `get` delivers an `int64_t *` (owned by the future): the
/// new expiry time in milliseconds.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is borrowed from `self_`, valid until it is destroyed, and is never passed
/// to `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(
    self_: *const kafka_admin_ExpireDelegationTokenResult_t,
) -> *const kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    unsafe { handle(self_) }.result.expiry_timestamp
}

/// Frees a result handle, and with it the future `expiry_timestamp` borrows
/// out; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ExpireDelegationTokenResult_destroy(
    self_: *mut kafka_admin_ExpireDelegationTokenResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_ExpireDelegationTokenResult_t` is built by `box_result::<Held, _>`, as
    // `destroy_result` requires.
    unsafe { destroy_result::<Held, _>(self_) }
}
