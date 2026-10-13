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

//! `kafka_admin_RemoveMembersFromConsumerGroupResult_t`:
//! `org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult`
//! (CLAUDE.md §4).

use crate::admin::RemoveMembersFromConsumerGroupResult;
use crate::ffi::admin::member_to_remove::{kafka_admin_MemberToRemove_t, member_to_remove_ref};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, out_slot, result_ref};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;

/// Opaque handle to a [`RemoveMembersFromConsumerGroupResult`], owned and
/// freed with [`kafka_admin_RemoveMembersFromConsumerGroupResult_destroy`].
#[repr(C)]
pub struct kafka_admin_RemoveMembersFromConsumerGroupResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_remove_members_from_consumer_group_result(
    result: RemoveMembersFromConsumerGroupResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) -> &'a ResultHandle<RemoveMembersFromConsumerGroupResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_RemoveMembersFromConsumerGroupResult_t` is built by
    // `box_result::<RemoveMembersFromConsumerGroupResult, _>` (in
    // `box_remove_members_from_consumer_group_result`), as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// `all()`: an owned `KafkaFuture<Void>` freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers `NULL` once every
/// requested member has been removed, or the first member's error.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_all(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// `memberResult(MemberToRemove member)`: delivers through `out_member_result`
/// an owned `KafkaFuture<Void>` (freed with `kafka_common_KafkaFuture_destroy`,
/// `get` delivering `NULL` once that member has been removed), or returns
/// the owned `IllegalArgumentError` when the result is in
/// `removeAll` mode or `member` was not part of the request. `member` is
/// borrowed for the call.
///
/// # Safety
///
/// `self_` must be a live result handle, `member` a valid member-to-remove
/// handle and `out_member_result` a valid slot.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
    member: *const kafka_admin_MemberToRemove_t,
    out_member_result: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: Per this function's `# Safety`, `member` is a valid member-to-remove handle, as
    // `member_to_remove_ref` requires; the borrow ends with this call, during which the caller
    // keeps the handle alive.
    let member = unsafe { member_to_remove_ref(member) };
    // SAFETY: Per this function's `# Safety`, `out_member_result` is a valid slot, as `out_slot`
    // requires.
    unsafe { out_slot(h.result.member_result(member), out_member_result, |f| h.ctx.void_future(&f)) }
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_RemoveMembersFromConsumerGroupResult_t` is built by
    // `box_result::<RemoveMembersFromConsumerGroupResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<RemoveMembersFromConsumerGroupResult, _>(self_) }
}
