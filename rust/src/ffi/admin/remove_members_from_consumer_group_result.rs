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

unsafe fn handle<'a>(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) -> &'a ResultHandle<RemoveMembersFromConsumerGroupResult> {
    unsafe { result_ref(self_) }
}

/// `all()`: an owned `KafkaFuture<Void>` freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers `NULL` once every
/// requested member has been removed, or the first member's error.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_all(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) -> *mut kafka_common_KafkaFuture_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(
    self_: *const kafka_admin_RemoveMembersFromConsumerGroupResult_t,
    member: *const kafka_admin_MemberToRemove_t,
    out_member_result: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let member = unsafe { member_to_remove_ref(member) };
    unsafe { out_slot(h.result.member_result(member), out_member_result, |f| h.ctx.void_future(&f)) }
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(
    self_: *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t,
) {
    unsafe { destroy_result::<RemoveMembersFromConsumerGroupResult, _>(self_) }
}
