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

//! `kafka_admin_DescribeUserScramCredentialsResult_t`:
//! `org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult`
//! (CLAUDE.md §4).

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};

use crate::admin::{DescribeUserScramCredentialsResult, UserScramCredentialsDescription};
use crate::ffi::admin::user_scram_credentials_description::{
    box_user_scram_credentials_description, destroy_user_scram_credentials_description_element,
};
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_map_element, destroy_result, result_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_string_keyed_map, box_string_list, c_str_to_string};

/// Opaque handle to a [`DescribeUserScramCredentialsResult`], owned and
/// freed with [`kafka_admin_DescribeUserScramCredentialsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeUserScramCredentialsResult_t {
    _private: [u8; 0],
}

/// Java's `Map<String, UserScramCredentialsDescription>` as an owned map of
/// owned `char *` user names, sorted, to owned
/// `kafka_admin_UserScramCredentialsDescription_t *`.
fn descriptions_map(map: HashMap<String, UserScramCredentialsDescription>) -> *mut c_void {
    let sorted: BTreeMap<String, UserScramCredentialsDescription> = map.into_iter().collect();
    box_string_keyed_map(
        sorted
            .into_iter()
            .map(|(user, d)| (user, box_user_scram_credentials_description(d) as *mut c_void)),
        Some(destroy_user_scram_credentials_description_element),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_user_scram_credentials_result(
    result: DescribeUserScramCredentialsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeUserScramCredentialsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
) -> &'a ResultHandle<DescribeUserScramCredentialsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeUserScramCredentialsResult_t` is built by
    // `box_result::<DescribeUserScramCredentialsResult, _>` (in
    // `box_describe_user_scram_credentials_result`), as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// # Returns
///
/// A future for the results of all described users with map keys (one per user) being consistent
/// with the contents of the list returned by
/// `kafka_admin_DescribeUserScramCredentialsResult_users`. The future will complete successfully
/// only if all such user descriptions complete successfully.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned and freed with `kafka_common_KafkaFuture_destroy`; its `get`
/// delivers a `kafka_Map_t *` (owned by the future) of `char *` user names, sorted, to
/// `kafka_admin_UserScramCredentialsDescription_t *`, or fails with the first user-level error
/// other than `RESOURCE_NOT_FOUND`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_all(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `descriptions_map` makes through
    // `box_string_keyed_map`, and the handle holds only owned data and no thread-bound state, so it
    // may be freed from any thread and read from several at once, as `handle_future` requires.
    unsafe { h.ctx.handle_future(&h.result.all(), descriptions_map, destroy_map_element) }
}

/// `users()`: a future; its `get` delivers a `kafka_List_t *` (owned by the future) of `char *`
/// user names, in the order the broker returned them, skipping the users that were not found.
///
/// # Returns
///
/// A future indicating the distinct users that meet the request criteria and that have at least one
/// credential. The future will not complete successfully if the user is not authorized to perform
/// the describe operation; otherwise, it will complete successfully as long as the list of users
/// with credentials can be successfully determined within some hard-coded timeout period. Note that
/// the returned list will not include users that do not exist/have no credentials: a request to
/// describe an explicit list of users, none of which existed/had a credential, will result in a
/// future that returns an empty list being returned here. A returned list will include users that
/// have a credential but that could not be described.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_users(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_list_element` frees exactly the list `box_string_list` makes through
    // `box_list`, and the handle holds only owned data and no thread-bound state, so it may be
    // freed from any thread and read from several at once, as `handle_future` requires.
    unsafe {
        h.ctx.handle_future(
            &h.result.users(),
            |users: Vec<String>| box_string_list(users) as *mut c_void,
            destroy_list_element,
        )
    }
}

/// # Parameters
///
/// - `user_name` - the name of the user description being requested
///
/// # Returns
///
/// A future indicating the description results for the given user. The future will complete with an
/// error if the future returned by `kafka_admin_DescribeUserScramCredentialsResult_users` completes
/// with an error. Note that if the given user does not exist in the list of described users then
/// the returned future will complete with an error answering
/// `kafka_common_Error_is_resource_not_found_error`.
///
/// # Safety
///
/// `self_` must be a live result handle and `user_name` a valid
/// NUL-terminated string.
///
/// The returned future is owned and freed with `kafka_common_KafkaFuture_destroy`; its `get`
/// delivers a `kafka_admin_UserScramCredentialsDescription_t *` owned by the future, or fails with
/// `RESOURCE_NOT_FOUND` when the user was not part of the request or has no credentials, or with
/// the user's own error.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_description(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
    user_name: *const c_char,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: Per this function's `# Safety`, `user_name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into a `String` during the call.
    let user_name = unsafe { c_str_to_string(user_name) };
    // SAFETY: `destroy_user_scram_credentials_description_element` frees exactly the handle
    // `box_user_scram_credentials_description` makes, and the handle holds only owned data and no
    // thread-bound state, so it may be freed from any thread and read from several at once, as
    // `handle_future` requires.
    unsafe {
        h.ctx.handle_future(
            &h.result.description(&user_name),
            |d| box_user_scram_credentials_description(d) as *mut c_void,
            destroy_user_scram_credentials_description_element,
        )
    }
}

/// Frees a result handle; `NULL` is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_destroy(
    self_: *mut kafka_admin_DescribeUserScramCredentialsResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeUserScramCredentialsResult_t` is built by
    // `box_result::<DescribeUserScramCredentialsResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<DescribeUserScramCredentialsResult, _>(self_) }
}
