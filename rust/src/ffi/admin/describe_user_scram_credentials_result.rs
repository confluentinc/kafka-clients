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

unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
) -> &'a ResultHandle<DescribeUserScramCredentialsResult> {
    unsafe { result_ref(self_) }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_Map_t *` (owned by the future) of `char *`
/// user names, sorted, to `kafka_admin_UserScramCredentialsDescription_t *`,
/// or fails with the first user-level error other than
/// `RESOURCE_NOT_FOUND`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_all(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.all(), descriptions_map, destroy_map_element)
}

/// `users()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of `char *`
/// user names, in the order the broker returned them, skipping the users
/// that were not found.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_users(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.users(),
        |users: Vec<String>| box_string_list(users) as *mut c_void,
        destroy_list_element,
    )
}

/// `description(String userName)`: an owned future freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a
/// `kafka_admin_UserScramCredentialsDescription_t *` owned by the future, or
/// fails with `RESOURCE_NOT_FOUND` when the user was not part of the
/// request or has no credentials, or with the user's own error.
///
/// # Safety
///
/// `self_` must be a live result handle and `user_name` a valid
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_description(
    self_: *const kafka_admin_DescribeUserScramCredentialsResult_t,
    user_name: *const c_char,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    let user_name = unsafe { c_str_to_string(user_name) };
    h.ctx.handle_future(
        &h.result.description(&user_name),
        |d| box_user_scram_credentials_description(d) as *mut c_void,
        destroy_user_scram_credentials_description_element,
    )
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeUserScramCredentialsResult_destroy(
    self_: *mut kafka_admin_DescribeUserScramCredentialsResult_t,
) {
    unsafe { destroy_result::<DescribeUserScramCredentialsResult, _>(self_) }
}
