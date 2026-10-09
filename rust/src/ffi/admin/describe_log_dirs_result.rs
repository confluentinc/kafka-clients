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

//! `kafka_admin_DescribeLogDirsResult_t`:
//! `org.apache.kafka.clients.admin.DescribeLogDirsResult` (CLAUDE.md §4).
//! Broker ids cross as boxed `int32_t *` map keys, compared by value.

use std::collections::{BTreeMap, HashMap};
use std::ffi::c_void;

use crate::admin::{DescribeLogDirsResult, LogDirDescription};
use crate::common::KafkaFuture;
use crate::ffi::admin::log_dir_description::{box_log_dir_description, destroy_log_dir_description_element};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_map, box_string_keyed_map, destroy_boxed, kafka_Map_t};

/// Opaque handle to a [`DescribeLogDirsResult`], owned and freed with
/// [`kafka_admin_DescribeLogDirsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeLogDirsResult_t {
    _private: [u8; 0],
}

/// Boxes a broker id as an owned `int32_t *` map key.
fn box_i32(broker_id: &i32) -> *mut c_void {
    Box::into_raw(Box::new(*broker_id)) as *mut c_void
}

/// Compares two `int32_t *` keys by value, for `kafka_Map_get`.
unsafe fn i32_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    unsafe { *(a as *const i32) == *(b as *const i32) }
}

/// Java's `Map<String, LogDirDescription>` as an owned map of owned `char *`
/// log-dir paths, sorted, to owned `kafka_admin_LogDirDescription_t *`.
fn log_dir_map(map: HashMap<String, LogDirDescription>) -> *mut c_void {
    let sorted: BTreeMap<String, LogDirDescription> = map.into_iter().collect();
    box_string_keyed_map(
        sorted
            .into_iter()
            .map(|(path, d)| (path, box_log_dir_description(d) as *mut c_void)),
        Some(destroy_log_dir_description_element),
    ) as *mut c_void
}

/// Java's `Map<Integer, Map<String, LogDirDescription>>` as an owned map of
/// owned `int32_t *` broker ids, sorted and compared by value, to the owned
/// `kafka_Map_t *` built by [`log_dir_map`].
fn all_descriptions_map(map: HashMap<i32, HashMap<String, LogDirDescription>>) -> *mut c_void {
    let sorted: BTreeMap<i32, HashMap<String, LogDirDescription>> = map.into_iter().collect();
    let entries = sorted
        .into_iter()
        .map(|(broker_id, dirs)| (box_i32(&broker_id), log_dir_map(dirs)))
        .collect();
    box_map(entries, Some(destroy_boxed::<i32>), Some(destroy_map_element), Some(i32_key_eq)) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_log_dirs_result(
    result: DescribeLogDirsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeLogDirsResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_DescribeLogDirsResult_t) -> &'a ResultHandle<DescribeLogDirsResult> {
    unsafe { result_ref(self_) }
}

/// `descriptions()`: an owned `kafka_Map_t` (freed with `kafka_Map_destroy`,
/// which frees its keys and values) of owned `int32_t *` broker ids, sorted
/// and compared by value in `kafka_Map_get`, to owned
/// `kafka_common_KafkaFuture_t *` whose `get` delivers a `kafka_Map_t *`
/// (owned by the future) of `char *` log-dir paths, sorted, to
/// `kafka_admin_LogDirDescription_t *`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_descriptions(
    self_: *const kafka_admin_DescribeLogDirsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let sorted: BTreeMap<&i32, &KafkaFuture<HashMap<String, LogDirDescription>>> =
        h.result.descriptions().iter().collect();
    h.ctx
        .keyed_future_map(sorted, box_i32, destroy_boxed::<i32>, i32_key_eq, |ctx, f| {
            ctx.handle_future(f, log_dir_map, destroy_map_element)
        })
}

/// `allDescriptions()`: an owned future freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a `kafka_Map_t *`
/// (owned by the future) of `int32_t *` broker ids, sorted and compared by
/// value in `kafka_Map_get`, to the per-broker `kafka_Map_t *` described in
/// [`kafka_admin_DescribeLogDirsResult_descriptions`].
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_all_descriptions(
    self_: *const kafka_admin_DescribeLogDirsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx
        .handle_future(&h.result.all_descriptions(), all_descriptions_map, destroy_map_element)
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeLogDirsResult_destroy(self_: *mut kafka_admin_DescribeLogDirsResult_t) {
    unsafe { destroy_result::<DescribeLogDirsResult, _>(self_) }
}
