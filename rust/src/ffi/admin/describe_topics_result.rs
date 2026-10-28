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

//! `kafka_admin_DescribeTopicsResult_t`:
//! `org.apache.kafka.clients.admin.DescribeTopicsResult` (CLAUDE.md §4).
//!
//! A result is keyed by topic id or by topic name, never both: the accessors
//! for the other key answer `NULL`, as Java's answer `null` (the Rust ones
//! `None`). Java's constructor is protected and its `ofTopicIds` /
//! `ofTopicNames` factories package-private, so C gets no constructor.

use std::collections::{BTreeMap, HashMap};
use std::ffi::c_void;

use crate::admin::{DescribeTopicsResult, TopicDescription};
use crate::common::{KafkaFuture, Uuid};
use crate::ffi::admin::topic_description::{box_topic_description, destroy_topic_description_element};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_t, uuid_of};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_map, box_string_keyed_map, destroy_boxed, kafka_Map_t};

/// Opaque handle to a [`DescribeTopicsResult`], owned and freed with
/// [`kafka_admin_DescribeTopicsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeTopicsResult_t {
    _private: [u8; 0],
}

/// Compares two `kafka_common_Uuid_t *` keys by value, for `kafka_Map_get`.
///
/// # Safety
///
/// `a` and `b` must each be `NULL` or a valid uuid handle, alive for this
/// call.
unsafe fn uuid_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    // SAFETY: `a` and `b` are non-null (checked above) and, per this function's `# Safety`, valid
    // uuid handles, alive for this call, as `uuid_of` requires.
    unsafe { uuid_of(a as *const kafka_common_Uuid_t) == uuid_of(b as *const kafka_common_Uuid_t) }
}

/// Boxes a topic id as an owned `kafka_common_Uuid_t *` map key.
fn box_uuid_key(id: &Uuid) -> *mut c_void {
    box_uuid(*id) as *mut c_void
}

/// The owned future a per-topic `KafkaFuture<TopicDescription>` becomes:
/// `get` delivers a `kafka_admin_TopicDescription_t *` owned by the future.
fn description_future(ctx: &FutureCtx, future: &KafkaFuture<TopicDescription>) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: `destroy_topic_description_element` frees exactly the handle `box_topic_description`
    // makes, and the handle holds only owned data and no thread-bound state, so it may be freed
    // from any thread and read from several at once, as `handle_future` requires.
    unsafe {
        ctx.handle_future(
            future,
            |d| box_topic_description(d) as *mut c_void,
            destroy_topic_description_element,
        )
    }
}

/// Java's `Map<String, TopicDescription>` as an owned map of owned `char *`
/// topic names, sorted, to owned `kafka_admin_TopicDescription_t *`.
fn names_map(map: HashMap<String, TopicDescription>) -> *mut c_void {
    let sorted: BTreeMap<String, TopicDescription> = map.into_iter().collect();
    box_string_keyed_map(
        sorted
            .into_iter()
            .map(|(name, d)| (name, box_topic_description(d) as *mut c_void)),
        Some(destroy_topic_description_element),
    ) as *mut c_void
}

/// Java's `Map<Uuid, TopicDescription>` as an owned map of owned
/// `kafka_common_Uuid_t *` keys (compared by value, ordered by their string
/// form) to owned `kafka_admin_TopicDescription_t *`.
fn ids_map(map: HashMap<Uuid, TopicDescription>) -> *mut c_void {
    let mut entries: Vec<(Uuid, TopicDescription)> = map.into_iter().collect();
    entries.sort_by_cached_key(|(id, _)| id.to_string());
    let entries = entries
        .into_iter()
        .map(|(id, d)| (box_uuid_key(&id), box_topic_description(d) as *mut c_void))
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<Uuid>),
        Some(destroy_topic_description_element),
        Some(uuid_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_topics_result(
    result: DescribeTopicsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_DescribeTopicsResult_t) -> &'a ResultHandle<DescribeTopicsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeTopicsResult_t` is built by
    // `box_result::<DescribeTopicsResult, _>` (in `box_describe_topics_result`), as `result_ref`
    // requires.
    unsafe { result_ref(self_) }
}

/// Use when `kafka_admin_Admin_describe_topics_with_topics` used a TopicIdCollection
///
/// # Returns
///
/// A map from topic IDs to futures which can be used to check the status of individual topics if
/// the request used topic IDs, otherwise return `NULL`.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned map is, for a result keyed by topic id, owned (freed with `kafka_Map_destroy`,
/// which frees its keys and values), of owned `kafka_common_Uuid_t *` keys, ordered by their string
/// form and compared by value in `kafka_Map_get`, to owned `kafka_common_KafkaFuture_t *` whose
/// `get` delivers a `kafka_admin_TopicDescription_t *` owned by the future.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_topic_id_values(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    match h.result.topic_id_values() {
        Some(map) => {
            let mut entries: Vec<(&Uuid, &KafkaFuture<TopicDescription>)> = map.iter().collect();
            entries.sort_by_cached_key(|(id, _)| id.to_string());
            h.ctx
                .keyed_future_map(entries, box_uuid_key, destroy_boxed::<Uuid>, uuid_key_eq, description_future)
        },
        None => std::ptr::null_mut(),
    }
}

/// Use when `kafka_admin_Admin_describe_topics_with_topics` used a TopicNameCollection
///
/// # Returns
///
/// A map from topic names to futures which can be used to check the status of individual topics if
/// the request used topic names, otherwise return `NULL`.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned map is, for a result keyed by topic name, owned (freed with `kafka_Map_destroy`,
/// which frees its keys and values), of owned `char *` topic names, sorted, to owned
/// `kafka_common_KafkaFuture_t *` whose `get` delivers a `kafka_admin_TopicDescription_t *` owned
/// by the future.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_topic_name_values(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    match h.result.topic_name_values() {
        Some(map) => h.ctx.string_keyed_future_map(map.iter(), description_future),
        None => std::ptr::null_mut(),
    }
}

/// # Returns
///
/// A future map from topic names to descriptions which can be used to check the status of
/// individual description if the describe topic request used topic names, otherwise return `NULL`,
/// this request succeeds only if all the topic descriptions succeed
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is, for a result keyed by topic name, owned and freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a `kafka_Map_t *` (owned by the future)
/// of `char *` topic names, sorted, to `kafka_admin_TopicDescription_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_all_topic_names(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    match h.result.all_topic_names() {
        // SAFETY: `destroy_map_element` frees exactly the map `names_map` makes through
        // `box_string_keyed_map`, and the handle holds only owned data and no thread-bound state,
        // so it may be freed from any thread and read from several at once, as `handle_future`
        // requires.
        Some(future) => unsafe { h.ctx.handle_future(&future, names_map, destroy_map_element) },
        None => std::ptr::null_mut(),
    }
}

/// # Returns
///
/// A future map from topic ids to descriptions which can be used to check the status of individual
/// description if the describe topic request used topic ids, otherwise return `NULL`, this request
/// succeeds only if all the topic descriptions succeed
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is, for a result keyed by topic id, owned and freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a `kafka_Map_t *` (owned by the future)
/// of `kafka_common_Uuid_t *` keys, ordered by their string form and compared by value in
/// `kafka_Map_get`, to `kafka_admin_TopicDescription_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_all_topic_ids(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    match h.result.all_topic_ids() {
        // SAFETY: `destroy_map_element` frees exactly the map `ids_map` makes through `box_map`,
        // and the handle holds only owned data and no thread-bound state, so it may be freed from
        // any thread and read from several at once, as `handle_future` requires.
        Some(future) => unsafe { h.ctx.handle_future(&future, ids_map, destroy_map_element) },
        None => std::ptr::null_mut(),
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
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_destroy(self_: *mut kafka_admin_DescribeTopicsResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeTopicsResult_t` is built by `box_result::<DescribeTopicsResult, _>`, as
    // `destroy_result` requires.
    unsafe { destroy_result::<DescribeTopicsResult, _>(self_) }
}
