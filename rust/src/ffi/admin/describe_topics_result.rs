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
//! The Rust result is an enum keyed either by topic id or by topic name, so
//! besides the Java accessors C gets the `kafka_admin_DescribeTopicsResult_e`
//! discriminator, `__enum`, and one factory per variant
//! (`by_topic_id` / `by_topic_name`, Java's `ofTopicIds` / `ofTopicNames`).
//! The accessors of the other variant answer `NULL`, as the Rust ones answer
//! `None`.

#![expect(non_camel_case_types)]

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};

use crate::admin::{DescribeTopicsResult, TopicDescription};
use crate::common::{KafkaFuture, Uuid};
use crate::ffi::admin::topic_description::{
    box_topic_description, destroy_topic_description_element, kafka_admin_TopicDescription_t, topic_description_ref,
};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::take_error;
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_t, uuid_of};
use crate::ffi::kafka_future::{kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_t};
use crate::ffi::util::{box_map, box_string_keyed_map, c_str_to_string, destroy_boxed, kafka_Map_t, map_entries};

/// Opaque handle to a [`DescribeTopicsResult`], owned and freed with
/// [`kafka_admin_DescribeTopicsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeTopicsResult_t {
    _private: [u8; 0],
}

/// The variants of [`DescribeTopicsResult`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_DescribeTopicsResult_e {
    by_topic_id,
    by_topic_name,
}

/// Exhaustive, so a variant Java adds fails to compile until it has its C
/// enumerator.
fn enum_of(result: &DescribeTopicsResult) -> kafka_admin_DescribeTopicsResult_e {
    match result {
        DescribeTopicsResult::ByTopicId(_) => kafka_admin_DescribeTopicsResult_e::by_topic_id,
        DescribeTopicsResult::ByTopicName(_) => kafka_admin_DescribeTopicsResult_e::by_topic_name,
    }
}

/// Compares two `kafka_common_Uuid_t *` keys by value, for `kafka_Map_get`.
unsafe fn uuid_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    unsafe { uuid_of(a as *const kafka_common_Uuid_t) == uuid_of(b as *const kafka_common_Uuid_t) }
}

/// Boxes a topic id as an owned `kafka_common_Uuid_t *` map key.
fn box_uuid_key(id: &Uuid) -> *mut c_void {
    box_uuid(*id) as *mut c_void
}

/// The owned future a per-topic `KafkaFuture<TopicDescription>` becomes:
/// `get` delivers a `kafka_admin_TopicDescription_t *` owned by the future.
fn description_future(ctx: &FutureCtx, future: &KafkaFuture<TopicDescription>) -> *mut kafka_common_KafkaFuture_t {
    ctx.handle_future(
        future,
        |d| box_topic_description(d) as *mut c_void,
        destroy_topic_description_element,
    )
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

/// Reads back a C future whose value is a `kafka_admin_TopicDescription_t *`
/// as a completed Rust future: `get` is called on it, so a future that is
/// still pending blocks until it resolves, and a failed one becomes a
/// future failed with the same error.
unsafe fn resolved_description(future: *const kafka_common_KafkaFuture_t) -> KafkaFuture<TopicDescription> {
    let mut value: *mut c_void = std::ptr::null_mut();
    let error = unsafe { kafka_common_KafkaFuture_get(future, &mut value) };
    match unsafe { take_error(error) } {
        Some(error) => KafkaFuture::completed_future(Err(error)),
        None => KafkaFuture::completed_future(Ok(unsafe {
            topic_description_ref(value as *const kafka_admin_TopicDescription_t)
        }
        .clone())),
    }
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_topics_result(
    result: DescribeTopicsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_DescribeTopicsResult_t) -> &'a ResultHandle<DescribeTopicsResult> {
    unsafe { result_ref(self_) }
}

/// `DescribeTopicsResult.ofTopicIds(Map<Uuid, KafkaFuture<TopicDescription>>)`:
/// builds the by-id variant from a borrowed map of
/// `const kafka_common_Uuid_t *` keys to `kafka_common_KafkaFuture_t *`
/// values whose value is a `kafka_admin_TopicDescription_t *` (such as one
/// from `kafka_common_KafkaFuture_completed_future`). A Rust future cannot
/// be built from an arbitrary C one, so each value is resolved with `get`
/// during the call — a pending future blocks until it resolves — and copied
/// into a completed future; a failed one keeps its error. Null reads as
/// empty. Owned, freed with [`kafka_admin_DescribeTopicsResult_destroy`];
/// the futures it hands out belong to no client.
///
/// # Safety
///
/// `value` must be null or a valid map whose keys are uuid handles and whose
/// values are future handles delivering topic-description handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_by_topic_id(
    value: *const kafka_Map_t,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    let futures: HashMap<Uuid, KafkaFuture<TopicDescription>> = unsafe { map_entries(value) }
        .iter()
        .map(|&(k, v)| {
            (unsafe { uuid_of(k as *const kafka_common_Uuid_t) }, unsafe {
                resolved_description(v as *const kafka_common_KafkaFuture_t)
            })
        })
        .collect();
    box_result(DescribeTopicsResult::of_topic_ids(futures), &FutureCtx::detached())
}

/// `DescribeTopicsResult.ofTopicNames(Map<String, KafkaFuture<TopicDescription>>)`:
/// builds the by-name variant from a borrowed map of `const char *` keys to
/// `kafka_common_KafkaFuture_t *` values, read exactly as in
/// [`kafka_admin_DescribeTopicsResult_by_topic_id`]. Owned, freed with
/// [`kafka_admin_DescribeTopicsResult_destroy`].
///
/// # Safety
///
/// `value` must be null or a valid map whose keys are NUL-terminated strings
/// and whose values are future handles delivering topic-description handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_by_topic_name(
    value: *const kafka_Map_t,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    let futures: HashMap<String, KafkaFuture<TopicDescription>> = unsafe { map_entries(value) }
        .iter()
        .map(|&(k, v)| {
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                resolved_description(v as *const kafka_common_KafkaFuture_t)
            })
        })
        .collect();
    box_result(DescribeTopicsResult::of_topic_names(futures), &FutureCtx::detached())
}

/// The variant of a result, for a `switch`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult__enum(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> kafka_admin_DescribeTopicsResult_e {
    enum_of(&unsafe { handle(self_) }.result)
}

/// `topicIdValues()`: for a by-id result, an owned `kafka_Map_t` (freed with
/// `kafka_Map_destroy`, which frees its keys and values) of owned
/// `kafka_common_Uuid_t *` keys, ordered by their string form and compared
/// by value in `kafka_Map_get`, to owned `kafka_common_KafkaFuture_t *`
/// whose `get` delivers a `kafka_admin_TopicDescription_t *` owned by the
/// future; `NULL` for a by-name result.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_topic_id_values(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_Map_t {
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

/// `topicNameValues()`: for a by-name result, an owned `kafka_Map_t` (freed
/// with `kafka_Map_destroy`, which frees its keys and values) of owned
/// `char *` topic names, sorted, to owned `kafka_common_KafkaFuture_t *`
/// whose `get` delivers a `kafka_admin_TopicDescription_t *` owned by the
/// future; `NULL` for a by-id result.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_topic_name_values(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    match h.result.topic_name_values() {
        Some(map) => h.ctx.string_keyed_future_map(map.iter(), description_future),
        None => std::ptr::null_mut(),
    }
}

/// `allTopicNames()`: for a by-name result, an owned future freed with
/// `kafka_common_KafkaFuture_destroy` whose `get` delivers a `kafka_Map_t *`
/// (owned by the future) of `char *` topic names, sorted, to
/// `kafka_admin_TopicDescription_t *`; `NULL` for a by-id result.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_all_topic_names(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    match h.result.all_topic_names() {
        Some(future) => h.ctx.handle_future(&future, names_map, destroy_map_element),
        None => std::ptr::null_mut(),
    }
}

/// `allTopicIds()`: for a by-id result, an owned future freed with
/// `kafka_common_KafkaFuture_destroy` whose `get` delivers a `kafka_Map_t *`
/// (owned by the future) of `kafka_common_Uuid_t *` keys, ordered by their
/// string form and compared by value in `kafka_Map_get`, to
/// `kafka_admin_TopicDescription_t *`; `NULL` for a by-name result.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_all_topic_ids(
    self_: *const kafka_admin_DescribeTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    match h.result.all_topic_ids() {
        Some(future) => h.ctx.handle_future(&future, ids_map, destroy_map_element),
        None => std::ptr::null_mut(),
    }
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTopicsResult_destroy(self_: *mut kafka_admin_DescribeTopicsResult_t) {
    unsafe { destroy_result::<DescribeTopicsResult, _>(self_) }
}
