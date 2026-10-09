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

//! `kafka_admin_ListTopicsResult_t`:
//! `org.apache.kafka.clients.admin.ListTopicsResult` (CLAUDE.md §4).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::c_void;

use crate::admin::{ListTopicsResult, TopicListing};
use crate::ffi::admin::topic_listing::{box_topic_listing, destroy_topic_listing_element};
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_map_element, destroy_result, result_ref,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_list, box_string_keyed_map, sorted_string_list};

/// Opaque handle to a [`ListTopicsResult`], owned and freed with
/// [`kafka_admin_ListTopicsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListTopicsResult_t {
    _private: [u8; 0],
}

/// Java's `Map<String, TopicListing>` as an owned map of owned `char *`
/// topic names, sorted, to owned `kafka_admin_TopicListing_t *`.
fn listings_map(map: HashMap<String, TopicListing>) -> *mut c_void {
    let sorted: BTreeMap<String, TopicListing> = map.into_iter().collect();
    box_string_keyed_map(
        sorted.into_iter().map(|(name, l)| (name, box_topic_listing(l) as *mut c_void)),
        Some(destroy_topic_listing_element),
    ) as *mut c_void
}

/// Java's `Collection<TopicListing>` (the values of an unordered map) as an
/// owned list of owned `kafka_admin_TopicListing_t *`, sorted by topic name
/// so the order a C caller sees is deterministic.
fn listings_list(mut listings: Vec<TopicListing>) -> *mut c_void {
    listings.sort_by(|a, b| a.name().cmp(b.name()));
    let elements = listings.into_iter().map(|l| box_topic_listing(l) as *mut c_void).collect();
    box_list(elements, Some(destroy_topic_listing_element)) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_topics_result(result: ListTopicsResult, ctx: &FutureCtx) -> *mut kafka_admin_ListTopicsResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_ListTopicsResult_t) -> &'a ResultHandle<ListTopicsResult> {
    unsafe { result_ref(self_) }
}

/// `namesToListings()`: an owned future freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a `kafka_Map_t *`
/// (owned by the future) of `char *` topic names, sorted, to
/// `kafka_admin_TopicListing_t *`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_names_to_listings(
    self_: *const kafka_admin_ListTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx
        .handle_future(&h.result.names_to_listings(), listings_map, destroy_map_element)
}

/// `listings()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of
/// `kafka_admin_TopicListing_t *`, sorted by topic name.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_listings(
    self_: *const kafka_admin_ListTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.listings(), listings_list, destroy_list_element)
}

/// `names()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of `char *`
/// topic names, sorted (Java's `Set<String>`).
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_names(
    self_: *const kafka_admin_ListTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(
        &h.result.names(),
        |names: HashSet<String>| sorted_string_list(names) as *mut c_void,
        destroy_list_element,
    )
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTopicsResult_destroy(self_: *mut kafka_admin_ListTopicsResult_t) {
    unsafe { destroy_result::<ListTopicsResult, _>(self_) }
}
