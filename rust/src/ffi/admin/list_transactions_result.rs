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

//! `kafka_admin_ListTransactionsResult_t`:
//! `org.apache.kafka.clients.admin.ListTransactionsResult` (CLAUDE.md §4).
//! Broker ids cross as boxed `int32_t *` map keys, compared by value.

use std::collections::{BTreeMap, HashMap};
use std::ffi::c_void;

use crate::admin::{ListTransactionsResult, TransactionListing};
use crate::common::KafkaFuture;
use crate::ffi::admin::transaction_listing::{box_transaction_listing, destroy_transaction_listing_element};
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_map_element, destroy_result, result_ref,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_list, box_map, destroy_boxed};

/// Opaque handle to a [`ListTransactionsResult`], owned and freed with
/// [`kafka_admin_ListTransactionsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListTransactionsResult_t {
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

/// Java's `Collection<TransactionListing>` as an owned list of owned
/// `kafka_admin_TransactionListing_t *`, in the order given.
fn listings_list(listings: Vec<TransactionListing>) -> *mut c_void {
    let elements = listings
        .into_iter()
        .map(|l| box_transaction_listing(l) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_transaction_listing_element)) as *mut c_void
}

/// [`listings_list`] for the listings of every broker flattened out of an
/// unordered map: sorted by transactional id so the order a C caller sees is
/// deterministic.
fn sorted_listings_list(mut listings: Vec<TransactionListing>) -> *mut c_void {
    listings.sort_by(|a, b| a.transactional_id().cmp(b.transactional_id()));
    listings_list(listings)
}

/// Java's `Map<Integer, Collection<TransactionListing>>` as an owned map of
/// owned `int32_t *` broker ids, sorted and compared by value, to the owned
/// `kafka_List_t *` built by [`listings_list`].
fn all_by_broker_id_map(map: HashMap<i32, Vec<TransactionListing>>) -> *mut c_void {
    let sorted: BTreeMap<i32, Vec<TransactionListing>> = map.into_iter().collect();
    let entries = sorted
        .into_iter()
        .map(|(broker_id, l)| (box_i32(&broker_id), listings_list(l)))
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<i32>),
        Some(destroy_list_element),
        Some(i32_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_transactions_result(
    result: ListTransactionsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ListTransactionsResult_t {
    box_result(result, ctx)
}

unsafe fn handle<'a>(self_: *const kafka_admin_ListTransactionsResult_t) -> &'a ResultHandle<ListTransactionsResult> {
    unsafe { result_ref(self_) }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_List_t *` (owned by the future) of
/// `kafka_admin_TransactionListing_t *` from every broker, sorted by
/// transactional id.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsResult_all(
    self_: *const kafka_admin_ListTransactionsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.all(), sorted_listings_list, destroy_list_element)
}

/// `byBrokerId()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_Map_t *` (owned by the future) of `int32_t *`
/// broker ids, sorted and compared by value in `kafka_Map_get`, to
/// `kafka_common_KafkaFuture_t *` (owned by the map) whose own `get`
/// delivers that broker's `kafka_List_t *` of
/// `kafka_admin_TransactionListing_t *`, in the order the broker returned
/// them. The outer future completes once the brokers are known, the inner
/// ones once each broker has answered.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsResult_by_broker_id(
    self_: *const kafka_admin_ListTransactionsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    let ctx = h.ctx.clone();
    h.ctx.handle_future(
        &h.result.by_broker_id(),
        move |map: HashMap<i32, KafkaFuture<Vec<TransactionListing>>>| {
            let sorted: BTreeMap<i32, KafkaFuture<Vec<TransactionListing>>> = map.into_iter().collect();
            ctx.keyed_future_map(sorted.iter(), box_i32, destroy_boxed::<i32>, i32_key_eq, |ctx, f| {
                ctx.handle_future(f, listings_list, destroy_list_element)
            }) as *mut c_void
        },
        destroy_map_element,
    )
}

/// `allByBrokerId()`: an owned future freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a `kafka_Map_t *`
/// (owned by the future) of `int32_t *` broker ids, sorted and compared by
/// value in `kafka_Map_get`, to that broker's `kafka_List_t *` of
/// `kafka_admin_TransactionListing_t *`, in the order the broker returned
/// them; it completes once every broker has answered.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsResult_all_by_broker_id(
    self_: *const kafka_admin_ListTransactionsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx
        .handle_future(&h.result.all_by_broker_id(), all_by_broker_id_map, destroy_map_element)
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsResult_destroy(self_: *mut kafka_admin_ListTransactionsResult_t) {
    unsafe { destroy_result::<ListTransactionsResult, _>(self_) }
}
