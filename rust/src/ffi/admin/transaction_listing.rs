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

//! `kafka_admin_TransactionListing_t`:
//! `org.apache.kafka.clients.admin.TransactionListing` (CLAUDE.md §4).

use std::ffi::{CString, c_char, c_void};

use crate::admin::TransactionListing;
use crate::ffi::admin::transaction_state::{
    kafka_admin_TransactionState_t, transaction_state_singleton, transaction_state_value_of,
};
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`TransactionListing`].
#[repr(C)]
pub struct kafka_admin_TransactionListing_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_TransactionListing_t`] points at: the value plus the
/// NUL-terminated transactional id its getter borrows out.
pub(crate) struct TransactionListingInner {
    listing: TransactionListing,
    transactional_id_c: CString,
}

impl TransactionListingInner {
    fn new(listing: TransactionListing) -> Self {
        let transactional_id_c = owned_c_string(listing.transactional_id());
        Self { listing, transactional_id_c }
    }
}

unsafe fn inner_ref<'a>(listing: *const kafka_admin_TransactionListing_t) -> &'a TransactionListingInner {
    unsafe { &*(listing as *const TransactionListingInner) }
}

/// Hands `listing` to C as an owned handle, freed with
/// [`kafka_admin_TransactionListing_destroy`].
pub(crate) fn box_transaction_listing(listing: TransactionListing) -> *mut kafka_admin_TransactionListing_t {
    Box::into_raw(Box::new(TransactionListingInner::new(listing))) as *mut kafka_admin_TransactionListing_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `listing` must be a live transaction-listing handle.
pub(crate) unsafe fn transaction_listing_ref<'a>(
    listing: *const kafka_admin_TransactionListing_t,
) -> &'a TransactionListing {
    &unsafe { inner_ref(listing) }.listing
}

/// Frees a `kafka_admin_TransactionListing_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned transaction-listing handle.
pub(crate) unsafe fn destroy_transaction_listing_element(element: *mut c_void) {
    unsafe { kafka_admin_TransactionListing_destroy(element as *mut kafka_admin_TransactionListing_t) }
}

/// `new TransactionListing(String transactionalId, long producerId,
/// TransactionState transactionState)`; `transaction_state` is a
/// `kafka_admin_TransactionState_t` singleton. Owned, freed with
/// [`kafka_admin_TransactionListing_destroy`].
///
/// # Safety
///
/// `transactional_id` must be a valid NUL-terminated string and
/// `transaction_state` a transaction-state singleton.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionListing_new(
    transactional_id: *const c_char,
    producer_id: i64,
    transaction_state: *const kafka_admin_TransactionState_t,
) -> *mut kafka_admin_TransactionListing_t {
    box_transaction_listing(TransactionListing::new(
        unsafe { c_str_to_string(transactional_id) },
        producer_id,
        unsafe { transaction_state_value_of(transaction_state) },
    ))
}

/// `transactionalId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid transaction-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionListing_transactional_id(
    self_: *const kafka_admin_TransactionListing_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.transactional_id_c.as_ptr()
}

/// `producerId()`.
///
/// # Safety
///
/// `self_` must be a valid transaction-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionListing_producer_id(
    self_: *const kafka_admin_TransactionListing_t,
) -> i64 {
    unsafe { transaction_listing_ref(self_) }.producer_id()
}

/// `state()`: the `kafka_admin_TransactionState_t` singleton, never freed.
///
/// # Safety
///
/// `self_` must be a valid transaction-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionListing_state(
    self_: *const kafka_admin_TransactionListing_t,
) -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(unsafe { transaction_listing_ref(self_) }.state())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid transaction-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionListing_to_string(
    self_: *const kafka_admin_TransactionListing_t,
) -> *mut c_char {
    into_c_string(&unsafe { transaction_listing_ref(self_) }.to_string())
}

/// Frees an owned transaction-listing handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionListing_destroy(self_: *mut kafka_admin_TransactionListing_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TransactionListingInner) });
    }
}
