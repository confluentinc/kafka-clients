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

//! `kafka_admin_ListTransactionsOptions_t`:
//! `org.apache.kafka.clients.admin.ListTransactionsOptions` (CLAUDE.md §4).
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `filter_states(self, states)` on the handle is Java's
//! `options.filterStates(states)` with the result stored back. The states
//! cross as borrowed `kafka_admin_TransactionState_t` singletons both ways;
//! the Java `Set`s come back as sorted lists so a C caller sees a
//! deterministic order. The handle keeps a NUL-terminated copy of the
//! transactional-id pattern so its getter borrows.

use std::ffi::{CString, c_char, c_void};
use std::ptr;

use crate::admin::{ListTransactionsOptions, TransactionState};
use crate::ffi::admin::transaction_state::{
    kafka_admin_TransactionState_t, transaction_state_singleton, transaction_state_value_of,
};
use crate::ffi::util::{box_list, c_str_to_option, destroy_boxed, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`ListTransactionsOptions`], owned by the caller and
/// freed with [`kafka_admin_ListTransactionsOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_ListTransactionsOptions_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_ListTransactionsOptions_t`] points at: the options
/// plus the NUL-terminated pattern its getter borrows out.
struct ListTransactionsOptionsInner {
    options: ListTransactionsOptions,
    pattern_c: Option<CString>,
}

impl ListTransactionsOptionsInner {
    fn new(options: ListTransactionsOptions) -> Self {
        let pattern_c = options.filtered_transactional_id_pattern().map(owned_c_string);
        Self { options, pattern_c }
    }

    /// Replaces the options, refreshing the cached pattern.
    fn replace(&mut self, update: impl FnOnce(ListTransactionsOptions) -> ListTransactionsOptions) {
        *self = Self::new(update(std::mem::take(&mut self.options)));
    }
}

unsafe fn inner_ref<'a>(options: *const kafka_admin_ListTransactionsOptions_t) -> &'a ListTransactionsOptionsInner {
    unsafe { &*(options as *const ListTransactionsOptionsInner) }
}

/// Mutable access to the handle's state, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle and no other reference to it may be live.
unsafe fn inner_mut<'a>(options: *mut kafka_admin_ListTransactionsOptions_t) -> &'a mut ListTransactionsOptionsInner {
    unsafe { &mut *(options as *mut ListTransactionsOptionsInner) }
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle.
pub(crate) unsafe fn list_transactions_options_ref<'a>(
    options: *const kafka_admin_ListTransactionsOptions_t,
) -> &'a ListTransactionsOptions {
    &unsafe { inner_ref(options) }.options
}

fn boxed(options: ListTransactionsOptions) -> *mut kafka_admin_ListTransactionsOptions_t {
    Box::into_raw(Box::new(ListTransactionsOptionsInner::new(options))) as *mut kafka_admin_ListTransactionsOptions_t
}

/// `new ListTransactionsOptions()`: no filter, with the client's default
/// API timeout. Owned, freed with [`kafka_admin_ListTransactionsOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListTransactionsOptions_new() -> *mut kafka_admin_ListTransactionsOptions_t {
    boxed(ListTransactionsOptions::new())
}

/// `AbstractOptions.timeoutMs(Integer timeoutMs)`: `-1` (any negative value)
/// stands for Java's `null`, the client's default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_set_timeout_ms(
    self_: *mut kafka_admin_ListTransactionsOptions_t,
    timeout_ms: i32,
) {
    unsafe { inner_mut(self_) }.replace(|options| options.set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms)));
}

/// `AbstractOptions.timeoutMs()`: the timeout in milliseconds, `-1` when the
/// client's default API timeout applies.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_timeout_ms(
    self_: *const kafka_admin_ListTransactionsOptions_t,
) -> i32 {
    unsafe { list_transactions_options_ref(self_) }.timeout_ms().unwrap_or(-1)
}

/// `ListTransactionsOptions.filterStates(Collection<TransactionState> states)`:
/// `states` is a borrowed `kafka_List_t` of `kafka_admin_TransactionState_t`
/// singletons, copied during the call (`NULL` reads as empty: no filter).
///
/// # Safety
///
/// `self_` must be a live handle; `states` must be `NULL` or a list of
/// transaction-state singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filter_states(
    self_: *mut kafka_admin_ListTransactionsOptions_t,
    states: *const kafka_List_t,
) {
    let states: Vec<TransactionState> = unsafe { list_elements(states) }
        .iter()
        .map(|&element| unsafe { transaction_state_value_of(element as *const kafka_admin_TransactionState_t) })
        .collect();
    unsafe { inner_mut(self_) }.replace(|options| options.filter_states(states));
}

/// `ListTransactionsOptions.filterProducerIds(Collection<Long> producerIds)`:
/// `producer_ids` is a borrowed `kafka_List_t` of `const int64_t *`, copied
/// during the call (`NULL` reads as empty: no filter).
///
/// # Safety
///
/// `self_` must be a live handle; `producer_ids` must be `NULL` or a list of
/// pointers to `int64_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filter_producer_ids(
    self_: *mut kafka_admin_ListTransactionsOptions_t,
    producer_ids: *const kafka_List_t,
) {
    let producer_ids: Vec<i64> = unsafe { list_elements(producer_ids) }
        .iter()
        .map(|&element| unsafe { *(element as *const i64) })
        .collect();
    unsafe { inner_mut(self_) }.replace(|options| options.filter_producer_ids(producer_ids));
}

/// `ListTransactionsOptions.filterOnDuration(long durationMs)`: only the
/// transactions running longer than `duration_ms`; a negative value disables
/// the filter.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filter_on_duration(
    self_: *mut kafka_admin_ListTransactionsOptions_t,
    duration_ms: i64,
) {
    unsafe { inner_mut(self_) }.replace(|options| options.filter_on_duration(duration_ms));
}

/// `ListTransactionsOptions.filterOnTransactionalIdPattern(String pattern)`:
/// `pattern` is copied during the call; `NULL` is Java's `null`, no pattern
/// filter.
///
/// # Safety
///
/// `self_` must be a live handle; `pattern` must be `NULL` or a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filter_on_transactional_id_pattern(
    self_: *mut kafka_admin_ListTransactionsOptions_t,
    pattern: *const c_char,
) {
    let pattern = unsafe { c_str_to_option(pattern) };
    unsafe { inner_mut(self_) }.replace(|options| options.filter_on_transactional_id_pattern(pattern));
}

/// `ListTransactionsOptions.filteredStates()`: an owned `kafka_List_t` of
/// borrowed `kafka_admin_TransactionState_t` singletons sorted by name,
/// freed with `kafka_List_destroy` (the singletons are never freed); empty
/// when no state filter is set.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filtered_states(
    self_: *const kafka_admin_ListTransactionsOptions_t,
) -> *mut kafka_List_t {
    let mut states: Vec<TransactionState> = unsafe { list_transactions_options_ref(self_) }
        .filtered_states()
        .iter()
        .copied()
        .collect();
    states.sort_unstable_by_key(|state| state.to_string());
    box_list(
        states
            .into_iter()
            .map(|state| transaction_state_singleton(state) as *mut c_void)
            .collect(),
        None,
    )
}

/// `ListTransactionsOptions.filteredProducerIds()`: an owned, sorted
/// `kafka_List_t` of owned `int64_t *`, freed with `kafka_List_destroy`;
/// empty when no producer-id filter is set.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filtered_producer_ids(
    self_: *const kafka_admin_ListTransactionsOptions_t,
) -> *mut kafka_List_t {
    let mut producer_ids: Vec<i64> = unsafe { list_transactions_options_ref(self_) }
        .filtered_producer_ids()
        .iter()
        .copied()
        .collect();
    producer_ids.sort_unstable();
    box_list(
        producer_ids
            .into_iter()
            .map(|id| Box::into_raw(Box::new(id)) as *mut c_void)
            .collect(),
        Some(destroy_boxed::<i64>),
    )
}

/// `ListTransactionsOptions.filteredDuration()`: the duration filter in
/// milliseconds, negative when no duration filter is set.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filtered_duration(
    self_: *const kafka_admin_ListTransactionsOptions_t,
) -> i64 {
    unsafe { list_transactions_options_ref(self_) }.filtered_duration()
}

/// `ListTransactionsOptions.filteredTransactionalIdPattern()`: a borrowed
/// string valid until the handle is destroyed or the pattern is set again,
/// `NULL` when no pattern filter is set.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_filtered_transactional_id_pattern(
    self_: *const kafka_admin_ListTransactionsOptions_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }
        .pattern_c
        .as_ref()
        .map_or(ptr::null(), |pattern| pattern.as_ptr())
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListTransactionsOptions_destroy(
    self_: *mut kafka_admin_ListTransactionsOptions_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ListTransactionsOptionsInner) });
    }
}
