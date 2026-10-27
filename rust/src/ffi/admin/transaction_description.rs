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

//! `kafka_admin_TransactionDescription_t`:
//! `org.apache.kafka.clients.admin.TransactionDescription` (CLAUDE.md §4).
//! A plain value class: the handle points at the Rust value. Java's
//! `OptionalLong transactionStartTimeMs()` crosses as `-1` for
//! `OptionalLong.empty()`.

use std::collections::HashSet;
use std::ffi::{c_char, c_void};

use crate::admin::TransactionDescription;
use crate::common::TopicPartition;
use crate::ffi::admin::transaction_state::{
    kafka_admin_TransactionState_t, transaction_state_singleton, transaction_state_value_of,
};
use crate::ffi::common::topic_partition::{list_topic_partitions, sorted_topic_partition_list};
use crate::ffi::ffi_guard;
use crate::ffi::util::{into_c_string, kafka_List_t};

/// Opaque handle to a [`TransactionDescription`].
#[repr(C)]
pub struct kafka_admin_TransactionDescription_t {
    _private: [u8; 0],
}

/// Hands `description` to C as an owned handle, freed with
/// [`kafka_admin_TransactionDescription_destroy`].
pub(crate) fn box_transaction_description(
    description: TransactionDescription,
) -> *mut kafka_admin_TransactionDescription_t {
    Box::into_raw(Box::new(description)) as *mut kafka_admin_TransactionDescription_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `description` must be a live transaction-description handle, kept alive
/// for `'a`.
pub(crate) unsafe fn transaction_description_ref<'a>(
    description: *const kafka_admin_TransactionDescription_t,
) -> &'a TransactionDescription {
    // SAFETY: Per this function's `# Safety`, `description` is a live transaction-description
    // handle kept alive for `'a`. Every `kafka_admin_TransactionDescription_t` comes from
    // `box_transaction_description`, which boxes a `TransactionDescription`, so the cast yields a
    // valid shared reference. The type has no setters, so nothing mutates it while borrowed.
    unsafe { &*(description as *const TransactionDescription) }
}

/// Frees a `kafka_admin_TransactionDescription_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned transaction-description handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_transaction_description_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned transaction-description handle
    // not yet destroyed, that no other call uses at the same time and that is not used afterwards,
    // as `kafka_admin_TransactionDescription_destroy` requires.
    unsafe { kafka_admin_TransactionDescription_destroy(element as *mut kafka_admin_TransactionDescription_t) }
}

/// Creates the description of a transaction.
///
/// # Parameters
///
/// - `coordinator_id` - the broker id of the transaction coordinator.
/// - `state` - the state of the transaction, a `kafka_admin_TransactionState_t` singleton.
/// - `producer_id` - the producer id of the transaction.
/// - `producer_epoch` - the producer epoch of the transaction.
/// - `transaction_timeout_ms` - the transaction timeout, in milliseconds.
/// - `transaction_start_time_ms` - the start time of the transaction, in milliseconds, `-1` for
///   none.
/// - `topic_partitions` - the partitions in the transaction, `NULL` for none.
///
/// # Safety
///
/// `state` must be a transaction-state singleton and `topic_partitions` `NULL` or a valid borrowed
/// list of `const kafka_common_TopicPartition_t *`, copied. The returned handle is owned, freed
/// with [`kafka_admin_TransactionDescription_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_new(
    coordinator_id: i32,
    state: *const kafka_admin_TransactionState_t,
    producer_id: i64,
    producer_epoch: i32,
    transaction_timeout_ms: i64,
    transaction_start_time_ms: i64,
    topic_partitions: *const kafka_List_t,
) -> *mut kafka_admin_TransactionDescription_t {
    let topic_partitions: HashSet<TopicPartition> =
        // SAFETY: Per this function's `# Safety`, `topic_partitions` is null or a valid list of
        // topic-partition handles, as `list_topic_partitions` requires.
        unsafe { list_topic_partitions(topic_partitions) }.into_iter().collect();
    box_transaction_description(TransactionDescription::new(
        coordinator_id,
        // SAFETY: Per this function's `# Safety`, `state` is a transaction-state singleton; only
        // the `transaction_state` module hands those out, as `transaction_state_value_of` requires.
        unsafe { transaction_state_value_of(state) },
        producer_id,
        producer_epoch,
        transaction_timeout_ms,
        (transaction_start_time_ms >= 0).then_some(transaction_start_time_ms),
        topic_partitions,
    ))
}

/// Returns the broker id of the transaction coordinator.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_coordinator_id(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    unsafe { transaction_description_ref(self_) }.coordinator_id()
}

/// Returns the state of the transaction.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle. The returned
/// `kafka_admin_TransactionState_t` singleton is never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_state(
    self_: *const kafka_admin_TransactionDescription_t,
) -> *const kafka_admin_TransactionState_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    transaction_state_singleton(unsafe { transaction_description_ref(self_) }.state())
}

/// Returns the producer id of the transaction.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_producer_id(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    unsafe { transaction_description_ref(self_) }.producer_id()
}

/// Returns the producer epoch of the transaction.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_producer_epoch(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    unsafe { transaction_description_ref(self_) }.producer_epoch()
}

/// Returns the transaction timeout, in milliseconds.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_transaction_timeout_ms(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    unsafe { transaction_description_ref(self_) }.transaction_timeout_ms()
}

/// Returns the start time of the transaction, in milliseconds, or `-1` for
/// `OptionalLong.empty()`.
///
/// A caught Rust panic returns -2, which is no transaction start time: Java reads a negative one as
/// `OptionalLong.empty()`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_transaction_start_time_ms(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    unsafe { transaction_description_ref(self_) }
        .transaction_start_time_ms()
        .unwrap_or(-1)
}

/// Returns the partitions in the transaction, ordered by topic then partition.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle. The returned list of
/// `kafka_common_TopicPartition_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_topic_partitions(
    self_: *const kafka_admin_TransactionDescription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    sorted_topic_partition_list(unsafe { transaction_description_ref(self_) }.topic_partitions())
}

/// Returns the text form of the description.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle. The returned string is owned, freed
/// with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_to_string(
    self_: *const kafka_admin_TransactionDescription_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid transaction-description handle,
    // kept alive for this call, as `transaction_description_ref` requires.
    into_c_string(&unsafe { transaction_description_ref(self_) }.to_string())
}

/// Frees an owned transaction-description handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_destroy(self_: *mut kafka_admin_TransactionDescription_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every transaction-description handle comes from
        // `box_transaction_description`, which boxes a `TransactionDescription`, so `Box::from_raw`
        // reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut TransactionDescription) });
    }
}
