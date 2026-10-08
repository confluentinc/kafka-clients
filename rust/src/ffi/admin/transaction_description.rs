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
/// `description` must be a live transaction-description handle.
pub(crate) unsafe fn transaction_description_ref<'a>(
    description: *const kafka_admin_TransactionDescription_t,
) -> &'a TransactionDescription {
    unsafe { &*(description as *const TransactionDescription) }
}

/// Frees a `kafka_admin_TransactionDescription_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned transaction-description handle.
pub(crate) unsafe fn destroy_transaction_description_element(element: *mut c_void) {
    unsafe { kafka_admin_TransactionDescription_destroy(element as *mut kafka_admin_TransactionDescription_t) }
}

/// `new TransactionDescription(int coordinatorId, TransactionState state,
/// long producerId, int producerEpoch, long transactionTimeoutMs,
/// OptionalLong transactionStartTimeMs, Set<TopicPartition> topicPartitions)`.
/// `state` is a `kafka_admin_TransactionState_t` singleton;
/// `transaction_start_time_ms` is `-1` for `OptionalLong.empty()`;
/// `topic_partitions` is a borrowed list of `const kafka_common_TopicPartition_t *`,
/// copied (null reads as empty). Owned, freed with
/// [`kafka_admin_TransactionDescription_destroy`].
///
/// # Safety
///
/// `state` must be a transaction-state singleton and `topic_partitions` null
/// or a valid list of topic-partition handles.
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
        unsafe { list_topic_partitions(topic_partitions) }.into_iter().collect();
    box_transaction_description(TransactionDescription::new(
        coordinator_id,
        unsafe { transaction_state_value_of(state) },
        producer_id,
        producer_epoch,
        transaction_timeout_ms,
        (transaction_start_time_ms >= 0).then_some(transaction_start_time_ms),
        topic_partitions,
    ))
}

/// `coordinatorId()`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_coordinator_id(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i32 {
    unsafe { transaction_description_ref(self_) }.coordinator_id()
}

/// `state()`: the `kafka_admin_TransactionState_t` singleton, never freed.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_state(
    self_: *const kafka_admin_TransactionDescription_t,
) -> *const kafka_admin_TransactionState_t {
    transaction_state_singleton(unsafe { transaction_description_ref(self_) }.state())
}

/// `producerId()`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_producer_id(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i64 {
    unsafe { transaction_description_ref(self_) }.producer_id()
}

/// `producerEpoch()`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_producer_epoch(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i32 {
    unsafe { transaction_description_ref(self_) }.producer_epoch()
}

/// `transactionTimeoutMs()`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_transaction_timeout_ms(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i64 {
    unsafe { transaction_description_ref(self_) }.transaction_timeout_ms()
}

/// `transactionStartTimeMs()`: the start time, or `-1` for
/// `OptionalLong.empty()`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_transaction_start_time_ms(
    self_: *const kafka_admin_TransactionDescription_t,
) -> i64 {
    unsafe { transaction_description_ref(self_) }
        .transaction_start_time_ms()
        .unwrap_or(-1)
}

/// `topicPartitions()`: an owned list of owned `kafka_common_TopicPartition_t *`
/// copies ordered by topic then partition, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_topic_partitions(
    self_: *const kafka_admin_TransactionDescription_t,
) -> *mut kafka_List_t {
    sorted_topic_partition_list(unsafe { transaction_description_ref(self_) }.topic_partitions())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid transaction-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_to_string(
    self_: *const kafka_admin_TransactionDescription_t,
) -> *mut c_char {
    into_c_string(&unsafe { transaction_description_ref(self_) }.to_string())
}

/// Frees an owned transaction-description handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TransactionDescription_destroy(self_: *mut kafka_admin_TransactionDescription_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TransactionDescription) });
    }
}
