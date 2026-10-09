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

//! `kafka_admin_ProducerState_t`: `org.apache.kafka.clients.admin.ProducerState`
//! (CLAUDE.md §4). A plain value class: the handle points at the Rust value.
//! Java's `OptionalInt coordinatorEpoch()` and `OptionalLong
//! currentTransactionStartOffset()` cross as `-1` when empty.

use std::ffi::{c_char, c_void};

use crate::admin::ProducerState;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`ProducerState`].
#[repr(C)]
pub struct kafka_admin_ProducerState_t {
    _private: [u8; 0],
}

/// Hands `state` to C as an owned handle, freed with
/// [`kafka_admin_ProducerState_destroy`].
pub(crate) fn box_producer_state(state: ProducerState) -> *mut kafka_admin_ProducerState_t {
    Box::into_raw(Box::new(state)) as *mut kafka_admin_ProducerState_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `state` must be a live producer-state handle.
pub(crate) unsafe fn producer_state_ref<'a>(state: *const kafka_admin_ProducerState_t) -> &'a ProducerState {
    unsafe { &*(state as *const ProducerState) }
}

/// Frees a `kafka_admin_ProducerState_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned producer-state handle.
pub(crate) unsafe fn destroy_producer_state_element(element: *mut c_void) {
    unsafe { kafka_admin_ProducerState_destroy(element as *mut kafka_admin_ProducerState_t) }
}

/// `new ProducerState(long producerId, int producerEpoch, int lastSequence,
/// long lastTimestamp, OptionalInt coordinatorEpoch, OptionalLong
/// currentTransactionStartOffset)`; `coordinator_epoch` and
/// `current_transaction_start_offset` are `-1` when empty. Owned, freed with
/// [`kafka_admin_ProducerState_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ProducerState_new(
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_transaction_start_offset: i64,
) -> *mut kafka_admin_ProducerState_t {
    box_producer_state(ProducerState::new(
        producer_id,
        producer_epoch,
        last_sequence,
        last_timestamp,
        (coordinator_epoch >= 0).then_some(coordinator_epoch),
        (current_transaction_start_offset >= 0).then_some(current_transaction_start_offset),
    ))
}

/// `producerId()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_producer_id(self_: *const kafka_admin_ProducerState_t) -> i64 {
    unsafe { producer_state_ref(self_) }.producer_id()
}

/// `producerEpoch()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_producer_epoch(self_: *const kafka_admin_ProducerState_t) -> i32 {
    unsafe { producer_state_ref(self_) }.producer_epoch()
}

/// `lastSequence()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_last_sequence(self_: *const kafka_admin_ProducerState_t) -> i32 {
    unsafe { producer_state_ref(self_) }.last_sequence()
}

/// `lastTimestamp()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_last_timestamp(self_: *const kafka_admin_ProducerState_t) -> i64 {
    unsafe { producer_state_ref(self_) }.last_timestamp()
}

/// `coordinatorEpoch()`: the epoch, or `-1` for `OptionalInt.empty()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_coordinator_epoch(self_: *const kafka_admin_ProducerState_t) -> i32 {
    unsafe { producer_state_ref(self_) }.coordinator_epoch().unwrap_or(-1)
}

/// `currentTransactionStartOffset()`: the offset, or `-1` for
/// `OptionalLong.empty()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_current_transaction_start_offset(
    self_: *const kafka_admin_ProducerState_t,
) -> i64 {
    unsafe { producer_state_ref(self_) }
        .current_transaction_start_offset()
        .unwrap_or(-1)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_to_string(self_: *const kafka_admin_ProducerState_t) -> *mut c_char {
    into_c_string(&unsafe { producer_state_ref(self_) }.to_string())
}

/// Frees an owned producer-state handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_destroy(self_: *mut kafka_admin_ProducerState_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ProducerState) });
    }
}
