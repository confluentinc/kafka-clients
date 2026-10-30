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
use crate::ffi::ffi_guard;
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
/// `state` must be a live producer-state handle, kept alive for `'a`.
pub(crate) unsafe fn producer_state_ref<'a>(state: *const kafka_admin_ProducerState_t) -> &'a ProducerState {
    // SAFETY: Per this function's `# Safety`, `state` is a live producer-state handle kept alive
    // for `'a`. Every `kafka_admin_ProducerState_t` comes from `box_producer_state`, which boxes a
    // `ProducerState`, so the cast yields a valid shared reference. The type has no setters, so
    // nothing mutates it while borrowed.
    unsafe { &*(state as *const ProducerState) }
}

/// Frees a `kafka_admin_ProducerState_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned producer-state handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_producer_state_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned producer-state handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_ProducerState_destroy` requires.
    unsafe { kafka_admin_ProducerState_destroy(element as *mut kafka_admin_ProducerState_t) }
}

/// Creates the state of an active producer on a partition.
///
/// # Parameters
///
/// - `producer_id` - the producer id.
/// - `producer_epoch` - the producer epoch.
/// - `last_sequence` - the last sequence number written by the producer.
/// - `last_timestamp` - the timestamp of the producer's last write.
/// - `coordinator_epoch` - the epoch of the transaction coordinator, `-1` for none.
/// - `current_transaction_start_offset` - the start offset of the producer's ongoing
///   transaction, `-1` for none.
///
/// # Returns
///
/// The producer state.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with `kafka_admin_ProducerState_destroy`.
#[ffi_guard]
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

/// Returns the producer id.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_producer_id(self_: *const kafka_admin_ProducerState_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid producer-state handle, kept alive
    // for this call, as `producer_state_ref` requires.
    unsafe { producer_state_ref(self_) }.producer_id()
}

/// Returns the producer epoch.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_producer_epoch(self_: *const kafka_admin_ProducerState_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid producer-state handle, kept alive
    // for this call, as `producer_state_ref` requires.
    unsafe { producer_state_ref(self_) }.producer_epoch()
}

/// Returns the last sequence number written by the producer.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_last_sequence(self_: *const kafka_admin_ProducerState_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid producer-state handle, kept alive
    // for this call, as `producer_state_ref` requires.
    unsafe { producer_state_ref(self_) }.last_sequence()
}

/// Returns the timestamp of the producer's last write.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_last_timestamp(self_: *const kafka_admin_ProducerState_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid producer-state handle, kept alive
    // for this call, as `producer_state_ref` requires.
    unsafe { producer_state_ref(self_) }.last_timestamp()
}

/// Returns the epoch of the transaction coordinator, or `-1` for `OptionalInt.empty()`.
///
/// A caught Rust panic returns -2, which is no coordinator epoch: Java reads a negative one as
/// `OptionalInt.empty()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_coordinator_epoch(self_: *const kafka_admin_ProducerState_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid producer-state handle, kept alive
    // for this call, as `producer_state_ref` requires.
    unsafe { producer_state_ref(self_) }.coordinator_epoch().unwrap_or(-1)
}

/// Returns the start offset of the producer's ongoing transaction, or `-1` for
/// `OptionalLong.empty()`.
///
/// A caught Rust panic returns -2, which is no transaction start offset: Java reads a negative one
/// as `OptionalLong.empty()`.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_current_transaction_start_offset(
    self_: *const kafka_admin_ProducerState_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid producer-state handle, kept alive
    // for this call, as `producer_state_ref` requires.
    unsafe { producer_state_ref(self_) }
        .current_transaction_start_offset()
        .unwrap_or(-1)
}

/// Returns the text form of the producer state.
///
/// # Safety
///
/// `self_` must be a valid producer-state handle. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_to_string(self_: *const kafka_admin_ProducerState_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid producer-state handle, kept alive
    // for this call, as `producer_state_ref` requires.
    into_c_string(&unsafe { producer_state_ref(self_) }.to_string())
}

/// Frees an owned producer-state handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ProducerState_destroy(self_: *mut kafka_admin_ProducerState_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every producer-state handle comes from `box_producer_state`, which boxes a
        // `ProducerState`, so `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut ProducerState) });
    }
}
