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

//! `kafka_admin_PartitionReassignment_t`:
//! `org.apache.kafka.clients.admin.PartitionReassignment` (CLAUDE.md §4). A
//! plain value class: the handle points at the Rust value. Java's
//! `List<Integer>` broker-id lists cross as `kafka_List_t`s of `int32_t *`.

use std::ffi::{c_char, c_void};

use crate::admin::PartitionReassignment;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, destroy_boxed, into_c_string, kafka_List_t, list_elements};

/// Opaque handle to a [`PartitionReassignment`].
#[repr(C)]
pub struct kafka_admin_PartitionReassignment_t {
    _private: [u8; 0],
}

/// Hands `reassignment` to C as an owned handle, freed with
/// [`kafka_admin_PartitionReassignment_destroy`].
pub(crate) fn box_partition_reassignment(
    reassignment: PartitionReassignment,
) -> *mut kafka_admin_PartitionReassignment_t {
    Box::into_raw(Box::new(reassignment)) as *mut kafka_admin_PartitionReassignment_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `reassignment` must be a valid partition-reassignment handle, kept alive
/// for `'a`.
pub(crate) unsafe fn partition_reassignment_ref<'a>(
    reassignment: *const kafka_admin_PartitionReassignment_t,
) -> &'a PartitionReassignment {
    // SAFETY: Per this function's `# Safety`, `reassignment` is a valid partition-reassignment
    // handle kept alive for `'a`; every `kafka_admin_PartitionReassignment_t` is the
    // `Box::into_raw` of a `Box<PartitionReassignment>` (`box_partition_reassignment`, the only
    // producer), so the shared reference is to a live `PartitionReassignment`, which no entry point
    // mutates.
    unsafe { &*(reassignment as *const PartitionReassignment) }
}

/// Frees a `kafka_admin_PartitionReassignment_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned partition-reassignment handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_partition_reassignment_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned partition-reassignment handle
    // not yet destroyed, that no other call uses at the same time and that is not used afterwards,
    // as `kafka_admin_PartitionReassignment_destroy` requires.
    unsafe { kafka_admin_PartitionReassignment_destroy(element as *mut kafka_admin_PartitionReassignment_t) }
}

/// A Java `List<Integer>` as an owned list of owned `int32_t *`.
fn broker_id_list(ids: &[i32]) -> *mut kafka_List_t {
    let elements = ids.iter().map(|&id| Box::into_raw(Box::new(id)) as *mut c_void).collect();
    box_list(elements, Some(destroy_boxed::<i32>))
}

/// Reads a list of `const int32_t *`; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements point at `int32_t`s,
/// kept alive and not mutated for this call.
unsafe fn list_broker_ids(list: *const kafka_List_t) -> Vec<i32> {
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list, alive and not mutated
    // for this call, as `list_elements` requires; the borrowed slice is dropped before the call
    // returns.
    unsafe { list_elements(list) }
        .iter()
        // SAFETY: Per this function's `# Safety`, each element of `list` points at a live
        // `int32_t`, read by copy while the list is alive.
        .map(|&element| unsafe { *(element as *const i32) })
        .collect()
}

/// `new PartitionReassignment(List<Integer> replicas, List<Integer>
/// addingReplicas, List<Integer> removingReplicas)`: three borrowed lists of
/// `const int32_t *`, copied (null reads as empty). Owned, freed with
/// [`kafka_admin_PartitionReassignment_destroy`].
///
/// # Safety
///
/// Each list must be null or a valid list whose elements point at `int32_t`s.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_new(
    replicas: *const kafka_List_t,
    adding_replicas: *const kafka_List_t,
    removing_replicas: *const kafka_List_t,
) -> *mut kafka_admin_PartitionReassignment_t {
    box_partition_reassignment(PartitionReassignment::new(
        // SAFETY: Per this function's `# Safety`, `replicas` is null or a valid list whose elements
        // point at `int32_t`s, kept alive and not mutated for this call, as `list_broker_ids`
        // requires.
        unsafe { list_broker_ids(replicas) },
        // SAFETY: Per this function's `# Safety`, `adding_replicas` is null or a valid list whose
        // elements point at `int32_t`s, kept alive and not mutated for this call, as
        // `list_broker_ids` requires.
        unsafe { list_broker_ids(adding_replicas) },
        // SAFETY: Per this function's `# Safety`, `removing_replicas` is null or a valid list whose
        // elements point at `int32_t`s, kept alive and not mutated for this call, as
        // `list_broker_ids` requires.
        unsafe { list_broker_ids(removing_replicas) },
    ))
}

/// `replicas()`: an owned list of owned `int32_t *` broker ids, in Java's
/// order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid partition-reassignment handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_replicas(
    self_: *const kafka_admin_PartitionReassignment_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-reassignment handle,
    // kept alive for this call, as `partition_reassignment_ref` requires.
    broker_id_list(unsafe { partition_reassignment_ref(self_) }.replicas())
}

/// `addingReplicas()`: an owned list of owned `int32_t *` broker ids, in
/// Java's order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid partition-reassignment handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_adding_replicas(
    self_: *const kafka_admin_PartitionReassignment_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-reassignment handle,
    // kept alive for this call, as `partition_reassignment_ref` requires.
    broker_id_list(unsafe { partition_reassignment_ref(self_) }.adding_replicas())
}

/// `removingReplicas()`: an owned list of owned `int32_t *` broker ids, in
/// Java's order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid partition-reassignment handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_removing_replicas(
    self_: *const kafka_admin_PartitionReassignment_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-reassignment handle,
    // kept alive for this call, as `partition_reassignment_ref` requires.
    broker_id_list(unsafe { partition_reassignment_ref(self_) }.removing_replicas())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid partition-reassignment handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_to_string(
    self_: *const kafka_admin_PartitionReassignment_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-reassignment handle,
    // kept alive for this call, as `partition_reassignment_ref` requires.
    into_c_string(&unsafe { partition_reassignment_ref(self_) }.to_string())
}

/// Frees an owned partition-reassignment handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_PartitionReassignment_destroy(self_: *mut kafka_admin_PartitionReassignment_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards; every handle is the `Box::into_raw` of a `Box<PartitionReassignment>`
        // (`box_partition_reassignment`), so `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut PartitionReassignment) });
    }
}
