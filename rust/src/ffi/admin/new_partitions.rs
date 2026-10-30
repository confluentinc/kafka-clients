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

//! `kafka_admin_NewPartitions_t`:
//! `org.apache.kafka.clients.admin.NewPartitions` (CLAUDE.md §4). A
//! `List<List<Integer>>` crosses as a `kafka_List_t` of `kafka_List_t *`
//! holding `int32_t *` elements.

use std::ffi::{c_char, c_void};

use crate::admin::NewPartitions;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, destroy_boxed, into_c_string, kafka_List_destroy, kafka_List_t, list_elements};

/// Opaque handle to a [`NewPartitions`].
#[repr(C)]
pub struct kafka_admin_NewPartitions_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_NewPartitions_t`] points at.
pub(crate) struct NewPartitionsInner {
    partitions: NewPartitions,
}

/// The value behind a handle.
///
/// # Safety
///
/// `partitions` must be a valid new-partitions handle, kept alive for `'a`.
pub(crate) unsafe fn new_partitions_ref<'a>(partitions: *const kafka_admin_NewPartitions_t) -> &'a NewPartitions {
    // SAFETY: Per this function's `# Safety`, `partitions` is a valid new-partitions handle kept
    // alive for `'a`; every `kafka_admin_NewPartitions_t` is the `Box::into_raw` of a
    // `Box<NewPartitionsInner>` (`box_new_partitions`, the only producer), so the shared reference
    // is to a live `NewPartitionsInner`, which no entry point mutates.
    &unsafe { &*(partitions as *const NewPartitionsInner) }.partitions
}

/// Hands `partitions` to C as an owned handle, freed with
/// [`kafka_admin_NewPartitions_destroy`].
pub(crate) fn box_new_partitions(partitions: NewPartitions) -> *mut kafka_admin_NewPartitions_t {
    Box::into_raw(Box::new(NewPartitionsInner { partitions })) as *mut kafka_admin_NewPartitions_t
}

/// Reads a borrowed list of `const int32_t *` into values; `NULL` reads as empty.
///
/// # Safety
///
/// `list` must be `NULL` or a valid list whose elements point at `int32_t`s,
/// kept alive and not mutated for this call.
unsafe fn list_i32(list: *const kafka_List_t) -> Vec<i32> {
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

/// Hands `values` to C as an owned list of `int32_t *`.
fn box_i32_list(values: &[i32]) -> *mut kafka_List_t {
    let elements = values.iter().map(|&v| Box::into_raw(Box::new(v)) as *mut c_void).collect();
    box_list(elements, Some(destroy_boxed::<i32>))
}

/// Frees a `kafka_List_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned list handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
unsafe fn destroy_list_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned list handle not yet destroyed,
    // that no other call uses at the same time and that is not used afterwards, as
    // `kafka_List_destroy` requires.
    unsafe { kafka_List_destroy(element as *mut kafka_List_t) };
}

/// Increase the partition count for a topic to the given `total_count`. The assignment of new
/// replicas to brokers will be decided by the broker.
///
/// # Parameters
///
/// - `total_count` - The total number of partitions after the operation succeeds.
///
/// # Returns
///
/// The new partitions.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with `kafka_admin_NewPartitions_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_NewPartitions_increase_to(total_count: i32) -> *mut kafka_admin_NewPartitions_t {
    box_new_partitions(NewPartitions::increase_to(total_count))
}

/// Increase the partition count for a topic to the given `total_count` assigning the new
/// partitions according to the given `new_assignments`. The length of the given
/// `new_assignments` should equal `total_count - oldCount`, since the assignment of existing
/// partitions are not changed. Each inner list of `new_assignments` should have a length equal to
/// the topic's replication factor. The first broker id in each inner list is the "preferred
/// replica".
///
/// # Parameters
///
/// - `total_count` - The total number of partitions after the operation succeeds.
/// - `new_assignments` - The replica assignments for the new partitions.
///
/// # Returns
///
/// The new partitions.
///
/// # Safety
///
/// `new_assignments` must be `NULL`, read as empty, or a valid borrowed list of
/// `const kafka_List_t *`, one per new partition, whose elements point at `int32_t` broker ids,
/// all kept alive and not mutated for this call; it is copied.
///
/// The returned handle is owned by the caller, freed with `kafka_admin_NewPartitions_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_increase_to_with_new_assignments(
    total_count: i32,
    new_assignments: *const kafka_List_t,
) -> *mut kafka_admin_NewPartitions_t {
    // SAFETY: Per this function's `# Safety`, `new_assignments` is null or a valid list, alive and
    // not mutated for this call, as `list_elements` requires; the borrowed slice is dropped at the
    // end of this statement.
    let new_assignments = unsafe { list_elements(new_assignments) }
        .iter()
        // SAFETY: Per this function's `# Safety`, each element of `new_assignments` is a valid list
        // whose elements point at `int32_t`s, alive and not mutated for this call, as `list_i32`
        // requires.
        .map(|&element| unsafe { list_i32(element as *const kafka_List_t) })
        .collect();
    box_new_partitions(NewPartitions::increase_to_with_new_assignments(total_count, new_assignments))
}

/// Returns the total number of partitions after the operation succeeds.
///
/// # Safety
///
/// `self_` must be a valid new-partitions handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_total_count(self_: *const kafka_admin_NewPartitions_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid new-partitions handle, kept alive
    // for this call, as `new_partitions_ref` requires.
    unsafe { new_partitions_ref(self_) }.total_count()
}

/// The replica assignments for the new partitions, or `NULL` if the assignment will be done by
/// the controller.
///
/// # Returns
///
/// A list of `kafka_List_t *` of `int32_t *` broker ids, in the order given, or `NULL`.
///
/// # Safety
///
/// `self_` must be a valid new-partitions handle. The returned list is owned, freed together with
/// its elements by `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_assignments(
    self_: *const kafka_admin_NewPartitions_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid new-partitions handle, kept alive
    // for this call, as `new_partitions_ref` requires.
    match unsafe { new_partitions_ref(self_) }.assignments() {
        None => std::ptr::null_mut(),
        Some(assignments) => {
            let elements = assignments.iter().map(|brokers| box_i32_list(brokers) as *mut c_void).collect();
            box_list(elements, Some(destroy_list_element))
        },
    }
}

/// Returns the text form of the new partitions.
///
/// # Safety
///
/// `self_` must be a valid new-partitions handle. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_to_string(self_: *const kafka_admin_NewPartitions_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid new-partitions handle, kept alive
    // for this call, as `new_partitions_ref` requires.
    into_c_string(&unsafe { new_partitions_ref(self_) }.to_string())
}

/// Frees an owned handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_destroy(self_: *mut kafka_admin_NewPartitions_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards; every handle is the `Box::into_raw` of a `Box<NewPartitionsInner>`
        // (`box_new_partitions`), so `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut NewPartitionsInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::{kafka_List_add, kafka_List_get, kafka_List_new, kafka_List_size, kafka_string_destroy};

    #[test]
    fn increase_to_without_assignments() {
        let partitions = kafka_admin_NewPartitions_increase_to(4);
        // SAFETY: Test code: `partitions` is the owned handle
        // `kafka_admin_NewPartitions_increase_to` returned, used only on this thread and destroyed
        // once, last; `s` is the owned string `kafka_admin_NewPartitions_to_string` returned, read
        // while alive and freed once with `kafka_string_destroy`; destroying null is a no-op.
        unsafe {
            let view = new_partitions_ref(partitions);
            assert_eq!((view.total_count(), view.assignments()), (4, None));
            assert_eq!(kafka_admin_NewPartitions_total_count(partitions), 4);
            assert!(kafka_admin_NewPartitions_assignments(partitions).is_null());
            let s = kafka_admin_NewPartitions_to_string(partitions);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), "(totalCount=4, newAssignments=None)");
            kafka_string_destroy(s);
            kafka_admin_NewPartitions_destroy(partitions);
            kafka_admin_NewPartitions_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn increase_to_with_assignments_copies_the_nested_lists() {
        // SAFETY: Test code: `inner` and `outer` are lists that own nothing, `inner` holding
        // pointers to the locals `b1` and `b2`, which outlive it, and `outer` holding `inner`; both
        // stay alive until the copy is made, then each is destroyed once, `outer` first, which
        // leaves `inner` alone. `partitions` is the owned handle the factory returned, destroyed
        // once, last; `assignments` is the owned list `kafka_admin_NewPartitions_assignments`
        // returned, whose elements are lists of `int32_t *`, read while alive and destroyed once,
        // which frees `first` with it.
        unsafe {
            let inner = kafka_List_new();
            let (mut b1, mut b2) = (1_i32, 2_i32);
            kafka_List_add(inner, &mut b1 as *mut i32 as *mut c_void);
            kafka_List_add(inner, &mut b2 as *mut i32 as *mut c_void);
            let outer = kafka_List_new();
            kafka_List_add(outer, inner as *mut c_void);
            let partitions = kafka_admin_NewPartitions_increase_to_with_new_assignments(2, outer);
            kafka_List_destroy(outer);
            kafka_List_destroy(inner);

            let view = new_partitions_ref(partitions);
            assert_eq!((view.total_count(), view.assignments()), (2, Some(&vec![vec![1, 2]])));
            let assignments = kafka_admin_NewPartitions_assignments(partitions);
            assert_eq!(kafka_List_size(assignments), 1);
            let first = kafka_List_get(assignments, 0) as *const kafka_List_t;
            assert_eq!(kafka_List_size(first), 2);
            assert_eq!(*(kafka_List_get(first, 0) as *const i32), 1);
            assert_eq!(*(kafka_List_get(first, 1) as *const i32), 2);
            kafka_List_destroy(assignments);
            kafka_admin_NewPartitions_destroy(partitions);
        }
    }
}
