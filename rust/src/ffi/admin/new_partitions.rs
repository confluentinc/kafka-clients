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
/// `partitions` must be a valid new-partitions handle.
pub(crate) unsafe fn new_partitions_ref<'a>(partitions: *const kafka_admin_NewPartitions_t) -> &'a NewPartitions {
    &unsafe { &*(partitions as *const NewPartitionsInner) }.partitions
}

/// Hands `partitions` to C as an owned handle, freed with
/// [`kafka_admin_NewPartitions_destroy`].
pub(crate) fn box_new_partitions(partitions: NewPartitions) -> *mut kafka_admin_NewPartitions_t {
    Box::into_raw(Box::new(NewPartitionsInner { partitions })) as *mut kafka_admin_NewPartitions_t
}

/// Reads a borrowed list of `const int32_t *` into values; null reads as empty.
unsafe fn list_i32(list: *const kafka_List_t) -> Vec<i32> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { *(element as *const i32) })
        .collect()
}

/// Hands `values` to C as an owned list of `int32_t *`.
fn box_i32_list(values: &[i32]) -> *mut kafka_List_t {
    let elements = values.iter().map(|&v| Box::into_raw(Box::new(v)) as *mut c_void).collect();
    box_list(elements, Some(destroy_boxed::<i32>))
}

/// Frees a `kafka_List_t *` element of an owned container.
unsafe fn destroy_list_element(element: *mut c_void) {
    unsafe { kafka_List_destroy(element as *mut kafka_List_t) };
}

/// `NewPartitions.increaseTo(int totalCount)`: the broker assigns the new
/// partitions' replicas. Owned, freed with
/// [`kafka_admin_NewPartitions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_NewPartitions_increase_to(total_count: i32) -> *mut kafka_admin_NewPartitions_t {
    box_new_partitions(NewPartitions::increase_to(total_count))
}

/// `NewPartitions.increaseTo(int totalCount, List<List<Integer>> newAssignments)`:
/// a borrowed list of `const kafka_List_t *` of `const int32_t *` broker ids,
/// one inner list per new partition, copied (`NULL` reads as empty). Owned,
/// freed with [`kafka_admin_NewPartitions_destroy`].
///
/// # Safety
///
/// `new_assignments` must be null or a valid list of that shape.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_increase_to_with_new_assignments(
    total_count: i32,
    new_assignments: *const kafka_List_t,
) -> *mut kafka_admin_NewPartitions_t {
    let new_assignments = unsafe { list_elements(new_assignments) }
        .iter()
        .map(|&element| unsafe { list_i32(element as *const kafka_List_t) })
        .collect();
    box_new_partitions(NewPartitions::increase_to_with_new_assignments(total_count, new_assignments))
}

/// `totalCount()`.
///
/// # Safety
///
/// `self_` must be a valid new-partitions handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_total_count(self_: *const kafka_admin_NewPartitions_t) -> i32 {
    unsafe { new_partitions_ref(self_) }.total_count()
}

/// `assignments()`: an owned list of owned `kafka_List_t *` of owned
/// `int32_t *` broker ids, in the order given, freed together with
/// `kafka_List_destroy`; `NULL` when the broker assigns the replicas (Java
/// returns null).
///
/// # Safety
///
/// `self_` must be a valid new-partitions handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_assignments(
    self_: *const kafka_admin_NewPartitions_t,
) -> *mut kafka_List_t {
    match unsafe { new_partitions_ref(self_) }.assignments() {
        None => std::ptr::null_mut(),
        Some(assignments) => {
            let elements = assignments.iter().map(|brokers| box_i32_list(brokers) as *mut c_void).collect();
            box_list(elements, Some(destroy_list_element))
        },
    }
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid new-partitions handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_to_string(self_: *const kafka_admin_NewPartitions_t) -> *mut c_char {
    into_c_string(&unsafe { new_partitions_ref(self_) }.to_string())
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitions_destroy(self_: *mut kafka_admin_NewPartitions_t) {
    if !self_.is_null() {
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
        unsafe {
            assert_eq!(*new_partitions_ref(partitions), NewPartitions::increase_to(4));
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

            assert_eq!(
                *new_partitions_ref(partitions),
                NewPartitions::increase_to_with_new_assignments(2, vec![vec![1, 2]])
            );
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
