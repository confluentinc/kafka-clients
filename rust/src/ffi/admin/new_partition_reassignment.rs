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

//! `kafka_admin_NewPartitionReassignment_t`:
//! `org.apache.kafka.clients.admin.NewPartitionReassignment` (CLAUDE.md §4).
//! A `List<Integer>` crosses as a `kafka_List_t` of `int32_t *`.

use std::ffi::c_void;

use crate::admin::NewPartitionReassignment;
use crate::ffi::admin::out_slot;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::util::{box_list, destroy_boxed, kafka_List_t, list_elements};

/// Opaque handle to a [`NewPartitionReassignment`].
#[repr(C)]
pub struct kafka_admin_NewPartitionReassignment_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_NewPartitionReassignment_t`] points at.
pub(crate) struct NewPartitionReassignmentInner {
    reassignment: NewPartitionReassignment,
}

/// The value behind a handle.
///
/// # Safety
///
/// `reassignment` must be a valid new-partition-reassignment handle.
pub(crate) unsafe fn new_partition_reassignment_ref<'a>(
    reassignment: *const kafka_admin_NewPartitionReassignment_t,
) -> &'a NewPartitionReassignment {
    &unsafe { &*(reassignment as *const NewPartitionReassignmentInner) }.reassignment
}

/// Hands `reassignment` to C as an owned handle, freed with
/// [`kafka_admin_NewPartitionReassignment_destroy`].
pub(crate) fn box_new_partition_reassignment(
    reassignment: NewPartitionReassignment,
) -> *mut kafka_admin_NewPartitionReassignment_t {
    Box::into_raw(Box::new(NewPartitionReassignmentInner { reassignment }))
        as *mut kafka_admin_NewPartitionReassignment_t
}

/// `new NewPartitionReassignment(List<Integer> targetReplicas)`: a borrowed
/// list of `const int32_t *` broker ids, copied (`NULL` reads as empty).
/// Delivers the owned handle through `out_new` (freed with
/// [`kafka_admin_NewPartitionReassignment_destroy`]), or returns the owned
/// `IllegalArgumentException` translation when the list is empty.
///
/// # Safety
///
/// `target_replicas` must be null or a valid list of `int32_t *` and
/// `out_new` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitionReassignment_new(
    target_replicas: *const kafka_List_t,
    out_new: *mut *mut kafka_admin_NewPartitionReassignment_t,
) -> *mut kafka_common_Error_t {
    let target_replicas = unsafe { list_elements(target_replicas) }
        .iter()
        .map(|&element| unsafe { *(element as *const i32) })
        .collect();
    unsafe {
        out_slot(
            NewPartitionReassignment::new(target_replicas),
            out_new,
            box_new_partition_reassignment,
        )
    }
}

/// `targetReplicas()`: an owned list of owned `int32_t *` broker ids, in the
/// order given, freed together with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid new-partition-reassignment handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitionReassignment_target_replicas(
    self_: *const kafka_admin_NewPartitionReassignment_t,
) -> *mut kafka_List_t {
    let elements = unsafe { new_partition_reassignment_ref(self_) }
        .target_replicas()
        .iter()
        .map(|&replica| Box::into_raw(Box::new(replica)) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_boxed::<i32>))
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitionReassignment_destroy(
    self_: *mut kafka_admin_NewPartitionReassignment_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut NewPartitionReassignmentInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::{kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size};

    #[test]
    fn new_rejects_an_empty_list_and_copies_a_full_one() {
        let mut reassignment = ptr::null_mut();
        unsafe {
            let error = kafka_admin_NewPartitionReassignment_new(ptr::null(), &mut reassignment);
            assert!(!error.is_null());
            assert_eq!(
                error_ref(error).error.message(),
                "Cannot create a new partition reassignment without any replicas"
            );
            kafka_common_Error_destroy(error);

            let list = kafka_List_new();
            let (mut b1, mut b2) = (5_i32, 7_i32);
            kafka_List_add(list, &mut b1 as *mut i32 as *mut c_void);
            kafka_List_add(list, &mut b2 as *mut i32 as *mut c_void);
            assert!(kafka_admin_NewPartitionReassignment_new(list, &mut reassignment).is_null());
            kafka_List_destroy(list);
            assert_eq!(
                *new_partition_reassignment_ref(reassignment),
                NewPartitionReassignment::new(vec![5, 7]).unwrap()
            );
            let replicas = kafka_admin_NewPartitionReassignment_target_replicas(reassignment);
            assert_eq!(kafka_List_size(replicas), 2);
            assert_eq!(*(kafka_List_get(replicas, 1) as *const i32), 7);
            kafka_List_destroy(replicas);
            kafka_admin_NewPartitionReassignment_destroy(reassignment);
            kafka_admin_NewPartitionReassignment_destroy(ptr::null_mut());
        }
    }
}
