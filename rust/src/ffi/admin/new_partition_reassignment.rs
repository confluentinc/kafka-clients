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
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
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
/// `reassignment` must be a valid new-partition-reassignment handle, kept
/// alive for `'a`.
pub(crate) unsafe fn new_partition_reassignment_ref<'a>(
    reassignment: *const kafka_admin_NewPartitionReassignment_t,
) -> &'a NewPartitionReassignment {
    // SAFETY: Per this function's `# Safety`, `reassignment` is a valid new-partition-reassignment
    // handle kept alive for `'a`; every `kafka_admin_NewPartitionReassignment_t` is the
    // `Box::into_raw` of a `Box<NewPartitionReassignmentInner>` (`box_new_partition_reassignment`,
    // the only producer), so the shared reference is to a live `NewPartitionReassignmentInner`,
    // which no entry point mutates.
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
/// Returns the owned handle (freed with
/// [`kafka_admin_NewPartitionReassignment_destroy`]), or stores the owned
/// `IllegalArgumentException` translation in `*out_err` and returns `NULL` when
/// the list is empty.
///
/// # Safety
///
/// `target_replicas` must be null or a valid list whose elements point at
/// `int32_t`s, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitionReassignment_new(
    target_replicas: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_NewPartitionReassignment_t {
    // SAFETY: Per this function's `# Safety`, `target_replicas` is null or a valid list, alive and
    // not mutated for this call, as `list_elements` requires; the borrowed slice is dropped at the
    // end of this statement.
    let target_replicas = unsafe { list_elements(target_replicas) }
        .iter()
        // SAFETY: Per this function's `# Safety`, each element of `target_replicas` points at a
        // live `int32_t`, read by copy while the list is alive.
        .map(|&element| unsafe { *(element as *const i32) })
        .collect();
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires.
    unsafe {
        deliver_value(
            NewPartitionReassignment::new(target_replicas),
            out_err,
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitionReassignment_target_replicas(
    self_: *const kafka_admin_NewPartitionReassignment_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid new-partition-reassignment handle,
    // kept alive for this call, as `new_partition_reassignment_ref` requires.
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
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewPartitionReassignment_destroy(
    self_: *mut kafka_admin_NewPartitionReassignment_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards; every handle is the `Box::into_raw` of a `Box<NewPartitionReassignmentInner>`
        // (`box_new_partition_reassignment`), so `from_raw` reclaims it exactly once.
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
        let mut error = ptr::null_mut();
        // SAFETY: Test code: `error` is a valid slot pointing at null before each call, holding
        // the owned error the first call stored, read while alive, destroyed once and reset;
        // `reassignment` is the owned handle the second `kafka_admin_NewPartitionReassignment_new`
        // returned, destroyed once, last; `list` is
        // an empty list that owns nothing, holding pointers to the locals `b1` and `b2`, which
        // outlive it, and is destroyed once after the copy; `replicas` is the owned list
        // `kafka_admin_NewPartitionReassignment_target_replicas` returned, whose elements point at
        // `int32_t`s, read while alive and destroyed once; destroying null is a no-op.
        unsafe {
            let failed = kafka_admin_NewPartitionReassignment_new(ptr::null(), &mut error);
            assert!(failed.is_null());
            assert!(!error.is_null());
            assert_eq!(
                error_ref(error).error.message(),
                "Cannot create a new partition reassignment without any replicas"
            );
            kafka_common_Error_destroy(error);
            error = ptr::null_mut();

            let list = kafka_List_new();
            let (mut b1, mut b2) = (5_i32, 7_i32);
            kafka_List_add(list, &mut b1 as *mut i32 as *mut c_void);
            kafka_List_add(list, &mut b2 as *mut i32 as *mut c_void);
            let reassignment = kafka_admin_NewPartitionReassignment_new(list, &mut error);
            assert!(error.is_null());
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
